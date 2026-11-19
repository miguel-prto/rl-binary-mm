"""Assembly of Polymarket CLOB events into rows of the normalised tables.

Pure: no file I/O. An :class:`Assembler` is fed the records of ONE raw file
(one connection, one UTC hour) in order and accumulates rows column by column;
:meth:`Assembler.take_batches` hands them over as Arrow record batches and
clears the buffers, so a writer can drain it every N lines and memory stays
bounded whatever the file size.

What happens to each line (``seq`` is its 0-based line number in the file):

- ``in`` frames go through :func:`parse_frame`; each event's ``asset_id`` is
  mapped to ``(market_id, outcome)`` with the asset map of the ``markets``
  table. Events of unknown assets are dropped and counted: the raw layer keeps
  them, so nothing is lost and the file can be re-normalised.
- Price changes: each frame carries one entry per token of a market. When the
  Up and Down entries are exact mirrors (same size, opposite side, prices
  adding up to 1, which held for 100 % of 5.6 M pairs on 13 Nov), only the Up
  row is kept, with ``mirrored=True``. Otherwise every entry is kept, with
  ``mirrored=False``.
- Book snapshots: kept for the first snapshot of each token in the file, then
  for the first one of each new exchange minute (decision 3, 2026-11-14).
  Each file is thereby self-contained for book reconstruction. The others are
  redundant with the deltas (validated separately, from the raw layer).
- Trades and tick size changes: one row each.
- ``meta`` lines and sent frames other than ``PING`` become
  ``recorder_events`` rows; ``PING`` / ``PONG`` are dropped.
"""

from __future__ import annotations

import collections
from dataclasses import dataclass, field
from typing import Any

import orjson
import pyarrow as pa
from marketrec.raw import RawRecord

from rl_binary_mm.store.markets import AssetMap, Outcome
from rl_binary_mm.store.normalize.polymarket import (
    BookSnapshot,
    LevelChange,
    TickSizeChange,
    Trade,
    parse_frame,
)
from rl_binary_mm.store.schema import RECORDER_EVENTS, TABLES

PRICE_ONE_E4 = 10_000
MS_PER_MINUTE = 60_000

OUTPUTS: dict[str, pa.Schema] = {**TABLES, "recorder_events": RECORDER_EVENTS}


@dataclass
class AssembleStats:
    """Counters for one file, for logs and the quality report."""

    lines: int = 0
    events: collections.Counter[str] = field(default_factory=collections.Counter)
    mirrored_pairs: int = 0
    unmirrored_groups: int = 0  # price-change entries of one market that were not a mirror pair
    unknown_asset_events: int = 0
    unknown_assets: set[str] = field(default_factory=set)
    snapshots_seen: int = 0
    snapshots_kept: int = 0
    empty_snapshots_kept: int = 0  # kept snapshots with no levels: they produce no rows
    rows: collections.Counter[str] = field(default_factory=collections.Counter)


class Assembler:
    """Turns the records of one raw file into table rows (see module docstring)."""

    def __init__(self, asset_map: AssetMap) -> None:
        self._assets = asset_map
        self._conn_id: str | None = None
        self._seq = -1
        self._last_snapshot_minute: dict[str, int] = {}
        self._buffers: dict[str, dict[str, list[Any]]] = {
            name: {column: [] for column in schema.names} for name, schema in OUTPUTS.items()
        }
        self.stats = AssembleStats()

    # ----------------------------------------------------------------- public

    def feed(self, record: RawRecord) -> None:
        """Process the next line of the file."""
        if self._conn_id is None:
            self._conn_id = record.conn_id
        elif record.conn_id != self._conn_id:
            raise ValueError(
                f"one Assembler per raw file: got conn_id {record.conn_id!r} "
                f"after {self._conn_id!r}"
            )
        self._seq += 1
        self.stats.lines += 1

        if record.direction == "in":
            self._feed_events(record)
        elif record.frame != "PING":  # meta lines and subscribe/unsubscribe frames
            self._add_recorder_event(record)

    @property
    def buffered_rows(self) -> int:
        return sum(len(next(iter(cols.values()))) for cols in self._buffers.values())

    def take_batches(self) -> dict[str, pa.RecordBatch]:
        """Return the buffered rows of every table (possibly empty) and clear them."""
        batches = {}
        for name, schema in OUTPUTS.items():
            columns = self._buffers[name]
            batches[name] = pa.RecordBatch.from_pydict(columns, schema=schema)
            for values in columns.values():
                values.clear()
        return batches

    # ---------------------------------------------------------------- private

    def _feed_events(self, record: RawRecord) -> None:
        changes: list[LevelChange] = []
        for event in parse_frame(record.frame):
            self.stats.events[type(event).__name__] += 1
            if isinstance(event, LevelChange):
                changes.append(event)  # handled together: mirrors span entries
                continue
            located = self._locate(event.asset_id)
            if located is None:
                continue
            if isinstance(event, BookSnapshot):
                self._add_snapshot(record, event, located)
            elif isinstance(event, Trade):
                self._add(
                    "trades",
                    record,
                    event.ts_exchange_ms,
                    located,
                    side=event.side,
                    price_e4=event.price_e4,
                    size_e6=event.size_e6,
                    fee_rate_bps=event.fee_rate_bps,
                    tx_hash=event.tx_hash,
                )
            elif isinstance(event, TickSizeChange):
                self._add(
                    "tick_size_changes",
                    record,
                    event.ts_exchange_ms,
                    located,
                    old_tick_e4=event.old_tick_e4,
                    new_tick_e4=event.new_tick_e4,
                )
        if changes:
            self._add_level_changes(record, changes)

    def _locate(self, asset_id: str) -> tuple[str, Outcome] | None:
        located = self._assets.get(asset_id)
        if located is None:
            self.stats.unknown_asset_events += 1
            self.stats.unknown_assets.add(asset_id)
        return located

    def _add_level_changes(self, record: RawRecord, changes: list[LevelChange]) -> None:
        by_market: dict[str, list[tuple[LevelChange, Outcome]]] = collections.defaultdict(list)
        for change in changes:
            located = self._locate(change.asset_id)
            if located is not None:
                by_market[located[0]].append((change, located[1]))

        for market_id, entries in by_market.items():
            pair = _mirror_pair(entries)
            if pair is not None:
                self.stats.mirrored_pairs += 1
                self._add_level_change(record, pair, market_id, "up", mirrored=True)
                continue
            self.stats.unmirrored_groups += 1
            for change, outcome in entries:
                self._add_level_change(record, change, market_id, outcome, mirrored=False)

    def _add_level_change(
        self,
        record: RawRecord,
        change: LevelChange,
        market_id: str,
        outcome: Outcome,
        *,
        mirrored: bool,
    ) -> None:
        self._add(
            "book_deltas",
            record,
            change.ts_exchange_ms,
            (market_id, outcome),
            side=change.side,
            price_e4=change.price_e4,
            size_e6=change.size_e6,
            mirrored=mirrored,
        )

    def _add_snapshot(
        self, record: RawRecord, book: BookSnapshot, located: tuple[str, Outcome]
    ) -> None:
        self.stats.snapshots_seen += 1
        minute = book.ts_exchange_ms // MS_PER_MINUTE
        if self._last_snapshot_minute.get(book.asset_id) == minute:
            return
        self._last_snapshot_minute[book.asset_id] = minute
        self.stats.snapshots_kept += 1
        if not book.bids and not book.asks:
            self.stats.empty_snapshots_kept += 1
        for side, levels in (("bid", book.bids), ("ask", book.asks)):
            for price_e4, size_e6 in levels:
                self._add(
                    "book_snapshots",
                    record,
                    book.ts_exchange_ms,
                    located,
                    side=side,
                    price_e4=price_e4,
                    size_e6=size_e6,
                )

    def _add_recorder_event(self, record: RawRecord) -> None:
        if record.direction == "meta":
            kind = orjson.loads(record.frame).get("event", "unknown")
        else:  # a frame we sent: the initial subscription or a later (un)subscribe
            kind = orjson.loads(record.frame).get("operation", "subscribe")
        self._append(
            "recorder_events",
            ts_recv_ns=record.recv_ns,
            conn_id=record.conn_id,
            seq=self._seq,
            kind=kind,
            detail=record.frame,
        )

    def _add(
        self,
        table: str,
        record: RawRecord,
        ts_exchange_ms: int,
        located: tuple[str, Outcome],
        **values: Any,
    ) -> None:
        self._append(
            table,
            ts_exchange_ms=ts_exchange_ms,
            ts_recv_ns=record.recv_ns,
            conn_id=record.conn_id,
            seq=self._seq,
            market_id=located[0],
            outcome=located[1],
            **values,
        )

    def _append(self, table: str, **values: Any) -> None:
        columns = self._buffers[table]
        for column, value in values.items():
            columns[column].append(value)
        self.stats.rows[table] += 1


def _mirror_pair(entries: list[tuple[LevelChange, Outcome]]) -> LevelChange | None:
    """The Up entry if ``entries`` are exactly an Up entry and its Down mirror."""
    if len(entries) != 2:
        return None
    by_outcome = {outcome: change for change, outcome in entries}
    up, down = by_outcome.get("up"), by_outcome.get("down")
    if up is None or down is None:
        return None
    is_mirror = (
        up.size_e6 == down.size_e6
        and up.side != down.side
        and up.price_e4 + down.price_e4 == PRICE_ONE_E4
        and up.ts_exchange_ms == down.ts_exchange_ms
    )
    return up if is_mirror else None

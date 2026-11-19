"""The ``markets`` table: one row per Polymarket market, from recorded Gamma data.

The recorder writes the Gamma event of each market it subscribes to into the
``pm_gamma`` raw source, once per shard: after a reconnect the new connection
does not repeat the markets it already knew. The metadata needed to normalise
an hour of ``pm_clob`` frames can therefore sit in an earlier hour or another
connection's file, so this table is built from *all* ``pm_gamma`` files (about
2 MB a day) and rebuilt from scratch on every run.

Its main use downstream is the asset map: CLOB events carry a token id
(``asset_id``) and the normaliser needs ``(market_id, outcome)``.

Parsing and validation of a Gamma event are delegated to
``marketrec.discovery.polymarket.parse_gamma_event``, the same function the
recorder used to decide what to subscribe to; ``tests/test_raw_contract.py``
pins that dependency. It is strict: an event that is not a verified crypto
Up/Down market raises instead of being guessed.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Literal

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from marketrec.discovery.polymarket import parse_gamma_event
from marketrec.raw import RawRecord, iter_raw_records

from rl_binary_mm.store.schema import MARKETS

Outcome = Literal["up", "down"]
AssetMap = Mapping[str, tuple[str, Outcome]]  # asset_id -> (market_id, outcome)

GAMMA_SOURCE = "pm_gamma"
VENUE = "polymarket"

_SLUG = re.compile(r"(?P<asset>[a-z]+)-updown-(?P<window>\d+[mh])-(?P<start>\d+)")


@dataclass(frozen=True, slots=True)
class MarketRow:
    """One row of the ``markets`` table (columns as in ``schema.MARKETS``)."""

    market_id: str
    slug: str
    underlying: str
    window: str
    window_start_ms: int
    window_end_ms: int
    up_asset_id: str
    down_asset_id: str
    tick_size_e4: int
    resolution_source: str | None
    discovered_recv_ns: int


def parse_slug(slug: str) -> tuple[str, str]:
    """``'btc-updown-15m-1794528600'`` -> ``('btc', '15m')``."""
    match = _SLUG.fullmatch(slug)
    if match is None:
        raise ValueError(f"not a crypto Up/Down slug: {slug!r}")
    return match["asset"], match["window"]


def market_from_gamma(frame: str, recv_ns: int) -> MarketRow:
    """Build a row from one recorded Gamma event (the ``frame`` of a raw record)."""
    event = orjson.loads(frame)
    asset, window = parse_slug(event["slug"])
    spec = parse_gamma_event(event, asset=asset, window=window)
    up, down = spec.asset_ids
    return MarketRow(
        market_id=spec.market_id,
        slug=spec.slug,
        underlying=spec.underlying,
        window=window,
        window_start_ms=spec.window_start_ms,
        window_end_ms=spec.window_end_ms,
        up_asset_id=up,
        down_asset_id=down,
        tick_size_e4=spec.tick_size_e4,
        resolution_source=event.get("resolutionSource") or None,
        discovered_recv_ns=recv_ns,
    )


def collect_markets(records: Iterable[RawRecord]) -> list[MarketRow]:
    """One row per market from ``pm_gamma`` records, in any order.

    The same market is recorded again by every shard restart. The earliest
    copy is kept. The copies must agree except for when they were recorded and
    the tick size, which can change during the market (0.01 -> 0.001 near the
    extremes; seen once in 2,036 records on 12-13 Nov). The tick size in this
    table is the one at first discovery; ``tick_size_changes`` is the reference.
    Rows are sorted by ``(window_start_ms, slug)``.
    """
    by_id: dict[str, MarketRow] = {}
    for record in records:
        if record.direction != "in":
            continue
        row = market_from_gamma(record.frame, record.recv_ns)
        seen = by_id.get(row.market_id)
        if seen is None:
            by_id[row.market_id] = row
            continue
        if _identity(seen) != _identity(row):
            raise ValueError(f"conflicting Gamma records for market {row.market_id}")
        if row.discovered_recv_ns < seen.discovered_recv_ns:
            by_id[row.market_id] = row
    return sorted(by_id.values(), key=lambda r: (r.window_start_ms, r.slug))


def _identity(row: MarketRow) -> MarketRow:
    """The fields that must agree between two records of the same market."""
    return replace(row, tick_size_e4=0, discovered_recv_ns=0)


def gamma_files(data_dir: Path) -> list[Path]:
    """Every recorded ``pm_gamma`` file under ``data_dir`` (the recorder's data root)."""
    return sorted((data_dir / "raw" / GAMMA_SOURCE).glob("date=*/*.jsonl.zst"))


def read_markets(data_dir: Path) -> list[MarketRow]:
    """Build the market rows from every ``pm_gamma`` file under ``data_dir``."""
    return collect_markets(r for path in gamma_files(data_dir) for r in iter_raw_records(path))


def markets_table(rows: Iterable[MarketRow]) -> pa.Table:
    return pa.Table.from_pylist([asdict(row) for row in rows], schema=MARKETS)


def markets_path(data_dir: Path) -> Path:
    return data_dir / "normalized" / "markets" / f"venue={VENUE}" / "markets.parquet"


def write_markets(rows: Iterable[MarketRow], data_dir: Path) -> Path:
    """Write the table, replacing the previous one atomically. Returns its path."""
    path = markets_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(markets_table(rows), tmp, compression="zstd")
    tmp.replace(path)
    return path


def asset_map(rows: Iterable[MarketRow]) -> dict[str, tuple[str, Outcome]]:
    """Map each token id to ``(market_id, outcome)``."""
    return _asset_map((row.market_id, row.up_asset_id, row.down_asset_id) for row in rows)


def load_asset_map(data_dir: Path) -> dict[str, tuple[str, Outcome]]:
    """Asset map from the written ``markets`` table."""
    columns = ["market_id", "up_asset_id", "down_asset_id"]
    table = pq.read_table(markets_path(data_dir), columns=columns)
    return _asset_map(zip(*(table.column(c).to_pylist() for c in columns), strict=True))


def _asset_map(markets: Iterable[tuple[str, str, str]]) -> dict[str, tuple[str, Outcome]]:
    out: dict[str, tuple[str, Outcome]] = {}
    for market_id, up, down in markets:
        out[up] = (market_id, "up")
        out[down] = (market_id, "down")
    return out

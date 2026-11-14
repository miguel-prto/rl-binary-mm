"""Pure parsers for Polymarket CLOB market-channel frames.

One raw frame (the ``frame`` string of a ``RawRecord``) becomes a list of typed
events. The parsers are a 1:1 translation: no state, no I/O, no knowledge of
which token is "Up". Mapping ``asset_id`` to ``(market_id, outcome)``,
collapsing mirrored entries, thinning snapshots and writing Parquet are done by
the assembly step, which also adds ``ts_recv_ns`` and ``conn_id`` from the raw
record.

Units: prices are ``price_e4`` (dollars x 10^4), sizes ``size_e6`` (shares x
10^6), timestamps ``ts_exchange_ms`` (venue time, ms since the Unix epoch,
UTC). Observed precision: prices up to 3 decimals, sizes up to 6.

Message facts used here are in market-recorder's ``docs/verification_log.md``:
``price_change.size`` is the new absolute size of the level; the initial dump
after subscribing is a JSON array of ``book`` events; later messages are single
objects; the server answers the text ``PING`` with ``PONG``.

Dropped on purpose:
- ``hash`` on books and price changes (decision 1, 2026-11-14): it dominates
  the Parquet size and the book is validated by reconstruction instead.
- ``best_bid`` / ``best_ask`` inside price changes and the ``best_bid_ask``
  event: both are derivable from the book.
- ``new_market`` (all new markets, not only ours; discovery uses Gamma) and
  ``market_resolved`` (rarely arrives before we unsubscribe; resolution will
  be fetched from Gamma).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import orjson

from rl_binary_mm.store.fixed import to_scaled

PRICE_SCALE = 4
SIZE_SCALE = 6

Side = Literal["bid", "ask"]
TradeSide = Literal["buy", "sell"]

# In a price change, BUY is a level on the bid side and SELL on the ask side.
_BOOK_SIDES: dict[str, Side] = {"BUY": "bid", "SELL": "ask"}

# In a trade, ``side`` is kept as the venue reports it. Its meaning is NOT
# verified: on 2026-11-12 22:00 UTC (6,823 trades), 79 % were a BUY at the
# best ask or a SELL at the best bid in force just before, which suggests
# the taker's direction, but the rest do not fit (possibly complementary
# matching between the Up and Down tokens). Check the docs before relying on it.
_TRADE_SIDES: dict[str, TradeSide] = {"BUY": "buy", "SELL": "sell"}

IGNORED_EVENT_TYPES = frozenset({"best_bid_ask", "new_market", "market_resolved"})


@dataclass(frozen=True, slots=True)
class LevelChange:
    """One entry of a ``price_change``: the level now holds ``size_e6`` (0 = gone)."""

    ts_exchange_ms: int
    market_id: str  # condition_id
    asset_id: str
    side: Side
    price_e4: int
    size_e6: int


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    """A ``book`` event: every level of one token's book, in the order received."""

    ts_exchange_ms: int
    market_id: str
    asset_id: str
    bids: tuple[tuple[int, int], ...]  # (price_e4, size_e6)
    asks: tuple[tuple[int, int], ...]


@dataclass(frozen=True, slots=True)
class Trade:
    """A ``last_trade_price`` event: one fill against the book."""

    ts_exchange_ms: int
    market_id: str
    asset_id: str
    side: TradeSide  # as reported; meaning unverified (see _TRADE_SIDES)
    price_e4: int
    size_e6: int
    fee_rate_bps: int
    tx_hash: str


@dataclass(frozen=True, slots=True)
class TickSizeChange:
    """A ``tick_size_change`` event (e.g. 0.01 -> 0.001 near the extremes)."""

    ts_exchange_ms: int
    market_id: str
    asset_id: str
    old_tick_e4: int
    new_tick_e4: int


Event = LevelChange | BookSnapshot | Trade | TickSizeChange


def parse_frame(frame: str) -> list[Event]:
    """Parse one received frame into events.

    Accepts a single event object or a JSON array of them (the initial dump).
    ``PONG`` and the ignored event types give ``[]``. Unknown event types and
    missing fields raise, so that a format change cannot pass unnoticed.
    """
    if frame == "PONG":
        return []
    obj = orjson.loads(frame)
    items = obj if isinstance(obj, list) else [obj]
    events: list[Event] = []
    for item in items:
        events.extend(_parse_event(item))
    return events


def _parse_event(obj: dict[str, Any]) -> list[Event]:
    event_type = obj.get("event_type")
    if event_type == "price_change":
        return list(_parse_price_change(obj))
    if event_type == "book":
        return [_parse_book(obj)]
    if event_type == "last_trade_price":
        return [_parse_last_trade_price(obj)]
    if event_type == "tick_size_change":
        return [_parse_tick_size_change(obj)]
    if event_type in IGNORED_EVENT_TYPES:
        return []
    raise ValueError(f"unknown Polymarket event_type: {event_type!r}")


def _parse_price_change(obj: dict[str, Any]) -> list[LevelChange]:
    ts = int(obj["timestamp"])
    market_id = obj["market"]
    return [
        LevelChange(
            ts_exchange_ms=ts,
            market_id=market_id,
            asset_id=entry["asset_id"],
            side=_BOOK_SIDES[entry["side"]],
            price_e4=to_scaled(entry["price"], PRICE_SCALE),
            size_e6=to_scaled(entry["size"], SIZE_SCALE),
        )
        for entry in obj["price_changes"]
    ]


def _parse_book(obj: dict[str, Any]) -> BookSnapshot:
    return BookSnapshot(
        ts_exchange_ms=int(obj["timestamp"]),
        market_id=obj["market"],
        asset_id=obj["asset_id"],
        bids=_parse_levels(obj["bids"]),
        asks=_parse_levels(obj["asks"]),
    )


def _parse_levels(levels: list[dict[str, str]]) -> tuple[tuple[int, int], ...]:
    return tuple(
        (to_scaled(level["price"], PRICE_SCALE), to_scaled(level["size"], SIZE_SCALE))
        for level in levels
    )


def _parse_last_trade_price(obj: dict[str, Any]) -> Trade:
    return Trade(
        ts_exchange_ms=int(obj["timestamp"]),
        market_id=obj["market"],
        asset_id=obj["asset_id"],
        side=_TRADE_SIDES[obj["side"]],
        price_e4=to_scaled(obj["price"], PRICE_SCALE),
        size_e6=to_scaled(obj["size"], SIZE_SCALE),
        fee_rate_bps=int(obj["fee_rate_bps"]),
        tx_hash=obj["transaction_hash"],
    )


def _parse_tick_size_change(obj: dict[str, Any]) -> TickSizeChange:
    return TickSizeChange(
        ts_exchange_ms=int(obj["timestamp"]),
        market_id=obj["market"],
        asset_id=obj["asset_id"],
        old_tick_e4=to_scaled(obj["old_tick_size"], PRICE_SCALE),
        new_tick_e4=to_scaled(obj["new_tick_size"], PRICE_SCALE),
    )

"""Parsers of Polymarket CLOB frames, tested on real recorded frames.

The fixture holds one frame per case, copied verbatim from
``data/raw/pm_clob/date=2026-11-12/pm_clob_22_20261112T224531-6f50.jsonl.zst``.
"""

from pathlib import Path

import orjson
import pytest

from rl_binary_mm.store.normalize.polymarket import (
    BookSnapshot,
    LevelChange,
    TickSizeChange,
    Trade,
    parse_frame,
)

FIXTURES = Path(__file__).parent / "fixtures" / "pm_clob_frames.jsonl"
FRAMES: dict[str, str] = {
    rec["name"]: rec["frame"]
    for rec in map(orjson.loads, FIXTURES.read_text(encoding="utf-8").splitlines())
}

BTC_5M = "0xad4f957246360f6bbe3f0e13b02645b66f4d8f487f54a9f034aa652f3fb46918"
BTC_5M_A = "8184144964907141096563128437515478043857459391118595175897395568769954534314"
BTC_5M_B = "22242433910742073955562533561715525506739883838710302829678990855721505723782"


def test_price_change_gives_one_level_change_per_entry() -> None:
    assert parse_frame(FRAMES["price_change"]) == [
        LevelChange(
            ts_exchange_ms=1794523531710,
            market_id=BTC_5M,
            asset_id=BTC_5M_A,
            side="bid",
            price_e4=2800,
            size_e6=453_730_000,
        ),
        LevelChange(
            ts_exchange_ms=1794523531710,
            market_id=BTC_5M,
            asset_id=BTC_5M_B,
            side="ask",
            price_e4=7200,
            size_e6=453_730_000,
        ),
    ]


def test_price_change_at_the_fine_tick() -> None:
    a, b = parse_frame(FRAMES["price_change_fine_tick"])
    assert isinstance(a, LevelChange) and isinstance(b, LevelChange)
    assert (a.side, a.price_e4, a.size_e6) == ("bid", 9990, 25_003_000_000)
    assert (b.side, b.price_e4, b.size_e6) == ("ask", 10, 25_003_000_000)
    # The two entries are mirrors: same size, opposite side, prices adding up to 1.
    assert a.price_e4 + b.price_e4 == 10_000


def test_book_keeps_every_level_in_the_order_received() -> None:
    [book] = parse_frame(FRAMES["book"])
    assert isinstance(book, BookSnapshot)
    assert (book.ts_exchange_ms, book.market_id, book.asset_id) == (
        1794523789364,
        BTC_5M,
        BTC_5M_A,
    )
    assert len(book.bids) == 12 and len(book.asks) == 21
    # Polymarket sends both sides worst price first.
    assert book.bids[0] == (100, 3_091_010_000)
    assert book.bids[-1] == (2000, 201_460_000)
    assert book.asks[0] == (9900, 4_511_130_000)
    assert book.asks[-1] == (5300, 39_100_000)


def test_initial_dump_is_an_array_of_books() -> None:
    events = parse_frame(FRAMES["initial_array"])
    assert len(events) == 2
    assert all(isinstance(e, BookSnapshot) for e in events)
    first = events[0]
    assert isinstance(first, BookSnapshot)
    assert first.ts_exchange_ms == 1794523801662
    assert first.bids[0] == (100, 96_490_440_000)


def test_last_trade_price() -> None:
    assert parse_frame(FRAMES["last_trade_price"]) == [
        Trade(
            ts_exchange_ms=1794523531850,
            market_id=BTC_5M,
            asset_id=BTC_5M_B,
            side="buy",
            price_e4=6200,
            size_e6=6_000_000,
            fee_rate_bps=0,
            tx_hash="0x668ff09842e5c123e2ca107d988a6a96562f71c65af3d11b8e14622caf4c73c4",
        )
    ]


def test_tick_size_change() -> None:
    [event] = parse_frame(FRAMES["tick_size_change"])
    assert isinstance(event, TickSizeChange)
    assert (event.ts_exchange_ms, event.old_tick_e4, event.new_tick_e4) == (
        1794523617376,
        100,
        10,
    )


@pytest.mark.parametrize("name", ["best_bid_ask", "new_market", "market_resolved", "pong"])
def test_ignored_frames_give_no_events(name: str) -> None:
    assert parse_frame(FRAMES[name]) == []


def test_unknown_event_type_raises() -> None:
    with pytest.raises(ValueError, match="unknown Polymarket event_type"):
        parse_frame('{"event_type": "something_new", "market": "0x1"}')


def test_missing_field_raises() -> None:
    frame = orjson.loads(FRAMES["last_trade_price"])
    del frame["transaction_hash"]
    with pytest.raises(KeyError):
        parse_frame(orjson.dumps(frame).decode())


def test_unexpected_precision_raises() -> None:
    frame = FRAMES["price_change"].replace('"price":"0.28"', '"price":"0.28001"')
    with pytest.raises(ValueError, match="more than 4 decimals"):
        parse_frame(frame)

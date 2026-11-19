"""Assembly of parsed events into table rows, on real frames.

The asset map is synthetic except for the fine-tick market, where the
recorded ``market_resolved`` event names 17993...222 as the Down token.
"""

import orjson
import pytest
from marketrec.raw import Direction, RawRecord

from rl_binary_mm.store.markets import AssetMap, Outcome
from rl_binary_mm.store.normalize.assemble import OUTPUTS, Assembler

BTC_5M = "0xad4f957246360f6bbe3f0e13b02645b66f4d8f487f54a9f034aa652f3fb46918"
BTC_5M_A = "8184144964907141096563128437515478043857459391118595175897395568769954534314"
BTC_5M_B = "22242433910742073955562533561715525506739883838710302829678990855721505723782"
FINE = "0x0d6bc05e20aaf93ecb051c78d080bffb43ddeebebde9bce7e1ebce97123d281e"
FINE_UP = "71181836240215394008734567686633689631795531931500535223961402931627942600071"
FINE_DOWN = "17993312019739972432536526956453233151833658758112475424626873084665959161222"

ASSETS: AssetMap = {
    BTC_5M_A: (BTC_5M, "up"),
    BTC_5M_B: (BTC_5M, "down"),
    FINE_UP: (FINE, "up"),
    FINE_DOWN: (FINE, "down"),
}
RECV_NS = 1_794_523_531_758_028_520


def rec(frame: str, direction: Direction = "in", recv_ns: int = RECV_NS) -> RawRecord:
    return RawRecord(recv_ns=recv_ns, conn_id="c1", direction=direction, frame=frame)


def run(*records: RawRecord, assets: AssetMap = ASSETS) -> tuple[dict[str, list[dict]], Assembler]:
    assembler = Assembler(assets)
    for record in records:
        assembler.feed(record)
    tables = {name: batch.to_pylist() for name, batch in assembler.take_batches().items()}
    return tables, assembler


def with_timestamp(frame: str, ts_ms: int) -> str:
    obj = orjson.loads(frame)
    obj["timestamp"] = str(ts_ms)
    return orjson.dumps(obj).decode()


# ------------------------------------------------------------- price changes


def test_a_mirrored_pair_keeps_only_the_up_row(pm_frames) -> None:
    tables, assembler = run(rec(pm_frames["price_change"]))
    assert tables["book_deltas"] == [
        {
            "ts_exchange_ms": 1794523531710,
            "ts_recv_ns": RECV_NS,
            "conn_id": "c1",
            "seq": 0,
            "market_id": BTC_5M,
            "outcome": "up",
            "side": "bid",
            "price_e4": 2800,
            "size_e6": 453_730_000,
            "mirrored": True,
        }
    ]
    assert assembler.stats.mirrored_pairs == 1
    assert assembler.stats.unmirrored_groups == 0


def test_the_up_row_is_chosen_by_the_asset_map_not_by_position(pm_frames) -> None:
    # In this frame the Down token comes first.
    tables, _ = run(rec(pm_frames["price_change_fine_tick"]))
    [row] = tables["book_deltas"]
    assert (row["outcome"], row["side"], row["price_e4"], row["mirrored"]) == (
        "up",
        "ask",
        10,
        True,
    )


def test_a_pair_that_is_not_a_mirror_keeps_both_rows(pm_frames) -> None:
    frame = orjson.loads(pm_frames["price_change"])
    frame["price_changes"][1]["size"] = "453.74"
    tables, assembler = run(rec(orjson.dumps(frame).decode()))
    rows = tables["book_deltas"]
    assert [(r["outcome"], r["size_e6"], r["mirrored"]) for r in rows] == [
        ("up", 453_730_000, False),
        ("down", 453_740_000, False),
    ]
    assert assembler.stats.unmirrored_groups == 1


def test_events_of_unknown_assets_are_dropped_and_counted(pm_frames) -> None:
    assets: AssetMap = {BTC_5M_A: (BTC_5M, "up")}
    tables, assembler = run(rec(pm_frames["price_change"]), assets=assets)
    [row] = tables["book_deltas"]
    assert (row["outcome"], row["mirrored"]) == ("up", False)
    assert assembler.stats.unknown_asset_events == 1
    assert assembler.stats.unknown_assets == {BTC_5M_B}


# ---------------------------------------------------------------- snapshots


def test_snapshots_keep_the_first_per_token_then_one_per_minute(pm_frames) -> None:
    t0 = 1_794_523_740_000  # start of a minute
    frames = [with_timestamp(pm_frames["book"], ts) for ts in (t0 + 5, t0 + 30_000, t0 + 60_000)]
    tables, assembler = run(*(rec(f) for f in frames))
    kept_seqs = sorted({r["seq"] for r in tables["book_snapshots"]})
    assert kept_seqs == [0, 2]
    assert len(tables["book_snapshots"]) == 2 * (12 + 21)
    assert (assembler.stats.snapshots_seen, assembler.stats.snapshots_kept) == (3, 2)


def test_snapshot_rows_have_one_row_per_level(pm_frames) -> None:
    tables, _ = run(rec(pm_frames["book"]))
    rows = tables["book_snapshots"]
    bids = [(r["price_e4"], r["size_e6"]) for r in rows if r["side"] == "bid"]
    asks = [(r["price_e4"], r["size_e6"]) for r in rows if r["side"] == "ask"]
    assert bids[0] == (100, 3_091_010_000) and len(bids) == 12
    assert asks[0] == (9900, 4_511_130_000) and len(asks) == 21
    assert {(r["market_id"], r["outcome"], r["seq"]) for r in rows} == {(BTC_5M, "up", 0)}


def test_the_initial_dump_keeps_every_token(pm_frames) -> None:
    books = orjson.loads(pm_frames["initial_array"])
    outcomes: list[Outcome] = ["up", "down"]
    pairs = zip(books, outcomes, strict=True)
    assets: AssetMap = {b["asset_id"]: (b["market"], o) for b, o in pairs}
    tables, assembler = run(rec(pm_frames["initial_array"]), assets=assets)
    assert assembler.stats.snapshots_kept == 2
    assert {r["outcome"] for r in tables["book_snapshots"]} == {"up", "down"}


def test_an_empty_snapshot_is_counted(pm_frames) -> None:
    frame = orjson.loads(pm_frames["book"])
    frame["bids"], frame["asks"] = [], []
    tables, assembler = run(rec(orjson.dumps(frame).decode()))
    assert tables["book_snapshots"] == []
    assert assembler.stats.empty_snapshots_kept == 1


# ----------------------------------------------------- trades, ticks, others


def test_trade_row(pm_frames) -> None:
    tables, _ = run(rec(pm_frames["last_trade_price"]))
    [row] = tables["trades"]
    assert row["outcome"] == "down"
    assert (row["side"], row["price_e4"], row["size_e6"], row["fee_rate_bps"]) == (
        "buy",
        6200,
        6_000_000,
        0,
    )
    assert row["tx_hash"].startswith("0x668ff098")


def test_tick_size_change_row(pm_frames) -> None:
    tables, _ = run(rec(pm_frames["tick_size_change"]))
    [row] = tables["tick_size_changes"]
    assert (row["market_id"], row["outcome"], row["old_tick_e4"], row["new_tick_e4"]) == (
        FINE,
        "up",
        100,
        10,
    )


def test_recorder_events_and_seq_follow_the_raw_lines(pm_frames) -> None:
    subscribe = '{"assets_ids":["1","2"],"type":"market","custom_feature_enabled":true}'
    tables, assembler = run(
        rec('{"event":"connecting","url":"wss://x"}', "meta"),
        rec(subscribe, "out"),
        rec('{"event":"connect","shard":"btc-5m","markets":1,"assets":2}', "meta"),
        rec("PING", "out"),
        rec("PONG"),
        rec(pm_frames["last_trade_price"]),
        rec('{"assets_ids":["3"],"operation":"unsubscribe"}', "out"),
        rec(pm_frames["best_bid_ask"]),
        rec('{"event":"disconnect","reason":"ConnectionError()"}', "meta"),
    )
    events = [(r["seq"], r["kind"]) for r in tables["recorder_events"]]
    assert events == [
        (0, "connecting"),
        (1, "subscribe"),
        (2, "connect"),
        (6, "unsubscribe"),
        (8, "disconnect"),
    ]
    assert tables["recorder_events"][1]["detail"] == subscribe
    assert [r["seq"] for r in tables["trades"]] == [5]
    assert assembler.stats.lines == 9


# ------------------------------------------------------------------ mechanics


def test_one_assembler_per_file() -> None:
    assembler = Assembler(ASSETS)
    assembler.feed(rec("PONG"))
    with pytest.raises(ValueError, match="one Assembler per raw file"):
        assembler.feed(RawRecord(recv_ns=RECV_NS, conn_id="c2", direction="in", frame="PONG"))


def test_take_batches_hands_over_every_table_and_clears(pm_frames) -> None:
    assembler = Assembler(ASSETS)
    assembler.feed(rec(pm_frames["price_change"]))
    assembler.feed(rec(pm_frames["last_trade_price"]))
    assert assembler.buffered_rows == 2

    batches = assembler.take_batches()
    assert set(batches) == set(OUTPUTS)
    for name, batch in batches.items():
        assert batch.schema.equals(OUTPUTS[name])
    assert (batches["book_deltas"].num_rows, batches["trades"].num_rows) == (1, 1)

    assert assembler.buffered_rows == 0
    assembler.feed(rec(pm_frames["last_trade_price"]))
    assert assembler.take_batches()["trades"].column("seq").to_pylist() == [2]

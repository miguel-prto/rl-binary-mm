"""The markets table, built from a real recorded Gamma event."""

from pathlib import Path

import orjson
import pyarrow.parquet as pq
import pytest
from marketrec.raw import Direction, RawRecord, RawWriter

from rl_binary_mm.store import markets
from rl_binary_mm.store.markets import MarketRow
from rl_binary_mm.store.schema import MARKETS

FIXTURE = orjson.loads(
    (Path(__file__).parent / "fixtures" / "pm_gamma_event.jsonl").read_text(encoding="utf-8")
)
FRAME: str = FIXTURE["frame"]
RECV_NS: int = FIXTURE["recv_ns"]

UP = "64950204629404933347914787145855478247165355037731544537194957107668717825503"
DOWN = "82400537461800861995124077264853582288726392728014715217294298272236913632461"
EXPECTED = MarketRow(
    market_id="0x6a0c5a067bdc9b6de0af67b44e819076591954917e8c286cef79dcc191358f3f",
    slug="eth-updown-5m-1794528600",
    underlying="ETH",
    window="5m",
    window_start_ms=1_794_528_600_000,
    window_end_ms=1_794_528_900_000,
    up_asset_id=UP,
    down_asset_id=DOWN,
    tick_size_e4=100,
    resolution_source="https://data.chain.link/streams/eth-usd-twap-60s-streams",
    discovered_recv_ns=RECV_NS,
)


def record(frame: str = FRAME, recv_ns: int = RECV_NS, direction: Direction = "in") -> RawRecord:
    return RawRecord(recv_ns=recv_ns, conn_id="c1", direction=direction, frame=frame)


def test_parse_slug() -> None:
    assert markets.parse_slug("btc-updown-15m-1794528600") == ("btc", "15m")
    with pytest.raises(ValueError, match="not a crypto Up/Down slug"):
        markets.parse_slug("will-it-rain-tomorrow")


def test_market_from_a_real_gamma_event() -> None:
    assert markets.market_from_gamma(FRAME, RECV_NS) == EXPECTED


def test_repeated_records_keep_the_earliest_receive_time() -> None:
    later = record(recv_ns=RECV_NS + 10**9)
    for records in ([record(), later], [later, record()]):
        assert markets.collect_markets(records) == [EXPECTED]


def test_a_later_tick_size_change_is_not_a_conflict() -> None:
    # Seen on real data: a shard restart re-recorded a market after its tick
    # had moved to 0.001. The tick at first discovery is kept.
    event = orjson.loads(FRAME)
    event["markets"][0]["orderPriceMinTickSize"] = 0.001
    finer = record(frame=orjson.dumps(event).decode(), recv_ns=RECV_NS + 10**9)
    assert markets.collect_markets([finer, record()]) == [EXPECTED]


def test_conflicting_records_raise() -> None:
    swapped = FRAME.replace(UP, "1").replace(DOWN, UP).replace('"1"', f'"{DOWN}"')
    with pytest.raises(ValueError, match="conflicting"):
        markets.collect_markets([record(), record(frame=swapped)])


def test_only_received_frames_are_read() -> None:
    assert markets.collect_markets([record(direction="meta", frame='{"event":"x"}')]) == []


def test_rows_are_sorted_by_window_start() -> None:
    event = orjson.loads(FRAME)
    event["markets"][0]["conditionId"] = "0xearlier"
    event["markets"][0]["clobTokenIds"] = '["1", "2"]'
    event["markets"][0]["eventStartTime"] = "2026-11-13T00:05:00Z"
    event["markets"][0]["endDate"] = "2026-11-13T00:10:00Z"
    event["slug"] = "eth-updown-5m-1794528300"
    rows = markets.collect_markets([record(), record(frame=orjson.dumps(event).decode())])
    assert [r.market_id for r in rows] == ["0xearlier", EXPECTED.market_id]


def test_read_write_and_asset_map_round_trip(tmp_path: Path) -> None:
    with RawWriter(tmp_path, markets.GAMMA_SOURCE, "c1", clock=lambda: RECV_NS) as writer:
        writer.write(FRAME, "in")
        writer.write(FRAME, "in")  # a shard restart records the market again

    rows = markets.read_markets(tmp_path)
    assert rows == [EXPECTED]

    path = markets.write_markets(rows, tmp_path)
    assert path == tmp_path / "normalized/markets/venue=polymarket/markets.parquet"
    assert not path.with_name(path.name + ".tmp").exists()
    table = pq.read_table(path)
    assert table.schema.equals(MARKETS)
    assert table.num_rows == 1

    expected_map = {UP: (EXPECTED.market_id, "up"), DOWN: (EXPECTED.market_id, "down")}
    assert markets.load_asset_map(tmp_path) == expected_map
    assert markets.asset_map(rows) == expected_map

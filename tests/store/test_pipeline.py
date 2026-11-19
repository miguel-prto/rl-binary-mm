"""End to end: raw files written with marketrec's RawWriter -> Parquet tables."""

import os
import time
from pathlib import Path

import orjson
import pyarrow.parquet as pq
import pytest
from marketrec.raw import RawWriter

from rl_binary_mm.store.normalize import pipeline
from rl_binary_mm.store.normalize.__main__ import main
from rl_binary_mm.store.normalize.assemble import OUTPUTS
from rl_binary_mm.store.normalize.pipeline import RawFile

FIXTURES = Path(__file__).parent / "fixtures"
NOON_NS = 1_794_484_800 * 10**9  # 2026-11-12T12:00:00Z
CONN = "20261112T120000-abcd"

BTC_5M = "0xad4f957246360f6bbe3f0e13b02645b66f4d8f487f54a9f034aa652f3fb46918"
BTC_5M_A = "8184144964907141096563128437515478043857459391118595175897395568769954534314"
BTC_5M_B = "22242433910742073955562533561715525506739883838710302829678990855721505723782"


def gamma_frame_for_btc_5m() -> str:
    """The real Gamma fixture, pointed at the market of the CLOB fixtures (A = Up)."""
    record = orjson.loads((FIXTURES / "pm_gamma_event.jsonl").read_text(encoding="utf-8"))
    event = orjson.loads(record["frame"])
    event["markets"][0]["conditionId"] = BTC_5M
    event["markets"][0]["clobTokenIds"] = orjson.dumps([BTC_5M_A, BTC_5M_B]).decode()
    return orjson.dumps(event).decode()


def write_raw(data_dir: Path, pm_frames: dict[str, str], conn_id: str = CONN) -> None:
    clock = iter(range(NOON_NS, NOON_NS + 10**9, 1000))
    with RawWriter(data_dir, "pm_gamma", conn_id, clock=lambda: NOON_NS) as gamma:
        gamma.write(gamma_frame_for_btc_5m(), "in")
    with RawWriter(data_dir, "pm_clob", conn_id, clock=lambda: next(clock)) as clob:
        clob.write('{"event":"connect","shard":"btc-5m","markets":1,"assets":2}', "meta")
        for name in ("price_change", "book", "last_trade_price", "best_bid_ask"):
            clob.write(pm_frames[name], "in")
        clob.write("PING", "out")
        clob.write("PONG", "in")
    age_raw_files(data_dir)


def age_raw_files(data_dir: Path, seconds: float = 3600) -> None:
    """Make every raw file look last written ``seconds`` ago (i.e. finished)."""
    old = time.time() - seconds
    for path in (data_dir / "raw").rglob("*.jsonl.zst"):
        os.utime(path, (old, old))


def read(data_dir: Path, table: str) -> list[dict]:
    raw = pipeline.raw_files(data_dir)[0]
    return pq.read_table(pipeline.output_paths(data_dir, raw)[table]).to_pylist()


def test_parse_raw_path() -> None:
    path = Path("data/raw/pm_clob/date=2026-11-13/pm_clob_18_20261113T100036-f1e5.jsonl.zst")
    assert pipeline.parse_raw_path(path) == RawFile(
        path=path, date="2026-11-13", hour="18", conn_id="20261113T100036-f1e5"
    )
    with pytest.raises(ValueError, match="not a pm_clob raw file"):
        pipeline.parse_raw_path(Path("data/raw/pm_gamma/date=2026-11-13/pm_gamma_18_x.jsonl.zst"))


def test_output_layout(tmp_path: Path) -> None:
    raw = RawFile(path=Path("x"), date="2026-11-12", hour="12", conn_id=CONN)
    paths = pipeline.output_paths(tmp_path, raw)
    assert set(paths) == {*OUTPUTS, "normalize_stats"}
    assert paths["trades"] == (
        tmp_path / f"normalized/trades/venue=polymarket/date=2026-11-12/hour=12/{CONN}.parquet"
    )


def test_end_to_end(tmp_path: Path, pm_frames: dict[str, str]) -> None:
    write_raw(tmp_path, pm_frames)
    result = pipeline.run(tmp_path)

    assert (len(result.done), result.failed, result.skipped) == (1, [], 0)
    assert (tmp_path / "normalized/markets/venue=polymarket/markets.parquet").exists()

    [delta] = read(tmp_path, "book_deltas")
    assert (delta["outcome"], delta["side"], delta["price_e4"], delta["mirrored"]) == (
        "up",
        "bid",
        2800,
        True,
    )
    assert delta["seq"] == 1 and delta["conn_id"] == CONN
    assert len(read(tmp_path, "book_snapshots")) == 12 + 21
    [trade] = read(tmp_path, "trades")
    assert (trade["outcome"], trade["seq"]) == ("down", 3)
    assert read(tmp_path, "tick_size_changes") == []
    assert [e["kind"] for e in read(tmp_path, "recorder_events")] == ["connect"]
    [stats] = read(tmp_path, "normalize_stats")
    assert stats["conn_id"] == CONN and stats["normalizer_version"] == pipeline.NORMALIZER_VERSION
    assert (stats["lines"], stats["mirrored_pairs"], stats["unknown_asset_events"]) == (7, 1, 0)
    assert (stats["snapshots_seen"], stats["snapshots_kept"], stats["trades"]) == (1, 1, 1)


def test_done_files_are_skipped_unless_forced(tmp_path: Path, pm_frames: dict[str, str]) -> None:
    write_raw(tmp_path, pm_frames)
    pipeline.run(tmp_path)

    again = pipeline.run(tmp_path)
    assert (len(again.done), again.skipped) == (0, 1)

    forced = pipeline.run(tmp_path, force=True)
    assert (len(forced.done), forced.skipped) == (1, 0)


def test_a_bad_file_fails_alone_and_leaves_no_output(
    tmp_path: Path, pm_frames: dict[str, str]
) -> None:
    write_raw(tmp_path, pm_frames)
    bad = "20261112T120000-bad0"
    with RawWriter(tmp_path, "pm_clob", bad, clock=lambda: NOON_NS) as clob:
        clob.write('{"event_type": "something_new"}', "in")
    age_raw_files(tmp_path)

    result = pipeline.run(tmp_path)
    assert len(result.done) == 1
    [(raw, error)] = result.failed
    assert raw.conn_id == bad and "unknown Polymarket event_type" in error
    leftovers = [p for p in (tmp_path / "normalized").rglob(f"*{bad}*")]
    assert leftovers == []


def test_cli_and_worker_processes(tmp_path: Path, pm_frames: dict[str, str]) -> None:
    write_raw(tmp_path, pm_frames)
    write_raw(tmp_path, pm_frames, conn_id="20261112T120000-ef01")
    assert main(["--data-dir", str(tmp_path), "--jobs", "2"]) == 0
    raws = pipeline.raw_files(tmp_path, "2026-11-12")
    assert len(raws) == 2 and all(pipeline.is_done(tmp_path, raw) for raw in raws)


def test_files_still_being_written_are_left_alone(
    tmp_path: Path, pm_frames: dict[str, str]
) -> None:
    write_raw(tmp_path, pm_frames)
    age_raw_files(tmp_path, seconds=60)  # modified a minute ago: may still grow
    result = pipeline.run(tmp_path)
    assert (result.done, result.failed, result.skipped) == ([], [], 0)
    assert not pipeline.is_done(tmp_path, pipeline.raw_files(tmp_path)[0])

    assert len(pipeline.run(tmp_path, min_age_s=0).done) == 1

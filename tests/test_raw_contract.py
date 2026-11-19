"""The parts of market-recorder this project relies on still behave as expected.

The contract between both repositories is the raw format and its reader, plus
two functions reused here: the Gamma event parser (markets table) and the
finished-file rule (normaliser). If a new ``marketrec`` tag changes any of
them, this file is the first thing to break.
"""

from pathlib import Path

import orjson
from marketrec.discovery.polymarket import parse_gamma_event
from marketrec.pull import is_finished
from marketrec.raw import RawRecord, RawWriter, iter_raw_records

# 2026-11-12T12:00:00Z
NOON_NS = 1_794_484_800 * 10**9


def test_frames_round_trip_through_the_raw_format(tmp_path: Path) -> None:
    frame = '{"event_type":"book","asset_id":"1","bids":[],"asks":[]}'
    with RawWriter(tmp_path, "pm_clob", "c1", clock=lambda: NOON_NS) as writer:
        writer.write(frame, "in")
        path = writer.path

    assert path is not None
    assert list(iter_raw_records(path)) == [
        RawRecord(recv_ns=NOON_NS, conn_id="c1", direction="in", frame=frame)
    ]


def test_gamma_events_are_parsed_by_marketrec() -> None:
    """The markets table delegates Gamma parsing to marketrec (see store/markets.py)."""
    fixture = Path(__file__).parent / "store" / "fixtures" / "pm_gamma_event.jsonl"
    event = orjson.loads(orjson.loads(fixture.read_text(encoding="utf-8"))["frame"])
    spec = parse_gamma_event(event, asset="eth", window="5m")

    assert spec.market_id == event["markets"][0]["conditionId"]
    assert spec.asset_ids == tuple(orjson.loads(event["markets"][0]["clobTokenIds"]))  # (up, down)
    assert spec.window_start_ms == 1_794_528_600_000


def test_finished_file_rule_used_by_the_normaliser() -> None:
    """pipeline.is_finished_raw delegates to marketrec.pull.is_finished."""
    path = "date=2026-11-12/pm_clob_12_20261112T120000-abcd.jsonl.zst"
    hour_end = NOON_NS // 10**9 + 3600
    rule = {"min_age_s": 600, "hour_grace_s": 1800}
    assert is_finished(path, hour_end, hour_end + 1800, **rule)
    assert not is_finished(path, hour_end, hour_end + 1799, **rule)  # hour just ended
    assert not is_finished(path, hour_end + 1500, hour_end + 1800, **rule)  # recently modified

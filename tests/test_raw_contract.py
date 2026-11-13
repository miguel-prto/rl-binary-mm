"""The raw format written by market-recorder is readable from this project.

This is the contract between both repositories: if a new ``marketrec`` tag
changes the format or the reader, this test is the first thing to break.
"""

from pathlib import Path

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

from pathlib import Path

import zstandard as zstd

from tfg.store.raw import RawRecord, RawWriter, iter_raw_records, raw_file_path

NS = 1_000_000_000
HOUR = 3_600 * NS
# 2026-11-12T23:00:00Z, so the hour crossing also crosses the date.
T0 = 1_794_524_400 * NS


class FakeClock:
    def __init__(self, now_ns: int = T0) -> None:
        self.now_ns = now_ns

    def __call__(self) -> int:
        return self.now_ns

    def advance(self, ns: int) -> None:
        self.now_ns += ns


def read_all(root: Path) -> list[RawRecord]:
    files = sorted((root / "raw").rglob("*.jsonl.zst"))
    return [rec for f in files for rec in iter_raw_records(f)]


def test_roundtrip_is_byte_faithful(tmp_path: Path) -> None:
    frames = [
        '{"event_type":"book","bids":[{"price":"0.52","size":"100"}]}',
        "PING",
        "unicode ✓ ünïcödé " + chr(0x2028) + " and\ttabs",  # U+2028 LINE SEPARATOR
        '{"spaces":  "kept   as-is"}',
    ]
    clock = FakeClock()
    with RawWriter(tmp_path, "pm_clob", "c1", clock=clock) as w:
        for i, frame in enumerate(frames):
            w.write(frame, "in" if i % 2 == 0 else "out")
            clock.advance(NS)

    records = read_all(tmp_path)
    assert [r.frame for r in records] == frames
    assert [r.direction for r in records] == ["in", "out", "in", "out"]
    assert [r.recv_ns for r in records] == [T0 + i * NS for i in range(4)]
    assert all(r.conn_id == "c1" for r in records)


def test_file_layout(tmp_path: Path) -> None:
    with RawWriter(tmp_path, "pm_clob", "c1", clock=FakeClock()) as w:
        w.write("x", "in")
        assert (
            w.path == tmp_path / "raw" / "pm_clob" / "date=2026-11-12" / "pm_clob_23_c1.jsonl.zst"
        )
    assert raw_file_path(tmp_path, "pm_clob", "c1", T0) == w.path


def test_rotates_on_hour_and_date_boundary(tmp_path: Path) -> None:
    clock = FakeClock()
    with RawWriter(tmp_path, "src", "c1", clock=clock) as w:
        w.write("a", "in")
        clock.advance(HOUR - 1)
        w.write("b", "in")  # last ns of the hour: same file
        first = w.path
        clock.advance(1)
        w.write("c", "in")  # first ns of the next hour (and next day): new file
        second = w.path

    assert first is not None and second is not None
    assert first.name == "src_23_c1.jsonl.zst" and first.parent.name == "date=2026-11-12"
    assert second.name == "src_00_c1.jsonl.zst" and second.parent.name == "date=2026-11-13"
    assert [r.frame for r in iter_raw_records(first)] == ["a", "b"]
    assert [r.frame for r in iter_raw_records(second)] == ["c"]


def test_reopen_within_same_hour_appends(tmp_path: Path) -> None:
    clock = FakeClock()
    with RawWriter(tmp_path, "src", "c1", clock=clock) as w:
        w.write("before-restart", "in")
    clock.advance(60 * NS)
    with RawWriter(tmp_path, "src", "c1", clock=clock) as w:
        w.write("after-restart", "in")
        path = w.path

    assert path is not None
    assert [r.frame for r in iter_raw_records(path)] == ["before-restart", "after-restart"]


def test_close_without_reaching_flush_threshold_leaves_valid_file(tmp_path: Path) -> None:
    w = RawWriter(tmp_path, "src", "c1", clock=FakeClock(), flush_every_lines=1000)
    w.write("only", "in")
    w.close()
    w.close()  # idempotent
    assert w.path is not None
    assert [r.frame for r in iter_raw_records(w.path)] == ["only"]


def test_periodic_flush_makes_data_readable_before_close(tmp_path: Path) -> None:
    clock = FakeClock()
    w = RawWriter(tmp_path, "src", "c1", clock=clock, flush_every_lines=2, flush_every_ns=5 * NS)
    w.write("1", "in")
    w.write("2", "in")  # second line triggers a block flush
    w.write("3", "in")  # still buffered
    assert w.path is not None
    assert [r.frame for r in iter_raw_records(w.path)] == ["1", "2"]

    clock.advance(5 * NS)  # still within the same hour: no rotation
    w.write("4", "in")  # time threshold triggers a flush
    assert [r.frame for r in iter_raw_records(w.path)] == ["1", "2", "3", "4"]
    w.close()


def test_write_after_close_raises(tmp_path: Path) -> None:
    import pytest

    w = RawWriter(tmp_path, "src", "c1", clock=FakeClock())
    w.close()
    with pytest.raises(ValueError, match="closed"):
        w.write("x", "in")


def test_reader_ignores_truncated_tail(tmp_path: Path) -> None:
    """A crash mid-line must not make the whole file unreadable."""
    good = b'{"recv_ns": 1, "conn_id": "c", "dir": "in", "frame": "ok"}\n'
    partial = b'{"recv_ns": 2, "conn_id": "c", "dir": "in", "fra'
    path = tmp_path / "t.jsonl.zst"
    path.write_bytes(zstd.ZstdCompressor().compress(good + partial))
    assert [r.frame for r in iter_raw_records(path)] == ["ok"]

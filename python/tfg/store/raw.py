"""Lossless raw capture of WebSocket frames.

The recorder stores every frame exactly as received (and every frame it sends),
one JSON line per frame, in hourly-rotated zstd-compressed files:

    {root}/raw/{source}/date=YYYY-MM-DD/{source}_{HH}_{conn_id}.jsonl.zst

Each line is ``{"recv_ns": int, "conn_id": str, "dir": "in"|"out", "frame": str}``.
``frame`` is the original text frame, never re-parsed or re-serialised, so the
raw layer is byte-faithful and parsers can be fixed and re-run later.

Design notes
- Synchronous and single-threaded on purpose: appending one line costs
  microseconds, and the module is testable without an event loop.
- Rotation is checked on every write from the injected ``clock`` (ns since the
  Unix epoch, UTC), so tests control time and no timers or threads are needed.
- A restart within the same hour appends a new zstd frame to the existing file;
  ``iter_raw_records`` reads across frames transparently.
- Data is flushed to the OS every ``flush_every_lines`` lines or
  ``flush_every_ns`` nanoseconds (whichever comes first) using a zstd block
  flush, so everything written before a process crash is decodable. ``fsync``
  only happens on rotation and close.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Literal

import orjson
import zstandard as zstd

Direction = Literal["in", "out"]

_NS_PER_HOUR = 3_600 * 1_000_000_000


@dataclass(frozen=True, slots=True)
class RawRecord:
    """One captured frame."""

    recv_ns: int
    conn_id: str
    direction: Direction
    frame: str


def raw_file_path(root: Path, source: str, conn_id: str, hour_start_ns: int) -> Path:
    """Path of the raw file covering the UTC hour that starts at ``hour_start_ns``."""
    dt = datetime.fromtimestamp(hour_start_ns / 1e9, tz=UTC)
    return root / "raw" / source / f"date={dt:%Y-%m-%d}" / f"{source}_{dt:%H}_{conn_id}.jsonl.zst"


class RawWriter:
    """Append-only writer of raw frames with hourly rotation.

    Use as a context manager, or call :meth:`close` explicitly. ``close`` is
    idempotent. Writing after ``close`` raises ``ValueError``.
    """

    def __init__(
        self,
        root: Path,
        source: str,
        conn_id: str,
        *,
        clock: Callable[[], int] = time.time_ns,
        flush_every_lines: int = 100,
        flush_every_ns: int = 2_000_000_000,
        zstd_level: int = 3,
    ) -> None:
        if flush_every_lines < 1:
            raise ValueError("flush_every_lines must be >= 1")
        if flush_every_ns < 0:
            raise ValueError("flush_every_ns must be >= 0")
        self._root = root
        self._source = source
        self._conn_id = conn_id
        self._clock = clock
        self._flush_every_lines = flush_every_lines
        self._flush_every_ns = flush_every_ns
        self._compressor = zstd.ZstdCompressor(level=zstd_level)

        self._fh: IO[bytes] | None = None
        self._zw: zstd.ZstdCompressionWriter | None = None
        self._current_hour_ns: int | None = None
        self._path: Path | None = None
        self._unflushed_lines = 0
        self._last_flush_ns = 0
        self._closed = False
        self.lines_written = 0

    # ----------------------------------------------------------------- public

    @property
    def path(self) -> Path | None:
        """File currently being written, or ``None`` before the first write."""
        return self._path

    def write(self, frame: str, direction: Direction) -> None:
        """Append one frame, rotating the output file if the UTC hour changed."""
        if self._closed:
            raise ValueError("RawWriter is closed")
        recv_ns = self._clock()
        hour_ns = recv_ns - recv_ns % _NS_PER_HOUR
        if hour_ns != self._current_hour_ns:
            self._rotate(hour_ns)
        assert self._zw is not None

        line = orjson.dumps(
            {"recv_ns": recv_ns, "conn_id": self._conn_id, "dir": direction, "frame": frame}
        )
        self._zw.write(line)
        self._zw.write(b"\n")
        self.lines_written += 1
        self._unflushed_lines += 1

        if (
            self._unflushed_lines >= self._flush_every_lines
            or recv_ns - self._last_flush_ns >= self._flush_every_ns
        ):
            self._flush(recv_ns)

    def flush(self) -> None:
        """Push buffered data to the OS so it is decodable even if the process dies."""
        if self._zw is not None:
            self._flush(self._clock())

    def close(self) -> None:
        """Finish the current zstd frame, fsync and close. Safe to call twice."""
        if self._closed:
            return
        self._closed = True
        self._close_current()

    def __enter__(self) -> RawWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---------------------------------------------------------------- private

    def _rotate(self, hour_ns: int) -> None:
        self._close_current()
        path = raw_file_path(self._root, self._source, self._conn_id, hour_ns)
        path.parent.mkdir(parents=True, exist_ok=True)
        # "ab": a restart within the same hour appends a new zstd frame.
        self._fh = open(path, "ab")  # noqa: SIM115 - lifetime managed by close()
        self._zw = self._compressor.stream_writer(self._fh, closefd=False)
        self._path = path
        self._current_hour_ns = hour_ns
        self._unflushed_lines = 0
        self._last_flush_ns = self._clock()

    def _flush(self, now_ns: int) -> None:
        assert self._zw is not None and self._fh is not None
        self._zw.flush(zstd.FLUSH_BLOCK)
        self._fh.flush()
        self._unflushed_lines = 0
        self._last_flush_ns = now_ns

    def _close_current(self) -> None:
        if self._zw is None or self._fh is None:
            return
        self._zw.flush(zstd.FLUSH_FRAME)
        self._zw.close()
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        self._zw = None
        self._fh = None
        self._current_hour_ns = None


def iter_raw_records(path: Path) -> Iterator[RawRecord]:
    """Yield the records of one raw file, in order, across appended zstd frames."""
    dctx = zstd.ZstdDecompressor()
    with open(path, "rb") as fh, dctx.stream_reader(fh, read_across_frames=True) as reader:
        buffer = b""
        while chunk := reader.read(1 << 20):
            buffer += chunk
            *lines, buffer = buffer.split(b"\n")
            for line in lines:
                yield _parse_line(line)
        # A file cut mid-line (crash before flush) ends here; a partial tail
        # is not a record, so it is ignored rather than raised.
        if buffer.endswith(b"}"):
            yield _parse_line(buffer)


def _parse_line(line: bytes) -> RawRecord:
    obj = orjson.loads(line)
    return RawRecord(
        recv_ns=obj["recv_ns"],
        conn_id=obj["conn_id"],
        direction=obj["dir"],
        frame=obj["frame"],
    )

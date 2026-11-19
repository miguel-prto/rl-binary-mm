"""Writing normalised tables to Parquet.

:class:`TableWriter` receives record batches of any size and writes row groups
of about ``row_group_rows`` rows: small row groups compress badly, and the
assembler hands over batches of whatever accumulated since its last drain.

Encodings, measured on a real hour of Polymarket ``book_deltas`` (3.3 M rows):
timestamps and ``seq`` are almost monotonic, so they use DELTA_BINARY_PACKED
(differences instead of values); ``seq`` goes from 12 to 0.3 bits per row and
the file shrinks to 55 % of a default write. Every other column keeps the
default dictionary encoding. Compression is zstd at its default level (level 9
saved only 4 % more).

The file is written as ``<name>.tmp`` and renamed on a clean close, so a crash
or an exception never leaves a truncated file under the final name. Closing is
split in :meth:`TableWriter.finish` (complete the ``.tmp``) and
:meth:`TableWriter.commit` (rename), so that several files can be finished
first and then committed together.
"""

from __future__ import annotations

from pathlib import Path
from types import TracebackType

import pyarrow as pa
import pyarrow.parquet as pq

DELTA_COLUMNS = frozenset({"ts_exchange_ms", "ts_recv_ns", "seq"})
ROW_GROUP_ROWS = 1 << 20


class TableWriter:
    """Writes one Parquet file atomically. Use as a context manager."""

    def __init__(
        self, path: Path, schema: pa.Schema, *, row_group_rows: int = ROW_GROUP_ROWS
    ) -> None:
        if row_group_rows < 1:
            raise ValueError("row_group_rows must be >= 1")
        self.path = path
        self._tmp = path.with_name(path.name + ".tmp")
        self._schema = schema
        self._row_group_rows = row_group_rows
        self._pending: list[pa.RecordBatch] = []
        self._pending_rows = 0
        self.rows_written = 0

        delta = [name for name in schema.names if name in DELTA_COLUMNS]
        path.parent.mkdir(parents=True, exist_ok=True)
        self._writer: pq.ParquetWriter | None = pq.ParquetWriter(
            self._tmp,
            schema,
            compression="zstd",
            # pyarrow documents a list of column names here; its stubs only say bool.
            use_dictionary=[n for n in schema.names if n not in DELTA_COLUMNS],  # pyright: ignore[reportArgumentType]
            column_encoding=dict.fromkeys(delta, "DELTA_BINARY_PACKED"),
        )

    def write(self, batch: pa.RecordBatch) -> None:
        """Queue a batch; full row groups are written as soon as they fill up."""
        if self._writer is None:
            raise ValueError("TableWriter is closed")
        if not batch.schema.equals(self._schema):
            raise ValueError(f"schema mismatch for {self.path.name}")
        if batch.num_rows == 0:
            return
        self._pending.append(batch)
        self._pending_rows += batch.num_rows
        if self._pending_rows >= self._row_group_rows:
            self._flush()

    def finish(self) -> None:
        """Write what is left and complete the temporary file."""
        if self._writer is None:
            return
        self._flush(final=True)
        self._writer.close()
        self._writer = None

    def commit(self) -> None:
        """Give the finished file its final name, replacing any previous one."""
        if self._writer is not None:
            raise ValueError("finish() before commit()")
        if self._tmp.exists():
            self._tmp.replace(self.path)

    def close(self) -> None:
        """Finish and commit."""
        self.finish()
        self.commit()

    def abort(self) -> None:
        """Discard the file. The final path is left untouched."""
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        self._tmp.unlink(missing_ok=True)

    def __enter__(self) -> TableWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.close()
        else:
            self.abort()

    def _flush(self, *, final: bool = False) -> None:
        """Write the pending rows in whole row groups; the remainder waits unless ``final``."""
        if not self._pending:
            return
        assert self._writer is not None
        table = pa.Table.from_batches(self._pending, schema=self._schema)
        size = self._row_group_rows
        ready = table.num_rows if final else table.num_rows - table.num_rows % size
        if ready:
            self._writer.write_table(table.slice(0, ready), row_group_size=size)
            self.rows_written += ready
        rest = table.slice(ready)
        self._pending = rest.to_batches()
        self._pending_rows = rest.num_rows

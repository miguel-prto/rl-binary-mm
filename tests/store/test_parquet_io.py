from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from rl_binary_mm.store.parquet_io import TableWriter

SCHEMA = pa.schema([pa.field("seq", pa.int64(), nullable=False), pa.field("side", pa.string())])


def batch(start: int, n: int) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {"seq": list(range(start, start + n)), "side": ["bid"] * n}, schema=SCHEMA
    )


def test_batches_are_regrouped_into_full_row_groups(tmp_path: Path) -> None:
    path = tmp_path / "t.parquet"
    with TableWriter(path, SCHEMA, row_group_rows=4) as writer:
        for start in range(0, 10, 3):  # batches of 3, 3, 3, 1 rows
            writer.write(batch(start, min(3, 10 - start)))

    md = pq.ParquetFile(path).metadata
    assert [md.row_group(i).num_rows for i in range(md.num_row_groups)] == [4, 4, 2]
    assert pq.read_table(path).column("seq").to_pylist() == list(range(10))
    assert writer.rows_written == 10


def test_delta_encoding_for_seq_and_dictionary_for_the_rest(tmp_path: Path) -> None:
    path = tmp_path / "t.parquet"
    with TableWriter(path, SCHEMA) as writer:
        writer.write(batch(0, 100))
    columns = pq.ParquetFile(path).metadata.row_group(0)
    assert "DELTA_BINARY_PACKED" in columns.column(0).encodings
    assert "RLE_DICTIONARY" in columns.column(1).encodings


def test_an_empty_table_still_produces_a_file(tmp_path: Path) -> None:
    path = tmp_path / "t.parquet"
    with TableWriter(path, SCHEMA) as writer:
        writer.write(batch(0, 0))
    table = pq.read_table(path)
    assert table.num_rows == 0 and table.schema.equals(SCHEMA)


def test_nothing_reaches_the_final_name_until_commit(tmp_path: Path) -> None:
    path = tmp_path / "t.parquet"
    writer = TableWriter(path, SCHEMA)
    writer.write(batch(0, 5))
    writer.finish()
    assert not path.exists()
    writer.commit()
    assert path.exists() and not (tmp_path / "t.parquet.tmp").exists()


def test_an_exception_discards_the_file_and_keeps_the_previous_one(tmp_path: Path) -> None:
    path = tmp_path / "t.parquet"
    with TableWriter(path, SCHEMA) as writer:
        writer.write(batch(0, 5))

    with pytest.raises(RuntimeError), TableWriter(path, SCHEMA) as writer:
        writer.write(batch(100, 5))
        raise RuntimeError("boom")

    assert pq.read_table(path).column("seq").to_pylist() == list(range(5))
    assert list(tmp_path.iterdir()) == [path]


def test_wrong_schema_is_rejected(tmp_path: Path) -> None:
    other = pa.RecordBatch.from_pydict({"x": [1]})
    with (
        pytest.raises(ValueError, match="schema mismatch"),
        TableWriter(tmp_path / "t.parquet", SCHEMA) as writer,
    ):
        writer.write(other)

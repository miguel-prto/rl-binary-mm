"""The schemas stay consistent with each other and with the parsed events."""

from dataclasses import fields

import pyarrow as pa
import pytest

from rl_binary_mm.store import schema
from rl_binary_mm.store.normalize.polymarket import LevelChange, TickSizeChange, Trade

COMMON = ["ts_exchange_ms", "ts_recv_ns", "conn_id", "seq", "market_id", "outcome"]


@pytest.mark.parametrize("name", list(schema.TABLES))
def test_every_table_starts_with_the_common_columns(name: str) -> None:
    assert schema.TABLES[name].names[: len(COMMON)] == COMMON


@pytest.mark.parametrize("name", list(schema.TABLES))
def test_prices_and_sizes_are_integers(name: str) -> None:
    for field in schema.TABLES[name]:
        if field.name.endswith(("_e4", "_e6")):
            assert pa.types.is_integer(field.type), field.name


@pytest.mark.parametrize(
    ("event", "table"),
    [
        (LevelChange, schema.BOOK_DELTAS),
        (Trade, schema.TRADES),
        (TickSizeChange, schema.TICK_SIZE_CHANGES),
    ],
)
def test_parsed_fields_have_a_column(event: type, table: pa.Schema) -> None:
    # asset_id is replaced by outcome when the events are assembled.
    missing = {f.name for f in fields(event)} - {"asset_id"} - set(table.names)
    assert not missing

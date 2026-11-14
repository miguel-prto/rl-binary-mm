"""Arrow schemas of the normalised tables: the single source of truth.

Every writer validates against these schemas and every reader can rely on
them. Files live under ``data/normalized/{table}/venue=.../date=.../hour=.../``;
``venue``, ``date`` and ``hour`` are partition keys, not columns in the files.

Units (D2 in ``docs/data_design.md``): ``*_e4`` are dollars x 10^4, ``*_e6``
shares x 10^6. Timestamps are integers, UTC: ``ts_exchange_ms`` is the venue's
time in ms, ``ts_recv_ns`` the recording server's clock in ns.

Ordering: events can arrive out of exchange-time order, so consumers sort by
``(ts_exchange_ms, seq)``. ``seq`` is the arrival position of the event inside
its raw file (one file per connection and hour), which breaks ties between
events with the same exchange timestamp.
"""

from __future__ import annotations

import pyarrow as pa

_COMMON = [
    pa.field("ts_exchange_ms", pa.int64(), nullable=True),
    pa.field("ts_recv_ns", pa.int64(), nullable=False),
    pa.field("conn_id", pa.string(), nullable=False),
    pa.field("seq", pa.int64(), nullable=False),
    pa.field("market_id", pa.string(), nullable=False),  # Polymarket: condition_id
    pa.field("outcome", pa.string(), nullable=False),  # 'up' | 'down' | 'yes' | 'no'
]

BOOK_DELTAS = pa.schema(
    [
        *_COMMON,
        pa.field("side", pa.string(), nullable=False),  # 'bid' | 'ask'
        pa.field("price_e4", pa.int32(), nullable=False),
        pa.field("size_e6", pa.int64(), nullable=False),  # new absolute size; 0 = level gone
        # True on an 'up' row whose 'down' mirror (same size, opposite side,
        # price 1 - p) was dropped to save space. When False, the 'down' row is
        # stored explicitly. A non-mirrored pair is a feed anomaly, not an
        # opportunity: both tokens are views of one book.
        pa.field("mirrored", pa.bool_(), nullable=False),
    ]
)

BOOK_SNAPSHOTS = pa.schema(
    [
        *_COMMON,
        pa.field("snapshot_id", pa.int64(), nullable=False),  # groups the rows of one snapshot
        pa.field("side", pa.string(), nullable=False),
        pa.field("price_e4", pa.int32(), nullable=False),
        pa.field("size_e6", pa.int64(), nullable=False),
    ]
)

TRADES = pa.schema(
    [
        *_COMMON,
        pa.field("side", pa.string(), nullable=False),  # 'buy' | 'sell', as reported
        pa.field("price_e4", pa.int32(), nullable=False),
        pa.field("size_e6", pa.int64(), nullable=False),
        pa.field("fee_rate_bps", pa.int32(), nullable=True),
        pa.field("tx_hash", pa.string(), nullable=True),
    ]
)

TICK_SIZE_CHANGES = pa.schema(
    [
        *_COMMON,
        pa.field("old_tick_e4", pa.int32(), nullable=False),
        pa.field("new_tick_e4", pa.int32(), nullable=False),
    ]
)

TABLES: dict[str, pa.Schema] = {
    "book_deltas": BOOK_DELTAS,
    "book_snapshots": BOOK_SNAPSHOTS,
    "trades": TRADES,
    "tick_size_changes": TICK_SIZE_CHANGES,
}

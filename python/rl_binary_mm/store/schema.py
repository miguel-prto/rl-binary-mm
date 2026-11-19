"""Arrow schemas of the normalised tables: the single source of truth.

Every writer validates against these schemas and every reader can rely on
them. Files live under ``data/normalized/{table}/venue=.../date=.../hour=.../``;
``venue``, ``date`` and ``hour`` are partition keys, not columns in the files.

Units (D2 in ``docs/data_design.md``): ``*_e4`` are dollars x 10^4, ``*_e6``
shares x 10^6. Timestamps are integers, UTC: ``ts_exchange_ms`` is the venue's
time in ms, ``ts_recv_ns`` the recording server's clock in ns.

Ordering: events can arrive out of exchange-time order, so consumers sort by
``(ts_exchange_ms, seq)`` with a stable sort. ``seq`` is the 0-based line
number of the event's frame in its raw file (one file per connection and
hour), so every row can be traced back to the frame it came from. Rows from
one frame (the two entries of a price change, the books of the initial dump)
share ``seq`` and keep the frame's order.
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

# One row per level. The rows of one snapshot share (conn_id, seq, market_id,
# outcome). Only the first snapshot of each token in each file and then the
# first of each new exchange minute are kept; the rest are redundant with the
# deltas and stay in the raw layer for validation.
BOOK_SNAPSHOTS = pa.schema(
    [
        *_COMMON,
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

# One row per market, rebuilt from every recorded Gamma response (not
# partitioned by hour: a market's metadata may have been recorded hours before
# its events, by an earlier connection of the same shard).
MARKETS = pa.schema(
    [
        pa.field("market_id", pa.string(), nullable=False),  # Polymarket: condition_id
        pa.field("slug", pa.string(), nullable=False),
        pa.field("underlying", pa.string(), nullable=False),  # e.g. 'BTC'
        pa.field("window", pa.string(), nullable=False),  # e.g. '15m'
        pa.field("window_start_ms", pa.int64(), nullable=False),
        pa.field("window_end_ms", pa.int64(), nullable=False),
        pa.field("up_asset_id", pa.string(), nullable=False),
        pa.field("down_asset_id", pa.string(), nullable=False),
        # At first discovery; it can change later (see the tick_size_changes table).
        pa.field("tick_size_e4", pa.int32(), nullable=False),
        pa.field("resolution_source", pa.string(), nullable=True),
        pa.field("discovered_recv_ns", pa.int64(), nullable=False),  # first time recorded
    ]
)

# The recorder's own log, from the raw ``meta`` lines (connecting, connect,
# disconnect, stale) and the frames it sent (subscribe, unsubscribe; not PING).
# Tells the replay environment which intervals are valid.
RECORDER_EVENTS = pa.schema(
    [
        pa.field("ts_recv_ns", pa.int64(), nullable=False),
        pa.field("conn_id", pa.string(), nullable=False),
        pa.field("seq", pa.int64(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("detail", pa.string(), nullable=False),  # the raw frame, verbatim
    ]
)

# One row per normalised raw file, written together with its other outputs.
# It is the record that the file was normalised, by which version of the
# normaliser, and with what losses: raw files are only deleted when
# ``unknown_asset_events`` is 0 (see store/retention.py).
NORMALIZE_STATS = pa.schema(
    [
        pa.field("conn_id", pa.string(), nullable=False),
        pa.field("raw_file", pa.string(), nullable=False),  # file name in raw/pm_clob/date=.../
        pa.field("raw_bytes", pa.int64(), nullable=False),
        pa.field("normalizer_version", pa.int32(), nullable=False),
        pa.field("normalized_at_ns", pa.int64(), nullable=False),  # wall clock, ns UTC
        pa.field("lines", pa.int64(), nullable=False),
        pa.field("level_changes", pa.int64(), nullable=False),
        pa.field("trades", pa.int64(), nullable=False),
        pa.field("tick_size_changes", pa.int64(), nullable=False),
        pa.field("mirrored_pairs", pa.int64(), nullable=False),
        pa.field("unmirrored_groups", pa.int64(), nullable=False),
        pa.field("unknown_asset_events", pa.int64(), nullable=False),
        pa.field("unknown_assets", pa.int64(), nullable=False),
        pa.field("snapshots_seen", pa.int64(), nullable=False),
        pa.field("snapshots_kept", pa.int64(), nullable=False),
        pa.field("empty_snapshots_kept", pa.int64(), nullable=False),
    ]
)

# Market-data tables: all start with the common columns.
TABLES: dict[str, pa.Schema] = {
    "book_deltas": BOOK_DELTAS,
    "book_snapshots": BOOK_SNAPSHOTS,
    "trades": TRADES,
    "tick_size_changes": TICK_SIZE_CHANGES,
}

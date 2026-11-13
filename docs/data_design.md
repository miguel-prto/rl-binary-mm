# Data design: from raw recordings to Parquet

Status: approved 2026-11-12 as part of the recorder design; split on
2026-11-13 when the recorder moved to its own repository. Living document;
update it when a decision changes.

## Where the data comes from

Recording is done by [market-recorder](https://github.com/miguel-prto/market-recorder)
(package `marketrec`), a separate repository shared with another project. It
stores every WebSocket frame untouched, with its receive time, in hourly
`jsonl.zst` files; see its `docs/design.md` for the raw format, sources and
deployment, and `docs/verification_log.md` for verified API facts.

This repository depends on `marketrec`, pinned to a tag, only to read those
files (`marketrec.raw.iter_raw_records`). Everything below, from normalisation
on, lives here.

## Core decisions

### D1. Normalised data keeps each venue's convention; the "Up book" is a view

Kalshi publishes bids for YES and bids for NO. Polymarket has two tokens (Up
and Down), each with its own book. The normalised tables store `(outcome, side)`
exactly as the venue reports them. The projection to a single "Up" book
(a NO bid at `p` is an Up ask at `1 - p`) is a DuckDB view, so the convention
lives in one tested place and normalisation loses nothing.

### D2. Fixed-scale integers, not ticks

Polymarket's tick size can change within a market (`tick_size_change`, e.g.
0.01 to 0.001 near the extremes). Kalshi quotes prices with 4 decimals.
Prices are stored as `price_e4: int32` (dollars x 10^4) and `tick_size_e4`
separately; sizes as `size_e6: int64`. Spot prices use `_e8`. Parsers fail
loudly if an input has more precision than the scale allows; they never round
silently. The C++ engine converts to ticks when loading.

## Normalised layer (Parquet)

Columns common to every event table:

- `ts_exchange_ms: int64?` venue timestamp, ms UTC, nullable.
- `ts_recv_ns: int64` recording server wall clock, ns UTC, NTP-synchronised.
- `conn_id: str`.

```
markets             1 row per market, updated at resolution
  venue, market_id (condition_id | ticker), underlying, window_start_ms,
  window_end_ms, up_asset_id?, down_asset_id?, tick_size_e4,
  resolution_source, strike_e8?, resolved_outcome?, discovered_recv_ns

book_snapshots      1 row per level
  venue, market_id, outcome ('up'|'down'|'yes'|'no'), side ('bid'|'ask'),
  price_e4, size_e6, snapshot_id, seq?, clock?, hash?

book_deltas         1 row per level change
  same keys, price_e4, new_size_e6, delta_e6, seq?, clock?, hash?

trades
  venue, market_id, outcome, price_e4, size_e6, taker_side,
  trade_id | tx_hash, fee_rate_bps?

spot_ticks
  source ('coinbase'|'binance'|'chainlink'|'chainlink_twap'), symbol,
  kind ('trade'|'quote'|'index'), price_e8, bid_e8?, ask_e8?, size_e8?

recorder_events     data-quality log, essential for replay
  source, conn_id, kind ('connect'|'disconnect'|'seq_gap'|'resync'|'stale'|'subscribe'),
  market_id?, detail
```

Notes:

- The tables above are the first draft. The Polymarket normaliser proposal
  (pending approval) revises them with what the recorded data showed: sizes
  are absolute, the `hash` column dominates storage, the Down entry of each
  `price_change` mirrors the Up entry, and events must be ordered by exchange
  timestamp.
- `recorder_events` tells the replay environment which intervals are valid;
  an episode containing a `seq_gap` is discarded rather than trained on.
- Partitioning: `data/normalized/{table}/venue=.../date=.../part-*.parquet`,
  DuckDB views on top.
- The L2-to-L3 inference rule (PLAN section 3.0) needs `trades` to be joinable
  to `book_deltas` by `(market_id, ts)`.

## Code layout

```
python/rl_binary_mm/
  store/
    schema.py       pyarrow schemas (single source of truth)
    normalize/      polymarket.py, kalshi.py, spot.py
    book_state.py   book reconstruction and validation (gaps, inconsistencies)
    duck.py         DuckDB views (incl. the "Up book" projection)
  data/             historical trade download
```

Tests: fixtures of real recorded frames -> normaliser -> expected rows;
`book_state` (snapshot + deltas, gap detection).

## Work order (data side)

1. Polymarket normaliser + `book_state` with real fixtures.
2. Kalshi and spot normalisers, as their connectors land in market-recorder.
3. Resolution outcomes for `markets` (Gamma re-query after the close).
4. Historical trade download.
5. Raw retention policy, once Parquet sizes are measured.

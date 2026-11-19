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

`python/rl_binary_mm/store/schema.py` is the source of truth for the columns;
this section explains them. Implemented for Polymarket (2026-11-19); Kalshi
and spot will follow the same conventions.

### Pipeline

```
python -m rl_binary_mm.store.normalize [--date D] [--force] [--jobs N]
```

1. `markets` is rebuilt from every `pm_gamma` raw file. The recorder writes a
   market's Gamma event once per shard, not once per connection, so the
   metadata for an hour can sit in an earlier file.
2. Each `pm_clob` raw file (one connection, one UTC hour) is normalised on its
   own into one file per table:
   `data/normalized/{table}/venue=polymarket/date=YYYY-MM-DD/hour=HH/{conn_id}.parquet`.
   The partition is the recording hour (the raw file's), so events near the
   boundary can have an exchange timestamp a few ms into the next hour.

Idempotent: a raw file is done when all its outputs exist; all of them are
written as temporary files and then renamed together. Events of tokens not in
`markets` are dropped and counted; the raw layer still has them.

### Tables

Columns common to every market-data table:

- `ts_exchange_ms: int64?` venue timestamp, ms UTC.
- `ts_recv_ns: int64` recording server wall clock, ns UTC, NTP-synchronised.
- `conn_id: str`.
- `seq: int64` 0-based line number of the frame in its raw file. Rows from
  one frame share it. Consumers sort by `(ts_exchange_ms, seq)`, stably,
  because frames can arrive out of exchange-time order.
- `market_id: str` (Polymarket: condition id), `outcome: 'up'|'down'`.

```
markets            1 row per market (not partitioned)
  market_id, slug, underlying, window, window_start_ms, window_end_ms,
  up_asset_id, down_asset_id, tick_size_e4 (at first discovery),
  resolution_source, discovered_recv_ns

book_deltas        1 row per level change
  common, side ('bid'|'ask'), price_e4, size_e6 (new absolute size, 0 = gone),
  mirrored

book_snapshots     1 row per level of a kept snapshot
  common, side, price_e4, size_e6

trades             1 row per last_trade_price
  common, side ('buy'|'sell', as reported), price_e4, size_e6,
  fee_rate_bps, tx_hash

tick_size_changes  common, old_tick_e4, new_tick_e4

recorder_events    the recorder's log: meta lines and subscribe/unsubscribe
  ts_recv_ns, conn_id, seq, kind, detail (raw frame, verbatim)

normalize_stats    1 row per normalised raw file: normalizer_version, line and
  event counts, mirrored / unmirrored pairs, dropped events of unknown
  assets, snapshots seen / kept. Raw retention relies on it.
```

Decisions (2026-11-14, measured on the data recorded 12-13 Nov):

- **No `hash`.** It dominated the size; the book is validated by
  reconstruction instead.
- **Mirrors.** Each `price_change` carries one entry per token, and the Down
  entry is the exact mirror of the Up one (same size, opposite side,
  price 1 - p): all but 1 of the 83.8 M pairs of 12 Nov. Only the Up row is
  stored, with `mirrored = true`; a pair that is not a mirror is stored in
  full with `mirrored = false`. It is a feed anomaly, not an arbitrage: both
  tokens are views of one book. The one case seen was transient: the Down
  entry already carried the next update, and the next pair, 61 ms later, was
  a mirror again.
- **Snapshots.** Kept: the first of each token in each raw file (each file
  can be replayed on its own), then the first of each new exchange minute.
  That is under 1 % of them; the rest are redundant with the deltas and the
  raw layer keeps them for validation.
- **`delta_e6` is not stored**; a view or the loader derives it.
- **Trade `side`** is stored as reported. Its meaning is not verified: 79 % of
  one hour's trades fit "taker side" (BUY at the best ask, SELL at the best
  bid). Check the docs before the simulator relies on it.
- **Tick size.** It changes during a market (0.01 to 0.001 near the
  extremes); `tick_size_changes` is the reference, `markets.tick_size_e4` is
  only the value at discovery.
- **Encoding.** Timestamps and `seq` use DELTA_BINARY_PACKED, other columns
  dictionary encoding, zstd, row groups of about 1 M rows.

Not implemented yet:

```
spot_ticks
  source ('coinbase'|'binance'|'chainlink'|'chainlink_twap'), symbol,
  kind ('trade'|'quote'|'index'), price_e8, bid_e8?, ask_e8?, size_e8?
```

Kalshi's `seq` / `clock` will need their own columns.

Notes:

- `recorder_events` tells the replay environment which intervals are valid;
  an episode containing a gap is discarded rather than trained on.
- The L2-to-L3 inference rule (PLAN section 3.0) needs `trades` to be joinable
  to `book_deltas` by `(market_id, ts)`.

## Code layout

```
python/rl_binary_mm/
  store/
    schema.py       pyarrow schemas (single source of truth)
    fixed.py        decimal strings -> fixed-scale integers
    markets.py      markets table and asset map, from pm_gamma
    parquet_io.py   atomic Parquet writer with the encodings above
    normalize/      polymarket.py (pure frame parsers), assemble.py (events ->
                    rows), pipeline.py + __main__.py (files and CLI)
    book_state.py   book reconstruction and validation (next)
    duck.py         DuckDB views (incl. the "Up book" projection)
  data/             historical trade download
```

Tests: real recorded frames in `tests/store/fixtures/`; parsers, assembly,
writer, and an end-to-end run on raw files written with `RawWriter`.

## Work order (data side)

1. Polymarket normaliser (done 2026-11-19) + `book_state` with real fixtures.
2. Kalshi and spot normalisers, as their connectors land in market-recorder.
3. Resolution outcomes for `markets` (Gamma re-query after the close).
4. Historical trade download.
5. Raw retention (done 2026-11-19, `store/retention.py`). Decided on
   2026-11-14 as 14 days after normalisation and validation; shortened on
   2026-11-19 to 2 days after a lossless normalisation, before `book_state`
   exists, because the recording server was filling up. Accepted risk: a
   normaliser bug found later cannot be fixed for the deleted days. Day 15 of
   each month is kept as a raw sample on the laptop.

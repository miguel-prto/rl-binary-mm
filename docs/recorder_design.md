# Recorder design (phase 1)

Status: approved 2026-11-12. Living document; update it when a decision changes.

## Goals

Record, 24/7 and losslessly, the order books and trades of short-dated crypto
"Up/Down" binary markets on Polymarket and Kalshi, plus the spot price of the
underlying, so that the data can later drive calibration and a replay
environment. Order books cannot be backfilled, so the recorder must be simple,
robust and deployed as early as possible.

## Core decisions

### D1. Two layers: lossless raw, offline normalisation

The recorder only appends every WebSocket frame, byte for byte, together with
the local receive time. A separate normaliser turns raw files into Parquet.

Why: venue APIs change often (Kalshi moved to `price_dollars` / `delta_fp`
strings, Polymarket replaced RTDS with PolyBolt). With raw data intact a parser
bug or a format change is fixed by re-normalising; a parser inside the
recorder would lose those hours. It also lets recording start before the
normaliser is finished.

### D2. Normalised data keeps each venue's convention; the "Up book" is a view

Kalshi publishes bids for YES and bids for NO. Polymarket has two tokens (Up
and Down), each with its own book. The normalised tables store `(outcome, side)`
exactly as the venue reports them. The projection to a single "Up" book
(a NO bid at `p` is an Up ask at `1 - p`) is a DuckDB view, so the convention
lives in one tested place and normalisation loses nothing.

### D3. Fixed-scale integers, not ticks

Polymarket's tick size can change within a market (`tick_size_change`, e.g.
0.01 to 0.001 near the extremes). Kalshi quotes prices with 4 decimals.
Prices are stored as `price_e4: int32` (dollars x 10^4) and `tick_size_e4`
separately; sizes as `size_e6: int64`. Spot prices use `_e8`. Parsers fail
loudly if an input has more precision than the scale allows; they never round
silently. The C++ engine converts to ticks when loading.

### D4. Isolation by scope

One container per (venue, scope): `pm-crypto`, `pm-macro`, `kalshi-crypto`,
`kalshi-macro`, `spot`. The 15-minute crypto books are the core of the thesis
and must never be at risk because of an optional scope.

## Sources

| Source | What | Verification status |
|---|---|---|
| Polymarket CLOB WS `wss://ws-subscriptions-clob.polymarket.com/ws/market` | `book`, `price_change`, `last_trade_price`, `tick_size_change`; with `custom_feature_enabled`: `best_bid_ask`, `new_market`, `market_resolved` | Official docs. Text `PING` every 10 s; subscribe/unsubscribe on an open connection. No auth. |
| Polymarket PolyBolt `wss://ws-live-v2.polymarket.com/ws` | `price.crypto` (Chainlink `btcusd`, ...) and `price.crypto.twap` (the resolution source) | Official docs. Requires CLOB API credentials (derived from a wallet signature; no funds needed). Timestamps in ms. |
| Kalshi WS `wss://external-api-ws.kalshi.com/` channels `orderbook_delta`, `trade` | Snapshot + deltas (`seq`, `clock`), trades (`trade_id`) | Fields verified in docs. Auth headers during the handshake; header names still to verify. One `orderbook_delta` subscription per connection; market limit per subscription undocumented. |
| Coinbase BTC-USD (and ETH, SOL, XRP) | trades + best bid/ask | Not verified yet. Coinbase is a BRTI constituent. |
| Binance BTCUSDT | aggregated trades + best bid/ask | Not verified yet. Binance blocks US IPs, hence the EU VPS. |

Open points to verify before coding each connector are tracked in
`docs/verification_log.md`.

## Market discovery

A separate component decides what to subscribe to; connectors only subscribe.

- Polymarket: slugs appear to be deterministic, `{asset}-updown-{window}-{t_start}`
  with `t_start` a multiple of the window length in seconds. This comes from
  third-party sources and must be confirmed against the Gamma API before use.
  Fallback: the `new_market` event.
- Kalshi: REST listing of open markets per series (`KXBTC15M`, others to be
  read from the series list, not assumed).
- Crypto policy: subscribe to the current window and the next two; unsubscribe
  on `market_resolved` or after a grace period past the close.
- Macro/political policy: a curated list (tags/series, filtered by volume,
  capped at N markets) refreshed hourly, plus an hourly REST catalogue of all
  active markets (metadata and best prices).

## Raw layer

```
data/raw/{source}/date=YYYY-MM-DD/{source}_{HH}_{conn_id}.jsonl.zst   (hourly rotation)
```

One line per frame:

```json
{"recv_ns": 1794000000000000000, "conn_id": "pm-clob-7f3a", "dir": "in", "frame": "<original text, unparsed>"}
```

`frame` is the original string (not re-serialised JSON). Outgoing frames
(`dir: "out"`, i.e. subscriptions) are also recorded so that the active
subscription set at any instant is known.

## Normalised layer (Parquet)

Columns common to every event table:

- `ts_exchange_ms: int64?` venue timestamp, ms UTC, nullable. For Polymarket
  the unit (ms) is inferred from examples; the docs do not state it.
- `ts_recv_ns: int64` VPS wall clock, ns UTC, synchronised with chrony.
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

- `new_size_e6` and `delta_e6` are both filled. Polymarket sends absolute level
  sizes, Kalshi sends deltas; the normaliser keeps book state anyway, so the
  replay loader never needs venue-specific logic. The L2-to-L3 inference rule
  (PLAN section 3.0) needs `trades` to be joinable to `book_deltas` by
  `(market_id, ts)`.
- `recorder_events` tells the replay environment which intervals are valid;
  an episode containing a `seq_gap` is discarded rather than trained on.
- Partitioning: `data/normalized/{table}/venue=.../date=.../part-*.parquet`,
  DuckDB views on top.

## Code layout

```
python/tfg/
  data/
    sources/      base.py (Connector protocol + reconnect loop with backoff),
                  polymarket_clob.py, polymarket_live.py, kalshi.py, coinbase.py, binance.py
    discovery/    polymarket.py, kalshi.py
    auth/         kalshi.py, polymarket.py
  store/
    raw.py        RawWriter: jsonl.zst, hourly rotation, clean shutdown
    schema.py     pyarrow schemas (single source of truth)
    normalize/    polymarket.py, kalshi.py, spot.py
    book_state.py book reconstruction and validation (gaps, inconsistencies)
    duck.py       DuckDB views (incl. the "Up book" projection)
  recorder/
    __main__.py   CLI: python -m tfg.recorder --source polymarket_clob
recorder/         Dockerfile, docker-compose.yml, config.yaml, deployment README
```

Dependencies kept minimal: `websockets`, `httpx`, `orjson`, `zstandard`,
`pyarrow`, `polars`, `duckdb`.

Health: each connector touches a heartbeat file read by the Docker healthcheck;
a daily gap report is produced from `recorder_events`.

Tests: fixtures of real recorded frames -> normaliser -> expected rows;
`book_state` (snapshot + deltas, gap detection); `RawWriter` rotation.

## Deployment

- EU VPS (Hetzner, Ubuntu 24.04). Latency to Kalshi is irrelevant for recording.
- Backups: Hetzner Storage Box via rclone (SFTP), hourly; plus periodic copies
  to a local disk. Final sizing after measuring 48 h of real volume.
- Secrets only in `.env` (gitignored). Polymarket: throwaway wallet without
  funds, used only to derive CLOB API credentials. Kalshi: read-only API key.

## Work order

1. Repo skeleton: pyproject (uv), ruff, pytest, CI, format hooks.
2. `RawWriter` + tests.
3. Polymarket market discovery, verified live against Gamma.
4. Polymarket CLOB connector recording raw on the VPS (earliest possible).
5. Polymarket normaliser + `book_state` with real fixtures.
6. Kalshi: auth, connector, normaliser.
7. Spot: Coinbase, Binance, PolyBolt.
8. Docker, deployment, backups, gap report.
9. Macro/political scope (discovery lists + REST catalogue).
10. (Separate module) historical trade download.

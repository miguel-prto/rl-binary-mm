# API verification log

Facts about external APIs that the code depends on. Each entry says where it
was verified and when. Unverified items are listed explicitly so they are not
coded by assumption.

## Verified (official docs, 2026-11-12)

- Polymarket CLOB market channel: URL, `PING` every 10 s, event types and
  fields (`book`, `price_change`, `last_trade_price`, `tick_size_change`,
  `best_bid_ask`, `new_market`, `market_resolved`), dynamic subscribe/unsubscribe.
  Source: https://docs.polymarket.com/developers/CLOB/websocket/market-channel
- Polymarket PolyBolt: URL `wss://ws-live-v2.polymarket.com/ws`, channels
  `price.crypto`, `price.crypto.twap`, auth required, ms timestamps, 2-minute
  snapshot on subscribe. Source: https://docs.polymarket.com/developers/CLOB/websocket/wss-overview
- Polymarket CLOB credentials: EIP-712 `ClobAuth` signature, `POST /auth/api-key`
  / `GET /auth/derive-api-key`, no funds required.
  Source: https://docs.polymarket.com/developers/CLOB/authentication
- Kalshi WS: host `external-api-ws.kalshi.com`, subscribe command format,
  `orderbook_snapshot` / `orderbook_delta` fields (`yes_dollars_fp`,
  `price_dollars`, `delta_fp`, `seq`, `clock`, `ts_ms`), `trade` fields,
  gap handling (`get_snapshot` or resubscribe).
  Sources: https://docs.kalshi.com/websockets/websocket-connection,
  https://docs.kalshi.com/websockets/orderbook-updates,
  https://docs.kalshi.com/websockets/public-trades

## Verified live against the Gamma API (2026-11-12)

`GET https://gamma-api.polymarket.com/events?slug={slug}` returns a list with
one event, whose `markets[0]` is the binary market.

- Slug pattern `{asset}-updown-{5m|15m}-{t_start}` confirmed for btc, eth,
  sol, xrp, with `t_start` the window start in Unix seconds (multiple of the
  window length). The 1h and 4h markets do **not** follow this slug pattern;
  their discovery is pending.
- Market fields: `conditionId`, `clobTokenIds` (a JSON string containing a
  list of two token ids, in the order of `outcomes` = `["Up", "Down"]`),
  `eventStartTime` (= window start), `endDate` (= window end),
  `orderPriceMinTickSize` (0.01), `orderMinSize` (5), `negRisk` false.
- `startDate` is the creation time, roughly 24 h before the window; 15-minute
  markets exist at least 64 windows (16 h) ahead but not 96 (24 h).
- Resolution: `resolutionSource` = Chainlink BTC/USD TWAP 60 s stream;
  `cryptoMarketConfig = {asset, duration, twapEnabled, twapLookbackSeconds: 60}`.
- Fees: `feeType = crypto_fees_v2`, `feeSchedule = {exponent: 1, rate: 0.07,
  takerOnly: true, rebateRate: 0.2}`, `takerBaseFee = makerBaseFee = 1000`,
  `makerRebatesFeeShareBps = 10000`. Semantics of these numbers still to be
  confirmed in the fees documentation before use in the simulator.
- `orderPriceMinTickSize` changes during a market's life: closed windows showed
  0.001 while upcoming ones showed 0.01. The value in a `MarketSpec` is only the
  tick at discovery time; the WebSocket `tick_size_change` event is the reference.
- Events carry `seriesSlug` / `series`, a likely route to discover 1h/4h markets.
- Liquidity rewards parameters: `rewardsMaxSpread = 4.5`, `rewardsMinSize = 50`.

## Observed on the live market channel (2026-11-12)

- Text `PING` is answered with `PONG`; subscribing on an open connection sends
  a `book` snapshot per added asset; `timestamp` fields are milliseconds.
- Initial dumps arrive as a JSON array of events; later messages are single
  JSON objects. No permessage-deflate is negotiated.
- Load: all 5m/15m markets of btc, eth, sol, xrp (64 assets) produce roughly
  1000-2800 msgs/s, about 34 MB of zstd-3 raw data in 7 minutes (~7 GB/day).
- With everything on one connection the server closed it twice in two minutes
  with code 1013 "slow consumer: send buffer full", although the client was
  keeping up. With one connection per (asset, window) no 1013 was seen in 7
  minutes. Short sample: to be confirmed by the 24 h run.
- In that 7-minute run all 8 connections were dropped at the same instant
  (TCP close without a close frame) about 4 minutes after connecting; they
  reconnected within 1-3 s. Cause unknown (local network or server side).

## Verified on recorded data (2026-11-12, BTC 15m shard, 13:00-14:00 UTC, 1.68 M price changes)

- `timestamp` is epoch milliseconds: on the NTP-synced server the receive time
  is 17 ms later at the median (p99 126 ms).
- `price_change.size` is the new absolute size of the level. Applying the
  changes to the first snapshot, with no later resync, reproduces every later
  `book` snapshot except for transient ordering effects (below).
- Messages can arrive out of timestamp order: 2,403 of 1,688,446 events had an
  earlier timestamp than the previous one, usually by under 10 ms, a few by
  200 ms or more. Snapshot mismatches: 30 of 6,600 in arrival order, 6 when
  events are applied in timestamp order.
- Every `price_change` carries two entries, one per token, that are exact
  mirrors (same size, opposite side, prices adding up to 1).
- A `book` snapshot for both tokens is sent with every trade.
- Prices have up to 3 decimals, book sizes up to 2, trade sizes up to 6.
- 3,283 of 3,297 trades have a `price_change` at the same token and price
  within 100 ms.
- `market_resolved` does arrive, but usually after the market has been
  unsubscribed: 14 events for roughly 128 markets that closed in two hours.
  `new_market` events are a firehose of all new Up/Down markets.
- Subscribing to one token only does not reduce `price_change` traffic and
  loses the trades of the other token.

## Not verified yet (do not code by assumption)

- Polymarket 1h and 4h "Up/Down" slugs or series ids.
- Polymarket fee-field semantics (`feeSchedule`, `takerBaseFee`, rebates).
- Polymarket limits: asset ids per connection, connections per IP.
- Kalshi auth header names and signed string for the WS handshake.
- Kalshi series tickers for ETH/SOL/XRP 15-minute markets; market fields for
  strike and open/close times; markets-per-subscription limit.
- Coinbase and Binance WebSocket channels and message formats.
- Geographic restrictions on Polymarket credential creation.

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

## Not verified yet (do not code by assumption)

- Polymarket market-channel timestamp unit (ms inferred from 13-digit examples).
- Polymarket `price_change.size` being the absolute level size (to be checked
  empirically against `best_bid`/`best_ask`).
- Polymarket 1h and 4h "Up/Down" slugs or series ids.
- Polymarket fee-field semantics (`feeSchedule`, `takerBaseFee`, rebates).
- Polymarket limits: asset ids per connection, connections per IP.
- Kalshi auth header names and signed string for the WS handshake.
- Kalshi series tickers for ETH/SOL/XRP 15-minute markets; market fields for
  strike and open/close times; markets-per-subscription limit.
- Coinbase and Binance WebSocket channels and message formats.
- Geographic restrictions on Polymarket credential creation.

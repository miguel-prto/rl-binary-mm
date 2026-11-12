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

## Not verified yet (do not code by assumption)

- Polymarket market-channel timestamp unit (ms inferred from 13-digit examples).
- Polymarket `price_change.size` being the absolute level size (to be checked
  empirically against `best_bid`/`best_ask`).
- Polymarket slug pattern `{asset}-updown-{window}-{t_start}` (third-party
  sources only); Gamma fields for `clobTokenIds`.
- Polymarket limits: asset ids per connection, connections per IP.
- Kalshi auth header names and signed string for the WS handshake.
- Kalshi series tickers for ETH/SOL/XRP 15-minute markets; market fields for
  strike and open/close times; markets-per-subscription limit.
- Coinbase and Binance WebSocket channels and message formats.
- Geographic restrictions on Polymarket credential creation.

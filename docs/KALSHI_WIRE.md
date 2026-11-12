# Kalshi public market-data wire message codec

Isolated Rust library. **No WebSocket transport, authentication, API call, live order route or production collector** exists in this package. Synthetic fixture tests only.

The JSON message shapes follow the publicly documented Kalshi Orderbook Updates and Public Trades channels:

- https://docs.kalshi.com/websockets/orderbook-updates
- https://docs.kalshi.com/websockets/public-trades

Capabilities:
- Decode `orderbook_snapshot`, `orderbook_delta`, `trade` into fixed-point Rust types.
- Exact `price_dollars` (4 decimals), quantity `_fp` (2 decimals), signed delta sizes.
- Preserve `sid`, `seq`, `market_ticker`, `clock`, sender/receiver timestamps.
- Drive the existing subscription-scoped `phoenix-l2` state machine from a synthetic snapshot + delta.
- Leave unknown message types explicitly `Unhandled`, to be archived rather than silently discarded.
- Do **not** infer maker queue-consuming prints from public trade taker fields.
- Hard cap 256 KiB wire message and 4096 levels per snapshot.

Still needed: read-only WSS transport/authentication, reconnect/resubscription, market discovery, sources of truth for authoritative receive timestamps, asynchronous bounded event backpressure, durable archive integration and real-feed capture verification. None have been deployed.

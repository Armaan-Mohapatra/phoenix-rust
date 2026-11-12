# Phoenix Rust vertical integration fixture

This is one **deterministic synthetic** market passing through several independently tested crates:

1. Kalshi-style public JSON snapshot and delta decode (no network)
2. Subscription-scoped Rust L2 reconstruction
3. Read-only synthetic events published to a dedicated recoverable research directory
4. Immutable Parquet SHA256 verification and idempotent reopening
5. Frozen strategy version, point-in-time model signal and ask/depth selection
6. Simulated marketable buy, fee and in-memory paper accounting
7. Simulated binary venue-final outcome and modeled net P&L
8. Second run produces identical results
9. Pending/invalid inputs yield no numeric net P&L

It does **NOT** establish historical strategy profitability, official contract settlement accuracy, independent reference provenance, real execution queue fill, network durability or compatibility with confidential E: drive sources. Only public synthetic fixtures are in this repo.

For an independently verified historical market, the remaining blockers are: authorized finalized source partitions, market/rules identity, effective historical fees, independent primary reference records, and realistic execution assumptions. A paid exchange feed, active collector, private API credentials or production deployment are neither required nor permitted for these fixture tests.

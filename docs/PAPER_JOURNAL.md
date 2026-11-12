# Persistent paper journal (research-only)

A bounded event-sourced virtual portfolio ledger built on the already tested research archive. This package has **no order-submission methods, network requests or access to existing Phoenix production databases**.

Features:
- Independent named virtual portfolios with starting balances.
- Explicit simulated buy, venue-final settlement, invalidation reversal, no-trade events.
- Apply validated transitions only after append-only Parquet event has been committed.
- Rebuild all balances, open positions, settled positions, fees and no-trade reasons after restart.
- Unknown/invalid fills and settlements fail closed; duplicate event sequences and overspending rejected.
- Keeps alternative portfolios separate. This is MODELED paper P&L, not actual exchange fills.
- Reuses durable archive lock, SHA256/manifest validation, corruption detection and file recovery.
- Unit tests use synthetic portfolios and simulate closing/reopening the journal on Windows and Linux.

Limitations:
- No real orders, live markets, unattended schedule, collector or continuous portfolio evaluation.
- A `Settle` event is a **caller-supplied venue-final outcome**, not an independently authenticated settlement. It requires an official hash but does not verify its origin.
- Does not yet implement partial fills, maker queue priority, auto-expiry or position reservations across coupled order legs.
- The underlying manifest index has finite prototype caps, and Windows directory power-loss persistence is not proven.
- Per-portfolio `seq` is a monotonic event number; timestamps are supplied by caller, not read from a trusted market clock.
- Existing research snapshots and production SQLite/WAL files must remain untouched.

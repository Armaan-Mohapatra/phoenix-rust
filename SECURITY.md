# Security and execution boundaries

- No software in this repository may contain an exchange ORDER endpoint, HTTP POST/DELETE route, API private key, exchange credential or live trading execution option.
- Read-only public REST code is separate from the deterministic offline workspace. Its tests must not access the exchange.
- Do not introduce unattended services, Task Scheduler modifications, collector restarts, active SQLite/WAL reads or production writes.
- Do not publish user proprietary historical trading data, confidential source paths, positions or strategy code while the repository is public.
- Before adopting any external package or repository, pin dependencies, inspect all build/install scripts and licenses and run an offline fixture test.
- Modeled paper P&L, venue-final evidence and independently corroborated settlement must remain separate.
- Public CI must use synthetic fixtures and read-only GitHub permissions. Never add credentials to Actions.

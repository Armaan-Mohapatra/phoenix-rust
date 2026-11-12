# hftbacktest — first directly linked upstream repository (conditional on CI)

- Upstream: https://github.com/nkaz001/hftbacktest
- Pinned revision: `5f3ec40b2afb764e0fea112f941ed85523ef4e88`
- Crate: `hftbacktest` v0.9.4, MIT (copyright 2022 nkaz001@protonmail.com).
- Source reviewed: `hftbacktest/src/backtest/models/queue.rs` (RiskAdverseQueueModel, QueueModel, LogProbQueueFunc2), `models/latency.rs` (ConstantLatency), `types.rs` (Order), and `depth/hashmapmarketdepth.rs` (HashMapMarketDepth).
- Feature isolation: Cargo Git dependency pinned to exact commit, `default-features=false,features=["backtest"]`, optional `upstream-hft`, **NO live or S3 feature**. Separate package not included in core Rust workspace. Rust version requirement 1.91.1.
- Phoenix destination: `integrations/hftbacktest-adapter/`.
- Test: same synthetic YES resting bid, queue ahead, explicit consuming prints and arrival cutoff; exercise the UPSTREAM RiskAdverseQueueModel and compare fills to Phoenix `maker_buy`. Test upstream ConstantLatency as an assumption.
- This does not provide a Kalshi-specific queue simulator, authenticated feeds, true price-time priority, or real after-fee P&L.
- Upstream models use f64 price/quantity, conventional bid/ask, sometimes probabilistic cancellations and no Kalshi contract-specific fees. Phoenix continues to own exact fixed-point ledger and fee calculations. Both outcomes remain separately reported and must match before the adapter is eligible even as a diagnostic.
- License: upstream MIT; keep this attribution and upstream license during distribution. This dependency is not vendored.
- Performance benefit: unmeasured; compare memory/CPU only after parity.
- If CI fails, set status `BLOCKED` instead of counting a direct integration.

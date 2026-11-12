# Offline deterministic risk guard — Tarpboi source port

- Source: [tarpboi/Prediction-Markets-Trading-Bot-Toolkits](https://github.com/tarpboi/Prediction-Markets-Trading-Bot-Toolkits/tree/71c0e4ff9e299f9a4d81757fd6b15cf54da69b38), MIT. Reviewed `src/service/risk_guard.rs` (`RiskGuard::check_fast`, `RiskGuard::check_with_book`, `BlockReason`) and `src/bot/runtime.rs` (Broker/StrategyLimits).
- Integration method: independent, original **Rust port of the safety behavior** (NOT a linked upstream dependency). No upstream code copied, no floating-point balances or live broker/service.
- Destination: `integrations/risk-guard/src/lib.rs`, tests synthetic on Windows/Linux.
- Specific improvements: exact 1/10000 dollar money, explicit caller-provided monotonically increasing timestamps, stale/future quote rejects, per-market and portfolio exposure checked using overflow-safe arithmetic, duplicate intent IDs, bounded per-market burst history and cooldown, explicit as-of orderbook depth recheck. 4096 identity/256 market caps.
- Limitation: In-memory single-process prototype with no event-sourced recovery. A `RequireDepth` result is **not an order authorization** and requires a separate fresh-book check before any modelled fill. No venue submission. Historical risk tuning and actual execution effectiveness unmeasured.
- Attribution: Tarpboi MIT-licensed source of publicly inspectable risk-control ideas, independently reimplemented. Prior upstream license and version referenced; do not import its execution endpoints.

# Verified mathematical source-port candidates

Original independently written Rust implementation; NO Python runtime, trading/exchange clients or source-code copies.

**Oracle3**: https://github.com/YichengYang-Ethan/oracle3-prediction-market-agent at `267fb7c48dae7c4ff8922c02a3a223443e51bf0b`, Apache-2.0. Reviewed `oracle3/arbitrage.py` (Quote, Leg, Basket, ConstraintCheck, fee-adjusted relation baskets) and `oracle3/fees.py` (KalshiSchedule). Rust implements a small subset of complement/implication/exclusivity/exactly-one arithmetic, conservative per-leg fee calculation, and missing quote rejection.

**Ryan Frigo Kalshi bot**: https://github.com/ryanfrigo/kalshi-ai-trading-bot at `5d7b70addcc61a1cc1f64f1025f3386888a210b7`, MIT. Reviewed `src/agent/edge.py` (brier_score, edge_vs_book, forward_only semantics). Rust implements fixed-point forward-only leak guard, min sample-size 10, Brier and after-executable-price realized probability delta.

**Limitations**: both modules operate on caller-supplied synthetic inputs. They cannot establish identical contract settlement rules, actual multi-leg fills, independently verified outcomes or reliable after-cost edge. Return `contract_relation_independently_verified=false`, `actual_fills_verified=false`, or `authenticated_independent_settlement=false` until separate provenance stages are implemented. No portfolio trading authority.

Test on Windows and Ubuntu and record `PORT_TO_RUST` only after successful CI; this is **not** direct upstream source dependency integration.

# Phase 6 — Receipt-time historical execution modeling

**Status: CODE_WRITTEN_UNTESTED / SOURCE INTEGRATION PARTIAL.** No Cargo, Rust compilation, tests, historical market simulations, benchmarks, CI, collectors or network calls. No live exchange order functionality.

Base: Phase 5.1 draft PR #27 at `5ede6f9ffe573166fdbfad003edbc06ae19ae5c8`.
Child branch: `codex/phase6-execution-engine-v1`.

## Single reusable execution model

Existing `crates/sim` remains the canonical simulation crate. The new `crates/sim/src/historical.rs` module models limit-buy YES/NO order intents using **fractional hundredths of one contract**, `phoenix-domain::Price` fixed to 1/10,000 USD ticks, and cost denominated in exact 1/1,000,000 USD units. It provides deterministic taker, maker and multi-scenario interfaces.

It models:

- Receipt-time signal, decision, arrival, deadline and optional cancellation; latest snapshot received no later than arrival; freshness checks; no using future quotes to decide.
- Marketable taker limit buys sweeping the **observed opposite-side depth** within the limit, with multi-level partial fills, insufficient liquidity/nonfills, and per-level fee quotes.
- Passive maker queue with displayed quantity ahead plus a caller-supplied conservative extra queue bound. Requires an explicitly complete trade-tape window; only prints after arrival and before cancellation/expiry can reduce queue ahead. No cancellation credit, hidden L3 priority or inferred maker fills from mere price touches.
- Explicit canceled, expired, unfilled, partially filled and filled states; source invalidity and unknown fee are errors, not no-fee fills.
- Price-level and source SHA, quote receipt, latency, modeled quantity and fee version references in the `ExecutionReport` evidence schema. `venue_fill_verified` is always false.
- Bounded scenario array (16), book samples (4096), levels (200 per side), maker prints (50,000). No brute-force or unbounded archive replay is performed by this module.

## Phase 5 fee and L2 integration

The existing `FeeSchedule`/ `FeeRegistry` now support exact fractional-fee calculation using the effective-dated historical formula, rounding step, minimum and maximum. A `HistoricalFeeBridge` binds an exact market ticker to its verified series/role/time schedule and implements the simulator's `HistoricalFeeSource` interface. Unknown, mismatched and future schedules fail closed. No estimated current standard fee is substituted for absent historical data.

The original `source-loader::forensic::Book` exposes an immutable best-bid-first fractional-depth view, **without duplicating book reconstruction**. `BookIntegrity::modeled_observation` converts the existing source state to the simulator's `FractionalBook` representation. The source-derived evidence currently has `complete_subscription_history_verified=false` and `modeled_execution_eligible=false`, so the bridge returns `DepthQuality::Unverified` and actual historical fills remain **blocked**. Tests can use labeled synthetic verified-depth fixtures to author behavior contracts, but that does not authenticate original-market history.

## Authored tests — NOT RUN

The new historical executor has synthetic tests for fractional multi-level taker sweeps and exact cost, maker marketability and displayed-queue wait, incomplete/duplicate tape, missing historical fee, pre-arrival cancellation, future signal, staleness and future book rejection, unsupported depth quality, malformed/crossed depth, nonmarketable no-fill, maker expiry and bounded scenario enumeration. Fee registry tests cover exact whole-vs-fractional parity and missing/mismatched schedule. L2 bridge tests ensure real-source provisional depth is not promoted.

## Integration and source gaps

1. **No fully verified historical execution eligibility yet:** the current Phase 4 source overlay cannot certify complete subscription provenance for the market. A reconstructed book is an observation, not executable fill proof.
2. **No authenticated complete maker trade-tape coverage:** the user-supplied `complete_for_window` flag is an explicit modeling assumption until independently verified.
3. **No real fee import:** historical series-specific fee rates and effective versions remain externally unverified; fee resolution correctly blocks them.
4. **No production hftbacktest wiring proved:** a pinned hftbacktest backtest-only adapter was referenced in the roadmap but its concrete current integration path was not independently resolved in this repository inspection. Do not claim it was integrated or executed.
5. **No original BTC ZIP replay, empirical fills, profitability or OOS results.** These remain Phase 15 validation, and no simulated fill is an actual venue fill.
6. The new model handles limit-buy exposure; comprehensive real-world order cancel/replace and portfolio-level accounting remain assigned to later paper-arena work.

## Static review and deferred validation

Source-only cross-file review checked Rust 1.78 API compatibility at a textual level, exposed module and dependency paths, fee bridge, source bridge, bounding limits and negative fixtures. This is **not** a compile, rustfmt or test result.

An independent Phase 15 operator must compile and run `phoenix-sim`, `phoenix-contract-economics`, and `phoenix-source-loader` as isolated/workspace crates, then replay authenticated source windows and compare to existing hftbacktest backtest-only outputs where available. Performance and throughput remain UNKNOWN.

## Handoff

Retain draft, stacked branch without merge. Do not start Phase 7 until the remaining Phase 6 source integration gaps (especially the existing backtest adapter contract and complete execution-evidence pathway) are resolved in source and recorded. Never run Phase 15 without separate authorization.

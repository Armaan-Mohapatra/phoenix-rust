# Phase 10 — Immutable experiments, split protection and event-cluster research

**CODE_WRITTEN_UNTESTED.** No Cargo, tests, historical simulations, strategy optimization, CI or production source operations executed.

Base: Phase 9 `a21f0322ea57249130f542a674099a83291786c4`.
Branch: `codex/phase10-research-validation-v1`.

## Reuse instead of replacing

Existing `crates/research` contains `SplitPlan`, `choose_trade`, legacy whole-contract `PaperLedger`, source-stage `brier_millionths` and event clustering. Phase 10 *extends* that crate through `experiments.rs`; existing `integrations/performance-report` delegates statistical metrics to this canonical module rather than constructing another evaluator. Legacy paper ledger remains unchanged.

## Actual source implementation

1. **Append-only experiments**: versioned experiment ID, code/config/dataset/manifest/fee/execution/risk hashes, seed, settlement tier, observation range, market selection, methodology, lineage and status. Same ID with modified immutable fingerprint is rejected. Dataset-scoped OOS event-use ledger rejects repeated holdout claims.
2. **Chronological protection**: discovery, embargo, validation, untouched OOS and no future outcome across boundaries. `assign_grouped_splits` keeps an event/market together, rejects conflicting reuse. Bounded 64-window walk-forward plan generation is declarative only.
3. **Exact monetary units**: standardized 1e-6 USD net/gross/fees/capital; `legacy_1e4_to_micro_usd` performs checked ×100 conversion from old 1e-4 USD `Money`. Fractional contract quantities remain explicit hundredths. Missing fee/execution/settlement never replaced with zero. Checked arithmetic, partial/nonfills/abstentions/risk rejections and pending outcomes reported separately.
4. **Evidence-sensitive metrics**: net/gross modeled P&L, fees, wins/losses/flats, average winning/losing result (exact rational representation), profit factor, drawdown, ROI on committed capital and market/event concentration counts. Real venue fill evidence always false. Missing outcomes have no invented settled P&L.
5. **Forecast scores**: Brier, market-implied Brier baseline, log loss, market-implied log loss, accuracy, ten-bin calibration, unique event clusters; all outcomes are caller supplied and source authenticity is NOT established by these calculations. Logarithmic statistical computations use f64 only for dimensionless probability metrics, **not for cash/price/fill money**.
6. **Clustered bootstrap**: seeded xorshift resampling at the event cluster level, not per correlated contract, bounded samples, minimum 20 independent event clusters, Bonferroni exploratory alpha record. It never labels results confirmatory significant without independent pre-registration and sample adequacy.
7. **Research reporting**: `integrations/performance-report/src/experiment_report.rs` adds counts for positive/negative/zero modeled results, explicit absent/unknown data, rejected/no-fill, source provisional, official-modeled and independent evidence **claims**. It never upgrades a caller evidence label to independent authentication.

## Authored tests — all NOT RUN

Frozen version conflict and parent/seed requirements; OOS reuse and embargo/time crossing; event grouping and walk-forward plans; negative results/drawdown, null settlement, missing fee/execution, double-counted fee rejection and 1e-4→1e-6 money bridge; deterministic cluster bootstrap, small clustered sample rejection and multiple-testing metadata; forecast future outcome reject; empty/nonfill-only report does not claim P&L.

## Critical limitations

Source authenticity is external. Market-family and regime breakdowns may be formed from separately tagged datasets, but this new module does not infer regimes from incomplete sources. The metric layer currently assumes caller-provided *correct* realized modeled cash-flow rows; Phase 12 must preserve fee attribution and source evidence. The OOS guard prevents duplicate **registered claims**, not undisclosed analysis outside this registry. CI/compiler/integration/performance tests NOT RUN. Positive expected return and statistical significance are NOT claimed.

Next: Phase 11 strategy-transformation hypotheses, discovery-only candidate planning, no executed optimization.

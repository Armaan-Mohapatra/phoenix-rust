# Phase 9 — Original October frozen strategy evidence, immutable registry

**CODE_WRITTEN_UNTESTED** — no Rust compiler, tests, collector, historical replay, optimization or CI run.

## Actual bounded source recovery (independent file inspection)

Archive: `LunaResearch_20261024_163344.zip` (user-uploaded offline copy). Member: `research/luna_strategy_tournament_freeze_latest.json`, **raw member SHA256** `911beb4d4f9dab3411e07be5aa71c82e79085140fe9bb9ac59cd7d8cb03d0d8b`. The manifest declares a **different semantic SHA field** `a73a4893f24d7813be53c4014c88fe2925ad46e8e943d9ae885ff7ac4fb47716`. Preserve both; do not incorrectly assert that the declared manifest SHA is the SHA of these serialized raw bytes. Frozen UTC time `2026-10-24T22:50:39.505374Z`, `luna_strategy_tournament_freeze_v2`, generation 2.

The original `project/luna_experimental_paper.py` file was present and its SHA256 matched the manifest's source-hash claim: `362038d643fbece296debc52a677c257894d7719517e04ac377d23dfc11e4d6c`. This supports source **identity** and certain algorithms, not complete historical configuration, fee evidence or trade-by-trade reproducibility.

### Eleven recovered *frozen-version identities* (ALL ORIGINAL_PARTIAL_DEFINITION)

| Strategy family | Original frozen version |
|---|---|
| book_flow_imbalance | `book_flow_imbalance_p0_v3` |
| conditional_luna_trust | `conditional_luna_trust_p0_v4_cached_forecast_13m` |
| l_mase_v2 | `l_mase_v2_seed_0p15_0p20_0p05_preopen_rule_p0_v5_cached_forecast_13m` |
| late_window_threshold | `late_window_threshold_p0_v3` |
| logistic_luna_hybrid | `logistic_luna_hybrid_p0_v4_cached_forecast_13m` |
| luna_frozen_directional | `frozen_luna_directional_p0_v4_cached_forecast_13m` |
| maker_queue | `maker_queue_execution_p0_v4_cached_forecast_13m` |
| market_only | `market_only_p0_v2` |
| market_value_discrepancy | `market_value_discrepancy_p0_v4_cached_forecast_13m` |
| settlement_threshold_model | `settlement_threshold_model_p0_v2` |
| volatility_regime_calibration | `volatility_regime_calibration_p0_v2` |

The same freeze manifest has a logistic calibrator intercept `-0.4036173719800784`, slope `1.3378481618469105`, L2 `0.02`, and nine training intervals in `2026-10-24T19:00:00Z..21:00:00Z` with training SHA `6587477e606685bbb6add8b32d7b848b889af7d4956d90d89d1b84672df6ccf7`. The `l_mase_v2` weights are Luna 0.15, reference 0.20 and flow 0.05, labeled by the original manifest **unverified research seeds**, not optimized or validated. The older `research/frozen_models/l_mase_seed_0p15_0p20_0p05_unvalidated_v1/manifest.json` additionally declares maximum entry 0.93, minimum 90 seconds to close, max quote age two seconds and edge candidates 0.01/0.02/0.03/0.05. It does not supply independently verified complete strategy-execution rules.

**Recovered exact frozen-version IDs: 11. Fully verified complete executable definitions: 0. Partial originals: 11. New reconstructed original strategies: 0.** No assertion that October IDs match later November production strategy configurations. Existence of research candidate variants is not proof of production activation or positive performance.

## Rust functionality

`crates/strategies/src/frozen_registry.rs` adds strict SHA-validated manifest importer, source provenance/identity, append-only frozen definition registry, exact version lineage, config/code/model hashes (nullable when not in original evidence), supported series and required historical features, risk/entry/exit/fee/execution contract fields, retired/proposed/frozen lifecycle, and refusal of in-place frozen mutation. Unverified October entries are nonexecutable `OriginalPartialDefinition` even if the manifest labels them frozen. Full definitions must supply complete parameters and historical provenance before promotion.

`PointInTimeSignal` and `DecisionTrace` offer causal decision records. Existing `phoenix-strategies::evaluate` and `phoenix-research::choose_trade` remain canonical for supported verified control recipes. No-trade, seeded random, simple directional and the market/implied/model/inverted/ensemble control labels are distinct from the recovered originals. Missing Perps/Kalshi features cause abstention, future signals are rejected, and every emitted decision retains version/hash, receipt time, hypothetical action and no verified fill.

## Authored regression tests (NOT RUN)

Exact SHA and malformed freeze handling, partial definition import and non-execution, frozen-mutation refusal, new lineage, future source-feature exclusion, deterministic seeded control, missing Perps abstention. Existing strategy model-edge and inversion synthetic tests remain in place.

## Known gaps / Phase 15

The October source did not unambiguously establish full per-version entry/exit, fee, execution, risk, activation/retirement and original paper portfolio state for all 11 versions. Imported manifest/file hashes are source identities, not authenticated version-independent settlement/fee provenance. No original source execution performed, compilation tests not run, and no profitability or November strategy parity claimed.

Next: Phase 10 statistical research/experiment engine stacked on Phase 9 final source SHA, without executing optimization or market simulations.

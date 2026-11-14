# Phase 11 — Provenance-preserving research hypothesis generation

**STATUS: CODE_WRITTEN_UNTESTED.** No actual strategy optimization, market simulations, profits, tests, Cargo, CI or production actions.

Base: Phase 10 PR #32 at `7750b99b79744d1a7e99b7d28667b220c1b5e212`.
Child: `codex/phase11-strategy-optimization-v1`.

## Actual implementation

`crates/strategies/src/transformations.rs` uses the existing Phase 9 immutable `Definition` and `ImmutableRegistry` together with Phase 10 immutable `ExperimentSpec` / `ImmutableExperiments`. **No parallel execution simulator** is introduced. Candidate records refer to the existing `phoenix-sim::historical` execution model and do not run it.

24 typed transformation kinds are available as bounded, hashed research PROPOSALS: full/conditional directional inversion, entry-price threshold, entry/exit timing, confidence, spread, depth, liquidity, volatility, regime and time-to-expiry filters, Perps direction/imbalance/trade flow, Kalshi–Perps disagreement, model–market disagreement, fee-aware abstention, maker/taker assumption, latency, sizing, combinations, weighted ensembles and risk exposure. All transforms specify a parameter name, bounded integer value, optional second value, optional required point-in-time feature, optional other frozen parent, reason and exact transform SHA.

`propose` accepts an existing immutable parent definition and an existing registered discovery-only experiment template. It rejects incomplete historical source definitions with missing original configurations, unknown historical fee SHA declarations, mutation of a frozen strategy, incompatible parameter values and missing combination parents. Each accepted candidate creates a **new version ID with explicit frozen-parent lineage**, a new SHA of the adjusted configuration, a proposed (not frozen) lifecycle, source-feature requirements and an immutable `PLANNED_DISCOVERY` Phase 10 experiment; it does not report a positive result. It never reuses an already labeled untouched OOS dataset as input for an optimization search within this source interface.

Candidate budgets are enforced per parent (max 32), globally (max 256) and per enumeration; duplicate hash variants are rejected. Seeded deterministic enumeration is independent of market outcomes and does not spawn search jobs. All proposed variants are `ReconstructedHypothesis`, not fake recovered originals, and never auto-promoted. Inverting a binary model probability twice restores the original probability, but **does not** imply inverted historical economic returns, fill rates or fees.

## Authored regression cases — all NOT RUN

- Exact probability inversion and double-inversion identity.
- Invalid thresholds and missing required Perps features.
- Exact transform hash reproducibility and duplicate detection.
- Candidate budget boundaries.
- Original parent SHA/lineage immutability and discovery-only experiment integration remain cross-crate static contract checks until Phase 15.
- Historical profit/reversal and strategy improvement are NOT evaluated.

## Caveats

1. The 11 recovered October strategies remain **ORIGINAL_PARTIAL_DEFINITION** and cannot be faithfully optimized until full original configuration and data provenance are recovered. No transformed version of those incomplete original definitions is fabricated.
2. A historical fee source SHA declaration is not independent fee authentication; Phase 15 must validate source rates/rounding/effective dates before interpreting any research result.
3. The new transformation planner itself only creates intent/configuration. The Phase 9 decision evaluator is deliberately not permitted to execute unvalidated `ReconstructedHypothesis` records as if they were verified originals. A later approved integration must map specific typed transformation kinds to real strategy predicates.
4. The source-only proposal API may have partial registry additions if a subsequent experiment insertion fails. The caller must pre-register the immutable parent template and use an isolated staging registry before publishing candidates; transactional persistence is not claimed.
5. The original Python/JSON candidate research archive contains speculative configurations, not verified positive out-of-sample performance; none are promoted.

Next source phase is Phase 12: persistent exact fractional Paper Arena and legacy journal compatibility. All tests remain NOT RUN.

## Causal proposal predicate execution

`apply_transform` adds actual no-lookahead decision predicates over an existing **hypothetical** Phase 9 `DecisionTrace` with a validated as-of `phoenix_domain::Book` and SHA-tagged `CausalFeature` values. Full/conditional inversion replaces the side only using an observed opposite-side **ask**, not an assumed complementary fill; missing conditional data abstains. Entry-price, minimum-quality and maximum-risk/spread/volatility/fee filters abstain on missing measurements, and sizing proposes an integer hundredths-of-contract quantity. Timing, maker/taker, latency and combining/ensembling variants explicitly return `RequiresExecutionEvidence` because transforming a decision without re-running the existing Phase 6 execution and Phase 10 validation engines would otherwise manufacture a result. All emitted outcomes remain research proposals, never venue fills.

Negative fixture tests additionally cover missing Perps feature abstention and future-received Perps data rejection; they were **NOT RUN**.

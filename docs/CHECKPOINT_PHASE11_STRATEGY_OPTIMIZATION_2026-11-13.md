# Phase 11 source checkpoint — Strategy improvement proposals

STATUS: **CODE_WRITTEN_UNTESTED**. No Rust compilation, Cargo testing, historical optimization, sweep execution, real market simulation, performance statistics, CI, production operation or live exchange orders.

Parent: Phase 10 PR #32 at `7750b99b79744d1a7e99b7d28667b220c1b5e212`.
Child branch: `codex/phase11-strategy-optimization-v1`.
Last source/documentation SHA before checkpoint: `13f9a136d4c77b10a71552d75b7d6a8e61c98606`.
Re-fetch final child SHA and PR from GitHub after this checkpoint.

Changed files: `crates/strategies/src/transformations.rs` (new 24-kind versioned strategy candidate planner and point-in-time decision predicates), `crates/strategies/src/lib.rs` (module), `docs/PHASE11_STRATEGY_TRANSFORMATIONS.md` (integration and limitations), this checkpoint.

Static check: canonical `crates/strategies::frozen_registry` parent SHA and Phase 10 `crates/research::experiments` lineage reused, no alternate execution simulator, bounded deterministic parameter generation, immutable derived config and discovery-only experiment declarations, separate outputs for sources requiring fresh Phase 6 execution. Historical source incomplete => original strategy derivation prohibited, not guessed. Checked source contract paths, no runtime semantics claimed.

Tests authored NOT RUN: full/double inversion, transform hash stability, parameter validation and budget guards, conditional missing feature, future Perps observation exclusion, missing Perps gate, and no automatic venue execution. Existing Phase 9–10 tests also remain NOT RUN.

Known issues reserved for Phase 14/15: full materialized historical strategy parameters remain unavailable (11 partial October original IDs), effective fee and maker execution models lack authenticated historical inputs, candidate publication currently requires an isolated staging registry to avoid partially appended state if experiment insert later fails, and actual candidate behavior needs Phase 6 simulation/Phase 10 OOS validation. **No positive-strategy result or successful optimization is claimed.**

Next: Phase 12 `codex/phase12-paper-arena-v1` stacked on this phase's independently re-fetched GitHub head. No local validation or collector execution.

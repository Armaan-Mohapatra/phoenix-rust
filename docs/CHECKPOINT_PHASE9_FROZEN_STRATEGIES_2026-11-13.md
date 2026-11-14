# Phase 9 frozen strategy registry — exact source checkpoint

Status: **CODE_WRITTEN_UNTESTED**. Source complete subject to Phase 15. No Cargo/Rust compilation, test, CI, Codex, historical replay, optimization or collector activity.

Base Phase 8 PR #30 `7ed0ccb6255703054012bea14bb562f10fa18410`.
Branch `codex/phase9-frozen-strategies-v1`.
Source/documentation checkpoint before this file: `b687f5f7ecb91298f857049225fa2aef8a17d1fa`.

Files changed: `crates/strategies/src/frozen_registry.rs` (new, immutable strategy-source registry and decision provenance, 7 authored regressions); `crates/strategies/src/lib.rs` (module export); `crates/strategies/Cargo.toml` (SHA/serde); `docs/PHASE9_FROZEN_STRATEGY_RECOVERY.md` (exact uploaded source evidence); this checkpoint.

Verified uploaded October frozen manifest: **11 version identities recovered, 0 complete original definitions, 11 partial definitions, 0 reconstructed original strategy hypotheses**. October source confirmed against SHA `362038d643fbece296debc52a677c257894d7719517e04ac377d23dfc11e4d6c`. Manifest raw bytes SHA `911beb4d4f9dab3411e07be5aa71c82e79085140fe9bb9ac59cd7d8cb03d0d8b`; internal semantic manifest SHA `a73a4893f24d7813be53c4014c88fe2925ad46e8e943d9ae885ff7ac4fb47716`.

Static source review: module path and SHA/serde dependencies established; preserved existing strategy evaluator; control baselines distinct from original source; nonexecutable partial definitions, immutable version lineage, matching code/config contract, future-received signal rejection and no-actual-fill claims. **STATIC ONLY, NOT COMPILED.**

Known unknowns: historical frozen entry/exit/risk/fee parameters and November parity not fully recoverable; no external source settlement authenticated; all Rust APIs/deps uncompiled. Tests NOT RUN.

Resume instruction: verify PR #31 draft and exact HEAD, confirm no Actions; create `codex/phase10-research-validation-v1` stacked on that exact SHA. Implement Phase 10, publish checkpoint, then Phase 11. No Phase 15.

# Phase 6 — precise restart checkpoint

**Status: PARTIAL_CODE_WRITTEN_UNTESTED.** This is NOT a complete/validated Phase 6 sign-off. Do not advance to Phase 7 until the missing integration contracts are resolved. Historical performance remains UNKNOWN.

Base: `codex/phase5-1-economics-corrections-v1` at `5ede6f9ffe573166fdbfad003edbc06ae19ae5c8`, draft PR #27.
Child: `codex/phase6-execution-engine-v1`.
Last published source/documentation SHA before this checkpoint: `ca065cfb4254c687c3280636d7deeeca66f4faf2`. Re-fetch final branch HEAD/PR after this checkpoint commit.

## Actual changed files

- `crates/sim/src/historical.rs` — new deterministic fixed-point execution module with conservative queue, taker sweeps, modeled order states, partial/nonfills, bounded scenarios, synthetic tests.
- `crates/sim/src/lib.rs` and `crates/sim/Cargo.toml` — expose the canonical module and add serde schema dependency.
- `integrations/contract-economics/src/lib.rs` and `integrations/contract-economics/Cargo.toml` — effective-dated fractional fee calculation and strict ticker-to-series fee bridge.
- `integrations/source-loader/src/lib.rs` and `integrations/source-loader/Cargo.toml` — full immutable fractional depth view and sim dependency.
- `integrations/source-loader/src/book_integrity.rs` — source-to-execution observation bridge that preserves unverified source status; no replay duplicate.
- `docs/PHASE6_HISTORICAL_EXECUTION.md` — full contract, authored test inventory and known blockers.
- This checkpoint.

## New tests: ALL NOT RUN

Core modeled-execution regression tests (~13 authored): exact fractional taker costs, partial fills and depth exhaustion; maker queue and expired nonfills; missing/duplicate tape, bad fee, book freshness, future signals, pre-arrival cancellation, invalid depth, no marketability, scenario bounds. Fee tests: fractional/whole parity, market/time rejection. Source tests: provisional depth never promoted to verified execution.

Static inspection only: confirmed module publication, dependency references, one source L2 owner, historical FeeRegistry bridge, bounded loads, Rust 1.78 style syntax and negative/nonfill fixtures. **No compilation or tests were executed.**

## Critical open gaps

- Resolve or explicitly retire the pre-existing pinned **hftbacktest backtest-only adapter** and document compatibility. Its actual usable integration path has not been verified.
- The current source overlay exports `DepthQuality::Unverified` for original captured books; modeled fills are blocked pending independently certified full-channel/snapshot continuity.
- Historical maker tape completeness needs independent source evidence; the caller-supplied flag does not authenticate source data.
- Original source schema, real fee revisions and actual Kalshi outcomes/benchmark data remain externally unverified. No real P&L or OOS testing is authorized here.
- Cross-package Rust compilation and semantics are untested and must be checked only in the separately authorized Phase 15 campaign.
- Follow-on lifecycle for sell/close, order replace and portfolio accounting remains for Phase 12.

## Exact resume instruction

1. Verify GitHub draft Phase 6 PR and re-fetch its final HEAD. Check parent PR #27 and `main` remain unchanged.
2. Continue **Phase 6 only**, from its child branch. Resolve hftbacktest adapter compatibility and other remaining source contracts; do not add an alternate execution simulator.
3. If reliable source implementation is complete, publish an updated Phase 6 checkpoint, still labeled CODE_WRITTEN_UNTESTED, and only then create the Phase 7 branch.
4. All commits `[skip ci]`. No Cargo, paid Actions, Codex execution, historical replay, production changes, collectors or exchange orders. Phase 15 needs fresh explicit authorization.

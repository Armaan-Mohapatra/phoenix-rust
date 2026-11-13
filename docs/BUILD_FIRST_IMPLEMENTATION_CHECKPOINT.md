# Build-first Phase 0 handoff / initial storage-development checkpoint

**Code status: CODE_WRITTEN_UNTESTED. No new CI, Cargo execution, local source replay, or deployment performed.**

Starting parent: PR #20 consolidated GitHub source commit `066cc4e3b04f1ffafbacdfbf9780424014f2899e`. Existing PR #20, all upstream drafts and `main` remain untouched.

## Changes started

1. `docs/DEFERRED_VALIDATION_BUILD_FIRST.md`: user-authorized no-paid-CI and final-local-validation process; source changes committed with `[skip ci]`; the old tests/workflows retained.
2. `integrations/durable-archive/src/lib.rs`: add per-sealed-partition typed Parquet and row-hash verification API. Existing full-archive visitor reuses the same verification primitive.
3. `integrations/indexed-archive/src/lib.rs`: on newly sealed publication, verify only that partition's on-disk manifest and typed rows before committing identities to redb. Cold reopening continues full sealed/archive index reconciliation. A new cold-restart regression covers six 32-row partitions.
4. `scripts/validate-phoenix-locally.ps1`: **prepared, not executed** one-shot local Cargo/Clippy/fmt/full-workspace/standalone crate and optional original BTC ZIP replay harness, with bounded Cargo build jobs, isolated outputs, no production-root writes, explicit offline-default dependency handling and machine-readable logs.

These changes target the per-append all-history verification cost. They are **not** a completed scalable archive: the legacy in-memory 250k-ID map and 2,000-partition limit are still present and Windows power-cut durability is unverified. No performance improvement has been measured.

## Deferred tests and required final gates

- The Rust changes have **not** been compiled and no test claims apply to them.
- The final local validation must exercise core + all standalone crates, optional pinned hftbacktest in backtest-only mode, archive reopen and fault cases, source-loader and real historical data, resource limits, source provenance and settlement evidence classifications.
- Actual primary CF BRTI evidence and independently fetched official final outcomes are not unlocked by code work.
- Retain all negative/inconclusive results, do not promote modeled fills to actual venue executions.

## Next code-only engineering work

Refactor `phoenix-durable-archive` under an explicitly opt-in disk-backed mode so it no longer accumulates the entire event-ID HashMap during open/publish. Preserve the read-only/sealed-manifest authority, fail-closed duplicate conflict detection, and complete verified recovery. Keep the current legacy mode and tests as compatibility references. Continue source-only implementation in a distinct focused change on this branch or a child branch; defer compilation until the final local validation stage.

No original Phoenix collectors, source files, WAL databases or actual orders were touched.

# Phase 5.1 — source-development checkpoint

Status: **CODE_WRITTEN_UNTESTED**. No Rust compilation, Cargo, CI, Codex execution, market simulation, benchmark, live API query, local-drive or collector operation occurred.

Base: PR #26 `codex/phase5-contract-settlement-v1` at `d1a491c80732cec67dd6e872e917eab62af2c007`.
Child: `codex/phase5-1-economics-corrections-v1`.
Verified last source/documentation commit *before this checkpoint file*: `a6bf77dc8d612052d492714e0a43d1df231e0137`. The PR head SHA must be independently re-fetched after publishing this checkpoint.

## Changed files

- `integrations/kalshi-read-only/src/lib.rs` — canonical ticker-only official/archived contract parsing; nullable UUID, event/type/status/settlement identity; conservative finalized classification; explicit separate historical GET preserving client-level rate limit; negative synthetic fixtures.
- `integrations/contract-economics/src/lib.rs` — independently hashed official contract rules/registry/primary methodology/primary response/primary observations, optional archived UUID, identity matching and time guards; price-only inputs cannot claim simulated fills or empirical net P&L; regression tests.
- `integrations/source-loader/src/book_integrity.rs` — one authoritative fractional L2 book and recorded application results.
- `integrations/source-loader/src/pipeline.rs` — consumes the existing book's application results, removes duplicate state ownership, documents cutoff counter scope.
- `docs/PHASE5_1_CORRECTIVE_INTEGRATION.md` — full design, tests and blockers.
- This checkpoint.

## New functionality and static review

All above source files were fetched from the GitHub branch after commits. Static source assertions inspected optional market IDs, documented archive route, separate benchmark hash validation, one replay book, unavailable price-only fill evidence, honest future-filter counting, and the continued authenticated-primary evidence requirement. This proves *source presence only*, not a working implementation.

## Authored tests, NOT RUN

Kalshi: documented missing UUID, pending/determined, provisional, scalar, canceled, void, conflict, archive path. Economics: independently sourced methodology SHA, optional UUID, event mismatch, hypothetical entry cannot become a fill. L2: unique application/outcome. All existing tests retained subject to Phase 15 compilation and execution.

## Uncertainties / blockers

- No root workspace or standalone-crate Cargo build. No new unit, integration, regression or old tests executed.
- API response shape inferred from Kalshi public documentation; no live API probe in source-development phase.
- No authenticated historical contract rule, fee revision, independent primary sample or official finalized BTC pilot result imported.
- Snapshot provenance, full-channel completeness, maker queue and actual fill evidence remain unverified.
- Contract/ref-price/settlement data cannot establish positive historical expected return.
- Final Windows run needs isolated checkout, offline cached toolchain/dependencies, strict read-only source inputs and SHA-logged results.

## Deferred local validation (prepare only; DO NOT EXECUTE)

```powershell
# Separate user-authorized Phase 15 only.
git rev-parse HEAD
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
# Also validate standalone integrations/kalshi-read-only, contract-economics,
# and source-loader and then run bounded original-source replay with real data.
```

## Next instruction

Continue into **Phase 6** only from the independently verified Phase 5.1 PR head. Implement the historical execution engine by reusing `crates/sim`, `crates/domain`, `crates/l2`, the original-source replay and the existing hftbacktest adapter. Publish a *new* child branch, draft PR and code-only checkpoint. Retain `[skip ci]`. Never execute Phase 15 without separate authorization.

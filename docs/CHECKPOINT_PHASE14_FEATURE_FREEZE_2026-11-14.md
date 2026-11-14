# PHOENIX RUST — Phase 14 feature-freeze checkpoint (2026-11-14 UTC)

**CLASSIFICATION: FEATURE_FROZEN_UNTESTED.**
Source development and the operator-controlled local validation campaign are authored, statically inspected and committed to the PRIVATE GitHub branch. They have **not** been compiled, run, simulated, benchmarked or deployed. This is not a runtime release.

## Exact ancestry and starting point

- Repository: `Armaan-Mohapatra/phoenix-rust`
- Parent Phase 13 draft PR #35: `codex/phase13-collector-orchestration-v1`, verified parent HEAD `d02fb14fbeabfc4b5d47250b0a60ce7b328e4cad`.
- Phase 14 child branch: `codex/phase14-integration-freeze-v1`, draft PR targeting Phase 13.
- Last pre-checkpoint source/script fix commit: `9d17a1181bf3d35199d6c8d7e95b0c57e3115d7a` (PowerShell harness now calls `Invoke-CargoValidation` rather than shadowing external `cargo`).
- The exact final checkpoint SHA must be obtained from GitHub after this file is created, and confirmed as the final draft PR head. It cannot be known until this commit is published.
- Main remains deliberately untouched; existing Phase 0–13 draft PRs and original source archives remain preserved.

## Real source implementation changed

- `integrations/risk-guard/src/lib.rs`: cloneable validated-risk state, explicit configuration identity and deterministic empty same-configuration replay, without rewriting legacy fixed-money units.
- `integrations/paper-journal/src/arena_v2.rs`: **RiskAuditV2** plus single `ActionV2::RiskAssessed` event to atomically persist the modeled risk result and accept/reject action in the existing journal; immutable audit of microUSD exposures, quote/depth, guard version and exact denial; recomputes exposure from actual journal state; marks unguarded legacy V2 Submit/Reject/Replace as lacking complete risk history; caps history to 4096; exact simulated fill cost/fee reconciliation and legitimate no-fill handling.
- `integrations/paper-journal/src/risk_bridge.rs`: rebuilds risk guard from durable journal for every guarded preflight, verifies original decision/cooldown/burst/duplicate semantics, exact denial reason, intact portfolio identity and guard limit identity; advances in-memory risk state only after durable journal append.
- `integrations/paper-journal/src/lib.rs`: poison latch for publication outcomes that may have committed without acknowledgment; requires verified cold reopen before another attempt.
- `integrations/paper-orchestrator/src/paper_pipeline.rs`: eliminates empty strategy SHA fallback, checks market/event/series+source manifest, handles zero modeled fills without fake P&L and rejects inconsistent execution reports before recording any fill.
- `integrations/performance-report/src/experiment_report.rs` and `Cargo.toml`: bind exact Phase 9 strategy config/definition SHA and lineage to Phase 10 experiments and source/fee/execution/risk IDs, with no synthetic claim promoted to independently authenticated status.
- `scripts/validate-phoenix-locally.ps1`: fully rewritten explicitly invoked, isolated Windows local test harness, exact Git HEAD/branch, standalone workspaces, separate hft toolchain, defaults offline, optional bounded original/stress/source replay, source-root exclusion, SHA-tagged results and opt-in low-rate system resource samples. **NOT EXECUTED**.
- `docs/RELEASE_GATES.md`: current source-only gate matrix, prior older CI separated.
- `docs/PHASE14_SOURCE_INVENTORY.md`: all 9 core and 18 standalone crates, source/CLI inventory, toolchain/license constraints.
- `docs/PHASE14_STATIC_SECURITY_AUDIT.md`: source provenance, paper risk durability and Windows archive/collector/reparse safety audit.
- `docs/PHASE14_VALIDATION_MATRIX.md`: 24 future validation categories, tests, inputs and BLOCKED classifications.
- `docs/PHASE14_FEATURE_FREEZE.md`: feature boundary, unresolved source evidence, no release authorization.
- This checkpoint file.

## Static review methodology and findings

Source files and Cargo declarations were fetched from the private repository. Textual source comparison checked module import/export paths, 1e-4 vs 1e-6 USD and 1e-2 contract units, Phase 6 fee/cost and source identity handoffs, Phase 7–8 market/Perps joins, Phase 9–11 frozen strategy lineage/OOS metadata, existing durable V2 journal and risk replay, Phase 10 reporter SHA reconciliation, and versioned source evidence. All 27 package manifest dependencies were inspected; no local cycle was identified. The checked Rust libraries had no remaining uses of selected post-1.78 APIs, including `Option::is_none_or`. This is **STATIC SOURCE INSPECTION**, not compilation, language-version certification or test passing.

Authored but **NOT RUN** regressions include: poisoned/uncertain journal writer, pre/post append crash modeling, reset/replay duplicate/cooldown state, cross-portfolio isolation, tampered risk verdict and exposure, legacy risk coverage blocking, fractional microUSD fill/no-fill and aggregate fees, missing frozen SHA and mismatched strategy/report lineage, source-root protection, mock retry/backpressure, and source/no-order assertions. Earlier synthetic tests remain unrun on this new SHA.

## Source/data limitations and independent blockers

1. Original Perps source schema and original venue contract/unit mappings are **UNKNOWN**; configured synthetic fixtures are not authoritative.
2. Phase 9 recovered 11 original October frozen strategy *version names*, but 0 fully verified executable definitions and no November parity. No original strategy test or optimization was performed.
3. Authenticated official Kalshi binary final, independent primary index, exact effective-dated fees, complete source L2/sessions and maker trade tape are unverified. Claimed source SHA or Boolean cannot authenticate a fill or settlement by itself.
4. Current Paper Arena still persists to legacy sealed `durable-archive`, not the scalable redb `indexed-archive`; long-running paper scale/cold-restart memory and real disk durability remain untested.
5. Windows filesystem reparse/junction/TOCTOU and sudden external HDD power-cut guarantees are NOT proven by file-sync or static code review.
6. Cargo resolution, compilation, rustfmt/Clippy, all root+standalone tests and source/physical fault injection are **NOT RUN**. Optional pinned hftbacktest remains on Rust 1.91.1; root workspace declares 1.78; generated lockfile resolution requires its own SHA record.
7. Operator-only optional read-only transport source is not proof of a deployed source collector; production installation and any exchange order path remain unapproved.
8. No actual returns, source P&L, statistical confidence, positive expectancy, throughput, or reliable CPU/RAM/disk measurements were generated.

## Required next step — explicit Phase 15 authorization ONLY

Use the final **verified Phase 14 PR HEAD** as the expected SHA in the newly prepared local validation script and use a separate isolated research checkout. Without a separate explicit authorization, do NOT run Cargo, tests, Clippy, rustfmt, historical market replays, optimization, benchmark jobs, collector installation, production writes or live exchange orders.

The prepared script is a validation harness, not a trigger; it requires `-ExecuteValidation`. All results must be logged PASS/FAIL/BLOCKED, with excluded authenticity gates still BLOCKED even after synthetic tests pass. Any discovered integration failure must produce a new reviewed corrective commit and an updated expected SHA before renewed validation.

**STOP BEFORE PHASE 15.** Research-only, paper-only, default-off network. Source checkpoint does not constitute release approval.

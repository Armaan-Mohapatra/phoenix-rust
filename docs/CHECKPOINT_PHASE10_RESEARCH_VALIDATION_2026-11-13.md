# Phase 10 exact source checkpoint

STATUS **CODE_WRITTEN_UNTESTED**. No Cargo, Rust compilation, tests, GitHub Actions, backtests, historical optimizations or production writes.

Parent: PR #31 / `codex/phase9-frozen-strategies-v1` at `a21f0322ea57249130f542a674099a83291786c4`.
Child branch: `codex/phase10-research-validation-v1`.
Last pre-checkpoint source/document commit: `7bf096f7e78ab9dc5834a2c05972cb25fefb597f`; re-fetch final head after checkpoint.

Changed files: `crates/research/src/experiments.rs` (new immutable experiments, grouped splits, walk-forward metadata, OOS reuse, model outcome metrics, exact microUSD, forecast/market comparison, cluster bootstrap), `crates/research/src/lib.rs`, `crates/research/Cargo.toml`, `integrations/performance-report/src/experiment_report.rs` (new evidence-sensitive report bridge), `integrations/performance-report/src/lib.rs`, `docs/PHASE10_EXPERIMENT_RESEARCH.md`, this checkpoint.

Static code review checked canonical research and existing report crate dependencies, unchanged legacy ledger, SHA/serde modules and new test source, checked money conversion, explicit no-outcome-as-zero, no independent source authentication, and event-group OOS guards. **Text/source check only; not a compilation outcome.**

Authored tests NOT RUN: version mutation/OOS reuse, group split conflict, walk-forward boundaries, negative and pending modeled returns, fee reconciliation, missing fill evidence, exact microUSD scale, seeded event-level bootstrap reproducibility/multiple comparison/sample floor, causal forecast scoring and nonfill report. No expected historical P&L claimed.

Known limitations: original true market settlement and fee inputs unverified; correlated event groups require real mapping; bootstrap CI exploratory only; test and build integration deferred to Phase 15.

Resume: verify Phase 10 draft PR/head and no paid Actions. Create Phase 11 `codex/phase11-strategy-optimization-v1` stacked on exact verified head. No automatic Phase 15.

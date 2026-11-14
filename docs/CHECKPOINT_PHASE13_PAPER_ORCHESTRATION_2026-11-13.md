# Phase 13 exact source-development checkpoint — offline and paper-only orchestrator

**STATUS: CODE_WRITTEN_UNTESTED.** No Rust compilation/Cargo/test runs, GitHub Actions, Codex executions, historical market backtests, source collector operations, production/active DB or WAL writes, deployment, live order submissions, or merges.

**Parent:** Phase 12 draft PR #34 `codex/phase12-paper-arena-v1` at `6ccd492f6e79a7c32c28e6678fe3a6e9787668ef`.
**Child:** `codex/phase13-collector-orchestration-v1`.
**Pre-checkpoint verified documentation/source commit:** `08e19d94a6bb76fed0ae3a538d9174eb3538f07a`. The exact child HEAD must be independently re-fetched after this checkpoint and draft PR are published.

## Actual changed files

- `integrations/kalshi-read-only/src/stream.rs` (new): original decoder reuse, bounded offline WSS source events, subscription-aware sequence ID and gap/duplicate quarantine, reconnect requires snapshot, backpressure and health telemetry.
- `integrations/kalshi-read-only/src/lib.rs`, `Cargo.toml`: module export and existing Kalshi wire dependency.
- `integrations/paper-orchestrator/Cargo.toml` (new standalone inert, no production process integration).
- `integrations/paper-orchestrator/src/lib.rs` (new): offline default modes, disabled opt-in transport, mock poll, bounded retries/backoff, source health, research-root/disk budget guard, read-only source archive adapter, checkpoint requiring resnapshot, optional finalized Perps import.
- `integrations/paper-orchestrator/src/perps_service.rs` (new): Phase 8 exact schema/SHA Perps normalizer and causal buffer/join with bounded history and checkpoint.
- `integrations/paper-orchestrator/src/paper_pipeline.rs` (new): Phase 9 frozen decision and Phase 7/8 market/Perps source SHA checks to existing Phase 12 risk/paper-journal and existing Phase 6 modeled execution; no venue order route.
- `integrations/risk-guard/src/lib.rs`: stabilize existing overflow-safe Option guards for workspace Rust MSRV without removing prior limit semantics.
- `docs/PHASE13_PAPER_ORCHESTRATION.md` and this checkpoint.

## Textual static integration review (NOT tests)

Checked existing original Kalshi decoder reuse, read-only public GET rate limiter unchanged, shared SID/connection sequence identity, resnapshot provisional only, bounded buffer and backpressure, optional network disabled by default, Phase 8 Perps source and Phase 7 join bridge, Phase 9 strategy decision, Phase 6 simulator, Phase 12 guarded durable paper journal, historical source SHA/fee/rules identity, explicit paper-only status, resource limits, and no order-entry function. Fifteen source assertions passed a text check. **No rustfmt, compiler, cargo check, Clippy, tests, or actual event simulation executed.**

## Tests authored — all NOT RUN

Snapshot/duplicate, source gaps and recovery, shutdown/backpressure, mock network disabled by default, backoff/finite retry exhaustion, source root protection, disk budget, restart checkpoint and no exchange route, Perps missing/restart hash mismatch, hypothetical one-cycle safety. All earlier source/ledger regression tests remain NOT RUN.

## Unverified or partial integration contracts

- Actual feed transport is a trait only: no live WSS connection, subscription launch, REST resnapshot loop or production collector installation is implemented/activated. Source state remains provisional; absence of original session proof cannot be deemed verified.
- Perps venue-specific schemas and exact original instrument definitions, authenticated Kalshi source completeness, historical fee schedule and official/independent settlement evidence remain UNKNOWN.
- The V2 paper ledger is durable through the existing archive but uncompiled/unexecuted. Cross-restart risk-burst/cooldown state and atomic risk preflight+durable publication require Phase 14 design/integration resolution.
- The source archive publisher requires external verified disk-usage budget; actual size may differ from predicted size and operator must reconcile. No unattended archive loop.
- Phase 6 maker queue tape provenance remains unverified. Real strategy P&L/significance/strategy enhancement, benchmark metrics and resource profile are UNKNOWN.
- Source-review status is not evidence of Rust compiler correctness or reliable performance.

## Delivery boundary and next authorized starting point

Once this checkpoint is published: create one draft PR targeting `codex/phase12-paper-arena-v1`, verify PR final head/parent, inspect all five phase draft statuses, all commits include `[skip ci]`, no PR-associated Actions/status, earlier branches and `main` preserved.

**STOP AFTER PHASE 13.** The next **separately requested** source phase is `codex/phase14-integration-freeze-v1`, based on the independently fetched Phase 13 head. Phase 14 must reconcile versioned interfaces/security/reporting and prepare a bounded local validation harness. Phase 15 must never start without fresh explicit authorization.

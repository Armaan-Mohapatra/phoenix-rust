# Phase 12 exact source checkpoint — Persistent fractional Paper Arena

STATUS: **CODE_WRITTEN_UNTESTED**. Not a validated runtime. No Cargo, Rust builds/tests, GitHub Actions, backtests, collector operations, live orders, production file writes or original DB/WAL modifications.

Phase 11 parent: PR #33 `codex/phase11-strategy-optimization-v1` at `3d25d7390130144aad425ab54650692fd07e884d`.
Phase 12 child: `codex/phase12-paper-arena-v1`.
Last source/docs commit before this checkpoint: `95585fcac437da6acef14f847ba649fe0b89807f`.
Re-fetch final GitHub head after this commit.

Files changed:
- `integrations/paper-journal/src/arena_v2.rs`: new fixed microUSD/hundredths-of-contract transition reducer with atomic conservation checks, hash/fee/source validations, reservations, partial fills, cancels/expiry, replacement, reductions, claim-only modeled settlement, invalidation/refund and Phase 6 execution report adapter.
- `integrations/paper-journal/src/risk_bridge.rs`: new guard-then-durable-append microUSD path, using existing risk guard and journal writer.
- `integrations/paper-journal/src/lib.rs`: original durable journal extended with serialized OpenV2/ArenaV2 events and source-version-gated PortfolioState. Legacy event/money serialization unchanged; V2 and old event types cannot mix in one portfolio.
- `integrations/paper-journal/Cargo.toml`: links existing risk-guard and existing Phase 6 sim.
- `integrations/risk-guard/src/lib.rs`: isolated checked ×100 microUSD risk-guard unit/limit constructor, preserves old legacy unit default.
- `docs/PHASE12_FRACTIONAL_PAPER_ARENA.md` and this checkpoint.

Static cross-file inspection verified durable paper archive is still the only writer, V2 exact string monetary fields, risk-guard crate/unit interface, explicit exact price×quantity in microUSD, no duplicate fill IDs, cloned reducer rollback on rejection, reserve and cash conservation invariant, no live exchange API. These are textual checks, NOT compiler or test results.

Authored tests: fractional partial fill, price improvement, cash reservation and release, cancellation/replacement, duplicate fill suppression, unknown settlement, exact no-rounding basis, modeled claim flags, invalidation fee/cash reversal, V2 journal restart and legacy coexistence, micro-unit risk bridge, bad risk unit reject. All tests **NOT RUN**.

Known blockers: user-supplied claim hashes are not authenticated official finals; no actual venue fills, source completeness or historical fees imported. Risk burst/cooldown snapshot reconstitution on restart, atomic external risk preflight vs durable archive transaction, and realized-outcome correction policy require Phase 13/14 review and Phase 15 tests. V2 state is **source-authored but not production ready**.

Next exact starting point: independently re-fetch Phase 12 draft PR final SHA, then create `codex/phase13-collector-orchestration-v1` stacked on that exact SHA. Keep read-only network transport disabled by default; do not activate collectors or use production roots. Stop after Phase 13; Phase 14–15 require separate requests.

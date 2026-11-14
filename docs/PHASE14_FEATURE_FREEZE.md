# PHOENIX RUST — Phase 14 final source-development feature freeze

**Classification: FEATURE_FROZEN_UNTESTED.** The planned Phase 14 source changes and local validation harness are written and GitHub checkpointed. This **does not** mean Rust compilation, tests, original-data compatibility, collector reliability, strategy profitability, OS-level atomicity or release readiness have been established.

Parent: Phase 13 `codex/phase13-collector-orchestration-v1` commit `d02fb14fbeabfc4b5d47250b0a60ce7b328e4cad`. Child: `codex/phase14-integration-freeze-v1`. No earlier source PR/main merge is authorized.

## Authored source repairs

1. Durable exact microUSD paper **risk assessment** is now persisted alongside its order acceptance or rejection in one `RiskAssessed` V2 event, not in a separate non-atomic risk file. Preflight uses a fresh guard and reconstructs the exact duplicate/burst/cooldown state from the confirmed journal before every new decision. Guard in-memory state advances **only after** successful durable append; uncertain archival outcomes poison the journal until reopening and replay. Replayed source event checks configuration identity, risk code, reason, recorded portfolio and market exposure against modeled pre-decision state. Unwitnessed legacy direct V2 risk changes remain non-promotable.
2. Strategy decision provenance now fails closed on an invalid original/frozen identity. Market/event/series, decision receipt clock, source catalog SHA, selected L2 SHA, contract rules and fee-source identity are checked at the handoff. No unverified source can turn a source-dependent modeled fill into actual venue execution.
3. Exact cost accounting checks aggregate fill quantity/cost/fee; genuinely unfilled modeled orders expire without fabricating a quote-source hash or zero-valued settlement. Inconsistent fill reports cancel pending modeled orders rather than crediting cash.
4. Reporting gains versioned strategy definition ID/config SHA equality, explicit original versus reconstructed strategy origin, parent lineage, experiment/dataset/fee/model/risk SHA fields and explicit `false` for report-side independent source authentication and actual order submission.
5. Paper V2 state clone/decision history limited to 4,096 lifetime orders per portfolio to match the bounded risk identity model. There is no silent pruning or legacy money-unit reinterpretation.

## Authored validation and evidence artifacts

- `scripts/validate-phoenix-locally.ps1`: operator-only `-ExecuteValidation`, exact SHA and branch, disjoint isolated clone, path/reparse protection, bounded local CPU job count/target disk, Rust 1.78 core and optional 1.91.1 pinned hft, all workspace and standalone manifests, fmt/Clippy/tests, optional bounded archive stress/BTC/multi-market replay, per-command logs/results/lockfile SHA and BLOCKED evidence gates. **Never invoked in this phase.**
- `docs/RELEASE_GATES.md`: authoritative, source-only gates; historical CI is kept separate from the current commit.
- `docs/PHASE14_SOURCE_INVENTORY.md`: 9 root + 18 standalone packages, CLI entrypoints, dependencies, toolchains, safety limits and missing original data.
- `docs/PHASE14_STATIC_SECURITY_AUDIT.md`: risk durability, source evidence, archive mode, filesystem/Windows, network, HFT licensing, cash units and resource-safety limitations.
- `docs/PHASE14_VALIDATION_MATRIX.md`: all 24 future local test/research/operational proof categories and their explicit opt-ins.

## Static source-only review outcome

- Cargo path inspection found no local dependency cycle in the 27 packages. The root workspace does not cover standalone crates, so the prepared harness enumerates each.
- Existing root and standalone library source files were scanned for selected Rust APIs incompatible with the advertised 1.78 MSRV (including `Option::is_none_or`, `div_ceil`, `std::sync::LazyLock`). No occurrences remained in the inspected source paths. This is **not** a guarantee about transitive dependencies or compiler compatibility.
- No second execution engine, source data importer, durable paper ledger, or financial metrics calculator was introduced.
- All new source regression tests were authored, reviewed statically and **NOT RUN**.
- `main`, production data roots, active collectors, exchange order APIs, source ZIP/Parquet archives and WAL databases remain untouched.

## Gate results not claimed

No historical strategy backtests or optimization, execution/fee simulations, authenticated Kalshi outcome, original Perps partition import, original strategy parity, cleaned OOS research, stress benchmarks, measured computer resource limits, Windows hard-link/power-cut proof, collector startup, deployment or exchange orders occurred.

The October frozen registry evidence still supports 11 **partial original version identities** and **0 complete executable original strategies**. A new transformation remains a hypothesis, never a proven improvement. All modeled fills, settlements and P&L stay conditional on original source proof and actual validation.

## Known blockers requiring separate authorization/evidence

1. Phase 15 Rust compilation + tests across root and standalone crates, including toolchain/dependency-resolution proof.
2. Actual fault injection on Windows storage, hard-link/junction/TOCTOU and physical sudden-power-loss recovery. The logical journal commit boundary does not solve physical filesystem durability.
3. Original-market channel continuity, complete L2 book, maker trade-tape, effective-dated fees and authenticated official+independent settlement.
4. Original Perps venue/schema/contract multipliers and original executable frozen strategy definitions.
5. Safe, measured Windows CPU/RAM/disk and broker-free paper restart behavior; actual historical P&L and untouched OOS significance.
6. Explicit standalone collector installation/migration permission, which has **not** been granted.
7. Full economic parity after partial sell, cancellation, fee rounding, risk record replay and source correction on actual unmodified archives.

## Research-only disposition

**Feature source locked for independent validation, NOT approved for merge/deployment.** Any runtime-discovered defect must be fixed on a separate reviewed branch and the current feature SHA revalidated. Do not treat older successful tests or synthetic fixtures as proof of this commit. Phase 15 starts only after separately explicit user authorization.

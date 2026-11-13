# Phoenix Rust — build-first, deferred-validation development mode

Status: **DEVELOPMENT ONLY / NOT VALIDATED**. Baseline is draft PR #20 at `066cc4e3b04f1ffafbacdfbf9780424014f2899e`. Production and `main` must remain untouched.

## Decision and cost policy

No paid GitHub-hosted Actions runners, no paid cloud CI, and no recurring local Codex sessions for incremental tests. Continue developing through the private GitHub repository, using focused source inspection, static review, and source-backed changes. **Do not run Rust compilers, tests, historical replay, or deployment in this stage.** Write test cases as features are implemented, but execute them only in the final isolated local validation window.

Every commit on this development branch must include `[skip ci]` (supported for ordinary GitHub `push` and `pull_request` events). This prevents runner allocation attempts without deleting the existing workflows. Skipped checks may remain pending. Do not falsify check status, weaken assertions, disable the existing workflows globally, or merge based on skipped checks.

### Evidence terminology

- **CODE_WRITTEN_UNTESTED**: source and intended tests committed; never call this passing or finished.
- **STATIC_REVIEWED**: source contracts and cross-file changes inspected; still not compiled.
- **VALIDATION_DEFERRED**: compilation, operating-system tests, original-source Rust parity, and performance validation pending.
- **BLOCKED_EXTERNAL_EVIDENCE**: fresh official Kalshi outcomes or licensed independent CF data not available.
- **VALIDATED_LOCALLY**: only after an actual command run with archived logs and exact Git SHA.
- **RELEASE_READY**: only after separately required data/replay/security gates succeed and deployment review.

Consolidated Phase 0 has source/blob reconciliation but GitHub-hosted tests were refused before runner assignment for an account billing/spending condition. Preserve the existing checkpoint, do not interpret the failures as compilation failures, and proceed with code authoring under **VALIDATION_DEFERRED**.

## Implementation sequence before local testing

1. **Archive scalability** — replace the legacy in-memory 250k event-ID authority under an isolated disk-backed mode; avoid O(N²) per-publish revalidation; bounded manifest/recovery indexes; retain original fail-closed archive mode for compatibility.
2. **Source-origin historical replay** — complete Rust reader → chronological event/orderbook pipeline, portable source manifests, session/sequence diagnostics, clock skew and snapshot validity; no claim of actual original-ZIP Rust parity.
3. **Historical contract rules and fees** — effective-dated Kalshi fee and payout registry, official outcome source provenance, scalar/void explicit rejection, independent-evidence tiers. CF primary data remains unavailable unless authorized separately.
4. **Execution simulator** — conservative maker/taker depth, partial fills, latency scenarios, cancellation and stale-book handling with explicit source state; retain optional backtest-only hftbacktest integration.
5. **Incremental historical Kalshi/Perps adapters** — bounded manifest selection, schema/unit/timestamp reconciliation and strict no-lookahead feature joins. No broad raw disk scans.
6. **Legacy frozen strategy imports and reproducible experiment design** — versioned source/config, controls, preassigned chronological discovery/validation/OOS and experiment registry.
7. **Strategy hypothesis engine** — inversion and filters, cost-aware abstention, regime tests; no genuine historical profitability claim until local real-source validation and independent settlement.
8. **Paper order lifecycle** — persist all simulated order transitions, reservations, risk decisions, reconciliation and restart recovery.
9. **Read-only collector implementation** — mock-only Kalshi/Perps ingestion, source/session continuity, buffering, sealed output, health monitoring. Do not start real collectors.
10. **Reporting and operational tooling** — evidence-tiered negative-inclusive reports, performance/resource instrumentation, integrity manifests and Windows setup/runbooks.
11. **Security and release preparation** — dependency/attribution review, no live endpoints, no keys or bulk source data, separated output roots, reproducible scripts.
12. **Final local validation (LAST)** — once code reaches feature freeze, run a single scripted verification campaign in an isolated Windows research checkout: root + standalone Rust packages, real archived ZIP allowlist, archive/recovery stress, contracts and execution parity, Perps schema/temporal joins, frozen OOS, power-loss limitations and resource benchmarks. Capture logs, hashes, failures and repairs. A failure triggers a focused fix and repeat of affected tests; no untested release declaration.

Items 1–11 are **coding work**; the actual verification portions of their original acceptance gates are deliberately postponed to item 12. The final source-data and independent-settlement evidence gates may remain blocked even after software compiles.

## Preservation and scope

- GitHub repo stays PRIVATE. Draft PRs #1 and #9–#20 remain intact, unmerged; `main` unchanged.
- This is an isolated new branch from Phase 0, not a replacement for the original review graph.
- Do not modify/delete/move original `E:\TradingNextData`, `E:\KalshiData`, C: databases, WAL files, production collectors, live orders or trading services. No recursive drive scans.
- Do not start collectors, install/deploy on the user's PC or submit exchange orders.
- Do not publish raw confidential market archives, account details, API credentials or restricted CF data to GitHub.
- Add positive/negative tests while writing code, but mark them **NOT RUN**.
- Preserve existing independently observed source checksums and synthetic regression results with their actual historical provenance; never upgrade evidence grade just because additional code exists.
- Every work-session handoff must contain commit SHA, changed files, precise source-only work, new/updated tests **NOT RUN**, unresolved blockers and next source task.

## Local validation without Codex token consumption

Rust `cargo` and `rustc` are ordinary local build tools and can run from PowerShell without an agent or Codex tokens. Prefer running the prepared final validation script directly. Codex should only be used for targeted compiler/test repair if conventional logs and source review are insufficient. A self-hosted GitHub runner is optional but not required; local PowerShell is the simplest last-stage path.

No tool execution, historical P&L validation, or production operation is implied by committing this document.

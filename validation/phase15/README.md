# Phoenix Rust — Phase 15 partial validation and operator handoff

**STATUS: PARTIAL, EXECUTION BLOCKED.** Phase 15A source review and script repair, plus bounded original ZIP/catalog SHA verification, completed without modifying production. The Windows computer, `E:\TradingNextData` and a local Rust/Pwsh toolchain are **not mounted or available in this ChatGPT execution environment**. Do not present static source checks, original ZIP hashes or reported research results as Rust compile/test success.

## Verified source baseline

- Private repository: `Armaan-Mohapatra/phoenix-rust`.
- Feature-frozen Phase 14 PR #36: `ada1f26fc59e5eb108db2cbc4aca8c055ed9bd04` (verified unchanged before branching).
- Phase 15 corrective branch: `codex/phase15-validation-repairs-v1`, targeting Phase 14; all commits `[skip ci]`.
- `main` and prior stacked draft PRs are preserved; no deployment, production data changes, CI, orders or collectors.

## Completed actual validation work

1. **Local environment:** Linux sandbox with ~5.8 GiB RAM, ~30 GiB free local working disk, no C:/E: mounts, no Rust, Cargo or PowerShell. A Git CLI exists; authenticated private repository contents accessible through GitHub connector, not as a runnable local working tree. See `environment-preflight.json`.
2. **Phase 15A script source repair:** Corrected stress `cargo test` argument ordering so Cargo `--offline` appears *before* the test-harness `--`. The million-ID source regression is `#[ignore]` and now receives `--ignored --test-threads=1`. Added root/standalone documentation tests, reject missing-leaf reparse ancestors and drive-root outputs, strengthen read-only source/output disjointness, and update the expected Phase 15 branch. See `scripts/validate-phoenix-locally.ps1` and `script-repair-review.md`. **Script not executed**: no Windows PowerShell present.
3. **Original uploads:** All **10 uploaded ZIP files (516,261,941 bytes)** hashed to SHA256 with read-only bounded 1 MiB chunks; hashes and lengths are in `uploaded-archive-sha256-manifest.json`.
4. **BTC pilot:** October 24 source catalog had **9,243 records**, matched documented SHA256 `55f869f1231b67948395ac16a33b50ec774711653da2a3d22c693050eda2db08`; selected **24/24** Parquet members (14,459,929 uncompressed bytes) present and matched independent decompressed SHA256 against the catalog. Zero missing/mismatch. Exact individual file proofs in `btc-pilot-member-integrity.json`. This is **file integrity only**, not source-book completeness, original Rust replay, price/fee correctness or settlement authenticity.
5. **Original strategy accessibility:** One archived `l_mase_seed_0p15_0p20_0p05_unvalidated_v1` manifest declares source SHA256 `538620e34e99e57de19f109ba5fdce9ad7adcb380934f7a8e5e478b0fed90f43`. None of 114 bounded archived Python sources matched. The similarly named `project/luna_experimental_paper.py` SHA is **different** (`362038d643fbece296debc52a677c257894d7719517e04ac377d23dfc11e4d6c`). Ten archived candidate specs are explicitly `PAPER_SPECS_ONLY_NONE_VALIDATED`. See `research-source-availability.json`. No original exact-version execution parity established.
6. **Settlement report:** An archived P0 JSON claims reconciliation for 9 tickers with 0 mismatches, **but it was not independently checked against an authenticated official API and primary benchmark**. It is not used as verified financial outcome evidence.

## Unexecuted / blocked stages

| Stage | Status | Reason |
|---|---|---|
| 15A local Windows preflight | **BLOCKED** | Windows computer and source E: volume not mounted |
| 15A script repairs | **SOURCE COMMITTED / NOT RUN** | No PowerShell for runtime verification |
| 15A Rust compile | **NOT RUN** | Cargo/rustc unavailable; no complete runnable checkout |
| 15B root + 18 standalone test suites | **NOT RUN** | Compilation and real toolchain blocked |
| 15C BTC source SHA subset | **PASS — FILE INTEGRITY ONLY** | 24 SHA checks, catalog SHA verified |
| 15C original market Rust replay | **NOT RUN** | Cargo and original Windows data environment unavailable |
| 15C other Kalshi/Perps/economics | **BLOCKED** | Original venue schema and independent proof unavailable |
| 15D reconstructed original strategies | **BLOCKED** | Exact frozen original source/version manifest mismatch |
| 15D real strategy backtests/optimization/OOS | **NOT RUN** | Software/runtime and authenticated economic inputs absent |
| 15E resource benchmarks | **NOT RUN** | 16 GiB Windows machine unavailable |
| 15F GitHub evidence | **PARTIAL — PUBLISHED** | Static source/data proof only; no compiler or benchmark logs to publish |

**Rust test results:** 0 passed, 0 failed, **0 Rust tests executed**. Pending test suites are not reported as passing. The completed ZIP/SHA checks are separately counted as source-file integrity validations.

## Go/no-go

- Software correctness: **NO-GO — compilation/tests not executed**.
- Original historical research readiness: **NO-GO — source integrity subset only, no complete book/fee/settlement proof**.
- Operational/paper service readiness: **NO-GO — no actual Windows recovery/resource evidence**.
- Strategy profitability: **NO VERIFIED POSITIVE EDGE OR OOS PERFORMANCE**; do not fabricate zero or positive P&L.

## Resume safely

See `WINDOWS_OPERATOR_HANDOFF.md` for a manual, exact-SHA, no-Codex local PowerShell path. Phase 15B–F must not inherit PASS statuses until actually executed and logged on the isolated Windows research checkout. Do not merge PRs or change production collectors, paper services, trading configurations, original source data or active databases.

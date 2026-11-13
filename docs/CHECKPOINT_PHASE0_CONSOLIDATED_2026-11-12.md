# Phoenix Rust — Phase 0 consolidated integration checkpoint
**Date:** 2026-11-12 PST / 2026-11-13 UTC. **STATUS: PARTIAL — CI RUNNER ALLOCATION BLOCKED.** This is NOT a Phase 0 completion declaration.

## Verified GitHub state and source identity

- Repository `Armaan-Mohapatra/phoenix-rust` was independently confirmed **PRIVATE**, with authorized write permission.
- `main` was observed at `de4d23db000509dd5ebd5916b10aa821308c6a45`; no PR was merged or main altered.
- Existing draft PR #16: `b4ff18ae38ca59af2756dd0faa4a6e94c58f6b41`.
- Existing draft PR #17: `32f1c3271412592ba3df07af6adc4be4a79ee3ce` (latest real-history source branch).
- Existing draft PR #18: `ca4f26138d2f46dbecb4f5eb87ed28188efb64fe` (separate redb identity index).
- Existing draft PR #19: `5008d5bf6da213d6fec737a95b40c178a9f2d2e3` (sealed Parquet + redb integration).
- New **draft PR #20**: https://github.com/Armaan-Mohapatra/phoenix-rust/pull/20, head `codex/phase0-consolidated-v1`, base `codex/sealed-index-integration-v1`.
- Actual GitHub compare merge-base for #17 and #19: `99fee1467b1eaf62b95c0cb92fb37c5dde273189`. Seven newer commits existed **on each side**, not in the other.
- Consolidation commit `a0883920081e4847236c7fed852aafe64997e0ba` was created as a normal child of #19's head, never a force-push. Test workflow/coverage commit `7085d579d694e90b8c1a5dc71bcaa2e916c09fb8` followed. This documentation-only commit will add a new final HEAD.

## Actual integration work and exact blob proof

GitHub compare determined 11 PR #17 affected paths needing reconciliation: 10 safely copied **by their original Git blob SHAs**, 1 overlapping `docs/RELEASE_GATES.md` manually combined without dropping either branch's text.

PR #17 exact files on the new branch:
- `integrations/source-loader/src/lib.rs`: shared-sid/channel sequence audits, preserved source timestamps and bounded Parquet loader.
- `integrations/source-loader/scripts/run-original-btc.ps1`: read-only Windows source execution with catalog/allowlist guards.
- `.github/workflows/source-loader.yml`: Rust source-loader/PowerShell syntax validation.
- `README.md`, `docs/CF_PRIMARY_SETTLEMENT_REQUEST.md`, `docs/CHECKPOINT_HISTORICAL_SOURCE_2026-11-12.md`, `docs/CHECKPOINT_ORIGINAL_SOURCE_CHANNEL_2026-11-12.md`, `docs/HISTORICAL_SOURCE_CHECKPOINT.json`, `docs/SOURCE_ORIGIN_REPLAY.md`, `docs/WINDOWS_ORIGINAL_BTC_HANDOFF.md`.

**Git tree audit verified all ten original blob SHA matches** and **zero missing paths** from either PR #17 or PR #19. Among 72 PR #19 existing source/workflow blobs inspected, **zero unexpected changes** (the two intentional PR #17 overrides were excluded from "unchanged" comparison). PR #19's exact redb, indexed-archive, recovery and 1,024-event tests remain preserved. The current consolidated tree had 109 files before this checkpoint; 16 standalone Cargo manifests, nine root workspace crates. Historical source evidence and prior checkpoints are not overwritten.

`.github/workflows/phase0-consolidated.yml` adds a Windows+Linux comprehensive matrix:
- root `cargo test --workspace --all-targets --offline` plus Clippy;
- all 16 standalone crates in storage, historical and quantitative groups;
- HftBacktest test with `--features upstream-hft` only, no live;
- explicit package-inventory comparison and per-manifest cargo metadata;
- PowerShell launcher parser (not execution).
See [exhaustive test coverage](PHASE0_TEST_COVERAGE.md).

## Actual CI outcome: BLOCKED, not passed

The Phase 0 **push workflow** [run 43375333014](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43375333014) and associated **PR workflow** [run 43375397417](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43375397417) both concluded `failure`, **before any runner assignment or test step**. Nine jobs in each run reported `runner_id=0`, `steps=[]`, and completed within 1–4 seconds. A retry of **only the manifest-coverage job** was accepted by GitHub but again failed before runner assignment, with zero steps. The existing `Rust offline tests`, `Combined Rust integration matrix`, and `source-loader` workflows triggered for PR #20 also failed the same way.

These failures are **not evidence of any Rust compilation or assertion failure**. They equally do **not** establish that this consolidated branch passes CI. Root cause of runner refusal is not confirmed by the available GitHub APIs; possible account spending/Actions availability/runner policy must be checked in the GitHub Actions UI and settings by an authorized user. Do not "fix" by deleting assertions or altering production.

Previously **separate** latest PR #17 CI workflows and PR #19 CI workflows were successful on Linux/Windows at their respective old HEADs. Those successes **cannot be inherited** as a test of #20. Container-backed local verification is not an alternative here: `cargo`, `rustc`, and `rustup` are absent and the official rust distribution hostname cannot resolve.

**State:** Combined compile, unit/integration tests, Clippy and metadata checks on #20 are **NOT RUN (pre-run infrastructure failure)**. Formatting check is **NOT RUN**, and a sweeping rustfmt reformat was deliberately excluded from focused Phase 0 work.

## Security and branch protection

- No real orders, enabled live exchange clients, credentials, raw private archives, production collectors, active databases, WAL files, or other source trading data were touched. Existing original draft branches are preserved.
- `GET /branches/main` confirms `protected=false`, no required status checks. Branch protection detail API returned 403 integration permission; rulesets API returned 403 indicating account plan restriction for private repository. No repository-wide setting was changed.
- **Recommendation:** require PR reviews, prohibit force-push/deletions, restrict merges to passing Phase 0 checks and enforce status checks. Depending on private-repository plan, enabling protection/rulesets may require a plan upgrade. Do not change visibility to public or remove private confidentiality for branch protection.

## Remaining Phase 0 completion gate

1. Resolve Actions runner allocation/access. Check the GitHub repository Actions UI for billing/spending limits, permitted GitHub-hosted runners and organization/repository Actions policies; do not infer a specific root cause without UI evidence.
2. Re-run the existing single Phase 0 matrix on the **same exact consolidated branch** after runner allocation is restored.
3. Investigate and correct **actual code test failures only if they occur**, and then re-run full Windows/Linux CI. Verify all nine matrix jobs and pre-existing overlapping suites. Record actual pass/fail and final SHA in a new completion checkpoint.
4. Keep Phase 0 PARTIAL until that succeeds; do not begin Phase 1, merge to main or touch source/production.

## Unchanged historical limitations

No full real-source Rust execution of original 24 Parquet members; no authenticated initial orderbook; no independent CF primary BRTI settlement; no claim of verified after-cost execution P&L. Archive publisher still has a 250k event in-memory ID map and unproven Windows metadata power-loss behavior. All published research checkpoints are retained verbatim.

**This checkpoint faithfully records code reconciliation and a pre-run CI infrastructure blocker.**

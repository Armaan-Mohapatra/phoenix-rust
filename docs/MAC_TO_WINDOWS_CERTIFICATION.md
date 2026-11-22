# Phoenix: Mac certification -> GitHub publication -> isolated Windows validation

**Status: HANDOFF PLAN ONLY.** This document has not run tests, published the Mac-local fixes, or deployed any runtime. It is intentionally maintained on a separate docs branch, not on `main`. This is a coordination artifact, not a release gate override.

## Scope and source of truth

Develop in three separate repositories, preserving their histories and working trees:

| Repository | Mac-local branch and last user-reported commit (NOT verified in GitHub) |
| --- | --- |
| `Armaan-Mohapatra/phoenix-rust` | `mac/phase1-replay-determinism` at `606d3355737b65f0b47b38eebe9d1e381cf54904` |
| `Armaan-Mohapatra/phoenix-agent-arena` | `mac/phase3-arena-integrity` at `9afc66fd9bc95926e4a78254ec5aff5ae818b023` |
| `Armaan-Mohapatra/kalshi15m-market-data` | `mac/phase4-collector-integrity` at `7f7115acc8080a8627a4af20781c84982aa00d15` |

These SHAs were reported by a local Claude session, not fetched from GitHub. Check all three with `git rev-parse HEAD`, `git status --porcelain=v1`, `git branch -vv` and `git remote -v` before making any changes or pushing.

The three mac/* branches are not yet published. Do not substitute `phoenix-rust/main`: it does not contain the active feature stack. The GitHub branch for this *document* is `docs/mac-validate-before-windows-20261121` based on Rust feature branch `feature/vibe-toolbox-phoenix-20261121`. Read it using `git fetch origin docs/mac-validate-before-windows-20261121` and `git show origin/docs/mac-validate-before-windows-20261121:docs/MAC_TO_WINDOWS_CERTIFICATION.md`; do not merge this docs branch into untested code as a prerequisite.

## Observed Mac baseline, NOT final certification

From the operator-supplied Claude report:
- Rust: `cargo nextest` 165/165, `verify.sh` PASS=61 FAIL=0 BLOCKED=1 NOT_RUN=3; fmt, clippy, deny, gitleaks reported clean.
- Arena: 170 Python tests passed.
- Collector: 185 Python tests passed, 1 skipped, 12 blocked because original frozen strategy fixture files are on Windows.
- A prior failed golden fingerprint test was fixed forward. Verify the exact FINAL commit, not an intermediate SHA.
- Local changes include macOS file-lock compatibility and book-integrity tests.
- `checkpoints/WINDOWS_HANDOFF.md` and `checkpoints/SHA_MANIFEST.txt` reportedly exist on the Mac, not GitHub.
- Unfinished: Rust 1.78 MSRV check; hftbacktest; original-data replay; Windows runtime; cross-repo quote bridge; archive recovery deep audit; complete paper-journal and source-loader audits; fee schedule/independent settlement evidence; Python dependency pins and collector cryptography advisory review.

Treat these as **prior claims requiring re-execution on final HEAD**. Do not imply that missing source, original data or independent settlement was validated.

## Gate M0: Mac workspace and integrity

- Work in isolated local clones/worktrees; inspect repository-specific `AGENTS.md`, `CLAUDE.md`, `SECURITY.md`, `docs/RELEASE_GATES.md` and active checkpoints.
- Confirm expected upstream repositories, visibility (Rust and collector are PUBLIC; Arena is PRIVATE), exact local branch HEAD and ancestry. Never force-push, rewrite shared history or blindly merge stacked PRs.
- Record macOS version, architecture, Rust/Cargo, toolchains, uv-managed Python, and installed tools; verify Python `ssl`, `xml.parsers.expat`, and environment imports without changing system Python. Mac setup's previous `project: unbound variable` bug must be reproduced and fixed in the separate local bootstrap script, then tested as idempotent.
- Confirm no secrets, credentials, strategy private data, original tapes, raw exports, SQLite/WAL/SHM, archives, `.venv`, build outputs, hidden auth files, or machine-specific private manifests are tracked or about to be pushed.
- Use bounded scratch/output folders, leave Windows data sources and active services untouched.

## Gate M1: Mac test matrix, run after the FINAL commit

Run all applicable targets in each repository, collecting actual exit status and a sanitized local evidence report:

1. Rust root: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace --all-targets` AND `cargo nextest run --workspace --all-targets` as appropriate; cover ignored/doc tests explicitly if specified by the repo. Run `scripts/verify.sh` with supported flags, not guessed ones. Run every relevant standalone `integrations/*` crate using its own Cargo manifest/test instructions, with bounded concurrency.
2. Rust dependency controls: `cargo deny check` and local `gitleaks` (redacted results), no automatic dependency changes. Test Rust 1.78 compatibility in an isolated toolchain if feasible; otherwise BLOCKED with reason.
3. Arena: syntax/import checks, full `unittest` discovery and targeted regression tests for lock contention, release-on-kill, portfolio conservation, duplicate settlement, and recovery; confirm macOS compatibility does not replace Windows `msvcrt`.
4. Collector: import/compile and complete tests; targeted snapshot/sequence, archive checkpoint recovery, gap, idempotence and low-disk refusal tests. Keep the 12 authentic frozen-strategy fixture tests BLOCKED if files are absent; never mock away the requirement and call them passed.
5. Cross-repo contract fixtures: producer -> collector -> normalized quote -> Arena ingestion -> modeled ledger; verify `kalshi_quotes.csv` source identity/schema, units, timestamps and quote-age restrictions. If actual Windows producer is unavailable, build hermetic contract tests only and mark authentic bridge BLOCKED.
6. Determinism, accounting and security: repeated replay, batch/thread/cache variants, SHA-stable results-only golden pin, timestamp/no-lookahead invariants, fee rounding, settlement provenance, journal restart/corruption, no duplicate cash/position effects. Clearly distinguish synthetic/venue-only/independently corroborated outcomes.
7. Audit Python dev dependency pins. Investigate the reported `cryptography 47.0.0` advisories; only upgrade in a separate reviewable change with compatibility and signing regression tests. Avoid changing operational collector dependencies without explicit approval.

**Do not mask exit codes with `| tail`, `grep`, redirection, or report formatting.** If piping to `tee`, enable `set -o pipefail` and record the actual first failing command. A historical incident falsely treated a failing nextest run as passing.

Each check is PASS, FAIL, BLOCKED (with named prerequisite) or NOT_RUN (with reason). `0 fails` for executed checks does not imply the whole release is green.

## Gate M2: Publish only after M0/M1 PASS

- Verify all macOS-executable tests pass on the EXACT HEAD that will be pushed. Where a legitimate platform-specific prerequisite is missing, document BLOCKED in the manifest; a FAIL is not a BLOCKED.
- Audit tracked and untracked changes and `.gitignore`. No raw research data or PII. Scan staged changes and history for secrets; never post raw secret scan output to issues.
- Confirm `git log -1` on each branch and check existing `.github/workflows` before every push, because the user does NOT authorize paid GitHub Actions. Ensure the pushed HEAD includes `[skip ci]` if applicable, and avoid workflows triggered by other events. If skip cannot be guaranteed for a repository's workflow configuration, STOP and report rather than incur paid CI. Never change repository Actions settings or GitHub billing automatically.
- Push the three **existing locally tested `mac/*` branches**, not `main`. No overwrites, force pushes, merges, or automatic PRs. Push should include code changes and sanitized concise validation receipts/checkpoints, never binary data dumps or internal credentials.
- Read back the remote branch commit SHA via `gh api`/GitHub and verify it equals the exact local tested commit. If a post-test metadata or CI-skip commit changes the SHA, rerun at least the applicable relevant tests, verify no code changed, and record the new tested SHA before claiming PASS.
- For each repository, report public/private visibility, branch name, local SHA, remote SHA, full command/status counts, logs/checkpoint locations, and any platform-blocked checks.
- Update sanitized GitHub issues/PR drafts only after reviewing workflow triggers. Never merge unreviewed stacked branches, deploy or enable orders.

## Gate W0: Windows installation (NOT authorized as part of Mac publication)

Only after the M2 GitHub SHAs are verified, prepare **reviewable** Windows PowerShell instructions with an exact-SHA detached or isolated checkout. Treat the original data roots as READ-ONLY. Do not inspect or mutate active SQLite/WAL, open event segments, production collectors or services. Do not automatically run or restart anything on Windows.

Use a separate scratch/runtime path such as an isolated Phoenix research lab. Validate:
- Native Windows toolchain and all Rust root + standalone tests, Python suites and authentic fixture tests;
- Source hashes, verified finalized manifests, sequence continuity, book state, precise fees and independent settlement tiers;
- Bounded replay with determinism parity against Mac (report platform differences, never re-pin silently);
- Collector/quote producer/arena schema compatibility and real-time freshness, ledger restart invariants and recovery;
- Paper-only isolated 24h+ soak after explicit authorization, with durable journal, disk/CPU/RAM budgets, error injection and rollback.

Do not label Windows validated, 24h-soak passed, paper-live ready, profitable or production-operational until evidence independently supports that status.

## Deliverables

A. `MAC_CERTIFICATION.md` (sanitized), exact full SHA matrix, commands and PASS/FAIL/BLOCKED/NOT_RUN per component.
B. Small sanitized machine-readable `mac-validation.json` with test counts, provenance and SHA fingerprints; no raw events/markets/strategies/credentials.
C. Three pushed `mac/*` branches after gate M2, read-back verified; otherwise an explicit non-publish verdict with blockers.
D. Updated `WINDOWS_HANDOFF.md` and `SHA_MANIFEST.txt` referencing only published and validated SHAs.
E. Final decision: `MAC_CERTIFIED_FOR_WINDOWS_TESTING`, `MAC_PARTIALLY_VALIDATED`, or `BLOCKED`. Do not confuse this with `WINDOWS_CERTIFIED`.

**Security and operating constraints:** No real orders; no new account credentials; no live collector/control changes; no actions on source drives; no background continuous trading; no paid CI; no Windows deployment in this stage; no secret, raw-data or strategy leak to public GitHub.

# Phoenix isolated Windows validation handoff

**Prepared on the Mac. NOT executed on Windows. Windows deployment is NOT authorized by this document.**
Paper-only. No real orders, no credentials in logs, no collector restarts, no scheduled tasks, no edits to
`E:\TradingNextData` or any active collector/database/WAL. See `docs/MAC_CERTIFICATION.md` for what the Mac proved.

## 0. Rules
- Original data roots are READ-ONLY. Never open an active SQLite/WAL/SHM or an open event segment.
- All work happens under `E:\PhoenixLab` (never under a directory named TradingNextData, KalshiData,
  KalshiRuntime or paper15m_runtime; the archive crates refuse those by design).
- Do not label anything "Windows validated", "soak passed" or "paper-live ready" without evidence listed below.
- Stop at the first unexplained FAIL. Never re-pin the golden hash or weaken a test to proceed.

## 1. Exact-SHA isolated checkout
The published branch tips are docs-only descendants of the tested code SHAs. SHAs below are the TESTED code;
the exact pushed tips are recorded in GitHub issue #49 (completion comment) and must be read from GitHub.
```powershell
$Lab = "E:\PhoenixLab"; New-Item -ItemType Directory -Force "$Lab\src","$Lab\out","$Lab\data-copy" | Out-Null
$Repos = @(
 @{n="phoenix-rust";           b="mac/phase1-replay-determinism"; tested="4b5325833ba5c6aefb046141b29a926027a9202c"},
 @{n="phoenix-agent-arena";    b="mac/phase3-arena-integrity";    tested="e1d685cf79cffef571955fecfc6c6badbd4d879f"},
 @{n="kalshi15m-market-data";  b="mac/phase4-collector-integrity";tested="8ffc40bfa63ec184112836255deb7e38f75c76f0"})
foreach ($r in $Repos) {
  $d = "$Lab\src\$($r.n)"
  if (-not (Test-Path $d)) { git clone --no-checkout "https://github.com/Armaan-Mohapatra/$($r.n).git" $d }
  git -C $d fetch origin $r.b
  $tip = (git -C $d rev-parse "origin/$($r.b)").Trim()
  git -C $d merge-base --is-ancestor $r.tested $tip; if ($LASTEXITCODE) { throw "$($r.n): tested SHA is not an ancestor of the tip" }
  $changed = git -C $d diff --name-only "$($r.tested)..$tip"
  if ($changed | Where-Object { $_ -notmatch '^docs/' }) { throw "$($r.n): tip differs from tested code outside docs/" }
  git -C $d checkout --detach $tip
  "$($r.n) tip=$tip tested=$($r.tested)" | Tee-Object -Append "$Lab\out\checkout_manifest.txt"
}
```
Never use the production checkouts. Never `git pull` into a working branch.

## 2. Toolchain and dependencies
```powershell
winget install -e --id Rustlang.Rustup; rustup default stable; rustup toolchain install 1.78.0 --profile minimal
cargo install cargo-nextest cargo-deny --locked
py -3.12 -m venv "$Lab\venv-kalshi"; & "$Lab\venv-kalshi\Scripts\pip" install -r "$Lab\src\kalshi15m-market-data\requirements-lock.txt" pytest
py -3.12 -m venv "$Lab\venv-arena";  & "$Lab\venv-arena\Scripts\pip"  install -r "$Lab\src\phoenix-agent-arena\requirements-dev.lock.txt" pytest
$env:CARGO_TARGET_DIR = "$Lab\out\target"; $env:TMP = "$Lab\out\tmp"; $env:TEMP = $env:TMP; New-Item -ItemType Directory -Force $env:TMP | Out-Null
```

## 3. Native regression matrix (every row needs a real exit code and a saved log under `E:\PhoenixLab\out\logs`)
Mac baselines in brackets; Windows should meet or exceed them.
| # | Command (cwd = repo) | Mac baseline |
|---|---|---|
| R1 | `cargo fmt --all -- --check` | PASS |
| R2 | `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| R3 | `cargo test --workspace --all-targets` and `cargo test --workspace --doc` | PASS |
| R4 | `cargo nextest run --workspace --all-targets` | 165/165 |
| R5 | each `integrations\*` (except hftbacktest-adapter): `cargo test --locked --all-targets` in that directory | all PASS |
| R6 | `cargo test --locked -- --ignored` in `integrations\durable-archive` (needs Developer Mode/admin for symlinks) | NOT_RUN on Mac; first execution is Windows |
| R7 | `cargo test --locked --release -- --ignored` in `integrations\disk-identity` (synthetic 1M ids) | PASS 17 s |
| R8 | `cargo deny check`; `rustup run 1.78.0 cargo check --workspace --all-targets` | PASS / PASS |
| R9 | `scripts\validate-phoenix-locally.ps1` (read it first; it is the project's own Windows gate) | NOT_RUN on Mac |
| A1 | Arena: `python -m compileall -q .`; `python -m unittest discover -s . -p "test_*.py"`; `python -m pytest -q` | 177 passed. Windows uses real `msvcrt` (`oslock.py` unused) |
| A2 | `uvx pip-audit -r requirements-dev.lock.txt --no-deps --disable-pip` | clean |
| K1 | Collector: `python -m pytest -q` WITH authentic `strategy\` files present | 189 passed/1 skipped + the 12 frozen-strategy tests must PASS |
| K2 | `gitleaks detect --redact` per repo | clean |

The 12 `FrozenStrategyTests` are BLOCKED on the Mac. Provide `strategy\` (frozen config, source snapshot, authority
manifest) by READ-ONLY copy into the isolated checkout; it is gitignored and must never be committed. Any failure there is a real finding.
Golden pin: `cargo test -p phoenix-campaign golden_economic_results_are_pinned` must give
`bf4123a18294ff3084d8dfe837da3508a5d3f04f8f6a519fae8693c10dd5e76b` (f64 statistics). If it differs, STOP and report platform
differences; do not re-pin.

## 4. Read-only authentic finalized-data replay
1. List finalized (closed) partitions only. Copy them into `E:\PhoenixLab\data-copy` with robocopy (source read-only, no `/MIR`, no `/MOVE`);
   record SHA-256 of every copied file in `out\source_hashes.txt` and compare with the originals.
2. Run the campaign/replay CLI (`cargo run -p phoenix-cli -- --help` for the exact subcommands; do not guess flags) on a bounded slice
   (a few hours of market data, not months). Outputs to `out\replay\`. Run twice, then once with different batch size and thread count.
   The results sets (`results`, `candidates`, `split_counts`, `normalized_input_sha256`) must be byte-identical. Do NOT compare
   `economic_fingerprint`/`experiment_sha256`: they embed the git HEAD by design.
3. Run `phoenix-collector-parity` on the same slice; any mismatch is a defect.
4. Fail-closed checks on COPIES only: flip one byte of a copied source; the run must refuse. Corrupt a cache file; it must refuse.
5. Do not evaluate the untouched confirmatory holdout except for one pre-registered final evaluation.

## 5. Source and settlement reconciliation
- Record, per market, which tier its final outcome has: `VenueFinal` (venue-reported; the code never upgrades this itself) or independently
  corroborated. Never merge the tiers in a report.
- Fees: campaign fees are HYPOTHETICAL (standard taker formula x multiplier). Authenticated historical fee schedules must be supplied
  before any net P&L is called verified; until then label results UNVERIFIED.
- Reconcile each portfolio: initial cash + sum(portfolio_events) equals reported cash; running balance never negative; exactly one
  `SETTLEMENT_PAYOUT` per settlement id (the Arena invariant test in `test_competition.py` is the model; run it on a COPY of the ledger).

## 6. Paper journal crash/restart (isolated fixtures, then a copy of a journal)
Hard-kill (`Stop-Process -Force`) the paper process mid-cycle in the lab; restart; verify no duplicate signals/fills/settlements and the
ledger equals the pre-kill state. Repeat once during a settlement window. Corrupt one journal record in a COPY; the loader must refuse
and never silently skip.

## 7. Collector-to-Arena integration (the authentic bridge is UNVERIFIED)
The producer of `kalshi_quotes.csv` is not in these repositories. On Windows:
1. Identify the actual producer script and its source commit; record in `out\bridge_identity.txt`.
2. With a production-copy finalized slice, produce a `kalshi_quotes.csv` in `out\` and run `test_collector_quote_contract.py` logic against
   its rows: `ya == 1 - nb`, `na == 1 - yb`, `0 <= bid <= ask <= 1`, ask depth equals the opposite bid's depth, 4-decimal precision,
   `source_received_at_utc <= decision time` and age <= 1000 ms, `sequence_gap_or_recovery_flag == NONE`, `book_valid == true`.
3. Run `PHOENIX_KALSHI_SRC=<lab collector checkout>\src` with the Arena suite so the contract tests execute (they skip otherwise).
Until step 2 passes on real output the bridge stays BLOCKED.

## 8. Limits (enforce, then log)
Process budgets for every lab run: CPU <= 4 cores (`start /affinity`), RAM <= 8 GB (Job Object or `Start-Process` watchdog that kills at the cap),
`out\` <= 50 GB (watchdog), alarm and stop when `E:` free space < 15%. Log rotation: <= 100 MB x 10 per component. Keep
`CARGO_BUILD_JOBS` <= 4. Never run lab work at higher priority than the production collector.

## 9. 24-hour paper-only soak plan (requires separate explicit authorization to start)
Preconditions: sections 3-7 PASS, K1 PASS, paper-only configuration verified by reading it, no credential able to place orders.
Run: isolated process, durable journal in `E:\PhoenixLab\out\soak`, health file every 60 s (heartbeat, sequence gaps, queue depth, disk, RSS, CPU).
Error injection (documented times): one hard kill + restart, one simulated disk-full on the LAB volume only, one source-gap fixture.
Pass criteria: zero duplicate signals/fills/settlements, ledger reconciles at every restart and at the end, no unbounded memory/disk growth,
all sequence gaps surfaced (none promoted to verified data), production collectors untouched (compare their service state and data-root hashes before/after).
Result classes: SOAK_PASSED requires all criteria; anything else is SOAK_FAILED or SOAK_INCONCLUSIVE with logs. A passed soak is still not profitability evidence.

## 10. Rollback and recovery
Stop the lab process. Delete `E:\PhoenixLab\out` (never anything else). To go back: `git checkout --detach <previous tip>` in the isolated checkout.
Production data and collectors are never modified, so there is nothing to restore. Keep `out\checkout_manifest.txt` and `source_hashes.txt` as the audit trail.

## 11. Decision gate
Proceed beyond isolated validation only when: all section 3 rows PASS on Windows, K1 including the 12 frozen tests PASS, replay determinism and
fail-closed checks PASS on authentic copies, bridge identity and contract verified on real output, cryptography advisory decision recorded,
and the owner gives separate explicit approval.

# PHOENIX — Phase 15 Windows operator handoff

**STATUS: PREPARED_NOT_EXECUTED.** This is a procedure for the real Windows laptop, not evidence that the ChatGPT Linux sandbox ran any Rust builds. Do not run on production collector folders or source drives.

## Step 1 — Preflight the actual local environment

Open a normal PowerShell terminal on the laptop:

~~~powershell
Test-Path -LiteralPath 'E:\TradingNextData'
Get-PSDrive -Name C,E | Select-Object Name,Free,Used
Get-CimInstance Win32_OperatingSystem | Select-Object TotalVisibleMemorySize,FreePhysicalMemory
git --version
rustup toolchain list
rustc --version
cargo --version
~~~

Keep E:\TradingNextData, E:\KalshiData, C:\KalshiRuntime and active database/WAL/collector folders strictly read-only. Check RAM, filesystem and offline Cargo cache before building. Root workspace advertises Rust 1.78.0; optional pinned HFT adapter requires 1.91.1. Never silently change the MSRV. Missing Windows MSVC linker, Rust components or dependencies are BLOCKED until independently resolved.

## Step 2 — Dedicated research checkout, NEVER production

After independently confirming paths are safe and are not symlinks/junctions into any production root, an operator with private GitHub access can create a fresh checkout. GitHub source retrieval is an explicit one-time network operation, distinct from the offline market/data tests. If an isolated clone already exists, use it instead; never overwrite a collector checkout.

~~~powershell
$Repo='E:\PhoenixResearch\phoenix-rust-phase15-source'
$Output='E:\PhoenixResearch\phoenix-rust-phase15-output'
$Remote='https://github.com/Armaan-Mohapatra/phoenix-rust.git'
$Branch='codex/phase15-validation-repairs-v1'
git clone --single-branch --branch $Branch $Remote $Repo
~~~

Get the **full exact SHA from the head of the Phase 15 corrective draft PR** and paste it below. It is NOT the original Phase 14 SHA.

~~~powershell
$Expected='<PASTE_FULL_40_DIGIT_PHASE15_PR_HEAD_SHA>'
$Observed=(& git -C $Repo rev-parse HEAD).Trim()
if($Observed -ne $Expected){throw "SOURCE_COMMIT_MISMATCH $Observed"}
~~~

No moving branch name is a substitute for a verified full commit SHA. If the code changes, update the expected SHA and use a clean isolated checkout.

## Step 3 — Phase 15A/B local tests, controlled manual launch

Run this only after intentionally authorizing local, isolated tests. It requires ExecuteValidation; omit all heavy optional flags initially.

~~~powershell
powershell.exe -NoProfile -NonInteractive -File "$Repo\scripts\validate-phoenix-locally.ps1" -RepoRoot $Repo -OutputDirectory $Output -ExpectedSha $Expected -ExpectedBranch $Branch -ExecuteValidation
~~~

This invokes root workspace AND all 18 standalone crate metadata, rustfmt, Clippy, unit/integration and documentation tests with Cargo build parallelism default 2, target budget 8 GiB and no production output. It excludes the ignored million-ID stress case, original BTC/multi-market replay, optimization, live network, collectors, and orders.

Review the generated isolated-output subfolder: preflight.json, results.json, summary.json, resolved_lockfile_sha256.json, optional sampled_resources.json and individual command logs. The script records PASS/FAIL/BLOCKED. If any required test fails, capture exact compiler output, do not skip the test, and do not claim software validation. Repair on a new reviewed branch/commit, then rerun the new SHA.

## Step 4 — Explicit later opt-ins only

Only after software correctness has been established and original input evidence independently reviewed:

- IncludeOptionalHft: pinned upstream backtest-only adapter, Rust 1.91.1 toolchain, dependencies must be available
- IncludeStress: the one ignored million-ID redb test, serial and isolated; never an unbounded production scan
- IncludeOriginalSourceAudit with HistoricalZipDirectory: six exact original October BTC ZIPs, read-only; narrow 24-member allowlist/source catalog SHA gate
- IncludeMultiMarketReplay with MultiMarketPlan and MultiMarketSourceZips: externally verified plan and bounded explicit ZIPs, not open-ended source discovery
- IncludePerformance: only system-level RAM/CPU/free-disk snapshots; not peak-process memory or real event throughput
- IncludeHypothesisResearch: records BLOCKED unless a pre-registered strategy/market/OOS plan has authentic data; no unattended optimization job

Do not use heavy original-source/stress flags automatically. Prioritize safe bounded batches on a 16 GiB laptop; avoid excessive external archive disk I/O.

## Publication and safety

For actual test outcomes, publish private GitHub evidence under validation/phase15: executed source SHA, Cargo.lock hashes, commands, exit statuses, exact test counts, negative results, source/member hashes, repaired commits, bounded resource measurements, and reproducible summaries. Keep raw archives, credentials, account records and licensed primary data OUT of GitHub. Use [skip ci] on any evidence commit and do not run paid Actions.

Separate software correctness, source completeness, independent settlement and fee proof, original strategy parity, untouched OOS performance, resource readiness and deployment authorization. An uploaded ZIP checksum match is not Rust compilation, a real venue fill or validated after-fee profits.

**Current status:** 15A Windows/Cargo BLOCKED, 15B NOT RUN, 15C 24 original BTC Parquet hashes verified only, 15D historical strategies NOT RUN, 15E NOT RUN, 15F PARTIAL evidence published. Do not merge, deploy, modify collectors or submit exchange orders.

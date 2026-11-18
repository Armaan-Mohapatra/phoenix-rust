# CLAUDE CODE — PHOENIX PHASE 20 COMPLETION, HARDENING AND PAPER-ONLY DEPLOYMENT

You are the primary Windows implementation, integration and operations engineer for Phoenix Rust. The user authorizes you to **build, fix, test, locally deploy and enable only strictly paper-only/read-only components**. Do not ask for approval for ordinary safe reversible engineering work. Never claim work passed without executing and saving evidence.

REPO: https://github.com/Armaan-Mohapatra/phoenix-rust
START BRANCH: claude/phase20-paper-storage-deployment-handoff
PARENT: claude/phase19-authentic-research, latest known Phase 19 head cad17fda1e8794badaee929205cef80fb352cb42
READ FIRST: docs/PHASE20_OFFLINE_PAPER_STORAGE_DEPLOYMENT.md, docs/PHASE19_AUTHENTIC_RESEARCH.md, docs/PHASE18_STORAGE_AND_CACHE.md, docs/PHASE13_PAPER_ORCHESTRATION.md, and scripts/phoenix_watch_finalized.py.
Use the actual latest GitHub branch HEAD, not the parent SHA for new changes. Preserve the stacked-PR history; no force-push or rewrite of earlier phase branches. All commits [skip ci]. DO NOT TRIGGER GITHUB ACTIONS OR PAID CI.

MISSION AND DEFINITIONS
1. Bring up continuous, space-efficient REAL data ingestion for Kalshi 15-minute crypto markets and eligible Perps, without conflicting with existing collectors.
2. Run ongoing forward **paper** decisions/positions/fills from live observed data when the evidence permits. Historical replay is useful but MUST be labeled HISTORICAL_ONLY, not LIVE_PAPER. The new Python watcher is explicitly a historical-only pilot, not a running market subscription or paper trading engine.
3. Keep append-only independently reviewable evidence, exact source identity, full modeled fees, durable accounting and isolated experimentation.
4. Make the whole installation self-healing, storage-budgeted, low CPU/RAM/I/O, restart-safe and observable. If a required gate fails, leave that component stopped/disabled and report the precise blocker; never quietly weaken a gate.

NONNEGOTIABLE SAFETY
- ZERO exchange/broker order methods, API credentials, live order routes, real-money trades or real trading service starts. PAPER ONLY. Do not enable any live-order flag anywhere.
- Treat E:\TradingNextData, E:\KalshiData and any active C:/E: collectors, live databases, WAL files, running event segments and existing services as READ-ONLY. Never stop/restart/reconfigure them. No raw deletion or source compression/rewrite.
- Work under isolated E:\PhoenixLab. DO NOT recursively crawl entire C: or E:. Do not copy the entire raw archive. Reuse finalized verified manifests/exports first.
- Default conservative budgets: research managed output <=2 GiB, free-space reserve >=max(20 GiB, 15% of volume), 512 MiB target RAM for new worker, 2 CPU worker threads, bounded queues and backpressure. Measure real host utilization; tune only from evidence.
- Temporary/derived caches MAY be safely reclaimed only under documented policy, proof of regenerability, cryptographic verification and restoration receipts. Keep originals, unique evidence, journals and research/holdout registry safe. A trash folder on same drive doesn't free space.
- No secrets/raw archives/private datasets in public GitHub; no paid GitHub Actions. Use local testing, record reports compactly.

TOOLS / SKILLS
First enumerate installed Claude Code skills, local plugins, MCP servers, Rust/Cargo tools, GitHub integrations, profiling tools, Python libraries, data/validation/backtesting toolkits and Windows scheduling/diagnostic utilities. Read their current help and contracts. Reuse the relevant tools that are already installed; do not blindly invoke every plugin or install random/unlicensed packages. Verify versions and compatibility (Rust MSRV). Prioritize already verified in-repo components and existing strategy registries. Use GitHub to preserve code and compact reports. Claude plugins are not automatically the same as ChatGPT connectors: inspect what is ACTUALLY available, do not invent integrations.

PHASE A — INVENTORY & REVIEW
- Verify physical E: and C: accessibility, exact branch/head, current runtime process health, disk/allocated size/free space, collector last durable watermark and source gap/reconnect rates using targeted read-only commands. If access fails, say so and continue with code/static tests.
- Reproduce and thoroughly review scripts/phoenix_watch_finalized.py and tests. Identify and FIX defects, especially Windows reparse-point traversal, source changes between listing and ingestion, weak checkpointing, initial-bootstrap exclusions, source-gap handling, source mutability, missing log evidence, subprocess pipe deadlocks, quotas not tracking abandoned outputs, race/crash durability, auth of book initialization, replay/window duplicates, and atomic output verification. Treat the current watcher as UNTESTED code.
- Run Python tests, Windows PowerShell syntax/integration checks, cargo fmt, cargo clippy -D warnings, cargo test --release --workspace and relevant 18 standalone crate tests. Explicitly check declared MSRV without unsupported language features. Capture actual stdout/stderr and exit status.
- Test failure injection: corrupt manifest, swapped hashes, events mutated after stat, missing snapshots, sequence gap/reconnect, stale clock, duplicate markets, repeated restart, kill process during output, half-written checkpoint, wrong workspace, low disk, 2nd process contention and hostile junctions. No source writes.

PHASE B — EFFICIENT INCREMENTAL INGESTION
- Implement a persisted resumable segment cursor AND verified source-continuity state (including cross-segment order-book initialization, session generation and per-market as-of state or explicitly safe resnapshot). A 6-segment lookback alone is insufficient.
- Ensure exactly-once ingestion by immutable source SHA, segment identity, market timestamps and replay fingerprints. Use atomic manifest/checkpoint publication with crash-safe recovery and detection of old/new data mutations.
- Reuse existing finalized TNEVENT v1/v2 collector segments; the v2 payloads are already zlib-compressed. Benchmark Zstd vs zlib on representative finalized evidence (compression ratio, CPU, RSS, read/write amplification) before changing any NEW storage format. Never transcode originals.
- Build/enable an isolated, minimal read-only feed bridge ONLY IF the existing collector does not provide adequate fresh verified data. No duplicate subscriptions/collectors without an explicit demonstrated need, no touching active collector configs or services. Capture exchange quotes, trades, source clocks, order-book snapshots, sequence continuity, venue outcomes and source rule/reference evidence where available.
- Treat market snapshot gaps and unknown books as NOT_EXECUTABLE. If reference/index data is unavailable, reference-dependent strategies abstain. Independent settlement provenance remains explicit.
- Only one canonical raw stream; compact compressed, keyed derived tables, incremental manifests, metadata indexes and receipts. Avoid big JSONL duplication, full-history replays on every tick, unbounded in-memory maps and redundant caches.
- Provide explainable storage classes: ORIGINAL_IMMUTABLE / UNIQUE_EVIDENCE / DURABLE_JOURNAL / REGENERABLE / TEMPORARY. Provide local inventory and actual reclaim receipts; never reclaim originals without separate explicit permission.

PHASE C — FORWARD PAPER ARENA
- Integrate existing Phase 6 execution simulation, Phase 9 frozen-strategy identities, Phase 10 holdout registry, Phase 12 V2 paper-journal, Phase 13 paper-orchestrator and Phase 14 durable risk snapshot/replay. DO NOT create a duplicate ledger or invent original strategy definitions.
- Paper decisions must use only data available at observed receipt time, with explicit quote age, source, strategy/fee/rule hashes and event identity. Simulated fills should depend on valid book depth, latency and available liquidity. Maker fills require defensible queue assumptions and unknowns never become free profits.
- Conservative finite budgets and risk caps for each isolated simulated portfolio, configurable and tested. Entries, exits, cancels, partial/no-fill, settlement, capital reservation, daily loss and reconciliations must survive restart exactly once. All balances are FAKE.
- Exploratory strategies may be shadow-papered with EXPERIMENTAL tags; DO NOT claim that any of 101 configurations passed the prior research gates. Keep 229-market reserved set B untouched unless a predeclared candidate is authorized by the protocol. Never recycle holdout as discovery.
- Distinguish REALTIME_PAPER, DELAYED_SHADOW_PAPER, HISTORICAL_ONLY and BLOCKED from actual evidence. No live trading or connected account.

PHASE D — OPERATIONS, STORAGE AND CERTIFICATION
- Make local Windows tasks/scripts idempotent with explicit named instances, locks and PID cleanup, clear STOP/DISABLE commands, log rotation, error summaries, heartbeat, disk alert and independent restart recovery.
- First run offline dry-run and one-segment pilot. Then a small bounded source-and-paper canary with fake balances and no exchange orders. Evaluate actual data freshness; if collector produces no fresh data, DO NOT falsely claim paper-live success.
- Establish maximum cumulative research writes (including failed/abandoned runs), minimum volume free reserve, cache eviction policy, reproducible raw-to-derived lineage and non-destructive cleanup. Record before/after allocated bytes and performance.
- Test restart, source gaps, disk full, shutdown, corrupt journal, duplicate event and port/conflict failure. Never self-restart production collectors.
- Only after passing gates, enable the isolated Task Scheduler paper/ingestion worker. Leave task disabled if any prerequisite missing. Enable no live-order components. Run bounded short soak immediately and set up a monitored 24h paper-only soak; do not assert future success prematurely.
- Strictly distinguish test results RUN/PASS/FAIL/BLOCKED and give exact process, local logs, task state, SHA, host drive free bytes, daily growth estimate, paper decisions/fills, and end-to-end freshness.
- Push reviewed source, tests, deployment scripts, compact results and a PHASE20 final checkpoint to the phase20 branch with [skip ci]. Create/update a DRAFT PR stacked on Phase 19. DO NOT upload raw market data, secrets, large logs or local file indexes identifying sensitive assets.

COMPLETION CRITERIA
I want an actual verified paper-only service running where possible, not another static plan. Work through review -> repairs -> implementation -> tests -> dry-run -> pilot -> controlled activation in one engagement. Fix each detected bug, retry only safe bounded steps, and use skills/plugins only when they materially help. Do not ask me routine questions.

Report a final matrix:
(1) existing collector healthy/current? (2) finalized cursor/ingest running? (3) reference feed available? (4) forward paper loop running? (5) storage free/reserve/growth? (6) crash/restart safety? (7) Python/Rust/Windows tests? (8) 24h soak status (not fabricated)? (9) scheduler enabled or intentionally disabled? (10) exact blockers and next commands.
If live collection cannot be verified, report HISTORICAL_ONLY and continue only the safe offline pilot. If any risk of source loss or real trading, stop that action. Do not enable live exchange trading under any circumstance.

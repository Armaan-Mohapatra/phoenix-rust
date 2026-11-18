# Phase 20: paper-only collection and storage deployment handoff

**Status: Handoff tooling committed; NOT deployed, NOT live-paper certified.** Branch:
`claude/phase20-paper-storage-deployment-handoff`, stacked on Phase 19 at
`cad17fda1e8794badaee929205cef80fb352cb42`.

## Existing evidence and limits

- Phase 19 authenticated integrity of 463 **finalized** TNEVENT segments, 158,968,742 events
  and 651 executable-verified 15-minute crypto markets from 2026-10-29 through 2026-10-31.
- Venue-reported outcome is not independently corroborated. No proven profitable strategy:
  101 configs, 0 passed all validation gates; untouched holdout remains untouched.
- `phoenix-tnevent` is **offline** and read-only. It does not subscribe to the market
  feed. Phase 13 paper-orchestrator has modeled paper-cycle interfaces but is not a
  certified unattended collector or live-paper runtime.
- Existing collector runtime and C:/E: free space are **not verified by this branch**.
  Do not claim this watcher creates new market-data observations: it only reads finalized
  segments when some other collector has produced them.

## Added

1. `scripts/phoenix_watch_finalized.py`: Python stdlib-only, fail-closed reader of
   immediately adjacent `segment-*.ready` directories. Source is never written.
   Dry-run by default; `--execute` explicitly opts into bounded derived writes.
2. `tests/test_phoenix_watch_finalized.py`: path isolation, dry-run, initial bootstrap,
   checkpoint continuity, source gaps, malformed manifests, output proof and disk budget tests.
3. `scripts/phoenix-phase20.ps1`: Windows doctor, local test/build/plan, one-batch pilot,
   continuous offline watcher and **disabled-only** Task Scheduler registration.
4. This handoff. Claude must finish integration and independently verify all security and
   operational properties before activating anything unattended.

## Why the watcher is intentionally limited

- Reads only finalized segments. Does not tail open archives, connect to private account APIs,
  launch network subscriptions or send any exchange order.
- Performs one-level inventory (no recursive full-drive crawl). Reuses the Phase 19 Rust
  SHA-256/CRC, sequence and book integrity verifier rather than adding a second parser.
- Checkpoint advances **only** after the Rust manifest proves every selected context segment
  was SHA-256-verified and any requested derived-case pass finished.
- Reprocesses a small configured lookback (default 6 segments) for book initialization;
  **this is not a persisted continuous book state**. Any gap in the selected source window
  is an error. Windows with no snapshot can produce zero valid cases.
- Initial default bootstraps from the most recent 8 finalized segments; the checkpoint marks
  older history excluded. This does not replace a separate bounded historical backfill.
- No automatic data or file deletion. Research write ceiling defaults to 2 GiB tracked output,
  E: free-space reserve defaults to 20 GiB. Source archives are not subject to this quota.
- A child process is terminated upon a disk-pressure alarm or bounded timeout. Monitoring
  is periodic rather than a filesystem reservation; therefore this is a **guardrail, not
  a strict hard-byte guarantee**. Claude should improve it for unattended certification.
- `--cases` adds derived case files; these can be large. Without `--cases`, only the
  source integrity/market coverage manifest is generated.
- The `runs` checkpoint is intentionally separate from any paper journal. Reports from
  this watcher are delayed/offline historical **not forward-live paper fills**.

## Operator commands (Windows PowerShell, from checked-out branch)

```powershell
.\scripts\phoenix-phase20.ps1 -Mode doctor
.\scripts\phoenix-phase20.ps1 -Mode test
.\scripts\phoenix-phase20.ps1 -Mode plan
# After proving local disk headroom, runtime authenticity, and tests:
.\scripts\phoenix-phase20.ps1 -Mode build -ConfirmOffline
.\scripts\phoenix-phase20.ps1 -Mode once -Execute -ConfirmOffline
# Optional, after reviewing available headroom and output growth:
.\scripts\phoenix-phase20.ps1 -Mode once -Cases -Execute -ConfirmOffline
# Register disabled only. DO NOT enable until the complete certification below:
.\scripts\phoenix-phase20.ps1 -Mode register-disabled -ConfirmOffline
```

No `--watch` or scheduled task is started by merely checking out this branch.

## Blockers for genuine continuously running paper mode

1. Confirm collectors have recent verified data. If not, do not relabel replay as
   real-time. Audit permissions and missing collection separately without modifying
   active collectors.
2. Integrate an independently tested bounded, read-only subscription pipeline or
   an authenticated handoff from the existing collector into the existing
   `paper-orchestrator`, `risk-guard`, `paper-journal` V2 and source-quality gates.
   Avoid duplicate raw writers and avoid a second independent paper ledger.
3. Transactionally record paper decisions, modeled fills, no-fills, cancellations,
   settlements, source fingerprints and restart recovery. No real-order route.
4. Resolve independent settlements, actual fee schedules, quote age / missing snapshots,
   reference-index feeds and source sequence gaps. Undefined P&L must remain null.
5. Build an incremental **resumable** TNEVENT verifier with persistent as-of book state
   and signed segment hash chain; overlap-window replay alone is insufficient.
6. Design and measure storage per 24 hours: observed raw bytes, ratio, CPU, RAM,
   read/write throughput and disk headroom. The collector's v2 format is already
   zlib compressed. Prove Zstd wins on actual source data before adopting it.
7. Add retention and reclamation for **verified regenerable** caches only, with
   hash receipts and rollback. Trash that stays on the same disk saves zero space.
   Preserve original segment files and unique evidence until separately approved.
8. Run 24+ hour paper-only soak with restart, source-gap, power-loss and disk-pressure
   injection; report restart correctness, duplicate decisions, byte growth and
   unexpected network/order route attempts. No production source changes.

## Release gates

`PASS` requires: pinned exact Git SHA, Windows toolchain/MSRV proof, tests,
security checks, final disk budgets measured on the host, read-only source integrity,
no CI/GitHub Actions usage, no real-order methods, meaningful live-data latency
and availability evidence, durable journal reconciliation across restarts,
and a stopped/disabled launch path that can be safely re-enabled.

**Fail-closed on any missing gate.** If source data or memory/disk is unavailable,
run the offline historical watcher only and report `HISTORICAL_ONLY` instead
of `PAPER_LIVE`.

## Safety and ownership

- Original `E:\TradingNextData`, `E:\KalshiData`, active SQLite/WAL, collectors
  and trading services stay read-only and untouched.
- Put all temporary and durable derived outputs under `E:\PhoenixLab`.
- No raw archives, secrets, private datasets, credentials or large binaries in GitHub.
- No trading service/order activation, no broker credentials, no production collector
  startup/stop/reconfiguration. Paper simulated orders only, after verification.
- No paid GitHub Actions; builds/tests locally.

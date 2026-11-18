# Phase 20 checkpoint (2026-11-18)

**Mode: HISTORICAL_ONLY.** No live paper trading is running. Nothing here is REALTIME_PAPER.

## Evidence
| Item | Result |
|---|---|
| Collector freshness | Last finalized segment 463 at 2026-10-31 03:36; `segment-464.open` stale; no collector process running (~18.3 days stale). Not restarted (read-only rule). |
| Watcher hardening | Per-instance lock with stale-owner recovery; quota measured from real `watch-*` bytes (counts abandoned runs); child output to file (no pipe deadlock); source re-stat before/after ingest; per-segment hash chain + processed map; free reserve = max(20 GiB, 15% of volume); JSONL log with rotation. |
| Python tests | 21 run, 20 pass, 1 skipped (symlink needs privilege; NTFS junction test ran and passed). |
| One-segment pilot | Segment 463 (+6 warm-up): 7/7 SHA-verified, 29,240 bytes, ~6 s. Rerun: "no new finalized segments" (idempotent), lock released. |
| Rust (validator, 1.78.0 MSRV core, 1.91.1 standalone crates) | `65f15b5`: 106 gates = 94 PASS / 0 FAIL / 12 BLOCKED. fmt, `clippy -D warnings`, tests and doctests pass for root + 18 standalone crates. Fixed: 1 `experiments.rs` lint plus mechanical lints in sim/strategies/campaign. |
| Validator fix | Native stderr no longer fails gates under Windows PowerShell 5.1 (judged by exit code). |
| Zstd vs zlib | One finalized segment (10.10 MB) recompresses to 9.84 MB (ratio 0.975) with zlib: already compressed. Zstd not installed, not measured; no format change justified. Originals never transcoded. |
| Scheduler | `Phoenix Finalized Research Paper Only` registered **Disabled**. |
| Storage | E: 480 GB free (49% used) at end. Pilot output 36 KB. Growth with no new segments: ~0. |

## 12 BLOCKED validator gates (by design, not hidden)
Opt-in research/stress/perf gates (`IncludeStress`, `IncludePerformance`, `IncludeHypothesisResearch`, `IncludeOriginalSourceAudit`, `IncludeMultiMarketReplay`, hftbacktest), power-loss faults, and data-provenance gates: source book continuity, effective-dated fees, independent settlement, original perps schema.

## Not done / blockers
1. **No fresh data.** Collector has produced nothing since 2026-10-31. A live feed bridge would need exchange API credentials, which this mission forbids; not built.
2. **Forward paper loop not built or running.** Needs fresh verified data plus integration with the paper-orchestrator, risk-guard and journal V2; not claimed.
3. Persisted cross-segment order-book state is not implemented (6-segment lookback remains; the hash chain detects mutation, not book continuity).
4. No 24 h soak, no power-loss or disk-full injection, and no CPU/RAM measurement (blocked gate). Not claimed.
5. Reclaim/inventory receipts for regenerable caches not implemented; nothing deleted.

## Next commands
```powershell
.\scripts\phoenix-phase20.ps1 -Mode plan
.\scripts\phoenix-phase20.ps1 -Mode once -Execute -ConfirmOffline   # after a collector produces new segments
Enable-ScheduledTask 'Phoenix Finalized Research Paper Only'          # only after fresh data + remaining gates
```

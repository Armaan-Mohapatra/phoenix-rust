# Phoenix Rust release gates

See the [initial 2026-11-12 code freeze](CHECKPOINT_2026-11-12.md) and the [post-freeze mission continuation](CONTINUATION_2026-11-12.md) for verified follow-up work.

A successful synthetic CI test proves software invariants on fixtures, not system readiness.

| Gate | Proof required | Current |
|---|---|---|
| G0 — Workspace | Rust compile/tests on Windows and Linux; static dependency review | TESTED |
| G1 — Binary economics | Fixed-point fee rounding; complementary books; incomplete/invalid states rejected | TESTED on synthetic |
| G2 — L2 stream | Snapshot/delta replay; subscription sequence gaps; conflated clock handling; fractional quantities identified | SYNTHETIC TESTS ONLY |
| G3 — Archive durability | Complete append-only snapshot/Parquet writer with SHA256 manifest, fsync/checkpoint recovery and sealed-only readers | **PARTIAL:** PR #9 adds locked, sealed bounded research partitions, fsync, SHA256 and crash quarantine; Windows directory power-cut durability, large indexes and real feed integration remain OPEN |
| G4 — Actual settlement | Exact contract rules and independently sourced reference data; market IDs match evaluated trades | **NOT DONE:** synthetic reference-window calculation only; PR #15 blocks unjustified independent evidence promotion |
| G5 — Historical replay | Full chronological market streams, measured latency, queue-aware fills, conservative missing-data behavior | **PARTIAL:** PR #12 passes a full **synthetic** wire→archive→strategy→modeled P&L replay; no verified real history/latency or queue parity |
| G6 — Strategies | Frozen code/config SHA, fully reproducible baseline and controls, no lookahead and untouched OOS | PARTIAL (interface only) |
| G7 — Paper trading | Persistent event-sourced multi-portfolio ledger, idempotent recovery, independent settlement | **PARTIAL:** PR #14 adds tested durable multi-portfolio paper event recovery, but no full order lifecycle, independent settlement or scale tests |
| G8 — Operations | Resource benchmarks, failure injections, telemetry, no collector disruptions, explicit install authorization | NOT DONE |

Go/no-go: No real data ingestion, no private source files, no daemon deployment, no production migration, and no exchange orders are authorized by this draft. A failing or blocked gate is not evidence of profitability.

## Test policy
- Train, validation and untouched confirmation periods remain separate; event-level dependence is clustered.
- No use of future market-book snapshots in decisions or simulated order arrival.
- Fees must reference contract-specific effective schedules, not a universal fee model.
- Missing independent settlement proof produces an explicit missing status, never zero P&L.
- Order-book gaps invalidate affected markets until resnapshot and never produce guaranteed fills.
- Disclose queue and latency modeling assumptions and adverse selection.
- No trial can be relabeled untouched after its results have influenced tuning.

For upstream design consult official Kalshi API docs at https://docs.kalshi.com/api-reference/market/get-market-orderbook and https://docs.kalshi.com/websockets/orderbook-updates.

## Reporting milestone

PR #13 supplies a standalone deterministic synthetic/research performance reporter. It preserves negative outcomes, leaves pending P&L null, and separates modelled settlement claims from actual independent proof. Not an automated publisher or a completed G8 operational release.

## 2026-11-12 upstream and real-data additions

- PR #16 assembles all previous sibling modules and adds a **direct, pinned, backtest-only hftbacktest** Rust queue/latency adapter. Upstream live features are disabled; synthetic queue parity and Linux/Windows tests passed.
- Source-level independent Rust mathematical ports from Apache-2.0 Oracle3 and MIT Ryan Frigo, plus an MIT Tarpboi-inspired risk guard, have passed standalone CI. These are **not direct upstream dependencies** and are not deployed.
- A minimal projected sample of a **real archived October 24 BTC paper ledger** passed fixed-point P&L arithmetic on Windows/Linux. It is not a raw orderbook replay, actual venue fill, independent index settlement or verified after-cost investment strategy.
- The full assembled integration workflow now includes the core, L2 codec, archive, historical-input, paper journal, reporting, HFT queue, three port families and real-ledger parity. Treat the latest CI result as authoritative; a passing fixture matrix cannot promote release gates G4/G5.

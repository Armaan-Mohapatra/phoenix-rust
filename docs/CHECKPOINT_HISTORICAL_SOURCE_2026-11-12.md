# Phoenix Rust — original-source replay and durability checkpoint
**2026-11-12 PST / 2026-11-13 UTC. PRIVATE repository.** Research-only. No merge, collector changes, production writes or trading orders.

## GitHub branch / PR status

- Historical branch `codex/real-btc-historical-replay-v1`, draft [PR #17](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/17), stacked on integrated upstream draft PR #16. Last historical implementation commit before this documentation checkpoint: `99fee1467b1eaf62b95c0cb92fb37c5dde273189`.
- Disk-index branch `codex/disk-id-index-v1`, draft [PR #18](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/18), stacked on PR #17 at `99fee146`. Verified index fix HEAD: `ca4f26138d2f46dbecb4f5eb87ed28188efb64fe`.
- Original PRs #1, #9–#16 untouched; `main` unchanged.

## Actual recovered data and integrity

1. Uploaded source ZIPs inspected **read-only**. Curation catalog SHA256 `55f869f1231b67948395ac16a33b50ec774711653da2a3d22c693050eda2db08`, 3,664,603 bytes, 9,243 manifest rows. Footer-only scan: 685 raw-event Parquet members with 0 missing/invalid receive-time footer statistics.
2. Original requested `KXBTC15M-26OCT241645-45` at 20:34:12 UTC has **no predecision curated book coverage**; keep its previously passed YES/NO paper arithmetic only.
3. Replacement market `KXBTC15M-26OCT241900-00`, market ID `01a16a5a-aeb3-7b39-8061-52758c8f1b48`, decision 22:47:39 UTC. Selected **24 source Parquet members** spanning 22:44:40–22:47:39 UTC, **194,253 source-declared rows across ALL markets**. Exact paths in [allowlist](BTC_20261024_ASOF_MEMBERS.txt). [Coverage provenance](BTC_20261024_SOURCE_COVERAGE.md). Per-member SHA256 stored in original curation catalog and automatically checked by Rust reader; representative verified member `segment_0082928055b8bd4b6483.parquet` SHA256 `d847d5e0248ebdfb19d90ea3b1b66b7e815cb965e4a22c2d6e9474f878afbdc7`.
4. Previous bounded **non-Rust** source processing reconstructed 63,813 market-specific deltas through the decision and five derived top-10 book checkpoints. No authenticated full opening orderbook snapshot; initial zero book assumption remains UNVERIFIED. The new Rust source loader has NOT yet processed all 63,813 actual records in the hosted environment.
5. Recorded Kalshi-relayed BRTI opening window sum USD cents 482,856,277 / 60 and closing sum 482,875,060 / 60, difference +$3.1305, implied **YES** under archived 15m rules. Archived Kalshi YES label agrees, but source is Kalshi pass-through, NOT independently authenticated CF primary.
6. Archived paper YES at $0.39 plus $0.0167 entry fee → +$0.5933 modeled net; NO at $0.62 plus $0.0165 → -$0.6365 modeled net. Rust previously reproduced both on 5 derived checkpoints. Neither was a verified actual exchange fill. Latencies 0/100/250/500ms are assumptions.

## Implemented in PR #17

- `integrations/kalshi-wire`: historical decoder retains original WSS sending/event timestamps, quarantines ~0.3s source-ahead-of-receipt clocks, and decodes actual Oct24 `ts_ms`-rather-than-`clock` frames. Full real message fixtures tested.
- `integrations/real-btc-replay`: fixed-point 120 recorded BRTI values, five real-source-derived book-state projections, causal future-leak rejection, modeled YES/NO taker and fee P&L, maker fail-closed.
- `integrations/source-loader`: original ZIP member Parquet reader with manifest SHA256/row-count verification, 250k scanned row cap, max 32 selected members, 16MiB/member, max 64MiB sorted selected messages, original message bytes/connection/sid/seq/source/receipt timestamps. Chronological sorting uses local receipt, not forward source time.
- Source-loader forensic L2 parser: exact 0.01-contract depth, snapshot-required, rejects invalid negative depth/reconnect/duplicate messages, and refuses to certify source book state based on market-only sequence. Current evidence: `UNKNOWN_INITIALIZATION` / provisional.
- `phoenix-source-audit` CLI: compile/tested by CI; can run on actual original ZIPs in an **authorized local Rust installation** with the published 24-member allowlist. No source writes or full drive scan. See [local instructions](SOURCE_ORIGIN_CLI.md).

## Verified CI

- PR #17 [source-origin Rust loader run 43356418199](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43356418199) **Windows+Linux PASS** (synthetic Parquet encoded with real WSS field shapes). Original historical regression and core Rust checks also passed at `99fee146`; last combined matrix Windows status must be rechecked before final claim.
- PR #18 [disk-identity run 43356964502](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43356964502) **Windows+Linux PASS** after compiler fixes. Independent, experimental `redb=4.5.0` ACID identity/partition index, 16MiB cache vs 1GiB default, 256 entries/transaction; tests duplicate, collision, rollback, restart, and refuse unknown existing directory. Not yet wired to the sealed Parquet publisher.

## Hard blockers and evidence grades

- **BLOCKED_INDEPENDENT_SETTLEMENT**: CF Benchmarks requires licensed `BRTI` `STREAM_HISTORICAL_VALUES` credentials for primary historical 1Hz index data. Kalshi relayed ticks and archived outcome are not independent. See [CF provenance request](CF_PRIMARY_SETTLEMENT_REQUEST.md).
- **BLOCKED_OFFICIAL_FRESH_FINAL**: historical archived YES label is not an independently re-fetched Kalshi market record; public endpoint retrieval was unsuccessful in this session.
- **BLOCKED_SOURCE_RUST_REAL_EXECUTION**: complete original ZIPs are accessible in ChatGPT's research container, **but that environment lacks Rust and Arrow/Parquet toolchain**; GitHub CI has Rust but not these private uploaded ZIPs. To test, run the local source-audit command with verified mounted source ZIPs. The 24-member file list is precise; do NOT fabricate a Rust raw replay from the 5 derived book checkpoints.
- **UNKNOWN_INITIALIZATION**: no verified initial snapshot and complete shared-channel session/sequence continuity. **Execution P&L remains MODELED/PROVISIONAL**; no high-confidence maker/taker fill claim. A zero-start delta reconstruction is not a full executable book.
- **DISK_INDEX_UNWIRED**: disk-backed index prototype passes tests, but existing `durable-archive` retains in-memory index and a bounded partition cap. Need verified sealed-manifest → redb commit ordering, crash-gap restart reconciliation and Windows power-cut tests before migration.
- **OOS_NOT_STARTED**: No frozen-strategy historical out-of-sample profitability claim; no strategy inversions promoted while execution/settlement gates remain incomplete.

## Next exact work

1. On an authorized Windows machine with the source ZIPs, run `phoenix-source-audit` with the 24-member allowlist and as-of cutoff 22:47:39Z. Save JSON output to a new research folder, **never** the live collector root. Verify original row count, market-specific delta/trade/snapshot count and source sha; measure source clock skew. Confirm whether any authentic full-channel opening snapshot exists; do not count market-only seq gaps as packet loss.
2. Independently obtain licensed CF Benchmarks BRTI raw hourly data and revisions for Oct24 22:00 UTC; compare exact 120 window timestamps. Refresh official Kalshi market result and effective series fees through an authorized read-only API without writing credentials to GitHub.
3. Reconcile disk-backed index against sealed archive manifests in crash tests and enforce Windows 16MiB cache. Do not replace production or published archive indexing until equivalent tests pass.
4. Only then admit a real-source full-depth execution replay, measured timestamp/latency constraints, paper order lifecycle and untouched OOS strategy research.

This document is a **preserved, reproducible checkpoint**, not a completed system or asynchronous work promise.

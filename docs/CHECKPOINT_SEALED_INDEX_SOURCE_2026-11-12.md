# Phoenix Rust — original historical source + sealed index integration checkpoint
2026-11-12 PST / 2026-11-13 UTC. PRIVATE repository; research-only. No mergers, production modifications, collector restarts, or live orders.

## Exact published development graph
- Integration draft [PR #16](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/16) preserved.
- Source replay draft [PR #17](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/17), branch `codex/real-btc-historical-replay-v1`, new Windows source audit launcher at commit `ff9bbf38254103300bd36356ed3379d2e18b7fe2`; PowerShell CI syntax-check change at `f13c022316d422e2f3519bb3946e8a5f2493933f`.
- Disk identity draft [PR #18](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/18) at `ca4f26138d2f46dbecb4f5eb87ed28188efb64fe`.
- Sealed-to-disk index integration draft [PR #19](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/19), `codex/sealed-index-integration-v1`, latest code commit before this checkpoint `b42ebe15a6a23d884e756bfea13dc75c9ae33175`. `main` untouched.

## Bounded original-source verification
- Original six user-uploaded LunaDelta market-data ZIP archives were confirmed accessible in this research container (not Windows production drives). No live database opened.
- Original curated catalog SHA256 `55f869f1231b67948395ac16a33b50ec774711653da2a3d22c693050eda2db08`.
- Exact one-market pre-decision allowlist: 24 original Parquet members selected through verified previous footer metadata, total **194,253 catalog-declared rows across all tickers**.
- This session separately **reopened each of all 24 exact source members** from the uploaded ZIPs and streamed SHA256 verification. **24 of 24 byte-for-byte matched**, total 14,459,929 uncompressed original Parquet bytes. No full event replay occurred in this check.
- Compact check output `phoenix_btc_24_members_verified.json`: SHA256 `9742ab1d192df47ca05e72fe85ed97674bc137dc1da8cef8393bf5fb8abbf962`, generated as a conversation research attachment, **not bulk uploaded to GitHub**. Per-member source hashes remain in the curated catalog. GitHub allowlist is `docs/BTC_20261024_ASOF_MEMBERS.txt` on PR #17.
- Market `KXBTC15M-26OCT241900-00`, decision 2026-10-24T22:47:39Z. Earlier non-Rust 63,813-delta market reconstruction and five top-10 projections remain source-derived **provisional**, unvalidated against a full Rust original Parquet replay.
- This environment has **no `cargo` or `rustc`**; outbound DNS to `static.rust-lang.org` fails. Isolated Rust installation is not available here without changing execution environment. GitHub CI has Rust but not the user-uploaded full ZIP archives.
- [Read-only Windows handoff](https://github.com/Armaan-Mohapatra/phoenix-rust/blob/codex/real-btc-historical-replay-v1/docs/WINDOWS_ORIGINAL_BTC_HANDOFF.md) and source execution script `integrations/source-loader/scripts/run-original-btc.ps1` validate input directories, all six ZIPs, catalog SHA and 24-member allowlist, then call the existing original-Parquet Rust loader. They do NOT run collectors or orders. **The actual original-source Rust run is still BLOCKED on available tooling/location**, not claimed as completed.

## Real disk-backed index integration
- PR #19 composes `phoenix-durable-archive` and `phoenix-disk-identity` (redb 4.5.0). Publication order is sealed synced Parquet/manifest **before** bounded 16MiB-cache transactional redb IDs. Reopening verifies sealed Parquet and per-row SHA256 identities, detects index-only phantom partitions, and repairs the gap where manifest was sealed but index transaction not committed.
- Synthetic regression tests: interrupted after sealed manifest; interrupted after redb commit; identical ID replay across restarts; unknown/unmarked existing path rejection; index-only phantom partition; tampered per-row manifest checksum; 1,024 IDs across four 256-row partitions with cold restart.
- **Windows + Linux passed** on initial [CI run 43365071186](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43365071186). The latest larger regression change `b42ebe15` triggered a newer run; inspect current status before saying all tests passed at HEAD.
- **Limitations:** existing publisher still rebuilds/stores a 250k-ID in-memory `HashMap`; disk-backed transactional wrapper is **not** a memory-scale replacement. Publisher still retains a 2,000-partition limit; every wrapper publish revalidates all already-sealed data, so throughput may become O(N²). Power-loss Windows directory metadata semantics and disk-full faults are unverified. No production storage migration.

## Evidence and settlement
- BTC BRTI opening USD-cent sum 482,856,277 across 60 recorded Kalshi-relayed seconds, closing 482,875,060 across 60 seconds; closing is greater, reconstructing archived YES. This is **not** authenticated primary CF evidence.
- Primary CF Benchmarks historical endpoint `/api/v1/history/values?id=BRTI&timespan=HOUR&timestamp=2026-10-24T22:00:00.000Z` requires licensed `STREAM_HISTORICAL_VALUES` permission. No usable credential or authenticated provider response was obtained. **BLOCKED_INDEPENDENT_SETTLEMENT**.
- A fresh read-only official Kalshi settlement retrieval for the exact ticker did not succeed through the available public tool. **BLOCKED_FRESH_OFFICIAL_FINAL**.
- No verified initial snapshot / full global channel continuity. NO authenticated original-source maker fills, no verified strategy return. Archived modeled YES +$0.5933 and NO -$0.6365 are NOT actual venue executions.

## Resume next
1. On a verified Windows research checkout, run the published PowerShell handoff with original six ZIPs. Confirm real market-specific deltas/trades/snapshots, clock skew, origin as-of time, and error classes. Preserve JSON in isolated research output outside collectors and databases.
2. Refactor the *publisher* to avoid its 250k in-memory ID map and O(N²) verification under a tested disk-index mode; retain sealed-manifest recovery semantics, benchmark RAM and measured I/O on bounded source.
3. Independently acquire authorized CF primary BRTI values and a fresh official Kalshi final; retain BLOCKED until evidence is authentic.
4. Complete actual L2 initialization and global session sequence continuity, then realistic fee-aware execution and frozen OOS tests. Preserve all negative/blocked results.

This checkpoint does **not** claim an independently verified complete backtest or guaranteed positive strategy return.

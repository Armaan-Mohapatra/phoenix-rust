# Phoenix historical original-source execution handoff — 2026-11-12 PST

**PRIVATE repository, research-only, main unchanged.** This document supplements, rather than replaces, [the earlier historical checkpoint](CHECKPOINT_HISTORICAL_SOURCE_2026-11-12.md). Draft [PR #17](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/17) is stacked on draft PR #16. Original paper parity source preserved.

## New verified source-side evidence

- Original six uploaded LunaDelta market ZIP files are accessible to this research container; no local C:/E: drives or active Phoenix collectors were inspected.
- Original curated raw-event catalog SHA256 `55f869f1231b67948395ac16a33b50ec774711653da2a3d22c693050eda2db08`, 9,243 entries.
- Selected market `KXBTC15M-26OCT241900-00`, decision `2026-10-24T22:47:39Z`.
- Previous bounded Parquet footer/index selection yielded **24 explicit original member names** with **194,253 catalog-declared rows across all tickers**. Exact filenames are in [BTC allowlist](BTC_20261024_ASOF_MEMBERS.txt).
- A separate **read-only byte-stream verification** reopened **all 24 selected original Parquet members** and independently SHA256-hashed **14,459,929 uncompressed source bytes**: **24/24 matching the previously recorded member digests**. No raw Parquet values were uploaded to GitHub. Compact conversation research attachment `phoenix_btc_24_members_verified.json` SHA256 `9742ab1d192df47ca05e72fe85ed97674bc137dc1da8cef8393bf5fb8abbf962`.
- This is source-byte integrity proof, **not a completed Rust execution** and not proof of actual venue fills or settled profitability.

## Rust functionality and tests

- `integrations/source-loader` reads only named ZIP Parquet members and checks each raw member SHA256, bound (250k scanned rows), per-market original WSS timestamps and local receipt order; already tested on Windows/Linux with synthetic Parquet that uses real October WSS field shapes.
- **New source diagnostics:** `source-loader::ChannelRun` summarizes shared sid/seq for **all market tickers** in the input window before target market filtering, avoiding false gap claims from other-market messages. It counts duplicates, order regressions and missing selected-window sequence numbers, with `complete_subscription_history_verified=false` until full subscription continuity is independently established. Implementation commit `a00d4d53208502fd028a40f1e5059208a7db6b73`. [Windows/Linux successful CI](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43366294613).
- The Windows-only-source [read-only launcher script](../integrations/source-loader/scripts/run-original-btc.ps1) checks all six ZIPs, the catalog's original SHA, the exact 24-member allowlist, research output isolation, and Cargo before calling the Rust original-event reader. [Launcher syntax and Rust test Windows/Linux CI](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43365798410) passed. See [usage and safety](WINDOWS_ORIGINAL_BTC_HANDOFF.md).
- Source tests and individual CI **DO NOT have the six original ZIPs**. No actual 63,813 market-delta original-source Rust replay occurred in this session.

## Execution blocker and evidence tiers

- This ChatGPT execution container has neither Rust/Cargo installed nor network DNS access to the official Rust distribution host. The original ZIPs are mounted here, but the actual Rust compiler is not. GitHub-hosted CI has Rust, not these private user-uploaded ZIPs. No unauthorized tool installation or raw archive publication was performed. **BLOCKED_LOCAL_RUST_EXECUTION**.
- Original orderbook 63,813-delta non-Rust reconstruction used an assumed empty starting state, not independently captured validated initial snapshot: **UNKNOWN_INITIALIZATION**. No high-confidence executable source book, no maker fill assertion.
- Historical BRTI observations via Kalshi relay imply YES, but separate licensed CF primary historical values have not been obtained. Provider [historical REST endpoint](https://docs.cfbenchmarks.com/api/rest/historical-values/) requires `STREAM_HISTORICAL_VALUES` entitlement: **BLOCKED_INDEPENDENT_SETTLEMENT**.
- Read-only public Kalshi API retrieval for the exact ticker was attempted but unavailable through the web interface: **BLOCKED_FRESH_OFFICIAL_FINAL**.
- Previously simulated YES +$0.5933 and NO -$0.6365 at one modeled unit remain **MODELED paper performance only**, no actual exchange execution.

## Exact next safe run

Run the published Windows launcher from an authorized research checkout with copies of the six original source ZIPs in a dedicated inputs directory. It will output compact source-row/sequence/book-state JSON outside production roots. Verify 24/24 manifest SHA, then compare the raw Rust market-specific delta and trade counts to the prior source-derived projections without hardcoding prices. If no initial validated snapshot exists, preserve UNKNOWN_INITIALIZATION and do not certify fills.

No original Phoenix data or service has been modified. Research remains unmerged and paper-only.

# Phoenix Rust — third-party adoption and historical compatibility checkpoint
**Date:** 2026-11-12 · **Repo:** https://github.com/Armaan-Mohapatra/phoenix-rust · **Branch:** `codex/integrated-upstream-adoption-v1`

**Status:** Research-only software development. The previous [2026-11-12 checkpoint](CONTINUATION_2026-11-12.md) is preserved, `main` unchanged. The new [draft PR #16](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/16) is unmerged, and production/data collectors/live orders untouched. Source-code changes before this checkpoint commit end at `9b8919b160c5f61f30b2d9e047b0a1036067caff`. The current PR branch may have a newer documentation-only checkpoint commit.

## Actual milestones

1. **PRIVATE confirmed.** GitHub repository visibility returned `private` and write permissions were present. Never infer that conversion from prior public status erases earlier exposure; do not commit keys, account data or bulk historical files.
2. **Combined architecture:** branch assembles the prior stacked durable archive, bounded Parquet historical input, Kalshi WSS public-message codec, L2 simulator, synthetic replay, persistent paper journal, source-only performance reporter and fail-closed independent-settlement classification. Four sibling-source files from PR #13 and one PR #15 domain file were incorporated by exact GitHub blob SHA; PRs #1/#9–#15 were preserved without merges to `main`.
3. **DIRECT_INTEGRATION — 1 of 25:** MIT `nkaz001/hftbacktest` pinned at `5f3ec40b2afb764e0fea112f941ed85523ef4e88`. `integrations/hftbacktest-adapter` has a genuine Git dependency with `default-features=false,features=["backtest"]`, live disabled. Exercises upstream `RiskAdverseQueueModel` and `ConstantLatency`, compares deterministic resting-YES queue-ahead/modeled fills with Phoenix conservative maker. [CI #43280365680](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43280365680) **Windows+Linux PASS**. No real Kalshi queue fill parity or speedup measurement.
4. **PORT_TO_RUST — 3 of 25:** Apache-2.0 Oracle3 static fee-aware binary relation baskets; MIT Ryan Frigo forward-only/Brier/executable-price edge diagnostics; MIT Tarpboi deterministic fixed-point portfolio/quote-depth/large-signal risk circuit. New packages `integrations/source-ports` and `integrations/risk-guard`. [CI #43281854717](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43281854717) and [CI #43284076370](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43284076370) **Windows+Linux PASS**. These are original Rust implementations, not upstream-linked code.
5. **All 25 upstream sources audited.** [Pinned source inventory](UPSTREAM_SOURCE_AUDIT_2026-11-12.md) includes exact commit, inspected functions/files, license, integration status and technical rationale. 21 remain source/test-only, blocked, or excluded. No Python/TypeScript running in the Rust research engine, no unlicensed/AGPL copied code.
6. **REAL archived PAPER ledger parity:** `integrations/real-ledger-parity` contains a *two-row minimal projection*, not an entire private archive. Source: uploaded `LunaDelta_20261025T083801Z_research_part003.zip` `project/luna_interval_ledger.csv` (36,081,908 bytes; documented original member SHA256 `6594c16bd097bc295e6ae6d3bcd16d5fad7b2c2aea9cf7b60f6f832b5b436b2b`). Ticker `KXBTC15M-26OCT241645-45`; archived paper YES 1 contract at $0.5800 + $0.0171 recorded fee → +$0.4029 recorded/model-calculated; archived paper NO at $0.4300 + $0.0172 fee → −$0.4472. A separate *archived* settlement reconciliation reports official YES; **no independent primary reference benchmark/source proof was obtained**. [CI #43282904791](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43282904791) **Windows+Linux PASS**. This is **not** a historical full-depth replay, an actual venue execution or independent settlement verification. See [source limitations](REAL_HISTORY_LEDGER_PARITY.md).
7. **Integrated test matrix** now exercises all core and standalone code packages on Windows and Linux: L2 decoding, archive, historical converter, synthetic vertical replay, persistent paper accounting, reporting, HFT queue parity, Oracle3/Ryan math, Tarp risk gate, real ledger arithmetic. First assembled matrix [CI #43281353030](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43281353030) Windows+Linux PASS. Later full updated matrix may still be running; verify latest run before stating it passed. Documentation-only changes are exempt from push matrix to reduce repeated CI.

## Current accounting of upstream adoption

- **1 DIRECT_INTEGRATION** (hftbacktest actual pinned Rust Git dependency + unit tests).
- **3 PORT_TO_RUST** (Oracle3, Ryan Frigo, Tarpboi behavior independently implemented and tested).
- **21 TEST_REFERENCE/SOURCE_ONLY/BLOCKED/EXCLUDED**.
- No third-party paper/live bot is authorized to submit exchange orders; no upstream source claims actual profitability. Hosted CI is not laptop deployment evidence.
- Numeric speedup, fee reductions, or realized returns attributable to reuse have **not** been measured.

## Unresolved critical gates

- **Independent market settlement:** Exact rules/fees and official final for a **single common tested ticker**, paired with independently authenticated CF reference-index observations (including required historical sample windows) are **MISSING**. Archived reported YES is not independent verification.
- **Real chronology:** No complete bounded historical quote/depth and public-trade stream for the 20:34:12Z BTC decision has been replayed with independently verified receive times, measured latency and maker queue priority; no true execution proof.
- **Kalshi + Perps:** Full schema, asset, fractional quantity and timestamps must be verified against finalized `E:\TradingNextData` exports on a properly authorized Windows-local research copy. Cloud GitHub connector cannot access `E:/C:` drives. Uploaded ZIPs are read-only sources and not wholesale imported.
- **Collector:** Authenticated read-only Kalshi WSS collector with sequence recovery, bounded queues and sealed manifest publication is incomplete; no production collector changed.
- **Durability/scale:** Disk-backed event identity index, full-window partitions beyond pilot limits and Windows directory/power-loss guarantee remain unproven.
- **Strategies:** Real frozen source/version parity, feature leak checks, repeated-trial controls, event-clustered confidence intervals, untouched OOS and measured fee/latency stress remain incomplete.
- **Security/supply chain:** MIT/Apache notices, pinned deps, Windows local MSVC, dependency audit, sandbox limits and GitHub branch protection need release review. The optional upstream crate's own live feature must stay off. Never copy unlicensed/AGPL sources.

## Exact next implementation sequence

1. Read GitHub PR #16 head, its README, [25-source audit](UPSTREAM_SOURCE_AUDIT_2026-11-12.md), and latest combined Windows/Linux CI. Correct any failures; do not re-audit the same 25 source trees.
2. Expand the **one real-market** fixture with an authorized, minimal finalized matching historical Kalshi orderbook/trades slice and true independent CF index ticks; verify precise ticker/contract rule/event timestamps. If independent ticks are not available, classify `BLOCKED_INDEPENDENT_SETTLEMENT`, never label performance verified.
3. Introduce **disk-backed bounded** event identity index and Windows-specific durability/fault tests before scaling from 250k IDs to tens of millions; keep old collectors and databases read-only.
4. Add current effective-dated series fee schedules and proof source IDs, with scalar/void rejection.
5. Extend full depth replay/partial fill and Kalshi-specific microstructure calibration, then preserve untouched OOS before testing strategy inversions.
6. Implement operational risk circuit wiring and durable paper execution lifecycle; benchmark CPU/RAM/disks on bounded fixtures.
7. Review/merge/deploy **only with separate user authorization**. No live trading.

**No background work implied.** Every source and checkpoint commit is in GitHub for an exact later continuation.

# Phase 7 — deterministic multi-market Kalshi history (source only)

**Status: CODE_WRITTEN_UNTESTED.** No Cargo, CI, paid Actions, local historical ZIP reads, collector operations, simulations, or production changes in this development phase.

Starting parent Phase 6 commit: `b2a122af397262ea49fad801bc4035b210f5c7f8`. Child: `codex/phase7-multimarket-history-v1` (stacked draft PR). No earlier branch or `main` changed.

## Existing source components reused

- `integrations/source-loader::catalog`: SHA-bearing source catalog, 32 per-request named Parquet members, 12 ZIPs, 16 MiB per member, 250,000 scanned rows and 64 MiB sorting cap. `source-loader::pipeline::run`: as-of source replay, fractional L2, original receipt times, per-channel sequence diagnostics and explicit missing evidence. `kalshi-wire` remains the authoritative archived WSS decoder.
- `integrations/source-loader/src/multi_market.rs`: newly authored bounded multi-market coordinator, not a competing ZIP/Parquet reader. Per-invocation max **four markets** with exact ticker/event/series identity and chronological decision-time sorting.
- Original pilot retained as `docs/BTC_20261024_ASOF_MEMBERS.txt`: **24** exact members for `KXBTC15M-26OCT241900-00`, declared catalog SHA-256 `55f869f1231b67948395ac16a33b50ec774711653da2a3d22c693050eda2db08`. Source member content and replay were **not** accessed or executed here.

## API and safe source-member discovery

`MarketSpec` requires unique exact ticker, event, series, asset family, source-receipt start/end and decision cutoff, explicit SHA-tagged member allowlist, and separately declared optional contract-rule/fee/official/independent hashes. The default known family mappings are BTC/KXBTC, ETH/KXETH, SOL/KXSOL, XRP/KXXRP; arbitrary additional families require an explicit external verified schema hash. These mappings are allowlisted candidates, **not** an assertion that every historical contract shares economics or that source completeness is proven.

`IndexedPartition` + `members_from_verified_index` permits selection from a caller-supplied, independently audited per-ticker partition index by exact ticker, SHA and time overlap. A global catalog alone cannot identify a market in a Parquet partition, so the engine **does not guess ticker membership from ZIP filenames**. The underlying original loader checks each selected source member's actual SHA and internal market rows before emitting replay evidence.

`run_batch` validates the entire bounded market list, sorts it by decision time/ticker, invokes the existing replay once per new market, and returns one deterministic `BatchOutcome` with separate market coverage, settlement *availability* declarations, exclusions and source status. No paper ledger outcome is upgraded to an official or independent settlement. No source orderbook is automatically execution eligible.

## Resuming and content-addressed checkpoints

`ReusableMarketCheckpoint` stores the precise source manifest digest, explicit partition digests embedded in its replay report, market/contract/fee/source/model fingerprint, engine revision SHA, serialized replay report and report SHA. Reuse validates every input and member SHA identity, not just a ticker or filename. A mismatch triggers replay instead of trusting the stale checkpoint. It is still a **prior hash-matched research checkpoint**, not a new independent file re-verification; the trust boundary and local persistence are the operator's responsibility.

`phoenix-multimarket-replay` is an operator-run CLI taking a bounded JSON plan, previous checkpoint JSON (or `-`), and up to 12 explicitly named original ZIPs. It verifies original catalog bytes against an expected SHA and emits JSON to **stdout only**. Store outputs only under a separate research workspace, not any production/source root.

## Phase 6 execution handoff

`simulate_eligible_historical_order` is a fail-closed bridge to the existing `phoenix-sim::historical::simulate_source_window`: exact source report, manifest-derived market, contract and fee declaration, separately reconstructed full-depth L2 and complete-session proof are required. The existing source-loader does not have independent full subscription verification; therefore its current original-source output continues to classify `modeled_execution_eligible=false`. Top-of-book checkpoint data is never promoted to complete order depth and no modeled trade, real fill or historical profit is fabricated.

## Authored NOT RUN tests

Multi-family ticker/series mapping, non-BTC family mismatch, custom schema requirement, duplicate/unknown partitions, index SHA mismatch, deterministic market/fee/contract fingerprint, invalid time ordering, hash-tampered and valid resume checkpoint source proofs, duplicate ticker, resource bounds and source-execution rejection. Existing source-loader SHA, as-of and missing-snapshot regressions remain authored but not run.

## Remaining evidence and compiler gaps

The source runs, source-index audits, real market-contract versions, historical fees and authenticated official/primary settlements remain unexecuted or unavailable. The complete subscriber-level history, maker tape and independent settlement cannot be established from market-filtered raw event rows alone. Code is not compiled or verified; Rust/API correctness, actual source performance and resource feasibility are Phase 15 validation gates. All new source changes use `[skip ci]` and must stay draft until later independent validation.

**Next phase: Phase 8 historical Perps adapters and causal Kalshi/Perps features.** Do not begin Phase 9 or execute Phase 15.

# PHOENIX Phase 7 — SOURCE CHECKPOINT (not validated)

**Status: CODE_WRITTEN_UNTESTED.** All Rust tests NOT RUN. Compilation, Clippy/rustfmt, historical market execution, official API queries, backtests and paid GitHub CI NOT RUN. No original Phoenix drives, collectors or exchange orders modified.

**Parent:** Phase 6 branch `codex/phase6-execution-engine-v1` @ `b2a122af397262ea49fad801bc4035b210f5c7f8` (PR #28).
**Branch:** `codex/phase7-multimarket-history-v1`.
**Last source/documentation commit before checkpoint:** `ae379b8850e79649a1a0161cdf109ffd2408ad80`.
After publishing, re-fetch exact GitHub HEAD and open draft PR stacked on Phase 6.

## Files changed (Phase 7 only)

- `integrations/source-loader/src/multi_market.rs` (new): source catalog and market-family registry, hash-verifying indexed market partitions, deterministic four-market batch, strict time/identity, SHA content-addressed resume reports, descriptive source/contract/settlement classifications and guarded Phase 6 execution handoff.
- `integrations/source-loader/src/lib.rs`: exposes new module, reuses existing bounded ZIP/Parquet reader.
- `integrations/source-loader/src/bin/phoenix-multimarket-replay.rs` (new): inspectable, operator-controlled read-only JSON plan/ZIP CLI with bounded files and SHA validation; outputs to stdout.
- `docs/PHASE7_MULTIMARKET_HISTORY.md`: full contract, original BTC pilot 24-member allowlist references, exclusions and deferred validation.
- This checkpoint document.

## Source-static review

Confirmed original `docs/BTC_20261024_ASOF_MEMBERS.txt` has 24 allowlisted original members and existing pilot catalog SHA `55f869f1231b67948395ac16a33b50ec774711653da2a3d22c693050eda2db08`. Confirmed `source-loader::catalog` indexes exact paths and enforces SHA/row counts before `pipeline::run`, and Phase 7 reuses it. Checked new module export, bounded 4×32 input, chronological market sorting, per-file SHA in checkpoints, fee and contract versions in fingerprints, no inferred settlements or live orders. This is **textual static inspection**, not a compiler result.

## Authored tests NOT RUN

BTC/ETH/SOL/XRP exact series identity; unsupported family rejection; time/duplicate market/partition failures; verified-index window overlap and SHA conflict; contract/fee/version fingerprint sensitivity; hash-attested resume vs tampered checkpoints; resource and batch limits; no automatic source-execution promotion. Previously authored original replay and fractional L2 tests remain present, not run.

## Remaining known blockers

- Original-source Kalshi replay/multi-market schema compatibility cannot be proven without Phase 15 runs.
- Source index is a supplied research artifact; original member data must still be SHA-verified on replay.
- Full L2 subscription continuity, empirically executable liquidity, complete maker prints and independent settlements remain unverified.
- Historical contract rules and effective fee schedules have not been independently imported; no actual modeled or official P&L measured.
- Build and standalone crate compatibility unknown; Windows offline source and resource gates deferred.

## Next EXACT instruction

Verify Phase 7 PR head and no Actions runs. Create `codex/phase8-perps-history-v1` from the final confirmed SHA. Implement Phase 8 in existing `integrations/historical-input` and Phase 7 interfaces with actual schemas where accessible; otherwise an explicit schema-driven mapping interface and synthetic tests. Publish new draft PR and independent checkpoint. Stop after Phase 8; DO NOT start Phase 9 or Phase 15.

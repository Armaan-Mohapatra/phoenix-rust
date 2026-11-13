# Phase 8 — Schema-configurable Perps history and causal Kalshi features

**Status: CODE_WRITTEN_UNTESTED; PRIVATE_DATASET_COMPATIBILITY_UNVERIFIED.** No Rust compilation, Cargo test, CI, historical market or Perps simulations, collector activity, active database access or live orders. No production changes. Parent: Phase 7 PR #29 at `6ed6febb90d51f045cd7b1366614ac1c732a0a37`; child branch `codex/phase8-perps-history-v1`.

## Source schema inspection and limitations

The existing `integrations/historical-input` crate has a verified **generic**, schema-configurable Parquet `ColumnMapping`, receipt/exchange timestamp parser (EpochNs, EpochMs, RFC3339), `CanonicalRecord` identities and `decimal_exact` parser. It does **not** publish a verified, private Phoenix Perps column/payload schema.

A read-only bounded ZIP-central-directory inventory was performed on the ten supplied October LunaDelta/LunaResearch archives. No archive member path explicitly identified a `perps` dataset or its schema. A bounded sample of `archive/curated/reference_prices_catalog.jsonl` showed a curation catalog with source filenames, SHA, rows, unpriced reference record counts and Parquet file lists, **not a Perps venue-trade/depth payload schema**. No raw market events were replayed, no ZIP extraction was performed, and no new private source data was committed. This absence does not prove no Perps files exist elsewhere; compatibility remains UNVERIFIED until actual bounded finalized Phoenix Perps schemas are supplied.

## Implemented source adapters

`integrations/historical-input/src/perps.rs` extends the existing historical-input normalization framework. `PerpsSchema` explicitly declares venue, observed instrument symbol, base/quote units, feed, four record kinds (trade, BBO, depth, reference), payload field names, original price/quantity scales and exact contract-to-base multiplier. **No historical private columns, contract sizes, liquidation/funding/OI metrics or exchange format are assumed.**

- `convert_bounded_partition` verifies original finalized Parquet bytes against a SHA-256, then calls the existing bounded Parquet `convert_small_parquet` and `convert_batch` before mapping its canonical rows. The schema itself is SHA-addressed, and record IDs/session, venue, raw event times, source partition and row digests are retained.
- `PerpsIndexedPartition` selects explicitly indexed finalized source partitions by venue/symbol/receipt overlap, exact schema hash, exact source-member SHA and path safety. Bounded to 32 selected members. It never walks disks recursively or accesses collector SQLite/WAL.
- Trade, best-bid/ask, best-first depth (max 200 levels), mark/index references where available, optional aggressor side and source timestamp are normalized. All raw numeric price/quantity fields **must be decimal strings**, never JSON float approximations; malformed price, duplicate levels, crossed depth, unknown field and lossy unit conversion fail closed.
- Source price and base-quantity units normalize exactly to 1e8 precision using fixed-point integers and checked arithmetic. More precise fields are rejected if they cannot be represented without rounding. Different venues/instruments require separately configured schemas.
- Missing source exchange clocks are tagged uncertain; source timestamps beyond receipt time fail closed. Excess source clock skew is surfaced as uncertain, not silently corrected.
- Original partitions larger than the historical-input 16 MiB / 1024 row cap must be split or streamed via separately approved finalized bounded partitions. No full-archive memory loads.

## Causal event buffering and features

`integrations/historical-input/src/perps_features.rs` adds `CausalPerpsBuffer` (32 instrument limit, 1024 observations per instrument, 32-trade rolling feature window). `ingest` enforces per-instrument chronological receipt ordering, identity deduplication/conflict detection, source sessions and bounded eviction. Cross-session reconnects are quarantined as `SessionUnverified`, not incorrectly combined. Buffers and their provenance can be serialized to a SHA-tagged `PerpsFeatureCheckpoint` and deterministically restored outside production sources.

`join` requires a Kalshi decision receipt cutoff, explicit quote/asset/venue mapping, maximum quote age and lookback. It **never uses Perps observations received after the decision**, even if already present in memory. Missing or stale venue quotes remain explicitly missing; unknown aggressor flow and mark/index prices remain optional rather than invented. The returned per-venue features include:

- Bid/ask and spread, quote spread in integer basis points.
- Top-book bid/ask quantity imbalance.
- Last recorded trade and last-pair price return in integer bps.
- Capped realized RMS of trade-to-trade bps returns, a *sampled volatility proxy*, not a complete traded-path standard deviation.
- Signed **known-side** trade flow plus count of unknown aggressor prints.
- Optional mark/index dislocation in integer bps when both reference values are present.
- Last source/quote timestamps, clock uncertainty, session, venue, instrument, source partition/schema/row SHA provenance and explicit NO_EMPIRICAL_PNL classification.

`integrations/source-loader/src/perps_bridge.rs` binds the existing Phase 7 `MarketSpec` and `MarketBatchOutcome` to this causal feature buffer. It checks exact ticker/event/series, market replay content SHA, reported Kalshi decision timestamp and source catalog SHA, maps BTC/ETH/SOL/XRP or explicitly documented other asset bases, then performs a strict receive-time join. No Perps feature is treated as a Kalshi fill, settlement or profit.

## Authored NOT RUN tests

Synthetic tests exist for two venues, exact 8-decimal price and contract-multiplier precision, lossy/float input rejection, depth sorting and crossed books, row identity conflicts, future exchange clock, missing reference prices, explicit missing/stale venue quote, future Perps observation exclusion, duplicate and out-of-order receipt, reconnect quarantine, clock uncertainty, bounded buffer eviction, integer returns/negative movement, signed trade flow, book imbalance, mark/index dislocation, checkpoint SHA tampering/restart, indexed partition path/SHA selection, and Phase 7 Kalshi replay identity/revision binding. Existing `ColumnMapping` RFC3339/EpochMs tests remain authored, not run.

## Strict evidence and known limitations

1. The original Perps source payload schema/column names, venue formats, instrument mapping and historical precision are **UNVERIFIED**. All new field mappings in tests are explicitly SYNTHETIC.
2. Historical-index and schema SHA declarations alone are not provider authentication. Original finalized bytes must independently hash-match, session continuity and completeness must be established.
3. There is no confirmed historical funding, liquidation, open interest, implied flow from trade prints lacking aggressor direction, or continuous high-frequency volatility path; none are filled in.
4. Compilation, static type correctness, all Rust tests, real-data parquet compatibility, source continuity and runtime resource costs must be established in Phase 15. No empirical strategy performance was computed.
5. Research outputs must remain outside production source roots; no collector or live service change is permitted.

**Stop after Phase 8.** Next new phase is Phase 9 Frozen Strategy Registry, on a branch stacked from the exact verified Phase 8 SHA, only after a separate user request. Never run Phase 15 without fresh authorization.

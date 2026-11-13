# PHOENIX Phase 8 — final source-development checkpoint

**Phase:** 8 Historical Perps. **Status:** CODE_WRITTEN_UNTESTED / ORIGINAL_PERPS_SCHEMA_UNVERIFIED.
**Parent:** Phase 7 branch `codex/phase7-multimarket-history-v1` at `6ed6febb90d51f045cd7b1366614ac1c732a0a37` (draft PR #29).
**Child:** `codex/phase8-perps-history-v1`. Re-fetch final GitHub PR/HEAD after this file is committed.
**Last pre-checkpoint commit:** `897cf4fe97ce89b1ba067db3498752e4a9e3c24a`.

## Source changes

1. `integrations/historical-input/src/perps.rs` — schema-driven exact-decimal Perps trades, bid/ask, full depth and optional mark/index importer, stable venue/session/event/source identity, SHA-checked bounded original Parquet bytes, source-index partition selection and controlled contract-unit normalization. No assumed private payload schema.
2. `integrations/historical-input/src/perps_features.rs` — bounded receipt-time causal Perps observation buffers and research features for spread, imbalance, signed known-side flow, last-pair return, sampled RMS return volatility proxy, and optional mark/index dislocation. Session reconnect quarantine, stale/future rejection, SHA-tagged resumable checkpoint.
3. `integrations/historical-input/src/lib.rs` and `Cargo.toml` — expose new Perps modules and serde/serde_json dependencies. Existing historical-input normalizer reused.
4. `integrations/source-loader/src/perps_bridge.rs` — exact Phase 7 market identity, replay SHA and receipt cutoff to Perps causal join. Explicit no empirical execution or independent settlement promotion.
5. `integrations/source-loader/src/lib.rs` and `Cargo.toml` — expose and depend on historical-input without a dependency cycle.
6. `docs/PHASE8_PERPS_HISTORY_AND_CAUSAL_FEATURES.md` — complete data contracts, limitations and synthetic test inventory.
7. This checkpoint file.

## Static cross-module review

Text inspection confirmed module exports, Cargo dependency edges, explicit schema mappings, original-byte partition SHA check, decimal string-only source payloads, zero-loss fixed-point conversion, bounded per-instrument buffers and index selectors, exact Phase 7 replay hash/identity/decision binding, and `received_ns<=decision` causal filtering. This **does not** certify compilation or successful tests.

## Tests authored (all NOT RUN)

Multiple synthetic venues, symbol/quote base mapping, exact decimal and contract-unit precision, source schema/partition provenance, missing/crossed/unsorted depth, duplicate conflict, timestamps and clock skew, per-venue out-of-order events and session resets, stale/future observations, missing Perps history, spread/orderbook imbalance, positive/negative returns, mark/index divergence, aggressor uncertainty, bounded memory, restart hash tamper and deterministic resumption, exact Phase 7 bridge and source identity mismatch. Historical-input preexisting timestamp/Parquet regression source retained.

## Material unknowns

- **Private real Perps original schema:** NOT FOUND in reviewed historical-input source or explicitly Perps-named members of the bounded supplied research ZIP central-directory inventory. This is absence of evidence in the inspected sources, not proof that no source exists elsewhere.
- Original Perps venue field mappings, instrument contract size, timestamp precision and live/session continuity have not been independently verified.
- No Perps source events were replayed or imported. All new fixture payloads are synthetic.
- End-to-end compiler/API compatibility, reference joins across actual exchange venues, time alignment, performance, strategy P&L and OOS remain unverified.
- No authenticated final Kalshi settlement, original historical execution proof or trading orders are implied by feature generation.

## Operational boundaries and exact next starting point

No Cargo, Rust compilation, tests, historical simulations, benchmarks, GitHub Actions execution, active DB/WAL reads, collector changes, production writes, trading services or live orders occurred. Every new commit has `[skip ci]`. Do not merge any stacked PR into `main`.

**STOP AFTER PHASE 8.** For Phase 9 in a future separately authorized session: re-fetch the Phase 8 draft PR and exact head SHA, create `codex/phase9-frozen-strategies-v1` based on that SHA, and reconstruct original archived frozen strategy definitions only where backed by real manifests/code, marking approximations separately. Phase 15 local validation requires a different explicit authorization.

# Phase 6 — final SOURCE-DEVELOPMENT checkpoint (updated November 13, 2026)

**Status: CODE_WRITTEN_UNTESTED — SOURCE CONTRACTS AUTHORED.**
All tests, Cargo, historical replay, paid CI, and performance benchmarks: **NOT RUN**. No production sources, collector settings, active database or orders modified.

Starting SHA: `1d80711e7d59174aa5966de601b9e510f856e486` (initial draft PR #28).
Parent: `codex/phase5-1-economics-corrections-v1` at `5ede6f9ffe573166fdbfad003edbc06ae19ae5c8`.
Branch: `codex/phase6-execution-engine-v1`; update and re-fetch PR #28 after this checkpoint is committed.

### Source deliverables

1. `crates/sim/src/historical.rs`: receipt-time, immutable exact-fractional limit-order lifecycle, level-by-level taker IOC, conservative maker queue and complete-tape checks, latency/staleness, cancellation/expiry, nonfills, exact money units, bounded scenario API, evidence schema. A new `simulate_source_window` requires a matched provenance/session/window witness rather than a standalone caller-supplied boolean. All outcomes remain MODELLED; no actual fills.
2. `integrations/hftbacktest-adapter/src/lib.rs`: verified existing *optional* upstream backtest-only adapter path, with a strictly lossless whole-lot compatibility bridge. Crate manifest keeps exact `5f3ec40b2afb764e0fea112f941ed85523ef4e88` pin and Rust 1.91.1 MSRV. No upstream f64 P&L or untested queue parity.
3. `integrations/contract-economics/src/lib.rs`: historical effective-dated fractional fee bridge, explicit no-unknown-fee-zero rule.
4. `integrations/source-loader/src/book_integrity.rs` and `src/lib.rs`: existing canonical fractional depth projected into simulator without adding a second book, preserving unverified source classification.
5. `integrations/source-loader/src/pipeline.rs`: fixed dangling `&book` reference in trailing checkpoint loop; both checkpoint paths now use `integrity.forensic_book()`.
6. Cargo dependency surfaces for the sim/economics/source-loader integration and `docs/PHASE6_HISTORICAL_EXECUTION.md` complete the contracts.

### Static review and tests (not run)

Source inspection confirms module references exist, the hftbacktest path/commit/toolchain are explicit, queue bridge rejects fractional precision loss, fee bridge uses series/role/time keys, future book and session mismatches are rejected in strict source mode, source-loader replay no longer accesses the deleted local `book` variable, all new commit messages include `[skip ci]`.

Synthetic test source exists for taker sweep, partial and nonfills, maker conservative queue, missing maker tape, conflicting/duplicate trades, future signal, stale and missing books, cancel before arrival, invalid depth, bounded scenarios, exact fractional fees, mismatched historical fees, strict witness/session/continuity and whole-contract hft adapter losslessness. All tests are **NOT RUN**.

### Source/data and compile uncertainties

Original tape authenticity, full subscribed-channel continuity and primary benchmark evidence remain externally blocked; source-derived projections cannot auto-promote to verified depth. Real fee schedules are not bundled. Optional hft comparison may fail compilation on toolchain or upstream API changes; no build has been performed. Historical execution is hypothetical until Phase 15 independently checks real source and conservative assumptions. No empirical P&L or OOS has been measured.

### Next exact instruction

Re-fetch PR #28 final HEAD and confirm no Actions runs. Create `codex/phase7-multimarket-history-v1` from that exact commit. Implement **Phase 7 only**, build on existing source-loader/manifest/decoder. Publish a new draft PR and new checkpoint before beginning Phase 8. Do not run Phase 15.

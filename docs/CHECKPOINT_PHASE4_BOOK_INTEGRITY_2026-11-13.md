# Phoenix Rust — Phase 4 book-integrity checkpoint (2026-11-13)

STATUS: CODE_WRITTEN_UNTESTED. SOURCE REVIEW ONLY; NO CARGO, CI, CODEX, ORIGINAL HISTORICAL ZIP EXECUTION, COLLECTOR OR LIVE ORDER ACTIVITY.

GitHub lineage: new draft PR #25 (codex/phase4-book-integrity-v1) stacked on Phase 3 draft PR #24 at a09e598c68327b3a944dfed26f97c4dd10a566ca. Last source/docs implementation commit 2904145389461e921e6e5c696475078b24ba8b48. This checkpoint becomes a new [skip ci] child of that commit; check exact final SHA at PR #25 head.

New code: integrations/source-loader/src/book_integrity.rs source-aware BookIntegrity and BookEvidence; source-loader/src/lib.rs atomic YES/NO snapshot replacement and invalid complementary-book checks; integrations/source-loader/src/pipeline.rs now emits typed book evidence in its versioned replay report. Documentation: docs/PHASE4_BOOK_INTEGRITY_AND_COVERAGE.md.

Evidence is source-derived, provisional and exact fractional (hundredths of contracts; 1/10000 USD ticks). Snapshot hashes, current SID and connection identity, first/last valid receipt, source timestamps, observed sequence gaps and duplicates, quote age, invalid counts and missing evidence are explicit. No market-filtered stream can self-certify a complete shared subscription. The modeled_execution_eligible flag is false unless a future independent verifier supplies actually authenticated completeness; this Phase 4 branch does not fabricate that verifier.

Authored, NOT RUN: initial snapshot/delta refusal, fractional delta, changed connection/SID, crossed complementary snapshot, stale quote, window-wide channel gap and duplicate diagnosis, bounded candidate ranking, earlier as-of checkpoint regression. All previous tests retained.

Source review: no new external dependency; reuses existing forensic Book and shared-channel data and does not convert fractional quantities via integer-only L2 API. Residual compiler risks include mutable map borrow checking in snapshot/delta checks and integration of the expanded report serialization. Full historical WSS session coverage and sourced resnapshot-after-reconnect cannot be authenticated from selected 24 original members in this build-first phase.

Future validation (NOT RUN): cargo test --manifest-path integrations/source-loader/Cargo.toml --all-targets; cargo test --manifest-path integrations/kalshi-wire/Cargo.toml --all-targets; cargo test -p phoenix-l2; rustfmt/Clippy where installed; original source as-of replay under isolated research authorization.

Phase 5 economic-rules work is explicitly separate. Previous PRs and main remain untouched; no production archives/DBs/writers were modified.
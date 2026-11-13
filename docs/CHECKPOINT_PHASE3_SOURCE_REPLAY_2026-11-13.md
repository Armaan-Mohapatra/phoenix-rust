# Phoenix Rust Phase 3 — original-source replay checkpoint

Status: CODE_WRITTEN_UNTESTED. All source work authored and statically reviewed; no Rust compilation, tests, private ZIP replay, CI, Codex, trade execution or production activity.

Lineage: Phase 2 draft PR #23 base ac5e6931e3a5fb866ca663b98b1301b0c1b3aee9. New draft PR #24, branch codex/phase3-source-replay-v1; last implementation commit d6eaac28dd66a5da064a3fdc9f4f0a0905ea0427. All commits carry [skip ci].

Files changed: integrations/source-loader/src/pipeline.rs (new), integrations/source-loader/src/lib.rs (module and synthetic original-Parquet integration tests), integrations/source-loader/src/bin/phoenix-original-replay.rs (read-only source CLI), docs/PHASE3_ORIGINAL_SOURCE_REPLAY.md (schema, limits, pilot and future command). No prior reader/codec or legacy fixture overwritten.

Implementation: calls the existing bounded SHA-verified ZIP Parquet reader, official source-origin message codec and exact fractional forensic book. Preserves original message bytes and raw hash in capture callbacks, source/sending/event/receipt time distinctions, original member and row, connection/SID/sequence. Enforces receipt cutoff BEFORE source ordering and shared-channel diagnostics, deterministic ordering and typed as-of book checkpoints. Discards exact sequenced duplicates; conflicting record identity fails closed. Never rounds fractional quantity. Emits a versioned report with catalog and per-member SHA, declared and decoded rows, receipt range, clocks, source quality gaps, provisional book and no-execution/settlement evidence flags. Earlier five projected top-ten books may be compared via explicitly non-probative optional API; 63,813 previously projected deltas are not asserted as actual Rust output.

Pilot: KXBTC15M-26OCT241900-00, decision 2026-10-24T22:47:39Z, existing 24-member allowlist, original curated SHA256 55f869f1231b67948395ac16a33b50ec774711653da2a3d22c693050eda2db08. 194,253 rows are prior all-market catalog declarations, NOT a newly executed Rust observation.

Regression tests authored NOT RUN: original synthetic ZIP source timestamps and as-of exclusions, checksum and traversal refusal, deterministic serialization, missing snapshot fail-closed, duplicate/conflict verdict, future checkpoint request refusal, and synthetic original-format fixture parity. Prior Kalshi wire/L2/checksum tests preserved. Remaining source ingestion risks include shared-channel subscription completeness unavailable from a market-only extract, timestamp encoding/version mismatch, unbounded historical coverage outside selected 24 partitions, root crate compatibility and first full Rust raw execution.

Explicit release boundaries: NO verified initial snapshot, full-channel historical continuity, execution-eligible liquidity, official/primary settlement, actual fills, or historical P&L. Phase 4 and Phase 5 remain separate branches; Phase 3 itself does not implement their certification gates.

Validation required later: isolated Cargo test of source-loader, kalshi-wire and historical-input, execution of original ZIP CLI with exact catalog SHA, JSON canonical repeatability, source-partition/projection comparison with no forced equality, clock/session diagnostics. None performed here.

Checkpoint commit is the direct child of d6eaac28dd66a5da064a3fdc9f4f0a0905ea0427 and verifiable as the PR #24 GitHub HEAD.
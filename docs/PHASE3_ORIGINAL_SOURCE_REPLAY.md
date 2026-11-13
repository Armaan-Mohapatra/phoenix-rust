# Phase 3 — original-source historical replay contract

STATUS: CODE_WRITTEN_UNTESTED. This phase made no source ZIP reads, historical simulations, toolchain calls, or private uploads.

Source-loader::pipeline::run reuses the original SHA-verifying bounded ZIP/Parquet reader, Kalshi wire decoder and forensic fractional book. It does not substitute the integer-only L2 book or old five top-10 projections as original event history.

- Allowlists: up to 32 named Parquet members across up to 12 ZIPs, 16 MiB per member, 4 MiB catalog, 250k scanned rows and 64 MiB in-memory deterministic event sorting, 64 as-of checkpoints.
- The original catalog SHA and member SHA proofs, row counts, raw WSS hashes, sending/source/receipt times, connection IDs, SID and sequence are preserved in the consumer and summarized without bulk raw payload in the JSON report.
- Source receipt cutoff applies before market event ordering and shared-channel diagnostics; no data received after the requested decision may affect its as-of book.
- Sequenced events deduplicate by connection+SID+sequence+type and raw SHA. A conflicting event aborts instead of promoting corrupted history. Unsequenced events do not receive guessed identities.
- Fractional book observations are reported in hundredths of a contract and 1/10,000 USD price ticks. Missing snapshots and incomplete channel history remain unverified.
- Previous top-10 projection comparisons are optional, clearly labeled, and never proof of original replay correctness or executable maker fills. No modeled P&L or independent settlement is claimed.

New read-only CLI phoenix-original-replay accepts TICKER START_UTC DECISION_UTC ALLOWLIST EXPECTED_CATALOG_SHA256 and the original ZIP paths. It reads catalog from first ZIP, verifies its SHA, and writes compact versioned JSON to stdout only.

Initial pilot target KXBTC15M-26OCT241900-00 at 2026-10-24T22:47:39Z. Existing allowlist docs/BTC_20261024_ASOF_MEMBERS.txt (24 original members, 194,253 catalog-declared all-market rows). Expected catalog SHA256 55f869f1231b67948395ac16a33b50ec774711653da2a3d22c693050eda2db08.

Future authorized local command (not run):

    cargo run --manifest-path integrations/source-loader/Cargo.toml --bin phoenix-original-replay -- KXBTC15M-26OCT241900-00 2026-10-24T22:44:40Z 2026-10-24T22:47:39Z docs/BTC_20261024_ASOF_MEMBERS.txt 55f869f1231b67948395ac16a33b50ec774711653da2a3d22c693050eda2db08 <original-ZIP-1> <original-ZIP-2> ...

No compiler, CI, historical ZIP replay or collector process was invoked. Full-channel book certification and settlement verification are separate phases.
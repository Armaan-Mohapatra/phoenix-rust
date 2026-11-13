# BTC original-event candidate coverage from verified Parquet footers

- Exact ticker: `KXBTC15M-26OCT241900-00`; selected as-of decision window: 2026-10-24 22:44:40Z to 22:47:39Z.
- Original archive source: six user-uploaded `LunaDelta_20261025T083801Z_market_data_part001.zip`–`part006.zip`; all accessed read-only, without a broad Windows drive scan.
- Curation catalog: `archive/curated/curation_catalog.jsonl`, 3,664,603 bytes, 9,243 entries; SHA-256 `55f869f1231b67948395ac16a33b50ec774711653da2a3d22c693050eda2db08`.
- Parquet footer scan: 685 raw-event members; **0 unreadable/unknown timestamp footer ranges**; **24 exact members** overlap the pre-decision receive-time interval, and their footers declare **194,253 rows across all markets**, NOT 194,253 BTC deltas or source-confirmed fills.
- Allowed source file names: [BTC bounded member allowlist](BTC_20261024_ASOF_MEMBERS.txt). They are HASH IDENTIFIERS, not data dumps.
- For each member, the Rust source loader verifies the original Parquet bytes against its per-member SHA256 in the curation catalog before admitting any row. The compact CSV including ZIP part, member SHA256, rows and receive-time min/max is also available as the research handoff artifact created from these same Parquet footers.
- Example byte-for-byte verified member: `segment_0082928055b8bd4b6483.parquet`, 611,640 bytes, SHA256 `d847d5e0248ebdfb19d90ea3b1b66b7e815cb965e4a22c2d6e9474f878afbdc7`, 8,190 declared rows.
- The bounded loader cap is now 250,000 scanned rows, sufficient for this confirmed one-market allowlist but intentionally far below a whole-archive replay. The reader buffers only market-matching messages under a separate 64 MiB cap.
- No 63,813-event Rust replay has been run yet against source bytes in hosted CI. Original ZIPs are mounted in the research execution container; Rust toolchain and Arrow/Parquet runtime are not available there. The test runner in GitHub Actions does **not** see local/ChatGPT-mounted ZIPs. Therefore **source-run parity remains BLOCKED_LOCAL_RUST_EXECUTION** and source-side book initialization remains UNKNOWN.
- Independently authenticated CF Benchmarks primary values remain missing; the source-derived relayed BRTI simple-mean computation remains PROVISIONAL, even when matching archived Kalshi YES labels.

No original source Parquet payload is published here, and no production source or collector has been changed.

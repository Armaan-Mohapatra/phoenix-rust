# Source-origin historical ZIP/Parquet loader — research-only

Reads **original** WSS `raw_message` bytes and source timestamp/sequence columns from specific ZIP-contained Parquet members selected through a precomputed member list, verifies each member SHA256 against `archive/curated/curation_catalog.jsonl`, and converts the original Kalshi wire payload through the historical decoder. Sort order is LOCAL RECEIPT timestamp, never exchange clocks that may be 0.3 seconds ahead. Source/event clock times, session IDs and sequence IDs are preserved. Explicit failure on source SHA mismatch, malicious member names, unknown columns, oversized partitions or row count conflicts.

**Status: CODE + SYNTHETIC PARQUET SCHEMA TESTS, NOT VERIFIED FULL REAL SOURCE REPLAY.** CI fixtures exercise archived 2026-10-24 field structures and timestamp values, but the actual 63,813 real source events have NOT been run through this Rust loader inside GitHub CI, because the full uploaded ZIPs and local E: drive are not available in GitHub Actions. They are accessible to the research container for read-only ZIP/footer inspection. Run a bounded local parity replay using authorized ZIPs before making a true Rust historical compatibility claim.

Actual source catalog confirmed read-only: `LunaDelta_20261025T083801Z_market_data_part001.zip!archive/curated/curation_catalog.jsonl`, SHA256 `55f869f1231b67948395ac16a33b50ec774711653da2a3d22c693050eda2db08`, 3,664,603 bytes, 9,243 rows. Example raw member `segment_0082928055b8bd4b6483.parquet` 611,640 bytes, recorded SHA256 `d847d5e0248ebdfb19d90ea3b1b66b7e815cb965e4a22c2d6e9474f878afbdc7` matching the curated catalog, 8,190 claimed rows. Those are source-integrity checks, not execution validation.

Boundaries: up to 12 caller-specified ZIP files, 32 explicit member names, 16 MiB/member, 4 MiB catalog, 150k scanned rows total, 64 MiB buffered selected messages, 256-row batches. Does not recursively scan disks, open production DBs or write into the ZIP source. Supports no automated venue orders or collector service.

**Cannot certify initialization from market-filtered deltas:** shared channel `seq` can skip when messages for other markets are omitted. A complete same-channel initial snapshot plus reconnect and global continuity evidence must be supplied separately. Default book initialization state is UNKNOWN, not a modeled executable book. Existing five derived-book checkpoints remain provisional.

Next: verify actual Parquet columns against the original uploaded files in an authorized local Rust environment; add no-raw-upload per-market source member selection, genuine whole-session L2 reconstruction and causal snapshot parity before using realistic maker/taker P&L.

## Additional forensic L2 gate

An exact 0.01-contract source-message L2 state machine is included in `source-loader::forensic`. It strictly rejects negative depth, duplicate sequence, invalid sessions, and missing initial snapshots. A snapshot extracted only for one market does NOT certify full subscription-channel continuity; all reconstructed books remain `PartialInitialization` and explicitly cannot authorize venue or high-confidence modeled fills until full authenticated source/channel verification. Tests exercise fractional size, restart/reconnect and invalid deltas; never use inferred empty books as proof.

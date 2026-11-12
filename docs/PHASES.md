# Phoenix Rust: staged delivery

Original offline-first Rust toolkit. No network access, collectors, services, credentials, or exchange orders. Tests and demo use synthetic-only data.

**Checkpoint:** [2026-11-12 status, code SHA, gates, estimates](CHECKPOINT_2026-11-12.md). Phase names below describe prototypes, not production-complete systems.

Implemented in development branch:
- Phase 0: workspace and Linux/Windows CI.
- Phase 1: fixed-point book and fee arithmetic, as-of causality, sequence gates and evidence-tiered settlement.
- Phase 2: depth-limited taker replay and conservative maker model with an explicit complete-tape precondition.
- Phase 3: fee-aware strategy/no-trade decision, inversion variant, split embargo, event summary, Brier score and in-memory paper accounting.
- Additional synthetic components: bounded CRC32 archive frames, index-window comparison, sequence-aware L2 reconstruction, offline decision-to-settlement pipeline, GET-only Kalshi REST adapter, and isolated in-memory ZSTD Parquet + SHA256 roundtrip.

Not implemented:
- Signed read-only WSS/REST ingestion, persistent raw event journal, sealed Parquet and source verification.
- Independent settlement reference-data retrieval, exact 60s index-average reconstruction and rules matching.
- Full event-stream L2 replay, latency models and proved queue reconstruction.
- Frozen registry of actual Phoenix strategy definitions and untouched historical performance.
- Persistent multi-portfolio paper ledger, operational health and installation.

Never infer a real fill or independent verification from this synthetic demo. Preserve old source data and operational collectors untouched.

Third-party adoption proposals: MIT hftbacktest for future Rust queue model; MIT Jon-Becker for Parquet indexing; Apache-2.0 Oracle3 for relational constraints; MIT ryanfrigo for scoring; MIT flumine for state-machine tests; MIT pmxt for read-only venue interface. Homerun AGPL and unlicensed bots are design references only. No upstream code copied.

Run cargo test --workspace --all-targets --offline, and cargo run -p phoenix-cli --offline.

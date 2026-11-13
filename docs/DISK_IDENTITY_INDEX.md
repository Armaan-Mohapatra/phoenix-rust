# Isolated research disk-backed event identity index (experimental)

Implements a persistent, transactional event-ID and sealed-partition index using `redb` `=4.5.0` (MIT/Apache-2.0, pure Rust). Unlike redb's default **1 GiB** cache, this research prototype configures **16 MiB** to suit Phoenix's constrained Windows laptop. At most 256 event IDs enter one transaction. Correctly handles identical replays, conflicting event fingerprints, manifest conflicts, rollback and process reopening in deterministic synthetic tests.

**Crucial: not yet wired to the existing `integrations/durable-archive` publisher.** It is a separately isolated experimental storage library. The current durable publisher still has the old 250,000-entry in-memory cap and must be migrated only after a verified transition protocol. In particular, original Parquet bytes/manifest must be committed and verified BEFORE indexing; on restart all sealed manifests must be replayed/reconciled to fix the interval between source publication and index commit. This stage does NOT claim power-loss durability of directory metadata on Windows or measured production throughput.

Root safety: creates a new research-only directory with a fixed marker; any preexisting directory without that exact marker is rejected, including source/production paths. Never touches live SQLite/WAL, source ZIPs, Kalshi collectors, trading services or actual orders.

Upstream crate: [redb 4.5.0](https://docs.rs/redb/4.5.0/redb/), MIT/Apache-2.0. No production data or credentials in GitHub. High-scale benchmarks and Windows power-cut tests remain outstanding.

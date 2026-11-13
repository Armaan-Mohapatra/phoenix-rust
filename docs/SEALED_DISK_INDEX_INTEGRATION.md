# Verified sealed archive → disk-backed identity integration

**Research-only, unmerged.** The new `integrations/indexed-archive` wrapper composes existing `durable-archive` and `disk-identity` (redb 4.5.0) without changing their production code paths.

The archive writes synced Parquet and seals a manifest **before** a bounded ACID redb transaction. Each reopen verifies all sealed Parquet bytes, decodes typed rows, rejects database-only manifest markers, then reconciles any missing index transactions with sealed-manifest event IDs. Fail-closed tests simulate interruption after manifest publication, interruption after database commit, duplicate recovery, and unsafe pre-existing roots.

redb still uses the verified 16 MiB cache limit with <=256 IDs per transaction. This *wires* disk identity to sealed archive, but **does not replace** the legacy in-memory HashMap or its 250k index limit; further migration and O(N²) manifest verify optimization are needed. Windows directory metadata power-loss, disk-full injection and scale benchmarks remain open. There are no collector/service/order/production data operations.
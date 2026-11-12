# Durable research archive: implementation and limitations

This package builds on `phoenix-parquet-batch` and is **not** a running Kalshi collector. It is intended for **new, dedicated research directories only**.

## Implemented invariants

- Exclusive operating-system advisory lock prevents two concurrent publishers; automatically released after process crash.
- Marker file rejects accidentally opening an existing directory not created for Phoenix research.
- Max 256 rows per published partition, 8 MiB Parquet bytes, 1 MiB manifest, 2,000 partitions and 250,000 event identities. Refuses writes after limits rather than exhausting workstation RAM.
- Deterministic content SHA256 partition ID, source/event scope, schema version, exact event IDs and per-row hashes.
- Duplicate suppression across sealed manifests; reuse of an event ID with changed content fails closed.
- Publish a file-synced Parquet partition before an independently file-synced JSON manifest, with atomic same-directory renames.
- A manifest with `status=sealed` is the commit marker. No manifest means the data is NOT committed.
- On restart, incomplete temp files and uncommitted data partitions are quarantined, not promoted or erased.
- Verification rechecks SHA256 and Parquet row counts on bounded partitions.
- Synthetic crash points test interrupted writes and idempotent retries on CI.

## Important unsolved gates

1. **Windows power-loss guarantee:** file contents are synced, but Rust std does not provide a portable directory metadata sync on Windows. Return `FileSyncedDirectoryNotProven`; do not claim power-cut durability. Windows-specific atomic replacement and filesystem semantics require further evaluation.
2. Source completeness is NOT established by the asserted `observed_sequence_gaps` field. Historical finalized partitions still require independent feed/manifests reconciliation.
3. Indexing reads bounded manifest metadata at startup and has an explicit 250k-event cap. Scaling to 100M+ events requires disk-backed paged indexing (e.g., redb/SQLite research copy), not an unlimited hash map.
4. `source_manifest_sha256` is a caller-supplied reference, not independently trusted signed provenance. No issuer verification is performed.
5. The v1 identity interface accepts caller-supplied event IDs. A source-specific adapter must derive and validate stable identities before publication.
6. No historical E:/C: source data, live collector, active SQLite, proprietary data or private strategy code is touched.

## Continuation

Verify CI on Ubuntu and Windows, then add historical read-only schema/identity adapters and a bounded sealed Parquet market fixture. Do not merge or deploy this package.

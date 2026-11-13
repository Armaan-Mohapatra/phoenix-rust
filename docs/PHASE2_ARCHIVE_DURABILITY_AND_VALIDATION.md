# Phoenix Rust Phase 2: Source-level archive durability and recovery

STATUS: CODE_WRITTEN_UNTESTED. No Cargo, GitHub Actions, Codex execution, original-source backtest or production process was run during this phase. The implementation is not claimed power-loss durable or release-ready.

## Publication and recovery states

The existing v1 Parquet/JSON sealed manifest format is preserved. The new recovery.rs module defines typed states:

- PREPARED_UNCOMMITTED: validated row or incomplete temporary source file. No committed evidence.
- DATA_SYNCED_UNSEALED: finalized source file without sealed manifest. Quarantine and retain as forensic evidence after restart; never promote.
- SEALED_NOT_INDEXED: authentic sealed JSON manifest and decoded Parquet row hashes, but no transactional redb identity marker. Recover by replaying source evidence into redb.
- FULLY_RECONCILED: published sealed source record and exact redb partition signature/event identities match.
- CORRUPT_OR_INCONSISTENT: missing manifest, checksum or event hash mismatch, version mismatch, or cross-partition event identity conflict. Fail closed and preserve files.
- DURABILITY_UNCERTAIN: a file/link/sync or database transition failed after possible publication. Poison the writer until a verified cold reopen.

No published event is accepted solely because redb has an index marker. No indexed event is accepted without source Parquet and sealed-manifest verification. Reopening iterates one manifest and at most 256 IDs per redb transaction, not all historical market history in RAM.

The archive-level PublishResult distinguishes SEALED_NOT_INDEXED from fully reconciled legacy publication. IndexedArchive only returns FULLY_RECONCILED after the redb transaction succeeds. Filesystem durability is a separate field and is not upgraded by logical reconciliation.

## Storage and OS safety

- Bounded Parquet and JSON bytes are staged with create_new, write_all and File::sync_all. Simulated partial writes and pre-sync interruptions leave pending files for quarantine.
- Publication uses same-volume fs::hard_link as a no-clobber create operation, then removes only its own successfully published temp link. It NEVER overwrites an existing sealed destination. A filesystem without supported hard linking fails closed; no unsafe copy/delete fallback.
- Sealed manifests are the publication authority. A file without a manifest is uncommitted. Temp and orphan files move to quarantine; conflicting quarantine paths reject rather than deleting evidence.
- Marker contents must exactly equal the research-only v1 marker. Unexpected existing paths and link/junction/reparse ancestors are rejected. Windows FILE_ATTRIBUTE_REPARSE_POINT is checked where supported.
- Research roots nested under TradingNextData, KalshiData, KalshiRuntime and paper15m_runtime are rejected. Original source/collector directories are not opened or modified by these GitHub source edits.
- Paths are checked before write. A malicious privileged adversary racing path substitutions after these checks remains a TOCTOU risk; full handle-relative filesystem confinement is not established.

## Windows durability (not verified by tests)

Microsoft documentation:
- https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers
- https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexa
- https://learn.microsoft.com/en-us/windows/win32/fileio/reparse-point-operations

File::sync_all flushes file contents through OS APIs. Successful hard-link creation and file synchronization do not establish survival of directory metadata through sudden power loss on every Windows filesystem or external USB device. MOVEFILE_WRITE_THROUGH documentation is not a generic guarantee for arbitrary directory rename/link power-cut safety. Windows responses continue to say FileSyncedDirectoryNotProven. Unix directory fsync is a software-level operation, not a physical power-failure validation. The required no-clobber hard-link capability should be checked on the actual research filesystem (NTFS is a candidate; other filesystems may not support it).

No Windows Registry settings, service settings, collector schedules, storage drivers or production data paths were changed.

## Redb ACID and failure invariants

- The redb metadata table records schema version 1; an incompatible explicit schema version fails closed. Existing compatible legacy tables can be marked with the same version without rewriting Parquet/JSON.
- A redb partition marker is committed in one transaction with at most 256 event-ID/digest mappings. A preexisting marker also verifies every expected identity.
- An identical event ID in a different newly sealed partition is a conflict even if the digest matches; only repeat reconciliation of the SAME partition is idempotent.
- Faults: transaction rejected; inserted rows before partition marker; marker written before commit; committed before acknowledgement. All pre-commit faults rely on transaction rollback. The last one must reopen as already committed.
- After seal but before redb commit, recovery checks the original source and replays only missing index transactions. After any ambiguous failure, the indexed writer is poisoned. A new verified process/reopen is required before any further publish.
- Cold-start corruption, missing sealed data, orphaned redb marker, incompatible database schema, duplicate IDs and conflicting signatures fail closed, with no automatic source-record overwrite.
- No continuous, unlimited retry loop or automatic production migration is introduced.

## Machine-readable report

IndexedArchive::recovery_report_json produces a versioned RecoverySummary with publication state, inspection_complete, durability assurance, counts of sealed/recovered/already-indexed partitions, quarantined temp/unsealed files, verified event references and observed duplicates. At most eight partition hashes and eight caller-declared source manifest hashes are retained. It includes error CATEGORY strings, not bulk event payloads, credentials or private trading balances.

Any field set to null denotes not independently established. In particular, no zero data-loss claim is inferred from recovered counters. On failed opening, IndexedArchive::failed_open_report records inspection_complete=false, a fail-closed state and the failure category; it does not pretend an incomplete scan found no missing records.

## New regression fixtures authored, NOT RUN

Archive staged-write fault matrix: before data write, during write, after write before sync, after data sync, before data publish, after data publication, during manifest write, after manifest write before sync, after manifest sync, before manifest publication, and after sealed manifest publication. Disk-full, read-only, permission-denied, disconnected device, failed sync and failed publish faults are covered by deterministic injected failures.

Redb regression fixtures: rollback before transaction; after row write; after partition marker; after commit before acknowledgement; unknown schema; same event ID across partitions; matching marker with missing ID. Other fixtures cover multi-partition partial recovery, duplicate suppression, source hash corruption, marker tampering, publisher contention, original v1 compatibility, and Unix symlink rejection. Windows reparse-point test is explicitly ignored until final local operator authorization.

## Final local validation, NOT AUTHORIZED FOR EXECUTION NOW

Use the dedicated, operator-gated scripts/validate-phase2-recovery.ps1 in a research checkout with outputs outside protected production paths. It sets CARGO_BUILD_JOBS=2, CARGO_NET_OFFLINE=true, isolated Cargo target paths, and saves SHA-tagged logs and JSON exit-status records.

Example for future authorized campaign (example paths are not guaranteed to exist):

    .\scripts\validate-phase2-recovery.ps1 -RepoRoot E:\PhoenixTooling\stage\phoenix-rust -OutputDirectory E:\PhoenixResearchHandoff\phase2 -ConfirmIsolatedResearchOnly

Optional RunIgnoredWindowsSymlink and RunMillionIdentityStress switches are NOT enabled by default. The script does not shut down Windows, disconnect USB devices, or emulate sudden power loss. True physical power-cut and disk-full on controlled sandbox media, peak RAM, CPU, disk write amplification and archive reopen timings all remain separate unexecuted gates. Do not run on the existing market-data or collector drives.

## Explicit limitations

- Source and deterministic fixtures are authored only. Rust compilation, cross-platform tests, rustfmt, Clippy and disk benchmarks are NOT RUN.
- Legacy RAM mode, 250k-event index and original v1 safety caps are preserved; Phase 1 external mode is isolated.
- Same-volume hard-link requirements may prevent operation on filesystems without supported link semantics; this is an explicit fail-closed result, not a fallback success.
- Windows power-cut durability, removable-media disconnect behavior and hostile concurrent path mutation are not proven.
- No historical BTC raw Rust replay, authenticated initial L2 snapshot, fresh official Kalshi settlement, licensed independent CF primary BRTI, executable historical order fill or profitable strategy is established by these changes.

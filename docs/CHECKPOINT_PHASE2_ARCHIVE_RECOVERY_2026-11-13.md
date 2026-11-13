# PHOENIX RUST — Phase 2 archive recovery development checkpoint
Date: 2026-11-13 UTC. Status: CODE_WRITTEN_UNTESTED / STATIC_SOURCE_REVIEWED / VALIDATION_DEFERRED.

**No Cargo, rustc, Clippy, Windows or Linux tests, GitHub Actions, Codex, source historical replay, collector activity, live orders or production changes were performed. No software was deployed or merged.**

## Verified repository and lineage

Repository: Armaan-Mohapatra/phoenix-rust, PRIVATE.
Starting Phase 1 head: c71ac496758c5a44dd375e2170b567830cc45b48.
New branch: codex/phase2-archive-recovery-v1.
New draft PR #23: https://github.com/Armaan-Mohapatra/phoenix-rust/pull/23
Latest implementation HEAD before this documentation checkpoint: 4011ee08a5e648168fc074c1f812c95fe01da5bb.
The checkpoint publication commit follows that HEAD; the exact final SHA is verifiable from PR #23 head.
Original PR #22 and all other development branches remain intact, and main has not been merged or reset.
Only six paths in source/validation scope were added/modified before this checkpoint; all source commits include [skip ci].

## Changed files and actual implementation

1. integrations/durable-archive/src/recovery.rs — typed PublicationState, Assurance, StorageFailure and FaultPoint; platform-specific OS error mappings; compact versioned RecoverySummary with explicit inspection_complete and unknown metrics; evidence SHA caps; deterministic error-stage classification.
2. integrations/durable-archive/src/lib.rs — v1 bytes and legacy mode preserved. Staged create_new source and manifest with bounded file writes, sync, no-clobber same-volume hard-link publication (no overwrite fallback); source-before-manifest-before-index ordering; post-seal uncertain results; marker exact-content checks; symlink/junction/reparse and protected-root refusal; forensic no-clobber quarantine; OS error classification; publication_state distinct from filesystem durability; test fault transitions.
3. integrations/disk-identity/src/lib.rs — compatible metadata schema version 1 marker (reject incompatible explicit versions); rollback and post-commit acknowledgement fault injection, matching-marker ID verification, reject cross-partition duplicates even with equal SHA, symlink/root checks, unchanged 16 MiB redb cache and max 256 IDs per commit.
4. integrations/indexed-archive/src/lib.rs — preserve Phase 1 lazy-manifest and bounded indexing. Connect source and redb fault points to the sealed/index recovery protocol; poison writer on ambiguous post-publication failure; require cold reopen and verified reconciliation. RecoverySummary JSON reports sealed/recovered/duplicate/quarantine counts, source/partition hashes, manual intervention, incomplete audits and uncertainty. No false zero-loss claim.
5. scripts/validate-phase2-recovery.ps1 — manual-only, explicit ConfirmIsolatedResearchOnly gate, isolated outputs/Cargo target, offline default, two concurrent build jobs, SHA-tagged reports. Deferred, not invoked.
6. docs/PHASE2_ARCHIVE_DURABILITY_AND_VALIDATION.md — precise state machine, failure handling, Windows/Unix semantics, named upstream official Win32 documentation, fault matrix and limits.

Existing source functionality preserved: historical bounded ZIP Parquet loader, Kalshi codec, fixed-point accounting, paper journal, redb index and source checksum verification, original BTC fixture, HftBacktest backtest-only, risk controls, performance reporting, original source manifests and development checkpoints.

## Fault coverage authored — ALL TESTS NOT RUN

Archive transitions: before data write, partial data write, full unsynced data, data sync, before/after data hard-link publication, partial manifest, full unsynced manifest, manifest sync, before/after manifest publication. Failures: disk full, disconnected storage, read-only/permission denied, file-sync, publish failure, existing filename collision, invalid marker, symlink/junction root, quarantine collision. Evidence policy: pre-seal data is not committed; post-seal unindexed is recovered from source rather than invented from redb.

Index states: before transaction, after inserting ID rows before marker, after marker before commit, committed before acknowledgement, schema incompatible, missing ID with matching partition marker, new cross-partition duplicate event ID and conflicting digest. Failed pre-commit transactions must rollback. A sealed manifest with no index is replayed on reopen; a phantom index-only partition is refused.

End-to-end: source publication after-seal error poisons writer, partially recovered multi-partition history, repeated restart idempotence, JSON evidence report, corrupted startup manifest preserved unchanged, existing v1 source-manifest bytes preserved. Phase 1 bounded-memory lazy-manifest tests retained. The explicit 1,048,576-logical-ID stress test remains ignored unless final local stage explicitly requests it. Windows reparse point test is separately ignored because local permission/machine policy is unknown.

## Source-level review findings

- Package interface review: durable-archive already depends on serde, serde_json and fs2; disk-identity retains pinned redb 4.5.0; indexed-archive already has its two local crate dependencies and serde_json. No new third-party dependency, live order capability, networking or credentials.
- Publication order is source Parquet -> sealed manifest -> verified row hashes -> redb ACID commit. Cross-crate error paths conservatively poison the indexed writer if a publication may have crossed an irreversible boundary.
- No-clobber same-volume hard-link publication is a deliberate compatibility tradeoff: filesystems without hard-link support fail closed, potentially including removable non-NTFS media. Production compatibility is unverified.
- Rust compile risks requiring actual local validation: cross-platform cfg paths, redb metadata table lifetime/transaction semantics, public PublishResult state field and test fixture API compatibility, and Windows filesystem API behavior. Static review is not a compiler.
- Snapshot/path checks are best-effort; a malicious privileged attacker changing path identities concurrently may bypass name-based checks absent handle-relative platform APIs.
- Source bytes are never overwritten when the archive or report is damaged; errors require manual investigation rather than auto repair. No infinite automatic retry loop.

## Windows and external evidence limits

Windows File::sync_all and no-clobber hard-link publication cannot prove sudden physical power-loss persistence of directory metadata on Windows NTFS, exFAT or USB devices. Durability classification remains FileSyncedDirectoryNotProven. No device detachment, filesystem remount, actual power interruption or disk-full physical test was executed.

Recovery counts describe archived sealed/source/index evidence only; they do not prove full market-feed capture, independent outcomes or absence of missing events upstream. No original 24-partition BTC full Rust replay, initial L2 snapshot, authorized CF primary BRTI, fresh official settlement or demonstrated after-cost strategy profitability occurred.

## Future local validation — explicitly deferred

From an isolated research checkout, after authorization:

    .\scripts\validate-phase2-recovery.ps1 -RepoRoot E:\PhoenixTooling\stage\phoenix-rust -OutputDirectory E:\PhoenixResearchHandoff\phase2 -ConfirmIsolatedResearchOnly

Expected ordinary tests: durable-archive, disk-identity and indexed-archive all targets plus Clippy, Linux parity where available, exact SHA and JSON exit statuses. Opt-in ignored Windows symlink and million-ID tests must run only when explicitly permitted on compatible research storage. A separate controlled Windows hardware fault campaign is still required. Cross-platform CI was not triggered.

## Completion declaration

Phase 2 source implementation and regression authoring: CODE_WRITTEN_UNTESTED.
Static cross-file review: performed (non-executable, not validation).
Windows power-loss, hard-link filesystem compatibility, actual crash recovery, performance benchmarks and original-source market validation: NOT RUN / UNVERIFIED.
The next roadmap phase (Phase 3 original historical replay) MUST NOT start in this conversation without a new user instruction.

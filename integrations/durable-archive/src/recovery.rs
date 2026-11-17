//! Explicit, non-executable recovery contract for offline research archives.
//! These classifications never imply physical power-loss durability.
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicationState {
    PreparedUncommitted,
    DataSyncedUnsealed,
    SealedNotIndexed,
    FullyReconciled,
    CorruptOrInconsistent,
    DurabilityUncertain,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StorageFailure {
    DiskFull,
    PermissionDenied,
    ReadOnlyFilesystem,
    InterruptedWrite,
    SyncFailed,
    RenameFailed,
    DisconnectedStorage,
    UnexpectedPath,
    DataCorruption,
    UnknownIo,
}
impl StorageFailure {
    pub fn from_io(e: &io::Error) -> Self {
        #[cfg(unix)]
        match e.raw_os_error() {
            Some(28) => return Self::DiskFull,           // ENOSPC
            Some(30) => return Self::ReadOnlyFilesystem, // EROFS
            _ => {}
        }
        #[cfg(windows)]
        match e.raw_os_error() {
            Some(112) => return Self::DiskFull,          // ERROR_DISK_FULL
            Some(19) => return Self::ReadOnlyFilesystem, // ERROR_WRITE_PROTECT
            Some(21) | Some(1167) => return Self::DisconnectedStorage,
            _ => {}
        }
        match e.kind() {
            io::ErrorKind::PermissionDenied => Self::PermissionDenied,
            io::ErrorKind::Interrupted
            | io::ErrorKind::WriteZero
            | io::ErrorKind::UnexpectedEof => Self::InterruptedWrite,
            _ => Self::UnknownIo,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FaultPoint {
    None,
    BeforeDataWrite,
    DuringDataWrite,
    AfterDataWriteBeforeSync,
    AfterDataSync,
    BeforeDataRename,
    AfterDataRename,
    DuringManifestWrite,
    AfterManifestWriteBeforeSync,
    AfterManifestSync,
    BeforeManifestRename,
    AfterManifestRename,
    DiskFullData,
    DiskFullManifest,
    PermissionDeniedData,
    ReadOnlyData,
    DisconnectedData,
    FailDataSync,
    FailManifestSync,
    FailDataPublish,
    FailManifestPublish,
}
impl FaultPoint {
    /// Exact phase represented by each injected transition. This is NOT a
    /// substitute for checking the persisted filesystem after a real failure.
    pub fn interrupted_state(self) -> PublicationState {
        match self {
            Self::None => PublicationState::FullyReconciled,
            Self::BeforeDataWrite
            | Self::DuringDataWrite
            | Self::AfterDataWriteBeforeSync
            | Self::AfterDataSync
            | Self::BeforeDataRename
            | Self::DiskFullData
            | Self::PermissionDeniedData
            | Self::ReadOnlyData
            | Self::DisconnectedData
            | Self::FailDataSync
            | Self::FailDataPublish => PublicationState::PreparedUncommitted,
            Self::AfterDataRename
            | Self::DuringManifestWrite
            | Self::AfterManifestWriteBeforeSync
            | Self::AfterManifestSync
            | Self::BeforeManifestRename
            | Self::DiskFullManifest
            | Self::FailManifestSync
            | Self::FailManifestPublish => PublicationState::DataSyncedUnsealed,
            Self::AfterManifestRename => PublicationState::SealedNotIndexed,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Assurance {
    FileSyncedDirectoryNotProven,
    FileAndDirectorySyncedSoftwareLevel,
    Uncertain,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecoverySummary {
    pub schema_version: u32,
    /// True only after all configured in-scope partitions were inspected.
    /// Counts in an interrupted report are partial observations, not zero loss.
    pub inspection_complete: bool,
    pub publication_state: PublicationState,
    pub assurance: Assurance,
    pub sealed_partitions_found: usize,
    pub index_transactions_recovered: usize,
    pub already_indexed_partitions: usize,
    pub verified_event_references: usize,
    pub quarantined_partial_files: usize,
    pub quarantined_unsealed_data_files: usize,
    pub rejected_corrupt_partitions: Option<usize>,
    pub duplicate_event_identities: Option<usize>,
    pub identity_conflicts: Option<usize>,
    pub missing_or_orphaned_records: Option<usize>,
    pub manual_intervention_required: bool,
    pub evidence_partition_sha256_samples: Vec<String>,
    pub source_manifest_sha256_samples: Vec<String>,
    pub errors: Vec<String>,
}
impl RecoverySummary {
    pub fn empty() -> Self {
        Self {
            schema_version: 1,
            inspection_complete: false,
            publication_state: PublicationState::PreparedUncommitted,
            assurance: Assurance::Uncertain,
            sealed_partitions_found: 0,
            index_transactions_recovered: 0,
            already_indexed_partitions: 0,
            verified_event_references: 0,
            quarantined_partial_files: 0,
            quarantined_unsealed_data_files: 0,
            rejected_corrupt_partitions: None,
            duplicate_event_identities: None,
            identity_conflicts: None,
            missing_or_orphaned_records: None,
            manual_intervention_required: false,
            evidence_partition_sha256_samples: Vec::new(),
            source_manifest_sha256_samples: Vec::new(),
            errors: Vec::new(),
        }
    }
    pub fn record_evidence(&mut self, sha256: &str) {
        // SHA identifiers only, never source rows, credentials or account info.
        if self.evidence_partition_sha256_samples.len() < 8 {
            self.evidence_partition_sha256_samples
                .push(sha256.to_owned());
        }
    }
    pub fn record_source_manifest(&mut self, sha256: &str) {
        if sha256.len() == 64
            && sha256.bytes().all(|c| c.is_ascii_hexdigit())
            && self.source_manifest_sha256_samples.len() < 8
        {
            self.source_manifest_sha256_samples.push(sha256.to_owned());
        }
    }
    pub fn blocked(mut self, category: &str) -> Self {
        self.publication_state = PublicationState::CorruptOrInconsistent;
        self.inspection_complete = false;
        self.manual_intervention_required = true;
        // Bounded reporting; error categories only, never free-form payloads.
        if self.errors.len() < 16 {
            self.errors.push(category.to_owned());
        }
        self
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unobserved_loss_is_unknown_not_zero() {
        let state = RecoverySummary::empty();
        assert_eq!(state.missing_or_orphaned_records, None);
        assert_eq!(state.identity_conflicts, None);
        assert_eq!(state.assurance, Assurance::Uncertain);
    }
    #[test]
    fn evidence_samples_and_error_categories_are_bounded() {
        let mut x = RecoverySummary::empty();
        for _ in 0..20 {
            x.record_evidence(&"a".repeat(64));
        }
        assert_eq!(x.evidence_partition_sha256_samples.len(), 8);
        let x = x.blocked("CORRUPT_MANIFEST");
        assert!(x.manual_intervention_required);
        assert_eq!(x.errors, vec!["CORRUPT_MANIFEST"]);
    }
    #[test]
    fn failure_points_have_explicit_source_evidence_states() {
        use FaultPoint as F;
        assert_eq!(
            F::DuringDataWrite.interrupted_state(),
            PublicationState::PreparedUncommitted
        );
        assert_eq!(
            F::AfterDataRename.interrupted_state(),
            PublicationState::DataSyncedUnsealed
        );
        assert_eq!(
            F::FailManifestPublish.interrupted_state(),
            PublicationState::DataSyncedUnsealed
        );
        assert_eq!(
            F::AfterManifestRename.interrupted_state(),
            PublicationState::SealedNotIndexed
        );
    }
    #[test]
    fn disk_and_permission_errors_classified_without_loss_claims() {
        let disk_full_code = if cfg!(windows) { 112 } else { 28 };
        assert_eq!(
            StorageFailure::from_io(&io::Error::from_raw_os_error(disk_full_code)),
            StorageFailure::DiskFull
        );
        assert_eq!(
            StorageFailure::from_io(&io::Error::new(io::ErrorKind::PermissionDenied, "no")),
            StorageFailure::PermissionDenied
        );
    }
}

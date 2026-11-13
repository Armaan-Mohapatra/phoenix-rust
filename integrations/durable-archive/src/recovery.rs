//! Explicit, non-executable recovery contract for offline research archives.
//! These classifications never imply physical power-loss durability.
use serde::{Deserialize,Serialize};
use std::io;

#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum PublicationState {
    PreparedUncommitted,
    DataSyncedUnsealed,
    SealedNotIndexed,
    FullyReconciled,
    CorruptOrInconsistent,
    DurabilityUncertain,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum StorageFailure {
    DiskFull,PermissionDenied,ReadOnlyFilesystem,InterruptedWrite,
    SyncFailed,RenameFailed,DisconnectedStorage,UnexpectedPath,
    DataCorruption,UnknownIo,
}
impl StorageFailure {
    pub fn from_io(e:&io::Error)->Self{
        match e.raw_os_error(){
            // POSIX ENOSPC and Win32 ERROR_DISK_FULL
            Some(28)|Some(112)=>return Self::DiskFull,
            // POSIX EROFS and Win32 ERROR_WRITE_PROTECT
            Some(30)|Some(19)=>return Self::ReadOnlyFilesystem,
            // Windows ERROR_NOT_READY / ERROR_DEVICE_NOT_CONNECTED;
            // classify on error code only, never infer actual data loss.
            Some(21)|Some(1167)=>return Self::DisconnectedStorage,
            _=>{}
        }
        match e.kind(){
            io::ErrorKind::PermissionDenied=>Self::PermissionDenied,
            io::ErrorKind::Interrupted|io::ErrorKind::WriteZero|
            io::ErrorKind::UnexpectedEof=>Self::InterruptedWrite,
            _=>Self::UnknownIo,
        }
    }
}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum FaultPoint {
    None,BeforeDataWrite,DuringDataWrite,AfterDataWriteBeforeSync,
    AfterDataSync,BeforeDataRename,AfterDataRename,
    DuringManifestWrite,AfterManifestWriteBeforeSync,
    AfterManifestSync,BeforeManifestRename,AfterManifestRename,
    DiskFullData,DiskFullManifest,FailDataSync,FailManifestSync,
    FailDataPublish,FailManifestPublish,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum Assurance {
    FileSyncedDirectoryNotProven,
    FileAndDirectorySyncedSoftwareLevel,
    Uncertain,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
pub struct RecoverySummary {
    pub schema_version:u32,
    pub publication_state:PublicationState,
    pub assurance:Assurance,
    pub sealed_partitions_found:usize,
    pub index_transactions_recovered:usize,
    pub already_indexed_partitions:usize,
    pub verified_event_references:usize,
    pub quarantined_partial_files:usize,
    pub quarantined_unsealed_data_files:usize,
    pub rejected_corrupt_partitions:Option<usize>,
    pub duplicate_event_identities:Option<usize>,
    pub identity_conflicts:Option<usize>,
    pub missing_or_orphaned_records:Option<usize>,
    pub manual_intervention_required:bool,
    pub evidence_partition_sha256_samples:Vec<String>,
    pub errors:Vec<String>,
}
impl RecoverySummary {
    pub fn empty()->Self{
        Self {
            schema_version:1,publication_state:PublicationState::PreparedUncommitted,
            assurance:Assurance::Uncertain,sealed_partitions_found:0,
            index_transactions_recovered:0,already_indexed_partitions:0,
            verified_event_references:0,quarantined_partial_files:0,
            quarantined_unsealed_data_files:0,rejected_corrupt_partitions:None,
            duplicate_event_identities:None,identity_conflicts:None,
            missing_or_orphaned_records:None,manual_intervention_required:false,
            evidence_partition_sha256_samples:Vec::new(),errors:Vec::new(),
        }
    }
    pub fn record_evidence(&mut self,sha256:&str){
        // SHA identifiers only, never source rows, credentials or account info.
        if self.evidence_partition_sha256_samples.len()<8{
            self.evidence_partition_sha256_samples.push(sha256.to_owned());
        }
    }
    pub fn blocked(mut self,category:&str)->Self{
        self.publication_state=PublicationState::CorruptOrInconsistent;
        self.manual_intervention_required=true;
        // Bounded reporting; error categories only, never free-form payloads.
        if self.errors.len()<16{self.errors.push(category.to_owned());}
        self
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]fn unobserved_loss_is_unknown_not_zero(){
        let state=RecoverySummary::empty();
        assert_eq!(state.missing_or_orphaned_records,None);
        assert_eq!(state.identity_conflicts,None);
        assert_eq!(state.assurance,Assurance::Uncertain);
    }
    #[test]fn evidence_samples_and_error_categories_are_bounded(){
        let mut x=RecoverySummary::empty();
        for _ in 0..20{x.record_evidence(&"a".repeat(64));}
        assert_eq!(x.evidence_partition_sha256_samples.len(),8);
        let x=x.blocked("CORRUPT_MANIFEST");
        assert!(x.manual_intervention_required);
        assert_eq!(x.errors,vec!["CORRUPT_MANIFEST"]);
    }
    #[test]fn disk_and_permission_errors_classified_without_loss_claims(){
        assert_eq!(StorageFailure::from_io(&io::Error::from_raw_os_error(28)),
            StorageFailure::DiskFull);
        assert_eq!(StorageFailure::from_io(&io::Error::new(
            io::ErrorKind::PermissionDenied,"no")),
            StorageFailure::PermissionDenied);
    }
}

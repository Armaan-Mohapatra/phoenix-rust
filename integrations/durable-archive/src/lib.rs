//! Crash-recoverable, bounded research-only partition publisher.
//! The caller supplies finalized data; this library never touches live feeds.
//! A committed manifest is the publication marker; orphan/temp bytes are
//! quarantined, never silently promoted or deleted.
use fs2::FileExt;
use phoenix_parquet_batch::{encode_finalized,verify_finalized,Manifest as ParquetManifest,Row};
use serde::{Deserialize,Serialize};
use sha2::{Digest,Sha256};
use std::collections::{BTreeMap,HashMap};
use std::fs::{self,File,OpenOptions};
use std::io::{Read,Write};
use std::path::{Path,PathBuf};
pub mod recovery;
use recovery::{Assurance,PublicationState,RecoverySummary,StorageFailure};

pub const MAX_ROWS_PER_PARTITION:usize=256;
pub const MAX_PARTITIONS:usize=2_000; // legacy RAM mode
pub const MAX_EXTERNAL_PARTITIONS:usize=100_000; // configurable bounded disk mode
pub const MAX_INDEXED_EVENTS:usize=250_000;
pub const MAX_PARTITION_BYTES:u64=8*1024*1024;
pub const MAX_MANIFEST_BYTES:u64=1*1024*1024;
const MARKER:&str="PHOENIX_RESEARCH_OUTPUT_ONLY";
const FORMAT:u32=1;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error), Json(serde_json::Error), Parquet(String),
    NotDedicatedResearchPath, Busy, InvalidInput, DuplicateConflict(String),
    CapacityExceeded, InvalidManifest, MissingCommittedPartition(String),
    DigestMismatch(String), ExistingUncommittedPartition,
    ExternalIndex(String), UnsafePath, InjectedStorage(StorageFailure),
    PublicationUncertain, InjectedFault,
}
impl From<std::io::Error> for Error {fn from(e:std::io::Error)->Self{Self::Io(e)}}
impl From<serde_json::Error> for Error {fn from(e:serde_json::Error)->Self{Self::Json(e)}}

#[derive(Clone,Debug)]
pub struct EventRow {
    /// Stable source-level event key; never a row index or a receive timestamp.
    pub event_id:String,
    pub data:Row,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct SourceScope {
    pub feed:String,pub session:String,pub market:String,
    /// A caller assertion, not certified archival continuity.
    pub observed_sequence_gaps:u64,
    pub quality_flags:Vec<String>,
    pub source_manifest_sha256:Option<String>,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct EventIdentity {pub id:String,pub row_sha256:String}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct SealedPartition {
    pub format_version:u32,pub status:String,pub partition_id:String,
    pub parquet_sha256:String,pub rows:usize,pub exchange_min_ns:i64,
    pub exchange_max_ns:i64,pub received_min_ns:i64,pub received_max_ns:i64,
    pub min_sequence:u64,pub max_sequence:u64,
    pub scope:SourceScope,pub events:Vec<EventIdentity>,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
pub enum Durability {
    /// File contents synced; directory metadata flush is not portable on Windows.
    FileSyncedDirectoryNotProven,
    FileAndDirectorySynced,
}
#[derive(Debug)]
pub struct PublishResult {
    pub partition_id:Option<String>,
    pub added:usize,pub duplicates:usize,pub durability:Durability,
}
#[derive(Debug,Default,Clone,Copy,PartialEq,Eq)]
pub struct RecoveryReport {
    pub quarantined_temp_files:usize,
    pub quarantined_orphan_partitions:usize,
    pub committed_partitions:usize,
}
pub use recovery::FaultPoint as Fault;

pub struct Archive {
    root:PathBuf,
    _writer_lock:File,
    ids:HashMap<String,String>,
    // When true, the paired transactional disk index is the identity authority.
    // The publisher must receive an explicit lookup callback per append.
    external_index_mode:bool,
    manifests:BTreeMap<String,SealedPartition>, // legacy only
    external_partition_count:usize,
    external_partition_limit:usize,
    recovery:RecoveryReport,
}
fn hash(data:&[u8])->String {format!("{:x}",Sha256::digest(data))}
fn is_digest(s:&str)->bool {
    s.len()==64&&s.bytes().all(|x|x.is_ascii_hexdigit())
}
fn valid_id(s:&str)->bool {
    !s.is_empty()&&s.len()<=128&&s.bytes().all(|b|
        b.is_ascii_alphanumeric()||matches!(b,b':'|b'.'|b'-'|b'_'|b'/'))
}
fn sha_row(r:&Row)->String {
    let mut h=Sha256::new();
    for v in [&r.feed,&r.session,&r.market] {
        h.update((v.len() as u64).to_le_bytes());h.update(v.as_bytes());
    }
    h.update(r.exchange_ns.to_le_bytes());h.update(r.received_ns.to_le_bytes());
    h.update(r.sequence.to_le_bytes());h.update([r.kind]);
    h.update((r.payload.len() as u64).to_le_bytes());h.update(&r.payload);
    format!("{:x}",h.finalize())
}
fn manifest_path(root:&Path,id:&str)->PathBuf {
    root.join("manifests").join(format!("{id}.json"))
}
fn data_path(root:&Path,id:&str)->PathBuf {
    root.join("partitions").join(format!("{id}.parquet"))
}
#[cfg(unix)]
fn sync_dir(path:&Path)->Result<bool,Error> {
    File::open(path)?.sync_all()?;Ok(true)
}
#[cfg(not(unix))]
fn sync_dir(_path:&Path)->Result<bool,Error> {
    // Portable std::fs cannot promise metadata durability on every Windows FS.
    Ok(false)
}
fn safe_read(path:&Path,cap:u64)->Result<Vec<u8>,Error>{
    reject_reparse(path)?;
    if !fs::symlink_metadata(path)?.is_file(){return Err(Error::UnsafePath);}
    if fs::metadata(path)?.len()>cap{return Err(Error::CapacityExceeded);}
    let mut data=Vec::new();
    File::open(path)?.take(cap+1).read_to_end(&mut data)?;
    if data.len() as u64>cap{return Err(Error::CapacityExceeded);}
    Ok(data)
}
fn temp_file(path:&Path,bytes:&[u8])->Result<(),Error> {
    reject_reparse(path)?;
    let mut f=OpenOptions::new().create_new(true).write(true).open(path)?;
    f.write_all(bytes)?;f.sync_all()?;Ok(())
}
fn quarantine(root:&Path,p:&Path)->Result<(),Error>{
    reject_reparse(root)?;reject_reparse(p)?;
    let name=p.file_name().ok_or(Error::InvalidInput)?;
    let target=root.join("quarantine").join(name);
    if target.exists(){return Err(Error::InvalidInput);}
    fs::rename(p,&target)?;
    sync_dir(&root.join("quarantine"))?;Ok(())
}

fn read_sealed_manifest(root:&Path,path:&Path)->Result<SealedPartition,Error>{
    if !path.is_file(){return Err(Error::InvalidManifest);}
    let m:SealedPartition=serde_json::from_slice(&safe_read(path,MAX_MANIFEST_BYTES)?)?;
    if m.format_version!=FORMAT||m.status!="sealed"||
        !is_digest(&m.partition_id)||m.parquet_sha256!=m.partition_id||
        m.events.len()!=m.rows||m.rows==0||m.rows>MAX_ROWS_PER_PARTITION||
        path.file_name().and_then(|s|s.to_str())!=
            Some(format!("{}.json",m.partition_id).as_str()){
        return Err(Error::InvalidManifest);
    }
    if !data_path(root,&m.partition_id).is_file(){
        return Err(Error::MissingCommittedPartition(m.partition_id));
    }
    let mut ids=std::collections::HashSet::new();
    for e in &m.events{
        if !valid_id(&e.id)||!is_digest(&e.row_sha256)||!ids.insert(e.id.as_str()){
            return Err(Error::InvalidManifest);
        }
    }
    Ok(m)
}


fn reject_reparse(path:&Path)->Result<(),Error>{
    if path.components().any(|p|matches!(p,std::path::Component::ParentDir)){
        return Err(Error::UnsafePath);
    }
    for component in path.ancestors(){
        match fs::symlink_metadata(component){
            Ok(meta)=>{
                if meta.file_type().is_symlink(){return Err(Error::UnsafePath);}
                #[cfg(windows)]
                {
                    use std::os::windows::fs::MetadataExt;
                    // FILE_ATTRIBUTE_REPARSE_POINT; also rejects junctions.
                    if meta.file_attributes()&0x400!=0{return Err(Error::UnsafePath);}
                }
            },
            Err(e) if e.kind()==std::io::ErrorKind::NotFound=>{},
            Err(e)=>return Err(Error::Io(e)),
        }
    }
    Ok(())
}
fn research_subdir(path:&Path)->Result<(),Error>{
    reject_reparse(path)?;
    if path.exists()&&!fs::symlink_metadata(path)?.is_dir(){
        return Err(Error::UnsafePath);
    }
    Ok(())
}
fn sealed_link_publish(from:&Path,to:&Path)->Result<bool,Error>{
    reject_reparse(from)?;reject_reparse(to)?;
    if fs::symlink_metadata(to).is_ok(){
        return Err(Error::ExistingUncommittedPartition);
    }
    // A hard-link creates the destination atomically *without overwriting*
    // an existing file on same-volume NTFS/ext4. Unsupported filesystems
    // fail CLOSED; never silently fall back to a copy/delete rename.
    fs::hard_link(from,to)?;
    fs::remove_file(from)?;
    sync_dir(to.parent().ok_or(Error::InvalidInput)?)
}
fn staged_write(path:&Path,bytes:&[u8],during:bool,pre_sync:bool,
    fail_sync:bool,fail_full:bool)->Result<(),Error>{
    reject_reparse(path)?;
    let mut file=OpenOptions::new().create_new(true).write(true).open(path)?;
    if fail_full{return Err(Error::InjectedStorage(StorageFailure::DiskFull));}
    if during{
        let take=(bytes.len()+1)/2;
        file.write_all(&bytes[..take])?;
        return Err(Error::InjectedStorage(StorageFailure::InterruptedWrite));
    }
    file.write_all(bytes)?;
    if pre_sync{return Err(Error::InjectedFault);}
    if fail_sync{return Err(Error::InjectedStorage(StorageFailure::SyncFailed));}
    file.sync_all()?;Ok(())
}

impl Archive {
    /// A new root is allowed; an existing root MUST contain our research marker.
    /// A single exclusive OS advisory lock prevents concurrent publishers and
    /// is released automatically after a crash.
    pub fn open(root:impl AsRef<Path>)->Result<Self,Error>{
        Self::open_with_mode(root,false,MAX_PARTITIONS)
    }
    /// Dedicated research mode for an external, transactional event-ID index.
    /// It never allocates the complete historical ID map in Rust heap memory.
    /// Callers MUST use publish_with_external_lookup, never plain publish.
    pub fn open_external_index(root:impl AsRef<Path>)->Result<Self,Error>{
        Self::open_external_index_with_limit(root,MAX_EXTERNAL_PARTITIONS)
    }
    pub fn open_external_index_with_limit(root:impl AsRef<Path>,cap:usize)
        ->Result<Self,Error>{
        Self::open_with_mode(root,true,cap)
    }
    fn open_with_mode(root:impl AsRef<Path>,external_index_mode:bool,cap:usize)
        ->Result<Self,Error>{
        if cap==0||cap>MAX_EXTERNAL_PARTITIONS{return Err(Error::CapacityExceeded);}
        let root=root.as_ref();
        reject_reparse(root)?;
        if root.exists(){
            reject_reparse(&root.join(MARKER))?;
            if fs::read(root.join(MARKER))?!=b"Phoenix Rust research-only archive v1\n" {
                return Err(Error::NotDedicatedResearchPath);
            }
        }else{
            fs::create_dir(root)?;
            temp_file(&root.join(MARKER),b"Phoenix Rust research-only archive v1\n")?;
            sync_dir(root)?;
        }
        let root=fs::canonicalize(root)?;
        reject_reparse(&root.join(".writer-lock"))?;
        let f=OpenOptions::new().create(true).read(true).write(true)
            .open(root.join(".writer-lock"))?;
        f.try_lock_exclusive().map_err(|_|Error::Busy)?;
        for sub in ["manifests","partitions","pending","quarantine"] {
            research_subdir(&root.join(sub))?;
            fs::create_dir_all(root.join(sub))?;
        }
        let mut archive=Self {root,_writer_lock:f,ids:HashMap::new(),
            external_index_mode,manifests:BTreeMap::new(),
            external_partition_count:0,external_partition_limit:cap,
            recovery:RecoveryReport::default()};
        archive.load_index()?;
        archive.recover_uncommitted()?;
        Ok(archive)
    }
    pub fn recovery(&self)->RecoveryReport {self.recovery}
    /// Machine-readable evidence status. The archive itself does not certify
    /// external redb reconciliation; the wrapper upgrades this state only
    /// after successful index commit and sealed-row verification.
    pub fn recovery_summary(&self)->RecoverySummary{
        let mut r=RecoverySummary::empty();
        r.sealed_partitions_found=self.committed_partitions();
        r.quarantined_partial_files=self.recovery.quarantined_temp_files;
        r.quarantined_unsealed_data_files=self.recovery.quarantined_orphan_partitions;
        r.publication_state=if self.external_index_mode{
            PublicationState::SealedNotIndexed
        }else{PublicationState::FullyReconciled};
        r.assurance=if cfg!(windows){Assurance::FileSyncedDirectoryNotProven}
            else{Assurance::FileAndDirectorySyncedSoftwareLevel};
        r
    }
    pub fn indexed_events(&self)->usize {self.ids.len()}
    pub fn committed_partitions(&self)->usize {
        if self.external_index_mode{self.external_partition_count}
        else{self.manifests.len()}
    }
    fn load_index(&mut self)->Result<(),Error>{
        let mut n=0usize;
        for item in fs::read_dir(self.root.join("manifests"))?{
            n=n.checked_add(1).ok_or(Error::CapacityExceeded)?;
            if n>self.external_partition_limit{return Err(Error::CapacityExceeded);}
            let item=item?;
            reject_reparse(&item.path())?;
            if !item.file_type()?.is_file(){return Err(Error::InvalidManifest);}
            let m=read_sealed_manifest(&self.root,&item.path())?;
            if !self.external_index_mode{
                if self.ids.len()+m.events.len()>MAX_INDEXED_EVENTS{
                    return Err(Error::CapacityExceeded);
                }
                for e in &m.events{
                    if self.ids.insert(e.id.clone(),e.row_sha256.clone()).is_some(){
                        return Err(Error::InvalidManifest);
                    }
                }
                self.manifests.insert(m.partition_id.clone(),m);
            }else{
                // Only the scalar partition count remains in memory.
                self.external_partition_count=n;
            }
        }
        self.recovery.committed_partitions=self.committed_partitions();
        Ok(())
    }
    fn recover_uncommitted(&mut self)->Result<(),Error>{
        let mut seen=0;
        for e in fs::read_dir(self.root.join("pending"))?{
            seen+=1;if seen>self.external_partition_limit{return Err(Error::CapacityExceeded);}
            let e=e?;reject_reparse(&e.path())?;
            if !e.file_type()?.is_file(){return Err(Error::InvalidInput);}
            quarantine(&self.root,&e.path())?;
            self.recovery.quarantined_temp_files+=1;
        }
        seen=0;
        for e in fs::read_dir(self.root.join("partitions"))?{
            seen+=1;if seen>self.external_partition_limit{return Err(Error::CapacityExceeded);}
            let e=e?;reject_reparse(&e.path())?;
            if !e.file_type()?.is_file(){return Err(Error::InvalidInput);}
            let stem=e.path().file_stem().and_then(|s|s.to_str())
                .ok_or(Error::InvalidInput)?.to_owned();
            if !(if self.external_index_mode{
                manifest_path(&self.root,&stem).is_file()
            }else{self.manifests.contains_key(&stem)}){
                quarantine(&self.root,&e.path())?;
                self.recovery.quarantined_orphan_partitions+=1;
            }
        }
        sync_dir(&self.root.join("partitions"))?;
        sync_dir(&self.root.join("pending"))?;
        Ok(())
    }
    pub fn publish(&mut self,scope:SourceScope,events:&[EventRow])
        ->Result<PublishResult,Error>{
        if self.external_index_mode{return Err(Error::InvalidInput);}
        self.publish_inner(scope,events,Fault::None,None)
    }

    /// Only for a publisher paired with a committed, recovered disk identity
    /// index. The callback must return authoritative event-ID -> row-digest
    /// mappings and must fail closed if the index cannot be queried.
    pub fn publish_with_external_lookup<F>(
        &mut self,scope:SourceScope,events:&[EventRow],lookup:F
    )->Result<PublishResult,Error>
    where F:FnMut(&str)->Result<Option<String>,Error>{
        self.publish_with_external_lookup_fault(scope,events,lookup,Fault::None)
    }
    /// Fault injection is isolated to the research publisher; no retries.
    /// A fault after manifest seal may have committed source data; reopen and
    /// reconcile before any subsequent publication.
    pub fn publish_with_external_lookup_fault<F>(
        &mut self,scope:SourceScope,events:&[EventRow],mut lookup:F,fault:Fault
    )->Result<PublishResult,Error>
    where F:FnMut(&str)->Result<Option<String>,Error>{
        if !self.external_index_mode{return Err(Error::InvalidInput);}
        self.publish_inner(scope,events,fault,Some(&mut lookup))
    }
    fn publish_inner(
        &mut self,scope:SourceScope,events:&[EventRow],fault:Fault,
        mut lookup:Option<&mut dyn FnMut(&str)->Result<Option<String>,Error>>
    )->Result<PublishResult,Error>{
        if events.is_empty()||events.len()>MAX_ROWS_PER_PARTITION||
            scope.feed.is_empty()||scope.session.is_empty()||
            scope.market.is_empty()||scope.quality_flags.len()>32 ||
            scope.quality_flags.iter().any(|s|s.len()>128)||
            scope.source_manifest_sha256.as_deref().is_some_and(|s|!is_digest(s)){
            return Err(Error::InvalidInput);
        }
        let mut fresh:Vec<Row>=Vec::new();
        let mut refs:Vec<EventIdentity>=Vec::new();
        let mut seen=HashMap::<String,String>::new();
        let mut dup=0;
        for e in events {
            if !valid_id(&e.event_id)||e.data.feed!=scope.feed||
                e.data.session!=scope.session||e.data.market!=scope.market||
                e.data.kind==0||e.data.exchange_ns>e.data.received_ns{
                return Err(Error::InvalidInput);
            }
            let digest=sha_row(&e.data);
            let previous=if let Some(v)=seen.get(&e.event_id){
                Some(v.clone())
            }else if self.external_index_mode{
                lookup.as_mut().ok_or(Error::InvalidInput)?(&e.event_id)?
            }else{
                self.ids.get(&e.event_id).cloned()
            };
            if let Some(prev)=previous{
                if prev!=digest{return Err(Error::DuplicateConflict(e.event_id.clone()));}
                dup+=1;continue;
            }
            seen.insert(e.event_id.clone(),digest.clone());
            refs.push(EventIdentity{id:e.event_id.clone(),row_sha256:digest});
            fresh.push(e.data.clone());
        }
        if fresh.is_empty(){return Ok(PublishResult{
            partition_id:None,added:0,duplicates:dup,
            durability:Durability::FileSyncedDirectoryNotProven});}
        if self.committed_partitions()>=self.external_partition_limit ||
            (!self.external_index_mode &&
                self.ids.len()+fresh.len()>MAX_INDEXED_EVENTS){
            return Err(Error::CapacityExceeded);
        }
        let (bytes,pmeta)=encode_finalized(&fresh)
            .map_err(|x|Error::Parquet(format!("{x:?}")))?;
        if bytes.len() as u64>MAX_PARTITION_BYTES{return Err(Error::CapacityExceeded);}
        let id=pmeta.sha256.clone();
        if data_path(&self.root,&id).exists()||manifest_path(&self.root,&id).exists(){
            return Err(Error::ExistingUncommittedPartition);
        }
        let m=SealedPartition{
            format_version:FORMAT,status:"sealed".into(),partition_id:id.clone(),
            parquet_sha256:id.clone(),rows:fresh.len(),
            exchange_min_ns:fresh.iter().map(|r|r.exchange_ns).min().unwrap(),
            exchange_max_ns:fresh.iter().map(|r|r.exchange_ns).max().unwrap(),
            received_min_ns:pmeta.received_min_ns,
            received_max_ns:pmeta.received_max_ns,
            min_sequence:fresh.iter().map(|r|r.sequence).min().unwrap(),
            max_sequence:fresh.iter().map(|r|r.sequence).max().unwrap(),
            scope,events:refs,
        };
        let json=serde_json::to_vec_pretty(&m)?;
        if json.len() as u64>MAX_MANIFEST_BYTES{return Err(Error::CapacityExceeded);}
        if matches!(fault,Fault::BeforeDataWrite){return Err(Error::InjectedFault);}
        let pending_data=self.root.join("pending").join(format!("{id}.parquet.tmp"));
        staged_write(&pending_data,&bytes,
            matches!(fault,Fault::DuringDataWrite),
            matches!(fault,Fault::AfterDataWriteBeforeSync),
            matches!(fault,Fault::FailDataSync),
            matches!(fault,Fault::DiskFullData))?;
        if matches!(fault,Fault::AfterDataSync|Fault::BeforeDataRename){
            return Err(Error::InjectedFault);
        }
        if matches!(fault,Fault::FailDataPublish){
            return Err(Error::InjectedStorage(StorageFailure::RenameFailed));
        }
        let durable_data_dir=sealed_link_publish(&pending_data,
            &data_path(&self.root,&id))?;
        if matches!(fault,Fault::AfterDataRename){return Err(Error::InjectedFault);}
        let pending_manifest=self.root.join("pending").join(format!("{id}.json.tmp"));
        staged_write(&pending_manifest,&json,
            matches!(fault,Fault::DuringManifestWrite),
            matches!(fault,Fault::AfterManifestWriteBeforeSync),
            matches!(fault,Fault::FailManifestSync),
            matches!(fault,Fault::DiskFullManifest))?;
        if matches!(fault,Fault::AfterManifestSync|Fault::BeforeManifestRename){
            return Err(Error::InjectedFault);
        }
        if matches!(fault,Fault::FailManifestPublish){
            return Err(Error::InjectedStorage(StorageFailure::RenameFailed));
        }
        let durable_manifest_dir=sealed_link_publish(&pending_manifest,
            &manifest_path(&self.root,&id))?;
        if matches!(fault,Fault::AfterManifestRename){
            return Err(Error::PublicationUncertain);
        }
        if !self.external_index_mode{
            for r in &m.events {self.ids.insert(r.id.clone(),r.row_sha256.clone());}
        }
        if self.external_index_mode{
            self.external_partition_count+=1;
        }else{
            self.manifests.insert(id.clone(),m);
        }
        Ok(PublishResult{partition_id:Some(id),added:fresh.len(),duplicates:dup,
            durability:if durable_data_dir&&durable_manifest_dir{
                Durability::FileAndDirectorySynced
            }else{Durability::FileSyncedDirectoryNotProven}})
    }
    /// Reads only bounded known sealed partitions; no drive scan.
    pub fn verify_all(&self)->Result<usize,Error>{
        self.visit_verified_rows(|_,_,_|Ok(()))?;
        Ok(self.committed_partitions())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    fn scope()->SourceScope {
        SourceScope{feed:"kalshi".into(),session:"session-a".into(),
            market:"KXBTC".into(),observed_sequence_gaps:0,
            quality_flags:vec![],source_manifest_sha256:None}
    }
    fn r(id:&str,seq:u64)->EventRow{
        EventRow{event_id:id.into(),data:Row{
            feed:"kalshi".into(),session:"session-a".into(),market:"KXBTC".into(),
            exchange_ns:seq as i64,received_ns:seq as i64+2,
            sequence:seq,kind:1,payload:vec![seq as u8,3,4]}}
    }
    fn root(t:&TempDir)->PathBuf{t.path().join("research-output")}
    #[test]fn publish_verify_reopen_idempotent(){
        let t=TempDir::new().unwrap();
        let mut a=Archive::open(root(&t)).unwrap();
        let outcome=a.publish(scope(),&[r("a",1),r("b",2)]).unwrap();
        assert_eq!((outcome.added,outcome.duplicates),(2,0));
        assert_eq!(a.verify_all().unwrap(),1);
        drop(a);
        let mut b=Archive::open(root(&t)).unwrap();
        assert_eq!((b.indexed_events(),b.committed_partitions()),(2,1));
        assert_eq!(b.publish(scope(),&[r("a",1),r("b",2)]).unwrap().duplicates,2);
        let mixed=b.publish(scope(),&[r("b",2),r("c",3)]).unwrap();
        assert_eq!((mixed.added,mixed.duplicates),(1,1));
        assert_eq!(b.verify_all().unwrap(),2);
    }
    #[test]fn conflicting_id_is_never_overwritten(){
        let t=TempDir::new().unwrap();
        let mut a=Archive::open(root(&t)).unwrap();
        a.publish(scope(),&[r("a",1)]).unwrap();
        assert!(matches!(a.publish(scope(),&[r("a",9)]),
            Err(Error::DuplicateConflict(_))));
    }
    #[test]fn simulated_crash_after_data_rename_quarantines_orphan(){
        let t=TempDir::new().unwrap();
        let mut a=Archive::open(root(&t)).unwrap();
        assert!(matches!(a.publish_inner(scope(),&[r("a",1)],
            Fault::AfterDataRename,None),Err(Error::InjectedFault)));
        drop(a);
        let mut b=Archive::open(root(&t)).unwrap();
        assert_eq!(b.recovery().quarantined_orphan_partitions,1);
        assert_eq!(b.committed_partitions(),0);
        b.publish(scope(),&[r("a",1)]).unwrap();
        assert_eq!(b.verify_all().unwrap(),1);
    }
    #[test]fn simulated_crash_mid_manifest_is_recoverable(){
        let t=TempDir::new().unwrap();
        let mut a=Archive::open(root(&t)).unwrap();
        assert!(matches!(a.publish_inner(scope(),&[r("a",1)],
            Fault::AfterManifestSync,None),Err(Error::InjectedFault)));
        drop(a);
        let mut b=Archive::open(root(&t)).unwrap();
        assert_eq!(b.recovery().quarantined_orphan_partitions,1);
        assert_eq!(b.recovery().quarantined_temp_files,1);
        b.publish(scope(),&[r("a",1)]).unwrap();
        assert_eq!(b.verify_all().unwrap(),1);
    }
    #[test]fn write_failure_does_not_publish_manifest(){
        let t=TempDir::new().unwrap();
        let mut a=Archive::open(root(&t)).unwrap();
        assert!(matches!(a.publish_inner(scope(),&[r("a",1)],
            Fault::BeforeDataWrite,None),Err(Error::InjectedFault)));
        assert_eq!(a.committed_partitions(),0);
    }
    #[test]fn corrupt_parquet_fails_verification(){
        let t=TempDir::new().unwrap();
        let mut a=Archive::open(root(&t)).unwrap();
        let id=a.publish(scope(),&[r("a",1)]).unwrap().partition_id.unwrap();
        let p=data_path(&root(&t),&id);
        let mut data=fs::read(&p).unwrap();data[20]^=1;fs::write(&p,data).unwrap();
        assert!(matches!(a.verify_all(),Err(Error::DigestMismatch(_))));
    }
    #[test]fn existing_unmarked_directory_is_rejected(){
        let t=TempDir::new().unwrap();
        assert!(matches!(Archive::open(t.path()),
            Err(Error::NotDedicatedResearchPath)));
    }
    #[test]fn external_mode_cannot_publish_without_authoritative_lookup(){
        let t=TempDir::new().unwrap();
        let mut a=Archive::open_external_index(root(&t)).unwrap();
        assert_eq!(a.indexed_events(),0);
        assert!(matches!(a.publish(scope(),&[r("a",1)]),Err(Error::InvalidInput)));
        let first=a.publish_with_external_lookup(scope(),&[r("a",1)],|_|Ok(None)).unwrap();
        assert_eq!(first.added,1);
        assert_eq!(a.indexed_events(),0);
        drop(a);
        let a=Archive::open_external_index(root(&t)).unwrap();
        assert_eq!(a.committed_partitions(),1);
        assert_eq!(a.indexed_events(),0);
    }
    #[test]fn simultaneous_publisher_is_rejected(){
        let t=TempDir::new().unwrap();
        let _a=Archive::open(root(&t)).unwrap();
        assert!(matches!(Archive::open(root(&t)),Err(Error::Busy)));
    }
    #[test]fn partial_frame_remains_unsealed_and_quarantined(){
        let t=TempDir::new().unwrap();
        let a=Archive::open(root(&t)).unwrap();drop(a);
        fs::write(root(&t).join("pending").join("partial.parquet.tmp"),b"partial").unwrap();
        let b=Archive::open(root(&t)).unwrap();
        assert_eq!(b.recovery().quarantined_temp_files,1);
        assert_eq!(b.committed_partitions(),0);
    }
}



impl Archive {
    // A single-partition reader is needed by the disk-backed wrapper so that
    // normal append operations do not reread every previously sealed partition.
    // The complete readback remains available for cold-start reconciliation.
    fn visit_one_verified<F>(
        &self, manifest:&SealedPartition, callback:&mut F
    )->Result<(),Error>
    where F:FnMut(&SealedPartition,&EventIdentity,&Row)->Result<(),Error>{
        let bytes=safe_read(
            &data_path(&self.root,&manifest.partition_id),MAX_PARTITION_BYTES)?;
        if hash(&bytes)!=manifest.parquet_sha256{
            return Err(Error::DigestMismatch(manifest.partition_id.clone()));
        }
        let meta=ParquetManifest{
            rows:manifest.rows,sha256:manifest.parquet_sha256.clone(),
            received_min_ns:manifest.received_min_ns,
            received_max_ns:manifest.received_max_ns,format:"parquet_zstd_v1",
        };
        let decoded=phoenix_parquet_batch::decode_finalized(&bytes,&meta)
            .map_err(|e|Error::Parquet(format!("{e:?}")))?;
        if decoded.len()!=manifest.events.len(){return Err(Error::InvalidManifest);}
        for (identity,row) in manifest.events.iter().zip(decoded.iter()){
            if identity.row_sha256!=sha_row(row){
                return Err(Error::DigestMismatch(identity.id.clone()));
            }
            callback(manifest,identity,row)?;
        }
        Ok(())
    }

    /// Verify and visit exactly one *already sealed* partition by its digest.
    /// Unknown or unsealed partitions must not be silently admitted.
    pub fn visit_verified_partition<F>(
        &self,partition_id:&str,mut callback:F
    )->Result<(),Error>
    where F:FnMut(&SealedPartition,&EventIdentity,&Row)->Result<(),Error>{
        if self.external_index_mode{
            if !is_digest(partition_id){return Err(Error::InvalidManifest);}
            let path=manifest_path(&self.root,partition_id);
            if !path.is_file(){return Err(Error::MissingCommittedPartition(partition_id.into()));}
            let manifest=read_sealed_manifest(&self.root,&path)?;
            self.visit_one_verified(&manifest,&mut callback)
        }else{
            let manifest=self.manifests.get(partition_id).ok_or_else(||
                Error::MissingCommittedPartition(partition_id.to_owned()))?;
            self.visit_one_verified(manifest,&mut callback)
        }
    }

    /// Reads only previously sealed and checksum-verified rows. The callback
    /// receives one row at a time; partitions remain strictly bounded.
    pub fn visit_verified_rows<F>(&self,mut callback:F)->Result<(),Error>
    where F:FnMut(&SealedPartition,&EventIdentity,&Row)->Result<(),Error>{
        if self.external_index_mode{
            let mut count=0usize;
            for entry in fs::read_dir(self.root.join("manifests"))?{
                let entry=entry?;
                if !entry.file_type()?.is_file(){return Err(Error::InvalidManifest);}
                count+=1;
                if count>self.external_partition_limit{return Err(Error::CapacityExceeded);}
                let manifest=read_sealed_manifest(&self.root,&entry.path())?;
                self.visit_one_verified(&manifest,&mut callback)?;
            }
            if count!=self.external_partition_count{return Err(Error::InvalidManifest);}
        }else{
            for manifest in self.manifests.values(){
                self.visit_one_verified(manifest,&mut callback)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod archived_row_tests{
    use super::*;
    use tempfile::TempDir;
    #[test]fn replays_committed_rows_in_digest_checked_order(){
        let t=TempDir::new().unwrap();let root=t.path().join("research");
        let mut a=Archive::open(&root).unwrap();
        let s=SourceScope{feed:"paper".into(),session:"s".into(),market:"P1".into(),
            observed_sequence_gaps:0,quality_flags:vec![],
            source_manifest_sha256:None};
        let payload=Row{feed:"paper".into(),session:"s".into(),market:"P1".into(),
            exchange_ns:10,received_ns:11,sequence:1,kind:50,payload:vec![0,22,250]};
        a.publish(s,&[EventRow{event_id:"P1:1".into(),data:payload}]).unwrap();
        let mut seen=Vec::new();
        a.visit_verified_rows(|manifest,id,row|{
            assert_eq!(manifest.rows,1);
            seen.push((id.id.clone(),row.sequence,row.payload.clone()));Ok(())
        }).unwrap();
        assert_eq!(seen,vec![("P1:1".into(),1,vec![0,22,250])]);
    }
}


#[cfg(test)]
mod external_manifest_paging_tests {
    use super::*;
    use tempfile::TempDir;
    fn scope()->SourceScope{
        SourceScope{feed:"kalshi".into(),session:"offline".into(),
            market:"BTC".into(),observed_sequence_gaps:0,quality_flags:vec![],
            source_manifest_sha256:None}
    }
    fn row(id:&str,n:u64)->EventRow{
        EventRow{event_id:id.into(),data:Row{
            feed:"kalshi".into(),session:"offline".into(),market:"BTC".into(),
            exchange_ns:n as i64,received_ns:n as i64+1,
            sequence:n,kind:1,payload:vec![n as u8,1]}}
    }
    #[test]
    fn disk_mode_does_not_retain_historical_event_identity_lists(){
        let t=TempDir::new().unwrap();let root=t.path().join("research");
        let mut a=Archive::open_external_index_with_limit(&root,8).unwrap();
        for i in 1..=7{
            a.publish_with_external_lookup(scope(),&[row(&format!("e{i}"),i)],
                |_|Ok(None)).unwrap();
        }
        assert_eq!(a.committed_partitions(),7);
        assert_eq!(a.manifests.len(),0);
        assert_eq!(a.ids.len(),0);
        assert_eq!(a.verify_all().unwrap(),7);
        drop(a);
        let b=Archive::open_external_index_with_limit(&root,8).unwrap();
        assert_eq!(b.committed_partitions(),7);
        assert!(b.manifests.is_empty());
        assert!(b.ids.is_empty());
        let mut count=0;
        b.visit_verified_rows(|_,_,_|{count+=1;Ok(())}).unwrap();
        assert_eq!(count,7);
    }
    #[test]
    fn external_partition_cap_is_explicit_and_reopen_respects_limit(){
        let t=TempDir::new().unwrap();let root=t.path().join("research");
        let mut a=Archive::open_external_index_with_limit(&root,2).unwrap();
        for n in 1..=2{
            a.publish_with_external_lookup(scope(),&[row(&format!("p{n}"),n)],
                |_|Ok(None)).unwrap();
        }
        assert!(matches!(a.publish_with_external_lookup(scope(),&[row("overflow",3)],
            |_|Ok(None)),Err(Error::CapacityExceeded)));
        drop(a);
        assert!(matches!(Archive::open_external_index_with_limit(&root,1),
            Err(Error::CapacityExceeded)));
    }
    #[test]
    fn existing_v1_sealed_archive_is_read_unchanged_in_disk_mode(){
        let t=TempDir::new().unwrap();let root=t.path().join("research");
        let mut legacy=Archive::open(&root).unwrap();
        let id=legacy.publish(scope(),&[row("legacy:1",1)])
            .unwrap().partition_id.unwrap();
        drop(legacy);
        let manifest=manifest_path(&root,&id);
        let original=fs::read(&manifest).unwrap();
        // Storage v1 remains readable without rewriting its manifests.
        let external=Archive::open_external_index(&root).unwrap();
        assert_eq!(external.committed_partitions(),1);
        assert_eq!(external.indexed_events(),0);
        assert_eq!(external.verify_all().unwrap(),1);
        assert_eq!(fs::read(&manifest).unwrap(),original);
    }
    #[test]
    fn invalid_external_cap_cannot_initialize_research_output(){
        let t=TempDir::new().unwrap();let root=t.path().join("new");
        assert!(matches!(Archive::open_external_index_with_limit(&root,0),
            Err(Error::CapacityExceeded)));
        assert!(!root.exists());
        assert!(matches!(Archive::open_external_index_with_limit(
            &root,MAX_EXTERNAL_PARTITIONS+1),Err(Error::CapacityExceeded)));
        assert!(!root.exists());
    }
    #[test]
    fn lazy_manifest_validation_detects_tampering(){
        let t=TempDir::new().unwrap();let root=t.path().join("research");
        let mut a=Archive::open_external_index(&root).unwrap();
        let id=a.publish_with_external_lookup(scope(),&[row("only",5)],
            |_|Ok(None)).unwrap().partition_id.unwrap();
        drop(a);
        let b=Archive::open_external_index(&root).unwrap();
        let mut count=0;
        b.visit_verified_partition(&id,|m,e,r|{
            assert_eq!(m.partition_id,id);
            assert_eq!(e.id,"only");
            assert_eq!(r.sequence,5);
            count+=1;Ok(())
        }).unwrap();
        assert_eq!(count,1);
        let path=manifest_path(&root,&id);
        let mut m:SealedPartition=serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        m.events[0].row_sha256="f".repeat(64);
        fs::write(path,serde_json::to_vec(&m).unwrap()).unwrap();
        assert!(matches!(b.verify_all(),Err(Error::DigestMismatch(_))));
    }
}

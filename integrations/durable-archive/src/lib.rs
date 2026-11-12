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

pub const MAX_ROWS_PER_PARTITION:usize=256;
pub const MAX_PARTITIONS:usize=2_000;
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
    InjectedFault,
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
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
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
#[derive(Debug,Clone,Copy)]
enum Fault {None,BeforeDataWrite,AfterDataSync,AfterDataRename,AfterManifestSync}

pub struct Archive {
    root:PathBuf,
    _writer_lock:File,
    ids:HashMap<String,String>,
    manifests:BTreeMap<String,SealedPartition>,
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
    if fs::metadata(path)?.len()>cap{return Err(Error::CapacityExceeded);}
    let mut data=Vec::new();
    File::open(path)?.take(cap+1).read_to_end(&mut data)?;
    if data.len() as u64>cap{return Err(Error::CapacityExceeded);}
    Ok(data)
}
fn temp_file(path:&Path,bytes:&[u8])->Result<(),Error> {
    let mut f=OpenOptions::new().create_new(true).write(true).open(path)?;
    f.write_all(bytes)?;f.sync_all()?;Ok(())
}
fn quarantine(root:&Path,p:&Path)->Result<(),Error>{
    let name=p.file_name().ok_or(Error::InvalidInput)?;
    let target=root.join("quarantine").join(name);
    if target.exists(){return Err(Error::InvalidInput);}
    fs::rename(p,&target)?;
    sync_dir(&root.join("quarantine"))?;Ok(())
}
impl Archive {
    /// A new root is allowed; an existing root MUST contain our research marker.
    /// A single exclusive OS advisory lock prevents concurrent publishers and
    /// is released automatically after a crash.
    pub fn open(root:impl AsRef<Path>)->Result<Self,Error>{
        let root=root.as_ref();
        if root.exists(){
            if !root.join(MARKER).is_file(){return Err(Error::NotDedicatedResearchPath);}
        }else{
            fs::create_dir(root)?;
            temp_file(&root.join(MARKER),b"Phoenix Rust research-only archive v1\n")?;
            sync_dir(root)?;
        }
        let root=fs::canonicalize(root)?;
        let f=OpenOptions::new().create(true).read(true).write(true)
            .open(root.join(".writer-lock"))?;
        f.try_lock_exclusive().map_err(|_|Error::Busy)?;
        for sub in ["manifests","partitions","pending","quarantine"] {
            fs::create_dir_all(root.join(sub))?;
        }
        let mut archive=Self {root,_writer_lock:f,ids:HashMap::new(),
            manifests:BTreeMap::new(),recovery:RecoveryReport::default()};
        archive.load_index()?;
        archive.recover_uncommitted()?;
        Ok(archive)
    }
    pub fn recovery(&self)->RecoveryReport {self.recovery}
    pub fn indexed_events(&self)->usize {self.ids.len()}
    pub fn committed_partitions(&self)->usize {self.manifests.len()}
    fn load_index(&mut self)->Result<(),Error>{
        let mut n=0;
        for item in fs::read_dir(self.root.join("manifests"))?{
            n+=1;if n>MAX_PARTITIONS{return Err(Error::CapacityExceeded);}
            let item=item?;
            if !item.file_type()?.is_file(){return Err(Error::InvalidManifest);}
            let p=item.path();
            let m:SealedPartition=serde_json::from_slice(
                &safe_read(&p,MAX_MANIFEST_BYTES)?)?;
            if m.format_version!=FORMAT||m.status!="sealed"||
                !is_digest(&m.partition_id)||m.parquet_sha256!=m.partition_id||
                m.events.len()!=m.rows||m.rows==0||m.rows>MAX_ROWS_PER_PARTITION||
                p.file_name().and_then(|s|s.to_str())!=
                    Some(format!("{}.json",m.partition_id).as_str()) {
                return Err(Error::InvalidManifest);
            }
            if !data_path(&self.root,&m.partition_id).is_file(){
                return Err(Error::MissingCommittedPartition(m.partition_id));
            }
            if self.ids.len()+m.events.len()>MAX_INDEXED_EVENTS{
                return Err(Error::CapacityExceeded);
            }
            for e in &m.events {
                if !valid_id(&e.id)||!is_digest(&e.row_sha256)||
                    self.ids.insert(e.id.clone(),e.row_sha256.clone()).is_some(){
                    return Err(Error::InvalidManifest);
                }
            }
            self.manifests.insert(m.partition_id.clone(),m);
        }
        self.recovery.committed_partitions=self.manifests.len();
        Ok(())
    }
    fn recover_uncommitted(&mut self)->Result<(),Error>{
        let mut seen=0;
        for e in fs::read_dir(self.root.join("pending"))?{
            seen+=1;if seen>MAX_PARTITIONS{return Err(Error::CapacityExceeded);}
            let e=e?;if !e.file_type()?.is_file(){return Err(Error::InvalidInput);}
            quarantine(&self.root,&e.path())?;
            self.recovery.quarantined_temp_files+=1;
        }
        seen=0;
        for e in fs::read_dir(self.root.join("partitions"))?{
            seen+=1;if seen>MAX_PARTITIONS{return Err(Error::CapacityExceeded);}
            let e=e?;if !e.file_type()?.is_file(){return Err(Error::InvalidInput);}
            let stem=e.path().file_stem().and_then(|s|s.to_str())
                .ok_or(Error::InvalidInput)?.to_owned();
            if !self.manifests.contains_key(&stem){
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
        self.publish_inner(scope,events,Fault::None)
    }
    fn publish_inner(&mut self,scope:SourceScope,events:&[EventRow],fault:Fault)
        ->Result<PublishResult,Error>{
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
            if let Some(prev)=seen.get(&e.event_id).or_else(||self.ids.get(&e.event_id)){
                if *prev!=digest{return Err(Error::DuplicateConflict(e.event_id.clone()));}
                dup+=1;continue;
            }
            seen.insert(e.event_id.clone(),digest.clone());
            refs.push(EventIdentity{id:e.event_id.clone(),row_sha256:digest});
            fresh.push(e.data.clone());
        }
        if fresh.is_empty(){return Ok(PublishResult{
            partition_id:None,added:0,duplicates:dup,
            durability:Durability::FileSyncedDirectoryNotProven});}
        if self.manifests.len()>=MAX_PARTITIONS ||
            self.ids.len()+fresh.len()>MAX_INDEXED_EVENTS{
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
        temp_file(&pending_data,&bytes)?;
        if matches!(fault,Fault::AfterDataSync){return Err(Error::InjectedFault);}
        fs::rename(&pending_data,data_path(&self.root,&id))?;
        let durable_data_dir=sync_dir(&self.root.join("partitions"))?;
        if matches!(fault,Fault::AfterDataRename){return Err(Error::InjectedFault);}
        let pending_manifest=self.root.join("pending").join(format!("{id}.json.tmp"));
        temp_file(&pending_manifest,&json)?;
        if matches!(fault,Fault::AfterManifestSync){return Err(Error::InjectedFault);}
        fs::rename(&pending_manifest,manifest_path(&self.root,&id))?;
        let durable_manifest_dir=sync_dir(&self.root.join("manifests"))?;
        for r in &m.events {self.ids.insert(r.id.clone(),r.row_sha256.clone());}
        self.manifests.insert(id.clone(),m);
        Ok(PublishResult{partition_id:Some(id),added:fresh.len(),duplicates:dup,
            durability:if durable_data_dir&&durable_manifest_dir{
                Durability::FileAndDirectorySynced
            }else{Durability::FileSyncedDirectoryNotProven}})
    }
    /// Reads only bounded known sealed partitions; no drive scan.
    pub fn verify_all(&self)->Result<usize,Error>{
        for m in self.manifests.values(){
            let bytes=safe_read(&data_path(&self.root,&m.partition_id),MAX_PARTITION_BYTES)?;
            if hash(&bytes)!=m.parquet_sha256{
                return Err(Error::DigestMismatch(m.partition_id.clone()));
            }
            let pm=ParquetManifest{rows:m.rows,sha256:m.parquet_sha256.clone(),
                received_min_ns:m.received_min_ns,
                received_max_ns:m.received_max_ns,format:"parquet_zstd_v1"};
            verify_finalized(&bytes,&pm)
                .map_err(|x|Error::Parquet(format!("{x:?}")))?;
        }
        Ok(self.manifests.len())
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
            Fault::AfterDataRename),Err(Error::InjectedFault)));
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
            Fault::AfterManifestSync),Err(Error::InjectedFault)));
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
            Fault::BeforeDataWrite),Err(Error::InjectedFault)));
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
    /// Read only previously sealed and checksum-verified rows. The callback
    /// receives one row at a time; partitions remain strictly bounded.
    pub fn visit_verified_rows<F>(&self,mut callback:F)->Result<(),Error>
    where F:FnMut(&SealedPartition,&EventIdentity,&Row)->Result<(),Error>{
        for manifest in self.manifests.values(){
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

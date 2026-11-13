//! Isolated research-only integration: sealed Parquet first, ACID index second.
//! No production collector, live SQLite/WAL or exchange endpoints.
use phoenix_disk_identity::Index;
use phoenix_durable_archive::{Archive,EventRow,PublishResult,SealedPartition,SourceScope,MAX_PARTITIONS,MAX_MANIFEST_BYTES};
use sha2::{Digest,Sha256};
use std::{collections::BTreeSet,fs::{self,File,OpenOptions},io::{Read,Write},path::{Path,PathBuf}};
const MARKER:&str="PHOENIX_RESEARCH_SEALED_INDEX_ONLY";
const MAGIC:&[u8]=b"Phoenix sealed+redb research archive v1\n";
#[derive(Debug)]
pub enum Error{UnsafeRoot,Io(String),Archive(String),Index(String),
    InvalidManifest,OrphanIndex(String),Interrupted,Poisoned}
fn io(e:impl std::fmt::Display)->Error{Error::Io(e.to_string())}
fn archive_error(e:impl std::fmt::Debug)->Error{Error::Archive(format!("{e:?}"))}
fn index_error(e:impl std::fmt::Debug)->Error{Error::Index(format!("{e:?}"))}
fn hash(bytes:&[u8])->String{format!("{:x}",Sha256::digest(bytes))}
fn sha_valid(s:&str)->bool{s.len()==64&&s.bytes().all(|c|c.is_ascii_hexdigit())}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum Fault{None,AfterSealedManifest,AfterIndexCommit}
#[derive(Debug,Clone,PartialEq,Eq,Default)]
pub struct Recovery{pub sealed:usize,pub replayed:usize,pub already_indexed:usize,pub event_references:usize}
pub struct IndexedArchive{root:PathBuf,archive:Archive,index:Index,poisoned:bool,recovery:Recovery}
impl IndexedArchive {
    pub fn open(path:impl AsRef<Path>)->Result<Self,Error>{
        let root=path.as_ref();
        if root.exists(){
            if fs::read(root.join(MARKER)).map_err(|_|Error::UnsafeRoot)?!=MAGIC {
                return Err(Error::UnsafeRoot);
            }
        }else{
            fs::create_dir(root).map_err(io)?;
            let mut f=OpenOptions::new().create_new(true).write(true)
                .open(root.join(MARKER)).map_err(io)?;
            f.write_all(MAGIC).map_err(io)?;
            f.sync_all().map_err(io)?;
        }
        let root=fs::canonicalize(root).map_err(io)?;
        // Archive's exclusive OS advisory lock covers this research root.
        let archive=Archive::open(root.join("sealed")).map_err(archive_error)?;
        let index=Index::open(root.join("event-index")).map_err(index_error)?;
        let mut this=Self{root,archive,index,poisoned:false,recovery:Recovery::default()};
        this.recovery=this.reconcile()?;
        Ok(this)
    }
    pub fn recovery(&self)->&Recovery{&self.recovery}
    pub fn find(&self,id:&str)->Result<Option<String>,Error>{self.index.get(id).map_err(index_error)}
    fn manifests(&self)->Result<Vec<(String,Vec<u8>,SealedPartition)>,Error>{
        let mut files=Vec::new();
        for item in fs::read_dir(self.root.join("sealed/manifests")).map_err(io)? {
            let item=item.map_err(io)?;
            if !item.file_type().map_err(io)?.is_file(){return Err(Error::InvalidManifest);}
            files.push(item.path());
            if files.len()>MAX_PARTITIONS{return Err(Error::InvalidManifest);}
        }
        files.sort();
        let mut out=Vec::with_capacity(files.len());
        for path in files{
            if fs::metadata(&path).map_err(io)?.len()>MAX_MANIFEST_BYTES {
                return Err(Error::InvalidManifest);
            }
            let mut data=Vec::new();
            File::open(&path).map_err(io)?.take(MAX_MANIFEST_BYTES+1)
                .read_to_end(&mut data).map_err(io)?;
            if data.len() as u64>MAX_MANIFEST_BYTES{return Err(Error::InvalidManifest);}
            let m:SealedPartition=serde_json::from_slice(&data)
                .map_err(|_|Error::InvalidManifest)?;
            let filename=path.file_name().and_then(|s|s.to_str()).unwrap_or("");
            if !sha_valid(&m.partition_id)||m.partition_id!=m.parquet_sha256||
                filename!=format!("{}.json",m.partition_id)||
                m.rows==0||m.rows>256||m.rows!=m.events.len()||m.status!="sealed" {
                return Err(Error::InvalidManifest);
            }
            out.push((m.partition_id.clone(),data,m));
        }
        Ok(out)
    }
    fn reconcile(&mut self)->Result<Recovery,Error>{
        // Re-verify Parquet SHA/typed row readback before promoting any ID.
        // Check each decoded row against its per-event SHA in the sealed manifest,
        // not merely the partition-level Parquet checksum.
        self.archive.visit_verified_rows(|_,_,_|Ok(())).map_err(archive_error)?;
        let manifests=self.manifests()?;
        let real=manifests.iter().map(|(s,_,_)|s.clone()).collect::<BTreeSet<_>>();
        // A database-only "committed" marker must never invent source data.
        for s in self.index.list_partition_ids(MAX_PARTITIONS+1).map_err(index_error)? {
            if !real.contains(&s){return Err(Error::OrphanIndex(s));}
        }
        let mut r=Recovery{sealed:manifests.len(),..Recovery::default()};
        for (id,manifest,part) in manifests{
            let events=part.events.iter()
                .map(|e|(e.id.as_str(),e.row_sha256.as_str()))
                .collect::<Vec<_>>();
            r.event_references+=events.len();
            let outcome=self.index.commit_verified_partition(&id,&hash(&manifest),&events)
                .map_err(index_error)?;
            if outcome.already_committed {r.already_indexed+=1}
            else{r.replayed+=1}
        }
        Ok(r)
    }

    /// Incrementally promote only the newly sealed partition. Cold opens still
    /// perform full archive and index reconciliation; appends must not perform
    /// an O(total historical rows) validation on every publication.
    fn commit_just_sealed(&mut self,id:&str)->Result<(),Error>{
        if !sha_valid(id){return Err(Error::InvalidManifest);}
        let path=self.root.join("sealed/manifests").join(format!("{id}.json"));
        if fs::metadata(&path).map_err(io)?.len()>MAX_MANIFEST_BYTES{
            return Err(Error::InvalidManifest);
        }
        let mut manifest_bytes=Vec::new();
        File::open(&path).map_err(io)?.take(MAX_MANIFEST_BYTES+1)
            .read_to_end(&mut manifest_bytes).map_err(io)?;
        if manifest_bytes.len() as u64>MAX_MANIFEST_BYTES{
            return Err(Error::InvalidManifest);
        }
        let persisted:SealedPartition=serde_json::from_slice(&manifest_bytes)
            .map_err(|_|Error::InvalidManifest)?;
        if persisted.partition_id!=id || persisted.parquet_sha256!=id ||
            persisted.status!="sealed" || persisted.rows==0 ||
            persisted.rows>256 || persisted.rows!=persisted.events.len(){
            return Err(Error::InvalidManifest);
        }

        // The in-memory sealed archive must agree *exactly* with the persisted
        // manifest, including source scope and ordered event identity list.
        // The archive's typed readback independently checks parquet and row SHA.
        let persisted_value=serde_json::to_value(&persisted)
            .map_err(|_|Error::InvalidManifest)?;
        let mut memory_matches=None;
        let mut verified=Vec::<(String,String)>::with_capacity(persisted.rows);
        self.archive.visit_verified_partition(id,|memory,event,_row|{
            if memory_matches.is_none(){
                let value=serde_json::to_value(memory)
                    .map_err(phoenix_durable_archive::Error::Json)?;
                memory_matches=Some(value==persisted_value);
            }
            verified.push((event.id.clone(),event.row_sha256.clone()));
            Ok(())
        }).map_err(archive_error)?;
        if memory_matches!=Some(true) || verified.len()!=persisted.events.len() ||
            persisted.events.iter().zip(verified.iter()).any(|(a,(id,digest))|
                a.id!=*id || a.row_sha256!=*digest){
            return Err(Error::InvalidManifest);
        }
        let events=verified.iter().map(|(id,digest)|(id.as_str(),digest.as_str()))
            .collect::<Vec<_>>();
        let outcome=self.index.commit_verified_partition(
            id,&hash(&manifest_bytes),&events).map_err(index_error)?;
        self.recovery.sealed+=1;
        self.recovery.event_references+=events.len();
        if outcome.already_committed{self.recovery.already_indexed+=1;}
        else{self.recovery.replayed+=1;}
        Ok(())
    }

    pub fn publish(&mut self,scope:SourceScope,rows:&[EventRow])
        ->Result<PublishResult,Error>{self.publish_fault(scope,rows,Fault::None)}
    pub fn publish_fault(&mut self,scope:SourceScope,rows:&[EventRow],fault:Fault)
        ->Result<PublishResult,Error>{
        if self.poisoned{return Err(Error::Poisoned);}
        let result=self.archive.publish(scope,rows).map_err(archive_error)?;
        if result.partition_id.is_none(){return Ok(result);}
        if fault==Fault::AfterSealedManifest {
            self.poisoned=true;return Err(Error::Interrupted);
        }
        // Reconcile exactly the newly sealed manifest; reopening still
        // performs full evidence verification and repairs missing transactions.
        if let Err(err)=self.commit_just_sealed(result.partition_id.as_deref().unwrap()){
            self.poisoned=true;
            return Err(err);
        }
        if fault==Fault::AfterIndexCommit{
            self.poisoned=true;return Err(Error::Interrupted);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use phoenix_parquet_batch::Row;
    use tempfile::TempDir;
    fn root(t:&TempDir)->PathBuf{t.path().join("new-research-root")}
    fn scope()->SourceScope{
        SourceScope{feed:"kalshi".into(),session:"synthetic".into(),
            market:"BTC".into(),observed_sequence_gaps:0,
            quality_flags:vec![],source_manifest_sha256:None}
    }
    fn event(id:&str,n:u64)->EventRow{EventRow{
        event_id:id.into(),data:Row{feed:"kalshi".into(),
            session:"synthetic".into(),market:"BTC".into(),
            exchange_ns:n as i64,received_ns:n as i64+10,
            sequence:n,kind:1,payload:vec![n as u8,3,4]}}}
    #[test]fn sealed_manifest_deduplicates_after_reopen(){
        let t=TempDir::new().unwrap();
        let mut a=IndexedArchive::open(root(&t)).unwrap();
        a.publish(scope(),&[event("a",1),event("b",2)]).unwrap();
        assert!(a.find("a").unwrap().is_some());drop(a);
        let mut b=IndexedArchive::open(root(&t)).unwrap();
        assert_eq!(b.recovery().sealed,1);
        assert_eq!(b.recovery().already_indexed,1);
        assert_eq!(b.recovery().replayed,0);
        assert_eq!(b.publish(scope(),&[event("a",1)]).unwrap().duplicates,1);
    }
    #[test]fn after_seal_before_index_restarts_and_replays(){
        let t=TempDir::new().unwrap();
        let mut a=IndexedArchive::open(root(&t)).unwrap();
        assert!(matches!(a.publish_fault(scope(),&[event("a",1)],
            Fault::AfterSealedManifest),Err(Error::Interrupted)));
        assert!(a.find("a").unwrap().is_none());
        assert!(matches!(a.publish(scope(),&[event("b",2)]),Err(Error::Poisoned)));
        drop(a);
        let b=IndexedArchive::open(root(&t)).unwrap();
        assert_eq!(b.recovery().replayed,1);
        assert!(b.find("a").unwrap().is_some());
    }
    #[test]fn after_index_commit_is_idempotent(){
        let t=TempDir::new().unwrap();let mut a=IndexedArchive::open(root(&t)).unwrap();
        assert!(matches!(a.publish_fault(scope(),&[event("a",1)],
            Fault::AfterIndexCommit),Err(Error::Interrupted)));
        drop(a);
        let b=IndexedArchive::open(root(&t)).unwrap();
        assert_eq!(b.recovery().already_indexed,1);
        assert_eq!(b.recovery().replayed,0);
    }
    #[test]fn corrupted_sealed_event_identity_is_not_promoted(){
        let t=TempDir::new().unwrap();let path=root(&t);
        let mut a=IndexedArchive::open(&path).unwrap();
        a.publish(scope(),&[event("original",1)]).unwrap();
        drop(a);
        let manifests=path.join("sealed/manifests");
        let file=fs::read_dir(manifests).unwrap().next().unwrap().unwrap().path();
        let mut data:serde_json::Value=serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
        data["events"][0]["row_sha256"]=serde_json::Value::String("f".repeat(64));
        fs::write(file,serde_json::to_vec(&data).unwrap()).unwrap();
        assert!(matches!(IndexedArchive::open(&path),Err(Error::Archive(_))));
    }
    #[test]fn four_256_event_sealed_partitions_survive_reopen(){
        let t=TempDir::new().unwrap();
        let mut a=IndexedArchive::open(root(&t)).unwrap();
        for n in 0..4u64 {
            let entries=(0..256u64).map(|k|{
                let seq=n*256+k+1;
                event(&format!("p{n}:{k}"),seq)
            }).collect::<Vec<_>>();
            let outcome=a.publish(scope(),&entries).unwrap();
            assert_eq!(outcome.added,256);
        }
        assert!(a.find("p0:0").unwrap().is_some());
        assert!(a.find("p3:255").unwrap().is_some());
        drop(a);
        let b=IndexedArchive::open(root(&t)).unwrap();
        assert_eq!(b.recovery().sealed,4);
        assert_eq!(b.recovery().event_references,1024);
        assert_eq!(b.recovery().already_indexed,4);
        assert_eq!(b.recovery().replayed,0);
    }

    #[test]fn incremental_commit_preserves_new_partition_and_reopen_counts(){
        let t=TempDir::new().unwrap();
        let mut a=IndexedArchive::open(root(&t)).unwrap();
        for n in 0..6u64{
            let rows=(0..32u64).map(|k|{
                let seq=n*32+k+1;
                event(&format!("inc{n}:{k}"),seq)
            }).collect::<Vec<_>>();
            assert_eq!(a.publish(scope(),&rows).unwrap().added,32);
            assert_eq!(a.recovery().sealed,(n+1) as usize);
            assert_eq!(a.recovery().event_references,((n+1)*32) as usize);
        }
        drop(a);
        let b=IndexedArchive::open(root(&t)).unwrap();
        assert_eq!(b.recovery().sealed,6);
        assert_eq!(b.recovery().already_indexed,6);
        assert_eq!(b.recovery().event_references,192);
        assert!(b.find("inc5:31").unwrap().is_some());
    }

    #[test]fn unknown_root_and_orphan_index_fail_closed(){
        let t=TempDir::new().unwrap();let path=root(&t);
        fs::create_dir(&path).unwrap();
        assert!(matches!(IndexedArchive::open(&path),Err(Error::UnsafeRoot)));
        fs::remove_dir(&path).unwrap();
        let a=IndexedArchive::open(&path).unwrap();
        let fake="a".repeat(64);let mh="b".repeat(64);
        a.index.commit_verified_partition(&fake,&mh,&[("fake",&fake)]).unwrap();
        drop(a);
        assert!(matches!(IndexedArchive::open(&path),Err(Error::OrphanIndex(_))));
    }
}
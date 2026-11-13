//! Isolated research-only integration: sealed Parquet first, ACID index second.
//! No production collector, live SQLite/WAL or exchange endpoints.
use phoenix_disk_identity::{Index,IndexFault};
use phoenix_durable_archive::{Fault as ArchiveFault,Durability};
use phoenix_durable_archive::recovery::{RecoverySummary,PublicationState,Assurance};
use phoenix_durable_archive::{Archive,EventRow,PublishResult,SealedPartition,SourceScope,MAX_EXTERNAL_PARTITIONS,MAX_MANIFEST_BYTES};
use sha2::{Digest,Sha256};
use std::{fs::{self,File,OpenOptions},io::{Read,Write},path::{Path,PathBuf}};
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
pub enum Fault{
    None,AfterSealedManifest,AfterIndexCommit,BeforeIndexTransaction,
    AfterRowsBeforeMarker,AfterMarkerBeforeCommit,
    AfterIndexCommitBeforeAcknowledgement,
}
#[derive(Debug,Clone,PartialEq,Eq,Default)]
pub struct Recovery{
    pub sealed:usize,pub replayed:usize,pub already_indexed:usize,
    pub event_references:usize,pub observed_duplicates:usize,
    pub sample_partition_hashes:Vec<String>,
}
pub struct IndexedArchive{root:PathBuf,archive:Archive,index:Index,poisoned:bool,recovery:Recovery}
impl IndexedArchive {
    pub fn open(path:impl AsRef<Path>)->Result<Self,Error>{
        Self::open_with_partition_limit(path,MAX_EXTERNAL_PARTITIONS)
    }
    pub fn open_with_partition_limit(path:impl AsRef<Path>,max_partitions:usize)
        ->Result<Self,Error>{
        if max_partitions==0||max_partitions>MAX_EXTERNAL_PARTITIONS{
            return Err(Error::InvalidManifest);
        }
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
        let archive=Archive::open_external_index_with_limit(
            root.join("sealed"),max_partitions).map_err(archive_error)?;
        let index=Index::open(root.join("event-index")).map_err(index_error)?;
        let mut this=Self{root,archive,index,poisoned:false,recovery:Recovery::default()};
        this.recovery=this.reconcile()?;
        Ok(this)
    }
    pub fn recovery(&self)->&Recovery{&self.recovery}
    /// A bounded structured summary. Unknown data loss remains None; a clean
    /// journal is not proof of complete upstream market-data capture.
    pub fn recovery_summary(&self)->RecoverySummary{
        let mut out=self.archive.recovery_summary();
        out.sealed_partitions_found=self.recovery.sealed;
        out.index_transactions_recovered=self.recovery.replayed;
        out.already_indexed_partitions=self.recovery.already_indexed;
        out.verified_event_references=self.recovery.event_references;
        out.duplicate_event_identities=if self.recovery.observed_duplicates>0{
            Some(self.recovery.observed_duplicates)
        }else{None};
        for id in &self.recovery.sample_partition_hashes{out.record_evidence(id);}
        out.publication_state=if self.poisoned{
            out.manual_intervention_required=true;
            out.errors.push("WRITER_POISONED_REOPEN_REQUIRED".into());
            PublicationState::DurabilityUncertain
        }else{PublicationState::FullyReconciled};
        out.assurance=if self.poisoned{Assurance::Uncertain}
        else if cfg!(windows){Assurance::FileSyncedDirectoryNotProven}
        else{Assurance::FileAndDirectorySyncedSoftwareLevel};
        out
    }
    pub fn recovery_report_json(&self)->Result<String,Error>{
        serde_json::to_string_pretty(&self.recovery_summary()).map_err(io)
    }
    pub fn find(&self,id:&str)->Result<Option<String>,Error>{self.index.get(id).map_err(index_error)}
    /// Disk-backed, exclusive-cursor partition listing. At most 512 strings
    /// are returned, independent of the total archive history.
    pub fn partition_page(&self,after:Option<&str>,limit:usize)
        ->Result<Vec<String>,Error>{
        self.index.partition_page(after,limit).map_err(index_error)
    }

    fn reconcile(&mut self)->Result<Recovery,Error>{
        // Disk is the publication authority. Detect database-only markers
        // without materializing the complete set of committed partition IDs.
        let folder=self.root.join("sealed/manifests");
        let mut phantom=None;
        self.index.visit_partition_ids(|id|{
            if !folder.join(format!("{id}.json")).is_file()&&phantom.is_none(){
                phantom=Some(id.to_owned());
            }
            Ok(())
        }).map_err(index_error)?;
        if let Some(id)=phantom{return Err(Error::OrphanIndex(id));}
        let mut recovered=Recovery::default();
        // Read one manifest (<=1 MiB) plus one Parquet partition at a time.
        // Every accepted ID is checked against the original decoded row hash
        // before the separate redb ACID transaction can commit it.
        for entry in fs::read_dir(&folder).map_err(io)?{
            let entry=entry.map_err(io)?;
            if !entry.file_type().map_err(io)?.is_file(){
                return Err(Error::InvalidManifest);
            }
            recovered.sealed=recovered.sealed.checked_add(1)
                .ok_or(Error::InvalidManifest)?;
            if recovered.sealed>MAX_EXTERNAL_PARTITIONS{
                return Err(Error::InvalidManifest);
            }
            let path=entry.path();
            if fs::metadata(&path).map_err(io)?.len()>MAX_MANIFEST_BYTES{
                return Err(Error::InvalidManifest);
            }
            let mut bytes=Vec::new();
            File::open(&path).map_err(io)?.take(MAX_MANIFEST_BYTES+1)
                .read_to_end(&mut bytes).map_err(io)?;
            if bytes.len() as u64>MAX_MANIFEST_BYTES{
                return Err(Error::InvalidManifest);
            }
            let m:SealedPartition=serde_json::from_slice(&bytes)
                .map_err(|_|Error::InvalidManifest)?;
            if !sha_valid(&m.partition_id)||m.parquet_sha256!=m.partition_id||
                m.format_version!=1||m.status!="sealed"||
                path.file_name().and_then(|s|s.to_str())!=
                    Some(format!("{}.json",m.partition_id).as_str())||
                m.rows==0||m.rows>256||m.rows!=m.events.len(){
                return Err(Error::InvalidManifest);
            }
            let mut checked=Vec::with_capacity(m.rows);
            self.archive.visit_verified_partition(&m.partition_id,|_,event,_|{
                checked.push((event.id.clone(),event.row_sha256.clone()));
                Ok(())
            }).map_err(archive_error)?;
            if checked.len()!=m.events.len()||
                checked.iter().zip(m.events.iter()).any(|((id,d),e)|
                    id!=&e.id||d!=&e.row_sha256){
                return Err(Error::InvalidManifest);
            }
            let refs=checked.iter().map(|(id,d)|(id.as_str(),d.as_str()))
                .collect::<Vec<_>>();
            let result=self.index.commit_verified_partition(
                &m.partition_id,&hash(&bytes),&refs).map_err(index_error)?;
            recovered.event_references=recovered.event_references
                .checked_add(refs.len()).ok_or(Error::InvalidManifest)?;
            if result.already_committed{
                recovered.already_indexed+=1;
            }else{
                recovered.replayed+=1;
            }
            if recovered.sample_partition_hashes.len()<8{
                recovered.sample_partition_hashes.push(m.partition_id.clone());
            }
        }
        if recovered.sealed!=self.archive.committed_partitions(){
            return Err(Error::InvalidManifest);
        }
        Ok(recovered)
    }

    /// Incrementally promote only the newly sealed partition. Cold opens still
    /// perform full archive and index reconciliation; appends must not perform
    /// an O(total historical rows) validation on every publication.
    fn commit_just_sealed(&mut self,id:&str,fault:IndexFault)->Result<(),Error>{
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

        // Reopen the specific sealed manifest lazily; its Parquet and per-row
        // SHA-256 checks are authoritative, not any heap-cached identities.
        let mut verified=Vec::<(String,String)>::with_capacity(persisted.rows);
        self.archive.visit_verified_partition(id,|memory,event,_row|{
            if memory.partition_id!=persisted.partition_id||
                memory.rows!=persisted.rows{
                return Err(phoenix_durable_archive::Error::InvalidManifest);
            }
            verified.push((event.id.clone(),event.row_sha256.clone()));
            Ok(())
        }).map_err(archive_error)?;
        if verified.len()!=persisted.events.len() ||
            persisted.events.iter().zip(verified.iter()).any(|(a,(id,digest))|
                a.id!=*id || a.row_sha256!=*digest){
            return Err(Error::InvalidManifest);
        }
        let events=verified.iter().map(|(id,digest)|(id.as_str(),digest.as_str()))
            .collect::<Vec<_>>();
        let outcome=self.index.commit_verified_partition_fault(
            id,&hash(&manifest_bytes),&events,fault).map_err(index_error)?;
        self.recovery.sealed+=1;
        self.recovery.event_references+=events.len();
        if outcome.already_committed{self.recovery.already_indexed+=1;}
        else{self.recovery.replayed+=1;}
        if self.recovery.sample_partition_hashes.len()<8{
            self.recovery.sample_partition_hashes.push(id.to_owned());
        }
        Ok(())
    }

    pub fn publish(&mut self,scope:SourceScope,rows:&[EventRow])
        ->Result<PublishResult,Error>{
        self.publish_with_faults(scope,rows,Fault::None,ArchiveFault::None)
    }
    pub fn publish_fault(&mut self,scope:SourceScope,rows:&[EventRow],
        fault:Fault)->Result<PublishResult,Error>{
        self.publish_with_faults(scope,rows,fault,ArchiveFault::None)
    }
    /// Source-stage crash injection, not a production order endpoint.
    pub fn publish_archive_fault(&mut self,scope:SourceScope,
        rows:&[EventRow],fault:ArchiveFault)->Result<PublishResult,Error>{
        self.publish_with_faults(scope,rows,Fault::None,fault)
    }
    fn publish_with_faults(&mut self,scope:SourceScope,rows:&[EventRow],
        fault:Fault,archive_fault:ArchiveFault)->Result<PublishResult,Error>{
        if self.poisoned{return Err(Error::Poisoned);}
        let disk_index=&self.index;
        let result=self.archive.publish_with_external_lookup_fault(
            scope,rows,|id|disk_index.get(id).map_err(|err|
                phoenix_durable_archive::Error::ExternalIndex(format!("{err:?}"))),
            archive_fault);
        let result=match result{
            Ok(v)=>v,
            Err(e)=>{
                // A failed publication may have passed the sealed-manifest
                // boundary. Never allow a second publication without a
                // recovered, fully verified cold reopen.
                if !matches!(e,phoenix_durable_archive::Error::InvalidInput|
                    phoenix_durable_archive::Error::DuplicateConflict(_)|
                    phoenix_durable_archive::Error::CapacityExceeded){
                    self.poisoned=true;
                }
                return Err(archive_error(e));
            }
        };
        self.recovery.observed_duplicates=self.recovery.observed_duplicates
            .saturating_add(result.duplicates);
        if result.partition_id.is_none(){return Ok(result);}
        if fault==Fault::AfterSealedManifest{
            self.poisoned=true;return Err(Error::Interrupted);
        }
        let index_fault=match fault{
            Fault::BeforeIndexTransaction=>IndexFault::RejectTransaction,
            Fault::AfterRowsBeforeMarker=>IndexFault::AfterRowInsertBeforeMarker,
            Fault::AfterMarkerBeforeCommit=>IndexFault::AfterMarkerBeforeCommit,
            Fault::AfterIndexCommitBeforeAcknowledgement=>
                IndexFault::AfterCommitBeforeAcknowledgement,
            _=>IndexFault::None,
        };
        if let Err(err)=self.commit_just_sealed(
            result.partition_id.as_deref().unwrap(),index_fault){
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

    #[test]fn redb_is_the_duplicate_authority_after_cold_restart(){
        let t=TempDir::new().unwrap();
        let mut a=IndexedArchive::open(root(&t)).unwrap();
        a.publish(scope(),&[event("unique",7)]).unwrap();
        assert_eq!(a.archive.indexed_events(),0);
        drop(a);
        let mut b=IndexedArchive::open(root(&t)).unwrap();
        assert_eq!(b.archive.indexed_events(),0);
        let duplicate=b.publish(scope(),&[event("unique",7)]).unwrap();
        assert_eq!((duplicate.added,duplicate.duplicates),(0,1));
        let conflict=b.publish(scope(),&[event("unique",8)]);
        assert!(matches!(conflict,Err(Error::Archive(_))));
        let next=b.publish(scope(),&[event("fresh",9)]).unwrap();
        assert_eq!(next.added,1);
        assert_eq!(b.recovery().sealed,2);
    }
    #[test]fn external_publisher_rejects_direct_index_bypass(){
        let t=TempDir::new().unwrap();
        let mut a=IndexedArchive::open(root(&t)).unwrap();
        // A caller without authoritative redb lookup must not publish data.
        assert!(matches!(a.archive.publish(scope(),&[event("x",1)]),
            Err(phoenix_durable_archive::Error::InvalidInput)));
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


    #[test]
    fn lazy_indexed_archive_reopens_and_reconciles_without_manifest_heap(){
        let t=TempDir::new().unwrap();
        let mut archive=IndexedArchive::open_with_partition_limit(root(&t),12).unwrap();
        for n in 0..10u64{
            let e=event(&format!("id:{n}"),n+1);
            archive.publish(scope(),&[e]).unwrap();
        }
        assert_eq!(archive.archive.committed_partitions(),10);
        assert_eq!(archive.archive.indexed_events(),0);
        drop(archive);
        let reopened=IndexedArchive::open_with_partition_limit(root(&t),12).unwrap();
        assert_eq!(reopened.recovery().sealed,10);
        assert_eq!(reopened.recovery().already_indexed,10);
        assert!(reopened.find("id:9").unwrap().is_some());
    }
    #[test]
    fn disk_index_limit_exceed_is_explicit(){
        let t=TempDir::new().unwrap();
        let mut archive=IndexedArchive::open_with_partition_limit(root(&t),2).unwrap();
        archive.publish(scope(),&[event("first",1)]).unwrap();
        archive.publish(scope(),&[event("second",2)]).unwrap();
        assert!(archive.publish(scope(),&[event("third",3)]).is_err());
        drop(archive);
        assert!(IndexedArchive::open_with_partition_limit(root(&t),1).is_err());
    }

    #[test]
    fn recovered_partition_cursor_pages_without_manifest_cache(){
        let t=TempDir::new().unwrap();
        let mut a=IndexedArchive::open_with_partition_limit(root(&t),16).unwrap();
        for n in 0..13u64{
            a.publish(scope(),&[event(&format!("n:{n}"),n+1)]).unwrap();
        }
        drop(a);
        let b=IndexedArchive::open_with_partition_limit(root(&t),16).unwrap();
        let mut cursor:Option<String>=None;
        let mut all=Vec::new();
        loop{
            let page=b.partition_page(cursor.as_deref(),4).unwrap();
            if page.is_empty(){break;}
            cursor=page.last().cloned();
            all.extend(page);
        }
        assert_eq!(all.len(),13);
        assert!(all.windows(2).all(|pair|pair[0]<pair[1]));
        assert_eq!(b.recovery().already_indexed,13);
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
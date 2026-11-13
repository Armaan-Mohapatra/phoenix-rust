//! Research-only disk-backed event identity index; NEVER open active SQLite
//! or WAL. The in-memory id HashMap in durable-archive remains unchanged
//! until indexed migration/partition recovery correctness is independently proven.
use redb::{Database,Builder,TableDefinition,ReadableTable,ReadableDatabase};
use sha2::{Sha256,Digest};
use std::{collections::BTreeMap,fs::{self,OpenOptions},io::Write,path::{Path,PathBuf}};
const MARKER:&str="PHOENIX_RESEARCH_INDEX_ONLY";
const CACHE_BYTES:usize=16*1024*1024;
const MAX_EVENTS_PER_TRANSACTION:usize=256;
const EVENTS:TableDefinition<&str,&str>=TableDefinition::new("event_ids_v1");
const PARTITIONS:TableDefinition<&str,&str>=TableDefinition::new("sealed_partitions_v1");

#[derive(Debug,PartialEq,Eq)]
pub enum Error {NotResearchPath,InvalidInput,CapacityExceeded,
    ConflictingEvent(String),ConflictingPartition,Storage(String)}
fn io(e:impl std::fmt::Display)->Error{Error::Storage(e.to_string())}
fn sha_ok(s:&str)->bool{s.len()==64&&s.bytes().all(|x|x.is_ascii_hexdigit())}
fn id_ok(s:&str)->bool{!s.is_empty()&&s.len()<=128&&
    s.bytes().all(|x|x.is_ascii_alphanumeric()||matches!(x,b'-'|b'_'|b'.'|b':'|b'/'))}
fn signature(events:&[(&str,&str)])->String{
    let mut h=Sha256::new();
    for (id,checksum) in events{
        h.update((id.len() as u64).to_le_bytes());
        h.update(id.as_bytes());
        h.update(checksum.as_bytes());
    }
    format!("{:x}",h.finalize())
}
pub struct Index {root:PathBuf,db:Database}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub struct ResultCount {pub inserted:usize,pub existing:usize,pub already_committed:bool}
impl Index {
    /// Existing directories without our exact research marker are rejected;
    /// never auto-initialize production/source data paths.
    pub fn open(root:impl AsRef<Path>)->Result<Self,Error>{
        let root=root.as_ref();
        if root.exists(){
            let content=fs::read(root.join(MARKER)).map_err(|_|Error::NotResearchPath)?;
            if content!=b"Phoenix offline research identity index v1\n"{
                return Err(Error::NotResearchPath);
            }
        }else{
            fs::create_dir(root).map_err(io)?;
            let mut marker=OpenOptions::new().create_new(true).write(true)
                .open(root.join(MARKER)).map_err(io)?;
            marker.write_all(b"Phoenix offline research identity index v1\n").map_err(io)?;
            marker.sync_all().map_err(io)?;
        }
        let root=fs::canonicalize(root).map_err(io)?;
        let mut builder=Builder::new();
        // redb defaults to a 1 GiB cache! The research-only Windows laptop
        // cannot afford that. Enforce a 16 MiB cache at every opening.
        builder.set_cache_size(CACHE_BYTES);
        let db=builder.create(root.join("identity.redb")).map_err(io)?;
        {
            let txn=db.begin_write().map_err(io)?;
            txn.open_table(EVENTS).map_err(io)?;
            txn.open_table(PARTITIONS).map_err(io)?;
            txn.commit().map_err(io)?;
        }
        Ok(Self{root,db})
    }
    pub fn root(&self)->&Path{&self.root}
    pub fn get(&self,id:&str)->Result<Option<String>,Error>{
        if !id_ok(id){return Err(Error::InvalidInput);}
        let tx=self.db.begin_read().map_err(io)?;
        let table=tx.open_table(EVENTS).map_err(io)?;
        let result=table.get(id).map_err(io)?.map(|v|v.value().to_owned());
        Ok(result)
    }
    /// Bounded partition enumeration only (never materializes the ID table).
    pub fn list_partition_ids(&self,max:usize)->Result<Vec<String>,Error>{
        if max==0||max>100_000{return Err(Error::InvalidInput);}
        let tx=self.db.begin_read().map_err(io)?;
        let table=tx.open_table(PARTITIONS).map_err(io)?;
        let mut ids=Vec::new();
        for item in table.iter().map_err(io)?{
            let (key,_)=item.map_err(io)?;
            if ids.len()>=max{return Err(Error::CapacityExceeded);}
            ids.push(key.value().to_string());
        }
        Ok(ids)
    }
    /// Stream committed partition IDs in key order, keeping no partition
    /// vector or event-ID map in heap memory. The caller may inspect an
    /// arbitrarily large committed database with a constant-size cursor.
    pub fn visit_partition_ids<F>(&self,mut visit:F)->Result<usize,Error>
    where F:FnMut(&str)->Result<(),Error>{
        let tx=self.db.begin_read().map_err(io)?;
        let table=tx.open_table(PARTITIONS).map_err(io)?;
        let mut n=0usize;
        for entry in table.iter().map_err(io)?{
            let (key,_value)=entry.map_err(io)?;
            visit(key.value())?;
            n=n.checked_add(1).ok_or(Error::CapacityExceeded)?;
        }
        Ok(n)
    }
    pub fn partition_indexed(&self,sha:&str)->Result<bool,Error>{
        if !sha_ok(sha){return Err(Error::InvalidInput);}
        let tx=self.db.begin_read().map_err(io)?;
        let table=tx.open_table(PARTITIONS).map_err(io)?;
        let exists=table.get(sha).map_err(io)?.is_some();
        Ok(exists)
    }
    /// Transaction commits manifest marker and all event identities AT ONCE.
    /// Caller MUST first verify the sealed data partition + manifest SHA256,
    /// and must replay sealed manifests after a crash before admitting new data.
    pub fn commit_verified_partition(&self,partition_sha:&str,
        manifest_sha:&str,events:&[(&str,&str)])->Result<ResultCount,Error>{
        if !sha_ok(partition_sha)||!sha_ok(manifest_sha)||events.is_empty()||
            events.len()>MAX_EVENTS_PER_TRANSACTION{
            return Err(Error::InvalidInput);
        }
        let mut unique=BTreeMap::new();
        for (id,digest) in events{
            if !id_ok(id)||!sha_ok(digest){return Err(Error::InvalidInput);}
            if let Some(old)=unique.insert(*id,*digest){
                if old!=*digest{return Err(Error::ConflictingEvent((*id).into()));}
                return Err(Error::InvalidInput);
            }
        }
        let encoded=format!("{manifest_sha}:{}",signature(events));
        let tx=self.db.begin_write().map_err(io)?;
        {
            let table=tx.open_table(PARTITIONS).map_err(io)?;
            let existing=table.get(partition_sha).map_err(io)?
                .map(|guard|guard.value().to_owned());
            if let Some(existing)=existing {
                if existing!=encoded{return Err(Error::ConflictingPartition);}
                return Ok(ResultCount{inserted:0,existing:events.len(),already_committed:true});
            }
        }
        let mut new_count=0;let mut old_count=0;
        {
            let mut table=tx.open_table(EVENTS).map_err(io)?;
            for (id,digest) in events{
                let old=table.get(*id).map_err(io)?.map(|v|v.value().to_owned());
                match old {
                    Some(x) if x!=*digest =>return Err(Error::ConflictingEvent((*id).into())),
                    Some(_)=>old_count+=1,
                    None=>{
                        table.insert(*id,*digest).map_err(io)?;
                        new_count+=1;
                    }
                }
            }
        }
        {
            let mut table=tx.open_table(PARTITIONS).map_err(io)?;
            table.insert(partition_sha,encoded.as_str()).map_err(io)?;
        }
        tx.commit().map_err(io)?;
        Ok(ResultCount{inserted:new_count,existing:old_count,already_committed:false})
    }
}
#[cfg(test)]
mod tests{
    use super::*;use tempfile::TempDir;
    fn sha(c:char)->String{c.to_string().repeat(64)}
    fn root(t:&TempDir)->PathBuf{t.path().join("isolated-offline-research-index")}
    #[test]
    fn restart_does_not_duplicate_events_or_rewrite_partitions(){
        let t=TempDir::new().unwrap();
        let a=sha('a');let b=sha('b');
        let idx=Index::open(root(&t)).unwrap();
        assert_eq!(idx.commit_verified_partition(&a,&b,&[
            ("P1:1",&a),("P1:2",&b)
        ]).unwrap(),ResultCount{inserted:2,existing:0,already_committed:false});
        drop(idx);
        let idx=Index::open(root(&t)).unwrap();
        assert_eq!(idx.get("P1:1").unwrap(),Some(a.clone()));
        assert_eq!(idx.commit_verified_partition(&a,&b,&[
            ("P1:1",&a),("P1:2",&b)
        ]).unwrap(),ResultCount{inserted:0,existing:2,already_committed:true});
        assert!(idx.partition_indexed(&a).unwrap());
    }
    #[test]
    fn conflicting_event_is_atomic_even_across_partition(){
        let t=TempDir::new().unwrap();
        let a=sha('a');let b=sha('b');let c=sha('c');
        let idx=Index::open(root(&t)).unwrap();
        idx.commit_verified_partition(&a,&b,&[("P:1",&a)]).unwrap();
        assert_eq!(idx.commit_verified_partition(&b,&c,&[
            ("P:2",&b),("P:1",&c)]),Err(Error::ConflictingEvent("P:1".into())));
        assert_eq!(idx.get("P:2").unwrap(),None);
        assert!(!idx.partition_indexed(&b).unwrap());
    }
    #[test]
    fn existing_unknown_directory_is_never_initialized(){
        let t=TempDir::new().unwrap();let path=t.path().join("production-folder");
        fs::create_dir(&path).unwrap();
        assert!(matches!(Index::open(&path),Err(Error::NotResearchPath)));
        assert!(!path.join("identity.redb").exists());
    }

    #[test]
    fn streaming_partition_walk_keeps_identity_index_on_disk(){
        let t=TempDir::new().unwrap();let idx=Index::open(root(&t)).unwrap();
        let mh=sha('f');
        for n in 0..40u32{
            let id=format!("{n:064x}");
            let event=format!("market:event:{n}");
            idx.commit_verified_partition(&id,&mh,&[(event.as_str(),mh.as_str())]).unwrap();
        }
        let mut observed=0usize;
        let mut previous=String::new();
        let count=idx.visit_partition_ids(|id|{
            assert!(previous.as_str()<id);
            previous=id.into();
            observed+=1;Ok(())
        }).unwrap();
        assert_eq!((count,observed),(40,40));
        assert_eq!(idx.get("market:event:39").unwrap(),Some(mh));
    }
    #[test]
    fn batch_is_bounded_to_existing_partition_size(){
        let t=TempDir::new().unwrap();let idx=Index::open(root(&t)).unwrap();
        let a=sha('a');let b=sha('b');
        let keys=(0..257).map(|i|(format!("x:{i}"),a.clone())).collect::<Vec<_>>();
        let borrowed=keys.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect::<Vec<_>>();
        assert_eq!(idx.commit_verified_partition(&a,&b,&borrowed),Err(Error::InvalidInput));
    }
}

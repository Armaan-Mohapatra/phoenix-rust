//! Research-only disk-backed event identity index; NEVER open active SQLite
//! or WAL. The in-memory id HashMap in durable-archive remains unchanged
//! until indexed migration/partition recovery correctness is independently proven.
use redb::{Builder, Database, ReadableDatabase, ReadableTable, TableDefinition};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
const MARKER: &str = "PHOENIX_RESEARCH_INDEX_ONLY";
const CACHE_BYTES: usize = 16 * 1024 * 1024;
const MAX_EVENTS_PER_TRANSACTION: usize = 256;
const EVENTS: TableDefinition<&str, &str> = TableDefinition::new("event_ids_v1");
const PARTITIONS: TableDefinition<&str, &str> = TableDefinition::new("sealed_partitions_v1");
const META: TableDefinition<&str, &str> = TableDefinition::new("phoenix_index_metadata_v1");
const FORMAT_VERSION: &str = "1";

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    NotResearchPath,
    InvalidInput,
    CapacityExceeded,
    ConflictingEvent(String),
    ConflictingPartition,
    UnsupportedFormat,
    InjectedFault,
    Storage(String),
}
fn io(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}
fn sha_ok(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|x| x.is_ascii_hexdigit())
}
fn id_ok(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|x| x.is_ascii_alphanumeric() || matches!(x, b'-' | b'_' | b'.' | b':' | b'/'))
}
fn signature(events: &[(&str, &str)]) -> String {
    let mut h = Sha256::new();
    for (id, checksum) in events {
        h.update((id.len() as u64).to_le_bytes());
        h.update(id.as_bytes());
        h.update(checksum.as_bytes());
    }
    format!("{:x}", h.finalize())
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexFault {
    None,
    RejectTransaction,
    AfterRowInsertBeforeMarker,
    AfterMarkerBeforeCommit,
    AfterCommitBeforeAcknowledgement,
}
fn reject_linked(path: &Path) -> Result<(), Error> {
    if path.components().any(|p| {
        let n = p.as_os_str().to_string_lossy();
        [
            "TradingNextData",
            "KalshiData",
            "KalshiRuntime",
            "paper15m_runtime",
        ]
        .iter()
        .any(|protected| n.eq_ignore_ascii_case(protected))
    }) {
        return Err(Error::NotResearchPath);
    }
    if path
        .components()
        .any(|p| matches!(p, std::path::Component::ParentDir))
    {
        return Err(Error::NotResearchPath);
    }
    for component in path.ancestors() {
        match fs::symlink_metadata(component) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(Error::NotResearchPath);
                }
                #[cfg(windows)]
                {
                    use std::os::windows::fs::MetadataExt;
                    if meta.file_attributes() & 0x400 != 0 {
                        return Err(Error::NotResearchPath);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io(e)),
        }
    }
    Ok(())
}
pub struct Index {
    root: PathBuf,
    db: Database,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultCount {
    pub inserted: usize,
    pub existing: usize,
    pub already_committed: bool,
}
impl Index {
    /// Existing directories without our exact research marker are rejected;
    /// never auto-initialize production/source data paths.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, Error> {
        let root = root.as_ref();
        reject_linked(root)?;
        reject_linked(&root.join(MARKER))?;
        reject_linked(&root.join("identity.redb"))?;
        if root.exists() {
            let content = fs::read(root.join(MARKER)).map_err(|_| Error::NotResearchPath)?;
            if content != b"Phoenix offline research identity index v1\n" {
                return Err(Error::NotResearchPath);
            }
        } else {
            fs::create_dir(root).map_err(io)?;
            let mut marker = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(root.join(MARKER))
                .map_err(io)?;
            marker
                .write_all(b"Phoenix offline research identity index v1\n")
                .map_err(io)?;
            marker.sync_all().map_err(io)?;
        }
        let root = fs::canonicalize(root).map_err(io)?;
        let mut builder = Builder::new();
        // redb defaults to a 1 GiB cache! The research-only Windows laptop
        // cannot afford that. Enforce a 16 MiB cache at every opening.
        builder.set_cache_size(CACHE_BYTES);
        let db = builder.create(root.join("identity.redb")).map_err(io)?;
        {
            let txn = db.begin_write().map_err(io)?;
            txn.open_table(EVENTS).map_err(io)?;
            txn.open_table(PARTITIONS).map_err(io)?;
            {
                let mut meta = txn.open_table(META).map_err(io)?;
                let saved = meta
                    .get("schema_version")
                    .map_err(io)?
                    .map(|guard| guard.value().to_owned());
                match saved.as_deref() {
                    Some(value) if value == FORMAT_VERSION => {}
                    None => {
                        meta.insert("schema_version", FORMAT_VERSION).map_err(io)?;
                    }
                    Some(_) => return Err(Error::UnsupportedFormat),
                }
            }
            txn.commit().map_err(io)?;
        }
        Ok(Self { root, db })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn format_version(&self) -> &'static str {
        FORMAT_VERSION
    }
    pub fn get(&self, id: &str) -> Result<Option<String>, Error> {
        if !id_ok(id) {
            return Err(Error::InvalidInput);
        }
        let tx = self.db.begin_read().map_err(io)?;
        let table = tx.open_table(EVENTS).map_err(io)?;
        let result = table.get(id).map_err(io)?.map(|v| v.value().to_owned());
        Ok(result)
    }
    /// Bounded partition enumeration only (never materializes the ID table).
    pub fn list_partition_ids(&self, max: usize) -> Result<Vec<String>, Error> {
        if max == 0 || max > 100_000 {
            return Err(Error::InvalidInput);
        }
        let tx = self.db.begin_read().map_err(io)?;
        let table = tx.open_table(PARTITIONS).map_err(io)?;
        let mut ids = Vec::new();
        for item in table.iter().map_err(io)? {
            let (key, _) = item.map_err(io)?;
            if ids.len() >= max {
                return Err(Error::CapacityExceeded);
            }
            ids.push(key.value().to_string());
        }
        Ok(ids)
    }
    /// Stream committed partition IDs in key order, keeping no partition
    /// vector or event-ID map in heap memory. The caller may inspect an
    /// arbitrarily large committed database with a constant-size cursor.
    pub fn visit_partition_ids<F>(&self, mut visit: F) -> Result<usize, Error>
    where
        F: FnMut(&str) -> Result<(), Error>,
    {
        let tx = self.db.begin_read().map_err(io)?;
        let table = tx.open_table(PARTITIONS).map_err(io)?;
        let mut n = 0usize;
        for entry in table.iter().map_err(io)? {
            let (key, _value) = entry.map_err(io)?;
            visit(key.value())?;
            n = n.checked_add(1).ok_or(Error::CapacityExceeded)?;
        }
        Ok(n)
    }
    /// Cursor-based page of partition IDs. The redb B+tree seeks to the
    /// exclusive cursor rather than rereading all prior keys. The maximum
    /// returned heap allocation is limited to 512 partition identifiers.
    pub fn partition_page(&self, after: Option<&str>, limit: usize) -> Result<Vec<String>, Error> {
        if limit == 0 || limit > 512 || after.is_some_and(|s| !sha_ok(s)) {
            return Err(Error::InvalidInput);
        }
        let tx = self.db.begin_read().map_err(io)?;
        let table = tx.open_table(PARTITIONS).map_err(io)?;
        let mut range = if let Some(cursor) = after {
            table
                .range::<&str>((
                    std::ops::Bound::Excluded(cursor),
                    std::ops::Bound::Unbounded,
                ))
                .map_err(io)?
        } else {
            table.iter().map_err(io)?
        };
        let mut page = Vec::with_capacity(limit);
        for entry in range.by_ref().take(limit) {
            let (key, _) = entry.map_err(io)?;
            page.push(key.value().to_owned());
        }
        Ok(page)
    }
    pub fn partition_indexed(&self, sha: &str) -> Result<bool, Error> {
        if !sha_ok(sha) {
            return Err(Error::InvalidInput);
        }
        let tx = self.db.begin_read().map_err(io)?;
        let table = tx.open_table(PARTITIONS).map_err(io)?;
        let exists = table.get(sha).map_err(io)?.is_some();
        Ok(exists)
    }
    /// Transaction commits manifest marker and all event identities AT ONCE.
    /// Caller MUST first verify the sealed data partition + manifest SHA256,
    /// and must replay sealed manifests after a crash before admitting new data.
    pub fn commit_verified_partition(
        &self,
        partition_sha: &str,
        manifest_sha: &str,
        events: &[(&str, &str)],
    ) -> Result<ResultCount, Error> {
        self.commit_verified_partition_fault(partition_sha, manifest_sha, events, IndexFault::None)
    }
    /// Deterministic ACID fault injection. All faults except the last occur
    /// before commit; dropping the write transaction must rollback mutations.
    pub fn commit_verified_partition_fault(
        &self,
        partition_sha: &str,
        manifest_sha: &str,
        events: &[(&str, &str)],
        fault: IndexFault,
    ) -> Result<ResultCount, Error> {
        if !sha_ok(partition_sha)
            || !sha_ok(manifest_sha)
            || events.is_empty()
            || events.len() > MAX_EVENTS_PER_TRANSACTION
        {
            return Err(Error::InvalidInput);
        }
        let mut unique = BTreeMap::new();
        for (id, digest) in events {
            if !id_ok(id) || !sha_ok(digest) {
                return Err(Error::InvalidInput);
            }
            if let Some(old) = unique.insert(*id, *digest) {
                if old != *digest {
                    return Err(Error::ConflictingEvent((*id).into()));
                }
                return Err(Error::InvalidInput);
            }
        }
        let encoded = format!("{manifest_sha}:{}", signature(events));
        if fault == IndexFault::RejectTransaction {
            return Err(Error::InjectedFault);
        }
        let tx = self.db.begin_write().map_err(io)?;
        let existing = {
            let table = tx.open_table(PARTITIONS).map_err(io)?;
            let found = table
                .get(partition_sha)
                .map_err(io)?
                .map(|guard| guard.value().to_owned());
            found
        };
        if let Some(existing) = existing {
            if existing != encoded {
                return Err(Error::ConflictingPartition);
            }
            // A matching marker alone cannot certify a corrupted or
            // incompletely reconstructed ID table. Every <=256 event entry
            // must still exist with the original manifest row digest.
            let table = tx.open_table(EVENTS).map_err(io)?;
            for (id, digest) in events {
                let found = table.get(*id).map_err(io)?.map(|v| v.value().to_owned());
                if found.as_deref() != Some(*digest) {
                    return Err(Error::ConflictingEvent((*id).to_owned()));
                }
            }
            return Ok(ResultCount {
                inserted: 0,
                existing: events.len(),
                already_committed: true,
            });
        }
        let mut new_count = 0;
        {
            let mut table = tx.open_table(EVENTS).map_err(io)?;
            for (id, digest) in events {
                let old = table.get(*id).map_err(io)?.map(|v| v.value().to_owned());
                match old {
                    // An identical ID in another sealed partition is still
                    // duplicate history. Only replay of the SAME existing
                    // partition marker is idempotent, never a new marker.
                    Some(_) => return Err(Error::ConflictingEvent((*id).into())),
                    None => {
                        table.insert(*id, *digest).map_err(io)?;
                        new_count += 1;
                    }
                }
            }
        }
        if fault == IndexFault::AfterRowInsertBeforeMarker {
            return Err(Error::InjectedFault);
        }
        {
            let mut table = tx.open_table(PARTITIONS).map_err(io)?;
            table.insert(partition_sha, encoded.as_str()).map_err(io)?;
        }
        if fault == IndexFault::AfterMarkerBeforeCommit {
            return Err(Error::InjectedFault);
        }
        tx.commit().map_err(io)?;
        if fault == IndexFault::AfterCommitBeforeAcknowledgement {
            return Err(Error::InjectedFault);
        }
        Ok(ResultCount {
            inserted: new_count,
            existing: 0,
            already_committed: false,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    fn sha(c: char) -> String {
        c.to_string().repeat(64)
    }
    fn root(t: &TempDir) -> PathBuf {
        t.path().join("isolated-offline-research-index")
    }
    #[test]
    fn restart_does_not_duplicate_events_or_rewrite_partitions() {
        let t = TempDir::new().unwrap();
        let a = sha('a');
        let b = sha('b');
        let idx = Index::open(root(&t)).unwrap();
        assert_eq!(
            idx.commit_verified_partition(&a, &b, &[("P1:1", &a), ("P1:2", &b)])
                .unwrap(),
            ResultCount {
                inserted: 2,
                existing: 0,
                already_committed: false
            }
        );
        drop(idx);
        let idx = Index::open(root(&t)).unwrap();
        assert_eq!(idx.get("P1:1").unwrap(), Some(a.clone()));
        assert_eq!(
            idx.commit_verified_partition(&a, &b, &[("P1:1", &a), ("P1:2", &b)])
                .unwrap(),
            ResultCount {
                inserted: 0,
                existing: 2,
                already_committed: true
            }
        );
        assert!(idx.partition_indexed(&a).unwrap());
    }
    #[test]
    fn conflicting_event_is_atomic_even_across_partition() {
        let t = TempDir::new().unwrap();
        let a = sha('a');
        let b = sha('b');
        let c = sha('c');
        let idx = Index::open(root(&t)).unwrap();
        idx.commit_verified_partition(&a, &b, &[("P:1", &a)])
            .unwrap();
        assert_eq!(
            idx.commit_verified_partition(&b, &c, &[("P:2", &b), ("P:1", &c)]),
            Err(Error::ConflictingEvent("P:1".into()))
        );
        assert_eq!(idx.get("P:2").unwrap(), None);
        assert!(!idx.partition_indexed(&b).unwrap());
    }
    #[test]
    fn matching_partition_marker_does_not_hide_missing_event_identity() {
        let t = TempDir::new().unwrap();
        let db = Index::open(root(&t)).unwrap();
        let part = sha('a');
        let manifest = sha('b');
        let digest = sha('c');
        db.commit_verified_partition(&part, &manifest, &[("BTC:1", digest.as_str())])
            .unwrap();
        // Inject a deliberately inconsistent redb state. The public
        // write API cannot produce it, but recovery must still reject it.
        let tx = db.db.begin_write().unwrap();
        {
            let mut table = tx.open_table(EVENTS).unwrap();
            table.remove("BTC:1").unwrap();
        }
        tx.commit().unwrap();
        assert_eq!(
            db.commit_verified_partition(&part, &manifest, &[("BTC:1", digest.as_str())]),
            Err(Error::ConflictingEvent("BTC:1".into()))
        );
    }
    #[test]
    fn identical_event_id_cannot_be_committed_into_two_partitions() {
        let t = TempDir::new().unwrap();
        let db = Index::open(root(&t)).unwrap();
        let a = sha('a');
        let b = sha('b');
        let digest = sha('c');
        let mh = sha('d');
        db.commit_verified_partition(&a, &mh, &[("same", digest.as_str())])
            .unwrap();
        assert_eq!(
            db.commit_verified_partition(&b, &mh, &[("same", digest.as_str())]),
            Err(Error::ConflictingEvent("same".into()))
        );
        assert!(!db.partition_indexed(&b).unwrap());
        assert_eq!(db.get("same").unwrap(), Some(digest));
    }
    #[test]
    fn interrupted_transaction_rolls_back_ids_and_partition_marker() {
        for fault in [
            IndexFault::RejectTransaction,
            IndexFault::AfterRowInsertBeforeMarker,
            IndexFault::AfterMarkerBeforeCommit,
        ] {
            let t = TempDir::new().unwrap();
            let root = root(&t);
            let id = sha('a');
            let mh = sha('b');
            let digest = sha('c');
            let index = Index::open(&root).unwrap();
            let result = index.commit_verified_partition_fault(
                &id,
                &mh,
                &[("event:a", digest.as_str())],
                fault,
            );
            assert_eq!(result, Err(Error::InjectedFault));
            assert!(!index.partition_indexed(&id).unwrap());
            assert_eq!(index.get("event:a").unwrap(), None);
            drop(index);
            let index = Index::open(&root).unwrap();
            assert_eq!(index.get("event:a").unwrap(), None);
            assert_eq!(
                index
                    .commit_verified_partition(&id, &mh, &[("event:a", digest.as_str())])
                    .unwrap()
                    .inserted,
                1
            );
        }
    }
    #[test]
    fn committed_before_ack_is_idempotent_after_reopen() {
        let t = TempDir::new().unwrap();
        let root = root(&t);
        let id = sha('a');
        let mh = sha('b');
        let digest = sha('c');
        let index = Index::open(&root).unwrap();
        assert_eq!(
            index.commit_verified_partition_fault(
                &id,
                &mh,
                &[("event:a", digest.as_str())],
                IndexFault::AfterCommitBeforeAcknowledgement
            ),
            Err(Error::InjectedFault)
        );
        drop(index);
        let index = Index::open(&root).unwrap();
        assert!(
            index
                .commit_verified_partition(&id, &mh, &[("event:a", digest.as_str())])
                .unwrap()
                .already_committed
        );
    }
    #[test]
    fn schema_version_mismatch_is_explicitly_rejected() {
        let t = TempDir::new().unwrap();
        let root = root(&t);
        let index = Index::open(&root).unwrap();
        let tx = index.db.begin_write().unwrap();
        {
            let mut meta = tx.open_table(META).unwrap();
            meta.insert("schema_version", "999").unwrap();
        }
        tx.commit().unwrap();
        drop(index);
        assert!(matches!(Index::open(&root), Err(Error::UnsupportedFormat)));
    }
    #[cfg(unix)]
    #[test]
    fn a_symlink_to_an_existing_directory_is_not_a_research_index() {
        use std::os::unix::fs::symlink;
        let t = TempDir::new().unwrap();
        let root = root(&t);
        let index = Index::open(&root).unwrap();
        drop(index);
        let alias = t.path().join("alias");
        symlink(&root, &alias).unwrap();
        assert!(matches!(Index::open(&alias), Err(Error::NotResearchPath)));
    }
    #[test]
    fn existing_unknown_directory_is_never_initialized() {
        let t = TempDir::new().unwrap();
        let path = t.path().join("production-folder");
        fs::create_dir(&path).unwrap();
        assert!(matches!(Index::open(&path), Err(Error::NotResearchPath)));
        assert!(!path.join("identity.redb").exists());
    }

    #[test]
    fn streaming_partition_walk_keeps_identity_index_on_disk() {
        let t = TempDir::new().unwrap();
        let idx = Index::open(root(&t)).unwrap();
        let mh = sha('f');
        for n in 0..40u32 {
            let id = format!("{n:064x}");
            let event = format!("market:event:{n}");
            idx.commit_verified_partition(&id, &mh, &[(event.as_str(), mh.as_str())])
                .unwrap();
        }
        let mut observed = 0usize;
        let mut previous = String::new();
        let count = idx
            .visit_partition_ids(|id| {
                assert!(previous.as_str() < id);
                previous = id.into();
                observed += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!((count, observed), (40, 40));
        assert_eq!(idx.get("market:event:39").unwrap(), Some(mh));
    }
    #[test]
    fn cursor_pagination_is_exclusive_deterministic_and_bounded() {
        let t = TempDir::new().unwrap();
        let db = Index::open(root(&t)).unwrap();
        let mh = sha('a');
        for n in 0..37u32 {
            let part = format!("{n:064x}");
            let event = format!("event:{n}");
            db.commit_verified_partition(&part, &mh, &[(event.as_str(), mh.as_str())])
                .unwrap();
        }
        let mut all = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = db.partition_page(cursor.as_deref(), 7).unwrap();
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= 7);
            cursor = page.last().cloned();
            all.extend(page);
        }
        assert_eq!(all.len(), 37);
        assert!(all.windows(2).all(|p| p[0] < p[1]));
        assert!(db.partition_page(cursor.as_deref(), 7).unwrap().is_empty());
        assert_eq!(db.partition_page(None, 513), Err(Error::InvalidInput));
    }
    /// Explicit opt-in stress regression for the final local benchmark
    /// campaign. Excluded from ordinary unit tests only because of disk I/O.
    #[test]
    #[ignore = "final local scale campaign: ~1,048,576 logical identities, 4096 ACID commits"]
    fn million_logical_ids_in_bounded_256_id_transactions() {
        let t = TempDir::new().unwrap();
        let root = root(&t);
        let digest = sha('f');
        {
            let db = Index::open(&root).unwrap();
            for p in 0..4096u32 {
                let partition = format!("{p:064x}");
                let owned = (0..256u32)
                    .map(|n| (format!("{p}:{n}"), digest.clone()))
                    .collect::<Vec<_>>();
                let refs = owned
                    .iter()
                    .map(|(a, b)| (a.as_str(), b.as_str()))
                    .collect::<Vec<_>>();
                let result = db
                    .commit_verified_partition(&partition, &digest, &refs)
                    .unwrap();
                assert_eq!(result.inserted, 256);
            }
        }
        let db = Index::open(&root).unwrap();
        let mut partitions = 0usize;
        db.visit_partition_ids(|_| {
            partitions += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(partitions, 4096);
        assert_eq!(db.get("4095:255").unwrap(), Some(digest));
    }
    #[test]
    fn batch_is_bounded_to_existing_partition_size() {
        let t = TempDir::new().unwrap();
        let idx = Index::open(root(&t)).unwrap();
        let a = sha('a');
        let b = sha('b');
        let keys = (0..257)
            .map(|i| (format!("x:{i}"), a.clone()))
            .collect::<Vec<_>>();
        let borrowed = keys
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            idx.commit_verified_partition(&a, &b, &borrowed),
            Err(Error::InvalidInput)
        );
    }
}

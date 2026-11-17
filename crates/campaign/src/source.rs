//! Case sources visited in bounded batches. The campaign makes several
//! ordered passes; a file source re-reads and re-hashes the file on each
//! pass and aborts if its bytes changed between passes. A cached source
//! decodes the JSON at most once and serves later passes (and later runs)
//! from a verified binary cache.
use crate::campaign::CampaignError;
use crate::fingerprint::{
    decode_case, decode_header, decode_trailer, encode_case, encode_header, encode_trailer, Sink,
    NORMALIZED_SCHEMA,
};
use crate::input::{CaseReader, HashingReader};
use crate::MarketCase;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub type Visitor<'a> = dyn FnMut(&[MarketCase]) -> Result<(), CampaignError> + 'a;

pub trait CaseSource {
    fn visit(&mut self, f: &mut Visitor<'_>) -> Result<(), CampaignError>;
    /// SHA-256 of the raw input bytes, if the source is a file.
    fn raw_sha256(&self) -> Option<String>;
    fn raw_bytes_read(&self) -> u64 {
        0
    }
    fn cache_bytes_read(&self) -> u64 {
        0
    }
    fn cache_status(&self) -> Option<CacheStatus> {
        None
    }
}

pub struct VecSource<'a> {
    pub cases: &'a [MarketCase],
    pub batch: usize,
}
impl CaseSource for VecSource<'_> {
    fn visit(&mut self, f: &mut Visitor<'_>) -> Result<(), CampaignError> {
        for chunk in self.cases.chunks(self.batch.max(1)) {
            f(chunk)?;
        }
        Ok(())
    }
    fn raw_sha256(&self) -> Option<String> {
        None
    }
}

/// Streaming JSONL source. Memory: one batch of cases.
pub struct FileSource {
    path: PathBuf,
    batch: usize,
    raw: Option<String>,
    bytes_read: u64,
}
impl FileSource {
    pub fn new(path: PathBuf, batch: usize) -> Self {
        Self {
            path,
            batch: batch.clamp(1, 4096),
            raw: None,
            bytes_read: 0,
        }
    }
}
impl CaseSource for FileSource {
    fn visit(&mut self, f: &mut Visitor<'_>) -> Result<(), CampaignError> {
        let file = File::open(&self.path).map_err(|e| CampaignError::Source(e.to_string()))?;
        let mut r = CaseReader::new(file);
        let mut buf = Vec::with_capacity(self.batch);
        for c in r.by_ref() {
            buf.push(c.map_err(|e| CampaignError::Source(format!("{e:?}")))?);
            if buf.len() == self.batch {
                f(&buf)?;
                buf.clear();
            }
        }
        if !buf.is_empty() {
            f(&buf)?;
        }
        let (sha, n) = r
            .finish()
            .map_err(|e| CampaignError::Source(format!("{e:?}")))?;
        self.bytes_read += n;
        match &self.raw {
            None => self.raw = Some(sha),
            Some(prev) if *prev != sha => return Err(CampaignError::SourceChanged),
            _ => {}
        }
        Ok(())
    }
    fn raw_sha256(&self) -> Option<String> {
        self.raw.clone()
    }
    fn raw_bytes_read(&self) -> u64 {
        self.bytes_read
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheStatus {
    /// A verified cache for these exact raw bytes existed; JSON was not decoded.
    Hit,
    /// JSON was decoded once (first pass) and the cache was written.
    Built,
}

/// Sidecar `<raw_sha256>.<schema>.json`; its presence marks the cache complete.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheMeta {
    pub schema: String,
    pub crate_version: String,
    pub raw_sha256: String,
    pub normalized_sha256: String,
    pub cases: u64,
    pub bytes: u64,
}

/// JSONL source backed by a content-addressed binary case cache.
///
/// Key: SHA-256 of the raw file + normalized schema + crate version, so a
/// different file, schema or decoder version never reuses a cache. The cache
/// bytes are exactly the canonical stream hashed by `normalized_input_sha256`;
/// every cache pass re-hashes them and must reproduce the recorded value or
/// the run aborts. The raw file's size and mtime are re-checked before and
/// after every pass (`SourceChanged`). Memory: one batch of cases.
pub struct CachedSource {
    path: PathBuf,
    dir: PathBuf,
    batch: usize,
    stamp: Option<(u64, Option<SystemTime>)>,
    meta: Option<CacheMeta>,
    raw_bytes: u64,
    cache_bytes: u64,
    status: Option<CacheStatus>,
}

struct Tee {
    w: BufWriter<File>,
    h: Sha256,
    bytes: u64,
    err: Option<std::io::Error>,
}
impl Sink for Tee {
    fn put(&mut self, b: &[u8]) {
        if self.err.is_none() {
            if let Err(e) = self.w.write_all(b) {
                self.err = Some(e);
            }
        }
        Digest::update(&mut self.h, b);
        self.bytes += b.len() as u64;
    }
}

fn src_err(e: impl std::fmt::Display) -> CampaignError {
    CampaignError::Source(e.to_string())
}

impl CachedSource {
    pub fn new(path: PathBuf, cache_dir: PathBuf, batch: usize) -> Self {
        Self {
            path,
            dir: cache_dir,
            batch: batch.clamp(1, 4096),
            stamp: None,
            meta: None,
            raw_bytes: 0,
            cache_bytes: 0,
            status: None,
        }
    }
    fn file(&self, raw: &str, ext: &str) -> PathBuf {
        self.dir.join(format!("{raw}.{NORMALIZED_SCHEMA}.{ext}"))
    }
    fn check_unchanged(&mut self) -> Result<(), CampaignError> {
        let m = std::fs::metadata(&self.path).map_err(src_err)?;
        let now = (m.len(), m.modified().ok());
        match self.stamp {
            None => self.stamp = Some(now),
            Some(s) if s != now => return Err(CampaignError::SourceChanged),
            _ => {}
        }
        Ok(())
    }
    /// Hashes the raw file (no JSON decoding) and returns a valid cache, if any.
    fn lookup(&mut self) -> Result<(String, Option<CacheMeta>), CampaignError> {
        let mut h = HashingReader::new(File::open(&self.path).map_err(src_err)?);
        std::io::copy(&mut h, &mut std::io::sink()).map_err(src_err)?;
        let (raw, n) = h.finish();
        self.raw_bytes += n;
        let found = std::fs::read(self.file(&raw, "json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<CacheMeta>(&b).ok())
            .filter(|m| {
                m.schema == NORMALIZED_SCHEMA
                    && m.crate_version == env!("CARGO_PKG_VERSION")
                    && m.raw_sha256 == raw
                    && std::fs::metadata(self.file(&raw, "bin")).is_ok_and(|x| x.len() == m.bytes)
            });
        Ok((raw, found))
    }
    fn build(&mut self, raw: String, f: &mut Visitor<'_>) -> Result<CacheMeta, CampaignError> {
        std::fs::create_dir_all(&self.dir).map_err(src_err)?;
        let tmp = self.file(&raw, &format!("bin.{}.tmp", std::process::id()));
        let m = match self.build_into(&raw, &tmp, f) {
            Ok(m) => m,
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                return Err(e);
            }
        };
        // bin first, sidecar last: a crash in between leaves no valid sidecar.
        std::fs::rename(&tmp, self.file(&raw, "bin")).map_err(src_err)?;
        let meta_tmp = self.file(&raw, &format!("json.{}.tmp", std::process::id()));
        let json = serde_json::to_vec_pretty(&m).map_err(src_err)?;
        std::fs::write(&meta_tmp, json).map_err(src_err)?;
        std::fs::rename(&meta_tmp, self.file(&raw, "json")).map_err(src_err)?;
        Ok(m)
    }
    fn build_into(
        &mut self,
        raw: &str,
        tmp: &Path,
        f: &mut Visitor<'_>,
    ) -> Result<CacheMeta, CampaignError> {
        let mut tee = Tee {
            w: BufWriter::with_capacity(1 << 20, File::create(tmp).map_err(src_err)?),
            h: Sha256::new(),
            bytes: 0,
            err: None,
        };
        encode_header(&mut tee);
        let mut r = CaseReader::new(File::open(&self.path).map_err(src_err)?);
        let mut buf = Vec::with_capacity(self.batch);
        let mut cases = 0u64;
        for c in r.by_ref() {
            let c = c.map_err(|e| CampaignError::Source(format!("{e:?}")))?;
            encode_case(&mut tee, &c);
            cases += 1;
            buf.push(c);
            if buf.len() == self.batch {
                f(&buf)?;
                buf.clear();
            }
        }
        if !buf.is_empty() {
            f(&buf)?;
        }
        encode_trailer(&mut tee, cases);
        let (sha, n) = r
            .finish()
            .map_err(|e| CampaignError::Source(format!("{e:?}")))?;
        self.raw_bytes += n;
        if sha != raw {
            return Err(CampaignError::SourceChanged);
        }
        if let Some(e) = tee.err.take() {
            return Err(src_err(e));
        }
        let file = tee.w.into_inner().map_err(|e| src_err(e.error()))?;
        file.sync_all().map_err(src_err)?;
        Ok(CacheMeta {
            schema: NORMALIZED_SCHEMA.into(),
            crate_version: env!("CARGO_PKG_VERSION").into(),
            raw_sha256: raw.into(),
            normalized_sha256: format!("{:x}", tee.h.finalize()),
            cases,
            bytes: tee.bytes,
        })
    }
    fn read_cache(&mut self, m: &CacheMeta, f: &mut Visitor<'_>) -> Result<(), CampaignError> {
        let bin = self.file(&m.raw_sha256, "bin");
        let corrupt = |why: String| {
            CampaignError::Source(format!(
                "case cache {} failed verification ({why}); delete it to rebuild",
                bin.display()
            ))
        };
        let file = File::open(&bin).map_err(|e| corrupt(e.to_string()))?;
        let mut r = BufReader::with_capacity(1 << 20, HashingReader::new(file));
        decode_header(&mut r).map_err(|e| corrupt(e.to_string()))?;
        let mut buf = Vec::with_capacity(self.batch);
        for _ in 0..m.cases {
            let c = decode_case(&mut r).map_err(|e| corrupt(e.to_string()))?;
            c.validate().map_err(|e| corrupt(format!("{e:?}")))?;
            buf.push(c);
            if buf.len() == self.batch {
                f(&buf)?;
                buf.clear();
            }
        }
        if !buf.is_empty() {
            f(&buf)?;
        }
        let n = decode_trailer(&mut r).map_err(|e| corrupt(e.to_string()))?;
        let mut extra = [0u8; 1];
        if n != m.cases || r.read(&mut extra).map_err(|e| corrupt(e.to_string()))? != 0 {
            return Err(corrupt("case count or length".into()));
        }
        let (sha, bytes) = r.into_inner().finish();
        self.cache_bytes += bytes;
        if sha != m.normalized_sha256 {
            return Err(corrupt("normalized sha256 mismatch".into()));
        }
        Ok(())
    }
}

impl CaseSource for CachedSource {
    fn visit(&mut self, f: &mut Visitor<'_>) -> Result<(), CampaignError> {
        self.check_unchanged()?;
        if let Some(m) = self.meta.clone() {
            self.read_cache(&m, f)?;
        } else {
            let (raw, found) = self.lookup()?;
            let m = match found {
                Some(m) => {
                    self.status = Some(CacheStatus::Hit);
                    self.read_cache(&m, f)?;
                    m
                }
                None => {
                    self.status = Some(CacheStatus::Built);
                    self.build(raw, f)?
                }
            };
            self.meta = Some(m);
        }
        self.check_unchanged()
    }
    fn raw_sha256(&self) -> Option<String> {
        self.meta.as_ref().map(|m| m.raw_sha256.clone())
    }
    fn raw_bytes_read(&self) -> u64 {
        self.raw_bytes
    }
    fn cache_bytes_read(&self) -> u64 {
        self.cache_bytes
    }
    fn cache_status(&self) -> Option<CacheStatus> {
        self.status
    }
}

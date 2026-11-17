//! Case sources visited in bounded batches. The campaign makes several
//! ordered passes; a file source re-reads and re-hashes the file on each
//! pass and aborts if its bytes changed between passes.
use crate::campaign::CampaignError;
use crate::input::CaseReader;
use crate::MarketCase;
use std::fs::File;
use std::path::PathBuf;

pub type Visitor<'a> = dyn FnMut(&[MarketCase]) -> Result<(), CampaignError> + 'a;

pub trait CaseSource {
    fn visit(&mut self, f: &mut Visitor<'_>) -> Result<(), CampaignError>;
    /// SHA-256 of the raw input bytes, if the source is a file.
    fn raw_sha256(&self) -> Option<String>;
    fn raw_bytes_read(&self) -> u64 {
        0
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

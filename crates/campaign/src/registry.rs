//! Durable experiment registry enforcing single-use holdouts.
//!
//! * Exclusive: `registry.lock` is created with `create_new`, which is
//!   atomic across processes on NTFS and POSIX filesystems. A second run
//!   (same or another process) fails with `Busy` instead of racing. A lock
//!   left by a crashed process is never auto-broken; the operator must
//!   confirm no run is active and delete it (`StaleLock` explains this).
//! * Exposure is tracked per holdout MARKET (`market@expiration_ns`), not per
//!   grid or dataset hash: any overlap is detected even if the grid, the
//!   config, or other parts of the dataset changed.
//! * A run record is written `Started` (atomically: `create_new` temp,
//!   fsync, rename, directory fsync) BEFORE the holdout is evaluated and is
//!   rewritten `Completed` afterwards. A crash leaves `Started`, which still
//!   counts as exposure and is reported as incomplete.
//! * Records that fail to parse make the registry refuse to operate
//!   (fail closed); temporaries from interrupted writes are ignored because
//!   they were never renamed into place.
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

const LOCK: &str = "registry.lock";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunState {
    Started,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Purpose {
    /// First and only evaluation of untouched holdout markets.
    Confirmatory,
    /// Operator-acknowledged reuse; never eligible for confirmation claims.
    ExploratoryReuse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub sequence: u64,
    pub experiment_sha256: String,
    pub normalized_input_sha256: String,
    pub raw_input_sha256: Option<String>,
    pub grid_sha256: String,
    pub code_revision: String,
    pub purpose: Purpose,
    pub state: RunState,
    pub holdout_markets: Vec<String>,
    pub report_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    Busy,
    StaleLock(PathBuf),
    Corrupt(PathBuf),
    Io(String),
}
fn io(e: std::io::Error) -> RegistryError {
    RegistryError::Io(e.to_string())
}

/// Prior use of any requested holdout market.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Exposure {
    pub overlapping_markets: usize,
    pub prior_runs: Vec<(u64, String, RunState)>,
    pub incomplete_prior_runs: usize,
}
impl Exposure {
    pub fn is_fresh(&self) -> bool {
        self.overlapping_markets == 0
    }
}

pub struct Registry {
    dir: PathBuf,
    lock: PathBuf,
}

impl Drop for Registry {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.lock);
    }
}

fn sync_dir(d: &Path) {
    // Directory fsync is required on POSIX for rename durability; it is
    // not supported on Windows, where NTFS journals the rename.
    #[cfg(unix)]
    if let Ok(f) = fs::File::open(d) {
        let _ = f.sync_all();
    }
    #[cfg(not(unix))]
    let _ = d;
}

fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), RegistryError> {
    let tmp = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    let _ = fs::remove_file(&tmp);
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .map_err(io)?;
        f.write_all(bytes).map_err(io)?;
        f.sync_all().map_err(io)?;
    }
    fs::rename(&tmp, dir.join(name)).map_err(io)?;
    sync_dir(dir);
    Ok(())
}

impl Registry {
    pub fn open(dir: &Path) -> Result<Self, RegistryError> {
        fs::create_dir_all(dir.join("runs")).map_err(io)?;
        let lock = dir.join(LOCK);
        match OpenOptions::new().write(true).create_new(true).open(&lock) {
            Ok(mut f) => {
                let _ = writeln!(f, "pid={}", std::process::id());
                let _ = f.sync_all();
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                return Err(if fs::read_to_string(&lock).is_ok() {
                    RegistryError::Busy
                } else {
                    RegistryError::StaleLock(lock)
                })
            }
            Err(e) => return Err(io(e)),
        }
        let r = Self {
            dir: dir.to_path_buf(),
            lock,
        };
        r.records()?; // fail closed on corruption before anything else
        Ok(r)
    }

    pub fn records(&self) -> Result<Vec<RunRecord>, RegistryError> {
        let mut out = Vec::new();
        for e in fs::read_dir(self.dir.join("runs")).map_err(io)? {
            let p = e.map_err(io)?.path();
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.starts_with('.') || !name.ends_with(".json") {
                continue;
            }
            let bytes = fs::read(&p).map_err(io)?;
            let rec: RunRecord =
                serde_json::from_slice(&bytes).map_err(|_| RegistryError::Corrupt(p.clone()))?;
            out.push(rec);
        }
        out.sort_by_key(|r| r.sequence);
        Ok(out)
    }

    pub fn exposure(&self, holdout: &[String]) -> Result<Exposure, RegistryError> {
        let want: HashSet<&str> = holdout.iter().map(String::as_str).collect();
        let mut x = Exposure::default();
        let mut hit: HashSet<&str> = HashSet::new();
        let recs = self.records()?;
        for r in &recs {
            let n = r
                .holdout_markets
                .iter()
                .filter(|m| want.contains(m.as_str()))
                .count();
            if n > 0 {
                for m in &r.holdout_markets {
                    if let Some(k) = want.get(m.as_str()) {
                        hit.insert(k);
                    }
                }
                x.prior_runs
                    .push((r.sequence, r.experiment_sha256.clone(), r.state));
                if r.state == RunState::Started {
                    x.incomplete_prior_runs += 1;
                }
            }
        }
        x.overlapping_markets = hit.len();
        Ok(x)
    }

    /// Records the holdout as exposed. Must be called before evaluation.
    pub fn begin(&self, mut rec: RunRecord) -> Result<RunHandle, RegistryError> {
        rec.sequence = self.records()?.last().map_or(1, |r| r.sequence + 1);
        rec.state = RunState::Started;
        rec.report_sha256 = None;
        let name = format!(
            "{:08}-{}.json",
            rec.sequence,
            &rec.experiment_sha256[..16.min(rec.experiment_sha256.len())]
        );
        write_atomic(
            &self.dir.join("runs"),
            &name,
            &serde_json::to_vec_pretty(&rec).expect("ser"),
        )?;
        Ok(RunHandle { name, rec })
    }

    pub fn complete(
        &self,
        mut h: RunHandle,
        report_sha256: String,
    ) -> Result<RunRecord, RegistryError> {
        h.rec.state = RunState::Completed;
        h.rec.report_sha256 = Some(report_sha256);
        write_atomic(
            &self.dir.join("runs"),
            &h.name,
            &serde_json::to_vec_pretty(&h.rec).expect("ser"),
        )?;
        Ok(h.rec)
    }
}

pub struct RunHandle {
    name: String,
    rec: RunRecord,
}
impl RunHandle {
    pub fn sequence(&self) -> u64 {
        self.rec.sequence
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(n: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("phoenix-reg-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }
    fn rec(exp: &str, grid: &str, hold: &[&str]) -> RunRecord {
        RunRecord {
            sequence: 0,
            experiment_sha256: exp.repeat(64 / exp.len()),
            normalized_input_sha256: "n".repeat(64),
            raw_input_sha256: None,
            grid_sha256: grid.into(),
            code_revision: "t".into(),
            purpose: Purpose::Confirmatory,
            state: RunState::Completed,
            holdout_markets: hold.iter().map(|s| s.to_string()).collect(),
            report_sha256: None,
        }
    }

    #[test]
    fn repeated_same_grid_and_altered_config_are_both_detected() {
        let d = dir("repeat");
        let r = Registry::open(&d).unwrap();
        let hold: Vec<String> = ["A@1", "B@2"].map(String::from).to_vec();
        assert!(r.exposure(&hold).unwrap().is_fresh());
        let h = r.begin(rec("a", "g1", &["A@1", "B@2"])).unwrap();
        r.complete(h, "r".into()).unwrap();
        // Identical grid again: still reuse.
        let x = r.exposure(&hold).unwrap();
        assert_eq!((x.overlapping_markets, x.prior_runs.len()), (2, 1));
        // Different grid/config but one overlapping market: reuse.
        let partial = vec!["B@2".to_string(), "C@3".to_string()];
        assert_eq!(r.exposure(&partial).unwrap().overlapping_markets, 1);
        // Disjoint holdout: fresh.
        assert!(r.exposure(&["D@4".to_string()]).unwrap().is_fresh());
        drop(r);
        // Persists across reopen.
        let r2 = Registry::open(&d).unwrap();
        assert_eq!(r2.records().unwrap()[0].state, RunState::Completed);
    }

    #[test]
    fn concurrent_open_is_refused_until_released() {
        let d = dir("conc");
        let a = Registry::open(&d).unwrap();
        let results: Vec<_> = (0..4)
            .map(|_| {
                let d = d.clone();
                std::thread::spawn(move || Registry::open(&d).err())
            })
            .map(|t| t.join().unwrap())
            .collect();
        assert!(results.iter().all(|e| *e == Some(RegistryError::Busy)));
        drop(a);
        assert!(Registry::open(&d).is_ok());
    }

    #[test]
    fn crash_mid_run_counts_as_exposure_and_torn_writes_are_ignored() {
        let d = dir("crash");
        let r = Registry::open(&d).unwrap();
        let h = r.begin(rec("b", "g", &["X@9"])).unwrap();
        assert_eq!(h.sequence(), 1);
        // Simulated crash: handle dropped without complete().
        drop(h);
        // Simulated torn write of a later record: temp never renamed.
        fs::write(d.join("runs").join(".00000002-x.json.999.tmp"), b"{partial").unwrap();
        let x = r.exposure(&["X@9".to_string()]).unwrap();
        assert_eq!((x.overlapping_markets, x.incomplete_prior_runs), (1, 1));
        let h2 = r.begin(rec("c", "g", &["Y@1"])).unwrap();
        assert_eq!(h2.sequence(), 2);
    }

    #[test]
    fn corrupt_records_fail_closed() {
        let d = dir("corrupt");
        drop(Registry::open(&d).unwrap());
        fs::write(d.join("runs").join("00000001-bad.json"), b"not json").unwrap();
        assert!(matches!(Registry::open(&d), Err(RegistryError::Corrupt(_))));
        // The failed open must not leave a lock behind.
        fs::remove_file(d.join("runs").join("00000001-bad.json")).unwrap();
        assert!(Registry::open(&d).is_ok());
    }
}

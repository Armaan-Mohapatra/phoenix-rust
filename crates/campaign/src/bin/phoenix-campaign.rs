//! Offline, paper-only research campaign runner. Never contacts an exchange.
//!
//!   phoenix-campaign --scenario efficient|lagged_quotes|shaded|peer_lead \
//!       [--assets 3] [--days 10] [--seed 1] --out DIR [--threads 2]
//!   phoenix-campaign --input markets.jsonl --out DIR [--threads 2]
//!       [--holdout-ledger FILE] [--acknowledge-holdout-reuse]
//!
//! The input file is opened read-only. DIR must be new or empty and must not
//! be inside a protected original-data root. Outputs are written atomically.
use phoenix_campaign::campaign::{dataset_sha256, grid_sha256, run, CampaignConfig};
use phoenix_campaign::input::read_cases;
use phoenix_campaign::strategies::default_grid;
use phoenix_campaign::synthetic::{generate, Scenario};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const PROTECTED: [&str; 2] = [r"e:\phoenixresearch", r"e:\tradingnextdata"];

fn normalized(p: &Path) -> String {
    p.to_string_lossy().replace('/', "\\").to_ascii_lowercase()
}

fn protected(p: &Path) -> bool {
    let s = normalized(p);
    PROTECTED
        .iter()
        .any(|r| s == *r || s.starts_with(&format!("{r}\\")))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)
}

fn arg(args: &[String], k: &str) -> Option<String> {
    args.iter()
        .position(|a| a == k)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    match real_main(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("phoenix-campaign: {e}");
            ExitCode::from(2)
        }
    }
}

fn real_main(args: &[String]) -> Result<(), String> {
    let out = PathBuf::from(arg(args, "--out").ok_or("--out DIR is required")?);
    if protected(&out) {
        return Err(format!(
            "refusing to write inside protected root: {}",
            out.display()
        ));
    }
    if out.exists()
        && fs::read_dir(&out)
            .map_err(|e| e.to_string())?
            .next()
            .is_some()
    {
        return Err(format!("output directory is not empty: {}", out.display()));
    }
    let num = |k: &str, d: u64| -> Result<u64, String> {
        arg(args, k).map_or(Ok(d), |v| {
            v.parse().map_err(|_| format!("{k} must be an integer"))
        })
    };
    let cases = if let Some(input) = arg(args, "--input") {
        let p = PathBuf::from(&input);
        let canon_in = fs::canonicalize(&p).map_err(|e| format!("{input}: {e}"))?;
        fs::create_dir_all(&out).map_err(|e| e.to_string())?;
        let canon_out = fs::canonicalize(&out).map_err(|e| e.to_string())?;
        if Some(canon_out.as_path()) == canon_in.parent() || canon_out == canon_in {
            return Err("output directory must not be the input file's directory".into());
        }
        let f = fs::File::open(&canon_in).map_err(|e| e.to_string())?;
        read_cases(f).map_err(|e| format!("{e:?}"))?
    } else {
        let name = arg(args, "--scenario").ok_or("--scenario or --input is required")?;
        let sc = Scenario::by_name(
            &name,
            num("--assets", 3)? as usize,
            num("--days", 10)? as u32,
            num("--seed", 1)?,
        )
        .ok_or_else(|| format!("unknown scenario {name}"))?;
        generate(&sc)
    };
    let grid = default_grid();
    let cfg = CampaignConfig {
        threads: num("--threads", 2)? as usize,
        ..Default::default()
    };

    if let Some(ledger) = arg(args, "--holdout-ledger") {
        let key = format!("{} {}", dataset_sha256(&cases), grid_sha256(&grid));
        let prior = fs::read_to_string(&ledger).unwrap_or_default();
        let ds = dataset_sha256(&cases);
        let reused = prior.lines().any(|l| l.starts_with(&ds) && l != key);
        if reused && !args.iter().any(|a| a == "--acknowledge-holdout-reuse") {
            return Err(
                "this dataset's holdout was already used with a different grid; \
                        results would no longer be out-of-sample (pass \
                        --acknowledge-holdout-reuse to record the reuse)"
                    .into(),
            );
        }
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&ledger)
            .map_err(|e| e.to_string())?;
        writeln!(f, "{key}{}", if reused { " REUSED" } else { "" }).map_err(|e| e.to_string())?;
    }

    let report = run(&cases, &grid, &cfg).map_err(|e| format!("{e:?}"))?;
    fs::create_dir_all(&out).map_err(|e| e.to_string())?;
    let json = serde_json::to_vec_pretty(&report).map_err(|e| e.to_string())?;
    write_atomic(&out.join("report.json"), &json).map_err(|e| e.to_string())?;

    let mut s = String::new();
    s += &format!(
        "provenance: {:?}\ndataset_sha256: {}\n",
        report.provenance, report.dataset_sha256
    );
    s += &format!(
        "splits: discovery={} validation={} holdout={} embargoed={}\n",
        report.split_counts.discovery,
        report.split_counts.validation,
        report.split_counts.holdout,
        report.split_counts.embargoed
    );
    s += &format!(
        "configs tried={} selected={} holdout evaluations={} candidates={}\n",
        report.configs_tried,
        report.selected_for_validation,
        report.holdout_evaluations,
        report.candidates.len()
    );
    s += &format!(
        "resources: cases={} snapshots={} decisions={} wall_ms={} decisions/s={:.0}\n\n",
        report.resources.cases,
        report.resources.snapshots,
        report.resources.decisions_evaluated,
        report.resources.wall_ms,
        report.resources.decisions_per_second
    );
    s += "family                   tte  disc_trades disc_net_usd  disc_z  verdict\n";
    for r in &report.results {
        s += &format!(
            "{:<24} {:>4} {:>11} {:>12.2} {:>7.2}  {:?}\n",
            format!("{:?}", r.config.family),
            r.config.tte_s,
            r.discovery.settled_trades,
            r.discovery.net_ticks as f64 / 10_000.0,
            r.discovery.test.map_or(f64::NAN, |t| t.z),
            r.verdict
        );
    }
    write_atomic(&out.join("summary.txt"), s.as_bytes()).map_err(|e| e.to_string())?;
    print!("{s}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn protected_roots_are_refused_case_insensitively() {
        assert!(protected(Path::new(r"E:\PhoenixResearch")));
        assert!(protected(Path::new(r"e:\phoenixresearch\out")));
        assert!(protected(Path::new("E:/TradingNextData/x")));
        assert!(!protected(Path::new(r"E:\PhoenixResearchOutputs")));
        assert!(!protected(Path::new("/tmp/out")));
    }
}

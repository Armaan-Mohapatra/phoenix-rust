//! Offline, paper-only research campaign runner. Never contacts an exchange.
//!
//! Synthetic engine check (in memory):
//!   phoenix-campaign --scenario efficient|lagged_quotes|shaded|peer_lead \
//!       [--assets 3] [--days 10] [--seed 1] --out DIR [--threads 2]
//! Export a synthetic JSONL fixture (for streaming tests/benchmarks):
//!   phoenix-campaign --export-scenario NAME [--assets/--days/--seed] --out DIR
//! Streaming file input (bounded memory):
//!   phoenix-campaign --input markets.jsonl --out DIR [--threads 2] [--batch 256]
//!       [--registry DIR [--confirmatory] [--allow-exploratory-reuse]]
//! Authentic replay report -> campaign JSONL (one market; read-only inputs):
//!   phoenix-campaign --adapt-replay REPORT.json --meta META.json \
//!       --references REFS.jsonl [--perp PERP.jsonl] [--settlement S.json] \
//!       [--allow-unverified-books] --out DIR
//!   (REFS/PERP lines: {"received_ns":..,"value":..}; see adapter.rs.)
//!
//! Without `--confirmatory` a real-data run is Exploratory. With it, a
//! registry is mandatory: a fresh holdout yields UntouchedConfirmatory; any
//! previously exposed holdout market aborts unless
//! `--allow-exploratory-reuse`, which downgrades the run to Exploratory and
//! records the reuse. Outputs go to a new or empty directory validated by
//! `paths::prepare_output_dir`; files are never overwritten.
use phoenix_campaign::campaign::{
    grid_sha256, run, run_source, CampaignConfig, EvidenceClass, Report,
};
use phoenix_campaign::fingerprint::{code_revision, experiment_sha256, Fingerprinter};
use phoenix_campaign::input::to_jsonl_line;
use phoenix_campaign::paths::{
    prepare_output_dir, protected_roots, safe_state_dir, write_new_file, PathError,
};
use phoenix_campaign::registry::{Purpose, Registry, RunRecord, RunState};
use phoenix_campaign::source::{CaseSource, FileSource};
use phoenix_campaign::strategies::default_grid;
use phoenix_campaign::synthetic::{generate, Scenario};
use phoenix_campaign::Provenance;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

fn arg(args: &[String], k: &str) -> Option<String> {
    args.iter()
        .position(|a| a == k)
        .and_then(|i| args.get(i + 1))
        .cloned()
}
fn flag(args: &[String], k: &str) -> bool {
    args.iter().any(|a| a == k)
}
fn pe(e: PathError) -> String {
    format!("{e:?}")
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

fn summary(r: &Report) -> String {
    let mut s = String::new();
    s += &format!(
        "evidence_class: {:?}  confirmatory_eligible: {}\n",
        r.evidence_class, r.confirmatory_eligible
    );
    s += &format!(
        "raw_input_sha256: {}\n",
        r.raw_input_sha256.as_deref().unwrap_or("n/a (in-memory)")
    );
    s += &format!(
        "normalized_input_sha256: {}\nexperiment_sha256: {}\ncode: {}\n",
        r.normalized_input_sha256, r.experiment_sha256, r.code_revision
    );
    let c = &r.split_counts;
    s += &format!(
        "splits: discovery={} validation={} holdout={} embargoed={}\n",
        c.discovery, c.validation, c.holdout, c.embargoed
    );
    s += &format!(
        "configs tried={} selected={} holdout_touched={} holdout evaluations={} passed_all_gates={}\n",
        r.configs_tried, r.selected_for_validation, r.holdout_touched, r.holdout_evaluations, r.candidates.len()
    );
    let x = &r.resources;
    s += &format!(
        "resources: cases={} snapshots={} passes={} raw_bytes_read={} decisions={} wall_ms={} decisions/s={:.0}\n\n",
        x.cases, x.snapshots, x.passes, x.raw_bytes_read, x.decisions_evaluated, x.wall_ms, x.decisions_per_second
    );
    s += "family                   tte  disc_trades disc_net_usd  disc_z  verdict\n";
    for row in &r.results {
        s += &format!(
            "{:<24} {:>4} {:>11} {:>12.2} {:>7.2}  {:?}\n",
            format!("{:?}", row.config.family),
            row.config.tte_s,
            row.discovery.settled_trades,
            row.discovery.net_ticks as f64 / 10_000.0,
            row.discovery.test.map_or(f64::NAN, |t| t.z),
            row.verdict
        );
    }
    s
}

fn real_main(args: &[String]) -> Result<(), String> {
    let out_req = PathBuf::from(arg(args, "--out").ok_or("--out DIR is required")?);
    let num = |k: &str, d: u64| -> Result<u64, String> {
        arg(args, k).map_or(Ok(d), |v| {
            v.parse()
                .map_err(|_| format!("{k} must be a non-negative integer"))
        })
    };
    let cfg = CampaignConfig {
        threads: num("--threads", 2)? as usize,
        batch_size: num("--batch", 256)? as usize,
        ..Default::default()
    };
    cfg.validate().map_err(|e| format!("{e:?}"))?;
    let scenario = |name: &str| {
        Scenario::by_name(
            name,
            num("--assets", 3)? as usize,
            num("--days", 10)? as u32,
            num("--seed", 1)?,
        )
        .ok_or_else(|| format!("unknown scenario {name}"))
    };
    let roots = protected_roots();
    let grid = default_grid();

    if let Some(name) = arg(args, "--export-scenario") {
        let out = prepare_output_dir(&out_req, &roots).map_err(pe)?;
        let text: String = generate(&scenario(&name)?)
            .iter()
            .map(|c| to_jsonl_line(c) + "\n")
            .collect();
        let p = write_new_file(&out, "cases.jsonl", text.as_bytes()).map_err(pe)?;
        println!(
            "{} bytes sha256={:x} -> {}",
            text.len(),
            Sha256::digest(text.as_bytes()),
            p.display()
        );
        return Ok(());
    }

    if let Some(report) = arg(args, "--adapt-replay") {
        use phoenix_campaign::adapter::{adapt, AdaptPolicy, MarketMeta, Observation, Settlement};
        let read = |k: &str| -> Result<Vec<u8>, String> {
            let p = arg(args, k).ok_or(format!("{k} FILE is required"))?;
            fs::read(&p).map_err(|e| format!("{p}: {e}"))
        };
        let obs = |bytes: &[u8]| -> Result<Vec<Observation>, String> {
            std::str::from_utf8(bytes)
                .map_err(|e| e.to_string())?
                .lines()
                .filter(|l| !l.trim().is_empty())
                .enumerate()
                .map(|(i, l)| serde_json::from_str(l).map_err(|e| format!("line {}: {e}", i + 1)))
                .collect()
        };
        let report_bytes = fs::read(&report).map_err(|e| format!("{report}: {e}"))?;
        let meta: MarketMeta =
            serde_json::from_slice(&read("--meta")?).map_err(|e| e.to_string())?;
        let refs = obs(&read("--references")?)?;
        let perp = if arg(args, "--perp").is_some() {
            obs(&read("--perp")?)?
        } else {
            Vec::new()
        };
        let settle: Option<Settlement> = match arg(args, "--settlement") {
            Some(_) => {
                Some(serde_json::from_slice(&read("--settlement")?).map_err(|e| e.to_string())?)
            }
            None => None,
        };
        let policy = AdaptPolicy {
            allow_unverified_books: flag(args, "--allow-unverified-books"),
            ..Default::default()
        };
        let (case, rep) = adapt(&report_bytes, &meta, &refs, &perp, settle.as_ref(), &policy)
            .map_err(|e| format!("{e:?}"))?;
        let out = prepare_output_dir(&out_req, &roots).map_err(pe)?;
        write_new_file(&out, "case.jsonl", (to_jsonl_line(&case) + "\n").as_bytes()).map_err(pe)?;
        write_new_file(
            &out,
            "adapt_report.json",
            &serde_json::to_vec_pretty(&rep).map_err(|e| e.to_string())?,
        )
        .map_err(pe)?;
        println!(
            "{}",
            serde_json::to_string(&rep).map_err(|e| e.to_string())?
        );
        return Ok(());
    }

    let Some(input) = arg(args, "--input") else {
        let name = arg(args, "--scenario")
            .ok_or("--scenario, --export-scenario or --input is required")?;
        let report =
            run(&generate(&scenario(&name)?), &grid, &cfg).map_err(|e| format!("{e:?}"))?;
        let out = prepare_output_dir(&out_req, &roots).map_err(pe)?;
        let json = serde_json::to_vec_pretty(&report).map_err(|e| e.to_string())?;
        write_new_file(&out, "report.json", &json).map_err(pe)?;
        write_new_file(&out, "summary.txt", summary(&report).as_bytes()).map_err(pe)?;
        print!("{}", summary(&report));
        return Ok(());
    };

    let input = fs::canonicalize(&input).map_err(|e| format!("{input}: {e}"))?;
    let out = prepare_output_dir(&out_req, &roots).map_err(pe)?;
    if input.parent() == Some(out.as_path()) {
        return Err("output directory must not be the input file's directory".into());
    }
    let confirmatory = flag(args, "--confirmatory");
    let allow_reuse = flag(args, "--allow-exploratory-reuse");
    let registry = match arg(args, "--registry") {
        Some(d) => {
            let canon = safe_state_dir(&PathBuf::from(d), &roots).map_err(pe)?;
            Some(Registry::open(&canon).map_err(|e| match e {
                phoenix_campaign::registry::RegistryError::Busy => format!(
                    "registry {} is locked by another run. If no phoenix-campaign process is \
                     running (e.g. after a crash), confirm that and delete {}",
                    canon.display(),
                    canon.join("registry.lock").display()
                ),
                other => format!("registry: {other:?}"),
            })?)
        }
        None if confirmatory => return Err("--confirmatory requires --registry DIR".into()),
        None => None,
    };
    let mut src = FileSource::new(input, cfg.batch_size);
    let code = code_revision();
    let grid_sha = grid_sha256(&grid);
    // Identity pass so the registry record names the exact experiment.
    let mut fp = Fingerprinter::default();
    let mut synthetic = false;
    let mut unverified = false;
    src.visit(&mut |b| {
        for c in b {
            synthetic |= c.provenance == Provenance::Synthetic;
            unverified |= c.provenance == Provenance::OperatorUnverified;
            fp.push(c);
        }
        Ok(())
    })
    .map_err(|e| format!("{e:?}"))?;
    let norm = fp.finish();
    let exp = experiment_sha256(&norm, &grid_sha, &cfg.economic_json(), &code);
    let raw = src.raw_sha256();
    let mut class = match (synthetic, confirmatory) {
        (true, true) => return Err("synthetic input can never be confirmatory".into()),
        (false, true) if unverified => return Err(
            "input contains books not certified executable (operator_unverified); exploratory only"
                .into(),
        ),
        (true, false) => EvidenceClass::SyntheticEngineCheck,
        (false, true) => EvidenceClass::UntouchedConfirmatory,
        (false, false) => EvidenceClass::Exploratory,
    };
    let start_class = class;
    let mut handle = None;
    let mut hook = |keys: &[String]| -> Result<(), String> {
        let Some(reg) = registry.as_ref() else {
            return Ok(());
        };
        let x = reg.exposure(keys).map_err(|e| format!("{e:?}"))?;
        let mut purpose = if class == EvidenceClass::UntouchedConfirmatory {
            Purpose::Confirmatory
        } else {
            Purpose::ExploratoryReuse
        };
        if !x.is_fresh() {
            if !allow_reuse {
                return Err(format!(
                    "{} holdout markets were already exposed by prior runs {:?} ({} incomplete); \
                     pass --allow-exploratory-reuse to continue as Exploratory",
                    x.overlapping_markets, x.prior_runs, x.incomplete_prior_runs
                ));
            }
            purpose = Purpose::ExploratoryReuse;
            if class == EvidenceClass::UntouchedConfirmatory {
                class = EvidenceClass::Exploratory;
            }
        }
        handle = Some(
            reg.begin(RunRecord {
                sequence: 0,
                experiment_sha256: exp.clone(),
                normalized_input_sha256: norm.clone(),
                raw_input_sha256: raw.clone(),
                grid_sha256: grid_sha.clone(),
                code_revision: code.clone(),
                purpose,
                state: RunState::Started,
                holdout_markets: keys.to_vec(),
                report_sha256: None,
            })
            .map_err(|e| format!("{e:?}"))?,
        );
        Ok(())
    };
    let mut r =
        run_source(&mut src, &grid, &cfg, start_class, &mut hook).map_err(|e| format!("{e:?}"))?;
    if r.normalized_input_sha256 != norm || r.raw_input_sha256 != raw {
        return Err("input changed between identity pass and evaluation".into());
    }
    if class != r.evidence_class {
        r.evidence_class = class;
        r.confirmatory_eligible = false;
    }
    let json = serde_json::to_vec_pretty(&r).map_err(|e| e.to_string())?;
    write_new_file(&out, "report.json", &json).map_err(pe)?;
    if let (Some(reg), Some(h)) = (registry.as_ref(), handle) {
        reg.complete(h, format!("{:x}", Sha256::digest(&json)))
            .map_err(|e| format!("{e:?}"))?;
    }
    write_new_file(&out, "summary.txt", summary(&r).as_bytes()).map_err(pe)?;
    print!("{}", summary(&r));
    Ok(())
}

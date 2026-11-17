//! Offline original-event audit; no source writes, network or order routes.
use chrono::DateTime;
use phoenix_source_loader::{forensic, replay, Request};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    env,
    fs::{self, File},
    io::Read,
    path::PathBuf,
};
use zip::ZipArchive;

fn run() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 7 {
        return Err("Usage: phoenix-source-audit TICKER START_UTC END_UTC MEMBERS.txt PART001.zip [PART002.zip ...]".into());
    }
    let ts = |raw: &str| {
        DateTime::parse_from_rfc3339(raw)
            .map_err(|e| e.to_string())?
            .timestamp_nanos_opt()
            .ok_or("timestamp overflow".to_owned())
    };
    let start = ts(&args[2])?;
    let end = ts(&args[3])?;
    let list = PathBuf::from(&args[4]);
    if fs::metadata(&list).map_err(|e| e.to_string())?.len() > 8192 {
        return Err("Member allowlist exceeds 8KiB".into());
    }
    let names = fs::read_to_string(list)
        .map_err(|e| e.to_string())?
        .lines()
        .filter(|s| !s.is_empty() && !s.starts_with('#'))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let zip_paths = args[5..].iter().map(PathBuf::from).collect::<Vec<_>>();
    if zip_paths.len() > 12 {
        return Err("Too many ZIPs".into());
    }
    let mut z = ZipArchive::new(File::open(&zip_paths[0]).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let mut source = z
        .by_name("archive/curated/curation_catalog.jsonl")
        .map_err(|e| e.to_string())?;
    if source.size() > 4 * 1024 * 1024 {
        return Err("Catalog too large".into());
    }
    let mut catalog = Vec::new();
    source
        .take(4 * 1024 * 1024 + 1)
        .read_to_end(&mut catalog)
        .map_err(|e| e.to_string())?;
    if catalog.len() > 4 * 1024 * 1024 {
        return Err("Catalog over bound".into());
    }
    let catalog_sha = format!("{:x}", Sha256::digest(&catalog));
    let request = Request {
        ticker: args[1].clone(),
        start_received_ns: start,
        end_received_ns: end,
        named_members: names,
        zip_paths,
    };
    let mut book = forensic::Book::new(&args[1]);
    let mut missing_snap = 0usize;
    let mut invalid_depth = 0usize;
    let mut session_changed = 0usize;
    let mut accepted_deltas = 0usize;
    let mut first_ns = None;
    let mut last_ns = None;
    let evidence = replay(&request, &catalog, |e, d| {
        first_ns.get_or_insert(e.received_ns);
        last_ns = Some(e.received_ns);
        match book.apply(e, d) {
            Ok(()) => {
                if matches!(&d.message, phoenix_kalshi_wire::Message::Delta(_)) {
                    accepted_deltas += 1;
                }
            }
            Err(forensic::Error::MissingSnapshot) => missing_snap += 1,
            Err(forensic::Error::NegativeDepth) => invalid_depth += 1,
            Err(forensic::Error::SessionChanged) => session_changed += 1,
            Err(x) => {
                return Err(phoenix_source_loader::Error::Wire(format!(
                    "forensic replay rejected source: {x:?}"
                )))
            }
        }
        Ok(())
    })
    .map_err(|e| format!("SOURCE_REPLAY_BLOCKED: {e:?}"))?;
    let result = json!({
        "status":"RESEARCH_ONLY",
        "catalog_sha256":catalog_sha,"historical":evidence,
        "first_receive_ns":first_ns,"last_receive_ns":last_ns,
        "book_state":format!("{:?}",book.status()),
        "deltas_applied_after_snapshot":accepted_deltas,
        "deltas_rejected_no_snapshot":missing_snap,
        "invalid_depth_count":invalid_depth,
        "session_reset_count":session_changed,
        "market_filtered_sequence_ambiguous":book.channel_sequence_ambiguous,
        "verified_executable_book":book.verified_executable(),
        "independent_reference_source_verified":false,
        "actual_exchange_fill_verified":false
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&result).map_err(|e| e.to_string())?
    );
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(2);
    }
}

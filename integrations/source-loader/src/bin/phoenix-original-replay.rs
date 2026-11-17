//! Read-only original-source replay; compact JSON goes to stdout only.
use chrono::DateTime;
use phoenix_source_loader::{pipeline, Request};
use sha2::{Digest, Sha256};
use std::{
    env,
    fs::{self, File},
    io::Read,
    path::PathBuf,
};
use zip::ZipArchive;
const MAX_CATALOG: usize = 4 * 1024 * 1024;
fn parse_time(s: &str) -> Result<i64, String> {
    DateTime::parse_from_rfc3339(s)
        .map_err(|e| e.to_string())?
        .timestamp_nanos_opt()
        .ok_or_else(|| "timestamp overflow".into())
}
fn run() -> Result<(), String> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() < 8 {
        return Err("Usage: phoenix-original-replay TICKER START_UTC DECISION_UTC MEMBERS.txt EXPECTED_CATALOG_SHA256 ZIP1.zip [ZIP2.zip ...]".into());
    }
    let ticker = args[1].clone();
    let start = parse_time(&args[2])?;
    let decision = parse_time(&args[3])?;
    if decision <= start || args[5].len() != 64 || !args[5].bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("Invalid decision window or expected catalog SHA256".into());
    }
    let allowlist = PathBuf::from(&args[4]);
    if fs::metadata(&allowlist).map_err(|e| e.to_string())?.len() > 8192 {
        return Err("Allowlist exceeds 8KiB".into());
    }
    let named_members = fs::read_to_string(&allowlist)
        .map_err(|e| e.to_string())?
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.starts_with('#'))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if named_members.len() > 32 || named_members.is_empty() {
        return Err("Allowlist count invalid".into());
    }
    let zip_paths = args[6..].iter().map(PathBuf::from).collect::<Vec<_>>();
    if zip_paths.is_empty() || zip_paths.len() > 12 {
        return Err("ZIP count out of bounds".into());
    }
    let mut archive = ZipArchive::new(File::open(&zip_paths[0]).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let entry = archive
        .by_name("archive/curated/curation_catalog.jsonl")
        .map_err(|e| e.to_string())?;
    if entry.size() > MAX_CATALOG as u64 {
        return Err("Catalog exceeds bound".into());
    }
    let mut catalog = Vec::new();
    entry
        .take(MAX_CATALOG as u64 + 1)
        .read_to_end(&mut catalog)
        .map_err(|e| e.to_string())?;
    if catalog.len() > MAX_CATALOG {
        return Err("Catalog read too long".into());
    }
    let digest = format!("{:x}", Sha256::digest(&catalog));
    if !digest.eq_ignore_ascii_case(&args[5]) {
        return Err(format!("Source manifest SHA mismatch: observed {digest}"));
    }
    let request = Request {
        ticker,
        start_received_ns: start,
        end_received_ns: decision,
        named_members,
        zip_paths,
    };
    let result = pipeline::run(&request, &catalog, decision, &[decision])
        .map_err(|e| format!("REPLAY_SOURCE_BLOCKED: {e:?}"))?;
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

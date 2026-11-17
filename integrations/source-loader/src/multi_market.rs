//! Phase 7: bounded content-addressed multi-market historical RESEARCH.
//! Reuses pipeline::run and the verified source manifest/ZIP reader; no
//! recursive directory scans, settlement inference, or venue execution.
use crate::{catalog, pipeline, Request};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

const MAX_MARKETS: usize = 4;
const MAX_MEMBERS: usize = 32;
const MAX_ZIPS: usize = 12;
const MAX_REPLAY_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Asset {
    Btc,
    Eth,
    Sol,
    Xrp,
    DocumentedOther,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceAvailability {
    ManifestOnly,
    ReplayIncomplete,
    SourceDerivedProvisional,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SettlementAvailability {
    Unknown,
    DeclaredOfficialUnverified,
    DeclaredPrimaryUnverified,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSpec {
    pub ticker: String,
    pub event: String,
    pub series: String,
    pub asset: Asset,
    pub start_received_ns: i64,
    pub end_received_ns: i64,
    pub decision_ns: i64,
    /// Explicit exact source allowlist. Never infer ticker membership from a
    /// file name; original source reader checks ticker inside every row.
    pub named_members: Vec<String>,
    /// Documentary SHA identifiers are declarations, not authenticated rules.
    pub contract_rules_sha256: Option<String>,
    pub historical_fee_sha256: Option<String>,
    pub official_final_sha256: Option<String>,
    pub independent_primary_sha256: Option<String>,
    /// Mandatory for a family outside the separately documented four.
    pub additional_family_schema_sha256: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReusableMarketCheckpoint {
    pub schema_version: u32,
    pub ticker: String,
    pub fingerprint_sha256: String,
    pub manifest_sha256: String,
    pub report_json: String,
    pub report_sha256: String,
    pub engine_revision_sha256: String,
}
#[derive(Debug, Clone, Serialize)]
pub struct MarketBatchOutcome {
    pub ticker: String,
    pub event: String,
    pub series: String,
    pub asset: Asset,
    pub classification: SourceAvailability,
    pub settlement: SettlementAvailability,
    pub contract_rules_available: bool,
    pub historical_fee_available: bool,
    pub reused_prior_verified_report: bool,
    pub fingerprint_sha256: String,
    pub source_manifest_sha256: String,
    pub replay_sha256: Option<String>,
    pub replay_json: Option<String>,
    pub report_exclusions: Vec<String>,
    pub historical_execution_eligible: bool,
    pub independent_settlement_eligible: bool,
}
#[derive(Debug, Serialize)]
pub struct BatchOutcome {
    pub schema_version: u32,
    pub engine_revision_sha256: String,
    pub source_manifest_sha256: String,
    pub markets: Vec<MarketBatchOutcome>,
    pub next_checkpoints: Vec<ReusableMarketCheckpoint>,
    pub no_live_execution: bool,
    pub no_empirical_pnl: bool,
}
#[derive(Debug)]
pub enum Error {
    InvalidBatch,
    InvalidIdentity,
    InvalidTime,
    InvalidManifest,
    MissingMember(String),
    DuplicateMember(String),
    DuplicateMarket,
    UnsupportedFamily,
    HashMismatch,
    InvalidCheckpoint,
    Source(crate::Error),
    Replay(pipeline::Error),
    Encoding,
}
fn sha(input: &[u8]) -> String {
    format!("{:x}", Sha256::digest(input))
}
fn valid_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|x| x.is_ascii_hexdigit())
}
fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|x| x.is_ascii_alphanumeric() || matches!(x, b'-' | b'_' | b'.' | b':'))
}
fn require_hash(x: &Option<String>) -> bool {
    x.as_ref().map_or(true, |s| valid_hash(s))
}
fn verify_spec(s: &MarketSpec) -> Result<(), Error> {
    if !valid_name(&s.ticker) || !valid_name(&s.event) || !valid_name(&s.series) {
        return Err(Error::InvalidIdentity);
    }
    if s.start_received_ns <= 0
        || s.end_received_ns <= s.start_received_ns
        || s.decision_ns < s.start_received_ns
        || s.decision_ns > s.end_received_ns
    {
        return Err(Error::InvalidTime);
    }
    if s.named_members.is_empty()
        || s.named_members.len() > MAX_MEMBERS
        || !require_hash(&s.contract_rules_sha256)
        || !require_hash(&s.historical_fee_sha256)
        || !require_hash(&s.official_final_sha256)
        || !require_hash(&s.independent_primary_sha256)
        || !require_hash(&s.additional_family_schema_sha256)
    {
        return Err(Error::InvalidManifest);
    }
    let prefix = match s.asset {
        Asset::Btc => "KXBTC",
        Asset::Eth => "KXETH",
        Asset::Sol => "KXSOL",
        Asset::Xrp => "KXXRP",
        Asset::DocumentedOther => "",
    };
    if s.asset == Asset::DocumentedOther {
        if s.additional_family_schema_sha256.is_none() {
            return Err(Error::UnsupportedFamily);
        }
    } else if !s.series.starts_with(prefix) {
        // Never reuse BTC contract economics for another series.
        return Err(Error::UnsupportedFamily);
    }
    let mut unique = BTreeSet::new();
    for name in &s.named_members {
        if !unique.insert(name) {
            return Err(Error::DuplicateMember(name.clone()));
        }
    }
    Ok(())
}
fn fingerprint(
    s: &MarketSpec,
    catalog_sha: &str,
    revision: &str,
    entries: &BTreeMap<String, String>,
) -> Result<String, Error> {
    let mut hash = Sha256::new();
    for val in [
        s.ticker.as_str(),
        s.event.as_str(),
        s.series.as_str(),
        catalog_sha,
        revision,
    ] {
        hash.update((val.len() as u64).to_le_bytes());
        hash.update(val.as_bytes());
    }
    hash.update([s.asset as u8]);
    for stamp in [s.start_received_ns, s.decision_ns, s.end_received_ns] {
        hash.update(stamp.to_le_bytes());
    }
    for val in [
        &s.contract_rules_sha256,
        &s.historical_fee_sha256,
        &s.official_final_sha256,
        &s.independent_primary_sha256,
        &s.additional_family_schema_sha256,
    ] {
        if let Some(v) = val {
            hash.update([1]);
            hash.update(v.as_bytes());
        } else {
            hash.update([0]);
        }
    }
    let mut names = s.named_members.clone();
    names.sort();
    for name in names {
        let digest = entries
            .get(&name)
            .ok_or(Error::MissingMember(name.clone()))?;
        hash.update((name.len() as u64).to_le_bytes());
        hash.update(name.as_bytes());
        hash.update(digest.as_bytes());
    }
    Ok(format!("{:x}", hash.finalize()))
}
fn valid_reuse(
    c: &ReusableMarketCheckpoint,
    s: &MarketSpec,
    expected: &str,
    catalog_sha: &str,
    revision: &str,
    entries: &BTreeMap<String, String>,
) -> bool {
    if c.schema_version != 1
        || c.ticker != s.ticker
        || c.fingerprint_sha256 != expected
        || c.manifest_sha256 != catalog_sha
        || c.engine_revision_sha256 != revision
        || c.report_json.is_empty()
        || c.report_json.len() > MAX_REPLAY_BYTES
        || sha(c.report_json.as_bytes()) != c.report_sha256
    {
        return false;
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&c.report_json) else {
        return false;
    };
    if v.get("market_ticker").and_then(|x| x.as_str()) != Some(s.ticker.as_str())
        || v.get("catalog_sha256").and_then(|x| x.as_str()) != Some(catalog_sha)
    {
        return false;
    }
    let Some(parts) = v.get("partition_proofs").and_then(|x| x.as_array()) else {
        return false;
    };
    if parts.len() != s.named_members.len() {
        return false;
    }
    let mut accounted = BTreeSet::new();
    for part in parts {
        let Some(name) = part.get("member").and_then(|x| x.as_str()) else {
            return false;
        };
        let Some(observed) = part.get("sha256").and_then(|x| x.as_str()) else {
            return false;
        };
        if !s.named_members.iter().any(|m| m == name)
            || !entries
                .get(name)
                .is_some_and(|digest| digest.eq_ignore_ascii_case(observed))
            || !accounted.insert(name)
        {
            return false;
        }
    }
    true
}
/// Optional caller-provided, independently derived per-market partition
/// index. A global SHA manifest alone does not reveal which ticker a member
/// contains; this typed index must be verified against original source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedPartition {
    pub ticker: String,
    pub member: String,
    pub member_sha256: String,
    pub first_received_ns: i64,
    pub last_received_ns: i64,
    pub independent_index_sha256: String,
}
/// Select an explicitly indexed bounded market window without opening files.
/// The source-loader subsequently independently verifies actual bytes.
pub fn members_from_verified_index(
    index: &[IndexedPartition],
    catalog_bytes: &[u8],
    ticker: &str,
    start_ns: i64,
    end_ns: i64,
    expected_index_sha256: &str,
) -> Result<Vec<String>, Error> {
    if index.len() > 10_000
        || !valid_name(ticker)
        || start_ns <= 0
        || end_ns <= start_ns
        || !valid_hash(expected_index_sha256)
    {
        return Err(Error::InvalidManifest);
    }
    let catalog = catalog(catalog_bytes).map_err(Error::Source)?;
    let mut names = BTreeSet::new();
    for part in index {
        if !valid_name(&part.ticker)
            || part.first_received_ns <= 0
            || part.first_received_ns > part.last_received_ns
            || part.independent_index_sha256 != expected_index_sha256
            || !valid_hash(&part.member_sha256)
        {
            return Err(Error::InvalidManifest);
        }
        if part.ticker != ticker
            || part.last_received_ns < start_ns
            || part.first_received_ns > end_ns
        {
            continue;
        }
        let source = catalog
            .get(&part.member)
            .ok_or_else(|| Error::MissingMember(part.member.clone()))?;
        if !source
            .parquet_sha256
            .eq_ignore_ascii_case(&part.member_sha256)
        {
            return Err(Error::HashMismatch);
        }
        if !names.insert(part.member.clone()) {
            return Err(Error::DuplicateMember(part.member.clone()));
        }
        if names.len() > MAX_MEMBERS {
            return Err(Error::InvalidBatch);
        }
    }
    if names.is_empty() {
        return Err(Error::MissingMember(ticker.into()));
    }
    Ok(names.into_iter().collect())
}
/// Caller provides explicit verified catalog bytes, allowlists and ZIP paths.
/// Processes at most four markets per call (<=32 named members each); caller
/// persists returned content-addressed checkpoints outside production data.
pub fn run_batch(
    specs: &[MarketSpec],
    manifest_bytes: &[u8],
    zip_paths: &[PathBuf],
    engine_revision_sha256: &str,
    prior: &[ReusableMarketCheckpoint],
) -> Result<BatchOutcome, Error> {
    if specs.is_empty()
        || specs.len() > MAX_MARKETS
        || zip_paths.is_empty()
        || zip_paths.len() > MAX_ZIPS
        || !valid_hash(engine_revision_sha256)
        || prior.len() > 256
    {
        return Err(Error::InvalidBatch);
    }
    let idx = catalog(manifest_bytes).map_err(Error::Source)?;
    let catalog_sha = sha(manifest_bytes);
    let entries = idx
        .iter()
        .map(|(k, v)| (k.clone(), v.parquet_sha256.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    for s in specs {
        verify_spec(s)?;
        if !seen.insert(&s.ticker) {
            return Err(Error::DuplicateMarket);
        }
        for member in &s.named_members {
            if !entries.contains_key(member) {
                return Err(Error::MissingMember(member.clone()));
            }
        }
    }
    let mut sorted = specs.iter().collect::<Vec<_>>();
    sorted.sort_by(|a, b| (a.decision_ns, &a.ticker).cmp(&(b.decision_ns, &b.ticker)));
    let mut markets = Vec::new();
    let mut next_checkpoints = Vec::new();
    for s in sorted {
        let fp = fingerprint(s, &catalog_sha, engine_revision_sha256, &entries)?;
        let match_prior = prior.iter().find(|p| p.ticker == s.ticker);
        let reused = match_prior.is_some_and(|p| {
            valid_reuse(p, s, &fp, &catalog_sha, engine_revision_sha256, &entries)
        });
        let report_json = if reused {
            match_prior.expect("checked").report_json.clone()
        } else {
            let request = Request {
                ticker: s.ticker.clone(),
                start_received_ns: s.start_received_ns,
                end_received_ns: s.end_received_ns,
                named_members: s.named_members.clone(),
                zip_paths: zip_paths.to_vec(),
            };
            let replay = pipeline::run(&request, manifest_bytes, s.decision_ns, &[s.decision_ns])
                .map_err(Error::Replay)?;
            serde_json::to_string(&replay).map_err(|_| Error::Encoding)?
        };
        if report_json.len() > MAX_REPLAY_BYTES {
            return Err(Error::Encoding);
        }
        let parsed: serde_json::Value =
            serde_json::from_str(&report_json).map_err(|_| Error::Encoding)?;
        let execution_eligible = parsed
            .pointer("/book_integrity/modeled_execution_eligible")
            .and_then(|x| x.as_bool())
            == Some(true)
            && parsed
                .pointer("/book_integrity/complete_subscription_history_verified")
                .and_then(|x| x.as_bool())
                == Some(true);
        let exclusions = parsed
            .get("missing_evidence")
            .and_then(|x| x.as_array())
            .map(|xs| {
                xs.iter()
                    .filter_map(|x| x.as_str().map(str::to_owned))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let class = match parsed.get("status").and_then(|x| x.as_str()) {
            Some("SOURCE_DERIVED_PROVISIONAL") => SourceAvailability::SourceDerivedProvisional,
            _ => SourceAvailability::ReplayIncomplete,
        };
        let settlement = if s.independent_primary_sha256.is_some() {
            SettlementAvailability::DeclaredPrimaryUnverified
        } else if s.official_final_sha256.is_some() {
            SettlementAvailability::DeclaredOfficialUnverified
        } else {
            SettlementAvailability::Unknown
        };
        let report_sha = sha(report_json.as_bytes());
        next_checkpoints.push(ReusableMarketCheckpoint {
            schema_version: 1,
            ticker: s.ticker.clone(),
            fingerprint_sha256: fp.clone(),
            manifest_sha256: catalog_sha.clone(),
            report_sha256: report_sha.clone(),
            report_json: report_json.clone(),
            engine_revision_sha256: engine_revision_sha256.into(),
        });
        markets.push(MarketBatchOutcome {
            ticker: s.ticker.clone(),
            event: s.event.clone(),
            series: s.series.clone(),
            asset: s.asset,
            classification: class,
            settlement,
            contract_rules_available: s.contract_rules_sha256.is_some(),
            historical_fee_available: s.historical_fee_sha256.is_some(),
            reused_prior_verified_report: reused,
            fingerprint_sha256: fp,
            source_manifest_sha256: catalog_sha.clone(),
            replay_sha256: Some(report_sha),
            replay_json: Some(report_json),
            report_exclusions: exclusions,
            historical_execution_eligible: execution_eligible,
            // A hash declaration does not prove independent settlement.
            independent_settlement_eligible: false,
        });
    }
    Ok(BatchOutcome {
        schema_version: 1,
        engine_revision_sha256: engine_revision_sha256.into(),
        source_manifest_sha256: catalog_sha,
        markets,
        next_checkpoints,
        no_live_execution: true,
        no_empirical_pnl: true,
    })
}
/// Source-to-execution integration is deliberately guarded. A top-of-book
/// ReplayReport cannot be expanded into L2 depth or a real fill. The caller
/// must provide separately reconstructed depth and strict source witness.
pub fn simulate_eligible_historical_order(
    market: &MarketBatchOutcome,
    intent: &phoenix_sim::historical::LimitIntent,
    books: &[phoenix_sim::historical::FractionalBook],
    tape: phoenix_sim::historical::TradeTape<'_>,
    source: &phoenix_sim::historical::SourceWindowEvidence,
    fees: &impl phoenix_sim::historical::HistoricalFeeSource,
) -> Result<phoenix_sim::historical::ExecutionReport, Error> {
    if !market.historical_execution_eligible
        || !market.report_exclusions.is_empty()
        || market.ticker != intent.market
        || market.ticker != source.market
        || !market.historical_fee_available
        || !market.contract_rules_available
    {
        return Err(Error::InvalidCheckpoint);
    }
    let replay = market
        .replay_json
        .as_deref()
        .ok_or(Error::InvalidCheckpoint)?;
    if market.replay_sha256.as_deref() != Some(sha(replay.as_bytes()).as_str()) {
        return Err(Error::InvalidCheckpoint);
    }
    let json: serde_json::Value =
        serde_json::from_str(replay).map_err(|_| Error::InvalidCheckpoint)?;
    if json
        .pointer("/book_integrity/complete_subscription_history_verified")
        .and_then(|v| v.as_bool())
        != Some(true)
        || json
            .pointer("/book_integrity/snapshot_origin_sha256")
            .and_then(|v| v.as_str())
            != Some(source.book_sha256.as_str())
        || json.get("market_ticker").and_then(|v| v.as_str()) != Some(intent.market.as_str())
    {
        return Err(Error::InvalidCheckpoint);
    }
    phoenix_sim::historical::simulate_source_window(intent, books, tape, source, fees)
        .map_err(|_| Error::InvalidCheckpoint)
}
/// A consumer may only link a separately reconstructed and authenticated
/// full-depth book to a replay; top-of-book checkpoint is never enough.
pub fn reject_unproven_execution_handoff(report: &MarketBatchOutcome) -> Result<(), Error> {
    if !report.historical_execution_eligible || !report.report_exclusions.is_empty() {
        return Err(Error::InvalidCheckpoint);
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn spec(ticker: &str, asset: Asset, series: &str) -> MarketSpec {
        MarketSpec {
            ticker: ticker.into(),
            event: "E1".into(),
            series: series.into(),
            asset,
            start_received_ns: 10,
            end_received_ns: 50,
            decision_ns: 40,
            named_members: vec!["archive/curated/raw_events/x.parquet".into()],
            contract_rules_sha256: None,
            historical_fee_sha256: None,
            official_final_sha256: None,
            independent_primary_sha256: None,
            additional_family_schema_sha256: None,
        }
    }
    /// The curation catalog is JSONL: exactly one object per physical line.
    /// A pretty-printed, multi-line object is correctly rejected by
    /// `catalog`, so fixtures must be a single line.
    fn jsonl_catalog() -> String {
        format!(
            r#"{{"parquet":"curated/raw_events/x.parquet","parquet_sha256":"{}","rows":2}}"#,
            "b".repeat(64)
        )
    }
    #[test]
    fn multi_line_catalog_object_is_rejected_not_silently_joined() {
        let wrapped = jsonl_catalog().replacen(',', ",\n", 1);
        assert!(matches!(
            crate::catalog(wrapped.as_bytes()),
            Err(crate::Error::InvalidCatalog)
        ));
        assert_eq!(crate::catalog(jsonl_catalog().as_bytes()).unwrap().len(), 1);
    }
    #[test]
    fn four_known_families_have_separate_explicit_series() {
        for (ticker, asset, series) in [
            ("BTC-1", Asset::Btc, "KXBTC15M"),
            ("ETH-1", Asset::Eth, "KXETH15M"),
            ("SOL-1", Asset::Sol, "KXSOL15M"),
            ("XRP-1", Asset::Xrp, "KXXRP15M"),
        ] {
            assert!(verify_spec(&spec(ticker, asset, series)).is_ok());
        }
        assert!(matches!(
            verify_spec(&spec("ETH", Asset::Btc, "KXETH15M")),
            Err(Error::UnsupportedFamily)
        ));
    }
    #[test]
    fn duplicate_members_and_invalid_clock_fail_before_source_reads() {
        let mut s = spec("BTC", Asset::Btc, "KXBTC15M");
        s.named_members.push(s.named_members[0].clone());
        assert!(matches!(verify_spec(&s), Err(Error::DuplicateMember(_))));
        s.named_members.pop();
        s.decision_ns = 51;
        assert!(matches!(verify_spec(&s), Err(Error::InvalidTime)));
    }
    #[test]
    fn additional_family_requires_documented_schema_digest() {
        let mut s = spec("OTHER", Asset::DocumentedOther, "OTHER-SERIES");
        assert!(matches!(verify_spec(&s), Err(Error::UnsupportedFamily)));
        s.additional_family_schema_sha256 = Some("f".repeat(64));
        assert!(verify_spec(&s).is_ok());
    }
    #[test]
    fn fingerprint_includes_market_contract_fee_and_all_members() {
        let a = spec("BTC", Asset::Btc, "KXBTC15M");
        let mut b = a.clone();
        b.historical_fee_sha256 = Some("c".repeat(64));
        let entries = BTreeMap::from([(
            "archive/curated/raw_events/x.parquet".into(),
            "b".repeat(64),
        )]);
        let f1 = fingerprint(&a, &"a".repeat(64), &"d".repeat(64), &entries).unwrap();
        let f2 = fingerprint(&b, &"a".repeat(64), &"d".repeat(64), &entries).unwrap();
        assert_ne!(f1, f2);
    }
    #[test]
    fn checkpoint_tamper_blocks_resume_without_source_reads() {
        let spec = spec("BTC", Asset::Btc, "KXBTC15M");
        let entries = BTreeMap::from([(
            "archive/curated/raw_events/x.parquet".into(),
            "b".repeat(64),
        )]);
        let fp = fingerprint(&spec, &"a".repeat(64), &"d".repeat(64), &entries).unwrap();
        let p = ReusableMarketCheckpoint {
            schema_version: 1,
            ticker: "BTC".into(),
            fingerprint_sha256: fp.clone(),
            manifest_sha256: "a".repeat(64),
            report_sha256: "z".repeat(64),
            report_json: "{}".into(),
            engine_revision_sha256: "d".repeat(64),
        };
        assert!(!valid_reuse(
            &p,
            &spec,
            &fp,
            &"a".repeat(64),
            &"d".repeat(64),
            &entries
        ));
    }
    #[test]
    fn indexed_discovery_rejects_unknown_and_conflicting_members() {
        let member = "archive/curated/raw_events/x.parquet";
        let catalog = jsonl_catalog();
        let idx = [IndexedPartition {
            ticker: "BTC".into(),
            member: member.into(),
            member_sha256: "b".repeat(64),
            first_received_ns: 10,
            last_received_ns: 20,
            independent_index_sha256: "d".repeat(64),
        }];
        let selected =
            members_from_verified_index(&idx, catalog.as_bytes(), "BTC", 15, 25, &"d".repeat(64))
                .unwrap();
        assert_eq!(selected, vec![member]);
        assert!(members_from_verified_index(
            &idx,
            catalog.as_bytes(),
            "ETH",
            15,
            25,
            &"d".repeat(64)
        )
        .is_err());
        let mut broken = idx.to_vec();
        broken[0].member_sha256 = "e".repeat(64);
        assert!(matches!(
            members_from_verified_index(
                &broken,
                catalog.as_bytes(),
                "BTC",
                15,
                25,
                &"d".repeat(64)
            ),
            Err(Error::HashMismatch)
        ));
    }
    #[test]
    fn genuine_reused_report_must_match_member_proofs_not_only_json_sha() {
        let x = spec("BTC", Asset::Btc, "KXBTC15M");
        let member = x.named_members[0].clone();
        let entries = BTreeMap::from([(member.clone(), "b".repeat(64))]);
        let fingerprint = fingerprint(&x, &"a".repeat(64), &"d".repeat(64), &entries).unwrap();
        let report = serde_json::json!({"market_ticker":"BTC",
            "catalog_sha256":"a".repeat(64),
            "partition_proofs":[{"member":member,"sha256":"b".repeat(64)}],
            "status":"INCOMPLETE","missing_evidence":["NO_SNAPSHOT"]});
        let report_json = report.to_string();
        let prior = ReusableMarketCheckpoint {
            schema_version: 1,
            ticker: "BTC".into(),
            fingerprint_sha256: fingerprint.clone(),
            manifest_sha256: "a".repeat(64),
            report_sha256: sha(report_json.as_bytes()),
            report_json,
            engine_revision_sha256: "d".repeat(64),
        };
        assert!(valid_reuse(
            &prior,
            &x,
            &fingerprint,
            &"a".repeat(64),
            &"d".repeat(64),
            &entries
        ));
        let mut wrong = prior;
        wrong.report_sha256 = "e".repeat(64);
        assert!(!valid_reuse(
            &wrong,
            &x,
            &fingerprint,
            &"a".repeat(64),
            &"d".repeat(64),
            &entries
        ));
    }
    #[test]
    fn duplicate_market_and_batch_bounds_stop_before_zip_io() {
        let one = spec("BTC", Asset::Btc, "KXBTC15M");
        let catalog = jsonl_catalog();
        let paths = vec![PathBuf::from("synthetic-not-on-disk.zip")];
        assert!(matches!(
            run_batch(
                &[one.clone(), one],
                catalog.as_bytes(),
                &paths,
                &"d".repeat(64),
                &[]
            ),
            Err(Error::DuplicateMarket)
        ));
        let many = vec![spec("BTC", Asset::Btc, "KXBTC15M"); 5];
        assert!(matches!(
            run_batch(&many, catalog.as_bytes(), &paths, &"d".repeat(64), &[]),
            Err(Error::InvalidBatch)
        ));
    }
    #[test]
    fn no_real_market_is_automatically_execution_eligible() {
        let out = MarketBatchOutcome {
            ticker: "BTC".into(),
            event: "E1".into(),
            series: "KXBTC15M".into(),
            asset: Asset::Btc,
            classification: SourceAvailability::ManifestOnly,
            settlement: SettlementAvailability::Unknown,
            contract_rules_available: false,
            historical_fee_available: false,
            reused_prior_verified_report: false,
            fingerprint_sha256: "a".repeat(64),
            source_manifest_sha256: "b".repeat(64),
            replay_sha256: None,
            replay_json: None,
            report_exclusions: vec!["MISSING_BOOK".into()],
            historical_execution_eligible: false,
            independent_settlement_eligible: false,
        };
        assert!(reject_unproven_execution_handoff(&out).is_err());
    }
}

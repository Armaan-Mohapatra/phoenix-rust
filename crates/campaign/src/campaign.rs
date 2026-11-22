//! Chronological research campaign with an untouched holdout.
//!
//! Protocol (fixed before any data is seen):
//! 1. Split cases by time with `phoenix_research::SplitPlan`
//!    (discovery | embargo | validation | embargo | holdout).
//! 2. DISCOVERY: evaluate every grid configuration. Keep those with at least
//!    `min_trades` settled trades and positive discovery z; take the top K.
//! 3. VALIDATION: one-sided cluster z-test with Bonferroni alpha/K. Also
//!    require a positive mean under fee and latency stress.
//! 4. HOLDOUT: evaluated once, only for validation survivors, with alpha/M.
//!    Before it is touched the caller's exposure hook runs (the durable
//!    registry records it); if there are no survivors it is never touched.
//!
//! Engine: the source is visited in passes (scan, discovery, validation,
//! holdout) in bounded batches, so a streaming file source uses memory
//! proportional to one batch plus an O(markets) identity index, never the
//! full snapshot set. The in-memory and streaming paths execute the same
//! code and produce identical reports for identical cases.
use crate::engine::{evaluate_case, Acc, ExecutionModel, Metrics};
use crate::fingerprint::{code_revision, experiment_sha256, Fingerprinter};
use crate::math::z_upper;
use crate::source::{CaseSource, VecSource};
use crate::strategies::StrategyConfig;
use crate::{MarketCase, Provenance};
use phoenix_research::{Split, SplitPlan};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

#[derive(Debug, Clone, Serialize)]
pub struct CampaignConfig {
    pub discovery_fraction_permille: i64,
    pub validation_fraction_permille: i64,
    /// Embargo between splits, seconds (>= one market window).
    pub embargo_s: i64,
    pub top_k: usize,
    pub min_trades: u64,
    pub alpha: f64,
    /// Hard cap on configurations tried (search budget).
    pub max_configs: usize,
    /// Worker threads. Kept low by default for a shared personal machine.
    pub threads: usize,
    /// Cases per batch; bounds streaming memory.
    pub batch_size: usize,
    pub execution: ExecutionModel,
    pub stress_fee_permille: u64,
    pub stress_latency_ns: i64,
}
impl Default for CampaignConfig {
    fn default() -> Self {
        Self {
            discovery_fraction_permille: 500,
            validation_fraction_permille: 250,
            embargo_s: 900,
            top_k: 8,
            min_trades: 30,
            alpha: 0.05,
            max_configs: 128,
            threads: 2,
            batch_size: 256,
            execution: ExecutionModel::default(),
            stress_fee_permille: 1500,
            stress_latency_ns: 1_000_000_000,
        }
    }
}
impl CampaignConfig {
    pub fn validate(&self) -> Result<(), CampaignError> {
        let bad = |s: String| Err(CampaignError::InvalidConfig(s));
        let (d, v) = (
            self.discovery_fraction_permille,
            self.validation_fraction_permille,
        );
        if d <= 0 || v <= 0 || d.saturating_add(v) >= 1000 {
            return bad(format!(
                "split fractions {d}/{v} permille must be >0 and leave a holdout"
            ));
        }
        if !(0..=86_400).contains(&self.embargo_s) {
            return bad(format!("embargo_s {}", self.embargo_s));
        }
        if !(1..=64).contains(&self.top_k) || self.min_trades == 0 {
            return bad("top_k must be 1..=64 and min_trades >= 1".into());
        }
        if !(self.alpha > 0.0 && self.alpha <= 0.5) {
            return bad(format!("alpha {}", self.alpha));
        }
        if !(1..=10_000).contains(&self.max_configs) || !(1..=16).contains(&self.threads) {
            return bad("max_configs 1..=10000, threads 1..=16".into());
        }
        if !(1..=4096).contains(&self.batch_size) {
            return bad(format!("batch_size {}", self.batch_size));
        }
        let stress_fee = ExecutionModel {
            fee_multiplier_permille: self.stress_fee_permille,
            ..self.execution
        };
        let stress_lat = ExecutionModel {
            latency_ns: self.stress_latency_ns,
            ..self.execution
        };
        for (name, x) in [
            ("execution", self.execution),
            ("fee stress", stress_fee),
            ("latency stress", stress_lat),
        ] {
            x.validate()
                .map_err(|e| CampaignError::InvalidConfig(format!("{name}: {e}")))?;
        }
        Ok(())
    }
    /// The parts of the config that change economic results. Threads and
    /// batch size are excluded so they cannot alter experiment identity.
    pub fn economic_json(&self) -> String {
        let mut v = serde_json::to_value(self).expect("serializable");
        let o = v.as_object_mut().expect("object");
        o.remove("threads");
        o.remove("batch_size");
        v.to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CampaignError {
    NoCases,
    EmptyGrid,
    InvalidCase { index: u64, reason: String },
    DuplicateMarket(String),
    DuplicateConfig(String),
    Unordered { index: u64 },
    NonContiguousCluster(String),
    InconsistentEvent(String),
    BudgetExceeded { requested: usize, budget: usize },
    InvalidConfig(String),
    MixedProvenance,
    ProvenanceMismatch,
    EmptySplit(&'static str),
    Source(String),
    SourceChanged,
    Exposure(String),
}

/// What a report may be used to claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum EvidenceClass {
    /// Synthetic input: validates the engine only.
    SyntheticEngineCheck,
    /// Real input, but the holdout was used before or not registry-guarded.
    Exploratory,
    /// Real input, registry-verified first use of every holdout market.
    UntouchedConfirmatory,
}

#[derive(Debug, Clone, Serialize)]
pub enum Verdict {
    RejectedDiscovery(String),
    RejectedValidation(String),
    RejectedHoldout(String),
    /// Passed every pre-registered gate on this dataset. What that permits
    /// depends on the report's `evidence_class`; never a profitability claim.
    PassedAllGates,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigReport {
    pub id: String,
    pub config_sha256: String,
    pub config: StrategyConfig,
    pub discovery: Metrics,
    pub validation: Option<Metrics>,
    pub validation_fee_stress: Option<Metrics>,
    pub validation_latency_stress: Option<Metrics>,
    pub holdout: Option<Metrics>,
    pub verdict: Verdict,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SplitCounts {
    pub discovery: usize,
    pub validation: usize,
    pub holdout: usize,
    pub embargoed: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct Resources {
    pub cases: u64,
    pub snapshots: u64,
    pub passes: u32,
    pub raw_bytes_read: u64,
    /// Bytes read from the binary case cache (0 without `--cache-dir`).
    pub cache_bytes_read: u64,
    pub case_cache: Option<crate::source::CacheStatus>,
    pub decisions_evaluated: u64,
    pub wall_ms: u128,
    pub decisions_per_second: f64,
    pub threads: usize,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub schema: &'static str,
    pub provenance: Provenance,
    pub evidence_class: EvidenceClass,
    /// True only for UntouchedConfirmatory with at least one candidate.
    pub confirmatory_eligible: bool,
    pub raw_input_sha256: Option<String>,
    pub normalized_input_sha256: String,
    pub experiment_sha256: String,
    pub grid_sha256: String,
    pub code_revision: String,
    pub config: CampaignConfig,
    pub split_plan: [i64; 4],
    pub split_counts: SplitCounts,
    pub configs_tried: usize,
    pub selected_for_validation: usize,
    pub validation_alpha_per_test: f64,
    pub validation_z_critical: f64,
    pub holdout_evaluations: usize,
    pub holdout_alpha_per_test: Option<f64>,
    pub holdout_touched: bool,
    pub candidates: Vec<String>,
    pub results: Vec<ConfigReport>,
    pub resources: Resources,
    pub limitations: Vec<&'static str>,
    /// How fees were obtained. Never "authenticated": the engine only applies a
    /// hypothetical fee formula (standard taker rate x configured multiplier).
    pub fee_basis: &'static str,
    /// Settlement evidence class. Venue-reported finals are not independent corroboration.
    pub settlement_evidence: &'static str,
}

pub const FEE_BASIS_HYPOTHETICAL: &str = "HYPOTHETICAL_STANDARD_TAKER_FORMULA_UNAUTHENTICATED";
pub const SETTLEMENT_EVIDENCE_VENUE_REPORTED: &str = "VENUE_REPORTED_NOT_INDEPENDENT";

pub fn grid_sha256(grid: &[StrategyConfig]) -> String {
    let mut h = Sha256::new();
    for g in grid {
        h.update(g.config_sha256().as_bytes());
    }
    format!("{:x}", h.finalize())
}

pub fn holdout_key(c: &MarketCase) -> String {
    format!("{}@{}", c.market, c.expiration_ns)
}

/// Pass-0 invariants over the ordered stream.
#[derive(Default)]
struct Scan {
    n: u64,
    snapshots: u64,
    t0: i64,
    t1: i64,
    last: Option<(i64, String)>,
    markets: HashSet<String>,
    closed: HashSet<String>,
    cur: Option<String>,
    events: HashMap<String, (i64, String)>,
    prov: Option<Provenance>,
    fp: Fingerprinter,
}
impl Scan {
    fn observe(&mut self, c: &MarketCase) -> Result<(), CampaignError> {
        let index = self.n;
        let invalid = |reason: String| CampaignError::InvalidCase { index, reason };
        c.validate().map_err(|e| invalid(format!("{e:?}")))?;
        // reference_cents == 0 means "no reference series available" (e.g. the TNEVENT capture has none);
        // every reference-based family abstains on it. Negative values are still corrupt data.
        if c.strike_cents <= 0 || c.snapshots.iter().any(|s| s.reference_cents < 0) {
            return Err(invalid("non-positive strike or negative reference".into()));
        }
        if self.markets.contains(&c.market) {
            return Err(CampaignError::DuplicateMarket(c.market.clone()));
        }
        let key = (c.expiration_ns, c.market.clone());
        if self.last.as_ref().is_some_and(|l| *l >= key) {
            return Err(CampaignError::Unordered { index });
        }
        self.markets.insert(c.market.clone());
        if self.cur.as_deref() != Some(&c.cluster) {
            if let Some(prev) = self.cur.replace(c.cluster.clone()) {
                self.closed.insert(prev);
            }
            if self.closed.contains(&c.cluster) {
                return Err(CampaignError::NonContiguousCluster(c.cluster.clone()));
            }
        }
        match self.events.get(&c.event) {
            Some((exp, asset)) if *exp != c.expiration_ns || *asset != c.asset => {
                return Err(CampaignError::InconsistentEvent(c.event.clone()))
            }
            Some(_) => {}
            None => {
                self.events
                    .insert(c.event.clone(), (c.expiration_ns, c.asset.clone()));
            }
        }
        match self.prov {
            None => self.prov = Some(c.provenance),
            Some(p) if p != c.provenance => return Err(CampaignError::MixedProvenance),
            _ => {}
        }
        if self.n == 0 {
            self.t0 = c.open_ns;
            self.t1 = c.expiration_ns;
        }
        self.t0 = self.t0.min(c.open_ns);
        self.t1 = self.t1.max(c.expiration_ns);
        self.snapshots += c.snapshots.len() as u64;
        self.last = Some(key);
        self.fp.push(c);
        self.n += 1;
        Ok(())
    }
}

fn plan(t0: i64, t1: i64, c: &CampaignConfig) -> SplitPlan {
    let span = i128::from(t1) - i128::from(t0);
    let at = |permille: i64| (i128::from(t0) + span * i128::from(permille) / 1000) as i64;
    let embargo = c.embargo_s.saturating_mul(1_000_000_000);
    let validation_end = at(c.discovery_fraction_permille + c.validation_fraction_permille);
    SplitPlan {
        discovery_end_ns: at(c.discovery_fraction_permille),
        validation_end_ns: validation_end,
        untouched_start_ns: validation_end.saturating_add(embargo),
        embargo_ns: embargo,
    }
}

type Job = (usize, ExecutionModel);

/// Feed a batch to every job's accumulator; jobs are split across threads,
/// each accumulator sees cases in stream order, so results are identical
/// for any thread count.
fn feed(
    accs: &mut [Acc],
    jobs: &[Job],
    grid: &[StrategyConfig],
    sel: &[&MarketCase],
    threads: usize,
) {
    let work = |a: &mut [Acc], j: &[Job]| {
        for (acc, (gi, x)) in a.iter_mut().zip(j) {
            for c in sel {
                acc.push(&evaluate_case(&grid[*gi], c, x));
            }
        }
    };
    let threads = threads.clamp(1, 16).min(jobs.len().max(1));
    if threads <= 1 {
        return work(accs, jobs);
    }
    let chunk = jobs.len().div_ceil(threads);
    std::thread::scope(|s| {
        for (a, j) in accs.chunks_mut(chunk).zip(jobs.chunks(chunk)) {
            s.spawn(move || work(a, j));
        }
    });
}

fn pass(
    src: &mut dyn CaseSource,
    sp: &SplitPlan,
    which: Split,
    jobs: &[Job],
    grid: &[StrategyConfig],
    threads: usize,
    mut extra: impl FnMut(&MarketCase, Split),
) -> Result<Vec<Metrics>, CampaignError> {
    let mut accs = vec![Acc::default(); jobs.len()];
    src.visit(&mut |batch| {
        let mut sel = Vec::with_capacity(batch.len());
        for c in batch {
            let s = sp.classify(c.open_ns, c.expiration_ns);
            extra(c, s);
            if s == which {
                sel.push(c);
            }
        }
        if !sel.is_empty() && !jobs.is_empty() {
            feed(&mut accs, jobs, grid, &sel, threads);
        }
        Ok(())
    })?;
    Ok(accs.into_iter().map(Acc::finish).collect())
}

fn z_of(m: &Metrics) -> f64 {
    m.test.map_or(f64::NEG_INFINITY, |t| t.z)
}

/// In-memory convenience (synthetic engine checks and tests).
pub fn run(
    cases: &[MarketCase],
    grid: &[StrategyConfig],
    cfg: &CampaignConfig,
) -> Result<Report, CampaignError> {
    let class = match cases.first().map(|c| c.provenance) {
        Some(Provenance::OperatorNormalized | Provenance::OperatorUnverified) => {
            EvidenceClass::Exploratory
        }
        _ => EvidenceClass::SyntheticEngineCheck,
    };
    let mut src = VecSource {
        cases,
        batch: cfg.batch_size,
    };
    run_source(&mut src, grid, cfg, class, &mut |_| Ok(()))
}

/// `on_holdout` receives every holdout market key before the holdout is
/// evaluated and may refuse (for example, the registry is unavailable).
pub fn run_source(
    src: &mut dyn CaseSource,
    grid: &[StrategyConfig],
    cfg: &CampaignConfig,
    class: EvidenceClass,
    on_holdout: &mut dyn FnMut(&[String]) -> Result<(), String>,
) -> Result<Report, CampaignError> {
    let started = Instant::now();
    cfg.validate()?;
    if grid.is_empty() {
        return Err(CampaignError::EmptyGrid);
    }
    if grid.len() > cfg.max_configs {
        return Err(CampaignError::BudgetExceeded {
            requested: grid.len(),
            budget: cfg.max_configs,
        });
    }
    let mut ids = HashSet::new();
    for g in grid {
        g.validate()
            .map_err(|e| CampaignError::InvalidConfig(format!("{e:?}")))?;
        if !ids.insert(g.config_sha256()) {
            return Err(CampaignError::DuplicateConfig(g.id()));
        }
    }

    // Pass 0: validate the whole stream before any evaluation.
    let mut scan = Scan::default();
    src.visit(&mut |b| b.iter().try_for_each(|c| scan.observe(c)))?;
    if scan.n == 0 {
        return Err(CampaignError::NoCases);
    }
    let provenance = scan.prov.expect("n > 0");
    match (class, provenance) {
        (EvidenceClass::SyntheticEngineCheck, Provenance::Synthetic)
        | (
            EvidenceClass::Exploratory,
            Provenance::OperatorNormalized | Provenance::OperatorUnverified,
        )
        | (EvidenceClass::UntouchedConfirmatory, Provenance::OperatorNormalized) => {}
        _ => return Err(CampaignError::ProvenanceMismatch),
    }
    let normalized = std::mem::take(&mut scan.fp).finish();
    let sp = plan(scan.t0, scan.t1, cfg);
    let grid_sha = grid_sha256(grid);
    let code = code_revision();
    let experiment = experiment_sha256(&normalized, &grid_sha, &cfg.economic_json(), &code);
    let mut decisions = 0u64;
    let mut passes = 1u32;

    // Pass 1: discovery (also counts splits and collects holdout keys).
    let all: Vec<Job> = (0..grid.len()).map(|i| (i, cfg.execution)).collect();
    let mut counts = SplitCounts {
        discovery: 0,
        validation: 0,
        holdout: 0,
        embargoed: 0,
    };
    let mut holdout_keys = Vec::new();
    let discovery = pass(
        src,
        &sp,
        Split::Discovery,
        &all,
        grid,
        cfg.threads,
        |c, s| match s {
            Split::Discovery => counts.discovery += 1,
            Split::Validation => counts.validation += 1,
            Split::Untouched => {
                counts.holdout += 1;
                holdout_keys.push(holdout_key(c));
            }
            Split::Embargo => counts.embargoed += 1,
        },
    )?;
    passes += 1;
    for (name, n) in [
        ("discovery", counts.discovery),
        ("validation", counts.validation),
        ("holdout", counts.holdout),
    ] {
        if n == 0 {
            return Err(CampaignError::EmptySplit(name));
        }
    }
    decisions += (grid.len() * counts.discovery) as u64;
    let mut ranked: Vec<usize> = (0..grid.len())
        .filter(|&i| discovery[i].settled_trades >= cfg.min_trades && z_of(&discovery[i]) > 0.0)
        .collect();
    ranked.sort_by(|&a, &b| {
        z_of(&discovery[b])
            .total_cmp(&z_of(&discovery[a]))
            .then(a.cmp(&b))
    });
    ranked.truncate(cfg.top_k);
    let k = ranked.len().max(1);
    let val_alpha = cfg.alpha / k as f64;
    let val_z = z_upper(val_alpha);

    // Pass 2: validation + stress, selected configurations only.
    let fee_x = ExecutionModel {
        fee_multiplier_permille: cfg.stress_fee_permille,
        ..cfg.execution
    };
    let lat_x = ExecutionModel {
        latency_ns: cfg.stress_latency_ns,
        ..cfg.execution
    };
    let vjobs: Vec<Job> = ranked
        .iter()
        .flat_map(|&i| [(i, cfg.execution), (i, fee_x), (i, lat_x)])
        .collect();
    let validated: Vec<(Metrics, Metrics, Metrics)> = if vjobs.is_empty() {
        Vec::new()
    } else {
        passes += 1;
        let v = pass(
            src,
            &sp,
            Split::Validation,
            &vjobs,
            grid,
            cfg.threads,
            |_, _| {},
        )?;
        v.chunks(3)
            .map(|t| (t[0].clone(), t[1].clone(), t[2].clone()))
            .collect()
    };
    decisions += (vjobs.len() * counts.validation) as u64;
    let passes_validation = |v: &(Metrics, Metrics, Metrics)| -> Result<(), String> {
        if v.0.settled_trades < cfg.min_trades {
            return Err(format!("only {} settled trades", v.0.settled_trades));
        }
        let z = z_of(&v.0);
        if z < val_z {
            return Err(format!("z {z:.3} < Bonferroni critical {val_z:.3}"));
        }
        if v.1.net_ticks <= 0 {
            return Err("non-positive net under fee stress".into());
        }
        if v.2.net_ticks <= 0 {
            return Err("non-positive net under latency stress".into());
        }
        Ok(())
    };
    let survivors: Vec<usize> = (0..ranked.len())
        .filter(|&j| passes_validation(&validated[j]).is_ok())
        .collect();

    // Pass 3: holdout, once, survivors only, after exposure is recorded.
    let m = survivors.len();
    let hold_alpha = (m > 0).then(|| cfg.alpha / m as f64);
    let holdout: Vec<Metrics> = if m == 0 {
        Vec::new()
    } else {
        on_holdout(&holdout_keys).map_err(CampaignError::Exposure)?;
        passes += 1;
        let hjobs: Vec<Job> = survivors
            .iter()
            .map(|&j| (ranked[j], cfg.execution))
            .collect();
        pass(
            src,
            &sp,
            Split::Untouched,
            &hjobs,
            grid,
            cfg.threads,
            |_, _| {},
        )?
    };
    decisions += (m * counts.holdout) as u64;

    let mut results = Vec::with_capacity(grid.len());
    let mut candidates = Vec::new();
    for (i, g) in grid.iter().enumerate() {
        let d = &discovery[i];
        let mut r = ConfigReport {
            id: g.id(),
            config_sha256: g.config_sha256(),
            config: g.clone(),
            discovery: d.clone(),
            validation: None,
            validation_fee_stress: None,
            validation_latency_stress: None,
            holdout: None,
            verdict: Verdict::RejectedDiscovery(if d.settled_trades < cfg.min_trades {
                format!("only {} settled trades", d.settled_trades)
            } else if z_of(d) <= 0.0 {
                "non-positive or untestable discovery mean".into()
            } else {
                format!("not in top {}", cfg.top_k)
            }),
        };
        if let Some(j) = ranked.iter().position(|&x| x == i) {
            let v = &validated[j];
            r.validation = Some(v.0.clone());
            r.validation_fee_stress = Some(v.1.clone());
            r.validation_latency_stress = Some(v.2.clone());
            r.verdict = match passes_validation(v) {
                Err(e) => Verdict::RejectedValidation(e),
                Ok(()) => {
                    let h = &holdout[survivors.iter().position(|&s| s == j).expect("survivor")];
                    r.holdout = Some(h.clone());
                    let hz = z_upper(hold_alpha.expect("m > 0"));
                    if h.settled_trades < cfg.min_trades {
                        Verdict::RejectedHoldout(format!(
                            "only {} settled trades",
                            h.settled_trades
                        ))
                    } else if z_of(h) < hz {
                        Verdict::RejectedHoldout(format!("z {:.3} < critical {hz:.3}", z_of(h)))
                    } else {
                        candidates.push(r.id.clone());
                        Verdict::PassedAllGates
                    }
                }
            };
        }
        results.push(r);
    }
    let wall = started.elapsed().as_millis();
    Ok(Report {
        schema: "phoenix-campaign-report-v2",
        provenance,
        evidence_class: class,
        confirmatory_eligible: class == EvidenceClass::UntouchedConfirmatory && !candidates.is_empty(),
        raw_input_sha256: src.raw_sha256(),
        normalized_input_sha256: normalized,
        experiment_sha256: experiment,
        grid_sha256: grid_sha,
        code_revision: code,
        config: cfg.clone(),
        split_plan: [sp.discovery_end_ns, sp.validation_end_ns, sp.untouched_start_ns, sp.embargo_ns],
        split_counts: counts,
        configs_tried: grid.len(),
        selected_for_validation: ranked.len(),
        validation_alpha_per_test: val_alpha,
        validation_z_critical: val_z,
        holdout_evaluations: m,
        holdout_alpha_per_test: hold_alpha,
        holdout_touched: m > 0,
        candidates,
        results,
        resources: Resources {
            cases: scan.n,
            snapshots: scan.snapshots,
            passes,
            raw_bytes_read: src.raw_bytes_read(),
            cache_bytes_read: src.cache_bytes_read(),
            case_cache: src.cache_status(),
            decisions_evaluated: decisions,
            wall_ms: wall,
            decisions_per_second: decisions as f64 / (wall.max(1) as f64 / 1000.0),
            threads: cfg.threads,
            batch_size: cfg.batch_size,
        },
        fee_basis: FEE_BASIS_HYPOTHETICAL,
        settlement_evidence: SETTLEMENT_EVIDENCE_VENUE_REPORTED,
        limitations: vec![
            "Fees are a hypothetical standard-taker formula scaled by fee_multiplier_permille, not a historically authenticated fee schedule; net results are UNVERIFIED until real fee evidence is supplied.",
            "Settlement outcomes are venue-reported finals, not independently corroborated.",
            "All fills are modeled taker fills against the book at arrival; no queue or maker model.",
            "Positions are held to expiration; no early exit is modeled.",
            "Cluster z-test assumes independence across clusters (time windows).",
            "PassedAllGates is meaningful only as qualified by evidence_class.",
            "Synthetic provenance results validate the engine, never real-market edge.",
        ],
    })
}

/// Deterministic part of a report (excludes wall-clock resource fields).
pub fn economic_fingerprint(r: &Report) -> String {
    let v = serde_json::json!({
        "results": r.results, "candidates": r.candidates, "splits": r.split_counts,
        "normalized": r.normalized_input_sha256, "experiment": r.experiment_sha256,
    });
    format!("{:x}", Sha256::digest(v.to_string().as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::to_jsonl_line;
    use crate::source::FileSource;
    use crate::strategies::{default_grid, Family};
    use crate::synthetic::{generate, Scenario};

    fn fast() -> CampaignConfig {
        CampaignConfig {
            threads: 2,
            ..Default::default()
        }
    }

    #[test]
    fn budget_identity_and_config_guards() {
        let cases = generate(&Scenario::efficient(1, 2, 1));
        let grid = default_grid();
        let tight = CampaignConfig {
            max_configs: 3,
            ..fast()
        };
        assert_eq!(
            run(&cases, &grid, &tight).unwrap_err(),
            CampaignError::BudgetExceeded {
                requested: grid.len(),
                budget: 3
            }
        );
        assert_eq!(
            run(&cases, &[], &fast()).unwrap_err(),
            CampaignError::EmptyGrid
        );
        let dup = vec![grid[0].clone(), grid[0].clone()];
        assert!(matches!(
            run(&cases, &dup, &fast()),
            Err(CampaignError::DuplicateConfig(_))
        ));
        let mut mixed = cases.clone();
        mixed[5].provenance = Provenance::OperatorNormalized;
        assert_eq!(
            run(&mixed, &grid[..2], &fast()).unwrap_err(),
            CampaignError::MixedProvenance
        );
        for bad in [
            CampaignConfig {
                discovery_fraction_permille: 800,
                validation_fraction_permille: 200,
                ..fast()
            },
            CampaignConfig {
                discovery_fraction_permille: 0,
                ..fast()
            },
            CampaignConfig {
                alpha: f64::NAN,
                ..fast()
            },
            CampaignConfig {
                alpha: 0.0,
                ..fast()
            },
            CampaignConfig {
                threads: 0,
                ..fast()
            },
            CampaignConfig {
                embargo_s: -1,
                ..fast()
            },
            CampaignConfig {
                stress_latency_ns: -5,
                ..fast()
            },
            CampaignConfig {
                execution: ExecutionModel {
                    quantity: 0,
                    ..Default::default()
                },
                ..fast()
            },
        ] {
            assert!(
                matches!(
                    run(&cases, &grid[..2], &bad),
                    Err(CampaignError::InvalidConfig(_))
                ),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn duplicate_unordered_inconsistent_and_noncontiguous_inputs_are_refused() {
        let cases = generate(&Scenario::efficient(2, 1, 1));
        let g = &default_grid()[..2];
        let mut dup = cases.clone();
        dup.insert(1, cases[0].clone());
        assert!(matches!(
            run(&dup, g, &fast()),
            Err(CampaignError::DuplicateMarket(_))
        ));
        let mut unordered = cases.clone();
        unordered.swap(0, 4);
        assert!(matches!(
            run(&unordered, g, &fast()),
            Err(CampaignError::Unordered { .. })
        ));
        let mut inconsistent = cases.clone();
        let ev = inconsistent[0].event.clone();
        inconsistent[3].event = ev;
        assert!(matches!(
            run(&inconsistent, g, &fast()),
            Err(CampaignError::InconsistentEvent(_))
        ));
        let mut split_cluster = cases.clone();
        let cl = split_cluster[0].cluster.clone();
        split_cluster[4].cluster = cl;
        assert!(matches!(
            run(&split_cluster, g, &fast()),
            Err(CampaignError::NonContiguousCluster(_))
        ));
        let mut neg = cases.clone();
        neg[2].strike_cents = 0;
        assert!(matches!(
            run(&neg, g, &fast()),
            Err(CampaignError::InvalidCase { index: 2, .. })
        ));
    }

    #[test]
    fn splits_are_chronological_and_embargoed() {
        let cases = generate(&Scenario::efficient(2, 4, 2));
        let r = run(&cases, &default_grid()[..2], &fast()).unwrap();
        let c = &r.split_counts;
        assert!(c.discovery > c.validation && c.validation > 0 && c.holdout > 0 && c.embargoed > 0);
        assert_eq!(
            c.discovery + c.validation + c.holdout + c.embargoed,
            cases.len()
        );
        let [d_end, v_end, u_start, _] = r.split_plan;
        assert!(d_end < v_end && v_end < u_start);
    }

    #[test]
    fn efficient_market_yields_no_candidates_and_never_touches_holdout() {
        for seed in [101, 202, 303, 404, 505] {
            let cases = generate(&Scenario::efficient(2, 8, seed));
            let mut touched = false;
            let mut src = VecSource {
                cases: &cases,
                batch: 256,
            };
            let r = run_source(
                &mut src,
                &default_grid(),
                &fast(),
                EvidenceClass::SyntheticEngineCheck,
                &mut |_| {
                    touched = true;
                    Ok(())
                },
            )
            .unwrap();
            assert!(r.candidates.is_empty(), "seed {seed}: {:?}", r.candidates);
            assert_eq!(touched, r.holdout_touched);
            let ctl = r
                .results
                .iter()
                .find(|x| x.config.family == Family::NoTrade)
                .unwrap();
            assert_eq!(ctl.discovery.net_ticks, 0);
        }
    }

    #[test]
    fn planted_quote_lag_is_detected_and_exposure_hook_runs_first() {
        let cases = generate(&Scenario::lagged_quotes(2, 8, 404));
        let mut exposed = Vec::new();
        let mut src = VecSource {
            cases: &cases,
            batch: 100,
        };
        let r = run_source(
            &mut src,
            &default_grid(),
            &fast(),
            EvidenceClass::SyntheticEngineCheck,
            &mut |k| {
                exposed = k.to_vec();
                Ok(())
            },
        )
        .unwrap();
        assert!(!r.candidates.is_empty(), "planted edge was not detected");
        assert_eq!(exposed.len(), r.split_counts.holdout);
        assert!(!r.confirmatory_eligible);
        assert!(r
            .results
            .iter()
            .filter(|x| matches!(x.config.family, Family::NoTrade | Family::CoinFlip))
            .all(|x| !matches!(x.verdict, Verdict::PassedAllGates)));
        let mut src = VecSource {
            cases: &cases,
            batch: 100,
        };
        let e = run_source(
            &mut src,
            &default_grid(),
            &fast(),
            EvidenceClass::SyntheticEngineCheck,
            &mut |_| Err("registry unavailable".into()),
        );
        assert_eq!(
            e.unwrap_err(),
            CampaignError::Exposure("registry unavailable".into())
        );
    }

    #[test]
    fn synthetic_cannot_be_reported_as_real_evidence() {
        let cases = generate(&Scenario::efficient(1, 2, 3));
        let mut src = VecSource {
            cases: &cases,
            batch: 64,
        };
        let e = run_source(
            &mut src,
            &default_grid()[..2],
            &fast(),
            EvidenceClass::UntouchedConfirmatory,
            &mut |_| Ok(()),
        );
        assert_eq!(e.unwrap_err(), CampaignError::ProvenanceMismatch);
    }

    #[test]
    fn deterministic_across_threads_and_batch_sizes() {
        let cases = generate(&Scenario::shaded(2, 4, 7));
        let grid = default_grid();
        let fps: Vec<String> = [(1, 1), (3, 17), (2, 256), (4, 4096)]
            .iter()
            .map(|&(threads, batch_size)| {
                economic_fingerprint(
                    &run(
                        &cases,
                        &grid,
                        &CampaignConfig {
                            threads,
                            batch_size,
                            ..Default::default()
                        },
                    )
                    .unwrap(),
                )
            })
            .collect();
        assert!(fps.windows(2).all(|w| w[0] == w[1]), "{fps:?}");
    }

    #[test]
    fn golden_economic_fingerprint_is_pinned() {
        // Any change here means campaign economics changed; review before re-pinning.
        let cases = generate(&Scenario::shaded(2, 4, 7));
        let r = run(&cases, &default_grid(), &CampaignConfig::default()).unwrap();
        assert_eq!(economic_fingerprint(&r), "eabcd29fb00c66eabcd4736beceb8b3e2dd6d1028e09d5fdaf554a90346b1e8f");
    }

    #[test]
    fn report_discloses_hypothetical_fees_and_venue_only_settlement() {
        let cases = generate(&Scenario::shaded(2, 4, 7));
        let r = run(&cases, &default_grid()[..2], &CampaignConfig::default()).unwrap();
        assert_eq!(r.fee_basis, FEE_BASIS_HYPOTHETICAL);
        assert_eq!(r.settlement_evidence, SETTLEMENT_EVIDENCE_VENUE_REPORTED);
        let j = serde_json::to_value(&r).unwrap();
        assert!(j["fee_basis"].as_str().unwrap().contains("UNAUTHENTICATED"));
        assert!(r.limitations.iter().any(|l| l.contains("hypothetical") && l.contains("UNVERIFIED")));
        assert!(r.limitations.iter().any(|l| l.contains("not independently corroborated")));
    }

    #[test]
    fn streaming_file_engine_matches_in_memory_engine_exactly() {
        let cases = generate(&Scenario::lagged_quotes(3, 6, 99));
        let tmp = TempDir::new("stream");
        let dir = &tmp.0;
        let path = dir.join("cases.jsonl");
        let text: String = cases.iter().map(|c| to_jsonl_line(c) + "\n").collect();
        std::fs::write(&path, &text).unwrap();
        let cfg = CampaignConfig {
            batch_size: 64,
            ..Default::default()
        };
        let mem = run(&cases, &default_grid(), &cfg).unwrap();
        let mut fs_src = FileSource::new(path.clone(), 64);
        let streamed = run_source(
            &mut fs_src,
            &default_grid(),
            &cfg,
            EvidenceClass::SyntheticEngineCheck,
            &mut |_| Ok(()),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&mem.results).unwrap(),
            serde_json::to_string(&streamed.results).unwrap()
        );
        assert!(!mem.candidates.is_empty());
        assert_eq!(mem.candidates, streamed.candidates);
        assert_eq!(
            mem.normalized_input_sha256,
            streamed.normalized_input_sha256
        );
        assert_eq!(mem.experiment_sha256, streamed.experiment_sha256);
        assert_eq!(economic_fingerprint(&mem), economic_fingerprint(&streamed));
        let raw = format!("{:x}", Sha256::digest(text.as_bytes()));
        assert_eq!(streamed.raw_input_sha256.as_deref(), Some(raw.as_str()));
        assert_eq!(streamed.resources.passes, 4);
        assert_eq!(streamed.resources.raw_bytes_read, text.len() as u64 * 4);
    }

    #[test]
    fn file_modified_between_passes_aborts() {
        struct Mutating {
            inner: FileSource,
            path: std::path::PathBuf,
            visits: u32,
        }
        impl CaseSource for Mutating {
            fn visit(&mut self, f: &mut crate::source::Visitor<'_>) -> Result<(), CampaignError> {
                self.visits += 1;
                if self.visits == 2 {
                    let mut t = std::fs::read_to_string(&self.path).unwrap();
                    t.push('\n'); // blank line: same cases, different bytes
                    std::fs::write(&self.path, t).unwrap();
                }
                self.inner.visit(f)
            }
            fn raw_sha256(&self) -> Option<String> {
                self.inner.raw_sha256()
            }
        }
        let cases = generate(&Scenario::efficient(1, 2, 5));
        let tmp = TempDir::new("mut");
        let dir = &tmp.0;
        let path = dir.join("m.jsonl");
        std::fs::write(
            &path,
            cases
                .iter()
                .map(|c| to_jsonl_line(c) + "\n")
                .collect::<String>(),
        )
        .unwrap();
        let mut src = Mutating {
            inner: FileSource::new(path.clone(), 32),
            path,
            visits: 0,
        };
        let e = run_source(
            &mut src,
            &default_grid()[..3],
            &fast(),
            EvidenceClass::SyntheticEngineCheck,
            &mut |_| Ok(()),
        );
        assert_eq!(e.unwrap_err(), CampaignError::SourceChanged);
    }

    /// Removes its directory on drop so fixtures never pile up in the temp dir.
    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new(name: &str) -> Self {
            let d = std::env::temp_dir().join(format!("phoenix-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&d);
            std::fs::create_dir_all(&d).unwrap();
            Self(d)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    type Fixture = (
        Vec<MarketCase>,
        std::path::PathBuf,
        std::path::PathBuf,
        TempDir,
    );
    fn cached_fixture(tag: &str) -> Fixture {
        let cases = generate(&Scenario::lagged_quotes(3, 6, 99));
        let tmp = TempDir::new(&format!("cache-{tag}"));
        let path = tmp.0.join("cases.jsonl");
        let text: String = cases.iter().map(|c| to_jsonl_line(c) + "\n").collect();
        std::fs::write(&path, text).unwrap();
        let cache = tmp.0.join("cache");
        (cases, path, cache, tmp)
    }

    fn run_cached(
        path: &std::path::Path,
        cache: &std::path::Path,
    ) -> Result<Report, CampaignError> {
        let cfg = CampaignConfig {
            batch_size: 64,
            ..Default::default()
        };
        let mut src = crate::source::CachedSource::new(path.to_path_buf(), cache.to_path_buf(), 64);
        run_source(
            &mut src,
            &default_grid(),
            &cfg,
            EvidenceClass::SyntheticEngineCheck,
            &mut |_| Ok(()),
        )
    }

    #[test]
    fn cached_source_matches_in_memory_cold_and_warm() {
        use crate::source::CacheStatus;
        let (cases, path, cache, _tmp) = cached_fixture("parity");
        let cfg = CampaignConfig {
            batch_size: 64,
            ..Default::default()
        };
        let mem = run(&cases, &default_grid(), &cfg).unwrap();
        let raw_len = std::fs::metadata(&path).unwrap().len();
        let cold = run_cached(&path, &cache).unwrap();
        let warm = run_cached(&path, &cache).unwrap();
        for r in [&cold, &warm] {
            assert_eq!(economic_fingerprint(&mem), economic_fingerprint(r));
            assert_eq!(
                serde_json::to_string(&mem.results).unwrap(),
                serde_json::to_string(&r.results).unwrap()
            );
            assert_eq!(r.resources.passes, 4);
        }
        assert_eq!(cold.raw_input_sha256, warm.raw_input_sha256);
        assert_eq!(cold.resources.case_cache, Some(CacheStatus::Built));
        assert_eq!(warm.resources.case_cache, Some(CacheStatus::Hit));
        // cold: hash pass + one JSON decode; warm: hash pass only.
        assert_eq!(cold.resources.raw_bytes_read, raw_len * 2);
        assert_eq!(warm.resources.raw_bytes_read, raw_len);
        assert!(warm.resources.cache_bytes_read > 0);
        // The cache bytes are the normalized stream itself.
        let bin = std::fs::read_dir(&cache)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().is_some_and(|x| x == "bin"))
            .unwrap();
        let digest = format!("{:x}", Sha256::digest(std::fs::read(bin).unwrap()));
        assert_eq!(digest, mem.normalized_input_sha256);
    }

    #[test]
    fn corrupt_cache_is_refused() {
        let (_, path, cache, _tmp) = cached_fixture("corrupt");
        run_cached(&path, &cache).unwrap();
        let bin = std::fs::read_dir(&cache)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().is_some_and(|x| x == "bin"))
            .unwrap();
        let mut b = std::fs::read(&bin).unwrap();
        // A quantity byte deep inside: still decodes, so only the hash catches it.
        let k = b.len() / 2;
        b[k] ^= 0x01;
        std::fs::write(&bin, b).unwrap();
        match run_cached(&path, &cache) {
            Err(CampaignError::Source(m)) => assert!(m.contains("failed verification"), "{m}"),
            other => panic!("corrupt cache accepted: {:?}", other.map(|r| r.resources)),
        }
    }

    #[test]
    fn cached_source_detects_raw_change_between_passes() {
        let (_, path, cache, _tmp) = cached_fixture("mutate");
        let mut src = crate::source::CachedSource::new(path.clone(), cache, 32);
        src.visit(&mut |_| Ok(())).unwrap();
        let mut t = std::fs::read_to_string(&path).unwrap();
        t.push('\n');
        std::fs::write(&path, t).unwrap();
        assert_eq!(
            src.visit(&mut |_| Ok(())).unwrap_err(),
            CampaignError::SourceChanged
        );
    }

    #[test]
    fn different_raw_bytes_never_reuse_a_cache() {
        use crate::source::CacheStatus;
        let (_, path, cache, _tmp) = cached_fixture("rekey");
        run_cached(&path, &cache).unwrap();
        let mut t = std::fs::read_to_string(&path).unwrap();
        t.push('\n'); // same cases, different raw bytes
        std::fs::write(&path, t).unwrap();
        let r = run_cached(&path, &cache).unwrap();
        assert_eq!(r.resources.case_cache, Some(CacheStatus::Built));
    }
}

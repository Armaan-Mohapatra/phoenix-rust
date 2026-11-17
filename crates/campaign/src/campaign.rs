//! Chronological research campaign with an untouched holdout.
//!
//! Protocol (fixed before any data is seen):
//! 1. Split cases by time with `phoenix_research::SplitPlan`
//!    (discovery | embargo | validation | embargo | holdout).
//! 2. DISCOVERY: evaluate every grid configuration. Keep those with at least
//!    `min_trades` settled trades and positive discovery z; take the top K.
//! 3. VALIDATION: one-sided cluster z-test with Bonferroni alpha/K. Also
//!    require a positive mean under fee and latency stress.
//! 4. HOLDOUT: evaluated once, only for validation survivors, with alpha/M
//!    (M = survivors). Only then may a configuration be labelled
//!    `PaperResearchCandidate`. Nothing is labelled profitable.
use crate::engine::{evaluate, summarize, ExecutionModel, Metrics};
use crate::math::z_upper;
use crate::strategies::StrategyConfig;
use crate::{MarketCase, Provenance};
use phoenix_research::{Split, SplitPlan};
use serde::Serialize;
use sha2::{Digest, Sha256};
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
            execution: ExecutionModel::default(),
            stress_fee_permille: 1500,
            stress_latency_ns: 1_000_000_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CampaignError {
    NoCases,
    InvalidCase(usize),
    BudgetExceeded { requested: usize, budget: usize },
    InvalidConfig(String),
    MixedProvenance,
    EmptySplit(&'static str),
}

#[derive(Debug, Clone, Serialize)]
pub enum Verdict {
    RejectedDiscovery(String),
    RejectedValidation(String),
    RejectedHoldout(String),
    /// Passed every pre-registered gate on this dataset. Eligible for
    /// paper-only forward testing; NOT evidence of real profitability.
    PaperResearchCandidate,
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

#[derive(Debug, Clone, Serialize)]
pub struct SplitCounts {
    pub discovery: usize,
    pub validation: usize,
    pub holdout: usize,
    pub embargoed: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct Resources {
    pub cases: usize,
    pub snapshots: usize,
    pub decisions_evaluated: u64,
    pub wall_ms: u128,
    pub decisions_per_second: f64,
    pub threads: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub schema: &'static str,
    pub provenance: Provenance,
    pub dataset_sha256: String,
    pub grid_sha256: String,
    pub implementation_revision: &'static str,
    pub config: CampaignConfig,
    pub split_plan: [i64; 4],
    pub split_counts: SplitCounts,
    pub configs_tried: usize,
    pub selected_for_validation: usize,
    pub validation_alpha_per_test: f64,
    pub validation_z_critical: f64,
    pub holdout_evaluations: usize,
    pub holdout_alpha_per_test: Option<f64>,
    pub candidates: Vec<String>,
    pub results: Vec<ConfigReport>,
    pub resources: Resources,
    pub limitations: Vec<&'static str>,
}

/// Hash of the ordered case identities and source hashes.
pub fn dataset_sha256(cases: &[MarketCase]) -> String {
    let mut h = Sha256::new();
    for c in cases {
        for part in [&c.market, &c.source_sha256, &c.rules_sha256] {
            h.update(part.as_bytes());
            h.update([0]);
        }
        h.update(c.expiration_ns.to_le_bytes());
        h.update([c
            .winner
            .map_or(2, |w| (w == phoenix_domain::Side::Yes) as u8)]);
    }
    format!("{:x}", h.finalize())
}

pub fn grid_sha256(grid: &[StrategyConfig]) -> String {
    let mut h = Sha256::new();
    for g in grid {
        h.update(g.config_sha256().as_bytes());
    }
    format!("{:x}", h.finalize())
}

fn plan(cases: &[MarketCase], c: &CampaignConfig) -> SplitPlan {
    let t0 = cases.iter().map(|x| x.open_ns).min().expect("non-empty");
    let t1 = cases
        .iter()
        .map(|x| x.expiration_ns)
        .max()
        .expect("non-empty");
    let span = (t1 - t0) as i128;
    let at = |permille: i64| t0 + (span * permille as i128 / 1000) as i64;
    let embargo = c.embargo_s * 1_000_000_000;
    let discovery_end = at(c.discovery_fraction_permille);
    let validation_end = at(c.discovery_fraction_permille + c.validation_fraction_permille);
    SplitPlan {
        discovery_end_ns: discovery_end,
        validation_end_ns: validation_end,
        untouched_start_ns: validation_end + embargo,
        embargo_ns: embargo,
    }
}

/// Bounded-parallel map preserving input order.
fn par_map<T: Sync, R: Send>(items: &[T], threads: usize, f: impl Fn(&T) -> R + Sync) -> Vec<R> {
    let threads = threads.clamp(1, 16).min(items.len().max(1));
    if threads == 1 {
        return items.iter().map(&f).collect();
    }
    let chunk = items.len().div_ceil(threads);
    std::thread::scope(|s| {
        let handles: Vec<_> = items
            .chunks(chunk)
            .map(|part| s.spawn(|| part.iter().map(&f).collect::<Vec<R>>()))
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("worker panicked"))
            .collect()
    })
}

fn z_of(m: &Metrics) -> f64 {
    m.test.map_or(f64::NEG_INFINITY, |t| t.z)
}

pub fn run(
    cases: &[MarketCase],
    grid: &[StrategyConfig],
    cfg: &CampaignConfig,
) -> Result<Report, CampaignError> {
    let started = Instant::now();
    if cases.is_empty() {
        return Err(CampaignError::NoCases);
    }
    if grid.len() > cfg.max_configs {
        return Err(CampaignError::BudgetExceeded {
            requested: grid.len(),
            budget: cfg.max_configs,
        });
    }
    for g in grid {
        g.validate()
            .map_err(|e| CampaignError::InvalidConfig(format!("{e:?}")))?;
    }
    for (i, c) in cases.iter().enumerate() {
        c.validate().map_err(|_| CampaignError::InvalidCase(i))?;
    }
    let provenance = cases[0].provenance;
    if cases.iter().any(|c| c.provenance != provenance) {
        return Err(CampaignError::MixedProvenance);
    }
    let sp = plan(cases, cfg);
    let (mut disc, mut val, mut hold, mut emb) = (vec![], vec![], vec![], 0usize);
    for c in cases {
        match sp.classify(c.open_ns, c.expiration_ns) {
            Split::Discovery => disc.push(c),
            Split::Validation => val.push(c),
            Split::Untouched => hold.push(c),
            Split::Embargo => emb += 1,
        }
    }
    for (name, v) in [
        ("discovery", &disc),
        ("validation", &val),
        ("holdout", &hold),
    ] {
        if v.is_empty() {
            return Err(CampaignError::EmptySplit(name));
        }
    }
    let mut decisions = 0u64;

    // 1. Discovery.
    let discovery: Vec<Metrics> = par_map(grid, cfg.threads, |g| {
        summarize(&evaluate(g, &disc, &cfg.execution))
    });
    decisions += (grid.len() * disc.len()) as u64;
    let mut ranked: Vec<usize> = (0..grid.len())
        .filter(|&i| discovery[i].settled_trades >= cfg.min_trades && z_of(&discovery[i]) > 0.0)
        .collect();
    ranked.sort_by(|&a, &b| {
        z_of(&discovery[b])
            .partial_cmp(&z_of(&discovery[a]))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.cmp(&b))
    });
    ranked.truncate(cfg.top_k);
    let k = ranked.len().max(1);
    let val_alpha = cfg.alpha / k as f64;
    let val_z = z_upper(val_alpha);

    // 2. Validation (+ stress) for selected configurations only.
    let fee_x = ExecutionModel {
        fee_multiplier_permille: cfg.stress_fee_permille,
        ..cfg.execution
    };
    let lat_x = ExecutionModel {
        latency_ns: cfg.stress_latency_ns,
        ..cfg.execution
    };
    let selected: Vec<&StrategyConfig> = ranked.iter().map(|&i| &grid[i]).collect();
    let validated: Vec<(Metrics, Metrics, Metrics)> = par_map(&selected, cfg.threads, |g| {
        (
            summarize(&evaluate(g, &val, &cfg.execution)),
            summarize(&evaluate(g, &val, &fee_x)),
            summarize(&evaluate(g, &val, &lat_x)),
        )
    });
    decisions += (selected.len() * val.len() * 3) as u64;
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
    let survivors: Vec<usize> = (0..selected.len())
        .filter(|&j| passes_validation(&validated[j]).is_ok())
        .collect();

    // 3. Holdout: single evaluation for survivors only.
    let m = survivors.len();
    let hold_alpha = (m > 0).then(|| cfg.alpha / m as f64);
    let survivor_cfgs: Vec<&StrategyConfig> = survivors.iter().map(|&j| selected[j]).collect();
    let holdout: Vec<Metrics> = par_map(&survivor_cfgs, cfg.threads, |g| {
        summarize(&evaluate(g, &hold, &cfg.execution))
    });
    decisions += (survivor_cfgs.len() * hold.len()) as u64;

    // Assemble verdicts.
    let mut results = Vec::with_capacity(grid.len());
    let mut candidates = Vec::new();
    for (i, g) in grid.iter().enumerate() {
        let d = &discovery[i];
        let sel = ranked.iter().position(|&r| r == i);
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
                "non-positive discovery mean".into()
            } else {
                format!("not in top {}", cfg.top_k)
            }),
        };
        if let Some(j) = sel {
            let v = &validated[j];
            r.validation = Some(v.0.clone());
            r.validation_fee_stress = Some(v.1.clone());
            r.validation_latency_stress = Some(v.2.clone());
            r.verdict = match passes_validation(v) {
                Err(e) => Verdict::RejectedValidation(e),
                Ok(()) => {
                    let h = &holdout[survivors.iter().position(|&s| s == j).expect("survivor")];
                    r.holdout = Some(h.clone());
                    let hz = z_upper(hold_alpha.expect("m>0"));
                    if h.settled_trades < cfg.min_trades {
                        Verdict::RejectedHoldout(format!(
                            "only {} settled trades",
                            h.settled_trades
                        ))
                    } else if z_of(h) < hz {
                        Verdict::RejectedHoldout(format!("z {:.3} < critical {hz:.3}", z_of(h)))
                    } else {
                        candidates.push(r.id.clone());
                        Verdict::PaperResearchCandidate
                    }
                }
            };
        }
        results.push(r);
    }
    let wall = started.elapsed().as_millis();
    Ok(Report {
        schema: "phoenix-campaign-report-v1",
        provenance,
        dataset_sha256: dataset_sha256(cases),
        grid_sha256: grid_sha256(grid),
        implementation_revision: crate::strategies::IMPLEMENTATION_REVISION,
        config: cfg.clone(),
        split_plan: [sp.discovery_end_ns, sp.validation_end_ns, sp.untouched_start_ns, sp.embargo_ns],
        split_counts: SplitCounts {
            discovery: disc.len(),
            validation: val.len(),
            holdout: hold.len(),
            embargoed: emb,
        },
        configs_tried: grid.len(),
        selected_for_validation: selected.len(),
        validation_alpha_per_test: val_alpha,
        validation_z_critical: val_z,
        holdout_evaluations: survivor_cfgs.len(),
        holdout_alpha_per_test: hold_alpha,
        candidates,
        results,
        resources: Resources {
            cases: cases.len(),
            snapshots: cases.iter().map(|c| c.snapshots.len()).sum(),
            decisions_evaluated: decisions,
            wall_ms: wall,
            decisions_per_second: decisions as f64 / (wall.max(1) as f64 / 1000.0),
            threads: cfg.threads,
        },
        limitations: vec![
            "All fills are modeled taker fills against the book at arrival; no queue or maker model.",
            "Positions are held to expiration; no early exit is modeled.",
            "Cluster z-test assumes independence across clusters (time windows).",
            "PaperResearchCandidate means passing these gates on this dataset only.",
            "Synthetic provenance results validate the engine, never real-market edge.",
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::{default_grid, Family};
    use crate::synthetic::{generate, Scenario};

    fn fast() -> CampaignConfig {
        CampaignConfig {
            threads: 2,
            ..Default::default()
        }
    }

    #[test]
    fn budget_and_identity_guards() {
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
        let mut mixed = cases.clone();
        mixed[0].provenance = Provenance::OperatorNormalized;
        assert_eq!(
            run(&mixed, &grid[..2], &fast()).unwrap_err(),
            CampaignError::MixedProvenance
        );
    }

    #[test]
    fn splits_are_chronological_and_embargoed() {
        let cases = generate(&Scenario::efficient(2, 4, 2));
        let r = run(&cases, &default_grid()[..2], &fast()).unwrap();
        let c = &r.split_counts;
        assert!(c.discovery > c.validation && c.validation > 0 && c.holdout > 0);
        assert!(c.embargoed > 0);
        assert_eq!(
            c.discovery + c.validation + c.holdout + c.embargoed,
            cases.len()
        );
        let [d_end, v_end, u_start, _] = r.split_plan;
        assert!(d_end < v_end && v_end < u_start);
    }

    #[test]
    fn efficient_market_yields_no_candidates_false_positive_control() {
        for seed in [101, 202, 303] {
            let cases = generate(&Scenario::efficient(2, 8, seed));
            let r = run(&cases, &default_grid(), &fast()).unwrap();
            assert!(r.candidates.is_empty(), "seed {seed}: {:?}", r.candidates);
            let ctl = r
                .results
                .iter()
                .find(|x| x.config.family == Family::NoTrade)
                .unwrap();
            assert_eq!(ctl.discovery.net_ticks, 0);
        }
    }

    #[test]
    fn planted_quote_lag_is_detected_power_check() {
        let cases = generate(&Scenario::lagged_quotes(2, 8, 404));
        let r = run(&cases, &default_grid(), &fast()).unwrap();
        assert!(!r.candidates.is_empty(), "planted edge was not detected");
        for id in &r.candidates {
            let res = r.results.iter().find(|x| &x.id == id).unwrap();
            assert!(
                matches!(
                    res.config.family,
                    Family::ReferenceModel
                        | Family::VolRegimeModel
                        | Family::LiquidityFilteredModel
                        | Family::Ensemble
                        | Family::Momentum
                ),
                "unexpected candidate family {:?}",
                res.config.family
            );
            assert!(res.holdout.as_ref().unwrap().net_ticks > 0);
        }
        // Controls are never candidates.
        assert!(r
            .results
            .iter()
            .filter(|x| matches!(x.config.family, Family::NoTrade | Family::CoinFlip))
            .all(|x| !matches!(x.verdict, Verdict::PaperResearchCandidate)));
    }

    #[test]
    fn campaign_is_deterministic_across_thread_counts() {
        let cases = generate(&Scenario::shaded(2, 4, 7));
        let grid = default_grid();
        let a = run(
            &cases,
            &grid,
            &CampaignConfig {
                threads: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let b = run(
            &cases,
            &grid,
            &CampaignConfig {
                threads: 3,
                ..Default::default()
            },
        )
        .unwrap();
        let strip = |r: &Report| {
            (
                serde_json::to_value(&r.results).unwrap(),
                r.candidates.clone(),
                r.dataset_sha256.clone(),
            )
        };
        assert_eq!(strip(&a), strip(&b));
    }
}

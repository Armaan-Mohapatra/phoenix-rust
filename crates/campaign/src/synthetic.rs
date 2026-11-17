//! Deterministic synthetic 15-minute binary markets with KNOWN ground truth.
//!
//! Purpose: validate the research engine, not discover real edges. Every case
//! is labelled `Provenance::Synthetic`; reports carry that label.
//!
//! * `efficient`: quotes are the true model probability +- half spread, so
//!   no strategy should survive costs. Used as a false-positive control.
//! * `lagged_quotes`: the market maker prices off a stale reference, which
//!   plants a real, model-detectable edge. Used as a power check.
//! * `shaded`: favorite-longshot compression toward 50%.
//! * `peer_lead`: one asset's reference follows the BTC factor with a delay.
use crate::math::{norm_cdf, Rng};
use crate::{MarketCase, Provenance, Snapshot};
use phoenix_domain::{Book, Level, Price, Side};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize)]
pub struct Scenario {
    pub name: String,
    /// 1..=3 of BTC, ETH, SOL.
    pub assets: usize,
    pub days: u32,
    pub seed: u64,
    /// Market maker uses the reference from this many seconds ago.
    pub quote_lag_s: i64,
    /// 1000 = unbiased; 800 compresses p toward 0.5 by 20%.
    pub shade_permille: i64,
    /// ETH/SOL reference follows the BTC factor this many seconds late.
    pub peer_lead_s: i64,
    pub half_spread_ticks: i64,
    pub snapshot_interval_s: i64,
    pub window_s: i64,
}

impl Scenario {
    fn base(name: &str, assets: usize, days: u32, seed: u64) -> Self {
        Self {
            name: name.into(),
            assets: assets.clamp(1, 3),
            days: days.max(1),
            seed,
            quote_lag_s: 0,
            shade_permille: 1000,
            peer_lead_s: 0,
            half_spread_ticks: 150,
            snapshot_interval_s: 10,
            window_s: 900,
        }
    }
    pub fn efficient(assets: usize, days: u32, seed: u64) -> Self {
        Self::base("efficient", assets, days, seed)
    }
    pub fn lagged_quotes(assets: usize, days: u32, seed: u64) -> Self {
        Self {
            quote_lag_s: 60,
            ..Self::base("lagged_quotes", assets, days, seed)
        }
    }
    pub fn shaded(assets: usize, days: u32, seed: u64) -> Self {
        Self {
            shade_permille: 700,
            ..Self::base("shaded", assets, days, seed)
        }
    }
    pub fn peer_lead(assets: usize, days: u32, seed: u64) -> Self {
        Self {
            peer_lead_s: 30,
            ..Self::base("peer_lead", assets.max(2), days, seed)
        }
    }
    pub fn by_name(name: &str, assets: usize, days: u32, seed: u64) -> Option<Self> {
        match name {
            "efficient" => Some(Self::efficient(assets, days, seed)),
            "lagged_quotes" => Some(Self::lagged_quotes(assets, days, seed)),
            "shaded" => Some(Self::shaded(assets, days, seed)),
            "peer_lead" => Some(Self::peer_lead(assets, days, seed)),
            _ => None,
        }
    }
    pub fn sha256(&self) -> String {
        let j = serde_json::to_string(self).expect("serializable");
        format!("{:x}", Sha256::digest(j.as_bytes()))
    }
}

const ASSETS: [(&str, f64); 3] = [("BTC", 6_000_000.0), ("ETH", 300_000.0), ("SOL", 15_000.0)];
/// Per-sqrt-second volatility for low and high regimes (alternating days).
const SIGMA: [f64; 2] = [4.0e-5, 1.0e-4];
/// Synthetic epoch (2026-01-01T00:00:00Z) so timestamps look like real ns.
const EPOCH_NS: i64 = 1_767_225_600_000_000_000;

fn ticks(x: i64) -> Price {
    Price::new(x.clamp(0, 10_000) as u16).expect("clamped")
}

/// Generates all markets, ordered by (expiration, asset).
pub fn generate(s: &Scenario) -> Vec<MarketCase> {
    let horizon = i64::from(s.days) * 86_400;
    let mut rng = Rng::new(s.seed);
    let sigma_at = |t: i64| SIGMA[((t / 86_400) % 2) as usize];
    // Per-second BTC factor increments; peers load on the (lagged) factor.
    let n = horizon as usize + 1;
    let factor: Vec<f64> = (0..n).map(|_| rng.normal()).collect();
    let mut paths: Vec<Vec<i64>> = Vec::with_capacity(s.assets);
    for (a, (_, start)) in ASSETS.iter().enumerate().take(s.assets) {
        let mut idio = Rng::new(s.seed ^ (0x1000 + a as u64));
        let mut x = *start;
        let mut path = Vec::with_capacity(n);
        path.push(x.round() as i64);
        for t in 1..n {
            let z = if a == 0 {
                factor[t]
            } else {
                let lagged = t as i64 - s.peer_lead_s;
                let f = if lagged >= 1 {
                    factor[lagged as usize]
                } else {
                    0.0
                };
                0.8 * f + 0.6 * idio.normal()
            };
            x *= 1.0 + sigma_at(t as i64) * z;
            path.push(x.round() as i64);
        }
        paths.push(path);
    }
    let rules = format!("{:x}", Sha256::digest(b"phoenix-synthetic-binary-rules-v1"));
    let source = s.sha256();
    let mut book_rng = Rng::new(s.seed ^ 0xb00c);
    let mut cases = Vec::new();
    let windows = horizon / s.window_s;
    for w in 0..windows {
        let open = w * s.window_s;
        let exp = open + s.window_s;
        for (a, path) in paths.iter().enumerate() {
            let name = ASSETS[a].0;
            let market = format!("KX{name}15M-SYN-{w:05}");
            let strike = path[open as usize];
            let peer = if a == 0 {
                paths.get(1)
            } else {
                Some(&paths[0])
            };
            let mut snaps = Vec::new();
            let mut seq = 0u64;
            let mut t = open;
            while t < exp {
                let seen = (t - s.quote_lag_s).max(0) as usize;
                let sm = path[seen] as f64;
                let tau = (exp - t) as f64;
                let d = (sm - strike as f64) / sm;
                let mut p = norm_cdf(d / (sigma_at(t) * tau.sqrt()));
                p = 0.5 + (p - 0.5) * s.shade_permille as f64 / 1000.0;
                let fair = (p * 10_000.0).round() as i64;
                let bid = ((fair - s.half_spread_ticks).div_euclid(100) * 100).clamp(0, 9800);
                let ask = ((fair + s.half_spread_ticks + 99).div_euclid(100) * 100)
                    .clamp(bid + 100, 10_000);
                let mut yes_bids = Vec::new();
                let mut no_bids = Vec::new();
                for lvl in 0..3 {
                    let yb = bid - 100 * lvl;
                    if yb >= 100 {
                        yes_bids.push(Level {
                            price: ticks(yb),
                            quantity: 10 + book_rng.below(190),
                        });
                    }
                    let nb = 10_000 - ask - 100 * lvl;
                    if nb >= 100 {
                        no_bids.push(Level {
                            price: ticks(nb),
                            quantity: 10 + book_rng.below(190),
                        });
                    }
                }
                let rx = EPOCH_NS + t * 1_000_000_000;
                seq += 1;
                snaps.push(Snapshot {
                    received_ns: rx,
                    book: Book {
                        market: market.clone(),
                        yes_bids,
                        no_bids,
                        exchange_ns: rx - 50_000_000,
                        received_ns: rx,
                        session_id: format!("syn-{}", s.seed),
                        sequence: seq,
                    },
                    reference_cents: path[t as usize],
                    peer_reference_cents: peer.map(|p| p[t as usize]),
                    perp_basis_bps: Some((book_rng.normal() * 8.0).round() as i32),
                });
                t += s.snapshot_interval_s;
            }
            let winner = if path[exp as usize] >= strike {
                Side::Yes
            } else {
                Side::No
            };
            cases.push(MarketCase {
                market,
                event: format!("{name}-W{w:05}"),
                cluster: format!("W{w:05}"),
                asset: name.into(),
                open_ns: EPOCH_NS + open * 1_000_000_000,
                expiration_ns: EPOCH_NS + exp * 1_000_000_000,
                strike_cents: strike,
                rules_sha256: rules.clone(),
                source_sha256: source.clone(),
                provenance: Provenance::Synthetic,
                snapshots: snaps,
                winner: Some(winner),
            });
        }
    }
    cases
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generation_is_deterministic_and_valid() {
        let a = generate(&Scenario::efficient(2, 1, 5));
        let b = generate(&Scenario::efficient(2, 1, 5));
        assert_eq!(a.len(), 2 * 96);
        for (x, y) in a.iter().zip(&b) {
            x.validate().unwrap();
            assert_eq!(x.snapshots, y.snapshots);
            assert_eq!(x.winner, y.winner);
        }
        let c = generate(&Scenario::efficient(2, 1, 6));
        assert_ne!(a[5].snapshots, c[5].snapshots);
    }
    #[test]
    fn efficient_quotes_are_roughly_calibrated() {
        // Mid-price Brier must beat the 0.25 coin baseline clearly.
        let cases = generate(&Scenario::efficient(1, 4, 21));
        // Deep in/out-of-the-money markets have no bid on one side, hence no
        // mid; those rows are excluded, and must not be the majority.
        let (mut brier, mut rows) = (0.0, 0usize);
        for c in &cases {
            let v = c.view_at(c.expiration_ns - 120_000_000_000).unwrap();
            let Some(mid) = v.yes_mid_ticks() else {
                continue;
            };
            let p = mid as f64 / 10_000.0;
            let y = if c.winner == Some(Side::Yes) {
                1.0
            } else {
                0.0
            };
            brier += (p - y) * (p - y);
            rows += 1;
        }
        assert!(rows * 2 > cases.len(), "{rows} of {}", cases.len());
        brier /= rows as f64;
        // Uninformed 0.5 forecast scores 0.25; calibrated quotes must beat it clearly.
        assert!(brier < 0.20, "brier {brier}");
    }
}

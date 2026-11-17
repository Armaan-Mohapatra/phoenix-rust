//! Decision-only replicas of the frozen October-2026 Luna strategies that can be reproduced from an
//! order book alone. Source: `project/luna_experimental_paper.py`, SHA-256
//! `362038d643fbece296debc52a677c257894d7719517e04ac377d23dfc11e4d6c` (matches the frozen manifest).
//!
//! * `market_only` (`market_only_p0_v2`): probability = quote midpoint.
//! * `book_flow_imbalance` (`book_flow_imbalance_p0_v3`), BOOK-ONLY SUBSET: the original also adds
//!   `0.03 * signed recent-trade flow` and `0.01 * clipped spot momentum`. Neither input exists in the
//!   captured cases, so this replica uses the book term only. It is NOT the full strategy.
//!
//! Strategies that need the cached LLM forecast or the CF Benchmarks reference (`luna_frozen_directional`,
//! `l_mase_v2`, `market_value_discrepancy`, `logistic_luna_hybrid`, `maker_queue`, `conditional_luna_trust`,
//! `settlement_threshold_model`, `late_window_threshold`) cannot be replicated from this capture, and
//! `volatility_regime_calibration` is 100 % fallback in the source (`REGIME_CALIBRATOR_NOT_FITTED`).
//!
//! Arithmetic mirrors the Python (f64, same expression order) so decisions can be compared exactly.
use serde::{Deserialize, Serialize};

pub const MIN_ESTIMATED_EDGE: f64 = 0.01;

/// Best-first levels as `(price ticks 1e-4, whole contracts)`.
#[derive(Debug, Clone, Deserialize)]
pub struct Obs {
    pub yes_bids: Vec<(u16, u64)>,
    pub no_bids: Vec<(u16, u64)>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Level(f64, f64);

#[derive(Debug, Clone, Copy, Default)]
struct Quotes {
    yes_bid: Option<Level>,
    yes_ask: Option<Level>,
    no_bid: Option<Level>,
    no_ask: Option<Level>,
}

fn valid(levels: &[(u16, u64)]) -> Vec<Level> {
    levels
        .iter()
        .map(|(p, q)| Level(f64::from(*p) / 10_000.0, *q as f64))
        .filter(|l| 0.0 < l.0 && l.0 < 1.0 && l.1 > 0.0)
        .collect()
}
fn best_bid(l: &[Level]) -> Option<Level> {
    l.iter()
        .copied()
        .reduce(|a, b| if b.0 > a.0 { b } else { a })
}

fn quotes(o: &Obs) -> Quotes {
    let (yb, nb) = (best_bid(&valid(&o.yes_bids)), best_bid(&valid(&o.no_bids)));
    // Kalshi books carry bids only: the opposite side's best bid is the complementary ask.
    Quotes {
        yes_bid: yb,
        no_bid: nb,
        yes_ask: nb.map(|b| Level(1.0 - b.0, b.1)),
        no_ask: yb.map(|b| Level(1.0 - b.0, b.1)),
    }
}

fn market_yes(q: &Quotes) -> Option<f64> {
    let (yb, ya, nb, na) = (q.yes_bid, q.yes_ask, q.no_bid, q.no_ask);
    if let (Some(yb), Some(ya)) = (yb, ya) {
        return Some((yb.0 + ya.0) / 2.0);
    }
    if let (Some(ya), Some(nb)) = (ya, nb) {
        return Some((ya.0 + 1.0 - nb.0) / 2.0);
    }
    if let (Some(ya), Some(na)) = (ya, na) {
        return Some((ya.0 + 1.0 - na.0) / 2.0);
    }
    if let Some(ya) = ya {
        return Some(ya.0);
    }
    na.map(|na| 1.0 - na.0)
}

/// `fee_per_contract(price, 1.0)`: 7 % quadratic fee rounded UP to 1e-4 USD.
pub fn fee(price: f64) -> f64 {
    (0.07 * 1.0 * price * (1.0 - price) * 10_000.0 - 1e-9).ceil() / 10_000.0
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Decision {
    pub p_yes: Option<f64>,
    pub recommendation: &'static str, // "YES" | "NO" | "SKIP"
    pub edge: Option<f64>,
}

fn recommend(p_yes: Option<f64>, q: &Quotes) -> Decision {
    let Some(p) = p_yes else {
        return Decision {
            p_yes,
            recommendation: "SKIP",
            edge: None,
        };
    };
    let mut best: Option<(f64, &'static str)> = None;
    for (name, prob, ask) in [("YES", p, q.yes_ask), ("NO", 1.0 - p, q.no_ask)] {
        let Some(a) = ask else { continue };
        if a.1 < 1.0 {
            continue;
        }
        // The Python rounds to 6 places before comparing.
        let ev = ((prob - a.0 - fee(a.0)) * 1e6).round() / 1e6;
        // `max((ev, side))` over tuples: larger EV wins; on an exact tie "YES" > "NO" lexicographically.
        if best.map_or(true, |(e, s)| ev > e || (ev == e && name > s)) {
            best = Some((ev, name));
        }
    }
    match best {
        Some((e, side)) if e > MIN_ESTIMATED_EDGE => Decision {
            p_yes,
            recommendation: side,
            edge: Some(e),
        },
        Some((e, _)) => Decision {
            p_yes,
            recommendation: "SKIP",
            edge: Some(e),
        },
        None => Decision {
            p_yes,
            recommendation: "SKIP",
            edge: None,
        },
    }
}

pub fn market_only(o: &Obs) -> Decision {
    let q = quotes(o);
    recommend(market_yes(&q), &q)
}

/// Book-only subset of `book_flow_imbalance_p0_v3`; falls back to the market midpoint exactly as the
/// original does when the preferred probability is unavailable.
pub fn book_flow(o: &Obs) -> Decision {
    let q = quotes(o);
    let mid = market_yes(&q);
    let bids = valid(&o.yes_bids);
    // yes_asks (complement of the NO bids), best (lowest) ask first, as presented to the original.
    let asks: Vec<Level> = valid(&o.no_bids)
        .iter()
        .map(|l| Level(1.0 - l.0, l.1))
        .collect();
    let bid_depth: f64 = bids.iter().take(5).map(|l| l.1).sum();
    let ask_depth: f64 = asks.iter().take(5).map(|l| l.1).sum();
    let imbalance =
        (bid_depth + ask_depth > 0.0).then(|| (bid_depth - ask_depth) / (bid_depth + ask_depth));
    let preferred = match (mid, imbalance) {
        (Some(m), Some(i)) => Some((m + (0.05 * i + 0.0 + 0.01 * 0.0)).clamp(0.01, 0.99)),
        _ => None,
    };
    recommend(preferred.or(mid), &q)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn obs(y: &[(u16, u64)], n: &[(u16, u64)]) -> Obs {
        Obs {
            yes_bids: y.to_vec(),
            no_bids: n.to_vec(),
        }
    }
    #[test]
    fn fee_rounds_up_to_one_tenth_of_a_cent() {
        assert_eq!(fee(0.5), 0.0175);
        assert_eq!(fee(0.99), 0.0007);
        assert_eq!(fee(0.0), 0.0);
    }
    #[test]
    fn market_only_is_a_structural_no_trade() {
        // p = (yes_bid + (1 - no_bid)) / 2 makes both sides' EV = -(half spread) - fee < 0, so it never trades.
        let d = market_only(&obs(&[(4000, 10), (3900, 5)], &[(5500, 7)]));
        assert!((d.p_yes.unwrap() - 0.425).abs() < 1e-12);
        assert_eq!(d.recommendation, "SKIP");
    }
    #[test]
    fn book_flow_shades_the_midpoint_by_depth_imbalance() {
        // heavy YES bids: imbalance > 0 raises p_yes above the midpoint; one-sided books fall back sanely.
        let d = book_flow(&obs(&[(4000, 90)], &[(5500, 10)]));
        let mid = (0.40 + 0.45) / 2.0;
        assert!(d.p_yes.unwrap() > mid);
        assert_eq!(book_flow(&obs(&[], &[])).recommendation, "SKIP");
        // YES bids only: midpoint falls back to the NO-ask complement (0.40), imbalance is +1 -> 0.40 + 0.05.
        let one_sided = book_flow(&obs(&[(4000, 10)], &[]));
        assert!((one_sided.p_yes.unwrap() - 0.45).abs() < 1e-12);
    }
}

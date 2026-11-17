//! Registry-driven strategy families.
//!
//! Every configuration is `(family, bounded integer params, decision time)`.
//! Its identity is the SHA-256 of a canonical JSON encoding plus the family
//! implementation revision, so any change in code meaning or parameters
//! produces a new ID. Strategies are pure functions of a [`CausalView`].
//!
//! Each family states a hypothesis. None is assumed to have an edge; the
//! campaign gate decides, and a rejection is a valid result.
use crate::math::{fnv1a, norm_cdf};
use crate::{Abstain, CausalView, Signal};
use phoenix_domain::{FeeRate, Price, Side};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Bump when any family's decision logic changes meaning.
/// r2: LeadLag trades peer-minus-own divergence (r1 required a flat own
/// reference and abstained on ~97% of decisions; underpowered by design).
pub const IMPLEMENTATION_REVISION: &str = "phoenix-campaign-strategies-r2";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Family {
    /// Control: never trades. Any non-zero result is an engine defect.
    NoTrade,
    /// Negative control: a seeded coin decides the side. Expected to lose
    /// roughly spread plus fees.
    CoinFlip,
    /// Favorite-longshot bias: high-priced sides are underpriced.
    Favorite,
    /// The reverse claim: cheap sides are underpriced (lottery demand).
    Longshot,
    /// Model-vs-market: P(YES) = Phi(ln-distance to strike / (sigma*sqrt(tau)))
    /// from realized reference volatility; trade the side whose fair value
    /// exceeds ask plus fee by a margin.
    ReferenceModel,
    /// Short-horizon reference momentum persists to expiry.
    Momentum,
    /// Market-price overreaction without reference movement mean-reverts.
    MeanReversion,
    /// Resting bid depth imbalance predicts the winner.
    BookImbalance,
    /// ReferenceModel restricted to one realized-volatility regime.
    VolRegimeModel,
    /// A peer asset's reference leads this asset's: when the peer has moved
    /// more than this asset over the lookback, this asset catches up.
    LeadLag,
    /// Perpetual-futures basis sign predicts spot direction.
    PerpBasis,
    /// Consensus of model, momentum and imbalance votes.
    Ensemble,
    /// Momentum signal inverted (tests whether momentum is actually reversal).
    InvertedMomentum,
    /// ReferenceModel only when spread and top-of-book depth are adequate.
    LiquidityFilteredModel,
    /// ReferenceModel whose reference is shifted by beta x (peer - own)
    /// return over the lookback: the expected catch-up if the peer leads.
    LeadAdjustedModel,
}

type Bounds = &'static [(&'static str, i64, i64)];
/// Parameter schema per family: name, inclusive min, inclusive max.
pub fn schema(f: Family) -> Bounds {
    use Family::*;
    match f {
        NoTrade => &[],
        CoinFlip => &[("seed", 0, 1 << 30), ("max_ask", 100, 9900)],
        Favorite => &[("min_ask", 5000, 9900), ("max_ask", 5000, 9900)],
        Longshot => &[("max_ask", 100, 5000)],
        ReferenceModel => &[("window", 5, 200), ("min_edge", 0, 5000)],
        Momentum | InvertedMomentum => &[
            ("lookback_s", 5, 600),
            ("threshold_bps", 1, 500),
            ("max_ask", 100, 9900),
        ],
        MeanReversion => &[
            ("lookback_s", 5, 600),
            ("move_ticks", 100, 5000),
            ("reference_tolerance_bps", 0, 500),
        ],
        BookImbalance => &[
            ("levels", 1, 10),
            ("threshold_permille", 1, 1000),
            ("max_ask", 100, 9900),
        ],
        VolRegimeModel => &[
            ("window", 5, 200),
            ("min_edge", 0, 5000),
            ("high_regime", 0, 1),
            ("sigma_threshold_ppm", 1, 100_000),
        ],
        LeadLag => &[
            ("lookback_s", 5, 600),
            ("divergence_bps", 1, 500),
            ("max_ask", 100, 9900),
        ],
        PerpBasis => &[("threshold_bps", 1, 1000), ("max_ask", 100, 9900)],
        Ensemble => &[("need", 2, 3)],
        LiquidityFilteredModel => &[
            ("window", 5, 200),
            ("min_edge", 0, 5000),
            ("max_spread", 100, 5000),
            ("min_depth", 1, 100_000),
        ],
        LeadAdjustedModel => &[
            ("window", 5, 200),
            ("min_edge", 0, 5000),
            ("lookback_s", 5, 600),
            ("beta_permille", 0, 2000),
        ],
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub family: Family,
    /// Decision time: seconds before expiration.
    pub tte_s: i64,
    pub params: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    UnknownParam(String),
    MissingParam(&'static str),
    OutOfBounds(&'static str),
    BadDecisionTime,
    Inconsistent,
}

impl StrategyConfig {
    pub fn new(family: Family, tte_s: i64, params: &[(&str, i64)]) -> Result<Self, ConfigError> {
        let c = Self {
            family,
            tte_s,
            params: params.iter().map(|(k, v)| ((*k).to_owned(), *v)).collect(),
        };
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(1..=3600).contains(&self.tte_s) {
            return Err(ConfigError::BadDecisionTime);
        }
        let s = schema(self.family);
        for k in self.params.keys() {
            if !s.iter().any(|(n, _, _)| n == k) {
                return Err(ConfigError::UnknownParam(k.clone()));
            }
        }
        for (name, lo, hi) in s {
            let v = *self
                .params
                .get(*name)
                .ok_or(ConfigError::MissingParam(name))?;
            if v < *lo || v > *hi {
                return Err(ConfigError::OutOfBounds(name));
            }
        }
        if self.family == Family::Favorite && self.p("min_ask") > self.p("max_ask") {
            return Err(ConfigError::Inconsistent);
        }
        Ok(())
    }
    fn p(&self, k: &str) -> i64 {
        self.params[k]
    }
    /// Canonical identity: SHA-256 over revision + canonical JSON.
    pub fn config_sha256(&self) -> String {
        let canonical = serde_json::to_string(self).expect("serializable");
        let mut h = Sha256::new();
        h.update(IMPLEMENTATION_REVISION.as_bytes());
        h.update([0]);
        h.update(canonical.as_bytes());
        format!("{:x}", h.finalize())
    }
    pub fn id(&self) -> String {
        format!("{:?}-{}", self.family, &self.config_sha256()[..12])
    }

    pub fn decide(&self, v: &CausalView<'_>) -> Signal {
        use Family::*;
        match self.family {
            NoTrade => Signal::Abstain(Abstain::Control),
            CoinFlip => {
                let side = if fnv1a(&[v.event(), &self.p("seed").to_string()]) & 1 == 0 {
                    Side::Yes
                } else {
                    Side::No
                };
                buy_if_ask_le(v, side, self.p("max_ask"), None)
            }
            Favorite => favorite(v, self.p("min_ask"), self.p("max_ask")),
            Longshot => longshot(v, self.p("max_ask")),
            ReferenceModel => reference_model(v, self.p("window"), self.p("min_edge")),
            Momentum => momentum(v, self.p("lookback_s"), self.p("threshold_bps"))
                .map_or_else(Signal::Abstain, |s| {
                    buy_if_ask_le(v, s, self.p("max_ask"), None)
                }),
            InvertedMomentum => momentum(v, self.p("lookback_s"), self.p("threshold_bps"))
                .map_or_else(Signal::Abstain, |s| {
                    buy_if_ask_le(v, opposite(s), self.p("max_ask"), None)
                }),
            MeanReversion => mean_reversion(
                v,
                self.p("lookback_s"),
                self.p("move_ticks"),
                self.p("reference_tolerance_bps"),
            ),
            BookImbalance => imbalance(v, self.p("levels"), self.p("threshold_permille"))
                .map_or_else(Signal::Abstain, |s| {
                    buy_if_ask_le(v, s, self.p("max_ask"), None)
                }),
            VolRegimeModel => {
                let Some(sigma) = realized_sigma(v, self.p("window")) else {
                    return Signal::Abstain(Abstain::InsufficientHistory);
                };
                let high = sigma * 1e6 >= self.p("sigma_threshold_ppm") as f64;
                if high != (self.p("high_regime") == 1) {
                    return Signal::Abstain(Abstain::RegimeMismatch);
                }
                reference_model(v, self.p("window"), self.p("min_edge"))
            }
            LeadLag => lead_lag(v, self.p("lookback_s"), self.p("divergence_bps"))
                .map_or_else(Signal::Abstain, |s| {
                    buy_if_ask_le(v, s, self.p("max_ask"), None)
                }),
            PerpBasis => {
                let Some(b) = v.latest().perp_basis_bps else {
                    return Signal::Abstain(Abstain::MissingFeature);
                };
                let t = self.p("threshold_bps") as i32;
                let side = if b >= t {
                    Side::Yes
                } else if b <= -t {
                    Side::No
                } else {
                    return Signal::Abstain(Abstain::NoSignal);
                };
                buy_if_ask_le(v, side, self.p("max_ask"), None)
            }
            Ensemble => ensemble(v, self.p("need")),
            LeadAdjustedModel => lead_adjusted_model(
                v,
                self.p("window"),
                self.p("min_edge"),
                self.p("lookback_s"),
                self.p("beta_permille"),
            ),
            LiquidityFilteredModel => {
                let b = &v.latest().book;
                match b.spread(Side::Yes) {
                    None => return Signal::Abstain(Abstain::NoQuote),
                    Some(s) if i64::from(s) > self.p("max_spread") => {
                        return Signal::Abstain(Abstain::SpreadTooWide)
                    }
                    _ => {}
                }
                let sig = reference_model(v, self.p("window"), self.p("min_edge"));
                if let Signal::Buy { side, .. } = sig {
                    let depth = b.ask_depth(side).first().map_or(0, |l| l.quantity);
                    if (depth as i64) < self.p("min_depth") {
                        return Signal::Abstain(Abstain::DepthTooThin);
                    }
                }
                sig
            }
        }
    }
}

fn opposite(s: Side) -> Side {
    match s {
        Side::Yes => Side::No,
        Side::No => Side::Yes,
    }
}

fn buy_if_ask_le(v: &CausalView<'_>, side: Side, max_ask: i64, fair_yes: Option<f64>) -> Signal {
    match v.best_ask(side) {
        None => Signal::Abstain(Abstain::NoQuote),
        Some(a) if i64::from(a.ticks()) > max_ask => Signal::Abstain(Abstain::PriceOutOfBand),
        Some(a) => Signal::Buy {
            side,
            limit: a,
            fair_yes,
        },
    }
}

fn favorite(v: &CausalView<'_>, lo: i64, hi: i64) -> Signal {
    let (Some(y), Some(n)) = (v.best_ask(Side::Yes), v.best_ask(Side::No)) else {
        return Signal::Abstain(Abstain::NoQuote);
    };
    let (side, ask) = if y >= n {
        (Side::Yes, y)
    } else {
        (Side::No, n)
    };
    let t = i64::from(ask.ticks());
    if t < lo || t > hi {
        return Signal::Abstain(Abstain::PriceOutOfBand);
    }
    Signal::Buy {
        side,
        limit: ask,
        fair_yes: None,
    }
}

fn longshot(v: &CausalView<'_>, hi: i64) -> Signal {
    let (Some(y), Some(n)) = (v.best_ask(Side::Yes), v.best_ask(Side::No)) else {
        return Signal::Abstain(Abstain::NoQuote);
    };
    let (side, ask) = if y <= n {
        (Side::Yes, y)
    } else {
        (Side::No, n)
    };
    if i64::from(ask.ticks()) > hi {
        return Signal::Abstain(Abstain::PriceOutOfBand);
    }
    Signal::Buy {
        side,
        limit: ask,
        fair_yes: None,
    }
}

/// Realized volatility of the reference per sqrt(second), from the last
/// `window` snapshot-to-snapshot simple returns. Needs at least 5 returns.
pub fn realized_sigma(v: &CausalView<'_>, window: i64) -> Option<f64> {
    let h = v.history();
    let take = (window as usize).min(h.len().saturating_sub(1));
    if take < 5 {
        return None;
    }
    let seg = &h[h.len() - take - 1..];
    let (mut ss, mut dt) = (0.0f64, 0.0f64);
    for w in seg.windows(2) {
        let (a, b) = (w[0].reference_cents as f64, w[1].reference_cents as f64);
        if a <= 0.0 {
            return None;
        }
        let r = (b - a) / a;
        ss += r * r;
        dt += (w[1].received_ns - w[0].received_ns) as f64 / 1e9;
    }
    if dt <= 0.0 {
        return None;
    }
    Some((ss / dt).sqrt())
}

/// Model probability that YES (reference >= strike at expiry) wins.
pub fn model_prob_yes(v: &CausalView<'_>, sigma: f64) -> Option<f64> {
    let s = v.latest().reference_cents as f64;
    let k = v.strike_cents() as f64;
    if s <= 0.0 || k <= 0.0 {
        return None;
    }
    let scale = sigma * v.seconds_to_expiry().sqrt();
    let d = (s - k) / s;
    if scale < 1e-12 {
        return Some(if d >= 0.0 { 1.0 } else { 0.0 });
    }
    Some(norm_cdf(d / scale))
}

/// Expected value per contract in ticks after a one-contract taker fee.
fn net_edge_ticks(fair_side: f64, ask: Price) -> i64 {
    let fee = FeeRate::STANDARD_TAKER.charge(1, ask).unwrap_or(i128::MAX) as i64;
    (fair_side * 10_000.0).floor() as i64 - i64::from(ask.ticks()) - fee
}

fn reference_model(v: &CausalView<'_>, window: i64, min_edge: i64) -> Signal {
    let Some(sigma) = realized_sigma(v, window) else {
        return Signal::Abstain(Abstain::InsufficientHistory);
    };
    let Some(p) = model_prob_yes(v, sigma) else {
        return Signal::Abstain(Abstain::MissingFeature);
    };
    trade_on_fair(v, p, min_edge)
}

fn lead_adjusted_model(
    v: &CausalView<'_>,
    window: i64,
    min_edge: i64,
    lookback_s: i64,
    beta: i64,
) -> Signal {
    let Some(sigma) = realized_sigma(v, window) else {
        return Signal::Abstain(Abstain::InsufficientHistory);
    };
    let Some(then) = v.at_or_before(lookback_s * 1_000_000_000) else {
        return Signal::Abstain(Abstain::InsufficientHistory);
    };
    let now = v.latest();
    let (Some(p1), Some(p0)) = (now.peer_reference_cents, then.peer_reference_cents) else {
        return Signal::Abstain(Abstain::MissingFeature);
    };
    if p0 <= 0 || then.reference_cents <= 0 {
        return Signal::Abstain(Abstain::MissingFeature);
    }
    let peer = (p1 - p0) as f64 / p0 as f64;
    let own = (now.reference_cents - then.reference_cents) as f64 / then.reference_cents as f64;
    let s = now.reference_cents as f64 * (1.0 + beta as f64 / 1000.0 * (peer - own));
    let k = v.strike_cents() as f64;
    let scale = sigma * v.seconds_to_expiry().sqrt();
    if s <= 0.0 || k <= 0.0 || scale < 1e-12 {
        return Signal::Abstain(Abstain::MissingFeature);
    }
    trade_on_fair(v, norm_cdf((s - k) / s / scale), min_edge)
}

fn trade_on_fair(v: &CausalView<'_>, p: f64, min_edge: i64) -> Signal {
    let (Some(y), Some(n)) = (v.best_ask(Side::Yes), v.best_ask(Side::No)) else {
        return Signal::Abstain(Abstain::NoQuote);
    };
    let ey = net_edge_ticks(p, y);
    let en = net_edge_ticks(1.0 - p, n);
    let (side, edge, ask) = if ey >= en {
        (Side::Yes, ey, y)
    } else {
        (Side::No, en, n)
    };
    if edge < min_edge {
        return Signal::Abstain(Abstain::EdgeBelowThreshold);
    }
    Signal::Buy {
        side,
        limit: ask,
        fair_yes: Some(p),
    }
}

fn ret_bps(now: i64, then: i64) -> Option<i64> {
    if then <= 0 {
        return None;
    }
    Some(((now - then) as i128 * 10_000 / then as i128) as i64)
}

fn momentum(v: &CausalView<'_>, lookback_s: i64, thr: i64) -> Result<Side, Abstain> {
    let then = v
        .at_or_before(lookback_s * 1_000_000_000)
        .ok_or(Abstain::InsufficientHistory)?;
    let r =
        ret_bps(v.latest().reference_cents, then.reference_cents).ok_or(Abstain::MissingFeature)?;
    if r >= thr {
        Ok(Side::Yes)
    } else if r <= -thr {
        Ok(Side::No)
    } else {
        Err(Abstain::NoSignal)
    }
}

fn mean_reversion(v: &CausalView<'_>, lookback_s: i64, move_ticks: i64, tol: i64) -> Signal {
    let Some(then) = v.at_or_before(lookback_s * 1_000_000_000) else {
        return Signal::Abstain(Abstain::InsufficientHistory);
    };
    let mid = |b: &phoenix_domain::Book| -> Option<i64> {
        Some(
            (i64::from(b.best_bid(Side::Yes)?.ticks()) + i64::from(b.best_ask(Side::Yes)?.ticks()))
                / 2,
        )
    };
    let (Some(m1), Some(m0)) = (mid(&v.latest().book), mid(&then.book)) else {
        return Signal::Abstain(Abstain::NoQuote);
    };
    let Some(r) = ret_bps(v.latest().reference_cents, then.reference_cents) else {
        return Signal::Abstain(Abstain::MissingFeature);
    };
    if r.abs() > tol {
        return Signal::Abstain(Abstain::NoSignal);
    }
    let side = if m1 - m0 >= move_ticks {
        Side::No
    } else if m0 - m1 >= move_ticks {
        Side::Yes
    } else {
        return Signal::Abstain(Abstain::NoSignal);
    };
    buy_if_ask_le(v, side, 9900, None)
}

fn imbalance(v: &CausalView<'_>, levels: i64, thr: i64) -> Result<Side, Abstain> {
    let b = &v.latest().book;
    let n = levels as usize;
    let y: u128 = b
        .yes_bids
        .iter()
        .take(n)
        .map(|l| u128::from(l.quantity))
        .sum();
    let no: u128 = b
        .no_bids
        .iter()
        .take(n)
        .map(|l| u128::from(l.quantity))
        .sum();
    if y + no == 0 {
        return Err(Abstain::NoQuote);
    }
    let imb = (y as i128 - no as i128) * 1000 / (y + no) as i128;
    if imb >= thr as i128 {
        Ok(Side::Yes)
    } else if imb <= -(thr as i128) {
        Ok(Side::No)
    } else {
        Err(Abstain::NoSignal)
    }
}

fn lead_lag(v: &CausalView<'_>, lookback_s: i64, divergence: i64) -> Result<Side, Abstain> {
    let then = v
        .at_or_before(lookback_s * 1_000_000_000)
        .ok_or(Abstain::InsufficientHistory)?;
    let now = v.latest();
    let (Some(p1), Some(p0)) = (now.peer_reference_cents, then.peer_reference_cents) else {
        return Err(Abstain::MissingFeature);
    };
    let peer = ret_bps(p1, p0).ok_or(Abstain::MissingFeature)?;
    let own = ret_bps(now.reference_cents, then.reference_cents).ok_or(Abstain::MissingFeature)?;
    let gap = peer - own;
    if gap >= divergence {
        Ok(Side::Yes)
    } else if gap <= -divergence {
        Ok(Side::No)
    } else {
        Err(Abstain::NoSignal)
    }
}

fn ensemble(v: &CausalView<'_>, need: i64) -> Signal {
    let model = reference_model(v, 30, 100);
    let mut yes = 0;
    let mut no = 0;
    let mut fair = None;
    if let Signal::Buy { side, fair_yes, .. } = model {
        fair = fair_yes;
        if side == Side::Yes {
            yes += 1
        } else {
            no += 1
        }
    }
    for s in [momentum(v, 60, 5), imbalance(v, 3, 300)]
        .into_iter()
        .flatten()
    {
        if s == Side::Yes {
            yes += 1
        } else {
            no += 1
        }
    }
    let side = if yes >= need && no == 0 {
        Side::Yes
    } else if no >= need && yes == 0 {
        Side::No
    } else {
        return Signal::Abstain(Abstain::NoConsensus);
    };
    buy_if_ask_le(v, side, 9500, fair)
}

/// The default bounded research grid. Size is fixed and reported; the
/// campaign refuses grids above its configured budget.
pub fn default_grid() -> Vec<StrategyConfig> {
    use Family::*;
    let mut g = Vec::new();
    let mut add = |f: Family, tte: i64, p: &[(&str, i64)]| {
        g.push(StrategyConfig::new(f, tte, p).expect("default grid is within schema"));
    };
    add(NoTrade, 300, &[]);
    add(CoinFlip, 300, &[("seed", 1), ("max_ask", 9900)]);
    for tte in [600, 300, 120] {
        for lo in [6500, 7500, 8500] {
            add(Favorite, tte, &[("min_ask", lo), ("max_ask", 9500)]);
        }
    }
    for tte in [600, 300] {
        for hi in [1500, 2500] {
            add(Longshot, tte, &[("max_ask", hi)]);
        }
    }
    for tte in [600, 300, 120, 60] {
        for w in [30, 90] {
            for e in [100, 300, 600] {
                add(ReferenceModel, tte, &[("window", w), ("min_edge", e)]);
            }
        }
    }
    for tte in [300, 120] {
        for lb in [30, 120] {
            for thr in [5, 15] {
                add(
                    Momentum,
                    tte,
                    &[
                        ("lookback_s", lb),
                        ("threshold_bps", thr),
                        ("max_ask", 7000),
                    ],
                );
            }
            for mv in [1000, 2000] {
                add(
                    MeanReversion,
                    tte,
                    &[
                        ("lookback_s", lb + 30),
                        ("move_ticks", mv),
                        ("reference_tolerance_bps", 5),
                    ],
                );
            }
        }
        for thr in [300, 600] {
            add(
                BookImbalance,
                tte,
                &[
                    ("levels", 3),
                    ("threshold_permille", thr),
                    ("max_ask", 8000),
                ],
            );
        }
        for high in [0, 1] {
            for sig in [30, 60] {
                add(
                    VolRegimeModel,
                    tte,
                    &[
                        ("window", 90),
                        ("min_edge", 300),
                        ("high_regime", high),
                        ("sigma_threshold_ppm", sig),
                    ],
                );
            }
        }
        for lb in [30, 60] {
            for thr in [5, 10] {
                add(
                    LeadLag,
                    tte,
                    &[
                        ("lookback_s", lb),
                        ("divergence_bps", thr),
                        ("max_ask", 8000),
                    ],
                );
            }
        }
        for thr in [5, 15] {
            add(PerpBasis, tte, &[("threshold_bps", thr), ("max_ask", 8000)]);
        }
        for need in [2, 3] {
            add(Ensemble, tte, &[("need", need)]);
        }
        add(
            InvertedMomentum,
            tte,
            &[
                ("lookback_s", 120),
                ("threshold_bps", 15),
                ("max_ask", 7000),
            ],
        );
        for sp in [200, 400] {
            for d in [20, 50] {
                add(
                    LiquidityFilteredModel,
                    tte,
                    &[
                        ("window", 90),
                        ("min_edge", 300),
                        ("max_spread", sp),
                        ("min_depth", d),
                    ],
                );
            }
        }
        for beta in [500, 800] {
            for lb in [30, 60] {
                add(
                    LeadAdjustedModel,
                    tte,
                    &[
                        ("window", 90),
                        ("min_edge", 300),
                        ("lookback_s", lb),
                        ("beta_permille", beta),
                    ],
                );
            }
        }
    }
    g
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::synthetic::{generate, Scenario};
    use std::collections::BTreeSet;

    #[test]
    fn schema_rejects_unknown_missing_and_out_of_bounds() {
        assert!(matches!(
            StrategyConfig::new(Family::Longshot, 300, &[]),
            Err(ConfigError::MissingParam("max_ask"))
        ));
        assert!(matches!(
            StrategyConfig::new(Family::Longshot, 300, &[("max_ask", 6000)]),
            Err(ConfigError::OutOfBounds("max_ask"))
        ));
        assert!(matches!(
            StrategyConfig::new(Family::NoTrade, 300, &[("x", 1)]),
            Err(ConfigError::UnknownParam(_))
        ));
        assert!(matches!(
            StrategyConfig::new(Family::NoTrade, 0, &[]),
            Err(ConfigError::BadDecisionTime)
        ));
        assert!(matches!(
            StrategyConfig::new(
                Family::Favorite,
                300,
                &[("min_ask", 9000), ("max_ask", 8000)]
            ),
            Err(ConfigError::Inconsistent)
        ));
    }

    #[test]
    fn grid_ids_are_unique_and_parameter_sensitive() {
        let g = default_grid();
        let ids: BTreeSet<_> = g.iter().map(|c| c.config_sha256()).collect();
        assert_eq!(ids.len(), g.len());
        let families: BTreeSet<_> = g.iter().map(|c| c.family).collect();
        assert_eq!(families.len(), 15);
        let a = StrategyConfig::new(Family::Longshot, 300, &[("max_ask", 1500)]).unwrap();
        let b = StrategyConfig::new(Family::Longshot, 300, &[("max_ask", 1501)]).unwrap();
        assert_ne!(a.config_sha256(), b.config_sha256());
        assert_eq!(a.config_sha256(), a.clone().config_sha256());
    }

    #[test]
    fn decisions_ignore_everything_after_decision_time() {
        // Causality invariance: rewriting every snapshot after the decision
        // (prices, references, basis, books) must not change any decision.
        let cases = generate(&Scenario::lagged_quotes(2, 1, 11));
        let grid = default_grid();
        for c in cases.iter().take(16) {
            let mut tampered = c.clone();
            let t = c.expiration_ns - 300 * 1_000_000_000;
            for s in tampered.snapshots.iter_mut().filter(|s| s.received_ns > t) {
                s.reference_cents = s.reference_cents * 3 + 17;
                s.peer_reference_cents = s.peer_reference_cents.map(|p| p / 2);
                s.perp_basis_bps = Some(999);
                s.book.yes_bids.reverse();
            }
            for cfg in grid.iter().filter(|g| g.tte_s == 300) {
                let a = cfg.decide(&c.view_at(t).unwrap());
                let b = cfg.decide(&tampered.view_at(t).unwrap());
                assert_eq!(a, b, "{} looked ahead", cfg.id());
            }
        }
    }

    #[test]
    fn control_never_trades_and_model_probability_is_sane() {
        let c = generate(&Scenario::efficient(1, 1, 3)).remove(0);
        let v = c.view_at(c.expiration_ns - 120_000_000_000).unwrap();
        let ctl = StrategyConfig::new(Family::NoTrade, 120, &[]).unwrap();
        assert_eq!(ctl.decide(&v), Signal::Abstain(Abstain::Control));
        let sigma = realized_sigma(&v, 90).unwrap();
        assert!(sigma > 0.0 && sigma < 0.01);
        let p = model_prob_yes(&v, sigma).unwrap();
        assert!((0.0..=1.0).contains(&p));
    }
}

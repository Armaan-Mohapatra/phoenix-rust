//! Strategy evaluation: causal decision -> modeled taker execution at
//! arrival -> binary settlement -> incremental metrics. All P&L is MODELED.
//!
//! Units: money fields ending `_ticks` are 1e-4 USD (phoenix-domain units).
use crate::strategies::StrategyConfig;
use crate::{Abstain, MarketCase, Provenance, Signal};
use phoenix_domain::{
    settle_binary, BinaryFinal, Evidence, FeeRate, ModeledBuyFill, Money, Side, TimestampNs,
};
use phoenix_sim::{taker_buy, OrderIntent};
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};

pub const MAX_QUANTITY: u64 = 1_000_000;
pub const MAX_LATENCY_NS: i64 = 60_000_000_000;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ExecutionModel {
    /// Contracts requested per decision (paper sizing), 1..=1e6.
    pub quantity: u64,
    /// Decision-to-exchange-arrival latency, 0..=60 s.
    pub latency_ns: i64,
    /// Maximum age of the book used at decision and at arrival, 1 ns..=1 h.
    pub max_quote_age_ns: i64,
    /// Fee scaling relative to the standard 7% taker formula, 0..=10000 permille.
    pub fee_multiplier_permille: u64,
}
impl Default for ExecutionModel {
    fn default() -> Self {
        Self {
            quantity: 10,
            latency_ns: 250_000_000,
            max_quote_age_ns: 15_000_000_000,
            fee_multiplier_permille: 1000,
        }
    }
}
impl ExecutionModel {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=MAX_QUANTITY).contains(&self.quantity) {
            return Err(format!(
                "quantity {} outside 1..={MAX_QUANTITY}",
                self.quantity
            ));
        }
        if !(0..=MAX_LATENCY_NS).contains(&self.latency_ns) {
            return Err(format!(
                "latency_ns {} outside 0..={MAX_LATENCY_NS}",
                self.latency_ns
            ));
        }
        if !(1..=3_600_000_000_000).contains(&self.max_quote_age_ns) {
            return Err(format!(
                "max_quote_age_ns {} invalid",
                self.max_quote_age_ns
            ));
        }
        if self.fee_multiplier_permille > 10_000 {
            return Err(format!(
                "fee multiplier {} > 10000",
                self.fee_multiplier_permille
            ));
        }
        Ok(())
    }
    pub fn fee_rate(&self) -> FeeRate {
        FeeRate {
            numerator: FeeRate::STANDARD_TAKER.numerator * self.fee_multiplier_permille,
            denominator: FeeRate::STANDARD_TAKER.denominator * 1000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum NoFillReason {
    LimitNotReachable,
    ExecutionRejected,
    /// Decision + latency would reach the exchange at or after expiration.
    ArrivalAfterExpiration,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Result_ {
    Abstained(Abstain),
    NoFill(NoFillReason),
    Filled {
        #[serde(serialize_with = "ser_side")]
        side: Side,
        requested: u64,
        filled: u64,
        cost_ticks: i128,
        fee_ticks: i128,
        /// `None` while settlement is unknown; never imputed.
        net_ticks: Option<i128>,
    },
}

fn ser_side<S: serde::Serializer>(s: &Side, ser: S) -> Result<S::Ok, S::Error> {
    ser.serialize_str(match s {
        Side::Yes => "yes",
        Side::No => "no",
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct Outcome {
    pub market: String,
    pub cluster: String,
    pub expiration_ns: TimestampNs,
    pub result: Result_,
    pub fair_yes: Option<f64>,
    pub market_mid_yes: Option<f64>,
    pub yes_won: Option<bool>,
}

fn final_for(c: &MarketCase) -> Option<BinaryFinal> {
    Some(BinaryFinal {
        market: c.market.clone(),
        rules_sha256: c.rules_sha256.clone(),
        winner: c.winner?,
        venue_final: Evidence {
            source_id: match c.provenance {
                Provenance::Synthetic => "SYNTHETIC-GENERATOR".into(),
                Provenance::OperatorNormalized | Provenance::OperatorUnverified => {
                    "OPERATOR-SUPPLIED-FINAL".into()
                }
            },
            content_sha256: c.source_sha256.clone(),
        },
        independent: None,
    })
}

/// Evaluate one configuration on one case. Exactly one decision per case.
/// Never panics on externally supplied cases that passed `validate`.
pub fn evaluate_case(cfg: &StrategyConfig, c: &MarketCase, x: &ExecutionModel) -> Outcome {
    let yes_won = c.winner.map(|w| w == Side::Yes);
    let mk = |result, fair_yes, mid| Outcome {
        market: c.market.clone(),
        cluster: c.cluster.clone(),
        expiration_ns: c.expiration_ns,
        result,
        fair_yes,
        market_mid_yes: mid,
        yes_won,
    };
    let decision_ns = cfg
        .tte_s
        .checked_mul(1_000_000_000)
        .and_then(|d| c.expiration_ns.checked_sub(d));
    let Some(view) = decision_ns.and_then(|d| c.view_at(d)) else {
        return mk(Result_::Abstained(Abstain::NoView), None, None);
    };
    let decision_ns = view.decision_ns();
    let mid = view.yes_mid_ticks().map(|m| f64::from(m) / 10_000.0);
    if decision_ns.saturating_sub(view.latest().received_ns) > x.max_quote_age_ns {
        return mk(Result_::Abstained(Abstain::NoQuote), None, mid);
    }
    let (side, limit, fair_yes) = match cfg.decide(&view) {
        Signal::Abstain(a) => return mk(Result_::Abstained(a), None, mid),
        Signal::Buy {
            side,
            limit,
            fair_yes,
        } => (side, limit, fair_yes.filter(|p| p.is_finite())),
    };
    let Some(arrival) = decision_ns.checked_add(x.latency_ns) else {
        return mk(
            Result_::NoFill(NoFillReason::ExecutionRejected),
            fair_yes,
            mid,
        );
    };
    if arrival >= c.expiration_ns {
        return mk(
            Result_::NoFill(NoFillReason::ArrivalAfterExpiration),
            fair_yes,
            mid,
        );
    }
    let idx = c.snapshots.partition_point(|s| s.received_ns <= arrival);
    let Some(book) = idx.checked_sub(1).and_then(|i| c.snapshots.get(i)) else {
        return mk(
            Result_::NoFill(NoFillReason::ExecutionRejected),
            fair_yes,
            mid,
        );
    };
    let intent = OrderIntent {
        market: c.market.clone(),
        side,
        quantity: x.quantity,
        buy_limit: limit,
        signal_received_ns: view.latest().received_ns,
        decision_ns,
        arrival_ns: arrival,
        max_age_ns: x.max_quote_age_ns,
        fee_rate: x.fee_rate(),
    };
    let fill = match taker_buy(&intent, std::slice::from_ref(&book.book)) {
        Ok(f) => f,
        Err(_) => {
            return mk(
                Result_::NoFill(NoFillReason::ExecutionRejected),
                fair_yes,
                mid,
            )
        }
    };
    let filled = fill.requested - fill.unfilled;
    if filled == 0 {
        return mk(
            Result_::NoFill(NoFillReason::LimitNotReachable),
            fair_yes,
            mid,
        );
    }
    let net = final_for(c).and_then(|fin| {
        let mut total: Money = 0;
        for lf in &fill.fills {
            let m = ModeledBuyFill {
                market: c.market.clone(),
                rules_sha256: c.rules_sha256.clone(),
                side,
                price: lf.price,
                quantity: lf.quantity,
                entry_fee: lf.fee,
            };
            total = total.checked_add(settle_binary(&m, &fin).ok()?.net)?;
        }
        Some(total)
    });
    mk(
        Result_::Filled {
            side,
            requested: fill.requested,
            filled,
            cost_ticks: fill.cost,
            fee_ticks: fill.fees,
            net_ticks: net,
        },
        fair_yes,
        mid,
    )
}

pub fn evaluate(cfg: &StrategyConfig, cases: &[&MarketCase], x: &ExecutionModel) -> Vec<Outcome> {
    cases.iter().map(|c| evaluate_case(cfg, c, x)).collect()
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Metrics {
    pub markets: u64,
    pub abstentions: BTreeMap<String, u64>,
    pub no_fill_reasons: BTreeMap<String, u64>,
    pub orders: u64,
    pub full_fills: u64,
    pub partial_fills: u64,
    pub no_fills: u64,
    pub pending_settlement: u64,
    pub contracts_filled: u64,
    pub cost_ticks: i128,
    pub fee_ticks: i128,
    /// Payout minus cost, before fees, on settled fills (1e-4 USD).
    pub gross_ticks: i128,
    /// After fees, on settled fills (1e-4 USD).
    pub net_ticks: i128,
    pub settled_trades: u64,
    pub wins: u64,
    pub losses: u64,
    /// Fraction of settled trades with positive net.
    pub win_rate: Option<f64>,
    /// net / markets evaluated (1e-4 USD per opportunity).
    pub ev_per_market_ticks: Option<f64>,
    /// net / settled trades (1e-4 USD per trade).
    pub ev_per_trade_ticks: Option<f64>,
    /// net / (cost + fees) of settled trades.
    pub return_on_capital: Option<f64>,
    pub profit_factor: Option<f64>,
    /// Largest peak-to-trough decline of cumulative net, chronological.
    pub max_drawdown_ticks: i128,
    /// Worst single settled trade (1e-4 USD).
    pub worst_trade_ticks: Option<i128>,
    /// Brier of the strategy's own P(YES), on rows where it formed one.
    pub brier_model: Option<f64>,
    /// Brier of the market mid on the same rows (baseline).
    pub brier_market_same_rows: Option<f64>,
    pub forecast_rows: u64,
    /// Independent clusters seen (each contributes net 0 if no trade).
    pub clusters: u64,
    /// Set if any integer accumulator overflowed; `test` is then `None`.
    pub arithmetic_overflow: bool,
    /// Set if rows of one cluster were not contiguous; `test` is `None`.
    pub cluster_order_violation: bool,
    /// One-sided independent-cluster test of mean net per cluster > 0.
    pub test: Option<ClusterTest>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub struct ClusterTest {
    pub clusters: u64,
    pub mean_ticks: f64,
    pub sd_ticks: f64,
    pub z: f64,
}

/// Exact-integer one-sample z over cluster sums: n, sum, sum of squares.
/// Returns `None` for n < 20, zero variance, overflow, or non-finite z
/// (a degenerate sample is not evidence).
pub fn cluster_test_from(n: u64, s: i128, q: i128) -> Option<ClusterTest> {
    if n < 20 {
        return None;
    }
    let ni = i128::from(n);
    let num = ni.checked_mul(q)?.checked_sub(s.checked_mul(s)?)?; // n*Q - S^2 >= 0
    if num <= 0 {
        return None;
    }
    let nf = n as f64;
    let var = num as f64 / (nf * (nf - 1.0));
    let mean = s as f64 / nf;
    let sd = var.sqrt();
    let z = mean / (sd / nf.sqrt());
    (z.is_finite() && sd.is_finite()).then_some(ClusterTest {
        clusters: n,
        mean_ticks: mean,
        sd_ticks: sd,
        z,
    })
}

pub fn cluster_test(per_cluster: &[i128]) -> Option<ClusterTest> {
    let mut s = 0i128;
    let mut q = 0i128;
    for v in per_cluster {
        s = s.checked_add(*v)?;
        q = q.checked_add(v.checked_mul(*v)?)?;
    }
    cluster_test_from(per_cluster.len() as u64, s, q)
}

/// Incremental metrics. Push outcomes in (expiration, market) order with
/// each cluster's rows contiguous; memory is O(1) in the number of rows.
#[derive(Debug, Clone, Default)]
pub struct Acc {
    m: Metrics,
    cur: Option<String>,
    cur_sum: i128,
    n: u64,
    s: i128,
    q: i128,
    cum: i128,
    peak: i128,
    won: i128,
    lost: i128,
    bm: f64,
    bk: f64,
}

fn add(a: &mut i128, v: i128, flag: &mut bool) {
    match a.checked_add(v) {
        Some(x) => *a = x,
        None => *flag = true,
    }
}

impl Acc {
    fn close(&mut self) {
        if self.cur.take().is_some() {
            let v = std::mem::take(&mut self.cur_sum);
            self.n += 1;
            let mut of = self.m.arithmetic_overflow;
            add(&mut self.s, v, &mut of);
            match v.checked_mul(v) {
                Some(sq) => add(&mut self.q, sq, &mut of),
                None => of = true,
            }
            self.m.arithmetic_overflow = of;
        }
    }
    pub fn push(&mut self, o: &Outcome) {
        if self.cur.as_deref() != Some(o.cluster.as_str()) {
            self.close();
            self.cur = Some(o.cluster.clone());
        }
        let m = &mut self.m;
        m.markets += 1;
        if let (Some(p), Some(y), Some(mid)) = (o.fair_yes, o.yes_won, o.market_mid_yes) {
            let t = if y { 1.0 } else { 0.0 };
            self.bm += (p - t).powi(2);
            self.bk += (mid - t).powi(2);
            m.forecast_rows += 1;
        }
        let mut of = m.arithmetic_overflow;
        match &o.result {
            Result_::Abstained(a) => *m.abstentions.entry(format!("{a:?}")).or_default() += 1,
            Result_::NoFill(r) => {
                m.orders += 1;
                m.no_fills += 1;
                *m.no_fill_reasons.entry(format!("{r:?}")).or_default() += 1;
            }
            Result_::Filled {
                requested,
                filled,
                cost_ticks,
                fee_ticks,
                net_ticks,
                ..
            } => {
                m.orders += 1;
                if filled == requested {
                    m.full_fills += 1
                } else {
                    m.partial_fills += 1
                }
                m.contracts_filled = m.contracts_filled.saturating_add(*filled);
                if let Some(net) = *net_ticks {
                    add(&mut m.cost_ticks, *cost_ticks, &mut of);
                    add(&mut m.fee_ticks, *fee_ticks, &mut of);
                    add(&mut m.net_ticks, net, &mut of);
                    add(&mut m.gross_ticks, net.saturating_add(*fee_ticks), &mut of);
                    m.settled_trades += 1;
                    m.worst_trade_ticks = Some(m.worst_trade_ticks.map_or(net, |w| w.min(net)));
                    if net > 0 {
                        m.wins += 1;
                        add(&mut self.won, net, &mut of);
                    } else {
                        m.losses += 1;
                        add(&mut self.lost, net.saturating_neg(), &mut of);
                    }
                    add(&mut self.cur_sum, net, &mut of);
                    add(&mut self.cum, net, &mut of);
                    self.peak = self.peak.max(self.cum);
                    m.max_drawdown_ticks =
                        m.max_drawdown_ticks.max(self.peak.saturating_sub(self.cum));
                } else {
                    m.pending_settlement += 1;
                }
            }
        }
        m.arithmetic_overflow = of;
    }
    pub fn finish(mut self) -> Metrics {
        self.close();
        let mut m = self.m;
        m.clusters = self.n;
        let st = m.settled_trades as f64;
        if m.settled_trades > 0 {
            m.win_rate = Some(m.wins as f64 / st);
            m.ev_per_trade_ticks = Some(m.net_ticks as f64 / st);
        }
        if m.markets > 0 {
            m.ev_per_market_ticks = Some(m.net_ticks as f64 / m.markets as f64);
        }
        let cap = m.cost_ticks.saturating_add(m.fee_ticks);
        if cap > 0 {
            m.return_on_capital = Some(m.net_ticks as f64 / cap as f64);
        }
        if self.lost > 0 {
            m.profit_factor = Some(self.won as f64 / self.lost as f64);
        }
        if m.forecast_rows > 0 {
            m.brier_model = Some(self.bm / m.forecast_rows as f64);
            m.brier_market_same_rows = Some(self.bk / m.forecast_rows as f64);
        }
        m.test = if m.arithmetic_overflow || m.cluster_order_violation {
            None
        } else {
            cluster_test_from(self.n, self.s, self.q)
        };
        m
    }
}

/// Convenience for arbitrary row sets: sorts, checks contiguity, folds.
pub fn summarize(rows: &[Outcome]) -> Metrics {
    let mut ordered: Vec<&Outcome> = rows.iter().collect();
    ordered.sort_by(|a, b| (a.expiration_ns, &a.market).cmp(&(b.expiration_ns, &b.market)));
    let mut closed: HashSet<&str> = HashSet::new();
    let mut cur: Option<&str> = None;
    let mut violation = false;
    for o in &ordered {
        if cur != Some(o.cluster.as_str()) {
            if let Some(c) = cur {
                closed.insert(c);
            }
            violation |= closed.contains(o.cluster.as_str());
            cur = Some(&o.cluster);
        }
    }
    let mut acc = Acc::default();
    ordered.iter().for_each(|o| acc.push(o));
    let mut m = acc.finish();
    if violation {
        m.cluster_order_violation = true;
        m.test = None;
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::math::{z_upper, Rng};
    use crate::strategies::{Family, StrategyConfig};
    use crate::synthetic::{generate, Scenario};

    #[test]
    fn no_trade_control_has_exactly_zero_pnl() {
        let cases = generate(&Scenario::efficient(2, 1, 4));
        let refs: Vec<&MarketCase> = cases.iter().collect();
        let cfg = StrategyConfig::new(Family::NoTrade, 300, &[]).unwrap();
        let m = summarize(&evaluate(&cfg, &refs, &ExecutionModel::default()));
        assert_eq!((m.orders, m.net_ticks, m.fee_ticks), (0, 0, 0));
        assert_eq!(m.abstentions["Control"], refs.len() as u64);
        assert_eq!(m.clusters, refs.len() as u64 / 2);
    }

    #[test]
    fn accounting_conserves_cash_per_settled_trade() {
        let cases = generate(&Scenario::efficient(1, 1, 8));
        let cfg =
            StrategyConfig::new(Family::CoinFlip, 300, &[("seed", 3), ("max_ask", 9900)]).unwrap();
        let x = ExecutionModel::default();
        let mut trades = 0;
        for c in &cases {
            let o = evaluate_case(&cfg, c, &x);
            if let Result_::Filled {
                side,
                filled,
                cost_ticks,
                fee_ticks,
                net_ticks,
                ..
            } = o.result
            {
                trades += 1;
                let payout = if Some(side) == c.winner {
                    10_000 * filled as i128
                } else {
                    0
                };
                assert_eq!(net_ticks, Some(payout - cost_ticks - fee_ticks));
                assert!(fee_ticks > 0 && cost_ticks > 0);
                assert!(filled <= x.quantity);
            }
        }
        assert!(trades > 50);
    }

    #[test]
    fn unknown_settlement_is_pending_not_imputed() {
        let mut cases = generate(&Scenario::efficient(1, 1, 8));
        for c in &mut cases {
            c.winner = None;
        }
        let refs: Vec<&MarketCase> = cases.iter().collect();
        let cfg =
            StrategyConfig::new(Family::CoinFlip, 300, &[("seed", 3), ("max_ask", 9900)]).unwrap();
        let m = summarize(&evaluate(&cfg, &refs, &ExecutionModel::default()));
        assert!(m.pending_settlement > 50);
        assert_eq!((m.settled_trades, m.net_ticks), (0, 0));
        assert!(m.test.is_none(), "all-zero clusters have zero variance");
    }

    #[test]
    fn higher_fees_never_improve_a_fixed_decision_set() {
        let cases = generate(&Scenario::efficient(1, 2, 9));
        let refs: Vec<&MarketCase> = cases.iter().collect();
        let cfg = StrategyConfig::new(
            Family::Favorite,
            300,
            &[("min_ask", 6500), ("max_ask", 9500)],
        )
        .unwrap();
        let base = summarize(&evaluate(&cfg, &refs, &ExecutionModel::default()));
        let pricey = summarize(&evaluate(
            &cfg,
            &refs,
            &ExecutionModel {
                fee_multiplier_permille: 2000,
                ..Default::default()
            },
        ));
        assert!(pricey.fee_ticks > base.fee_ticks);
        assert!(pricey.net_ticks < base.net_ticks);
    }

    #[test]
    fn stale_books_and_late_arrivals_are_refused() {
        let cases = generate(&Scenario::efficient(1, 1, 2));
        let cfg =
            StrategyConfig::new(Family::CoinFlip, 300, &[("seed", 1), ("max_ask", 9900)]).unwrap();
        let x = ExecutionModel {
            max_quote_age_ns: 1,
            ..Default::default()
        };
        let o = evaluate_case(&cfg, &cases[0], &x);
        assert_eq!(o.result, Result_::NoFill(NoFillReason::ExecutionRejected));
        let off =
            StrategyConfig::new(Family::CoinFlip, 295, &[("seed", 1), ("max_ask", 9900)]).unwrap();
        assert_eq!(
            evaluate_case(&off, &cases[0], &x).result,
            Result_::Abstained(Abstain::NoQuote)
        );
        // 1 s before expiry with 2 s latency: the order would arrive late.
        let late =
            StrategyConfig::new(Family::CoinFlip, 10, &[("seed", 1), ("max_ask", 9900)]).unwrap();
        let slow = ExecutionModel {
            latency_ns: 20_000_000_000,
            ..Default::default()
        };
        let quoted = cases
            .iter()
            .find(|c| {
                matches!(
                    evaluate_case(&late, c, &ExecutionModel::default()).result,
                    Result_::Filled { .. }
                )
            })
            .expect("some market is quoted 10 s before expiry");
        assert_eq!(
            evaluate_case(&late, quoted, &slow).result,
            Result_::NoFill(NoFillReason::ArrivalAfterExpiration)
        );
    }

    #[test]
    fn invalid_execution_models_are_rejected() {
        for bad in [
            ExecutionModel {
                quantity: 0,
                ..Default::default()
            },
            ExecutionModel {
                quantity: MAX_QUANTITY + 1,
                ..Default::default()
            },
            ExecutionModel {
                latency_ns: -1,
                ..Default::default()
            },
            ExecutionModel {
                latency_ns: i64::MAX,
                ..Default::default()
            },
            ExecutionModel {
                max_quote_age_ns: 0,
                ..Default::default()
            },
            ExecutionModel {
                fee_multiplier_permille: 10_001,
                ..Default::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
        assert!(ExecutionModel::default().validate().is_ok());
    }

    #[test]
    fn cluster_test_degenerate_inputs_are_none_not_nan() {
        assert!(cluster_test(&[5; 30]).is_none(), "zero variance");
        assert!(cluster_test(&[1; 19]).is_none(), "too few clusters");
        assert!(cluster_test(&[i128::MAX, 1]).is_none());
        assert!(cluster_test_from(30, i128::MAX, i128::MAX).is_none());
        let t = cluster_test(&(0..40).map(|i| i % 7 - 2).collect::<Vec<_>>()).unwrap();
        assert!(t.z.is_finite());
    }

    #[test]
    fn cluster_z_test_has_nominal_false_positive_rate_and_power() {
        // Null: mean-zero cluster sums. One-sided alpha = 0.05.
        let crit = z_upper(0.05);
        let mut rng = Rng::new(77);
        let (trials, n) = (4000, 120);
        let mut rejections = 0;
        for _ in 0..trials {
            let v: Vec<i128> = (0..n)
                .map(|_| (rng.normal() * 500.0).round() as i128)
                .collect();
            if cluster_test(&v).is_some_and(|t| t.z >= crit) {
                rejections += 1;
            }
        }
        let fpr = rejections as f64 / trials as f64;
        assert!((0.035..=0.065).contains(&fpr), "empirical FPR {fpr}");
        // Planted effect of 0.3 sd: power should be high at n = 120.
        let mut hits = 0;
        for _ in 0..500 {
            let v: Vec<i128> = (0..n)
                .map(|_| (150.0 + rng.normal() * 500.0).round() as i128)
                .collect();
            if cluster_test(&v).is_some_and(|t| t.z >= crit) {
                hits += 1;
            }
        }
        assert!(hits > 400, "power {hits}/500");
    }

    #[test]
    fn non_contiguous_clusters_disable_the_test() {
        let cases = generate(&Scenario::efficient(1, 1, 8));
        let cfg =
            StrategyConfig::new(Family::CoinFlip, 300, &[("seed", 3), ("max_ask", 9900)]).unwrap();
        let mut rows: Vec<Outcome> = cases
            .iter()
            .map(|c| evaluate_case(&cfg, c, &ExecutionModel::default()))
            .collect();
        assert!(!summarize(&rows).cluster_order_violation);
        rows[10].cluster = rows[0].cluster.clone();
        let m = summarize(&rows);
        assert!(m.cluster_order_violation && m.test.is_none());
    }
}

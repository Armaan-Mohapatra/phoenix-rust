//! Strategy evaluation: causal decision -> modeled taker execution at
//! arrival -> binary settlement -> metrics. All P&L is MODELED.
//!
//! Units: money fields ending `_ticks` are 1e-4 USD (phoenix-domain units).
use crate::strategies::StrategyConfig;
use crate::{Abstain, MarketCase, Provenance, Signal};
use phoenix_domain::{
    settle_binary, BinaryFinal, Evidence, FeeRate, ModeledBuyFill, Money, Side, TimestampNs,
};
use phoenix_sim::{taker_buy, OrderIntent};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ExecutionModel {
    /// Contracts requested per decision (paper sizing).
    pub quantity: u64,
    /// Decision-to-exchange-arrival latency.
    pub latency_ns: i64,
    /// Maximum age of the book used at decision and at arrival.
    pub max_quote_age_ns: i64,
    /// Fee scaling relative to the standard 7% taker formula, permille.
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
                Provenance::OperatorNormalized => "OPERATOR-NORMALIZED-FINAL".into(),
            },
            content_sha256: c.source_sha256.clone(),
        },
        independent: None,
    })
}

/// Evaluate one configuration on one case. Exactly one decision per case.
pub fn evaluate_case(cfg: &StrategyConfig, c: &MarketCase, x: &ExecutionModel) -> Outcome {
    let decision_ns = c.expiration_ns - cfg.tte_s * 1_000_000_000;
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
    let Some(view) = c.view_at(decision_ns) else {
        return mk(Result_::Abstained(Abstain::NoView), None, None);
    };
    let mid = view.yes_mid_ticks().map(|m| f64::from(m) / 10_000.0);
    if decision_ns - view.latest().received_ns > x.max_quote_age_ns {
        return mk(Result_::Abstained(Abstain::NoQuote), None, mid);
    }
    let (side, limit, fair_yes) = match cfg.decide(&view) {
        Signal::Abstain(a) => return mk(Result_::Abstained(a), None, mid),
        Signal::Buy {
            side,
            limit,
            fair_yes,
        } => (side, limit, fair_yes),
    };
    let arrival = decision_ns + x.latency_ns;
    let idx = c.snapshots.partition_point(|s| s.received_ns <= arrival);
    let books = [c.snapshots[idx - 1].book.clone()];
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
    let fill = match taker_buy(&intent, &books) {
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
    /// Independent-cluster test of mean net per cluster > 0.
    pub test: Option<ClusterTest>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ClusterTest {
    pub clusters: u64,
    pub mean_ticks: f64,
    pub sd_ticks: f64,
    pub z: f64,
}

/// One-sample z statistic over independent cluster sums (clusters with no
/// trade contribute 0, so the test is per opportunity, not per trade).
pub fn cluster_test(per_cluster: &[i128]) -> Option<ClusterTest> {
    let n = per_cluster.len();
    if n < 20 {
        return None;
    }
    let nf = n as f64;
    let mean = per_cluster.iter().map(|v| *v as f64).sum::<f64>() / nf;
    let var = per_cluster
        .iter()
        .map(|v| (*v as f64 - mean).powi(2))
        .sum::<f64>()
        / (nf - 1.0);
    let sd = var.sqrt();
    let z = if sd > 0.0 {
        mean / (sd / nf.sqrt())
    } else if mean > 0.0 {
        f64::INFINITY
    } else {
        0.0
    };
    Some(ClusterTest {
        clusters: n as u64,
        mean_ticks: mean,
        sd_ticks: sd,
        z,
    })
}

pub fn summarize(rows: &[Outcome]) -> Metrics {
    let mut m = Metrics {
        markets: rows.len() as u64,
        ..Default::default()
    };
    let mut ordered: Vec<&Outcome> = rows.iter().collect();
    ordered.sort_by(|a, b| (a.expiration_ns, &a.market).cmp(&(b.expiration_ns, &b.market)));
    let mut clusters: BTreeMap<&str, i128> = BTreeMap::new();
    let (mut cum, mut peak) = (0i128, 0i128);
    let (mut won, mut lost) = (0i128, 0i128);
    let (mut bm, mut bk) = (0.0f64, 0.0f64);
    for o in &ordered {
        clusters.entry(o.cluster.as_str()).or_insert(0);
        if let (Some(p), Some(y)) = (o.fair_yes, o.yes_won) {
            if let Some(mid) = o.market_mid_yes {
                let t = if y { 1.0 } else { 0.0 };
                bm += (p - t).powi(2);
                bk += (mid - t).powi(2);
                m.forecast_rows += 1;
            }
        }
        match &o.result {
            Result_::Abstained(a) => *m.abstentions.entry(format!("{a:?}")).or_default() += 1,
            Result_::NoFill(_) => {
                m.orders += 1;
                m.no_fills += 1;
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
                m.contracts_filled += filled;
                let Some(net) = net_ticks else {
                    m.pending_settlement += 1;
                    continue;
                };
                m.cost_ticks += cost_ticks;
                m.fee_ticks += fee_ticks;
                m.net_ticks += net;
                m.gross_ticks += net + fee_ticks;
                m.settled_trades += 1;
                m.worst_trade_ticks = Some(m.worst_trade_ticks.map_or(*net, |w| w.min(*net)));
                if *net > 0 {
                    m.wins += 1;
                    won += net;
                } else {
                    m.losses += 1;
                    lost -= net;
                }
                *clusters.get_mut(o.cluster.as_str()).expect("inserted") += net;
                cum += net;
                peak = peak.max(cum);
                m.max_drawdown_ticks = m.max_drawdown_ticks.max(peak - cum);
            }
        }
    }
    let st = m.settled_trades as f64;
    if m.settled_trades > 0 {
        m.win_rate = Some(m.wins as f64 / st);
        m.ev_per_trade_ticks = Some(m.net_ticks as f64 / st);
    }
    if m.markets > 0 {
        m.ev_per_market_ticks = Some(m.net_ticks as f64 / m.markets as f64);
    }
    let cap = m.cost_ticks + m.fee_ticks;
    if cap > 0 {
        m.return_on_capital = Some(m.net_ticks as f64 / cap as f64);
    }
    if lost > 0 {
        m.profit_factor = Some(won as f64 / lost as f64);
    }
    if m.forecast_rows > 0 {
        m.brier_model = Some(bm / m.forecast_rows as f64);
        m.brier_market_same_rows = Some(bk / m.forecast_rows as f64);
    }
    let sums: Vec<i128> = clusters.values().copied().collect();
    m.test = cluster_test(&sums);
    m
}

#[cfg(test)]
mod tests {
    use super::*;
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
    }

    #[test]
    fn accounting_conserves_cash_per_settled_trade() {
        // net == payout - cost - fee for every fill, payout in {0, 1$*qty}.
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
    }

    #[test]
    fn higher_fees_and_latency_never_improve_a_fixed_decision_set() {
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
    fn stale_books_at_arrival_are_rejected() {
        let cases = generate(&Scenario::efficient(1, 1, 2));
        let cfg =
            StrategyConfig::new(Family::CoinFlip, 300, &[("seed", 1), ("max_ask", 9900)]).unwrap();
        let x = ExecutionModel {
            max_quote_age_ns: 1,
            ..Default::default()
        };
        // The decision-time book is fresh (aligned 10s grid) but 250ms of
        // arrival latency makes it older than 1ns, so execution must refuse.
        let o = evaluate_case(&cfg, &cases[0], &x);
        assert_eq!(o.result, Result_::NoFill(NoFillReason::ExecutionRejected));
        // A decision between snapshots sees a stale book and abstains.
        let off =
            StrategyConfig::new(Family::CoinFlip, 295, &[("seed", 1), ("max_ask", 9900)]).unwrap();
        let o = evaluate_case(&off, &cases[0], &x);
        assert_eq!(o.result, Result_::Abstained(Abstain::NoQuote));
    }
}

//! Phoenix research campaigns (paper-only, offline).
//!
//! Pipeline: normalized market cases -> causal as-of views -> registered
//! strategy configurations -> modeled taker execution (`phoenix-sim`) ->
//! binary settlement (`phoenix-domain`) -> per-split metrics -> chronological
//! discovery / validation / single-use holdout gate -> JSON report.
//!
//! Hard boundaries:
//! * No network, no exchange client, no order submission. Every fill is a
//!   MODELED hypothesis against recorded or synthetic books.
//! * A strategy only ever receives a [`CausalView`], which contains snapshots
//!   whose `received_ns <= decision_ns`. Future data is not reachable.
//! * Money is integer 1e-4 USD ("ticks", as in `phoenix-domain`). Floating
//!   point is used only for model probabilities and test statistics, through
//!   the deterministic routines in [`math`].
pub mod adapter;
pub mod campaign;
pub mod engine;
pub mod fingerprint;
pub mod input;
pub mod math;
pub mod paths;
pub mod registry;
pub mod source;
pub mod strategies;
pub mod synthetic;

use phoenix_domain::{Book, Price, Side, TimestampNs};
use serde::{Deserialize, Serialize};

/// One recorded observation inside a market's life.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Local arrival time of this observation. The only clock strategies use.
    pub received_ns: TimestampNs,
    pub book: Book,
    /// Underlying reference (spot index proxy) in 1e-2 USD, as observed at
    /// `received_ns`.
    pub reference_cents: i64,
    /// A related asset's reference (cross-asset hypotheses), as observed at
    /// `received_ns`. `None` when not recorded.
    pub peer_reference_cents: Option<i64>,
    /// Perpetual-futures basis (perp mark minus spot) in basis points.
    pub perp_basis_bps: Option<i32>,
}

/// Where a case came from. Synthetic results are never evidence of real edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Provenance {
    Synthetic,
    OperatorNormalized,
    /// Operator data whose books were NOT certified executable by the source
    /// verifier (see `adapter`). Usable for exploratory research only.
    OperatorUnverified,
}

/// One short-duration binary market from open to expiration.
#[derive(Debug, Clone)]
pub struct MarketCase {
    pub market: String,
    pub event: String,
    /// Statistical dependence unit. Markets in the same time window (e.g.
    /// BTC and ETH 15-minute contracts expiring together) share a cluster.
    pub cluster: String,
    pub asset: String,
    pub open_ns: TimestampNs,
    pub expiration_ns: TimestampNs,
    /// YES pays if the reference at expiration is >= strike.
    pub strike_cents: i64,
    pub rules_sha256: String,
    pub source_sha256: String,
    pub provenance: Provenance,
    /// Sorted strictly by `received_ns`.
    pub snapshots: Vec<Snapshot>,
    /// Final winner, `None` when settlement is unknown (never guessed).
    pub winner: Option<Side>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaseError {
    Empty,
    Unordered,
    BadWindow,
    BookInvalid,
    MarketMismatch,
    BadIdentity,
}

impl MarketCase {
    pub fn validate(&self) -> Result<(), CaseError> {
        let hex = |s: &str| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit());
        if self.market.is_empty()
            || self.event.is_empty()
            || self.cluster.is_empty()
            || !hex(&self.rules_sha256)
            || !hex(&self.source_sha256)
        {
            return Err(CaseError::BadIdentity);
        }
        if self.open_ns >= self.expiration_ns {
            return Err(CaseError::BadWindow);
        }
        if self.snapshots.is_empty() {
            return Err(CaseError::Empty);
        }
        if self
            .snapshots
            .windows(2)
            .any(|w| w[0].received_ns >= w[1].received_ns)
        {
            return Err(CaseError::Unordered);
        }
        for s in &self.snapshots {
            if s.book.market != self.market {
                return Err(CaseError::MarketMismatch);
            }
            if s.book.validate().is_err() || s.book.received_ns != s.received_ns {
                return Err(CaseError::BookInvalid);
            }
            if s.received_ns < self.open_ns || s.received_ns > self.expiration_ns {
                return Err(CaseError::BadWindow);
            }
        }
        Ok(())
    }

    /// The only constructor of strategy inputs. Truncates history at the
    /// decision time; returns `None` when nothing had arrived yet.
    pub fn view_at(&self, decision_ns: TimestampNs) -> Option<CausalView<'_>> {
        let n = self
            .snapshots
            .partition_point(|s| s.received_ns <= decision_ns);
        if n == 0 || decision_ns >= self.expiration_ns {
            return None;
        }
        Some(CausalView {
            market: &self.market,
            event: &self.event,
            asset: &self.asset,
            open_ns: self.open_ns,
            expiration_ns: self.expiration_ns,
            strike_cents: self.strike_cents,
            decision_ns,
            history: &self.snapshots[..n],
        })
    }
}

/// Everything a strategy may know at `decision_ns`. Fields are private
/// outside this crate's accessors so no caller can widen the slice.
#[derive(Debug, Clone, Copy)]
pub struct CausalView<'a> {
    market: &'a str,
    event: &'a str,
    asset: &'a str,
    open_ns: TimestampNs,
    expiration_ns: TimestampNs,
    strike_cents: i64,
    decision_ns: TimestampNs,
    history: &'a [Snapshot],
}

impl<'a> CausalView<'a> {
    pub fn market(&self) -> &'a str {
        self.market
    }
    pub fn event(&self) -> &'a str {
        self.event
    }
    pub fn asset(&self) -> &'a str {
        self.asset
    }
    pub fn open_ns(&self) -> TimestampNs {
        self.open_ns
    }
    pub fn strike_cents(&self) -> i64 {
        self.strike_cents
    }
    pub fn decision_ns(&self) -> TimestampNs {
        self.decision_ns
    }
    pub fn seconds_to_expiry(&self) -> f64 {
        (self.expiration_ns - self.decision_ns) as f64 / 1e9
    }
    /// Snapshots with `received_ns <= decision_ns`, oldest first.
    pub fn history(&self) -> &'a [Snapshot] {
        self.history
    }
    pub fn latest(&self) -> &'a Snapshot {
        self.history.last().expect("view is never empty")
    }
    /// Latest snapshot received at or before `decision_ns - lookback_ns`.
    pub fn at_or_before(&self, lookback_ns: i64) -> Option<&'a Snapshot> {
        let t = self.decision_ns.checked_sub(lookback_ns)?;
        let n = self.history.partition_point(|s| s.received_ns <= t);
        n.checked_sub(1).map(|i| &self.history[i])
    }
    pub fn best_ask(&self, side: Side) -> Option<Price> {
        self.latest().book.best_ask(side)
    }
    /// Mid of the YES market in ticks, when both sides are quoted.
    pub fn yes_mid_ticks(&self) -> Option<u32> {
        let b = &self.latest().book;
        let bid = b.best_bid(Side::Yes)?.ticks() as u32;
        let ask = b.best_ask(Side::Yes)?.ticks() as u32;
        Some((bid + ask) / 2)
    }
}

/// Why a strategy declined to trade. Kept for every decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Abstain {
    Control,
    NoView,
    NoQuote,
    InsufficientHistory,
    MissingFeature,
    NoSignal,
    EdgeBelowThreshold,
    PriceOutOfBand,
    SpreadTooWide,
    DepthTooThin,
    RegimeMismatch,
    NoConsensus,
}

/// A strategy's decision: buy one side up to a limit, or abstain.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Signal {
    Abstain(Abstain),
    Buy {
        side: Side,
        limit: Price,
        /// The strategy's probability that YES wins, when it forms one.
        /// Used only for forecast scoring (Brier) against the outcome.
        fair_yes: Option<f64>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::synthetic::{generate, Scenario};

    #[test]
    fn view_never_contains_future_snapshots() {
        let cases = generate(&Scenario::efficient(3, 1, 7));
        for c in &cases {
            c.validate().unwrap();
            for offset_s in [890i64, 600, 300, 60, 1] {
                let t = c.expiration_ns - offset_s * 1_000_000_000;
                let v = c.view_at(t).unwrap();
                assert!(v.history().iter().all(|s| s.received_ns <= t));
                let next = c.snapshots.get(v.history().len());
                assert!(next.map_or(true, |s| s.received_ns > t));
            }
            assert!(c.view_at(c.expiration_ns).is_none());
            assert!(c.view_at(c.open_ns - 1).is_none());
        }
    }

    #[test]
    fn invalid_cases_are_rejected() {
        let mut c = generate(&Scenario::efficient(1, 1, 1)).remove(0);
        c.validate().unwrap();
        let mut unordered = c.clone();
        unordered.snapshots.swap(0, 1);
        assert_eq!(unordered.validate(), Err(CaseError::Unordered));
        c.rules_sha256 = "x".into();
        assert_eq!(c.validate(), Err(CaseError::BadIdentity));
    }
}

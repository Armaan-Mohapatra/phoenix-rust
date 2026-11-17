//! Offline-only Kalshi-style binary market domain primitives.
//! All prices use 1/10_000 of one US dollar; P&L uses 1/10_000 USD.
//! No floating point, network clients, orders, or disk access.

use std::collections::HashMap;

pub const ONE_DOLLAR: u16 = 10_000;
pub type Money = i128;
pub type TimestampNs = i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Yes,
    No,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidPrice,
    InvalidLevel,
    InvalidBook,
    InvalidFee,
    ArithmeticOverflow,
    FutureData,
    StaleData,
    MissingEvidence,
    EvidenceNotIndependent,
    MarketMismatch,
    RuleMismatch,
    ZeroQuantity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Price(u16);
impl Price {
    pub fn new(value: u16) -> Result<Self, Error> {
        if value > ONE_DOLLAR {
            return Err(Error::InvalidPrice);
        }
        Ok(Self(value))
    }
    pub fn ticks(self) -> u16 {
        self.0
    }
    pub fn complement(self) -> Self {
        Self(ONE_DOLLAR - self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: Price,
    pub quantity: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Book {
    pub market: String,
    pub yes_bids: Vec<Level>,
    pub no_bids: Vec<Level>,
    pub exchange_ns: TimestampNs,
    pub received_ns: TimestampNs,
    pub session_id: String,
    pub sequence: u64,
}
impl Book {
    pub fn validate(&self) -> Result<(), Error> {
        if self.market.is_empty()
            || self.session_id.is_empty()
            || self.exchange_ns > self.received_ns
        {
            return Err(Error::InvalidBook);
        }
        for levels in [&self.yes_bids, &self.no_bids] {
            if levels.iter().any(|l| l.quantity == 0)
                || levels.windows(2).any(|w| w[0].price <= w[1].price)
            {
                return Err(Error::InvalidLevel);
            }
        }
        if let (Some(y), Some(n)) = (self.yes_bids.first(), self.no_bids.first()) {
            if u32::from(y.price.0) + u32::from(n.price.0) > u32::from(ONE_DOLLAR) {
                return Err(Error::InvalidBook);
            }
        }
        Ok(())
    }
    pub fn best_bid(&self, side: Side) -> Option<Price> {
        match side {
            Side::Yes => self.yes_bids.first(),
            Side::No => self.no_bids.first(),
        }
        .map(|l| l.price)
    }
    pub fn best_ask(&self, side: Side) -> Option<Price> {
        self.best_bid(match side {
            Side::Yes => Side::No,
            Side::No => Side::Yes,
        })
        .map(Price::complement)
    }
    pub fn spread(&self, side: Side) -> Option<u16> {
        Some(self.best_ask(side)?.ticks() - self.best_bid(side)?.ticks())
    }
    pub fn ask_depth(&self, side: Side) -> &[Level] {
        match side {
            Side::Yes => &self.no_bids,
            Side::No => &self.yes_bids,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeRate {
    pub numerator: u64,
    pub denominator: u64,
}
impl FeeRate {
    pub const ZERO: Self = Self {
        numerator: 0,
        denominator: 1,
    };
    pub const STANDARD_TAKER: Self = Self {
        numerator: 7,
        denominator: 100,
    };
    pub const MAKER_IF_APPLICABLE: Self = Self {
        numerator: 7,
        denominator: 400,
    };

    /// C * rate * P*(1-P), rounded upward to $0.0001 for one price-level
    /// aggregate. Callers must choose the appropriate historical product/role rate.
    pub fn charge(self, quantity: u64, price: Price) -> Result<Money, Error> {
        if quantity == 0 {
            return Err(Error::ZeroQuantity);
        }
        if self.denominator == 0 {
            return Err(Error::InvalidFee);
        }
        let n = u128::from(quantity)
            .checked_mul(u128::from(self.numerator))
            .and_then(|x| x.checked_mul(u128::from(price.0)))
            .and_then(|x| x.checked_mul(u128::from(ONE_DOLLAR - price.0)))
            .ok_or(Error::ArithmeticOverflow)?;
        let d = u128::from(self.denominator)
            .checked_mul(u128::from(ONE_DOLLAR))
            .ok_or(Error::ArithmeticOverflow)?;
        let charge = n / d + u128::from(n % d != 0);
        Money::try_from(charge).map_err(|_| Error::ArithmeticOverflow)
    }
}

/// A snapshot may only be used if it had ARRIVED by the strategy's decision
/// or simulated exchange-arrival time, and remains within the freshness cap.
pub fn require_asof(
    snapshot_received_ns: TimestampNs,
    asof_ns: TimestampNs,
    max_age_ns: i64,
) -> Result<(), Error> {
    if snapshot_received_ns > asof_ns {
        return Err(Error::FutureData);
    }
    if max_age_ns < 0 || asof_ns.saturating_sub(snapshot_received_ns) > max_age_ns {
        return Err(Error::StaleData);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceStatus {
    First,
    Continuous,
    Gap { missing: u64 },
    DuplicateOrReordered,
}
#[derive(Default)]
pub struct SequenceTracker {
    last: HashMap<(String, String), u64>,
}
impl SequenceTracker {
    pub fn observe(&mut self, feed: &str, session: &str, sequence: u64) -> SequenceStatus {
        let key = (feed.into(), session.into());
        match self.last.get(&key).copied() {
            None => {
                self.last.insert(key, sequence);
                SequenceStatus::First
            }
            Some(prev) if sequence <= prev => SequenceStatus::DuplicateOrReordered,
            Some(prev) => {
                self.last.insert(key, sequence);
                if sequence == prev + 1 {
                    SequenceStatus::Continuous
                } else {
                    SequenceStatus::Gap {
                        missing: sequence - prev - 1,
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evidence {
    pub source_id: String,
    pub content_sha256: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryFinal {
    pub market: String,
    pub rules_sha256: String,
    pub winner: Side,
    pub venue_final: Evidence,
    /// Must refer to independently sourced primary outcome observations,
    /// not another copy of the venue response.
    pub independent: Option<Evidence>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceTier {
    VenueFinal,
    Independent,
}
impl BinaryFinal {
    /// Distinct identifiers and hashes are NOT sufficient provenance to
    /// certify independent reference observations. Until an authenticated
    /// source+rules verifier exists this returns at most VenueFinal.
    pub fn tier(&self) -> Result<EvidenceTier, Error> {
        if self.market.trim().is_empty()
            || self.rules_sha256.trim().is_empty()
            || self.venue_final.source_id.trim().is_empty()
            || self.venue_final.content_sha256.trim().is_empty()
        {
            return Err(Error::MissingEvidence);
        }
        match &self.independent {
            None => Ok(EvidenceTier::VenueFinal),
            Some(other)
                if other.source_id.trim().is_empty()
                    || other.content_sha256.trim().is_empty()
                    || other.source_id == self.venue_final.source_id
                    || other.content_sha256 == self.venue_final.content_sha256 =>
            {
                Err(Error::EvidenceNotIndependent)
            }
            Some(_) => Ok(EvidenceTier::VenueFinal),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModeledBuyFill {
    pub market: String,
    pub rules_sha256: String,
    pub side: Side,
    pub price: Price,
    pub quantity: u64,
    pub entry_fee: Money,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettledPnl {
    pub net: Money,
    pub tier: EvidenceTier,
}

/// Modeled buy-to-expiry P&L, not a real executed trade. Unknown settlement
/// or invalid/mismatched evidence must remain NULL in downstream reporting.
pub fn settle_binary(
    fill: &ModeledBuyFill,
    final_result: &BinaryFinal,
) -> Result<SettledPnl, Error> {
    if fill.quantity == 0 {
        return Err(Error::ZeroQuantity);
    }
    if fill.market != final_result.market {
        return Err(Error::MarketMismatch);
    }
    if fill.rules_sha256 != final_result.rules_sha256 {
        return Err(Error::RuleMismatch);
    }
    let tier = final_result.tier()?;
    let q = Money::from(fill.quantity);
    let stake = Money::from(fill.price.0)
        .checked_mul(q)
        .ok_or(Error::ArithmeticOverflow)?;
    let payout = if fill.side == final_result.winner {
        Money::from(ONE_DOLLAR)
            .checked_mul(q)
            .ok_or(Error::ArithmeticOverflow)?
    } else {
        0
    };
    let net = payout
        .checked_sub(stake)
        .and_then(|x| x.checked_sub(fill.entry_fee))
        .ok_or(Error::ArithmeticOverflow)?;
    Ok(SettledPnl { net, tier })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn p(x: u16) -> Price {
        Price::new(x).unwrap()
    }
    fn book() -> Book {
        Book {
            market: "M".into(),
            yes_bids: vec![Level {
                price: p(4000),
                quantity: 5,
            }],
            no_bids: vec![Level {
                price: p(5700),
                quantity: 2,
            }],
            exchange_ns: 90,
            received_ns: 100,
            session_id: "S1".into(),
            sequence: 1,
        }
    }
    #[test]
    fn yes_no_complement_and_spread() {
        let b = book();
        b.validate().unwrap();
        assert_eq!(b.best_ask(Side::Yes), Some(p(4300)));
        assert_eq!(b.best_ask(Side::No), Some(p(6000)));
        assert_eq!(b.spread(Side::Yes), Some(300));
    }
    #[test]
    fn reject_bad_books() {
        let mut b = book();
        b.no_bids[0].price = p(7000);
        assert_eq!(b.validate(), Err(Error::InvalidBook));
        let mut b = book();
        b.yes_bids.push(b.yes_bids[0]);
        assert_eq!(b.validate(), Err(Error::InvalidLevel));
    }
    #[test]
    fn fee_schedule_rounding() {
        assert_eq!(FeeRate::STANDARD_TAKER.charge(1, p(5000)).unwrap(), 175);
        assert_eq!(FeeRate::STANDARD_TAKER.charge(1, p(5500)).unwrap(), 174);
        assert_eq!(FeeRate::MAKER_IF_APPLICABLE.charge(1, p(5000)).unwrap(), 44);
        assert_eq!(FeeRate::ZERO.charge(1, p(5000)).unwrap(), 0);
    }
    #[test]
    fn causality_and_freshness() {
        assert_eq!(require_asof(110, 100, 20), Err(Error::FutureData));
        assert_eq!(require_asof(90, 100, 5), Err(Error::StaleData));
        require_asof(90, 100, 10).unwrap();
    }
    #[test]
    fn sequence_gaps_are_not_clean() {
        let mut t = SequenceTracker::default();
        assert_eq!(t.observe("kalshi", "a", 4), SequenceStatus::First);
        assert_eq!(
            t.observe("kalshi", "a", 6),
            SequenceStatus::Gap { missing: 1 }
        );
        assert_eq!(
            t.observe("kalshi", "a", 6),
            SequenceStatus::DuplicateOrReordered
        );
        assert_eq!(t.observe("kalshi", "b", 1), SequenceStatus::First);
    }
    fn final_result() -> BinaryFinal {
        BinaryFinal {
            market: "M".into(),
            rules_sha256: "r".into(),
            winner: Side::Yes,
            venue_final: Evidence {
                source_id: "kalshi".into(),
                content_sha256: "x".into(),
            },
            independent: None,
        }
    }
    #[test]
    fn evidence_and_pnl() {
        let f = ModeledBuyFill {
            market: "M".into(),
            rules_sha256: "r".into(),
            side: Side::Yes,
            price: p(5500),
            quantity: 1,
            entry_fee: 174,
        };
        let s = final_result();
        assert_eq!(
            settle_binary(&f, &s).unwrap(),
            SettledPnl {
                net: 4326,
                tier: EvidenceTier::VenueFinal
            }
        );
        let bad = BinaryFinal {
            independent: Some(s.venue_final.clone()),
            ..s.clone()
        };
        assert_eq!(settle_binary(&f, &bad), Err(Error::EvidenceNotIndependent));
        let distinct = BinaryFinal {
            independent: Some(Evidence {
                source_id: "independent-index".into(),
                content_sha256: "z".into(),
            }),
            ..s
        };
        assert_eq!(
            settle_binary(&f, &distinct).unwrap().tier,
            EvidenceTier::VenueFinal
        );
    }
    #[test]
    fn reject_rule_mismatch() {
        let f = ModeledBuyFill {
            market: "M".into(),
            rules_sha256: "r2".into(),
            side: Side::Yes,
            price: p(5000),
            quantity: 1,
            entry_fee: 0,
        };
        assert_eq!(settle_binary(&f, &final_result()), Err(Error::RuleMismatch));
    }
}

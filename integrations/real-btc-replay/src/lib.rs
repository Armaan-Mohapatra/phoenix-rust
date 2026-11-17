//! A genuine source-derived, bounded BTC historical replay regression.
//! The BRTI arrays are actual 1 Hz Kalshi-forwarded CF Benchmark observations
//! and books are top-10 projections reconstructed OFFLINE from archived real
//! WSS deltas. Never represent them as authenticated independent CF source,
//! a full raw-event Rust replay, an actual order fill or an L3 maker queue.
use phoenix_domain::{
    settle_binary, BinaryFinal, Book, Evidence, EvidenceTier, FeeRate, Level, ModeledBuyFill,
    Money, Price, Side,
};
use phoenix_sim::{maker_buy, taker_buy, Error as SimError, OrderIntent};

pub const TICKER: &str = "KXBTC15M-26OCT241900-00";
pub const RULES_SHA256: &str = "4863a05531b5a2225fe6a0992b44a366599fa3f8346864a74cb8dccd5315c51e";
pub const SOURCE_LEDGER_SHA256: &str =
    "6594c16bd097bc295e6ae6d3bcd16d5fad7b2c2aea9cf7b60f6f832b5b436b2b";
pub const DECISION_NS: i64 = 1_792_882_059_000_000_000; // 2026-10-24T22:47:39Z
pub const OPEN_AVG_KNOWN_NS: i64 = 1_792_881_901_785_000_000; // 22:45:01.785
pub const CLOSE_AVG_KNOWN_NS: i64 = 1_792_882_800_011_000_000; // 23:00:00.011

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PilotError {
    BadFixture,
    NotEnoughReferenceData,
    MissingEvidenceAtDecision,
    UnverifiedInitialization,
    IncompleteMakerTape,
    NoTakerFill,
    PriceMismatch,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowOutcome {
    pub open_sum_cents: i64,
    pub close_sum_cents: i64,
    pub winner: Side,
    pub independent_primary_source_authenticated: bool,
}
/// A full 60-source-second window, with sequential, inferred 1 Hz timestamps.
/// Source-specific local receipt timestamps are NOT embedded here; original
/// segment SHA list and timing bounds are recorded in the evidence manifest.
fn parse_window(line: &str, expected: &str) -> Result<[i64; 60], PilotError> {
    let mut cells = line.trim().split(',');
    if cells.next() != Some(expected) {
        return Err(PilotError::BadFixture);
    }
    let start = match expected {
        "OPEN" => "2026-10-24T22:44:00Z",
        "CLOSE" => "2026-10-24T22:59:00Z",
        _ => return Err(PilotError::BadFixture),
    };
    if cells.next() != Some(start) {
        return Err(PilotError::BadFixture);
    }
    let readings: Vec<i64> = cells
        .map(|v| v.parse::<i64>().map_err(|_| PilotError::BadFixture))
        .collect::<Result<_, _>>()?;
    let arr: [i64; 60] = readings
        .try_into()
        .map_err(|_| PilotError::NotEnoughReferenceData)?;
    if arr.iter().any(|v| *v <= 0) {
        return Err(PilotError::BadFixture);
    }
    Ok(arr)
}
pub fn reconstruct_benchmark() -> Result<WindowOutcome, PilotError> {
    let s = include_str!("../fixtures/brti_1hz_real_cents.txt");
    let mut lines = s.lines().filter(|l| !l.is_empty());
    let open = parse_window(lines.next().ok_or(PilotError::BadFixture)?, "OPEN")?;
    let close = parse_window(lines.next().ok_or(PilotError::BadFixture)?, "CLOSE")?;
    if lines.next().is_some() {
        return Err(PilotError::BadFixture);
    }
    let o = open
        .iter()
        .try_fold(0i64, |acc, v| acc.checked_add(*v))
        .ok_or(PilotError::BadFixture)?;
    let c = close
        .iter()
        .try_fold(0i64, |acc, v| acc.checked_add(*v))
        .ok_or(PilotError::BadFixture)?;
    Ok(WindowOutcome {
        open_sum_cents: o,
        close_sum_cents: c,
        winner: if c >= o { Side::Yes } else { Side::No },
        independent_primary_source_authenticated: false,
    })
}
pub fn payout_known_at(asof_ns: i64) -> Result<WindowOutcome, PilotError> {
    if asof_ns < CLOSE_AVG_KNOWN_NS {
        return Err(PilotError::MissingEvidenceAtDecision);
    }
    reconstruct_benchmark()
}
/// This is a PREPROCESSED REAL historical top-10 checkpoint, NOT a raw WSS
/// snapshot or independent proof of full book initialization.
#[derive(Debug, Clone)]
pub struct BookProjection {
    pub asof_ns: i64,
    pub last_selected_seq: u64,
    pub prior_deltas: u64,
    pub yes: Vec<(u16, u64)>,
    pub no: Vec<(u16, u64)>,
}
fn utc_ms(t: &str) -> Result<i64, PilotError> {
    match t {
        "2026-10-24T22:47:37.491Z" => Ok(1_792_882_057_491),
        "2026-10-24T22:47:39.000Z" => Ok(1_792_882_059_000),
        "2026-10-24T22:47:39.100Z" => Ok(1_792_882_059_100),
        "2026-10-24T22:47:39.250Z" => Ok(1_792_882_059_250),
        "2026-10-24T22:47:39.500Z" => Ok(1_792_882_059_500),
        _ => Err(PilotError::BadFixture),
    }
}
fn levels(s: &str) -> Result<Vec<(u16, u64)>, PilotError> {
    let mut out = Vec::new();
    for entry in s.split(',') {
        let (p, q) = entry.split_once(':').ok_or(PilotError::BadFixture)?;
        let price = p.parse::<u16>().map_err(|_| PilotError::BadFixture)?;
        let qty_fp = q.parse::<u64>().map_err(|_| PilotError::BadFixture)?;
        if price == 0 || price >= 10_000 || qty_fp < 100 {
            return Err(PilotError::BadFixture);
        }
        out.push((price, qty_fp));
    }
    if out.len() != 10 || out.windows(2).any(|w| w[0].0 <= w[1].0) {
        return Err(PilotError::BadFixture);
    }
    Ok(out)
}
pub fn load_projected_books() -> Result<Vec<BookProjection>, PilotError> {
    let mut out = Vec::new();
    for l in include_str!("../fixtures/book_top10_real.txt")
        .lines()
        .filter(|l| !l.is_empty())
    {
        let parts: Vec<&str> = l.split('|').collect();
        if parts.len() != 5 {
            return Err(PilotError::BadFixture);
        }
        let asof_ns = utc_ms(parts[0])?
            .checked_mul(1_000_000)
            .ok_or(PilotError::BadFixture)?;
        let sequence = parts[1]
            .parse::<u64>()
            .map_err(|_| PilotError::BadFixture)?;
        let count = parts[2]
            .parse::<u64>()
            .map_err(|_| PilotError::BadFixture)?;
        if out.last().is_some_and(|x: &BookProjection| {
            asof_ns <= x.asof_ns || sequence <= x.last_selected_seq || count <= x.prior_deltas
        }) {
            return Err(PilotError::BadFixture);
        }
        let yes = levels(parts[3])?;
        let no = levels(parts[4])?;
        if u32::from(yes[0].0) + u32::from(no[0].0) > 10_000 {
            return Err(PilotError::BadFixture);
        }
        out.push(BookProjection {
            asof_ns,
            last_selected_seq: sequence,
            prior_deltas: count,
            yes,
            no,
        });
    }
    if out.len() != 5 {
        return Err(PilotError::BadFixture);
    }
    Ok(out)
}
/// Conservative integer-contract projection: archived fractional depth is
/// in 0.01 contracts and is always floored (never rounded upward) to units.
pub fn as_book(p: &BookProjection) -> Result<Book, PilotError> {
    let to_book_levels = |data: &[(u16, u64)]| -> Result<Vec<Level>, PilotError> {
        data.iter()
            .map(|(price, fp)| {
                Ok(Level {
                    price: Price::new(*price).map_err(|_| PilotError::BadFixture)?,
                    quantity: fp / 100,
                })
            })
            .collect()
    };
    let b = Book {
        market: TICKER.to_owned(),
        yes_bids: to_book_levels(&p.yes)?,
        no_bids: to_book_levels(&p.no)?,
        // Event-exchange timestamps in this capture run ahead of local receipt
        // timestamps. Zero denotes UNKNOWN ALIGNED EXCHANGE TIME, not a
        // fabricated source timestamp. The raw exchange timestamps remain in
        // original files for future clock-skew calibration.
        exchange_ns: 0,
        received_ns: p.asof_ns,
        session_id: "8a4171d6a1c54c5486c4b73e7f69f7da".to_owned(),
        sequence: p.last_selected_seq,
    };
    b.validate().map_err(|_| PilotError::BadFixture)?;
    Ok(b)
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModeledCase {
    pub side: Side,
    pub arrival_offset_ms: u32,
    pub ask_ticks: u16,
    pub depth_whole_contracts: u64,
    pub entry_fee_centicents: Money,
    pub modeled_net_centicents: Money,
    pub actual_exchange_fill: false_bool::False,
    pub independently_verified_reference: false_bool::False,
}
/// Enforce literal false flags at type level so no downstream code mistakes
/// this result for a venue execution/independently sourced settlement.
mod false_bool {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct False;
}
pub fn modeled_taker(side: Side, arrival_offset_ms: u32) -> Result<ModeledCase, PilotError> {
    if !matches!(arrival_offset_ms, 0 | 100 | 250 | 500) {
        return Err(PilotError::BadFixture);
    }
    let w = load_projected_books()?;
    let obs = w
        .iter()
        .find(|x| x.asof_ns == DECISION_NS + i64::from(arrival_offset_ms) * 1_000_000)
        .ok_or(PilotError::BadFixture)?;
    let book = as_book(obs)?;
    let ask = book.best_ask(side).ok_or(PilotError::NoTakerFill)?;
    let max_price = match side {
        Side::Yes => Price::new(3900),
        Side::No => Price::new(6200),
    }
    .map_err(|_| PilotError::BadFixture)?;
    // Never silently pay above the original recorded paper limit.
    let o = OrderIntent {
        market: TICKER.to_owned(),
        side,
        quantity: 1,
        buy_limit: max_price,
        signal_received_ns: DECISION_NS,
        decision_ns: DECISION_NS,
        arrival_ns: obs.asof_ns,
        max_age_ns: 1_000_000_000,
        fee_rate: FeeRate::STANDARD_TAKER,
    };
    let b = taker_buy(&o, &[book.clone()]).map_err(|_| PilotError::NoTakerFill)?;
    if b.unfilled != 0 || b.fills.len() != 1 {
        return Err(PilotError::NoTakerFill);
    }
    let level = &b.fills[0];
    if ask != level.price {
        return Err(PilotError::PriceMismatch);
    }
    let final_result = BinaryFinal {
        market: TICKER.into(),
        rules_sha256: RULES_SHA256.into(),
        winner: payout_known_at(CLOSE_AVG_KNOWN_NS)?.winner,
        // Source claim was archived by historical paper ledger. Unlike live
        // signed REST verification, this does not independently authenticate.
        venue_final: Evidence {
            source_id: "archived-kalshi-finalized-claim".into(),
            content_sha256: SOURCE_LEDGER_SHA256.into(),
        },
        independent: None,
    };
    let fill = ModeledBuyFill {
        market: TICKER.into(),
        rules_sha256: RULES_SHA256.into(),
        side,
        price: level.price,
        quantity: 1,
        entry_fee: level.fee,
    };
    let outcome = settle_binary(&fill, &final_result).map_err(|_| PilotError::BadFixture)?;
    if outcome.tier != EvidenceTier::VenueFinal {
        return Err(PilotError::BadFixture);
    }
    let depth_whole_contracts = book.ask_depth(side)[0].quantity;
    Ok(ModeledCase {
        side,
        arrival_offset_ms,
        ask_ticks: ask.ticks(),
        depth_whole_contracts,
        entry_fee_centicents: level.fee,
        modeled_net_centicents: outcome.net,
        actual_exchange_fill: false_bool::False,
        independently_verified_reference: false_bool::False,
    })
}
/// Without a verified full public consuming-trade tape and proven queue
/// initialization, "maker price touched" does NOT confer a simulated fill.
pub fn conservative_maker_is_blocked() -> Result<(), PilotError> {
    let obs = load_projected_books()?
        .into_iter()
        .find(|x| x.asof_ns == DECISION_NS)
        .ok_or(PilotError::BadFixture)?;
    let b = as_book(&obs)?;
    let i = OrderIntent {
        market: TICKER.into(),
        side: Side::Yes,
        quantity: 1,
        buy_limit: Price::new(3800).map_err(|_| PilotError::BadFixture)?,
        signal_received_ns: DECISION_NS,
        decision_ns: DECISION_NS,
        arrival_ns: DECISION_NS,
        max_age_ns: 1_000_000_000,
        fee_rate: FeeRate::ZERO,
    };
    match maker_buy(&i, &[b], &[], DECISION_NS + 250_000_000, false) {
        Err(SimError::IncompleteTape) => Err(PilotError::IncompleteMakerTape),
        _ => Err(PilotError::UnverifiedInitialization),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_one_hundred_twenty_real_1hz_brti_observations() {
        let w = reconstruct_benchmark().unwrap();
        assert_eq!(w.open_sum_cents, 482_856_277);
        assert_eq!(w.close_sum_cents, 482_875_060);
        assert_eq!(w.winner, Side::Yes);
        assert!(!w.independent_primary_source_authenticated);
    }
    #[test]
    fn future_closing_index_is_never_available_at_decision() {
        assert_eq!(
            payout_known_at(DECISION_NS),
            Err(PilotError::MissingEvidenceAtDecision)
        );
        assert_eq!(
            payout_known_at(CLOSE_AVG_KNOWN_NS - 1),
            Err(PilotError::MissingEvidenceAtDecision)
        );
        assert_eq!(
            payout_known_at(CLOSE_AVG_KNOWN_NS).unwrap().winner,
            Side::Yes
        );
        assert!(OPEN_AVG_KNOWN_NS < DECISION_NS);
    }
    #[test]
    fn actual_projected_books_are_monotonic_and_validate() {
        let v = load_projected_books().unwrap();
        assert_eq!(v[0].prior_deltas, 62_808);
        assert_eq!(v[1].prior_deltas, 63_813);
        for b in &v {
            as_book(b).unwrap();
        }
        assert_eq!(
            as_book(&v[0]).unwrap().best_ask(Side::Yes),
            Some(Price::new(4000).unwrap())
        );
        assert_eq!(
            as_book(&v[1]).unwrap().best_ask(Side::Yes),
            Some(Price::new(3900).unwrap())
        );
    }
    #[test]
    fn real_market_yes_and_no_arrival_sensitivity_and_fee_pnl() {
        for ms in [0, 100, 250, 500] {
            let yes = modeled_taker(Side::Yes, ms).unwrap();
            let no = modeled_taker(Side::No, ms).unwrap();
            assert_eq!((yes.ask_ticks, no.ask_ticks), (3900, 6200));
            assert_eq!(yes.entry_fee_centicents, 167);
            assert_eq!(no.entry_fee_centicents, 165);
            assert_eq!(yes.modeled_net_centicents, 5933);
            assert_eq!(no.modeled_net_centicents, -6365);
            assert!(yes.depth_whole_contracts >= 1);
            assert!(no.depth_whole_contracts >= 1);
        }
    }
    #[test]
    fn maker_is_not_a_free_fill_without_queue_proof() {
        assert_eq!(
            conservative_maker_is_blocked(),
            Err(PilotError::IncompleteMakerTape)
        );
    }
    #[test]
    fn source_rule_and_ledger_checks_not_promoted_to_independent() {
        assert_eq!(TICKER, "KXBTC15M-26OCT241900-00");
        assert_eq!(RULES_SHA256.len(), 64);
        let case = modeled_taker(Side::No, 0).unwrap();
        assert_eq!(case.modeled_net_centicents, -6365);
        assert_eq!(case.actual_exchange_fill, false_bool::False);
        assert_eq!(case.independently_verified_reference, false_bool::False);
    }
}

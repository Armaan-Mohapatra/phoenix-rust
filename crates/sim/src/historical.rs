//! Source-gated historical LIMIT order simulation. The existing phoenix-sim
//! crate remains the single execution engine; this module extends it to
//! fractional depth, archived fee quotes, receipt-time causality and lifecycles.
//! No API/network, order submission or actual venue fill claims.
use phoenix_domain::{Money, Price, Side};
use serde::Serialize;
use std::collections::BTreeSet;

const MAX_BOOKS: usize = 4096;
const MAX_LEVELS: usize = 200;
const MAX_PRINTS: usize = 50_000;
const MAX_SCENARIOS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LiquidityRole {
    Taker,
    Maker,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderState {
    Unfilled,
    PartiallyFilled,
    Filled,
    Cancelled,
    Expired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DepthQuality {
    Unverified,
    VerifiedSourceWindow,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    InvalidOrder,
    InvalidBook,
    InvalidTape,
    MissingBook,
    UnverifiedSource,
    StaleBook,
    FutureSignal,
    IncompleteTape,
    MarketableMaker,
    MissingFee,
    Overflow,
    Capacity,
    DuplicatePrint,
}
#[derive(Debug, Clone)]
pub struct FractionalLevel {
    pub price: Price,
    pub quantity_hundredths: u64,
}
#[derive(Debug, Clone)]
pub struct FractionalBook {
    pub market: String,
    pub received_ns: i64,
    pub exchange_ns: i64,
    pub session_id: String,
    pub snapshot_sha256: String,
    pub quality: DepthQuality,
    /// Highest bid to lowest bid; quantities are hundredths of one contract.
    pub yes_bids: Vec<FractionalLevel>,
    pub no_bids: Vec<FractionalLevel>,
}
impl FractionalBook {
    pub fn validate(&self) -> Result<(), Error> {
        if self.market.is_empty()
            || self.session_id.is_empty()
            || self.received_ns <= 0
            || self.exchange_ns > self.received_ns
            || !digest(&self.snapshot_sha256)
        {
            return Err(Error::InvalidBook);
        }
        for side in [&self.yes_bids, &self.no_bids] {
            if side.len() > MAX_LEVELS
                || side.iter().any(|l| l.quantity_hundredths == 0)
                || side.windows(2).any(|w| w[0].price <= w[1].price)
            {
                return Err(Error::InvalidBook);
            }
        }
        if self
            .yes_bids
            .first()
            .zip(self.no_bids.first())
            .is_some_and(|(y, n)| u32::from(y.price.ticks()) + u32::from(n.price.ticks()) > 10_000)
        {
            return Err(Error::InvalidBook);
        }
        Ok(())
    }
    fn side_bids(&self, side: Side) -> &[FractionalLevel] {
        match side {
            Side::Yes => &self.yes_bids,
            Side::No => &self.no_bids,
        }
    }
    fn opposite_bids(&self, side: Side) -> &[FractionalLevel] {
        match side {
            Side::Yes => &self.no_bids,
            Side::No => &self.yes_bids,
        }
    }
}
#[derive(Debug, Clone)]
pub struct LimitIntent {
    pub market: String,
    pub strategy_order_id: String,
    pub side: Side,
    pub role: LiquidityRole,
    pub quantity_hundredths: u64,
    pub limit_price: Price,
    pub signal_received_ns: i64,
    pub decision_ns: i64,
    pub arrival_ns: i64,
    pub deadline_ns: i64,
    pub cancelled_at_ns: Option<i64>,
    pub max_quote_age_ns: i64,
    pub maker_queue_ahead_extra_hundredths: u64,
}
#[derive(Debug, Clone)]
pub struct ConsumingPrint {
    pub market: String,
    pub trade_id: String,
    pub resting_side: Side,
    pub price: Price,
    pub quantity_hundredths: u64,
    pub received_ns: i64,
}
#[derive(Debug, Clone)]
pub struct TradeTape<'a> {
    pub complete_for_window: bool,
    pub prints: &'a [ConsumingPrint],
}
/// Explicit complete source-window witness. This checks consistency, not
/// cryptographic authentication of third-party history. The source importer
/// must independently verify every digest/sequence before supplying it.
#[derive(Debug, Clone)]
pub struct SourceWindowEvidence {
    pub market: String,
    pub session_id: String,
    pub manifest_sha256: String,
    pub book_sha256: String,
    pub tape_sha256: String,
    pub first_received_ns: i64,
    pub last_received_ns: i64,
    pub tape_first_received_ns: i64,
    pub tape_last_received_ns: i64,
    pub channel_continuity_verified: bool,
    pub snapshot_origin_verified: bool,
    pub tape_coverage_verified: bool,
    pub missing_channel_sequences: u64,
    pub duplicate_or_regressing_sequences: u64,
}
/// Strict gate rejects missing verification or incompatible sessions; no
/// caller boolean or a single SHA is itself proof of external authenticity.
pub fn simulate_source_window(
    o: &LimitIntent,
    books: &[FractionalBook],
    tape: TradeTape<'_>,
    proof: &SourceWindowEvidence,
    fee: &impl HistoricalFeeSource,
) -> Result<ExecutionReport, Error> {
    validate_intent(o)?;
    let b = select_book(o, books)?;
    if proof.market != o.market
        || proof.session_id != b.session_id
        || proof.book_sha256 != b.snapshot_sha256
        || !digest(&proof.manifest_sha256)
        || !digest(&proof.book_sha256)
        || !digest(&proof.tape_sha256)
        || proof.first_received_ns <= 0
        || proof.last_received_ns < proof.first_received_ns
        || proof.tape_last_received_ns < proof.tape_first_received_ns
        || b.received_ns < proof.first_received_ns
        || b.received_ns > proof.last_received_ns
        || o.arrival_ns > proof.last_received_ns
        || !proof.channel_continuity_verified
        || !proof.snapshot_origin_verified
        || proof.missing_channel_sequences > 0
        || proof.duplicate_or_regressing_sequences > 0
    {
        return Err(Error::UnverifiedSource);
    }
    if o.role == LiquidityRole::Maker {
        if !proof.tape_coverage_verified
            || !tape.complete_for_window
            || proof.tape_first_received_ns > o.arrival_ns
            || proof.tape_last_received_ns < o.deadline_ns
        {
            return Err(Error::IncompleteTape);
        }
    }
    let mut r = simulate(o, books, tape, fee)?;
    r.assumptions
        .push("SOURCE_WINDOW_COHERENCE_CHECKED_NOT_PROVIDER_AUTHENTICATED".into());
    r.assumptions
        .push(format!("SOURCE_MANIFEST_SHA256={}", proof.manifest_sha256));
    r.assumptions
        .push(format!("SOURCE_TAPE_SHA256={}", proof.tape_sha256));
    Ok(r)
}
#[derive(Debug, Clone)]
pub struct FeeQuote {
    /// Fixed-point 1/10,000 USD fee units (same scale as domain::Money).
    pub fee_centicents: Money,
    pub effective_schedule_sha256: String,
}
/// Caller must link this to the separately verified effective-dated
/// contract-economics registry. UNKNOWN_FEE must return None, never zero.
pub trait HistoricalFeeSource {
    fn quote(
        &self,
        market: &str,
        role: LiquidityRole,
        price: Price,
        quantity_hundredths: u64,
        at_ns: i64,
    ) -> Option<FeeQuote>;
}
#[derive(Debug, Clone, Serialize)]
pub struct FillEvidence {
    pub price_ticks_1e4: u16,
    pub quantity_hundredths: u64,
    /// Exact cost uses 1/1,000,000 USD units (price 1e-4 * quantity 1e-2).
    pub cost_micro_usd: i128,
    pub fee_centicents: Money,
    pub fee_schedule_sha256: String,
}
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionReport {
    pub schema_version: u32,
    pub market: String,
    pub strategy_order_id: String,
    pub role: LiquidityRole,
    pub state: OrderState,
    pub submitted_hundredths: u64,
    pub filled_hundredths: u64,
    pub unfilled_hundredths: u64,
    pub modeled_fills: Vec<FillEvidence>,
    pub modeled_cost_micro_usd: i128,
    pub modeled_fees_centicents: Money,
    pub quote_received_ns: Option<i64>,
    pub order_arrival_ns: i64,
    pub event_deadline_ns: i64,
    pub quote_source_sha256: Option<String>,
    pub conservative_queue_ahead_hundredths: Option<u64>,
    pub latency_ns: i64,
    pub assumptions: Vec<String>,
    pub venue_fill_verified: bool,
}
fn digest(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|x| x.is_ascii_hexdigit())
}
fn validate_intent(o: &LimitIntent) -> Result<(), Error> {
    if o.market.is_empty()
        || o.strategy_order_id.is_empty()
        || o.quantity_hundredths == 0
        || o.signal_received_ns <= 0
        || o.decision_ns > o.arrival_ns
        || o.max_quote_age_ns < 0
        || o.deadline_ns < o.decision_ns
        || o.cancelled_at_ns.is_some_and(|c| c < o.decision_ns)
    {
        return Err(Error::InvalidOrder);
    }
    if o.signal_received_ns > o.decision_ns {
        return Err(Error::FutureSignal);
    }
    Ok(())
}
fn select_book<'a>(
    o: &LimitIntent,
    books: &'a [FractionalBook],
) -> Result<&'a FractionalBook, Error> {
    if books.len() > MAX_BOOKS {
        return Err(Error::Capacity);
    }
    let b = books
        .iter()
        .filter(|b| b.market == o.market && b.received_ns <= o.arrival_ns)
        .max_by_key(|b| b.received_ns)
        .ok_or(Error::MissingBook)?;
    b.validate()?;
    if b.quality != DepthQuality::VerifiedSourceWindow {
        return Err(Error::UnverifiedSource);
    }
    if o.arrival_ns
        .checked_sub(b.received_ns)
        .map_or(true, |age| age < 0 || age > o.max_quote_age_ns)
    {
        return Err(Error::StaleBook);
    }
    Ok(b)
}
fn empty_report(o: &LimitIntent, state: OrderState) -> ExecutionReport {
    ExecutionReport {
        schema_version: 1,
        market: o.market.clone(),
        strategy_order_id: o.strategy_order_id.clone(),
        role: o.role,
        state,
        submitted_hundredths: o.quantity_hundredths,
        filled_hundredths: 0,
        unfilled_hundredths: o.quantity_hundredths,
        modeled_fills: Vec::new(),
        modeled_cost_micro_usd: 0,
        modeled_fees_centicents: 0,
        quote_received_ns: None,
        order_arrival_ns: o.arrival_ns,
        event_deadline_ns: o.deadline_ns,
        quote_source_sha256: None,
        conservative_queue_ahead_hundredths: None,
        latency_ns: o.arrival_ns.saturating_sub(o.decision_ns),
        assumptions: vec!["MODELED_ONLY_NOT_VENUE_EXECUTION".into()],
        venue_fill_verified: false,
    }
}
fn add_fill(
    report: &mut ExecutionReport,
    fee: &impl HistoricalFeeSource,
    o: &LimitIntent,
    price: Price,
    quantity: u64,
    at_ns: i64,
) -> Result<(), Error> {
    if quantity == 0 {
        return Ok(());
    }
    let q = fee
        .quote(&o.market, o.role, price, quantity, at_ns)
        .ok_or(Error::MissingFee)?;
    if q.fee_centicents < 0 || !digest(&q.effective_schedule_sha256) {
        return Err(Error::MissingFee);
    }
    let micro = i128::from(quantity)
        .checked_mul(i128::from(price.ticks()))
        .ok_or(Error::Overflow)?;
    report.modeled_cost_micro_usd = report
        .modeled_cost_micro_usd
        .checked_add(micro)
        .ok_or(Error::Overflow)?;
    report.modeled_fees_centicents = report
        .modeled_fees_centicents
        .checked_add(q.fee_centicents)
        .ok_or(Error::Overflow)?;
    report.filled_hundredths = report
        .filled_hundredths
        .checked_add(quantity)
        .ok_or(Error::Overflow)?;
    if report.filled_hundredths > o.quantity_hundredths {
        return Err(Error::Overflow);
    }
    report.unfilled_hundredths = o.quantity_hundredths - report.filled_hundredths;
    report.modeled_fills.push(FillEvidence {
        price_ticks_1e4: price.ticks(),
        quantity_hundredths: quantity,
        cost_micro_usd: micro,
        fee_centicents: q.fee_centicents,
        fee_schedule_sha256: q.effective_schedule_sha256,
    });
    Ok(())
}
/// Deterministic historical simulation: caller-supplied source certification
/// is a precondition, NOT authenticated proof returned by this API.
pub fn simulate(
    o: &LimitIntent,
    books: &[FractionalBook],
    tape: TradeTape<'_>,
    fee: &impl HistoricalFeeSource,
) -> Result<ExecutionReport, Error> {
    validate_intent(o)?;
    if o.cancelled_at_ns.is_some_and(|t| t <= o.arrival_ns) {
        return Ok(empty_report(o, OrderState::Cancelled));
    }
    if o.deadline_ns < o.arrival_ns {
        return Ok(empty_report(o, OrderState::Expired));
    }
    let b = select_book(o, books)?;
    let mut r = empty_report(o, OrderState::Unfilled);
    r.quote_received_ns = Some(b.received_ns);
    r.quote_source_sha256 = Some(b.snapshot_sha256.clone());
    r.assumptions
        .push("RECEIPT_TIME_CAUSAL_BOOK_SNAPSHOT".into());
    r.assumptions
        .push("HYPOTHETICAL_MODEL_SOURCE_NOT_AUTHENTICATED".into());
    let effective_deadline = o
        .cancelled_at_ns
        .map_or(o.deadline_ns, |t| t.min(o.deadline_ns));
    if o.role == LiquidityRole::Taker {
        // A marketable limit buy sweeps observed opposite bids (best ask first).
        // Nonmarketable orders stay unfilled; no hidden liquidity is assumed.
        let mut remaining = o.quantity_hundredths;
        for level in b.opposite_bids(o.side) {
            let price = level.price.complement();
            if price > o.limit_price {
                break;
            }
            let q = remaining.min(level.quantity_hundredths);
            add_fill(&mut r, fee, o, price, q, o.arrival_ns)?;
            remaining -= q;
            if remaining == 0 {
                break;
            }
        }
        r.assumptions.push("TAKER_IOC_VISIBLE_DEPTH_ONLY".into());
    } else {
        if !tape.complete_for_window {
            return Err(Error::IncompleteTape);
        }
        if tape.prints.len() > MAX_PRINTS {
            return Err(Error::Capacity);
        }
        if b.opposite_bids(o.side)
            .first()
            .is_some_and(|l| o.limit_price >= l.price.complement())
        {
            return Err(Error::MarketableMaker);
        }
        let displayed = b
            .side_bids(o.side)
            .iter()
            .find(|l| l.price == o.limit_price)
            .map_or(0, |l| l.quantity_hundredths);
        let mut queue = displayed
            .checked_add(o.maker_queue_ahead_extra_hundredths)
            .ok_or(Error::Overflow)?;
        r.conservative_queue_ahead_hundredths = Some(queue);
        let mut seen = BTreeSet::new();
        let mut relevant = tape
            .prints
            .iter()
            .filter(|p| {
                p.market == o.market
                    && p.resting_side == o.side
                    && p.price == o.limit_price
                    && p.received_ns > o.arrival_ns
                    && p.received_ns <= effective_deadline
            })
            .collect::<Vec<_>>();
        relevant.sort_by_key(|p| (&p.received_ns, &p.trade_id));
        for p in relevant {
            if p.trade_id.is_empty() || p.quantity_hundredths == 0 {
                return Err(Error::InvalidTape);
            }
            if !seen.insert(&p.trade_id) {
                return Err(Error::DuplicatePrint);
            }
            let consuming_queue = queue.min(p.quantity_hundredths);
            queue -= consuming_queue;
            let qty = (p.quantity_hundredths - consuming_queue).min(r.unfilled_hundredths);
            add_fill(&mut r, fee, o, o.limit_price, qty, p.received_ns)?;
            if r.unfilled_hundredths == 0 {
                break;
            }
        }
        r.assumptions
            .push("MAKER_DISPLAYED_QUEUE_AHEAD_NO_CANCEL_CREDIT".into());
        r.assumptions
            .push("TAPE_COMPLETENESS_CALLER_ASSERTED".into());
    }
    r.state = if r.unfilled_hundredths == 0 {
        OrderState::Filled
    } else if r.filled_hundredths > 0 {
        OrderState::PartiallyFilled
    } else if o.cancelled_at_ns.is_some_and(|c| c <= o.deadline_ns) {
        OrderState::Cancelled
    } else if o.role == LiquidityRole::Maker {
        OrderState::Expired
    } else {
        OrderState::Unfilled
    };
    Ok(r)
}
/// Bounded latency/quote-freshness scenarios; does not optimize against OOS.
pub fn simulate_scenarios(
    orders: &[LimitIntent],
    books: &[FractionalBook],
    tape: TradeTape<'_>,
    fee: &impl HistoricalFeeSource,
) -> Result<Vec<Result<ExecutionReport, Error>>, Error> {
    if orders.len() > MAX_SCENARIOS {
        return Err(Error::Capacity);
    }
    Ok(orders
        .iter()
        .map(|o| {
            simulate(
                o,
                books,
                TradeTape {
                    complete_for_window: tape.complete_for_window,
                    prints: tape.prints,
                },
                fee,
            )
        })
        .collect())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn p(x: u16) -> Price {
        Price::new(x).unwrap()
    }
    struct FixtureFee;
    impl HistoricalFeeSource for FixtureFee {
        fn quote(&self, _: &str, _: LiquidityRole, _: Price, _: u64, _: i64) -> Option<FeeQuote> {
            Some(FeeQuote {
                fee_centicents: 100,
                effective_schedule_sha256: "f".repeat(64),
            })
        }
    }
    fn o(role: LiquidityRole) -> LimitIntent {
        LimitIntent {
            market: "BTC".into(),
            strategy_order_id: "ord1".into(),
            side: Side::Yes,
            role,
            quantity_hundredths: 500,
            limit_price: p(4800),
            signal_received_ns: 10,
            decision_ns: 20,
            arrival_ns: 30,
            deadline_ns: 60,
            cancelled_at_ns: None,
            max_quote_age_ns: 30,
            maker_queue_ahead_extra_hundredths: 0,
        }
    }
    fn b() -> FractionalBook {
        FractionalBook {
            market: "BTC".into(),
            received_ns: 25,
            exchange_ns: 24,
            session_id: "session".into(),
            snapshot_sha256: "a".repeat(64),
            quality: DepthQuality::VerifiedSourceWindow,
            yes_bids: vec![FractionalLevel {
                price: p(4000),
                quantity_hundredths: 200,
            }],
            no_bids: vec![
                FractionalLevel {
                    price: p(5500),
                    quantity_hundredths: 125,
                },
                FractionalLevel {
                    price: p(5300),
                    quantity_hundredths: 200,
                },
            ],
        }
    }
    fn t<'a>(p: &'a [ConsumingPrint]) -> TradeTape<'a> {
        TradeTape {
            complete_for_window: true,
            prints: p,
        }
    }
    #[test]
    fn fractional_taker_sweeps_depth_with_exact_micro_usd_cost() {
        let report = simulate(&o(LiquidityRole::Taker), &[b()], t(&[]), &FixtureFee).unwrap();
        assert_eq!(report.filled_hundredths, 325);
        assert_eq!(report.unfilled_hundredths, 175);
        assert_eq!(report.modeled_fills.len(), 2);
        assert_eq!(report.modeled_cost_micro_usd, 125 * 4500 + 200 * 4700);
        assert_eq!(report.state, OrderState::PartiallyFilled);
        assert!(!report.venue_fill_verified);
    }
    #[test]
    fn marketable_maker_is_rejected() {
        assert_eq!(
            simulate(&o(LiquidityRole::Maker), &[b()], t(&[]), &FixtureFee).unwrap_err(),
            Error::MarketableMaker
        );
    }
    #[test]
    fn unknown_fee_fail_closed_even_for_visible_depth() {
        struct NoFee;
        impl HistoricalFeeSource for NoFee {
            fn quote(
                &self,
                _: &str,
                _: LiquidityRole,
                _: Price,
                _: u64,
                _: i64,
            ) -> Option<FeeQuote> {
                None
            }
        }
        assert_eq!(
            simulate(&o(LiquidityRole::Taker), &[b()], t(&[]), &NoFee).unwrap_err(),
            Error::MissingFee
        );
    }
    #[test]
    fn future_and_stale_books_are_never_selected() {
        let mut book = b();
        book.received_ns = 31;
        assert_eq!(
            simulate(&o(LiquidityRole::Taker), &[book], t(&[]), &FixtureFee).unwrap_err(),
            Error::MissingBook
        );
        let mut book = b();
        book.received_ns = 1;
        assert_eq!(
            simulate(&o(LiquidityRole::Taker), &[book], t(&[]), &FixtureFee).unwrap_err(),
            Error::InvalidBook
        ); // exchange_ns 24 > received 1
        let mut book = b();
        book.received_ns = 2;
        book.exchange_ns = 1;
        let mut intent = o(LiquidityRole::Taker);
        intent.max_quote_age_ns = 3;
        assert_eq!(
            simulate(&intent, &[book], t(&[]), &FixtureFee).unwrap_err(),
            Error::StaleBook
        );
    }
    #[test]
    fn missing_verification_blocks_all_modeled_fills() {
        let mut book = b();
        book.quality = DepthQuality::Unverified;
        assert_eq!(
            simulate(&o(LiquidityRole::Taker), &[book], t(&[]), &FixtureFee).unwrap_err(),
            Error::UnverifiedSource
        );
    }
    #[test]
    fn maker_rejects_incomplete_tape_and_duplicate_trades() {
        let mut x = o(LiquidityRole::Maker);
        x.limit_price = p(4000);
        let print = ConsumingPrint {
            market: "BTC".into(),
            trade_id: "t1".into(),
            resting_side: Side::Yes,
            price: p(4000),
            quantity_hundredths: 250,
            received_ns: 40,
        };
        assert_eq!(
            simulate(
                &x,
                &[b()],
                TradeTape {
                    complete_for_window: false,
                    prints: &[print.clone()]
                },
                &FixtureFee
            )
            .unwrap_err(),
            Error::IncompleteTape
        );
        assert_eq!(
            simulate(&x, &[b()], t(&[print.clone(), print]), &FixtureFee).unwrap_err(),
            Error::DuplicatePrint
        );
    }
    #[test]
    fn maker_credits_prints_only_after_queue_ahead() {
        let mut x = o(LiquidityRole::Maker);
        x.limit_price = p(4000);
        let print = ConsumingPrint {
            market: "BTC".into(),
            trade_id: "t1".into(),
            resting_side: Side::Yes,
            price: p(4000),
            quantity_hundredths: 250,
            received_ns: 40,
        };
        let report = simulate(&x, &[b()], t(&[print]), &FixtureFee).unwrap();
        assert_eq!(report.conservative_queue_ahead_hundredths, Some(200));
        assert_eq!(report.filled_hundredths, 50);
        assert_eq!(report.state, OrderState::PartiallyFilled);
    }
    #[test]
    fn cancelled_before_arrival_is_not_a_fill() {
        let mut x = o(LiquidityRole::Taker);
        x.cancelled_at_ns = Some(25);
        let report = simulate(&x, &[], t(&[]), &FixtureFee).unwrap();
        assert_eq!(report.state, OrderState::Cancelled);
        assert_eq!(report.filled_hundredths, 0);
    }
    #[test]
    fn source_depth_cannot_be_unsorted_or_crossed() {
        let mut book = b();
        book.no_bids.swap(0, 1);
        assert_eq!(book.validate(), Err(Error::InvalidBook));
        let mut book = b();
        book.yes_bids[0].price = p(6000);
        assert_eq!(book.validate(), Err(Error::InvalidBook));
    }
    #[test]
    fn nonmarketable_taker_has_no_phantom_liquidity() {
        let mut intent = o(LiquidityRole::Taker);
        intent.limit_price = p(2000);
        let r = simulate(&intent, &[b()], t(&[]), &FixtureFee).unwrap();
        assert_eq!(r.state, OrderState::Unfilled);
        assert!(r.modeled_fills.is_empty());
        assert_eq!(r.modeled_fees_centicents, 0);
    }
    #[test]
    fn maker_unfilled_order_expires_without_a_fill() {
        let mut intent = o(LiquidityRole::Maker);
        intent.limit_price = p(4000);
        let r = simulate(&intent, &[b()], t(&[]), &FixtureFee).unwrap();
        assert_eq!(r.state, OrderState::Expired);
        assert_eq!(r.filled_hundredths, 0);
    }
    #[test]
    fn future_signal_is_rejected_before_book_access() {
        let mut intent = o(LiquidityRole::Taker);
        intent.signal_received_ns = 21;
        assert_eq!(
            simulate(&intent, &[b()], t(&[]), &FixtureFee).unwrap_err(),
            Error::FutureSignal
        );
    }
    #[test]
    fn strict_window_requires_session_and_complete_channel_and_tape() {
        let order = o(LiquidityRole::Maker);
        let proof = SourceWindowEvidence {
            market: "BTC".into(),
            session_id: "session".into(),
            manifest_sha256: "b".repeat(64),
            book_sha256: "a".repeat(64),
            tape_sha256: "c".repeat(64),
            first_received_ns: 1,
            last_received_ns: 100,
            tape_first_received_ns: 20,
            tape_last_received_ns: 100,
            channel_continuity_verified: true,
            snapshot_origin_verified: true,
            tape_coverage_verified: false,
            missing_channel_sequences: 0,
            duplicate_or_regressing_sequences: 0,
        };
        assert_eq!(
            simulate_source_window(&order, &[b()], t(&[]), &proof, &FixtureFee).unwrap_err(),
            Error::IncompleteTape
        );
        let mut inconsistent = proof;
        inconsistent.session_id = "foreign".into();
        assert_eq!(
            simulate_source_window(
                &o(LiquidityRole::Taker),
                &[b()],
                t(&[]),
                &inconsistent,
                &FixtureFee
            )
            .unwrap_err(),
            Error::UnverifiedSource
        );
    }
    #[test]
    fn source_window_rejects_missing_sequence_history() {
        let proof = SourceWindowEvidence {
            market: "BTC".into(),
            session_id: "session".into(),
            manifest_sha256: "b".repeat(64),
            book_sha256: "a".repeat(64),
            tape_sha256: "c".repeat(64),
            first_received_ns: 1,
            last_received_ns: 100,
            tape_first_received_ns: 20,
            tape_last_received_ns: 100,
            channel_continuity_verified: true,
            snapshot_origin_verified: true,
            tape_coverage_verified: true,
            missing_channel_sequences: 1,
            duplicate_or_regressing_sequences: 0,
        };
        assert_eq!(
            simulate_source_window(
                &o(LiquidityRole::Taker),
                &[b()],
                t(&[]),
                &proof,
                &FixtureFee
            )
            .unwrap_err(),
            Error::UnverifiedSource
        );
    }
    #[test]
    fn scenario_list_is_bounded() {
        let orders = vec![o(LiquidityRole::Taker); 17];
        assert_eq!(
            simulate_scenarios(&orders, &[b()], t(&[]), &FixtureFee).unwrap_err(),
            Error::Capacity
        );
    }
}

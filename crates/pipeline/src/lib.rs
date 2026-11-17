//! Deterministic offline market-case evaluation. Never send exchange orders.
//! Every numerical result is for a MODELED fill under supplied data assumptions.
use phoenix_domain::{
    BinaryFinal, Book, EvidenceTier, ModeledBuyFill, Money, SequenceStatus, SequenceTracker,
};
use phoenix_research::{PaperLedger, Split, SplitPlan};
use phoenix_sim::{taker_buy, OrderIntent};
use phoenix_strategies::{evaluate, Decision, FrozenSpec, ModelSignal};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    UnsealedSource,
    InvalidIdentity,
    InvalidBook,
    SequenceGap,
    NoSplit,
    InvalidStrategy,
    InvalidExecution,
    InsufficientCapital,
    SettlementMismatch,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Abstained,
    NoFill,
    PendingSettlement,
    ModeledSettled,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evaluation {
    pub event_id: String,
    pub market: String,
    pub strategy_id: String,
    pub split: Split,
    pub stage: Stage,
    pub modeled_quantity: u64,
    pub fees_centicents: Money,
    pub net_pnl_centicents: Option<Money>,
    pub tier: Option<EvidenceTier>,
    pub reason: String,
}
pub struct Case {
    pub event_id: String,
    pub market: String,
    pub source_sha256: String,
    pub source_sealed: bool,
    pub source_sequence_complete: bool,
    pub books: Vec<Book>,
    pub predictions: Vec<ModelSignal>,
    pub strategy: FrozenSpec,
    pub split_plan: SplitPlan,
    pub feature_start_ns: i64,
    pub outcome_end_ns: i64,
    pub decision_ns: i64,
    pub modeled_arrival_ns: i64,
    pub quantity: u64,
    pub cash_centicents: Money,
    pub rules_sha256: String,
    pub finalized: Option<BinaryFinal>,
}
fn hex64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|x| x.is_ascii_hexdigit())
}
pub fn evaluate_case(c: &Case) -> Result<Evaluation, Error> {
    if !c.source_sealed || !c.source_sequence_complete || !hex64(&c.source_sha256) {
        return Err(Error::UnsealedSource);
    }
    if c.market.is_empty() || c.event_id.is_empty() || !hex64(&c.rules_sha256) {
        return Err(Error::InvalidIdentity);
    }
    let mut tracker = SequenceTracker::default();
    for b in &c.books {
        if b.market != c.market || b.validate().is_err() {
            return Err(Error::InvalidBook);
        }
        match tracker.observe(&c.market, &b.session_id, b.sequence) {
            SequenceStatus::Gap { .. } | SequenceStatus::DuplicateOrReordered => {
                return Err(Error::SequenceGap)
            }
            SequenceStatus::First | SequenceStatus::Continuous => {}
        }
    }
    let split = c.split_plan.classify(c.feature_start_ns, c.outcome_end_ns);
    if split == Split::Embargo {
        return Err(Error::NoSplit);
    }
    let decision_book = c
        .books
        .iter()
        .filter(|b| b.received_ns <= c.decision_ns)
        .max_by_key(|b| b.received_ns)
        .ok_or(Error::InvalidBook)?;
    let decision = evaluate(&c.strategy, c.decision_ns, decision_book, &c.predictions)
        .map_err(|_| Error::InvalidStrategy)?;
    let base = |stage: Stage,
                q: u64,
                fees: Money,
                net: Option<Money>,
                tier: Option<EvidenceTier>,
                reason: &str| Evaluation {
        event_id: c.event_id.clone(),
        market: c.market.clone(),
        strategy_id: c.strategy.id.clone(),
        split,
        stage,
        modeled_quantity: q,
        fees_centicents: fees,
        net_pnl_centicents: net,
        tier,
        reason: reason.into(),
    };
    let choice = match decision {
        Decision::Abstain(reason) => {
            return Ok(base(
                Stage::Abstained,
                0,
                0,
                None,
                None,
                &format!("{reason:?}"),
            ))
        }
        Decision::Trade(choice) => choice,
    };
    let order = OrderIntent {
        market: c.market.clone(),
        side: choice.side,
        quantity: c.quantity,
        buy_limit: choice.limit,
        signal_received_ns: decision_book.received_ns,
        decision_ns: c.decision_ns,
        arrival_ns: c.modeled_arrival_ns,
        max_age_ns: c.strategy.max_quote_age_ns,
        fee_rate: c.strategy.fee_rate,
    };
    let exec = taker_buy(&order, &c.books).map_err(|_| Error::InvalidExecution)?;
    let total_quantity = exec.requested - exec.unfilled;
    if total_quantity == 0 {
        return Ok(base(Stage::NoFill, 0, 0, None, None, "No executable depth"));
    }
    let mut ledger = PaperLedger::new(c.cash_centicents).map_err(|_| Error::InsufficientCapital)?;
    for (i, fill) in exec.fills.iter().enumerate() {
        ledger
            .post_modeled_buy(
                format!("{}-{i}", c.market),
                ModeledBuyFill {
                    market: c.market.clone(),
                    rules_sha256: c.rules_sha256.clone(),
                    side: choice.side,
                    price: fill.price,
                    quantity: fill.quantity,
                    entry_fee: fill.fee,
                },
            )
            .map_err(|_| Error::InsufficientCapital)?;
    }
    let final_result = match &c.finalized {
        None => {
            return Ok(base(
                Stage::PendingSettlement,
                total_quantity,
                exec.fees,
                None,
                None,
                "No finalized settlement matched",
            ))
        }
        Some(s) => s,
    };
    if final_result.market != c.market || final_result.rules_sha256 != c.rules_sha256 {
        return Err(Error::SettlementMismatch);
    }
    let tier = final_result.tier().map_err(|_| Error::SettlementMismatch)?;
    for i in 0..exec.fills.len() {
        ledger
            .settle(&format!("{}-{i}", c.market), final_result)
            .map_err(|_| Error::SettlementMismatch)?;
    }
    Ok(base(
        Stage::ModeledSettled,
        total_quantity,
        exec.fees,
        Some(ledger.realized_net()),
        Some(tier),
        "Model-only settlement of replayed fills",
    ))
}
#[cfg(test)]
mod tests {
    use super::*;
    use phoenix_domain::{Evidence, FeeRate, Level, Price, Side};
    use phoenix_strategies::Recipe;
    fn p(x: u16) -> Price {
        Price::new(x).unwrap()
    }
    fn case() -> Case {
        let book = Book {
            market: "M".into(),
            yes_bids: vec![Level {
                price: p(4000),
                quantity: 4,
            }],
            no_bids: vec![Level {
                price: p(5500),
                quantity: 4,
            }],
            exchange_ns: 120,
            received_ns: 130,
            session_id: "s".into(),
            sequence: 1,
        };
        let strategy = FrozenSpec {
            id: "baseline-v1".into(),
            code_sha256: "a".repeat(64),
            config_sha256: "b".repeat(64),
            recipe: Recipe::ModelEdge,
            min_net_edge_ticks: 200,
            max_quote_age_ns: 100,
            fee_rate: FeeRate::STANDARD_TAKER,
            max_spread_ticks: None,
        };
        Case {
            event_id: "E".into(),
            market: "M".into(),
            source_sha256: "c".repeat(64),
            source_sealed: true,
            source_sequence_complete: true,
            books: vec![book],
            predictions: vec![ModelSignal {
                asof_received_ns: 140,
                probability_yes: p(6500),
                model_weight: 1,
            }],
            strategy,
            split_plan: SplitPlan {
                discovery_end_ns: 100,
                validation_end_ns: 200,
                untouched_start_ns: 250,
                embargo_ns: 10,
            },
            feature_start_ns: 120,
            outcome_end_ns: 190,
            decision_ns: 150,
            modeled_arrival_ns: 155,
            quantity: 1,
            cash_centicents: 100_000,
            rules_sha256: "d".repeat(64),
            finalized: Some(BinaryFinal {
                market: "M".into(),
                rules_sha256: "d".repeat(64),
                winner: Side::Yes,
                venue_final: Evidence {
                    source_id: "synthetic".into(),
                    content_sha256: "venue".into(),
                },
                independent: None,
            }),
        }
    }
    #[test]
    fn synthetic_complete_model_reconciles() {
        let o = evaluate_case(&case()).unwrap();
        assert_eq!(o.stage, Stage::ModeledSettled);
        assert_eq!(o.split, Split::Validation);
        assert_eq!(o.modeled_quantity, 1);
        assert_eq!(o.net_pnl_centicents, Some(5_326));
        assert_eq!(o.tier, Some(EvidenceTier::VenueFinal));
    }
    #[test]
    fn missing_settlement_never_becomes_zero_pnl() {
        let mut c = case();
        c.finalized = None;
        let o = evaluate_case(&c).unwrap();
        assert_eq!(o.stage, Stage::PendingSettlement);
        assert_eq!(o.net_pnl_centicents, None);
    }
    #[test]
    fn gapped_archive_is_not_tradable() {
        let mut c = case();
        let mut gap = c.books[0].clone();
        gap.sequence = 3;
        gap.received_ns += 1;
        c.books.push(gap);
        assert_eq!(evaluate_case(&c), Err(Error::SequenceGap));
    }
    #[test]
    fn unsealed_archive_rejected() {
        let mut c = case();
        c.source_sealed = false;
        assert_eq!(evaluate_case(&c), Err(Error::UnsealedSource));
    }
    #[test]
    fn no_trade_and_no_fill_are_distinct() {
        let mut c = case();
        c.strategy.recipe = Recipe::NoTrade;
        assert_eq!(evaluate_case(&c).unwrap().stage, Stage::Abstained);
        let mut c = case();
        c.books[0].no_bids.clear();
        assert_eq!(evaluate_case(&c).unwrap().stage, Stage::Abstained);
    }
}

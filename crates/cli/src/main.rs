//! Synthetic-only offline smoke demonstration; never contacts an exchange.
use phoenix_domain::{BinaryFinal, Book, Evidence, FeeRate, Level, ModeledBuyFill, Price, Side};
use phoenix_research::{choose_trade, PaperLedger, SelectMode};
use phoenix_sim::{taker_buy, OrderIntent};
fn main() {
    let p = |v| Price::new(v).expect("valid fixture");
    let b = Book {
        market: "SYNTHETIC".into(),
        yes_bids: vec![Level {
            price: p(4000),
            quantity: 3,
        }],
        no_bids: vec![Level {
            price: p(5500),
            quantity: 3,
        }],
        exchange_ns: 5,
        received_ns: 10,
        session_id: "synthetic".into(),
        sequence: 1,
    };
    let c = choose_trade(
        p(6500),
        b.best_ask(Side::Yes),
        b.best_ask(Side::No),
        FeeRate::STANDARD_TAKER,
        200,
        SelectMode::FairValue,
    )
    .unwrap();
    let o = OrderIntent {
        market: b.market.clone(),
        side: c.side,
        quantity: 1,
        buy_limit: c.limit,
        signal_received_ns: 10,
        decision_ns: 20,
        arrival_ns: 30,
        max_age_ns: 100,
        fee_rate: FeeRate::STANDARD_TAKER,
    };
    let r = taker_buy(&o, &[b]).unwrap();
    let f = &r.fills[0];
    let mut l = PaperLedger::new(1_000_000).unwrap();
    l.post_modeled_buy(
        "demo".into(),
        ModeledBuyFill {
            market: "SYNTHETIC".into(),
            rules_sha256: "synthetic-rule".into(),
            side: c.side,
            price: f.price,
            quantity: f.quantity,
            entry_fee: f.fee,
        },
    )
    .unwrap();
    let settlement = BinaryFinal {
        market: "SYNTHETIC".into(),
        rules_sha256: "synthetic-rule".into(),
        winner: Side::Yes,
        venue_final: Evidence {
            source_id: "fixture".into(),
            content_sha256: "fixture-hash".into(),
        },
        independent: None,
    };
    let result = l.settle("demo", &settlement).unwrap();
    println!(
        "SYNTHETIC ONLY, modeled net P&L in 1/10000 USD: {}",
        result.pnl.net
    );
}

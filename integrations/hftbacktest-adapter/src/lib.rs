//! OPT-IN, backtest-only adapter for nkaz001/hftbacktest, pinned at a source SHA.
//! Kalshi complementary binary YES/NO BIDS are handled as independent resting
//! bid queues: never pretend they are conventional exchange asks.
//! Never use upstream f64 book/fee accounting for Phoenix financial P&L.
#[cfg(feature = "upstream-hft")]
mod enabled {
    use hftbacktest::{
        backtest::models::{ConstantLatency, LatencyModel, QueueModel, RiskAdverseQueueModel},
        depth::{HashMapMarketDepth, L2MarketDepth, MarketDepth},
        types::{OrdType, Order, Side as HftSide, TimeInForce},
    };
    use phoenix_domain::{Book, Price, Side};
    use phoenix_sim::{maker_buy, ConsumingTrade, MakerResult, OrderIntent};

    #[derive(Debug, PartialEq, Eq)]
    pub enum Error { InvalidBook, InvalidQuantity, PriceRoundTrip, NoRestingLevel, Replay }
    pub struct QueueComparison {
        pub hft_modeled: u64, pub phoenix_conservative: u64,
        pub hft_queue_ahead: u64, pub phoenix_queue_ahead: u64,
        /// MUST be true before treating any hft prediction as even a research
        /// sensitivity. Divergence is not resolved by selecting the higher fill.
        pub exact_parity: bool,
    }
    /// HftBacktest uses float price and quantity. Preserve the canonical
    /// integer price as source of truth and verify numeric roundtrips.
    fn hft_bid_queue(book:&Book,side:Side,level:Price)
        -> Result<(HashMapMarketDepth,u64),Error>
    {
        book.validate().map_err(|_|Error::InvalidBook)?;
        let at=match side { Side::Yes=>&book.yes_bids,Side::No=>&book.no_bids };
        let quantity=at.iter().find(|l|l.price==level)
            .map(|l|l.quantity).ok_or(Error::NoRestingLevel)?;
        if quantity>(1u64<<53){return Err(Error::InvalidQuantity);}
        let px=f64::from(level.ticks())/10_000.0;
        let mut md=HashMapMarketDepth::new(0.0001,1.0);
        md.update_bid_depth(px,quantity as f64,book.received_ns);
        if md.best_bid_tick()!=i64::from(level.ticks()) ||
           md.bid_qty_at_tick(md.best_bid_tick())!=quantity as f64{
            return Err(Error::PriceRoundTrip);
        }
        Ok((md,quantity))
    }
    /// Same snapshot, arrival and explicit *consuming* prints as Phoenix.
    /// HftBacktest's risk-adverse queue may use a different partial-fill
    /// convention. Until equality holds, report both and **NO automatic fill**.
    pub fn compare_risk_adverse_queue(
        request:&OrderIntent,book:&Book,prints:&[ConsumingTrade],
        deadline_ns:i64
    )->Result<QueueComparison,Error>{
        let (depth,ahead)=hft_bid_queue(book,request.side,request.buy_limit)?;
        let mut order=Order::new(
            1,i64::from(request.buy_limit.ticks()),0.0001,
            request.quantity as f64,HftSide::Buy,OrdType::Limit,TimeInForce::GTX);
        let model=RiskAdverseQueueModel::<HashMapMarketDepth>::new();
        model.new_order(&mut order,&depth);
        let mut matched=0u64;
        let mut sorted=prints.iter().collect::<Vec<_>>();
        sorted.sort_by_key(|p|p.received_ns);
        for p in sorted {
            if p.market!=request.market || p.resting_side!=request.side ||
                p.price!=request.buy_limit || p.received_ns<=request.arrival_ns||
                p.received_ns>deadline_ns {continue;}
            if p.quantity>(1u64<<53){return Err(Error::InvalidQuantity);}
            model.trade(&mut order,p.quantity as f64,&depth);
            let q=model.is_filled(&mut order,&depth);
            if !q.is_finite()||q<0.0{return Err(Error::Replay);}
            // The upstream model returns estimated matched lots, not an
            // authorization to fill beyond the original order quantity.
            let remaining=request.quantity-matched;
            let n=(q as u64).min(remaining);
            matched+=n;
            if matched==request.quantity{break;}
        }
        let phx:MakerResult=maker_buy(request,std::slice::from_ref(book),
            prints,deadline_ns,true).map_err(|_|Error::Replay)?;
        Ok(QueueComparison{
            hft_modeled:matched,phoenix_conservative:phx.matched,
            hft_queue_ahead:ahead,phoenix_queue_ahead:phx.queue_ahead,
            exact_parity:matched==phx.matched && ahead==phx.queue_ahead,
        })
    }
    /// Whole-contract comparison ONLY. Fractional quantities and levels are
    /// unsupported by this exact adapter and must never be rounded to lots.
    /// This is a sensitivity comparison, not empirically proven maker fills.
    pub fn compare_fractional_when_lossless(
        request:&phoenix_sim::historical::LimitIntent,
        book:&phoenix_sim::historical::FractionalBook,
        prints:&[phoenix_sim::historical::ConsumingPrint],
        deadline_ns:i64,
    )->Result<QueueComparison,Error>{
        use phoenix_domain::{Level,FeeRate};
        use phoenix_sim::historical::{LiquidityRole,DepthQuality};
        if request.role!=LiquidityRole::Maker||
            request.market!=book.market||
            book.quality!=DepthQuality::VerifiedSourceWindow||
            request.maker_queue_ahead_extra_hundredths!=0||
            request.quantity_hundredths%100!=0||
            request.quantity_hundredths/100>(1u64<<53)||
            book.yes_bids.iter().chain(book.no_bids.iter())
                .any(|l|l.quantity_hundredths%100!=0||
                    l.quantity_hundredths/100>(1u64<<53))||
            prints.iter().any(|p|p.quantity_hundredths%100!=0||
                p.quantity_hundredths/100>(1u64<<53)){
            return Err(Error::InvalidQuantity);
        }
        let levels=|src:&[phoenix_sim::historical::FractionalLevel]|
            ->Vec<Level>{src.iter().map(|l|Level{
                price:l.price,quantity:l.quantity_hundredths/100}).collect()};
        let legacy_book=Book{market:book.market.clone(),
            yes_bids:levels(&book.yes_bids),no_bids:levels(&book.no_bids),
            exchange_ns:book.exchange_ns,received_ns:book.received_ns,
            session_id:book.session_id.clone(),sequence:0};
        legacy_book.validate().map_err(|_|Error::InvalidBook)?;
        let legacy_order=OrderIntent{
            market:request.market.clone(),side:request.side,
            quantity:request.quantity_hundredths/100,
            buy_limit:request.limit_price,
            signal_received_ns:request.signal_received_ns,
            decision_ns:request.decision_ns,arrival_ns:request.arrival_ns,
            max_age_ns:request.max_quote_age_ns,
            // Queue-only sensitivity; upstream fee/P&L must not be used.
            fee_rate:FeeRate::ZERO,
        };
        let legacy_prints=prints.iter().map(|p|ConsumingTrade{
            market:p.market.clone(),resting_side:p.resting_side,
            price:p.price,quantity:p.quantity_hundredths/100,
            received_ns:p.received_ns}).collect::<Vec<_>>();
        compare_risk_adverse_queue(&legacy_order,&legacy_book,&legacy_prints,
            deadline_ns)
    }
    /// Upstream latency model is only an *assumption*, not an observed Kalshi RTT.
    pub fn model_constant_latency_ns(entry:i64,response:i64)->Result<(i64,i64),Error>{
        if entry<0||response<0{return Err(Error::Replay);}
        let mut hft=ConstantLatency::new(entry,response);
        let o=Order::new(1,5000,0.0001,1.0,HftSide::Buy,OrdType::Limit,TimeInForce::GTX);
        Ok((hft.entry(100,&o),hft.response(100,&o)))
    }
}
#[cfg(feature="upstream-hft")]
pub use enabled::*;

#[cfg(all(test,feature="upstream-hft"))]
mod tests {
    use super::*;
    use phoenix_domain::{Book,FeeRate,Level,Price,Side};
    use phoenix_sim::{OrderIntent,ConsumingTrade};
    fn p(n:u16)->Price {Price::new(n).unwrap()}
    fn book()->Book {
        Book{market:"KX-BINARY".into(),
            yes_bids:vec![Level{price:p(4000),quantity:5}],
            no_bids:vec![Level{price:p(5500),quantity:3}],
            exchange_ns:10,received_ns:15,session_id:"synthetic".into(),
            sequence:1}
    }
    fn req()->OrderIntent {
        OrderIntent{market:"KX-BINARY".into(),side:Side::Yes,
            quantity:2,buy_limit:p(4000),signal_received_ns:10,
            decision_ns:20,arrival_ns:25,max_age_ns:30,fee_rate:FeeRate::ZERO}
    }
    fn prints()->Vec<ConsumingTrade>{
        vec![4,2,2].into_iter().enumerate().map(|(i,q)|ConsumingTrade{
            market:"KX-BINARY".into(),resting_side:Side::Yes,price:p(4000),
            quantity:q,received_ns:30+10*i as i64}).collect()
    }
    #[test]
    fn queue_agreement_with_identical_synthetic_consuming_prints(){
        let b=book();let order=req();let ticks=prints();
        for (deadline,expected) in [(35,0),(45,1),(55,2)]{
            let c=compare_risk_adverse_queue(&order,&b,&ticks,deadline).unwrap();
            assert_eq!(c.hft_queue_ahead,5);
            assert_eq!(c.phoenix_queue_ahead,5);
            assert_eq!(c.phoenix_conservative,expected);
            assert_eq!(c.hft_modeled,expected);
            assert!(c.exact_parity);
        }
    }
    #[test]
    fn no_consuming_prints_means_zero_maker_fills(){
        let c=compare_risk_adverse_queue(&req(),&book(),&[],55).unwrap();
        assert_eq!((c.hft_modeled,c.phoenix_conservative),(0,0));
    }
    #[test]
    fn upstream_latency_is_assumption_and_never_negative(){
        assert_eq!(model_constant_latency_ns(40,25).unwrap(),(40,25));
        assert!(model_constant_latency_ns(-1,25).is_err());
    }
    #[test]
    fn fractional_quantity_cannot_be_rounded_to_whole_lots(){
        use phoenix_sim::historical::{
            LimitIntent,FractionalBook,FractionalLevel,DepthQuality,LiquidityRole,
        };
        let b=FractionalBook{market:"KX-BINARY".into(),
            received_ns:15,exchange_ns:10,session_id:"synthetic".into(),
            snapshot_sha256:"a".repeat(64),
            quality:DepthQuality::VerifiedSourceWindow,
            yes_bids:vec![FractionalLevel{price:p(4000),quantity_hundredths:500}],
            no_bids:vec![FractionalLevel{price:p(5500),quantity_hundredths:300}]};
        let order=LimitIntent{market:"KX-BINARY".into(),
            strategy_order_id:"x".into(),side:Side::Yes,
            role:LiquidityRole::Maker,quantity_hundredths:125,
            limit_price:p(4000),signal_received_ns:10,
            decision_ns:20,arrival_ns:25,deadline_ns:60,
            cancelled_at_ns:None,max_quote_age_ns:30,
            maker_queue_ahead_extra_hundredths:0};
        assert!(compare_fractional_when_lossless(&order,&b,&[],60).is_err());
    }

}

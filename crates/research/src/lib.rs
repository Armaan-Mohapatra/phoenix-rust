//! Offline strategy selection, split protection, and append-only paper accounting.
//! Historical and simulated results must never be mistaken for actual fills.
use std::collections::{HashMap,HashSet};
use phoenix_domain::{BinaryFinal,Error as DomainError,FeeRate,Money,ModeledBuyFill,Price,SettledPnl,Side,settle_binary,ONE_DOLLAR};

#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum Split { Discovery, Validation, Embargo, Untouched }
#[derive(Debug,Clone,Copy)]
pub struct SplitPlan {
    pub discovery_end_ns:i64, pub validation_end_ns:i64,
    pub untouched_start_ns:i64, pub embargo_ns:i64,
}
impl SplitPlan {
    pub fn classify(&self, feature_start_ns:i64, outcome_end_ns:i64)->Split {
        if feature_start_ns>outcome_end_ns || self.embargo_ns<0 ||
           self.discovery_end_ns>=self.validation_end_ns ||
           self.validation_end_ns>=self.untouched_start_ns ||
           feature_start_ns<0 {return Split::Embargo;}
        if outcome_end_ns<=self.discovery_end_ns {return Split::Discovery;}
        if feature_start_ns>=self.discovery_end_ns.saturating_add(self.embargo_ns) &&
           outcome_end_ns<=self.validation_end_ns {return Split::Validation;}
        if feature_start_ns>=self.untouched_start_ns &&
            outcome_end_ns>=self.untouched_start_ns {return Split::Untouched;}
        Split::Embargo
    }
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub struct TradeChoice {pub side:Side,pub limit:Price,pub predicted_edge_ticks:i64}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum SelectMode {FairValue,InvertScore}
/// Choose based on executable ask, not midpoint. InvertScore is an explicitly
/// labeled *research variant*, not a blanket reversal of losing P&L.
pub fn choose_trade(q_yes:Price,yes_ask:Option<Price>,no_ask:Option<Price>,
    fee:FeeRate,min_net_edge_ticks:i64,mode:SelectMode)->Option<TradeChoice> {
    let q= match mode {SelectMode::FairValue=>i64::from(q_yes.ticks()),
        SelectMode::InvertScore=>i64::from(ONE_DOLLAR-q_yes.ticks())};
    let variants=[(Side::Yes,yes_ask,q),(Side::No,no_ask,i64::from(ONE_DOLLAR)-q)];
    variants.into_iter().filter_map(|(side,ask,fair)|{
        let price=ask?;
        // Single-contract fee in $0.0001 equals price ticks.
        let charge=fee.charge(1,price).ok()?;
        let edge=fair-i64::from(price.ticks())-i64::try_from(charge).ok()?;
        if edge>=min_net_edge_ticks {
            Some(TradeChoice{side,limit:price,predicted_edge_ticks:edge})
        } else {None}
    }).max_by_key(|c| c.predicted_edge_ticks)
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum LedgerError {
    Domain(DomainError),DuplicateOrder,UnknownOrder,AlreadySettled,
    InsufficientFunds,InvalidFee,Overflow,
}
impl From<DomainError> for LedgerError {fn from(e:DomainError)->Self{Self::Domain(e)}}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct PaperPosition {pub fill:ModeledBuyFill,pub total_paid:Money}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct SettledPaper {
    pub order_id:String,pub pnl:SettledPnl,pub payout:Money,
}
#[derive(Default)]
pub struct PaperLedger {
    cash:Money,
    orders:HashMap<String,PaperPosition>,
    settled:HashSet<String>,
    realized_net:Money,
}
impl PaperLedger {
    pub fn new(starting_cash_centicents:Money)->Result<Self,LedgerError>{
        if starting_cash_centicents<0 {return Err(LedgerError::InsufficientFunds);}
        Ok(Self{cash:starting_cash_centicents,..Self::default()})
    }
    pub fn available_cash(&self)->Money {self.cash}
    pub fn realized_net(&self)->Money {self.realized_net}
    pub fn unsettled_orders(&self)->usize {self.orders.len()-self.settled.len()}
    pub fn post_modeled_buy(&mut self,id:String,fill:ModeledBuyFill)->Result<(),LedgerError>{
        if self.orders.contains_key(&id) {return Err(LedgerError::DuplicateOrder);}
        if fill.entry_fee<0 {return Err(LedgerError::InvalidFee);}
        if fill.quantity==0 {return Err(LedgerError::Domain(DomainError::ZeroQuantity));}
        let stake=Money::from(fill.quantity).checked_mul(Money::from(fill.price.ticks()))
            .ok_or(LedgerError::Overflow)?;
        let paid=stake.checked_add(fill.entry_fee).ok_or(LedgerError::Overflow)?;
        if paid>self.cash {return Err(LedgerError::InsufficientFunds);}
        self.cash-=paid;
        self.orders.insert(id,PaperPosition{fill,total_paid:paid});Ok(())
    }
    pub fn settle(&mut self,id:&str,final_result:&BinaryFinal)
        ->Result<SettledPaper,LedgerError> {
        if self.settled.contains(id) {return Err(LedgerError::AlreadySettled);}
        let record=self.orders.get(id).ok_or(LedgerError::UnknownOrder)?;
        let pnl=settle_binary(&record.fill,final_result)?;
        let payout=record.total_paid.checked_add(pnl.net).ok_or(LedgerError::Overflow)?;
        let next_cash=self.cash.checked_add(payout).ok_or(LedgerError::Overflow)?;
        let next_realized=self.realized_net.checked_add(pnl.net).ok_or(LedgerError::Overflow)?;
        self.cash=next_cash;self.realized_net=next_realized;
        self.settled.insert(id.to_owned());
        Ok(SettledPaper{order_id:id.to_owned(),pnl,payout})
    }
}
/// Distinct independent event clusters, not number of correlated contracts.
pub fn event_cluster_sum(rows:&[(String,Money)])->Vec<(String,Money)>{
    let mut groups=std::collections::BTreeMap::<String,Money>::new();
    for (event,pnl) in rows { *groups.entry(event.clone()).or_default()+=*pnl; }
    groups.into_iter().collect()
}
/// Brier score as millionths (rounded down); use as a diagnostic only.
pub fn brier_millionths(predictions:&[(Price,bool)])->Option<u64>{
    if predictions.is_empty(){return None;}
    let squared:u128=predictions.iter().map(|(p,y)|{
        let target=if *y {i128::from(ONE_DOLLAR)}else{0};
        let e=target-i128::from(p.ticks());
        (e*e) as u128
    }).sum();
    let scale=u128::from(ONE_DOLLAR)*u128::from(ONE_DOLLAR);
    u64::try_from(squared.checked_mul(1_000_000)? /
        (predictions.len() as u128).checked_mul(scale)?).ok()
}
#[cfg(test)]
mod tests {
    use super::*;use phoenix_domain::{Evidence,EvidenceTier};
    fn p(x:u16)->Price {Price::new(x).unwrap()}
    fn outcome()->BinaryFinal{BinaryFinal{market:"BTC".into(),rules_sha256:"rulehash".into(),
        winner:Side::Yes,venue_final:Evidence{source_id:"kalshi".into(),
        content_sha256:"venuehash".into()},independent:None}}
    #[test] fn split_protects_crossing_events(){
        let s=SplitPlan{discovery_end_ns:100,validation_end_ns:200,
            untouched_start_ns:250,embargo_ns:10};
        assert_eq!(s.classify(10,90),Split::Discovery);
        assert_eq!(s.classify(90,120),Split::Embargo);
        assert_eq!(s.classify(120,190),Split::Validation);
        assert_eq!(s.classify(190,260),Split::Embargo);
        assert_eq!(s.classify(251,275),Split::Untouched);
    }
    #[test] fn cost_gate_and_inverse_differ(){
        let fair=choose_trade(p(6500),Some(p(6000)),Some(p(4300)),
            FeeRate::STANDARD_TAKER,200,SelectMode::FairValue).unwrap();
        assert_eq!(fair.side,Side::Yes);
        let inverse=choose_trade(p(6500),Some(p(6000)),Some(p(3000)),
            FeeRate::ZERO,0,SelectMode::InvertScore).unwrap();
        assert_eq!(inverse.side,Side::No);
    }
    #[test] fn ledger_is_idempotent_and_pnl_conserving(){
        let mut ledger=PaperLedger::new(100_000).unwrap(); // $10
        let fill=ModeledBuyFill{market:"BTC".into(),rules_sha256:"rulehash".into(),
            side:Side::Yes,price:p(5500),quantity:1,entry_fee:174};
        ledger.post_modeled_buy("o1".into(),fill.clone()).unwrap();
        assert_eq!(ledger.available_cash(),100_000-5_674);
        assert_eq!(ledger.post_modeled_buy("o1".into(),fill),Err(LedgerError::DuplicateOrder));
        let result=ledger.settle("o1",&outcome()).unwrap();
        assert_eq!(result.payout,10_000);
        assert_eq!(result.pnl.net,4_326);
        assert_eq!(result.pnl.tier,EvidenceTier::VenueFinal);
        assert_eq!(ledger.available_cash(),104_326);
        assert_eq!(ledger.realized_net(),4_326);
        assert_eq!(ledger.settle("o1",&outcome()),Err(LedgerError::AlreadySettled));
    }
    #[test] fn no_settlement_no_cash_release(){
        let mut l=PaperLedger::new(20_000).unwrap();
        let f=ModeledBuyFill{market:"BTC".into(),rules_sha256:"rulehash".into(),
            side:Side::No,price:p(5000),quantity:1,entry_fee:0};
        l.post_modeled_buy("o".into(),f).unwrap();
        let mut o=outcome();o.rules_sha256="different".into();
        assert_eq!(l.settle("o",&o),Err(LedgerError::Domain(DomainError::RuleMismatch)));
        assert_eq!(l.available_cash(),15_000);
    }
    #[test] fn cluster_outcomes_not_trades(){
        let c=event_cluster_sum(&[("a".into(),30),("a".into(),-10),("b".into(),7)]);
        assert_eq!(c,vec![("a".into(),20),("b".into(),7)]);
    }
    #[test] fn brier_is_fractional_not_percent(){
        assert_eq!(brier_millionths(&[(p(5000),true),(p(5000),false)]),Some(250_000));
    }
}

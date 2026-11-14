//! Research-only, deterministic, fixed-point preflight risk gate.
//! Independently implemented from public MIT Tarpboi risk-guard concepts.
//! No broker, networking, clock reads, credentials, order APIs, or f64 money.
use std::collections::{BTreeSet,HashMap,VecDeque};

pub type Money=i128; // $0.0001
const MAX_IDENTITIES:usize=4_096;
const MAX_MARKETS:usize=256;

#[derive(Clone,Copy,Debug)]
pub struct Limits {
    pub max_single_order:Money,
    pub max_market_exposure:Money,
    pub max_portfolio_exposure:Money,
    pub min_quote_depth:Money,
    pub large_order_threshold:Money,
    pub large_burst_count:usize,
    pub lookback_ns:i64,
    pub cooldown_ns:i64,
}
impl Limits {
    pub fn validate(&self)->bool {
        self.max_single_order>0&&self.max_market_exposure>0&&
        self.max_portfolio_exposure>0&&
        self.max_single_order<=self.max_market_exposure&&
        self.max_market_exposure<=self.max_portfolio_exposure&&
        self.min_quote_depth>=0&&self.large_order_threshold>0&&
        self.large_burst_count>=2&&self.large_burst_count<=32&&
        self.lookback_ns>0&&self.cooldown_ns>0
    }
}
#[derive(Clone,Debug)]
pub struct Intent<'a>{
    pub id:&'a str,pub market:&'a str,pub at_ns:i64,
    pub proposed_notional:Money,
    pub current_market_exposure:Money,
    pub current_portfolio_exposure:Money,
    pub quote_received_ns:i64,
    pub max_quote_age_ns:i64,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum Block {
    InvalidConfiguration,InvalidInput,NonmonotonicTime,DuplicateIntent,
    CapacityExceeded,StaleOrFutureQuote,SingleOrderLimit,
    MarketExposureLimit,PortfolioExposureLimit,LargeBurstTrip,
    CooldownActive,InsufficientDepth,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum Decision {Allow,RequireDepth,Block(Block)}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum AccountingUnit {LegacyTenThousandths,MicroUsd}
#[derive(Clone)]
pub struct Guard{
    limits:Limits,
    accounting_unit:AccountingUnit,
    recent:HashMap<String,VecDeque<i64>>,
    identities:BTreeSet<String>,
    last_ns:Option<i64>,tripped_until:Option<i64>,
}
impl Guard {
    pub fn new(limits:Limits)->Result<Self,Block>{
        if !limits.validate(){return Err(Block::InvalidConfiguration);}
        Ok(Self{limits,accounting_unit:AccountingUnit::LegacyTenThousandths,
            recent:HashMap::new(),identities:BTreeSet::new(),
            last_ns:None,tripped_until:None})
    }
    /// Construct a new isolated guard with ALL monetary limits promoted
    /// losslessly from legacy 1e-4 USD to microUSD (1e-6 USD). The Intent
    /// passed to this guard must be denominated in microUSD.
    pub fn new_micro(mut limits:Limits)->Result<Self,Block>{
        limits.max_single_order=limits.max_single_order.checked_mul(100)
            .ok_or(Block::InvalidConfiguration)?;
        limits.max_market_exposure=limits.max_market_exposure.checked_mul(100)
            .ok_or(Block::InvalidConfiguration)?;
        limits.max_portfolio_exposure=limits.max_portfolio_exposure.checked_mul(100)
            .ok_or(Block::InvalidConfiguration)?;
        limits.min_quote_depth=limits.min_quote_depth.checked_mul(100)
            .ok_or(Block::InvalidConfiguration)?;
        limits.large_order_threshold=limits.large_order_threshold.checked_mul(100)
            .ok_or(Block::InvalidConfiguration)?;
        let mut result=Self::new(limits)?;
        result.accounting_unit=AccountingUnit::MicroUsd;
        Ok(result)
    }
    pub fn accounting_unit(&self)->AccountingUnit{self.accounting_unit}
    /// A canonical, explicit configuration identity, preserving monetary
    /// unit. This is a guard-configuration tag, NOT a source hash.
    pub fn configuration_identity(&self)->String{
        format!("{:?}:{}:{}:{}:{}:{}:{}:{}:{}",
            self.accounting_unit,self.limits.max_single_order,
            self.limits.max_market_exposure,self.limits.max_portfolio_exposure,
            self.limits.min_quote_depth,self.limits.large_order_threshold,
            self.limits.large_burst_count,self.limits.lookback_ns,
            self.limits.cooldown_ns)
    }
    /// Restart from the identical limit/unit contract but *without*
    /// in-memory decisions. The caller must replay the durable risk audit
    /// before using this guard to authorize a new hypothetical order.
    pub fn empty_same_configuration(&self)->Self{
        Self{limits:self.limits,accounting_unit:self.accounting_unit,
            recent:HashMap::new(),identities:BTreeSet::new(),
            last_ns:None,tripped_until:None}
    }
    pub fn registered_intents(&self)->usize{self.identities.len()}

    pub fn preflight(&mut self,i:&Intent<'_>)->Decision{
        if i.id.is_empty()||i.id.len()>128||i.market.is_empty()||
            i.market.len()>128||i.at_ns<0||i.proposed_notional<=0||
            i.current_market_exposure<0||i.current_portfolio_exposure<0||
            i.current_market_exposure>i.current_portfolio_exposure||
            i.max_quote_age_ns<0 {
            return Decision::Block(Block::InvalidInput);
        }
        if self.last_ns.is_some_and(|last|i.at_ns<last){
            return Decision::Block(Block::NonmonotonicTime);
        }
        if self.identities.contains(i.id){
            return Decision::Block(Block::DuplicateIntent);
        }
        if self.identities.len()>=MAX_IDENTITIES{
            return Decision::Block(Block::CapacityExceeded);
        }
        if i.quote_received_ns>i.at_ns ||
            i.at_ns.saturating_sub(i.quote_received_ns)>i.max_quote_age_ns{
            return Decision::Block(Block::StaleOrFutureQuote);
        }
        if let Some(until)=self.tripped_until{
            if i.at_ns<until{return Decision::Block(Block::CooldownActive);}
            self.tripped_until=None;
        }
        if i.proposed_notional>self.limits.max_single_order{
            return Decision::Block(Block::SingleOrderLimit);
        }
        if i.current_market_exposure.checked_add(i.proposed_notional)
            .map_or(true,|new|new>self.limits.max_market_exposure){
            return Decision::Block(Block::MarketExposureLimit);
        }
        if i.current_portfolio_exposure.checked_add(i.proposed_notional)
            .map_or(true,|new|new>self.limits.max_portfolio_exposure){
            return Decision::Block(Block::PortfolioExposureLimit);
        }
        if self.recent.len()>=MAX_MARKETS&&!self.recent.contains_key(i.market){
            return Decision::Block(Block::CapacityExceeded);
        }
        self.last_ns=Some(i.at_ns);
        self.identities.insert(i.id.to_owned());
        if i.proposed_notional>=self.limits.large_order_threshold{
            let previous=self.recent.entry(i.market.into()).or_default();
            while previous.front().is_some_and(|old|
                i.at_ns.saturating_sub(*old)>self.limits.lookback_ns){
                previous.pop_front();
            }
            previous.push_back(i.at_ns);
            if previous.len()>=self.limits.large_burst_count {
                self.tripped_until=i.at_ns.checked_add(self.limits.cooldown_ns);
                return Decision::Block(Block::LargeBurstTrip);
            }
            return Decision::RequireDepth;
        }
        Decision::Allow
    }
    /// Only an explicit caller-supplied, as-of-executable-side depth
    /// check is allowed; a stale book or inferred midpoint is not depth proof.
    pub fn check_depth(&self,depth_notional:Money)->Decision{
        if depth_notional<self.limits.min_quote_depth{
            Decision::Block(Block::InsufficientDepth)
        }else{Decision::Allow}
    }
    pub fn tripped_until(&self)->Option<i64>{self.tripped_until}
}
#[cfg(test)]
mod tests{
    use super::*;
    fn cfg()->Limits{Limits{
        max_single_order:50_000,max_market_exposure:100_000,
        max_portfolio_exposure:300_000,min_quote_depth:40_000,
        large_order_threshold:20_000,large_burst_count:2,
        lookback_ns:100,cooldown_ns:200,
    }}
    fn i<'a>(id:&'a str,market:&'a str,at_ns:i64,amount:Money)->Intent<'a>{
        Intent{id,market,at_ns,proposed_notional:amount,
            current_market_exposure:0,current_portfolio_exposure:0,
            quote_received_ns:at_ns-2,max_quote_age_ns:10}
    }
    #[test]fn small_order_passes_and_oversized_one_fails(){
        let mut g=Guard::new(cfg()).unwrap();
        assert_eq!(g.preflight(&i("s","BTC",10,10_000)),Decision::Allow);
        assert_eq!(g.preflight(&i("huge","BTC",11,60_000)),
            Decision::Block(Block::SingleOrderLimit));
    }
    #[test]fn large_burst_trips_and_blocks_until_cooldown(){
        let mut g=Guard::new(cfg()).unwrap();
        assert_eq!(g.preflight(&i("a","BTC",10,30_000)),Decision::RequireDepth);
        assert_eq!(g.preflight(&i("b","BTC",20,30_000)),
            Decision::Block(Block::LargeBurstTrip));
        assert_eq!(g.tripped_until(),Some(220));
        assert_eq!(g.preflight(&i("c","BTC",100,5_000)),
            Decision::Block(Block::CooldownActive));
        assert_eq!(g.preflight(&i("d","BTC",230,5_000)),Decision::Allow);
    }
    #[test]fn thin_book_and_stale_quotes_fail_closed(){
        let mut g=Guard::new(cfg()).unwrap();
        assert_eq!(g.preflight(&i("a","BTC",10,30_000)),Decision::RequireDepth);
        assert_eq!(g.check_depth(39_999),Decision::Block(Block::InsufficientDepth));
        let mut stale=i("b","ETH",30,5_000);stale.quote_received_ns=0;
        assert_eq!(g.preflight(&stale),Decision::Block(Block::StaleOrFutureQuote));
        let mut future=i("c","ETH",30,5_000);future.quote_received_ns=31;
        assert_eq!(g.preflight(&future),Decision::Block(Block::StaleOrFutureQuote));
    }
    #[test]fn checked_market_and_portfolio_exposure(){
        let mut g=Guard::new(cfg()).unwrap();
        let mut a=i("a","BTC",10,20_000);a.current_market_exposure=90_000;
        a.current_portfolio_exposure=100_000;
        assert_eq!(g.preflight(&a),Decision::Block(Block::MarketExposureLimit));
        a.id="b";a.current_market_exposure=0;a.current_portfolio_exposure=290_000;
        assert_eq!(g.preflight(&a),Decision::Block(Block::PortfolioExposureLimit));
    }
    #[test]fn clock_backwards_and_duplicate_intents_rejected(){
        let mut g=Guard::new(cfg()).unwrap();
        assert_eq!(g.preflight(&i("a","BTC",30,5_000)),Decision::Allow);
        assert_eq!(g.preflight(&i("a","BTC",31,5_000)),
            Decision::Block(Block::DuplicateIntent));
        assert_eq!(g.preflight(&i("b","BTC",20,5_000)),
            Decision::Block(Block::NonmonotonicTime));
    }
    #[test]fn micro_usd_risk_mode_uses_lossless_money_limits(){
        let mut guard=Guard::new_micro(cfg()).unwrap();
        assert_eq!(guard.accounting_unit(),AccountingUnit::MicroUsd);
        let intent=Intent{id:"M",market:"BTC",at_ns:100,
            proposed_notional:5_000_000,current_market_exposure:0,
            current_portfolio_exposure:0,quote_received_ns:99,
            max_quote_age_ns:10};
        assert_eq!(guard.preflight(&intent),Decision::Allow);
        assert_eq!(guard.preflight(&Intent{proposed_notional:5_100_001,
            id:"B",at_ns:101,..intent}),Decision::Block(Block::SingleOrderLimit));
    }
    #[test]fn lossless_configuration_clone_replay_and_cooldown_are_stable(){
        let mut g=Guard::new_micro(cfg()).unwrap();
        let tag=g.configuration_identity();
        let risk=Intent{id:"a",market:"BTC",at_ns:100,
            proposed_notional:3_000_000,current_market_exposure:0,
            current_portfolio_exposure:0,quote_received_ns:99,
            max_quote_age_ns:20};
        assert_eq!(g.preflight(&risk),Decision::RequireDepth);
        let mut replay=g.empty_same_configuration();
        assert_eq!(replay.configuration_identity(),tag);
        assert_eq!(replay.registered_intents(),0);
        assert_eq!(replay.preflight(&risk),Decision::RequireDepth);
        assert_eq!(replay.registered_intents(),1);
        assert_eq!(replay.preflight(&risk),Decision::Block(Block::DuplicateIntent));
    }
    #[test]fn invalid_configurations_never_start(){
        let mut c=cfg();c.large_burst_count=0;
        assert!(matches!(Guard::new(c),Err(Block::InvalidConfiguration)));
    }
}

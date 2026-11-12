//! Research-only event-sourced paper accounting. No order APIs, credentials,
//! exchange connectivity or background services.
use phoenix_durable_archive::{Archive,EventRow,SourceScope};
use phoenix_parquet_batch::Row;
use serde::{Deserialize,Serialize};
use std::collections::{BTreeMap,HashMap,HashSet};
use std::path::Path;
pub type Money=i128;
const ONE_DOLLAR:Money=10_000;
const FEED:&str="paper_sim";
const SESSION:&str="offline-paper";

#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="snake_case")]
pub enum Side {Yes,No}
#[derive(Debug,Clone,PartialEq,Eq,Serialize,Deserialize)]
#[serde(tag="type",rename_all="snake_case")]
pub enum Event {
    Open {portfolio:String,seq:u64,at_ns:i64,starting_cash:Money},
    Buy {portfolio:String,seq:u64,at_ns:i64,order_id:String,market:String,
        rules_sha256:String,side:Side,price_ticks:u16,quantity:u64,fee:Money},
    Settle {portfolio:String,seq:u64,at_ns:i64,order_id:String,market:String,
        rules_sha256:String,winner:Side,official_sha256:String},
    Invalidate {portfolio:String,seq:u64,at_ns:i64,order_id:String,reason:String},
    NoTrade {portfolio:String,seq:u64,at_ns:i64,reason:String},
}
impl Event {
    pub fn portfolio(&self)->&str {match self{
        Self::Open{portfolio,..}|Self::Buy{portfolio,..}|
        Self::Settle{portfolio,..}|Self::Invalidate{portfolio,..}|
        Self::NoTrade{portfolio,..}=>portfolio,
    }}
    pub fn seq(&self)->u64 {match self{
        Self::Open{seq,..}|Self::Buy{seq,..}|Self::Settle{seq,..}|
        Self::Invalidate{seq,..}|Self::NoTrade{seq,..}=>*seq,
    }}
    pub fn at_ns(&self)->i64 {match self{
        Self::Open{at_ns,..}|Self::Buy{at_ns,..}|Self::Settle{at_ns,..}|
        Self::Invalidate{at_ns,..}|Self::NoTrade{at_ns,..}=>*at_ns,
    }}
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct Position {
    pub market:String,pub rules_sha256:String,pub side:Side,
    pub price_ticks:u16,pub quantity:u64,pub total_paid:Money,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct PortfolioState {
    pub cash:Money,pub realized_net:Money,pub positions:HashMap<String,Position>,
    pub completed:HashSet<String>,pub invalidated_count:u64,
    pub skipped_count:u64,pub settled_count:u64,pub next_seq:u64,
    pub last_recorded_ns:i64,
}
impl PortfolioState {
    fn start(cash:Money,at_ns:i64)->Self {
        Self {cash,realized_net:0,positions:HashMap::new(),
            completed:HashSet::new(),invalidated_count:0,skipped_count:0,
            settled_count:0,next_seq:1,last_recorded_ns:at_ns}
    }
}
#[derive(Debug,PartialEq,Eq)]
pub enum Error {
    InvalidEvent,BadSequence,NoPortfolio,DuplicatePortfolio,
    DuplicateOrder,UnknownOrder,InsufficientFunds,ConflictingMarket,
    EvidenceMissing,Overflow,CorruptJournal(String),
    Storage(String),
}
fn digest(s:&str)->bool {
    s.len()==64&&s.bytes().all(|b|b.is_ascii_hexdigit())
}
fn apply(states:&mut HashMap<String,PortfolioState>,e:&Event)->Result<(),Error>{
    let name=e.portfolio();
    if name.is_empty()||name.len()>100||e.at_ns()<0{return Err(Error::InvalidEvent);}
    match e {
        Event::Open{seq,starting_cash,..}=>{
            if *seq!=0||*starting_cash<0{return Err(Error::InvalidEvent);}
            if states.contains_key(name){return Err(Error::DuplicatePortfolio);}
            states.insert(name.to_owned(),PortfolioState::start(*starting_cash,e.at_ns()));
            Ok(())
        }
        _=>{
            let s=states.get_mut(name).ok_or(Error::NoPortfolio)?;
            if e.seq()!=s.next_seq||e.at_ns()<s.last_recorded_ns {
                return Err(Error::BadSequence);
            }
            match e {
                Event::Buy{order_id,market,rules_sha256,side,price_ticks,
                    quantity,fee,..}=>{
                    if order_id.is_empty()||market.is_empty()||!digest(rules_sha256)||
                        *price_ticks>10_000||*quantity==0||*fee<0{
                        return Err(Error::InvalidEvent);
                    }
                    if s.positions.contains_key(order_id)||s.completed.contains(order_id){
                        return Err(Error::DuplicateOrder);
                    }
                    let total=Money::from(*price_ticks)
                        .checked_mul(Money::from(*quantity))
                        .and_then(|v|v.checked_add(*fee)).ok_or(Error::Overflow)?;
                    if total>s.cash{return Err(Error::InsufficientFunds);}
                    s.cash-=total;
                    s.positions.insert(order_id.clone(),Position{
                        market:market.clone(),rules_sha256:rules_sha256.clone(),
                        side:*side,price_ticks:*price_ticks,quantity:*quantity,
                        total_paid:total,
                    });
                }
                Event::Settle{order_id,market,rules_sha256,winner,
                    official_sha256,..}=>{
                    if !digest(official_sha256){return Err(Error::EvidenceMissing);}
                    let pos=s.positions.get(order_id).ok_or(Error::UnknownOrder)?;
                    if pos.market!=*market||pos.rules_sha256!=*rules_sha256{
                        return Err(Error::ConflictingMarket);
                    }
                    let payout=if pos.side==*winner{
                        Money::from(pos.quantity).checked_mul(ONE_DOLLAR)
                            .ok_or(Error::Overflow)?
                    }else{0};
                    let net=payout.checked_sub(pos.total_paid).ok_or(Error::Overflow)?;
                    s.cash=s.cash.checked_add(payout).ok_or(Error::Overflow)?;
                    s.realized_net=s.realized_net.checked_add(net).ok_or(Error::Overflow)?;
                    s.positions.remove(order_id);
                    s.completed.insert(order_id.clone());
                    s.settled_count+=1;
                }
                Event::Invalidate{order_id,reason,..}=>{
                    if reason.trim().is_empty(){return Err(Error::InvalidEvent);}
                    let pos=s.positions.get(order_id).ok_or(Error::UnknownOrder)?;
                    s.cash=s.cash.checked_add(pos.total_paid).ok_or(Error::Overflow)?;
                    s.positions.remove(order_id);
                    s.completed.insert(order_id.clone());
                    s.invalidated_count+=1;
                }
                Event::NoTrade{reason,..}=>{
                    if reason.trim().is_empty(){return Err(Error::InvalidEvent);}
                    s.skipped_count+=1;
                }
                Event::Open{..}=>unreachable!(),
            }
            s.next_seq+=1;s.last_recorded_ns=e.at_ns();Ok(())
        }
    }
}
pub struct Journal {
    archive:Archive,
    states:HashMap<String,PortfolioState>,
}
impl Journal {
    pub fn open(path:impl AsRef<Path>)->Result<Self,Error>{
        let archive=Archive::open(path)
            .map_err(|e|Error::Storage(format!("{e:?}")))?;
        let mut all=BTreeMap::<(String,u64),Event>::new();
        archive.visit_verified_rows(|manifest,id,row|{
            let issue=|reason:&str|phoenix_durable_archive::Error::Parquet(reason.to_owned());
            if manifest.scope.feed!=FEED||manifest.scope.session!=SESSION||
                row.feed!=FEED||row.session!=SESSION||row.kind!=50 {
                return Err(issue("wrong feed, session or kind"));
            }
            let e:Event=serde_json::from_slice(&row.payload).map_err(|_|issue("invalid serialized paper event"))?;
            if e.portfolio()!=row.market||e.at_ns()!=row.received_ns||
                row.exchange_ns!=row.received_ns||e.seq()!=row.sequence||
                id.id!=format!("{}:{}",e.portfolio(),e.seq()){
                return Err(issue("identity, sequence, timestamp or event-id mismatch"));
            }
            if all.insert((e.portfolio().into(),e.seq()),e).is_some(){
                return Err(issue("duplicate portfolio/sequence in archive"));
            }
            Ok(())
        }).map_err(|e|Error::CorruptJournal(format!("{e:?}")))?;
        let mut states=HashMap::new();
        for e in all.values(){apply(&mut states,e)?;}
        Ok(Self{archive,states})
    }
    pub fn state(&self,portfolio:&str)->Option<&PortfolioState>{
        self.states.get(portfolio)
    }
    pub fn portfolios(&self)->usize{self.states.len()}
    /// Simulate transition first. Persist durable event BEFORE mutating state.
    /// Repeated event IDs are rejected; restart reconstruction is idempotent.
    pub fn append(&mut self,e:Event)->Result<(),Error>{
        let mut proposed=self.states.clone();
        apply(&mut proposed,&e)?;
        let payload=serde_json::to_vec(&e)
            .map_err(|x|Error::CorruptJournal(x.to_string()))?;
        let scope=SourceScope{feed:FEED.into(),session:SESSION.into(),
            market:e.portfolio().into(),observed_sequence_gaps:0,
            quality_flags:vec!["MODELED_ONLY".into()],source_manifest_sha256:None};
        let event=EventRow{
            event_id:format!("{}:{}",e.portfolio(),e.seq()),
            data:Row{feed:FEED.into(),session:SESSION.into(),
                market:e.portfolio().into(),exchange_ns:e.at_ns(),
                received_ns:e.at_ns(),sequence:e.seq(),kind:50,payload},
        };
        let p=self.archive.publish(scope,&[event])
            .map_err(|x|Error::Storage(format!("{x:?}")))?;
        if p.added!=1{return Err(Error::DuplicateOrder);}
        self.states=proposed;Ok(())
    }
    pub fn verify_archive(&self)->Result<usize,Error>{
        self.archive.verify_all().map_err(|e|Error::Storage(format!("{e:?}")))
    }
}
#[cfg(test)]
mod tests{
    use super::*;use tempfile::TempDir;
    fn open(name:&str,cash:Money)->Event{Event::Open{
        portfolio:name.into(),seq:0,at_ns:100,starting_cash:cash}}
    fn buy(portfolio:&str,id:&str)->Event{Event::Buy{
        portfolio:portfolio.into(),seq:1,at_ns:110,
        order_id:id.into(),market:"SYNTHETIC".into(),
        rules_sha256:"a".repeat(64),side:Side::Yes,
        price_ticks:5500,quantity:1,fee:174}}
    #[test]fn restart_recovers_multiple_portfolios_and_balances(){
        let t=TempDir::new().unwrap();let path=t.path().join("paper");
        let mut j=Journal::open(&path).unwrap();
        j.append(open("A",100_000)).unwrap();
        j.append(open("B",200_000)).unwrap();
        j.append(buy("A","order1")).unwrap();
        j.append(Event::NoTrade{portfolio:"B".into(),seq:1,
            at_ns:111,reason:"spread too wide".into()}).unwrap();
        assert_eq!(j.state("A").unwrap().cash,94_326);
        assert_eq!(j.state("B").unwrap().skipped_count,1);
        assert_eq!(j.verify_archive().unwrap(),4);
        drop(j);
        let mut restored=Journal::open(&path).unwrap();
        assert_eq!(restored.portfolios(),2);
        assert_eq!(restored.state("A").unwrap().cash,94_326);
        assert_eq!(restored.state("A").unwrap().next_seq,2);
        restored.append(Event::Settle{portfolio:"A".into(),seq:2,at_ns:200,
            order_id:"order1".into(),market:"SYNTHETIC".into(),
            rules_sha256:"a".repeat(64),winner:Side::Yes,
            official_sha256:"b".repeat(64)}).unwrap();
        assert_eq!(restored.state("A").unwrap().cash,104_326);
        assert_eq!(restored.state("A").unwrap().realized_net,4_326);
        drop(restored);
        let after=Journal::open(&path).unwrap();
        assert_eq!(after.state("A").unwrap().cash,104_326);
        assert_eq!(after.state("A").unwrap().positions.len(),0);
        assert_eq!(after.state("A").unwrap().settled_count,1);
    }
    #[test]fn invalidation_restores_escrow_without_pnl(){
        let t=TempDir::new().unwrap();
        let mut j=Journal::open(t.path().join("paper")).unwrap();
        j.append(open("A",20_000)).unwrap();
        j.append(buy("A","badcycle")).unwrap();
        j.append(Event::Invalidate{portfolio:"A".into(),seq:2,at_ns:120,
            order_id:"badcycle".into(),reason:"source gap".into()}).unwrap();
        assert_eq!(j.state("A").unwrap().cash,20_000);
        assert_eq!(j.state("A").unwrap().realized_net,0);
        assert_eq!(j.state("A").unwrap().invalidated_count,1);
    }
    #[test]fn rejected_transition_never_writes_archive(){
        let t=TempDir::new().unwrap();
        let mut j=Journal::open(t.path().join("paper")).unwrap();
        j.append(open("A",1000)).unwrap();
        assert_eq!(j.append(buy("A","excess")),Err(Error::InsufficientFunds));
        assert_eq!(j.verify_archive().unwrap(),1);
        assert_eq!(j.state("A").unwrap().next_seq,1);
    }
    #[test]fn duplicate_sequence_rejected(){
        let t=TempDir::new().unwrap();
        let mut j=Journal::open(t.path().join("paper")).unwrap();
        j.append(open("A",20_000)).unwrap();
        assert_eq!(j.append(open("A",20_000)),Err(Error::DuplicatePortfolio));
        j.append(buy("A","x")).unwrap();
        assert_eq!(j.append(buy("A","x")),Err(Error::BadSequence));
    }
}
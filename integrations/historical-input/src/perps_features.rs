//! Receipt-time causal Perps feature buffers for Kalshi research decisions.
//! Values use exact 1e8 base/quote units. Bps are explicitly quantized
//! integer analytical features, not source-price or exchange fill precision.
use crate::perps::{PerpsEvent,PerpsObservation,Aggressor,DepthLevel};
use serde::Serialize;
use std::collections::{BTreeMap,BTreeSet,VecDeque};

const MAX_INSTRUMENTS:usize=32;
const MAX_OBSERVATIONS:usize=1024;
const MAX_LOOKBACK_EVENTS:usize=32;
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum Error{InvalidRequest,InvalidObservation,Overflow,DuplicateConflict,
    OutOfOrder,TooManyInstruments,SessionUnverified,NoFreshBook}
#[derive(Debug,Clone)]
struct PairBuffer{
    session:String,last_received_ns:i64,session_reset:bool,
    records:VecDeque<PerpsObservation>,
    seen:BTreeMap<String,String>,
}
#[derive(Default)]
pub struct CausalPerpsBuffer{
    pairs:BTreeMap<(String,String),PairBuffer>,
}
#[derive(Debug,Clone)]
pub struct KalshiPerpsJoin{
    pub kalshi_ticker:String,pub asset_base:String,pub quote_asset:String,
    pub decision_received_ns:i64,pub lookback_ns:i64,pub max_quote_age_ns:i64,
    pub allowed_venues:BTreeSet<String>,pub require_clock_certain:bool,
}
#[derive(Debug,Clone,Serialize)]
pub struct VenueFeatures{
    pub venue:String,pub observed_symbol:String,
    pub source_session:String,pub kalshi_decision_received_ns:i64,
    pub latest_quote_received_ns:i64,
    pub latest_trade_received_ns:Option<i64>,
    pub best_bid_1e8:i128,pub best_ask_1e8:i128,
    pub quote_spread_1e8:i128,pub spread_bps:i128,
    pub book_top_imbalance_bps:i128,
    pub last_trade_price_1e8:Option<i128>,
    pub last_pair_return_bps:Option<i128>,
    pub realized_rms_return_bps:Option<u128>,
    pub signed_known_trade_flow_base_1e8:Option<i128>,
    pub unknown_aggressor_trades:u32,
    pub mark_1e8:Option<i128>,pub index_1e8:Option<i128>,
    pub mark_index_dislocation_bps:Option<i128>,
    pub source_clock_uncertain:bool,
    pub contributing_row_sha256:Vec<String>,
    pub contributing_partition_sha256:Vec<String>,
    pub contributing_schema_sha256:Vec<String>,
    pub observations_considered:usize,
    pub no_future_data_used:bool,
}
#[derive(Debug,Clone,Serialize)]
pub struct JoinedPerpsFeatures{
    pub kalshi_ticker:String,pub decision_received_ns:i64,
    pub venues:Vec<VenueFeatures>,pub missing_venues:Vec<String>,
    pub provenance_tier:String,pub no_empirical_pnl:bool,
}
fn bps_delta(a:i128,b:i128)->Result<i128,Error>{
    if b<=0{return Err(Error::InvalidObservation);}
    a.checked_sub(b).and_then(|d|d.checked_mul(10_000))
        .map(|n|n/b).ok_or(Error::Overflow)
}
fn top_book(ev:&PerpsEvent)->Option<(&DepthLevel,&DepthLevel)>{
    match ev{
        PerpsEvent::Bbo{bid,ask}=>Some((bid,ask)),
        PerpsEvent::Depth{bids,asks}=>bids.first().zip(asks.first()),
        _=>None,
    }
}
fn isqrt(n:u128)->u128{
    if n<2{return n;}
    let mut x=n;let mut y=(x+1)/2;
    while y<x{x=y;y=(x+n/x)/2;}
    x
}
impl CausalPerpsBuffer{
    pub fn ingest(&mut self,r:PerpsObservation)->Result<bool,Error>{
        if r.venue.is_empty()||r.observed_symbol.is_empty()||
            r.source_session.is_empty()||r.source_event_id.is_empty()||
            r.received_ns<=0||r.row_sha256.len()!=64||
            r.source_partition_sha256.len()!=64||
            r.source_schema_sha256.len()!=64||
            r.exchange_ns.is_some_and(|ts|ts>r.received_ns){
            return Err(Error::InvalidObservation);
        }
        let key=(r.venue.clone(),r.observed_symbol.clone());
        if !self.pairs.contains_key(&key)&&self.pairs.len()>=MAX_INSTRUMENTS{
            return Err(Error::TooManyInstruments);
        }
        let slot=self.pairs.entry(key).or_insert_with(||PairBuffer{
            session:r.source_session.clone(),last_received_ns:r.received_ns,
            session_reset:false,records:VecDeque::new(),seen:BTreeMap::new()});
        if slot.session!=r.source_session{
            // Restart source continuity is NOT implicitly proven by a new
            // observation. Until an external authenticated session anchor is
            // available, join must fail closed for this pair.
            slot.records.clear();slot.seen.clear();
            slot.session=r.source_session.clone();
            slot.last_received_ns=r.received_ns;slot.session_reset=true;
        }
        if let Some(prior)=slot.seen.get(&r.source_event_id){
            if prior!=&r.row_sha256{return Err(Error::DuplicateConflict);}
            return Ok(false);
        }
        if r.received_ns<slot.last_received_ns{return Err(Error::OutOfOrder);}
        slot.last_received_ns=r.received_ns;
        slot.seen.insert(r.source_event_id.clone(),r.row_sha256.clone());
        slot.records.push_back(r);
        while slot.records.len()>MAX_OBSERVATIONS{
            if let Some(prior)=slot.records.pop_front(){
                slot.seen.remove(&prior.source_event_id);
            }
        }
        Ok(true)
    }
    pub fn instrument_count(&self)->usize{self.pairs.len()}
    pub fn observation_count(&self)->usize{
        self.pairs.values().map(|x|x.records.len()).sum()
    }
    pub fn join(&self,req:&KalshiPerpsJoin)
        ->Result<JoinedPerpsFeatures,Error>{
        if req.kalshi_ticker.is_empty()||req.asset_base.is_empty()||
            req.quote_asset.is_empty()||req.decision_received_ns<=0||
            req.lookback_ns<=0||req.max_quote_age_ns<0||
            req.allowed_venues.is_empty()||req.allowed_venues.len()>MAX_INSTRUMENTS{
            return Err(Error::InvalidRequest);
        }
        let cutoff=req.decision_received_ns;
        let earliest=cutoff.checked_sub(req.lookback_ns)
            .ok_or(Error::InvalidRequest)?;
        let mut venues=Vec::new();
        let mut seen_venues=BTreeSet::new();
        for ((venue,symbol),slot) in &self.pairs{
            if !req.allowed_venues.contains(venue){continue;}
            if slot.session_reset{return Err(Error::SessionUnverified);}
            let mut relevant=slot.records.iter().filter(|r|
                r.received_ns<=cutoff&&r.received_ns>=earliest&&
                r.base_asset==req.asset_base&&r.quote_asset==req.quote_asset)
                .collect::<Vec<_>>();
            relevant.sort_by(|a,b|a.received_ns.cmp(&b.received_ns)
                .then(a.source_event_id.cmp(&b.source_event_id)));
            if relevant.is_empty(){continue;}
            let selected_quote=relevant.iter().rev().find(|r|
                top_book(&r.event).is_some()).copied();
            let Some(quote)=selected_quote else{continue;};
            let age=cutoff.checked_sub(quote.received_ns)
                .ok_or(Error::InvalidRequest)?;
            if age>req.max_quote_age_ns{continue;}
            let (bid,ask)=top_book(&quote.event).ok_or(Error::NoFreshBook)?;
            let sum=bid.quantity_base_1e8.checked_add(ask.quantity_base_1e8)
                .ok_or(Error::Overflow)?;
            if bid.price_1e8>ask.price_1e8||sum<=0{
                return Err(Error::InvalidObservation);
            }
            let imbalance=bid.quantity_base_1e8.checked_sub(
                ask.quantity_base_1e8).and_then(|x|x.checked_mul(10_000))
                .map(|x|x/sum).ok_or(Error::Overflow)?;
            let denom=bid.price_1e8.checked_add(ask.price_1e8)
                .ok_or(Error::Overflow)?;
            let spread=ask.price_1e8-bid.price_1e8;
            let spread_bps=spread.checked_mul(20_000)
                .map(|n|n/denom).ok_or(Error::Overflow)?;
            let trades=relevant.iter().filter_map(|r|match &r.event{
                PerpsEvent::Trade{price_1e8,quantity_base_1e8,aggressor}=>
                    Some((r,*price_1e8,*quantity_base_1e8,aggressor)),
                _=>None}).rev().take(MAX_LOOKBACK_EVENTS)
                .collect::<Vec<_>>();
            let latest_trade=trades.first().map(|(r,p,_,_)|(*r,*p));
            let last_pair_return=if trades.len()>1{
                Some(bps_delta(trades[0].1,trades[1].1)?)
            }else{None};
            let mut squared=0u128;let mut count=0u128;
            for pair in trades.windows(2){
                let delta=bps_delta(pair[0].1,pair[1].1)?;
                let abs=delta.unsigned_abs();
                squared=squared.checked_add(abs.checked_mul(abs)
                    .ok_or(Error::Overflow)?).ok_or(Error::Overflow)?;
                count+=1;
            }
            let rms=if count>0{Some(isqrt(squared/count))}else{None};
            let mut flow:Option<i128>=None;
            let mut unknown=0u32;
            for (_,_,qty,side) in &trades{
                let v=match side{
                    Some(Aggressor::Buy)=>Some(*qty),
                    Some(Aggressor::Sell)=>qty.checked_neg(),
                    None=>{unknown+=1;None},
                };
                if let Some(n)=v{
                    flow=Some(flow.unwrap_or(0).checked_add(n)
                        .ok_or(Error::Overflow)?);
                }
            }
            let reference=relevant.iter().rev().find_map(|r|
                match &r.event{PerpsEvent::Reference{mark_1e8,index_1e8}=>
                    Some((*mark_1e8,*index_1e8)),_=>None});
            let (mark,index)=reference.unwrap_or((None,None));
            let dislocation=match (mark,index){
                (Some(m),Some(i))=>Some(bps_delta(m,i)?),_=>None};
            let mut row_shas=BTreeSet::new();
            let mut partition_shas=BTreeSet::new();
            let mut schema_shas=BTreeSet::new();
            for r in &relevant{
                row_shas.insert(r.row_sha256.clone());
                partition_shas.insert(r.source_partition_sha256.clone());
                schema_shas.insert(r.source_schema_sha256.clone());
            }
            let clock_uncertain=relevant.iter().any(|r|r.source_clock_uncertain);
            if clock_uncertain&&req.require_clock_certain{continue;}
            seen_venues.insert(venue.clone());
            venues.push(VenueFeatures{
                venue:venue.clone(),observed_symbol:symbol.clone(),
                source_session:slot.session.clone(),
                kalshi_decision_received_ns:cutoff,
                latest_quote_received_ns:quote.received_ns,
                latest_trade_received_ns:latest_trade.map(|x|x.0.received_ns),
                best_bid_1e8:bid.price_1e8,best_ask_1e8:ask.price_1e8,
                quote_spread_1e8:spread,spread_bps,
                book_top_imbalance_bps:imbalance,
                last_trade_price_1e8:latest_trade.map(|x|x.1),
                last_pair_return_bps:last_pair_return,
                realized_rms_return_bps:rms,
                signed_known_trade_flow_base_1e8:flow,
                unknown_aggressor_trades:unknown,
                mark_1e8:mark,index_1e8:index,
                mark_index_dislocation_bps:dislocation,
                source_clock_uncertain:clock_uncertain,
                contributing_row_sha256:row_shas.into_iter().collect(),
                contributing_partition_sha256:partition_shas.into_iter().collect(),
                contributing_schema_sha256:schema_shas.into_iter().collect(),
                observations_considered:relevant.len(),no_future_data_used:true,
            });
        }
        let missing=req.allowed_venues.difference(&seen_venues)
            .cloned().collect::<Vec<_>>();
        Ok(JoinedPerpsFeatures{kalshi_ticker:req.kalshi_ticker.clone(),
            decision_received_ns:cutoff,venues,missing_venues:missing,
            provenance_tier:"SOURCE_HASH_CHECKED_NOT_PROVIDER_AUTHENTICATED".into(),
            no_empirical_pnl:true})
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    fn obs(venue:&str,at:i64,id:&str,event:PerpsEvent)->PerpsObservation{
        PerpsObservation{venue:venue.into(),observed_symbol:"BTCUSDT".into(),
            base_asset:"BTC".into(),quote_asset:"USDT".into(),
            source_session:"s1".into(),source_event_id:id.into(),
            received_ns:at,exchange_ns:Some(at-1),
            source_clock_uncertain:false,sequence:Some(at as u64),
            source_partition_sha256:"a".repeat(64),
            source_schema_sha256:"b".repeat(64),row_sha256:"c".repeat(64),
            event}
    }
    fn quote()->PerpsEvent{
        PerpsEvent::Bbo{
            bid:DepthLevel{price_1e8:100_000_000,quantity_base_1e8:200_000_000},
            ask:DepthLevel{price_1e8:101_000_000,quantity_base_1e8:100_000_000}}
    }
    fn req()->KalshiPerpsJoin{
        KalshiPerpsJoin{kalshi_ticker:"KXBTC-1".into(),
            asset_base:"BTC".into(),quote_asset:"USDT".into(),
            decision_received_ns:100,lookback_ns:90,max_quote_age_ns:20,
            allowed_venues:BTreeSet::from(["A".into(),"B".into()]),
            require_clock_certain:true}
    }
    #[test]fn joins_only_received_and_fresh_perps_quotes(){
        let mut b=CausalPerpsBuffer::default();
        b.ingest(obs("A",90,"id1",quote())).unwrap();
        b.ingest(obs("A",110,"id2",PerpsEvent::Trade{
            price_1e8:300_000_000,quantity_base_1e8:100,
            aggressor:Some(Aggressor::Buy)})).unwrap();
        let out=b.join(&req()).unwrap();
        assert_eq!(out.venues.len(),1);
        assert_eq!(out.venues[0].venue,"A");
        assert!(out.venues[0].last_trade_price_1e8.is_none());
        assert_eq!(out.missing_venues,vec!["B"]);
    }
    #[test]fn duplicate_and_out_of_order_fail_closed(){
        let mut b=CausalPerpsBuffer::default();
        b.ingest(obs("A",90,"id1",quote())).unwrap();
        assert!(!b.ingest(obs("A",90,"id1",quote())).unwrap());
        let mut conflict=obs("A",90,"id1",quote());
        conflict.row_sha256="e".repeat(64);
        assert_eq!(b.ingest(conflict),Err(Error::DuplicateConflict));
        assert_eq!(b.ingest(obs("A",80,"id0",quote())),
            Err(Error::OutOfOrder));
    }
    #[test]fn session_reconnect_is_not_implicitly_validated(){
        let mut b=CausalPerpsBuffer::default();
        b.ingest(obs("A",90,"id1",quote())).unwrap();
        let mut second=obs("A",95,"id2",quote());
        second.source_session="s2".into();b.ingest(second).unwrap();
        assert_eq!(b.join(&req()),Err(Error::SessionUnverified));
    }
    #[test]fn stale_quote_causes_missing_venue_not_future_lookahead(){
        let mut b=CausalPerpsBuffer::default();
        b.ingest(obs("A",50,"id1",quote())).unwrap();
        let out=b.join(&req()).unwrap();
        assert!(out.venues.is_empty());
        assert_eq!(out.missing_venues.len(),2);
    }
    #[test]fn bounded_buffers_and_multi_venue_ordering(){
        let mut b=CausalPerpsBuffer::default();
        for i in 1..1100{
            b.ingest(obs("A",i, &format!("id{i}"),quote())).unwrap();
        }
        assert_eq!(b.observation_count(),MAX_OBSERVATIONS);
        b.ingest(obs("B",1100,"extra",quote())).unwrap();
        assert_eq!(b.instrument_count(),2);
    }
    #[test]fn return_spread_imbalance_and_trade_flow_are_exact_integer_features(){
        let mut b=CausalPerpsBuffer::default();
        b.ingest(obs("A",80,"q",quote())).unwrap();
        b.ingest(obs("A",81,"t1",PerpsEvent::Trade{
            price_1e8:100_000_000,quantity_base_1e8:10,
            aggressor:Some(Aggressor::Buy)})).unwrap();
        b.ingest(obs("A",82,"t2",PerpsEvent::Trade{
            price_1e8:110_000_000,quantity_base_1e8:4,
            aggressor:Some(Aggressor::Sell)})).unwrap();
        let r=b.join(&req()).unwrap();
        let v=&r.venues[0];
        assert_eq!(v.last_pair_return_bps,Some(1000));
        assert_eq!(v.signed_known_trade_flow_base_1e8,Some(6));
        assert_eq!(v.book_top_imbalance_bps,3333);
        assert!(v.realized_rms_return_bps.is_some());
    }
}

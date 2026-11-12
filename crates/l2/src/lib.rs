//! Kalshi-style subscription-scoped L2 reconstruction from typed messages.
//! Level quantities are 1/100 contracts. No fractional quantity is silently
//! converted to an integer-contract research book.
use std::collections::{BTreeMap,HashMap};
use phoenix_domain::{Book,Level,Price,Side};

#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum Error {WrongSubscription,DuplicateSequence,SequenceGap,
    MissingSnapshot,InvalidDelta,MalformedSnapshot,OutOfOrder,
    UnsupportedFractional,InvalidBook}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum SnapshotStatus {First,Continuous,RecoveredGap}
#[derive(Debug,Clone)]
pub struct Snapshot {
    pub sid:u64,pub sequence:u64,pub market:String,pub clock:u64,
    pub received_ns:i64,pub exchange_ns:i64,
    pub yes_fp:Vec<(Price,u64)>,pub no_fp:Vec<(Price,u64)>,
}
#[derive(Debug,Clone)]
pub struct Delta {
    pub sid:u64,pub sequence:u64,pub market:String,pub clock:u64,
    pub received_ns:i64,pub exchange_ns:i64,
    pub side:Side,pub price:Price,pub delta_fp:i64,
}
#[derive(Debug,Clone)]
struct MarketBook {
    yes:BTreeMap<Price,u64>,no:BTreeMap<Price,u64>,
    last_clock:u64,received_ns:i64,exchange_ns:i64,sequence:u64,
}
pub struct L2Session {
    sid:u64,session:String,last_seq:Option<u64>,
    books:HashMap<String,MarketBook>,
    pub had_gap:bool,
}
impl L2Session {
    pub fn new(sid:u64,session:String)->Self{
        Self{sid,session,last_seq:None,books:HashMap::new(),had_gap:false}
    }
    fn advance(&mut self, sid:u64,seq:u64)->Result<SnapshotStatus,Error>{
        if sid!=self.sid{return Err(Error::WrongSubscription);}
        let result=match self.last_seq {
            None=>SnapshotStatus::First,
            Some(prev) if seq<=prev =>return Err(Error::DuplicateSequence),
            Some(prev) if seq-prev>1 => {
                self.books.clear();self.had_gap=true;SnapshotStatus::RecoveredGap
            }
            Some(_)=>SnapshotStatus::Continuous,
        };
        self.last_seq=Some(seq);Ok(result)
    }
    pub fn on_snapshot(&mut self,s:Snapshot)->Result<SnapshotStatus,Error>{
        if s.market.is_empty()||s.exchange_ns>s.received_ns{
            return Err(Error::MalformedSnapshot);
        }
        let status=self.advance(s.sid,s.sequence)?;
        let build=|levels:Vec<(Price,u64)>|->Result<BTreeMap<Price,u64>,Error>{
            let mut out=BTreeMap::new();
            for (price,qty) in levels {
                if qty==0||out.insert(price,qty).is_some(){
                    return Err(Error::MalformedSnapshot);
                }
            }
            Ok(out)
        };
        let yes=build(s.yes_fp)?;let no=build(s.no_fp)?;
        self.books.insert(s.market,MarketBook{
            yes,no,last_clock:s.clock,received_ns:s.received_ns,
            exchange_ns:s.exchange_ns,sequence:s.sequence});
        Ok(status)
    }
    pub fn on_delta(&mut self,d:Delta)->Result<(),Error>{
        if d.market.is_empty()||d.exchange_ns>d.received_ns{
            return Err(Error::InvalidDelta);
        }
        let status=self.advance(d.sid,d.sequence)?;
        if status==SnapshotStatus::RecoveredGap{return Err(Error::SequenceGap);}
        let b=self.books.get_mut(&d.market).ok_or(Error::MissingSnapshot)?;
        if d.received_ns<b.received_ns{
            self.books.remove(&d.market);return Err(Error::OutOfOrder);
        }
        let map=match d.side {Side::Yes=>&mut b.yes,Side::No=>&mut b.no};
        let existing=map.get(&d.price).copied().unwrap_or(0);
        let after=i128::from(existing)+i128::from(d.delta_fp);
        if after<0||after>i128::from(u64::MAX){
            self.books.remove(&d.market);return Err(Error::InvalidDelta);
        }
        if after==0 {map.remove(&d.price);}
        else {map.insert(d.price,after as u64);}
        // Do not enforce monotonically increasing exchange clock: in conflated
        // subscriptions clocks may decline between otherwise contiguous seqs.
        b.last_clock=d.clock;b.received_ns=d.received_ns;
        b.exchange_ns=d.exchange_ns;b.sequence=d.sequence;Ok(())
    }
    pub fn ready_integer_book(&self,market:&str)->Result<Book,Error>{
        let b=self.books.get(market).ok_or(Error::MissingSnapshot)?;
        let convert=|m:&BTreeMap<Price,u64>|->Result<Vec<Level>,Error>{
            m.iter().rev().map(|(price,fp)|{
                if fp%100!=0 {return Err(Error::UnsupportedFractional);}
                Ok(Level{price:*price,quantity:fp/100})
            }).collect()
        };
        let snapshot=Book{market:market.into(),yes_bids:convert(&b.yes)?,
            no_bids:convert(&b.no)?,exchange_ns:b.exchange_ns,
            received_ns:b.received_ns,session_id:self.session.clone(),
            sequence:b.sequence};
        snapshot.validate().map_err(|_|Error::InvalidBook)?;
        Ok(snapshot)
    }
    pub fn tracked_markets(&self)->usize{self.books.len()}
}
#[cfg(test)]
mod tests{
    use super::*;
    fn p(x:u16)->Price {Price::new(x).unwrap()}
    fn snap(seq:u64)->Snapshot {Snapshot{sid:7,sequence:seq,market:"M".into(),
        clock:100,received_ns:200,exchange_ns:190,
        yes_fp:vec![(p(4000),500)],no_fp:vec![(p(5500),300)]}}
    fn delta(seq:u64,clock:u64,change:i64)->Delta{
        Delta{sid:7,sequence:seq,market:"M".into(),clock,
            received_ns:201+seq as i64,exchange_ns:200+seq as i64,
            side:Side::Yes,price:p(4000),delta_fp:change}}
    #[test]fn reconstruct_aggregate_level_deltas(){
        let mut s=L2Session::new(7,"ws-session".into());
        assert_eq!(s.on_snapshot(snap(1)),Ok(SnapshotStatus::First));
        s.on_delta(delta(2,101,200)).unwrap();
        let b=s.ready_integer_book("M").unwrap();
        assert_eq!(b.yes_bids[0].quantity,7);
        assert_eq!(b.best_ask(Side::Yes),Some(p(4500)));
    }
    #[test]fn gaps_quarantine_until_fresh_snapshot(){
        let mut s=L2Session::new(7,"s".into());s.on_snapshot(snap(1)).unwrap();
        assert_eq!(s.on_delta(delta(3,101,100)),Err(Error::SequenceGap));
        assert_eq!(s.ready_integer_book("M"),Err(Error::MissingSnapshot));
        assert_eq!(s.on_snapshot(snap(4)),Ok(SnapshotStatus::Continuous));
        assert_eq!(s.ready_integer_book("M").unwrap().sequence,4);
        assert!(s.had_gap);
    }
    #[test]fn duplicate_rejected_no_double_apply(){
        let mut s=L2Session::new(7,"s".into());s.on_snapshot(snap(1)).unwrap();
        assert_eq!(s.on_delta(delta(1,101,100)),Err(Error::DuplicateSequence));
        assert_eq!(s.ready_integer_book("M").unwrap().yes_bids[0].quantity,5);
    }
    #[test]fn conflated_clocks_can_decrease(){
        let mut s=L2Session::new(7,"s".into());s.on_snapshot(snap(1)).unwrap();
        s.on_delta(delta(2,55,100)).unwrap();
        assert_eq!(s.ready_integer_book("M").unwrap().yes_bids[0].quantity,6);
    }
    #[test]fn fractional_size_fails_closed(){
        let mut s=L2Session::new(7,"s".into());
        let mut x=snap(1);x.yes_fp[0].1=125;s.on_snapshot(x).unwrap();
        assert_eq!(s.ready_integer_book("M"),Err(Error::UnsupportedFractional));
    }
    #[test]fn underflow_requires_new_snapshot(){
        let mut s=L2Session::new(7,"s".into());s.on_snapshot(snap(1)).unwrap();
        assert_eq!(s.on_delta(delta(2,101,-900)),Err(Error::InvalidDelta));
        assert_eq!(s.ready_integer_book("M"),Err(Error::MissingSnapshot));
    }
}

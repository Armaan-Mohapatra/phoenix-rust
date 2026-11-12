//! Offline orderbook execution hypotheses: every fill is MODELED, never actual.
use phoenix_domain::{require_asof,Book,Error as DomainError,FeeRate,Money,Price,Side,TimestampNs};

#[derive(Debug,Clone,PartialEq,Eq)]
pub enum Error { Domain(DomainError), InvalidTime, NoArrivalBook, IncompleteTape,
    MarketableMaker, Overflow, ZeroQuantity }
impl From<DomainError> for Error { fn from(e:DomainError)->Self { Self::Domain(e) } }

#[derive(Debug,Clone)]
pub struct OrderIntent {
    pub market:String, pub side:Side, pub quantity:u64, pub buy_limit:Price,
    pub signal_received_ns:TimestampNs, pub decision_ns:TimestampNs,
    pub arrival_ns:TimestampNs, pub max_age_ns:i64, pub fee_rate:FeeRate,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct LevelFill { pub price:Price, pub quantity:u64, pub fee:Money }
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct TakerResult {
    pub fills:Vec<LevelFill>, pub requested:u64, pub unfilled:u64,
    pub cost:Money, pub fees:Money, pub book_received_ns:TimestampNs,
}
fn arrival_book<'a>(o:&OrderIntent, books:&'a [Book]) -> Result<&'a Book,Error> {
    if o.quantity==0 {return Err(Error::ZeroQuantity);}
    if o.arrival_ns<o.decision_ns {return Err(Error::InvalidTime);}
    require_asof(o.signal_received_ns,o.decision_ns,o.max_age_ns)?;
    let b=books.iter().filter(|b|b.market==o.market && b.received_ns<=o.arrival_ns)
        .max_by_key(|b|(b.received_ns,b.sequence)).ok_or(Error::NoArrivalBook)?;
    b.validate()?;
    require_asof(b.received_ns,o.arrival_ns,o.max_age_ns)?;
    Ok(b)
}
/// Consume displayed opposite-side bids with a limit; fee rounding is per level.
/// Caller must separately certify the replay segment has unbroken book sequences.
pub fn taker_buy(o:&OrderIntent,books:&[Book])->Result<TakerResult,Error> {
    let b=arrival_book(o,books)?;
    let mut left=o.quantity; let mut cost:Money=0;let mut fees:Money=0;
    let mut fills=Vec::new();
    for level in b.ask_depth(o.side) {
        let ask=level.price.complement();
        if ask>o.buy_limit {break;}
        let n=left.min(level.quantity);
        if n==0 {continue;}
        let cost_level=Money::from(n).checked_mul(Money::from(ask.ticks()))
            .ok_or(Error::Overflow)?;
        let fee_level=o.fee_rate.charge(n,ask)?;
        cost=cost.checked_add(cost_level).ok_or(Error::Overflow)?;
        fees=fees.checked_add(fee_level).ok_or(Error::Overflow)?;
        fills.push(LevelFill{price:ask,quantity:n,fee:fee_level});
        left-=n;if left==0 {break;}
    }
    Ok(TakerResult{fills,requested:o.quantity,unfilled:left,cost,fees,
        book_received_ns:b.received_ns})
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct ConsumingTrade {
    pub market:String, pub resting_side:Side, pub price:Price,
    pub quantity:u64, pub received_ns:TimestampNs,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct MakerResult {
    pub price:Price,pub queue_ahead:u64,pub matched:u64,pub unfilled:u64,
    pub fees:Money,pub model:&'static str,
}
/// Conservatively join behind displayed size; credit only prints against this
/// resting price/side, never assumed cancellations. Requires complete tape.
pub fn maker_buy(o:&OrderIntent,books:&[Book],tape:&[ConsumingTrade],
    deadline_ns:TimestampNs,complete_tape:bool)->Result<MakerResult,Error> {
    if !complete_tape {return Err(Error::IncompleteTape);}
    if deadline_ns<o.arrival_ns {return Err(Error::InvalidTime);}
    let b=arrival_book(o,books)?;
    if b.best_ask(o.side).is_some_and(|ask|o.buy_limit>=ask) {
        return Err(Error::MarketableMaker);
    }
    let bids=match o.side {Side::Yes=>&b.yes_bids,Side::No=>&b.no_bids};
    let mut ahead=bids.iter().find(|l|l.price==o.buy_limit).map_or(0,|l|l.quantity);
    let original_ahead=ahead;let mut left=o.quantity;
    let mut ticks=tape.iter().enumerate().collect::<Vec<_>>();
    ticks.sort_by_key(|(i,t)|(t.received_ns,*i));
    for (_,t) in ticks {
        if t.market!=o.market || t.resting_side!=o.side || t.price!=o.buy_limit ||
            t.received_ns<=o.arrival_ns || t.received_ns>deadline_ns {continue;}
        let used=ahead.min(t.quantity);ahead-=used;
        left-=left.min(t.quantity-used);
        if left==0 {break;}
    }
    let matched=o.quantity-left;
    let fees=if matched==0 {0} else {o.fee_rate.charge(matched,o.buy_limit)?};
    Ok(MakerResult{price:o.buy_limit,queue_ahead:original_ahead,matched,
        unfilled:left,fees,model:"no_cancellation_credit"})
}
#[cfg(test)]
mod tests {
    use super::*;use phoenix_domain::Level;
    fn p(x:u16)->Price {Price::new(x).unwrap()}
    fn books()->Vec<Book>{vec![
        Book{market:"BTC".into(),yes_bids:vec![Level{price:p(4000),quantity:5}],
            no_bids:vec![Level{price:p(5500),quantity:2},Level{price:p(5400),quantity:4}],
            exchange_ns:10,received_ns:15,session_id:"s".into(),sequence:1},
        Book{market:"BTC".into(),yes_bids:vec![Level{price:p(4100),quantity:5}],
            no_bids:vec![Level{price:p(5300),quantity:10}],
            exchange_ns:30,received_ns:35,session_id:"s".into(),sequence:2}]}
    fn order()->OrderIntent{OrderIntent{market:"BTC".into(),side:Side::Yes,quantity:5,
        buy_limit:p(4600),signal_received_ns:10,decision_ns:20,
        arrival_ns:25,max_age_ns:30,fee_rate:FeeRate::STANDARD_TAKER}}
    #[test] fn sweep_multiple_levels(){let r=taker_buy(&order(),&books()).unwrap();
        assert_eq!(r.unfilled,0);assert_eq!(r.fills.len(),2);
        assert_eq!((r.fills[0].price,r.fills[0].quantity),(p(4500),2));
        assert_eq!((r.fills[1].price,r.fills[1].quantity),(p(4600),3));
        assert_eq!(r.cost,2*4500+3*4600);
        assert_eq!(r.fees,FeeRate::STANDARD_TAKER.charge(2,p(4500)).unwrap()+
            FeeRate::STANDARD_TAKER.charge(3,p(4600)).unwrap());}
    #[test] fn asof_is_not_lookahead(){let mut o=order();
        assert_eq!(taker_buy(&o,&books()).unwrap().book_received_ns,15);
        o.arrival_ns=14;
        assert_eq!(taker_buy(&o,&books()),Err(Error::InvalidTime));
        o.arrival_ns=45;o.decision_ns=40;o.signal_received_ns=41;
        assert_eq!(taker_buy(&o,&books()),Err(Error::Domain(DomainError::FutureData)));}
    #[test] fn no_liquidity_is_no_fill(){let mut o=order();o.quantity=100;
        let r=taker_buy(&o,&books()).unwrap();assert_eq!(r.unfilled,94);}
    #[test] fn maker_queue_requires_consume_and_complete_tape(){
        let mut o=order();o.buy_limit=p(4000);o.quantity=2;o.fee_rate=FeeRate::ZERO;
        assert_eq!(maker_buy(&o,&books(),&[],60,false),Err(Error::IncompleteTape));
        let trade=|q,t|ConsumingTrade{market:"BTC".into(),resting_side:Side::Yes,
            price:p(4000),quantity:q,received_ns:t};
        let tape=[trade(4,30),trade(2,40),trade(2,50)];
        let a=maker_buy(&o,&books(),&tape,45,true).unwrap();
        assert_eq!((a.queue_ahead,a.matched,a.unfilled),(5,1,1));
        let b=maker_buy(&o,&books(),&tape,55,true).unwrap();
        assert_eq!((b.matched,b.unfilled),(2,0));}
    #[test] fn maker_must_not_cross_ask(){
        assert_eq!(maker_buy(&order(),&books(),&[],60,true),Err(Error::MarketableMaker));}
}
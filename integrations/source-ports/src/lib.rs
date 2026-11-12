//! Offline-only, independent Rust implementation of public mathematical
//! constraints from Oracle3's Apache-2.0 fee-aware binary relations and
//! Ryan's MIT forward-only edge diagnostics. No Python runtime imported.
//! All relations are *hypotheses* until actual contract rules are corroborated.
use phoenix_domain::{FeeRate, Price, Side};

const PAYOUT:i128=10_000;
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum Relation { Complement, Implication, Exclusivity, ExactlyOne }
#[derive(Debug,Clone,Copy)]
pub struct Quote {
    pub yes_ask:Option<Price>,pub no_ask:Option<Price>,pub taker_fee:FeeRate,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub struct Leg { pub market_index:usize,pub side:Side,pub cost:i128,pub fees:i128 }
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct Basket {
    pub legs:Vec<Leg>,pub guaranteed_payout_under_assumption:i128,
    pub hypothetical_net_after_fees:i128,
    pub contract_relation_independently_verified:bool,
    pub actual_fills_verified:bool,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum Error {InsufficientMarkets,MissingQuote,UnknownRelation,FeeOverflow,
    UnsafeEventTiming,SampleTooSmall}
fn leg(q:&Quote,market_index:usize,side:Side)->Result<Leg,Error>{
    let price=match side{Side::Yes=>q.yes_ask,Side::No=>q.no_ask}
        .ok_or(Error::MissingQuote)?;
    let fees=q.taker_fee.charge(1,price).map_err(|_|Error::FeeOverflow)?;
    Ok(Leg{market_index,side,cost:i128::from(price.ticks()),fees})
}
fn basket(quotes:&[Quote],sides:&[Side],min_payout:i128)->Result<Basket,Error>{
    let mut legs=Vec::new();let mut sum=0i128;
    for (i,side) in sides.iter().enumerate(){
        let l=leg(&quotes[i],i,*side)?;
        sum=sum.checked_add(l.cost).and_then(|x|x.checked_add(l.fees))
            .ok_or(Error::FeeOverflow)?;
        legs.push(l);
    }
    Ok(Basket{
        legs,guaranteed_payout_under_assumption:min_payout,
        hypothetical_net_after_fees:min_payout-sum,
        contract_relation_independently_verified:false,
        actual_fills_verified:false,
    })
}
/// Only one static, one-unit-per-leg basket is returned; no orders are sent.
/// Relation truth, market identity and complete multi-leg fills are UNVERIFIED.
pub fn evaluate_relation(rel:Relation,quotes:&[Quote])->Result<Basket,Error>{
    if quotes.len()<2||quotes.len()>16{return Err(Error::InsufficientMarkets);}
    let n=quotes.len();
    let candidates=match rel{
        Relation::Complement if n==2=>vec![
            basket(quotes,&[Side::Yes,Side::Yes],PAYOUT)?,
            basket(quotes,&[Side::No,Side::No],PAYOUT)?],
        Relation::Implication if n==2=>vec![
            basket(quotes,&[Side::No,Side::Yes],PAYOUT)?],
        Relation::Exclusivity=>vec![
            basket(quotes,&vec![Side::No;n],(n as i128-1)*PAYOUT)?],
        Relation::ExactlyOne=>vec![
            basket(quotes,&vec![Side::Yes;n],PAYOUT)?,
            basket(quotes,&vec![Side::No;n],(n as i128-1)*PAYOUT)?],
        _=>return Err(Error::UnknownRelation),
    };
    candidates.into_iter()
        .max_by_key(|b|b.hypothetical_net_after_fees)
        .ok_or(Error::UnknownRelation)
}
#[derive(Debug,Clone,Copy)]
pub struct ForwardSignal {
    pub decision_ns:i64,pub outcome_public_ns:Option<i64>,
    pub estimated_yes:Price,pub executable_yes_ask:Price,pub yes_won:Option<bool>,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum ForwardStatus { ForwardSettled, Pending, LeakedOrAmbiguous }
/// Deliberately refuses to treat a resolved-at-entry or undated label as
/// valid after-trade evidence, matching Ryan's forward-only test principle.
pub fn forward_status(s:&ForwardSignal)->ForwardStatus {
    match(s.outcome_public_ns,s.yes_won){
        (Some(end),Some(_)) if end>s.decision_ns=>ForwardStatus::ForwardSettled,
        (None,None)=>ForwardStatus::Pending,
        _=>ForwardStatus::LeakedOrAmbiguous,
    }
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub struct ForwardDiagnostics {
    pub records:usize,pub wins:usize,
    /// Signed mean over-sample edge vs executable quote, in 1e-4 probability
    /// units; non-economic, before execution slippage, fees and sizing.
    pub mean_realized_edge_ticks:i128,
    pub brier_micro:u64,
    pub authenticated_independent_settlement:bool,
}
/// Sample-size guard and strict outcome-time gate. Caller supplies outcomes;
/// this report never claims their independent provenance is authenticated.
pub fn forward_edge(rows:&[ForwardSignal])->Result<ForwardDiagnostics,Error>{
    if rows.len()<10{return Err(Error::SampleTooSmall);}
    if rows.iter().any(|r|forward_status(r)!=ForwardStatus::ForwardSettled){
        return Err(Error::UnsafeEventTiming);
    }
    let mut delta=0i128;let mut brier=0u128;let mut wins=0usize;
    for r in rows{
        let outcome=if r.yes_won==Some(true){wins+=1;PAYOUT}else{0};
        delta+=outcome-i128::from(r.executable_yes_ask.ticks());
        let err=outcome-i128::from(r.estimated_yes.ticks());
        brier+=(err*err) as u128;
    }
    let n=rows.len() as i128;
    let denom=(rows.len() as u128)*(PAYOUT as u128)*(PAYOUT as u128);
    Ok(ForwardDiagnostics{
        records:rows.len(),wins,
        mean_realized_edge_ticks:delta/n,
        brier_micro:(brier*1_000_000/denom) as u64,
        authenticated_independent_settlement:false,
    })
}
#[cfg(test)]
mod tests {
    use super::*;fn p(x:u16)->Price{Price::new(x).unwrap()}
    fn quote(yes:u16,no:u16)->Quote{
        Quote{yes_ask:Some(p(yes)),no_ask:Some(p(no)),
            taker_fee:FeeRate::STANDARD_TAKER}
    }
    #[test]fn oracle3_complement_fees_can_destroy_gross_edge(){
        let good=evaluate_relation(Relation::Complement,&[
            quote(4500,5500),quote(4500,5500)]).unwrap();
        assert!(good.hypothetical_net_after_fees>0);
        assert_eq!(good.guaranteed_payout_under_assumption,10_000);
        assert!(!good.contract_relation_independently_verified);
        assert!(!good.actual_fills_verified);
        let not_profitable=evaluate_relation(Relation::Complement,&[
            quote(5000,5000),quote(5000,5000)]).unwrap();
        assert!(not_profitable.hypothetical_net_after_fees<0);
    }
    #[test]fn relation_identity_is_not_proof(){
        let o=evaluate_relation(Relation::Implication,&[
            quote(5000,5000),quote(4000,6000)]).unwrap();
        assert_eq!(o.legs[0].side,Side::No);
        assert_eq!(o.legs[1].side,Side::Yes);
        assert!(!o.contract_relation_independently_verified);
    }
    #[test]fn forward_score_rejects_leakage_and_tiny_sample(){
        let r=ForwardSignal{decision_ns:100,outcome_public_ns:Some(200),
            estimated_yes:p(6000),executable_yes_ask:p(5500),yes_won:Some(true)};
        assert_eq!(forward_status(&r),ForwardStatus::ForwardSettled);
        assert_eq!(forward_edge(&[r]),Err(Error::SampleTooSmall));
        let mut items=vec![r;10];
        items[0].outcome_public_ns=Some(99);
        assert_eq!(forward_edge(&items),Err(Error::UnsafeEventTiming));
    }
    #[test]fn forward_brier_and_edge_preserve_loss(){
        let items=vec![ForwardSignal{decision_ns:100,outcome_public_ns:Some(200),
            estimated_yes:p(8000),executable_yes_ask:p(6000),yes_won:Some(false)};10];
        let summary=forward_edge(&items).unwrap();
        assert_eq!(summary.mean_realized_edge_ticks,-6000);
        assert_eq!(summary.brier_micro,640_000);
        assert!(!summary.authenticated_independent_settlement);
    }
}

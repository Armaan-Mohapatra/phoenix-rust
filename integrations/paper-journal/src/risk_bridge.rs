//! Explicit V2 micro-USD risk preflight before durable paper Submit.
//! Uses existing risk-guard; never authorizes or places venue orders.
use crate::{Journal,Event};
use crate::arena_v2::{ActionV2,IntentV2,PortfolioV2};
use phoenix_risk_guard::{Guard,Intent,Decision,Block,AccountingUnit};
#[derive(Debug,PartialEq,Eq)]
pub enum Outcome{Accepted,Rejected(Block)}
#[derive(Debug)]
pub enum Error{InvalidUnit,UnknownPortfolio,Arithmetic,
    InvalidIntent,Journal(crate::Error)}
fn market_exposure(state:&PortfolioV2,market:&str)->Option<i128>{
    let positions=state.positions.values().filter(|x|x.market==market)
        .try_fold(0i128,|s,p|s.checked_add(p.open_cost_micro))?;
    let reserved=state.orders.values().filter(|x|x.intent.market==market)
        .try_fold(0i128,|s,o|s.checked_add(o.reserved_micro))?;
    positions.checked_add(reserved)
}
/// Caller provides explicit decision-time quote depth; no boolean or SHA
/// in this helper can authenticate the underlying market source. An
/// independent source-provenance verifier must gate its use in orchestration.
pub fn preflight_and_append(journal:&mut Journal,guard:&mut Guard,
    portfolio:&str,seq:u64,at_ns:i64,intent:IntentV2,
    observed_depth_micro:i128,max_quote_age_ns:i64)
    ->Result<Outcome,Error>{
    if guard.accounting_unit()!=AccountingUnit::MicroUsd{
        return Err(Error::InvalidUnit);
    }
    let state=journal.state(portfolio).ok_or(Error::UnknownPortfolio)?;
    let account=state.arena_v2.as_ref().ok_or(Error::UnknownPortfolio)?;
    if state.next_seq!=seq||at_ns<=0||max_quote_age_ns<0||
        observed_depth_micro<0{return Err(Error::InvalidIntent);}
    let notional=i128::from(intent.quantity_hundredths)
        .checked_mul(i128::from(intent.limit_ticks))
        .and_then(|x|x.checked_add(intent.fee_budget_micro))
        .ok_or(Error::Arithmetic)?;
    if notional<=0{return Err(Error::InvalidIntent);}
    let market=market_exposure(account,&intent.market)
        .ok_or(Error::Arithmetic)?;
    let total=account.total_invested_micro()
        .map_err(|_|Error::Arithmetic)?
        .checked_add(account.reserved_cash_micro).ok_or(Error::Arithmetic)?;
    let risk=Intent{id:&intent.order_id,market:&intent.market,
        at_ns,proposed_notional:notional,
        current_market_exposure:market,current_portfolio_exposure:total,
        quote_received_ns:intent.quote_received_ns,max_quote_age_ns};
    let decision=guard.preflight(&risk);
    let decision=if decision==Decision::RequireDepth{
        guard.check_depth(observed_depth_micro)
    }else{decision};
    let (action,outcome)=match decision{
        Decision::Allow=>(ActionV2::Submit{intent},Outcome::Accepted),
        Decision::Block(block)=>(ActionV2::Reject{
            order_id:intent.order_id,
            reason:format!("RISK_GUARD_{block:?}")},Outcome::Rejected(block)),
        Decision::RequireDepth=>return Err(Error::InvalidIntent),
    };
    journal.append(Event::ArenaV2{portfolio:portfolio.into(),seq,at_ns,action})
        .map_err(Error::Journal)?;
    Ok(outcome)
}
#[cfg(test)]
mod tests{
    use super::*;use tempfile::TempDir;
    use phoenix_risk_guard::Limits;
    fn limits()->Limits{Limits{
        max_single_order:20_000,max_market_exposure:50_000,
        max_portfolio_exposure:100_000,min_quote_depth:0,
        large_order_threshold:30_000,large_burst_count:3,
        lookback_ns:100,cooldown_ns:100}}
    fn intent()->IntentV2{IntentV2{
        order_id:"risk-test".into(),market:"M".into(),
        rules_sha256:"a".repeat(64),strategy_version_sha256:"b".repeat(64),
        source_sha256:"c".repeat(64),fee_schedule_sha256:"d".repeat(64),
        side:crate::Side::Yes,limit_ticks:5000,
        quantity_hundredths:100,fee_budget_micro:100,
        quote_received_ns:100,deadline_ns:1000}}
    #[test]fn verified_unit_guard_can_preflight_without_truncation(){
        let folder=TempDir::new().unwrap();
        let mut journal=Journal::open(folder.path().join("paper")).unwrap();
        journal.append(Event::OpenV2{portfolio:"A".into(),seq:0,
            at_ns:100,starting_cash_micro:5_000_000}).unwrap();
        let mut guard=Guard::new_micro(limits()).unwrap();
        let result=preflight_and_append(&mut journal,&mut guard,"A",1,
            101,intent(),1_000_000,100).unwrap();
        assert_eq!(result,Outcome::Accepted);
        assert!(journal.state("A").unwrap().arena_v2.as_ref()
            .unwrap().reserved_cash_micro>0);
    }
    #[test]fn legacy_unit_guard_is_refused(){
        let folder=TempDir::new().unwrap();
        let mut journal=Journal::open(folder.path().join("paper")).unwrap();
        let mut guard=Guard::new(limits()).unwrap();
        assert!(matches!(preflight_and_append(&mut journal,&mut guard,"A",1,
            101,intent(),1_000_000,100),Err(Error::InvalidUnit)));
    }
}

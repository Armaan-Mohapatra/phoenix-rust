//! Durable, restart-safe microUSD paper risk protocol.
//! One RiskAssessed V2 event contains the full replayable risk preflight
//! AND the corresponding simulated Submit/Reject. The journal is the sole
//! durable writer; failed publication never mutates the supplied Guard.
use crate::{Journal,Event};
use crate::arena_v2::{ActionV2,IntentV2,PortfolioV2,RiskAuditV2};
use phoenix_risk_guard::{Guard,Intent,Decision,Block,AccountingUnit};

#[derive(Debug,PartialEq,Eq)]
pub enum Outcome{Accepted,Rejected(Block)}
#[derive(Debug)]
pub enum Error{
    InvalidUnit,UnknownPortfolio,Arithmetic,InvalidIntent,
    RiskHistoryIncomplete,RiskReplayMismatch,GuardConfigMismatch,
    Journal(crate::Error),
}
fn market_exposure(state:&PortfolioV2,market:&str)->Option<i128>{
    let positions=state.positions.values().filter(|x|x.market==market)
        .try_fold(0i128,|s,p|s.checked_add(p.open_cost_micro))?;
    let reserved=state.orders.values().filter(|x|x.intent.market==market)
        .try_fold(0i128,|s,o|s.checked_add(o.reserved_micro))?;
    positions.checked_add(reserved)
}
fn outcome_code(decision:Decision)->Result<String,Error>{
    match decision{
        Decision::Allow=>Ok("ALLOW".into()),
        Decision::Block(block)=>Ok(format!("BLOCK_{block:?}")),
        Decision::RequireDepth=>Err(Error::RiskReplayMismatch),
    }
}
fn evaluate(guard:&mut Guard,audit:&RiskAuditV2)->Result<Decision,Error>{
    let request=Intent{id:&audit.intent_id,market:&audit.market,
        at_ns:audit.at_ns,proposed_notional:audit.proposed_notional_micro,
        current_market_exposure:audit.current_market_micro,
        current_portfolio_exposure:audit.current_portfolio_micro,
        quote_received_ns:audit.quote_received_ns,
        max_quote_age_ns:audit.max_quote_age_ns};
    let prelim=guard.preflight(&request);
    Ok(if prelim==Decision::RequireDepth{
        guard.check_depth(audit.observed_depth_micro)
    }else{prelim})
}
/// Deterministic replay must occur before EACH new risk decision, including
/// after failed publication or restart. A legacy direct V2 submit/reject
/// makes the portfolio ineligible until migrated using an explicit policy.
pub fn replay_guard_from_journal(
    journal:&Journal,portfolio:&str,configured_guard:&Guard
)->Result<Guard,Error>{
    if configured_guard.accounting_unit()!=AccountingUnit::MicroUsd{
        return Err(Error::InvalidUnit);
    }
    let account=journal.state(portfolio)
        .and_then(|s|s.arena_v2.as_ref())
        .ok_or(Error::UnknownPortfolio)?;
    if !account.risk_history_complete{
        return Err(Error::RiskHistoryIncomplete);
    }
    let mut rebuilt=configured_guard.empty_same_configuration();
    let identity=rebuilt.configuration_identity();
    for record in &account.risk_history{
        if record.guard_configuration_identity!=identity{
            return Err(Error::GuardConfigMismatch);
        }
        let decision=evaluate(&mut rebuilt,record)?;
        if outcome_code(decision)?!=record.outcome_code{
            return Err(Error::RiskReplayMismatch);
        }
        // Each audited action must have been applied to this same
        // paper portfolio through the one-event durable transition.
        if record.outcome_code=="ALLOW"{
            if !account.orders.contains_key(&record.intent_id){
                return Err(Error::RiskHistoryIncomplete);
            }
        }else if account.rejected.get(&record.intent_id).map(String::as_str)
            !=Some(format!("RISK_GUARD_{}",record.outcome_code).as_str()){
            return Err(Error::RiskReplayMismatch);
        }
    }
    Ok(rebuilt)
}
/// Risk decisions and paper orders are one journal event; risk guard
/// state is updated in memory only AFTER the durable archive confirms
/// a newly added event. An ambiguous archive failure requires Journal
/// reopening/replay; no unchecked retry is authorized.
pub fn preflight_and_append(journal:&mut Journal,guard:&mut Guard,
    portfolio:&str,seq:u64,at_ns:i64,intent:IntentV2,
    observed_depth_micro:i128,max_quote_age_ns:i64)
    ->Result<Outcome,Error>{
    let mut recovered=replay_guard_from_journal(journal,portfolio,guard)?;
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
    let audit=RiskAuditV2{intent_id:intent.order_id.clone(),
        market:intent.market.clone(),at_ns,proposed_notional_micro:notional,
        current_market_micro:market,current_portfolio_micro:total,
        observed_depth_micro,quote_received_ns:intent.quote_received_ns,
        max_quote_age_ns,
        guard_configuration_identity:recovered.configuration_identity(),
        outcome_code:String::new()};
    let decision=evaluate(&mut recovered,&audit)?;
    let code=outcome_code(decision)?;
    let outcome=match decision{
        Decision::Allow=>Outcome::Accepted,
        Decision::Block(b)=>Outcome::Rejected(b),
        Decision::RequireDepth=>return Err(Error::RiskReplayMismatch),
    };
    let audit=RiskAuditV2{outcome_code:code,..audit};
    journal.append(Event::ArenaV2{portfolio:portfolio.into(),seq,at_ns,
        action:ActionV2::RiskAssessed{intent,audit}})
        .map_err(Error::Journal)?;
    *guard=recovered;
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
        let replay=replay_guard_from_journal(&journal,"A",&guard).unwrap();
        assert_eq!(replay.registered_intents(),1);
        assert!(journal.state("A").unwrap().arena_v2.as_ref()
            .unwrap().reserved_cash_micro>0);
    }
    #[test]fn failed_publication_cannot_mutate_preflight_guard(){
        let folder=TempDir::new().unwrap();
        let mut journal=Journal::open(folder.path().join("paper")).unwrap();
        journal.append(Event::OpenV2{portfolio:"A".into(),seq:0,
            at_ns:100,starting_cash_micro:5_000_000}).unwrap();
        let mut guard=Guard::new_micro(limits()).unwrap();
        let before=guard.registered_intents();
        // Bad sequence simulates a failed append boundary without writes.
        assert!(preflight_and_append(&mut journal,&mut guard,"A",2,
            101,intent(),1_000_000,100).is_err());
        assert_eq!(guard.registered_intents(),before);
        assert!(journal.state("A").unwrap().arena_v2.as_ref()
            .unwrap().risk_history.is_empty());
    }
    #[test]fn replay_fails_closed_after_untracked_legacy_v2_submit(){
        let folder=TempDir::new().unwrap();
        let mut journal=Journal::open(folder.path().join("paper")).unwrap();
        journal.append(Event::OpenV2{portfolio:"A".into(),seq:0,
            at_ns:100,starting_cash_micro:5_000_000}).unwrap();
        journal.append(Event::ArenaV2{portfolio:"A".into(),seq:1,
            at_ns:101,action:ActionV2::Submit{intent:intent()}}).unwrap();
        let guard=Guard::new_micro(limits()).unwrap();
        assert!(matches!(replay_guard_from_journal(&journal,"A",&guard),
            Err(Error::RiskHistoryIncomplete)));
    }
    #[test]fn repeated_replay_preserves_duplicate_identity_and_rejections(){
        let folder=TempDir::new().unwrap();
        let mut journal=Journal::open(folder.path().join("paper")).unwrap();
        journal.append(Event::OpenV2{portfolio:"A".into(),seq:0,
            at_ns:100,starting_cash_micro:5_000_000}).unwrap();
        let mut guard=Guard::new_micro(limits()).unwrap();
        assert_eq!(preflight_and_append(&mut journal,&mut guard,"A",1,
            101,intent(),1_000_000,100).unwrap(),Outcome::Accepted);
        let one=replay_guard_from_journal(&journal,"A",&guard).unwrap();
        let two=replay_guard_from_journal(&journal,"A",&one).unwrap();
        assert_eq!(one.registered_intents(),two.registered_intents());
        let mut again=intent();again.order_id="different".into();
        again.quantity_hundredths=100_000;
        let denial=preflight_and_append(&mut journal,&mut guard,"A",2,
            102,again,0,100).unwrap();
        assert_eq!(denial,Outcome::Rejected(Block::SingleOrderLimit));
        assert_eq!(journal.state("A").unwrap().arena_v2.as_ref()
            .unwrap().risk_history.len(),2);
    }
    #[test]fn crash_after_durable_commit_before_in_memory_guard_update_is_replayed(){
        let folder=TempDir::new().unwrap();
        let path=folder.path().join("paper");
        let mut journal=Journal::open(&path).unwrap();
        journal.append(Event::OpenV2{portfolio:"A".into(),seq:0,
            at_ns:100,starting_cash_micro:5_000_000}).unwrap();
        let mut live=Guard::new_micro(limits()).unwrap();
        preflight_and_append(&mut journal,&mut live,"A",1,101,
            intent(),1_000_000,100).unwrap();
        // Simulate process failure: discard stateful guard and journal.
        drop(live);drop(journal);
        let restored=Journal::open(&path).unwrap();
        let new_guard=Guard::new_micro(limits()).unwrap();
        let recovered=replay_guard_from_journal(&restored,"A",&new_guard).unwrap();
        assert_eq!(recovered.registered_intents(),1);
        assert_eq!(restored.state("A").unwrap().next_seq,2);
    }
    #[test]fn fabricated_recorded_allow_is_detected_by_replay(){
        use crate::arena_v2::RiskAuditV2;
        let folder=TempDir::new().unwrap();
        let mut journal=Journal::open(folder.path().join("paper")).unwrap();
        journal.append(Event::OpenV2{portfolio:"A".into(),seq:0,
            at_ns:100,starting_cash_micro:5_000_000}).unwrap();
        let guard=Guard::new_micro(limits()).unwrap();
        let bad=intent();
        let a=RiskAuditV2{intent_id:bad.order_id.clone(),
            market:bad.market.clone(),at_ns:101,
            proposed_notional_micro:500_100,
            current_market_micro:0,current_portfolio_micro:0,
            observed_depth_micro:0,
            quote_received_ns:bad.quote_received_ns,max_quote_age_ns:0,
            guard_configuration_identity:guard.configuration_identity(),
            outcome_code:"ALLOW".into()};
        // The paper reducer cannot authenticate a forged risk decision;
        // recovery MUST independently replay and reject it.
        journal.append(Event::ArenaV2{portfolio:"A".into(),seq:1,
            at_ns:101,action:ActionV2::RiskAssessed{
                intent:bad,audit:a}}).unwrap();
        assert!(matches!(replay_guard_from_journal(&journal,"A",&guard),
            Err(Error::RiskReplayMismatch)));
    }
    #[test]fn legacy_unit_guard_is_refused(){
        let folder=TempDir::new().unwrap();
        let mut journal=Journal::open(folder.path().join("paper")).unwrap();
        let mut guard=Guard::new(limits()).unwrap();
        assert!(matches!(preflight_and_append(&mut journal,&mut guard,"A",1,
            101,intent(),1_000_000,100),Err(Error::InvalidUnit)));
    }
}

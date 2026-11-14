//! Deterministic one-cycle paper pipeline (never live orders).
//! Frozen decision -> evidence -> risk -> durable paper submit -> modeled
//! execution -> durable fills/expiry -> pending settlement. Not unattended.
use crate::Mode;
use phoenix_strategies::frozen_registry::{
    Definition,DecisionContext,Action,decide};
use phoenix_strategies::FrozenSpec;
use phoenix_source_loader::multi_market::MarketBatchOutcome;
use phoenix_sim::historical::{
    LimitIntent,FractionalBook,SourceWindowEvidence,TradeTape,LiquidityRole,
    HistoricalFeeSource,simulate,simulate_source_window};
use phoenix_paper_journal::{Journal,Event,Side as PaperSide};
use phoenix_paper_journal::arena_v2::{
    IntentV2,ActionV2,modeled_fill_actions};
use phoenix_paper_journal::risk_bridge::{preflight_and_append,Outcome as RiskOutcome};
use phoenix_risk_guard::Guard;
use phoenix_domain::Side;
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum Stage{Abstained,SourceBlocked,RiskRejected,NoFill,
    PendingSettlement,PartialModeled,DurableFailure}
#[derive(Debug,Clone)]
pub struct Outcome{
    pub stage:Stage,pub market:String,pub strategy_version_sha256:String,
    pub strategy_id:String,pub modeled_quantity_hundredths:u64,
    pub order_id:Option<String>,pub reason:String,
    pub independently_authenticated_source:bool,
    pub actual_venue_fill_verified:bool,
    pub actual_order_submitted:bool,
}
#[derive(Debug)]
pub enum Error{InvalidCycle,WrongMode,UnknownPortfolio,InvalidSignal,
    InvalidExecution,Journal(phoenix_paper_journal::Error),
    Risk(String),Overflow}
#[derive(Debug,Clone)]
pub struct CycleConfig{
    pub portfolio:String,pub order_id:String,
    pub submitted_hundredths:u64,pub latency_ns:i64,
    pub quote_max_age_ns:i64,pub deadline_ns:i64,
    pub maker_extra_queue_hundredths:u64,
    pub fee_schedule_sha256:String,pub contract_rules_sha256:String,
    pub role:LiquidityRole,
    pub fee_budget_micro:i128,pub quote_depth_micro:i128,
    pub synthetic_fixture_only:bool,
}
fn digest(s:&str)->bool{s.len()==64&&s.bytes().all(|b|b.is_ascii_hexdigit())}
fn paper_side(s:Side)->PaperSide{
    match s{Side::Yes=>PaperSide::Yes,Side::No=>PaperSide::No}
}
fn next_sequence(j:&Journal,portfolio:&str)->Result<u64,Error>{
    j.state(portfolio).and_then(|s|s.arena_v2.as_ref()
        .map(|_|s.next_seq)).ok_or(Error::UnknownPortfolio)
}
fn append(j:&mut Journal,portfolio:&str,at_ns:i64,
    action:ActionV2)->Result<(),Error>{
    let seq=next_sequence(j,portfolio)?;
    j.append(Event::ArenaV2{portfolio:portfolio.into(),seq,
        at_ns,action}).map_err(Error::Journal)
}
fn base(config:&CycleConfig,context:&DecisionContext,
    strategy:&Definition)->Outcome{
    Outcome{stage:Stage::Abstained,
        market:context.market.clone(),strategy_id:strategy.id.clone(),
        strategy_version_sha256:strategy.canonical_sha256().unwrap_or_default(),
        modeled_quantity_hundredths:0,order_id:None,
        reason:"NOT_EVALUATED".into(),
        independently_authenticated_source:false,
        actual_venue_fill_verified:false,actual_order_submitted:false}
}
/// Execute exactly ONE synthetic or source-evidence-gated *paper* cycle.
/// Must be invoked only after portfolio creation and isolated risk-guard init.
/// No automatic collector/service/scheduler or exchange connection exists.
pub fn run_cycle(
    mode:Mode,config:&CycleConfig,market:&MarketBatchOutcome,
    strategy:&Definition,context:&DecisionContext,
    legacy_control:Option<&FrozenSpec>,
    books:&[FractionalBook],tape:TradeTape<'_>,
    strict_source:Option<&SourceWindowEvidence>,
    historical_fees:&impl HistoricalFeeSource,
    guard:&mut Guard,journal:&mut Journal,
)->Result<Outcome,Error>{
    if !matches!(mode,Mode::PaperOnly|Mode::SyntheticFixture|
        Mode::FinalizedHistory)||
        config.portfolio.is_empty()||config.order_id.is_empty()||
        config.submitted_hundredths==0||config.latency_ns<0||
        config.quote_max_age_ns<0||config.fee_budget_micro<0||
        config.quote_depth_micro<0||
        !digest(&config.fee_schedule_sha256)||
        !digest(&config.contract_rules_sha256)||
        context.market!=market.ticker||context.market!=
            strict_source.map_or(context.market.as_str(),|s|s.market.as_str()){
        return Err(Error::InvalidCycle);
    }
    if !config.synthetic_fixture_only&&
        (strict_source.is_none()||!market.historical_execution_eligible||
         !market.report_exclusions.is_empty()){
        return Ok(Outcome{stage:Stage::SourceBlocked,
            reason:"HISTORICAL_SOURCE_PROVENANCE_NOT_AUTHENTICATED".into(),
            ..base(config,context,strategy)});
    }
    if mode!=Mode::SyntheticFixture&&config.synthetic_fixture_only{
        return Err(Error::WrongMode);
    }
    let trace=decide(strategy,context,legacy_control)
        .map_err(|_|Error::InvalidSignal)?;
    let mut result=base(config,context,strategy);
    result.strategy_version_sha256=trace.version_sha256;
    let (side,limit)=match trace.action{
        Action::Abstain=>{
            append(journal,&config.portfolio,context.decision_ns,
                ActionV2::NoTrade{reason:trace.reason.clone()})?;
            result.reason=trace.reason;
            return Ok(result);
        },
        Action::ProposedBuy{side,limit}=>(side,limit),
    };
    let arrival_ns=context.decision_ns.checked_add(config.latency_ns)
        .ok_or(Error::Overflow)?;
    if config.deadline_ns<arrival_ns{return Err(Error::InvalidCycle);}
    let intent=LimitIntent{
        market:context.market.clone(),strategy_order_id:config.order_id.clone(),
        side,role:config.role, // maker requires independent complete queue tape
        quantity_hundredths:config.submitted_hundredths,
        limit_price:limit,signal_received_ns:context.book.received_ns,
        decision_ns:context.decision_ns,arrival_ns,
        deadline_ns:config.deadline_ns,cancelled_at_ns:None,
        max_quote_age_ns:config.quote_max_age_ns,
        maker_queue_ahead_extra_hundredths:config.maker_extra_queue_hundredths,
    };
    // Market-manifest SHA is not a contract-rules SHA.
    let source_book=books.iter().filter(|b|b.market==context.market&&
        b.received_ns<=arrival_ns).max_by_key(|b|b.received_ns);
    let source_sha=source_book.map(|x|x.snapshot_sha256.clone())
        .unwrap_or_default();
    let order=IntentV2{
        order_id:config.order_id.clone(),market:context.market.clone(),
        rules_sha256:config.contract_rules_sha256.clone(),
        strategy_version_sha256:result.strategy_version_sha256.clone(),
        source_sha256:source_sha,
        fee_schedule_sha256:config.fee_schedule_sha256.clone(),
        side:paper_side(side),limit_ticks:limit.ticks(),
        quantity_hundredths:config.submitted_hundredths,
        fee_budget_micro:config.fee_budget_micro,
        quote_received_ns:context.book.received_ns,
        deadline_ns:config.deadline_ns,
    };
    let seq=next_sequence(journal,&config.portfolio)?;
    let accepted=preflight_and_append(journal,guard,
        &config.portfolio,seq,context.decision_ns,order,
        config.quote_depth_micro,config.quote_max_age_ns)
        .map_err(|e|Error::Risk(format!("{e:?}")))?;
    match accepted{
        RiskOutcome::Rejected(block)=>{
            result.stage=Stage::RiskRejected;
            result.reason=format!("RISK_BLOCK_{block:?}");
            return Ok(result);
        },
        RiskOutcome::Accepted=>{},
    }
    result.order_id=Some(config.order_id.clone());
    // No source book from a market-only quote can be automatically
    // considered executable. All outcomes are MODELED not exchange fills.
    let modeled=if config.synthetic_fixture_only{
        simulate(&intent,books,tape,historical_fees)
    }else{
        simulate_source_window(&intent,books,tape,
            strict_source.ok_or(Error::InvalidCycle)?,historical_fees)
    };
    let modeled=match modeled{
        Ok(value)=>value,
        Err(e)=>{
            append(journal,&config.portfolio,arrival_ns,
                ActionV2::Cancel{order_id:config.order_id.clone(),
                    reason:format!("EXECUTION_EVIDENCE_BLOCKED_{e:?}")})?;
            result.stage=Stage::SourceBlocked;
            result.reason=format!("MODEL_BLOCK_{e:?}");
            return Ok(result);
        }
    };
    let fills=modeled_fill_actions(&config.order_id,&modeled)
        .map_err(|_|Error::InvalidExecution)?;
    let total_fee_micro=modeled.modeled_fees_centicents
        .checked_mul(100).ok_or(Error::Overflow)?;
    if total_fee_micro>config.fee_budget_micro||
        modeled.quote_source_sha256.as_deref()!=Some(source_sha.as_str())||
        modeled.modeled_fills.iter().any(|f|
            f.fee_schedule_sha256!=config.fee_schedule_sha256){
        append(journal,&config.portfolio,arrival_ns,
            ActionV2::Cancel{order_id:config.order_id.clone(),
                reason:"MODEL_FEE_OR_SOURCE_MISMATCH".into()})?;
        result.stage=Stage::SourceBlocked;
        result.reason="MODEL_FEE_OR_SOURCE_MISMATCH".into();
        return Ok(result);
    }
    for fill in fills{
        append(journal,&config.portfolio,arrival_ns,fill)?;
    }
    result.modeled_quantity_hundredths=modeled.filled_hundredths;
    if modeled.unfilled_hundredths>0{
        append(journal,&config.portfolio,
            config.deadline_ns.max(arrival_ns),
            ActionV2::Expire{order_id:config.order_id.clone()})?;
    }
    result.stage=if modeled.filled_hundredths==0{Stage::NoFill}
        else if modeled.unfilled_hundredths>0{Stage::PartialModeled}
        else{Stage::PendingSettlement};
    result.reason="MODELED_EXECUTION_PENDING_OFFICIAL_SETTLEMENT".into();
    Ok(result)
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]fn safety_flags_cannot_turn_a_paper_cycle_into_live_order(){
        let cfg=CycleConfig{portfolio:"P".into(),order_id:"o".into(),
            submitted_hundredths:100,latency_ns:0,
            quote_max_age_ns:1,deadline_ns:1,
            maker_extra_queue_hundredths:0,
            fee_schedule_sha256:"a".repeat(64),
            contract_rules_sha256:"b".repeat(64),role:LiquidityRole::Taker,
            fee_budget_micro:100,quote_depth_micro:100,
            synthetic_fixture_only:true};
        assert!(!base(&cfg,&DecisionContext{
            market:"M".into(),event:"E".into(),series:"S".into(),
            decision_ns:1,book:phoenix_domain::Book{
                market:"M".into(),yes_bids:vec![],no_bids:vec![],
                exchange_ns:1,received_ns:1,sequence:1,
                session_id:"mock".into()},signals:vec![],random_seed:None},
            &fixture_definition()).actual_order_submitted);
    }
    fn fixture_definition()->Definition{
        use phoenix_strategies::frozen_registry::{Origin,Lifecycle,Control};
        Definition{id:"control".into(),family:"no-trade".into(),
            model_version:"v1".into(),lineage_parent:None,
            source_id:"synthetic".into(),source_document_sha256:None,
            code_sha256:Some("a".repeat(64)),config_sha256:Some("b".repeat(64)),
            manifest_sha256:"c".repeat(64),registry_version:"r1".into(),
            parameters:Default::default(),supported_series:Default::default(),
            required_kalshi_features:Default::default(),
            required_perps_features:Default::default(),timeframe_ns:None,
            entry_rule:None,exit_rule:None,risk_rule:None,sizing_rule:None,
            fee_rule:None,execution_model:None,history_requirements:vec![],
            frozen_at_utc:None,retired_at_utc:None,origin:Origin::Control,
            lifecycle:Lifecycle::Frozen,control:Some(Control::NoTrade)}
    }
}

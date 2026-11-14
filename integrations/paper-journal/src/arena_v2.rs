//! V2 fractional, micro-USD paper arena reducer.
//! The durable writer and replay remain paper_journal::Journal. This reducer
//! NEVER writes a second ledger or connects to an order venue.
use super::Side;
use serde::{Deserialize,Serialize};
use std::collections::{BTreeMap,BTreeSet};
pub type MicroUsd=i128; // 1/1,000,000 US dollar
pub const WHOLE_CONTRACT_HUNDREDTHS:u64=100;
pub const PAYOUT_TICKS_MICRO:u16=10_000; // qty(1e-2)*p(1e-4) -> USD(1e-6)
// Same finite portfolio decision budget as existing risk-guard identity cap.
// The reducer clones proposed state before durable publication.
const MAX_ORDERS:usize=4_096;
fn valid_hash(s:&str)->bool{
    s.len()==64&&s.bytes().all(|b|b.is_ascii_hexdigit())
}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum OrderStatus{Pending,Partial,Filled,Cancelled,Expired,Invalidated}
#[derive(Debug,Clone,PartialEq,Eq,Serialize,Deserialize)]
pub struct IntentV2{
    pub order_id:String,pub market:String,pub rules_sha256:String,
    pub strategy_version_sha256:String,pub source_sha256:String,
    pub fee_schedule_sha256:String,pub side:Side,pub limit_ticks:u16,
    pub quantity_hundredths:u64,
    #[serde(with="super::money_as_string")]
    pub fee_budget_micro:MicroUsd,
    pub quote_received_ns:i64,pub deadline_ns:i64,
}
/// Risk evidence is part of a *single* already-durable journal event.
/// Exact preflight inputs are serialized to allow deterministic guard replay.
#[derive(Debug,Clone,PartialEq,Eq,Serialize,Deserialize)]
pub struct RiskAuditV2{
    pub intent_id:String,pub market:String,pub at_ns:i64,
    #[serde(with="super::money_as_string")]
    pub proposed_notional_micro:MicroUsd,
    #[serde(with="super::money_as_string")]
    pub current_market_micro:MicroUsd,
    #[serde(with="super::money_as_string")]
    pub current_portfolio_micro:MicroUsd,
    #[serde(with="super::money_as_string")]
    pub observed_depth_micro:MicroUsd,
    pub quote_received_ns:i64,pub max_quote_age_ns:i64,
    pub guard_configuration_identity:String,pub outcome_code:String,
}
#[derive(Debug,Clone,PartialEq,Eq,Serialize,Deserialize)]
#[serde(tag="action",rename_all="snake_case")]
pub enum ActionV2{
    RiskAssessed{intent:IntentV2,audit:RiskAuditV2},
    Submit{intent:IntentV2},
    Reject{order_id:String,reason:String},
    Fill{order_id:String,fill_id:String,
        quantity_hundredths:u64,price_ticks:u16,
        #[serde(with="super::money_as_string")]
        fee_micro:MicroUsd,
        fee_schedule_sha256:String,source_sha256:String},
    Cancel{order_id:String,reason:String},
    Expire{order_id:String},
    Replace{order_id:String,replacement:IntentV2,reason:String},
    Reduce{order_id:String,quantity_hundredths:u64,
        exit_price_ticks:u16,
        #[serde(with="super::money_as_string")]
        exit_fee_micro:MicroUsd,source_sha256:String},
    SettleClaim{order_id:String,winner:Side,
        official_response_sha256:String,rules_sha256:String},
    Invalidate{order_id:String,reason:String},
    NoTrade{reason:String},
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct OrderV2{
    pub intent:IntentV2,pub status:OrderStatus,
    pub filled_hundredths:u64,pub reserved_micro:MicroUsd,
    pub remaining_fee_budget_micro:MicroUsd,
    pub seen_fill_ids:BTreeSet<String>,
    pub paid_fees_micro:MicroUsd,pub realized_reduction:bool,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct PositionV2{
    pub market:String,pub rules_sha256:String,pub side:Side,
    pub quantity_hundredths:u64,pub open_cost_micro:MicroUsd,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct PortfolioV2{
    pub initial_cash_micro:MicroUsd,pub available_cash_micro:MicroUsd,
    pub reserved_cash_micro:MicroUsd,pub realized_net_micro:MicroUsd,
    pub cumulative_fees_micro:MicroUsd,
    pub orders:BTreeMap<String,OrderV2>,
    pub positions:BTreeMap<String,PositionV2>,
    pub completed:BTreeSet<String>,pub rejected:BTreeMap<String,String>,
    pub pending_settlement_count:u64,pub modeled_settlements:u64,
    pub invalidated:u64,pub skipped:u64,
    pub source_evidence_authenticated_by_journal:bool,
    /// V2 legacy direct Submit/Reject are allowed for offline fixtures, but
    /// cannot be promoted into a restart-safe guarded portfolio.
    pub risk_history_complete:bool,
    pub risk_history:Vec<RiskAuditV2>,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum Error{InvalidIntent,InvalidEvent,DuplicateOrder,UnknownOrder,
    InsufficientFunds,MissingFee,InconsistentSource,InvalidQuantity,
    InvalidPrice,UnknownSettlement,Arithmetic,AlreadyCompleted,
    InexactCostBasis,Capacity,InvalidReversal,ConservationFailure}
impl PortfolioV2{
    pub fn new(initial_cash_micro:MicroUsd)->Result<Self,Error>{
        if initial_cash_micro<0{return Err(Error::InsufficientFunds);}
        Ok(Self{initial_cash_micro,available_cash_micro:initial_cash_micro,
            reserved_cash_micro:0,realized_net_micro:0,cumulative_fees_micro:0,
            orders:BTreeMap::new(),positions:BTreeMap::new(),
            completed:BTreeSet::new(),rejected:BTreeMap::new(),
            pending_settlement_count:0,modeled_settlements:0,
            invalidated:0,skipped:0,source_evidence_authenticated_by_journal:false,
            risk_history_complete:true,risk_history:Vec::new()})
    }
    pub fn unrealized_net_micro(&self)->Option<MicroUsd>{
        // No contemporaneous independently verified liquidation price here.
        None
    }
    pub fn total_invested_micro(&self)->Result<MicroUsd,Error>{
        self.positions.values().try_fold(0i128,|sum,p|
            sum.checked_add(p.open_cost_micro).ok_or(Error::Arithmetic))
    }
    pub fn conservation_check(&self)->Result<(),Error>{
        if self.available_cash_micro<0||self.reserved_cash_micro<0||
            self.orders.len()>MAX_ORDERS||self.risk_history.len()>4096{
            return Err(Error::ConservationFailure);
        }
        let expected=self.orders.values().try_fold(0i128,|total,o|
            total.checked_add(o.reserved_micro).ok_or(Error::Arithmetic))?;
        if expected!=self.reserved_cash_micro{
            return Err(Error::ConservationFailure);
        }
        // Open position cost + cash + reservation must equal starting
        // capital + explicitly realized modeled P&L, with fees included in
        // the cost basis and realized reductions.
        let holdings=self.total_invested_micro()?;
        let total=self.available_cash_micro
            .checked_add(self.reserved_cash_micro)
            .and_then(|v|v.checked_add(holdings)).ok_or(Error::Arithmetic)?;
        let expected=self.initial_cash_micro.checked_add(self.realized_net_micro)
            .ok_or(Error::Arithmetic)?;
        if total!=expected{return Err(Error::ConservationFailure);}
        Ok(())
    }
    fn accepted(intent:&IntentV2,at_ns:i64)->Result<MicroUsd,Error>{
        if intent.order_id.is_empty()||intent.market.is_empty()||
            !valid_hash(&intent.rules_sha256)||
            !valid_hash(&intent.source_sha256)||
            !valid_hash(&intent.fee_schedule_sha256)||
            !valid_hash(&intent.strategy_version_sha256)||
            intent.limit_ticks>10_000||intent.quantity_hundredths==0||
            intent.quote_received_ns>at_ns||intent.quote_received_ns<=0||
            intent.deadline_ns<=at_ns||intent.fee_budget_micro<0{
            return Err(Error::InvalidIntent);
        }
        i128::from(intent.quantity_hundredths)
            .checked_mul(i128::from(intent.limit_ticks))
            .and_then(|x|x.checked_add(intent.fee_budget_micro))
            .ok_or(Error::Arithmetic)
    }
    fn submit(&mut self,i:IntentV2,at_ns:i64)->Result<(),Error>{
        if self.orders.len()>=MAX_ORDERS{return Err(Error::Capacity);}
        if self.orders.contains_key(&i.order_id)||
            self.completed.contains(&i.order_id)||
            self.rejected.contains_key(&i.order_id){
            return Err(Error::DuplicateOrder);
        }
        let reserve=Self::accepted(&i,at_ns)?;
        if reserve>self.available_cash_micro{
            return Err(Error::InsufficientFunds);
        }
        self.available_cash_micro-=reserve;
        self.reserved_cash_micro=self.reserved_cash_micro.checked_add(reserve)
            .ok_or(Error::Arithmetic)?;
        self.orders.insert(i.order_id.clone(),OrderV2{
            remaining_fee_budget_micro:i.fee_budget_micro,intent:i,
            status:OrderStatus::Pending,filled_hundredths:0,
            reserved_micro:reserve,seen_fill_ids:BTreeSet::new(),
            paid_fees_micro:0,realized_reduction:false});
        self.pending_settlement_count+=1;Ok(())
    }
    fn cancel(&mut self,id:&str,status:OrderStatus)->Result<(),Error>{
        let o=self.orders.get_mut(id).ok_or(Error::UnknownOrder)?;
        if !matches!(o.status,OrderStatus::Pending|OrderStatus::Partial){
            return Err(Error::AlreadyCompleted);
        }
        self.available_cash_micro=self.available_cash_micro
            .checked_add(o.reserved_micro).ok_or(Error::Arithmetic)?;
        self.reserved_cash_micro=self.reserved_cash_micro
            .checked_sub(o.reserved_micro).ok_or(Error::Arithmetic)?;
        o.reserved_micro=0;o.remaining_fee_budget_micro=0;
        o.status=status;
        if o.filled_hundredths==0{
            self.pending_settlement_count=self.pending_settlement_count
                .checked_sub(1).ok_or(Error::Arithmetic)?;
            self.completed.insert(id.into());
        }
        Ok(())
    }
    pub fn apply(&mut self,action:&ActionV2,at_ns:i64)->Result<(),Error>{
        // State changes are transactional even in nonjournal test callers.
        let mut pending=self.clone();
        pending.apply_inner(action,at_ns)?;
        pending.conservation_check()?;
        *self=pending;
        Ok(())
    }
    fn apply_inner(&mut self,action:&ActionV2,at_ns:i64)->Result<(),Error>{
        if at_ns<=0{return Err(Error::InvalidEvent);}
        match action{
            ActionV2::RiskAssessed{intent,audit}=>{
                if self.risk_history.len()>=4096||
                    audit.intent_id!=intent.order_id||
                    audit.market!=intent.market||audit.at_ns!=at_ns||
                    audit.quote_received_ns!=intent.quote_received_ns||
                    audit.max_quote_age_ns<0||
                    audit.observed_depth_micro<0||
                    audit.guard_configuration_identity.is_empty()||
                    audit.guard_configuration_identity.len()>512||
                    audit.outcome_code.len()>128||
                    audit.outcome_code.is_empty()||
                    audit.proposed_notional_micro!=Self::accepted(intent,at_ns)?||
                    audit.current_market_micro<0||
                    audit.current_portfolio_micro<0||
                    audit.current_market_micro>audit.current_portfolio_micro{
                    return Err(Error::InvalidEvent);
                }
                // Recompute exposure from the actual PRE-DECISION journal
                // state. A caller cannot forge a smaller exposure to bypass
                // the modeled market/portfolio cap and survive restart.
                let market_positions=self.positions.values()
                    .filter(|p|p.market==intent.market)
                    .try_fold(0i128,|acc,p|
                        acc.checked_add(p.open_cost_micro).ok_or(Error::Arithmetic))?;
                let market_reserved=self.orders.values()
                    .filter(|o|o.intent.market==intent.market)
                    .try_fold(0i128,|acc,o|
                        acc.checked_add(o.reserved_micro).ok_or(Error::Arithmetic))?;
                let global_reserve=self.reserved_cash_micro;
                let actual_market=market_positions.checked_add(market_reserved)
                    .ok_or(Error::Arithmetic)?;
                let actual_portfolio=self.total_invested_micro()?
                    .checked_add(global_reserve).ok_or(Error::Arithmetic)?;
                if audit.current_market_micro!=actual_market||
                    audit.current_portfolio_micro!=actual_portfolio{
                    return Err(Error::InvalidEvent);
                }
                if audit.outcome_code=="ALLOW"{
                    self.submit(intent.clone(),at_ns)?;
                }else if audit.outcome_code.starts_with("BLOCK_"){
                    if self.orders.contains_key(&intent.order_id)||
                        self.rejected.contains_key(&intent.order_id)||
                        self.completed.contains(&intent.order_id){
                        return Err(Error::DuplicateOrder);
                    }
                    self.rejected.insert(intent.order_id.clone(),
                        format!("RISK_GUARD_{}",audit.outcome_code));
                }else{return Err(Error::InvalidEvent);}
                self.risk_history.push(audit.clone());
            },
            ActionV2::Submit{intent}=>{
                self.risk_history_complete=false;
                self.submit(intent.clone(),at_ns)?;
            },
            ActionV2::Reject{order_id,reason}=>{
                self.risk_history_complete=false;
                if order_id.is_empty()||reason.trim().is_empty()||
                    self.rejected.contains_key(order_id)||
                    self.orders.contains_key(order_id){
                    return Err(Error::DuplicateOrder);
                }
                self.rejected.insert(order_id.clone(),reason.clone());
            },
            ActionV2::Fill{order_id,fill_id,quantity_hundredths,
                price_ticks,fee_micro,fee_schedule_sha256,source_sha256}=>{
                if fill_id.is_empty()||*quantity_hundredths==0||
                    *price_ticks>10_000||*fee_micro<0{
                    return Err(Error::InvalidEvent);
                }
                let o=self.orders.get_mut(order_id).ok_or(Error::UnknownOrder)?;
                if !matches!(o.status,OrderStatus::Pending|OrderStatus::Partial)||
                    at_ns>o.intent.deadline_ns{
                    return Err(Error::AlreadyCompleted);
                }
                if !o.seen_fill_ids.insert(fill_id.clone()){
                    return Err(Error::DuplicateOrder);
                }
                if fee_schedule_sha256!=&o.intent.fee_schedule_sha256||
                    !valid_hash(source_sha256)||
                    source_sha256!=&o.intent.source_sha256{
                    return Err(Error::InconsistentSource);
                }
                let remaining=o.intent.quantity_hundredths-o.filled_hundredths;
                if *quantity_hundredths>remaining||
                    *price_ticks>o.intent.limit_ticks||
                    *fee_micro>o.remaining_fee_budget_micro{
                    return Err(Error::InvalidQuantity);
                }
                let qty=i128::from(*quantity_hundredths);
                let spent=qty.checked_mul(i128::from(*price_ticks))
                    .and_then(|x|x.checked_add(*fee_micro))
                    .ok_or(Error::Arithmetic)?;
                let reserved_decrement=qty.checked_mul(i128::from(o.intent.limit_ticks))
                    .and_then(|x|x.checked_add(*fee_micro))
                    .ok_or(Error::Arithmetic)?;
                let improvement=reserved_decrement.checked_sub(spent)
                    .ok_or(Error::Arithmetic)?;
                o.reserved_micro=o.reserved_micro.checked_sub(reserved_decrement)
                    .ok_or(Error::Arithmetic)?;
                self.reserved_cash_micro=self.reserved_cash_micro
                    .checked_sub(reserved_decrement).ok_or(Error::Arithmetic)?;
                self.available_cash_micro=self.available_cash_micro
                    .checked_add(improvement).ok_or(Error::Arithmetic)?;
                self.cumulative_fees_micro=self.cumulative_fees_micro
                    .checked_add(*fee_micro).ok_or(Error::Arithmetic)?;
                o.remaining_fee_budget_micro-=*fee_micro;
                o.paid_fees_micro=o.paid_fees_micro.checked_add(*fee_micro)
                    .ok_or(Error::Arithmetic)?;
                o.filled_hundredths+=*quantity_hundredths;
                o.status=if o.filled_hundredths==o.intent.quantity_hundredths{
                    OrderStatus::Filled
                }else{OrderStatus::Partial};
                let pos=self.positions.entry(order_id.clone()).or_insert_with(||PositionV2{
                    market:o.intent.market.clone(),rules_sha256:o.intent.rules_sha256.clone(),
                    side:o.intent.side,quantity_hundredths:0,open_cost_micro:0});
                pos.quantity_hundredths=pos.quantity_hundredths
                    .checked_add(*quantity_hundredths).ok_or(Error::Arithmetic)?;
                pos.open_cost_micro=pos.open_cost_micro
                    .checked_add(spent).ok_or(Error::Arithmetic)?;
                if o.status==OrderStatus::Filled{
                    // Residual fee budget is not a charge. Release reservation.
                    self.available_cash_micro=self.available_cash_micro
                        .checked_add(o.reserved_micro).ok_or(Error::Arithmetic)?;
                    self.reserved_cash_micro=self.reserved_cash_micro
                        .checked_sub(o.reserved_micro).ok_or(Error::Arithmetic)?;
                    o.reserved_micro=0;o.remaining_fee_budget_micro=0;
                }
            },
            ActionV2::Cancel{order_id,reason}=>{
                if reason.trim().is_empty(){return Err(Error::InvalidEvent);}
                self.cancel(order_id,OrderStatus::Cancelled)?;
            },
            ActionV2::Expire{order_id}=>{
                let o=self.orders.get(order_id).ok_or(Error::UnknownOrder)?;
                if at_ns<o.intent.deadline_ns{return Err(Error::InvalidEvent);}
                self.cancel(order_id,OrderStatus::Expired)?;
            },
            ActionV2::Replace{order_id,replacement,reason}=>{
                // Historical direct replacements are replayable, but not
                // reconstructible as guarded decisions. A future guarded
                // replacement must persist its own RiskAssessed witness.
                self.risk_history_complete=false;
                if reason.trim().is_empty()||order_id==&replacement.order_id{
                    return Err(Error::InvalidEvent);
                }
                self.cancel(order_id,OrderStatus::Cancelled)?;
                self.submit(replacement.clone(),at_ns)?;
            },
            ActionV2::Reduce{order_id,quantity_hundredths,
                exit_price_ticks,exit_fee_micro,source_sha256}=>{
                if *quantity_hundredths==0||*exit_price_ticks>10_000||
                    *exit_fee_micro<0||!valid_hash(source_sha256){
                    return Err(Error::InvalidEvent);
                }
                let p=self.positions.get_mut(order_id).ok_or(Error::UnknownOrder)?;
                if *quantity_hundredths>p.quantity_hundredths{
                    return Err(Error::InvalidQuantity);
                }
                let original=p.quantity_hundredths;
                let share=if *quantity_hundredths==original{
                    p.open_cost_micro
                }else{
                    let weighted=p.open_cost_micro
                        .checked_mul(i128::from(*quantity_hundredths))
                        .ok_or(Error::Arithmetic)?;
                    if weighted%i128::from(original)!=0{
                        return Err(Error::InexactCostBasis);
                    }
                    weighted/i128::from(original)
                };
                let proceeds=i128::from(*quantity_hundredths)
                    .checked_mul(i128::from(*exit_price_ticks))
                    .and_then(|v|v.checked_sub(*exit_fee_micro))
                    .ok_or(Error::Arithmetic)?;
                if proceeds<0{return Err(Error::InsufficientFunds);}
                self.available_cash_micro=self.available_cash_micro
                    .checked_add(proceeds).ok_or(Error::Arithmetic)?;
                self.realized_net_micro=self.realized_net_micro
                    .checked_add(proceeds-share).ok_or(Error::Arithmetic)?;
                self.cumulative_fees_micro=self.cumulative_fees_micro
                    .checked_add(*exit_fee_micro).ok_or(Error::Arithmetic)?;
                if let Some(o)=self.orders.get_mut(order_id){o.realized_reduction=true;}
                p.quantity_hundredths-=*quantity_hundredths;
                p.open_cost_micro-=share;
                if p.quantity_hundredths==0{
                    self.positions.remove(order_id);
                    self.pending_settlement_count-=1;
                    self.completed.insert(order_id.clone());
                }
            },
            ActionV2::SettleClaim{order_id,winner,
                official_response_sha256,rules_sha256}=>{
                if !valid_hash(official_response_sha256)||
                    !valid_hash(rules_sha256){return Err(Error::UnknownSettlement);}
                let pos=self.positions.get(order_id).ok_or(Error::UnknownOrder)?;
                if &pos.rules_sha256!=rules_sha256{
                    return Err(Error::InconsistentSource);
                }
                let qty=pos.quantity_hundredths;
                let paid=pos.open_cost_micro;
                let payout=if pos.side==*winner{
                    i128::from(qty).checked_mul(i128::from(PAYOUT_TICKS_MICRO))
                        .ok_or(Error::Arithmetic)?
                }else{0};
                // Release open residual order reservation, if any.
                if let Some(o)=self.orders.get_mut(order_id){
                    self.available_cash_micro=self.available_cash_micro
                        .checked_add(o.reserved_micro).ok_or(Error::Arithmetic)?;
                    self.reserved_cash_micro=self.reserved_cash_micro
                        .checked_sub(o.reserved_micro).ok_or(Error::Arithmetic)?;
                    o.reserved_micro=0;o.remaining_fee_budget_micro=0;
                    o.status=OrderStatus::Filled;
                }
                self.available_cash_micro=self.available_cash_micro
                    .checked_add(payout).ok_or(Error::Arithmetic)?;
                self.realized_net_micro=self.realized_net_micro
                    .checked_add(payout-paid).ok_or(Error::Arithmetic)?;
                self.positions.remove(order_id);
                self.pending_settlement_count-=1;
                self.modeled_settlements+=1;
                self.completed.insert(order_id.clone());
            },
            ActionV2::Invalidate{order_id,reason}=>{
                if reason.trim().is_empty(){return Err(Error::InvalidEvent);}
                let o=self.orders.get_mut(order_id).ok_or(Error::UnknownOrder)?;
                if self.completed.contains(order_id)||o.status==OrderStatus::Invalidated||
                    o.realized_reduction{
                    return Err(Error::InvalidReversal);
                }
                let invested=self.positions.remove(order_id)
                    .map_or(0,|p|p.open_cost_micro);
                // Retraction of unclosed modeled entries only; do not
                // reverse already realized settlement/exit as a fake refund.
                let refund=invested.checked_add(o.reserved_micro)
                    .ok_or(Error::Arithmetic)?;
                self.available_cash_micro=self.available_cash_micro
                    .checked_add(refund).ok_or(Error::Arithmetic)?;
                self.reserved_cash_micro=self.reserved_cash_micro
                    .checked_sub(o.reserved_micro).ok_or(Error::Arithmetic)?;
                o.reserved_micro=0;o.status=OrderStatus::Invalidated;
                self.pending_settlement_count-=1;
                self.cumulative_fees_micro=self.cumulative_fees_micro
                    .checked_sub(o.paid_fees_micro).ok_or(Error::Arithmetic)?;
                self.invalidated+=1;
                self.completed.insert(order_id.clone());
            },
            ActionV2::NoTrade{reason}=>{
                if reason.trim().is_empty(){return Err(Error::InvalidEvent);}
                self.skipped+=1;
            },
        }
        Ok(())
    }
}
/// Pure integration of Phase 6 fixed-point execution evidence with paper
/// accounting. Caller must submit order first. Each level gets stable
/// sequence-derived fill IDs; repeat attempts safely fail closed.
pub fn modeled_fill_actions(
    order_id:&str,report:&phoenix_sim::historical::ExecutionReport,
)->Result<Vec<ActionV2>,Error>{
    if order_id!=report.strategy_order_id||report.venue_fill_verified||
        report.filled_hundredths>report.submitted_hundredths{
        return Err(Error::InvalidEvent);
    }
    if report.filled_hundredths==0{
        // A legitimately unfilled modeled order has neither monetary cost
        // nor fee nor a fabricated fill/source SHA.
        if !report.modeled_fills.is_empty()||
            report.modeled_cost_micro_usd!=0||
            report.modeled_fees_centicents!=0{
            return Err(Error::InvalidEvent);
        }
        return Ok(Vec::new());
    }
    let source=report.quote_source_sha256.as_ref()
        .ok_or(Error::InconsistentSource)?;
    if !valid_hash(source){return Err(Error::InconsistentSource);}
    let mut out=Vec::new();
    let mut sum=0u64;let mut total_cost=0i128;
    let mut total_fee=0i128;
    for (i,f) in report.modeled_fills.iter().enumerate(){
        if f.quantity_hundredths==0||!valid_hash(&f.fee_schedule_sha256){
            return Err(Error::MissingFee);
        }
        sum=sum.checked_add(f.quantity_hundredths)
            .ok_or(Error::Arithmetic)?;
        if f.cost_micro_usd!=i128::from(f.quantity_hundredths)
            .checked_mul(i128::from(f.price_ticks_1e4))
            .ok_or(Error::Arithmetic)?{
            return Err(Error::Arithmetic);
        }
        total_cost=total_cost.checked_add(f.cost_micro_usd)
            .ok_or(Error::Arithmetic)?;
        if f.fee_centicents<0{return Err(Error::MissingFee);}
        total_fee=total_fee.checked_add(f.fee_centicents)
            .ok_or(Error::Arithmetic)?;
        out.push(ActionV2::Fill{order_id:order_id.into(),
            fill_id:format!("{}:level:{}",order_id,i),
            quantity_hundredths:f.quantity_hundredths,
            price_ticks:f.price_ticks_1e4,fee_micro:f.fee_centicents
                .checked_mul(100).ok_or(Error::Arithmetic)?,
            fee_schedule_sha256:f.fee_schedule_sha256.clone(),
            source_sha256:source.clone()});
    }
    if sum!=report.filled_hundredths||
        total_cost!=report.modeled_cost_micro_usd||
        total_fee!=report.modeled_fees_centicents{
        return Err(Error::InvalidQuantity);
    }
    Ok(out)
}
#[cfg(test)]
mod tests{
    use super::*;
    fn intent(id:&str)->IntentV2{IntentV2{
        order_id:id.into(),market:"M".into(),rules_sha256:"a".repeat(64),
        strategy_version_sha256:"b".repeat(64),source_sha256:"c".repeat(64),
        fee_schedule_sha256:"d".repeat(64),side:Side::Yes,limit_ticks:6000,
        quantity_hundredths:150,fee_budget_micro:300,
        quote_received_ns:90,deadline_ns:300}}
    #[test]fn exact_fractional_cash_reservation_and_reduction(){
        let mut s=PortfolioV2::new(2_000_000).unwrap();
        s.apply(&ActionV2::Submit{intent:intent("o")},100).unwrap();
        assert_eq!(s.reserved_cash_micro,900_300);
        s.apply(&ActionV2::Fill{order_id:"o".into(),
            fill_id:"f1".into(),quantity_hundredths:50,
            price_ticks:5500,fee_micro:100,fee_schedule_sha256:"d".repeat(64),
            source_sha256:"c".repeat(64)},120).unwrap();
        assert_eq!(s.positions["o"].open_cost_micro,275_100);
        assert_eq!(s.orders["o"].filled_hundredths,50);
        s.apply(&ActionV2::Cancel{order_id:"o".into(),
            reason:"synthetic test".into()},130).unwrap();
        assert_eq!(s.reserved_cash_micro,0);
        s.conservation_check().unwrap();
    }
    #[test]fn modeled_execution_report_zero_fills_never_invents_cost_or_fee(){
        use phoenix_sim::historical::{ExecutionReport,LiquidityRole,OrderState};
        let r=ExecutionReport{schema_version:1,market:"M".into(),
            strategy_order_id:"o".into(),role:LiquidityRole::Taker,
            state:OrderState::Unfilled,submitted_hundredths:100,
            filled_hundredths:0,unfilled_hundredths:100,
            modeled_fills:vec![],modeled_cost_micro_usd:0,
            modeled_fees_centicents:0,quote_received_ns:None,
            order_arrival_ns:100,event_deadline_ns:200,
            quote_source_sha256:None,conservative_queue_ahead_hundredths:None,
            latency_ns:0,assumptions:vec![],venue_fill_verified:false};
        assert!(modeled_fill_actions("o",&r).unwrap().is_empty());
        let mut fake=r;fake.modeled_fees_centicents=1;
        assert_eq!(modeled_fill_actions("o",&fake),Err(Error::InvalidEvent));
    }
    #[test]fn zero_cash_phantom_duplicate_and_missing_outcome_blocked(){
        let mut s=PortfolioV2::new(100_000).unwrap();
        assert_eq!(s.apply(&ActionV2::Submit{intent:intent("o")},100),
            Err(Error::InsufficientFunds));
        assert!(s.positions.is_empty());
    }
    #[test]fn invalidation_refunds_partial_fill_and_does_not_leave_phantom_fee(){
        let mut s=PortfolioV2::new(2_000_000).unwrap();
        s.apply(&ActionV2::Submit{intent:intent("o")},100).unwrap();
        s.apply(&ActionV2::Fill{order_id:"o".into(),fill_id:"f1".into(),
            quantity_hundredths:50,price_ticks:5500,fee_micro:100,
            fee_schedule_sha256:"d".repeat(64),
            source_sha256:"c".repeat(64)},110).unwrap();
        assert_eq!(s.cumulative_fees_micro,100);
        s.apply(&ActionV2::Invalidate{order_id:"o".into(),
            reason:"source gap".into()},120).unwrap();
        assert_eq!(s.available_cash_micro,2_000_000);
        assert_eq!(s.reserved_cash_micro,0);
        assert_eq!(s.cumulative_fees_micro,0);
        assert!(s.positions.is_empty());
        assert_eq!(s.invalidated,1);
    }
    #[test]fn replacement_is_atomic_and_duplicate_fills_cannot_spend_twice(){
        let mut s=PortfolioV2::new(3_000_000).unwrap();
        s.apply(&ActionV2::Submit{intent:intent("old")},100).unwrap();
        let replacement=intent("new");
        s.apply(&ActionV2::Replace{order_id:"old".into(),
            replacement,reason:"resubmit lower latency".into()},120).unwrap();
        assert_eq!(s.orders["old"].status,OrderStatus::Cancelled);
        assert_eq!(s.orders["new"].status,OrderStatus::Pending);
        let fill=ActionV2::Fill{order_id:"new".into(),fill_id:"f1".into(),
            quantity_hundredths:50,price_ticks:5500,fee_micro:100,
            fee_schedule_sha256:"d".repeat(64),source_sha256:"c".repeat(64)};
        s.apply(&fill,130).unwrap();
        let before=s.available_cash_micro;
        assert_eq!(s.apply(&fill,131),Err(Error::DuplicateOrder));
        assert_eq!(s.available_cash_micro,before);
    }
    #[test]fn historical_replacement_blocks_unwitnessed_future_risk_promotion(){
        let mut s=PortfolioV2::new(3_000_000).unwrap();
        s.apply(&ActionV2::Submit{intent:intent("old")},100).unwrap();
        assert!(!s.risk_history_complete);
        s.apply(&ActionV2::Replace{order_id:"old".into(),
            replacement:intent("new"),reason:"synthetic".into()},110).unwrap();
        assert!(!s.risk_history_complete);
    }
    #[test]fn forged_exposure_cannot_bypass_journal_state(){
        let mut s=PortfolioV2::new(3_000_000).unwrap();
        s.apply(&ActionV2::Submit{intent:intent("old")},100).unwrap();
        let now=intent("new");
        let a=RiskAuditV2{intent_id:now.order_id.clone(),
            market:now.market.clone(),at_ns:110,
            proposed_notional_micro:900_300,
            current_market_micro:0,current_portfolio_micro:0,
            observed_depth_micro:1_000_000,
            quote_received_ns:90,max_quote_age_ns:100,
            guard_configuration_identity:"configured".into(),
            outcome_code:"ALLOW".into()};
        let before=s.clone();
        assert_eq!(s.apply(&ActionV2::RiskAssessed{
            intent:now,audit:a},110),Err(Error::InvalidEvent));
        assert_eq!(s,before);
    }
    #[test]fn durable_risk_assessment_is_one_event_and_direct_legacy_submit_is_flagged(){
        let mut s=PortfolioV2::new(2_000_000).unwrap();
        let i=intent("reviewed");
        let a=RiskAuditV2{intent_id:i.order_id.clone(),market:i.market.clone(),
            at_ns:100,proposed_notional_micro:900_300,
            current_market_micro:0,current_portfolio_micro:0,
            observed_depth_micro:1_000_000,
            quote_received_ns:90,max_quote_age_ns:20,
            guard_configuration_identity:"unit:configured".into(),
            outcome_code:"ALLOW".into()};
        s.apply(&ActionV2::RiskAssessed{intent:i,audit:a},100).unwrap();
        assert_eq!(s.risk_history.len(),1);
        assert!(s.risk_history_complete);
        s.apply(&ActionV2::Submit{intent:intent("old")},101).unwrap();
        assert!(!s.risk_history_complete);
    }
    #[test]fn unknown_settlement_keeps_position_pending_and_exact_cost_basis(){
        let mut s=PortfolioV2::new(2_000_000).unwrap();
        s.apply(&ActionV2::Submit{intent:intent("o")},100).unwrap();
        s.apply(&ActionV2::Fill{order_id:"o".into(),fill_id:"f1".into(),
            quantity_hundredths:50,price_ticks:5500,fee_micro:100,
            fee_schedule_sha256:"d".repeat(64),source_sha256:"c".repeat(64)},120).unwrap();
        assert!(s.apply(&ActionV2::SettleClaim{order_id:"o".into(),
            winner:Side::Yes,official_response_sha256:"unavailable".into(),
            rules_sha256:"a".repeat(64)},320).is_err());
        assert_eq!(s.pending_settlement_count,1);
        assert!(s.unrealized_net_micro().is_none());
    }
    #[test]fn wrong_basis_allocation_rejected_without_mutating_portfolio(){
        let mut s=PortfolioV2::new(2_000_000).unwrap();
        s.apply(&ActionV2::Submit{intent:intent("o")},100).unwrap();
        s.apply(&ActionV2::Fill{order_id:"o".into(),fill_id:"f1".into(),
            quantity_hundredths:50,price_ticks:5501,fee_micro:101,
            fee_schedule_sha256:"d".repeat(64),source_sha256:"c".repeat(64)},120).unwrap();
        let original=s.clone();
        assert_eq!(s.apply(&ActionV2::Reduce{order_id:"o".into(),
            quantity_hundredths:1,exit_price_ticks:6000,exit_fee_micro:0,
            source_sha256:"e".repeat(64)},140),Err(Error::InexactCostBasis));
        assert_eq!(s,original);
    }
    #[test]fn settlement_claim_does_not_claim_provider_authentication(){
        let mut s=PortfolioV2::new(2_000_000).unwrap();
        s.apply(&ActionV2::Submit{intent:intent("o")},100).unwrap();
        s.apply(&ActionV2::Fill{order_id:"o".into(),
            fill_id:"f1".into(),quantity_hundredths:150,
            price_ticks:5500,fee_micro:300,fee_schedule_sha256:"d".repeat(64),
            source_sha256:"c".repeat(64)},120).unwrap();
        s.apply(&ActionV2::SettleClaim{order_id:"o".into(),
            winner:Side::No,official_response_sha256:"e".repeat(64),
            rules_sha256:"a".repeat(64)},310).unwrap();
        assert_eq!(s.realized_net_micro,-825_300);
        assert_eq!(s.modeled_settlements,1);
        assert!(!s.source_evidence_authenticated_by_journal);
        assert_eq!(s.pending_settlement_count,0);
    }
}

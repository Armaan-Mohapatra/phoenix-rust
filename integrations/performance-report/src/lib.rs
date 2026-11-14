//! Compact deterministic synthetic/research performance summaries.
//! This library never upgrades modeled outcomes into verified real fills.
pub mod experiment_report;
use phoenix_pipeline::{Evaluation,Stage};
use phoenix_research::Split;
use phoenix_domain::EvidenceTier;
use serde::Serialize;
use std::collections::{BTreeMap,BTreeSet};

pub type Money=i128;
#[derive(Debug,Clone)]
pub struct TimedEvaluation {
    pub decided_at_ns:i64,
    pub result:Evaluation,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize)]
pub enum Basis {Synthetic,UnverifiedHistorical,VenueFinalModeled}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum Error {InvalidRun,InvalidRow,ArithmeticOverflow}
#[derive(Debug,Clone,PartialEq,Eq,Serialize,Default)]
pub struct SplitCounts {
    pub discovery:u64,pub validation:u64,pub untouched:u64,
}
#[derive(Debug,Clone,PartialEq,Eq,Serialize)]
pub struct Report {
    pub run_id:String,pub dataset_sha256:String,pub strategy_id:String,
    pub basis:Basis,
    pub decisions:u64,pub unique_events:u64,pub unique_markets:u64,
    pub abstentions:u64,pub unfilled:u64,pub pending_settlements:u64,
    pub settled_modeled:u64,
    pub venue_final_claims:u64,pub independent_tier_claims:u64,
    pub modeled_fees:Money,pub settled_net:Option<Money>,
    pub winning_settled:u64,pub losing_settled:u64,pub flat_settled:u64,
    pub max_drawdown_on_settled:Option<Money>,
    pub split_counts:SplitCounts,
    pub notes:Vec<String>,
}
fn digest(s:&str)->bool{
    s.len()==64&&s.bytes().all(|b|b.is_ascii_hexdigit())
}
pub fn summarize(run_id:&str,dataset_sha256:&str,
    strategy_id:&str,basis:Basis,entries:&[TimedEvaluation])
    ->Result<Report,Error>{
    if run_id.is_empty()||run_id.len()>128||!digest(dataset_sha256)||
        strategy_id.is_empty()||strategy_id.len()>128 {
        return Err(Error::InvalidRun);
    }
    let mut ordered=entries.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|x|(x.decided_at_ns,&x.result.event_id,&x.result.market));
    let mut events=BTreeSet::new();
    let mut markets=BTreeSet::new();
    let mut report=Report{
        run_id:run_id.into(),dataset_sha256:dataset_sha256.into(),
        strategy_id:strategy_id.into(),basis,
        decisions:0,unique_events:0,unique_markets:0,
        abstentions:0,unfilled:0,pending_settlements:0,settled_modeled:0,
        venue_final_claims:0,independent_tier_claims:0,
        modeled_fees:0,settled_net:None,
        winning_settled:0,losing_settled:0,flat_settled:0,
        max_drawdown_on_settled:None,split_counts:SplitCounts::default(),
        notes:vec!["All filled orders are modeled; report does not establish actual venue fills."
            .into(),"Missing settlements remain NULL, never zero P&L.".into()],
    };
    let (mut running,mut high,mut max_dd)=(0i128,0i128,0i128);
    for row in ordered {
        let r=&row.result;
        if row.decided_at_ns<0||r.event_id.is_empty()||r.market.is_empty()||
            r.strategy_id!=strategy_id||r.fees_centicents<0 ||
            (r.stage!=Stage::ModeledSettled &&
                (r.net_pnl_centicents.is_some()||r.tier.is_some()))||
            (r.stage==Stage::ModeledSettled &&
                (r.net_pnl_centicents.is_none()||r.tier.is_none())){
            return Err(Error::InvalidRow);
        }
        report.decisions+=1;events.insert(&r.event_id);markets.insert(&r.market);
        match r.split{
            Split::Discovery=>report.split_counts.discovery+=1,
            Split::Validation=>report.split_counts.validation+=1,
            Split::Untouched=>report.split_counts.untouched+=1,
            Split::Embargo=>return Err(Error::InvalidRow),
        }
        match r.stage{
            Stage::Abstained=>report.abstentions+=1,
            Stage::NoFill=>report.unfilled+=1,
            Stage::PendingSettlement=>report.pending_settlements+=1,
            Stage::ModeledSettled=>{
                report.settled_modeled+=1;
                report.venue_final_claims+=1;
                if r.tier==Some(EvidenceTier::Independent) {
                    // A claim of independent evidence is NOT proof that it is
                    // authentic or independent; this name is intentional.
                    report.independent_tier_claims+=1;
                }
                let net=r.net_pnl_centicents.unwrap();
                report.settled_net=Some(report.settled_net.unwrap_or(0)
                    .checked_add(net).ok_or(Error::ArithmeticOverflow)?);
                running=running.checked_add(net).ok_or(Error::ArithmeticOverflow)?;
                high=high.max(running);
                max_dd=max_dd.max(high.checked_sub(running)
                    .ok_or(Error::ArithmeticOverflow)?);
                if net>0{report.winning_settled+=1;}
                else if net<0{report.losing_settled+=1;}
                else{report.flat_settled+=1;}
            },
        }
        report.modeled_fees=report.modeled_fees.checked_add(r.fees_centicents)
            .ok_or(Error::ArithmeticOverflow)?;
    }
    report.unique_events=events.len() as u64;
    report.unique_markets=markets.len() as u64;
    if report.settled_modeled>0{
        report.max_drawdown_on_settled=Some(max_dd);
    }
    if report.independent_tier_claims>0 {
        report.notes.push("Independent tier was caller-asserted; verify source provenance separately.".into());
    }
    Ok(report)
}
pub fn json(report:&Report)->Result<String,serde_json::Error>{
    serde_json::to_string_pretty(report)
}
#[cfg(test)]
mod tests{
    use super::*;
    fn item(id:&str,stage:Stage,pnl:Option<i128>,tier:Option<EvidenceTier>,
        at:i64)->TimedEvaluation{
        TimedEvaluation{decided_at_ns:at,result:Evaluation{
            event_id:id.into(),market:format!("M-{id}"),strategy_id:"v1".into(),
            split:Split::Validation,stage,modeled_quantity:if pnl.is_some(){1}else{0},
            fees_centicents:if pnl.is_some(){10}else{0},
            net_pnl_centicents:pnl,tier,reason:"synthetic".into(),
        }}
    }
    #[test]fn losses_are_not_filtered_and_pending_is_null(){
        let rows=[
            item("a",Stage::ModeledSettled,Some(-1000),Some(EvidenceTier::VenueFinal),1),
            item("b",Stage::PendingSettlement,None,None,2),
            item("c",Stage::Abstained,None,None,3),
            item("d",Stage::ModeledSettled,Some(300),Some(EvidenceTier::VenueFinal),4),
        ];
        let r=summarize("demo",&"a".repeat(64),"v1",Basis::Synthetic,&rows).unwrap();
        assert_eq!(r.decisions,4);assert_eq!(r.pending_settlements,1);
        assert_eq!(r.settled_modeled,2);assert_eq!(r.settled_net,Some(-700));
        assert_eq!(r.max_drawdown_on_settled,Some(1000));
        assert_eq!(r.losing_settled,1);
        assert!(json(&r).unwrap().contains("-700"));
    }
    #[test]fn all_pending_has_no_pnl_number(){
        let rows=[item("a",Stage::PendingSettlement,None,None,1)];
        let r=summarize("demo",&"a".repeat(64),"v1",Basis::Synthetic,&rows).unwrap();
        assert_eq!(r.settled_net,None);
        assert_eq!(r.max_drawdown_on_settled,None);
        assert!(json(&r).unwrap().contains("\"settled_net\": null"));
    }
    #[test]fn fabricated_pnl_with_no_fill_is_rejected(){
        let rows=[item("a",Stage::NoFill,Some(100),None,1)];
        assert_eq!(summarize("demo",&"a".repeat(64),"v1",Basis::Synthetic,&rows),
            Err(Error::InvalidRow));
    }
    #[test]fn ordering_does_not_change_report(){
        let one=[
            item("a",Stage::ModeledSettled,Some(-3),Some(EvidenceTier::VenueFinal),1),
            item("b",Stage::ModeledSettled,Some(7),Some(EvidenceTier::VenueFinal),2),
        ];
        let two=[one[1].clone(),one[0].clone()];
        assert_eq!(json(&summarize("demo",&"a".repeat(64),"v1",Basis::Synthetic,&one).unwrap()).unwrap(),
            json(&summarize("demo",&"a".repeat(64),"v1",Basis::Synthetic,&two).unwrap()).unwrap());
    }
}

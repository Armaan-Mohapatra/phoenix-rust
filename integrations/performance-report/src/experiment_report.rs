//! Evidence-aware research report adapter using the canonical
//! phoenix_research::experiments metrics; never a second P&L engine.
use phoenix_research::experiments::{
    ExperimentSpec,PositionSample,ForecastSample,Performance,ForecastScore,
    Evidence,ExecutionStatus,summarize_positions,score_forecasts};
use serde::Serialize;
use std::collections::BTreeMap;
#[derive(Debug,Serialize)]
pub struct ExperimentReport{
    pub schema_version:u32,pub experiment_id:String,
    pub experiment_sha256:String,pub dataset_sha256:String,
    pub strategy_id:String,pub modeled:Performance,
    pub forecasting:Option<ForecastScore>,
    pub evidence_tiers:BTreeMap<String,u64>,
    pub execution_outcomes:BTreeMap<String,u64>,
    pub negative_outcomes_included:bool,
    pub pending_settlements_excluded_from_realized_pnl:bool,
    pub actual_venue_fills_verified:bool,
    pub independent_settlements_authenticated_by_reporter:bool,
    pub notes:Vec<String>,
}
#[derive(Debug,PartialEq,Eq)]
pub enum Error{InvalidExperiment,InvalidRow,MetricCalculation,
    ForecastMeasurement}
pub fn summarize_experiment(spec:&ExperimentSpec,
    positions:&[PositionSample],forecasts:&[ForecastSample])
    ->Result<ExperimentReport,Error>{
    let id=spec.fingerprint().map_err(|_|Error::InvalidExperiment)?;
    if positions.iter().any(|p|p.strategy!=spec.strategy_id||
        p.decision_ns<spec.source_start_ns||
        p.decision_ns>spec.source_end_ns){
        return Err(Error::InvalidRow);
    }
    let modeled=summarize_positions(positions).map_err(|_|Error::MetricCalculation)?;
    let forecasting=if forecasts.is_empty(){None}
        else{Some(score_forecasts(forecasts)
            .map_err(|_|Error::ForecastMeasurement)?)};
    let mut tiers=BTreeMap::<String,u64>::new();
    let mut outcomes=BTreeMap::<String,u64>::new();
    for position in positions{
        let tier=match position.evidence{
            Evidence::Synthetic=>"SYNTHETIC",
            Evidence::SourceProvisional=>"SOURCE_PROVISIONAL",
            Evidence::OfficialModeled=>"OFFICIAL_FINAL_MODELED",
            Evidence::IndependentCorroborated=>"INDEPENDENT_CORROBORATED_CLAIM",
        };
        *tiers.entry(tier.into()).or_default()+=1;
        let status=match position.execution{
            ExecutionStatus::Abstain=>"ABSTAIN",
            ExecutionStatus::Rejected=>"REJECTED",
            ExecutionStatus::Nonfill=>"UNFILLED",
            ExecutionStatus::Partial=>"MODELED_PARTIAL",
            ExecutionStatus::Full=>"MODELED_FULL",
        };
        *outcomes.entry(status.into()).or_default()+=1;
    }
    Ok(ExperimentReport{schema_version:1,experiment_id:spec.id.clone(),
        experiment_sha256:id,dataset_sha256:spec.dataset_sha256.clone(),
        strategy_id:spec.strategy_id.clone(),
        modeled,forecasting,evidence_tiers:tiers,execution_outcomes:outcomes,
        negative_outcomes_included:true,
        pending_settlements_excluded_from_realized_pnl:true,
        actual_venue_fills_verified:false,
        independent_settlements_authenticated_by_reporter:false,
        notes:vec![
            "REPORTS_MODELLED_SOURCE_EVIDENCE_NOT_ACTUAL_VENUE_FILLS".into(),
            "SOURCE_EVIDENCE_LABELS_REQUIRE_EXTERNAL_AUTHENTICATION".into(),
            "MISSING_PNL_AND_FEES_ARE_NULL_NOT_ZERO".into(),
            "OOS_STATUS_AND_MULTIPLE_COMPARISONS_ARE_EXPERIMENT_METADATA".into(),
        ]})
}
#[cfg(test)]
mod tests{
    use super::*;use phoenix_research::experiments::{Split,ExecutionStatus};
    #[test]fn empty_run_never_reports_fabricated_profit(){
        let p=ExperimentSpec{id:"e1".into(),parent_id:None,
            dataset_sha256:"a".repeat(64),source_manifest_sha256:"b".repeat(64),
            strategy_id:"s1".into(),config_sha256:"c".repeat(64),
            execution_model_sha256:"d".repeat(64),fee_schedule_sha256:None,
            risk_sha256:"e".repeat(64),source_start_ns:1,source_end_ns:500,
            discovery_end_ns:100,validation_end_ns:200,
            untouched_start_ns:250,embargo_ns:10,selected_markets:vec![],
            random_seed:1,statistical_method:"event cluster".into(),
            status:"PENDING".into(),settlement_evidence:Evidence::Synthetic,
            discovery_selection_sha256:None,failed_reason:None};
        let row=PositionSample{event:"evt".into(),market:"BTC".into(),
            strategy:"s1".into(),split:Split::Validation,
            decision_ns:110,outcome_received_ns:None,
            evidence:Evidence::Synthetic,execution:ExecutionStatus::Nonfill,
            gross_micro_usd:None,fee_micro_usd:None,net_micro_usd:None,
            capital_micro_usd:None,submitted_hundredths:100,
            filled_hundredths:0,side:"yes".into(),
            allowed_for_estimation:false};
        let out=summarize_experiment(&p,&[row],&[]).unwrap();
        assert_eq!(out.modeled.net_micro_usd,None);
        assert!(!out.actual_venue_fills_verified);
        assert_eq!(out.execution_outcomes["UNFILLED"],1);
    }
}

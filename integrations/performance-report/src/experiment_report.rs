//! Evidence-aware research report adapter using the canonical
//! phoenix_research::experiments metrics; never a second P&L engine.
use phoenix_strategies::frozen_registry::{Definition,Origin};
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
/// Separately labels true frozen-code provenance versus experimental
/// derivation. Source SHA and caller evidence are IDENTITIES, not
/// independent authentication by the report author.
#[derive(Debug,Serialize)]
pub struct StrategyExperimentReport{
    pub report:ExperimentReport,
    pub strategy_definition_sha256:String,
    pub strategy_origin:String,
    pub strategy_parent_id:Option<String>,
    pub strategy_model_version:String,
    pub experiment_config_sha256:String,
    pub source_manifest_sha256:String,
    pub execution_model_sha256:String,
    pub fee_schedule_sha256:Option<String>,
    pub risk_configuration_sha256:String,
    pub experiment_parent_id:Option<String>,
    pub experiment_statistical_method:String,
    pub discovery_selection_sha256:Option<String>,
    pub original_strategy_authentication_verified_by_reporter:bool,
    pub untouched_oos_eligibility_independently_verified:bool,
    pub actual_order_submitted:bool,
}
pub fn summarize_with_strategy_definition(
    spec:&ExperimentSpec,definition:&Definition,
    positions:&[PositionSample],forecasts:&[ForecastSample]
)->Result<StrategyExperimentReport,Error>{
    definition.validate().map_err(|_|Error::InvalidExperiment)?;
    if definition.id!=spec.strategy_id||
        definition.config_sha256.as_deref()!=Some(spec.config_sha256.as_str()){
        return Err(Error::InvalidExperiment);
    }
    let hash=definition.canonical_sha256()
        .map_err(|_|Error::InvalidExperiment)?;
    let base=summarize_experiment(spec,positions,forecasts)?;
    let origin=format!("{:?}",definition.origin);
    let verified=definition.origin==Origin::OriginalVerifiedDefinition;
    // "verified" here only indicates a complete original *record*, not
    // external authenticity, OOS statistics, or real trading outcomes.
    let _=verified;
    Ok(StrategyExperimentReport{
        report:base,
        strategy_definition_sha256:hash,strategy_origin:origin,
        strategy_parent_id:definition.lineage_parent.clone(),
        strategy_model_version:definition.model_version.clone(),
        experiment_config_sha256:spec.config_sha256.clone(),
        source_manifest_sha256:spec.source_manifest_sha256.clone(),
        execution_model_sha256:spec.execution_model_sha256.clone(),
        fee_schedule_sha256:spec.fee_schedule_sha256.clone(),
        risk_configuration_sha256:spec.risk_sha256.clone(),
        experiment_parent_id:spec.parent_id.clone(),
        experiment_statistical_method:spec.statistical_method.clone(),
        discovery_selection_sha256:spec.discovery_selection_sha256.clone(),
        original_strategy_authentication_verified_by_reporter:false,
        untouched_oos_eligibility_independently_verified:false,
        actual_order_submitted:false,
    })
}
#[cfg(test)]
mod tests{
    use super::*;use phoenix_research::experiments::{Split,ExecutionStatus};
    #[test]fn source_and_strategy_configuration_hashes_cannot_be_relabelled(){
        use phoenix_strategies::frozen_registry::{Origin,Lifecycle,Control};
        let def=Definition{id:"s1".into(),family:"control".into(),
            model_version:"v1".into(),lineage_parent:None,
            source_id:"synthetic".into(),source_document_sha256:None,
            code_sha256:Some("a".repeat(64)),
            config_sha256:Some("b".repeat(64)),
            manifest_sha256:"c".repeat(64),registry_version:"r1".into(),
            parameters:Default::default(),supported_series:Default::default(),
            required_kalshi_features:Default::default(),
            required_perps_features:Default::default(),timeframe_ns:None,
            entry_rule:None,exit_rule:None,risk_rule:None,
            sizing_rule:None,fee_rule:None,execution_model:None,
            history_requirements:vec![],frozen_at_utc:None,
            retired_at_utc:None,origin:Origin::Control,
            lifecycle:Lifecycle::Frozen,control:Some(Control::NoTrade)};
        let spec=ExperimentSpec{id:"e1".into(),parent_id:None,
            dataset_sha256:"a".repeat(64),source_manifest_sha256:"b".repeat(64),
            strategy_id:"s1".into(),config_sha256:"d".repeat(64),
            execution_model_sha256:"e".repeat(64),fee_schedule_sha256:None,
            risk_sha256:"f".repeat(64),source_start_ns:1,source_end_ns:500,
            discovery_end_ns:100,validation_end_ns:200,
            untouched_start_ns:250,embargo_ns:10,selected_markets:vec![],
            random_seed:1,statistical_method:"event-cluster".into(),
            status:"PLANNED_DISCOVERY".into(),settlement_evidence:Evidence::Synthetic,
            discovery_selection_sha256:None,failed_reason:None};
        assert!(matches!(summarize_with_strategy_definition(
            &spec,&def,&[],&[]),Err(Error::InvalidExperiment)));
    }
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

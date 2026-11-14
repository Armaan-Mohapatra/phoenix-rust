//! Immutable, cluster-aware OFFLINE research experiments.
//! No strategy optimization runs, production access, venue fills or profits
//! are inferred from incomplete source data.
use serde::{Serialize,Deserialize};
use sha2::{Digest,Sha256};
use std::collections::{BTreeMap,BTreeSet};
const MAX_PLANS:usize=4096;
const MAX_ROWS:usize=100_000;
const MAX_CLUSTERS:usize=20_000;
fn sha(b:&[u8])->String{format!("{:x}",Sha256::digest(b))}
fn digest(s:&str)->bool{s.len()==64&&s.bytes().all(|c|c.is_ascii_hexdigit())}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum Evidence{Synthetic,SourceProvisional,OfficialModeled,IndependentCorroborated}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum Split{Discovery,Validation,Embargo,UntouchedOos}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum ExecutionStatus{Abstain,Rejected,Nonfill,Partial,Full}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ExperimentSpec{
    pub id:String,pub parent_id:Option<String>,pub dataset_sha256:String,
    pub source_manifest_sha256:String,pub strategy_id:String,
    pub config_sha256:String,pub execution_model_sha256:String,
    pub fee_schedule_sha256:Option<String>,pub risk_sha256:String,
    pub source_start_ns:i64,pub source_end_ns:i64,
    pub discovery_end_ns:i64,pub validation_end_ns:i64,
    pub untouched_start_ns:i64,pub embargo_ns:i64,
    pub selected_markets:Vec<String>,pub random_seed:u64,
    pub statistical_method:String,pub status:String,
    pub settlement_evidence:Evidence,
    pub discovery_selection_sha256:Option<String>,
    pub failed_reason:Option<String>,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum Error{InvalidExperiment,VersionConflict,InvalidSplit,Overlap,
    OosReuse,DatasetConflict,UnknownParent,InvalidRow,InvalidFee,
    Arithmetic,TooManyRows,TooManyClusters,NoIndependentClusters,
    MissingOutcome,MissingExecutionEvidence,InvalidPrediction}
impl ExperimentSpec{
    pub fn validate(&self)->Result<(),Error>{
        if self.id.is_empty()||self.id.len()>128||
            self.strategy_id.is_empty()||self.statistical_method.is_empty()||
            ![&self.dataset_sha256,&self.source_manifest_sha256,
                &self.config_sha256,&self.execution_model_sha256,
                &self.risk_sha256].iter().all(|s|digest(s))||
            self.fee_schedule_sha256.as_ref().is_some_and(|s|!digest(s))||
            self.discovery_selection_sha256.as_ref().is_some_and(|s|!digest(s))||
            self.source_start_ns<=0||
            !(self.source_start_ns<self.discovery_end_ns&&
                self.discovery_end_ns<self.validation_end_ns&&
                self.validation_end_ns<self.untouched_start_ns&&
                self.untouched_start_ns<=self.source_end_ns)||
            self.embargo_ns<0||self.selected_markets.len()>10_000||
            self.status.is_empty(){
            return Err(Error::InvalidExperiment);
        }
        let a=self.discovery_end_ns.checked_add(self.embargo_ns)
            .ok_or(Error::Arithmetic)?;
        let b=self.validation_end_ns.checked_add(self.embargo_ns)
            .ok_or(Error::Arithmetic)?;
        if a>=self.validation_end_ns||b>=self.untouched_start_ns{
            return Err(Error::InvalidSplit);
        }
        Ok(())
    }
    pub fn fingerprint(&self)->Result<String,Error>{
        self.validate()?;
        Ok(sha(&serde_json::to_vec(self).map_err(|_|Error::InvalidExperiment)?))
    }
    pub fn split_for(&self,first_feature_ns:i64,outcome_ns:i64)->Split{
        if first_feature_ns<self.source_start_ns||
            first_feature_ns>outcome_ns||outcome_ns>self.source_end_ns{
            return Split::Embargo;
        }
        if outcome_ns<=self.discovery_end_ns{return Split::Discovery;}
        if first_feature_ns>=self.discovery_end_ns.saturating_add(self.embargo_ns)&&
            outcome_ns<=self.validation_end_ns{return Split::Validation;}
        if first_feature_ns>=self.untouched_start_ns&&
            outcome_ns>=self.untouched_start_ns{return Split::UntouchedOos;}
        Split::Embargo
    }
}
#[derive(Default)]
pub struct ImmutableExperiments{
    plans:BTreeMap<String,ExperimentSpec>,
    /// An OOS event touched by selection is forever contaminated until
    /// explicitly relabeled as an exploratory reuse outside this registry.
    oos_uses:BTreeMap<(String,String),String>,
}
impl ImmutableExperiments{
    pub fn insert(&mut self,p:ExperimentSpec)->Result<String,Error>{
        let hash=p.fingerprint()?;
        if self.plans.len()>=MAX_PLANS{return Err(Error::TooManyRows);}
        if let Some(old)=self.plans.get(&p.id){
            return if old.fingerprint()?==hash{Ok(hash)}
                else{Err(Error::VersionConflict)};
        }
        if p.parent_id.as_ref().is_some_and(|id|
            id==&p.id||!self.plans.contains_key(id)){
            return Err(Error::UnknownParent);
        }
        self.plans.insert(p.id.clone(),p);Ok(hash)
    }
    pub fn get(&self,id:&str)->Option<&ExperimentSpec>{self.plans.get(id)}
    pub fn claim_oos(&mut self,dataset:&str,event:&str,experiment:&str)
        ->Result<(),Error>{
        if !digest(dataset)||event.is_empty()||experiment.is_empty()||
            !self.plans.contains_key(experiment){
            return Err(Error::InvalidExperiment);
        }
        let spec=&self.plans[experiment];
        if spec.dataset_sha256!=dataset{return Err(Error::DatasetConflict);}
        let key=(dataset.into(),event.into());
        if self.oos_uses.contains_key(&key){return Err(Error::OosReuse);}
        self.oos_uses.insert(key,experiment.into());Ok(())
    }
    pub fn repeated_holdouts(&self)->usize{self.oos_uses.len()}
}
/// Explicit scale bridge: legacy Money is 1/10,000 USD; historical
/// fractional execution money is 1/1,000,000 USD. Checked 100x conversion.
pub fn legacy_1e4_to_micro_usd(value:i128)->Result<i128,Error>{
    value.checked_mul(100).ok_or(Error::Arithmetic)
}
#[derive(Debug,Clone)]
pub struct EventGroup{
    pub event:String,pub market:String,
    pub first_feature_ns:i64,pub last_outcome_ns:i64,
}
/// Every overlapping market/event belongs to exactly one split. Gaps
/// and embargo-classified groups are not magically relabeled train/OOS.
pub fn assign_grouped_splits(p:&ExperimentSpec,groups:&[EventGroup])
    ->Result<BTreeMap<(String,String),Split>,Error>{
    p.validate()?;
    if groups.len()>MAX_ROWS{return Err(Error::TooManyRows);}
    let mut out=BTreeMap::new();
    for g in groups{
        if g.event.is_empty()||g.market.is_empty()||
            g.last_outcome_ns<g.first_feature_ns{
            return Err(Error::InvalidSplit);
        }
        let split=p.split_for(g.first_feature_ns,g.last_outcome_ns);
        let key=(g.event.clone(),g.market.clone());
        if let Some(other)=out.get(&key){
            if *other!=split{return Err(Error::Overlap);}
        }else{out.insert(key,split);}
    }
    Ok(out)
}
#[derive(Debug,Clone)]
pub struct WalkForwardSpecification{
    pub initial_discovery_end_ns:i64,pub validation_width_ns:i64,
    pub embargo_ns:i64,pub step_ns:i64,pub windows:u32,
}
impl WalkForwardSpecification{
    /// Planning only. Caller must still register immutable versions and
    /// never reuse an already opened untouched holdout for selection.
    pub fn windows(&self)->Result<Vec<(i64,i64,i64)>,Error>{
        if self.initial_discovery_end_ns<=0||
            self.validation_width_ns<=0||self.step_ns<=0||
            self.embargo_ns<0||self.windows==0||self.windows>64{
            return Err(Error::InvalidExperiment);
        }
        let mut windows=Vec::new();
        for n in 0..self.windows{
            let start=self.initial_discovery_end_ns
                .checked_add(i64::from(n).checked_mul(self.step_ns)
                .ok_or(Error::Arithmetic)?).ok_or(Error::Arithmetic)?;
            let val=start.checked_add(self.embargo_ns)
                .ok_or(Error::Arithmetic)?;
            let end=val.checked_add(self.validation_width_ns)
                .ok_or(Error::Arithmetic)?;
            windows.push((start,val,end));
        }
        Ok(windows)
    }
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct PositionSample{
    pub event:String,pub market:String,pub strategy:String,
    pub split:Split,pub decision_ns:i64,pub outcome_received_ns:Option<i64>,
    pub evidence:Evidence,pub execution:ExecutionStatus,
    /// An explicitly modeled cash flow in micro-USD (1e-6 USD). Never f64.
    pub gross_micro_usd:Option<i128>,pub fee_micro_usd:Option<i128>,
    pub net_micro_usd:Option<i128>,pub capital_micro_usd:Option<i128>,
    pub submitted_hundredths:u64,pub filled_hundredths:u64,
    pub side:String,pub allowed_for_estimation:bool,
}
#[derive(Debug,Clone,Serialize)]
pub struct MoneyRatio{pub numerator:i128,pub denominator:i128}
#[derive(Debug,Clone,Serialize)]
pub struct Performance{
    pub records:u64,pub independent_event_clusters:u64,
    pub market_count:u64,pub abstentions:u64,pub rejected:u64,
    pub unfilled:u64,pub partial:u64,pub fully_filled:u64,
    pub pending_settlements:u64,pub missing_execution_evidence:u64,
    pub gross_micro_usd:Option<i128>,pub fees_micro_usd:Option<i128>,
    pub net_micro_usd:Option<i128>,pub settled:u64,
    pub wins:u64,pub losses:u64,pub flat:u64,
    pub win_rate:Option<MoneyRatio>,pub average_win_micro:Option<MoneyRatio>,
    pub average_loss_micro:Option<MoneyRatio>,
    pub profit_factor:Option<MoneyRatio>,
    pub maximum_drawdown_micro_usd:Option<i128>,
    pub return_on_committed_capital:Option<MoneyRatio>,
    pub peak_capital_micro_usd:Option<i128>,
    pub missing_data_codes:Vec<String>,pub inferred_venue_fills:bool,
}
fn ratio(n:i128,d:i128)->Option<MoneyRatio>{
    if d==0{None}else{Some(MoneyRatio{numerator:n,denominator:d})}
}
pub fn summarize_positions(rows:&[PositionSample])->Result<Performance,Error>{
    if rows.len()>MAX_ROWS{return Err(Error::TooManyRows);}
    let mut ordered=rows.iter().collect::<Vec<_>>();
    ordered.sort_by(|a,b|a.decision_ns.cmp(&b.decision_ns)
        .then(a.event.cmp(&b.event)).then(a.market.cmp(&b.market)));
    let mut event_ids=BTreeSet::new();let mut markets=BTreeSet::new();
    let mut count=[0u64;5];let mut pending=0u64;let mut missing=0u64;
    let (mut net,mut gross,mut fee,mut risk,mut wins,mut losses,mut flats)=
        (0i128,0i128,0i128,0i128,0u64,0u64,0u64);
    let (mut win_sum,mut loss_sum)=(0i128,0i128);
    let (mut dd,mut peak,mut running)=(0i128,0i128,0i128);
    let mut settled=0u64;
    let mut reasons=BTreeSet::new();
    for r in ordered{
        if r.event.is_empty()||r.market.is_empty()||r.strategy.is_empty()||
            r.decision_ns<=0||r.split==Split::Embargo||
            r.filled_hundredths>r.submitted_hundredths||
            r.outcome_received_ns.is_some_and(|x|x<=r.decision_ns){
            return Err(Error::InvalidRow);
        }
        let i=match r.execution{
            ExecutionStatus::Abstain=>0,ExecutionStatus::Rejected=>1,
            ExecutionStatus::Nonfill=>2,ExecutionStatus::Partial=>3,
            ExecutionStatus::Full=>4,
        };
        count[i]+=1;event_ids.insert(&r.event);markets.insert(&r.market);
        if event_ids.len()>MAX_CLUSTERS{return Err(Error::TooManyClusters);}
        if r.filled_hundredths==0 && i>2||
            r.filled_hundredths>0&&i<3||
            i==4&&r.filled_hundredths!=r.submitted_hundredths{
            return Err(Error::InvalidRow);
        }
        if r.filled_hundredths>0 &&
            (r.capital_micro_usd.is_none()||
             r.capital_micro_usd.is_some_and(|v|v<=0)){
            return Err(Error::MissingExecutionEvidence);
        }
        if r.gross_micro_usd.is_none()||
            r.fee_micro_usd.is_none()||
            r.net_micro_usd.is_none(){
            if r.filled_hundredths>0{
                pending+=1;
                if r.outcome_received_ns.is_none(){
                    reasons.insert("OUTCOME_OR_SETTLEMENT_PENDING");
                }else{
                    reasons.insert("UNSUPPORTED_PARTIAL_PNL_EVIDENCE");
                }
            }else{missing+=1;}
            continue;
        }
        if r.filled_hundredths==0||r.outcome_received_ns.is_none()||
            !r.allowed_for_estimation{
            return Err(Error::MissingOutcome);
        }
        let g=r.gross_micro_usd.unwrap();
        let f=r.fee_micro_usd.unwrap();
        let n=r.net_micro_usd.unwrap();
        if f<0||g.checked_sub(f)!=Some(n){return Err(Error::InvalidFee);}
        net=net.checked_add(n).ok_or(Error::Arithmetic)?;
        gross=gross.checked_add(g).ok_or(Error::Arithmetic)?;
        fee=fee.checked_add(f).ok_or(Error::Arithmetic)?;
        risk=risk.checked_add(r.capital_micro_usd.unwrap())
            .ok_or(Error::Arithmetic)?;
        running=running.checked_add(n).ok_or(Error::Arithmetic)?;
        peak=peak.max(running);dd=dd.max(peak.checked_sub(running)
            .ok_or(Error::Arithmetic)?);
        settled+=1;
        if n>0{wins+=1;win_sum=win_sum.checked_add(n).ok_or(Error::Arithmetic)?;}
        else if n<0{losses+=1;loss_sum=loss_sum.checked_add(-n)
            .ok_or(Error::Arithmetic)?;}
        else{flats+=1;}
    }
    let have=settled>0;
    Ok(Performance{records:rows.len() as u64,
        independent_event_clusters:event_ids.len() as u64,
        market_count:markets.len() as u64,abstentions:count[0],
        rejected:count[1],unfilled:count[2],partial:count[3],
        fully_filled:count[4],pending_settlements:pending,
        missing_execution_evidence:missing,
        gross_micro_usd:if have{Some(gross)}else{None},
        fees_micro_usd:if have{Some(fee)}else{None},
        net_micro_usd:if have{Some(net)}else{None},
        settled,wins,losses,flat:flats,
        win_rate:ratio(i128::from(wins),i128::from(settled)),
        average_win_micro:ratio(win_sum,i128::from(wins)),
        average_loss_micro:ratio(loss_sum,i128::from(losses)),
        profit_factor:ratio(win_sum,loss_sum),maximum_drawdown_micro_usd:
            if have{Some(dd)}else{None},
        return_on_committed_capital:ratio(net,risk),
        peak_capital_micro_usd:if risk>0{Some(risk)}else{None},
        missing_data_codes:reasons.into_iter().map(str::to_owned).collect(),
        inferred_venue_fills:false})
}
#[derive(Debug,Clone)]
pub struct ForecastSample{
    pub event:String,pub decision_ns:i64,pub resolved_at_ns:i64,
    pub probability_yes_ticks:u16,pub market_probability_ticks:u16,
    pub yes_won:bool,pub evidence:Evidence,
}
#[derive(Debug,Clone,Serialize)]
pub struct CalibrationBin{pub lower_ticks:u16,pub upper_ticks:u16,
    pub count:u64,pub observed_yes:u64,pub predicted_sum_ticks:u128}
#[derive(Debug,Clone,Serialize)]
pub struct ForecastScore{
    pub observations:u64,pub unique_event_clusters:u64,
    pub brier_millionths:u64,pub market_brier_millionths:u64,
    pub mean_log_loss: f64,pub market_mean_log_loss:f64,
    pub accuracy:Option<MoneyRatio>,pub calibration:Vec<CalibrationBin>,
    pub evidence_verified_by_engine:bool,pub significance_established:bool,
}
pub fn score_forecasts(rows:&[ForecastSample])->Result<ForecastScore,Error>{
    if rows.is_empty()||rows.len()>MAX_ROWS{return Err(Error::TooManyRows);}
    let mut clusters=BTreeSet::new();
    let (mut brier,mut market,mut loss,mut market_loss,mut correct)=
        (0u128,0u128,0f64,0f64,0u64);
    let mut bins=(0..10).map(|i|CalibrationBin{
        lower_ticks:(i*1000) as u16,upper_ticks:((i+1)*1000) as u16,
        count:0,observed_yes:0,predicted_sum_ticks:0u128})
        .collect::<Vec<_>>();
    for r in rows{
        if r.event.is_empty()||r.decision_ns<=0||
            r.resolved_at_ns<=r.decision_ns||
            r.probability_yes_ticks>10_000||r.market_probability_ticks>10_000{
            return Err(Error::InvalidPrediction);
        }
        clusters.insert(&r.event);
        let target=if r.yes_won{10_000i128}else{0};
        let delta=target-i128::from(r.probability_yes_ticks);
        let reference_delta=target-i128::from(r.market_probability_ticks);
        brier=brier.checked_add((delta*delta) as u128)
            .ok_or(Error::Arithmetic)?;
        market=market.checked_add((reference_delta*reference_delta) as u128)
            .ok_or(Error::Arithmetic)?;
        let ll=|ticks:u16|{
            let p=(f64::from(ticks)/10_000.0).clamp(1e-12,1.0-1e-12);
            if r.yes_won{-p.ln()}else{-(1.0-p).ln()}
        };
        loss+=ll(r.probability_yes_ticks);
        market_loss+=ll(r.market_probability_ticks);
        if (r.probability_yes_ticks>=5000)==r.yes_won{correct+=1;}
        let idx=(r.probability_yes_ticks/1000).min(9) as usize;
        bins[idx].count+=1;
        bins[idx].observed_yes+=u64::from(r.yes_won);
        bins[idx].predicted_sum_ticks+=u128::from(r.probability_yes_ticks);
    }
    let n=rows.len() as u128;
    let norm=100_000_000u128*n;
    Ok(ForecastScore{observations:rows.len() as u64,
        unique_event_clusters:clusters.len() as u64,
        brier_millionths:u64::try_from(brier*1_000_000/norm)
            .map_err(|_|Error::Arithmetic)?,
        market_brier_millionths:u64::try_from(market*1_000_000/norm)
            .map_err(|_|Error::Arithmetic)?,
        mean_log_loss:loss/rows.len() as f64,
        market_mean_log_loss:market_loss/rows.len() as f64,
        accuracy:ratio(i128::from(correct),rows.len() as i128),
        calibration:bins,evidence_verified_by_engine:false,
        significance_established:false})
}
#[derive(Debug,Clone,Serialize)]
pub struct ClusterInterval{
    pub unique_clusters:usize,pub resamples:usize,pub seed:u64,
    pub lower_mean_micro_usd:i128,pub upper_mean_micro_usd:i128,
    pub multiple_comparisons:u32,pub adjusted_alpha_millionths:u32,
    pub confirmatory_significance:bool,
}
/// Event-cluster block bootstrap (resample *entire event sums*, not trades),
/// seeded xorshift64. Exploratory CI only: validity also requires an
/// independent pre-registered testing design, untouched holdout and power.
pub fn clustered_bootstrap(rows:&[(String,i128)],seed:u64,
    resamples:usize,comparison_count:u32)->Result<ClusterInterval,Error>{
    if rows.is_empty()||rows.len()>MAX_ROWS||
        resamples<100||resamples>4096||comparison_count==0||
        comparison_count>10_000{return Err(Error::InvalidExperiment);}
    let mut groups=BTreeMap::<String,i128>::new();
    for (event,net) in rows{
        if event.is_empty(){return Err(Error::InvalidRow);}
        let entry=groups.entry(event.clone()).or_default();
        *entry=entry.checked_add(*net).ok_or(Error::Arithmetic)?;
    }
    if groups.len()<20{return Err(Error::NoIndependentClusters);}
    if groups.len()>MAX_CLUSTERS{return Err(Error::TooManyClusters);}
    let v=groups.values().copied().collect::<Vec<_>>();
    let mut rng=if seed==0{0x9e3779b97f4a7c15u64}else{seed};
    let mut draws=Vec::with_capacity(resamples);
    for _ in 0..resamples{
        let mut total=0i128;
        for _ in 0..v.len(){
            rng^=rng<<13;rng^=rng>>7;rng^=rng<<17;
            total=total.checked_add(v[(rng as usize)%v.len()])
                .ok_or(Error::Arithmetic)?;
        }
        draws.push(total/v.len() as i128);
    }
    draws.sort_unstable();
    let index_low=resamples/40;let index_high=resamples-1-index_low;
    Ok(ClusterInterval{unique_clusters:v.len(),resamples,seed,
        lower_mean_micro_usd:draws[index_low],
        upper_mean_micro_usd:draws[index_high],
        multiple_comparisons:comparison_count,
        adjusted_alpha_millionths:50_000/comparison_count,
        confirmatory_significance:false})
}
#[cfg(test)]
mod tests{
    use super::*;
    fn spec()->ExperimentSpec{
        ExperimentSpec{id:"exp-v1".into(),parent_id:None,
            dataset_sha256:"a".repeat(64),source_manifest_sha256:"b".repeat(64),
            strategy_id:"s1".into(),config_sha256:"c".repeat(64),
            execution_model_sha256:"d".repeat(64),
            fee_schedule_sha256:Some("e".repeat(64)),risk_sha256:"f".repeat(64),
            source_start_ns:1,source_end_ns:500,discovery_end_ns:100,
            validation_end_ns:200,untouched_start_ns:250,embargo_ns:10,
            selected_markets:vec!["BTC".into()],random_seed:42,
            statistical_method:"event-block-bootstrap".into(),
            status:"PENDING".into(),settlement_evidence:Evidence::Synthetic,
            discovery_selection_sha256:None,failed_reason:None}
    }
    fn row(event:&str,net:Option<i128>)->PositionSample{
        PositionSample{event:event.into(),market:event.into(),strategy:"s1".into(),
            split:Split::Validation,decision_ns:50,outcome_received_ns:Some(80),
            evidence:Evidence::Synthetic,execution:ExecutionStatus::Full,
            gross_micro_usd:net.map(|x|x+5),fee_micro_usd:Some(5),
            net_micro_usd:net,capital_micro_usd:Some(1000),
            submitted_hundredths:100,filled_hundredths:100,side:"yes".into(),
            allowed_for_estimation:true}
    }
    #[test]fn immutable_experiment_reuse_and_future_oos_block(){
        let mut reg=ImmutableExperiments::default();reg.insert(spec()).unwrap();
        let mut modified=spec();modified.random_seed=43;
        assert_eq!(reg.insert(modified),Err(Error::VersionConflict));
        reg.claim_oos(&"a".repeat(64),"EVENT1","exp-v1").unwrap();
        assert_eq!(reg.claim_oos(&"a".repeat(64),"EVENT1","exp-v1"),Err(Error::OosReuse));
        assert_eq!(spec().split_for(95,120),Split::Embargo);
        assert_eq!(spec().split_for(260,300),Split::UntouchedOos);
    }
    #[test]fn negative_returns_pending_and_zero_not_conflated(){
        let mut pending=row("c",None);pending.outcome_received_ns=None;
        pending.fee_micro_usd=None;
        let summary=summarize_positions(&[row("a",Some(-100)),
            row("b",Some(150)),pending]).unwrap();
        assert_eq!(summary.net_micro_usd,Some(50));
        assert_eq!(summary.pending_settlements,1);
        assert_eq!(summary.maximum_drawdown_micro_usd,Some(100));
        assert_eq!(summary.wins,1);assert_eq!(summary.losses,1);
    }
    #[test]fn fees_and_missing_execution_are_not_fake_zero(){
        let mut wrong=row("a",Some(5));
        wrong.fee_micro_usd=Some(7);
        assert!(matches!(summarize_positions(&[wrong]),Err(Error::InvalidFee)));
        let mut missing=row("b",None);missing.capital_micro_usd=None;
        assert!(matches!(summarize_positions(&[missing]),
            Err(Error::MissingExecutionEvidence)));
    }
    #[test]fn event_cluster_resampling_is_seeded_and_rejects_low_n(){
        let sample=(0..40).map(|i|(format!("event-{i}"),i as i128-20))
            .collect::<Vec<_>>();
        let a=clustered_bootstrap(&sample,42,300,4).unwrap();
        let b=clustered_bootstrap(&sample,42,300,4).unwrap();
        assert_eq!(a.lower_mean_micro_usd,b.lower_mean_micro_usd);
        assert!(!a.confirmatory_significance);
        assert_eq!(a.adjusted_alpha_millionths,12_500);
        assert!(clustered_bootstrap(&sample[..4],42,300,4).is_err());
    }
    #[test]fn micro_usd_unit_bridge_rejects_overflow_and_preserves_exactness(){
        assert_eq!(legacy_1e4_to_micro_usd(10_000).unwrap(),1_000_000);
        assert_eq!(legacy_1e4_to_micro_usd(-1).unwrap(),-100);
        assert!(legacy_1e4_to_micro_usd(i128::MAX).is_err());
    }
    #[test]fn market_events_cannot_be_split_across_validation_and_oos(){
        let groups=[EventGroup{event:"evt".into(),market:"BTC".into(),
            first_feature_ns:110,last_outcome_ns:190},
            EventGroup{event:"evt".into(),market:"BTC".into(),
            first_feature_ns:260,last_outcome_ns:300}];
        assert!(matches!(assign_grouped_splits(&spec(),&groups),
            Err(Error::Overlap)));
    }
    #[test]fn walk_forward_plans_are_bounded_and_do_not_select_oos(){
        let p=WalkForwardSpecification{initial_discovery_end_ns:100,
            validation_width_ns:50,embargo_ns:10,step_ns:60,windows:2};
        assert_eq!(p.windows().unwrap(),vec![(100,110,160),(160,170,220)]);
    }
    #[test]fn forecast_score_refuses_future_public_outcome(){
        let x=ForecastSample{event:"x".into(),decision_ns:20,resolved_at_ns:19,
            probability_yes_ticks:7000,market_probability_ticks:6000,
            yes_won:false,evidence:Evidence::Synthetic};
        assert!(score_forecasts(&[x]).is_err());
    }
}

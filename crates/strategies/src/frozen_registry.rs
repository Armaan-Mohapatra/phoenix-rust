//! Append-only, original-evidence-aware strategy definitions.
//! Named October "frozen" versions are never auto-promoted to executable
//! strategies: missing historical parameters remain UNKNOWN.
use serde::{Serialize,Deserialize};
use sha2::{Digest,Sha256};
use std::collections::{BTreeMap,BTreeSet};
use phoenix_domain::{Price,Side};
use crate::{Decision,Abstain,FrozenSpec,ModelSignal,evaluate};
use phoenix_domain::Book;

const MAX_STRATEGIES:usize=512;
const MAX_PARAMS:usize=128;
const MAX_SIGNALS:usize=32;
fn sha(s:&[u8])->String{format!("{:x}",Sha256::digest(s))}
fn digest(s:&str)->bool{s.len()==64&&s.bytes().all(|b|b.is_ascii_hexdigit())}
fn name(s:&str)->bool{!s.is_empty()&&s.len()<=192&&s.bytes().all(|b|
    b.is_ascii_alphanumeric()||matches!(b,b'-'|b'_'|b'.'|b':'|b'/'))}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum Origin {OriginalVerifiedDefinition,OriginalPartialDefinition,
    ReconstructedHypothesis,UnavailableSourceDefinition,Control}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum Lifecycle {Frozen,Proposed,Retired}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum Control {NoTrade,MarketImplied,SeededRandom,SimpleDirectional,
    ModelEdge,InvertedModelEdge,WeightedEnsemble}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct Definition{
    pub id:String,pub family:String,pub model_version:String,
    pub lineage_parent:Option<String>,pub source_id:String,
    pub source_document_sha256:Option<String>,
    pub code_sha256:Option<String>,pub config_sha256:Option<String>,
    pub manifest_sha256:String,pub registry_version:String,
    pub parameters:BTreeMap<String,String>,
    pub supported_series:BTreeSet<String>,
    pub required_kalshi_features:BTreeSet<String>,
    pub required_perps_features:BTreeSet<String>,
    pub timeframe_ns:Option<i64>,pub entry_rule:Option<String>,
    pub exit_rule:Option<String>,pub risk_rule:Option<String>,
    pub sizing_rule:Option<String>,pub fee_rule:Option<String>,
    pub execution_model:Option<String>,pub history_requirements:Vec<String>,
    pub frozen_at_utc:Option<String>,pub retired_at_utc:Option<String>,
    pub origin:Origin,pub lifecycle:Lifecycle,pub control:Option<Control>,
}
#[derive(Default)]
pub struct ImmutableRegistry{versions:BTreeMap<String,Definition>}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum Error{InvalidDefinition,InvalidHash,VersionConflict,UnknownParent,
    TooManyStrategies,MutationOfFrozen,UnavailableEvidence,
    FutureFeature,WrongMarket,UnsupportedSeries,UnknownModel}
impl Definition{
    pub fn validate(&self)->Result<(),Error>{
        if !name(&self.id)||!name(&self.family)||!name(&self.model_version)||
            !name(&self.registry_version)||!digest(&self.manifest_sha256)||
            self.parameters.len()>MAX_PARAMS||
            self.source_id.is_empty()||self.source_id.len()>512||
            self.code_sha256.as_ref().is_some_and(|s|!digest(s))||
            self.config_sha256.as_ref().is_some_and(|s|!digest(s))||
            self.source_document_sha256.as_ref().is_some_and(|s|!digest(s))||
            self.timeframe_ns.is_some_and(|t|t<=0){
            return Err(Error::InvalidDefinition);
        }
        if self.origin==Origin::OriginalVerifiedDefinition&&
            (self.code_sha256.is_none()||self.config_sha256.is_none()||
             self.entry_rule.is_none()||self.exit_rule.is_none()||
             self.risk_rule.is_none()||self.fee_rule.is_none()||
             self.execution_model.is_none()||self.sizing_rule.is_none()){
            return Err(Error::UnavailableEvidence);
        }
        if self.origin==Origin::Control && self.control.is_none(){
            return Err(Error::InvalidDefinition);
        }
        if self.origin!=Origin::Control && self.control.is_some(){
            return Err(Error::InvalidDefinition);
        }
        Ok(())
    }
    pub fn canonical_sha256(&self)->Result<String,Error>{
        self.validate()?;
        let b=serde_json::to_vec(self).map_err(|_|Error::InvalidDefinition)?;
        Ok(sha(&b))
    }
}
impl ImmutableRegistry{
    pub fn insert(&mut self,d:Definition)->Result<String,Error>{
        d.validate()?;
        if self.versions.len()>=MAX_STRATEGIES{return Err(Error::TooManyStrategies);}
        let identity=d.canonical_sha256()?;
        if let Some(old)=self.versions.get(&d.id){
            return if old.canonical_sha256()?==identity{
                Ok(identity)
            }else{Err(Error::MutationOfFrozen)};
        }
        if let Some(parent)=&d.lineage_parent{
            if parent==&d.id||!self.versions.contains_key(parent){
                return Err(Error::UnknownParent);
            }
        }
        self.versions.insert(d.id.clone(),d);Ok(identity)
    }
    pub fn get(&self,id:&str)->Option<&Definition>{self.versions.get(id)}
    pub fn len(&self)->usize{self.versions.len()}
    pub fn records(&self)->impl Iterator<Item=&Definition>{self.versions.values()}
    /// Material changes are NEVER applied in place. Caller must supply a
    /// new version ID and explicit lineage to the immutable frozen parent.
    pub fn derive(&mut self,parent:&str,mut child:Definition)
        ->Result<String,Error>{
        if !self.versions.contains_key(parent){return Err(Error::UnknownParent);}
        child.lineage_parent=Some(parent.into());
        self.insert(child)
    }
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct OctoberFrozenEvidence{
    pub version:String,pub manifest_file_sha256:String,
    pub declared_manifest_sha256:String,
    pub freeze_time_utc:String,
    pub source_code_sha256:Option<String>,
    pub strategy_versions:BTreeMap<String,String>,
    pub incomplete_reasons:BTreeMap<String,Vec<String>>,
}
/// Parse exact observed October frozen manifest structure. A source SHA
/// inside a manifest is a claim; caller supplies independently checked
/// byte SHA and archived source identity.
pub fn recover_october_freeze(body:&[u8],expected_file_sha256:&str)
    ->Result<OctoberFrozenEvidence,Error>{
    if body.len()>256*1024||!digest(expected_file_sha256)||
        sha(body)!=expected_file_sha256{return Err(Error::InvalidHash);}
    let v:serde_json::Value=serde_json::from_slice(body)
        .map_err(|_|Error::InvalidDefinition)?;
    let get=|key:&str|v.get(key).and_then(|a|a.as_str())
        .map(str::to_owned).ok_or(Error::InvalidDefinition);
    let model=get("manifest_version")?;
    let frozen=get("freeze_time_utc")?;
    let declared=get("manifest_sha256")?;
    if !digest(&declared){return Err(Error::InvalidHash);}
    let versions=v.get("strategy_versions").and_then(|x|x.as_object())
        .ok_or(Error::InvalidDefinition)?;
    if versions.is_empty()||versions.len()>MAX_STRATEGIES{
        return Err(Error::TooManyStrategies);
    }
    let mut ids=BTreeMap::new();let mut reasons=BTreeMap::new();
    for (id,raw) in versions{
        let version=raw.as_str().ok_or(Error::InvalidDefinition)?;
        if !name(id)||!name(version){return Err(Error::InvalidDefinition);}
        ids.insert(id.clone(),version.into());
        reasons.insert(id.clone(),vec![
            "VERSION_ID_AND_FREEZE_TIME_NOT_COMPLETE_ENTRY_EXIT_CONFIG".into(),
            "ORIGINAL_PARAMETER_AND_RULE_PROVENANCE_UNVERIFIED".into(),
            "NOVEMBER_PRODUCTION_VERSION_NOT_INFERRED_FROM_OCTOBER".into()]);
    }
    let code=v.pointer("/source_sha256/luna_experimental_paper.py")
        .and_then(|x|x.as_str()).map(str::to_owned);
    if code.as_ref().is_some_and(|s|!digest(s)){return Err(Error::InvalidHash);}
    Ok(OctoberFrozenEvidence{version:model,manifest_file_sha256:sha(body),
        declared_manifest_sha256:declared,freeze_time_utc:frozen,
        source_code_sha256:code,strategy_versions:ids,
        incomplete_reasons:reasons})
}
pub fn import_partial_october(registry:&mut ImmutableRegistry,
    evidence:&OctoberFrozenEvidence)->Result<usize,Error>{
    let mut n=0;
    for (family,version) in &evidence.strategy_versions{
        registry.insert(Definition{
            id:version.clone(),family:family.clone(),model_version:version.clone(),
            lineage_parent:None,source_id:"LunaResearch_20261024_163344.zip/research/luna_strategy_tournament_freeze_latest.json".into(),
            source_document_sha256:Some(evidence.manifest_file_sha256.clone()),
            code_sha256:evidence.source_code_sha256.clone(),
            config_sha256:None,
            manifest_sha256:evidence.declared_manifest_sha256.clone(),
            registry_version:evidence.version.clone(),parameters:BTreeMap::new(),
            supported_series:BTreeSet::new(),required_kalshi_features:BTreeSet::new(),
            required_perps_features:BTreeSet::new(),timeframe_ns:None,
            entry_rule:None,exit_rule:None,risk_rule:None,sizing_rule:None,
            fee_rule:None,execution_model:None,
            history_requirements:vec!["ARCHIVED_SOURCE_CONFIG_AND_COMPLETE_DECISION_RULES_REQUIRED".into()],
            frozen_at_utc:Some(evidence.freeze_time_utc.clone()),retired_at_utc:None,
            origin:Origin::OriginalPartialDefinition,
            lifecycle:Lifecycle::Frozen,control:None})?;
        n+=1;
    }
    Ok(n)
}
#[derive(Debug,Clone)]
pub struct PointInTimeSignal{
    pub name:String,pub value_ticks:u16,pub received_ns:i64,
    pub source_sha256:String,pub source_kind:String,
}
#[derive(Debug,Clone)]
pub struct DecisionContext{
    pub market:String,pub event:String,pub series:String,
    pub decision_ns:i64,pub book:Book,pub signals:Vec<PointInTimeSignal>,
    pub random_seed:Option<u64>,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum Action{Abstain,ProposedBuy{side:Side,limit:Price}}
#[derive(Debug,Clone)]
pub struct DecisionTrace{
    pub strategy_id:String,pub version_sha256:String,
    pub market:String,pub event:String,pub decided_at_ns:i64,
    pub action:Action,pub reason:String,pub source_sha256:Vec<String>,
    pub historical_evidence:String,pub venue_fill_verified:bool,
}
/// Do not execute a partially recovered original; controls are explicit and
/// use existing phoenix-strategies evaluator. Random is a labeled control.
pub fn decide(d:&Definition,c:&DecisionContext,
    spec:Option<&FrozenSpec>)->Result<DecisionTrace,Error>{
    d.validate()?;
    if c.market!=c.book.market||c.market.is_empty()||
        c.event.is_empty()||c.decision_ns<=0{
        return Err(Error::WrongMarket);
    }
    if c.signals.len()>MAX_SIGNALS||c.book.received_ns>c.decision_ns||
        c.signals.iter().any(|s|s.received_ns>c.decision_ns||
            s.received_ns<0||!digest(&s.source_sha256)){
        return Err(Error::FutureFeature);
    }
    let version=d.canonical_sha256()?;
    let mut hashes=c.signals.iter().map(|s|s.source_sha256.clone())
        .collect::<Vec<_>>();
    hashes.sort();hashes.dedup();
    let mut trace=DecisionTrace{
        strategy_id:d.id.clone(),version_sha256:version,
        market:c.market.clone(),event:c.event.clone(),
        decided_at_ns:c.decision_ns,action:Action::Abstain,
        reason:"UNAVAILABLE_OR_UNVERIFIED_FROZEN_DEFINITION".into(),
        source_sha256:hashes,
        historical_evidence:"ORIGINAL_PARTIAL_NOT_EXECUTABLE".into(),
        venue_fill_verified:false,
    };
    if !d.supported_series.is_empty()&&!d.supported_series.contains(&c.series){
        return Err(Error::UnsupportedSeries);
    }
    if d.origin!=Origin::Control{return Ok(trace);}
    let control=d.control.ok_or(Error::InvalidDefinition)?;
    if control==Control::NoTrade{
        trace.reason="NO_TRADE_CONTROL".into();return Ok(trace);
    }
    if control==Control::SeededRandom{
        let seed=c.random_seed.ok_or(Error::UnknownModel)?;
        let mut x=seed ^ u64::from(c.decision_ns as u32);
        x^=x<<13;x^=x>>7;x^=x<<17;
        let side=if x&1==0{Side::Yes}else{Side::No};
        let price=c.book.best_ask(side).ok_or(Error::UnknownModel)?;
        trace.action=Action::ProposedBuy{side,limit:price};
        trace.reason="SEEDED_RANDOM_CONTROL_NOT_HISTORICAL".into();
        trace.historical_evidence="SYNTHETIC_CONTROL".into();
        return Ok(trace);
    }
    if control==Control::SimpleDirectional{
        let s=c.signals.first().ok_or(Error::UnknownModel)?;
        let side=if s.value_ticks>=5000{Side::Yes}else{Side::No};
        trace.action=Action::ProposedBuy{side,
            limit:c.book.best_ask(side).ok_or(Error::UnknownModel)?};
        trace.reason="SIMPLE_DIRECTIONAL_CONTROL_NOT_HISTORICAL".into();
        trace.historical_evidence="SYNTHETIC_CONTROL".into();
        return Ok(trace);
    }
    let mut model=Vec::new();
    for signal in &c.signals{
        model.push(ModelSignal{asof_received_ns:signal.received_ns,
            probability_yes:Price::new(signal.value_ticks)
                .map_err(|_|Error::UnknownModel)?,model_weight:1});
    }
    let old=spec.ok_or(Error::UnknownModel)?;
    let decision=evaluate(old,c.decision_ns,&c.book,&model)
        .map_err(|_|Error::UnknownModel)?;
    match decision{
        Decision::Trade(trade)=>{
            trace.action=Action::ProposedBuy{side:trade.side,limit:trade.limit};
            trace.reason="EXISTING_EVALUATOR_PROPOSAL".into();
        },
        Decision::Abstain(abstain)=>{
            trace.reason=format!("{abstain:?}");
        }
    }
    trace.historical_evidence="SYNTHETIC_CONTROL".into();
    Ok(trace)
}
#[cfg(test)]
mod tests{
    use super::*;use phoenix_domain::Level;
    fn control()->Definition{
        Definition{id:"demo".into(),family:"control".into(),model_version:"v1".into(),
            lineage_parent:None,source_id:"synthetic".into(),source_document_sha256:None,
            code_sha256:Some("a".repeat(64)),config_sha256:Some("b".repeat(64)),
            manifest_sha256:"c".repeat(64),registry_version:"r1".into(),
            parameters:BTreeMap::new(),supported_series:BTreeSet::new(),
            required_kalshi_features:BTreeSet::new(),required_perps_features:BTreeSet::new(),
            timeframe_ns:Some(900_000_000_000),entry_rule:None,exit_rule:None,
            risk_rule:None,sizing_rule:None,fee_rule:None,execution_model:None,
            history_requirements:vec![],frozen_at_utc:None,retired_at_utc:None,
            origin:Origin::Control,lifecycle:Lifecycle::Frozen,
            control:Some(Control::NoTrade)}
    }
    fn context()->DecisionContext{
        DecisionContext{market:"BTC".into(),event:"E1".into(),series:"KXBTC".into(),
            decision_ns:200,book:Book{market:"BTC".into(),
                yes_bids:vec![Level{price:Price::new(4000).unwrap(),quantity:10}],
                no_bids:vec![Level{price:Price::new(5500).unwrap(),quantity:10}],
                exchange_ns:90,received_ns:100,
                session_id:"synthetic".into(),sequence:1},
            signals:vec![],random_seed:Some(42)}
    }
    #[test]fn original_freeze_requires_exact_sha_and_does_not_invent_parameters(){
        let bytes=br#"{"manifest_version":"v1","freeze_time_utc":"2026-10-24T00:00:00Z",
            "manifest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "strategy_versions":{"market_only":"market_only_p0_v2"}}"#;
        assert!(recover_october_freeze(bytes,&"f".repeat(64)).is_err());
        let original=recover_october_freeze(bytes,&sha(bytes)).unwrap();
        let mut registry=ImmutableRegistry::default();
        assert_eq!(import_partial_october(&mut registry,&original).unwrap(),1);
        let row=registry.get("market_only_p0_v2").unwrap();
        assert_eq!(row.origin,Origin::OriginalPartialDefinition);
        assert!(row.config_sha256.is_none());
        assert!(decide(row,&context(),None).unwrap().action==Action::Abstain);
    }
    #[test]fn frozen_mutation_fails_but_lineage_creates_distinct_version(){
        let mut registry=ImmutableRegistry::default();
        registry.insert(control()).unwrap();
        let mut altered=control();altered.parameters.insert("x".into(),"2".into());
        assert_eq!(registry.insert(altered.clone()),Err(Error::MutationOfFrozen));
        altered.id="demo-v2".into();
        registry.derive("demo",altered).unwrap();
        assert_eq!(registry.get("demo-v2").unwrap().lineage_parent.as_deref(),Some("demo"));
    }
    #[test]fn future_features_and_control_abstention(){
        let d=control();
        assert_eq!(decide(&d,&context(),None).unwrap().action,Action::Abstain);
        let mut c=context();c.signals.push(PointInTimeSignal{name:"model".into(),
            value_ticks:6000,received_ns:201,source_sha256:"a".repeat(64),
            source_kind:"synthetic".into()});
        assert!(matches!(decide(&d,&c,None),Err(Error::FutureFeature)));
    }
    #[test]fn seeded_control_is_deterministic_and_not_a_verified_fill(){
        let mut d=control();d.control=Some(Control::SeededRandom);
        let a=decide(&d,&context(),None).unwrap();
        let b=decide(&d,&context(),None).unwrap();
        assert_eq!(a.action,b.action);
        assert!(!a.venue_fill_verified);
        assert_eq!(a.historical_evidence,"SYNTHETIC_CONTROL");
    }
}

//! Historical contract economics and immutable evidence gates.
//! No historical rules, fee rates, primary ticks or official finals are bundled.
//! All arithmetic delegates to existing domain/settlement primitives.
use phoenix_domain::{self as domain,FeeRate,Price,Side,Money,ModeledBuyFill,
    BinaryFinal,Evidence as DomainEvidence,settle_binary};
use phoenix_settlement::{IndexTick,WindowRule,compare_window_averages};
use phoenix_kalshi_read_only::{OfficialMarketEvidence,OfficialStatus};
use serde::{Deserialize,Serialize};
use sha2::{Digest,Sha256};
pub mod primary;

const MAX_RECORDS:usize=4096;
const MAX_TICKS:usize=3600;
fn sha_ok(s:&str)->bool{s.len()==64&&s.bytes().all(|b|b.is_ascii_hexdigit())}
fn valid_name(s:&str)->bool{
    !s.is_empty()&&s.len()<=128&&s.bytes().all(|b|
        b.is_ascii_alphanumeric()||matches!(b,b'-'|b'_'|b':'|b'.'))
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum Error{
    InvalidRule,UnknownRule,MultipleRules,UnsupportedContract,
    FutureRule,UnknownFee,InvalidFee,Overflow,InvalidSource,
    WrongMarket,WindowMismatch,MissingPrimary,InvalidPrimary,
    UnauthenticatedPrimary,ConflictingSettlement,UnknownOfficial,
    Domain(domain::Error),Window(phoenix_settlement::Error),
}
impl From<domain::Error> for Error{fn from(e:domain::Error)->Self{Self::Domain(e)}}
impl From<phoenix_settlement::Error> for Error{
    fn from(e:phoenix_settlement::Error)->Self{Self::Window(e)}
}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum ContractType {BinaryIndexComparison,Unsupported,Voided,Cancelled,Scalar}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
pub struct WindowDefinition{
    pub opening_start_sec:i64,pub closing_start_sec:i64,
    pub samples_per_window:u32,pub tie_resolves_yes:bool,
}
impl WindowDefinition{
    pub fn to_existing_rule(self)->WindowRule{
        WindowRule{opening_start_sec:self.opening_start_sec,
            closing_start_sec:self.closing_start_sec,
            samples_per_window:self.samples_per_window,
            tie_resolves_yes:self.tie_resolves_yes}
    }
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ContractDefinition{
    pub ticker:String,pub market_id:String,
    pub event_id:String,pub series:String,
    pub contract_type:ContractType,
    pub market_open_ns:i64,pub expiration_ns:i64,
    pub rule_effective_from_ns:i64,pub rule_effective_until_ns:Option<i64>,
    pub rule_published_ns:i64,pub rule_observed_ns:i64,
    pub reference_provider:String,pub benchmark_id:String,
    pub methodology_revision:String,
    pub window:WindowDefinition,
    pub payout_ticks:u16,
    pub source_document_id:String,
    pub source_document_sha256:String,pub version_sha256:String,
}
impl ContractDefinition{
    fn validate(&self)->Result<(),Error>{
        if !valid_name(&self.ticker)||!valid_name(&self.market_id)||
            !valid_name(&self.event_id)||!valid_name(&self.series)||
            self.source_document_id.is_empty()||
            !sha_ok(&self.source_document_sha256)||!sha_ok(&self.version_sha256)||
            self.reference_provider.is_empty()||self.benchmark_id.is_empty()||
            self.methodology_revision.is_empty()||
            self.market_open_ns>=self.expiration_ns||
            self.rule_effective_from_ns>=self.expiration_ns||
            self.rule_published_ns>self.rule_observed_ns||
            self.rule_effective_until_ns.is_some_and(|v|v<=self.rule_effective_from_ns)||
            self.window.samples_per_window==0||
            self.window.samples_per_window>MAX_TICKS as u32{
            return Err(Error::InvalidRule);
        }
        let last_open=self.window.opening_start_sec.checked_add(
            i64::from(self.window.samples_per_window)).ok_or(Error::Overflow)?;
        if self.window.closing_start_sec<last_open{return Err(Error::InvalidRule);}
        if self.contract_type!=ContractType::BinaryIndexComparison||
            self.payout_ticks!=10_000{return Err(Error::UnsupportedContract);}
        Ok(())
    }
}
#[derive(Default)]
pub struct ContractRegistry{records:Vec<ContractDefinition>}
impl ContractRegistry{
    pub fn insert(&mut self,r:ContractDefinition)->Result<(),Error>{
        r.validate()?;
        if self.records.len()>=MAX_RECORDS{return Err(Error::InvalidRule);}
        let end=r.rule_effective_until_ns.unwrap_or(i64::MAX);
        for old in &self.records{
            if old.ticker!=r.ticker{continue;}
            let other_end=old.rule_effective_until_ns.unwrap_or(i64::MAX);
            if r.rule_effective_from_ns<other_end&&old.rule_effective_from_ns<end{
                return Err(Error::MultipleRules);
            }
        }
        self.records.push(r);Ok(())
    }
    pub fn resolve(&self,ticker:&str,asof_ns:i64)
        ->Result<&ContractDefinition,Error>{
        let mut found=None;
        for r in &self.records{
            if r.ticker!=ticker||asof_ns<r.rule_effective_from_ns||
                r.rule_effective_until_ns.is_some_and(|v|asof_ns>=v){continue;}
            if asof_ns<r.rule_published_ns||asof_ns<r.rule_observed_ns{
                return Err(Error::FutureRule);
            }
            if found.is_some(){return Err(Error::MultipleRules);}
            found=Some(r);
        }
        found.ok_or(Error::UnknownRule)
    }
}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum FeeRole {Maker,Taker}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum FeeFormula {QuadraticPrice,FlatPerContract}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum FeeAggregation {PerFill,PerPriceLevel}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct FeeSchedule{
    pub series:String,pub role:FeeRole,
    pub effective_from_ns:i64,pub effective_until_ns:Option<i64>,
    pub published_ns:i64,pub observed_ns:i64,
    pub source_sha256:String,pub source_document_id:String,
    pub formula:FeeFormula,pub rate_numerator:u64,pub rate_denominator:u64,
    pub rounding_step_centicents:u64,
    pub aggregation:FeeAggregation,
    pub minimum_fee_centicents:Option<u64>,
    pub maximum_fee_centicents:Option<u64>,
}
#[derive(Debug,Clone,PartialEq,Eq,Serialize,Deserialize)]
pub struct ChargedFee{
    pub fee_centicents:Money,
    pub source_sha256:String,pub role:FeeRole,
    pub schedule_series:String,
    pub evaluated_at_ns:Option<i64>,
    pub schedule_effective_from_ns:i64,
    pub aggregation:FeeAggregation,
}
#[derive(Default)]
pub struct FeeRegistry{records:Vec<FeeSchedule>}
impl FeeRegistry{
    pub fn insert(&mut self,r:FeeSchedule)->Result<(),Error>{
        if !valid_name(&r.series)||!sha_ok(&r.source_sha256)||
            r.source_document_id.is_empty()||r.rate_denominator==0||
            r.rounding_step_centicents==0||
            r.effective_until_ns.is_some_and(|v|v<=r.effective_from_ns)||
            r.published_ns>r.observed_ns||self.records.len()>=MAX_RECORDS||
            (r.minimum_fee_centicents.is_some()&&r.maximum_fee_centicents.is_some()&&
             r.minimum_fee_centicents>r.maximum_fee_centicents){
            return Err(Error::InvalidFee);
        }
        let end=r.effective_until_ns.unwrap_or(i64::MAX);
        for old in &self.records{
            if old.series!=r.series||old.role!=r.role{continue;}
            let old_end=old.effective_until_ns.unwrap_or(i64::MAX);
            if r.effective_from_ns<old_end&&old.effective_from_ns<end{
                return Err(Error::InvalidFee);
            }
        }
        self.records.push(r);Ok(())
    }
    pub fn resolve(&self,series:&str,role:FeeRole,asof_ns:i64)
        ->Result<&FeeSchedule,Error>{
        let mut found=None;
        for r in &self.records{
            if r.series!=series||r.role!=role||
                asof_ns<r.effective_from_ns||
                r.effective_until_ns.is_some_and(|v|asof_ns>=v){continue;}
            if asof_ns<r.published_ns||asof_ns<r.observed_ns{
                return Err(Error::FutureRule);
            }
            if found.is_some(){return Err(Error::InvalidFee);}
            found=Some(r);
        }
        found.ok_or(Error::UnknownFee)
    }
    /// Only registry resolution can produce a fee with evidenced effective
    /// time and series, suitable for the modeled accounting interface.
    pub fn charge_at(&self,series:&str,role:FeeRole,asof_ns:i64,
        fills:&[(Price,u64)])->Result<ChargedFee,Error>{
        let schedule=self.resolve(series,role,asof_ns)?;
        let mut charge=schedule.charge(fills)?;
        charge.evaluated_at_ns=Some(asof_ns);
        Ok(charge)
    }
}
fn ceil_step(value:Money,step:Money)->Result<Money,Error>{
    if value<0||step<=0{return Err(Error::InvalidFee);}
    let increment=step.checked_sub(1).ok_or(Error::Overflow)?;
    value.checked_add(increment).and_then(|v|v.checked_div(step))
        .and_then(|v|v.checked_mul(step)).ok_or(Error::Overflow)
}
impl FeeSchedule{
    pub fn charge(&self,fills:&[(Price,u64)])->Result<ChargedFee,Error>{
        if fills.is_empty()||fills.len()>256||
            fills.iter().any(|(_,quantity)|*quantity==0){
            return Err(Error::InvalidFee);
        }
        let mut amount=0i128;
        let rounded=|price:Price,qty:u64|->Result<Money,Error>{
            let raw=match self.formula{
                FeeFormula::QuadraticPrice=>FeeRate{
                    numerator:self.rate_numerator,denominator:self.rate_denominator
                }.charge(qty,price)?,
                FeeFormula::FlatPerContract=>{
                    let n=Money::from(self.rate_numerator)
                        .checked_mul(Money::from(qty)).ok_or(Error::Overflow)?;
                    let d=Money::from(self.rate_denominator);
                    n.checked_add(d-1).and_then(|n|n.checked_div(d))
                        .ok_or(Error::Overflow)?
                },
            };
            ceil_step(raw,Money::from(self.rounding_step_centicents))
        };
        match self.aggregation{
            FeeAggregation::PerFill=>{
                // A fill aggregation has one price, not fabricated averaging.
                let price=fills[0].0;
                if fills.iter().any(|(p,_)|*p!=price){
                    return Err(Error::InvalidFee);
                }
                let qty=fills.iter().try_fold(0u64,|a,(_,q)|
                    a.checked_add(*q)).ok_or(Error::Overflow)?;
                amount=rounded(price,qty)?;
            },
            FeeAggregation::PerPriceLevel=>{
                for (price,qty) in fills{
                    amount=amount.checked_add(rounded(*price,*qty)?)
                        .ok_or(Error::Overflow)?;
                }
            },
        }
        if let Some(minimum)=self.minimum_fee_centicents{
            amount=amount.max(Money::from(minimum));
        }
        if let Some(limit)=self.maximum_fee_centicents{
            amount=amount.min(Money::from(limit));
        }
        Ok(ChargedFee{fee_centicents:amount,
            source_sha256:self.source_sha256.clone(),role:self.role,
            schedule_series:self.series.clone(),evaluated_at_ns:None,
            schedule_effective_from_ns:self.effective_from_ns,
            aggregation:self.aggregation})
    }
}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum OutcomeTier {
    UnknownOrPending,ArchivedPaper,SourceDerivedProvisional,
    FreshOfficial,IndependentCorroborated,ConflictingOrInvalid,
}
#[derive(Debug,Clone)]
pub struct PrimaryTick {pub second_utc:i64,pub value_1e8:i128}
#[derive(Debug,Clone)]
pub struct PrimaryImport {
    pub provider:String,pub benchmark_id:String,
    pub methodology_revision:String,pub source_sha256:String,
    pub ticks_sha256:String,
    pub document_sha256:String,pub retrieved_at_ns:i64,
    pub observed_at_ns:i64,pub first_second_utc:i64,
    pub ticks:Vec<PrimaryTick>,
}
pub fn primary_ticks_digest(ticks:&[PrimaryTick])->Result<String,Error>{
    if ticks.is_empty()||ticks.len()>MAX_TICKS{return Err(Error::InvalidPrimary);}
    let mut h=Sha256::new();
    h.update((ticks.len() as u64).to_le_bytes());
    for tick in ticks{
        h.update(tick.second_utc.to_le_bytes());
        h.update(tick.value_1e8.to_le_bytes());
    }
    Ok(format!("{:x}",h.finalize()))
}
/// Authorization and provenance validation must be provided by a trusted
/// external primary-source integration, never by two different labels.
pub trait PrimaryProvenanceVerifier {
    fn authenticate(&self,source:&PrimaryImport,contract:&ContractDefinition)->bool;
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct SettlementAssessment{
    pub tier:OutcomeTier,pub market:String,
    pub official_winner:Option<String>,pub source_derived_winner:Option<String>,
    pub official_source_sha256:Option<String>,
    pub primary_source_sha256:Option<String>,
    pub blocked_reasons:Vec<String>,
    // Not publicly constructible: only a trusted primary verifier can set it.
    authenticated_primary:bool,
}
fn as_text(side:Side)->String{
    match side{Side::Yes=>"yes".into(),Side::No=>"no".into()}
}
fn official_winner(o:&OfficialMarketEvidence)->Option<Side>{
    match o.status{
        OfficialStatus::FinalYes=>Some(Side::Yes),
        OfficialStatus::FinalNo=>Some(Side::No),
        _=>None,
    }
}
/// Structural 1Hz completeness test only. Passing does NOT authenticate the
/// origin of local samples or their compliance with a real contract.
pub fn validate_primary_windows(
    contract:&ContractDefinition,source:&PrimaryImport)
    ->Result<phoenix_settlement::WindowComparison,Error>{
    contract.validate()?;
    if source.provider!=contract.reference_provider||
        source.benchmark_id!=contract.benchmark_id||
        source.methodology_revision!=contract.methodology_revision||
        !sha_ok(&source.source_sha256)||!sha_ok(&source.document_sha256)||
        !sha_ok(&source.ticks_sha256)||
        primary_ticks_digest(&source.ticks)?!=source.ticks_sha256||
        source.document_sha256!=contract.source_document_sha256||
        source.observed_at_ns>source.retrieved_at_ns||
        source.ticks.len()>MAX_TICKS||source.ticks.is_empty()||
        source.first_second_utc!=contract.window.opening_start_sec{
        return Err(Error::InvalidPrimary);
    }
    let source_ticks=source.ticks.iter().map(|t|IndexTick{
        second_utc:t.second_utc,value_1e8:t.value_1e8}).collect::<Vec<_>>();
    Ok(compare_window_averages(&source_ticks,
        contract.window.to_existing_rule())?)
}
/// Requires authenticated primary origin, exact window and ticker matching.
/// Neither an archived Kalshi relay nor different filenames grant independence.
pub fn corroborate_primary(
    contract:&ContractDefinition,
    official:&OfficialMarketEvidence,
    source:&PrimaryImport,verifier:&impl PrimaryProvenanceVerifier)
    ->Result<SettlementAssessment,Error>{
    contract.validate()?;
    if official.ticker!=contract.ticker||official.market_id!=contract.market_id{
        return Err(Error::WrongMarket);
    }
    if !official.is_authenticated_public_get(){
        return Err(Error::UnknownOfficial);
    }
    let official_side=official_winner(official).ok_or(Error::UnknownOfficial)?;
    let derived=validate_primary_windows(contract,source)?;
    if !verifier.authenticate(source,contract){
        return Err(Error::UnauthenticatedPrimary);
    }
    let tier=if official_side==derived.derived_winner{
        OutcomeTier::IndependentCorroborated
    }else{OutcomeTier::ConflictingOrInvalid};
    Ok(SettlementAssessment{
        tier,market:contract.ticker.clone(),
        official_winner:Some(as_text(official_side)),
        source_derived_winner:Some(as_text(derived.derived_winner)),
        official_source_sha256:Some(official.source_sha256.clone()),
        primary_source_sha256:Some(source.source_sha256.clone()),
        blocked_reasons:if tier==OutcomeTier::ConflictingOrInvalid{
            vec!["PRIMARY_OFFICIAL_CONFLICT".into()]
        }else{vec![]},
        authenticated_primary:tier==OutcomeTier::IndependentCorroborated,
    })
}
pub fn assess_official(
    contract:&ContractDefinition,official:Option<&OfficialMarketEvidence>)
    ->SettlementAssessment{
    let mut out=SettlementAssessment{
        tier:OutcomeTier::UnknownOrPending,market:contract.ticker.clone(),
        official_winner:None,source_derived_winner:None,
        official_source_sha256:None,primary_source_sha256:None,
        blocked_reasons:vec!["BLOCKED_INDEPENDENT_SETTLEMENT".into()],
        authenticated_primary:false,
    };
    match official{
        None=>out.blocked_reasons.push("MISSING_FRESH_OFFICIAL_FINAL".into()),
        Some(o) if o.ticker!=contract.ticker||o.market_id!=contract.market_id=>{
            out.tier=OutcomeTier::ConflictingOrInvalid;
            out.blocked_reasons.push("MARKET_IDENTITY_MISMATCH".into());
        },
        Some(o)=>{
            if !o.is_authenticated_public_get(){
                out.blocked_reasons.push("UNVERIFIED_LOCAL_OFFICIAL_PAYLOAD".into());
            }else if let Some(w)=official_winner(o){
                out.tier=OutcomeTier::FreshOfficial;
                out.official_winner=Some(as_text(w));
                out.official_source_sha256=Some(o.source_sha256.clone());
            }else{
                out.blocked_reasons.push("NOT_FINALIZED_BINARY".into());
            }
        },
    }
    out
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ModeledAccounting {
    pub market:String,pub modeled_fill:bool,
    pub entry_price_ticks:u16,pub quantity:u64,
    pub modeled_fee_centicents:Option<Money>,
    pub official_result:Option<String>,
    pub independently_verified:bool,
    pub contract_payout_centicents:Option<Money>,
    pub net_modeled_pnl_centicents:Option<Money>,
    pub uncertainty:Vec<String>,
}
pub fn modeled_expiry_pnl(
    contract:&ContractDefinition,side:Side,price:Price,quantity:u64,
    fee:Option<&ChargedFee>,official:Option<&OfficialMarketEvidence>)
    ->Result<ModeledAccounting,Error>{
    contract.validate()?;
    if quantity==0{return Err(Error::InvalidFee);}
    if let Some(e)=fee{
        if e.schedule_series!=contract.series||!sha_ok(&e.source_sha256)||
            e.evaluated_at_ns.map_or(true,|at|
                at<contract.market_open_ns||at>contract.expiration_ns||
                at<e.schedule_effective_from_ns){
            return Err(Error::InvalidFee);
        }
    }
    let mut out=ModeledAccounting{
        market:contract.ticker.clone(),modeled_fill:true,
        entry_price_ticks:price.ticks(),quantity,
        modeled_fee_centicents:fee.map(|f|f.fee_centicents),
        official_result:None,independently_verified:false,
        contract_payout_centicents:None,net_modeled_pnl_centicents:None,
        uncertainty:Vec::new(),
    };
    if fee.is_none(){out.uncertainty.push("UNKNOWN_HISTORICAL_FEE".into());}
    let outcome=assess_official(contract,official);
    if outcome.tier!=OutcomeTier::FreshOfficial{
        out.uncertainty.push("MISSING_FRESH_OFFICIAL_FINAL".into());
        return Ok(out);
    }
    out.official_result=outcome.official_winner.clone();
    let winner=if outcome.official_winner.as_deref()==Some("yes"){
        Side::Yes
    }else{Side::No};
    let f=match fee{Some(f)=>f,None=>return Ok(out)};
    if quantity==0{return Err(Error::InvalidFee);}
    let fill=ModeledBuyFill{market:contract.ticker.clone(),
        rules_sha256:contract.version_sha256.clone(),
        side,price,quantity,entry_fee:f.fee_centicents};
    let binary=BinaryFinal{market:contract.ticker.clone(),
        rules_sha256:contract.version_sha256.clone(),winner,
        venue_final:DomainEvidence{
            source_id:"kalshi-official-market-final".into(),
            content_sha256:official.unwrap().source_sha256.clone(),
        },independent:None};
    let modeled=settle_binary(&fill,&binary)?;
    let payout=if side==winner{
        Money::from(contract.payout_ticks).checked_mul(Money::from(quantity))
            .ok_or(Error::Overflow)?
    }else{0};
    out.contract_payout_centicents=Some(payout);
    out.net_modeled_pnl_centicents=Some(modeled.net);
    out.uncertainty.push("EXECUTION_REMAINS_MODELED".into());
    out.uncertainty.push("BLOCKED_INDEPENDENT_SETTLEMENT".into());
    Ok(out)
}
/// Report independent settlement only from a separately authenticated
/// provenance assessment matching the same contract and official winner.
pub fn modeled_expiry_pnl_with_assessment(
    contract:&ContractDefinition,side:Side,price:Price,quantity:u64,
    fee:Option<&ChargedFee>,official:Option<&OfficialMarketEvidence>,
    assessment:Option<&SettlementAssessment>)->Result<ModeledAccounting,Error>{
    let mut result=modeled_expiry_pnl(contract,side,price,quantity,fee,official)?;
    if let Some(a)=assessment{
        if a.market!=contract.ticker{return Err(Error::WrongMarket);}
        if a.tier==OutcomeTier::ConflictingOrInvalid{
            return Err(Error::ConflictingSettlement);
        }
        if a.tier==OutcomeTier::IndependentCorroborated&&a.authenticated_primary&&
            official.is_some_and(|o|
                a.official_source_sha256.as_deref()==Some(o.source_sha256.as_str())&&
                o.is_authenticated_public_get())&&
            a.official_winner==result.official_result&&
            a.primary_source_sha256.as_deref().is_some_and(sha_ok){
            result.independently_verified=true;
            result.uncertainty.retain(|x|x!="BLOCKED_INDEPENDENT_SETTLEMENT");
        }
    }
    Ok(result)
}
#[cfg(test)]
mod tests{
    use super::*;
    fn rule()->ContractDefinition{
        ContractDefinition{ticker:"KXBTC-TEST".into(),market_id:"MARKET-1".into(),
            event_id:"EVENT-1".into(),series:"KXBTC15M".into(),
            contract_type:ContractType::BinaryIndexComparison,
            market_open_ns:1,expiration_ns:200_000_000_000,
            rule_effective_from_ns:0,rule_effective_until_ns:None,
            rule_published_ns:0,rule_observed_ns:0,
            reference_provider:"CF Benchmarks".into(),
            benchmark_id:"BRTI".into(),methodology_revision:"reviewed-r1".into(),
            window:WindowDefinition{opening_start_sec:0,closing_start_sec:60,
                samples_per_window:60,tie_resolves_yes:true},
            payout_ticks:10000,source_document_id:"rule-pdf".into(),
            source_document_sha256:"a".repeat(64),
            version_sha256:"b".repeat(64)}
    }
    fn fee(role:FeeRole)->FeeSchedule{
        FeeSchedule{series:"KXBTC15M".into(),role,
            effective_from_ns:0,effective_until_ns:Some(100),
            published_ns:0,observed_ns:0,source_sha256:"a".repeat(64),
            source_document_id:"test-schedule".into(),
            formula:FeeFormula::QuadraticPrice,
            rate_numerator:7,rate_denominator:100,
            rounding_step_centicents:100,aggregation:FeeAggregation::PerFill,
            minimum_fee_centicents:None,maximum_fee_centicents:None}
    }
    #[test]fn effective_rules_never_look_forward_or_overlap(){
        let mut r=ContractRegistry::default();
        r.insert(rule()).unwrap();
        assert!(r.resolve("KXBTC-TEST",0).is_ok());
        assert!(matches!(r.resolve("OTHER",5),Err(Error::UnknownRule)));
        assert_eq!(r.insert(rule()),Err(Error::MultipleRules));
    }
    #[test]fn role_and_effective_schedule_are_required(){
        let mut f=FeeRegistry::default();f.insert(fee(FeeRole::Taker)).unwrap();
        assert!(matches!(f.resolve("KXBTC15M",FeeRole::Maker,5),Err(Error::UnknownFee)));
        assert!(matches!(f.resolve("KXBTC15M",FeeRole::Taker,100),Err(Error::UnknownFee)));
        assert!(f.resolve("KXBTC15M",FeeRole::Taker,50).is_ok());
    }
    #[test]fn maker_taker_schedule_uses_existing_fixed_point_rates(){
        let p=Price::new(5000).unwrap();
        let computed=fee(FeeRole::Taker).charge(&[(p,2)]).unwrap();
        assert_eq!(computed.fee_centicents%100,0);
        assert!(computed.fee_centicents>0);
    }
    #[test]fn historical_fee_accounting_requires_time_and_matching_series(){
        let mut registry=FeeRegistry::default();
        registry.insert(fee(FeeRole::Taker)).unwrap();
        let amount=registry.charge_at("KXBTC15M",FeeRole::Taker,50,
            &[(Price::new(5000).unwrap(),1)]).unwrap();
        assert_eq!(amount.evaluated_at_ns,Some(50));
        let mut mismatch=amount.clone();mismatch.schedule_series="OTHER".into();
        assert!(matches!(modeled_expiry_pnl(&rule(),Side::Yes,
            Price::new(5000).unwrap(),1,Some(&mismatch),None),
            Err(Error::InvalidFee)));
        assert!(modeled_expiry_pnl(&rule(),Side::Yes,
            Price::new(5000).unwrap(),1,Some(&amount),None).unwrap()
            .net_modeled_pnl_centicents.is_none());
    }
    #[test]fn invalid_contract_types_and_primary_missing_fail_closed(){
        let mut r=rule();r.contract_type=ContractType::Scalar;
        assert_eq!(r.validate(),Err(Error::UnsupportedContract));
        let r=rule();let outcome=assess_official(&r,None);
        assert_eq!(outcome.tier,OutcomeTier::UnknownOrPending);
    }
    #[test]fn primary_window_gap_is_not_interpolated(){
        struct TrustedForFixture;
        impl PrimaryProvenanceVerifier for TrustedForFixture{
            fn authenticate(&self,_:&PrimaryImport,_:&ContractDefinition)->bool{true}
        }
        let ticks=(0..120).filter(|s|*s!=61).map(|s|PrimaryTick{
            second_utc:s,value_1e8:100+if s>=60{1}else{0}}).collect::<Vec<_>>();
        let ticks_sha256=primary_ticks_digest(&ticks).unwrap();
        let p=PrimaryImport{provider:"CF Benchmarks".into(),benchmark_id:"BRTI".into(),
            methodology_revision:"reviewed-r1".into(),source_sha256:"c".repeat(64),
            ticks_sha256,document_sha256:"a".repeat(64),
            retrieved_at_ns:20,observed_at_ns:10,
            first_second_utc:0,ticks};
        assert_eq!(validate_primary_windows(&rule(),&p),
            Err(Error::Window(phoenix_settlement::Error::MissingSample)));
        // A local JSON body is not a fresh official HTTPS response, even when
        // its status field says settled.
        let raw=br#"{"market":{"ticker":"KXBTC-TEST","market_id":"MARKET-1",
            "status":"settled","result":"yes"}}"#;
        let o=phoenix_kalshi_read_only::parse_official_market("KXBTC-TEST",raw,20)
            .unwrap();
        assert_eq!(corroborate_primary(&rule(),&o,&p,&TrustedForFixture),
            Err(Error::UnknownOfficial));
    }
}

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
    pub ticker:String,pub market_id:Option<String>,
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
        if !valid_name(&self.ticker)||
            self.market_id.as_deref().is_some_and(|id|!valid_name(id))||
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
/// Exact fractional-contract fee schedule extension used by phoenix-sim.
/// Quantities are hundredths of one contract, never rounded to whole units.
impl FeeSchedule{
    pub fn charge_fractional(&self,fills:&[(Price,u64)])
        ->Result<ChargedFee,Error>{
        if fills.is_empty()||fills.len()>256||
            fills.iter().any(|(_,q)|*q==0){return Err(Error::InvalidFee);}
        let raw_for=|p:Price,qty_fp:u64|->Result<Money,Error>{
            let n=match self.formula{
                FeeFormula::QuadraticPrice=>{
                    u128::from(qty_fp).checked_mul(u128::from(self.rate_numerator))
                        .and_then(|n|n.checked_mul(u128::from(p.ticks())))
                        .and_then(|n|n.checked_mul(u128::from(10_000-p.ticks())))
                        .ok_or(Error::Overflow)?
                },
                FeeFormula::FlatPerContract=>{
                    u128::from(qty_fp).checked_mul(u128::from(self.rate_numerator))
                        .ok_or(Error::Overflow)?
                },
            };
            let multiplier:u128=match self.formula{
                FeeFormula::QuadraticPrice=>1_000_000, // 100 fractional * 10,000 ticks
                FeeFormula::FlatPerContract=>100,
            };
            let d=u128::from(self.rate_denominator).checked_mul(multiplier)
                .ok_or(Error::Overflow)?;
            if d==0{return Err(Error::InvalidFee);}
            let q=n/d+u128::from(n%d!=0);
            let units=Money::try_from(q).map_err(|_|Error::Overflow)?;
            ceil_step(units,Money::from(self.rounding_step_centicents))
        };
        let amount=match self.aggregation{
            FeeAggregation::PerFill=>{
                let p=fills[0].0;
                if fills.iter().any(|(other,_)|*other!=p){
                    return Err(Error::InvalidFee);
                }
                let quantity=fills.iter().try_fold(0u64,|sum,(_,q)|
                    sum.checked_add(*q)).ok_or(Error::Overflow)?;
                raw_for(p,quantity)?
            },
            FeeAggregation::PerPriceLevel=>{
                let mut sum:Money=0;
                for (p,q) in fills{
                    sum=sum.checked_add(raw_for(*p,*q)?).ok_or(Error::Overflow)?;
                }
                sum
            },
        };
        let amount=if let Some(min)=self.minimum_fee_centicents{
            amount.max(Money::from(min))
        }else{amount};
        let amount=if let Some(max)=self.maximum_fee_centicents{
            amount.min(Money::from(max))
        }else{amount};
        Ok(ChargedFee{fee_centicents:amount,
            source_sha256:self.source_sha256.clone(),role:self.role,
            schedule_series:self.series.clone(),evaluated_at_ns:None,
            schedule_effective_from_ns:self.effective_from_ns,
            aggregation:self.aggregation})
    }
}
impl FeeRegistry{
    pub fn charge_fractional_at(&self,series:&str,role:FeeRole,
        asof_ns:i64,fills:&[(Price,u64)])->Result<ChargedFee,Error>{
        let schedule=self.resolve(series,role,asof_ns)?;
        let mut fee=schedule.charge_fractional(fills)?;
        fee.evaluated_at_ns=Some(asof_ns);
        Ok(fee)
    }
}
/// Binding from a unique contract ticker to its original effective-dated
/// series fees; unknown or future fees never default to zero.
pub struct HistoricalFeeBridge<'a>{
    pub registry:&'a FeeRegistry,
    pub ticker:&'a str,
    pub series:&'a str,
}
impl phoenix_sim::historical::HistoricalFeeSource for HistoricalFeeBridge<'_>{
    fn quote(&self,market:&str,role:phoenix_sim::historical::LiquidityRole,
        price:Price,quantity_hundredths:u64,at_ns:i64)
        ->Option<phoenix_sim::historical::FeeQuote>{
        if market!=self.ticker{return None;}
        let fee_role=match role{
            phoenix_sim::historical::LiquidityRole::Maker=>FeeRole::Maker,
            phoenix_sim::historical::LiquidityRole::Taker=>FeeRole::Taker,
        };
        let charged=self.registry.charge_fractional_at(self.series,
            fee_role,at_ns,&[(price,quantity_hundredths)]).ok()?;
        Some(phoenix_sim::historical::FeeQuote{
            fee_centicents:charged.fee_centicents,
            effective_schedule_sha256:charged.source_sha256,
        })
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
    // SHA of the primary benchmark methodology document, NOT the Kalshi
    // contract rules document; these are independent provenance identities.
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
/// Every independent source hash is retained independently, never compared
/// to an unrelated document merely because it is another SHA-256 digest.
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct SettlementProvenance{
    pub contract_rules_document_sha256:String,
    pub contract_registry_version_sha256:String,
    pub primary_methodology_document_sha256:Option<String>,
    pub primary_methodology_revision:Option<String>,
    pub primary_response_sha256:Option<String>,
    pub primary_observations_sha256:Option<String>,
    pub official_response_sha256:Option<String>,
    pub official_retrieved_at_ns:Option<i64>,
    pub primary_retrieved_at_ns:Option<i64>,
    pub primary_observed_at_ns:Option<i64>,
}
impl SettlementProvenance{
    fn from_contract(c:&ContractDefinition)->Self{
        Self{contract_rules_document_sha256:c.source_document_sha256.clone(),
            contract_registry_version_sha256:c.version_sha256.clone(),
            primary_methodology_document_sha256:None,
            primary_methodology_revision:None,
            primary_response_sha256:None,primary_observations_sha256:None,
            official_response_sha256:None,official_retrieved_at_ns:None,
            primary_retrieved_at_ns:None,primary_observed_at_ns:None}
    }
    fn with_official(mut self,o:&OfficialMarketEvidence)->Self{
        self.official_response_sha256=Some(o.source_sha256.clone());
        self.official_retrieved_at_ns=Some(o.retrieved_at_ns);
        self
    }
    fn with_primary(mut self,p:&PrimaryImport)->Self{
        self.primary_methodology_document_sha256=Some(p.document_sha256.clone());
        self.primary_methodology_revision=Some(p.methodology_revision.clone());
        self.primary_response_sha256=Some(p.source_sha256.clone());
        self.primary_observations_sha256=Some(p.ticks_sha256.clone());
        self.primary_retrieved_at_ns=Some(p.retrieved_at_ns);
        self.primary_observed_at_ns=Some(p.observed_at_ns);
        self
    }
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct SettlementAssessment{
    pub tier:OutcomeTier,pub market:String,
    pub official_winner:Option<String>,pub source_derived_winner:Option<String>,
    pub official_source_sha256:Option<String>,
    pub primary_source_sha256:Option<String>,
    pub blocked_reasons:Vec<String>,
    pub provenance:SettlementProvenance,
    // Not publicly constructible: only a trusted primary verifier can set it.
    authenticated_primary:bool,
}
fn as_text(side:Side)->String{
    match side{Side::Yes=>"yes".into(),Side::No=>"no".into()}
}
/// Exact contract identity is required. The Kalshi API documents ticker as
/// canonical; its market_id is optional. When both authoritative records
/// contain an archived ID or event ticker they must not conflict.
fn official_identity_matches(c:&ContractDefinition,o:&OfficialMarketEvidence)->bool{
    o.ticker==c.ticker&&o.market_type.as_deref()==Some("binary")&&
    !matches!((&o.market_id,&c.market_id),
        (Some(official),Some(archived)) if official!=archived)&&
    o.event_ticker.as_deref().map_or(true,|event|event==c.event_id)
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
        source.observed_at_ns>source.retrieved_at_ns||
        source.ticks.len()>MAX_TICKS||source.ticks.is_empty()||
        source.first_second_utc!=contract.window.opening_start_sec{
        return Err(Error::InvalidPrimary);
    }
    // The complete primary capture cannot be observed before its last
    // second has elapsed. This is separate from source/data authenticity.
    let last=source.ticks.iter().map(|t|t.second_utc).max()
        .ok_or(Error::MissingPrimary)?;
    let last_tick_end_ns=last.checked_add(1)
        .and_then(|s|s.checked_mul(1_000_000_000))
        .ok_or(Error::Overflow)?;
    if source.observed_at_ns<last_tick_end_ns{return Err(Error::InvalidPrimary);}
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
    if !official_identity_matches(contract,official){
        return Err(Error::WrongMarket);
    }
    if official.retrieved_at_ns<contract.expiration_ns{
        return Err(Error::UnknownOfficial);
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
        provenance:SettlementProvenance::from_contract(contract)
            .with_official(official).with_primary(source),
        authenticated_primary:tier==OutcomeTier::IndependentCorroborated,
    })
}
/// An archived ledger claim or source-derived winner never substitutes for a
/// fresh official finalized market and independently authenticated reference.
pub fn assess_provisional(
    contract:&ContractDefinition,market:&str,winner:Option<Side>,
    source_sha256:&str,is_archived_paper:bool)->Result<SettlementAssessment,Error>{
    contract.validate()?;
    if market!=contract.ticker{return Err(Error::WrongMarket);}
    if !sha_ok(source_sha256){return Err(Error::InvalidSource);}
    Ok(SettlementAssessment{
        tier:if is_archived_paper{OutcomeTier::ArchivedPaper}
            else{OutcomeTier::SourceDerivedProvisional},
        market:market.to_owned(),
        official_winner:None,source_derived_winner:winner.map(as_text),
        official_source_sha256:None,primary_source_sha256:Some(source_sha256.into()),
        blocked_reasons:vec![
            "NOT_FRESH_OFFICIAL_FINAL".into(),
            "BLOCKED_INDEPENDENT_SETTLEMENT".into(),
        ],provenance:SettlementProvenance::from_contract(contract),
        authenticated_primary:false,
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
        provenance:SettlementProvenance::from_contract(contract),
    };
    match official{
        None=>out.blocked_reasons.push("MISSING_FRESH_OFFICIAL_FINAL".into()),
        Some(o) if !official_identity_matches(contract,o)=>{
            out.tier=OutcomeTier::ConflictingOrInvalid;
            out.blocked_reasons.push("MARKET_IDENTITY_MISMATCH".into());
        },
        Some(o)=>{
            out.provenance=out.provenance.with_official(o);
            if !o.is_authenticated_public_get(){
                out.blocked_reasons.push("UNVERIFIED_LOCAL_OFFICIAL_PAYLOAD".into());
            }else if o.retrieved_at_ns<contract.expiration_ns{
                out.blocked_reasons.push("OFFICIAL_RESPONSE_PREDATES_EXPIRATION".into());
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
        // No historical order execution evidence exists in this price-only API.
        market:contract.ticker.clone(),modeled_fill:false,
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
    // Payout math may be evaluated, but a hypothetical supplied entry price
    // is NOT a backtested fill; empirical net modeled P&L remains unavailable.
    let _counterfactual_net=modeled.net;
    out.uncertainty.push("NO_MODELED_EXECUTION_EVIDENCE".into());
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
        ContractDefinition{ticker:"KXBTC-TEST".into(),market_id:Some("MARKET-1".into()),
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
    #[test]fn paper_and_source_derived_never_claim_official_evidence(){
        let p=assess_provisional(&rule(),"KXBTC-TEST",Some(Side::Yes),
            &"a".repeat(64),true).unwrap();
        assert_eq!(p.tier,OutcomeTier::ArchivedPaper);
        assert!(p.official_winner.is_none());
        assert!(!p.authenticated_primary);
        let s=assess_provisional(&rule(),"KXBTC-TEST",Some(Side::No),
            &"b".repeat(64),false).unwrap();
        assert_eq!(s.tier,OutcomeTier::SourceDerivedProvisional);
        assert!(s.official_source_sha256.is_none());
    }
    #[test]fn level_rounding_and_fill_rounding_are_separate(){
        let p1=Price::new(4000).unwrap();
        let p2=Price::new(5000).unwrap();
        let mut f=fee(FeeRole::Taker);
        assert!(matches!(f.charge(&[(p1,1),(p2,1)]),Err(Error::InvalidFee)));
        f.aggregation=FeeAggregation::PerPriceLevel;
        let actual=f.charge(&[(p1,1),(p2,1)]).unwrap();
        assert_eq!(actual.fee_centicents%100,0);
        assert!(actual.fee_centicents>=200);
    }
    #[test]fn future_fee_revision_is_not_applied_retroactively(){
        let mut f=FeeRegistry::default();
        let mut schedule=fee(FeeRole::Maker);
        schedule.published_ns=60;schedule.observed_ns=70;
        f.insert(schedule).unwrap();
        assert!(matches!(f.resolve("KXBTC15M",FeeRole::Maker,50),
            Err(Error::FutureRule)));
    }
    #[test]fn complete_primary_reference_yields_provisional_yes_only(){
        let ticks=(0..120).map(|s|PrimaryTick{
            second_utc:s,value_1e8:if s<60{100}else{101}
        }).collect::<Vec<_>>();
        let digest=primary_ticks_digest(&ticks).unwrap();
        let data=PrimaryImport{
            provider:"CF Benchmarks".into(),benchmark_id:"BRTI".into(),
            methodology_revision:"reviewed-r1".into(),
            source_sha256:"c".repeat(64),ticks_sha256:digest,
            document_sha256:"a".repeat(64),retrieved_at_ns:250_000_000_000,
            observed_at_ns:200_000_000_000,first_second_utc:0,ticks,
        };
        let result=validate_primary_windows(&rule(),&data).unwrap();
        assert_eq!(result.derived_winner,Side::Yes);
        assert_eq!(result.sample_count,60);
        // A complete series alone is NOT verified independent provenance.
        let fixture=phoenix_kalshi_read_only::parse_official_market(
            "KXBTC-TEST",br#"{"market":{"ticker":"KXBTC-TEST",
            "market_id":"MARKET-1","status":"settled","result":"yes"}}"#,100)
            .unwrap();
        assert_eq!(assess_official(&rule(),Some(&fixture)).tier,
            OutcomeTier::UnknownOrPending);
    }
    #[test]fn contract_registry_allows_no_unverified_uuid_without_synthesizing_it(){
        let mut c=rule();
        c.market_id=None;
        c.validate().unwrap();
        let official=phoenix_kalshi_read_only::parse_official_market("KXBTC-TEST",
            br#"{"market":{"ticker":"KXBTC-TEST","event_ticker":"EVENT-1",
                "market_type":"binary","status":"finalized","result":"yes"}}"#,
            250_000_000_000).unwrap();
        assert_eq!(official.market_id,None);
        assert!(official_identity_matches(&c,&official));
        let mut different=c.clone();different.event_id="DIFFERENT".into();
        assert!(!official_identity_matches(&different,&official));
    }
    #[test]fn independent_methodology_document_hash_need_not_equal_contract_hash(){
        let ticks=(0..120).map(|second_utc|PrimaryTick{
            second_utc,value_1e8:100+if second_utc>=60{1}else{0}
        }).collect::<Vec<_>>();
        let p=PrimaryImport{
            provider:"CF Benchmarks".into(),benchmark_id:"BRTI".into(),
            methodology_revision:"reviewed-r1".into(),
            source_sha256:"c".repeat(64),ticks_sha256:primary_ticks_digest(&ticks).unwrap(),
            document_sha256:"d".repeat(64),retrieved_at_ns:250_000_000_000,
            observed_at_ns:200_000_000_000,first_second_utc:0,ticks};
        assert_eq!(validate_primary_windows(&rule(),&p).unwrap().derived_winner,
            Side::Yes);
        let evidence=SettlementProvenance::from_contract(&rule()).with_primary(&p);
        assert_eq!(evidence.contract_rules_document_sha256,"a".repeat(64));
        assert_eq!(evidence.primary_methodology_document_sha256,Some("d".repeat(64)));
    }
    #[test]fn arbitrary_entry_price_never_proves_a_modeled_fill(){
        let p=Price::new(5000).unwrap();
        let fee=fee(FeeRole::Taker);
        let mut charged=fee.charge(&[(p,1)]).unwrap();
        charged.evaluated_at_ns=Some(50);
        let result=modeled_expiry_pnl(&rule(),Side::Yes,p,1,Some(&charged),None).unwrap();
        assert!(!result.modeled_fill);
        assert!(result.net_modeled_pnl_centicents.is_none());
    }
    #[test]fn fractional_quantity_fee_is_exact_and_matches_whole_contract(){
        let schedule=fee(FeeRole::Taker);
        let price=Price::new(5000).unwrap();
        let whole=schedule.charge(&[(price,1)]).unwrap();
        let fp=schedule.charge_fractional(&[(price,100)]).unwrap();
        assert_eq!(whole.fee_centicents,fp.fee_centicents);
        let half=schedule.charge_fractional(&[(price,50)]).unwrap();
        assert!(half.fee_centicents>0);
        assert!(half.fee_centicents<=whole.fee_centicents);
    }
    #[test]fn historical_fee_bridge_rejects_wrong_market_and_future_rate(){
        use phoenix_sim::historical::{HistoricalFeeSource,LiquidityRole};
        let mut registry=FeeRegistry::default();
        registry.insert(fee(FeeRole::Taker)).unwrap();
        let bridge=HistoricalFeeBridge{registry:&registry,
            ticker:"KXBTC-TEST",series:"KXBTC15M"};
        let p=Price::new(5000).unwrap();
        assert!(bridge.quote("OTHER",LiquidityRole::Taker,p,100,50).is_none());
        assert!(bridge.quote("KXBTC-TEST",LiquidityRole::Taker,p,100,100)
            .is_none());
        assert!(bridge.quote("KXBTC-TEST",LiquidityRole::Taker,p,50,50)
            .is_some());
    }
    #[test]fn historical_fee_checked_overflow_does_not_wrap(){
        let mut f=fee(FeeRole::Taker);
        f.rate_numerator=u64::MAX;
        f.rate_denominator=1;
        let result=f.charge(&[(Price::new(5000).unwrap(),u64::MAX)]);
        assert!(result.is_err());
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
            retrieved_at_ns:250_000_000_000,
            observed_at_ns:200_000_000_000,
            first_second_utc:0,ticks};
        assert_eq!(validate_primary_windows(&rule(),&p),
            Err(Error::Window(phoenix_settlement::Error::MissingSample)));
        // A local JSON body is not a fresh official HTTPS response, even when
        // its status field says settled.
        let raw=br#"{"market":{"ticker":"KXBTC-TEST","market_id":"MARKET-1",
            "status":"settled","result":"yes"}}"#;
        let o=phoenix_kalshi_read_only::parse_official_market("KXBTC-TEST",raw,20)
            .unwrap();
        assert!(matches!(corroborate_primary(&rule(),&o,&p,&TrustedForFixture),
            Err(Error::UnknownOfficial)));
    }
}

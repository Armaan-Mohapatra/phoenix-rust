//! Phase 11: immutable, budgeted candidate PROPOSALS, never auto-execution.
//! Uses frozen_registry Definition and research::experiments; no second
//! simulator or reclassification of exploratory OOS as untouched.
use crate::frozen_registry::{Definition, ImmutableRegistry, Lifecycle, Origin};
use phoenix_research::experiments::{Evidence, ExperimentSpec, ImmutableExperiments};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
fn sha(s: &[u8]) -> String {
    format!("{:x}", Sha256::digest(s))
}
fn digest(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}
/// Invert a binary YES probability exactly. Inverting twice is identity;
/// price and expected *profit* are not inverted because fees/spread remain.
pub fn invert_probability(ticks: u16) -> Result<u16, Error> {
    if ticks > 10_000 {
        return Err(Error::InvalidTransform);
    }
    Ok(10_000 - ticks)
}
const MAX_VARIANTS_PER_PARENT: usize = 32;
const MAX_GLOBAL: usize = 256;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Kind {
    FullInversion,
    ConditionalInversion,
    EntryPriceThreshold,
    EntryTiming,
    ExitTiming,
    ConfidenceFilter,
    SpreadFilter,
    DepthFilter,
    LiquidityFilter,
    VolatilityFilter,
    MarketRegimeFilter,
    TimeToExpiryFilter,
    PerpsDirectional,
    PerpsImbalance,
    PerpsTradeFlow,
    KalshiPerpsDivergence,
    ModelMarketDisagreement,
    FeeAwareAbstention,
    MakerTakerChoice,
    LatencyVariation,
    PositionSizeVariation,
    StrategyCombination,
    WeightedEnsemble,
    RiskExposureLimit,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transform {
    pub kind: Kind,
    pub parameter_name: String,
    pub value: i64,
    pub secondary: Option<i64>,
    pub required_feature: Option<String>,
    pub other_parent_id: Option<String>,
    pub rationale: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candidate {
    pub id: String,
    pub parent_id: String,
    pub parent_definition_sha256: String,
    pub transformation_sha256: String,
    pub config_sha256: String,
    pub derived_definition_sha256: String,
    pub discovery_only: bool,
    pub eligible_for_auto_promotion: bool,
    pub execution_model_kind: String,
    pub comparisons_declared: u32,
    pub intended_experiment_id: String,
    pub required_features: BTreeSet<String>,
    pub rejection_reasons: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct Budget {
    pub max_total: usize,
    pub max_per_parent: usize,
    pub max_transformations: usize,
    pub seed: u64,
    pub max_statistical_comparisons: u32,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidParent,
    UnverifiedOriginal,
    InvalidTransform,
    InvalidBudget,
    DuplicatedTransform,
    TooManyCandidates,
    MissingOtherParent,
    RegistryConflict,
    ExperimentConflict,
    UnverifiedFee,
    UntouchedOosRisk,
}
impl Budget {
    pub fn validate(&self) -> Result<(), Error> {
        if self.max_total == 0
            || self.max_total > MAX_GLOBAL
            || self.max_per_parent == 0
            || self.max_per_parent > MAX_VARIANTS_PER_PARENT
            || self.max_transformations == 0
            || self.max_transformations > MAX_GLOBAL
            || self.max_statistical_comparisons == 0
            || self.max_statistical_comparisons > 10_000
        {
            return Err(Error::InvalidBudget);
        }
        Ok(())
    }
}
impl Transform {
    pub fn validate(&self) -> Result<(), Error> {
        if self.parameter_name.is_empty()
            || self.parameter_name.len() > 128
            || self.rationale.trim().is_empty()
            || self.rationale.len() > 1000
            || self
                .required_feature
                .as_ref()
                .is_some_and(|f| f.trim().is_empty())
        {
            return Err(Error::InvalidTransform);
        }
        let bound = match self.kind {
            Kind::FullInversion | Kind::ConditionalInversion => self.value == 1,
            Kind::EntryPriceThreshold
            | Kind::ConfidenceFilter
            | Kind::SpreadFilter
            | Kind::DepthFilter
            | Kind::LiquidityFilter
            | Kind::VolatilityFilter
            | Kind::PerpsImbalance
            | Kind::KalshiPerpsDivergence
            | Kind::ModelMarketDisagreement
            | Kind::FeeAwareAbstention => self.value >= 0 && self.value <= 10_000,
            Kind::EntryTiming
            | Kind::ExitTiming
            | Kind::TimeToExpiryFilter
            | Kind::LatencyVariation => self.value >= 0 && self.value <= 900_000,
            Kind::PositionSizeVariation | Kind::RiskExposureLimit => {
                self.value > 0 && self.value <= 1_000_000
            }
            Kind::MarketRegimeFilter | Kind::PerpsDirectional | Kind::PerpsTradeFlow => {
                self.value >= -10_000 && self.value <= 10_000
            }
            Kind::MakerTakerChoice => self.value == 0 || self.value == 1,
            Kind::StrategyCombination | Kind::WeightedEnsemble => {
                self.value >= 0
                    && self.value <= 10_000
                    && self
                        .secondary
                        .is_some_and(|v| v >= 0 && v <= 10_000 && v + self.value == 10_000)
                    && self.other_parent_id.as_ref().is_some_and(|x| !x.is_empty())
            }
        };
        if !bound {
            return Err(Error::InvalidTransform);
        }
        if matches!(
            self.kind,
            Kind::ConditionalInversion
                | Kind::VolatilityFilter
                | Kind::MarketRegimeFilter
                | Kind::PerpsDirectional
                | Kind::PerpsImbalance
                | Kind::PerpsTradeFlow
                | Kind::KalshiPerpsDivergence
        ) && self.required_feature.is_none()
        {
            return Err(Error::InvalidTransform);
        }
        Ok(())
    }
    pub fn fingerprint(&self) -> Result<String, Error> {
        self.validate()?;
        Ok(sha(
            &serde_json::to_vec(self).map_err(|_| Error::InvalidTransform)?
        ))
    }
}
fn xorshift(x: &mut u64) {
    *x ^= *x << 13;
    *x ^= *x >> 7;
    *x ^= *x << 17;
}
/// Deterministically author at most budget.max_total proposals. No market
/// scan, simulation, random search execution or data-bound optimization.
pub fn propose(
    parent: &Definition,
    transforms: &[Transform],
    budget: &Budget,
    registry: &mut ImmutableRegistry,
    experiments: &mut ImmutableExperiments,
    experiment_template: &ExperimentSpec,
) -> Result<Vec<Candidate>, Error> {
    budget.validate()?;
    if transforms.len() > budget.max_transformations
        || transforms.len() > budget.max_total
        || transforms.len() > budget.max_per_parent
    {
        return Err(Error::TooManyCandidates);
    }
    parent.validate().map_err(|_| Error::InvalidParent)?;
    if parent.origin == Origin::OriginalPartialDefinition
        || parent.origin == Origin::UnavailableSourceDefinition
        || parent.config_sha256.is_none()
        || parent.code_sha256.is_none()
    {
        return Err(Error::UnverifiedOriginal);
    }
    let original = registry.get(&parent.id).ok_or(Error::InvalidParent)?;
    let parent_sha = parent
        .canonical_sha256()
        .map_err(|_| Error::InvalidParent)?;
    if original
        .canonical_sha256()
        .map_err(|_| Error::InvalidParent)?
        != parent_sha
    {
        return Err(Error::InvalidParent);
    }
    if experiment_template.strategy_id != parent.id
        || experiment_template.dataset_sha256.len() != 64
        || experiment_template.fee_schedule_sha256.is_none()
        || !digest(&experiment_template.fee_schedule_sha256.clone().unwrap())
    {
        return Err(Error::UnverifiedFee);
    }
    if experiment_template.discovery_selection_sha256.is_some()
        || experiment_template.status != "PLANNED_DISCOVERY"
    {
        return Err(Error::UntouchedOosRisk);
    }
    let mut ordered = transforms.iter().collect::<Vec<_>>();
    let mut rng = if budget.seed == 0 {
        0x9e3779b97f4a7c15
    } else {
        budget.seed
    };
    // Order is seed-defined, stable and *not* dependent on market outcomes.
    let mut keyed = Vec::new();
    for t in ordered.drain(..) {
        t.validate()?;
        xorshift(&mut rng);
        keyed.push((rng, t.fingerprint()?, t));
    }
    keyed.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for (_, transform_sha, t) in keyed {
        if !seen.insert(transform_sha.clone()) {
            return Err(Error::DuplicatedTransform);
        }
        if matches!(t.kind, Kind::StrategyCombination | Kind::WeightedEnsemble) {
            let other = t
                .other_parent_id
                .as_ref()
                .ok_or(Error::MissingOtherParent)?;
            if registry.get(other).is_none() || other == &parent.id {
                return Err(Error::MissingOtherParent);
            }
        }
        let mut child = parent.clone();
        let stem = format!("{}-hyp-{}", parent.id, &transform_sha[..16]);
        child.id = stem.clone();
        child.model_version = format!("{}-hyp", parent.model_version);
        child.origin = Origin::ReconstructedHypothesis;
        child.lifecycle = Lifecycle::Proposed;
        child.control = None;
        child.lineage_parent = Some(parent.id.clone());
        child.frozen_at_utc = None;
        child.retired_at_utc = None;
        child.parameters.insert(
            format!("{:?}", t.kind),
            format!("{}={};{:?}", t.parameter_name, t.value, t.secondary),
        );
        child
            .parameters
            .insert("transform_sha256".into(), transform_sha.clone());
        child
            .parameters
            .insert("discovery_only".into(), "true".into());
        if let Some(f) = &t.required_feature {
            if matches!(
                t.kind,
                Kind::PerpsDirectional
                    | Kind::PerpsImbalance
                    | Kind::PerpsTradeFlow
                    | Kind::KalshiPerpsDivergence
            ) {
                child.required_perps_features.insert(f.clone());
            } else {
                child.required_kalshi_features.insert(f.clone());
            }
        }
        // New SHA always represents the new derived exact parameter JSON,
        // never overrides or relabels the original frozen configuration.
        let conf_bytes =
            serde_json::to_vec(&child.parameters).map_err(|_| Error::InvalidTransform)?;
        let config = sha(&conf_bytes);
        child.config_sha256 = Some(config.clone());
        let required_features = child
            .required_perps_features
            .iter()
            .chain(child.required_kalshi_features.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        let derived_sha = child
            .canonical_sha256()
            .map_err(|_| Error::InvalidTransform)?;
        // Append immutable strategy first; the caller must use a distinct
        // immutable experiment ID. No cross-phase OOS leak allowed.
        registry
            .derive(&parent.id, child)
            .map_err(|_| Error::RegistryConflict)?;
        let mut experiment = experiment_template.clone();
        experiment.id = format!("{}-discovery", stem);
        experiment.parent_id = Some(experiment_template.id.clone());
        experiment.strategy_id = stem.clone();
        experiment.config_sha256 = config.clone();
        experiment.status = "PLANNED_DISCOVERY".into();
        experiment.random_seed = budget.seed;
        let experiment_id = experiment.id.clone();
        experiments
            .insert(experiment)
            .map_err(|_| Error::ExperimentConflict)?;
        out.push(Candidate {
            id: stem,
            parent_id: parent.id.clone(),
            parent_definition_sha256: parent_sha.clone(),
            transformation_sha256: transform_sha,
            config_sha256: config,
            derived_definition_sha256: derived_sha,
            discovery_only: true,
            eligible_for_auto_promotion: false,
            execution_model_kind: "EXISTING_PHOENIX_SIM_HISTORICAL".into(),
            comparisons_declared: budget.max_statistical_comparisons,
            intended_experiment_id: experiment_id,
            required_features,
            rejection_reasons: vec![
                "NO_MARKET_BACKTEST_EXECUTED".into(),
                "VALIDATION_AND_OOS_INDEPENDENT_PROOF_REQUIRED".into(),
            ],
        });
    }
    Ok(out)
}
/// Apply a bounded subset of candidate transforms to a *hypothetical*
/// parent decision using only causal, source-tagged observations. Transforms
/// requiring historic book/fee replays return RequiresExecutionEvidence,
/// never manufacture prices, fills or outcomes.
#[derive(Debug, Clone)]
pub struct CausalFeature {
    pub name: String,
    pub value: i64,
    pub received_ns: i64,
    pub source_sha256: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppliedStatus {
    Kept,
    Abstained,
    Transformed,
    RequiresExecutionEvidence,
}
#[derive(Debug, Clone)]
pub struct AppliedDecision {
    pub parent_strategy_id: String,
    pub transformation_sha256: String,
    pub action: crate::frozen_registry::Action,
    pub status: AppliedStatus,
    pub reason: String,
    pub proposed_quantity_hundredths: Option<u64>,
    pub verified_venue_execution: bool,
}
pub fn apply_transform(
    t: &Transform,
    parent: &crate::frozen_registry::DecisionTrace,
    book: &phoenix_domain::Book,
    features: &[CausalFeature],
) -> Result<AppliedDecision, Error> {
    use crate::frozen_registry::Action;
    use phoenix_domain::Side;
    t.validate()?;
    if parent.market != book.market
        || book.validate().is_err()
        || parent.decided_at_ns < book.received_ns
        || features.len() > 64
        || features
            .iter()
            .any(|f| f.received_ns > parent.decided_at_ns || !digest(&f.source_sha256))
    {
        return Err(Error::UntouchedOosRisk);
    }
    let key = t.fingerprint()?;
    let mut result = AppliedDecision {
        parent_strategy_id: parent.strategy_id.clone(),
        transformation_sha256: key,
        action: parent.action.clone(),
        status: AppliedStatus::Kept,
        reason: "CAUSAL_PROPOSAL_UNTESTED".into(),
        proposed_quantity_hundredths: None,
        verified_venue_execution: false,
    };
    let feature = |key: &str| features.iter().find(|f| f.name == key).map(|f| f.value);
    if parent.action == Action::Abstain {
        result.status = AppliedStatus::Abstained;
        result.reason = "PARENT_ABSTAINED_NO_FORCED_FILL".into();
        return Ok(result);
    }
    let read = || t.required_feature.as_deref().and_then(feature);
    match t.kind {
        Kind::FullInversion | Kind::ConditionalInversion => {
            if t.kind == Kind::ConditionalInversion && read().is_none() {
                result.action = Action::Abstain;
                result.status = AppliedStatus::Abstained;
                result.reason = "CONDITIONAL_FEATURE_MISSING".into();
                return Ok(result);
            }
            let should = if t.kind == Kind::FullInversion {
                true
            } else {
                read().is_some_and(|v| v >= t.secondary.unwrap_or(0))
            };
            if should {
                if let Action::ProposedBuy { side, .. } = parent.action {
                    let opposite = match side {
                        Side::Yes => Side::No,
                        Side::No => Side::Yes,
                    };
                    if let Some(ask) = book.best_ask(opposite) {
                        result.action = Action::ProposedBuy {
                            side: opposite,
                            limit: ask,
                        };
                        result.status = AppliedStatus::Transformed;
                        result.reason = "OPPOSITE_SIDE_AT_ASOF_OBSERVED_ASK".into();
                    } else {
                        result.action = Action::Abstain;
                        result.status = AppliedStatus::Abstained;
                        result.reason = "OPPOSITE_SIDE_ASK_MISSING".into();
                    }
                }
            }
        }
        Kind::EntryPriceThreshold => {
            if let Action::ProposedBuy { limit, .. } = parent.action {
                if i64::from(limit.ticks()) > t.value {
                    result.action = Action::Abstain;
                    result.status = AppliedStatus::Abstained;
                    result.reason = "ENTRY_PRICE_CAP".into();
                }
            }
        }
        Kind::ConfidenceFilter
        | Kind::DepthFilter
        | Kind::LiquidityFilter
        | Kind::ModelMarketDisagreement
        | Kind::PerpsImbalance
        | Kind::PerpsDirectional
        | Kind::PerpsTradeFlow
        | Kind::KalshiPerpsDivergence => {
            if !read().is_some_and(|v| v >= t.value) {
                result.action = Action::Abstain;
                result.status = AppliedStatus::Abstained;
                result.reason = "MISSING_OR_BELOW_REQUIRED_CAUSAL_FEATURE".into();
            }
        }
        Kind::SpreadFilter
        | Kind::VolatilityFilter
        | Kind::MarketRegimeFilter
        | Kind::TimeToExpiryFilter
        | Kind::FeeAwareAbstention
        | Kind::RiskExposureLimit => {
            // These need an explicit independent point-in-time measurement;
            // absent measurement never gets a default zero.
            let measurement = t.required_feature.as_deref().and_then(feature);
            if !measurement.is_some_and(|v| v <= t.value) {
                result.action = Action::Abstain;
                result.status = AppliedStatus::Abstained;
                result.reason = "UNVERIFIED_OR_ABOVE_FILTER_LIMIT".into();
            }
        }
        Kind::PositionSizeVariation => {
            result.proposed_quantity_hundredths =
                Some(u64::try_from(t.value).map_err(|_| Error::InvalidTransform)?);
            result.status = AppliedStatus::Transformed;
            result.reason = "HYPOTHETICAL_SIZING_ONLY".into();
        }
        Kind::EntryTiming
        | Kind::ExitTiming
        | Kind::MakerTakerChoice
        | Kind::LatencyVariation
        | Kind::StrategyCombination
        | Kind::WeightedEnsemble => {
            result.status = AppliedStatus::RequiresExecutionEvidence;
            result.reason = "REQUIRES_NEW_ASOF_SIMULATION_OR_OTHER_STRATEGY".into();
        }
    }
    Ok(result)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn transform(kind: Kind, val: i64) -> Transform {
        Transform {
            kind,
            parameter_name: "threshold".into(),
            value: val,
            secondary: None,
            required_feature: None,
            other_parent_id: None,
            rationale: "synthetic candidate not validated".into(),
        }
    }
    #[test]
    fn inversion_and_double_inversion_are_different_proposals() {
        assert_eq!(invert_probability(8_000).unwrap(), 2_000);
        assert_eq!(
            invert_probability(invert_probability(8_000).unwrap()).unwrap(),
            8_000
        );
        let a = transform(Kind::FullInversion, 1);
        let mut b = transform(Kind::FullInversion, 1);
        b.parameter_name = "second inversion".into();
        assert_ne!(a.fingerprint().unwrap(), b.fingerprint().unwrap());
        assert_eq!(a.kind, Kind::FullInversion);
    }
    #[test]
    fn invalid_thresholds_or_missing_features_rejected() {
        assert!(transform(Kind::EntryPriceThreshold, 10001)
            .validate()
            .is_err());
        assert!(transform(Kind::PerpsImbalance, 100).validate().is_err());
        assert!(transform(Kind::MakerTakerChoice, 2).validate().is_err());
    }
    #[test]
    fn repeated_transform_has_identical_hash_and_can_be_deduplicated() {
        let a = transform(Kind::SpreadFilter, 250);
        assert_eq!(
            a.fingerprint().unwrap(),
            transform(Kind::SpreadFilter, 250).fingerprint().unwrap()
        );
    }
    #[test]
    fn unknown_caused_filter_abstains_instead_of_guessing_zero() {
        use crate::frozen_registry::{Action, DecisionTrace};
        let t = Transform {
            required_feature: Some("perps_imbalance".into()),
            ..transform(Kind::PerpsImbalance, 150)
        };
        let parent = DecisionTrace {
            strategy_id: "syn".into(),
            version_sha256: "a".repeat(64),
            market: "M".into(),
            event: "E".into(),
            decided_at_ns: 200,
            action: Action::ProposedBuy {
                side: phoenix_domain::Side::Yes,
                limit: phoenix_domain::Price::new(5500).unwrap(),
            },
            reason: "synthetic".into(),
            source_sha256: vec![],
            historical_evidence: "SYNTHETIC".into(),
            venue_fill_verified: false,
        };
        let book = phoenix_domain::Book {
            market: "M".into(),
            yes_bids: vec![phoenix_domain::Level {
                price: phoenix_domain::Price::new(4000).unwrap(),
                quantity: 1,
            }],
            no_bids: vec![phoenix_domain::Level {
                price: phoenix_domain::Price::new(4500).unwrap(),
                quantity: 1,
            }],
            exchange_ns: 100,
            received_ns: 150,
            session_id: "S".into(),
            sequence: 1,
        };
        let out = apply_transform(&t, &parent, &book, &[]).unwrap();
        assert_eq!(out.action, Action::Abstain);
        assert!(!out.verified_venue_execution);
        let future = [CausalFeature {
            name: "perps_imbalance".into(),
            value: 300,
            received_ns: 201,
            source_sha256: "b".repeat(64),
        }];
        assert!(apply_transform(&t, &parent, &book, &future).is_err());
    }
    #[test]
    fn uncontrolled_candidate_budget_is_not_allowed() {
        assert!(Budget {
            max_total: 257,
            max_per_parent: 32,
            max_transformations: 256,
            seed: 1,
            max_statistical_comparisons: 1
        }
        .validate()
        .is_err());
    }
}

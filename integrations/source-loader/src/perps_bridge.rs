//! Phase 8: Phase 7 market identity joined to causal Perps feature buffers.
//! Read-only research. Does not transform Perps into Kalshi venue fills.
use crate::multi_market::{Asset, MarketBatchOutcome, MarketSpec};
use phoenix_historical_input::perps_features::{
    CausalPerpsBuffer, JoinedPerpsFeatures, KalshiPerpsJoin,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    MarketMismatch,
    MissingVerifiedReplay,
    FutureDecision,
    MissingAssetMapping,
    InvalidSourceProvenance,
    FeatureJoin,
}
#[derive(Debug, Serialize)]
pub struct KalshiPerpsFeatureReport {
    pub kalshi_ticker: String,
    pub kalshi_event: String,
    pub kalshi_series: String,
    pub decision_received_ns: i64,
    pub kalshi_replay_sha256: String,
    pub kalshi_catalog_sha256: String,
    pub perps: JoinedPerpsFeatures,
    pub source_eligibility: String,
    pub empirical_execution_verified: bool,
    pub independent_settlement_verified: bool,
    pub historical_pnl_available: bool,
}
fn sha(s: &str) -> String {
    format!("{:x}", Sha256::digest(s.as_bytes()))
}
fn base(spec: &MarketSpec, explicit_other: Option<&str>) -> Result<String, Error> {
    Ok(match spec.asset {
        Asset::Btc => "BTC".into(),
        Asset::Eth => "ETH".into(),
        Asset::Sol => "SOL".into(),
        Asset::Xrp => "XRP".into(),
        Asset::DocumentedOther => {
            if spec.additional_family_schema_sha256.is_none() {
                return Err(Error::MissingAssetMapping);
            }
            let asset = explicit_other.ok_or(Error::MissingAssetMapping)?;
            if asset.is_empty()
                || asset.len() > 32
                || !asset
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
            {
                return Err(Error::MissingAssetMapping);
            }
            asset.into()
        }
    })
}
/// A Perps feature is causal only when its local *receipt timestamp* precedes
/// this market's actual documented decision cutoff. Kalshi replay evidence is
/// still provisional unless separately certified; neither side is a fill.
#[allow(clippy::too_many_arguments)] // explicit evidence inputs; a params struct is a follow-up API change
pub fn join_market_features(
    spec: &MarketSpec,
    outcome: &MarketBatchOutcome,
    buffer: &CausalPerpsBuffer,
    quote_asset: &str,
    other_asset: Option<&str>,
    allowed_venues: BTreeSet<String>,
    max_quote_age_ns: i64,
    lookback_ns: i64,
    require_clock_certain: bool,
) -> Result<KalshiPerpsFeatureReport, Error> {
    if spec.ticker != outcome.ticker
        || spec.event != outcome.event
        || spec.series != outcome.series
        || spec.asset != outcome.asset
        || spec.decision_ns <= 0
        || spec.decision_ns > spec.end_received_ns
    {
        return Err(Error::MarketMismatch);
    }
    let json = outcome
        .replay_json
        .as_deref()
        .ok_or(Error::MissingVerifiedReplay)?;
    let digest = outcome
        .replay_sha256
        .as_deref()
        .ok_or(Error::MissingVerifiedReplay)?;
    if sha(json) != digest {
        return Err(Error::InvalidSourceProvenance);
    }
    let report: serde_json::Value =
        serde_json::from_str(json).map_err(|_| Error::InvalidSourceProvenance)?;
    if report.get("market_ticker").and_then(|v| v.as_str()) != Some(spec.ticker.as_str())
        || report
            .get("decision_asof_received_ns")
            .and_then(|v| v.as_i64())
            != Some(spec.decision_ns)
        || report.get("catalog_sha256").and_then(|v| v.as_str())
            != Some(outcome.source_manifest_sha256.as_str())
    {
        return Err(Error::InvalidSourceProvenance);
    }
    if quote_asset.is_empty()
        || quote_asset.len() > 16
        || !quote_asset
            .bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
    {
        return Err(Error::MissingAssetMapping);
    }
    let request = KalshiPerpsJoin {
        kalshi_ticker: spec.ticker.clone(),
        asset_base: base(spec, other_asset)?,
        quote_asset: quote_asset.into(),
        decision_received_ns: spec.decision_ns,
        lookback_ns,
        max_quote_age_ns,
        allowed_venues,
        require_clock_certain,
    };
    let joined = buffer.join(&request).map_err(|_| Error::FeatureJoin)?;
    if joined.venues.iter().any(|v| {
        v.latest_quote_received_ns > spec.decision_ns
            || v.latest_trade_received_ns
                .is_some_and(|ts| ts > spec.decision_ns)
    }) {
        return Err(Error::FutureDecision);
    }
    Ok(KalshiPerpsFeatureReport {
        kalshi_ticker: spec.ticker.clone(),
        kalshi_event: spec.event.clone(),
        kalshi_series: spec.series.clone(),
        decision_received_ns: spec.decision_ns,
        kalshi_replay_sha256: digest.into(),
        kalshi_catalog_sha256: outcome.source_manifest_sha256.clone(),
        perps: joined,
        source_eligibility: "SOURCE_DERIVED_MARKET_NOT_AUTHENTICATED".into(),
        empirical_execution_verified: false,
        independent_settlement_verified: false,
        historical_pnl_available: false,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::multi_market::{SettlementAvailability, SourceAvailability};
    fn spec() -> MarketSpec {
        MarketSpec {
            ticker: "KXBTC15M-SYNTHETIC".into(),
            event: "E1".into(),
            series: "KXBTC15M".into(),
            asset: Asset::Btc,
            start_received_ns: 10,
            end_received_ns: 100,
            decision_ns: 90,
            named_members: vec!["archive/curated/raw_events/x.parquet".into()],
            contract_rules_sha256: None,
            historical_fee_sha256: None,
            official_final_sha256: None,
            independent_primary_sha256: None,
            additional_family_schema_sha256: None,
        }
    }
    fn outcome() -> MarketBatchOutcome {
        let content = serde_json::json!({"market_ticker":"KXBTC15M-SYNTHETIC",
            "decision_asof_received_ns":90,"catalog_sha256":"a".repeat(64)})
        .to_string();
        MarketBatchOutcome {
            ticker: "KXBTC15M-SYNTHETIC".into(),
            event: "E1".into(),
            series: "KXBTC15M".into(),
            asset: Asset::Btc,
            classification: SourceAvailability::ReplayIncomplete,
            settlement: SettlementAvailability::Unknown,
            contract_rules_available: false,
            historical_fee_available: false,
            reused_prior_verified_report: false,
            fingerprint_sha256: "b".repeat(64),
            source_manifest_sha256: "a".repeat(64),
            replay_sha256: Some(sha(&content)),
            replay_json: Some(content),
            report_exclusions: vec!["MISSING_SNAPSHOT".into()],
            historical_execution_eligible: false,
            independent_settlement_eligible: false,
        }
    }
    #[test]
    fn empty_perps_history_is_explicit_and_no_future_fill() {
        let r = join_market_features(
            &spec(),
            &outcome(),
            &CausalPerpsBuffer::default(),
            "USDT",
            None,
            BTreeSet::from(["VENUE_A".into()]),
            20,
            90,
            true,
        )
        .unwrap();
        assert_eq!(r.perps.missing_venues, vec!["VENUE_A"]);
        assert!(r.perps.venues.is_empty());
        assert!(!r.empirical_execution_verified);
        assert!(!r.historical_pnl_available);
    }
    #[test]
    fn forged_or_mismatched_market_replay_is_rejected() {
        let mut out = outcome();
        out.replay_sha256 = Some("f".repeat(64));
        assert!(matches!(
            join_market_features(
                &spec(),
                &out,
                &CausalPerpsBuffer::default(),
                "USDT",
                None,
                BTreeSet::from(["VENUE_A".into()]),
                20,
                90,
                true
            ),
            Err(Error::InvalidSourceProvenance)
        ));
        let mut wrong = spec();
        wrong.decision_ns = 95;
        assert!(join_market_features(
            &wrong,
            &outcome(),
            &CausalPerpsBuffer::default(),
            "USDT",
            None,
            BTreeSet::from(["VENUE_A".into()]),
            20,
            90,
            true
        )
        .is_err());
    }
}

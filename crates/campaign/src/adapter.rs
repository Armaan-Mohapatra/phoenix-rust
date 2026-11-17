//! Authentic-data adapter: existing source-loader replay report -> one
//! campaign `MarketCase`. Read-only; consumes the JSON written by the
//! verified `phoenix-source-loader` replay (`pipeline::ReplayReport`,
//! schema 1), whose checkpoints come from hash-proven sealed Parquet
//! members, sequence/continuity checks and book-integrity evidence.
//!
//! Joined inputs and rules (nothing is interpolated or inferred):
//! * Book: top of book per checkpoint (`best_yes_bid`, `best_no_bid`).
//!   Quantities are hundredths of a contract; they are floored to whole
//!   contracts and a level below one contract is dropped (conservative).
//!   Depth is 1 level, so a taker larger than displayed top size is
//!   partially filled, never assumed filled deeper.
//! * Executability: the replay marks a checkpoint `execution_eligible` only
//!   via an authenticated snapshot verifier. At Phase 17 that verifier is
//!   intentionally absent (`verified_executable()` returns false), so
//!   REAL replay checkpoints are never eligible. By default the adapter
//!   therefore refuses (`NoExecutionEligibleCheckpoints`). With
//!   `allow_unverified_books` the case is labelled
//!   `Provenance::OperatorUnverified`, which the campaign accepts only as
//!   Exploratory evidence.
//! * Reference: latest operator-supplied observation with
//!   `received_ns <= checkpoint` and age <= `max_reference_age_ns`;
//!   otherwise the checkpoint is skipped and counted.
//! * Perp basis: same as-of rule, optional.
//! * Strike, identities and rules hash: from operator metadata, which must
//!   name the same market as the replay report.
//! * Winner: only from an explicitly supplied settlement record carrying
//!   an evidence hash; absent means pending, never guessed.
//! * `source_sha256` = SHA-256 of the replay report bytes actually read.
use crate::{MarketCase, Provenance, Snapshot};
use phoenix_domain::{Book, Level, Price, Side};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Deserialize)]
struct Lvl {
    price_ticks_1e4: u16,
    quantity_hundredths: u64,
}
#[derive(Deserialize)]
struct Ckpt {
    asof_received_ns: i64,
    last_applied_received_ns: Option<i64>,
    connection_id: Option<String>,
    book_state: String,
    best_yes_bid: Option<Lvl>,
    best_no_bid: Option<Lvl>,
    execution_eligible: bool,
}
#[derive(Deserialize)]
struct ReportView {
    schema_version: u32,
    market_ticker: String,
    decoded_hash_chain_sha256: String,
    checkpoints: Vec<Ckpt>,
    #[serde(default)]
    missing_evidence: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MarketMeta {
    pub market: String,
    pub event: String,
    pub cluster: String,
    pub asset: String,
    pub open_ns: i64,
    pub expiration_ns: i64,
    pub strike_cents: i64,
    pub rules_sha256: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Observation {
    pub received_ns: i64,
    pub value: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Settlement {
    pub market: String,
    pub winner: String,
    /// Hash of the official final (and, ideally, independent reference)
    /// evidence the operator verified.
    pub evidence_sha256: String,
}

#[derive(Debug, Clone, Copy)]
pub struct AdaptPolicy {
    pub max_reference_age_ns: i64,
    pub max_perp_age_ns: i64,
    pub allow_unverified_books: bool,
    pub min_snapshots: usize,
}
impl Default for AdaptPolicy {
    fn default() -> Self {
        Self {
            max_reference_age_ns: 2_000_000_000,
            max_perp_age_ns: 10_000_000_000,
            allow_unverified_books: false,
            min_snapshots: 10,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptError {
    Parse(String),
    UnsupportedSchema(u32),
    WrongMarket { report: String, meta: String },
    BadMeta(String),
    UnorderedObservations(&'static str),
    NoExecutionEligibleCheckpoints { checkpoints: usize },
    TooFewSnapshots { usable: usize, required: usize },
    BadSettlement(String),
    InvalidCase(String),
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct AdaptReport {
    pub checkpoints: usize,
    pub used: usize,
    pub skipped_not_execution_eligible: usize,
    pub skipped_outside_market_window: usize,
    pub skipped_no_reference: usize,
    pub skipped_stale_reference: usize,
    pub skipped_crossed_or_invalid_book: usize,
    pub levels_dropped_below_one_contract: usize,
    pub unverified_books_admitted: bool,
    pub settlement_known: bool,
    pub replay_missing_evidence: Vec<String>,
    pub replay_hash_chain_sha256: String,
    pub source_report_sha256: String,
}

fn hex64(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn asof(obs: &[Observation], t: i64) -> Option<Observation> {
    let n = obs.partition_point(|o| o.received_ns <= t);
    n.checked_sub(1).map(|i| obs[i])
}

pub fn adapt(
    report_bytes: &[u8],
    meta: &MarketMeta,
    references: &[Observation],
    perp_basis_bps: &[Observation],
    settlement: Option<&Settlement>,
    policy: &AdaptPolicy,
) -> Result<(MarketCase, AdaptReport), AdaptError> {
    let r: ReportView =
        serde_json::from_slice(report_bytes).map_err(|e| AdaptError::Parse(e.to_string()))?;
    if r.schema_version != 1 {
        return Err(AdaptError::UnsupportedSchema(r.schema_version));
    }
    if r.market_ticker != meta.market {
        return Err(AdaptError::WrongMarket {
            report: r.market_ticker,
            meta: meta.market.clone(),
        });
    }
    if !hex64(&meta.rules_sha256) || meta.strike_cents <= 0 || meta.open_ns >= meta.expiration_ns {
        return Err(AdaptError::BadMeta(
            "rules hash, strike or window invalid".into(),
        ));
    }
    for (name, o) in [("references", references), ("perp_basis", perp_basis_bps)] {
        if o.windows(2).any(|w| w[0].received_ns >= w[1].received_ns) {
            return Err(AdaptError::UnorderedObservations(name));
        }
    }
    let winner = match settlement {
        None => None,
        Some(s) if s.market != meta.market => {
            return Err(AdaptError::BadSettlement("market mismatch".into()))
        }
        Some(s) if !hex64(&s.evidence_sha256) => {
            return Err(AdaptError::BadSettlement("evidence hash".into()))
        }
        Some(s) => Some(match s.winner.as_str() {
            "yes" => Side::Yes,
            "no" => Side::No,
            w => return Err(AdaptError::BadSettlement(format!("winner {w:?}"))),
        }),
    };
    let source = format!("{:x}", Sha256::digest(report_bytes));
    let mut rep = AdaptReport {
        checkpoints: r.checkpoints.len(),
        replay_missing_evidence: r.missing_evidence.clone(),
        replay_hash_chain_sha256: r.decoded_hash_chain_sha256.clone(),
        source_report_sha256: source.clone(),
        settlement_known: winner.is_some(),
        ..Default::default()
    };
    if !policy.allow_unverified_books && !r.checkpoints.iter().any(|c| c.execution_eligible) {
        return Err(AdaptError::NoExecutionEligibleCheckpoints {
            checkpoints: r.checkpoints.len(),
        });
    }
    let mut snaps: Vec<Snapshot> = Vec::new();
    let mut unverified = false;
    let level = |l: &Option<Lvl>, dropped: &mut usize| -> Option<Level> {
        let l = l.as_ref()?;
        let q = l.quantity_hundredths / 100;
        if q == 0 {
            *dropped += 1;
            return None;
        }
        Some(Level {
            price: Price::new(l.price_ticks_1e4).ok()?,
            quantity: q,
        })
    };
    for (i, c) in r.checkpoints.iter().enumerate() {
        let t = c.asof_received_ns;
        if !c.execution_eligible && !policy.allow_unverified_books {
            rep.skipped_not_execution_eligible += 1;
            continue;
        }
        if t < meta.open_ns
            || t >= meta.expiration_ns
            || snaps.last().is_some_and(|s| s.received_ns >= t)
        {
            rep.skipped_outside_market_window += 1;
            continue;
        }
        let Some(rf) = asof(references, t) else {
            rep.skipped_no_reference += 1;
            continue;
        };
        if t - rf.received_ns > policy.max_reference_age_ns || rf.value <= 0 {
            rep.skipped_stale_reference += 1;
            continue;
        }
        let perp = asof(perp_basis_bps, t)
            .filter(|p| t - p.received_ns <= policy.max_perp_age_ns)
            .and_then(|p| i32::try_from(p.value).ok());
        let mut dropped = 0;
        let book = Book {
            market: meta.market.clone(),
            yes_bids: level(&c.best_yes_bid, &mut dropped).into_iter().collect(),
            no_bids: level(&c.best_no_bid, &mut dropped).into_iter().collect(),
            exchange_ns: c.last_applied_received_ns.unwrap_or(t).min(t),
            received_ns: t,
            session_id: c
                .connection_id
                .clone()
                .unwrap_or_else(|| "replay-checkpoint".into()),
            sequence: i as u64 + 1,
        };
        rep.levels_dropped_below_one_contract += dropped;
        if book.validate().is_err() || c.book_state.is_empty() {
            rep.skipped_crossed_or_invalid_book += 1;
            continue;
        }
        unverified |= !c.execution_eligible;
        snaps.push(Snapshot {
            received_ns: t,
            book,
            reference_cents: rf.value,
            peer_reference_cents: None,
            perp_basis_bps: perp,
        });
    }
    rep.used = snaps.len();
    if snaps.len() < policy.min_snapshots {
        return Err(AdaptError::TooFewSnapshots {
            usable: snaps.len(),
            required: policy.min_snapshots,
        });
    }
    rep.unverified_books_admitted = unverified;
    let case = MarketCase {
        market: meta.market.clone(),
        event: meta.event.clone(),
        cluster: meta.cluster.clone(),
        asset: meta.asset.clone(),
        open_ns: meta.open_ns,
        expiration_ns: meta.expiration_ns,
        strike_cents: meta.strike_cents,
        rules_sha256: meta.rules_sha256.clone(),
        source_sha256: source,
        provenance: if unverified {
            Provenance::OperatorUnverified
        } else {
            Provenance::OperatorNormalized
        },
        snapshots: snaps,
        winner,
    };
    case.validate()
        .map_err(|e| AdaptError::InvalidCase(format!("{e:?}")))?;
    Ok((case, rep))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::campaign::{run_source, CampaignConfig, CampaignError, EvidenceClass};
    use crate::source::VecSource;
    use crate::strategies::default_grid;

    const OPEN: i64 = 1_000_000_000_000;
    const EXP: i64 = OPEN + 900_000_000_000;

    fn report(eligible: bool, n: usize) -> Vec<u8> {
        let cps: Vec<_> = (0..n)
            .map(|i| {
                let t = OPEN + (i as i64 + 1) * 15_000_000_000;
                serde_json::json!({
                    "asof_received_ns": t, "last_applied_received_ns": t - 5,
                    "last_message_sha256": null, "connection_id": "c1", "sid": 1,
                    "last_sequence": i, "book_state": "Valid",
                    "best_yes_bid": {"price_ticks_1e4": 4000, "quantity_hundredths": 1250},
                    "best_yes_ask": null,
                    "best_no_bid": {"price_ticks_1e4": 5500, "quantity_hundredths": if i == 3 { 40 } else { 900 }},
                    "best_no_ask": null, "execution_eligible": eligible
                })
            })
            .collect();
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1, "status": "Provisional", "market_ticker": "KXBTC15M-T1",
            "decoded_hash_chain_sha256": "d".repeat(64), "checkpoints": cps,
            "missing_evidence": ["NO_AUTHENTICATED_SNAPSHOT"], "extra_field_ignored": 1
        }))
        .unwrap()
    }
    fn meta() -> MarketMeta {
        MarketMeta {
            market: "KXBTC15M-T1".into(),
            event: "KXBTC15M-E1".into(),
            cluster: "W1".into(),
            asset: "BTC".into(),
            open_ns: OPEN,
            expiration_ns: EXP,
            strike_cents: 6_000_000,
            rules_sha256: "a".repeat(64),
        }
    }
    fn refs() -> Vec<Observation> {
        (0..=900)
            .map(|s| Observation {
                received_ns: OPEN + s * 1_000_000_000,
                value: 6_000_000 + s,
            })
            .collect()
    }

    #[test]
    fn real_replay_checkpoints_are_refused_by_default() {
        // The Phase 17 replay never marks checkpoints execution-eligible.
        let e = adapt(
            &report(false, 40),
            &meta(),
            &refs(),
            &[],
            None,
            &AdaptPolicy::default(),
        );
        assert_eq!(
            e.unwrap_err(),
            AdaptError::NoExecutionEligibleCheckpoints { checkpoints: 40 }
        );
    }

    #[test]
    fn unverified_override_is_labelled_and_cannot_be_confirmatory() {
        let p = AdaptPolicy {
            allow_unverified_books: true,
            ..Default::default()
        };
        let (c, rep) = adapt(&report(false, 40), &meta(), &refs(), &[], None, &p).unwrap();
        assert_eq!(c.provenance, Provenance::OperatorUnverified);
        assert!(rep.unverified_books_admitted);
        assert_eq!(c.winner, None, "no settlement supplied -> pending");
        assert_eq!(rep.levels_dropped_below_one_contract, 1);
        assert_eq!(c.snapshots[0].book.yes_bids[0].quantity, 12);
        assert!(c.snapshots[3].book.no_bids.is_empty());
        let cases = vec![c];
        let mut src = VecSource {
            cases: &cases,
            batch: 8,
        };
        let e = run_source(
            &mut src,
            &default_grid()[..2],
            &CampaignConfig::default(),
            EvidenceClass::UntouchedConfirmatory,
            &mut |_| Ok(()),
        );
        assert_eq!(e.unwrap_err(), CampaignError::ProvenanceMismatch);
    }

    #[test]
    fn eligible_books_join_references_as_of_without_interpolation() {
        let (c, rep) = adapt(
            &report(true, 40),
            &meta(),
            &refs(),
            &[
                Observation {
                    received_ns: OPEN,
                    value: 9,
                },
                Observation {
                    received_ns: OPEN + 29_000_000_000,
                    value: 7,
                },
            ],
            Some(&Settlement {
                market: "KXBTC15M-T1".into(),
                winner: "yes".into(),
                evidence_sha256: "e".repeat(64),
            }),
            &AdaptPolicy::default(),
        )
        .unwrap();
        assert_eq!(c.provenance, Provenance::OperatorNormalized);
        assert_eq!(c.winner, Some(Side::Yes));
        let s0 = &c.snapshots[0];
        assert_eq!(s0.reference_cents, 6_000_015, "ref at exactly t=15s");
        // Perp observed at t=0 is 15 s old at the first checkpoint (> 10 s cap).
        assert_eq!(s0.perp_basis_bps, None);
        // At t=30 s the t=29 s observation (age 1 s) is used, not interpolated.
        assert_eq!(c.snapshots[1].perp_basis_bps, Some(7));
        assert!(!rep.unverified_books_admitted);
        assert_eq!(rep.used, c.snapshots.len());
        assert_eq!(
            c.source_sha256,
            format!("{:x}", Sha256::digest(report(true, 40)))
        );
    }

    #[test]
    fn stale_or_missing_references_are_skipped_and_counted() {
        let sparse: Vec<Observation> = vec![Observation {
            received_ns: OPEN,
            value: 6_000_000,
        }];
        let e = adapt(
            &report(true, 40),
            &meta(),
            &sparse,
            &[],
            None,
            &AdaptPolicy::default(),
        );
        assert!(matches!(
            e,
            Err(AdaptError::TooFewSnapshots { usable: 0, .. })
        ));
        let wrong = MarketMeta {
            market: "OTHER".into(),
            ..meta()
        };
        assert!(matches!(
            adapt(
                &report(true, 40),
                &wrong,
                &refs(),
                &[],
                None,
                &AdaptPolicy::default()
            ),
            Err(AdaptError::WrongMarket { .. })
        ));
        let mut unordered = refs();
        unordered.swap(1, 2);
        assert_eq!(
            adapt(
                &report(true, 40),
                &meta(),
                &unordered,
                &[],
                None,
                &AdaptPolicy::default()
            )
            .unwrap_err(),
            AdaptError::UnorderedObservations("references")
        );
        let bad = Settlement {
            market: "KXBTC15M-T1".into(),
            winner: "maybe".into(),
            evidence_sha256: "e".repeat(64),
        };
        assert!(matches!(
            adapt(
                &report(true, 40),
                &meta(),
                &refs(),
                &[],
                Some(&bad),
                &AdaptPolicy::default()
            ),
            Err(AdaptError::BadSettlement(_))
        ));
    }
}

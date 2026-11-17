//! Operator-normalized market input (JSON Lines, one market per line).
//!
//! This is the boundary for authentic historical data: an operator exports
//! verified Kalshi books and reference observations into this schema (for
//! example from the sealed Parquet archives via the source-loader crates)
//! and the campaign reads it READ-ONLY. Nothing here fabricates continuity:
//! malformed, unordered or inconsistent lines are rejected with line numbers.
//!
//! ```json
//! {"market":"KXBTC15M-...","event":"...","cluster":"2026-10-24T19:00Z",
//!  "asset":"BTC","open_ns":1,"expiration_ns":2,"strike_cents":6000000,
//!  "rules_sha256":"<64 hex>","source_sha256":"<64 hex>","winner":"yes",
//!  "snapshots":[{"received_ns":1,"exchange_ns":1,"sequence":1,
//!    "session_id":"s","yes_bids":[[4000,10]],"no_bids":[[5500,7]],
//!    "reference_cents":6000100,"peer_reference_cents":null,
//!    "perp_basis_bps":null}]}
//! ```
//! Prices are 1e-4 USD ticks (0..=10000); bids are best-first.
use crate::{MarketCase, Provenance, Snapshot};
use phoenix_domain::{Book, Level, Price, Side};
use serde::Deserialize;
use std::io::{BufRead, BufReader, Read};

pub const MAX_LINE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CASES: usize = 200_000;
pub const MAX_SNAPSHOTS_PER_CASE: usize = 20_000;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSnapshot {
    received_ns: i64,
    exchange_ns: i64,
    sequence: u64,
    session_id: String,
    yes_bids: Vec<(u16, u64)>,
    no_bids: Vec<(u16, u64)>,
    reference_cents: i64,
    peer_reference_cents: Option<i64>,
    perp_basis_bps: Option<i32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCase {
    market: String,
    event: String,
    cluster: String,
    asset: String,
    open_ns: i64,
    expiration_ns: i64,
    strike_cents: i64,
    rules_sha256: String,
    source_sha256: String,
    winner: Option<String>,
    snapshots: Vec<RawSnapshot>,
}

#[derive(Debug)]
pub enum InputError {
    Io(String),
    LineTooLong(usize),
    Parse { line: usize, message: String },
    Invalid { line: usize, reason: String },
    TooManyCases,
}

fn levels(raw: &[(u16, u64)], line: usize) -> Result<Vec<Level>, InputError> {
    raw.iter()
        .map(|(p, q)| {
            Ok(Level {
                price: Price::new(*p).map_err(|_| InputError::Invalid {
                    line,
                    reason: format!("price {p} out of range"),
                })?,
                quantity: *q,
            })
        })
        .collect()
}

pub fn read_cases(reader: impl Read) -> Result<Vec<MarketCase>, InputError> {
    let mut out = Vec::new();
    let mut r = BufReader::new(reader);
    let mut buf = String::new();
    let mut line = 0usize;
    loop {
        buf.clear();
        let n = r
            .by_ref()
            .take(MAX_LINE_BYTES as u64 + 1)
            .read_line(&mut buf)
            .map_err(|e| InputError::Io(e.to_string()))?;
        if n == 0 {
            break;
        }
        line += 1;
        if n > MAX_LINE_BYTES {
            return Err(InputError::LineTooLong(line));
        }
        if buf.trim().is_empty() {
            continue;
        }
        if out.len() >= MAX_CASES {
            return Err(InputError::TooManyCases);
        }
        let raw: RawCase = serde_json::from_str(&buf).map_err(|e| InputError::Parse {
            line,
            message: e.to_string(),
        })?;
        if raw.snapshots.len() > MAX_SNAPSHOTS_PER_CASE {
            return Err(InputError::Invalid {
                line,
                reason: "too many snapshots".into(),
            });
        }
        let winner = match raw.winner.as_deref() {
            None => None,
            Some("yes") => Some(Side::Yes),
            Some("no") => Some(Side::No),
            Some(other) => {
                return Err(InputError::Invalid {
                    line,
                    reason: format!("winner {other:?}"),
                })
            }
        };
        let mut snapshots = Vec::with_capacity(raw.snapshots.len());
        for s in raw.snapshots {
            snapshots.push(Snapshot {
                received_ns: s.received_ns,
                book: Book {
                    market: raw.market.clone(),
                    yes_bids: levels(&s.yes_bids, line)?,
                    no_bids: levels(&s.no_bids, line)?,
                    exchange_ns: s.exchange_ns,
                    received_ns: s.received_ns,
                    session_id: s.session_id,
                    sequence: s.sequence,
                },
                reference_cents: s.reference_cents,
                peer_reference_cents: s.peer_reference_cents,
                perp_basis_bps: s.perp_basis_bps,
            });
        }
        let case = MarketCase {
            market: raw.market,
            event: raw.event,
            cluster: raw.cluster,
            asset: raw.asset,
            open_ns: raw.open_ns,
            expiration_ns: raw.expiration_ns,
            strike_cents: raw.strike_cents,
            rules_sha256: raw.rules_sha256,
            source_sha256: raw.source_sha256,
            provenance: Provenance::OperatorNormalized,
            snapshots,
            winner,
        };
        case.validate().map_err(|e| InputError::Invalid {
            line,
            reason: format!("{e:?}"),
        })?;
        out.push(case);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn line(winner: &str, second_rx: i64) -> String {
        format!(
            r#"{{"market":"M1","event":"E1","cluster":"C1","asset":"BTC","open_ns":0,"expiration_ns":1000,"strike_cents":100,"rules_sha256":"{a}","source_sha256":"{b}","winner":{winner},"snapshots":[{{"received_ns":10,"exchange_ns":9,"sequence":1,"session_id":"s","yes_bids":[[4000,5]],"no_bids":[[5500,5]],"reference_cents":100,"peer_reference_cents":null,"perp_basis_bps":null}},{{"received_ns":{second_rx},"exchange_ns":19,"sequence":2,"session_id":"s","yes_bids":[[4100,5]],"no_bids":[[5500,5]],"reference_cents":101,"peer_reference_cents":null,"perp_basis_bps":3}}]}}"#,
            a = "a".repeat(64),
            b = "b".repeat(64)
        )
    }
    #[test]
    fn valid_lines_parse_with_operator_provenance() {
        let data = format!("{}\n\n{}\n", line("\"yes\"", 20), line("null", 20));
        let cases = read_cases(data.as_bytes()).unwrap();
        assert_eq!(cases.len(), 2);
        assert_eq!(cases[0].provenance, Provenance::OperatorNormalized);
        assert_eq!(cases[0].winner, Some(Side::Yes));
        assert_eq!(cases[1].winner, None);
        assert_eq!(cases[0].snapshots[1].perp_basis_bps, Some(3));
    }
    #[test]
    fn malformed_input_is_rejected_with_line_numbers() {
        let unordered = format!("{}\n{}\n", line("\"no\"", 20), line("\"no\"", 5));
        assert!(matches!(
            read_cases(unordered.as_bytes()),
            Err(InputError::Invalid { line: 2, .. })
        ));
        let bad_winner = line("\"maybe\"", 20);
        assert!(matches!(
            read_cases(bad_winner.as_bytes()),
            Err(InputError::Invalid { line: 1, .. })
        ));
        let extra = line("\"yes\"", 20).replacen("\"asset\"", "\"surprise\":1,\"asset\"", 1);
        assert!(matches!(
            read_cases(extra.as_bytes()),
            Err(InputError::Parse { line: 1, .. })
        ));
        let crossed = line("\"yes\"", 20).replace("[[5500,5]]", "[[6500,5]]");
        assert!(matches!(
            read_cases(crossed.as_bytes()),
            Err(InputError::Invalid { .. })
        ));
    }
}

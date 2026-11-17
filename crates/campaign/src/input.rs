//! Operator-normalized market input (JSON Lines, one market per line),
//! decoded as a stream.
//!
//! This is the boundary for authentic historical data. The file is opened
//! read-only and never modified. Nothing fabricates continuity: malformed,
//! unordered or inconsistent lines are rejected with their line numbers.
//!
//! ```json
//! {"market":"KXBTC15M-...","event":"...","cluster":"2026-10-24T19:00Z",
//!  "asset":"BTC","open_ns":1,"expiration_ns":2,"strike_cents":6000000,
//!  "rules_sha256":"<64 hex>","source_sha256":"<64 hex>","winner":"yes",
//!  "provenance":"operator_normalized",
//!  "snapshots":[{"received_ns":1,"exchange_ns":1,"sequence":1,
//!    "session_id":"s","yes_bids":[[4000,10]],"no_bids":[[5500,7]],
//!    "reference_cents":6000100,"peer_reference_cents":null,
//!    "perp_basis_bps":null}]}
//! ```
//! Prices are 1e-4 USD ticks (0..=10000); bids are best-first.
//! `provenance` is "operator_normalized" (default when absent) or
//! "synthetic"; synthetic exports can never be read back as operator data.
use crate::{MarketCase, Provenance, Snapshot};
use phoenix_domain::{Book, Level, Price, Side};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader, Read};

pub const MAX_LINE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CASES: usize = 200_000;
pub const MAX_SNAPSHOTS_PER_CASE: usize = 20_000;

#[derive(Deserialize, Serialize)]
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

#[derive(Deserialize, Serialize)]
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
    #[serde(default)]
    provenance: Option<String>,
    snapshots: Vec<RawSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

fn decode(buf: &str, line: usize) -> Result<MarketCase, InputError> {
    let raw: RawCase = serde_json::from_str(buf).map_err(|e| InputError::Parse {
        line,
        message: e.to_string(),
    })?;
    if raw.snapshots.len() > MAX_SNAPSHOTS_PER_CASE {
        return Err(InputError::Invalid {
            line,
            reason: "too many snapshots".into(),
        });
    }
    let invalid = |reason: String| InputError::Invalid { line, reason };
    let winner = match raw.winner.as_deref() {
        None => None,
        Some("yes") => Some(Side::Yes),
        Some("no") => Some(Side::No),
        Some(other) => return Err(invalid(format!("winner {other:?}"))),
    };
    let provenance = match raw.provenance.as_deref() {
        None | Some("operator_normalized") => Provenance::OperatorNormalized,
        Some("synthetic") => Provenance::Synthetic,
        Some(other) => return Err(invalid(format!("provenance {other:?}"))),
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
        provenance,
        snapshots,
        winner,
    };
    case.validate().map_err(|e| invalid(format!("{e:?}")))?;
    Ok(case)
}

/// Hashes every byte read from the underlying source.
pub struct HashingReader<R> {
    inner: R,
    h: Sha256,
    bytes: u64,
}
impl<R: Read> HashingReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            h: Sha256::new(),
            bytes: 0,
        }
    }
    pub fn finish(self) -> (String, u64) {
        (format!("{:x}", self.h.finalize()), self.bytes)
    }
}
impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.h.update(&buf[..n]);
        self.bytes += n as u64;
        Ok(n)
    }
}

/// Streaming decoder: holds at most one line and one case at a time.
pub struct CaseReader<R: Read> {
    r: BufReader<HashingReader<R>>,
    buf: String,
    line: usize,
    cases: usize,
    done: bool,
}
impl<R: Read> CaseReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            r: BufReader::with_capacity(1 << 16, HashingReader::new(inner)),
            buf: String::new(),
            line: 0,
            cases: 0,
            done: false,
        }
    }
    /// Raw SHA-256 and byte count of the whole source. Reads any remaining
    /// bytes first so the hash always covers the complete file.
    pub fn finish(mut self) -> Result<(String, u64), InputError> {
        std::io::copy(&mut self.r, &mut std::io::sink())
            .map_err(|e| InputError::Io(e.to_string()))?;
        Ok(self.r.into_inner().finish())
    }
}
impl<R: Read> Iterator for CaseReader<R> {
    type Item = Result<MarketCase, InputError>;
    fn next(&mut self) -> Option<Self::Item> {
        while !self.done {
            self.buf.clear();
            let n = match (&mut self.r)
                .take(MAX_LINE_BYTES as u64 + 1)
                .read_line(&mut self.buf)
            {
                Ok(n) => n,
                Err(e) => {
                    self.done = true;
                    return Some(Err(InputError::Io(e.to_string())));
                }
            };
            if n == 0 {
                self.done = true;
                return None;
            }
            self.line += 1;
            if n > MAX_LINE_BYTES {
                self.done = true;
                return Some(Err(InputError::LineTooLong(self.line)));
            }
            if self.buf.trim().is_empty() {
                continue;
            }
            if self.cases >= MAX_CASES {
                self.done = true;
                return Some(Err(InputError::TooManyCases));
            }
            self.cases += 1;
            let r = decode(&self.buf, self.line);
            if r.is_err() {
                self.done = true;
            }
            return Some(r);
        }
        None
    }
}

/// Convenience for small inputs and tests (materializes everything).
pub fn read_cases(reader: impl Read) -> Result<Vec<MarketCase>, InputError> {
    CaseReader::new(reader).collect()
}

/// Canonical compact JSONL line for one case (no trailing newline).
pub fn to_jsonl_line(c: &MarketCase) -> String {
    let lv = |l: &[Level]| {
        l.iter()
            .map(|x| (x.price.ticks(), x.quantity))
            .collect::<Vec<_>>()
    };
    let raw = RawCase {
        market: c.market.clone(),
        event: c.event.clone(),
        cluster: c.cluster.clone(),
        asset: c.asset.clone(),
        open_ns: c.open_ns,
        expiration_ns: c.expiration_ns,
        strike_cents: c.strike_cents,
        rules_sha256: c.rules_sha256.clone(),
        source_sha256: c.source_sha256.clone(),
        winner: c
            .winner
            .map(|w| if w == Side::Yes { "yes" } else { "no" }.to_owned()),
        provenance: Some(
            match c.provenance {
                Provenance::Synthetic => "synthetic",
                Provenance::OperatorNormalized => "operator_normalized",
            }
            .into(),
        ),
        snapshots: c
            .snapshots
            .iter()
            .map(|s| RawSnapshot {
                received_ns: s.received_ns,
                exchange_ns: s.book.exchange_ns,
                sequence: s.book.sequence,
                session_id: s.book.session_id.clone(),
                yes_bids: lv(&s.book.yes_bids),
                no_bids: lv(&s.book.no_bids),
                reference_cents: s.reference_cents,
                peer_reference_cents: s.peer_reference_cents,
                perp_basis_bps: s.perp_basis_bps,
            })
            .collect(),
    };
    serde_json::to_string(&raw).expect("serializable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::synthetic::{generate, Scenario};
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
        let prov =
            line("\"yes\"", 20).replacen("\"winner\"", "\"provenance\":\"official\",\"winner\"", 1);
        assert!(matches!(
            read_cases(prov.as_bytes()),
            Err(InputError::Invalid { .. })
        ));
    }
    #[test]
    fn jsonl_roundtrip_is_exact_and_keeps_synthetic_label() {
        let cases = generate(&Scenario::peer_lead(2, 1, 5));
        let text: String = cases.iter().map(|c| to_jsonl_line(c) + "\n").collect();
        let mut r = CaseReader::new(text.as_bytes());
        let back: Vec<_> = r.by_ref().collect::<Result<_, _>>().unwrap();
        let (sha, n) = r.finish().unwrap();
        assert_eq!(n, text.len() as u64);
        assert_eq!(sha, format!("{:x}", Sha256::digest(text.as_bytes())));
        assert_eq!(back.len(), cases.len());
        for (a, b) in back.iter().zip(&cases) {
            assert_eq!(a.snapshots, b.snapshots);
            assert_eq!(a.provenance, Provenance::Synthetic);
            assert_eq!((a.winner, a.strike_cents), (b.winner, b.strike_cents));
        }
    }
}

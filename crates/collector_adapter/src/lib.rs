//! Read-only adapter for the native collector's per-market book feed (NDJSON).
//!
//! Rule (owner-approved, validated against the collector's own `OrderBook` on a full
//! market): replay ONE market from a WebSocket snapshot plus its deltas in arrival
//! order. REST snapshots are a separate lineage and are never in this feed. Sequence
//! continuity is per market here, not per subscription (the journal keeps only the
//! six research families). Every step is compared with the collector's recorded
//! `executable_quotes`; nothing is "assumed equal".
//!
//! Units: prices are 1/10_000 dollar, quantities 1/100 contract. A value with more
//! precision than that is rejected, never rounded.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const PRICE_SCALE: u32 = 4;
const QTY_SCALE: u32 = 2;
const ONE: i64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    BadNumber(String),
    TooPrecise(String),
    UnknownSide(String),
    MissingField(&'static str),
    NegativeQuantity,
}

/// Parse a plain decimal string into an integer with `scale` fractional digits.
pub fn parse_fixed(s: &str, scale: u32) -> Result<i64, Error> {
    let t = s.trim();
    let (neg, body) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t),
    };
    let (int_part, frac_part) = body.split_once('.').unwrap_or((body, ""));
    if int_part.is_empty() && frac_part.is_empty() || !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(Error::BadNumber(s.to_string()));
    }
    let scale = scale as usize;
    if frac_part.len() > scale && frac_part[scale..].chars().any(|c| c != '0') {
        return Err(Error::TooPrecise(s.to_string()));
    }
    let mut frac = frac_part.chars().take(scale).collect::<String>();
    while frac.len() < scale {
        frac.push('0');
    }
    let digits = format!("{}{}", if int_part.is_empty() { "0" } else { int_part }, frac);
    let v: i64 = digits.parse().map_err(|_| Error::BadNumber(s.to_string()))?;
    Ok(if neg { -v } else { v })
}

#[derive(Debug, Deserialize)]
pub struct Oracle {
    pub yes_bid: Option<String>,
    pub no_bid: Option<String>,
    pub yes_ask: Option<String>,
    pub no_ask: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Line {
    pub t: String,
    pub seq: Option<u64>,
    pub sid: Option<u64>,
    pub ticker: String,
    pub side: Option<String>,
    pub price: Option<String>,
    pub delta: Option<String>,
    pub yes: Option<Vec<(String, String)>>,
    pub no: Option<Vec<(String, String)>>,
    pub oracle: Oracle,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Book {
    yes: BTreeMap<i64, i64>,
    no: BTreeMap<i64, i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Quotes {
    pub yes_bid: Option<i64>,
    pub no_bid: Option<i64>,
    pub yes_ask: Option<i64>,
    pub no_ask: Option<i64>,
}

impl Book {
    fn levels(raw: &[(String, String)]) -> Result<BTreeMap<i64, i64>, Error> {
        let mut out = BTreeMap::new();
        for (p, q) in raw {
            let (price, qty) = (parse_fixed(p, PRICE_SCALE)?, parse_fixed(q, QTY_SCALE)?);
            if price > 0 && qty > 0 {
                out.insert(price, qty);
            }
        }
        Ok(out)
    }
    pub fn snapshot(&mut self, yes: &[(String, String)], no: &[(String, String)]) -> Result<(), Error> {
        let (y, n) = (Self::levels(yes)?, Self::levels(no)?);
        self.yes = y;
        self.no = n;
        Ok(())
    }
    pub fn delta(&mut self, side: &str, price: &str, delta: &str) -> Result<(), Error> {
        let levels = match side.to_ascii_lowercase().as_str() {
            "yes" => &mut self.yes,
            "no" => &mut self.no,
            other => return Err(Error::UnknownSide(other.to_string())),
        };
        let (p, d) = (parse_fixed(price, PRICE_SCALE)?, parse_fixed(delta, QTY_SCALE)?);
        let size = levels.get(&p).copied().unwrap_or(0)
            .checked_add(d)
            .ok_or_else(|| Error::BadNumber("quantity sum overflow".to_string()))?;
        if size < 0 {
            return Err(Error::NegativeQuantity);
        }
        if size == 0 {
            levels.remove(&p);
        } else {
            levels.insert(p, size);
        }
        Ok(())
    }
    /// Asks are derived from the complementary bids, exactly as the collector does.
    pub fn quotes(&self) -> Quotes {
        let yes_bid = self.yes.keys().next_back().copied();
        let no_bid = self.no.keys().next_back().copied();
        Quotes {
            yes_bid,
            no_bid,
            yes_ask: no_bid.map(|n| ONE - n),
            no_ask: yes_bid.map(|y| ONE - y),
        }
    }
}

fn oracle_quotes(o: &Oracle) -> Result<Quotes, Error> {
    let f = |v: &Option<String>| v.as_deref().map(|s| parse_fixed(s, PRICE_SCALE)).transpose();
    Ok(Quotes { yes_bid: f(&o.yes_bid)?, no_bid: f(&o.no_bid)?, yes_ask: f(&o.yes_ask)?, no_ask: f(&o.no_ask)? })
}

#[derive(Debug, Default, Serialize, PartialEq, Eq)]
pub struct Report {
    pub lines: u64,
    pub snapshots: u64,
    pub deltas: u64,
    pub quote_matches: u64,
    pub quote_mismatches: u64,
    pub negative_quantity_errors: u64,
    pub other_errors: u64,
    pub out_of_order_seq: u64,
    pub first_mismatch_line: Option<u64>,
}

impl Report {
    pub fn parity_ok(&self) -> bool {
        self.lines > 0 && self.snapshots > 0 && self.quote_mismatches == 0
            && self.negative_quantity_errors == 0 && self.other_errors == 0
            && self.out_of_order_seq == 0
    }
}

/// Replay one market's feed. Every delta must follow a verified snapshot
/// with an exact next sequence. After any gap or invalid book mutation the
/// lineage is unusable until a fresh snapshot. A bad line is counted, never skipped.
pub fn replay<I: IntoIterator<Item = String>>(lines: I) -> Report {
    let mut book = Book::default();
    let mut report = Report::default();
    let mut last_seq: Option<u64> = None;
    let mut has_snapshot = false;
    let mut last_sid: Option<u64> = None;
    let mut market: Option<String> = None;
    for raw in lines {
        report.lines += 1;
        let line: Line = match serde_json::from_str(&raw) {
            Ok(l) => l,
            Err(_) => {
                report.other_errors += 1;
                has_snapshot = false;
                last_seq = None;
                continue;
            }
        };
        if line.ticker.trim().is_empty() {
            report.other_errors += 1;
            has_snapshot = false;
            last_seq = None;
            continue;
        }
        match &market {
            Some(expected) if expected != &line.ticker => {
                report.other_errors += 1; // a one-market feed changed identity
                has_snapshot = false;
                last_seq = None;
                continue;
            }
            None => market = Some(line.ticker.clone()),
            _ => {}
        }

        match line.t.as_str() {
            "snap" => {
                // An authentic WebSocket snapshot has a sid/seq. Without a seq
                // the next delta cannot be certified as contiguous.
                has_snapshot = false;
                last_seq = None;
                if line.seq.is_none() || line.sid.is_none() {
                    report.other_errors += 1;
                    continue;
                }
            }
            "delta" => {
                if !has_snapshot {
                    report.other_errors += 1; // no synchronized book lineage
                    continue;
                }
                if last_sid != line.sid || last_seq.and_then(|s| s.checked_add(1)) != line.seq {
                    report.out_of_order_seq += 1; // duplicates, gaps, missing seq, overflow
                    has_snapshot = false;
                    last_seq = None;
                    continue;
                }
            }
            _ => {
                report.other_errors += 1;
                has_snapshot = false;
                last_seq = None;
                continue;
            }
        }

        let applied = match line.t.as_str() {
            "snap" => {
                report.snapshots += 1;
                book.snapshot(line.yes.as_deref().unwrap_or(&[]), line.no.as_deref().unwrap_or(&[]))
            }
            "delta" => {
                report.deltas += 1;
                match (&line.side, &line.price, &line.delta) {
                    (Some(s), Some(p), Some(d)) => book.delta(s, p, d),
                    _ => Err(Error::MissingField("side/price/delta")),
                }
            }
            _ => unreachable!("event type checked above"),
        };
        match applied {
            Err(Error::NegativeQuantity) => report.negative_quantity_errors += 1,
            Err(_) => report.other_errors += 1,
            Ok(()) => {
                has_snapshot = true;
                last_seq = line.seq;
                last_sid = line.sid;
                match oracle_quotes(&line.oracle) {
                    Ok(expected) if expected == book.quotes() => report.quote_matches += 1,
                    Ok(_) => {
                        report.quote_mismatches += 1;
                        report.first_mismatch_line.get_or_insert(report.lines);
                    }
                    Err(_) => report.other_errors += 1,
                }
                continue;
            }
        }
        has_snapshot = false;
        last_seq = None;
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(t: &str, seq: u64, extra: &str, yb: &str, nb: &str, ya: &str, na: &str) -> String {
        format!(r#"{{"t":"{t}","seq":{seq},"sid":2,"ticker":"M",{extra}"oracle":{{"yes_bid":{yb},"no_bid":{nb},"yes_ask":{ya},"no_ask":{na}}}}}"#)
    }

    #[test]
    fn parse_fixed_is_exact_and_rejects_precision_loss() {
        assert_eq!(parse_fixed("0.7400", 4), Ok(7400));
        assert_eq!(parse_fixed("0.001", 4), Ok(10));
        assert_eq!(parse_fixed("-3.00", 2), Ok(-300));
        assert_eq!(parse_fixed("2418074.71", 2), Ok(241_807_471));
        assert_eq!(parse_fixed("1.2300", 2), Ok(123)); // trailing zeros beyond scale are fine
        assert!(matches!(parse_fixed("1.234", 2), Err(Error::TooPrecise(_))));
        assert!(matches!(parse_fixed("abc", 2), Err(Error::BadNumber(_))));
    }

    #[test]
    fn replays_snapshot_then_deltas_and_matches_oracle() {
        let snap = line("snap", 1, r#""yes":[["0.6000","10.00"]],"no":[["0.3800","5.00"]],"#,
            r#""0.6000""#, r#""0.3800""#, r#""0.6200""#, r#""0.4000""#);
        // a new better yes bid at 0.61, then it is fully removed again
        let add = line("delta", 2, r#""side":"yes","price":"0.6100","delta":"2.00","#,
            r#""0.6100""#, r#""0.3800""#, r#""0.6200""#, r#""0.3900""#);
        let rem = line("delta", 3, r#""side":"yes","price":"0.6100","delta":"-2.00","#,
            r#""0.6000""#, r#""0.3800""#, r#""0.6200""#, r#""0.4000""#);
        let r = replay([snap, add, rem]);
        assert_eq!((r.snapshots, r.deltas, r.quote_matches, r.quote_mismatches), (1, 2, 3, 0));
        assert!(r.parity_ok());
    }

    #[test]
    fn empty_book_has_no_quotes_and_matches_a_null_oracle() {
        let r = replay([line("snap", 1, r#""yes":[],"no":[],"#, "null", "null", "null", "null")]);
        assert_eq!(r.quote_matches, 1);
    }

    #[test]
    fn mismatch_negative_quantity_and_bad_lines_are_counted_never_skipped() {
        let snap = line("snap", 1, r#""yes":[["0.6000","1.00"]],"no":[],"#,
            r#""0.6000""#, "null", "null", r#""0.4000""#);
        let wrong = line("delta", 2, r#""side":"yes","price":"0.6000","delta":"1.00","#,
            r#""0.5000""#, "null", "null", r#""0.5000""#); // oracle disagrees
        let neg = line("delta", 3, r#""side":"no","price":"0.3000","delta":"-1.00","#, "null", "null", "null", "null");
        let r = replay([snap, wrong, neg, "not json".to_string()]);
        assert_eq!(r.quote_mismatches, 1);
        assert_eq!(r.first_mismatch_line, Some(2));
        assert_eq!(r.negative_quantity_errors, 1);
        assert_eq!(r.other_errors, 1);
        assert!(!r.parity_ok());
    }

    #[test]
    fn delta_quantity_overflow_is_an_error_not_a_crash_or_wrapped_book() {
        let mut book = Book::default();
        // 2 fractional digits: this quantity reaches i64::MAX exactly.
        assert_eq!(book.delta("yes", "0.5000", "92233720368547758.07"), Ok(()));
        assert!(matches!(
            book.delta("yes", "0.5000", "0.01"),
            Err(Error::BadNumber(_))
        ));
    }

    #[test]
    fn missing_initial_snapshot_never_certifies_parity() {
        let orphan = line("delta", 2, r#""side":"yes","price":"0.5000","delta":"1.00","#,
            r#""0.5000""#, "null", "null", r#""0.5000""#);
        let report = replay([orphan]);
        assert_eq!(report.other_errors, 1);
        assert!(!report.parity_ok());
    }

    #[test]
    fn missing_seq_and_switched_market_fail_closed() {
        let snap = line("snap", 1, r#""yes":[],"no":[],"#, "null", "null", "null", "null");
        let absent_seq = line("delta", 2, r#""side":"yes","price":"0.5000","delta":"1.00","#,
            r#""0.5000""#, "null", "null", r#""0.5000""#).replace(r#""seq":2"#, r#""seq":null"#);
        let report = replay([snap.clone(), absent_seq]);
        assert_eq!(report.out_of_order_seq, 1);
        assert!(!report.parity_ok());

        let changed = snap.replace(r#""ticker":"M""#, r#""ticker":"DIFFERENT""#);
        let report = replay([snap, changed]);
        assert_eq!(report.other_errors, 1);
        assert!(!report.parity_ok());
    }

    #[test]
    fn delta_with_new_subscription_id_requires_a_fresh_snapshot() {
        let snap = line("snap", 1, r#""yes":[],"no":[],"#, "null", "null", "null", "null");
        // A new connection may reuse numeric sequence values. SID must still match.
        let wrong_sid = line("delta", 2, r#""side":"yes","price":"0.5000","delta":"1.00","#,
            r#""0.5000""#, "null", "null", r#""0.5000""#)
            .replace(r#""sid":2"#, r#""sid":3"#);
        let report = replay([snap, wrong_sid]);
        assert_eq!(report.out_of_order_seq, 1);
        assert!(!report.parity_ok());
    }

    #[test]
    fn out_of_order_sequence_is_reported() {
        let mk = |seq: u64| format!(
            r#"{{"t":"delta","seq":{seq},"sid":2,"ticker":"M","side":"yes","price":"0.5000","delta":"1.00","oracle":{{"yes_bid":"0.5000","no_bid":null,"yes_ask":null,"no_ask":"0.5000"}}}}"#);
        let snap = line("snap", 1, r#""yes":[],"no":[],"#, "null", "null", "null", "null");
        let r = replay([snap, mk(5), mk(4)]);
        assert_eq!(r.out_of_order_seq, 1);
        assert!(!r.parity_ok());
    }
}

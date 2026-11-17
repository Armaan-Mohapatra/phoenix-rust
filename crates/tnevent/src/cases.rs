//! Pass 2: build campaign `MarketCase`s from verified books.
//!
//! * Only markets the manifest classed as eligible are built, and a sample exists only while the
//!   book is verified (snapshot-initialized, contiguous, uncrossed).
//! * One sample per 500 ms bucket (the LAST verified state in the bucket), timestamped with the
//!   true local receipt time of the event that produced that state. Nothing is stamped later
//!   than it was known, and a quiet book keeps its real age, so the engine's quote-age refusal
//!   still sees staleness. Intermediate states inside a bucket are dropped (conservative).
//! * `reference_cents` is 0 ("not available"): no spot-reference series exists for this capture,
//!   and every reference-based family abstains on it rather than guessing.
//! * Winner = the venue's own `determined` lifecycle result. It is venue-reported, NOT verified
//!   against the underlying index; the manifest records that source.
use crate::book::{BookState, Outcome, Verifier};
use crate::container::{Limits, Segment, TnError};
use crate::corpus::{research_asset, Eligibility, MarketRecord, GRACE_NS};
use crate::event::{EventRef, Meta};
use phoenix_campaign::input::to_jsonl_line;
use phoenix_campaign::{MarketCase, Provenance, Snapshot};
use phoenix_domain::Side;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone, Copy)]
pub struct BuildConfig {
    pub sample_ns: i64,
    pub depth: usize,
    pub include_partial: bool,
}
impl Default for BuildConfig {
    fn default() -> Self {
        Self {
            sample_ns: 500_000_000,
            depth: 10,
            include_partial: false,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BuildReport {
    pub cases_written: u64,
    pub snapshots_written: u64,
    pub skipped_truncated_by_corpus_end: u64,
    pub skipped_invalid: Vec<String>,
    pub skipped_no_samples: u64,
    pub bytes_written: u64,
    pub output_sha256: String,
}

struct Build {
    snaps: Vec<Snapshot>,
    last_emit_recv: i64,
    segs: BTreeSet<u64>,
}

pub struct CaseBuilder<'a, W: Write> {
    cfg: BuildConfig,
    info: HashMap<String, &'a MarketRecord>,
    seg_sha: BTreeMap<u64, String>,
    verifier: Verifier,
    state: HashMap<String, Build>,
    done: HashSet<String>,
    ready: BTreeMap<(i64, String), MarketCase>,
    out: W,
    hasher: Sha256,
    pub report: BuildReport,
    stream_last_recv: i64,
    next_scan_ns: i64,
}

fn included(r: &MarketRecord, cfg: &BuildConfig) -> bool {
    match r.eligibility {
        Some(Eligibility::ExecutableVerified) => true,
        Some(Eligibility::PartialVerified) => cfg.include_partial,
        _ => false,
    }
}

/// `2026-10-30T13:00Z` (cluster label: all markets expiring in the same minute share it).
pub fn utc_minute(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60
    )
}

impl<'a, W: Write> CaseBuilder<'a, W> {
    pub fn new(
        cfg: BuildConfig,
        records: &'a [MarketRecord],
        seg_sha: BTreeMap<u64, String>,
        out: W,
    ) -> Self {
        let info = records
            .iter()
            .filter(|r| included(r, &cfg))
            .map(|r| (r.ticker.clone(), r))
            .collect();
        Self {
            cfg,
            info,
            seg_sha,
            verifier: Verifier::new(),
            state: HashMap::new(),
            done: HashSet::new(),
            ready: BTreeMap::new(),
            out,
            hasher: Sha256::new(),
            report: BuildReport::default(),
            stream_last_recv: 0,
            next_scan_ns: 0,
        }
    }

    pub fn process_segment(&mut self, id: u64, dir: &Path) -> Result<(), TnError> {
        let seg = Segment::open(dir, Limits::default())?;
        seg.visit(&mut |ev| self.on_event(id, ev))?;
        Ok(())
    }

    fn emit_sample(&mut self, ticker: &str) {
        let Some(b) = self.verifier.book(ticker) else {
            return;
        };
        let (Some(info), Some(st)) = (self.info.get(ticker), self.state.get_mut(ticker)) else {
            return;
        };
        let recv = b.last_recv_ns;
        if recv <= st.last_emit_recv || b.stream.is_none() {
            return;
        }
        // Only a verified-or-intact ladder is ever emitted; ladders are never left holding rejected updates.
        let (Some(open), Some(close)) = (info.open_ns, info.close_ns) else {
            return;
        };
        if recv < open || recv > close {
            st.last_emit_recv = recv;
            return;
        }
        let (run, gen) = b.stream.clone().unwrap();
        let session = format!("{run}:g{gen}");
        if let Some(book) = b.to_book(ticker, recv, recv, &session, self.cfg.depth) {
            st.snaps.push(Snapshot {
                received_ns: recv,
                book,
                reference_cents: 0,
                peer_reference_cents: None,
                perp_basis_bps: None,
            });
            st.last_emit_recv = recv;
        }
    }

    fn on_event(&mut self, seg: u64, ev: EventRef<'_>) -> Result<(), TnError> {
        let meta = Meta::parse(ev.meta, ev.index)?;
        if meta.venue() != "kalshi" || meta.is_rest() {
            return Ok(());
        }
        let Ok(recv) = meta.received_ns(ev.index) else {
            return Ok(());
        };
        self.stream_last_recv = self.stream_last_recv.max(recv);
        let kind = meta.kind();
        if kind != "orderbook_snapshot" && kind != "orderbook_delta" {
            self.verifier.observe_other(&meta);
            self.flush_transitions();
        }
        if kind == "orderbook_snapshot" || kind == "orderbook_delta" {
            let ticker = meta.ticker();
            if !self.info.contains_key(ticker) || self.done.contains(ticker) {
                self.verifier.observe_sequence(&meta);
                self.flush_transitions();
            } else {
                let bucket = recv.div_euclid(self.cfg.sample_ns);
                let st = self
                    .state
                    .entry(ticker.to_owned())
                    .or_insert_with(|| Build {
                        snaps: vec![],
                        last_emit_recv: i64::MIN,
                        segs: BTreeSet::new(),
                    });
                st.segs.insert(seg);
                // Flush the previous bucket's final state BEFORE this event changes the book.
                if let Some(b) = self.verifier.book(ticker) {
                    if b.last_recv_ns > st.last_emit_recv
                        && b.last_recv_ns.div_euclid(self.cfg.sample_ns) < bucket
                    {
                        self.emit_sample(ticker);
                    }
                }
                let out = self.verifier.on_orderbook(&meta, ev.msg, recv);
                for (t, _) in self.verifier.take_transitions() {
                    // A break caused by another market's event leaves this ladder intact: flush it.
                    // For the event's own market, a `Broke` outcome means the event itself was rejected,
                    // so nothing newer than the last flush is trustworthy.
                    if t != ticker || !matches!(out, Outcome::Broke(_)) {
                        self.emit_sample(&t);
                    }
                }
            }
        }
        if recv >= self.next_scan_ns {
            self.next_scan_ns = recv + 5_000_000_000;
            self.finalize_due(recv)?;
        }
        Ok(())
    }

    /// A break caused by a message that is not for a market leaves that market's ladder intact: flush it.
    fn flush_transitions(&mut self) {
        for (t, _) in self.verifier.take_transitions() {
            self.emit_sample(&t);
        }
    }

    fn finalize_due(&mut self, now: i64) -> Result<(), TnError> {
        let due: Vec<String> = self
            .state
            .keys()
            .filter(|t| {
                self.info
                    .get(*t)
                    .and_then(|i| i.close_ns)
                    .is_some_and(|c| c + GRACE_NS < now)
            })
            .cloned()
            .collect();
        for t in due {
            self.finalize(&t);
        }
        // Emit finished cases in (expiration, ticker) order once every market of that expiration is complete.
        let limit = now - 3 * GRACE_NS;
        let keys: Vec<(i64, String)> = self
            .ready
            .keys()
            .take_while(|k| k.0 < limit)
            .cloned()
            .collect();
        for k in keys {
            if let Some(c) = self.ready.remove(&k) {
                self.write_case(c)?;
            }
        }
        Ok(())
    }

    fn finalize(&mut self, ticker: &str) {
        self.emit_sample(ticker);
        let Some(st) = self.state.remove(ticker) else {
            return;
        };
        self.done.insert(ticker.to_owned());
        let still = self
            .verifier
            .book(ticker)
            .map(|b| b.state == BookState::Verified)
            .unwrap_or(false);
        let _ = still;
        self.verifier.forget(ticker);
        let info = self.info[ticker];
        let (Some(open), Some(close), Some(strike)) = (info.open_ns, info.close_ns, info.strike)
        else {
            return;
        };
        if st.snaps.is_empty() {
            self.report.skipped_no_samples += 1;
            return;
        }
        let series = ticker.split('-').next().unwrap_or("");
        let event = ticker
            .rsplit_once('-')
            .map_or(ticker, |(e, _)| e)
            .to_owned();
        let winner = match info.result.as_deref() {
            Some("yes") => Some(Side::Yes),
            Some("no") => Some(Side::No),
            _ => None,
        };
        let rules = info.rules_text_sha256.clone().unwrap_or_else(|| {
            format!(
                "{:x}",
                Sha256::digest(format!("tnevent:rules-text-unavailable:{series}"))
            )
        });
        let mut h = Sha256::new();
        h.update(b"tnevent-source-v1");
        for s in &st.segs {
            h.update(s.to_be_bytes());
            h.update(
                self.seg_sha
                    .get(s)
                    .map(String::as_str)
                    .unwrap_or("")
                    .as_bytes(),
            );
        }
        let case = MarketCase {
            market: ticker.to_owned(),
            event,
            cluster: utc_minute(close),
            asset: research_asset(ticker).unwrap_or("").to_owned(),
            open_ns: open,
            expiration_ns: close,
            strike_cents: ((strike * 100.0).round() as i64).max(1),
            rules_sha256: rules,
            source_sha256: format!("{:x}", h.finalize()),
            provenance: Provenance::OperatorNormalized,
            snapshots: st.snaps,
            winner,
        };
        match case.validate() {
            Ok(()) => {
                self.ready.insert((close, ticker.to_owned()), case);
            }
            Err(e) => self.report.skipped_invalid.push(format!("{ticker}: {e:?}")),
        }
    }

    fn write_case(&mut self, c: MarketCase) -> Result<(), TnError> {
        let line = to_jsonl_line(&c) + "\n";
        self.out.write_all(line.as_bytes())?;
        self.hasher.update(line.as_bytes());
        self.report.cases_written += 1;
        self.report.snapshots_written += c.snapshots.len() as u64;
        self.report.bytes_written += line.len() as u64;
        Ok(())
    }

    /// Finalizes everything that closed within the corpus; markets still open at the end are
    /// incomplete and are counted, never emitted.
    pub fn finish(mut self) -> Result<BuildReport, TnError> {
        let tickers: Vec<String> = self.state.keys().cloned().collect();
        for t in tickers {
            let closed = self
                .info
                .get(&t)
                .and_then(|i| i.close_ns)
                .is_some_and(|c| c <= self.stream_last_recv);
            if closed {
                self.finalize(&t);
            } else {
                self.state.remove(&t);
                self.report.skipped_truncated_by_corpus_end += 1;
            }
        }
        let keys: Vec<(i64, String)> = self.ready.keys().cloned().collect();
        for k in keys {
            if let Some(c) = self.ready.remove(&k) {
                self.write_case(c)?;
            }
        }
        self.out.flush()?;
        self.report.output_sha256 = format!("{:x}", self.hasher.finalize());
        Ok(self.report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_label_matches_known_instants() {
        assert_eq!(utc_minute(1_793_365_200_000_000_000), "2026-10-30T13:00Z");
        assert_eq!(utc_minute(0), "1970-01-01T00:00Z");
        assert_eq!(
            utc_minute(951_782_400_000_000_000 + 60_000_000_000 * 61),
            "2000-02-29T01:01Z"
        );
    }
}

//! Corpus-level pass: one streaming sweep over finalized segments that produces the canonical
//! market manifest (coverage, verification, settlement evidence, eligibility class).
use crate::book::{BookState, Break, Outcome, Verifier};
use crate::container::{Limits, Segment, TnError};
use crate::event::{decode_lifecycle, parse_utc_ns, EventRef, Meta, RestMarkets};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

pub const GRACE_NS: i64 = 30_000_000_000;

/// Crypto 15-minute binary series the research harness supports.
pub fn research_asset(ticker: &str) -> Option<&'static str> {
    let series = ticker.split('-').next()?;
    Some(match series {
        "KXBTC15M" => "BTC",
        "KXETH15M" => "ETH",
        "KXSOL15M" => "SOL",
        "KXXRP15M" => "XRP",
        "KXDOGE15M" => "DOGE",
        "KXBNB15M" => "BNB",
        _ => return None,
    })
}

/// `segment-00000200.ready` directories under `root`, ascending by id. Junctions and symlinks are skipped.
pub fn list_segments(root: &Path) -> Result<Vec<(u64, PathBuf)>, TnError> {
    let mut v = vec![];
    for e in std::fs::read_dir(root)? {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        let Some(id) = name
            .strip_prefix("segment-")
            .and_then(|r| r.strip_suffix(".ready"))
            .and_then(|d| d.parse::<u64>().ok())
        else {
            continue;
        };
        if e.file_type()?.is_dir() && !e.file_type()?.is_symlink() {
            v.push((id, e.path()));
        }
    }
    v.sort();
    Ok(v)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Eligibility {
    /// Snapshot-initialized within 30 s of open, no break to close, venue result, strike, window.
    ExecutableVerified,
    /// Some verified coverage but late initialization and/or breaks. Samples that exist are individually verified.
    PartialVerified,
    /// Order-book updates observed but no snapshot ever initialized the book.
    NoInitialization,
    NoWindow,
    NoStrike,
    NoSettlement,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketRecord {
    pub ticker: String,
    pub asset: String,
    pub open_ns: Option<i64>,
    pub close_ns: Option<i64>,
    pub strike: Option<f64>,
    pub result: Option<String>,
    pub result_source: Option<String>,
    pub rules_text_sha256: Option<String>,
    pub first_recv_ns: Option<i64>,
    pub last_recv_ns: Option<i64>,
    pub snapshots: u64,
    pub deltas_applied: u64,
    pub deltas_before_init: u64,
    pub deltas_while_broken: u64,
    pub trades: u64,
    pub first_snapshot_recv_ns: Option<i64>,
    pub verified_ns: i64,
    pub window_ns: Option<i64>,
    pub coverage: Option<f64>,
    pub break_count: u64,
    pub breaks: Vec<String>,
    pub first_segment: Option<u64>,
    pub last_segment: Option<u64>,
    pub segments: Vec<u64>,
    pub eligibility: Option<Eligibility>,
}

#[derive(Default)]
struct Acc {
    rec: Option<MarketRecord>,
    verified_since: Option<i64>,
    seg_set: BTreeSet<u64>,
}

#[derive(Debug, Default, Serialize)]
pub struct SegmentRecord {
    pub id: u64,
    pub format_version: u16,
    pub manifest_events: u64,
    pub events: u64,
    pub sha256: String,
    pub sha256_verified: bool,
    pub first_recv_ns: Option<i64>,
    pub last_recv_ns: Option<i64>,
    pub orderbook_snapshots: u64,
    pub orderbook_deltas: u64,
    pub trades: u64,
    pub lifecycle: u64,
    pub rest: u64,
    pub other: u64,
    pub non_kalshi: u64,
}

pub struct ManifestBuilder {
    pub verifier: Verifier,
    acc: HashMap<String, Acc>,
    pub segments: Vec<SegmentRecord>,
    seg_sha: BTreeMap<u64, String>,
    prev_ob_recv: i64,
    pub stream_last_recv: i64,
    pub verify_hash: bool,
}

impl ManifestBuilder {
    pub fn new(verify_hash: bool) -> Self {
        Self {
            verifier: Verifier::new(),
            acc: HashMap::new(),
            segments: vec![],
            seg_sha: BTreeMap::new(),
            prev_ob_recv: 0,
            stream_last_recv: 0,
            verify_hash,
        }
    }
    pub fn segment_sha(&self) -> &BTreeMap<u64, String> {
        &self.seg_sha
    }

    fn entry(&mut self, ticker: &str) -> &mut Acc {
        self.acc.entry(ticker.to_owned()).or_insert_with(|| Acc {
            rec: Some(MarketRecord {
                ticker: ticker.to_owned(),
                asset: research_asset(ticker).unwrap_or("").to_owned(),
                open_ns: None,
                close_ns: None,
                strike: None,
                result: None,
                result_source: None,
                rules_text_sha256: None,
                first_recv_ns: None,
                last_recv_ns: None,
                snapshots: 0,
                deltas_applied: 0,
                deltas_before_init: 0,
                deltas_while_broken: 0,
                trades: 0,
                first_snapshot_recv_ns: None,
                verified_ns: 0,
                window_ns: None,
                coverage: None,
                break_count: 0,
                breaks: vec![],
                first_segment: None,
                last_segment: None,
                segments: vec![],
                eligibility: None,
            }),
            ..Default::default()
        })
    }

    pub fn process_segment(&mut self, id: u64, dir: &Path) -> Result<(), TnError> {
        let seg = Segment::open(dir, Limits::default())?;
        let mut sr = SegmentRecord {
            id,
            format_version: seg.manifest.segment_format_version,
            manifest_events: seg.manifest.event_count,
            sha256: seg.manifest.sha256.clone(),
            ..Default::default()
        };
        if self.verify_hash {
            seg.verify_hash()?;
            sr.sha256_verified = true;
        }
        self.seg_sha.insert(id, seg.manifest.sha256.clone());
        let stats = seg.visit(&mut |ev| self.on_event(id, ev, &mut sr))?;
        sr.events = stats.events;
        self.segments.push(sr);
        Ok(())
    }

    fn on_event(
        &mut self,
        seg: u64,
        ev: EventRef<'_>,
        sr: &mut SegmentRecord,
    ) -> Result<(), TnError> {
        let meta = Meta::parse(ev.meta, ev.index)?;
        if meta.venue() != "kalshi" {
            sr.non_kalshi += 1;
            return Ok(());
        }
        let recv = match meta.received_ns(ev.index) {
            Ok(r) => r,
            Err(e) if meta.is_rest() => {
                let _ = e;
                0
            }
            Err(e) => return Err(e),
        };
        if recv > 0 {
            sr.first_recv_ns.get_or_insert(recv);
            sr.last_recv_ns = Some(recv);
            self.stream_last_recv = self.stream_last_recv.max(recv);
        }
        let ticker = meta.ticker();
        match meta.kind() {
            "orderbook_snapshot" | "orderbook_delta" => {
                if meta.kind() == "orderbook_snapshot" {
                    sr.orderbook_snapshots += 1;
                } else {
                    sr.orderbook_deltas += 1;
                }
                if research_asset(ticker).is_none() {
                    self.verifier.observe_sequence(&meta);
                    self.prev_ob_recv = recv;
                    return Ok(());
                }
                let out = self.verifier.on_orderbook(&meta, ev.msg, recv);
                let prev = self.prev_ob_recv;
                self.prev_ob_recv = recv;
                let a = self.entry(ticker);
                a.seg_set.insert(seg);
                let r = a.rec.as_mut().unwrap();
                r.first_recv_ns.get_or_insert(recv);
                r.last_recv_ns = Some(recv);
                match out {
                    Outcome::Initialized => {
                        r.snapshots += 1;
                        r.first_snapshot_recv_ns.get_or_insert(recv);
                        a.verified_since = Some(recv);
                    }
                    Outcome::Applied => r.deltas_applied += 1,
                    Outcome::IgnoredUnknown => r.deltas_before_init += 1,
                    Outcome::IgnoredBroken => r.deltas_while_broken += 1,
                    Outcome::Duplicate => {}
                    Outcome::Broke(_) => {}
                }
                for (t, why) in self.verifier.take_transitions() {
                    self.close_span(&t, prev.max(0), why);
                }
            }
            "trade" => {
                sr.trades += 1;
                if research_asset(ticker).is_some() {
                    let r = self.entry(ticker).rec.as_mut().unwrap();
                    r.trades += 1;
                }
            }
            "market_lifecycle_v2" | "event_lifecycle" => {
                sr.lifecycle += 1;
                if research_asset(ticker).is_some() && meta.kind() == "market_lifecycle_v2" {
                    if let Ok(w) = decode_lifecycle(ev.msg) {
                        let l = w.body;
                        let r = self.entry(ticker).rec.as_mut().unwrap();
                        match l.event_type {
                            "metadata_updated" => {
                                if let Some(k) = l.floor_strike {
                                    r.strike = Some(k);
                                }
                            }
                            "determined" => {
                                if let Some(res) = l.result {
                                    if r.result.is_none() {
                                        r.result = Some(res.to_owned());
                                        r.result_source = Some("lifecycle_determined".into());
                                    }
                                }
                            }
                            "created" => {
                                r.open_ns = r.open_ns.or(l.open_ts.map(|s| s * 1_000_000_000));
                                r.close_ns = r.close_ns.or(l.close_ts.map(|s| s * 1_000_000_000));
                            }
                            "close_date_updated" => {
                                if let Some(c) = l.close_ts {
                                    r.close_ns = Some(c * 1_000_000_000);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            "market_discovery" | "market_lifecycle_refresh" => {
                sr.rest += 1;
                if let Ok(m) = serde_json::from_slice::<RestMarkets<'_>>(ev.msg) {
                    for mk in m.markets {
                        if research_asset(mk.ticker).is_none() {
                            continue;
                        }
                        let r = self.entry(mk.ticker).rec.as_mut().unwrap();
                        if let Some(o) = mk.open_time.and_then(parse_utc_ns) {
                            r.open_ns = Some(o);
                        }
                        if let Some(c) = mk.close_time.and_then(parse_utc_ns) {
                            r.close_ns = Some(c);
                        }
                        if let Some(k) = mk.floor_strike {
                            r.strike = Some(k);
                        }
                        if let (None, Some(res)) = (&r.result, mk.result.filter(|s| !s.is_empty()))
                        {
                            r.result = Some(res.to_owned());
                            r.result_source = Some("rest_listing".into());
                        }
                        if r.rules_text_sha256.is_none() {
                            if let Some(p) = mk.rules_primary {
                                let mut h = Sha256::new();
                                h.update(p.as_bytes());
                                h.update([0]);
                                h.update(mk.rules_secondary.unwrap_or("").as_bytes());
                                r.rules_text_sha256 = Some(format!("{:x}", h.finalize()));
                            }
                        }
                    }
                }
            }
            _ => sr.other += 1,
        }
        Ok(())
    }

    /// Ends a market's verified span at `end_ns` (clamped into its window when known).
    fn close_span(&mut self, ticker: &str, end_ns: i64, why: Break) {
        let a = self.entry(ticker);
        let r = a.rec.as_mut().unwrap();
        if let Some(start) = a.verified_since.take() {
            let (s, e) = clamp(start, end_ns, r.open_ns, r.close_ns);
            r.verified_ns += (e - s).max(0);
        }
        r.break_count += 1;
        if r.breaks.len() < 8 {
            r.breaks.push(format!("{why:?}"));
        }
    }

    /// Final classification; call once after the last segment.
    pub fn finish(
        mut self,
    ) -> (
        Vec<MarketRecord>,
        Vec<SegmentRecord>,
        BTreeMap<u64, String>,
        crate::book::VerifierStats,
    ) {
        let end = self.stream_last_recv;
        let tickers: Vec<String> = self.acc.keys().cloned().collect();
        for t in &tickers {
            let still_verified = self
                .verifier
                .book(t)
                .map(|b| b.state == BookState::Verified)
                .unwrap_or(false);
            let a = self.acc.get_mut(t).unwrap();
            let r = a.rec.as_mut().unwrap();
            if let (Some(start), true) = (a.verified_since.take(), still_verified) {
                let stop = r.close_ns.map_or(end, |c| c.min(end));
                let (s, e) = clamp(start, stop, r.open_ns, r.close_ns);
                r.verified_ns += (e - s).max(0);
            }
            r.segments = a.seg_set.iter().copied().collect();
            r.first_segment = r.segments.first().copied();
            r.last_segment = r.segments.last().copied();
            if let (Some(o), Some(c)) = (r.open_ns, r.close_ns) {
                r.window_ns = Some(c - o);
                if c > o {
                    r.coverage = Some((r.verified_ns as f64 / (c - o) as f64).clamp(0.0, 1.0));
                }
            }
            r.eligibility = Some(classify(r));
        }
        let mut recs: Vec<MarketRecord> = self.acc.into_values().filter_map(|a| a.rec).collect();
        recs.sort_by(|a, b| (a.close_ns, &a.ticker).cmp(&(b.close_ns, &b.ticker)));
        (recs, self.segments, self.seg_sha, self.verifier.stats)
    }
}

fn clamp(start: i64, end: i64, open: Option<i64>, close: Option<i64>) -> (i64, i64) {
    let s = open.map_or(start, |o| start.max(o));
    let e = close.map_or(end, |c| end.min(c));
    (s, e.max(s))
}

pub fn classify(r: &MarketRecord) -> Eligibility {
    let (Some(open), Some(close)) = (r.open_ns, r.close_ns) else {
        return Eligibility::NoWindow;
    };
    if close <= open {
        return Eligibility::NoWindow;
    }
    if r.strike.is_none() {
        return Eligibility::NoStrike;
    }
    if !matches!(r.result.as_deref(), Some("yes") | Some("no")) {
        return Eligibility::NoSettlement;
    }
    let Some(first) = r.first_snapshot_recv_ns else {
        return Eligibility::NoInitialization;
    };
    if r.break_count == 0 && first <= open + GRACE_NS && r.coverage.unwrap_or(0.0) >= 0.95 {
        Eligibility::ExecutableVerified
    } else {
        Eligibility::PartialVerified
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rec() -> MarketRecord {
        MarketRecord {
            ticker: "KXBTC15M-X".into(),
            asset: "BTC".into(),
            open_ns: Some(0),
            close_ns: Some(900_000_000_000),
            strike: Some(1.0),
            result: Some("yes".into()),
            result_source: None,
            rules_text_sha256: None,
            first_recv_ns: None,
            last_recv_ns: None,
            snapshots: 1,
            deltas_applied: 0,
            deltas_before_init: 0,
            deltas_while_broken: 0,
            trades: 0,
            first_snapshot_recv_ns: Some(1_000_000_000),
            verified_ns: 899_000_000_000,
            window_ns: Some(900_000_000_000),
            coverage: Some(0.99),
            break_count: 0,
            breaks: vec![],
            first_segment: None,
            last_segment: None,
            segments: vec![],
            eligibility: None,
        }
    }
    #[test]
    fn eligibility_ladder_is_fail_closed() {
        assert_eq!(classify(&rec()), Eligibility::ExecutableVerified);
        let mut r = rec();
        r.first_snapshot_recv_ns = Some(120_000_000_000); // initialized two minutes after open
        assert_eq!(classify(&r), Eligibility::PartialVerified);
        let mut r = rec();
        r.break_count = 1;
        assert_eq!(classify(&r), Eligibility::PartialVerified);
        let mut r = rec();
        r.first_snapshot_recv_ns = None;
        assert_eq!(classify(&r), Eligibility::NoInitialization);
        let mut r = rec();
        r.result = Some("scalar".into());
        assert_eq!(classify(&r), Eligibility::NoSettlement);
        let mut r = rec();
        r.result = None;
        assert_eq!(classify(&r), Eligibility::NoSettlement);
        let mut r = rec();
        r.strike = None;
        assert_eq!(classify(&r), Eligibility::NoStrike);
        let mut r = rec();
        r.close_ns = None;
        assert_eq!(classify(&r), Eligibility::NoWindow);
    }
    #[test]
    fn only_supported_crypto_series_are_research_markets() {
        assert_eq!(research_asset("KXBTC15M-26OCT300900-00"), Some("BTC"));
        assert_eq!(research_asset("KXEURUSD15M-26OCT300900-30"), None);
        assert_eq!(research_asset("KXTSAD-26OCT30-T1800000"), None);
        assert_eq!(research_asset("KXBTCPERP"), None);
    }
}

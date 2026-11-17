//! Event metadata and venue-message decoding (exact decimals, no floating point money).
use crate::container::TnError;
use serde::Deserialize;

/// One decoded container event: metadata and the venue's original message bytes.
#[derive(Debug, Clone, Copy)]
pub struct EventRef<'a> {
    pub index: u64,
    pub meta: &'a [u8],
    pub msg: &'a [u8],
}

/// Observed metadata array (all 12 sampled segments, 4 event families):
/// `[1, venue, ticker, channel, type, sending_utc, field6_utc, received_utc, seq, generation, key]`.
///
/// * `received_utc` (µs) is the collector's local arrival time: the only clock research uses.
/// * `sending_utc` is the venue's `sending_ts_ms` and runs ~0.3 s AHEAD of local receipt.
/// * `field6_utc` is always earlier than `received_utc`; its meaning is NOT established and it
///   is carried verbatim, never used.
/// * `generation` is the WebSocket connection generation (also embedded in `key`); sequence
///   numbers restart with each generation. REST events carry `seq = null`, `generation = 0`.
#[derive(Debug, Deserialize)]
pub struct Meta<'a>(
    pub u32,
    #[serde(borrow)] pub &'a str,
    #[serde(borrow)] pub &'a str,
    #[serde(borrow)] pub &'a str,
    #[serde(borrow)] pub &'a str,
    #[serde(borrow)] pub Option<&'a str>,
    #[serde(borrow)] pub Option<&'a str>,
    #[serde(borrow)] pub Option<&'a str>,
    pub Option<u64>,
    pub u32,
    #[serde(borrow)] pub Option<&'a str>,
);

impl<'a> Meta<'a> {
    pub fn parse(bytes: &'a [u8], event: u64) -> Result<Self, TnError> {
        let m: Meta = serde_json::from_slice(bytes).map_err(|e| TnError::Meta {
            event,
            why: e.to_string(),
        })?;
        if m.0 != 1 {
            return Err(TnError::Meta {
                event,
                why: format!("unsupported metadata version {}", m.0),
            });
        }
        Ok(m)
    }
    pub fn venue(&self) -> &'a str {
        self.1
    }
    pub fn ticker(&self) -> &'a str {
        self.2
    }
    pub fn channel(&self) -> &'a str {
        self.3
    }
    pub fn kind(&self) -> &'a str {
        self.4
    }
    pub fn seq(&self) -> Option<u64> {
        self.8
    }
    pub fn generation(&self) -> u32 {
        self.9
    }
    pub fn is_rest(&self) -> bool {
        self.3.starts_with("REST")
    }
    /// Local receipt time, ns since the Unix epoch.
    pub fn received_ns(&self, event: u64) -> Result<i64, TnError> {
        let s = self.7.ok_or(TnError::Meta {
            event,
            why: "missing received timestamp".into(),
        })?;
        parse_utc_ns(s).ok_or(TnError::Meta {
            event,
            why: format!("bad timestamp {s:?}"),
        })
    }
    /// `run<hex>` identifier of the collector process, from the dedupe key (`run:gN:sidK:...`).
    pub fn run_id(&self) -> &'a str {
        self.10.and_then(|k| k.split(':').next()).unwrap_or("")
    }
}

/// `YYYY-MM-DDTHH:MM:SS[.f{1,9}]Z` -> ns since epoch. Rejects anything else.
pub fn parse_utc_ns(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || *b.last()? != b'Z'
    {
        return None;
    }
    let num = |r: std::ops::Range<usize>| -> Option<i64> {
        let t = s.get(r)?;
        if t.bytes().all(|c| c.is_ascii_digit()) {
            t.parse().ok()
        } else {
            None
        }
    };
    let (y, mo, d, h, mi, sec) = (
        num(0..4)?,
        num(5..7)?,
        num(8..10)?,
        num(11..13)?,
        num(14..16)?,
        num(17..19)?,
    );
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    let mut frac_ns = 0i64;
    if b.len() > 20 {
        if b[19] != b'.' {
            return None;
        }
        let digits = &s[20..s.len() - 1];
        if digits.is_empty() || digits.len() > 9 || !digits.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        frac_ns = digits.parse::<i64>().ok()? * 10i64.pow(9 - digits.len() as u32);
    } else if b[19] != b'Z' {
        return None;
    }
    // Days from civil (Howard Hinnant).
    let (y2, m2) = if mo <= 2 {
        (y - 1, mo + 9)
    } else {
        (y, mo - 3)
    };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let doy = (153 * m2 + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 24 + h) * 60 + mi) * 60 * 1_000_000_000 + sec * 1_000_000_000 + frac_ns)
}

/// Exact decimal -> integer scaled by 10^scale. Digits beyond `scale` must be zero
/// (otherwise precision would be lost silently). Accepts an optional leading '-'.
pub fn parse_scaled(s: &str, scale: u32) -> Option<i64> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    if int.is_empty()
        || !int.bytes().all(|c| c.is_ascii_digit())
        || !frac.bytes().all(|c| c.is_ascii_digit())
        || int.len() > 15
    {
        return None;
    }
    let keep = frac.len().min(scale as usize);
    if frac[keep..].bytes().any(|c| c != b'0') {
        return None;
    }
    let mut v: i64 = int.parse().ok()?;
    v = v.checked_mul(10i64.pow(scale))?;
    let mut f = 0i64;
    for (i, c) in frac[..keep].bytes().enumerate() {
        f += i64::from(c - b'0') * 10i64.pow(scale - 1 - i as u32);
    }
    let v = v.checked_add(f)?;
    Some(if neg { -v } else { v })
}
/// "0.9900" dollars -> 9900 ticks (1e-4 USD), 0..=10000.
pub fn parse_ticks(s: &str) -> Option<u16> {
    let v = parse_scaled(s, 4)?;
    u16::try_from(v).ok().filter(|t| *t <= 10_000)
}
/// "3040.50" contracts -> 304050 hundredths (signed for deltas).
pub fn parse_hundredths(s: &str) -> Option<i64> {
    parse_scaled(s, 2)
}

#[derive(Debug, Deserialize)]
struct Envelope<'a, T> {
    #[serde(borrow, rename = "type")]
    kind: &'a str,
    #[serde(default)]
    sid: Option<u64>,
    #[serde(default)]
    seq: Option<u64>,
    msg: T,
}

#[derive(Debug, Deserialize)]
pub struct DeltaBody<'a> {
    #[serde(borrow)]
    pub market_ticker: &'a str,
    #[serde(borrow)]
    pub price_dollars: &'a str,
    #[serde(borrow)]
    pub delta_fp: &'a str,
    #[serde(borrow)]
    pub side: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct SnapshotBody<'a> {
    #[serde(borrow)]
    pub market_ticker: &'a str,
    #[serde(borrow, default)]
    pub yes_dollars_fp: Vec<[&'a str; 2]>,
    #[serde(borrow, default)]
    pub no_dollars_fp: Vec<[&'a str; 2]>,
}

#[derive(Debug, Default, Deserialize)]
pub struct LifecycleBody<'a> {
    #[serde(borrow, default)]
    pub event_type: &'a str,
    #[serde(borrow, default)]
    pub market_ticker: &'a str,
    #[serde(default)]
    pub floor_strike: Option<f64>,
    #[serde(borrow, default)]
    pub strike_type: Option<&'a str>,
    #[serde(borrow, default)]
    pub result: Option<&'a str>,
    #[serde(borrow, default)]
    pub settlement_value: Option<&'a str>,
    #[serde(default)]
    pub determination_ts: Option<i64>,
    #[serde(default)]
    pub open_ts: Option<i64>,
    #[serde(default)]
    pub close_ts: Option<i64>,
}

/// Envelope header only (type, sid, seq) plus a typed body.
pub struct Wire<T> {
    pub kind: String,
    pub sid: Option<u64>,
    pub seq: Option<u64>,
    pub body: T,
}

pub fn decode_delta(msg: &[u8]) -> Result<Wire<DeltaBody<'_>>, TnError> {
    decode(msg)
}
pub fn decode_snapshot(msg: &[u8]) -> Result<Wire<SnapshotBody<'_>>, TnError> {
    decode(msg)
}
pub fn decode_lifecycle(msg: &[u8]) -> Result<Wire<LifecycleBody<'_>>, TnError> {
    decode(msg)
}

fn decode<'a, T: Deserialize<'a>>(msg: &'a [u8]) -> Result<Wire<T>, TnError> {
    let e: Envelope<'a, T> = serde_json::from_slice(msg).map_err(|e| TnError::Meta {
        event: 0,
        why: e.to_string(),
    })?;
    Ok(Wire {
        kind: e.kind.to_owned(),
        sid: e.sid,
        seq: e.seq,
        body: e.msg,
    })
}

/// REST market listing (`market_discovery`, `market_lifecycle_refresh`): only the fields research needs.
#[derive(Debug, Deserialize)]
pub struct RestMarkets<'a> {
    #[serde(borrow, default)]
    pub markets: Vec<RestMarket<'a>>,
}
#[derive(Debug, Deserialize)]
pub struct RestMarket<'a> {
    #[serde(borrow, default)]
    pub ticker: &'a str,
    #[serde(borrow, default)]
    pub open_time: Option<&'a str>,
    #[serde(borrow, default)]
    pub close_time: Option<&'a str>,
    #[serde(borrow, default)]
    pub result: Option<&'a str>,
    #[serde(borrow, default)]
    pub rules_primary: Option<&'a str>,
    #[serde(borrow, default)]
    pub rules_secondary: Option<&'a str>,
    #[serde(default)]
    pub floor_strike: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_match_known_epochs() {
        assert_eq!(parse_utc_ns("1970-01-01T00:00:00Z"), Some(0));
        // 1793364277.930 s == 2026-10-30T12:44:37.930Z (from the venue's own ts_ms in a real delta)
        assert_eq!(
            parse_utc_ns("2026-10-30T12:44:37.930Z"),
            Some(1_793_364_277_930_000_000)
        );
        assert_eq!(
            parse_utc_ns("2026-10-30T12:44:37.532894Z"),
            Some(1_793_364_277_532_894_000)
        );
        assert_eq!(
            parse_utc_ns("2000-02-29T00:00:00Z"),
            Some(951_782_400_000_000_000)
        );
        for bad in [
            "",
            "2026-10-30 12:44:37Z",
            "2026-13-30T12:44:37Z",
            "2026-10-30T12:44:37.Z",
            "2026-10-30T12:44:37.1234567890Z",
            "2026-10-30T12:44:37+00:00",
        ] {
            assert_eq!(parse_utc_ns(bad), None, "{bad}");
        }
    }

    #[test]
    fn decimals_are_exact_and_refuse_precision_loss() {
        assert_eq!(parse_ticks("0.9900"), Some(9900));
        assert_eq!(parse_ticks("0.0010"), Some(10));
        assert_eq!(parse_ticks("1.0000"), Some(10_000));
        assert_eq!(parse_ticks("1.0001"), None);
        assert_eq!(parse_ticks("0.00105"), None); // would lose a digit
        assert_eq!(parse_ticks("0.990000"), Some(9900)); // trailing zeros are fine
        assert_eq!(parse_hundredths("3040.50"), Some(304_050));
        assert_eq!(parse_hundredths("-27.00"), Some(-2_700));
        assert_eq!(parse_hundredths("1"), Some(100));
        assert_eq!(parse_hundredths("0.005"), None);
        assert_eq!(parse_hundredths("1e3"), None);
        assert_eq!(parse_hundredths(""), None);
        assert_eq!(parse_hundredths("-"), None);
    }

    #[test]
    fn real_meta_and_delta_shapes_parse() {
        let meta = br#"[1,"kalshi","KXBTC15M-26OCT300845-45","1","orderbook_delta","2026-10-30T12:44:37.930Z","2026-10-30T12:44:37.384Z","2026-10-30T12:44:37.532894Z",958128,2,"run2c9e:g2:sid1:seq958128:typeorderbook_delta"]"#;
        let m = Meta::parse(meta, 0).unwrap();
        assert_eq!(
            (m.venue(), m.kind(), m.seq(), m.generation(), m.run_id()),
            ("kalshi", "orderbook_delta", Some(958128), 2, "run2c9e")
        );
        assert_eq!(m.received_ns(0).unwrap(), 1_793_364_277_532_894_000);
        let msg = br#"{"type":"orderbook_delta","sid":1,"seq":958128,"msg":{"market_ticker":"KXBTC15M-26OCT300845-45","market_id":"x","clock":1,"price_dollars":"0.9900","delta_fp":"-27.00","side":"no","ts":"t","ts_ms":1},"sending_ts_ms":1}"#;
        let d = decode_delta(msg).unwrap();
        assert_eq!(
            (d.kind.as_str(), d.sid, d.seq, d.body.side, d.body.delta_fp),
            ("orderbook_delta", Some(1), Some(958128), "no", "-27.00")
        );
        let rest = br#"[1,"kalshi","*","REST:window","market_discovery",null,"2026-10-30T12:49:09.582Z","2026-10-30T12:49:09.594092Z",null,0,null]"#;
        let r = Meta::parse(rest, 1).unwrap();
        assert!(r.is_rest() && r.seq().is_none() && r.run_id().is_empty());
        assert!(Meta::parse(br#"[2,"k"]"#, 2).is_err());
        assert!(Meta::parse(b"not json", 3).is_err());
    }

    #[test]
    fn lifecycle_result_and_strike_parse() {
        let det = br#"{"type":"market_lifecycle_v2","sid":2,"seq":1,"msg":{"market_ticker":"T","determination_ts":1793364300,"result":"no","settlement_value":"0.0000","event_type":"determined"}}"#;
        let l = decode_lifecycle(det).unwrap().body;
        assert_eq!(
            (l.event_type, l.result, l.determination_ts),
            ("determined", Some("no"), Some(1_793_364_300))
        );
        let meta = br#"{"type":"market_lifecycle_v2","sid":2,"seq":2,"msg":{"event_type":"metadata_updated","market_ticker":"T","strike_type":"greater_or_equal","floor_strike":77192.24}}"#;
        assert_eq!(
            decode_lifecycle(meta).unwrap().body.floor_strike,
            Some(77192.24)
        );
    }
}

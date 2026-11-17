//! Dataset and experiment identity.
//!
//! Four distinct identities are reported and never conflated:
//! * `raw_input_sha256`: SHA-256 of the exact input file bytes as read
//!   (recomputed on every streaming pass; a change aborts the run).
//! * `normalized_input_sha256`: SHA-256 of a canonical, length-prefixed
//!   binary encoding of EVERY field the engine can read: identities,
//!   timestamps, every book level, references, peer/basis features,
//!   settlement, provenance and upstream source hashes. JSON formatting,
//!   whitespace and key order do not affect it; any decision-relevant
//!   change does.
//! * `experiment_sha256`: normalized input + grid + campaign config + code.
//! * `code_revision`: crate version, strategy revision and git commit.
use crate::{MarketCase, Provenance, Snapshot};
use phoenix_domain::{Book, Level, Price, Side};
use sha2::{Digest, Sha256};
use std::io::Read;

pub const NORMALIZED_SCHEMA: &str = "phoenix-normalized-case-v1";

/// Byte sink for the canonical encoding: the SHA-256 hasher (identity) and
/// the binary case cache share one encoder, so the cache file's bytes are
/// exactly what `normalized_input_sha256` hashes.
pub trait Sink {
    fn put(&mut self, b: &[u8]);
}
impl Sink for Sha256 {
    fn put(&mut self, b: &[u8]) {
        Digest::update(self, b);
    }
}
impl Sink for Vec<u8> {
    fn put(&mut self, b: &[u8]) {
        self.extend_from_slice(b);
    }
}

fn s(h: &mut impl Sink, v: &str) {
    h.put(&(v.len() as u64).to_le_bytes());
    h.put(v.as_bytes());
}
fn i(h: &mut impl Sink, v: i64) {
    h.put(&v.to_le_bytes());
}
fn u(h: &mut impl Sink, v: u64) {
    h.put(&v.to_le_bytes());
}
fn oi(h: &mut impl Sink, v: Option<i64>) {
    match v {
        None => h.put(&[0u8]),
        Some(x) => {
            h.put(&[1u8]);
            i(h, x)
        }
    }
}
fn levels(h: &mut impl Sink, l: &[Level]) {
    u(h, l.len() as u64);
    for x in l {
        u(h, u64::from(x.price.ticks()));
        u(h, x.quantity);
    }
}

/// Stream header and trailer around the encoded cases.
pub fn encode_header(h: &mut impl Sink) {
    s(h, NORMALIZED_SCHEMA);
}
pub fn encode_trailer(h: &mut impl Sink, cases: u64) {
    u(h, cases);
}

/// Feeds one case's canonical encoding into `h`.
pub fn encode_case(h: &mut impl Sink, c: &MarketCase) {
    for f in [&c.market, &c.event, &c.cluster, &c.asset] {
        s(h, f);
    }
    i(h, c.open_ns);
    i(h, c.expiration_ns);
    i(h, c.strike_cents);
    s(h, &c.rules_sha256);
    s(h, &c.source_sha256);
    h.put(&[match c.provenance {
        Provenance::Synthetic => 1u8,
        Provenance::OperatorNormalized => 2,
        Provenance::OperatorUnverified => 3,
    }]);
    h.put(&[match c.winner {
        None => 0u8,
        Some(Side::Yes) => 1,
        Some(Side::No) => 2,
    }]);
    u(h, c.snapshots.len() as u64);
    for x in &c.snapshots {
        i(h, x.received_ns);
        i(h, x.reference_cents);
        oi(h, x.peer_reference_cents);
        oi(h, x.perp_basis_bps.map(i64::from));
        s(h, &x.book.market);
        i(h, x.book.exchange_ns);
        i(h, x.book.received_ns);
        s(h, &x.book.session_id);
        u(h, x.book.sequence);
        levels(h, &x.book.yes_bids);
        levels(h, &x.book.no_bids);
    }
}

// ---- inverse of the encoding above (binary case cache) ----
// Caps bound allocations before the end-of-stream hash check can run.
const MAX_STR: u64 = crate::input::MAX_LINE_BYTES as u64;
const MAX_LEVELS: u64 = 10_001; // strictly decreasing prices in 0..=10000

fn bad(why: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, why.to_string())
}
fn rd<const N: usize>(r: &mut impl Read) -> std::io::Result<[u8; N]> {
    let mut b = [0u8; N];
    r.read_exact(&mut b)?;
    Ok(b)
}
fn ru(r: &mut impl Read) -> std::io::Result<u64> {
    Ok(u64::from_le_bytes(rd(r)?))
}
fn ri(r: &mut impl Read) -> std::io::Result<i64> {
    Ok(i64::from_le_bytes(rd(r)?))
}
fn rs(r: &mut impl Read) -> std::io::Result<String> {
    let n = ru(r)?;
    if n > MAX_STR {
        return Err(bad("string too long"));
    }
    let mut b = vec![0u8; n as usize];
    r.read_exact(&mut b)?;
    String::from_utf8(b).map_err(|_| bad("invalid utf-8"))
}
fn roi(r: &mut impl Read) -> std::io::Result<Option<i64>> {
    match rd::<1>(r)?[0] {
        0 => Ok(None),
        1 => ri(r).map(Some),
        _ => Err(bad("option tag")),
    }
}
fn rlevels(r: &mut impl Read) -> std::io::Result<Vec<Level>> {
    let n = ru(r)?;
    if n > MAX_LEVELS {
        return Err(bad("too many levels"));
    }
    (0..n)
        .map(|_| {
            let t = u16::try_from(ru(r)?).map_err(|_| bad("price"))?;
            let price = Price::new(t).map_err(|_| bad("price"))?;
            Ok(Level {
                price,
                quantity: ru(r)?,
            })
        })
        .collect()
}

pub fn decode_header(r: &mut impl Read) -> std::io::Result<()> {
    if rs(r)? != NORMALIZED_SCHEMA {
        return Err(bad("normalized schema mismatch"));
    }
    Ok(())
}
pub fn decode_trailer(r: &mut impl Read) -> std::io::Result<u64> {
    ru(r)
}

/// Inverse of [`encode_case`].
pub fn decode_case(r: &mut impl Read) -> std::io::Result<MarketCase> {
    let (market, event, cluster, asset) = (rs(r)?, rs(r)?, rs(r)?, rs(r)?);
    let (open_ns, expiration_ns, strike_cents) = (ri(r)?, ri(r)?, ri(r)?);
    let (rules_sha256, source_sha256) = (rs(r)?, rs(r)?);
    let provenance = match rd::<1>(r)?[0] {
        1 => Provenance::Synthetic,
        2 => Provenance::OperatorNormalized,
        3 => Provenance::OperatorUnverified,
        _ => return Err(bad("provenance tag")),
    };
    let winner = match rd::<1>(r)?[0] {
        0 => None,
        1 => Some(Side::Yes),
        2 => Some(Side::No),
        _ => return Err(bad("winner tag")),
    };
    let n = ru(r)?;
    if n > crate::input::MAX_SNAPSHOTS_PER_CASE as u64 {
        return Err(bad("too many snapshots"));
    }
    let mut snapshots = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let received_ns = ri(r)?;
        let reference_cents = ri(r)?;
        let peer_reference_cents = roi(r)?;
        let perp_basis_bps = match roi(r)? {
            None => None,
            Some(v) => Some(i32::try_from(v).map_err(|_| bad("perp basis"))?),
        };
        let book = Book {
            market: rs(r)?,
            exchange_ns: ri(r)?,
            received_ns: ri(r)?,
            session_id: rs(r)?,
            sequence: ru(r)?,
            yes_bids: rlevels(r)?,
            no_bids: rlevels(r)?,
        };
        snapshots.push(Snapshot {
            received_ns,
            book,
            reference_cents,
            peer_reference_cents,
            perp_basis_bps,
        });
    }
    Ok(MarketCase {
        market,
        event,
        cluster,
        asset,
        open_ns,
        expiration_ns,
        strike_cents,
        rules_sha256,
        source_sha256,
        provenance,
        snapshots,
        winner,
    })
}

/// Incremental fingerprint over an ordered case stream.
pub struct Fingerprinter {
    h: Sha256,
    n: u64,
}
impl Default for Fingerprinter {
    fn default() -> Self {
        let mut h = Sha256::new();
        encode_header(&mut h);
        Self { h, n: 0 }
    }
}
impl Fingerprinter {
    pub fn push(&mut self, c: &MarketCase) {
        self.n += 1;
        encode_case(&mut self.h, c);
    }
    pub fn finish(mut self) -> String {
        encode_trailer(&mut self.h, self.n);
        format!("{:x}", self.h.finalize())
    }
}

pub fn normalized_sha256(cases: &[MarketCase]) -> String {
    let mut f = Fingerprinter::default();
    cases.iter().for_each(|c| f.push(c));
    f.finish()
}

pub fn code_revision() -> String {
    format!(
        "phoenix-campaign-{}+{}+git:{}",
        env!("CARGO_PKG_VERSION"),
        crate::strategies::IMPLEMENTATION_REVISION,
        option_env!("PHOENIX_GIT_SHA").unwrap_or("unknown")
    )
}

pub fn experiment_sha256(normalized: &str, grid: &str, config_json: &str, code: &str) -> String {
    let mut h = Sha256::new();
    for p in ["phoenix-experiment-v1", normalized, grid, config_json, code] {
        s(&mut h, p);
    }
    format!("{:x}", h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{read_cases, to_jsonl_line};
    use crate::synthetic::{generate, Scenario};

    #[test]
    fn every_decision_relevant_field_changes_the_fingerprint() {
        let base = generate(&Scenario::efficient(1, 1, 3));
        let h0 = normalized_sha256(&base);
        assert_eq!(h0, normalized_sha256(&base.clone()));
        type Mutation = Box<dyn Fn(&mut MarketCase)>;
        let muts: Vec<Mutation> = vec![
            Box::new(|c| c.snapshots[5].book.no_bids[0].quantity += 1),
            Box::new(|c| c.snapshots[5].book.yes_bids.pop().map(|_| ()).unwrap_or(())),
            Box::new(|c| c.snapshots[7].reference_cents += 1),
            Box::new(|c| c.snapshots[7].peer_reference_cents = Some(1)),
            Box::new(|c| c.snapshots[7].perp_basis_bps = Some(-1)),
            Box::new(|c| c.snapshots[9].book.sequence += 1000),
            Box::new(|c| c.snapshots[9].book.exchange_ns -= 1),
            Box::new(|c| c.strike_cents += 1),
            Box::new(|c| c.winner = None),
            Box::new(|c| c.source_sha256 = "c".repeat(64)),
            Box::new(|c| c.provenance = Provenance::OperatorNormalized),
        ];
        for (k, m) in muts.iter().enumerate() {
            let mut v = base.clone();
            m(&mut v[3]);
            assert_ne!(normalized_sha256(&v), h0, "mutation {k} not detected");
        }
        // Order matters: the engine is order-sensitive (drawdown, splits).
        let mut swapped = base.clone();
        swapped.swap(0, 1);
        assert_ne!(normalized_sha256(&swapped), h0);
    }

    #[test]
    fn binary_encoding_round_trips_every_field() {
        let mut cases = generate(&Scenario::peer_lead(2, 1, 11));
        cases[0].winner = None;
        cases[1].provenance = Provenance::OperatorUnverified;
        cases[1].snapshots[0].perp_basis_bps = Some(-7);
        let mut buf = Vec::new();
        encode_header(&mut buf);
        cases.iter().for_each(|c| encode_case(&mut buf, c));
        encode_trailer(&mut buf, cases.len() as u64);
        assert_eq!(
            format!("{:x}", Sha256::digest(&buf)),
            normalized_sha256(&cases)
        );
        let mut r = buf.as_slice();
        decode_header(&mut r).unwrap();
        let back: Vec<MarketCase> = (0..cases.len())
            .map(|_| decode_case(&mut r).unwrap())
            .collect();
        assert_eq!(decode_trailer(&mut r).unwrap(), cases.len() as u64);
        assert!(r.is_empty());
        for (a, b) in cases.iter().zip(&back) {
            assert_eq!(to_jsonl_line(a), to_jsonl_line(b));
            assert_eq!(a.snapshots, b.snapshots);
        }
        // Truncation and absurd lengths are errors, never panics or huge allocations.
        let hdr = 8 + NORMALIZED_SCHEMA.len();
        assert!(decode_case(&mut &buf[hdr..hdr + 20]).is_err());
        let mut bad = buf.clone();
        bad[hdr + 7] = 0xff; // high byte of the first case's market length
        let mut r = bad.as_slice();
        decode_header(&mut r).unwrap();
        assert!(decode_case(&mut r).is_err());
    }

    #[test]
    fn formatting_changes_keep_normalized_but_not_raw_identity() {
        let cases = generate(&Scenario::efficient(1, 1, 4))[..3].to_vec();
        let compact: String = cases.iter().map(|c| to_jsonl_line(c) + "\n").collect();
        // Same JSON values, different bytes: spaces after separators.
        let spaced = compact.replace("\":", "\": ").replace(",\"", ", \"");
        assert_ne!(compact, spaced);
        let a = read_cases(compact.as_bytes()).unwrap();
        let b = read_cases(spaced.as_bytes()).unwrap();
        assert_eq!(normalized_sha256(&a), normalized_sha256(&b));
        assert_eq!(normalized_sha256(&a), normalized_sha256(&cases));
        let raw = |x: &str| format!("{:x}", Sha256::digest(x.as_bytes()));
        assert_ne!(raw(&compact), raw(&spaced));
    }
}

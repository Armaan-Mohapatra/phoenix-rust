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
use crate::{MarketCase, Provenance};
use phoenix_domain::{Level, Side};
use sha2::{Digest, Sha256};

pub const NORMALIZED_SCHEMA: &str = "phoenix-normalized-case-v1";

fn s(h: &mut Sha256, v: &str) {
    h.update((v.len() as u64).to_le_bytes());
    h.update(v.as_bytes());
}
fn i(h: &mut Sha256, v: i64) {
    h.update(v.to_le_bytes());
}
fn u(h: &mut Sha256, v: u64) {
    h.update(v.to_le_bytes());
}
fn oi(h: &mut Sha256, v: Option<i64>) {
    match v {
        None => h.update([0u8]),
        Some(x) => {
            h.update([1u8]);
            i(h, x)
        }
    }
}
fn levels(h: &mut Sha256, l: &[Level]) {
    u(h, l.len() as u64);
    for x in l {
        u(h, u64::from(x.price.ticks()));
        u(h, x.quantity);
    }
}

/// Feeds one case's canonical encoding into `h`.
pub fn encode_case(h: &mut Sha256, c: &MarketCase) {
    for f in [&c.market, &c.event, &c.cluster, &c.asset] {
        s(h, f);
    }
    i(h, c.open_ns);
    i(h, c.expiration_ns);
    i(h, c.strike_cents);
    s(h, &c.rules_sha256);
    s(h, &c.source_sha256);
    h.update([match c.provenance {
        Provenance::Synthetic => 1u8,
        Provenance::OperatorNormalized => 2,
    }]);
    h.update([match c.winner {
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

/// Incremental fingerprint over an ordered case stream.
pub struct Fingerprinter {
    h: Sha256,
    n: u64,
}
impl Default for Fingerprinter {
    fn default() -> Self {
        let mut h = Sha256::new();
        s(&mut h, NORMALIZED_SCHEMA);
        Self { h, n: 0 }
    }
}
impl Fingerprinter {
    pub fn push(&mut self, c: &MarketCase) {
        self.n += 1;
        encode_case(&mut self.h, c);
    }
    pub fn finish(mut self) -> String {
        u(&mut self.h, self.n);
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

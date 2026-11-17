//! Order-book reconstruction with explicit, fail-closed verification.
//!
//! A market's book is `Verified` only after a full `orderbook_snapshot` in the CURRENT
//! connection generation, and stays verified only while every applicable update is accounted
//! for. There is no backward reconstruction, no assumed-empty initial book and no interpolation.
//!
//! Anything that makes a book untrustworthy sets `Broken(reason)`; further deltas for that
//! market are ignored (and counted) until a new snapshot re-initializes it:
//! * a sequence gap or regression on the market's `(run, generation, channel)` stream
//!   (sequence numbers are global per stream, so a gap can hide any market's update);
//! * a new connection generation (books of the old generation are unusable);
//! * negative depth, a crossed book (best YES bid + best NO bid > $1.00), a record for the
//!   wrong market, or a price/quantity that cannot be represented exactly.
use crate::event::{decode_delta, decode_snapshot, parse_hundredths, parse_ticks, Meta};
use phoenix_domain::{Book, Level, Price};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Break {
    Gap { expected: u64, got: u64 },
    Regression { last: u64, got: u64 },
    NewGeneration,
    NegativeDepth,
    Crossed,
    WrongMarket,
    BadPrecision,
    Malformed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookState {
    /// No snapshot yet in this connection generation. Updates are ignored.
    Unknown,
    Verified,
    Broken(Break),
}

type StreamKey = (String, u32, String); // (run id, connection generation, channel/sid)

#[derive(Debug, Clone)]
pub struct MarketBook {
    yes: BTreeMap<u16, u64>, // price ticks -> hundredths of a contract
    no: BTreeMap<u16, u64>,
    pub state: BookState,
    pub stream: Option<(String, u32)>,
    /// Local receipt time (ns) of the last event applied to this book.
    pub last_recv_ns: i64,
    pub last_seq: u64,
    pub applied_since_snapshot: u64,
}

impl MarketBook {
    fn new() -> Self {
        Self {
            yes: BTreeMap::new(),
            no: BTreeMap::new(),
            state: BookState::Unknown,
            stream: None,
            last_recv_ns: 0,
            last_seq: 0,
            applied_since_snapshot: 0,
        }
    }
    fn crossed(&self) -> bool {
        match (self.yes.keys().next_back(), self.no.keys().next_back()) {
            (Some(y), Some(n)) => u32::from(*y) + u32::from(*n) > 10_000,
            _ => false,
        }
    }
    pub fn best_yes_bid(&self) -> Option<(u16, u64)> {
        self.yes.iter().next_back().map(|(p, q)| (*p, *q))
    }
    pub fn best_no_bid(&self) -> Option<(u16, u64)> {
        self.no.iter().next_back().map(|(p, q)| (*p, *q))
    }
    pub fn depth(&self) -> (usize, usize) {
        (self.yes.len(), self.no.len())
    }
    /// Campaign `Book`: best-first levels, quantities floored to whole contracts (sub-contract
    /// levels dropped, as the Phase 17 adapter does), top `depth` levels per side.
    pub fn to_book(
        &self,
        market: &str,
        received_ns: i64,
        exchange_ns: i64,
        session_id: &str,
        depth: usize,
    ) -> Option<Book> {
        let levels = |m: &BTreeMap<u16, u64>| -> Option<Vec<Level>> {
            m.iter()
                .rev()
                .filter(|(_, q)| **q >= 100)
                .take(depth)
                .map(|(p, q)| {
                    Some(Level {
                        price: Price::new(*p).ok()?,
                        quantity: *q / 100,
                    })
                })
                .collect()
        };
        Some(Book {
            market: market.to_owned(),
            yes_bids: levels(&self.yes)?,
            no_bids: levels(&self.no)?,
            exchange_ns,
            received_ns,
            session_id: session_id.to_owned(),
            sequence: self.last_seq,
        })
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct VerifierStats {
    pub snapshots: u64,
    pub deltas_applied: u64,
    pub deltas_before_init: u64,
    pub deltas_while_broken: u64,
    pub duplicates: u64,
    pub gaps: u64,
    pub regressions: u64,
    pub generation_changes: u64,
    pub wrong_market: u64,
    pub negative_depth: u64,
    pub crossed: u64,
    pub bad_precision: u64,
    pub malformed: u64,
}

/// What an orderbook event did, for callers that sample or count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Initialized,
    Applied,
    IgnoredUnknown,
    IgnoredBroken,
    Duplicate,
    Broke(Break),
}

#[derive(Default)]
pub struct Verifier {
    streams: HashMap<StreamKey, u64>,
    current: HashMap<String, (String, u32)>, // channel -> newest (run, generation)
    books: HashMap<String, MarketBook>,
    pub stats: VerifierStats,
    /// Every Verified -> Broken transition since the last `take_transitions`, with the market
    /// it happened to. A gap breaks ALL books of a stream, so callers need this, not just the
    /// per-event `Outcome`.
    transitions: Vec<(String, Break)>,
}

impl Verifier {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn book(&self, ticker: &str) -> Option<&MarketBook> {
        self.books.get(ticker)
    }
    pub fn tickers(&self) -> impl Iterator<Item = &String> {
        self.books.keys()
    }
    /// Sequence accounting for an event whose book we do not keep (other markets share the same
    /// stream, so skipping them would make every gap invisible).
    pub fn observe_sequence(&mut self, meta: &Meta<'_>) {
        if let Some(seq) = meta.seq() {
            let _ = self.track(meta, seq);
        }
    }
    /// Drops a finished market's book (bounded memory over a multi-day corpus).
    pub fn forget(&mut self, ticker: &str) {
        self.books.remove(ticker);
    }
    pub fn take_transitions(&mut self) -> Vec<(String, Break)> {
        std::mem::take(&mut self.transitions)
    }

    fn break_book(b: &mut MarketBook, why: Break) {
        if b.state != BookState::Broken(why) {
            b.state = BookState::Broken(why);
        }
    }

    /// Advances the channel's sequence tracking. Returns `Err(Duplicate)` for a repeated
    /// sequence number (the event must be ignored) and breaks every book of the stream on a
    /// gap or regression.
    fn track(&mut self, meta: &Meta<'_>, seq: u64) -> Result<(), Outcome> {
        let (run, gen, chan) = (
            meta.run_id().to_owned(),
            meta.generation(),
            meta.channel().to_owned(),
        );
        // New connection generation on this channel: books from older generations are unusable.
        let newest = self.current.get(&chan).cloned();
        if newest.as_ref().map_or(true, |n| (n.1, &n.0) < (gen, &run)) {
            if newest.is_some() {
                self.stats.generation_changes += 1;
                for (t, b) in self.books.iter_mut() {
                    if b.stream.as_ref().is_some_and(|s| *s != (run.clone(), gen))
                        && b.state == BookState::Verified
                    {
                        Self::break_book(b, Break::NewGeneration);
                        self.transitions.push((t.clone(), Break::NewGeneration));
                    }
                }
            }
            self.current.insert(chan.clone(), (run.clone(), gen));
        }
        let key: StreamKey = (run.clone(), gen, chan);
        match self.streams.get(&key).copied() {
            None => {}
            Some(last) if seq == last => {
                self.stats.duplicates += 1;
                return Err(Outcome::Duplicate);
            }
            Some(last) if seq == last + 1 => {}
            Some(last) => {
                let why = if seq > last {
                    Break::Gap {
                        expected: last + 1,
                        got: seq,
                    }
                } else {
                    Break::Regression { last, got: seq }
                };
                if seq > last {
                    self.stats.gaps += 1;
                } else {
                    self.stats.regressions += 1;
                }
                for (t, b) in self.books.iter_mut() {
                    if b.stream.as_ref().is_some_and(|s| *s == (run.clone(), gen))
                        && b.state == BookState::Verified
                    {
                        Self::break_book(b, why);
                        self.transitions.push((t.clone(), why));
                    }
                }
            }
        }
        self.streams.insert(key, seq);
        Ok(())
    }

    /// Feeds one `orderbook_snapshot` / `orderbook_delta` event. `recv_ns` is its local arrival time.
    pub fn on_orderbook(&mut self, meta: &Meta<'_>, msg: &[u8], recv_ns: i64) -> Outcome {
        let ticker = meta.ticker();
        let Some(seq) = meta.seq() else {
            self.stats.malformed += 1;
            return Outcome::Broke(Break::Malformed);
        };
        if let Err(o) = self.track(meta, seq) {
            return o;
        }
        let stream = (meta.run_id().to_owned(), meta.generation());
        let book = self
            .books
            .entry(ticker.to_owned())
            .or_insert_with(MarketBook::new);
        let transitions = &mut self.transitions;
        let mut broke = |s: &mut VerifierStats, b: &mut MarketBook, why: Break| {
            if b.state == BookState::Verified {
                transitions.push((ticker.to_owned(), why));
            }
            match why {
                Break::WrongMarket => s.wrong_market += 1,
                Break::NegativeDepth => s.negative_depth += 1,
                Break::Crossed => s.crossed += 1,
                Break::BadPrecision => s.bad_precision += 1,
                _ => s.malformed += 1,
            }
            Self::break_book(b, why);
            Outcome::Broke(why)
        };
        if meta.kind() == "orderbook_snapshot" {
            let snap = match decode_snapshot(msg) {
                Ok(s) => s,
                Err(_) => return broke(&mut self.stats, book, Break::Malformed),
            };
            if snap.body.market_ticker != ticker {
                return broke(&mut self.stats, book, Break::WrongMarket);
            }
            let (mut yes, mut no) = (BTreeMap::new(), BTreeMap::new());
            for (dst, src) in [
                (&mut yes, &snap.body.yes_dollars_fp),
                (&mut no, &snap.body.no_dollars_fp),
            ] {
                for [p, q] in src {
                    let (Some(p), Some(q)) = (parse_ticks(p), parse_hundredths(q)) else {
                        return broke(&mut self.stats, book, Break::BadPrecision);
                    };
                    if q < 0 {
                        return broke(&mut self.stats, book, Break::NegativeDepth);
                    }
                    if q > 0 {
                        *dst.entry(p).or_insert(0u64) += q as u64;
                    }
                }
            }
            // Validate before replacing: a rejected snapshot must leave the last verified ladder intact.
            if matches!((yes.keys().next_back(), no.keys().next_back()), (Some(y), Some(n)) if u32::from(*y) + u32::from(*n) > 10_000)
            {
                return broke(&mut self.stats, book, Break::Crossed);
            }
            book.yes = yes;
            book.no = no;
            book.state = BookState::Verified;
            book.stream = Some(stream);
            book.last_recv_ns = recv_ns;
            book.last_seq = seq;
            book.applied_since_snapshot = 0;
            self.stats.snapshots += 1;
            return Outcome::Initialized;
        }
        // orderbook_delta
        match book.state {
            BookState::Unknown => {
                self.stats.deltas_before_init += 1;
                return Outcome::IgnoredUnknown;
            }
            BookState::Broken(_) => {
                self.stats.deltas_while_broken += 1;
                return Outcome::IgnoredBroken;
            }
            BookState::Verified => {}
        }
        if book.stream.as_ref() != Some(&stream) {
            // Verified in another generation but not yet broken by a tracked change: treat as stale.
            return broke(&mut self.stats, book, Break::NewGeneration);
        }
        let d = match decode_delta(msg) {
            Ok(d) => d,
            Err(_) => return broke(&mut self.stats, book, Break::Malformed),
        };
        if d.body.market_ticker != ticker {
            return broke(&mut self.stats, book, Break::WrongMarket);
        }
        let (Some(p), Some(dq)) = (
            parse_ticks(d.body.price_dollars),
            parse_hundredths(d.body.delta_fp),
        ) else {
            return broke(&mut self.stats, book, Break::BadPrecision);
        };
        let side = match d.body.side {
            "yes" => &mut book.yes,
            "no" => &mut book.no,
            _ => return broke(&mut self.stats, book, Break::Malformed),
        };
        let cur = side.get(&p).copied().unwrap_or(0);
        let next = cur as i64 + dq;
        if next < 0 {
            return broke(&mut self.stats, book, Break::NegativeDepth);
        }
        let set = |side: &mut BTreeMap<u16, u64>, q: u64| {
            if q == 0 {
                side.remove(&p);
            } else {
                side.insert(p, q);
            }
        };
        set(side, next as u64);
        if book.crossed() {
            // Revert: the ladder must always equal the last VERIFIED state.
            let side = if d.body.side == "yes" {
                &mut book.yes
            } else {
                &mut book.no
            };
            set(side, cur);
            return broke(&mut self.stats, book, Break::Crossed);
        }
        book.last_recv_ns = recv_ns;
        book.last_seq = seq;
        book.applied_since_snapshot += 1;
        self.stats.deltas_applied += 1;
        Outcome::Applied
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(ticker: &str, kind: &str, seq: u64, gen: u32, run: &str) -> String {
        format!(
            "[1,\"kalshi\",\"{ticker}\",\"1\",\"{kind}\",\"2026-10-30T12:00:00.000Z\",\"2026-10-30T12:00:00.000Z\",\"2026-10-30T12:00:00.000000Z\",{seq},{gen},\"{run}:g{gen}:sid1:seq{seq}:type{kind}\"]"
        )
    }
    fn snap(ticker: &str, yes: &str, no: &str) -> String {
        format!("{{\"type\":\"orderbook_snapshot\",\"sid\":1,\"seq\":0,\"msg\":{{\"market_ticker\":\"{ticker}\",\"yes_dollars_fp\":{yes},\"no_dollars_fp\":{no}}}}}")
    }
    fn delta(ticker: &str, side: &str, price: &str, d: &str) -> String {
        format!("{{\"type\":\"orderbook_delta\",\"sid\":1,\"seq\":0,\"msg\":{{\"market_ticker\":\"{ticker}\",\"price_dollars\":\"{price}\",\"delta_fp\":\"{d}\",\"side\":\"{side}\"}}}}")
    }
    fn feed(
        v: &mut Verifier,
        ticker: &str,
        kind: &str,
        seq: u64,
        gen: u32,
        run: &str,
        msg: &str,
    ) -> Outcome {
        let m = meta(ticker, kind, seq, gen, run);
        let meta = Meta::parse(m.as_bytes(), 0).unwrap();
        v.on_orderbook(&meta, msg.as_bytes(), 1_000 + seq as i64)
    }
    const T: &str = "KXBTC15M-26OCT300900-00";

    #[test]
    fn delta_before_snapshot_is_ignored_and_never_creates_a_book() {
        let mut v = Verifier::new();
        assert_eq!(
            feed(
                &mut v,
                T,
                "orderbook_delta",
                1,
                1,
                "r",
                &delta(T, "yes", "0.4000", "10.00")
            ),
            Outcome::IgnoredUnknown
        );
        assert_eq!(v.book(T).unwrap().state, BookState::Unknown);
        assert_eq!(v.stats.deltas_before_init, 1);
        assert!(
            v.book(T).unwrap().best_yes_bid().is_none(),
            "no assumed-empty book"
        );
    }

    #[test]
    fn snapshot_then_deltas_reconstruct_exactly() {
        let mut v = Verifier::new();
        let s = snap(
            T,
            r#"[["0.4000","10.00"],["0.3900","5.50"]]"#,
            r#"[["0.5500","7.00"]]"#,
        );
        assert_eq!(
            feed(&mut v, T, "orderbook_snapshot", 1, 1, "r", &s),
            Outcome::Initialized
        );
        assert_eq!(
            feed(
                &mut v,
                T,
                "orderbook_delta",
                2,
                1,
                "r",
                &delta(T, "yes", "0.4000", "-4.00")
            ),
            Outcome::Applied
        );
        assert_eq!(
            feed(
                &mut v,
                T,
                "orderbook_delta",
                3,
                1,
                "r",
                &delta(T, "no", "0.5600", "2.00")
            ),
            Outcome::Applied
        );
        assert_eq!(
            feed(
                &mut v,
                T,
                "orderbook_delta",
                4,
                1,
                "r",
                &delta(T, "yes", "0.3900", "-5.50")
            ),
            Outcome::Applied
        );
        let b = v.book(T).unwrap();
        assert_eq!(b.state, BookState::Verified);
        assert_eq!(b.best_yes_bid(), Some((4000, 600)));
        assert_eq!(b.best_no_bid(), Some((5600, 200)));
        assert_eq!(b.depth(), (1, 2));
        let book = b.to_book(T, 5, 4, "r:g1", 10).unwrap();
        book.validate().unwrap();
        assert_eq!(book.yes_bids[0].quantity, 6); // 6.00 contracts
        assert_eq!(book.no_bids[0].price.ticks(), 5600);
    }

    #[test]
    fn sequence_gap_breaks_every_book_in_the_stream_until_resnapshot() {
        let mut v = Verifier::new();
        let t2 = "KXETH15M-26OCT300900-00";
        feed(
            &mut v,
            T,
            "orderbook_snapshot",
            1,
            1,
            "r",
            &snap(T, r#"[["0.4000","10.00"]]"#, "[]"),
        );
        feed(
            &mut v,
            t2,
            "orderbook_snapshot",
            2,
            1,
            "r",
            &snap(t2, r#"[["0.3000","10.00"]]"#, "[]"),
        );
        // seq 3 and 4 never arrive: the lost message could belong to either market.
        assert_eq!(
            feed(
                &mut v,
                T,
                "orderbook_delta",
                5,
                1,
                "r",
                &delta(T, "yes", "0.4000", "1.00")
            ),
            Outcome::IgnoredBroken
        );
        assert!(matches!(
            v.book(T).unwrap().state,
            BookState::Broken(Break::Gap {
                expected: 3,
                got: 5
            })
        ));
        assert!(matches!(
            v.book(t2).unwrap().state,
            BookState::Broken(Break::Gap { .. })
        ));
        assert_eq!(v.stats.gaps, 1);
        assert_eq!(
            feed(
                &mut v,
                T,
                "orderbook_snapshot",
                6,
                1,
                "r",
                &snap(T, r#"[["0.4100","3.00"]]"#, "[]")
            ),
            Outcome::Initialized
        );
        assert_eq!(v.book(T).unwrap().state, BookState::Verified);
        assert!(
            matches!(v.book(t2).unwrap().state, BookState::Broken(_)),
            "other market stays broken"
        );
    }

    #[test]
    fn duplicates_are_ignored_and_regressions_break() {
        let mut v = Verifier::new();
        feed(
            &mut v,
            T,
            "orderbook_snapshot",
            10,
            1,
            "r",
            &snap(T, r#"[["0.4000","10.00"]]"#, "[]"),
        );
        let d = delta(T, "yes", "0.4000", "1.00");
        assert_eq!(
            feed(&mut v, T, "orderbook_delta", 11, 1, "r", &d),
            Outcome::Applied
        );
        assert_eq!(
            feed(&mut v, T, "orderbook_delta", 11, 1, "r", &d),
            Outcome::Duplicate,
            "same seq must not apply twice"
        );
        assert_eq!(v.book(T).unwrap().best_yes_bid(), Some((4000, 1100)));
        feed(&mut v, T, "orderbook_delta", 9, 1, "r", &d);
        assert!(matches!(
            v.book(T).unwrap().state,
            BookState::Broken(Break::Regression { last: 11, got: 9 })
        ));
    }

    #[test]
    fn new_generation_invalidates_old_books_until_resnapshot() {
        let mut v = Verifier::new();
        feed(
            &mut v,
            T,
            "orderbook_snapshot",
            1,
            1,
            "r",
            &snap(T, r#"[["0.4000","10.00"]]"#, "[]"),
        );
        // reconnect: generation 2, sequence restarts; the delta has no snapshot in gen 2
        assert_eq!(
            feed(
                &mut v,
                T,
                "orderbook_delta",
                1,
                2,
                "r",
                &delta(T, "yes", "0.4000", "1.00")
            ),
            Outcome::IgnoredBroken
        );
        assert_eq!(
            v.book(T).unwrap().state,
            BookState::Broken(Break::NewGeneration)
        );
        assert_eq!(v.stats.generation_changes, 1);
        assert_eq!(
            feed(
                &mut v,
                T,
                "orderbook_snapshot",
                2,
                2,
                "r",
                &snap(T, r#"[["0.4200","2.00"]]"#, "[]")
            ),
            Outcome::Initialized
        );
        assert_eq!(v.book(T).unwrap().best_yes_bid(), Some((4200, 200)));
    }

    #[test]
    fn negative_depth_crossed_wrong_market_and_precision_all_fail_closed() {
        let mut v = Verifier::new();
        feed(
            &mut v,
            T,
            "orderbook_snapshot",
            1,
            1,
            "r",
            &snap(T, r#"[["0.4000","1.00"]]"#, r#"[["0.5000","1.00"]]"#),
        );
        assert_eq!(
            feed(
                &mut v,
                T,
                "orderbook_delta",
                2,
                1,
                "r",
                &delta(T, "yes", "0.4000", "-2.00")
            ),
            Outcome::Broke(Break::NegativeDepth)
        );
        let mut v = Verifier::new();
        feed(
            &mut v,
            T,
            "orderbook_snapshot",
            1,
            1,
            "r",
            &snap(T, r#"[["0.4000","1.00"]]"#, r#"[["0.5000","1.00"]]"#),
        );
        assert_eq!(
            feed(
                &mut v,
                T,
                "orderbook_delta",
                2,
                1,
                "r",
                &delta(T, "no", "0.6100", "1.00")
            ),
            Outcome::Broke(Break::Crossed),
            "0.40 + 0.61 > 1.00"
        );
        let mut v = Verifier::new();
        feed(
            &mut v,
            T,
            "orderbook_snapshot",
            1,
            1,
            "r",
            &snap(T, r#"[["0.4000","1.00"]]"#, "[]"),
        );
        assert_eq!(
            feed(
                &mut v,
                T,
                "orderbook_delta",
                2,
                1,
                "r",
                &delta("KXOTHER-1", "yes", "0.4000", "1.00")
            ),
            Outcome::Broke(Break::WrongMarket)
        );
        let mut v = Verifier::new();
        feed(
            &mut v,
            T,
            "orderbook_snapshot",
            1,
            1,
            "r",
            &snap(T, r#"[["0.4000","1.00"]]"#, "[]"),
        );
        assert_eq!(
            feed(
                &mut v,
                T,
                "orderbook_delta",
                2,
                1,
                "r",
                &delta(T, "yes", "0.40005", "1.00")
            ),
            Outcome::Broke(Break::BadPrecision)
        );
        let mut v = Verifier::new();
        let crossed = snap(T, r#"[["0.7000","1.00"]]"#, r#"[["0.4000","1.00"]]"#);
        assert_eq!(
            feed(&mut v, T, "orderbook_snapshot", 1, 1, "r", &crossed),
            Outcome::Broke(Break::Crossed),
            "a crossed snapshot never initializes"
        );
        assert!(matches!(
            v.book(T).unwrap().state,
            BookState::Broken(Break::Crossed)
        ));
    }

    #[test]
    fn rejected_updates_never_corrupt_the_last_verified_ladder() {
        let mut v = Verifier::new();
        feed(
            &mut v,
            T,
            "orderbook_snapshot",
            1,
            1,
            "r",
            &snap(T, r#"[["0.4000","1.00"]]"#, r#"[["0.5000","1.00"]]"#),
        );
        feed(
            &mut v,
            T,
            "orderbook_delta",
            2,
            1,
            "r",
            &delta(T, "no", "0.6100", "1.00"),
        ); // crosses -> reverted
        let b = v.book(T).unwrap();
        assert!(matches!(b.state, BookState::Broken(Break::Crossed)));
        assert_eq!(
            (b.best_yes_bid(), b.best_no_bid()),
            (Some((4000, 100)), Some((5000, 100)))
        );
        let mut v = Verifier::new();
        feed(
            &mut v,
            T,
            "orderbook_snapshot",
            1,
            1,
            "r",
            &snap(T, r#"[["0.4000","1.00"]]"#, "[]"),
        );
        feed(
            &mut v,
            T,
            "orderbook_snapshot",
            2,
            1,
            "r",
            &snap(T, r#"[["0.7000","1.00"]]"#, r#"[["0.4000","1.00"]]"#),
        ); // crossed snapshot
        assert_eq!(
            v.book(T).unwrap().best_yes_bid(),
            Some((4000, 100)),
            "ladder from the last verified snapshot is kept"
        );
    }

    #[test]
    fn malformed_json_and_missing_sequence_fail_closed() {
        let mut v = Verifier::new();
        assert_eq!(
            feed(&mut v, T, "orderbook_snapshot", 1, 1, "r", "{not json"),
            Outcome::Broke(Break::Malformed)
        );
        let m = "[1,\"kalshi\",\"T\",\"1\",\"orderbook_delta\",null,null,\"2026-10-30T12:00:00.000000Z\",null,1,null]";
        let meta = Meta::parse(m.as_bytes(), 0).unwrap();
        assert_eq!(
            v.on_orderbook(&meta, b"{}", 1),
            Outcome::Broke(Break::Malformed)
        );
    }

    #[test]
    fn sub_contract_levels_are_dropped_not_rounded_up() {
        let mut v = Verifier::new();
        feed(
            &mut v,
            T,
            "orderbook_snapshot",
            1,
            1,
            "r",
            &snap(T, r#"[["0.4000","0.99"],["0.3900","1.99"]]"#, "[]"),
        );
        let b = v.book(T).unwrap().to_book(T, 5, 4, "s", 10).unwrap();
        assert_eq!(b.yes_bids.len(), 1);
        assert_eq!(
            (b.yes_bids[0].price.ticks(), b.yes_bids[0].quantity),
            (3900, 1)
        );
    }
}

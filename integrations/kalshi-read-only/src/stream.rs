//! Phase 13: strictly read-only bounded market-data stream *decoder* and
//! queue. There is NO WebSocket/network transport or order submission here.
//! Optional read-only network polling must be injected separately by operator.
use phoenix_kalshi_wire::{decode_archived, ArchivedDecoded, Message};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
fn sha(s: &[u8]) -> String {
    format!("{:x}", Sha256::digest(s))
}
const MAX_SESSIONS: usize = 128;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    Fresh,
    Stale,
    Future,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    AwaitingSnapshot,
    Provisional,
    Quarantined,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Classification {
    Snapshot,
    Delta,
    Trade,
    Unhandled,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireObservation {
    pub connection_id: String,
    pub subscription_sid: u64,
    pub sequence: u64,
    pub market: String,
    pub received_ns: i64,
    pub message_sha256: String,
    pub classification: Classification,
    pub raw_sending_ns: i64,
    pub raw_event_ns: Option<i64>,
    pub reconstructed_state: State,
    pub source_clock_uncertain: bool,
    pub evidence_authenticated: bool,
}
#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub max_queue: usize,
    pub max_age_ns: i64,
    pub max_raw_bytes: usize,
    pub max_gap: u64,
    pub reject_on_overflow: bool,
}
impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_queue: 2048,
            max_age_ns: 5_000_000_000,
            max_raw_bytes: 256 * 1024,
            max_gap: 0,
            reject_on_overflow: true,
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct Health {
    pub received: u64,
    pub accepted: u64,
    pub duplicates: u64,
    pub conflicts: u64,
    pub gaps: u64,
    pub reconnects: u64,
    pub source_clock_uncertain: u64,
    pub stale: u64,
    pub queue_rejections: u64,
    pub quarantines: u64,
    pub archive_writes_authorized: bool,
    pub exchange_order_route_available: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidConfig,
    InvalidInput,
    MalformedWire,
    QueueFull,
    Conflict,
    Gap,
    Stale,
    OutOfOrder,
    Capacity,
    ReconnectionPending,
}
#[derive(Debug, Clone)]
struct Subscription {
    seq: Option<u64>,
    state: State,
    seen: BTreeMap<u64, String>,
}
pub struct ReadOnlyStream {
    config: StreamConfig,
    subscriptions: BTreeMap<(String, u64), Subscription>,
    queue: VecDeque<WireObservation>,
    health: Health,
    stopped: bool,
}
impl ReadOnlyStream {
    pub fn new(config: StreamConfig) -> Result<Self, Error> {
        if config.max_queue == 0
            || config.max_queue > 16_384
            || config.max_age_ns <= 0
            || config.max_raw_bytes == 0
            || config.max_raw_bytes > 256 * 1024
            || config.max_gap > 100_000
        {
            return Err(Error::InvalidConfig);
        }
        Ok(Self {
            config,
            subscriptions: BTreeMap::new(),
            queue: VecDeque::new(),
            health: Health::default(),
            stopped: false,
        })
    }
    pub fn health(&self) -> &Health {
        &self.health
    }
    pub fn pending(&self) -> usize {
        self.queue.len()
    }
    pub fn stop(&mut self) {
        self.stopped = true;
    }
    pub fn observe_reconnect(&mut self, connection: &str) -> Result<(), Error> {
        if connection.is_empty() || connection.len() > 128 {
            return Err(Error::InvalidInput);
        }
        for ((conn, _), sub) in &mut self.subscriptions {
            if conn == connection {
                sub.state = State::AwaitingSnapshot;
                sub.seq = None;
                sub.seen.clear();
            }
        }
        self.health.reconnects += 1;
        Ok(())
    }
    pub fn pop(&mut self) -> Option<WireObservation> {
        self.queue.pop_front()
    }
    pub fn classify_age(&self, receipt_ns: i64, now_ns: i64) -> Freshness {
        if receipt_ns > now_ns {
            Freshness::Future
        } else if now_ns - receipt_ns > self.config.max_age_ns {
            Freshness::Stale
        } else {
            Freshness::Fresh
        }
    }
    /// Decodes already received wire observations. No collector is started.
    /// Shared sid/seq is tracked across every market, never keyed by ticker.
    pub fn ingest(
        &mut self,
        connection: &str,
        raw: &str,
        received_ns: i64,
        observed_now_ns: i64,
    ) -> Result<WireObservation, Error> {
        if self.stopped {
            return Err(Error::ReconnectionPending);
        }
        if connection.is_empty()
            || connection.len() > 128
            || received_ns <= 0
            || observed_now_ns < received_ns
            || raw.len() > self.config.max_raw_bytes
        {
            return Err(Error::InvalidInput);
        }
        self.health.received += 1;
        if self.classify_age(received_ns, observed_now_ns) != Freshness::Fresh {
            self.health.stale += 1;
            return Err(Error::Stale);
        }
        if self.queue.len() >= self.config.max_queue {
            self.health.queue_rejections += 1;
            return Err(Error::QueueFull);
        }
        let decoded: ArchivedDecoded =
            decode_archived(raw, received_ns).map_err(|_| Error::MalformedWire)?;
        let (class, sid, seq, market) = match &decoded.message {
            Message::Snapshot(s) => (
                Classification::Snapshot,
                s.sid,
                s.sequence,
                s.market.clone(),
            ),
            Message::Delta(d) => (Classification::Delta, d.sid, d.sequence, d.market.clone()),
            Message::Trade(t) => (Classification::Trade, t.sid, t.sequence, t.market.clone()),
            Message::Unhandled(_) => return Err(Error::InvalidInput),
        };
        if self.subscriptions.len() >= MAX_SESSIONS
            && !self.subscriptions.contains_key(&(connection.into(), sid))
        {
            return Err(Error::Capacity);
        }
        let digest = sha(raw.as_bytes());
        let sub = self
            .subscriptions
            .entry((connection.into(), sid))
            .or_insert_with(|| Subscription {
                seq: None,
                state: State::AwaitingSnapshot,
                seen: BTreeMap::new(),
            });
        if let Some(previous) = sub.seen.get(&seq) {
            if previous == &digest {
                self.health.duplicates += 1;
                return Err(Error::OutOfOrder);
            }
            sub.state = State::Quarantined;
            self.health.conflicts += 1;
            return Err(Error::Conflict);
        }
        if sub.seq.is_some_and(|previous| seq <= previous) {
            sub.state = State::Quarantined;
            self.health.quarantines += 1;
            return Err(Error::OutOfOrder);
        }
        if sub
            .seq
            .is_some_and(|previous| seq > previous.saturating_add(self.config.max_gap + 1))
        {
            sub.state = State::Quarantined;
            self.health.gaps += 1;
            if class != Classification::Snapshot {
                // Only a fresh source snapshot may reinitialize the stream;
                // it still remains PROVISIONAL without full-channel proof.
                return Err(Error::Gap);
            }
            sub.seq = None;
            sub.seen.clear();
        }
        if sub.state == State::AwaitingSnapshot && class != Classification::Snapshot {
            self.health.quarantines += 1;
            return Err(Error::ReconnectionPending);
        }
        sub.seq = Some(seq);
        sub.seen.insert(seq, digest.clone());
        if sub.seen.len() > 4096 {
            if let Some(key) = sub.seen.keys().next().copied() {
                sub.seen.remove(&key);
            }
        }
        if class == Classification::Snapshot {
            sub.state = State::Provisional;
        }
        if sub.state != State::Provisional {
            self.health.quarantines += 1;
        }
        if decoded.source_ahead_of_receipt {
            self.health.source_clock_uncertain += 1;
        }
        let item = WireObservation {
            connection_id: connection.into(),
            subscription_sid: sid,
            sequence: seq,
            market,
            received_ns,
            message_sha256: digest,
            classification: class,
            raw_sending_ns: decoded.raw_sending_ns,
            raw_event_ns: decoded.raw_event_ns,
            reconstructed_state: sub.state,
            source_clock_uncertain: decoded.source_ahead_of_receipt,
            evidence_authenticated: false,
        };
        self.queue.push_back(item.clone());
        self.health.accepted += 1;
        Ok(item)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn snap(seq: u64) -> String {
        format!(
            r#"{{
        "type":"orderbook_snapshot","sid":1,"seq":{seq},
        "sending_ts_ms":1000,"msg":{{"market_ticker":"M","clock":1,
        "yes_dollars_fp":[["0.4000","1.00"]],
        "no_dollars_fp":[["0.5000","1.00"]]}}}}"#
        )
    }
    #[test]
    fn snapshot_is_provisional_and_duplicate_rejected() {
        let mut stream = ReadOnlyStream::new(StreamConfig::default()).unwrap();
        let s = snap(1);
        let r = stream
            .ingest("c", &s, 1_100_000_000, 1_100_000_000)
            .unwrap();
        assert_eq!(r.reconstructed_state, State::Provisional);
        assert!(!r.evidence_authenticated);
        assert_eq!(
            stream.ingest("c", &s, 1_100_000_000, 1_100_000_000),
            Err(Error::OutOfOrder)
        );
    }
    #[test]
    fn backpressure_and_shutdown_never_emit_order_routes() {
        let mut cfg = StreamConfig::default();
        cfg.max_queue = 1;
        let mut stream = ReadOnlyStream::new(cfg).unwrap();
        stream
            .ingest("c", &snap(1), 1_100_000_000, 1_100_000_000)
            .unwrap();
        assert_eq!(
            stream.ingest("c", &snap(2), 1_100_000_001, 1_100_000_001),
            Err(Error::QueueFull)
        );
        assert!(!stream.health().exchange_order_route_available);
        stream.stop();
        assert!(stream
            .ingest("c", &snap(3), 1_100_000_002, 1_100_000_002)
            .is_err());
    }
    #[test]
    fn disconnected_session_requires_fresh_snapshot() {
        let mut stream = ReadOnlyStream::new(StreamConfig::default()).unwrap();
        stream
            .ingest("c", &snap(1), 1_100_000_000, 1_100_000_000)
            .unwrap();
        stream.observe_reconnect("c").unwrap();
        assert!(stream.health().reconnects > 0);
    }
}

//! Strict offline decoder for documented Kalshi public market-data WSS messages.
//! No WebSocket transport, credentials, exchange orders, or network requests.
use phoenix_domain::{Price, Side};
use phoenix_l2::{Delta, Snapshot};
use serde_json::Value;

pub const MAX_WIRE_BYTES: usize = 256 * 1024;
pub const MAX_SNAPSHOT_LEVELS: usize = 4096;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Oversize,
    Malformed,
    MissingField(&'static str),
    InvalidPrice,
    InvalidQuantity,
    InvalidTimestamp,
    UnknownSide,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicTrade {
    pub sid: u64,
    pub sequence: u64,
    pub trade_id: String,
    pub market: String,
    pub yes_price: Price,
    pub no_price: Price,
    pub count_fp: u64,
    pub received_ns: i64,
    pub event_ns: Option<i64>,
    pub taker_outcome: Option<Side>,
    pub taker_book_side: Option<String>,
    /// A trade is NOT automatically a queue-consuming print; it does not
    /// encode inferred resting order side/priority.
    pub is_block: bool,
}
#[derive(Debug, Clone)]
pub enum Message {
    Snapshot(Snapshot),
    Delta(Delta),
    Trade(PublicTrade),
    Unhandled(String),
}
fn req<'a>(v: &'a Value, k: &'static str) -> Result<&'a Value, Error> {
    v.get(k).ok_or(Error::MissingField(k))
}
fn text(v: &Value, k: &'static str) -> Result<String, Error> {
    req(v, k)?
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or(Error::MissingField(k))
}
fn u64_at(v: &Value, k: &'static str) -> Result<u64, Error> {
    req(v, k)?.as_u64().ok_or(Error::MissingField(k))
}
/// Decimal conversion uses no floating point; exact scale only.
fn decimal(input: &str, scale: usize, negative: bool) -> Result<i128, Error> {
    let raw = input.trim();
    let neg = raw.starts_with('-');
    if neg && !negative {
        return Err(Error::InvalidQuantity);
    }
    let body = if neg { &raw[1..] } else { raw };
    let mut parts = body.split('.');
    let whole = parts.next().ok_or(Error::Malformed)?;
    let frac = parts.next().unwrap_or("");
    if parts.next().is_some()
        || whole.is_empty()
        || frac.len() > scale
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(Error::Malformed);
    }
    let factor = 10_i128.pow(scale as u32);
    let left = whole
        .parse::<i128>()
        .map_err(|_| Error::InvalidQuantity)?
        .checked_mul(factor)
        .ok_or(Error::InvalidQuantity)?;
    let right = if frac.is_empty() {
        0
    } else {
        frac.parse::<i128>()
            .map_err(|_| Error::InvalidQuantity)?
            .checked_mul(10_i128.pow((scale - frac.len()) as u32))
            .ok_or(Error::InvalidQuantity)?
    };
    let out = left.checked_add(right).ok_or(Error::InvalidQuantity)?;
    if neg {
        out.checked_neg().ok_or(Error::InvalidQuantity)
    } else {
        Ok(out)
    }
}
fn price(input: &str) -> Result<Price, Error> {
    let ticks = decimal(input, 4, false).map_err(|_| Error::InvalidPrice)?;
    Price::new(u16::try_from(ticks).map_err(|_| Error::InvalidPrice)?)
        .map_err(|_| Error::InvalidPrice)
}
fn qty_fp(input: &str) -> Result<u64, Error> {
    u64::try_from(decimal(input, 2, false).map_err(|_| Error::InvalidQuantity)?)
        .map_err(|_| Error::InvalidQuantity)
}
fn delta_fp(input: &str) -> Result<i64, Error> {
    i64::try_from(decimal(input, 2, true).map_err(|_| Error::InvalidQuantity)?)
        .map_err(|_| Error::InvalidQuantity)
}
fn timestamp_ms(v: &Value, k: &'static str) -> Result<i64, Error> {
    let ms = req(v, k)?.as_i64().ok_or(Error::InvalidTimestamp)?;
    ms.checked_mul(1_000_000).ok_or(Error::InvalidTimestamp)
}
fn source_time_ns(v: &Value, received_ns: i64) -> Result<i64, Error> {
    let ns = timestamp_ms(v, "sending_ts_ms")?;
    if ns > received_ns {
        return Err(Error::InvalidTimestamp);
    }
    Ok(ns)
}
fn side(s: &str) -> Result<Side, Error> {
    match s {
        "yes" => Ok(Side::Yes),
        "no" => Ok(Side::No),
        _ => Err(Error::UnknownSide),
    }
}
fn levels(v: &Value, k: &'static str) -> Result<Vec<(Price, u64)>, Error> {
    let rows = req(v, k)?.as_array().ok_or(Error::MissingField(k))?;
    if rows.len() > MAX_SNAPSHOT_LEVELS {
        return Err(Error::Oversize);
    }
    rows.iter()
        .map(|row| {
            let a = row.as_array().ok_or(Error::Malformed)?;
            if a.len() != 2 {
                return Err(Error::Malformed);
            }
            let p = price(a[0].as_str().ok_or(Error::InvalidPrice)?)?;
            let q = qty_fp(a[1].as_str().ok_or(Error::InvalidQuantity)?)?;
            if q == 0 {
                return Err(Error::InvalidQuantity);
            }
            Ok((p, q))
        })
        .collect()
}
pub fn decode(raw: &str, received_ns: i64) -> Result<Message, Error> {
    if raw.len() > MAX_WIRE_BYTES {
        return Err(Error::Oversize);
    }
    let root: Value = serde_json::from_str(raw).map_err(|_| Error::Malformed)?;
    let typ = text(&root, "type")?;
    // Unrecognized message types stay explicitly unhandled; raw transport
    // archive must preserve them separately, including subscriptions/acks.
    if !matches!(
        typ.as_str(),
        "orderbook_snapshot" | "orderbook_delta" | "trade"
    ) {
        return Ok(Message::Unhandled(typ));
    }
    let sid = u64_at(&root, "sid")?;
    let seq = u64_at(&root, "seq")?;
    let exchange_ns = source_time_ns(&root, received_ns)?;
    let msg = req(&root, "msg")?;
    let market = text(msg, "market_ticker")?;
    match typ.as_str() {
        "orderbook_snapshot" => Ok(Message::Snapshot(Snapshot {
            sid,
            sequence: seq,
            market,
            clock: u64_at(msg, "clock")?,
            received_ns,
            exchange_ns,
            yes_fp: levels(msg, "yes_dollars_fp")?,
            no_fp: levels(msg, "no_dollars_fp")?,
        })),
        "orderbook_delta" => Ok(Message::Delta(Delta {
            sid,
            sequence: seq,
            market,
            clock: u64_at(msg, "clock")?,
            received_ns,
            exchange_ns,
            side: side(&text(msg, "side")?)?,
            price: price(&text(msg, "price_dollars")?)?,
            delta_fp: delta_fp(&text(msg, "delta_fp")?)?,
        })),
        "trade" => {
            let yes = price(&text(msg, "yes_price_dollars")?)?;
            let no = price(&text(msg, "no_price_dollars")?)?;
            if u32::from(yes.ticks()) + u32::from(no.ticks()) != 10000 {
                return Err(Error::InvalidPrice);
            }
            let event_ns = msg
                .get("ts_ms")
                .and_then(Value::as_i64)
                .and_then(|v| v.checked_mul(1_000_000));
            if event_ns.is_some_and(|v| v > received_ns) {
                return Err(Error::InvalidTimestamp);
            }
            let outcome = msg
                .get("taker_outcome_side")
                .and_then(Value::as_str)
                .map(side)
                .transpose()?;
            let book_side = msg
                .get("taker_book_side")
                .and_then(Value::as_str)
                .map(str::to_owned);
            Ok(Message::Trade(PublicTrade {
                sid,
                sequence: seq,
                trade_id: text(msg, "trade_id")?,
                market,
                yes_price: yes,
                no_price: no,
                count_fp: qty_fp(&text(msg, "count_fp")?)?,
                received_ns,
                event_ns,
                taker_outcome: outcome,
                taker_book_side: book_side,
                is_block: msg
                    .get("is_block_trade")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            }))
        }
        _ => unreachable!(),
    }
}

/// Read-only accommodation for historical Oct 2026 wire clock skew and
/// alternate per-event timestamp fields. The strict decoder is unchanged.
/// Original stamps remain available; any positive future skew beyond 2s fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockOrigin {
    Clock,
    MessageTsMs,
}
#[derive(Debug, Clone)]
pub struct ArchivedDecoded {
    pub message: Message,
    pub raw_sending_ns: i64,
    pub raw_event_ns: Option<i64>,
    pub source_ahead_of_receipt: bool,
    pub clock_origin: ClockOrigin,
}
pub fn decode_archived(raw: &str, receipt_ns: i64) -> Result<ArchivedDecoded, Error> {
    if raw.len() > MAX_WIRE_BYTES {
        return Err(Error::Oversize);
    }
    let mut root: Value = serde_json::from_str(raw).map_err(|_| Error::Malformed)?;
    let sending = timestamp_ms(&root, "sending_ts_ms")?;
    if receipt_ns <= 0 || sending < 0 || sending.saturating_sub(receipt_ns) > 2_000_000_000 {
        return Err(Error::InvalidTimestamp);
    }
    let typ = text(&root, "type")?;
    let mut clock_origin = ClockOrigin::Clock;
    let mut raw_event_ns = None;
    let mut event_ahead = false;
    if matches!(
        typ.as_str(),
        "orderbook_snapshot" | "orderbook_delta" | "trade"
    ) {
        let msg = root.get_mut("msg").ok_or(Error::MissingField("msg"))?;
        raw_event_ns = msg
            .get("ts_ms")
            .and_then(Value::as_i64)
            .and_then(|n| n.checked_mul(1_000_000));
        event_ahead = raw_event_ns.is_some_and(|n| n > receipt_ns);
        if raw_event_ns.is_some_and(|n| n.saturating_sub(receipt_ns) > 2_000_000_000) {
            return Err(Error::InvalidTimestamp);
        }
        if !msg.get("clock").is_some_and(Value::is_u64)
            && matches!(typ.as_str(), "orderbook_snapshot" | "orderbook_delta")
        {
            let clock = msg
                .get("ts_ms")
                .and_then(Value::as_u64)
                .ok_or(Error::MissingField("clock or ts_ms"))?;
            msg.as_object_mut()
                .ok_or(Error::Malformed)?
                .insert("clock".into(), Value::from(clock));
            clock_origin = ClockOrigin::MessageTsMs;
        }
        // A public-trade event time in the capture's future is not
        // causally available. The raw source value remains above.
        if typ == "trade" && event_ahead {
            msg.as_object_mut().ok_or(Error::Malformed)?.remove("ts_ms");
        }
    }
    let send_ahead = sending > receipt_ns;
    if send_ahead {
        // Zero is an explicit UNKNOWN aligned exchange timestamp sentinel;
        // never replace source time with receipt time.
        root.as_object_mut()
            .ok_or(Error::Malformed)?
            .insert("sending_ts_ms".into(), Value::from(0));
    }
    let canonical = serde_json::to_string(&root).map_err(|_| Error::Malformed)?;
    let message = decode(&canonical, receipt_ns)?;
    Ok(ArchivedDecoded {
        message,
        raw_sending_ns: sending,
        raw_event_ns,
        source_ahead_of_receipt: send_ahead || event_ahead,
        clock_origin,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use phoenix_l2::L2Session;
    const SNAP: &str = r#"{
        "type":"orderbook_snapshot","sending_ts_ms":1000,"sid":2,"seq":2,
        "msg":{"market_ticker":"SYNTH-BTC","clock":418,
        "yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]],
        "no_dollars_fp":[["0.5400","20.00"],["0.5600","146.00"]]}}"#;
    const DELTA: &str = r#"{
        "type":"orderbook_delta","sending_ts_ms":1001,"sid":2,"seq":3,
        "msg":{"market_ticker":"SYNTH-BTC","clock":419,"price_dollars":"0.2200",
        "delta_fp":"-54.00","side":"yes"}}"#;
    const TRADE: &str = r#"{
        "type":"trade","sending_ts_ms":1002,"sid":11,"seq":2,
        "msg":{"trade_id":"fixture","market_ticker":"SYNTH-BTC",
        "yes_price_dollars":"0.3600","no_price_dollars":"0.6400",
        "count_fp":"136.00","taker_side":"no","taker_outcome_side":"no",
        "taker_book_side":"ask","is_block_trade":false,"ts_ms":1002}}"#;
    #[test]
    fn snapshot_delta_update_reconstructs_l2_without_network() {
        let mut sess = L2Session::new(2, "synthetic-session".into());
        match decode(SNAP, 1_200_000_000).unwrap() {
            Message::Snapshot(s) => {
                sess.on_snapshot(s).unwrap();
            }
            _ => panic!("expected snapshot"),
        }
        match decode(DELTA, 1_210_000_000).unwrap() {
            Message::Delta(d) => {
                sess.on_delta(d).unwrap();
            }
            _ => panic!("expected delta"),
        }
        let b = sess.ready_integer_book("SYNTH-BTC").unwrap();
        assert_eq!(b.yes_bids[0].quantity, 279);
        assert_eq!(b.no_bids[0].quantity, 146);
        assert_eq!(b.best_ask(Side::Yes).unwrap().ticks(), 4400);
    }
    #[test]
    fn public_trade_does_not_invent_maker_side() {
        match decode(TRADE, 1_300_000_000).unwrap() {
            Message::Trade(t) => {
                assert_eq!(t.count_fp, 13600);
                assert_eq!(t.taker_outcome, Some(Side::No));
                assert_eq!(t.taker_book_side.as_deref(), Some("ask"));
            }
            _ => panic!("expected trade"),
        }
    }
    #[test]
    fn fixedpoint_precise_no_float_rounding() {
        assert_eq!(price("0.0201").unwrap().ticks(), 201);
        assert_eq!(qty_fp("0.01").unwrap(), 1);
        assert_eq!(delta_fp("-1.25").unwrap(), -125);
        assert!(price("0.00001").is_err());
        assert!(qty_fp("-1.00").is_err());
    }
    #[test]
    fn malformed_and_future_rejected() {
        assert!(decode(SNAP, 999_000_000).is_err());
        assert!(decode("{}", 1_500_000_000).is_err());
        assert!(matches!(
            decode(r#"{"type":"subscription_ack"}"#, 2_000_000_000),
            Ok(Message::Unhandled(_))
        ));
    }
}

#[cfg(test)]
mod real_archival_message_tests {
    use super::*;
    const DELTA: &str = r#"{"type":"orderbook_delta","sid":1,"seq":7574674,"msg":{"delta_fp":"-15.95","market_id":"01a16a5a-aeb3-7b39-8061-52758c8f1b48","market_ticker":"KXBTC15M-26OCT241900-00","price_dollars":"0.3900","side":"yes","ts":"2026-10-24T22:47:37.403Z","ts_ms":1792882057403},"sending_ts_ms":1792882057407}"#;
    const TRADE: &str = r#"{"type":"trade","sid":2,"seq":523370,"msg":{"count_fp":"15.95","is_block_trade":false,"market_ticker":"KXBTC15M-26OCT241900-00","no_price_dollars":"0.6100","taker_book_side":"ask","taker_outcome_side":"no","taker_side":"no","trade_id":"0724b07d-7329-b6f7-2851-8aaa6ac4c834","ts":1792882057,"ts_ms":1792882057403,"yes_price_dollars":"0.3900"},"sending_ts_ms":1792882057407}"#;
    const RECEIPT: i64 = 1_792_882_057_074_000_000;
    #[test]
    fn actual_delta_without_clock_is_decoded_but_source_skew_is_flagged() {
        assert!(decode(DELTA, RECEIPT).is_err());
        let parsed = decode_archived(DELTA, RECEIPT).unwrap();
        assert!(parsed.source_ahead_of_receipt);
        assert_eq!(parsed.clock_origin, ClockOrigin::MessageTsMs);
        assert_eq!(parsed.raw_sending_ns, 1_792_882_057_407_000_000);
        assert_eq!(parsed.raw_event_ns, Some(1_792_882_057_403_000_000));
        match parsed.message {
            Message::Delta(d) => {
                assert_eq!(d.market, "KXBTC15M-26OCT241900-00");
                assert_eq!(d.delta_fp, -1595);
                assert_eq!(d.price.ticks(), 3900);
                assert_eq!(d.exchange_ns, 0);
                assert_eq!(d.clock, 1792882057403);
            }
            _ => panic!("Expected real orderbook delta"),
        }
    }
    #[test]
    fn real_trade_future_time_never_leaks_into_model() {
        let parsed = decode_archived(TRADE, RECEIPT).unwrap();
        assert!(parsed.source_ahead_of_receipt);
        assert_eq!(parsed.raw_event_ns, Some(1_792_882_057_403_000_000));
        match parsed.message {
            Message::Trade(t) => {
                assert_eq!(t.count_fp, 1595);
                assert_eq!(t.yes_price.ticks(), 3900);
                assert_eq!(t.event_ns, None);
            }
            _ => panic!("Expected real trade"),
        }
    }
    #[test]
    fn excessive_future_skew_rejected() {
        assert!(matches!(
            decode_archived(DELTA, RECEIPT - 3_000_000_000),
            Err(Error::InvalidTimestamp)
        ));
    }
}

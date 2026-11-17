//! Phase 8 Perps ingestion: schema-declared exact fixed-point observations.
//! No private source schema is bundled; caller must supply a verified mapping.
//! Original Parquet bytes can be digest-verified by convert_bounded_partition.
use crate::{convert_small_parquet, decimal_exact, CanonicalRecord, ColumnMapping};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const MAX_PRICE_SCALE: u32 = 12;
const TARGET_SCALE: u32 = 8;
const MAX_DEPTH: usize = 200;
const MAX_EVENTS: usize = 1024;
const MAX_SOURCE_BYTES: usize = 16 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidSchema,
    UnknownInstrument,
    InvalidField,
    UnknownKind,
    InvalidDecimal,
    LossyConversion,
    InvalidDepth,
    ClockSkew,
    SourceMismatch,
    InvalidProvenance,
    DuplicateConflict,
    TooManyRows,
    NonChronological,
    Source(crate::Error),
}
fn sha(s: &[u8]) -> String {
    format!("{:x}", Sha256::digest(s))
}
fn valid_sha(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|x| x.is_ascii_hexdigit())
}
fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|x| x.is_ascii_alphanumeric() || matches!(x, b'-' | b'_' | b'/' | b'.' | b':'))
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerpsFieldMap {
    pub trade_price: String,
    pub trade_quantity: String,
    pub aggressor_side: Option<String>,
    pub bid_price: String,
    pub bid_quantity: String,
    pub ask_price: String,
    pub ask_quantity: String,
    pub depth_bids: String,
    pub depth_asks: String,
    pub mark_price: Option<String>,
    pub index_price: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerpsSchema {
    pub schema_name: String,
    pub venue: String,
    pub observed_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub source_feed: String,
    pub trade_kind: String,
    pub bbo_kind: String,
    pub depth_kind: String,
    pub reference_kind: String,
    pub price_source_scale: u32,
    pub quantity_source_scale: u32,
    /// Exact source contract size in base asset units; never inferred.
    pub contracts_to_base_numerator: u64,
    pub contracts_to_base_denominator: u64,
    pub permitted_clock_skew_ns: i64,
    pub fields: PerpsFieldMap,
}
impl PerpsSchema {
    pub fn validate(&self) -> Result<(), Error> {
        for name in [
            &self.schema_name,
            &self.venue,
            &self.observed_symbol,
            &self.base_asset,
            &self.quote_asset,
            &self.source_feed,
            &self.trade_kind,
            &self.bbo_kind,
            &self.depth_kind,
            &self.reference_kind,
        ] {
            if !valid_name(name) {
                return Err(Error::InvalidSchema);
            }
        }
        if self.price_source_scale > MAX_PRICE_SCALE
            || self.quantity_source_scale > MAX_PRICE_SCALE
            || self.contracts_to_base_numerator == 0
            || self.contracts_to_base_denominator == 0
            || self.permitted_clock_skew_ns < 0
        {
            return Err(Error::InvalidSchema);
        }
        let mut kinds = std::collections::BTreeSet::new();
        for kind in [
            &self.trade_kind,
            &self.bbo_kind,
            &self.depth_kind,
            &self.reference_kind,
        ] {
            if !kinds.insert(kind) {
                return Err(Error::InvalidSchema);
            }
        }
        for name in [
            &self.fields.trade_price,
            &self.fields.trade_quantity,
            &self.fields.bid_price,
            &self.fields.bid_quantity,
            &self.fields.ask_price,
            &self.fields.ask_quantity,
            &self.fields.depth_bids,
            &self.fields.depth_asks,
        ] {
            if name.is_empty() {
                return Err(Error::InvalidSchema);
            }
        }
        Ok(())
    }
    pub fn schema_sha256(&self) -> Result<String, Error> {
        self.validate()?;
        let encoded = serde_json::to_vec(self).map_err(|_| Error::InvalidSchema)?;
        Ok(sha(&encoded))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepthLevel {
    pub price_1e8: i128,
    pub quantity_base_1e8: i128,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Aggressor {
    Buy,
    Sell,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PerpsEvent {
    Trade {
        price_1e8: i128,
        quantity_base_1e8: i128,
        aggressor: Option<Aggressor>,
    },
    Bbo {
        bid: DepthLevel,
        ask: DepthLevel,
    },
    Depth {
        bids: Vec<DepthLevel>,
        asks: Vec<DepthLevel>,
    },
    Reference {
        mark_1e8: Option<i128>,
        index_1e8: Option<i128>,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerpsObservation {
    pub venue: String,
    pub observed_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub source_session: String,
    pub source_event_id: String,
    pub received_ns: i64,
    pub exchange_ns: Option<i64>,
    pub source_clock_uncertain: bool,
    pub sequence: Option<u64>,
    pub source_partition_sha256: String,
    pub source_schema_sha256: String,
    pub row_sha256: String,
    pub event: PerpsEvent,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionContract {
    pub source_member: String,
    pub expected_partition_sha256: String,
    pub expected_schema_sha256: String,
    pub expected_rows: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerpsIndexedPartition {
    pub venue: String,
    pub observed_symbol: String,
    pub member: String,
    pub partition_sha256: String,
    pub schema_sha256: String,
    pub first_received_ns: i64,
    pub last_received_ns: i64,
    pub declared_rows: usize,
}
/// Select only manifest-indexed, finalized candidate partitions. No implicit
/// directory walk or inference that index metadata proves original file bytes.
pub fn select_indexed_partitions(
    index: &[PerpsIndexedPartition],
    venue: &str,
    symbol: &str,
    start_ns: i64,
    end_ns: i64,
    schema_sha256: &str,
) -> Result<Vec<PerpsIndexedPartition>, Error> {
    if index.len() > 10_000
        || !valid_name(venue)
        || !valid_name(symbol)
        || !valid_sha(schema_sha256)
        || start_ns <= 0
        || end_ns <= start_ns
    {
        return Err(Error::InvalidProvenance);
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut selected = Vec::new();
    for p in index {
        if !valid_name(&p.venue)
            || !valid_name(&p.observed_symbol)
            || !valid_sha(&p.partition_sha256)
            || !valid_sha(&p.schema_sha256)
            || p.member.is_empty()
            || p.member.starts_with('/')
            || p.member
                .split('/')
                .any(|x| x == "." || x == ".." || x.is_empty())
            || p.first_received_ns <= 0
            || p.last_received_ns < p.first_received_ns
            || p.declared_rows == 0
            || p.declared_rows > MAX_EVENTS
        {
            return Err(Error::InvalidProvenance);
        }
        if p.venue != venue
            || p.observed_symbol != symbol
            || p.last_received_ns < start_ns
            || p.first_received_ns > end_ns
        {
            continue;
        }
        if p.schema_sha256 != schema_sha256 {
            return Err(Error::SourceMismatch);
        }
        if !seen.insert(&p.member) {
            return Err(Error::DuplicateConflict);
        }
        selected.push(p.clone());
        if selected.len() > 32 {
            return Err(Error::TooManyRows);
        }
    }
    selected.sort_by(|a, b| {
        a.first_received_ns
            .cmp(&b.first_received_ns)
            .then(a.member.cmp(&b.member))
    });
    Ok(selected)
}
fn power10(n: u32) -> Result<i128, Error> {
    10_i128.checked_pow(n).ok_or(Error::InvalidDecimal)
}
fn fixed_from_text(raw: &str, scale: u32) -> Result<i128, Error> {
    // Do not convert JSON number/f64 approximations to financial precision.
    let exact = decimal_exact(raw, scale).map_err(|_| Error::InvalidDecimal)?;
    let q = if scale <= TARGET_SCALE {
        exact
            .checked_mul(power10(TARGET_SCALE - scale)?)
            .ok_or(Error::InvalidDecimal)?
    } else {
        let d = power10(scale - TARGET_SCALE)?;
        if exact % d != 0 {
            return Err(Error::LossyConversion);
        }
        exact / d
    };
    Ok(q)
}
fn fixed(v: &Value, key: &str, scale: u32) -> Result<i128, Error> {
    let raw = v
        .get(key)
        .and_then(Value::as_str)
        .ok_or(Error::InvalidField)?;
    fixed_from_text(raw, scale)
}
fn price(v: &Value, key: &str, s: &PerpsSchema) -> Result<i128, Error> {
    let x = fixed(v, key, s.price_source_scale)?;
    if x <= 0 {
        return Err(Error::InvalidDecimal);
    }
    Ok(x)
}
fn quantity(v: &Value, key: &str, s: &PerpsSchema) -> Result<i128, Error> {
    let x = fixed(v, key, s.quantity_source_scale)?;
    if x <= 0 {
        return Err(Error::InvalidDecimal);
    }
    let n = x
        .checked_mul(i128::from(s.contracts_to_base_numerator))
        .ok_or(Error::InvalidDecimal)?;
    let d = i128::from(s.contracts_to_base_denominator);
    if n % d != 0 {
        return Err(Error::LossyConversion);
    }
    let q = n / d;
    if q <= 0 {
        return Err(Error::InvalidDecimal);
    }
    Ok(q)
}
fn level(v: &Value, price_key: &str, qty_key: &str, s: &PerpsSchema) -> Result<DepthLevel, Error> {
    Ok(DepthLevel {
        price_1e8: price(v, price_key, s)?,
        quantity_base_1e8: quantity(v, qty_key, s)?,
    })
}
fn levels(v: &Value, key: &str, s: &PerpsSchema, bid: bool) -> Result<Vec<DepthLevel>, Error> {
    let raw = v
        .get(key)
        .and_then(Value::as_array)
        .ok_or(Error::InvalidDepth)?;
    if raw.is_empty() || raw.len() > MAX_DEPTH {
        return Err(Error::InvalidDepth);
    }
    let mut out = Vec::with_capacity(raw.len());
    for entry in raw {
        let cols = entry.as_array().ok_or(Error::InvalidDepth)?;
        if cols.len() != 2 || !cols[0].is_string() || !cols[1].is_string() {
            return Err(Error::InvalidDepth);
        }
        let price = fixed_from_text(
            cols[0].as_str().ok_or(Error::InvalidDepth)?,
            s.price_source_scale,
        )?;
        let qty = fixed_from_text(
            cols[1].as_str().ok_or(Error::InvalidDepth)?,
            s.quantity_source_scale,
        )?;
        if price <= 0 || qty <= 0 {
            return Err(Error::InvalidDepth);
        }
        let product = qty
            .checked_mul(i128::from(s.contracts_to_base_numerator))
            .ok_or(Error::InvalidDepth)?;
        let divisor = i128::from(s.contracts_to_base_denominator);
        if product % divisor != 0 {
            return Err(Error::LossyConversion);
        }
        out.push(DepthLevel {
            price_1e8: price,
            quantity_base_1e8: product / divisor,
        });
    }
    if out.windows(2).any(|x| {
        if bid {
            x[0].price_1e8 <= x[1].price_1e8
        } else {
            x[0].price_1e8 >= x[1].price_1e8
        }
    }) {
        return Err(Error::InvalidDepth);
    }
    Ok(out)
}
fn decode_event(r: &CanonicalRecord, s: &PerpsSchema) -> Result<PerpsEvent, Error> {
    let v: Value = serde_json::from_slice(&r.payload).map_err(|_| Error::InvalidField)?;
    if !v.is_object() {
        return Err(Error::InvalidField);
    }
    if r.record_kind == s.trade_kind {
        let side = match &s.fields.aggressor_side {
            None => None,
            Some(key) => match v.get(key).and_then(Value::as_str) {
                Some("buy" | "BUY") => Some(Aggressor::Buy),
                Some("sell" | "SELL") => Some(Aggressor::Sell),
                None => None,
                _ => return Err(Error::InvalidField),
            },
        };
        Ok(PerpsEvent::Trade {
            price_1e8: price(&v, &s.fields.trade_price, s)?,
            quantity_base_1e8: quantity(&v, &s.fields.trade_quantity, s)?,
            aggressor: side,
        })
    } else if r.record_kind == s.bbo_kind {
        let bid = level(&v, &s.fields.bid_price, &s.fields.bid_quantity, s)?;
        let ask = level(&v, &s.fields.ask_price, &s.fields.ask_quantity, s)?;
        if bid.price_1e8 > ask.price_1e8 {
            return Err(Error::InvalidDepth);
        }
        Ok(PerpsEvent::Bbo { bid, ask })
    } else if r.record_kind == s.depth_kind {
        let bids = levels(&v, &s.fields.depth_bids, s, true)?;
        let asks = levels(&v, &s.fields.depth_asks, s, false)?;
        if bids[0].price_1e8 > asks[0].price_1e8 {
            return Err(Error::InvalidDepth);
        }
        Ok(PerpsEvent::Depth { bids, asks })
    } else if r.record_kind == s.reference_kind {
        let opt = |key: &Option<String>| -> Result<Option<i128>, Error> {
            match key {
                Some(k) if v.get(k).is_some() && !v.get(k).is_some_and(Value::is_null) => {
                    Ok(Some(price(&v, k, s)?))
                }
                _ => Ok(None),
            }
        };
        let mark = opt(&s.fields.mark_price)?;
        let index = opt(&s.fields.index_price)?;
        if mark.is_none() && index.is_none() {
            return Err(Error::InvalidField);
        }
        Ok(PerpsEvent::Reference {
            mark_1e8: mark,
            index_1e8: index,
        })
    } else {
        Err(Error::UnknownKind)
    }
}
fn read_one(
    r: &CanonicalRecord,
    s: &PerpsSchema,
    p: &PartitionContract,
) -> Result<PerpsObservation, Error> {
    if r.feed != s.source_feed
        || r.market != s.observed_symbol
        || r.received_ns <= 0
        || r.session.is_empty()
        || r.exchange_ns.is_some_and(|x| x > r.received_ns)
    {
        return Err(Error::SourceMismatch);
    }
    let skew = r.exchange_ns.map_or(true, |ts| {
        r.received_ns
            .checked_sub(ts)
            .map_or(true, |d| d > s.permitted_clock_skew_ns)
    });
    Ok(PerpsObservation {
        venue: s.venue.clone(),
        observed_symbol: s.observed_symbol.clone(),
        base_asset: s.base_asset.clone(),
        quote_asset: s.quote_asset.clone(),
        source_session: r.session.clone(),
        source_event_id: r.stable_event_id.clone(),
        received_ns: r.received_ns,
        exchange_ns: r.exchange_ns,
        source_clock_uncertain: skew,
        sequence: r.sequence,
        source_partition_sha256: p.expected_partition_sha256.clone(),
        source_schema_sha256: p.expected_schema_sha256.clone(),
        row_sha256: r.row_sha256.clone(),
        event: decode_event(r, s)?,
    })
}
/// Decode caller supplied canonical records without reading source files.
/// Source partition authentication must have occurred before this call.
pub fn normalize_records(
    rows: &[CanonicalRecord],
    s: &PerpsSchema,
    contract: &PartitionContract,
) -> Result<Vec<PerpsObservation>, Error> {
    s.validate()?;
    if !valid_sha(&contract.expected_partition_sha256)
        || !valid_sha(&contract.expected_schema_sha256)
        || s.schema_sha256()? != contract.expected_schema_sha256
        || rows.len() != contract.expected_rows
        || rows.len() > MAX_EVENTS
        || contract.source_member.is_empty()
    {
        return Err(Error::InvalidProvenance);
    }
    let mut seen = BTreeMap::<String, String>::new();
    let mut result = Vec::new();
    for row in rows {
        if let Some(prior) = seen.get(&row.stable_event_id) {
            if prior != &row.row_sha256 {
                return Err(Error::DuplicateConflict);
            }
            continue;
        }
        seen.insert(row.stable_event_id.clone(), row.row_sha256.clone());
        result.push(read_one(row, s, contract)?);
    }
    result.sort_by(|a, b| {
        (a.received_ns, &a.source_session, &a.source_event_id).cmp(&(
            b.received_ns,
            &b.source_session,
            &b.source_event_id,
        ))
    });
    Ok(result)
}
/// Small finalized input is bounded to original historical-input reader's
/// 16MiB/1024 rows; caller must shard larger exports into verified partitions.
pub fn convert_bounded_partition(
    bytes: &[u8],
    columns: &ColumnMapping<'_>,
    schema: &PerpsSchema,
    source: &PartitionContract,
) -> Result<Vec<PerpsObservation>, Error> {
    if bytes.len() > MAX_SOURCE_BYTES {
        return Err(Error::TooManyRows);
    }
    if sha(bytes) != source.expected_partition_sha256 {
        return Err(Error::InvalidProvenance);
    }
    let rows = convert_small_parquet(bytes, columns).map_err(Error::Source)?;
    normalize_records(&rows, schema, source)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::IdentityQuality;
    fn s(venue: &str) -> PerpsSchema {
        PerpsSchema {
            schema_name: "synthetic-venue-v1".into(),
            venue: venue.into(),
            observed_symbol: "BTCUSDT".into(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
            source_feed: "synthetic-perps".into(),
            trade_kind: "trade".into(),
            bbo_kind: "bbo".into(),
            depth_kind: "depth".into(),
            reference_kind: "mark".into(),
            price_source_scale: 8,
            quantity_source_scale: 8,
            contracts_to_base_numerator: 1,
            contracts_to_base_denominator: 1,
            permitted_clock_skew_ns: 100,
            fields: PerpsFieldMap {
                trade_price: "price".into(),
                trade_quantity: "quantity".into(),
                aggressor_side: Some("aggressor".into()),
                bid_price: "bid".into(),
                bid_quantity: "bid_qty".into(),
                ask_price: "ask".into(),
                ask_quantity: "ask_qty".into(),
                depth_bids: "bids".into(),
                depth_asks: "asks".into(),
                mark_price: Some("mark".into()),
                index_price: Some("index".into()),
            },
        }
    }
    fn row(kind: &str, payload: &str, seq: u64) -> CanonicalRecord {
        CanonicalRecord {
            feed: "synthetic-perps".into(),
            market: "BTCUSDT".into(),
            session: "test-session".into(),
            record_kind: kind.into(),
            exchange_ns: Some(90),
            received_ns: 100,
            sequence: Some(seq),
            payload: payload.as_bytes().to_vec(),
            stable_event_id: format!("id{seq}"),
            row_sha256: sha(payload.as_bytes()),
            identity_quality: IdentityQuality::Sequenced,
        }
    }
    fn contract(s: &PerpsSchema, count: usize) -> PartitionContract {
        PartitionContract {
            source_member: "synthetic.parquet".into(),
            expected_partition_sha256: "a".repeat(64),
            expected_schema_sha256: s.schema_sha256().unwrap(),
            expected_rows: count,
        }
    }
    #[test]
    fn exact_decimal_trade_and_multiple_venues() {
        for venue in ["VENUE_A", "VENUE_B"] {
            let schema = s(venue);
            let r = row(
                "trade",
                r#"{"price":"123.45678901",
                "quantity":"0.00000002","aggressor":"buy"}"#,
                1,
            );
            let v = normalize_records(&[r], &schema, &contract(&schema, 1)).unwrap();
            assert_eq!(v[0].venue, venue);
            assert!(matches!(
                &v[0].event,
                PerpsEvent::Trade {
                    price_1e8: 12345678901,
                    quantity_base_1e8: 2,
                    ..
                }
            ));
        }
    }
    #[test]
    fn invalid_depth_and_float_price_rejected() {
        let schema = s("VENUE_A");
        let bad = row(
            "depth",
            r#"{"bids":[["100","2"],["101","2"]],
            "asks":[["102","2"]]}"#,
            1,
        );
        assert!(normalize_records(&[bad], &schema, &contract(&schema, 1)).is_err());
        let float = row("trade", r#"{"price":123.5,"quantity":"1"}"#, 1);
        assert!(normalize_records(&[float], &schema, &contract(&schema, 1)).is_err());
    }
    #[test]
    fn lossless_precision_and_provenance_checks() {
        let mut schema = s("VENUE_A");
        schema.price_source_scale = 12;
        let too_precise = row("trade", r#"{"price":"123.123456789001","quantity":"1"}"#, 1);
        assert!(matches!(
            normalize_records(&[too_precise], &schema, &contract(&schema, 1)),
            Err(Error::LossyConversion)
        ));
        let r = row("trade", r#"{"price":"123.5","quantity":"1"}"#, 1);
        let mut wrong = contract(&schema, 1);
        wrong.expected_schema_sha256 = "c".repeat(64);
        assert!(normalize_records(&[r], &schema, &wrong).is_err());
    }
    #[test]
    fn contract_multiplier_and_depth_precision_never_round() {
        let mut schema = s("VENUE_A");
        schema.contracts_to_base_numerator = 3;
        schema.contracts_to_base_denominator = 2;
        let trade = row(
            "trade",
            r#"{"price":"123.12345678","quantity":"2","aggressor":"sell"}"#,
            1,
        );
        let result = normalize_records(&[trade], &schema, &contract(&schema, 1)).unwrap();
        assert!(matches!(
            &result[0].event,
            PerpsEvent::Trade {
                price_1e8: 12312345678,
                quantity_base_1e8: 300000000,
                ..
            }
        ));
        let depth = row(
            "depth",
            r#"{"bids":[["100","0.00000001"]],
            "asks":[["101","1"]]}"#,
            2,
        );
        assert!(matches!(
            normalize_records(&[depth], &schema, &contract(&schema, 1)),
            Err(Error::LossyConversion)
        ));
    }
    #[test]
    fn duplicate_sequence_with_changed_payload_is_rejected() {
        let schema = s("VENUE_A");
        let a = row("trade", r#"{"price":"100","quantity":"1"}"#, 1);
        let b = row("trade", r#"{"price":"101","quantity":"1"}"#, 1);
        assert!(matches!(
            normalize_records(&[a, b], &schema, &contract(&schema, 2)),
            Err(Error::DuplicateConflict)
        ));
    }
    #[test]
    fn indexed_finalized_partition_selection_is_bounded() {
        let schema = s("VENUE_A");
        let digest = schema.schema_sha256().unwrap();
        let row = PerpsIndexedPartition {
            venue: "VENUE_A".into(),
            observed_symbol: "BTCUSDT".into(),
            member: "verified/finalized/segment_1.parquet".into(),
            partition_sha256: "b".repeat(64),
            schema_sha256: digest.clone(),
            first_received_ns: 100,
            last_received_ns: 200,
            declared_rows: 2,
        };
        let selected = select_indexed_partitions(
            std::slice::from_ref(&row),
            "VENUE_A",
            "BTCUSDT",
            110,
            150,
            &digest,
        )
        .unwrap();
        assert_eq!(selected.len(), 1);
        let mut invalid = row;
        invalid.member = "../active.sqlite-wal".into();
        assert!(
            select_indexed_partitions(&[invalid], "VENUE_A", "BTCUSDT", 110, 150, &digest).is_err()
        );
    }
    #[test]
    fn future_exchange_clock_and_missing_mark_are_rejected() {
        let schema = s("VENUE_A");
        let mut future = row("trade", r#"{"price":"100","quantity":"1"}"#, 1);
        future.exchange_ns = Some(101);
        assert!(normalize_records(&[future], &schema, &contract(&schema, 1)).is_err());
        let mark = row("mark", r#"{"not_mark":"100"}"#, 1);
        assert!(normalize_records(&[mark], &schema, &contract(&schema, 1)).is_err());
    }
}

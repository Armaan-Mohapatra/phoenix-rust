//! Bounded, schema-configured, read-only historical Parquet normalization.
//! This crate contains NO private Phoenix source schemas or data. Consumers
//! supply mappings locally; unit tests construct synthetic Parquet fixtures.
pub mod perps;
pub mod perps_features;
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use chrono::DateTime;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub const MAX_INPUT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_ROWS: usize = 1024;
#[derive(Debug, Clone, Copy)]
pub enum TimestampEncoding {
    EpochNs,
    EpochMs,
    Rfc3339,
}
#[derive(Debug, Clone)]
pub struct ColumnMapping<'a> {
    pub feed: &'a str,
    pub market: &'a str,
    pub session: &'a str,
    pub record_kind: &'a str,
    pub received: &'a str,
    pub exchange: Option<&'a str>,
    pub sequence: Option<&'a str>,
    pub payload: &'a str,
    pub timestamp: TimestampEncoding,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityQuality {
    Sequenced,
    Unsequenced,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRecord {
    pub feed: String,
    pub market: String,
    pub session: String,
    pub record_kind: String,
    pub exchange_ns: Option<i64>,
    pub received_ns: i64,
    pub sequence: Option<u64>,
    pub payload: Vec<u8>,
    pub stable_event_id: String,
    pub row_sha256: String,
    pub identity_quality: IdentityQuality,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    FileTooLarge,
    TooManyRows,
    MissingColumn(String),
    InvalidType(String),
    MissingValue(String),
    InvalidTimestamp,
    InvalidSequence,
    FutureExchangeTime,
    MissingIdentity,
    Overflow,
    Parquet(String),
    DecimalPrecision,
}
fn sha(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}
fn as_text(b: &RecordBatch, name: &str, row: usize) -> Result<Option<String>, Error> {
    let i = b
        .schema()
        .index_of(name)
        .map_err(|_| Error::MissingColumn(name.to_owned()))?;
    let col = b.column(i);
    if col.is_null(row) {
        return Ok(None);
    }
    if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(s.value(row).to_owned()));
    }
    if let Some(s) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(s.value(row).to_string()));
    }
    if let Some(s) = col.as_any().downcast_ref::<UInt64Array>() {
        return Ok(Some(s.value(row).to_string()));
    }
    Err(Error::InvalidType(name.to_owned()))
}
fn required(b: &RecordBatch, name: &str, row: usize) -> Result<String, Error> {
    let value = as_text(b, name, row)?.ok_or_else(|| Error::MissingValue(name.to_owned()))?;
    if value.is_empty() {
        return Err(Error::MissingValue(name.to_owned()));
    }
    Ok(value)
}
fn raw_payload(b: &RecordBatch, name: &str, row: usize) -> Result<Vec<u8>, Error> {
    let i = b
        .schema()
        .index_of(name)
        .map_err(|_| Error::MissingColumn(name.to_owned()))?;
    let col = b.column(i);
    if col.is_null(row) {
        return Err(Error::MissingValue(name.to_owned()));
    }
    if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(s.value(row).as_bytes().to_vec());
    }
    if let Some(s) = col.as_any().downcast_ref::<BinaryArray>() {
        return Ok(s.value(row).to_vec());
    }
    Err(Error::InvalidType(name.to_owned()))
}
fn parse_ts(input: &str, kind: TimestampEncoding) -> Result<i64, Error> {
    match kind {
        TimestampEncoding::EpochNs => input.parse::<i64>().map_err(|_| Error::InvalidTimestamp),
        TimestampEncoding::EpochMs => input
            .parse::<i64>()
            .map_err(|_| Error::InvalidTimestamp)
            .and_then(|v| v.checked_mul(1_000_000).ok_or(Error::Overflow)),
        TimestampEncoding::Rfc3339 => DateTime::parse_from_rfc3339(input)
            .map_err(|_| Error::InvalidTimestamp)?
            .timestamp_nanos_opt()
            .ok_or(Error::Overflow),
    }
}
fn identity(
    source: &str,
    market: &str,
    session: &str,
    kind: &str,
    sequence: Option<u64>,
    recv_ns: i64,
    payload: &[u8],
) -> String {
    let mut h = Sha256::new();
    for s in [source, market, session, kind] {
        h.update((s.len() as u64).to_le_bytes());
        h.update(s.as_bytes());
    }
    // No usable sequence means weak event identity, explicitly flagged.
    match sequence {
        Some(n) => {
            h.update([1]);
            h.update(n.to_le_bytes());
        }
        None => {
            h.update([0]);
            h.update(recv_ns.to_le_bytes());
            h.update(Sha256::digest(payload));
        }
    }
    format!("{:x}", h.finalize())
}
pub fn convert_batch(
    b: &RecordBatch,
    m: &ColumnMapping<'_>,
) -> Result<Vec<CanonicalRecord>, Error> {
    if b.num_rows() > MAX_ROWS {
        return Err(Error::TooManyRows);
    }
    let mut result = Vec::with_capacity(b.num_rows());
    for i in 0..b.num_rows() {
        let feed = required(b, m.feed, i)?;
        let market = required(b, m.market, i)?;
        let session = required(b, m.session, i)?;
        let record_kind = required(b, m.record_kind, i)?;
        let received_ns = parse_ts(&required(b, m.received, i)?, m.timestamp)?;
        let exchange_ns = match m.exchange {
            Some(field) => as_text(b, field, i)?
                .map(|v| parse_ts(&v, m.timestamp))
                .transpose()?,
            None => None,
        };
        if exchange_ns.is_some_and(|v| v > received_ns) {
            return Err(Error::FutureExchangeTime);
        }
        let sequence = match m.sequence {
            Some(field) => as_text(b, field, i)?
                .map(|v| v.parse::<u64>().map_err(|_| Error::InvalidSequence))
                .transpose()?,
            None => None,
        };
        let payload = raw_payload(b, m.payload, i)?;
        if feed.len() > 128
            || market.len() > 128
            || session.len() > 128
            || record_kind.len() > 128
            || payload.len() > 16_384
        {
            return Err(Error::MissingIdentity);
        }
        let stable_event_id = identity(
            &feed,
            &market,
            &session,
            &record_kind,
            sequence,
            received_ns,
            &payload,
        );
        let row_sha256 = sha(&payload);
        result.push(CanonicalRecord {
            feed,
            market,
            session,
            record_kind,
            exchange_ns,
            received_ns,
            sequence,
            payload,
            stable_event_id,
            row_sha256,
            identity_quality: if sequence.is_some() {
                IdentityQuality::Sequenced
            } else {
                IdentityQuality::Unsequenced
            },
        });
    }
    Ok(result)
}
/// Whole file limited to 16 MiB, 1024 rows. Larger historical exports must be
/// streamed in bounded row groups by a later authorized adapter.
pub fn convert_small_parquet(
    bytes: &[u8],
    m: &ColumnMapping<'_>,
) -> Result<Vec<CanonicalRecord>, Error> {
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(Error::FileTooLarge);
    }
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .map_err(|e| Error::Parquet(e.to_string()))?
        .with_batch_size(128)
        .build()
        .map_err(|e| Error::Parquet(e.to_string()))?;
    let mut result = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| Error::Parquet(e.to_string()))?;
        if result.len() + batch.num_rows() > MAX_ROWS {
            return Err(Error::TooManyRows);
        }
        result.extend(convert_batch(&batch, m)?);
    }
    Ok(result)
}
/// Exact decimal parser; does not silently round floating-point approximations.
/// Result is signed fixed-point units (e.g. scale=8 is $1e-8).
pub fn decimal_exact(value: &str, scale: u32) -> Result<i128, Error> {
    if scale > 18 {
        return Err(Error::DecimalPrecision);
    }
    let raw = value.trim();
    if raw.is_empty() || raw.contains(['e', 'E', '+']) {
        return Err(Error::DecimalPrecision);
    }
    let negative = raw.starts_with('-');
    let v = if negative { &raw[1..] } else { raw };
    let (integer, fraction) = v.split_once('.').unwrap_or((v, ""));
    if integer.is_empty()
        || !integer.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > scale as usize
    {
        return Err(Error::DecimalPrecision);
    }
    let power = 10_i128.checked_pow(scale).ok_or(Error::Overflow)?;
    let base = integer
        .parse::<i128>()
        .map_err(|_| Error::Overflow)?
        .checked_mul(power)
        .ok_or(Error::Overflow)?;
    let fraction_int = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i128>()
            .map_err(|_| Error::Overflow)?
            .checked_mul(10_i128.pow(scale - fraction.len() as u32))
            .ok_or(Error::Overflow)?
    };
    let n = base.checked_add(fraction_int).ok_or(Error::Overflow)?;
    if negative {
        n.checked_neg().ok_or(Error::Overflow)
    } else {
        Ok(n)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn map() -> ColumnMapping<'static> {
        ColumnMapping {
            feed: "source",
            market: "symbol",
            session: "session",
            record_kind: "kind",
            received: "received",
            exchange: Some("exchange"),
            sequence: Some("seq"),
            payload: "payload",
            timestamp: TimestampEncoding::Rfc3339,
        }
    }
    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("source", DataType::Utf8, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("session", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("received", DataType::Utf8, false),
            Field::new("exchange", DataType::Utf8, true),
            Field::new("seq", DataType::Int64, true),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["kalshi", "kalshi"])),
            Arc::new(StringArray::from(vec!["SYNTHETIC", "SYNTHETIC"])),
            Arc::new(StringArray::from(vec!["session-1", "session-1"])),
            Arc::new(StringArray::from(vec!["book", "trade"])),
            Arc::new(StringArray::from(vec![
                "2026-11-12T12:00:00.123456789Z",
                "2026-11-12T12:00:01Z",
            ])),
            Arc::new(StringArray::from(vec![Some("2026-11-12T12:00:00Z"), None])),
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(StringArray::from(vec!["{\"yes\":50}", "{\"trade\":1}"])),
        ];
        RecordBatch::try_new(schema, columns).unwrap()
    }
    #[test]
    fn preserves_unknown_clock_and_sequence() {
        let rows = convert_batch(&batch(), &map()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].identity_quality, IdentityQuality::Sequenced);
        assert_eq!(rows[1].identity_quality, IdentityQuality::Unsequenced);
        assert_eq!(rows[1].exchange_ns, None);
        assert_eq!(rows[1].sequence, None);
        assert_ne!(rows[0].stable_event_id, rows[1].stable_event_id);
    }
    #[test]
    fn exact_nanos_and_missing_future_rejected() {
        let rows = convert_batch(&batch(), &map()).unwrap();
        assert_eq!(rows[0].received_ns % 1_000_000_000, 123_456_789);
        assert!(matches!(
            parse_ts("2026-11-12T12:00:00", TimestampEncoding::Rfc3339),
            Err(Error::InvalidTimestamp)
        ));
    }
    #[test]
    fn parquet_roundtrip_with_synthetic_input() {
        let b = batch();
        let mut out = Vec::new();
        let mut w = ArrowWriter::try_new(&mut out, b.schema(), None).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
        let rows = convert_small_parquet(&out, &map()).unwrap();
        assert_eq!(rows.len(), 2);
    }
    #[test]
    fn fixed_point_never_uses_f64() {
        assert_eq!(decimal_exact("1.23456789", 8).unwrap(), 123_456_789);
        assert_eq!(decimal_exact("-0.125", 4).unwrap(), -1250);
        assert!(matches!(
            decimal_exact("0.123456", 4),
            Err(Error::DecimalPrecision)
        ));
        assert!(matches!(
            decimal_exact("1e-8", 8),
            Err(Error::DecimalPrecision)
        ));
    }
}

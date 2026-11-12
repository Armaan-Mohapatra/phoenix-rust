//! Bounded Arrow/Parquet batch codec for finalized research export partitions.
//! This is not a running collector or an fsync-safe archival publisher.
use std::sync::Arc;
use arrow_array::{ArrayRef,BinaryArray,Int64Array,RecordBatch,StringArray,UInt64Array,UInt8Array};
use arrow_schema::{DataType,Field,Schema};
use bytes::Bytes;
use parquet::arrow::{ArrowWriter,arrow_reader::ParquetRecordBatchReaderBuilder};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use sha2::{Digest,Sha256};

pub const MAX_ROWS:usize=4_096;
pub const MAX_PAYLOAD_BYTES:usize=16_384;
#[derive(Clone,Debug)]
pub struct Row {
    pub feed:String,pub session:String,pub market:String,
    pub exchange_ns:i64,pub received_ns:i64,pub sequence:u64,
    pub kind:u8,pub payload:Vec<u8>,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct Manifest{
    pub rows:usize,pub sha256:String,pub received_min_ns:i64,
    pub received_max_ns:i64,pub format:&'static str,
}
#[derive(Debug)]
pub enum Error{InvalidInput,StringError(String),DigestMismatch,CountMismatch}
fn schema()->Arc<Schema>{
    Arc::new(Schema::new(vec![
        Field::new("feed",DataType::Utf8,false),
        Field::new("session",DataType::Utf8,false),
        Field::new("market",DataType::Utf8,false),
        Field::new("exchange_ns",DataType::Int64,false),
        Field::new("received_ns",DataType::Int64,false),
        Field::new("sequence",DataType::UInt64,false),
        Field::new("kind",DataType::UInt8,false),
        Field::new("payload",DataType::Binary,false),
    ]))
}
fn sha256(data:&[u8])->String {format!("{:x}",Sha256::digest(data))}
/// Creates a single small Parquet partition buffer for *already finalized*
/// input batches. The caller owns durable temp-file, fsync, atomic rename and
/// sealed manifest commit responsibilities.
pub fn encode_finalized(rows:&[Row])->Result<(Vec<u8>,Manifest),Error>{
    if rows.is_empty()||rows.len()>MAX_ROWS||rows.iter().any(|r|
        r.feed.is_empty()||r.session.is_empty()||r.market.is_empty()||
        r.payload.len()>MAX_PAYLOAD_BYTES||r.exchange_ns>r.received_ns||r.kind==0){
        return Err(Error::InvalidInput);
    }
    let mut payload=Vec::new();
    let mut offsets=Vec::with_capacity(rows.len());
    for r in rows {offsets.push(r.payload.as_slice());}
    let arrays:Vec<ArrayRef>=vec![
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r|r.feed.as_str()))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r|r.session.as_str()))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r|r.market.as_str()))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r|r.exchange_ns))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r|r.received_ns))),
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r|r.sequence))),
        Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r|r.kind))),
        Arc::new(BinaryArray::from_iter_values(offsets.into_iter())),
    ];
    let batch=RecordBatch::try_new(schema(),arrays)
        .map_err(|e|Error::StringError(e.to_string()))?;
    let props=WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default())).build();
    {
        let mut writer=ArrowWriter::try_new(&mut payload,batch.schema(),Some(props))
            .map_err(|e|Error::StringError(e.to_string()))?;
        writer.write(&batch).map_err(|e|Error::StringError(e.to_string()))?;
        writer.close().map_err(|e|Error::StringError(e.to_string()))?;
    }
    let min=rows.iter().map(|r|r.received_ns).min().unwrap();
    let max=rows.iter().map(|r|r.received_ns).max().unwrap();
    let manifest=Manifest{rows:rows.len(),sha256:sha256(&payload),
        received_min_ns:min,received_max_ns:max,format:"parquet_zstd_v1"};
    Ok((payload,manifest))
}
/// A byte buffer is not automatically sealed; verification requires an
/// independently retained manifest generated BEFORE any untrusted transfer.
pub fn verify_finalized(data:&[u8],manifest:&Manifest)->Result<(),Error>{
    if sha256(data)!=manifest.sha256{return Err(Error::DigestMismatch);}
    let builder=ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(data))
        .map_err(|e|Error::StringError(e.to_string()))?;
    let mut reader=builder.build().map_err(|e|Error::StringError(e.to_string()))?;
    let mut count=0usize;
    for batch in &mut reader{
        count+=batch.map_err(|e|Error::StringError(e.to_string()))?.num_rows();
        if count>MAX_ROWS {return Err(Error::CountMismatch);}
    }
    if count!=manifest.rows{return Err(Error::CountMismatch);}
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    fn rows()->Vec<Row>{vec![
        Row{feed:"kalshi".into(),session:"x".into(),market:"M1".into(),
            exchange_ns:8,received_ns:10,sequence:1,kind:1,payload:vec![1,2,3]},
        Row{feed:"kalshi".into(),session:"x".into(),market:"M1".into(),
            exchange_ns:11,received_ns:12,sequence:2,kind:2,payload:vec![4,5]},
    ]}
    #[test]fn roundtrip_sha_and_counts(){
        let (b,m)=encode_finalized(&rows()).unwrap();
        assert_eq!(m.rows,2);
        assert_eq!((m.received_min_ns,m.received_max_ns),(10,12));
        verify_finalized(&b,&m).unwrap();
    }
    #[test]fn corrupted_bytes_do_not_pass(){
        let (mut b,m)=encode_finalized(&rows()).unwrap();b[10]^=1;
        assert!(matches!(verify_finalized(&b,&m),Err(Error::DigestMismatch)));
    }
    #[test]fn missing_and_oversize_rejected(){
        assert!(matches!(encode_finalized(&[]),Err(Error::InvalidInput)));
        let mut r=rows();r[0].payload=vec![0;MAX_PAYLOAD_BYTES+1];
        assert!(matches!(encode_finalized(&r),Err(Error::InvalidInput)));
    }
}

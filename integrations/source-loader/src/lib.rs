//! Read-only, bounded source-origin Phoenix Parquet replay from explicitly named
//! members of user-supplied LunaDelta ZIPs. No drive scanning/network/orders.
use arrow_array::{Array,Int64Array,UInt64Array,StringArray,LargeStringArray};
use bytes::Bytes;
use chrono::DateTime;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use phoenix_kalshi_wire::{decode_archived,ArchivedDecoded,Message};
use serde::{Deserialize,Serialize};
use sha2::{Sha256,Digest};
use std::{collections::HashMap,fs::File,io::Read,path::PathBuf};
use zip::ZipArchive;

const MEMBER_LIMIT:u64=16*1024*1024;
const CATALOG_LIMIT:usize=4*1024*1024;
const MAX_MEMBERS:usize=32;
const MAX_SCANNED:usize=150_000;
const MAX_SORT_BYTES:usize=64*1024*1024;
const PREFIX:&str="archive/curated/raw_events/";

#[derive(Debug)]
pub enum Error {InvalidRequest,InvalidCatalog,InvalidMember,MissingMember,
    TooLarge,TooManyRows,InvalidSource,MissingColumn(&'static str),
    InvalidRow(&'static str),BadTimestamp,ChecksumMismatch(String),
    Parquet(String),Zip(String),Io(String),Wire(String)}
#[derive(Deserialize)]
pub struct ManifestEntry { pub parquet:String,pub parquet_sha256:String,pub rows:usize }
#[derive(Debug,Clone,Serialize)]
pub struct PartitionProof {pub member:String,pub sha256:String,pub rows:usize,
    pub first_received_ns:i64,pub last_received_ns:i64}
#[derive(Debug,Clone,Serialize)]
pub struct CapturedMessage {
    pub member:String,pub row_index:usize,pub ticker:String,
    pub received_ns:i64,pub raw_sending_ns:i64,pub raw_event_ns:Option<i64>,
    pub source_clock_ahead:bool,pub connection_id:String,
    pub sid:Option<u64>,pub seq:Option<u64>,pub raw_sha256:String,
    pub raw_message:String,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize)]
pub enum Initialization {VerifiedInitialization,VerifiedResnapshot,
    PartialInitialization,SequenceGap,UnknownInitialization,InvalidBook}
/// A single-market selection does not contain the other markets sharing a
/// subscription sequence. Full-channel/snapshot identity MUST be corroborated
/// before allowing verified book initialization or maker queue calculations.
pub fn classify_book(snapshot_verified:bool,resnapshot_verified:bool,
    full_channel_gap:bool,negative_depth:bool,assumed_empty_start:bool)
    ->Initialization{
    if negative_depth{return Initialization::InvalidBook;}
    if full_channel_gap{return Initialization::SequenceGap;}
    if snapshot_verified {
        return if resnapshot_verified{Initialization::VerifiedResnapshot}
            else{Initialization::VerifiedInitialization};
    }
    if assumed_empty_start{return Initialization::PartialInitialization;}
    Initialization::UnknownInitialization
}
#[derive(Debug,Serialize)]
pub struct Evidence {
    pub ticker:String,pub scanned_rows:usize,pub matching_events:usize,
    pub snapshots:usize,pub deltas:usize,pub trades:usize,
    pub reported_clock_ahead:usize,pub partition_proofs:Vec<PartitionProof>,
    pub initialization:Initialization,
    pub independent_cf_settlement_proven:bool,pub actual_venue_fill_proven:bool,
}
#[derive(Debug,Clone)]
pub struct Request {
    pub ticker:String,pub start_received_ns:i64,pub end_received_ns:i64,
    pub named_members:Vec<String>,pub zip_paths:Vec<PathBuf>,
}
fn valid_digest(s:&str)->bool{s.len()==64&&s.bytes().all(|b|b.is_ascii_hexdigit())}
fn sha(data:&[u8])->String{format!("{:x}",Sha256::digest(data))}
fn valid_member(s:&str)->bool{s.starts_with(PREFIX)&&s.ends_with(".parquet")&&
    !s.split('/').any(|p|p==""||p=="."||p=="..")}
pub fn catalog(data:&[u8])->Result<HashMap<String,ManifestEntry>,Error>{
    if data.len()>CATALOG_LIMIT{return Err(Error::TooLarge);}
    let body=std::str::from_utf8(data).map_err(|_|Error::InvalidCatalog)?;
    let mut out=HashMap::new();
    for line in body.lines().filter(|x|!x.is_empty()){
        let row:ManifestEntry=serde_json::from_str(line)
            .map_err(|_|Error::InvalidCatalog)?;
        let name=format!("archive/{}",row.parquet);
        if !valid_member(&name)||!valid_digest(&row.parquet_sha256)||
            row.rows==0||out.insert(name,row).is_some(){
            return Err(Error::InvalidCatalog);
        }
        if out.len()>10_000{return Err(Error::TooLarge);}
    }
    if out.is_empty(){return Err(Error::InvalidCatalog);}
    Ok(out)
}
fn member(zips:&[PathBuf],name:&str)->Result<Vec<u8>,Error>{
    if !valid_member(name)||zips.is_empty()||zips.len()>12{
        return Err(Error::InvalidMember);
    }
    for path in zips {
        let f=File::open(path).map_err(|e|Error::Io(e.to_string()))?;
        let mut zip=ZipArchive::new(f).map_err(|e|Error::Zip(e.to_string()))?;
        let attempt=zip.by_name(name);
        match attempt{
            Ok(mut source)=>{
                if source.size()>MEMBER_LIMIT||source.is_dir(){return Err(Error::TooLarge);}
                let mut data=Vec::new();
                source.take(MEMBER_LIMIT+1).read_to_end(&mut data)
                    .map_err(|e|Error::Io(e.to_string()))?;
                if data.len() as u64>MEMBER_LIMIT{return Err(Error::TooLarge);}
                return Ok(data);
            }
            Err(zip::result::ZipError::FileNotFound)=>{},
            Err(e)=>return Err(Error::Zip(e.to_string())),
        }
    }
    Err(Error::MissingMember)
}
fn field<'a>(b:&'a arrow_array::RecordBatch,k:&'static str)
    ->Result<&'a dyn Array,Error>{
    let index=b.schema().index_of(k).map_err(|_|Error::MissingColumn(k))?;
    Ok(b.column(index).as_ref())
}
fn string(b:&arrow_array::RecordBatch,k:&'static str,i:usize)
    ->Result<Option<String>,Error>{
    let a=field(b,k)?;if a.is_null(i){return Ok(None);}
    if let Some(v)=a.as_any().downcast_ref::<StringArray>(){
        return Ok(Some(v.value(i).to_owned()));
    }
    if let Some(v)=a.as_any().downcast_ref::<LargeStringArray>(){
        return Ok(Some(v.value(i).to_owned()));
    }
    Err(Error::InvalidRow(k))
}
fn number(b:&arrow_array::RecordBatch,k:&'static str,i:usize)
    ->Result<Option<u64>,Error>{
    let a=field(b,k)?;if a.is_null(i){return Ok(None);}
    if let Some(v)=a.as_any().downcast_ref::<UInt64Array>(){return Ok(Some(v.value(i)));}
    if let Some(v)=a.as_any().downcast_ref::<Int64Array>(){
        return u64::try_from(v.value(i)).map(Some).map_err(|_|Error::InvalidRow(k));
    }
    Err(Error::InvalidRow(k))
}
fn timestamp(raw:&str)->Result<i64,Error>{
    DateTime::parse_from_rfc3339(raw).map_err(|_|Error::BadTimestamp)?
        .timestamp_nanos_opt().ok_or(Error::BadTimestamp)
}
/// A bounded, deterministic in-memory merge of selected Parquet source events.
/// The source has already been restricted by explicit partition names, ticker,
/// and receipt-time range. MAX_SORT_BYTES limits memory; no root walks.
pub fn replay<F>(req:&Request,manifest:&[u8],mut consume:F)->Result<Evidence,Error>
where F:FnMut(&CapturedMessage,&ArchivedDecoded)->Result<(),Error>{
    if req.ticker.is_empty()||req.ticker.len()>128||
        req.start_received_ns>=req.end_received_ns||
        req.named_members.is_empty()||req.named_members.len()>MAX_MEMBERS||
        req.zip_paths.is_empty(){return Err(Error::InvalidRequest);}
    let index=catalog(manifest)?;
    let mut parts=Vec::new();
    let mut selected=Vec::<CapturedMessage>::new();
    let mut scan_count=0usize;let mut stored_bytes=0usize;
    for name in &req.named_members{
        if !valid_member(name){return Err(Error::InvalidMember);}
        let row=index.get(name).ok_or(Error::InvalidCatalog)?;
        let bytes=member(&req.zip_paths,name)?;
        let observed=sha(&bytes);
        if !observed.eq_ignore_ascii_case(&row.parquet_sha256){
            return Err(Error::ChecksumMismatch(name.clone()));
        }
        let reader=ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .map_err(|e|Error::Parquet(e.to_string()))?
            .with_batch_size(256).build()
            .map_err(|e|Error::Parquet(e.to_string()))?;
        let mut local=0usize;let mut min=i64::MAX;let mut max=i64::MIN;
        for batch in reader {
            let batch=batch.map_err(|e|Error::Parquet(e.to_string()))?;
            scan_count=scan_count.checked_add(batch.num_rows()).ok_or(Error::TooManyRows)?;
            if scan_count>MAX_SCANNED{return Err(Error::TooManyRows);}
            for i in 0..batch.num_rows(){
                let received=string(&batch,"received_at_utc",i)?
                    .ok_or(Error::InvalidRow("received_at_utc"))?;
                let at=timestamp(&received)?;min=min.min(at);max=max.max(at);
                local+=1;
                if at<req.start_received_ns||at>req.end_received_ns{continue;}
                let ticker=string(&batch,"ticker",i)?.unwrap_or_default();
                if ticker!=req.ticker{continue;}
                let raw=string(&batch,"raw_message",i)?
                    .ok_or(Error::InvalidRow("raw_message"))?;
                if raw.len()>256*1024{return Err(Error::TooLarge);}
                let connection=string(&batch,"connection_id",i)?
                    .ok_or(Error::InvalidRow("connection_id"))?;
                let decoded=decode_archived(&raw,at)
                    .map_err(|e|Error::Wire(format!("{e:?}")))?;
                let is_for_market=match &decoded.message{
                    Message::Snapshot(x)=>x.market==ticker,
                    Message::Delta(x)=>x.market==ticker,
                    Message::Trade(x)=>x.market==ticker,
                    _=>false,
                };
                if !is_for_market{continue;}
                stored_bytes=stored_bytes.checked_add(raw.len()+connection.len()+name.len()+256)
                    .ok_or(Error::TooLarge)?;
                if stored_bytes>MAX_SORT_BYTES{return Err(Error::TooLarge);}
                selected.push(CapturedMessage{
                    member:name.clone(),row_index:local-1,ticker,
                    received_ns:at,raw_sending_ns:decoded.raw_sending_ns,
                    raw_event_ns:decoded.raw_event_ns,
                    source_clock_ahead:decoded.source_ahead_of_receipt,
                    connection_id:connection,sid:number(&batch,"sid",i)?,
                    seq:number(&batch,"seq",i)?,raw_sha256:sha(raw.as_bytes()),
                    raw_message:raw,
                });
            }
        }
        if local!=row.rows{return Err(Error::InvalidCatalog);}
        parts.push(PartitionProof{member:name.clone(),sha256:observed,
            rows:local,first_received_ns:min,last_received_ns:max});
    }
    selected.sort_by(|a,b|(
        a.received_ns,&a.connection_id,a.sid,a.seq,&a.member,a.row_index
    ).cmp(&(b.received_ns,&b.connection_id,b.sid,b.seq,&b.member,b.row_index)));
    let mut snapshots=0usize;let mut deltas=0usize;let mut trades=0usize;let mut skew=0usize;
    for event in &selected{
        let decoded=decode_archived(&event.raw_message,event.received_ns)
            .map_err(|e|Error::Wire(format!("{e:?}")))?;
        match decoded.message{
            Message::Snapshot(_)=>snapshots+=1,
            Message::Delta(_)=>deltas+=1,
            Message::Trade(_)=>trades+=1,
            Message::Unhandled(_)=>{},
        }
        if event.source_clock_ahead{skew+=1;}
        consume(event,&decoded)?;
    }
    Ok(Evidence{ticker:req.ticker.clone(),scanned_rows:scan_count,
        matching_events:selected.len(),snapshots,deltas,trades,
        reported_clock_ahead:skew,partition_proofs:parts,
        // A market-filtered replay cannot certify the shared-channel
        // sequence or source snapshot initialization.
        initialization:Initialization::UnknownInitialization,
        independent_cf_settlement_proven:false,actual_venue_fill_proven:false})
}


#[cfg(test)]
mod tests{
    use super::*;
    use arrow_array::{ArrayRef,RecordBatch,UInt64Array};
    use arrow_schema::{Schema,Field,DataType};
    use parquet::arrow::ArrowWriter;
    use std::{sync::Arc,io::Write};
    use tempfile::TempDir;
    use zip::{ZipWriter,write::FileOptions,CompressionMethod};

    const NAME:&str="archive/curated/raw_events/record_date=2026-10-24/segment_fixture.parquet";
    const DELTA:&str=r#"{"type":"orderbook_delta","sid":1,"seq":7574674,"msg":{"delta_fp":"-15.95","market_ticker":"KXBTC15M-26OCT241900-00","price_dollars":"0.3900","side":"yes","ts_ms":1792882057403},"sending_ts_ms":1792882057407}"#;
    const TRADE:&str=r#"{"type":"trade","sid":2,"seq":523370,"msg":{"count_fp":"15.95","is_block_trade":false,"market_ticker":"KXBTC15M-26OCT241900-00","no_price_dollars":"0.6100","trade_id":"bounded-real-format","ts_ms":1792882057403,"yes_price_dollars":"0.3900"},"sending_ts_ms":1792882057407}"#;
    fn setup(t:&TempDir)->(Request,Vec<u8>){
        let schema=Arc::new(Schema::new(vec![
            Field::new("received_at_utc",DataType::Utf8,false),
            Field::new("ticker",DataType::Utf8,false),
            Field::new("raw_message",DataType::Utf8,false),
            Field::new("connection_id",DataType::Utf8,false),
            Field::new("sid",DataType::UInt64,false),
            Field::new("seq",DataType::UInt64,false),
        ]));
        let columns:Vec<ArrayRef>=vec![
            Arc::new(StringArray::from(vec!["2026-10-24T22:47:37.104Z",
                "2026-10-24T22:47:37.074Z"])),
            Arc::new(StringArray::from(vec!["KXBTC15M-26OCT241900-00",
                "KXBTC15M-26OCT241900-00"])),
            Arc::new(StringArray::from(vec![TRADE,DELTA])),
            Arc::new(StringArray::from(vec!["S1","S1"])),
            Arc::new(UInt64Array::from(vec![2,1])),
            Arc::new(UInt64Array::from(vec![523370,7574674])),
        ];
        let batch=RecordBatch::try_new(schema,columns).unwrap();
        let mut buf=Vec::new();
        {
            let mut writer=ArrowWriter::try_new(&mut buf,batch.schema(),None).unwrap();
            writer.write(&batch).unwrap();writer.close().unwrap();
        }
        let checksum=sha(&buf);
        let path=t.path().join("research_part001.zip");
        let mut zip=ZipWriter::new(File::create(&path).unwrap());
        zip.start_file(NAME,FileOptions::default()
            .compression_method(CompressionMethod::Stored)).unwrap();
        zip.write_all(&buf).unwrap();zip.finish().unwrap();
        let index=format!(r#"{{"parquet":"curated/raw_events/record_date=2026-10-24/segment_fixture.parquet","parquet_sha256":"{checksum}","rows":2}}"#).into_bytes();
        let req=Request{ticker:"KXBTC15M-26OCT241900-00".into(),
            start_received_ns:1_792_882_057_000_000_000,
            end_received_ns:1_792_882_058_000_000_000,
            named_members:vec![NAME.into()],zip_paths:vec![path]};
        (req,index)
    }
    #[test]
    fn bounded_original_wire_payloads_sort_by_receipt_and_preserve_skew(){
        let t=TempDir::new().unwrap();let (req,index)=setup(&t);
        let mut observed=Vec::new();
        let proof=replay(&req,&index,|e,decoded|{
            observed.push((e.received_ns,decoded.raw_sending_ns,e.seq));Ok(())
        }).unwrap();
        assert_eq!(proof.scanned_rows,2);
        assert_eq!(proof.matching_events,2);
        assert_eq!((proof.deltas,proof.trades),(1,1));
        assert_eq!(proof.reported_clock_ahead,2);
        assert_eq!(proof.initialization,Initialization::UnknownInitialization);
        assert_eq!(observed[0].2,Some(7574674));
        assert!(observed[0].0<observed[1].0);
    }
    #[test]
    fn digest_conflict_and_path_traversal_fail_closed(){
        let t=TempDir::new().unwrap();let (mut req,index)=setup(&t);
        let mut bad=String::from_utf8(index.clone()).unwrap();
        let marker="\"parquet_sha256\":\"";
        let offset=bad.find(marker).unwrap()+marker.len();
        bad.replace_range(offset..offset+1,"f");
        assert!(matches!(replay(&req,bad.as_bytes(),|_,_|Ok(())),
            Err(Error::ChecksumMismatch(_))));
        req.named_members=vec!["archive/curated/raw_events/../secret.parquet".into()];
        assert!(matches!(replay(&req,&index,|_,_|Ok(())),
            Err(Error::InvalidMember)));
    }
    #[test]
    fn no_verified_snapshot_never_promotes_a_market_book(){
        assert_eq!(classify_book(false,false,false,false,true),
            Initialization::PartialInitialization);
        assert_eq!(classify_book(false,false,true,false,true),
            Initialization::SequenceGap);
        assert_eq!(classify_book(true,false,false,false,false),
            Initialization::VerifiedInitialization);
        assert_eq!(classify_book(true,true,false,false,false),
            Initialization::VerifiedResnapshot);
        assert_eq!(classify_book(true,false,false,true,false),
            Initialization::InvalidBook);
    }
}

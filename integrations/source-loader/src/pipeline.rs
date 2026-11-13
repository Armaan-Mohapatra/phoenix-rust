//! Source-origin, bounded chronological research replay. No venue execution.
use crate::{forensic, replay, CapturedMessage, ChannelRun, Evidence, Initialization, Request};
use phoenix_domain::Side;
use phoenix_kalshi_wire::{ArchivedDecoded, Message};
use serde::{Deserialize,Serialize};
use sha2::{Digest,Sha256};
use std::collections::BTreeMap;

const MAX_CHECKPOINTS:usize=64;
const MAX_IDENTITIES:usize=250_000;

#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum EvidenceClass {SourceDerivedProvisional,Incomplete,Invalid}

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct FractionalLevel {
    pub price_ticks_1e4:u16,
    pub quantity_hundredths:u64,
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct Checkpoint {
    pub asof_received_ns:i64,
    pub last_applied_received_ns:Option<i64>,
    pub last_message_sha256:Option<String>,
    pub connection_id:Option<String>,
    pub sid:Option<u64>,
    pub last_sequence:Option<u64>,
    pub book_state:String,
    pub best_yes_bid:Option<FractionalLevel>,
    pub best_yes_ask:Option<FractionalLevel>,
    pub best_no_bid:Option<FractionalLevel>,
    pub best_no_ask:Option<FractionalLevel>,
    pub execution_eligible:bool,
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ProjectionReference {
    pub asof_received_ns:i64,
    pub fixture_sha256:String,
    pub yes_bid:Option<FractionalLevel>,
    pub yes_ask:Option<FractionalLevel>,
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct ProjectionComparison {
    pub asof_received_ns:i64,
    pub fixture_sha256:String,
    pub matching_asof_checkpoint:bool,
    pub bids_equal:Option<bool>,
    pub asks_equal:Option<bool>,
    pub parity_is_historical_proof:bool,
}
#[derive(Debug,Serialize)]
pub struct ReplayReport {
    pub schema_version:u32,
    pub status:EvidenceClass,
    pub market_ticker:String,
    pub decision_asof_received_ns:i64,
    pub catalog_sha256:String,
    pub selected_members:usize,
    pub partition_proofs:Vec<crate::PartitionProof>,
    pub declared_rows:usize,
    pub decoded_source_rows:usize,
    pub matching_observations:usize,
    pub accepted_observations:usize,
    pub snapshots_observed:usize,
    pub deltas_observed:usize,
    pub trades_observed:usize,
    pub first_applied_received_ns:Option<i64>,
    pub last_applied_received_ns:Option<i64>,
    pub source_clock_ahead_observations:usize,
    pub duplicate_observations:usize,
    pub conflicting_observations:usize,
    pub malformed_or_excluded_observations:usize,
    pub future_observations_excluded:usize,
    pub missing_snapshot_deltas:usize,
    pub invalid_depth_observations:usize,
    pub session_change_observations:usize,
    pub channel_runs:Vec<ChannelRun>,
    pub checkpoints:Vec<Checkpoint>,
    pub initial_state:String,
    pub missing_evidence:Vec<String>,
    pub decoded_hash_chain_sha256:String,
    pub independent_settlement_verified:bool,
    pub venue_execution_verified:bool,
}
#[derive(Debug)]
pub enum Error {
    InvalidRequest,Member(String),Source(crate::Error),
    DuplicateConflict(String),Malformed(String),Capacity,
}
impl From<crate::Error> for Error {
    fn from(e:crate::Error)->Self{Self::Source(e)}
}
fn level(q:Option<(phoenix_domain::Price,u64)>)->Option<FractionalLevel>{
    q.map(|(p,v)|FractionalLevel{price_ticks_1e4:p.ticks(),quantity_hundredths:v})
}
fn kind(m:&Message)->&'static str{
    match m{
        Message::Snapshot(_)=>"snapshot",
        Message::Delta(_)=>"delta",
        Message::Trade(_)=>"trade",
        Message::Unhandled(_)=>"unhandled",
    }
}
fn checkpoint(book:&forensic::Book,at:i64,last:&Option<CapturedMessage>)->Checkpoint{
    Checkpoint{
        asof_received_ns:at,
        last_applied_received_ns:last.as_ref().map(|x|x.received_ns),
        last_message_sha256:last.as_ref().map(|x|x.raw_sha256.clone()),
        connection_id:last.as_ref().map(|x|x.connection_id.clone()),
        sid:last.as_ref().and_then(|x|x.sid),
        last_sequence:last.as_ref().and_then(|x|x.seq),
        book_state:format!("{:?}",book.status()),
        best_yes_bid:level(book.best_bid_fp(Side::Yes)),
        best_yes_ask:level(book.best_ask_fp(Side::Yes)),
        best_no_bid:level(book.best_bid_fp(Side::No)),
        best_no_ask:level(book.best_ask_fp(Side::No)),
        // Even a valid top-of-book is not proof of authenticated source
        // initialization or executable market liquidity.
        execution_eligible:book.verified_executable(),
    }
}
fn missing_initial(status:Initialization)->bool{
    !matches!(status,Initialization::VerifiedInitialization|
        Initialization::VerifiedResnapshot)
}
fn stable_message_key(event:&CapturedMessage,decoded:&ArchivedDecoded)->String{
    if let (Some(sid),Some(seq))=(event.sid,event.seq){
        format!("{}|{sid}|{seq}|{}",event.connection_id,kind(&decoded.message))
    }else{
        // Cannot claim two unsequenced records were identical source events.
        format!("unsequenced|{}|{}",event.member,event.row_index)
    }
}
pub fn compare_projection(report:&ReplayReport,refs:&[ProjectionReference])
    ->Result<Vec<ProjectionComparison>,Error>{
    if refs.len()>MAX_CHECKPOINTS{return Err(Error::Capacity);}
    let mut out=Vec::new();
    for r in refs{
        let p=report.checkpoints.iter().find(|c|c.asof_received_ns==r.asof_received_ns);
        out.push(ProjectionComparison{
            asof_received_ns:r.asof_received_ns,
            fixture_sha256:r.fixture_sha256.clone(),
            matching_asof_checkpoint:p.is_some(),
            bids_equal:p.map(|p|p.best_yes_bid.as_ref()
                .map(|v|(v.price_ticks_1e4,v.quantity_hundredths))==
                r.yes_bid.as_ref().map(|v|(v.price_ticks_1e4,v.quantity_hundredths))),
            asks_equal:p.map(|p|p.best_yes_ask.as_ref()
                .map(|v|(v.price_ticks_1e4,v.quantity_hundredths))==
                r.yes_ask.as_ref().map(|v|(v.price_ticks_1e4,v.quantity_hundredths))),
            parity_is_historical_proof:false,
        });
    }
    Ok(out)
}
/// A bounded source-driven replay. This API intentionally reports observed
/// fractional book state but does not invoke any trading or settlement path.
/// Exact catalog bytes are caller supplied and SHA-256 included in the report.
pub fn run(req:&Request,catalog_bytes:&[u8],decision_ns:i64,
    checkpoint_asof_ns:&[i64])->Result<ReplayReport,Error>{
    if decision_ns<=0||decision_ns<req.start_received_ns||
        decision_ns>req.end_received_ns||checkpoint_asof_ns.len()>MAX_CHECKPOINTS||
        checkpoint_asof_ns.iter().any(|t|*t>decision_ns||*t<req.start_received_ns||
            *t<=0)||checkpoint_asof_ns.windows(2).any(|pair|pair[0]>=pair[1]){
        return Err(Error::InvalidRequest);
    }
    let catalog_sha256=format!("{:x}",Sha256::digest(catalog_bytes));
    let mut book=forensic::Book::new(&req.ticker);
    let mut chain=Sha256::new();
    let mut keyed=BTreeMap::<String,String>::new();
    let mut snapshots=0usize;
    let mut deltas=0usize;
    let mut trades=0usize;
    let mut duplicates=0usize;
    let mut conflicts=0usize;
    let mut excluded=0usize;
    let mut future=0usize;
    let mut missing_snap=0usize;
    let mut invalid_depth=0usize;
    let mut changed=0usize;
    let mut accepted=0usize;
    let mut clocks=0usize;
    let mut first=None;
    let mut last:Option<CapturedMessage>=None;
    let mut checkpoints=Vec::<Checkpoint>::new();
    let mut next=0usize;
    let evidence:Evidence=replay(req,catalog_bytes,|event,decoded|{
        if event.received_ns>decision_ns{
            future+=1;
            return Ok(());
        }
        while next<checkpoint_asof_ns.len()&&
            checkpoint_asof_ns[next]<event.received_ns{
            checkpoints.push(checkpoint(&book,checkpoint_asof_ns[next],&last));
            next+=1;
        }
        let key=stable_message_key(event,decoded);
        if let Some(previous)=keyed.get(&key){
            if previous!=&event.raw_sha256{
                conflicts+=1;
                return Err(crate::Error::InvalidSource);
            }
            duplicates+=1;return Ok(());
        }
        if keyed.len()>=MAX_IDENTITIES{return Err(crate::Error::TooManyRows);}
        keyed.insert(key,event.raw_sha256.clone());
        match &decoded.message{
            Message::Snapshot(_)=>snapshots+=1,
            Message::Delta(_)=>deltas+=1,
            Message::Trade(_)=>trades+=1,
            Message::Unhandled(_)=>{excluded+=1;return Ok(());},
        }
        clocks+=usize::from(event.source_clock_ahead);
        match book.apply(event,decoded){
            Ok(())=>{},
            Err(forensic::Error::MissingSnapshot)=>missing_snap+=1,
            Err(forensic::Error::SessionChanged)=>changed+=1,
            Err(forensic::Error::NegativeDepth|
                forensic::Error::InvalidSnapshot)=>{invalid_depth+=1;excluded+=1;},
            Err(_)=>excluded+=1,
        }
        chain.update((event.raw_sha256.len() as u64).to_le_bytes());
        chain.update(event.raw_sha256.as_bytes());
        first.get_or_insert(event.received_ns);
        last=Some(event.clone());
        accepted+=1;
        Ok(())
    })?;
    while next<checkpoint_asof_ns.len(){
        checkpoints.push(checkpoint(&book,checkpoint_asof_ns[next],&last));
        next+=1;
    }
    if checkpoints.last().is_none_or(|c|c.asof_received_ns!=decision_ns){
        checkpoints.push(checkpoint(&book,decision_ns,&last));
    }
    let declared_rows=evidence.partition_proofs.iter()
        .try_fold(0usize,|sum,p|sum.checked_add(p.rows)).ok_or(Error::Capacity)?;
    let mut missing_evidence=Vec::new();
    if missing_initial(book.status()){
        missing_evidence.push("UNVERIFIED_HISTORICAL_INITIAL_SNAPSHOT".into());
    }
    if evidence.channel_runs.iter().any(|c|
        !c.complete_subscription_history_verified){
        missing_evidence.push("UNVERIFIED_FULL_SUBSCRIPTION_CONTINUITY".into());
    }
    if invalid_depth>0{missing_evidence.push("INVALID_DEPTH_OBSERVED".into());}
    missing_evidence.push("NO_INDEPENDENT_SETTLEMENT_EVIDENCE".into());
    missing_evidence.push("NO_VERIFIED_EXCHANGE_EXECUTION".into());
    Ok(ReplayReport{
        schema_version:1,status:if invalid_depth>0{EvidenceClass::Invalid}
            else if missing_snap>0||missing_initial(book.status()){EvidenceClass::Incomplete}
            else{EvidenceClass::SourceDerivedProvisional},
        market_ticker:req.ticker.clone(),decision_asof_received_ns:decision_ns,
        catalog_sha256,selected_members:evidence.partition_proofs.len(),
        partition_proofs:evidence.partition_proofs,
        declared_rows,decoded_source_rows:evidence.scanned_rows,
        matching_observations:evidence.matching_events,
        accepted_observations:accepted,
        snapshots_observed:snapshots,deltas_observed:deltas,trades_observed:trades,
        first_applied_received_ns:first,
        last_applied_received_ns:last.as_ref().map(|e|e.received_ns),
        source_clock_ahead_observations:clocks,
        duplicate_observations:duplicates,conflicting_observations:conflicts,
        malformed_or_excluded_observations:excluded,
        future_observations_excluded:future,
        missing_snapshot_deltas:missing_snap,invalid_depth_observations:invalid_depth,
        session_change_observations:changed,channel_runs:evidence.channel_runs,
        checkpoints,initial_state:format!("{:?}",book.status()),missing_evidence,
        decoded_hash_chain_sha256:format!("{:x}",chain.finalize()),
        independent_settlement_verified:false,venue_execution_verified:false,
    })
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]fn comparison_cannot_promote_earlier_fixture_to_execution_proof(){
        let fixture=Checkpoint{
            asof_received_ns:100,last_applied_received_ns:Some(99),
            last_message_sha256:None,connection_id:None,sid:Some(1),
            last_sequence:Some(7),book_state:"PartialInitialization".into(),
            best_yes_bid:Some(FractionalLevel{
                price_ticks_1e4:4000,quantity_hundredths:125}),
            best_yes_ask:None,best_no_bid:None,best_no_ask:None,
            execution_eligible:false,
        };
        let report=ReplayReport{
            schema_version:1,status:EvidenceClass::Incomplete,
            market_ticker:"BTC".into(),decision_asof_received_ns:100,
            catalog_sha256:"a".repeat(64),selected_members:0,
            partition_proofs:vec![],declared_rows:0,decoded_source_rows:0,
            matching_observations:0,accepted_observations:0,
            snapshots_observed:0,deltas_observed:0,trades_observed:0,
            first_applied_received_ns:Some(99),last_applied_received_ns:Some(99),
            source_clock_ahead_observations:0,duplicate_observations:0,
            conflicting_observations:0,malformed_or_excluded_observations:0,
            future_observations_excluded:0,missing_snapshot_deltas:0,
            invalid_depth_observations:0,session_change_observations:0,
            channel_runs:vec![],checkpoints:vec![fixture],
            initial_state:"PartialInitialization".into(),missing_evidence:vec![],
            decoded_hash_chain_sha256:"b".repeat(64),
            independent_settlement_verified:false,venue_execution_verified:false,
        };
        let references=[ProjectionReference{
            asof_received_ns:100,fixture_sha256:"f".repeat(64),
            yes_bid:Some(FractionalLevel{
                price_ticks_1e4:4000,quantity_hundredths:125}),yes_ask:None
        }];
        let comparisons=compare_projection(&report,&references).unwrap();
        assert_eq!(comparisons[0].bids_equal,Some(true));
        assert!(!comparisons[0].parity_is_historical_proof);
        assert!(!report.checkpoints[0].execution_eligible);
    }
    #[test]fn no_future_checkpoint_can_be_requested(){
        let req=Request{ticker:"M".into(),start_received_ns:10,end_received_ns:20,
            named_members:vec!["archive/curated/raw_events/a.parquet".into()],
            zip_paths:vec![]};
        assert!(matches!(run(&req,b"",15,&[16]),Err(Error::InvalidRequest)));
        assert!(matches!(run(&req,b"",15,&[11,11]),Err(Error::InvalidRequest)));
    }
}

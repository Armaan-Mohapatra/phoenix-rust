//! Fail-closed source-aware historical L2 evidence overlay.
//! The existing forensic::Book applies all fractional deltas. This layer
//! deliberately does not certify a market-filtered sequence as full channel.
use crate::{CapturedMessage, ChannelRun, forensic};
use phoenix_domain::Side;
use phoenix_kalshi_wire::{ArchivedDecoded,Message};
use serde::{Serialize,Deserialize};

#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum BookClass{
    UnknownInitialization,MissingSnapshot,PartialInitialization,
    ObservedSnapshot,ObservedResnapshot,SequenceGap,ReconnectUnverified,
    InvalidBook,StaleBook,SourceClockUncertain,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum ChannelQuality{
    NotObserved,MarketFilteredOnly,ObservedWindowContinuous,
    ObservedWindowGaps,ObservedWindowDuplicatesOrRegressions,
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct BookEvidence {
    pub market_ticker:String,
    pub source_partition_count:usize,
    pub source_manifest_sha256:Option<String>,
    pub connection_id:Option<String>,
    pub sid:Option<u64>,
    pub snapshot_origin_sha256:Option<String>,
    pub first_valid_received_ns:Option<i64>,
    pub last_valid_received_ns:Option<i64>,
    pub last_source_sending_ns:Option<i64>,
    pub last_source_event_ns:Option<i64>,
    pub last_sequence:Option<u64>,
    pub snapshots_observed:usize,
    pub resnapshots_observed:usize,
    pub reconnects_without_resnapshot:usize,
    pub invalid_depth_events:usize,
    pub observed_full_channel_gaps:u64,
    pub duplicate_channel_messages:u64,
    pub regressing_channel_messages:u64,
    pub channel_quality:ChannelQuality,
    pub complete_subscription_history_verified:bool,
    pub source_clock_uncertain:bool,
    pub last_quote_age_ns:Option<i64>,
    pub fractional_depth_supported:bool,
    pub classification:BookClass,
    pub modeled_execution_eligible:bool,
    pub missing_evidence:Vec<String>,
}
pub struct BookIntegrity {
    market:String,
    book:forensic::Book,
    connection:Option<String>,
    sid:Option<u64>,
    snap_hash:Option<String>,
    first:Option<i64>,last:Option<i64>,
    sending:Option<i64>,event_ns:Option<i64>,seq:Option<u64>,
    snapshots:usize,resnapshots:usize,reconnects:usize,invalid:usize,
    clock_uncertain:bool,class:BookClass,
    // The one underlying forensic book is applied at most once per message.
    // Replay may inspect the outcome without maintaining a competing book.
    last_apply:Option<Result<(),forensic::Error>>,
}
impl BookIntegrity {
    pub fn new(ticker:&str)->Self{
        Self{
            market:ticker.to_owned(),book:forensic::Book::new(ticker),
            connection:None,sid:None,snap_hash:None,
            first:None,last:None,sending:None,event_ns:None,seq:None,
            snapshots:0,resnapshots:0,reconnects:0,invalid:0,
            clock_uncertain:false,class:BookClass::UnknownInitialization,
            last_apply:None,
        }
    }
    pub fn forensic_book(&self)->&forensic::Book{&self.book}
    pub fn last_apply_result(&self)->Option<&Result<(),forensic::Error>>{
        self.last_apply.as_ref()
    }
    pub fn best_bid_fp(&self,side:Side)->Option<(phoenix_domain::Price,u64)>{
        self.book.best_bid_fp(side)
    }
    pub fn best_ask_fp(&self,side:Side)->Option<(phoenix_domain::Price,u64)>{
        self.book.best_ask_fp(side)
    }
    /// Source event is already SHA-checked, typed and sorted by receipt time.
    /// Return a classification, not an order fill or verified executable book.
    pub fn observe(&mut self,e:&CapturedMessage,d:&ArchivedDecoded)->BookClass{
        self.last_apply=None;
        if e.ticker!=self.market{
            self.last_apply=Some(Err(forensic::Error::WrongMarket));
            self.class=BookClass::InvalidBook;return self.class;
        }
        if self.last.is_some_and(|last|e.received_ns<last){
            self.last_apply=Some(Err(forensic::Error::TimeReversal));
            self.class=BookClass::InvalidBook;return self.class;
        }
        match &d.message{
            Message::Snapshot(s)=>{
                if s.market!=self.market{
                    self.last_apply=Some(Err(forensic::Error::WrongMarket));
                    self.class=BookClass::InvalidBook;return self.class;
                }
                let is_resnapshot=self.snapshots>0;
                let applied=self.book.apply(e,d);
                self.last_apply=Some(applied.clone());
                match applied{
                    Ok(())=>{
                        self.snapshots+=1;
                        if is_resnapshot{self.resnapshots+=1;}
                        self.snap_hash=Some(e.raw_sha256.clone());
                        self.connection=Some(e.connection_id.clone());
                        self.sid=Some(s.sid);
                        self.first.get_or_insert(e.received_ns);
                        self.last=Some(e.received_ns);
                        self.seq=Some(s.sequence);
                        self.class=if is_resnapshot{
                            BookClass::ObservedResnapshot
                        }else{BookClass::PartialInitialization};
                    },
                    Err(_)=>{self.invalid+=1;self.class=BookClass::InvalidBook;}
                }
            },
            Message::Delta(x)=>{
                if self.connection.as_deref().is_some_and(|v|
                    v!=e.connection_id.as_str())||self.sid.is_some_and(|v|v!=x.sid){
                    self.reconnects+=1;
                    // Trigger the existing forensic engine's session reset;
                    // do not leave last session's visible bids behind.
                    let applied=self.book.apply(e,d);
                    self.last_apply=Some(applied);
                    self.class=BookClass::ReconnectUnverified;
                    self.connection=None;
                    self.sid=None;
                    self.snap_hash=None;
                }else{
                    let applied=self.book.apply(e,d);
                    self.last_apply=Some(applied.clone());
                    match applied{
                        Ok(())=>{
                            self.first.get_or_insert(e.received_ns);
                            self.last=Some(e.received_ns);
                            self.seq=Some(x.sequence);
                            if self.snap_hash.is_none(){
                                self.class=BookClass::MissingSnapshot;
                            }
                        },
                        Err(forensic::Error::MissingSnapshot)=>{
                            self.class=BookClass::MissingSnapshot;
                        },
                        Err(forensic::Error::SessionChanged)=>{
                            self.reconnects+=1;
                            self.class=BookClass::ReconnectUnverified;
                            self.snap_hash=None;
                        },
                        Err(_)=>{
                            self.invalid+=1;
                            self.class=BookClass::InvalidBook;
                        },
                    }
                }
            },
            Message::Trade(_)|Message::Unhandled(_)=>{},
        }
        self.clock_uncertain|=e.source_clock_ahead;
        self.sending=Some(e.raw_sending_ns);
        self.event_ns=e.raw_event_ns;
        if let (Some((y,_)),Some((n,_)))=(
            self.book.best_bid_fp(Side::Yes),
            self.book.best_bid_fp(Side::No)){
            // Complementary YES/NO bids cannot exceed $1 collectively.
            if u32::from(y.ticks())+u32::from(n.ticks())>10_000{
                self.invalid+=1;
                self.class=BookClass::InvalidBook;
            }
        }
        self.class
    }
    /// The source reader's summaries span ALL tickers in the bounded input,
    /// but only a selected window. They cannot certify a whole subscription.
    pub fn evidence(&self,runs:&[ChannelRun],asof_ns:i64,
        max_quote_age_ns:i64,source_partition_count:usize,
        source_manifest_sha256:Option<&str>)->BookEvidence{
        let mut gaps=0u64;let mut duplicates=0u64;let mut regressions=0u64;
        let mut any=false;
        for r in runs{
            if self.connection.as_deref()==Some(r.connection_id.as_str())&&
                self.sid==Some(r.sid){
                any=true;
                gaps=gaps.saturating_add(r.missing_in_selected_window);
                duplicates=duplicates.saturating_add(r.duplicates);
                regressions=regressions.saturating_add(r.regressions);
            }
        }
        let quality=if !any{ChannelQuality::NotObserved}
            else if gaps>0{ChannelQuality::ObservedWindowGaps}
            else if duplicates>0||regressions>0{
                ChannelQuality::ObservedWindowDuplicatesOrRegressions
            }else{ChannelQuality::ObservedWindowContinuous};
        let age=self.last.and_then(|ns|asof_ns.checked_sub(ns));
        let mut class=self.class;
        if gaps>0{class=BookClass::SequenceGap;}
        else if matches!(class,BookClass::PartialInitialization|
            BookClass::ObservedResnapshot) && self.clock_uncertain{
            class=BookClass::SourceClockUncertain;
        }
        if age.is_some_and(|x|x<0||x>max_quote_age_ns){
            class=BookClass::StaleBook;
        }
        if self.invalid>0{class=BookClass::InvalidBook;}
        let mut missing=Vec::new();
        if self.snap_hash.is_none(){missing.push("NO_VERIFIED_STARTING_SNAPSHOT".into());}
        missing.push("COMPLETE_SUBSCRIPTION_HISTORY_NOT_PROVEN".into());
        if gaps>0{missing.push("OBSERVED_FULL_CHANNEL_SEQUENCE_GAPS".into());}
        if self.clock_uncertain{missing.push("SOURCE_CLOCK_SKEW_UNCERTAIN".into());}
        if self.reconnects>0{missing.push("RECONNECT_REQUIRES_AUTHENTICATED_RESNAPSHOT".into());}
        if self.invalid>0{missing.push("INVALID_BOOK_EVENTS".into());}
        if age.is_none(){missing.push("NO_VALID_QUOTE_RECEIPT_TIMESTAMP".into());}
        BookEvidence{
            market_ticker:self.market.clone(),
            source_partition_count,
            source_manifest_sha256:source_manifest_sha256.map(str::to_owned),
            connection_id:self.connection.clone(),sid:self.sid,
            snapshot_origin_sha256:self.snap_hash.clone(),
            first_valid_received_ns:self.first,last_valid_received_ns:self.last,
            last_source_sending_ns:self.sending,
            last_source_event_ns:self.event_ns,last_sequence:self.seq,
            snapshots_observed:self.snapshots,resnapshots_observed:self.resnapshots,
            reconnects_without_resnapshot:self.reconnects,
            invalid_depth_events:self.invalid,observed_full_channel_gaps:gaps,
            duplicate_channel_messages:duplicates,
            regressing_channel_messages:regressions,channel_quality:quality,
            complete_subscription_history_verified:false,
            source_clock_uncertain:self.clock_uncertain,last_quote_age_ns:age,
            fractional_depth_supported:true,classification:class,
            modeled_execution_eligible:false,missing_evidence:missing,
        }
    }
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct Candidate {
    pub market:String,pub source_manifest_sha256:String,
    pub is_finalized:bool,pub snapshot_count:u32,
    pub first_received_ns:i64,pub last_received_ns:i64,
    pub gap_count:u64,pub has_start_snapshot:bool,
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct CoverageCandidate{
    pub market:String,pub source_manifest_sha256:String,
    pub observable_snapshot_quality:u8,
    pub finalized:bool,
    pub independently_certified:bool,
}
/// Bounded, manifest-derived ranking only: no raw archive or network scan.
pub fn rank_snapshot_candidates(candidates:&[Candidate])
    ->Result<Vec<CoverageCandidate>,&'static str>{
    if candidates.len()>256{return Err("TOO_MANY_CANDIDATES");}
    let mut out=Vec::new();
    for c in candidates{
        if c.market.is_empty()||c.source_manifest_sha256.len()!=64||
            !c.source_manifest_sha256.bytes().all(|b|b.is_ascii_hexdigit())||
            c.first_received_ns>c.last_received_ns{
            return Err("INVALID_CATALOG_CANDIDATE");
        }
        let quality=u8::from(c.snapshot_count>0)+
            u8::from(c.has_start_snapshot)+u8::from(c.gap_count==0);
        out.push(CoverageCandidate{market:c.market.clone(),
            source_manifest_sha256:c.source_manifest_sha256.clone(),
            observable_snapshot_quality:quality,finalized:c.is_finalized,
            independently_certified:false,
        });
    }
    out.sort_by(|a,b|b.finalized.cmp(&a.finalized)
        .then(b.observable_snapshot_quality.cmp(&a.observable_snapshot_quality))
        .then(a.market.cmp(&b.market)));
    Ok(out)
}
#[cfg(test)]
mod tests{
    use super::*;
    use crate::CapturedMessage;
    use phoenix_kalshi_wire::decode_archived;
    fn row(raw:&str,at:i64,sid:u64,seq:u64,conn:&str)->(CapturedMessage,ArchivedDecoded){
        let e=CapturedMessage{member:"part.parquet".into(),row_index:0,
            ticker:"M".into(),received_ns:at,
            raw_sending_ns:at-100_000_000,raw_event_ns:Some(at-100_000_000),
            source_clock_ahead:false,connection_id:conn.into(),
            sid:Some(sid),seq:Some(seq),raw_sha256:"a".repeat(64),
            raw_message:raw.into()};
        let d=decode_archived(raw,at).unwrap();
        (e,d)
    }
    #[test]fn missing_snapshot_never_becomes_executable(){
        let raw=r#"{"type":"orderbook_delta","sid":1,"seq":1,"sending_ts_ms":1000,
          "msg":{"market_ticker":"M","clock":1,"side":"yes",
          "price_dollars":"0.4000","delta_fp":"0.25"}}"#;
        let (e,d)=row(raw,1_100_000_000,1,1,"conn");
        let mut book=BookIntegrity::new("M");
        assert_eq!(book.observe(&e,&d),BookClass::MissingSnapshot);
        let state=book.evidence(&[],1_100_000_000,1_000_000_000,1,None);
        assert!(!state.modeled_execution_eligible);
        assert!(!state.complete_subscription_history_verified);
    }
    #[test]fn shared_channel_gap_is_not_inferred_from_single_market_sequence(){
        let raw=r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"sending_ts_ms":1000,
          "msg":{"market_ticker":"M","clock":1,
          "yes_dollars_fp":[["0.4000","1.25"]],
          "no_dollars_fp":[["0.5000","1.00"]]}}"#;
        let (e,d)=row(raw,1_100_000_000,1,1,"conn");
        let mut book=BookIntegrity::new("M");
        book.observe(&e,&d);
        let runs=[ChannelRun{connection_id:"conn".into(),sid:1,channel:"orderbook_delta".into(),
            observed_messages:7,first_received_ns:1,last_received_ns:2,
            first_seq:1,last_seq:9,missing_in_selected_window:2,
            duplicates:0,regressions:0,complete_subscription_history_verified:false}];
        let state=book.evidence(&runs,1_100_000_000,1_000_000_000,1,None);
        assert_eq!(state.classification,BookClass::SequenceGap);
        assert!(!state.modeled_execution_eligible);
    }
    #[test]fn fractional_l2_updates_remain_exact_without_rounding(){
        let snapshot=r#"{"type":"orderbook_snapshot","sid":1,"seq":1,
          "sending_ts_ms":1000,"msg":{"market_ticker":"M","clock":1,
          "yes_dollars_fp":[["0.4000","1.25"]],
          "no_dollars_fp":[["0.5000","1.00"]]}}"#;
        let delta=r#"{"type":"orderbook_delta","sid":1,"seq":2,
          "sending_ts_ms":1100,"msg":{"market_ticker":"M","clock":2,
          "side":"yes","price_dollars":"0.4000","delta_fp":"-0.25"}}"#;
        let (a,b)=row(snapshot,1_100_000_000,1,1,"conn");
        let (c,d)=row(delta,1_200_000_000,1,2,"conn");
        let mut state=BookIntegrity::new("M");
        state.observe(&a,&b);state.observe(&c,&d);
        let top=state.best_bid_fp(Side::Yes).unwrap();
        assert_eq!(top.1,100);
        assert_eq!(top.0.ticks(),4000);
        assert!(!state.evidence(&[],1_200_000_000,100_000_000,1,None)
            .modeled_execution_eligible);
    }
    #[test]fn new_connection_needs_snapshot_to_resume_books(){
        let snapshot=r#"{"type":"orderbook_snapshot","sid":1,"seq":1,
          "sending_ts_ms":1000,"msg":{"market_ticker":"M","clock":1,
          "yes_dollars_fp":[["0.4000","1.25"]],
          "no_dollars_fp":[["0.5000","1.00"]]}}"#;
        let delta=r#"{"type":"orderbook_delta","sid":2,"seq":3,
          "sending_ts_ms":1100,"msg":{"market_ticker":"M","clock":2,
          "side":"yes","price_dollars":"0.4000","delta_fp":"0.25"}}"#;
        let (a,b)=row(snapshot,1_100_000_000,1,1,"connA");
        let (c,d)=row(delta,1_200_000_000,2,3,"connB");
        let mut state=BookIntegrity::new("M");
        state.observe(&a,&b);
        assert_eq!(state.observe(&c,&d),BookClass::ReconnectUnverified);
        assert_eq!(state.best_bid_fp(Side::Yes),None);
        assert_eq!(state.evidence(&[],1_200_000_000,100_000_000,1,None)
            .reconnects_without_resnapshot,1);
    }
    #[test]fn crossed_complementary_snapshot_is_invalid(){
        let snapshot=r#"{"type":"orderbook_snapshot","sid":1,"seq":1,
          "sending_ts_ms":1000,"msg":{"market_ticker":"M","clock":1,
          "yes_dollars_fp":[["0.6000","1.00"]],
          "no_dollars_fp":[["0.5000","1.00"]]}}"#;
        let (a,b)=row(snapshot,1_100_000_000,1,1,"A");
        let mut state=BookIntegrity::new("M");
        assert_eq!(state.observe(&a,&b),BookClass::InvalidBook);
    }
    #[test]fn old_quotes_are_stale_not_executable(){
        let snapshot=r#"{"type":"orderbook_snapshot","sid":1,"seq":1,
          "sending_ts_ms":1000,"msg":{"market_ticker":"M","clock":1,
          "yes_dollars_fp":[["0.4000","1.00"]],
          "no_dollars_fp":[["0.5000","1.00"]]}}"#;
        let (a,b)=row(snapshot,1_100_000_000,1,1,"A");
        let mut state=BookIntegrity::new("M");state.observe(&a,&b);
        let result=state.evidence(&[],3_000_000_000,100_000_000,1,None);
        assert_eq!(result.classification,BookClass::StaleBook);
        assert!(!result.modeled_execution_eligible);
    }
    #[test]fn duplicate_shared_channel_messages_remain_partial(){
        let snapshot=r#"{"type":"orderbook_snapshot","sid":1,"seq":1,
          "sending_ts_ms":1000,"msg":{"market_ticker":"M","clock":1,
          "yes_dollars_fp":[["0.4000","1.00"]],
          "no_dollars_fp":[["0.5000","1.00"]]}}"#;
        let (a,b)=row(snapshot,1_100_000_000,1,1,"conn");
        let mut state=BookIntegrity::new("M");state.observe(&a,&b);
        let run=[ChannelRun{connection_id:"conn".into(),sid:1,
            channel:"orderbook_delta".into(),observed_messages:2,
            first_received_ns:1,last_received_ns:2,first_seq:1,
            last_seq:1,missing_in_selected_window:0,duplicates:1,
            regressions:0,complete_subscription_history_verified:false}];
        let result=state.evidence(&run,1_100_000_000,1_000_000_000,1,None);
        assert_eq!(result.channel_quality,
            ChannelQuality::ObservedWindowDuplicatesOrRegressions);
        assert!(!result.complete_subscription_history_verified);
    }
    #[test]fn book_integrity_exposes_exactly_one_forensic_application_result(){
        let snapshot=r#"{"type":"orderbook_snapshot","sid":1,"seq":1,
            "sending_ts_ms":1000,"msg":{"market_ticker":"M","clock":1,
            "yes_dollars_fp":[["0.4000","1.00"]],
            "no_dollars_fp":[["0.5000","1.00"]]}}"#;
        let (e,d)=row(snapshot,1_100_000_000,1,1,"conn");
        let mut integrity=BookIntegrity::new("M");
        integrity.observe(&e,&d);
        assert_eq!(integrity.last_apply_result(),Some(&Ok(())));
        assert_eq!(integrity.forensic_book().best_bid_fp(Side::Yes),
            integrity.best_bid_fp(Side::Yes));
        // A second application would produce DuplicateSequence for deltas;
        // replay consumes the recorded outcome instead of applying twice.
    }
    #[test]fn manifest_rankings_do_not_claim_certified_availability(){
        let data=[Candidate{market:"M".into(),
            source_manifest_sha256:"a".repeat(64),is_finalized:true,
            snapshot_count:2,first_received_ns:1,last_received_ns:2,
            gap_count:0,has_start_snapshot:true}];
        let list=rank_snapshot_candidates(&data).unwrap();
        assert_eq!(list[0].observable_snapshot_quality,3);
        assert!(!list[0].independently_certified);
    }
}

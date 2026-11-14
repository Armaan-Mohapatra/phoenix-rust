//! Explicitly configured read-only / paper-only Phoenix orchestration.
//! No background collector launcher, trading connector or order API.
//! All methods are inert until called by an approved operator.
pub mod paper_pipeline;
use phoenix_kalshi_read_only::stream::{
    ReadOnlyStream,StreamConfig,WireObservation,Error as StreamError,Health};
use phoenix_historical_input::perps::{
    PerpsObservation,PerpsSchema,PartitionContract,convert_bounded_partition};
use phoenix_historical_input::ColumnMapping;
use phoenix_durable_archive::{Archive,EventRow,SourceScope};
use phoenix_parquet_batch::Row;
use serde::{Serialize,Deserialize};
use sha2::{Digest,Sha256};
use std::path::Path;

fn sha(s:&[u8])->String{format!("{:x}",Sha256::digest(s))}
#[derive(Debug,Clone,Copy,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum Mode{SyntheticFixture,FinalizedHistory,IsolatedReadOnly,PaperOnly}
#[derive(Debug,Clone)]
pub struct Config{
    pub mode:Mode,pub isolated_network_enabled:bool,
    pub max_queue:usize,pub max_ram_bytes:u64,
    pub max_research_disk_bytes:u64,pub max_retries:u8,
    pub base_backoff_ns:i64,pub max_age_ns:i64,
    pub expected_research_output_root:Option<String>,
}
impl Default for Config{
    fn default()->Self{
        Self{mode:Mode::SyntheticFixture,isolated_network_enabled:false,
            max_queue:2048,max_ram_bytes:128*1024*1024,
            max_research_disk_bytes:512*1024*1024,
            max_retries:3,base_backoff_ns:1_000_000_000,
            max_age_ns:5_000_000_000,
            expected_research_output_root:None}
    }
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum Error{InvalidConfig,UnsafeOutputRoot,OfflineOnly,
    DisabledTransport,NoLiveRoutes,Source(StreamError),
    PerpsSchema,SourceClockUncertain,Archive(String),Backpressure,
    RetryExhausted,Stopped,InvalidCheckpoint,FutureData}
impl From<StreamError> for Error{fn from(e:StreamError)->Self{Self::Source(e)}}
fn safe_research_root(path:&str)->bool{
    let p=path.replace('/',"\\").to_ascii_lowercase();
    let components=p.split('\\').collect::<Vec<_>>();
    if p.is_empty()||p.len()>512||p.contains("..")||
        components.iter().any(|s|s.is_empty()&&*s!=components[0])||
        !p.contains("research")||
        p.contains("tradingnextdata")||p.contains("kalshidata")||
        p.contains("kalshiruntime")||p.contains("paper15m_runtime")||
        p.contains("windows\\system32")||p.contains("collector"){
        return false;
    }
    // Reject relative or UNC paths by default. A dedicated drive-rooted
    // PhoenixResearch output is required for subsequent archive validation.
    p.len()>=4 && p.as_bytes()[1]==b':' && p.as_bytes()[2]==b'\\'
}
impl Config{
    pub fn validate(&self)->Result<(),Error>{
        if self.max_queue==0||self.max_queue>4096||
            self.max_ram_bytes<32*1024*1024||
            self.max_ram_bytes>512*1024*1024||
            self.max_research_disk_bytes<64*1024*1024||
            self.max_research_disk_bytes>4*1024*1024*1024||
            self.max_retries>5||self.base_backoff_ns<=0||
            self.base_backoff_ns>60_000_000_000||self.max_age_ns<=0{
            return Err(Error::InvalidConfig);
        }
        if self.mode==Mode::IsolatedReadOnly&&
            !self.isolated_network_enabled{return Err(Error::DisabledTransport);}
        if self.isolated_network_enabled&&self.mode!=Mode::IsolatedReadOnly{
            return Err(Error::InvalidConfig);
        }
        if self.expected_research_output_root.as_ref()
            .is_some_and(|s|!safe_research_root(s)){
            return Err(Error::UnsafeOutputRoot);
        }
        Ok(())
    }
}
#[derive(Debug,Clone)]
pub struct RawReadOnlyEnvelope{
    pub connection_id:String,pub received_ns:i64,
    pub observed_now_ns:i64,pub raw:String,
}
/// Contains ONLY read/poll ability, no POST, orders, account actions,
/// exchange credentials or implicit timers.
pub trait ReadOnlyTransport{
    fn poll_raw(&mut self)->Result<Option<RawReadOnlyEnvelope>,String>;
}
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct Checkpoint{
    pub schema_version:u32,pub mode:Mode,
    pub accepted_events:u64,pub rejected_events:u64,
    pub restart_requires_snapshot:bool,
    pub serialized_config_sha256:String,
    pub input_manifest_sha256:Option<String>,
    pub publisher_active:bool,pub exchange_order_available:bool,
}
pub struct Orchestrator{
    config:Config,kalshi:ReadOnlyStream,
    accepted:u64,rejected:u64,failures:u8,
    next_retry_after_ns:Option<i64>,stopped:bool,
}
impl Orchestrator{
    pub fn new(config:Config)->Result<Self,Error>{
        config.validate()?;
        let stream=ReadOnlyStream::new(StreamConfig{
            max_queue:config.max_queue,max_age_ns:config.max_age_ns,
            max_raw_bytes:256*1024,max_gap:0,reject_on_overflow:true})?;
        Ok(Self{config,kalshi:stream,accepted:0,rejected:0,
            failures:0,next_retry_after_ns:None,stopped:false})
    }
    pub fn config(&self)->&Config{&self.config}
    pub fn health(&self)->&Health{self.kalshi.health()}
    pub fn pending(&self)->usize{self.kalshi.pending()}
    pub fn next_observation(&mut self)->Option<WireObservation>{
        self.kalshi.pop()
    }
    pub fn stop(&mut self){self.stopped=true;self.kalshi.stop();}
    pub fn observe_disconnect(&mut self,connection:&str)->Result<(),Error>{
        if self.stopped{return Err(Error::Stopped);}
        self.kalshi.observe_reconnect(connection).map_err(Error::Source)
    }
    pub fn ingest_raw(&mut self,e:&RawReadOnlyEnvelope)
        ->Result<WireObservation,Error>{
        if self.stopped{return Err(Error::Stopped);}
        let observed=self.kalshi.ingest(&e.connection_id,&e.raw,
            e.received_ns,e.observed_now_ns);
        match observed{
            Ok(value)=>{self.accepted+=1;Ok(value)},
            Err(StreamError::QueueFull)=>{
                self.rejected+=1;Err(Error::Backpressure)},
            Err(err)=>{self.rejected+=1;Err(Error::Source(err))}
        }
    }
    /// Optional caller-poll for the explicitly authorized read-only mode.
    /// Never invoked automatically by constructing the orchestrator.
    pub fn poll_once<T:ReadOnlyTransport>(&mut self,t:&mut T,
        observed_now_ns:i64)->Result<Option<WireObservation>,Error>{
        if self.stopped{return Err(Error::Stopped);}
        if self.config.mode!=Mode::IsolatedReadOnly||
            !self.config.isolated_network_enabled{
            return Err(Error::DisabledTransport);
        }
        if self.next_retry_after_ns.is_some_and(|at|observed_now_ns<at){
            return Err(Error::DisabledTransport);
        }
        match t.poll_raw(){
            Ok(Some(raw))=>{
                if raw.observed_now_ns>observed_now_ns{
                    return Err(Error::FutureData);
                }
                self.failures=0;self.next_retry_after_ns=None;
                self.ingest_raw(&raw).map(Some)
            },
            Ok(None)=>Ok(None),
            Err(_)=>{
                self.failures=self.failures.saturating_add(1);
                if self.failures>self.config.max_retries{
                    return Err(Error::RetryExhausted);
                }
                let factor=1i64.checked_shl(u32::from(self.failures-1))
                    .ok_or(Error::InvalidConfig)?;
                let delay=self.config.base_backoff_ns
                    .checked_mul(factor).ok_or(Error::InvalidConfig)?;
                self.next_retry_after_ns=observed_now_ns.checked_add(delay);
                Ok(None)
            }
        }
    }
    /// Perps import from finalized, byte/SHA verified Parquet only. The
    /// source schema still must be independently verified for the venue.
    pub fn perps_finalized(&self,parquet:&[u8],columns:&ColumnMapping<'_>,
        schema:&PerpsSchema,source:&PartitionContract)
        ->Result<Vec<PerpsObservation>,Error>{
        if self.stopped{return Err(Error::Stopped);}
        convert_bounded_partition(parquet,columns,schema,source)
            .map_err(|_|Error::PerpsSchema)
    }
    pub fn checkpoint(&self)->Result<Checkpoint,Error>{
        let settings=format!("{:?}|{:?}|{}|{}|{}|{}|{}|{}",
            self.config.mode,self.config.isolated_network_enabled,
            self.config.max_queue,self.config.max_ram_bytes,
            self.config.max_research_disk_bytes,self.config.max_retries,
            self.config.base_backoff_ns,self.config.max_age_ns);
        Ok(Checkpoint{schema_version:1,mode:self.config.mode,
            accepted_events:self.accepted,rejected_events:self.rejected,
            restart_requires_snapshot:true,
            serialized_config_sha256:sha(settings.as_bytes()),
            input_manifest_sha256:None,publisher_active:false,
            exchange_order_available:false})
    }
    /// Idempotent recovery requires replaying and independently checking
    /// the source manifests; this checkpoint NEVER resumes a live socket.
    pub fn verify_restart(&self,c:&Checkpoint)->Result<(),Error>{
        if c.schema_version!=1||c.mode!=self.config.mode||
            !c.restart_requires_snapshot||c.exchange_order_available||
            c.publisher_active||
            c.serialized_config_sha256!=self.checkpoint()?.serialized_config_sha256{
            return Err(Error::InvalidCheckpoint);
        }
        Ok(())
    }
}
/// Strict caller budget with an independently checked starting usage from
/// an existing research-only manifest. It never discovers/recurses into
/// source roots or silently assumes empty disk usage.
#[derive(Debug,Clone)]
pub struct DiskBudget{
    pub limit_bytes:u64,pub confirmed_used_bytes:u64,
    pub usage_manifest_sha256:String,
}
impl DiskBudget{
    pub fn validate(&self)->Result<(),Error>{
        if self.limit_bytes==0||self.limit_bytes>4*1024*1024*1024||
            self.confirmed_used_bytes>self.limit_bytes||
            self.usage_manifest_sha256.len()!=64||
            !self.usage_manifest_sha256.bytes().all(|x|x.is_ascii_hexdigit()){
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }
    pub fn reserve(&self,needed:u64)->Result<(),Error>{
        self.validate()?;
        if self.confirmed_used_bytes.checked_add(needed)
            .map_or(true,|v|v>self.limit_bytes){
            return Err(Error::Backpressure);
        }
        Ok(())
    }
}
/// Research writer: invoked only by explicit operator call and only with
/// verified original wire bytes. The durable archive validates dedicated
/// research-root marker, atomic commit and on-disk integrity.
pub fn publish_read_only_observation(root:&str,
    e:&RawReadOnlyEnvelope,o:&WireObservation,
    budget:&mut DiskBudget)->Result<(),Error>{
    if !safe_research_root(root){return Err(Error::UnsafeOutputRoot);}
    if o.connection_id!=e.connection_id||
        o.received_ns!=e.received_ns||
        o.message_sha256!=sha(e.raw.as_bytes())||
        o.source_clock_uncertain{
        return Err(Error::SourceClockUncertain);
    }
    let exchange_ns=o.raw_event_ns.unwrap_or(o.raw_sending_ns);
    if exchange_ns<=0||exchange_ns>o.received_ns{
        return Err(Error::SourceClockUncertain);
    }
    let scope=SourceScope{feed:"kalshi_public_readonly".into(),
        session:o.connection_id.clone(),market:o.market.clone(),
        observed_sequence_gaps:0,
        quality_flags:vec!["SOURCE_PROVISIONAL_NOT_VENUE_FILL".into()],
        source_manifest_sha256:None};
    let event=EventRow{event_id:format!("{}:{}:{}",
        o.connection_id,o.subscription_sid,o.sequence),
        data:Row{feed:"kalshi_public_readonly".into(),
            session:o.connection_id.clone(),market:o.market.clone(),
            exchange_ns,received_ns:o.received_ns,sequence:o.sequence,
            kind:40,payload:e.raw.as_bytes().to_vec()}};
    // Source events are capped at 256KiB, and the bounded buffer checks
    // projected bytes plus conservative metadata overhead. The actual
    // archive format may consume more: caller must reconcile against
    // committed manifests before subsequent publication.
    let projected=(e.raw.len() as u64).checked_add(32*1024)
        .ok_or(Error::Backpressure)?;
    budget.reserve(projected)?;
    let archive=Archive::open(Path::new(root))
        .map_err(|x|Error::Archive(format!("{x:?}")))?;
    let published=archive.publish(scope,&[event])
        .map_err(|x|Error::Archive(format!("{x:?}")))?;
    if published.added!=1{return Err(Error::Archive("DUPLICATE_OR_NO_CHANGE".into()));}
    budget.confirmed_used_bytes=budget.confirmed_used_bytes
        .checked_add(projected).ok_or(Error::Backpressure)?;
    Ok(())
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]fn default_is_offline_and_research_root_protection_is_strict(){
        let c=Config::default();
        assert_eq!(c.mode,Mode::SyntheticFixture);
        assert!(!c.isolated_network_enabled);
        assert!(c.validate().is_ok());
        assert!(!safe_research_root("E:\\TradingNextData\\paper"));
        assert!(!safe_research_root("E:\\KalshiData\\paper15m_runtime"));
        assert!(!safe_research_root("C:\\Windows\\System32"));
        assert!(safe_research_root("E:\\PhoenixResearch\\offline"));
    }
    #[test]fn mock_polling_never_activates_without_explicit_mode(){
        struct Mock;
        impl ReadOnlyTransport for Mock{
            fn poll_raw(&mut self)->Result<Option<RawReadOnlyEnvelope>,String>{
                panic!("disabled polling must not invoke the transport");
            }
        }
        let mut o=Orchestrator::new(Config::default()).unwrap();
        assert!(matches!(o.poll_once(&mut Mock,100),Err(Error::DisabledTransport)));
        assert!(!o.health().exchange_order_route_available);
    }
    #[test]fn disk_budget_requires_prior_verified_usage_and_fails_closed(){
        let mut budget=DiskBudget{limit_bytes:1024*1024,
            confirmed_used_bytes:1024*1024,
            usage_manifest_sha256:"a".repeat(64)};
        assert_eq!(budget.reserve(1),Err(Error::Backpressure));
        budget.confirmed_used_bytes=0;
        assert!(budget.reserve(512).is_ok());
        budget.usage_manifest_sha256.clear();
        assert!(budget.reserve(512).is_err());
    }
    #[test]fn checkpoint_recovery_requires_fresh_snapshot(){
        let mut o=Orchestrator::new(Config::default()).unwrap();
        let c=o.checkpoint().unwrap();
        assert!(o.verify_restart(&c).is_ok());
        let mut forged=c;forged.exchange_order_available=true;
        assert!(o.verify_restart(&forged).is_err());
        o.stop();
        assert!(matches!(o.ingest_raw(&RawReadOnlyEnvelope{
            connection_id:"s".into(),received_ns:1,
            observed_now_ns:1,raw:"{}".into()}),Err(Error::Stopped)));
    }
}

//! Public Kalshi market-data GET adapter. No authentication, WebSocket,
//! order APIs, POST/PUT/DELETE, portfolio data, or trading functionality.
//! Network is NEVER accessed by unit tests.
use std::{io::Read,time::{Duration,Instant},sync::{Arc,Mutex}};
use sha2::{Digest,Sha256};

pub const BASE:&str="https://external-api.kalshi.com/trade-api/v2";
pub const MAX_RESPONSE_BYTES:u64=2_000_000;
#[derive(Debug)]
pub enum Error {InvalidTicker,InvalidLimit,InvalidCursor,Http(reqwest::Error),
    Io(std::io::Error),BadStatus(u16),ResponseTooLarge,Json(serde_json::Error),
    RateLimited,InvalidFinal}
impl From<reqwest::Error> for Error {fn from(x:reqwest::Error)->Self{Self::Http(x)}}
impl From<std::io::Error> for Error {fn from(x:std::io::Error)->Self{Self::Io(x)}}
impl From<serde_json::Error> for Error {fn from(x:serde_json::Error)->Self{Self::Json(x)}}

fn good_ticker(t:&str)->bool {
    !t.is_empty()&&t.len()<=128&&t.bytes().all(|b|
        b.is_ascii_uppercase()||b.is_ascii_digit()||b==b'-'||b==b'_')
}
#[derive(Clone,Debug)]
pub struct ReadOnlyKalshi {
    client:reqwest::blocking::Client,
    minimum_interval:Duration,
    last_request:Arc<Mutex<Option<Instant>>>,
}
impl ReadOnlyKalshi {
    pub fn new()->Result<Self,Error>{Self::with_minimum_interval(Duration::from_secs(1))}
    /// A shared client-level lower-bound on request spacing; never sleeps
    /// or retries internally. The user explicitly invokes each historical GET.
    pub fn with_minimum_interval(minimum_interval:Duration)->Result<Self,Error>{
        let client=reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("PhoenixRustOfflineResearch/0.1 (read-only)")
            .build()?;
        Ok(Self{client,minimum_interval,
            last_request:Arc::new(Mutex::new(None))})
    }
    fn build_url(path:&str,params:&[(&str,&str)]) -> reqwest::Url {
        let mut url=reqwest::Url::parse(BASE).expect("fixed reviewed Kalshi HTTPS URL");
        url.set_path(&format!("/trade-api/v2/{path}"));
        if !params.is_empty() {
            url.query_pairs_mut().extend_pairs(params);
        }
        url
    }
    fn get(&self,url:reqwest::Url)->Result<serde_json::Value,Error> {
        let mut lock=self.last_request.lock().map_err(|_|Error::RateLimited)?;
        if lock.as_ref().is_some_and(|previous|
            previous.elapsed()<self.minimum_interval){return Err(Error::RateLimited);}
        *lock=Some(Instant::now());drop(lock);
        let response=self.client.get(url).header("Accept","application/json").send()?;
        let status=response.status();
        if !status.is_success(){return Err(Error::BadStatus(status.as_u16()));}
        let mut limited=response.take(MAX_RESPONSE_BYTES+1);
        let mut bytes=Vec::new();
        limited.read_to_end(&mut bytes)?;
        if bytes.len() as u64>MAX_RESPONSE_BYTES{return Err(Error::ResponseTooLarge);}
        Ok(serde_json::from_slice(&bytes)?)
    }
    /// Max one page (1..=200) and explicit cursor. A separate bounded
    /// caller controls pagination and rate limits.
    pub fn list_markets(&self,limit:u16,cursor:Option<&str>)
        ->Result<serde_json::Value,Error> {
        if !(1..=200).contains(&limit){return Err(Error::InvalidLimit);}
        let limit_text=limit.to_string();
        let mut query=vec![("limit",limit_text.as_str())];
        if let Some(c)=cursor {
            if c.len()>1024||c.bytes().any(|b|b.is_ascii_control()){
                return Err(Error::InvalidCursor);
            }
            if !c.is_empty(){query.push(("cursor",c));}
        }
        self.get(Self::build_url("markets",&query))
    }
    pub fn market(&self,ticker:&str)->Result<serde_json::Value,Error>{
        if !good_ticker(ticker){return Err(Error::InvalidTicker);}
        self.get(Self::build_url(&format!("markets/{ticker}"),&[]))
    }
    /// Documented public archive tier for markets beyond the historical cutoff.
    /// No fallback to a private or third-party settlement source.
    pub fn historical_market(&self,ticker:&str)->Result<serde_json::Value,Error>{
        if !good_ticker(ticker){return Err(Error::InvalidTicker);}
        self.get(Self::build_url(&format!("historical/markets/{ticker}"),&[]))
    }
    pub fn orderbook(&self,ticker:&str,depth:Option<u8>)
        ->Result<serde_json::Value,Error>{
        if !good_ticker(ticker){return Err(Error::InvalidTicker);}
        if depth.is_some_and(|d|d==0){return Err(Error::InvalidLimit);}
        let depth_text=depth.map(|d|d.to_string());
        let params=depth_text.as_deref().map(|s|vec![("depth",s)]).unwrap_or_default();
        self.get(Self::build_url(&format!("markets/{ticker}/orderbook"),&params))
    }
    /// Exactly one manually invoked read-only official historical market GET.
    /// A finalized record requires the exact expected ticker and documented
    /// binary settlement semantics. market_id is optional in the official schema.
    pub fn official_result(&self,ticker:&str)
        ->Result<OfficialMarketEvidence,Error>{
        let (value,origin)=match self.market(ticker){
            Ok(value)=>(value,"kalshi-public-market-get"),
            Err(Error::BadStatus(404))=>(self.historical_market(ticker)?,
                "kalshi-public-historical-market-get"),
            Err(err)=>return Err(err),
        };
        let observed_ns=std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_|Error::InvalidFinal)?.as_nanos();
        let retrieved_at_ns=i64::try_from(observed_ns)
            .map_err(|_|Error::InvalidFinal)?;
        let canonical=serde_json::to_vec(&value)?;
        let mut result=parse_official_market(ticker,&canonical,retrieved_at_ns)?;
        // Only this reviewed HTTPS GET can mark an official live response.
        // Timestamp is captured here, not accepted from an arbitrary caller.
        result.authenticated_public_get=true;
        result.source_id=origin.into();
        Ok(result)
    }

}

#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum OfficialStatus {
    FinalYes,FinalNo,Pending,Cancelled,Void,Scalar,Unknown,
}
#[derive(Debug,Clone)]
pub struct OfficialMarketEvidence {
    pub ticker:String,pub market_id:Option<String>,pub event_ticker:Option<String>,
    pub market_type:Option<String>,pub settlement_ts:Option<String>,
    pub status:OfficialStatus,pub source_sha256:String,
    pub retrieved_at_ns:i64,pub source_id:String,
    authenticated_public_get:bool,
}
impl OfficialMarketEvidence{
    pub fn is_authenticated_public_get(&self)->bool{
        self.authenticated_public_get
    }
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum FinalRevision {
    SameEvidence,NewFinal,UpdatedPending,ConflictingFinal,TimeRegression,
}
/// Compare two read-only official market observations. Conflicting binary
/// finals must be retained for manual investigation, never auto-overwritten.
pub fn classify_official_revision(
    prior:&OfficialMarketEvidence,current:&OfficialMarketEvidence)
    ->Result<FinalRevision,Error>{
    if prior.ticker!=current.ticker||
        matches!((&prior.market_id,&current.market_id),
            (Some(a),Some(b)) if a!=b)||
        matches!((&prior.event_ticker,&current.event_ticker),
            (Some(a),Some(b)) if a!=b)||
        prior.market_type!=current.market_type{
        return Err(Error::InvalidFinal);
    }
    if current.retrieved_at_ns<prior.retrieved_at_ns{
        return Ok(FinalRevision::TimeRegression);
    }
    if prior.source_sha256==current.source_sha256{
        return Ok(FinalRevision::SameEvidence);
    }
    let is_final=|s:OfficialStatus|matches!(s,
        OfficialStatus::FinalYes|OfficialStatus::FinalNo);
    if is_final(prior.status){
        return Ok(FinalRevision::ConflictingFinal);
    }
    if is_final(current.status){return Ok(FinalRevision::NewFinal);}
    Ok(FinalRevision::UpdatedPending)
}
/// Pure mock-friendly parser. SHA256 covers canonical JSON bytes provided by
/// this call, not a claim that the HTTP transport was independently audited.
pub fn parse_official_market(expected:&str,body:&[u8],retrieved_at_ns:i64)
    ->Result<OfficialMarketEvidence,Error>{
    if !good_ticker(expected)||body.is_empty()||
        body.len() as u64>MAX_RESPONSE_BYTES||retrieved_at_ns<=0{
        return Err(Error::InvalidFinal);
    }
    let root:serde_json::Value=serde_json::from_slice(body)?;
    let m=root.get("market").unwrap_or(&root);
    let ticker=m.get("ticker").and_then(|v|v.as_str())
        .ok_or(Error::InvalidFinal)?;
    if ticker!=expected{return Err(Error::InvalidFinal);}
    // The authoritative Get Market schema requires a ticker, NOT a UUID.
    // Preserve a separately provided archived ID, without fabricating one.
    let market_id=match m.get("market_id"){
        None|Some(serde_json::Value::Null)=>None,
        Some(serde_json::Value::String(v)) if !v.trim().is_empty()=>Some(v.clone()),
        _=>return Err(Error::InvalidFinal),
    };
    let event_ticker=match m.get("event_ticker"){
        None|Some(serde_json::Value::Null)=>None,
        Some(serde_json::Value::String(v)) if !v.trim().is_empty()=>Some(v.clone()),
        _=>return Err(Error::InvalidFinal),
    };
    let market_type=m.get("market_type").and_then(|v|v.as_str());
    if m.get("market_type").is_some()&&
        !matches!(market_type,Some("binary"|"scalar")){
        return Err(Error::InvalidFinal);
    }
    let settlement_ts=match m.get("settlement_ts"){
        None|Some(serde_json::Value::Null)=>None,
        Some(serde_json::Value::String(v)) if !v.trim().is_empty()=>Some(v.clone()),
        _=>return Err(Error::InvalidFinal),
    };
    let is_provisional=match m.get("is_provisional"){
        None|Some(serde_json::Value::Null)=>false,
        Some(serde_json::Value::Bool(value))=>*value,
        _=>return Err(Error::InvalidFinal),
    };
    let status=m.get("status").and_then(|v|v.as_str()).unwrap_or("");
    let result=m.get("result").and_then(|v|v.as_str()).unwrap_or("");
    // "determined" is not "finalized"; settlement can still be revised.
    // An absent market_type never promotes a local partial fixture to final.
    let classification=match (status,result){
        ("cancelled"|"canceled",_)=>OfficialStatus::Cancelled,
        ("voided"|"void",_)=>OfficialStatus::Void,
        (_, "scalar") if matches!(market_type,Some("scalar"))=>OfficialStatus::Scalar,
        ("settled"|"finalized",_) if market_type==Some("scalar")=>
            OfficialStatus::Scalar,
        ("settled"|"finalized","yes"|"YES"|"Y")
            if market_type==Some("binary")&&!is_provisional=>
                OfficialStatus::FinalYes,
        ("settled"|"finalized","no"|"NO"|"N")
            if market_type==Some("binary")&&!is_provisional=>
                OfficialStatus::FinalNo,
        ("open"|"active"|"closed"|"determined"|"disputed"|"amended"|"initialized",_)
            =>OfficialStatus::Pending,
        ("settled"|"finalized",_) if is_provisional=>OfficialStatus::Pending,
        _=>OfficialStatus::Unknown,
    };
    Ok(OfficialMarketEvidence{
        ticker:ticker.to_owned(),market_id,event_ticker,
        market_type:market_type.map(str::to_owned),settlement_ts,
        status:classification,source_sha256:format!("{:x}",Sha256::digest(body)),
        retrieved_at_ns,source_id:"unverified-local-json".into(),
        authenticated_public_get:false,
    })
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]fn exact_official_final_is_distinct_from_closed_market(){
        let yes=br#"{"market":{"ticker":"KXBTC-TEST","market_id":"1",
            "market_type":"binary","status":"settled","result":"yes"}}"#;
        let record=parse_official_market("KXBTC-TEST",yes,100).unwrap();
        assert_eq!(record.status,OfficialStatus::FinalYes);
        let closed=br#"{"market":{"ticker":"KXBTC-TEST","market_id":"1",
            "market_type":"binary","status":"closed","result":"yes"}}"#;
        assert_eq!(parse_official_market("KXBTC-TEST",closed,100)
            .unwrap().status,OfficialStatus::Pending);
    }
    #[test]fn changed_final_records_are_conflicts_not_silent_replacements(){
        let a=parse_official_market("KXBTC-TEST",
            br#"{"market":{"ticker":"KXBTC-TEST","market_id":"1",
            "market_type":"binary","status":"settled","result":"yes"}}"#,100).unwrap();
        let b=parse_official_market("KXBTC-TEST",
            br#"{"market":{"ticker":"KXBTC-TEST","market_id":"1",
            "market_type":"binary","status":"settled","result":"no"}}"#,101).unwrap();
        assert_eq!(classify_official_revision(&a,&b).unwrap(),
            FinalRevision::ConflictingFinal);
        assert_eq!(classify_official_revision(&b,&a).unwrap(),
            FinalRevision::TimeRegression);
    }
    #[test]fn documented_ticker_identity_and_conflicting_ticker_block(){
        let b=br#"{"market":{"ticker":"MISMATCH","market_id":"1",
            "market_type":"binary","status":"settled","result":"yes"}}"#;
        assert!(parse_official_market("KXBTC-TEST",b,100).is_err());
        let missing=br#"{"market":{"ticker":"KXBTC-TEST",
            "market_type":"binary","status":"settled","result":"no"}}"#;
        let parsed=parse_official_market("KXBTC-TEST",missing,100).unwrap();
        assert_eq!(parsed.market_id,None);
        assert_eq!(parsed.status,OfficialStatus::FinalNo);
    }
    #[test]fn cancelled_void_scalar_never_promote_binary_final(){
        for (kind,result,expected) in [
            ("canceled","yes",OfficialStatus::Cancelled),
            ("void","yes",OfficialStatus::Void),
            ("settled","scalar",OfficialStatus::Scalar),
        ]{
            let market_type=if result=="scalar"{"scalar"}else{"binary"};
            let data=format!(r#"{{"market":{{"ticker":"KXBTC-TEST",
                "market_id":"1","market_type":"{market_type}",
                "status":"{kind}","result":"{result}"}}}}"#);
            assert_eq!(parse_official_market("KXBTC-TEST",data.as_bytes(),100)
                .unwrap().status,expected);
        }
    }
    #[test]fn archived_and_live_market_paths_are_distinct_get_only_routes(){
        let u=ReadOnlyKalshi::build_url("historical/markets/KXBTC-TEST",&[]);
        assert_eq!(u.path(),"/trade-api/v2/historical/markets/KXBTC-TEST");
        assert_eq!(u.scheme(),"https");
    }
    #[test]fn documented_shape_without_uuid_finalizes_only_binary_not_provisional(){
        let yes=br#"{"market":{"ticker":"KXBTC-TEST","event_ticker":"E1",
            "market_type":"binary","status":"finalized","result":"yes",
            "settlement_ts":null,"is_provisional":false}}"#;
        let o=parse_official_market("KXBTC-TEST",yes,100).unwrap();
        assert_eq!(o.status,OfficialStatus::FinalYes);
        assert_eq!(o.market_id,None);
        assert!(!o.is_authenticated_public_get());
        let pending=br#"{"market":{"ticker":"KXBTC-TEST","market_type":"binary",
            "status":"determined","result":"yes"}}"#;
        assert_eq!(parse_official_market("KXBTC-TEST",pending,100)
            .unwrap().status,OfficialStatus::Pending);
        let provisional=br#"{"market":{"ticker":"KXBTC-TEST","market_type":"binary",
            "status":"finalized","result":"no","is_provisional":true}}"#;
        assert_eq!(parse_official_market("KXBTC-TEST",provisional,100)
            .unwrap().status,OfficialStatus::Pending);
        let missing_type=br#"{"market":{"ticker":"KXBTC-TEST",
            "status":"finalized","result":"yes"}}"#;
        assert_eq!(parse_official_market("KXBTC-TEST",missing_type,100)
            .unwrap().status,OfficialStatus::Unknown);
    }
    #[test]fn different_archived_ids_and_conflicting_final_statuses_never_merge(){
        let a=parse_official_market("KXBTC-TEST",
            br#"{"market":{"ticker":"KXBTC-TEST","market_id":"A",
                "market_type":"binary","status":"finalized","result":"yes"}}"#,100).unwrap();
        let b=parse_official_market("KXBTC-TEST",
            br#"{"market":{"ticker":"KXBTC-TEST","market_id":"B",
                "market_type":"binary","status":"finalized","result":"yes"}}"#,101).unwrap();
        assert!(classify_official_revision(&a,&b).is_err());
    }
    #[test]fn urls_are_https_and_get_only(){
        let u=ReadOnlyKalshi::build_url("markets/BTC-15M/orderbook",&[("depth","5")]);
        assert_eq!(u.scheme(),"https");
        assert_eq!(u.host_str(),Some("external-api.kalshi.com"));
        assert_eq!(u.path(),"/trade-api/v2/markets/BTC-15M/orderbook");
        assert_eq!(u.query(),Some("depth=5"));
    }
    #[test]fn invalid_ticker_blocked_before_network(){
        assert!(!good_ticker("../../orders"));
        assert!(!good_ticker("BTC%2Forders"));
        assert!(good_ticker("KXBTC15M-26NOV12T1200"));
    }
    #[test]fn query_cursor_escapes_special_characters(){
        let u=ReadOnlyKalshi::build_url("markets",&[("cursor","a+b/c==")]);
        assert!(u.as_str().contains("a%2Bb%2Fc%3D%3D"));
    }
}

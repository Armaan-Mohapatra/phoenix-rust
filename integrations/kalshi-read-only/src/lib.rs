//! Public Kalshi market-data GET adapter. No authentication, WebSocket,
//! order APIs, POST/PUT/DELETE, portfolio data, or trading functionality.
//! Network is NEVER accessed by unit tests.
use std::{io::Read,time::Duration};

pub const BASE:&str="https://external-api.kalshi.com/trade-api/v2";
pub const MAX_RESPONSE_BYTES:u64=2_000_000;
#[derive(Debug)]
pub enum Error {InvalidTicker,InvalidLimit,InvalidCursor,Http(reqwest::Error),
    Io(std::io::Error),BadStatus(u16),ResponseTooLarge,Json(serde_json::Error)}
impl From<reqwest::Error> for Error {fn from(x:reqwest::Error)->Self{Self::Http(x)}}
impl From<std::io::Error> for Error {fn from(x:std::io::Error)->Self{Self::Io(x)}}
impl From<serde_json::Error> for Error {fn from(x:serde_json::Error)->Self{Self::Json(x)}}

fn good_ticker(t:&str)->bool {
    !t.is_empty()&&t.len()<=128&&t.bytes().all(|b|
        b.is_ascii_uppercase()||b.is_ascii_digit()||b==b'-'||b==b'_')
}
#[derive(Clone,Debug)]
pub struct ReadOnlyKalshi {client:reqwest::blocking::Client}
impl ReadOnlyKalshi {
    pub fn new()->Result<Self,Error>{
        let client=reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("PhoenixRustOfflineResearch/0.1 (read-only)")
            .build()?;
        Ok(Self{client})
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
    pub fn orderbook(&self,ticker:&str,depth:Option<u8>)
        ->Result<serde_json::Value,Error>{
        if !good_ticker(ticker){return Err(Error::InvalidTicker);}
        if depth.is_some_and(|d|d==0){return Err(Error::InvalidLimit);}
        let depth_text=depth.map(|d|d.to_string());
        let params=depth_text.as_deref().map(|s|vec![("depth",s)]).unwrap_or_default();
        self.get(Self::build_url(&format!("markets/{ticker}/orderbook"),&params))
    }
}
#[cfg(test)]
mod tests{
    use super::*;
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

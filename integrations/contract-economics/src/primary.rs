//! Optional, locally supplied primary-reference importer. No CF network,
//! authentication, account data or redistributable licensed ticks bundled.
//! Provider-origin authentication remains an independent external gate.
use super::{Error,PrimaryImport,PrimaryTick,primary_ticks_digest};
use serde::Deserialize;
use sha2::{Digest,Sha256};
use std::collections::BTreeSet;

pub const MAX_PRIMARY_EXPORT_BYTES:usize=512*1024;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalPrimaryEnvelope {
    provider:String,
    benchmark_id:String,
    methodology_revision:String,
    document_sha256:String,
    retrieved_at_ns:i64,
    observed_at_ns:i64,
    first_second_utc:i64,
    ticks:Vec<LocalTick>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalTick {
    second_utc:i64,
    // Use exact integer decimal strings; JSON floats are not acceptable.
    value_1e8:String,
}
/// Convert a bounded, already authorized, externally provided local export
/// into an immutable evidence candidate, not an authenticated final result.
pub fn import_local_primary_json(bytes:&[u8])->Result<PrimaryImport,Error>{
    if bytes.is_empty()||bytes.len()>MAX_PRIMARY_EXPORT_BYTES{
        return Err(Error::InvalidPrimary);
    }
    let input:LocalPrimaryEnvelope=serde_json::from_slice(bytes)
        .map_err(|_|Error::InvalidPrimary)?;
    if input.provider.trim().is_empty()||input.benchmark_id.trim().is_empty()||
        input.methodology_revision.trim().is_empty()||
        input.document_sha256.len()!=64||
        !input.document_sha256.bytes().all(|b|b.is_ascii_hexdigit())||
        input.observed_at_ns<=0||input.retrieved_at_ns<input.observed_at_ns||
        input.ticks.is_empty()||input.ticks.len()>3600{
        return Err(Error::InvalidPrimary);
    }
    let mut ticks=Vec::with_capacity(input.ticks.len());
    let mut seen=BTreeSet::new();
    for t in input.ticks{
        if t.value_1e8.is_empty()||t.value_1e8.len()>40||
            !t.value_1e8.bytes().all(|b|b.is_ascii_digit())||
            !seen.insert(t.second_utc){
            return Err(Error::InvalidPrimary);
        }
        let value=t.value_1e8.parse::<i128>()
            .map_err(|_|Error::InvalidPrimary)?;
        if value<=0{return Err(Error::InvalidPrimary);}
        ticks.push(PrimaryTick{second_utc:t.second_utc,value_1e8:value});
    }
    ticks.sort_by_key(|t|t.second_utc);
    if ticks[0].second_utc!=input.first_second_utc{
        return Err(Error::InvalidPrimary);
    }
    let digest=primary_ticks_digest(&ticks)?;
    Ok(PrimaryImport{
        provider:input.provider,benchmark_id:input.benchmark_id,
        methodology_revision:input.methodology_revision,
        source_sha256:format!("{:x}",Sha256::digest(bytes)),
        ticks_sha256:digest,
        document_sha256:input.document_sha256,
        retrieved_at_ns:input.retrieved_at_ns,
        observed_at_ns:input.observed_at_ns,
        first_second_utc:input.first_second_utc,ticks,
    })
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn lossless_primary_integer_import_requires_exact_epoch_and_hash(){
        let raw=format!(r#"{{"provider":"CF Benchmarks","benchmark_id":"BRTI",
          "methodology_revision":"r1","document_sha256":"{}",
          "retrieved_at_ns":100,"observed_at_ns":90,
          "first_second_utc":1,"ticks":[
          {{"second_utc":1,"value_1e8":"100000001"}},
          {{"second_utc":2,"value_1e8":"100000002"}}]}}"#,"a".repeat(64));
        let result=import_local_primary_json(raw.as_bytes()).unwrap();
        assert_eq!(result.ticks[0].value_1e8,100000001);
        assert_eq!(result.source_sha256.len(),64);
        assert_eq!(result.ticks_sha256,primary_ticks_digest(&result.ticks).unwrap());
    }
    #[test]
    fn duplicates_and_float_encodings_are_rejected(){
        let duplicate=format!(r#"{{"provider":"CF Benchmarks",
            "benchmark_id":"BRTI","methodology_revision":"r1",
            "document_sha256":"{}","retrieved_at_ns":100,"observed_at_ns":90,
            "first_second_utc":1,"ticks":[
              {{"second_utc":1,"value_1e8":"1"}},
              {{"second_utc":1,"value_1e8":"1"}}]}}"#,"a".repeat(64));
        assert!(import_local_primary_json(duplicate.as_bytes()).is_err());
        let floats=duplicate.replace("\"1\"","1.0");
        assert!(import_local_primary_json(floats.as_bytes()).is_err());
    }
}

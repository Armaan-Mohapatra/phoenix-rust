//! Operator-controlled read-only batched multi-market source replay.
//! Prints one self-contained JSON report/checkpoint to stdout; no writes.
use phoenix_source_loader::multi_market::{
    MarketSpec,ReusableMarketCheckpoint,run_batch};
use serde::Deserialize;
use sha2::{Digest,Sha256};
use std::{env,fs::{self,File},io::Read,path::PathBuf};
use zip::ZipArchive;
const MAX_PLAN:usize=128*1024;
const MAX_PRIOR:usize=4*1024*1024;
const MAX_CATALOG:u64=4*1024*1024;
#[derive(Deserialize)]
struct Plan{
    engine_revision_sha256:String,
    expected_catalog_sha256:String,
    markets:Vec<MarketSpec>,
}
fn bounded_file(path:&str,max:usize)->Result<Vec<u8>,String>{
    let info=fs::metadata(path).map_err(|e|e.to_string())?;
    if info.len()>max as u64{return Err("INPUT_TOO_LARGE".into());}
    let data=fs::read(path).map_err(|e|e.to_string())?;
    if data.len()>max{return Err("INPUT_TOO_LARGE".into());}
    Ok(data)
}
fn run()->Result<(),String>{
    let args=env::args().collect::<Vec<_>>();
    if args.len()<4{
        return Err("usage: phoenix-multimarket-replay PLAN.json PREVIOUS.json|- SOURCE.zip [SOURCE2.zip ...]".into());
    }
    let plan:Plan=serde_json::from_slice(&bounded_file(&args[1],MAX_PLAN)?)
        .map_err(|e|e.to_string())?;
    if plan.markets.is_empty()||plan.markets.len()>4||
        args.len()-3>12{
        return Err("BOUNDED_INPUT_REQUIRED".into());
    }
    let prior:Vec<ReusableMarketCheckpoint>=if args[2]=="-"{
        vec![]
    }else{
        serde_json::from_slice(&bounded_file(&args[2],MAX_PRIOR)?)
            .map_err(|e|e.to_string())?
    };
    let zip_paths=args[3..].iter().map(PathBuf::from).collect::<Vec<_>>();
    let mut zip=ZipArchive::new(
        File::open(&zip_paths[0]).map_err(|e|e.to_string())?)
        .map_err(|e|e.to_string())?;
    let mut entry=zip.by_name("archive/curated/curation_catalog.jsonl")
        .map_err(|e|e.to_string())?;
    if entry.size()>MAX_CATALOG{return Err("CATALOG_TOO_LARGE".into());}
    let mut manifest=Vec::new();
    entry.take(MAX_CATALOG+1).read_to_end(&mut manifest)
        .map_err(|e|e.to_string())?;
    if manifest.len() as u64>MAX_CATALOG{
        return Err("CATALOG_TOO_LARGE".into());
    }
    let observed=format!("{:x}",Sha256::digest(&manifest));
    if !observed.eq_ignore_ascii_case(&plan.expected_catalog_sha256){
        return Err("CATALOG_SHA_MISMATCH".into());
    }
    let out=run_batch(&plan.markets,&manifest,&zip_paths,
        &plan.engine_revision_sha256,&prior).map_err(|e|format!("{e:?}"))?;
    println!("{}",serde_json::to_string_pretty(&out)
        .map_err(|e|e.to_string())?);
    Ok(())
}
fn main(){
    if let Err(err)=run(){
        eprintln!("PHOENIX_MULTI_MARKET_BLOCKED: {err}");
        std::process::exit(2);
    }
}

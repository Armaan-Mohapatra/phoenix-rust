//! Offline, read-only TNEVENT tooling.
//!
//!   phoenix-tnevent verify   --root DIR [--from N] [--to M]
//!       manifest SHA-256 + size, block/event CRC32, event count, for every finalized segment
//!   phoenix-tnevent manifest --root DIR --out DIR [--from N] [--to M] [--no-hash]
//!       market manifest (coverage, verification, settlement evidence, eligibility class)
//!   phoenix-tnevent cases    --root DIR --manifest DIR/markets.jsonl --out DIR [--partial]
//!       campaign-ready cases.jsonl (cluster-ordered), for markets the manifest accepted
//!
//! Output directories must be new/empty and outside protected roots (`PHOENIX_PROTECTED_ROOTS`).
use phoenix_campaign::paths::{prepare_output_dir, protected_roots};
use phoenix_tnevent::cases::{BuildConfig, CaseBuilder};
use phoenix_tnevent::container::{Limits, Segment};
use phoenix_tnevent::corpus::{list_segments, Eligibility, ManifestBuilder, MarketRecord};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

fn arg(args: &[String], k: &str) -> Option<String> {
    args.iter()
        .position(|a| a == k)
        .and_then(|i| args.get(i + 1))
        .cloned()
}
fn flag(args: &[String], k: &str) -> bool {
    args.iter().any(|a| a == k)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("phoenix-tnevent: {e}");
            ExitCode::from(2)
        }
    }
}

fn segments(args: &[String]) -> Result<Vec<(u64, PathBuf)>, String> {
    let root = PathBuf::from(arg(args, "--root").ok_or("--root DIR is required")?);
    let from: u64 = arg(args, "--from").map_or(0, |s| s.parse().unwrap_or(0));
    let to: u64 = arg(args, "--to").map_or(u64::MAX, |s| s.parse().unwrap_or(u64::MAX));
    Ok(list_segments(&root)
        .map_err(|e| format!("{e:?}"))?
        .into_iter()
        .filter(|(id, _)| *id >= from && *id <= to)
        .collect())
}

fn run(args: &[String]) -> Result<(), String> {
    let cmd = args
        .get(1)
        .map(String::as_str)
        .ok_or("usage: phoenix-tnevent verify|manifest|cases ...")?;
    let started = Instant::now();
    match cmd {
        "verify" => {
            let segs = segments(args)?;
            let (mut events, mut bad) = (0u64, 0u64);
            for (id, dir) in &segs {
                let r = Segment::open(dir, Limits::default()).and_then(|s| {
                    s.verify_hash()?;
                    s.visit(&mut |_| Ok(()))
                });
                match r {
                    Ok(st) => events += st.events,
                    Err(e) => {
                        bad += 1;
                        println!("segment {id}: FAILED {e:?}");
                    }
                }
            }
            println!(
                "verify: segments={} failed={} events={} seconds={:.1}",
                segs.len(),
                bad,
                events,
                started.elapsed().as_secs_f64()
            );
            if bad > 0 {
                return Err(format!("{bad} segment(s) failed verification"));
            }
        }
        "manifest" => {
            let segs = segments(args)?;
            let out = prepare_output_dir(
                &PathBuf::from(arg(args, "--out").ok_or("--out DIR is required")?),
                &protected_roots(),
            )
            .map_err(|e| format!("{e:?}"))?;
            let mut b = ManifestBuilder::new(!flag(args, "--no-hash"));
            for (i, (id, dir)) in segs.iter().enumerate() {
                b.process_segment(*id, dir)
                    .map_err(|e| format!("segment {id}: {e:?}"))?;
                if (i + 1) % 25 == 0 {
                    eprintln!(
                        "  {}/{} segments, {:.0}s",
                        i + 1,
                        segs.len(),
                        started.elapsed().as_secs_f64()
                    );
                }
            }
            let (markets, segrecs, seg_sha, vstats) = b.finish();
            let mut w = BufWriter::new(
                File::create_new(out.join("markets.jsonl")).map_err(|e| e.to_string())?,
            );
            for m in &markets {
                writeln!(
                    w,
                    "{}",
                    serde_json::to_string(m).map_err(|e| e.to_string())?
                )
                .map_err(|e| e.to_string())?;
            }
            w.flush().map_err(|e| e.to_string())?;
            let mut w = BufWriter::new(
                File::create_new(out.join("segments.jsonl")).map_err(|e| e.to_string())?,
            );
            for s in &segrecs {
                writeln!(
                    w,
                    "{}",
                    serde_json::to_string(s).map_err(|e| e.to_string())?
                )
                .map_err(|e| e.to_string())?;
            }
            w.flush().map_err(|e| e.to_string())?;
            let mut by_class: BTreeMap<String, u64> = BTreeMap::new();
            let mut by_asset: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
            for m in &markets {
                let c = format!("{:?}", m.eligibility.unwrap_or(Eligibility::NoWindow));
                *by_class.entry(c.clone()).or_default() += 1;
                *by_asset
                    .entry(m.asset.clone())
                    .or_default()
                    .entry(c)
                    .or_default() += 1;
            }
            let summary = serde_json::json!({
                "schema": "phoenix-tnevent-manifest-v1", "segments": segrecs.len(), "segments_sha_verified": segrecs.iter().filter(|s| s.sha256_verified).count(),
                "events": segrecs.iter().map(|s| s.events).sum::<u64>(), "markets": markets.len(), "by_eligibility": by_class, "by_asset": by_asset,
                "first_segment": segrecs.first().map(|s| s.id), "last_segment": segrecs.last().map(|s| s.id),
                "first_recv_ns": segrecs.iter().filter_map(|s| s.first_recv_ns).min(), "last_recv_ns": segrecs.iter().filter_map(|s| s.last_recv_ns).max(),
                "orderbook_snapshots": segrecs.iter().map(|s| s.orderbook_snapshots).sum::<u64>(), "orderbook_deltas": segrecs.iter().map(|s| s.orderbook_deltas).sum::<u64>(),
                "trades": segrecs.iter().map(|s| s.trades).sum::<u64>(),
                "stream_verification": {"snapshots": vstats.snapshots, "deltas_applied": vstats.deltas_applied, "deltas_before_init": vstats.deltas_before_init,
                    "deltas_while_broken": vstats.deltas_while_broken, "duplicates": vstats.duplicates, "gaps": vstats.gaps, "regressions": vstats.regressions,
                    "generation_changes": vstats.generation_changes, "wrong_market": vstats.wrong_market, "negative_depth": vstats.negative_depth,
                    "crossed": vstats.crossed, "bad_precision": vstats.bad_precision, "malformed": vstats.malformed},
                "segment_sha256": seg_sha, "seconds": started.elapsed().as_secs_f64(),
                "settlement_evidence": "venue-reported lifecycle `determined` results; not independently verified against the underlying index",
            });
            File::create_new(out.join("summary.json"))
                .and_then(|mut f| {
                    f.write_all(serde_json::to_string_pretty(&summary).unwrap().as_bytes())
                })
                .map_err(|e| e.to_string())?;
            println!("{}", serde_json::to_string_pretty(&summary).unwrap());
        }
        "cases" => {
            let segs = segments(args)?;
            let mpath = arg(args, "--manifest").ok_or("--manifest markets.jsonl is required")?;
            let mut records: Vec<MarketRecord> = vec![];
            for line in BufReader::new(File::open(&mpath).map_err(|e| e.to_string())?).lines() {
                records.push(
                    serde_json::from_str(&line.map_err(|e| e.to_string())?)
                        .map_err(|e| e.to_string())?,
                );
            }
            let seg_sha: BTreeMap<u64, String> = {
                let sp = PathBuf::from(&mpath).with_file_name("segments.jsonl");
                let mut m = BTreeMap::new();
                for line in BufReader::new(File::open(sp).map_err(|e| e.to_string())?).lines() {
                    let v: serde_json::Value =
                        serde_json::from_str(&line.map_err(|e| e.to_string())?)
                            .map_err(|e| e.to_string())?;
                    m.insert(
                        v["id"].as_u64().unwrap_or(0),
                        v["sha256"].as_str().unwrap_or("").to_owned(),
                    );
                }
                m
            };
            let out = prepare_output_dir(
                &PathBuf::from(arg(args, "--out").ok_or("--out DIR is required")?),
                &protected_roots(),
            )
            .map_err(|e| format!("{e:?}"))?;
            let file = BufWriter::with_capacity(
                1 << 20,
                File::create_new(out.join("cases.jsonl")).map_err(|e| e.to_string())?,
            );
            let cfg = BuildConfig {
                include_partial: flag(args, "--partial"),
                ..BuildConfig::default()
            };
            let mut b = CaseBuilder::new(cfg, &records, seg_sha, file);
            for (i, (id, dir)) in segs.iter().enumerate() {
                b.process_segment(*id, dir)
                    .map_err(|e| format!("segment {id}: {e:?}"))?;
                if (i + 1) % 25 == 0 {
                    eprintln!(
                        "  {}/{} segments, {:.0}s",
                        i + 1,
                        segs.len(),
                        started.elapsed().as_secs_f64()
                    );
                }
            }
            let rep = b.finish().map_err(|e| format!("{e:?}"))?;
            let summary = serde_json::json!({
                "schema": "phoenix-tnevent-cases-v1", "cases_written": rep.cases_written, "snapshots_written": rep.snapshots_written, "bytes": rep.bytes_written,
                "output_sha256": rep.output_sha256, "skipped_truncated_by_corpus_end": rep.skipped_truncated_by_corpus_end, "skipped_no_samples": rep.skipped_no_samples,
                "skipped_invalid": rep.skipped_invalid, "include_partial": cfg.include_partial, "sample_ns": cfg.sample_ns, "depth": cfg.depth,
                "reference_cents": "0 = not available (no spot series in this capture)", "provenance": "operator_normalized", "seconds": started.elapsed().as_secs_f64(),
            });
            File::create_new(out.join("cases_summary.json"))
                .and_then(|mut f| {
                    f.write_all(serde_json::to_string_pretty(&summary).unwrap().as_bytes())
                })
                .map_err(|e| e.to_string())?;
            println!("{}", serde_json::to_string_pretty(&summary).unwrap());
        }
        other => return Err(format!("unknown command {other:?}")),
    }
    Ok(())
}

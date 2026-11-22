//! usage: phoenix-collector-parity <feed.ndjson>
//! Replays one market's native book feed and prints a JSON parity report.
//! Exit code 0 only when every step matched the collector's recorded quotes.
use phoenix_collector_adapter::replay;
use std::io::{BufRead, BufReader};

fn main() {
    let path = match std::env::args().nth(1) {
        Some(p) => p,
        None => {
            eprintln!("usage: phoenix-collector-parity <feed.ndjson>");
            std::process::exit(64);
        }
    };
    let file = std::fs::File::open(&path).unwrap_or_else(|e| {
        eprintln!("cannot open {path}: {e}");
        std::process::exit(66);
    });
    let lines = BufReader::new(file).lines().map_while(Result::ok);
    let report = replay(lines);
    println!(
        "{}",
        serde_json::to_string(&report).expect("report serializes")
    );
    std::process::exit(if report.parity_ok() { 0 } else { 2 });
}

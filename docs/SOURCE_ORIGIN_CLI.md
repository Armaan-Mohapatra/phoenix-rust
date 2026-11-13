# Local read-only historical source audit CLI

Run from a research/staging checkout with the **actual** uploaded LunaDelta files accessible on the computer, not from GitHub Actions.
Pass ticker, RFC3339 start/end, an 8KiB or smaller text file of **exact selected raw-events Parquet member names**, and 1–12 source ZIP files (the first must contain the curation catalog).

Command example (adjust paths to locations actually verified):

  cargo run --release --manifest-path integrations/source-loader/Cargo.toml --bin phoenix-source-audit -- KXBTC15M-26OCT241900-00 2026-10-24T22:44:40Z 2026-10-24T22:47:39Z E:\\ResearchInputs\\btc_members.txt E:\\ResearchInputs\\LunaDelta_20261025T083801Z_market_data_part001.zip E:\\ResearchInputs\\LunaDelta_20261025T083801Z_market_data_part002.zip

Output is JSON printed to stdout; redirect to a dedicated research output folder if needed. No collector/database/source changes, no trading orders, no credentials.
Reader checks SHA256 for every selected source member against original curated catalog, then decodes real WSS rows by local receipt time and attempts a strict fractional book replay.
Without a full verified initial snapshot, source session and global shared-channel sequence coverage, it returns UNKNOWN/PARTIAL initialization rather than certifying an executable book.
GitHub CI only tests synthetic Parquet containing actual historical message field formats; private full ZIP inputs are not present in Actions.
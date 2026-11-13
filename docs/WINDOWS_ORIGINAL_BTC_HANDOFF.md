# Windows local, read-only real BTC source audit

Require a separately verified Rust/Cargo toolchain in an isolated research checkout and the 6 uploaded original ZIP archives accessible in a dedicated input directory. Do not install or modify Phoenix's active collectors.

Example (replace with paths verified on your Windows device):

    & "E:\PhoenixTooling\stage\phoenix-rust\integrations\source-loader\scripts\run-original-btc.ps1" -RepoRoot "E:\PhoenixTooling\stage\phoenix-rust" -InputsDirectory "E:\PhoenixResearchInputs" -OutputDirectory "E:\PhoenixResearchHandoff\btc_20261024"

The script validates 24 exact allowed members, the original inside-ZIP curation catalog SHA256, all six ZIP locations, output-folder separation, and Cargo availability. The Rust CLI then checks per-member SHA256, reads bounded Parquet rows and reports the market's observed original messages and forensic initialization status. Output is compact JSON outside source directories.

Historical Rust source execution has NOT been run in the hosted research container: Rust/Cargo are absent and the official distribution host could not be resolved. GitHub Actions does not possess these private original ZIPs. Do not report a source replay as complete merely because synthetic CI passed.

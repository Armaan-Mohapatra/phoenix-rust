# Phase 14 — Phase 15 local Windows validation matrix (prepared, NOT RUN)

**No compilation, tests, replay, benchmarks or local operator action occurred during Phase 14.** All below are FUTURE tests, not test results. The local source-only launcher is `scripts/validate-phoenix-locally.ps1`. It requires `-ExecuteValidation`, exact Git `-ExpectedSha`, explicit output outside source/collector roots, an installed offline Rust toolchain/cache and a separately authorized operator. Each logged command receives SHA256-tagged output and machine-readable PASS/FAIL/BLOCKED status, never silently upgrading incomplete external evidence.

| # | Validation category | Prepared command/fixture | Eligibility and required evidence |
|---:|---|---|---|
| 1 | Git SHA/branch | `git rev-parse HEAD` + `git branch --show-current` | Exact final Phase 14 HEAD and existing isolated research source |
| 2 | Isolated checkout | `git clone --no-hardlinks --no-checkout --local` then `git checkout --detach SHA` | Dedicated research output, ancestor/reparse safety |
| 3 | Toolchain/cache preflight | `rustup toolchain list`, `rustup run 1.78.0 rustc`, `cargo metadata` | Dependencies locally cached unless operator explicitly allows download; capture resulting Cargo.lock SHA256 |
| 4 | Root + standalone crates | `cargo +1.78.0 test --workspace --all-targets` AND enumerate all 18 standalone `Cargo.toml` paths | Prevent false root-only test coverage; hft separate |
| 5 | Optional pinned hftbacktest | `cargo +1.91.1 test --manifest-path integrations/hftbacktest-adapter/Cargo.toml --features upstream-hft` | `-IncludeOptionalHft`, cached/approved pinned Git SHA `5f3ec40...`, no live features |
| 6 | rustfmt/Clippy | root and each standalone `cargo fmt -- --check`, `cargo clippy --all-targets -- -D warnings` | Local version-compatible rustfmt/Clippy, do not silently repair generated code |
| 7 | All unit/integration tests | all Cargo workspace/standalone test sources | Fail any regression, annotate originally synthetic evidence |
| 8 | Archive corruption/recovery/million-ID | durable-archive/indexed-archive default regressions; disk-identity `million_logical_ids_in_bounded_256_id_transactions` filtered under `-IncludeStress` | Explicit heavy opt-in and bounded isolated output |
| 9 | Windows publication/fs checks | archive's reparse-point and recovery synthetic tests; separate physical NTFS hard-link and power-cut campaign | Hardware physical power-cut proof remains BLOCKED until independently specified |
| 10 | Original BTC ZIP/Parquet | `-IncludeOriginalSourceAudit` launches verified 24-member `run-original-btc.ps1` | Six historical market_data ZIPs; expected catalog SHA; source read-only |
| 11 | Multi-market replay | `-IncludeMultiMarketReplay -MultiMarketPlan ... -MultiMarketSourceZips ...` invokes `phoenix-multimarket-replay` | Externally verified bounded market plan, <=4 markets/call, <=12 ZIP files, SHA-matched catalog |
| 12 | Original Perps schema | Phase 8 perps unit/parser tests; actual finalized partition importer only with real schema contract | Original Perps venue/instrument/quantity scale/partition SHA unavailable; mark BLOCKED |
| 13 | Source/book integrity | `source-loader` original decoder/channel/L2 tests, authenticated source snapshots | Full channel sequence, reconnect and tape witness unavailable; empirical completeness BLOCKED |
| 14 | Historical fees and settlement | contract-economics + settlement crate tests and original primary rules | Authenticated effective-dated fee tables, real Kalshi official final and independent reference; BLOCKED |
| 15 | Historical maker/taker | `sim::historical` synthetic taker/maker/queue tests and optional pinned hft comparison | Original queue tape proof required for real results; no venue fill claims |
| 16 | Frozen original strategy parity | strategy importer/version tests; compare archived Python per-decision records if available | 11 October version identities, 0 fully recoverable executable originals; parity BLOCKED |
| 17 | Strategy inversion/candidates | Phase 11 transform budgets, inversion, causal feature gating tests | No actual optimization search unless an approved discovery plan exists |
| 18 | Chronological split/OOS | Phase 10 experiment/embargo/cluster/repeated holdout tests | Signed dataset and pre-registered untouched OOS assignment required for empirical confirmation |
| 19 | Arena ledger/restart/cash | V2 journal partial, replace, exact microUSD, restart and multi-portfolio tests | Dedicated isolated archive location; source authority false |
| 20 | Risk durability | new `risk_bridge` tampered replay/cooldown/failed append tests and V2 single-event audit | No external panic injection run; demonstrate cold reopen and no duplicate risk |
| 21 | Mock collector/orchestration | Phase 13 read-only queue/reconnect/partial source and mock rate-limit tests | Network transport disabled by default; no collector launch |
| 22 | Reporting/provenance | Phase 10–14 net/gross/fee/rejection/pending/version/config SHA and model controls | Missing P&L NULL; no synthetic→authenticated upgrade |
| 23 | CPU/RAM/disk/throughput | command wall duration captured; resource benchmarks only with `-IncludePerformance` | Peak RAM/CPU/throughput BLOCKED until explicit safe monitor and benchmark design; no invented results |
| 24 | Auditable logs | separate command logs + `results.json`, `summary.json`, `resolved_lockfile_sha256.json` | SHA256 per log; maintain FAIL/BLOCKED after abrupt command exception; status never release ready by default |

## Invocation template — future operator only

PowerShell (do **not** run this during Phase 14):

```powershell
powershell.exe -NoProfile -NonInteractive -File .\scripts\validate-phoenix-locally.ps1 `
  -RepoRoot 'E:\PhoenixTooling\stage\phoenix-rust' `
  -OutputDirectory 'E:\PhoenixResearch\isolated_validation' `
  -ExpectedSha '<REPLACE_WITH_VERIFIED_PHASE14_40_HEX_SHA>' `
  -ExpectedBranch 'codex/phase14-integration-freeze-v1' `
  -ExecuteValidation
```

The paths above are **illustrative** research-only staging locations, not verified current filesystem locations. The actual operator must first verify that they exist, are isolated and do not resolve through junctions to protected source roots. Future tasks with original historical data require explicit flags and the relevant bounded original source files; the command above deliberately does not scan those inputs.

## No-go and missing approvals

- `-IncludeOptionalHft` changes the toolchain to 1.91.1 for the optional pinned backtest crate only. It does **not** install or execute HFT services.
- `-IncludeStress` authorizes only the bounded million-ID source test, not an unbounded disk scan.
- `-IncludeOriginalSourceAudit` requires six exact source ZIPs and checks the original catalog SHA before invoking a narrow original BTC audit. Do not infer book completeness from matching ZIP members.
- `-IncludeMultiMarketReplay` requires a saved plan, previous report (or '-'), and explicit ZIP paths. Its saved source plan must match the catalog SHA, strategy version and market identity.
- `-IncludeHypothesisResearch` does not automatically run model optimization. It records BLOCKED where actual experiment/OOS evidence and independently approved bounded search plans are absent.
- The script does not launch collectors, mutate original sources, or place exchange orders. A completed local synthetic test suite does not authorize trading or release.
- Physical power-loss durability, real Perps schema compatibility, complete market source, independent settlement, original strategy parity and authentic OOS results remain separate external evidence gates.

**Final matrix status: PREPARED_NOT_RUN.** No PASS values from this plan should be interpreted as actual current test outcomes.

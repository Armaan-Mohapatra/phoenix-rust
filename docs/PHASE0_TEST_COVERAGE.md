# Phase 0 comprehensive integration CI inventory
**Single consolidated source of test coverage**: `.github/workflows/phase0-consolidated.yml`.

The root workspace declares nine Rust crates; root tests run on both Ubuntu and Windows. The 16 standalone `integrations/*/Cargo.toml` packages are exhaustively checked for presence by CI before compiling any suites. Suites run with `--all-targets` on both operating systems. The optional HftBacktest package is exercised with `--features upstream-hft` (backtest-only feature).

| CI job | Tested code |
|---|---|
| Manifest coverage | 16 standalone package names, root and standalone Cargo metadata |
| Root workspace (Win+Linux) | domain, sim, research, cli, archive, settlement, strategies, pipeline, l2; Clippy analysis |
| Storage (Win+Linux) | parquet-batch, durable-archive, disk-identity, indexed-archive, paper-journal |
| Historical (Win+Linux) | kalshi-read-only, kalshi-wire, historical-input, source-loader, real-btc-replay, real-ledger-parity, vertical-fixture; Windows launcher parser |
| Quantitative (Win+Linux) | source-ports, risk-guard, performance-report, pinned HftBacktest queue/latency adapter |

CI is **synthetic-fixture only**, except for previously pinned minimal archived source projections. It does not access private raw ZIP archives, authorized Windows drives, licensed CF primary data or active market collectors. A passing matrix is an integration-baseline result, not historical backtest or deployment evidence.

Formatting check is not included as a gate because the existing compact prototype code is not rustfmt-normalized; broad source reformatting would obscure the functional Phase 0 reconciliation. No formatting result is claimed. Feature changes and tests are strictly limited to existing implemented modules.

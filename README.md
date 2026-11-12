# Phoenix Rust

An **offline-first, Rust-only prediction-market research and execution-simulation workspace**. The repository contains no real-order execution API, trading credentials, background collector installation, or production database access.

**Status:** development prototype. Synthetic tests passing on Linux and Windows do **not** establish a profitable strategy or a production-ready data collector.

**Development freeze checkpoint:** [2026-11-12 source-backed snapshot](docs/CHECKPOINT_2026-11-12.md). This is documentation, not an implemented release.

**Latest post-checkpoint development:** [mission continuation, PR links and verified limitations](docs/CONTINUATION_2026-11-12.md). New experimental crates are on separate stacked draft PRs; no merge or production deployment.

## Implemented

| Crate | Capability |
|---|---|
| phoenix-domain | Binary YES/NO books, fixed-point fees, causality, sequence status, evidence-tiered settlement P&L |
| phoenix-archive | Bounded, versioned and CRC32-protected research frames; detects truncation/corruption |
| phoenix-settlement | Strict equal-sample index window comparisons; missing/duplicate observations fail closed |
| phoenix-l2 | Subscription-scoped snapshot/delta reconstruction, sequence gap resets, fractional-size rejection |
| phoenix-sim | Depth-limited taker simulation and conservative maker queue hypothesis |
| phoenix-strategies | Immutable version hashes, market-implied/model/inverted/ensemble hypotheses and no-trade reasons |
| phoenix-research | Chronological splits, embargo, Brier diagnostics, event-level aggregation, in-memory paper accounting |
| phoenix-pipeline | Integrates input integrity, strategy choice, simulated execution, settlement and report states |
| phoenix-cli | Small **synthetic** trade-to-payout smoke demonstration |

Two isolated optional integrations: `integrations/kalshi-read-only` (public HTTP GET only; no order methods, no credentials) and `integrations/parquet-batch` (bounded in-memory Arrow/ZSTD Parquet batch, SHA256/count verification; **not a durable archive publisher**). Tests are synthetic; the HTTP adapter never calls Kalshi from unit tests.

## Test

- cargo test --workspace --all-targets --offline
- cargo run -p phoenix-cli --offline
- cargo test --manifest-path integrations/kalshi-read-only/Cargo.toml

The separate HTTP adapter test fetches third-party *build dependencies* when first built, but never makes a Kalshi API call from its unit tests.

## Strict boundaries

- All fills are **modeled**, not recorded from an exchange. No live-order code exists.
- A venue-final outcome is **not** independently corroborated until matched to an independent primary evidence source.
- Archived source completeness is an external prerequisite. No engine may assert complete history based on a synthetic fixture.
- Markets with scalar/void/fractional payouts, unknown fee rules or incomplete books are not eligible for binary integer replay.
- Settlement-index windows are configurable; a synthetic 60-sample fixture is not itself a verified Kalshi settlement rule.
- The HTTP adapter is a read-only library, **not** an operational WebSocket collector or durable market-data pipeline.
- Never commit raw market archives, API credentials, private strategy records, or local runtime files.
- The existing collectors and paper-trading processes are unrelated and remain untouched.

## Outstanding phases

Actual sealed Parquet collection, authenticated read-only WebSocket ingestion, source SHA256 proof, independent reference-index settlement matching, full replay with measured latency and partial fills, frozen historical strategy adapters, true untouched out-of-sample tests, persistent accounting and resource-safe operations are **not implemented**.

See docs/RELEASE_GATES.md for acceptance criteria and docs/UPSTREAM.md for proposed external library adoption. Only explicitly reviewed MIT/Apache-2.0 code may be linked; AGPL and unlicensed projects are references only.

**Security notice:** The GitHub repository was created with public visibility. Do not import private historical Phoenix data, proprietary strategy code, source paths, or credentials until repository privacy and permissions are verified.

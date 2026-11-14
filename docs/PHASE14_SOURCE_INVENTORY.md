# Phase 14 — Exact Rust source and dependency inventory

**Inventory based on private GitHub Phase 14 branch source tree (static only).** 9 root workspace crates + 18 standalone `integrations/*/Cargo.toml` crates; 27 package manifests plus the root `Cargo.toml`. Root workspace `resolver=2`, `edition=2021`, `rust-version=1.78`. Standalone crates have independent `[workspace]`, so a root `cargo test --workspace` does NOT test all integrations. Rust build/tests NOT RUN.

## Root workspace crates

| Package path | Actual purpose |
|---|---|
| `crates/domain` | fixed-point money/price, source time and settlement types |
| `crates/sim` | existing single canonical maker/taker and historical fixed-point execution simulator |
| `crates/research` | source-stage strategy selection, experiment registry, split/metrics/statistical protections |
| `crates/strategies` | frozen strategy recipes, October recovery/import, immutable lineage and transformation proposals |
| `crates/pipeline` | offline evaluation case and modeled settlement |
| `crates/l2` | Kalshi quote book snapshot/delta representation |
| `crates/settlement` | binary final and independent reference source gates |
| `crates/archive` | root-workspace archive components |
| `crates/cli` | legacy synthetic/offline CLI entry point |

## Standalone integration crates

| Path | Scope | Important dependency |
|---|---|---|
| `contract-economics` | contract rules, effective fee source, settlement handoff | domain, sim, settlement, kalshi-read-only |
| `disk-identity` | disk redb identity index | `redb=4.5.0` exact pin |
| `durable-archive` | research-only sealed manifests, bounded publication and recovery | parquet-batch; fs2 |
| `hftbacktest-adapter` | optional upstream queue/latency comparison, **backtest only** | pinned hftbacktest Git `5f3ec40b2afb764e0fea112f941ed85523ef4e88`, optional `upstream-hft`, edition 2024, **Rust 1.91.1** |
| `historical-input` | exact original Parquet normalization, timestamp parsing, Perps configurable schemas/features | Arrow/Parquet 53.4.0 |
| `indexed-archive` | sealed archive + transactional redb identity index | disk-identity + durable-archive |
| `kalshi-read-only` | GET-only public client and offline bounded WSS decoder queue | reqwest 0.12 + kalshi-wire |
| `kalshi-wire` | strict offline Kalshi snapshots/deltas/trades decoder | domain, l2 |
| `paper-journal` | **sole** durable multi-portfolio paper event journal, V2 fractional arena and risk replay | durable-archive, sim, risk-guard |
| `paper-orchestrator` | default-off read-only source, Perps and one-cycle paper model/report routing | source-loader, historical-input, strategies, sim, risk-guard, journal |
| `parquet-batch` | finalized Parquet/manifest encoding, hash verification | Arrow/Parquet 53.4.0 |
| `performance-report` | modeled metrics adapter and Phase 14 strategy/expt hash audit | pipeline, research, strategies |
| `real-btc-replay` | narrow historical BTC archived-case model | domain, sim |
| `real-ledger-parity` | existing archived paper ledger parity fixtures | fixture CSV |
| `risk-guard` | exact money limits, duplicate and burst/cooldown, deterministic replay | pure local; no exchange code |
| `source-loader` | original ZIP/Parquet, channel/book integrity, multi-market and Kalshi/Perps joins | historical-input, kalshi-wire, sim |
| `source-ports` | independent mathematical research ports | domain |
| `vertical-fixture` | existing synthetic end-to-end research pipeline tests | older root crates, archive, settlement |

**Dependency direction:** core domain → sim/research/strategies/pipeline, Kalshi wire and historical-input → source-loader, Phase 12 journal → risk-guard+sim+archive, Phase 13 orchestrator → all relevant standalone existing libraries, performance reporter → strategies+research. Static Cargo path review found no local cyclic dependency; Cargo verification remains necessary. No alternate live-order connector was added.

## CLI entry points and scripts

- `crates/cli/src/main.rs` — core synthetic/offline CLI.
- `integrations/source-loader/src/bin/phoenix-source-audit.rs` — operator-controlled historical source audit CLI.
- `integrations/source-loader/src/bin/phoenix-original-replay.rs` — bounded original history replay CLI.
- `integrations/source-loader/src/bin/phoenix-multimarket-replay.rs` — saved market plan and previous checkpoint to JSON, no unbounded discovery.
- `integrations/source-loader/scripts/run-original-btc.ps1` — exact historical BTC ZIP/Parquet source read-only launch helper.
- `scripts/validate-phase2-recovery.ps1` — older source recovery validation helper.
- `scripts/validate-phoenix-locally.ps1` — Phase 14 newly updated **PREPARED ONLY**, operator-only isolated Phase 15 local validation campaign.

## Reproducible local toolchain and version constraints

1. The core and most standalone crates advertise Rust edition 2021; root states Rust **1.78**. Use installed `1.78.0` where supported, or record any dependency/feature incompatibility as BLOCKED rather than silently upgrading the published support claim.
2. `hftbacktest-adapter` requires **Rust 1.91.1** and edition 2024, isolated from core. Its optional upstream Git dependency is pinned at the exact revision above, with `default-features=false, features=["backtest"]`. Do not unpin or enable live/S3 features.
3. The source tree did not show a tracked `Cargo.lock` for every independent workspace. Reproducible version resolution requires saving generated lockfiles and SHA256 in the isolated validation output; otherwise classify a rebuild as **VERSION_RESOLUTION_UNVERIFIED**. Dependencies may themselves require higher MSRVs; only Cargo can verify.
4. Key dependency declarations observed: `redb=4.5.0`, `arrow-array=53.4.0`, `parquet=53.4.0`, `reqwest=0.12`, `serde=1`, `sha2=0.10`, `chrono=0.4`, `zip=0.6`, `fs2=0.4`, `tempfile=3`. Other transitive resolved versions/licensing are **not audited** until the local validation campaign.
5. Third-party upstream `nkaz001/hftbacktest` license is MIT per repository documentation. This is not a license clearance of the full transitive dependency graph. Do not remove MIT copyright/attribution on any permitted distribution.

## Authored regression source (NOT RUN)

Existing modules include synthetic fixture tests for domain/sim, original L2/session/sequence integrity, archive corruption and recovery, redb million logical identity stress, historical BTC source, fee/settlement constraints, Perps exact decimal, frozen registry/OOS/transformations, paper cash/restart, risk durability and orchestration backpressure. Phase 14 adds explicit risk replay/fault tests, exact fill reconciliation, provenance mismatch tests and strategy-report config lineage test. No success/failure result has been obtained for the Phase 14 code.

## Evidence/source requirements

- Original verified ZIP central member digest, original partitions/manifests, historical source session/snapshot/sequence continuity and meaningful arrival latency.
- Original effective-dated fee schedules and exact real contract terms.
- Kalshi official finalized outcome via authenticated read-only source **and** independently sourced index/primary reference with consistent rules/time.
- Original Perps venue/schema/instrument/unit fields; not inferred from generic hypothetical fixtures.
- Complete October frozen strategy configurations (only **11 version IDs** and source identity are established, all **ORIGINAL_PARTIAL_DEFINITION**); November versions cannot be assumed equal.
- Pre-registered discovery/validation/untouched OOS experiment markets and full negative/flat/abstain accounting.
- Actual Windows filesystem hard-link/ACL/reparse behavior, power-loss evidence and bounded hardware resource profiles.

## Safety defaults and limits

The new orchestrator defaults to synthetic fixture mode, disabled read-only network, 2,048-message bounded queue, a configured 128 MiB budget, configured 512 MiB research disk limit, three retries and no live order route. These budgets are *requested software configuration*, **not measured working-set guarantees**. V2 modeled paper order history is bounded to 4,096 orders per portfolio. Existing paper journal still uses legacy `durable-archive` (NOT automatically the new indexed archive) and requires isolated output directories; no production migration is authorized.

**Current result:** SOURCE_INVENTORIED_UNTESTED. Do not claim machine compatibility, backtest performance or release readiness from this document.

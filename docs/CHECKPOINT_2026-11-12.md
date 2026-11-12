# Phoenix Rust development checkpoint — 2026-11-12

**Freeze point (code):** `fba3b6e3fd8f9391ffe4ada330ff245fda7982d8` on `codex/rust-foundation-v1`. Documentation-only checkpoint follows this SHA. This is a source-backed snapshot, **not a release, deployment approval, or proof of real trading profitability**.

## GitHub state at freeze

- Repository: https://github.com/Armaan-Mohapatra/phoenix-rust — **PUBLIC** (must be changed to PRIVATE before importing confidential Phoenix sources).
- Default `main`: `de4d23db000509dd5ebd5916b10aa821308c6a45` (initial README only).
- Working branch: `codex/rust-foundation-v1`, code head `fba3b6e3fd8f9391ffe4ada330ff245fda7982d8`.
- PR: https://github.com/Armaan-Mohapatra/phoenix-rust/pull/1 — open **DRAFT**, not merged.
- Previous clean-slate incubation work: https://github.com/Armaan-Mohapatra/phoenix-agent-arena/pull/4; do not merge, deploy or treat as independent production installation.
- No production process, collector, active database or real-money trading operation is changed by this work.

## Implemented and tested — scope is synthetic/offline

| Source | Verified capability | Limit |
|---|---|---|
| `crates/domain` | fixed-point YES/NO price/depth, caller-supplied fee formula, receive-time causality, subscription/session sequence status and evidence-tiered binary accounting | no full Kalshi contract/rule/date-dependent fee registry |
| `crates/archive` | 1 MiB bounded frame encoding, CRC32, corruption/truncation detection | **not** durable journal, signed source proof or fsync-safe archive |
| `crates/settlement` | exact-sample configurable index-window sums with missing/duplicate rejection | no official rule parser, authoritative index fetch or independent source matching |
| `crates/l2` | synthetic subscription-scoped snapshot/delta aggregation; gaps invalidate books; fractional quantities are rejected | no authenticated WSS consumer, historical continuity proof or fractional replay |
| `crates/sim` | displayed-depth taker sweeps; cautious maker queue-ahead hypothesis requiring caller-attested complete tape | no observed matching-engine priority, real latency estimates, queue-cancel reconstruction or fill validation |
| `crates/strategies` | synthetic frozen version/config hashes, no-trade, market-implied, model edge, inverse, weighted ensemble and abstention | none of the legacy strategies imported or independently reproduced |
| `crates/research` | split/embargo helpers, event-level P&L sums, Brier metric and in-memory modeled paper ledger | no formal OOS registry, inference/bootstraps or persistent ledger/recovery |
| `crates/pipeline` | synthetic case → decision → modeled depth fill → pending/venue-final settlement state, rejects asserted unsealed/gapped data | “source sealed/complete” assertions are supplied by caller, not established by collector |
| `crates/cli` | end-to-end **synthetic-only** trade-to-payout smoke | not a real historical backtest |
| `integrations/kalshi-read-only` | HTTPS GET-only public market/orderbook adapter, URL and input validation tested offline | no operational market discovery loop, WSS, credential use or collector |
| `integrations/parquet-batch` | Arrow/ZSTD Parquet memory-batch round-trip, bounded input, SHA256 and count check | **not** atomic durable publisher; no independent manifest attestation, crash recovery or finalized market ingestion |

**Interpretation:** Workspace `crates/*` includes **nine Rust crates**; separate integrations contain **two additional Rust packages**. Tests verify specific fixture invariants, not integrated operational readiness.

## Latest code commits

- `fba3b6e` — isolated bounded ZSTD Parquet batch and SHA256 verification adapter (last pre-checkpoint code commit).
- `11c48eb` — architecture/upstream/security/release-gate docs.
- `ef9a124` — L2 snapshot/delta reconstruction.
- `ae65ae8` — offline decision-to-settlement replay pipeline.
- `17f4de4` — GET-only Kalshi HTTP adapter.
- `11a350e` — strategy/version gate.
- `2ca7f03` — index-window settlement fixture.
- `007b978` — archive frame codec.
- `0633e42`, `3f69778`, `79d3c82`, `135fb23` — foundation, simulation, research and CI.

## CI verified at code SHA fba3b6e

- Rust offline tests (Windows **and** Ubuntu): https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43247890493 — **success**.
- Optional read-only HTTP adapter (Windows **and** Ubuntu): https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43247889953 — **success**.
- Arrow/Parquet batch adapter (Ubuntu only): https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43247889883 — **success**.
- No CI result establishes live API connectivity, realistic execution, historical settlement matching, complete raw capture or a full end-to-end replay on actual Phoenix data.
- Local Windows native builds were not verified in this checkpoint; earlier workstation verification reported MSVC/Windows SDK missing.

## Immediate work at freeze

The last substantive implementation was the bounded Parquet writer/reader adapter at `fba3b6e`; immediately prior to checkpoint, the active focus was **historical data durability and sealed archive construction**. It passes synthetic checksum/roundtrip tests but has not implemented temp write + flush/fsync + atomic rename, committed manifests, restart recovery or provenance sealing. Continue from this gap rather than introducing more strategy variants.

## Remaining phases and rough engineering effort

Estimates are **engineering effort, not guaranteed calendar delivery**, assuming one experienced Rust developer, usable specs/data and prompt access to tests; blocked data/license issues can extend them substantially. Some work may overlap.

| Phase / GitHub tracker | Remaining implementation and acceptance evidence | Approx. engineer-days |
|---|---|---:|
| Security [#2](https://github.com/Armaan-Mohapatra/phoenix-rust/issues/2) | Switch repository to private; verify roles, secrets policy and branch protections | 0.5–2 (owner action needed) |
| Data ingest/archive [#3](https://github.com/Armaan-Mohapatra/phoenix-rust/issues/3) | authenticated read-only Kalshi WSS, REST pagination/rate control, session-aware sequence handling, bounded queues, journal, durable sealed Parquet/manifest, crash and disk-full tests | 10–20 |
| Independent settlement [#4](https://github.com/Armaan-Mohapatra/phoenix-rust/issues/4) | effective-dated rules/fee catalog, exact market-ID joins, verified independent reference-index observations; scalar/void handling | 8–18 |
| Historical execution replay [#5](https://github.com/Armaan-Mohapatra/phoenix-rust/issues/5) | chronological event replay, queue/depth, cancel/partial fill, measured latency/adverse selection, optional MIT hftbacktest parity | 12–25 |
| Strategies and OOS [#6](https://github.com/Armaan-Mohapatra/phoenix-rust/issues/6) | freeze/import legacy source/config, baseline parity, inversions/regime filters, event-purged splits, multiplicity/uncertainty and after-cost stress | 10–20 |
| Persistent paper ledger [#7](https://github.com/Armaan-Mohapatra/phoenix-rust/issues/7) | event-sourced durable multi-portfolio accounting, idempotent recovery and reconciliation | 8–16 |
| Operations/release [#8](https://github.com/Armaan-Mohapatra/phoenix-rust/issues/8) | resource/throughput benchmarks, fault injection, telemetry, deployments reviewed separately | 8–15 |
| Perps + legacy data adapters (untracked subwork) | read-only schema transforms, timestamp/sequence/identity validation and legacy manifests, historical settlement and book backfill | 5–12 |

Total sequential effort approximately **62–128 engineer-days**, before unpredictable data access, scope changes and independent forward validation. Do not interpret as a promised delivery date.

## Historical data compatibility

**NOT YET COMPATIBLE END TO END.** Existing historical Kalshi and Perps datasets are not being read by this Rust project. No conversion, schema parity, field-level evidence, or replay benchmark was verified against actual finalized Phoenix Parquet, SQLite exports, JSON manifests, prior ZIP archives, or Perps books. The new data model assumes a supplied binary market ID, snapshot/delta content, timestamps and fee/rules hashes; it does not yet translate legacy field names, raw WebSocket types, fine-grained decimal units, Perps orderbook semantics or independently verified settlements. No claim of 147M events replayed is justified.

## External repository integration status

**0 of the 25 previously researched GitHub source repositories are vendored, linked as dependencies or running as integrations.** The adoption matrix at `docs/UPSTREAM.md` is **reference/reimplementation planning only**. MIT hftbacktest, Jon-Becker, Ryan's bot, flumine, pmxt and Apache-2.0 Oracle3 informed designs/tests; no direct source was incorporated. Homerun is AGPL-3.0 (architecture-only); repositories with no declared license remain no-copy references. Rust crates.io dependencies (`arrow-*`, `parquet`, `sha2`, `reqwest`, `serde_json`) are separately declared libraries, **not integration of the 25 GitHub repositories**. Prior local Python research-capability verification does not establish Rust integration.

## Critical risks / deployment no-go

1. **PUBLIC repo:** forbid importing proprietary Phoenix source, history, trading credentials or personal records until changed to PRIVATE.
2. **Evidence provenance:** a different asserted `source_id`+hash is not actual independent corroboration. Build source-authenticated provenance/identity/rules joins.
3. **Settlement gap:** no independent official/reference outcome linked to a tested historical trade; no actual rule/settlement compatibility.
4. **Execution realism:** no measured order-arrival latency, order queue position, cancel-ahead data, partial-fill parity or actual fills; incomplete event history cannot establish queue.
5. **Fee/rules correctness:** hard-coded example rates and fixed binary payouts are insufficient for effective-dated fee schedules, event overrides, fractional/scalar/void outcomes and settlement windows.
6. **Archive durability:** CRC frames and an in-memory SHA256 Parquet buffer are not an fsync-safe journal, sealed durable partition or crash-recoverable capture system.
7. **Workstation compatibility:** Windows CI synthetic success does not imply the local MSVC/SDK, memory/disk, or runtime services are deployment-ready.
8. **Research integrity:** existing backtests had zero overlap with 18 independent settlements in the supplied prior audit; no untouched OOS experiment on verified data exists here.
9. **Supply-chain/licensing:** optional third-party integration requires pinned commits, SPDX/license/NOTICE checks, feature gating and SBOM/security review; do not copy AGPL or unknown-license code.
10. **Architecture:** live collection, signal generation, replay, paper ledger, performance publisher, Perps adapters and authorized deployment remain separate incomplete features; never treat the synthetic demo as a functioning bot.

## Efficient next approach (not started)

Reuse the verified nine-crate workspace and two adapter libraries; prioritize **one** durable archive + signed/rule-evidence acceptance slice before further strategy work. Preserve one canonical market/event ID and event/receive timestamps across adapters. Use synthetic golden fixtures, sealed tiny Parquet samples and deterministic cross-module tests; reserve full replay for bounded finalized partitions. Avoid repeated repository cloning/installation, full-drive scans, duplicated CI on every prose-only change and loading entire logs into Codex context. Add hftbacktest as optional **backtest-only** crate only after resource/licensing/parity validation. Require a security gate before private source migration.

**Checkpoint only. No code changes, tests started manually, merges, production reads/writes, live trading, or deployment.**

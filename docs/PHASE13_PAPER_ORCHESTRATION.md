# Phoenix Rust Phase 13 — Offline-first read-only source and paper orchestration

**Status: CODE_WRITTEN_UNTESTED.** This is a source-development checkpoint only. Compilation, tests, collector execution, live network polling, market replay, strategy optimization, paper cycle execution, CI and any live orders were **NOT RUN**. No changes were made to the user’s original Phoenix source drives or active services.

Parent: Phase 12 draft PR #34 at `6ccd492f6e79a7c32c28e6678fe3a6e9787668ef`. Child: `codex/phase13-collector-orchestration-v1`. No earlier draft PR, branch or `main` is merged or rewritten.

## Reused interfaces

- `integrations/kalshi-wire::decode_archived` remains the existing strict WSS/historical source decoder; the new stream component does **not** duplicate financial/market decoding or create an exchange order API.
- `integrations/kalshi-read-only` retains the existing bounded HTTPS GET-only public-market/market/orderbook client and its manual client rate limiter. This phase adds a separate **offline** stream normalization and bounded queue, not an actively running WebSocket collector.
- `integrations/historical-input` + Phase 8 Perps adapters remain the source-schema, exact-decimal and causal feature normalizers. No original Perps private payload columns are fabricated.
- Phase 7 `source-loader` historical evidence and Perps/Kalshi market identity bridge, Phase 9 `phoenix-strategies::frozen_registry::decide`, Phase 6 `phoenix-sim::historical` modeled execution, Phase 12 `paper-journal::risk_bridge` and `arena_v2`, and `phoenix-durable-archive` remain the authoritative implementations. No separate strategy simulator or durable paper ledger was created.

## Actual code delivered

### Offline Kalshi ingestion and queue

`integrations/kalshi-read-only/src/stream.rs` defines `ReadOnlyStream`, `StreamConfig`, `WireObservation`, source-stamped classification, subscription health and shared connection+SID sequence accounting. It checks event identity, receipt time freshness, malformed/oversized messages, duplicates, conflicts, gaps, backpressure and bounded memory. Gaps are quarantined; a new source snapshot may reset local state but remains **PROVISIONAL** without independently verified full subscription continuity. Reconnect clears local sequence state and requires resnapshot. Trade prints are **not** automatically designated maker-queue-consuming trades.

### Modes and explicitly disabled transport

The new standalone `integrations/paper-orchestrator` crate defaults to `SyntheticFixture` and `isolated_network_enabled=false`. Other defined modes are `FinalizedHistory`, `IsolatedReadOnly`, and `PaperOnly`. `ReadOnlyTransport` is a caller-supplied **poll-only** interface, lacking order submission. Its `poll_once` is disabled unless `IsolatedReadOnly` mode AND an explicit operator opt-in are provided. There is no automatic startup, schedule, trading service, credential management, long-running loop or production collector control. Polling errors use bounded attempts, explicit next-retry timestamps and finite exponential backoff. Process restart requires a new source snapshot; saved status cannot claim continuous connectivity.

The default conservative laptop configuration is a **2,048-message queue**, **128 MiB configured RAM budget**, **512 MiB research-disk budget**, **three retries**, one-second initial retry delay and five-second quote freshness. These are configuration limits or requested budgets, **not measured total-process memory guarantees**. No collector was launched.

### Read-only archive boundary

`publish_read_only_observation` is an **explicit optional** caller API using the preexisting `durable-archive` writer. It validates source event identity, hash, timestamps, dedicated research output path and a caller-supplied SHA-tagged disk-usage budget. Production roots such as `E:\TradingNextData` and `E:\KalshiData` are rejected before opening an archive. No automatic publishing is enabled, no source root is modified, and the user must independently verify original source data, safe path semantics and remaining disk usage before any later authorized execution.

### Perps service and causal join

`paper-orchestrator::perps_service` delegates finalized Parquet/SHA/schema validation and exact decimal normalization to the Phase 8 `historical-input`. A bounded `CausalPerpsBuffer` stores verified-shape observations and allows timestamped joins only when receipt precedes a Kalshi decision, with data-source SHA and session metadata retained. Checkpoint restore uses the Phase 8 SHA-based resumable format; it is not provider authentication. Actual Perps private schema compatibility remains **UNVERIFIED**.

### Single-cycle paper-only execution

`paper-orchestrator::paper_pipeline::run_cycle` is a caller-controlled one-cycle path:

Source/market identity → Perps feature SHA/decision matching (when required) → Phase 9 frozen strategy/control decision → source gating → Phase 12 risk preflight → durable V2 paper order → Phase 6 modeled execution → durable modeled fills, cancellation or expiry → pending settlement. No result is an actual venue fill, and no exchange order connector exists.

The integration retains strategy version, market/event, decision/receipt times, selected L2 SHA, contract-rules SHA, historical fee schedule SHA and assumptions. Missing original-source eligibility, missing/foreign Perps features, unverified source book, missing fee or mismatched execution hashes block or cancel a modeled cycle. The journal never auto-settles an unknown market.

## Tests authored — **ALL NOT RUN**

Source tests: mock snapshot/duplicate, gap/reconnect/resnapshot, backpressure, shutdown, no exchange-order route. Orchestrator tests: default-off transport, mock rate-limit/backoff and retry exhaustion, dangerous production output roots, bounded disk quota, restart requiring new snapshot and no live routes. Perps-service tests: empty source coverage and tampered restore version. Paper pipeline: synthetic safety flags and no actual order submission. Previously authored Phase 6–12 integration tests remain unrun.

## Known source and runtime limitations

1. Static source review only. Rust/Cargo version compatibility, type correctness, isolated workspace dependencies, Windows paths, memory/disk behavior and crash consistency must be proven in Phase 15.
2. The new queue is an **in-memory decoder/dispatcher, not a deployed collector**. Live WSS subscriptions, REST resnapshot coordination and independent full-feed session completeness are not authenticated or proven; a source gap is not silently repaired by a local snapshot.
3. `run_cycle` only produces paper-modeled executions from existing evidence; maker tape completeness and actual original-source execution eligibility remain externally blocked. Perps source schema/version fields and authentic Kalshi fee schedules/settlement evidence are unverified.
4. The canonical V2 journal persists modeled order/position state. Risk-guard burst/cooldown state is **not yet durably reconstructed across restarts**, and preflight plus durable append do not form a single atomic transaction. Phase 14 should resolve this before claiming reliable unattended paper mode.
5. Phase 12 modeled settlement claims require external official/primary provenance verification; they are not authenticated results. Full transaction invalidation after realized settlement is not supported by silent history rewrite.
6. The new synthetic/orchestration tests have not been executed. No actual strategy outcomes, profits, statistical significance, paper performance or throughput measurements exist for these phase changes.
7. `RiskGuard::new_micro` preserves the old 1e-4 USD API and converts configured limits using checked ×100. Phase 13 additionally replaces legacy `Option::is_none_or` uses with stable `map_or` to avoid known workspace MSRV syntax mismatch; this is still uncompiled.

**Handoff: stop after Phase 13.** The next separately authorized source-development step is Phase 14 — final integration/reporting/security review and feature freeze — from the **independently verified final Phase 13 PR HEAD**. Phase 15 compilation/replay/testing requires separate authorization and must not be performed as part of this batch.

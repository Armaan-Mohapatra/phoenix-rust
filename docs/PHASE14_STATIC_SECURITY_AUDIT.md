# Phase 14 — Static source-integrity, durability and security review

**CODE_WRITTEN_UNTESTED.** This is a source-level audit of GitHub code; no Rust compile, Cargo test, rustfmt, Clippy, simulation, GitHub Actions, Windows filesystem operation, collector, production DB access or exchange operation was performed.

## Highest-risk repairs authored on Phase 14 branch

### 1. Durable risk decision and restart replay

Prior `risk_bridge::preflight_and_append` mutated a stateful `Guard` before calling `Journal::append`. A journal publication error or crash could leave duplicate-intent, large-burst or cooldown state disconnected from the sole durable paper ledger.

The repair introduces `arena_v2::ActionV2::RiskAssessed {intent,audit}` as ONE event containing the complete risk decision + proposed modeled order or rejection, exact microUSD exposure/fee/quote/depth inputs, result code and full configuration identity. The existing journal persists it as one sealed event before updating the caller's in-memory guard.

`risk_bridge::replay_guard_from_journal` reconstructs risk decisions from an empty same-limits guard, verifies decision codes, order/rejection identities and configuration equality, and rejects incomplete legacy/direct-V2 risk histories. Guarded submit always rebuilds from durable records BEFORE assessing a new order; successful journal append updates the caller guard only afterward. The risky pre-append guard mutation was removed. Existing `ActionV2::Submit/Reject/Replace` remain for synthetic/legacy replay, but mark risk coverage incomplete; no subsequent guarded submission can silently bypass this.

`Journal::append` latches an ambiguous archive publication outcome and forbids further appends until independently reopened and replayed. This is a **logical replay/commit boundary**, not proof of atomic power-cut durability on Windows.

Regression source includes: pre-append failure, restart after a durable commit with old guard discarded, deterministic replay/idempotence, tampered stored verdict, incomplete legacy risk history, same-unit guard identity, and synthetic poison-latch/reopen. **All tests NOT RUN.**

Residual: A directly authored `RiskAssessed` record carries caller-supplied values; external source/root authenticity still matters. Replay checks deterministic internal self-consistency but cannot cryptographically attest a source or actual market depth. 4,096 lifetime risk identities/order history limit; no silent pruning/rotation. A hardware crash/failure-injection campaign is required.

### 2. Source and strategy provenance

Previously `paper_pipeline::base` used `strategy.canonical_sha256().unwrap_or_default()`, silently turning an invalid frozen strategy identity into an empty version hash. It now returns an error on invalid canonical identity.

Other explicit integration guards include exact market/event/series match, decision time before observation, source manifest match when not synthetic, perps joined-market/decision/row SHA and clock checks, exact historical contract-rules SHA, selected L2 quote SHA, fee schedule SHA and modeled report identity. A failed or unauthenticated gate produces a blocked/no-trade modeled status, not a verified venue fill. Legitimate synthetic cases remain labeled synthetic.

`arena_v2::modeled_fill_actions` now checks aggregate submitted vs filled quantities, total modeled cost/fee, exact price×hundredths quantity scaling, nonnegative fees and source hashes. Legitimate no-fill reports have no invented quote-source SHA and no fee; the paper pipeline journals expiry without phantom cash/P&L. A report with inconsistent fills cancels the proposed order rather than silently posting modeled fills.

**Unresolved authenticity limit:** Source SHA strings and `SourceWindowEvidence` Boolean declarations identify expected evidence; they do not constitute external proof of complete original WSS subscription, maker tape, independent reference settlement, or official fee schedule. Phase 5/6 source gates remain conservative; the original Perps private schema remains unknown.

### 3. Reporting and experimental isolation

The canonical `phoenix-research::experiments` module remains the metrics/OOS owner. `performance-report::experiment_report::summarize_with_strategy_definition` binds the experiment to the frozen strategy's exact ID and config SHA, retains definition SHA, origin, parent lineage, model version, original source manifest and fee/risk/execution SHA. It leaves source authentication and untouched-OOS independent proof **false**, regardless of a caller label. Pending/unknown P&L stays NULL and no actual exchange-order submission is asserted.

The Phase 9 importer recovered 11 named October *versions* but no complete original executable definitions. Source-level strategy mutation/reconstruction uses explicit derived lineage. Phase 11 candidate transformations remain exploratory and are not proof of a profitable inverse.

### 4. Archive mode, publication and filesystem constraints

The existing paper journal still stores events in `phoenix-durable-archive` using its sealed-manifest/Parquet writer. **It does NOT automatically switch to `phoenix-indexed-archive` or redb**, and cannot claim indexed-scale capacity or verified million-identity behavior. The indexed archive uses immutable sealed Parquet evidence followed by transactional redb identity mapping. Indexed and legacy growth/performance must be tested independently.

Source review of archive modules shows research-only marker/path checking, no-clobber creation via hard links, manifest/Parquet hash verification, orphan quarantine, source-replay recovery gates and Windows reparse-point checks. This is not security proof against an attacker racing directory substitution (TOCTOU); no trusted handle-relative directory confinement or atomic power-loss transaction across OS+external USB disk was established.

An operator must use an isolated, known research volume, deny write access from untrusted users, verify same-volume hard link support, and never write under `E:\TradingNextData`, `E:\KalshiData`, `C:\KalshiRuntime` or an active collector root. Writing to a newly created *research* folder on E: is not authorization to modify source data elsewhere.

### 5. Network, credentials, collector and order interface

`kalshi-read-only` is GET-only with a client-level minimum request interval, no exchange-order verbs. The Phase 13 queue is offline-decoder-only; a provided `ReadOnlyTransport` trait does not instantiate a live WSS collector. Network polling requires explicit `Mode::IsolatedReadOnly` AND `isolated_network_enabled=true`, and is never started during construction. The default mode is `SyntheticFixture`. Order orchestration uses only `phoenix-sim::historical` and `phoenix-paper-journal`; no exchange order entry route exists in these authored integrations.

Optional `hftbacktest-adapter` enables pinned backtest-only upstream models, **never live/S3**. Rust 1.91.1 and the pinned upstream revision must be independently reproduced before comparison; no historical maker performance was measured. Third-party MIT attribution applies. No repository secret/credential scan or full transitive license audit was run.

### 6. Numeric, resource and failure safety

Fixed-point money units:
- legacy domain/journal/risk: 1/10,000 USD;
- historical execution/V2 journal: 1/1,000,000 USD;
- exact order quantity: hundredths of one binary contract;
- exact conversion: legacy amount ×100, checked; no monetary f64 conversions.

V2 journal order/risk-history growth now capped at 4,096; original journal still clones proposed state on every append and reconstructs durable history at cold open. This can be expensive and must be benchmarked. Orchestrator's 2,048-message source queue, 128 MiB configured RAM and 512 MiB configured disk limit are **not runtime-measured process memory/physical disk guarantees**. Original input selection is bounded by explicit ZIP/index/Parquet member lists; no recursive C:/E: scan or active WAL access was performed.

## Current material blockers

- Build/type correctness, core Rust 1.78-compatible dependency resolution, optional hft 1.91.1 build, Cargo feature graph, and security/lint audit **NOT RUN**.
- Windows NTFS/reparse/hard-link and physical abrupt-power-loss durability **NOT TESTED**.
- Archived frozen 11 strategies lack complete original entry/exit/risk config; no true original-vs-Rust decision parity.
- Authentic official/independent settlements, fee rules, market-source completeness, Perps schema/units, full maker tape, historical execution, untouched OOS and real after-cost performance **UNVERIFIED**.
- Durable risk protocol has synthetic tests authored but no actual crashed-process/failed-fsync test. A validation failure must not be treated as permission to deploy.
- Journal's per-append state clones and risk replay are bounded but may be expensive; actual CPU/RAM/disk/throughput profile unknown.

**Source-security conclusion:** No production or exchange operation was performed. The new source is paper/research-only and conservative by design. It is **not** externally validated security, storage durability, real-world execution fidelity or release readiness.

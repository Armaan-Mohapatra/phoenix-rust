# Phase 12 — Event-sourced micro-USD fractional Paper Arena (V2)

**STATUS: CODE_WRITTEN_UNTESTED.** No Cargo/compilation/tests/CI, live/real trades, collector activation, production file writes or original SQLite/WAL access. Parent Phase 11 PR #33 `3d25d7390130144aad425ab54650692fd07e884d`. Child `codex/phase12-paper-arena-v1`.

## Single existing durable ledger, versioned schema

Original `integrations/paper-journal/src/lib.rs` uses `phoenix-durable-archive::Archive` and serializes signed 128-bit monetary amounts as decimal strings. The new code **does not create another paper archive or SQLite ledger**. Instead, it extends the existing `Event` with `OpenV2` and `ArenaV2` variants and extends the same `PortfolioState` with an optional V2 state. Legacy `Open/Buy/Settle/Invalidate/NoTrade` byte representation and 1e-4 USD money definitions remain unchanged; legacy and V2 events cannot mix within a portfolio. Every V2 event passes through existing `Journal::append`, which validates a cloned proposed state before durable archival, then updates in-memory state only when publication succeeds. Restart replay reconstructs both legacy and V2 state by persisted sequence/time/identity.

## Exact historical fill accounting

`arena_v2::PortfolioV2` works in **micro-USD = 1e-6 USD** and **hundredths of a contract = 1e-2 contract**. Multiplying `price_ticks(1e-4 USD)` × `quantity_hundredths(1e-2)` is exactly micro-USD. No floating point, truncation, whole-contract rounding, or reinterpretation of old journal records. Every cash/position/reserved/reconciliation operation uses checked `i128`.

- Submit: source/strategy/fee/rules hashes and clock/expiration, reserve worst-case limit × quantity + explicit fee budget (not an unknown zero assumed), check cash.
- Reject: immutable risk rejection with reason; no reservation.
- Fill: unique fill IDs, source/fee identity, limit-price condition, remaining quantity, partial/full position basis, exact fee, release any price improvement and unused reservation. Atomic reducer prevents partial mutation on rejected events.
- Cancel/expire: release only remaining reservation and preserve existing partial positions.
- Replace: one journaled atomic replacement, old unfilled remainder released and new order reserved, original filled lots retained.
- Reduce: modeled closing proceeds less exit fee, exact cost-basis allocation and realized P&L; fractional basis that cannot be represented exactly in micro-USD fails closed instead of rounding silently.
- SettleClaim: requires a nonempty historical outcome claim and 64-character source/rules digests; payout is exactly 10,000 microUSD ticks per hundredth contract on winning YES/NO. **Journal cannot authenticate the official Kalshi source**: this is *modeled claim settlement*, never a verified official result, and must not be used as positive research evidence without Phase 5 independent source authentication. Unknown/missing result remains pending.
- Invalidate: compensating reverse of **unclosed** modeled fills and reservations with no profit, reinstates cash and retracts previously modeled fees. Previously realized exits cannot be silently rolled back.
- Reconcile: checks `available + reserved + invested basis = initial + realized`, reserve sum across orders, cash nonnegativity and ID/status consistency; `unrealized_net_micro()` remains NULL without contemporaneous price evidence.
- All source outcomes are labeled modeled; no authenticated venue fill claim is exposed.

`arena_v2::modeled_fill_actions` converts Phase 6 exact fractional `ExecutionReport` fills into journal V2 fill events with stable IDs, cost equality and exact 1e-4 USD fee→microUSD ×100 conversion, while preserving quote/fee SHA. It does not submit orders or assume actual execution.

## Existing risk-guard integration

Existing `integrations/risk-guard` now has an isolated `new_micro` constructor that converts **all** configured fixed-money risk limits ×100 using checked arithmetic. Existing legacy Guard defaults and public APIs remain intact. New `paper-journal::risk_bridge::preflight_and_append` checks V2 ledger/account units, current market and portfolio invested/reserved exposure, stale quotes, duplicate intents, configured single-order/portfolio/market limits, burst/cooldown and explicit depth requirement, then submits or journals rejection. Direct low-level `ArenaV2::Submit` remains available for deterministic offline historical event replay; **Phase 13 orchestration must use the guarded path**.

## Authored tests — NOT RUN

- Fractional partial/whole cost and exact reserve/cash improvement; cancellation/expiry and replacement; duplicate fill idempotence; insufficient funds and rejection; missing settlement keeps P&L pending; claim-only settlement not authenticated; invalidation reverses unclosed modeled fees/positions; fractional basis non-divisibility rejected; multi-portfolio independence and deterministic restart via existing archive; legacy event serialization (existing regressions); risk bridge with exact micro-unit limits.
- Corrupted archive and source-hash conflicts are fail-closed by the existing durable archive verifier, but no new real filesystem corruption test was executed.

## Known limitations (not a validated platform)

- No compiler checks, unit/integration tests, synthetic replay, stress/resource benchmarks or real historical payouts executed.
- Source SHA and final-result hashes are **claims**, not independent authentication. No authenticated private Kalshi settlement or market-data continuity was imported.
- Cross-restart risk-guard sliding burst/cooldown state requires Phase 13 replay/reconciliation; V2 journal itself persists accepted/rejected order intent and positions.
- Missing original fee schedule cannot be treated as a zero-budget free fill; upstream source must supply an effective-dated verified fee schedule.
- Settlement correction after a previously realized modeled settlement requires explicit compensating amendment policy; in-place history rewrite prohibited.
- Risk preflight and journal publication are separate objects: a failed durable write may reserve a risk-guard duplicate identity in memory, so Phase 13 must rebuild the guard from durable confirmed intents after a failure. No automatic retry that could double-count fills.

Next Phase 13: default-off, read-only market-data and paper orchestration that uses the current source validators, the Phase 9–11 decision/research records, Phase 6 simulated fills and this V2 durable arena. No collector launch or live orders.

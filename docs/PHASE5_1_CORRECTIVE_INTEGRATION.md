# Phase 5.1 — Contract provenance, official identity and replay-accounting correction

**Status: CODE_WRITTEN_UNTESTED.** All new and existing tests remain **NOT RUN**. No Cargo, compilation, historical replay, benchmarks, Actions, collector changes, exchange calls, or live trading were executed by this phase.

**Base:** draft PR #26, `codex/phase5-contract-settlement-v1`, commit `d1a491c80732cec67dd6e872e917eab62af2c007`.
**Child:** `codex/phase5-1-economics-corrections-v1`. Stacked; do not merge or rewrite parent PRs.

## 1. Official Kalshi ticker identity and archived lookup

The published Kalshi [Get Market](https://docs.kalshi.com/api-reference/market/get-market) and [Get Historical Market](https://docs.kalshi.com/api-reference/historical/get-historical-market) schemas identify the contract by ticker and do not require a `market_id` field. The adapter now accepts an exact canonical ticker with an *optional* market UUID/ID and retains a separately verified archived identifier if one exists. Explicit conflicting identifiers are rejected when both are present. Exact event ticker, contract type, and same-ticker contract definitions remain distinct.

- The official response parser records nullable `market_id`, `event_ticker`, `market_type`, and `settlement_ts`. Missing `market_type` is never sufficient to assert a binary final.
- `finalized` or legacy `settled` with binary YES/NO and no provisional flag is classified as a final result. `determined`, `disputed`, `amended`, `closed`, and ongoing states remain pending. Canceled/void/scalar are never promoted to binary settlement. Missing/invalid fields fail closed or remain unknown.
- The public GET adapter exposes explicit `official_result(ticker)` and `official_historical_result(ticker)` methods. There is **no immediate automatic 404 retry**, because the client enforces a minimum interval and a second immediate request would fail the shared rate limiter. Each call is separate, operator-invoked, GET-only, HTTPS, bounded and no automatic retry.
- The local/mock parser never authenticates a final or claims actual API retrieval. Only the network method marks `authenticated_public_get`.
- The contract registry now permits a missing archived UUID; it does **not** manufacture a new ID for a ticker.

## 2. Independent benchmark provenance

A Kalshi rules-document digest must never be compared for equality against a separately sourced primary benchmark methodology digest. `validate_primary_windows` now checks source integrity and exact provider/benchmark/methodology identity, source observation and retrieval times, 1-Hz completeness, window bounds and tick hash **without requiring cross-document SHA equality**.

`SettlementProvenance` independently retains:

1. Kalshi contract rules document SHA.
2. Contract registry version SHA.
3. Primary benchmark methodology document SHA and methodology revision.
4. Primary response/export SHA.
5. Primary normalized observation SHA.
6. Official Kalshi response SHA.
7. Official retrieval, primary observation and primary retrieval times.

An independently corroborated outcome still requires an actual authenticated official GET, exact contract identity and a trusted *external* `PrimaryProvenanceVerifier`, as well as concordant complete reference windows and YES/NO winner. No CF Benchmarks observations, credentials or contract terms were fabricated or included.

## 3. Replay/L2 state and future-event counts

`BookIntegrity` now owns the single `forensic::Book` instance used for historical replay. It exposes the result of that book's single application to the replay pipeline, including negative-depth, missing snapshot and session-change errors. The replay pipeline no longer applies each message to a second mutable book.

The `future_observations_excluded` count remains callback-scoped: the source loader selects an as-of-bounded slice before callbacks run, so it **cannot count** upstream-filtered future rows. A new `future_exclusion_count_scope` marker records that limitation; a reported zero is not evidence that no later source events existed.

L2 snapshots still classify as provisional unless a separate full-channel/subscription continuity verifier establishes appropriate source completeness. Neither a visible quote nor its apparent limit-price touch proves maker-queue priority or historical execution.

## 4. Modeled accounting and nonfills

The existing price-only `modeled_expiry_pnl` interface no longer sets `modeled_fill=true`. It can calculate contract payout scenarios but returns no empirical net modeled P&L without modeled execution evidence. It explicitly records `NO_MODELED_EXECUTION_EVIDENCE`. The historical execution engine in Phase 6 must supply that missing order-lifecycle proof, not assume that a caller-chosen entry price was filled.

## Authored tests — NOT RUN

- Official documented ticker-only binary final, wrong ticker, optional archived ID, two conflicting archived IDs, `determined` pending, provisional final withheld, scalar/cancel/void, final revision conflict, and explicit archive URL routing.
- Independent methodology SHA different from Kalshi rules SHA while retaining complete source validation.
- No fabricated archived UUID and event mismatch rejection.
- Arbitrary entry price must not assert a fill or net empirical P&L.
- Single forensic application result visible through `BookIntegrity`, with previous negative lifecycle tests retained.

## Static integration review (not compilation)

Direct cross-file review checked the optional identity representation, Kalshi adapter constructor paths, versioned source and primary hash separation, L2 ownership, downstream `ReplayReport` field initialization, and absence of a second replay-book apply. This is source inspection only, **not** a successful Cargo build, rustfmt result, Clippy result or test run.

## Known unresolved risks and Phase 15 gates

- All source modifications remain uncompiled; type, API and transitive crate compatibility must be established in the isolated Windows validation campaign.
- Official historical rules and complete settlement/primary observations have not been retrieved or independently authenticated.
- Archived `market_id` and event metadata can be absent; conflicting identified sources must remain blocked rather than merged.
- No source-derived L2 book is eligible for real-world execution or maker-queue inference by itself.
- The Phase 6 execution engine must distinguish observed opposite-side depth, simulated queue outcomes, partial/nonfills and hypothetical prices.
- Real effective-dated Kalshi fee schedules, exact price rules, and primary methodology evidence remain externally blocked.
- No executable performance, profitability, positive OOS result or production readiness is claimed.

Next authorized source-development phase: Phase 6 — realistic historical execution engine. Actual compilation/testing is reserved for Phase 15, subject to separate user authorization.

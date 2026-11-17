# Phase 16B — research campaign engine (`crates/campaign`)

Paper-only, offline. No network, no exchange client, no order path.

## Pipeline

```
MarketCase (synthetic | operator-normalized JSONL, read-only)
  -> MarketCase::validate (identity hashes, ordering, books, window)
  -> CausalView::view_at(decision)       # history truncated at decision time
  -> StrategyConfig::decide              # pure fn of the view
  -> phoenix_sim::taker_buy at decision+latency, as-of book, per-level fee
  -> phoenix_domain::settle_binary per fill level (unknown winner = pending)
  -> engine::summarize (explicit units, cluster test)
  -> campaign::run (discovery -> validation+stress -> single holdout)
  -> report.json + summary.txt (atomic writes)
```

Money is integer 1e-4 USD. Floating point is used only for model
probabilities and test statistics, using deterministic IEEE-only routines
(`math.rs`) so Windows and Linux produce identical results.

## Protocol (fixed in code before evaluation)

| Stage | Data | Rule |
|---|---|---|
| Split | time-ordered | 50% discovery, 25% validation, 25% holdout; 900 s embargo (`SplitPlan`) |
| Discovery | discovery | all configs (budget-capped, default 128); keep >= 30 settled trades and z > 0; top 8 by z |
| Validation | validation | one-sided cluster z >= z(alpha/K), Bonferroni, alpha = 0.05; net > 0 under 1.5x fees and under 1 s latency |
| Holdout | holdout | evaluated once, survivors only; z >= z(alpha/M) |
| Label | — | `PaperResearchCandidate` = passed these gates on this dataset only |

The z-test runs over independent clusters (time windows; BTC/ETH/SOL markets
expiring together share one cluster), and clusters with no trade count as 0.
So the test is per opportunity, not per trade. The CLI's `--holdout-ledger`
refuses to reuse a dataset's holdout with a different grid unless the reuse is
acknowledged and recorded.

Note: `phoenix_research::experiments::clustered_bootstrap` always returns a
95% interval and only *reports* the adjusted alpha. It is not used as a gate
here.

## Strategy families (15; 101 configurations in the default grid)

NoTrade (control) · CoinFlip (negative control) · Favorite · Longshot ·
ReferenceModel · Momentum · MeanReversion · BookImbalance · VolRegimeModel ·
LeadLag · PerpBasis · Ensemble · InvertedMomentum · LiquidityFilteredModel ·
LeadAdjustedModel. Hypotheses and bounded parameter schemas are in
`strategies.rs`. Identity = SHA-256(revision + canonical JSON).

## Results (all SYNTHETIC; they validate the engine, not any real edge)

3 assets x 20 days = 5,760 markets, 518,400 snapshots, seed 2026, 2 threads,
release build, Linux x86_64, rustc 1.97.0. Strategy revision r2.

| Scenario | Planted truth | Candidates | Which |
|---|---|---|---|
| efficient | none | **0 / 101** | — (false-positive control holds) |
| lagged_quotes | maker prices off a 60 s stale reference | 8 | all ReferenceModel |
| shaded | probabilities compressed 30% toward 0.5 | 8 | Favorite (2), ReferenceModel (6) |
| peer_lead | ETH/SOL follow BTC by 30 s | **0** | LeadAdjustedModel positive but below Bonferroni threshold |

Peer-lead power check: at 60 days (17,280 markets) LeadAdjustedModel's
validation z rose from 0.7–1.3 to 1.5–1.96 (critical 2.50). The planted lead
explains roughly 6% of the remaining variance, so the gate needs more data. It
is working as designed, not failing.

Development note: LeadLag r1 required a nearly flat own reference and
abstained on ~97% of decisions; r2 trades the peer-minus-own divergence. The
revision constant changed, so r1 and r2 IDs cannot collide. This change was
made against synthetic data only.

## Resources (measured)

| Markets | Snapshots | Decisions | Campaign wall | Process wall | Peak RSS |
|---|---|---|---|---|---|
| 5,760 | 518,400 | ~0.31 M | 0.23–0.33 s | ~1.0 s | 267–283 MB |
| 17,280 | 1,555,200 | 0.98 M | 1.19 s | 3.2 s | 794 MB |

Bottleneck: every snapshot is held in memory (~0.5 KB each). For multi-month
real data, evaluate per market from a streamed source to keep RAM bounded.

## Real data

`input.rs` defines the operator-normalized JSONL boundary (read-only, strict
schema, line-numbered errors, size caps). **No authentic Kalshi data was
available in this environment, so no real-data campaign has been run.**

```
phoenix-campaign --input markets.jsonl --out D:\PhoenixOutputs\campaign-001 \
    --holdout-ledger D:\PhoenixOutputs\holdout-ledger.txt --threads 2
```
`--out` must be new or empty; the tool refuses `E:\PhoenixResearch` and
`E:\TradingNextData` and their subdirectories.

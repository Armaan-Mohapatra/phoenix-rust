# Phase 19 — authentic collector data, verified books, first real campaign

Paper-only and offline. Base: Phase 18 (`8ded455`). Machine-local evidence (full storage audit, cleanup
plan, receipts) lives on the research host under `E:\PhoenixLab\maintenance`; compact reproducible records
are in `research/phase19/`. Format reference: `docs/TNEVENT_FORMAT.md`.

## 1. What was decoded (crates/tnevent)

TradingNext collector segments (v1 raw stream, v2 zlib blocks) decode in Rust with every length bounded and
every checksum verified. **463/463 finalized segments verify** (manifest SHA-256, size, event count, block
CRC32, per-event CRC32): **158,968,742 events**, 2026-10-29T19:50Z to 2026-10-31T10:36Z, 4,814 order-book
snapshots, 154.3 M deltas, 4.48 M trades. (Last session's "82.5 M" was an earlier status-file reading.)

## 2. Book verification, and two verifier defects found by cross-checking

`book.rs` verifies fail-closed (snapshot-initialized in the current connection generation; breaks on gaps,
regressions, new generation, negative depth, crossed book, wrong market, inexact decimals). Checking the
first real-corpus pass against an independent gap classifier exposed two defects of mine, both fixed with
tests: (1) stored control messages (`ok`, `subscribed`) consume stream sequence numbers and were not counted,
producing **217 spurious gaps (real: 1)**; (2) verified-span bookkeeping lost spans across reconnects.
Corpus result (`research/phase19/tnevent-manifest-summary.json`): 150.3 M of 154.3 M deltas applied to verified
books, 1 real sequence gap, 55 connection generations, 0 crossed/negative-depth/wrong-market events.

| Market class (825 supported crypto 15-minute markets) | Count |
|---|---:|
| ExecutableVerified (snapshot within 30 s of open, ≥ 90 % verified-time coverage, strike, venue result) | **651** |
| PartialVerified | 16 |
| NoSettlement / NoStrike / NoWindow / NoInitialization | 89 / 55 / 11 / 3 |

By asset (executable): BTC 111, ETH 111, SOL 111, XRP 111, DOGE 104, BNB 103. Median verified coverage 100 %.
**Settlement is venue-reported** (`determined` lifecycle result); it is not verified against the underlying
index. There is **no spot-reference series** in this capture.

## 3. First authentic campaign (exploratory)

Protocol (`research/phase19/split-A-B.json`): chronological. Set A = first 422 executable markets (closing
2026-10-30T05:30Z to 2026-10-31T00:45Z) is the research set; set B = 229 later markets is **reserved and
untouched** unless a candidate passes validation. Cases: one verified sample per 500 ms bucket with the true
receipt time (staleness stays visible), top 10 levels, quantities floored to whole contracts,
`reference_cents = 0` meaning unavailable (the ingest guard now accepts 0; negatives are still rejected).

Result on A (`campaign-A-summary.txt`): 101 configurations, splits 188 discovery / 108 validation / 108
holdout / 18 embargoed markets. **0 passed all gates; the holdout was not evaluated.**

* 14 reference-based families made 0 trades (they abstain without a reference). BLOCKED, not "no edge".
* CoinFlip −$32.49, Longshot (4 configs) −$87.55, Favorite (9 configs) −$115.40 in discovery overall.
* Four configurations reached validation; all were rejected because the validation split has **18
  independent time-window clusters, below the 20 the cluster test needs** (z undefined):

| config | disc trades / net / ROI | val trades / net / ROI | val net, fee ×1.5 / latency 1 s |
|---|---|---|---|
| Favorite 300 s, ask 0.65–0.95 | 106 / +$14.21 / +1.7 % | 72 / +$12.20 / +2.2 % | +$8.58 / +$9.49 |
| Favorite 300 s, ask 0.75–0.95 | 80 / +$15.48 / +2.3 % | 52 / +$19.96 / +4.8 % | +$17.81 / +$20.14 |
| Favorite 300 s, ask 0.85–0.95 | 51 / +$1.32 / +0.3 % | 30 / −$2.83 / −1.2 % | −$3.73 / −$3.82 |
| BookImbalance 300 s | 78 / +$4.14 / +2.3 % | 46 / −$49.72 / −33.7 % | −$52.33 / −$42.78 |

Reading: two Favorite variants are positive in both splits and survive the stress cases, but the discovery
statistic is z = 0.29 (34 clusters) and the effect is about 2 % of capital on one-contract orders over ~2 days.
That is a **hypothesis the data cannot confirm**: a 2.6-day capture lacks the independent windows to test a
~2 % effect. No profitability claim is made. Fills are modeled taker fills against verified books; there is no
maker/queue model.

## 4. Frozen strategies

Source `luna_experimental_paper.py` matches its frozen SHA-256 locally. Decision parity (real frozen Python vs
Rust, same 25,000 real observations): `market_only_p0_v2` and the **book-only subset** of
`book_flow_imbalance_p0_v3` agree on recommendation, probability and edge in 25,000/25,000
(`research/phase19/frozen-parity.json`). `market_only` is a structural no-trade (EV = −half-spread − fee).
The other strategies consume a cached LLM forecast or a CF Benchmarks reference that this capture does not
contain: parity BLOCKED. `volatility_regime_calibration` is 100 % fallback in the source.

## 5. Not done

Transformations beyond the existing grid (inversion = Longshot vs Favorite only), early-exit and maker
models, portfolio tests, multiple-testing correction beyond the engine's Bonferroni gates, economic
evaluation of the frozen replicas, and any confirmatory run (set B unused). Perps capture (2.5 h of
`KXBTCPERP`/`KXETHPERP` order books) was decoded structurally but not joined.

## Reproduce

```powershell
.\scripts\phoenix.ps1 manifest                       # decode + verify -> catalog\tnevent-<stamp>
.\scripts\phoenix.ps1 cases -Manifest <that dir>     # verified books -> derived\cases-<stamp>
phoenix-campaign --input cases.jsonl --cache-dir <dir> --out <new dir>
python scripts\parity_frozen.py --frozen luna_experimental_paper.py --cases cases.jsonl --exe phoenix-tnevent.exe
```

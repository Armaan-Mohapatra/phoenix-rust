# Phase 17 — research integrity, streaming, authentic-data adapter

Paper-only and offline. Base: PR #39 at `673ad44`.

## Identities (reported separately in every report)

| Field | Covers |
|---|---|
| `raw_input_sha256` | Exact input file bytes. Recomputed on every streaming pass; a change aborts with `SourceChanged`. |
| `normalized_input_sha256` | Canonical length-prefixed encoding of every field the engine reads: identities, window, strike, every book level, `exchange_ns`/`received_ns`, session, sequence, reference, peer, perp basis, winner, provenance, upstream source and rules hashes. JSON formatting does not change it. |
| `experiment_sha256` | normalized input + grid + economic config (threads/batch excluded) + code revision |
| `code_revision` | crate version + strategy revision + git SHA (`build.rs`) |

Tests: 11 single-field mutations and a reordering each change the
fingerprint; whitespace-only JSON changes keep the normalized hash and change
the raw hash.

**Not verified here:** each case's `source_sha256` is a claim about upstream
bytes. The adapter sets it to the SHA-256 of the replay report it actually
read. Upstream archive hashes are verified by the existing source-loader
crates, not re-verified by the campaign.

## Single-use holdout (`registry.rs`, `--registry`, `--confirmatory`)

- **Locking:** `registry.lock` is created with `create_new`, so concurrent runs fail with an actionable error. A lock left by a crash is never auto-broken.
- **Exposure granularity:** tracked per holdout market (`market@expiration_ns`), so reuse is detected even when the grid, the config, or other parts of the data change.
- **Crash safety:** a `Started` record is written atomically before the holdout is touched and marked `Completed` after. A crash leaves `Started`, which still counts as exposure.
- **Unused holdouts:** if nothing survives validation, the holdout is never evaluated and nothing is recorded.
- **Corruption:** a corrupt record makes the registry refuse to open (fails closed).
- **Evidence classes:**
  - `SyntheticEngineCheck`: synthetic input.
  - `Exploratory`: real input without `--confirmatory`, after an acknowledged reuse, or with unverified books.
  - `UntouchedConfirmatory`: registry-verified first use only.

Process-level acceptance run (Linux):

| # | Case | Result |
|---|---|---|
| 1 | synthetic + `--confirmatory` | refused |
| 2 | first confirmatory run | `UntouchedConfirmatory` |
| 3 | same data and grid again | refused (382 markets exposed) |
| 4 | `--allow-exploratory-reuse` | `Exploratory`, recorded as `ExploratoryReuse` |
| 5 | dataset with one changed quantity | still refused |
| 6 | two concurrent processes | one succeeds, one refused (`Busy`) |
| 7 | output path under the protected root / via a symlink / non-empty | all refused |

## Path safety (`paths.rs`)

- Lexical deny of protected roots (case-insensitive, `/` and `\` equivalent).
- No `..` components.
- Every existing component is checked with `symlink_metadata`: symlinks and Windows reparse points (junctions) are refused.
- Canonical deny of the deepest existing ancestor.
- The output directory is created, or must already be empty, and is re-verified after creation.
- Files are written `create_new` + rename and never overwritten.
- Extra protected roots: `PHOENIX_PROTECTED_ROOTS` (`;`-separated).
- Not tested here: Windows reparse-point behaviour; no Windows host.

## Validation and arithmetic

- **Rejected configurations:** invalid quantity, latency, quote age, fee multiplier, split fractions, alpha (including NaN), threads or batch size; empty or duplicate grids.
- **Rejected input:** duplicate markets, unordered input, non-contiguous clusters, inconsistent event metadata, non-positive strike or reference.
- **Timestamps:** decision and arrival times use checked arithmetic, and arrival at or after expiration is a `NoFill`.
- **Cluster test:** exact integer sums. It returns `None`, never NaN or infinity, for n < 20, zero variance or overflow.

## Streaming (`source.rs`, `--input`)

Four ordered passes: scan/validate, discovery, validation, holdout. Each runs in bounded batches, and in-memory and streaming share one implementation.

Lagged-quotes scenario, 3 assets, seed 2026, 2 threads, batch 256:

| Days | Markets | File | In-memory wall / RSS | Streaming wall / RSS | Identical results |
|---|---|---|---|---|---|
| 20 | 5,760 | 140 MB | 0.99 s / 265 MB | 4.15 s / 13 MB | yes |
| 60 | 17,280 | 421 MB | 2.89 s / 790 MB | 12.6 s / 17 MB | yes |
| 120 | 34,560 | 841 MB | 6.53 s / 1,577 MB | 26.7 s / 22 MB | yes |

- **Floating point:** identical results are expected, not luck. Both paths run the same accumulators in the same order, money is integer, and the z-test sums are exact integers.
- **Cost:** streaming is about 4× slower because JSON is decoded on each pass (raw bytes read = 4 × file size).
- **Next step:** cache a binary partition after pass 0.
- **Not implemented:** checkpoint/resume. An interrupted run restarts, and its registry record stays `Started`, i.e. counted as exposed.

## Statistics

- **`clustered_bootstrap` fixed:** it reported alpha/m but always cut a 95% interval. It now uses alpha/m, reports the coverage, and flags tails with fewer than 10 draws.
- **Bootstrap coverage:** at m = 1, the null coverage check falls in [0.88, 0.99].
- **Cluster z-test, one-sided α = 0.05:** empirical false-positive rate within [3.5%, 6.5%] over 4,000 null trials; power above 80% for a 0.3-sd effect at n = 120.
- **Null campaigns:** 5 seeds of the efficient scenario produce 0 candidates.

## Authentic-data adapter (`adapter.rs`, `--adapt-replay`)

Converts one existing `phoenix-source-loader` replay report (schema 1) into a campaign case. Operator metadata, as-of reference and perp observations, and a settlement record with an evidence hash are joined in. Nothing is interpolated, and a missing settlement stays pending.

**Blocking finding:** the replay's `verified_executable()` returns `false` by design (no authenticated snapshot verifier exists yet). So no authentic checkpoint is currently execution-eligible.
- **Default:** the adapter refuses (`NoExecutionEligibleCheckpoints`).
- **`--allow-unverified-books`:** produces `operator_unverified` cases, which are exploratory only and refused for `--confirmatory`.

**Status:** tested on fixtures only. No authentic data was available in this environment.

## Not done in Phase 17

- **P3:** maker/queue models, early exit, capital constraints. The campaign is still taker-only, top-of-book from the adapter, and holds to expiry.
- **P5:** paper-journal integration of candidates.
- **P6:** Windows packaging and supervisor.
- **P7:** strategy changes. None; the grid is unchanged at 15 families and 101 configurations.

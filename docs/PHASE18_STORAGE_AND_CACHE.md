# Phase 18 — storage audit tooling, binary case cache, Windows fixes

Paper-only and offline. Base: Phase 17 at `4d79f47`. Machine-specific audit outputs (file
lists, duplicate groups, cleanup approvals) stay on the research host under
`E:\PhoenixLab\maintenance\storage-audits\` and are not committed.

## Binary case cache (`source::CachedSource`, `--cache-dir DIR`)

Phase 17 decoded the JSONL input on every pass (identity pass + 4 campaign passes = 5 decodes).

- The cache file is exactly the canonical stream that `normalized_input_sha256` hashes
  (`fingerprint::encode_*`, now generic over a `Sink`; `decode_*` is its inverse). Its SHA-256
  is therefore the normalized identity; no extra checksum format.
- Key: `<raw_sha256>.<NORMALIZED_SCHEMA>.bin` + sidecar `.json` (schema, crate version, raw and
  normalized SHA-256, case count, byte length). The sidecar is written last; a crash leaves no
  valid entry. A different raw file, schema or crate version never hits.
- First visit hashes the raw file (no JSON decode). Hit: every pass decodes the binary cache.
  Miss: one JSON decode writes the cache while feeding the pass.
- Every cache pass re-hashes the bytes it decodes and must match the sidecar's normalized
  SHA-256; decoded cases are re-validated; allocations are capped before the hash check.
- The raw file's size and mtime are checked before and after every pass (`SourceChanged`).
- Only decoded **inputs** are cached, never results, so a cached result from another strategy
  version or config cannot be reused. `resources.case_cache` (`built`/`hit`) and
  `resources.cache_bytes_read` are reported.

Benchmarks (Windows 11, E: HDD-backed, lagged_quotes 3 assets seed 2026, 2 threads, batch 256,
one fresh process per run):

| days / markets / JSON | Phase 17 CPU s | warm cache CPU s | engine ms P17 → warm | wall s P17 → warm | peak RSS MB |
|---|---:|---:|---:|---:|---:|
| 20 / 5,760 / 140 MB | 3.95 | 2.25 | 2,889 → 1,550 | 7.68 → 2.19 | 17.7 → 17.0 |
| 60 / 17,280 / 421 MB | 11.50 | 6.89 | 8,727 → 4,865 | 12.75 → 6.71 | 23.1 → 23.7 |
| 120 / 34,560 / 841 MB | 24.16 | 13.70 | 17,906 → 8,797 | 32.52 → 23.18 | 29.7 → 30.8 |

Results, candidates, split counts and all identities are byte-identical across Phase 17,
Phase 18 without cache, cold cache and warm cache at every size. The cache is 76.5 % of the
JSON size, so bytes read fall only ~3.5 %; at 120 days the run is disk-read bound. Next steps
(not done): compact varint cache with its own checksum; fused single-pass evaluation.

## Windows fixes in `paths.rs` (pre-existing failures)

On Windows the Phase 17 campaign suite failed 3/46 tests. Cause: `inspect()` called
`symlink_metadata` on a verbatim prefix component (`\\?\C:`), which opens the raw volume and
returns ERROR_INVALID_FUNCTION; and verbatim paths keep `..` as a `Normal` component, so the
traversal check missed it. Prefix/root components are now skipped and a literal `..` is refused.
The traversal test builds its path from a string because `PathBuf::join` normalizes `..` on
verbatim bases. Suite on Windows: 51/51; clippy `-D warnings` clean.

Tests now remove their temp fixtures (`TempDir` drop guard); before, each `cargo test` run left
~370 MB in the temp directory.

## `run-original-btc.ps1`

Under Windows PowerShell 5.1, `ErrorActionPreference=Stop` turned cargo's stderr progress into a
terminating error, so the script never ran on Windows. The cargo call now runs with `Continue`
and is judged by its exit code.

First authentic Windows run (read-only): 24/24 allowlisted members SHA-verified against the
inside-ZIP catalog, 194,253 rows scanned, 69,570 events for `KXBTC15M-26OCT241900-00`
(63,813 deltas, 5,757 trades), channel gaps/duplicates/regressions 0 — but **0 snapshots**, so
all deltas are rejected and the book is `UnknownInitialization`. No case can be adapted from this
window; authentic campaign research remains BLOCKED on a window with a snapshot (or a
full-channel snapshot verifier).

## Storage tooling

- `scripts/phoenix_storage.py` (stdlib): `inventory` (bounded, junctions not followed,
  logical + allocated bytes, per-category totals), `dupes` (size → head/tail prefilter →
  full SHA-256), `cleanup` (dry-run default; only `temporary`/`regenerable` folders inside the
  workspace; `--apply` moves to `maintenance\trash`, never deletes), `selftest`.
- `scripts/phoenix.ps1`: `doctor`, `storage-audit`, `cleanup [-Apply]`, `research -Cases X.jsonl`.
  Research root `PHOENIX_LAB` (default `E:\PhoenixLab`); builds go to its `cache\build\target`.
- `scripts/bench-campaign.ps1`: wall/CPU/peak RSS/IO per run.

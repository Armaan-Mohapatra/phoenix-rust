# Phoenix Rust — Phase 1 storage code checkpoint (2026-11-12 PST)

**Status: CODE_WRITTEN_UNTESTED / STATIC_SOURCE_REVIEWED / VALIDATION_DEFERRED.** This is the Phase 1 source-development checkpoint, NOT a successful test or proof of million-ID performance. Do not begin Phase 2 automatically.

## Exact GitHub lineage

- Private repository: `Armaan-Mohapatra/phoenix-rust`.
- Starting PR #21: `codex/build-first-deferred-validation-v1` at `c0cc1026b307beb0287c57119268339fecc5660c`.
- Isolated continuation: `codex/phase1-scalable-storage-v1`, draft [PR #22](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/22), based on PR #21.
- Eight implementation/test-only commits, ending at `f2dd7cd75e059f05215046d408d71c6547f4a306`, all with `[skip ci]`. New checkpoint-only commit follows this parent.
- GitHub compare `c0cc1026...f2dd7cd7` has 8 commits, 3 modified Rust files, 0 deletions and 0 unrelated code paths. Exact source files and Git tree content were re-fetched for static review.
- Existing drafts #1/#9–#21 and `main` preserved. No live services, collectors, order routes, active SQLite/WAL or trading source archives opened or changed.

## What Phase 1 source code now implements

**`integrations/durable-archive/src/lib.rs`**
- Keeps the existing legacy `Archive::open` v1 format and 2,000-partition/250,000-ID memory-mode safety limits.
- Disk-mode `Archive::open_external_index_with_limit(root,cap)` now validates `1 <= cap <= 100,000`; default disk cap is 100,000 sealed partitions. This is a finite, configurable protection limit, **not** a validated throughput target.
- The disk-backed mode no longer populates the in-memory `BTreeMap<String,SealedPartition>` with full historical `events` lists. It retains only a scalar count and loads individual sealed v1 manifests under the preexisting 1-MiB manifest cap.
- Bounded on-demand manifest verification checks file identity, format version, sealed status, event IDs, per-row hashes and matching Parquet bytes. External recovery/orphan scanning and `visit_verified_rows` process one manifest at a time. `verify_all` now checks all per-event payload hashes, not just partition-level bytes.
- No migration of source files and no new file format. An authored compatibility test reads a legacy-written v1 manifest in external read-only mode without changing its bytes.

**`integrations/disk-identity/src/lib.rs`**
- Streams indexed partition IDs through redb without materializing every key.
- Supports exclusive cursor pagination via `partition_page(after,limit)`, with `1 <= limit <= 512` and SHA-256-like cursor validation. Uses B+tree range seek rather than rescanning from the first key.
- A preexisting partition marker must still match all expected event-ID/digest records. Missing or mismatched entries fail closed; no premature idempotence success from the marker alone.
- Existing 16-MiB redb cache policy and <=256 identities per transaction are unchanged.

**`integrations/indexed-archive/src/lib.rs`**
- Opens the external archive with the caller's explicit partition cap; exposes partition-page lookups.
- Cold recovery streams sealed manifests from disk, checks every Parquet partition and exact per-row identity, and replays only missing redb transactions. Redb-only phantom partition markers are still rejected.
- Removes the former all-manifests `Vec` and `BTreeSet` from recovery, preserving the single newly sealed-partition append/promotion path already introduced in PR #21.
- The resulting cold start is still **O(number of sealed partitions + events)** in disk I/O; it is not constant-time or fully benchmarked. It is intended to be bounded in heap use, subject to future measurement.

## Authored regression tests — all NOT RUN

- Disk-mode startup and cold reopening without retained historical row lists.
- Configurable-cap refusal, missing manifest, corrupted row hashes and invalid root/cap.
- Exact v1 format compatibility without rewriting old manifest bytes.
- redb key stream and deterministic exclusive pagination with 37 sample partitions.
- Indexed-archive recovery and cursor paging across 10–13 sealed partitions.
- Existing-marker-vs-missing-event test using intentionally inconsistent redb state.
- Existing seal/reopen/duplicate/phantom-marker tests remain intact.
- Separate **explicitly ignored**, opt-in million-ID stress test: 4,096 partitions × 256 identities = 1,048,576 logical IDs in bounded transactions. This MUST be invoked intentionally in the final local validation campaign. Its `#[ignore]` marker is disclosed, not counted as a passing standard test.

## Final local validation commands — prepared, NOT RUN

Use the existing isolated Windows validation harness from PR #21 for root and standalone modules. Focused Phase 1 commands:

```powershell
cargo test --manifest-path integrations/durable-archive/Cargo.toml --all-targets
cargo test --manifest-path integrations/disk-identity/Cargo.toml --all-targets
cargo test --manifest-path integrations/indexed-archive/Cargo.toml --all-targets
cargo clippy --manifest-path integrations/durable-archive/Cargo.toml --all-targets
cargo clippy --manifest-path integrations/disk-identity/Cargo.toml --all-targets
cargo clippy --manifest-path integrations/indexed-archive/Cargo.toml --all-targets
cargo test --manifest-path integrations/disk-identity/Cargo.toml million_logical_ids_in_bounded_256_id_transactions -- --ignored --nocapture
```

The final test must also measure RAM, CPU time, cold-start I/O, write amplification, transaction duration, disk footprint and Windows crash recovery, including a >2,000-partition test on an isolated research drive, before declaring scalability proven. The million-ID test is deliberately high-I/O and must NOT be run on the production capture drive.

## Explicit unresolved risks

1. **Uncompiled code:** no `cargo`, `rustc`, tests, rustfmt, Clippy or performance experiments executed after the PR #21 baseline. Source-level review cannot rule out compiler or cross-platform faults.
2. **Legacy limits remain:** the old memory-mode 250,000-ID/2,000-partition caps remain by design. Only the opt-in external indexed mode uses lazy manifests.
3. **Finite disk-mode cap:** 100,000 partitions × 256 events gives a **theoretical** 25,600,000-identity ceiling, not a demonstrated capability.
4. **Cold-open cost:** every sealed source partition is still reverified against bytes and row-level manifests. RAM is designed to be bounded; cold-start CPU and disk I/O still grow linearly with history.
5. **Windows directory durability:** metadata sync after rename is not established as power-cut safe. Disk-full, torn writes, lost directory entries and simultaneous process failures are Phase 2/final-local recovery gates.
6. **Indexed storage migration:** no deployment or automatic upgrade of production storage. Existing v1 research manifests remain byte-compatible; using the dedicated new indexed research root requires normal sealed-manifest-to-redb reconciliation.
7. **Independent settlement and actual execution:** no original 24-partition Rust source run, verified starting orderbook, official authenticated final or licensed primary CF Benchmarks series was obtained. No historical performance claims.

## Phase boundary

**Stop after Phase 1.** This checkpoint permits the next focused Phase 2 development prompt, but the Phase 1 code is not validated until the final authorized local test campaign. No CI was requested; source commits use `[skip ci]` to avoid paid GitHub runner allocation.

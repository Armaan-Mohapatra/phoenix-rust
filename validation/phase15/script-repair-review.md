# Phase 15A — validation script source repair review

**Status: CODE_REPAIRED_NOT_EXECUTED.** No PowerShell interpreter or Rust/Cargo available in the current Linux sandbox. This is an actual source patch with static argument inspection, NOT a successful PowerShell parse, compilation, or test run.

## Verified original defect and fix

Original Phase 14 stress command assembled:

```powershell
@('test','--manifest-path',$manifest,'million_logical_ids_in_bounded_256_id_transactions','--','--nocapture') + $script:offlineArgs
```

That places Cargo `--offline` **after** `--`, incorrectly passing it to the Rust test binary. The source test `integrations/disk-identity/src/lib.rs` is annotated:

```rust
#[test]
#[ignore = "final local scale campaign: ~1,048,576 logical identities, 4096 ACID commits"]
fn million_logical_ids_in_bounded_256_id_transactions()
```

Corrected invocation:

```powershell
Invoke-CargoValidation 'disk-index-million-ids' $CoreToolchain (
    @('test','--manifest-path',(Join-Path $checkout 'integrations/disk-identity/Cargo.toml')) +
    $script:offlineArgs +
    @('million_logical_ids_in_bounded_256_id_transactions',
      '--','--ignored','--test-threads=1','--nocapture'))
```

The test is deliberately **not run unless `-IncludeStress` is supplied**. The normal root/standalone Cargo tests omit ignored cases. The filter lacks `--exact` because it is an unqualified unique name; an exact filter would require the fully qualified Rust test name.

## Other argument and safety checks

- Root and standalone `cargo metadata/test/clippy` options keep `--offline` before the `--` Rust/Clippy argument boundary. `cargo fmt` passes `-- --check` to rustfmt as intended. Optional upstream HFT `--features upstream-hft` is passed to Cargo, not to the test executable. Bounded multi-market inputs follow Cargo `--` intentionally as binary arguments.
- Added root `cargo test --doc` and each standalone `cargo test --doc` so documenting tests does not depend on `--all-targets`.
- Default branch verification now expects `codex/phase15-validation-repairs-v1`, and the operator must supply the **exact final Phase 15 corrective SHA**, not the Phase 14 SHA.
- The ancestor reparse/junction check now climbs **all existing ancestors even when the output directory itself does not yet exist**. Previously it exited when the requested leaf was absent.
- Refuses using an entire drive root for validation output. Rejects source ZIP directory and research output overlapping in either direction. Multimarket plan, ZIPs and prior checkpoints must exist outside the output write location.
- `-ExecuteValidation` remains mandatory. Stress, original BTC replay, multi-market replay, HFT, hypothesis searches and resource monitoring still require independent explicit switches.
- `CARGO_BUILD_JOBS=2` default and isolated research `CARGO_TARGET_DIR` are retained. The script never writes to an original market directory or active SQLite/WAL path.
- Existing machine resource snapshots are explicitly labeled **not peak process memory or market throughput evidence**.

## Verification status

| Check | Status |
|---|---|
| Direct source review of ignored test annotation | **PASS — STATIC** |
| Correct Rust-vs-Cargo argument placement | **PASS — STATIC** |
| Default stress opt-in boundary exists | **PASS — STATIC** |
| No Windows PowerShell parser execution | **NOT RUN** |
| Actual Rust test discovered/executed | **NOT RUN** |
| Windows reparse/path integration tests | **NOT RUN** |
| Phase 15A Rust compiler/toolchain preflight | **BLOCKED** |

Any failure on the real Windows machine must be preserved in exact SHA-tagged command logs. Never silently skip or mark the failed software test PASS. See `WINDOWS_OPERATOR_HANDOFF.md`.

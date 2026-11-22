# Phoenix Mac certification (sanitized)

**Verdict: `MAC_CERTIFIED_FOR_WINDOWS_TESTING`.** This is NOT `WINDOWS_CERTIFIED`, not paper-live
ready, and says nothing about profitability. All results are software checks on synthetic fixtures.
Machine-readable copy: `docs/mac-validation.json`.

## Tested code commits (the published branch tips are docs-only descendants of these)
| Repo | Branch | Tested code SHA |
|---|---|---|
| phoenix-rust | `mac/phase1-replay-determinism` | `4b5325833ba5c6aefb046141b29a926027a9202c` |
| phoenix-agent-arena | `mac/phase3-arena-integrity` | `e1d685cf79cffef571955fecfc6c6badbd4d879f` |
| kalshi15m-market-data | `mac/phase4-collector-integrity` | `8ffc40bfa63ec184112836255deb7e38f75c76f0` |

Rule: a branch tip may differ from the tested SHA only by files under `docs/` (and a `[skip ci]`
message in Arena). Verify with `git diff --name-only <tested>..<tip>`.

## Environment
macOS 26.2 arm64; rustc/cargo 1.100.0 (+1.78.0 MSRV); uv 0.13.9 with uv-managed CPython 3.12.13 (system and
Homebrew Python untouched; Homebrew's `pyexpat` links an older system libexpat so `platform.mac_ver()`
returned empty there); nextest 0.9.152, cargo-deny 0.22.0, gitleaks 8.30.1, pip-audit via uvx.

## Totals (final run, real exit codes, one log per check)
PASS 90 · FAIL 1 · BLOCKED 2 · NOT_RUN 4

| Area | Result |
|---|---|
| Rust workspace: fmt, clippy `-D warnings`, `cargo test --all-targets`, doctests, nextest (165) | PASS |
| Rust 17 standalone integration crates: test, doc, clippy, fmt (each own manifest, `--locked`) | PASS |
| Ignored 1M-identity storage scale test (synthetic, 17 s) | PASS |
| `scripts/verify.sh` | PASS=62 FAIL=0 BLOCKED=0 NOT_RUN=3 |
| MSRV 1.78 `check --all-targets` | PASS |
| cargo-deny (licenses/advisories/bans/sources) | PASS |
| Replay determinism: 3 repeated runs of determinism/golden/streaming/cache/corruption tests | PASS |
| Arena: compile, imports, `unittest discover`, pytest (177), lock + ledger + quote-contract tests | PASS |
| Arena pinned dev lock, pip-audit | PASS (no known vulnerabilities) |
| Collector runnable suite (189 passed, 1 skipped), book/sequence, recovery, low-disk refusal, signing | PASS |
| Collector runnable suite under a simulated 1 MiB-free disk | PASS |
| gitleaks, all three repos, history + tree | PASS (no leaks) |
| Collector pip-audit (`cryptography 47.0.0`) | **FAIL** (open item below) |
| Collector `FrozenStrategyTests` (12) | **BLOCKED**: need Windows-only `strategy/` files; all 12 fail with exactly that cause |
| Authentic collector -> Arena quote bridge | **BLOCKED**: producer is not in these repos; fixture contract only |
| Durable-archive Windows symlink test; hftbacktest build; original-data replay; Windows runtime | NOT_RUN |

## Defects found and repaired
| Severity | Defect | Fix |
|---|---|---|
| High | Arena imported Windows-only `msvcrt` unconditionally: all 21 test modules failed on macOS | `oslock.py` (`fcntl.flock` shim, tested for cross-process contention and release-on-kill); Windows still uses real `msvcrt` |
| High | Six standalone crates failed on macOS: path-safety checks correctly reject symlinked ancestors and macOS `$TMPDIR` is under `/var` -> `/private/var` | `verify.sh` canonicalizes `TMPDIR`; safety code unchanged |
| Med | Campaign reports never stated fees are hypothetical or settlement is venue-only | `fee_basis`, `settlement_evidence`, limitation text (fingerprints unchanged) |
| Med | Collector order book silently dropped malformed snapshot levels and `delta()` could revive an invalidated book | Fail closed; worker resyncs; strategy treats as ineligible; 6 regression tests proven to fail without the fix |
| Med | Three crash-recovery tests depended on the host having >20 GiB free disk | tests set `min_free_bytes=0`; real low-disk refusal still tested; suite verified at simulated 1 MiB free |
| Low | `verify.sh` MSRV gate depended on the rustup `cargo` proxy | uses `rustup run 1.78.0 cargo` |
| Low | Golden pin hashed a HEAD-dependent value | results-only pin, stable across commits |
| Low | Bootstrap `local project=... target=...$project` unbound variable (`set -u`) | split declarations; idempotency (create, rerun preserves, incomplete env preserved) tested |
| Low | Host-dependent notify test; no signing tests; Arena dev deps unpinned | hermetic test; signing characterization; `requirements-dev.lock.txt` |

## Open items
1. **cryptography 47.0.0 advisories (medium, OPEN).** Fixed in 48.0.1/49/50 (partly outside `pyproject` `<48`). The
   collector only uses it for RSA-PSS/Ed25519 signing. Signing tests and the full runnable collector suite pass on 50.0.0 in an
   isolated venv. The pin is unchanged pending owner approval and a Windows signing check with a real read-only key.
2. Authentic quote bridge (high): unverified until tested against the Windows producer.
3. 12 frozen-strategy tests, original-data replay, Windows runtime: Windows only.

## Evidence classes (never conflate)
Fees: hypothetical formula, not an authenticated historical schedule. Settlement: venue-reported, not independent.
Strategy numbers: synthetic fixtures. Golden pin (results only): `bf4123a18294ff3084d8dfe837da3508a5d3f04f8f6a519fae8693c10dd5e76b`;
`experiment_sha256` embeds the git HEAD by design. Float differences on Windows must be investigated, never re-pinned silently.

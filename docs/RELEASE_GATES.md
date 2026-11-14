# Phoenix Rust — current release and research evidence gates

**Authoritative Phase 14 status: SOURCE INTEGRATION PREPARED, UNTESTED.**
This table supersedes older gate summaries for the *current* stacked feature branch. Past CI and synthetic test successes on older commit SHAs do **not** transfer to the Phase 14 commit, and none are called runtime proof for the changed source.

| Gate | Required verification | Phase 14 status |
|---|---|---|
| G0 Build and dependency graph | Exact clean Windows SHA checkout; Cargo metadata, compile, fmt, clippy, ALL root and standalone tests; pin/cache audit | **NOT RUN — SOURCE ONLY** |
| G1 Fixed-point economics | 1e-4 USD legacy vs 1e-6 USD micro ledger, 1e-2 contract, effective-dated fees/rounding, no fee double count | **NOT RUN**; checked interfaces only |
| G2 Market stream/L2 integrity | True shared-channel SID/session continuity, snapshot+delta gap detection, genuine source order book completeness | **UNVERIFIED ORIGINAL DATA** |
| G3 Sealed archive durability | Immutable Parquet+manifest, no overwrite, redb indexed reconciliation, cold-reopen corruption/fault stress; actual Windows hard-link and power-loss behavior | **NOT RUN**; scalable indexed mode is separate from legacy paper-journal writer |
| G4 Contract/settlement | Original contract identity/rules, official finalized result and separately verified independent primary reference, no revision conflict | **UNVERIFIED**; journal SettleClaim is a modeled claim, not authenticated |
| G5 Historical execution | Original-source replay; arrival and queue evidence; taker/maker comparison; source continuity; nonfills/partial fills; historical fee correctness | **NOT RUN**; simulation source does not prove venue fills |
| G6 Frozen strategy parity | Reproducible archived original code+configuration; controls/hypotheses separated; original decisions reproduced; temporal/OOS safeguards | **BLOCKED**: 11 original October frozen version IDs, 0 complete executable originals |
| G7 Paper Arena durability | Exact fractional cash conservation; persisted risk decisions/restart replay/cooldown; 2+ portfolio isolation; fault injection; corruption; duplicate suppression | **SOURCE REPAIRED, NOT RUN** |
| G8 Statistical research | Discovery/validation/embargo/untouched OOS; event cluster dependence; negative/inconclusive outcomes; after-cost return and multiple comparisons | **NOT RUN**; no confirmatory returns or significance |
| G9 Perps source | Real venue schema, unit/precision, session mappings, causal timestamp checks, independent original source hashes | **UNVERIFIED ORIGINAL SCHEMA** |
| G10 Operational security | Safe output root, symlink/junction/TOCTOU, disk/RAM/CPU bounds, stopped/disabled network by default, no exchange-order route | **STATIC REVIEW ONLY** |
| G11 Windows validation | Operator-approved isolated offline tests, physical hard-link/reparse/safe shutdown and power-loss checks; exact machine-readable logs | **SCRIPT AUTHORED, NOT EXECUTED** |
| G12 Install/deploy | Separate production migration/collector approval, documented recovery playbook, no live orders | **NOT AUTHORIZED** |

**No-go:** Current source is not a release candidate, not validated paper-trading infrastructure and not evidence of a profitable strategy. Do not deploy, merge, touch collectors, migrate original data or route exchange orders.

## Earlier tests versus current development

- Earlier November 12 snapshots and older PR commits reported synthetic Windows/Linux CI passes on portions of original core, wire, sealed archive, index, and paper-journal code. These claims apply **only to their specific original SHAs/test fixtures** and cannot prove new Phase 5.1–14 behavior.
- Phase 6–14 source development is stacked, uncompiled and untested. Each child branch has a draft PR and `[skip ci]` commits; no source branch in this stack is merged to `main`.
- Original BTC dataset ZIP and selected Parquet member hashes were previously inspected; no Phase 14 original-source Rust replay or authentic settlement run occurred.
- Authenticated market publication, complete historical book, effective-dated fee rules, original October strategy parameters, Perps original schema and untouched OOS outcomes remain separate missing evidence.

## Explicit local validation order

The authorized *future* operator invokes `scripts/validate-phoenix-locally.ps1` with `-ExecuteValidation`, exact expected Phase 14 HEAD and isolated output root. With default switches it prepares an isolated local checkout and runs bounded tests, cargo fmt/clippy and all standalone crates **only then**. Optional upstream hftbacktest requires Rust 1.91.1 and a separate opt-in; core workspace declares Rust 1.78.0. Expensive archive stress/original BTC replay/optimization and resource benchmarking require independent switches and authentic inputs. Missing market source evidence results in **BLOCKED**, not false success.

Successful software tests will be a necessary but insufficient release gate. Historical execution/fees/settlements, source continuity, independent financial outcomes, and Windows physical power-loss behavior require their own proof.

See `PHASE14_SOURCE_INVENTORY.md`, `PHASE14_STATIC_SECURITY_AUDIT.md`, `PHASE14_VALIDATION_MATRIX.md`, and the exact Phase 14 feature-freeze checkpoint.

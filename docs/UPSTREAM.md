# Phoenix Rust upstream repository adoption — source-verified 2026-11-12

See [full 25-repository pinned source audit](UPSTREAM_SOURCE_AUDIT_2026-11-12.md) for exact upstream SHAs, paths, functions, licenses, integration decisions, and test evidence.

**Status: 1 direct pinned Rust dependency integrated and tested; 3 independent Rust source-backed ports tested; 21 other references/blocked/excluded.**

- **DIRECT_INTEGRATION:** `nkaz001/hftbacktest` at SHA `5f3ec40b...`, optional backtest-only Rust queue/latency adapter, Windows/Linux [CI success](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43280365680). No live feature, no real-fill parity.
- **PORT_TO_RUST (original Rust code):** Oracle3 Apache-2.0 static fee-aware binary constraints, Ryan Frigo MIT forward-only edge/leakage tests in `integrations/source-ports`, Windows/Linux [CI success](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43281854717). Neither authenticates an outcome.
- **PORT_TO_RUST (original Rust code):** Tarpboi MIT two-stage risk circuit, fixed-point money and deterministic as-of gates, in `integrations/risk-guard`; Windows/Linux [CI success](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43284076370). No live broker linked.
- **TEST_REFERENCE:** Jon-Becker indexer, flumine simulation, digital-oracle cache/snapshots, pmxt read-only venue normalization, rodlaf maker quote sizing, Octagon clustered bootstrap, CloddsBot risk circuits, machinа-sports Kalshi docs, livetennis matching, Kalshi official starter.
- **SOURCE_ONLY:** CarlosIbCu historical hourly market slug, dexorynlabs planned 1c/99c bots.
- **BLOCKED:** Homerun AGPL-3.0 copying, unlicensed taetaehoho/ImMike/meloner/suislanchez/abhaymettu implementations.
- **EXCLUDED:** Kuest exchange infrastructure, Awesome list-only repo, Nour-Br README-only repo.

The only direct upstream source integration is an OPTIONAL Git dependency in a separate package. The Rust workspace has no order-submission endpoint. **No real historical data or actual fills validated.**

Prior local audit of 25 clones was not accessible here; this file verifies current remote commit content and existing GitHub CI, not the user laptop's E: clones. Do not merge, deploy, alter collectors or production data.

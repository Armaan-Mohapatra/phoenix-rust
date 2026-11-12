# Phoenix Rust — verified continuation checkpoint (2026-11-12)

**This is a GitHub development checkpoint, not a finished system or authorization to deploy.** Original [freeze checkpoint](CHECKPOINT_2026-11-12.md) remains intact. New work is in isolated draft pull requests; `main` is unchanged.

## Security and immutability

- GitHub repository **PUBLIC** at checkpoint. Owner must switch to PRIVATE and assess previous visibility before importing confidential Phoenix data or proprietary strategy definitions.
- Existing production Phoenix collectors, E:/C: source data, active SQLite/WAL, trading configuration, jobs and live orders have **not** been accessed or changed.
- No real market orders or order submission APIs introduced; build-only GitHub Actions tests are synthetic.
- Legacy uploaded LunaDelta archives were inspected **read-only locally** for curated metadata/schema field types, but no confidential records were published; actual historical parity remains untested in Rust.

## Branch/PR dependency graph (all draft, unmerged)

```text
main (original initial commit)
  └─ codex/rust-foundation-v1 → PR #1 (2026-11-12 freeze 5bb336fe)
       └─ codex/durable-archive-v1 → PR #9, eb95a5c
            └─ codex/historical-adapter-v1 → PR #10, 8180da2
                 └─ codex/kalshi-wire-codec-v1 → PR #11, 07ee75c
                      └─ codex/integrated-replay-fixture-v1 → PR #12, fb0d7c6
                           ├─ codex/paper-journal-v1 → PR #14, a7bf259 (plus checkpoint docs)
                           ├─ codex/performance-report-v1 → PR #13, f040176
                           └─ codex/settlement-evidence-guard-v1 → PR #15, d8dc6d4
```

All links: https://github.com/Armaan-Mohapatra/phoenix-rust/pulls

## Actual tested code added

1. **Durable research publisher** ([PR #9](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/9)): isolated Rust package on existing Parquet codec; writer lock, fixed bounded partition/manifests, SHA256, event-ID dedupe, file sync, atomic same-volume renames, quarantine of orphan/temp writes, simulated crash recovery. **Limitations:** 256 rows/partition, 2,000 partitions, 250k event IDs; no production-size on-disk index; Windows directory metadata power loss not certified. Ubuntu+Windows passed in [CI #43272133466](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43272133466).

2. **Generic historical input** ([PR #10](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/10)): external column mapping, bounded Parquet decode, RFC3339 nanosecond/epoch conversions, nullable sequence and exchange time, deterministic IDs and exact decimal parser. **Not verified on actual Phoenix or Perps files;** no private mapping/data committed. Ubuntu+Windows passed [CI #43273164670](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43273164670).

3. **Kalshi public-message codec** ([PR #11](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/11)): offline strict JSON snapshot, delta and trade messages, fixed-point dollar/fp fields, L2 reconstruction, timestamps and sid/seq. No WSS connection, collector or credentials. Ubuntu+Windows passed [CI #43273867846](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43273867846).

4. **Synthetic vertical replay** ([PR #12](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/12)): synthetic WSS→L2→sealed/archive reopen→frozen strategy→taker simulation→paper ledger→venue-final binary payout; reproducible deterministic net and explicitly pending/unsealed error cases. **Not a real historical backtest.** Ubuntu+Windows passed [CI #43274210787](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43274210787).

5. **Persistent paper journal** ([PR #14](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/14)): typed Parquet readback, archive verified row replay, multi-portfolio modeled buys, settlements, invalidations, no-trade records; restart-state reconciliation, idempotent sequences and exact i128 cash/fee JSON string handling. Three initial CI failures uncovered and resolved a serde i128 deserialization bug. Latest fixed commit **a7bf259bf48cd6895f45e3a89976b0f77c5eaaef**, Ubuntu+Windows passed [CI #43276307015](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43276307015). **Not a production paper Arena**: no complete order lifecycle or independently authenticated source outcome.

6. **Performance summary** ([PR #13](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/13)): deterministic split-level modeled metrics, nullable pending net, negative results retained, drawdown and separate asserted evidence tiers, JSON. Latest **f040176109b86bdddcfd673435bfdd749f7b33bb**, Ubuntu+Windows passed [CI #43276197126](https://github.com/Armaan-Mohapatra/phoenix-rust/actions/runs/43276197126). No operational publisher.

7. **Independent-proof fail-close change** ([PR #15](https://github.com/Armaan-Mohapatra/phoenix-rust/pull/15)): removes automatic promotion from venue-final to independently verified merely based on differing source names and hashes. **d8dc6d41091bb8a6931b07f85537a4d267f67803**, CI status must be checked separately; does not implement independent primary-source authentication.

## Exact continuation order

1. Security owner action: private repo and access review before importing actual source records.
2. Finish G3: disk-backed event index (bounded memory), durable Windows metadata protocol and recovery/fault benchmarks; preserve source/manifest attestations.
3. Validate G4: effective-dated contract rules and fees; a single exact binary market ID, official venue-final result, and independent primary reference observation window **must actually intersect** the tested market (previous pilot had 0/18 overlap).
4. Full historical **Kalshi + Perps** conversion using authorized finalized local exports, with retained raw values, fractional units and timestamp/sequence quality. Do not claim compatibility from generic fixtures.
5. Full bounded chronological L2/tape replay; measured latency, cancellation/partial fills and queue model, possible MIT hftbacktest backtest-only adapter after parity/license gate.
6. Frozen legacy strategy registry and research-safe OOS evaluation, with repeated trials/market clusters tracked.
7. Complete paper execution lifecycle, operational monitoring and reporting, resource tests and explicit deployment review.

**Upstream source integration:** Still **0/25** third-party GitHub repos directly linked or vendored. New Rust Arrow/Parquet/serde/sha2/fs2/reqwest crates are separate dependencies. Prior 25-repo audit informs architecture and test design; no copied unlicensed or AGPL code.

## No-go constraints

- No authenticated independent settlement from actual historical markets or verified fee schedule.
- No real orderbook queue/latency execution parity; fills remain simulated.
- No fully tested WSS collector/archiver for continuous live capture, no real Perps adapter.
- Public repo, no private Phoenix input to CI.
- Windows power-loss durability and long-horizon scale/resource benchmarks incomplete.
- Do not merge stacked PRs, deploy, install, run collectors, alter original files or enable live trading.

**Next exact code task:** On a new branch based on reviewed PR #9, replace its finite in-memory event-ID index with disk-backed bounded indexing and add controlled Windows durability fault tests. In parallel, prepare an authorized privately executed one-market Kalshi/Perps historical schema and settlement join test. No new strategies until that test matches genuine independent settlement evidence.

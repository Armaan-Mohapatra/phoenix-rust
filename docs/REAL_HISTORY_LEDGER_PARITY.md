# Minimal real historical Phoenix paper-ledger parity check

This very small private-repository fixture was projected **without user account identifiers, private strategy parameters or credentials** from:
- User-supplied `LunaDelta_20261025T083801Z_research_part003.zip`
- Member `project/luna_interval_ledger.csv`
- Original member file SHA256 `6594c16bd097bc295e6ae6d3bcd16d5fad7b2c2aea9cf7b60f6f832b5b436b2b` (36,081,908 bytes, **not uploaded**)
- Exact market `KXBTC15M-26OCT241645-45`; October 24, 2026 paper entries at 20:34:12 UTC, recorded interval 20:30 to 20:45 UTC.
- YES one model contract: 0.5800 cost + 0.0171 recorded fee = 0.5971, archive reported YES payout → **+0.4029**.
- NO one model contract: 0.4300 cost + 0.0172 fee = 0.4472, archive reported YES payout → **-0.4472**.

Separate user-supplied archived `outputs/luna_settlement_reconciliation.json` says this ticker was **officially finalized YES**, checked at 20:46:02 UTC, source labeled `Kalshi official market endpoint`. This is only an archived report assertion, **not independently retrieved, authenticated or corroborated by primary CF reference observations in this task**.

Tests verify parsing exact numeric strings, entry timing, settlement-status gating and ledger arithmetic of two **real archived paper rows**. They do not prove actual fills or orderbook liquidity; the row prices and fees are archival values, not an independent applicable fee schedule.

**Limitations / blocked full historical replay:** no independent CF index open/close 60-second reference observations matched to this ticker; no independently verified contract-specific fee terms; no bounded orderbook delta/tape fixture for that exact 20:34:12 timestamp; no live/actual order fills. This test is **REAL ARCHIVED PAPER LEDGER PARITY ONLY** and must not be promoted to full historical strategy/execution validation.

Do not publish the original 36MB file or claim the earlier archive's label is independent settlement proof.

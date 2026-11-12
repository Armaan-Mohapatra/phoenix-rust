# Generic historical market-data input adapter

This package uses **caller-defined column maps**, not private Phoenix datasets or copied proprietary schemas. It accepts small Parquet sources, preserves original raw payloads, keeps source and receipt timestamps separate, and reports absent sequence/exchange-time values as `None`.

## What is tested

- Synthetic Arrow/Parquet columns, RFC3339 nanosecond timestamps, nullable sequences and exchange times.
- Deterministic event identities using source/market/session/kind/sequence, and a separately marked weak identity when sequence is unavailable.
- Original raw payload hash and no implicit binary-price conversion.
- Exact decimal parsing for Perps/reference observations without floating-point rounding.
- Strict input bounds (16 MiB file, 1024 rows, 16 KiB payload per event).

## What is not established

- No historical Phoenix export has been compiled through this adapter in GitHub CI; synthetic tests do not prove compatibility.
- Batch API is not yet a streaming reader for millions of rows. A bounded row-group streaming interface and disk-backed event index are needed before 100M+ history.
- Caller needs to supply real historical field mapping **locally after the GitHub repository is private**. Do not commit private mapped records, original ZIP files or machine paths.
- Snapshot/delta message payloads are preserved but not decoded into the domain L2 engine here.
- Actual Perps orderbook semantics, reference-index rules and settlement joins remain untested.

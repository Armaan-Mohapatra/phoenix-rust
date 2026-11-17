# TNEVENT segment format (TradingNext collector), as decoded and verified

Reverse-engineered from the finalized collector segments on the research host (no collector source was
found on disk). Every statement below was checked against real data; "NOT ESTABLISHED" marks what was not.
Implementation: `crates/tnevent` (`container.rs`, `event.rs`). Reader never opens `*.open` segments.

## Segment directory

`segment-NNNNNNNN.ready/` holds `events.bin`, `manifest.json`, `segment.json`. The manifest carries
`segment_format_version`, `event_count`, `file_bytes`, `record_bytes`, `max_event_bytes` (4 MiB) and the
SHA-256 of `events.bin`. All 463 finalized Kalshi segments match their manifest (SHA-256, size, event count).

## File layout

```text
file    = "TNEVENT\0"  u16be(version)  body
v1 body = event*                                   segments 1..22  (no compression)
v2 body = block*                                   segments 23..463
block   = 0x01  u32be(L)  u32be(raw_len)  u32be(crc32(raw))  zlib_stream[L]
raw     = event*                                   (decompressed block; tiles exactly)
event   = u32be(total)  u32be(crc32(rest))  rest[total]
rest    = u32be(meta_len)  meta_json[meta_len]  message[total - 4 - meta_len]
```

Verified on the real corpus: block header CRC32 over the decompressed block (1,787/1,787 blocks in the
probe segment); event CRC32 over `meta_len + meta + message` (435,885/435,885 events). A zlib stream is
complete (Adler-32 present) and each block is independent.

## Metadata array

`[1, venue, ticker, channel, type, sending_utc, field6_utc, received_utc, seq, generation, key]`

| idx | meaning | notes |
|---|---|---|
| 0 | metadata version | always 1 |
| 1 | venue | `kalshi` (also `perps` in the separate perps capture) |
| 2 | ticker | `*` for non-market REST events |
| 3 | channel | WebSocket `sid` ("1" order book, "2" lifecycle, "3" trades) or `REST:*` |
| 4 | type | see below |
| 5 | `sending_ts_ms` as UTC | venue clock; runs ~0.3 s AHEAD of local receipt |
| 6 | **NOT ESTABLISHED** | always earlier than field 7; carried, never used |
| 7 | local receipt time (µs) | the only clock research uses |
| 8 | seq | per (run, generation, sid) stream; `null` for REST |
| 9 | connection generation | restarts sequence numbers; 0 for REST |
| 10 | dedupe key | `run<hex>:g<gen>:sid<n>:seq<seq>:type<type>` |

Types seen: `orderbook_snapshot`, `orderbook_delta`, `trade`, `market_lifecycle_v2` (`created`,
`metadata_updated`, `close_date_updated`, `determined`, `settled`, `deactivated`), `event_lifecycle`,
`subscribed`/`ok`, REST `market_discovery`, `market_lifecycle_refresh`, `market_catalog` (perps).

## Messages

The message bytes are the venue's original JSON. Prices are dollar strings with up to 4 decimals
(`"0.9900"` → 9,900 ticks of 1e-4 USD); sizes are fixed-point contracts with 2 decimals (`"3040.50"`).
Parsing is exact: more precision than representable is an error, never a rounding.

* snapshot: `msg.yes_dollars_fp`, `msg.no_dollars_fp` = `[["price","qty"], ...]` (bids per side)
* delta: `msg.price_dollars`, `msg.delta_fp` (signed), `msg.side` (`yes`/`no`)
* lifecycle `metadata_updated`: `floor_strike`; `determined`: `result` (`yes`/`no`/`scalar`), `settlement_value`
* REST market listings: `markets[].{ticker, open_time, close_time, result, rules_primary, floor_strike}`

## Verification semantics (`book.rs`)

A market's book is `Verified` only after an `orderbook_snapshot` in the current connection generation and
stays verified only while every applicable update is accounted for. The sequence counter is global per
(run, generation, sid), so a gap breaks every market in the stream. Exact rules and their tests are in
`book.rs`; summary: no assumed-empty books, no backward reconstruction, duplicates ignored, regressions /
gaps / new generations / negative depth / crossed books / wrong-market records / inexact decimals break
the book until the next snapshot.

## Settlement evidence

`determined` lifecycle events (and, as a fallback, REST listings with a non-empty `result`) are the
**venue's own** result. They are not verified against the underlying index. Reports label this
`venue_determined`.

## Not available in this capture

No spot-reference price series (the "perps" capture is the exchange's own perpetual order book for ~2.5 h
and is not a reference for the 15-minute markets). Strategies that need a reference abstain.

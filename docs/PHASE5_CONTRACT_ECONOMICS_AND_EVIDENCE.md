# Phase 5 — Effective-dated contracts, fees and settlement evidence

STATUS: CODE_WRITTEN_UNTESTED. No locally executed Rust, historical trade replay, GitHub Actions, Codex, or external Kalshi/CF Benchmarks queries. No real historical fees or contract rules are bundled.

## Source architecture

- New standalone integrations/contract-economics crate depends on the existing root phoenix-domain fixed-point Money, Price, FeeRate and settle_binary APIs, and the existing root phoenix-settlement compare_window_averages. It does not recalculate floats or assume a current standard fee for historic markets.
- ContractRegistry stores individually sourced contract definitions with exact ticker and market ID, event/series, binary payoff, opening/closing 1-Hz rule windows, tie treatment, rule effective and published/observed dates, methodology revision and SHA-256 references. Missing, overlapping, retroactively published, canceled/void/scalar/unsupported rules fail closed.
- FeeRegistry stores bounded exact series/role/effective schedules with explicit formula (existing quadratic FeeRate or flat-per-contract), numerator, denominator, per-fill versus per-price-level aggregation, centicent rounding step, optional minimum and maximum, and documentary source SHA. No match yields UNKNOWN_FEE, never zero. Only charge_at() yields a fee with a verified effective-time and series binding suitable for modeled accounting.
- The existing integrations/kalshi-read-only client now includes a bounded GET-only, no-auto-retry official_result() adapter, exact ticker/market ID checks, a shared client-level minimum-request interval (default one second), and HTTP 10-second timeout. Pure mock parser is UNVERIFIED_LOCAL_OFFICIAL_PAYLOAD and cannot promote to a fresh official final. HTTPS GET from the reviewed fixed public market host is the only in-code path that marks authenticated official provenance.
- Optional integrations/contract-economics/src/primary.rs accepts locally supplied authorized immutable JSON primary observations using exact integer strings (value_1e8), bounded input bytes and sample count, original local bytes SHA-256, canonical 1Hz tick digest, provider identity, methodology revision and retrieved/observed times. It rejects duplicate timestamps, floating input, impossible pre-observation timestamps and nonmatching source-document digests. NO credentials, copyrighted tick archives, or CF network clients are bundled.
- PrimaryProvenanceVerifier is an injected trusted-provider verification boundary. An unverified local export is not independent CF evidence. compare_window_averages requires an actual primary tick at every second in both specified windows and rejects duplicates/missing values; real entitlement/primary-authentication evidence remains an external prerequisite.
- Separate evidence tiers include unknown/pending, archived-paper, source-derived provisional, fresh-official, independently corroborated and conflict. A private assessment marker prevents an arbitrary outside struct literal from claiming authenticated primary status. Independent state also requires exact official source hash and winner matching, ticker, rule windows, methodology and source identity. This API still requires a genuinely trustworthy PrimaryProvenanceVerifier at deployment; an arbitrary injected verifier must never be mistaken for authenticated provider proof.
- modeled_expiry_pnl delegates to existing domain::settle_binary with a modeled fill, exact historical fee and official result, records net P&L only with those actual inputs and keeps modeled fill distinct from real venue execution. A separate optional assessment may upgrade independent provenance only when the authenticated primary gate passed. Negative results are never suppressed.

## BTC pilot evidence requirements

Market: KXBTC15M-26OCT241900-00. Opening 2026-10-24 22:44:00Z–22:45:00Z, closing 2026-10-24 22:59:00Z–23:00:00Z, each requiring 60 exact seconds and rule-specific ties/boundaries. These timestamps were supplied for research, not proof that official historical contract rules have been authenticated. No actual BRTI primary data, Kalshi official results, fee-schedule revision or authenticated contract terms have been obtained. All real historical independent/outcome claims remain BLOCKED.

The official market API response may expose the canonical ticker without a separate market_id field. The current strict parser rejects missing market_id rather than fabricating an identity. Matching to a properly evidenced canonical ID from another authoritative source is an explicit future adapter/validation gate, not a fabricated default.

## Authored tests, not run

Contract: missing/overlapping/future versions, unsupported scalar and canceled types, exact reference window and tie conditions. Fees: maker/taker differences, out-of-date schedules, price-dependent charge via existing FeeRate, level-versus-fill rounding, huge input overflow, minimum and maximum metadata. Official: mock settled YES/NO, pending closed, invalid ticker, missing market ID, canceled/void/scalar. Primary: 1-Hz complete pilot-like windows, missing and duplicate values, unsupported float encodings, source hash and observation-time checks. Evidence: paper and derived do not become official; user-imported official JSON stays unverified; independent tier requires trusted provider and official provenance. Accounting: no settled net P&L with absent fee or official outcome.

## Deferred local verification commands (not executed)

    cargo test --manifest-path integrations/contract-economics/Cargo.toml --all-targets
    cargo test --manifest-path integrations/kalshi-read-only/Cargo.toml --all-targets
    cargo test -p phoenix-domain --all-targets
    cargo test -p phoenix-settlement --all-targets
    cargo clippy --manifest-path integrations/contract-economics/Cargo.toml --all-targets

Authorized CF Benchmarks credentials must be supplied locally through a separately audited, read-only primary-source implementation. No credential code or tokens have been pushed to GitHub. The historical contract/rule/fee registries ship empty by design; all recorded real-source evidence remains missing until authoritative historical terms are obtained.

No production collectors, order submission, customer accounts, active databases or production source files were touched.
# Phase 4: source-aware historical fractional orderbook integrity

Status: CODE_WRITTEN_UNTESTED. No Cargo, rustfmt, Clippy, GitHub Actions, Codex or original-source replay executed.

The new source-loader::book_integrity layer delegates all price and hundredths-of-contract depth updates to the existing forensic::Book. It is an evidence overlay, not a replacement exchange, maker queue, or duplicate market-data framework. Phase 3 pipeline includes a BookEvidence object in its versioned report. Existing source hashes and causal cutoff remain intact.

BookClass classifications: UNKNOWN_INITIALIZATION, MISSING_SNAPSHOT, PARTIAL_INITIALIZATION, OBSERVED_SNAPSHOT, OBSERVED_RESNAPSHOT, SEQUENCE_GAP, RECONNECT_UNVERIFIED, INVALID_BOOK, STALE_BOOK and SOURCE_CLOCK_UNCERTAIN. A parsed source snapshot is merely observed; it is NOT authenticated first-subscription initialization by itself.

Shared-channel ChannelRun diagnostics are calculated by source-loader before market filtering, grouped by connection+SID+channel for a bounded source window. Missing sequences on the BTC-only stream do not independently prove lost Kalshi messages. The evidence layer differentiates observed-window gaps, duplicates, regressions and missing channel coverage. It always leaves complete_subscription_history_verified=false until a separately authenticated full-channel source exists.

forensic::Book now builds both sides of a snapshot before installing either; it rejects a crossed complementary snapshot and invalid delta. Depth updates remain checked i128, exact hundredths, with negative quantities and fractional integer conversion refused. No verified executable liquidity or maker fill is claimed. Quote age and clock uncertainty are reported separately. New SID or connection changes require a new observed snapshot; old book quotes cannot qualify.

A bounded pure rank_snapshot_candidates API accepts up to 256 manifest-derived candidate metadata records (finalized status, snapshot count, source date span, gaps, first snapshot) and returns a reproducible ranking. This is NOT live market discovery and cannot claim any market is actually finalized without user-supplied authoritative metadata. No source archive scans are performed.

Authored regression fixtures NOT RUN: missing opening snapshot, observed original-format fractional snapshot/delta, market-filtered multi-ticker ambiguity, shared-channel gap and duplicate, reconnect and changed SID, invalid complementary/crossed book, stale quote, invalid/no-lookahead checkpoints and coverage candidate ranking. Existing forensic, kalshi-wire and source-loader tests remain unchanged except for atomic/cross checks.

Remaining evidence gates: complete original WSS subscription from its actual initial snapshot, all reconnect histories, verified source-session provenance, snapshot hashes from actual full event tape, real source comparisons, and any execution eligibility are UNVERIFIED. The reported model execution eligibility is therefore false for existing partial archives. Phase 5 remains solely about economic rules and independent settlement.
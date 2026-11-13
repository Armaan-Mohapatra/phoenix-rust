# Primary independent CF Benchmarks BRTI settlement data requirement

Target: `KXBTC15M-26OCT241900-00`, UTC 2026-10-24, open average window [22:44:00,22:45:00), close average [22:59:00,23:00:00).

**Public documentation verified, historical data itself NOT available without license.**
- Official CF Benchmarks history API: https://docs.cfbenchmarks.com/api/rest/historical-values/
- Endpoint: `GET https://www.cfbenchmarks.com/api/v1/history/values?id=BRTI&timespan=HOUR&timestamp=2026-10-24T22:00:00.000Z`.
- Requires an authorized CF Benchmarks index subscription with `STREAM_HISTORICAL_VALUES` permission, API username/key supplied privately outside source control. No key is available to this session. Do not attempt to bypass access controls.
- Expected source: original **primary CF Benchmarks BRTI** time-value series for the stated UTC hour, 1Hz timestamps sufficient to cover both 60-second windows, with verifiable retrieval time, provider response digest, index methodology revision, and any amendment/repeat history. Independently verify proper rounding and missing-value/void rules.
- The 120 already archived source ticks were relayed through **Kalshi-authenticated WebSocket**, not a separate CF primary response. Their agreement with archived Kalshi YES is useful but insufficient for the independent evidence tier.
- On successful authorized retrieval: preserve **minimal** metadata (source URI, normalized timestamp coverage, SHA256, sum/mean, revision/provenance ID, exact ticker/rules identity) in a private research report. Do not commit raw copyrighted licensed benchmark history or authentication credentials.
- If access remains unavailable, classify `BLOCKED_INDEPENDENT_SETTLEMENT`, never `FULLY_VERIFIED`.

Current general Kalshi explanation: https://help.kalshi.com/en/articles/13823838-crypto-markets (15m compares preceding opening and closing minute averages; CF API relayed series may reflect revisions). General descriptions do not override exact historical contract terms.

No primary CF settlement value was fetched by this development session.

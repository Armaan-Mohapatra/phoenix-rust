# Proposed third-party adoption (source review, not integration claims)

All code currently in this repository is original and does not vendor the external projects below. A source may be adopted only after pinned SHA, LICENSE/NOTICE, behavioral parity and dependency review.

| Source | Specific purpose | Status |
|---|---|---|
| nkaz001/hftbacktest (MIT) | Reuse or adapt Rust backtest-only queue/latency modeling | CANDIDATE, not linked |
| Jon-Becker/prediction-market-analysis (MIT) | Incremental Parquet schemas, dedupe, manifest/indexing | Design/test reference |
| YichengYang-Ethan/oracle3-prediction-market-agent (Apache-2.0) | Fee-aware implication/exclusivity constraint hypotheses | Translate to Rust later |
| ryanfrigo/kalshi-ai-trading-bot (MIT) | Forecast calibration, executable odds, abstention | Partial concepts reproduced independently |
| betcode-org/flumine (MIT) | Event/order lifecycle tests, cancellations, partial fills | Reference, not a Kalshi engine |
| pmxt-dev/pmxt (MIT) | Read-only normalized multi-venue data interfaces | Reference |
| braedonsaunders/homerun (AGPL-3.0) | L2 replay and walk-forward research architecture | Reference ONLY, no code copied |
| komako-workshop/digital-oracle (MIT) | Timestamped reference sources and fixtures | Future |
| rodlaf/KalshiMarketMaker (MIT) | Maker inventory risk and spread strategy hypotheses | Future |
| OctagonAI/kalshi-trading-bot-cli (MIT) | Model-comparison and research journaling | Future |
| tarpboi/Prediction-Markets-Trading-Bot-Toolkits (MIT) | Rust venue abstraction and risk controls | Future |
| alsk1992/CloddsBot (MIT) | Decision traces and risk constraints | Reference, no agent automation |
| taetaehoho/poly-kalshi-arb (unlicensed) | Paired-leg risk hypotheses | No copy |
| ImMike/polymarket-arbitrage (unlicensed) | Cross-venue matching ideas | No copy |
| CarlosIbCu/polymarket-kalshi-btc-arbitrage-bot (MIT) | Contract strike identity tests | Future |
| meloner3/poly-kalshi-sports-bot (unlicensed) | Event matching | No copy |
| dexorynlabs/polymarket-trading-bots (MIT) | Extreme-price strategy stress tests | Reference only |
| suislanchez/polymarket-kalshi-weather-bot (unlicensed) | Weather ensemble research | Future |
| machina-sports/sports-skills (MIT) | Sports event source mapping | Future |
| livetennisapi/polymarket-tennis (MIT) | Observe-only tennis result matching | Future |
| kuest/prediction-market (custom) | Building a new exchange is out of scope | Exclude |
| aarora4/Awesome-Prediction-Market-Tools (no declared license) | Discovery index | Reference only |
| Nour-Br/Kalshi-crypto-15m-trading-bot (unlicensed) | No reproducible public methodology established | No copy |
| Kalshi/kalshi-starter-code-python (unlicensed) | Reference API requests and auth signatures | No copy |
| abhaymettu/kalshi-calibration (unlicensed, archived) | Brier/fee research questions | No copy |

All annotations are provisional metadata and not code-license legal determinations. Incomplete modules and backtest marketing claims are not accepted as production-grade evidence.

# Source-backed performance reporting — synthetic baseline

The new `phoenix-performance-report` crate summarizes **modeled** pipeline evaluations, using stable ordering and an explicit dataset SHA256 and strategy version.

The result counts abstentions, no-fills, pending outcomes, venue-final modeled settlements and purported independent evidence *claims* separately. It never turns missing P&L into $0, drops negative trades or calls modeled P&L actual money.

A compact JSON report includes model fees, settled-only P&L, win/loss counts, drawdown and discovery/validation/untouched sample counts.

Only synthetic tests are available. Current code does not authenticate primary settlement evidence, compute full account NAV through partial fills, perform statistically corrected hypothesis testing, or publish periodic production reports. Actual data-quality and portfolio comparisons require an approved private data integration.

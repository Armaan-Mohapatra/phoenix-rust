//! Phase 8 source-compatible, bounded Perps feature service.
//! Original Perps dataset fields remain UNVERIFIED until real source schemas
//! are supplied. No Perps trading connection, funding or order methods.
use phoenix_historical_input::perps_features::{CausalPerpsBuffer, PerpsFeatureCheckpoint};
use phoenix_historical_input::{
    perps::{convert_bounded_partition, PartitionContract, PerpsSchema},
    ColumnMapping,
};
use phoenix_source_loader::multi_market::{MarketBatchOutcome, MarketSpec};
use phoenix_source_loader::perps_bridge::{join_market_features, KalshiPerpsFeatureReport};
use std::collections::BTreeSet;
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    InvalidSource,
    DuplicateOrOutOfOrder,
    UnknownMarket,
    InvalidCheckpoint,
    Capacity,
}
#[derive(Default)]
pub struct PerpsFeatureService {
    buffer: CausalPerpsBuffer,
    published_rows: u64,
}
impl PerpsFeatureService {
    pub fn new() -> Self {
        Self::default()
    }
    /// One finalized byte-SHA and schema-SHA validated partition at a time;
    /// source collation and joins stay inside the Phase 8 implementation.
    pub fn import_finalized(
        &mut self,
        bytes: &[u8],
        columns: &ColumnMapping<'_>,
        schema: &PerpsSchema,
        contract: &PartitionContract,
    ) -> Result<usize, Error> {
        let rows = convert_bounded_partition(bytes, columns, schema, contract)
            .map_err(|_| Error::InvalidSource)?;
        let mut staged = CausalPerpsBuffer::restore(
            &self
                .buffer
                .checkpoint(&"a".repeat(64))
                .map_err(|_| Error::InvalidCheckpoint)?,
            &"a".repeat(64),
        )
        .map_err(|_| Error::InvalidCheckpoint)?;
        let mut accepted = 0usize;
        for row in rows {
            if staged
                .ingest(row)
                .map_err(|_| Error::DuplicateOrOutOfOrder)?
            {
                accepted += 1;
            }
        }
        self.buffer = staged;
        self.published_rows = self
            .published_rows
            .checked_add(accepted as u64)
            .ok_or(Error::Capacity)?;
        Ok(accepted)
    }
    #[allow(clippy::too_many_arguments)] // explicit evidence inputs; a params struct is a follow-up API change
    pub fn joined_features(
        &self,
        spec: &MarketSpec,
        market: &MarketBatchOutcome,
        quote_asset: &str,
        other_asset: Option<&str>,
        venues: BTreeSet<String>,
        max_quote_age_ns: i64,
        lookback_ns: i64,
    ) -> Result<KalshiPerpsFeatureReport, Error> {
        join_market_features(
            spec,
            market,
            &self.buffer,
            quote_asset,
            other_asset,
            venues,
            max_quote_age_ns,
            lookback_ns,
            true,
        )
        .map_err(|_| Error::UnknownMarket)
    }
    pub fn checkpoint(&self, revision_sha256: &str) -> Result<PerpsFeatureCheckpoint, Error> {
        self.buffer
            .checkpoint(revision_sha256)
            .map_err(|_| Error::InvalidCheckpoint)
    }
    pub fn restore(
        &mut self,
        c: &PerpsFeatureCheckpoint,
        revision_sha256: &str,
    ) -> Result<(), Error> {
        let restored =
            CausalPerpsBuffer::restore(c, revision_sha256).map_err(|_| Error::InvalidCheckpoint)?;
        self.buffer = restored;
        Ok(())
    }
    pub fn instrument_count(&self) -> usize {
        self.buffer.instrument_count()
    }
    pub fn observation_count(&self) -> usize {
        self.buffer.observation_count()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_service_never_claims_source_instrument_coverage() {
        let s = PerpsFeatureService::new();
        assert_eq!(s.instrument_count(), 0);
        assert_eq!(s.observation_count(), 0);
        let cp = s.checkpoint(&"a".repeat(64)).unwrap();
        assert!(cp.pairs.is_empty());
        assert!(!cp.pairs_sha256.is_empty());
    }
    #[test]
    fn altered_resume_source_version_rejected() {
        let mut s = PerpsFeatureService::new();
        let cp = s.checkpoint(&"a".repeat(64)).unwrap();
        assert!(s.restore(&cp, &"b".repeat(64)).is_err());
    }
}

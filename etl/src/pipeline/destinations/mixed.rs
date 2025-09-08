use async_trait::async_trait;
use std::collections::HashMap;
use thiserror::Error;
use tokio_postgres::types::PgLsn;

use crate::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::PipelineResumptionState,
};
use postgres::schema::{TableId, TableSchema};

use super::{BatchDestination, DestinationError};

#[cfg(feature = "bigquery")]
use super::bigquery::BigQueryBatchDestination;
#[cfg(feature = "clickhouse")]
use super::clickhouse::ClickHouseBatchDestination;

/// A destination that can hold multiple types of concrete destinations.
///
/// This enum allows you to treat different destination types uniformly
/// while maintaining type safety and specific functionality.
pub enum MixedDestination {
    #[cfg(feature = "bigquery")]
    BigQuery(BigQueryBatchDestination),
    #[cfg(feature = "clickhouse")]
    ClickHouse(ClickHouseBatchDestination),
}

/// Error type for mixed destination operations.
#[derive(Debug, Error)]
#[error("Mixed destination error: {0}")]
pub struct MixedDestinationError(pub anyhow::Error);

impl From<anyhow::Error> for MixedDestinationError {
    fn from(err: anyhow::Error) -> Self {
        MixedDestinationError(err)
    }
}

impl DestinationError for MixedDestinationError {}

#[async_trait]
impl BatchDestination for MixedDestination {
    type Error = MixedDestinationError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        match self {
            #[cfg(feature = "bigquery")]
            MixedDestination::BigQuery(dest) => dest
                .get_resumption_state()
                .await
                .map_err(|e| MixedDestinationError(e.into())),
            #[cfg(feature = "clickhouse")]
            MixedDestination::ClickHouse(dest) => dest
                .get_resumption_state()
                .await
                .map_err(|e| MixedDestinationError(e.into())),
        }
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        match self {
            #[cfg(feature = "bigquery")]
            MixedDestination::BigQuery(dest) => dest
                .write_table_schemas(table_schemas)
                .await
                .map_err(|e| MixedDestinationError(e.into())),
            #[cfg(feature = "clickhouse")]
            MixedDestination::ClickHouse(dest) => dest
                .write_table_schemas(table_schemas)
                .await
                .map_err(|e| MixedDestinationError(e.into())),
        }
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        match self {
            #[cfg(feature = "bigquery")]
            MixedDestination::BigQuery(dest) => dest
                .write_table_rows(rows, table_id)
                .await
                .map_err(|e| MixedDestinationError(e.into())),
            #[cfg(feature = "clickhouse")]
            MixedDestination::ClickHouse(dest) => dest
                .write_table_rows(rows, table_id)
                .await
                .map_err(|e| MixedDestinationError(e.into())),
        }
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        match self {
            #[cfg(feature = "bigquery")]
            MixedDestination::BigQuery(dest) => dest
                .write_cdc_events(events)
                .await
                .map_err(|e| MixedDestinationError(e.into())),
            #[cfg(feature = "clickhouse")]
            MixedDestination::ClickHouse(dest) => dest
                .write_cdc_events(events)
                .await
                .map_err(|e| MixedDestinationError(e.into())),
        }
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        match self {
            #[cfg(feature = "bigquery")]
            MixedDestination::BigQuery(dest) => dest
                .table_copied(table_id)
                .await
                .map_err(|e| MixedDestinationError(e.into())),
            #[cfg(feature = "clickhouse")]
            MixedDestination::ClickHouse(dest) => dest
                .table_copied(table_id)
                .await
                .map_err(|e| MixedDestinationError(e.into())),
        }
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        match self {
            #[cfg(feature = "bigquery")]
            MixedDestination::BigQuery(dest) => dest
                .truncate_table(table_id)
                .await
                .map_err(|e| MixedDestinationError(e.into())),
            #[cfg(feature = "clickhouse")]
            MixedDestination::ClickHouse(dest) => dest
                .truncate_table(table_id)
                .await
                .map_err(|e| MixedDestinationError(e.into())),
        }
    }
}

impl MixedDestination {
    /// Create a new BigQuery mixed destination.
    #[cfg(feature = "bigquery")]
    pub fn bigquery(destination: BigQueryBatchDestination) -> Self {
        MixedDestination::BigQuery(destination)
    }

    /// Create a new ClickHouse mixed destination.
    #[cfg(feature = "clickhouse")]
    pub fn clickhouse(destination: ClickHouseBatchDestination) -> Self {
        MixedDestination::ClickHouse(destination)
    }

    /// Get the destination type as a string for logging/debugging.
    pub fn destination_type(&self) -> &'static str {
        match self {
            #[cfg(feature = "bigquery")]
            MixedDestination::BigQuery(_) => "bigquery",
            #[cfg(feature = "clickhouse")]
            MixedDestination::ClickHouse(_) => "clickhouse",
        }
    }

    /// Check if this is a BigQuery destination.
    #[cfg(feature = "bigquery")]
    pub fn is_bigquery(&self) -> bool {
        matches!(self, MixedDestination::BigQuery(_))
    }

    /// Check if this is a ClickHouse destination.
    #[cfg(feature = "clickhouse")]
    pub fn is_clickhouse(&self) -> bool {
        matches!(self, MixedDestination::ClickHouse(_))
    }
}

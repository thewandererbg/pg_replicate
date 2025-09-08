use std::collections::HashMap;

use async_trait::async_trait;
use postgres::schema::{TableId, TableSchema};
use thiserror::Error;
use tokio_postgres::types::PgLsn;

use crate::conversions::{cdc_event::CdcEvent, table_row::TableRow};

use super::PipelineResumptionState;

#[cfg(feature = "bigquery")]
pub mod bigquery;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "stdout")]
pub mod stdout;

// Mixed destination is available when both bigquery and clickhouse features are enabled
#[cfg(all(feature = "bigquery", feature = "clickhouse"))]
pub mod mixed;

pub trait DestinationError: std::error::Error + Send + Sync + 'static {}

#[derive(Debug, Error)]
#[error("unreachable")]
pub enum InfallibleDestinationError {}
impl DestinationError for InfallibleDestinationError {}

#[async_trait]
pub trait BatchDestination {
    type Error: DestinationError;
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error>;
    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error>;
    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error>;
    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error>;
    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error>;
    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error>;
}

// Re-exports for convenience when features are enabled
#[cfg(feature = "bigquery")]
pub use bigquery::BigQueryBatchDestination;

#[cfg(feature = "clickhouse")]
pub use clickhouse::ClickHouseBatchDestination;

#[cfg(feature = "stdout")]
pub use stdout::StdoutDestination;

#[cfg(all(feature = "bigquery", feature = "clickhouse"))]
pub use mixed::{MixedDestination, MixedDestinationError};

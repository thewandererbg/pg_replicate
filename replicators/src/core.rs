use std::{collections::HashMap, io::BufReader, vec};

use crate::config::load_replicator_config;
use config::shared::{DestinationConfig, ReplicatorConfig};

use async_trait::async_trait;
use etl::{
    conversions::{cdc_event::CdcEvent, table_row::TableRow},
    pipeline::{
        batching::{data_pipeline::BatchDataPipeline, BatchConfig},
        destinations::{
            bigquery::BigQueryBatchDestination, clickhouse::ClickHouseBatchDestination,
            BatchDestination, DestinationError,
        },
        sources::postgres::{PostgresSource, TableNamesFrom},
        PipelineAction, PipelineResumptionState,
    },
    SslMode,
};
use postgres::{
    schema::{TableId, TableSchema},
    tokio::config::PgConnectionConfig,
};
use thiserror::Error;
use tokio_postgres::types::PgLsn;
use tracing::{error, info};

#[derive(Debug, Error)]
pub enum ReplicatorError {
    #[error("The destination {0} is currently unsupported")]
    UnsupportedDestination(String),
}

// Simple enum approach - let the pipeline handle the specifics
pub enum MixedDestination {
    BigQuery(BigQueryBatchDestination),
    ClickHouse(ClickHouseBatchDestination),
}

// Create a unified error type
#[derive(Debug, Error)]
#[error(transparent)]
pub struct UnifiedDestinationError(anyhow::Error);

impl DestinationError for UnifiedDestinationError {}

impl From<anyhow::Error> for UnifiedDestinationError {
    fn from(err: anyhow::Error) -> Self {
        UnifiedDestinationError(err)
    }
}

// Implement BatchDestination for MixedDestination
#[async_trait]
impl BatchDestination for MixedDestination {
    type Error = UnifiedDestinationError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        match self {
            MixedDestination::BigQuery(dest) => dest
                .get_resumption_state()
                .await
                .map_err(|e| UnifiedDestinationError(e.into())),
            MixedDestination::ClickHouse(dest) => dest
                .get_resumption_state()
                .await
                .map_err(|e| UnifiedDestinationError(e.into())),
        }
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        match self {
            MixedDestination::BigQuery(dest) => dest
                .write_table_schemas(table_schemas)
                .await
                .map_err(|e| UnifiedDestinationError(e.into())),
            MixedDestination::ClickHouse(dest) => dest
                .write_table_schemas(table_schemas)
                .await
                .map_err(|e| UnifiedDestinationError(e.into())),
        }
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        match self {
            MixedDestination::BigQuery(dest) => dest
                .write_table_rows(rows, table_id)
                .await
                .map_err(|e| UnifiedDestinationError(e.into())),
            MixedDestination::ClickHouse(dest) => dest
                .write_table_rows(rows, table_id)
                .await
                .map_err(|e| UnifiedDestinationError(e.into())),
        }
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        match self {
            MixedDestination::BigQuery(dest) => dest
                .write_cdc_events(events)
                .await
                .map_err(|e| UnifiedDestinationError(e.into())),
            MixedDestination::ClickHouse(dest) => dest
                .write_cdc_events(events)
                .await
                .map_err(|e| UnifiedDestinationError(e.into())),
        }
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        match self {
            MixedDestination::BigQuery(dest) => dest
                .table_copied(table_id)
                .await
                .map_err(|e| UnifiedDestinationError(e.into())),
            MixedDestination::ClickHouse(dest) => dest
                .table_copied(table_id)
                .await
                .map_err(|e| UnifiedDestinationError(e.into())),
        }
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        match self {
            MixedDestination::BigQuery(dest) => dest
                .truncate_table(table_id)
                .await
                .map_err(|e| UnifiedDestinationError(e.into())),
            MixedDestination::ClickHouse(dest) => dest
                .truncate_table(table_id)
                .await
                .map_err(|e| UnifiedDestinationError(e.into())),
        }
    }
}

pub async fn start_replicator() -> anyhow::Result<()> {
    let replicator_config = load_replicator_config()?;
    let source = &replicator_config.source;

    info!(
        host = &source.host,
        port = source.port,
        dbname = &source.name,
        username = &source.username,
        slot_name = &source.slot_name,
        "source settings"
    );

    // Log destination settings with per-destination batch configs
    for (index, destination_config) in replicator_config.get_destinations().iter().enumerate() {
        match destination_config {
            DestinationConfig::BigQuery {
                project_id,
                dataset_id,
                gcp_sa_key_path,
                max_staleness_mins,
                batch,
            } => {
                info!(
                    destination_index = index,
                    project_id,
                    dataset_id,
                    gcp_sa_key_path,
                    max_staleness_mins,
                    max_size = batch.max_size,
                    max_fill_ms = batch.max_fill_ms,
                    "destination settings (BigQuery)"
                );
            }
            DestinationConfig::ClickHouse {
                url,
                database,
                username,
                batch,
                ..
            } => {
                info!(
                    destination_index = index,
                    url,
                    database,
                    username,
                    max_size = batch.max_size,
                    max_fill_ms = batch.max_fill_ms,
                    "destination settings (ClickHouse)"
                );
            }
            _ => {
                info!(destination_index = index, destination = ?destination_config, "destination settings (unsupported)");
            }
        }
    }

    info!(
        publication_name = &replicator_config.pipeline.publication_name,
        destination_count = replicator_config.destination_count(),
        "pipeline settings"
    );

    // SSL setup
    let mut trusted_root_certs = vec![];
    let ssl_mode = if source.tls.enabled {
        let mut root_certs_reader = BufReader::new(source.tls.trusted_root_certs.as_bytes());
        for cert in rustls_pemfile::certs(&mut root_certs_reader) {
            trusted_root_certs.push(cert?);
        }
        SslMode::VerifyFull
    } else {
        SslMode::Disable
    };

    let postgres_source = PostgresSource::new(
        PgConnectionConfig {
            host: source.host.clone(),
            port: source.port,
            name: source.name.clone(),
            username: source.username.clone(),
            password: source.password.clone().map(Into::into),
            ssl_mode,
        },
        trusted_root_certs,
        Some(source.slot_name.clone()),
        TableNamesFrom::Publication(replicator_config.pipeline.publication_name.clone()),
    )
    .await?;

    // Initialize destinations with their individual batch configs
    let destinations_with_configs = init_destinations_with_configs(&replicator_config).await?;

    // Create pipeline with per-destination configs
    BatchDataPipeline::new(
        postgres_source,
        destinations_with_configs,
        PipelineAction::Both,
    )
    .start()
    .await?;

    Ok(())
}

async fn init_destinations_with_configs(
    config: &ReplicatorConfig,
) -> anyhow::Result<Vec<(MixedDestination, BatchConfig, String)>> {
    let mut destinations_with_configs = Vec::new();

    for (index, destination_config) in config.get_destinations().iter().enumerate() {
        match destination_config {
            DestinationConfig::BigQuery {
                project_id,
                dataset_id,
                gcp_sa_key_path,
                max_staleness_mins,
                batch,
            } => {
                info!(
                    destination_index = index,
                    project_id,
                    dataset_id,
                    max_size = batch.max_size,
                    max_fill_ms = batch.max_fill_ms,
                    "Initializing BigQuery destination"
                );
                let dest = BigQueryBatchDestination::new_with_key_path(
                    project_id.clone(),
                    dataset_id.clone(),
                    gcp_sa_key_path,
                    *max_staleness_mins,
                )
                .await?;

                let batch_config = BatchConfig::from_config(batch.max_size, batch.max_fill_ms);
                let destination_id = format!("bigquery_{}", index);

                destinations_with_configs.push((
                    MixedDestination::BigQuery(dest),
                    batch_config,
                    destination_id,
                ));
            }
            DestinationConfig::ClickHouse {
                url,
                database,
                username,
                password,
                batch,
            } => {
                info!(
                    destination_index = index,
                    url,
                    database,
                    username,
                    max_size = batch.max_size,
                    max_fill_ms = batch.max_fill_ms,
                    "Initializing ClickHouse destination"
                );
                let dest = ClickHouseBatchDestination::new_with_credentials(
                    url.clone(),
                    database.clone(),
                    username.clone(),
                    password.clone(),
                )
                .await?;

                let batch_config = BatchConfig::from_config(batch.max_size, batch.max_fill_ms);
                let destination_id = format!("clickhouse_{}", index);

                destinations_with_configs.push((
                    MixedDestination::ClickHouse(dest),
                    batch_config,
                    destination_id,
                ));
            }
            _ => {
                return Err(ReplicatorError::UnsupportedDestination(format!(
                    "Destination {}: {:?}",
                    index, destination_config
                ))
                .into());
            }
        }
    }

    info!(
        destination_count = destinations_with_configs.len(),
        "Successfully initialized all destinations with individual batch configs"
    );
    Ok(destinations_with_configs)
}

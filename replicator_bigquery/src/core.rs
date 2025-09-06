use std::{io::BufReader, time::Duration, vec};

use crate::config::load_replicator_config;
use config::shared::{DestinationConfig, ReplicatorConfig};

use etl::{
    pipeline::{
        batching::{data_pipeline::BatchDataPipeline, BatchConfig},
        destinations::bigquery::BigQueryBatchDestination,
        sources::postgres::{PostgresSource, TableNamesFrom},
        PipelineAction,
    },
    SslMode,
};
use postgres::tokio::config::PgConnectionConfig;
use thiserror::Error;
use tracing::{error, info};

#[derive(Debug, Error)]
pub enum ReplicatorError {
    #[error("The destination {0} is currently unsupported")]
    UnsupportedDestination(String),
}

pub async fn start_replicator() -> anyhow::Result<()> {
    let replicator_config = load_replicator_config()?;
    let source = &replicator_config.source;

    info!(source = ?replicator_config.source);
    info!(destinations = ?replicator_config.destinations);

    info!(
        max_size = &replicator_config.pipeline.batch.max_size,
        max_fill_ms = &replicator_config.pipeline.batch.max_fill_ms,
        publication_name = &replicator_config.pipeline.publication_name,
        destination_count = replicator_config.destination_count(),
        "pipeline settings"
    );

    // Set up certificates and SSL mode.
    let mut trusted_root_certs = vec![];
    let ssl_mode = if source.tls.enabled {
        let mut root_certs_reader = BufReader::new(source.tls.trusted_root_certs.as_bytes());
        for cert in rustls_pemfile::certs(&mut root_certs_reader) {
            let cert = cert?;
            trusted_root_certs.push(cert);
        }
        SslMode::VerifyFull
    } else {
        SslMode::Disable
    };

    let options = PgConnectionConfig {
        host: source.host.clone(),
        port: source.port,
        name: source.name.clone(),
        username: source.username.clone(),
        password: source.password.clone().map(Into::into),
        ssl_mode,
    };

    let postgres_source = PostgresSource::new(
        options,
        trusted_root_certs,
        Some(source.slot_name.clone()),
        TableNamesFrom::Publication(replicator_config.pipeline.publication_name.clone()),
    )
    .await?;

    let destinations = init_destinations(&replicator_config).await?;

    let batch_config = BatchConfig::new(
        replicator_config.pipeline.batch.max_size,
        Duration::from_millis(replicator_config.pipeline.batch.max_fill_ms),
    );

    let mut pipeline = BatchDataPipeline::new(
        postgres_source,
        destinations,
        PipelineAction::Both,
        batch_config,
    );

    pipeline.start().await?;
    Ok(())
}

async fn init_destinations(
    config: &ReplicatorConfig,
) -> anyhow::Result<Vec<BigQueryBatchDestination>> {
    let mut destinations = Vec::new();

    for (index, destination_config) in config.destinations.as_vec().iter().enumerate() {
        match destination_config {
            DestinationConfig::BigQuery {
                project_id,
                dataset_id,
                gcp_sa_key_path,
                max_staleness_mins,
            } => {
                info!(
                    destination_index = index,
                    project_id = project_id,
                    dataset_id = dataset_id,
                    "Initializing BigQuery destination"
                );

                let destination = BigQueryBatchDestination::new_with_key_path(
                    project_id.clone(),
                    dataset_id.clone(),
                    gcp_sa_key_path,
                    *max_staleness_mins,
                )
                .await?;
                destinations.push(destination);
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
        destination_count = destinations.len(),
        "Successfully initialized all destinations"
    );
    Ok(destinations)
}

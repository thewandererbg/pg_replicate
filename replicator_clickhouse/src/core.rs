use std::io::BufReader;

use crate::config::load_replicator_config;
use config::shared::{DestinationConfig, ReplicatorConfig};

use etl::{
    pipeline::{
        batching::{data_pipeline::BatchDataPipeline, BatchConfig},
        destinations::{
            clickhouse::ClickHouseBatchDestination,
            MixedDestination,
        },
        sources::postgres::{PostgresSource, TableNamesFrom},
        PipelineAction,
    },
    SslMode,
};
use postgres::tokio::config::PgConnectionConfig;
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error)]
pub enum ReplicatorError {
    #[error("The destination {0} is currently unsupported")]
    UnsupportedDestination(String),
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

    info!(
        publication_name = &replicator_config.pipeline.publication_name,
        destination_count = replicator_config.destination_count(),
        destination_names = ?replicator_config.get_named_destinations(),
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

    let destinations_with_configs = init_destinations_with_configs(&replicator_config).await?;

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

    for (name, destination_config) in config.get_named_destinations() {
        match destination_config {
            DestinationConfig::ClickHouse {
                url,
                database,
                username,
                password,
                batch,
            } => {
                info!(
                    destination_name = %name,
                    url,
                    database,
                    username,
                    max_size = batch.max_size,
                    max_fill_ms = batch.max_fill_ms,
                    "Initializing ClickHouse destination"
                );

                let dest = ClickHouseBatchDestination::new_with_credentials(
                    url, database, username, password,
                )
                .await?;

                let batch_config = BatchConfig::from_config(batch.max_size, batch.max_fill_ms);
                let mixed_dest = MixedDestination::clickhouse(dest);

                destinations_with_configs.push((mixed_dest, batch_config, name.clone()));
            }
            _ => {
                return Err(ReplicatorError::UnsupportedDestination(format!(
                    "Destination '{}': {:?}",
                    name, destination_config
                ))
                .into());
            }
        }
    }

    info!(
        destination_count = destinations_with_configs.len(),
        destination_names = ?destinations_with_configs.iter().map(|(_, _, name)| name).collect::<Vec<_>>(),
        "Successfully initialized all destinations with individual batch configs"
    );

    Ok(destinations_with_configs)
}

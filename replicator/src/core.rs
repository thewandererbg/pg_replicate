use std::{io::BufReader, time::Duration, vec};

use crate::config::load_replicator_config;
use config::shared::{DestinationConfig, ReplicatorConfig};

use etl::{
    pipeline::{
        batching::{data_pipeline::BatchDataPipeline, BatchConfig},
        destinations::clickhouse::ClickHouseBatchDestination,
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
    info!(
        host = &source.host,
        port = source.port,
        dbname = &source.name,
        username = &source.username,
        slot_name = &source.slot_name,
        "source settings"
    );

    if let DestinationConfig::ClickHouse {
        url,
        database,
        username,
        ..
    } = &replicator_config.destination
    {
        info!(url, database, username, "destination settings (ClickHouse)");
    } else {
        info!(
            destination = ?replicator_config.destination,
            "destination settings (unsupported or unknown type)"
        );
    }

    info!(
        max_size = &replicator_config.pipeline.batch.max_size,
        max_fill_ms = &replicator_config.pipeline.batch.max_fill_ms,
        publication_name = &replicator_config.pipeline.publication_name,
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

    let clickhouse_destination = init_destination(&replicator_config).await?;

    let batch_config = BatchConfig::new(
        replicator_config.pipeline.batch.max_size,
        Duration::from_millis(replicator_config.pipeline.batch.max_fill_ms),
    );
    let mut pipeline = BatchDataPipeline::new(
        postgres_source,
        clickhouse_destination,
        PipelineAction::Both,
        batch_config,
    );

    pipeline.start().await?;
    Ok(())
}

async fn init_destination(config: &ReplicatorConfig) -> anyhow::Result<ClickHouseBatchDestination> {
    match &config.destination {
        DestinationConfig::ClickHouse {
            url,
            database,
            username,
            password,
        } => Ok(ClickHouseBatchDestination::new_with_credentials(
            url.clone(),
            database.clone(),
            username.clone(),
            password.clone(),
        )
        .await?),
        _ => {
            Err(ReplicatorError::UnsupportedDestination(format!("{:?}", config.destination)).into())
        }
    }
}

use std::{error::Error, io::BufReader, time::Duration, vec};

use configuration::{get_configuration, BatchSettings, SinkSettings, SourceSettings, TlsSettings};
use pg_replicate::{
    pipeline::{
        batching::{data_pipeline::BatchDataPipeline, BatchConfig},
        sinks::clickhouse::ClickHouseBatchSink,
        sources::postgres::{PostgresSource, TableNamesFrom},
        PipelineAction,
    },
    SslMode,
};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod configuration;

// APP_SOURCE__POSTGRES__PASSWORD and APP_SINK__CLICK_HOUSE__PASSWORD environment variables must be set
// before running because these are sensitive values which can't be configured in the config files
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Err(e) = main_impl().await {
        error!("{e}");
    }

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "replicator=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn set_log_level() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
}

async fn main_impl() -> Result<(), Box<dyn Error>> {
    set_log_level();
    init_tracing();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let settings = get_configuration()?;

    info!("settings: {settings:#?}");

    settings.tls.validate()?;

    let SourceSettings::Postgres {
        host,
        port,
        name,
        username,
        password,
        slot_name,
        publication,
    } = settings.source;

    let TlsSettings {
        trusted_root_certs,
        enabled,
    } = settings.tls;

    let mut trusted_root_certs_vec = vec![];
    let ssl_mode = if enabled {
        let mut root_certs_reader = BufReader::new(trusted_root_certs.as_bytes());
        for cert in rustls_pemfile::certs(&mut root_certs_reader) {
            let cert = cert?;
            trusted_root_certs_vec.push(cert);
        }

        SslMode::VerifyFull
    } else {
        SslMode::Disable
    };

    let postgres_source = PostgresSource::new(
        &host,
        port,
        &name,
        &username,
        password,
        ssl_mode,
        trusted_root_certs_vec,
        Some(slot_name),
        TableNamesFrom::Publication(publication),
    )
    .await?;

    let SinkSettings::ClickHouse {
        url,
        database,
        username,
        password,
    } = settings.sink;

    let clickhouse_sink =
        ClickHouseBatchSink::new_with_credentials(url, database, username, password).await?;

    let BatchSettings {
        max_size,
        max_fill_secs,
    } = settings.batch;

    let batch_config = BatchConfig::new(max_size, Duration::from_secs(max_fill_secs));
    let mut pipeline = BatchDataPipeline::new(
        postgres_source,
        clickhouse_sink,
        PipelineAction::Both,
        batch_config,
    );

    pipeline.start().await?;

    Ok(())
}

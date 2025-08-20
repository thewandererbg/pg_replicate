use telemetry::init_tracing;

use crate::core::start_replicator;

mod config;
mod core;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app_name = env!("CARGO_BIN_NAME");
    let _log_flusher = init_tracing(app_name, false)?;

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    loop {
        match start_replicator().await {
            Ok(()) => break Ok(()),
            Err(e) if e.to_string().contains("schema change detected:") => {
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                continue;
            }
            Err(e) => break Err(e),
        }
    }
}

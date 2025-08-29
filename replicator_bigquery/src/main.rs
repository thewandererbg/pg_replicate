use crate::core::start_replicator;
use std::time::{Duration, Instant};
use telemetry::init_tracing;
use tracing::{error, warn};

mod config;
mod core;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app_name = env!("CARGO_BIN_NAME");
    let _log_flusher = init_tracing(app_name, false)?;

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    const INITIAL_BACKOFF: u64 = 10; // 10 seconds
    const MAX_BACKOFF_TIME: u64 = 900; // 15 minutes
    const RESET_THRESHOLD: u64 = 1800; // 30 minutes

    let mut backoff_secs = INITIAL_BACKOFF;
    let mut total_backoff_time = 0;

    loop {
        let started = Instant::now();
        let result = start_replicator().await;
        let ran_for = started.elapsed();

        match result {
            Ok(()) => break Ok(()),

            Err(e) if e.to_string().contains("schema change detected:") => {
                tokio::time::sleep(Duration::from_secs(10)).await;
                backoff_secs = INITIAL_BACKOFF;
                total_backoff_time = 0;
                continue;
            }

            Err(e) => {
                let msg = e.to_string().to_lowercase();
                let is_retryable = msg.contains("tls handshake eof")
                    || msg.contains("client error (connect)")
                    || msg.contains("connection reset")
                    || msg.contains("broken pipe")
                    || msg.contains("timeout")
                    || msg.contains("deadline exceeded")
                    || msg.contains("unavailable")
                    || msg.contains("goaway");

                if !is_retryable {
                    break Err(e);
                }
                // reset if it ran ok for ≥ 1hour
                if ran_for >= Duration::from_secs(RESET_THRESHOLD) {
                    backoff_secs = INITIAL_BACKOFF;
                    total_backoff_time = 0;
                }

                total_backoff_time += backoff_secs;
                if total_backoff_time >= MAX_BACKOFF_TIME {
                    error!(
                        "Connection failures persisted for {} seconds, giving up",
                        total_backoff_time
                    );
                    break Err(e);
                }

                warn!(
                    "Connection failure, retrying in {}s (total backoff: {}s)",
                    backoff_secs, total_backoff_time
                );
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(300);
                continue;
            }
        }
    }
}

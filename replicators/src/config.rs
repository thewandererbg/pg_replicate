use config::load_config;
use config::shared::ReplicatorConfig;
use tracing::info;

/// Loads the [`ReplicatorConfig`] and validates it.
pub fn load_replicator_config() -> anyhow::Result<ReplicatorConfig> {
    info!("Loading replicator config");
    let config = load_config::<ReplicatorConfig>()?;
    config.validate()?;

    Ok(config)
}

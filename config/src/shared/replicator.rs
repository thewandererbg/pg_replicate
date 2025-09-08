use crate::shared::pipeline::PipelineConfig;
use crate::shared::{
    BatchConfig, DestinationConfig, SourceConfig, StateStoreConfig, ValidationError,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Configuration for the replicator service.
///
/// This struct aggregates all configuration required to run the replicator, including source,
/// destinations, pipeline, state store, and optional Supabase-specific settings.
///
/// The [`ReplicatorConfig`] is typically deserialized from a configuration file and passed to the
/// replicator at startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ReplicatorConfig {
    /// Configuration for the source Postgres instance.
    pub source: SourceConfig,
    /// Configuration for the state store used to persist replication state.
    pub state_store: StateStoreConfig,
    /// Configuration for replication destinations.
    pub destinations: DestinationsConfig,
    /// Configuration for the replication pipeline.
    pub pipeline: PipelineConfig,
}

/// Configuration for a specific destination type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DestinationTypeConfig {
    #[serde(rename = "clickhouse")]
    ClickHouse(ClickHouseConfig),
    #[serde(rename = "bigquery")]
    BigQuery(BigQueryConfig),
    // Easy to add new destination types here in the future
    // #[serde(rename = "kafka")]
    // Kafka(KafkaConfig),
}

/// Configuration for multiple named destinations with environment variable support.
///
/// This uses a type alias for HashMap to enable proper serialization/deserialization
/// while maintaining compatibility with environment variable overrides.
pub type DestinationsConfig = HashMap<String, DestinationTypeConfig>;

/// ClickHouse destination configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseConfig {
    /// ClickHouse server URL
    pub url: String,
    /// ClickHouse database name
    pub database: String,
    /// ClickHouse username
    pub username: String,
    /// ClickHouse password
    pub password: String,
    /// Batch processing configuration.
    #[serde(default)]
    pub batch: BatchConfig,
}

impl ClickHouseConfig {
    /// Validates the ClickHouse configuration
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.url.is_empty() {
            return Err(ValidationError::InvalidDestination(
                "ClickHouse URL cannot be empty".to_string(),
            ));
        }

        // Basic URL format validation
        if !self.url.starts_with("http://") && !self.url.starts_with("https://") {
            return Err(ValidationError::InvalidDestination(
                "ClickHouse URL must start with http:// or https://".to_string(),
            ));
        }

        if self.database.is_empty() {
            return Err(ValidationError::InvalidDestination(
                "ClickHouse database cannot be empty".to_string(),
            ));
        }
        if self.username.is_empty() {
            return Err(ValidationError::InvalidDestination(
                "ClickHouse username cannot be empty".to_string(),
            ));
        }
        if self.password.is_empty() {
            return Err(ValidationError::InvalidDestination(
                "ClickHouse password cannot be empty".to_string(),
            ));
        }

        // Validate batch configuration
        self.batch.validate()?;

        Ok(())
    }
}

/// BigQuery destination configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BigQueryConfig {
    /// Google Cloud project identifier.
    pub project_id: String,
    /// BigQuery dataset identifier.
    pub dataset_id: String,
    /// Service account key for authenticating with BigQuery.
    pub gcp_sa_key_path: String,
    /// Maximum staleness in minutes for BigQuery CDC reads.
    pub max_staleness_mins: u16,
    /// Batch processing configuration.
    #[serde(default)]
    pub batch: BatchConfig,
}

impl BigQueryConfig {
    /// Validates the BigQuery configuration
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.project_id.is_empty() {
            return Err(ValidationError::InvalidDestination(
                "BigQuery project_id cannot be empty".to_string(),
            ));
        }
        if self.dataset_id.is_empty() {
            return Err(ValidationError::InvalidDestination(
                "BigQuery dataset_id cannot be empty".to_string(),
            ));
        }
        if self.gcp_sa_key_path.is_empty() {
            return Err(ValidationError::InvalidDestination(
                "BigQuery gcp_sa_key_path cannot be empty".to_string(),
            ));
        }

        // Validate that the service account key file exists
        if !Path::new(&self.gcp_sa_key_path).exists() {
            return Err(ValidationError::InvalidDestination(format!(
                "BigQuery service account key file does not exist: {}",
                self.gcp_sa_key_path
            )));
        }

        // Validate reasonable range for staleness
        if self.max_staleness_mins > 1440 {
            // 24 hours
            return Err(ValidationError::InvalidDestination(
                "BigQuery max_staleness_mins should not exceed 1440 minutes (24 hours)".to_string(),
            ));
        }

        // Validate batch configuration
        self.batch.validate()?;

        Ok(())
    }
}

impl DestinationTypeConfig {
    /// Validates the destination configuration based on its type
    pub fn validate(&self) -> Result<(), ValidationError> {
        match self {
            DestinationTypeConfig::ClickHouse(config) => config.validate(),
            DestinationTypeConfig::BigQuery(config) => config.validate(),
        }
    }

    /// Converts the destination type config to a DestinationConfig enum
    pub fn to_destination_config(&self) -> DestinationConfig {
        match self {
            DestinationTypeConfig::ClickHouse(ch) => DestinationConfig::ClickHouse {
                url: ch.url.clone(),
                database: ch.database.clone(),
                username: ch.username.clone(),
                password: ch.password.clone(),
                batch: ch.batch.clone(),
            },
            DestinationTypeConfig::BigQuery(bq) => DestinationConfig::BigQuery {
                project_id: bq.project_id.clone(),
                dataset_id: bq.dataset_id.clone(),
                gcp_sa_key_path: bq.gcp_sa_key_path.clone(),
                max_staleness_mins: bq.max_staleness_mins,
                batch: bq.batch.clone(),
            },
        }
    }
}

/// Extension trait to add helper methods to DestinationsConfig (HashMap).
///
/// This trait provides all the helper methods that were previously on the struct,
/// allowing for clean method chaining and better ergonomics.
pub trait DestinationsConfigExt {
    /// Get all configured destinations as a vector of DestinationConfig enums.
    /// This is the primary method for accessing destinations.
    fn to_destination_configs(&self) -> Vec<DestinationConfig>;

    /// Get destination configurations with their names as a vector of tuples.
    fn get_named_destinations(&self) -> Vec<(String, DestinationConfig)>;

    /// Get all destination names as a vector.
    fn get_destination_names(&self) -> Vec<&String>;

    /// Check if any ClickHouse destinations are configured.
    fn has_clickhouse(&self) -> bool;

    /// Check if any BigQuery destinations are configured.
    fn has_bigquery(&self) -> bool;

    /// Get all ClickHouse destinations with their names.
    fn get_clickhouse_destinations(&self) -> Vec<(&String, &ClickHouseConfig)>;

    /// Get all BigQuery destinations with their names.
    fn get_bigquery_destinations(&self) -> Vec<(&String, &BigQueryConfig)>;

    /// Validates all configured destinations.
    fn validate(&self) -> Result<(), ValidationError>;
}

impl DestinationsConfigExt for DestinationsConfig {
    fn to_destination_configs(&self) -> Vec<DestinationConfig> {
        self.values()
            .map(|dest| dest.to_destination_config())
            .collect()
    }

    fn get_named_destinations(&self) -> Vec<(String, DestinationConfig)> {
        self.iter()
            .map(|(name, dest)| (name.clone(), dest.to_destination_config()))
            .collect()
    }

    fn get_destination_names(&self) -> Vec<&String> {
        self.keys().collect()
    }

    fn has_clickhouse(&self) -> bool {
        self.values()
            .any(|dest| matches!(dest, DestinationTypeConfig::ClickHouse(_)))
    }

    fn has_bigquery(&self) -> bool {
        self.values()
            .any(|dest| matches!(dest, DestinationTypeConfig::BigQuery(_)))
    }

    fn get_clickhouse_destinations(&self) -> Vec<(&String, &ClickHouseConfig)> {
        self.iter()
            .filter_map(|(name, dest)| match dest {
                DestinationTypeConfig::ClickHouse(config) => Some((name, config)),
                _ => None,
            })
            .collect()
    }

    fn get_bigquery_destinations(&self) -> Vec<(&String, &BigQueryConfig)> {
        self.iter()
            .filter_map(|(name, dest)| match dest {
                DestinationTypeConfig::BigQuery(config) => Some((name, config)),
                _ => None,
            })
            .collect()
    }

    fn validate(&self) -> Result<(), ValidationError> {
        if self.is_empty() {
            return Err(ValidationError::NoDestinations);
        }

        // Validate each destination
        for (name, dest) in self {
            dest.validate().map_err(|_| {
                // Don't double-wrap the error, just add context about which destination failed
                ValidationError::InvalidDestination(format!(
                    "Destination '{}' failed validation",
                    name
                ))
            })?;
        }

        Ok(())
    }
}

impl ReplicatorConfig {
    /// Get all configured destinations.
    /// This is the primary method for accessing destinations from ReplicatorConfig.
    pub fn get_destinations(&self) -> Vec<DestinationConfig> {
        self.destinations.to_destination_configs()
    }

    /// Get destination configurations with their names.
    pub fn get_named_destinations(&self) -> Vec<(String, DestinationConfig)> {
        self.destinations.get_named_destinations()
    }

    /// Get the number of configured destinations.
    pub fn destination_count(&self) -> usize {
        self.destinations.len()
    }

    /// Check if multiple destinations are configured.
    pub fn has_multiple_destinations(&self) -> bool {
        self.destinations.len() > 1
    }

    /// Check if there are no destinations configured.
    pub fn is_empty(&self) -> bool {
        self.destinations.is_empty()
    }

    /// Get a specific destination by name.
    pub fn get_destination(&self, name: &str) -> Option<&DestinationTypeConfig> {
        self.destinations.get(name)
    }

    /// Validates the loaded [`ReplicatorConfig`].
    ///
    /// Checks the validity of the TLS configuration and validates all destination configurations.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] if validation fails.
    pub fn validate(&self) -> Result<(), ValidationError> {
        // Validate TLS configuration
        self.source.tls.validate()?;

        // Validate destinations configuration
        self.destinations.validate()?;

        Ok(())
    }
}

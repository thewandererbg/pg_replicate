use crate::shared::pipeline::PipelineConfig;
use crate::shared::{
    BatchConfig, DestinationConfig, SourceConfig, StateStoreConfig, ValidationError,
};
use serde::{Deserialize, Serialize};
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

/// Configuration for multiple destinations with environment variable support.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationsConfig {
    /// ClickHouse destination configuration (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clickhouse: Option<ClickHouseConfig>,
    /// BigQuery destination configuration (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bigquery: Option<BigQueryConfig>,
}

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

impl DestinationsConfig {
    /// Get all configured destinations as a vector of DestinationConfig enums.
    /// This is the primary method for accessing destinations.
    pub fn to_destination_configs(&self) -> Vec<DestinationConfig> {
        let mut destinations = Vec::new();

        if let Some(ch) = &self.clickhouse {
            destinations.push(DestinationConfig::ClickHouse {
                url: ch.url.clone(),
                database: ch.database.clone(),
                username: ch.username.clone(),
                password: ch.password.clone(),
                batch: ch.batch.clone(),
            });
        }

        if let Some(bq) = &self.bigquery {
            destinations.push(DestinationConfig::BigQuery {
                project_id: bq.project_id.clone(),
                dataset_id: bq.dataset_id.clone(),
                gcp_sa_key_path: bq.gcp_sa_key_path.clone(),
                max_staleness_mins: bq.max_staleness_mins,
                batch: bq.batch.clone(),
            });
        }

        destinations
    }

    pub fn has_clickhouse(&self) -> bool {
        self.clickhouse
            .as_ref()
            .map_or(false, |ch| !ch.url.is_empty())
    }

    /// Check if BigQuery destination is configured
    pub fn has_bigquery(&self) -> bool {
        self.bigquery
            .as_ref()
            .map_or(false, |bq| !bq.project_id.is_empty())
    }

    /// Get the number of configured destinations
    pub fn len(&self) -> usize {
        let mut count = 0;
        if self.clickhouse.is_some() {
            count += 1;
        }
        if self.bigquery.is_some() {
            count += 1;
        }
        count
    }

    /// Check if there are no destinations configured
    pub fn is_empty(&self) -> bool {
        self.clickhouse.is_none() && self.bigquery.is_none()
    }

    /// Validates all configured destinations
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.is_empty() {
            return Err(ValidationError::NoDestinations);
        }

        if !self.has_bigquery() {
            return Err(ValidationError::NoBigQueryConfig);
        }

        if !self.has_clickhouse() {
            return Err(ValidationError::NoClickHouseConfig);
        }

        // Validate ClickHouse configuration if present
        if let Some(ch) = &self.clickhouse {
            ch.validate().map_err(|e| {
                ValidationError::InvalidDestination(format!("ClickHouse validation failed: {}", e))
            })?;
        }

        // Validate BigQuery configuration if present
        if let Some(bq) = &self.bigquery {
            bq.validate().map_err(|e| {
                ValidationError::InvalidDestination(format!("BigQuery validation failed: {}", e))
            })?;
        }

        Ok(())
    }
}

impl ReplicatorConfig {
    /// Get all configured destinations (exactly 2 required).
    /// This is the primary method for accessing destinations from ReplicatorConfig.
    pub fn get_destinations(&self) -> Vec<DestinationConfig> {
        self.destinations.to_destination_configs()
    }

    /// Get the number of configured destinations
    pub fn destination_count(&self) -> usize {
        self.destinations.len()
    }

    /// Check if multiple destinations are configured
    pub fn has_multiple_destinations(&self) -> bool {
        self.destinations.len() > 1
    }

    /// Check if there are no destinations configured
    pub fn is_empty(&self) -> bool {
        self.destinations.is_empty()
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

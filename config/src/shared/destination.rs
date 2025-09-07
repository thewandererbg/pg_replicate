use serde::{Deserialize, Serialize};
use std::fmt;

use crate::shared::ValidationError;

/// Configuration options for supported data destinations.
///
/// This enum is used to specify the destination type and its configuration
/// for the replicator. Variants correspond to different supported destinations.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationConfig {
    /// In-memory destination for ephemeral or test data.
    Memory,
    /// Google BigQuery destination configuration.
    ///
    /// Use this variant to configure a BigQuery destination, including
    /// project and dataset identifiers, service account credentials, and
    /// optional staleness settings.
    BigQuery {
        /// Google Cloud project identifier.
        project_id: String,
        /// BigQuery dataset identifier.
        dataset_id: String,
        /// Service account key for authenticating with BigQuery.
        gcp_sa_key_path: String,
        /// Maximum staleness in minutes for BigQuery CDC reads.
        max_staleness_mins: u16,
        /// Batch processing configuration.
        batch: BatchConfig,
    },
    /// Clickhouse destination configuration.
    ClickHouse {
        /// ClickHouse server URL
        url: String,
        /// ClickHouse database name
        database: String,
        /// ClickHouse username
        username: String,
        /// ClickHouse password
        password: String,
        /// Batch processing configuration.
        batch: BatchConfig,
    },
}

impl fmt::Debug for DestinationConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory => f.write_str("Memory"),
            Self::BigQuery {
                project_id,
                dataset_id,
                gcp_sa_key_path: _,
                max_staleness_mins,
                batch,
            } => f
                .debug_struct("BigQuery")
                .field("project_id", project_id)
                .field("dataset_id", dataset_id)
                .field("gcp_sa_key_path", &"REDACTED")
                .field("max_staleness_mins", max_staleness_mins)
                .field("batch", batch)
                .finish(),
            Self::ClickHouse {
                url,
                database,
                username,
                password: _,
                batch,
            } => f
                .debug_struct("ClickHouse")
                .field("url", url)
                .field("database", database)
                .field("username", username)
                .field("password", &"REDACTED")
                .field("batch", batch)
                .finish(),
        }
    }
}

impl Default for DestinationConfig {
    fn default() -> Self {
        Self::Memory
    }
}

/// Batch processing configuration for pipelines.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BatchConfig {
    /// Maximum number of items in a batch for table copy and event streaming.
    pub max_size: usize,
    /// Maximum time, in milliseconds, to wait for a batch to fill before processing.
    pub max_fill_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: 1000,
            max_fill_ms: 1000,
        }
    }
}

impl BatchConfig {
    /// Validates the batch configuration
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.max_size == 0 {
            return Err(ValidationError::InvalidDestination(
                "Batch max_size must be greater than 0".to_string(),
            ));
        }
        if self.max_fill_ms == 0 {
            return Err(ValidationError::InvalidDestination(
                "Batch max_fill_ms must be greater than 0".to_string(),
            ));
        }
        Ok(())
    }
}

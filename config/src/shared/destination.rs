use serde::{Deserialize, Serialize};
use std::fmt;

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
            } => f
                .debug_struct("BigQuery")
                .field("project_id", project_id)
                .field("dataset_id", dataset_id)
                .field("gcp_sa_key_path", &"REDACTED")
                .field("max_staleness_mins", max_staleness_mins)
                .finish(),
            Self::ClickHouse {
                url,
                database,
                username,
                password: _,
            } => f
                .debug_struct("ClickHouse")
                .field("url", url)
                .field("database", database)
                .field("username", username)
                .field("password", &"REDACTED")
                .finish(),
        }
    }
}

impl Default for DestinationConfig {
    fn default() -> Self {
        Self::Memory
    }
}

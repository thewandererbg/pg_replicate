use crate::shared::pipeline::PipelineConfig;
use crate::shared::{DestinationConfig, SourceConfig, StateStoreConfig, ValidationError};
use serde::{Deserialize, Serialize};

/// Configuration for the replicator service.
///
/// This struct aggregates all configuration required to run the replicator, including source,
/// destinations (one or more), pipeline, state store, and optional Supabase-specific settings.
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
    ///
    /// Can be either a single destination (for backward compatibility) or multiple destinations.
    #[serde(flatten)]
    pub destinations: DestinationConfigs,
    /// Configuration for the replication pipeline.
    pub pipeline: PipelineConfig,
}

/// Represents either single or multiple destination configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DestinationConfigs {
    /// Single destination (backward compatibility)
    Single { destination: DestinationConfig },
    /// Multiple destinations
    Multiple {
        destinations: Vec<DestinationConfig>,
    },
}

impl DestinationConfigs {
    /// Get all destinations as a vector
    pub fn as_vec(&self) -> Vec<&DestinationConfig> {
        match self {
            DestinationConfigs::Single { destination } => vec![destination],
            DestinationConfigs::Multiple { destinations } => destinations.iter().collect(),
        }
    }

    /// Get all destinations as owned vector
    pub fn into_vec(self) -> Vec<DestinationConfig> {
        match self {
            DestinationConfigs::Single { destination } => vec![destination],
            DestinationConfigs::Multiple { destinations } => destinations,
        }
    }

    /// Get the number of destinations
    pub fn len(&self) -> usize {
        match self {
            DestinationConfigs::Single { .. } => 1,
            DestinationConfigs::Multiple { destinations } => destinations.len(),
        }
    }

    /// Check if there are no destinations
    pub fn is_empty(&self) -> bool {
        match self {
            DestinationConfigs::Single { .. } => false,
            DestinationConfigs::Multiple { destinations } => destinations.is_empty(),
        }
    }
}

impl ReplicatorConfig {
    /// Validates the loaded [`ReplicatorConfig`].
    ///
    /// Checks the validity of the TLS configuration and ensures at least one destination is configured.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationError`] if validation fails.
    pub fn validate(&self) -> Result<(), ValidationError> {
        // Validate TLS configuration
        self.source.tls.validate()?;

        // Ensure at least one destination is configured
        if self.destinations.is_empty() {
            return Err(ValidationError::NoDestinations);
        }

        // // Validate each destination
        // for destination in self.destinations.as_vec() {
        //     // Add destination-specific validation here if needed
        //     // For example: destination.validate()?;
        // }

        Ok(())
    }

    /// Get the number of configured destinations
    pub fn destination_count(&self) -> usize {
        self.destinations.len()
    }

    /// Check if multiple destinations are configured
    pub fn has_multiple_destinations(&self) -> bool {
        self.destinations.len() > 1
    }
}

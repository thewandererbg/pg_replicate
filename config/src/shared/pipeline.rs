use serde::{Deserialize, Serialize};

/// Configuration for a pipeline's batching and worker retry behavior.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PipelineConfig {
    /// The unique identifier for this pipeline.
    ///
    /// A pipeline id determines isolation between pipelines, in terms of replication slots and state
    /// store.
    pub id: u64,
    /// Name of the Postgres publication to use for logical replication.
    pub publication_name: String,
}

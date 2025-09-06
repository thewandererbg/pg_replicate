use thiserror::Error;

/// Errors that can occur during configuration validation.
#[derive(Debug, Error)]
pub enum ValidationError {
    /// TLS is enabled but no trusted root certificates are provided.
    #[error("Invalid TLS config: `trusted_root_certs` must be set when `enabled` is true")]
    MissingTrustedRootCerts,

    /// General configuration validation error.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// No destinations configured.
    #[error("At least one destination must be configured")]
    NoDestinations,

    /// Duplicate destination names or conflicting configurations.
    #[error("Duplicate or conflicting destination configuration: {0}")]
    DuplicateDestination(String),

    /// Invalid destination configuration.
    #[error("Invalid destination configuration: {0}")]
    InvalidDestination(String),
}

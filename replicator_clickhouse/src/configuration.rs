use std::fmt::Debug;

use thiserror::Error;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceSettings {
    Postgres {
        /// Host on which Postgres is running
        host: String,

        /// Port on which Postgres is running
        port: u16,

        /// Postgres database name
        name: String,

        /// Postgres database user name
        username: String,

        /// Postgres database user password
        password: Option<String>,

        /// Postgres slot name
        slot_name: String,

        /// Postgres publication name
        publication: String,
    },
}

impl Debug for SourceSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres {
                host,
                port,
                name,
                username,
                password: _,
                slot_name,
                publication,
            } => f
                .debug_struct("Postgres")
                .field("host", host)
                .field("port", port)
                .field("name", name)
                .field("username", username)
                .field("password", &"REDACTED")
                .field("slot_name", slot_name)
                .field("publication", publication)
                .finish(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DestinationSettings {
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

impl Debug for DestinationSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BatchSettings {
    /// maximum batch size in number of events
    pub max_size: usize,

    /// maximum duration, in seconds, to wait for a batch to fill
    pub max_fill_secs: u64,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TlsSettings {
    /// trusted root certificates in PEM format
    pub trusted_root_certs: String,

    /// true when TLS is enabled
    pub enabled: bool,
}

impl Debug for TlsSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsSettings")
            .field("trusted_root_certs", &"OMITTED")
            .field("enabled", &self.enabled)
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum TlsSettingsError {
    #[error("Invalid TLS settings: `trusted_root_certs` must be set when `enabled` is true")]
    MissingTrustedRootCerts,
}

impl TlsSettings {
    pub fn validate(&self) -> Result<(), TlsSettingsError> {
        if self.enabled && self.trusted_root_certs.is_empty() {
            return Err(TlsSettingsError::MissingTrustedRootCerts);
        }
        Ok(())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Settings {
    pub source: SourceSettings,
    pub destination: DestinationSettings,
    pub batch: BatchSettings,
    pub tls: TlsSettings,
    pub project: String,
}

pub fn get_configuration() -> Result<Settings, config::ConfigError> {
    let base_path = std::env::current_dir().expect("Failed to determine the current directory");
    let configuration_directory = base_path.join("configuration");

    // Detect the running environment.
    // Default to `dev` if unspecified.
    let environment: Environment = std::env::var("APP_ENVIRONMENT")
        .unwrap_or_else(|_| DEV_ENV_NAME.into())
        .try_into()
        .expect("Failed to parse APP_ENVIRONMENT.");

    let environment_filename = format!("{}.yaml", environment.as_str());
    let settings = config::Config::builder()
        .add_source(config::File::from(
            configuration_directory.join("base.yaml"),
        ))
        .add_source(config::File::from(
            configuration_directory.join(environment_filename),
        ))
        // Add in settings from environment variables (with a prefix of APP and '__' as separator)
        // E.g. `APP_SINK__CLICK_HOUSE__URL=http://localhost:8123 would set `Settings { destination: ClickHouse { url }}` to http://localhost:8123
        .add_source(
            config::Environment::with_prefix("APP")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;

    settings.try_deserialize::<Settings>()
}

const DEV_ENV_NAME: &str = "dev";
const PROD_ENV_NAME: &str = "prod";

/// The possible runtime environment for our application.
pub enum Environment {
    Dev,
    Prod,
}

impl Environment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Dev => DEV_ENV_NAME,
            Environment::Prod => PROD_ENV_NAME,
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "dev" => Ok(Self::Dev),
            "prod" => Ok(Self::Prod),
            other => Err(format!(
                "{other} is not a supported environment. Use either `{DEV_ENV_NAME}` or `{PROD_ENV_NAME}`.",
            )),
        }
    }
}

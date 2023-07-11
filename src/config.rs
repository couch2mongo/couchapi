use config::{Config, ConfigError, Environment};
use serde_derive::Deserialize;
use std::error::Error;

fn default_log_level() -> LogLevel {
    LogLevel::Info
}

fn default_log_format() -> LogFormat {
    LogFormat::Compact
}

#[derive(Debug, Deserialize)]
pub enum LogFormat {
    Compact,
    Json,
}

#[derive(Debug, Deserialize)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    #[serde(default)]
    pub debug: bool,

    pub mongodb_connect_string: String,
    pub mongodb_database: String,

    #[serde(default = "default_log_format")]
    pub log_format: LogFormat,

    #[serde(default = "default_log_level")]
    pub log_level: LogLevel,
}

impl Settings {
    pub fn new(config_file: Option<String>) -> Result<Self, ConfigError> {
        let mut config_builder =
            Config::builder().add_source(Environment::with_prefix("couch_stream"));

        match config_file {
            None => {}
            Some(file) => {
                config_builder = config_builder.add_source(config::File::with_name(&file));
            }
        }

        config_builder.build()?.try_deserialize()
    }

    pub fn configure_logging(&self) {
        let x = tracing_subscriber::fmt();

        let y = match self.log_level {
            LogLevel::Debug => x.with_max_level(tracing::Level::DEBUG),
            LogLevel::Info => x.with_max_level(tracing::Level::INFO),
            LogLevel::Warn => x.with_max_level(tracing::Level::WARN),
            LogLevel::Error => x.with_max_level(tracing::Level::ERROR),
        };

        match self.log_format {
            LogFormat::Compact => {
                y.compact().init();
            }
            LogFormat::Json => {
                y.json().init();
            }
        };
    }

    pub async fn get_mongodb_client(&self) -> Result<mongodb::Client, Box<dyn Error>> {
        let client = mongodb::Client::with_uri_str(self.mongodb_connect_string.as_str()).await?;

        Ok(client)
    }

    pub async fn get_mongodb_database(&self) -> Result<mongodb::Database, Box<dyn Error>> {
        let client = self.get_mongodb_client().await?;
        let db = client.database(self.mongodb_database.as_str());

        Ok(db)
    }
}

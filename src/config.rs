use config::{Config, ConfigError, Environment};
use maplit::hashmap;
use serde_derive::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use tracing::{error, info};
use tracing_subscriber::fmt::format::FmtSpan;
use walkdir::WalkDir;

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

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct DesignView {
    pub match_fields: Vec<String>,
    pub aggregation: Vec<String>,
    pub key_fields: Vec<String>,
    pub value_fields: Vec<String>,
    pub filter_insert_index: usize,
}

#[derive(Debug, Deserialize)]
pub struct DesignMapping {
    // Keyed by ViewGroup then by View
    pub view_groups: HashMap<String, HashMap<String, DesignView>>,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    #[serde(default)]
    pub debug: bool,

    pub mongodb_connect_string: String,
    pub mongodb_database: String,

    pub views: Option<HashMap<String, DesignMapping>>,
    pub view_folder: Option<String>,
    pub updates_folder: Option<String>,

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

    pub fn maybe_add_views_from_files(&mut self) {
        if self.views.is_some() {
            info!("views already configured");

            return;
        }

        if self.view_folder.is_none() {
            error!("no view folder configured");

            return;
        }

        let walker = WalkDir::new("./views").into_iter();
        let mut view_groups: HashMap<String, DesignMapping> = HashMap::new();

        for entry in walker {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };

            let path = entry.path();

            let file_name = match path.file_name() {
                Some(file_name) => file_name,
                None => continue,
            };

            let file_name_str = match file_name.to_str() {
                Some(s) => s,
                None => continue,
            };

            if !file_name_str.ends_with(".toml") {
                continue;
            }

            let view_group_name = path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|os_str| os_str.to_str())
                .map(|s| s.to_string())
                .unwrap_or_default();

            let db_name = path
                .parent()
                .and_then(|p| p.parent())
                .and_then(|p| p.file_name())
                .and_then(|os_str| os_str.to_str())
                .map(|s| s.to_string())
                .unwrap_or_default();

            let view_name = file_name_str.replace(".toml", "");

            let contents = match fs::read_to_string(path) {
                Ok(c) => c,
                Err(_) => {
                    println!("could not read file");
                    continue;
                }
            };

            let design_view: DesignView = match toml::from_str(&contents) {
                Ok(design_view) => design_view,
                Err(_) => {
                    println!("could not parse file");
                    continue;
                }
            };

            info!(
                db_name = db_name.as_str(),
                view_group_name = view_group_name.as_str(),
                view_name = view_name.as_str(),
                "adding view"
            );

            view_groups.insert(
                db_name,
                DesignMapping {
                    view_groups: hashmap! {
                        view_group_name => hashmap! {
                            view_name => design_view
                        }
                    },
                },
            );
        }

        self.views = Some(view_groups);
    }

    pub fn configure_logging(&self) {
        let mut x = tracing_subscriber::fmt();

        if self.debug {
            x = x.with_span_events(FmtSpan::CLOSE);
        }

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

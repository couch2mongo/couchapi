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

fn default_listen_address() -> String {
    "0.0.0.0:3000".to_string()
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
pub struct ReduceView {
    pub aggregation: Vec<String>,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct DesignView {
    pub match_fields: Vec<String>,
    pub sort_fields: Option<Vec<String>>,
    pub aggregation: Vec<String>,
    pub key_fields: Vec<String>,
    pub value_fields: Vec<String>,
    pub filter_insert_index: usize,
    pub reduce: Option<HashMap<String, ReduceView>>,

    #[serde(default)]
    pub single_item_key_is_list: bool,

    #[serde(default)]
    pub single_item_value_is_dict: bool,

    pub break_glass_js_script: Option<String>,

    #[serde(default)]
    pub omit_null_keys_in_value: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DesignMapping {
    // Keyed by ViewGroup then by View
    pub view_groups: HashMap<String, HashMap<String, DesignView>>,
}

#[derive(Debug, Deserialize)]
pub struct CouchDb {
    /// url defines the URL of the CouchDB server.
    pub url: String,

    /// username defines the username to use when connecting to the CouchDB server.
    pub username: Option<String>,

    /// password defines the password to use when connecting to the CouchDB server.
    pub password: Option<String>,

    /// When set to try, we will attempt to read a view from CouchDB if the view is not found in
    /// the configuration file. This is useful for migrating from CouchDB to MongoDB.
    #[serde(default)]
    pub read_through: bool,

    /// When set to true, we will not write to MongoDB but instead only to CouchDB
    #[serde(default)]
    pub read_only: bool,

    /// A list of databases that we will read views from CouchDB instead of MongoDB
    /// if the view doesn't exist.
    pub read_through_databases: Option<Vec<String>>,

    /// A list of databases that we will only read from MongoDB and write to CouchDB
    pub read_only_databases: Option<Vec<String>>,

    /// mappings defines which CouchDB database to use on read and write. The key is the MongoDB
    /// Collection name and the value is the CouchDB database name.
    pub mappings: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    #[serde(default)]
    pub debug: bool,

    #[serde(default = "default_listen_address")]
    pub listen_address: String,

    pub mongodb_connect_string: String,
    pub mongodb_database: String,

    pub views: Option<HashMap<String, DesignMapping>>,
    pub view_folder: Option<String>,
    pub updates_folder: Option<String>,

    pub couchdb_settings: Option<CouchDb>,

    #[serde(default = "default_log_format")]
    pub log_format: LogFormat,

    #[serde(default = "default_log_level")]
    pub log_level: LogLevel,
}

impl Settings {
    /// This method creates a new `Settings` struct by reading configuration data from the
    /// environment and/or a configuration file. If a configuration file is provided, it is read
    /// and added as a source of configuration data. The method then attempts to deserialize the
    /// configuration data into a `Settings` struct. If successful, the `Settings` struct is
    /// returned. If an error occurs during the deserialization process, a `ConfigError` is
    /// returned.
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

    /// This method checks if views are already configured and if a view folder is configured. If
    /// views are already configured or a view folder is not configured, it returns. Otherwise, it
    /// reads all the files in the view folder with the extension ".toml" and parses them into
    /// `DesignView` structs. It then inserts these views into a `HashMap` of `DesignMapping`
    /// structs, which is then inserted into the `views` field of the `Settings` struct.
    pub fn maybe_add_views_from_files(&mut self) {
        // Check if views are already configured
        if self.views.is_some() {
            info!("views already configured");
            return;
        }

        // Check if a view folder is configured
        if self.view_folder.is_none() {
            error!("no view folder configured");
            return;
        }

        // Iterate over all files in the view folder with the extension ".toml"
        let walker = WalkDir::new(self.view_folder.as_ref().unwrap()).into_iter();
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

            // Extract the view group name, database name, and view name from the file path
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

            // Read the contents of the file and parse it into a `DesignView` struct
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

            // Insert the view into the `view_groups` HashMap
            info!(
                db_name = db_name.as_str(),
                view_group_name = view_group_name.as_str(),
                view_name = view_name.as_str(),
                "adding view"
            );

            // Create an empty view group IF we need one
            let design_mapping = view_groups.entry(db_name.clone()).or_insert(DesignMapping {
                view_groups: hashmap! {},
            });

            let db_mapping = design_mapping
                .view_groups
                .entry(view_group_name.clone())
                .or_insert(hashmap! {});
            db_mapping.insert(view_name.clone(), design_view);
        }

        // Insert the `view_groups` HashMap into the `views` field of the `Settings` struct
        self.views = Some(view_groups);
    }

    /// Configures the logging system based on the values of the `debug`, `log_level`, and
    /// `log_format` fields of the `CouchDb` struct.
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

    /// Asynchronously returns a `mongodb::Client` instance for the MongoDB database specified in
    /// the `mongodb_connect_string` field of the `CouchDb` struct.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `mongodb::Client` instance if the operation is successful,
    /// or a `Box<dyn Error>` if an error occurs.
    pub async fn get_mongodb_client(&self) -> Result<mongodb::Client, Box<dyn Error>> {
        let client = mongodb::Client::with_uri_str(self.mongodb_connect_string.as_str()).await?;

        Ok(client)
    }

    /// Asynchronously returns a `mongodb::Database` instance for the MongoDB database specified in
    /// the `mongodb_database` field of the `CouchDb` struct.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `mongodb::Database` instance if the operation is successful,
    /// or a `Box<dyn Error>` if an error occurs.
    pub async fn get_mongodb_database(&self) -> Result<mongodb::Database, Box<dyn Error>> {
        let client = self.get_mongodb_client().await?;
        let db = client.database(self.mongodb_database.as_str());

        Ok(db)
    }
}

impl CouchDb {
    /// Returns the mapped value for the given database name, if it exists in the `mappings`
    /// HashMap. If the database name is not found in the `mappings` HashMap, the original
    /// database name is returned.
    ///
    /// # Arguments
    ///
    /// * `db` - A string slice that holds the name of the database to be mapped.
    pub fn map_for_db(&self, db: &str) -> String {
        self.mappings
            .as_ref()
            .and_then(|m| m.get(db))
            .map(|s| s.to_string())
            .unwrap_or_else(|| db.to_string())
    }

    /// Returns `true` either if read_through is `true` or if the given database name is found
    /// in the `read_only_databases` vector. Otherwise, returns `false`.
    pub fn should_read_through(&self, db: &str) -> bool {
        self.read_through
            || self
                .read_through_databases
                .as_ref()
                .unwrap_or(&vec![])
                .contains(&db.to_string())
    }

    /// Returns `true` either if read_only is `true` or if the given database name is found
    /// in the `read_only_databases` vector. Otherwise, returns `false`.
    pub fn is_read_only(&self, db: &str) -> bool {
        self.read_only
            || self
                .read_only_databases
                .as_ref()
                .unwrap_or(&vec![])
                .contains(&db.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::CouchDb;
    use std::collections::HashMap;

    #[test]
    fn test_no_mappings() {
        let couch = CouchDb {
            url: "".to_string(),
            username: None,
            password: None,
            read_through: false,
            read_only: false,
            mappings: None,
            read_through_databases: None,
            read_only_databases: None,
        };
        assert_eq!(couch.map_for_db("test_db"), "test_db".to_string());
    }

    #[test]
    fn test_with_mappings_no_match() {
        let mut map = HashMap::new();
        map.insert("other_db".to_string(), "mapped_value".to_string());

        let couch = CouchDb {
            url: "".to_string(),
            username: None,
            password: None,
            read_through: false,
            read_only: false,
            mappings: Some(map),
            read_through_databases: None,
            read_only_databases: None,
        };
        assert_eq!(couch.map_for_db("test_db"), "test_db".to_string());
    }

    #[test]
    fn test_with_mappings_with_match() {
        let mut map = HashMap::new();
        map.insert("test_db".to_string(), "mapped_value".to_string());

        let couch = CouchDb {
            url: "".to_string(),
            username: None,
            password: None,
            read_through: false,
            read_only: false,
            mappings: Some(map),
            read_through_databases: None,
            read_only_databases: None,
        };
        assert_eq!(couch.map_for_db("test_db"), "mapped_value".to_string());
    }

    #[test]
    fn test_should_read_through() {
        let db = CouchDb {
            url: "https://example.com".to_string(),
            username: None,
            password: None,
            read_through: false,
            read_only: false,
            read_through_databases: None,
            read_only_databases: None,
            mappings: None,
        };

        // 1. Default behavior
        assert!(!db.should_read_through("test_db"));

        // 2. Set read_through to true
        let db = CouchDb {
            read_through: true,
            ..db
        };
        assert!(db.should_read_through("test_db"));

        // 3. Database in read_through_databases
        let db = CouchDb {
            read_through: false,
            read_through_databases: Some(vec!["test_db".to_string()]),
            ..db
        };
        assert!(db.should_read_through("test_db"));

        // 4. Database NOT in read_through_databases
        assert!(!db.should_read_through("other_db"));
    }

    #[test]
    fn test_is_read_only() {
        let db = CouchDb {
            url: "https://example.com".to_string(),
            username: None,
            password: None,
            read_through: false,
            read_only: false,
            read_through_databases: None,
            read_only_databases: None,
            mappings: None,
        };

        // 1. Default behavior
        assert!(!db.is_read_only("test_db"));

        // 2. Set read_only to true
        let db = CouchDb {
            read_only: true,
            ..db
        };
        assert!(db.is_read_only("test_db"));

        // 3. Database in read_only_databases
        let db = CouchDb {
            read_only: false,
            read_only_databases: Some(vec!["test_db".to_string()]),
            ..db
        };
        assert!(db.is_read_only("test_db"));

        // 4. Database NOT in read_only_databases
        assert!(!db.is_read_only("other_db"));
    }
}

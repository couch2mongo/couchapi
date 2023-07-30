use crate::config::DesignMapping;
use crate::db::Database;
use std::collections::HashMap;

pub struct AppState {
    pub db: Box<dyn Database + Send + Sync>,
    pub views: Option<HashMap<String, DesignMapping>>,
}

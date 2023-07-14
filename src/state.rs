use crate::db::Database;

pub struct AppState {
    pub db: Box<dyn Database + Send + Sync>,
}

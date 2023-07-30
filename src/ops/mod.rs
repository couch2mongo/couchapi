pub mod create_update;
pub mod delete;
pub mod get;

use crate::state::AppState;
use axum::http::StatusCode;
use axum::Json;
use serde_json::{json, Value};
use std::error::Error;
use std::sync::Arc;

/// check_conflict checks to see if the document exists and if it does, returns a 409
/// conflict error.
pub async fn check_conflict(
    state: Arc<AppState>,
    collection: String,
    id: &str,
) -> Result<(StatusCode, Json<Value>), Box<dyn Error>> {
    // Grab the document to determine if it exists or not
    let document = match state.db.find_one(collection, id.to_string()).await {
        Ok(document) => document,
        Err(e) => {
            return Err(Box::new(e));
        }
    };

    // This would be weird - but we should say
    if document.is_none() {
        return Ok((StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))));
    }

    // Looks like a standard conflict
    Ok((StatusCode::CONFLICT, Json(json!({"error": "conflict"}))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::MockDatabase;
    use assert_json_diff::assert_json_eq;
    use mongodb::error::Error as MongoError;
    use std::sync::Arc;

    #[tokio::test]
    async fn check_conflict_throws_error_on_find_one_error() {
        let mut mock = MockDatabase::new();

        mock.expect_find_one()
            .returning(|_, _| Box::pin(async { Err(MongoError::custom("nothing")) }));

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
        });

        let result = check_conflict(state.clone(), "test_db".to_string(), "test_id").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn check_conflict_returns_not_found_when_document_does_not_exist() {
        let mut mock = MockDatabase::new();

        mock.expect_find_one()
            .returning(|_, _| Box::pin(async { Ok(None) }));

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
        });

        let result = check_conflict(state.clone(), "test_db".to_string(), "test_id")
            .await
            .unwrap();

        assert_eq!(result.0, StatusCode::NOT_FOUND);
        assert_json_eq!(result.1 .0, json!({ "error": "not found" }));
    }

    #[tokio::test]
    async fn check_conflict_returns_conflict_when_document_exists() {
        let mut mock = MockDatabase::new();

        mock.expect_find_one()
            .returning(|_, _| Box::pin(async { Ok(Some(bson::doc! { "_id": "test_id" })) }));

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
        });

        let result = check_conflict(state.clone(), "test_db".to_string(), "test_id")
            .await
            .unwrap();

        assert_eq!(result.0, StatusCode::CONFLICT);
        assert_eq!(result.1 .0, json!({ "error": "conflict" }));
    }
}

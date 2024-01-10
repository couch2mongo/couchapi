pub mod bulk;
pub mod create_update;
pub mod delete;
pub mod get;
mod get_js;
pub mod update;

use crate::state::AppState;
use axum::http::StatusCode;
use axum::Json;
use bson::Document;
use serde_json::{json, Value};
use std::error::Error;
use std::sync::Arc;

pub type JsonWithStatusCodeResponse = (StatusCode, Json<Value>);

/// check_conflict checks to see if the document exists and if it does, returns a 409
/// conflict error.
pub async fn check_conflict(
    state: Arc<AppState>,
    collection: String,
    id: &str,
) -> Result<JsonWithStatusCodeResponse, Box<dyn Error>> {
    // Grab the document to determine if it exists or not
    let document = match state.db.find_one(&collection, id).await {
        Ok(document) => document,
        Err(e) => {
            return Err(Box::new(e));
        }
    };

    // This would be weird - but we should say
    if document.is_none() {
        return Ok((StatusCode::NOT_FOUND, Json(json!({"error": "not_found"}))));
    }

    // Looks like a standard conflict
    Ok((StatusCode::CONFLICT, Json(json!({"error": "conflict"}))))
}

/// get_item_from_db returns the document from the database or a 404 if it doesn't exist
pub async fn get_item_from_db(
    state: Arc<AppState>,
    db: String,
    id: String,
) -> Result<Document, JsonWithStatusCodeResponse> {
    let document = match state.db.find_one(&db, &id).await {
        Ok(d) => match d {
            Some(d) => d,
            None => {
                return Err((StatusCode::NOT_FOUND, Json(json!({"error": "not_found"}))));
            }
        },
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            ));
        }
    };

    Ok(document)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::MockDatabase;
    use assert_json_diff::assert_json_eq;
    use mongodb::error::Error as MongoError;
    use std::sync::Arc;

    #[tokio::test]
    async fn get_item_from_db_returns_document_when_found() {
        let mut mock = MockDatabase::new();

        let expected_document = bson::doc! { "name": "test" };

        mock.expect_find_one()
            .withf(move |_, id| id == "test_id")
            .returning(move |_, _| {
                Box::pin(async move { Ok(Some(bson::doc! { "name": "test" })) })
            });

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

        let result = get_item_from_db(state.clone(), "test_db".to_string(), "test_id".to_string())
            .await
            .unwrap();

        assert_eq!(result, expected_document);
    }

    #[tokio::test]
    async fn get_item_from_db_returns_not_found_when_document_does_not_exist() {
        let mut mock = MockDatabase::new();

        mock.expect_find_one()
            .returning(|_, _| Box::pin(async { Ok(None) }));

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

        let result = get_item_from_db(state.clone(), "test_db".to_string(), "test_id".to_string())
            .await
            .unwrap_err();

        assert_eq!(result.0, StatusCode::NOT_FOUND);
        assert_json_eq!(result.1 .0, json!({ "error": "not_found" }));
    }

    #[tokio::test]
    async fn get_item_from_db_returns_internal_server_error_on_find_one_error() {
        let mut mock = MockDatabase::new();

        mock.expect_find_one()
            .returning(|_, _| Box::pin(async { Err(MongoError::custom("nothing")) }));

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

        let result = get_item_from_db(state.clone(), "test_db".to_string(), "test_id".to_string())
            .await
            .unwrap_err();

        assert_eq!(result.0, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(result.1 .0.get("error").is_some());
    }

    #[tokio::test]
    async fn check_conflict_throws_error_on_find_one_error() {
        let mut mock = MockDatabase::new();

        mock.expect_find_one()
            .returning(|_, _| Box::pin(async { Err(MongoError::custom("nothing")) }));

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
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
            updates_folder: None,
            couchdb_details: None,
        });

        let result = check_conflict(state.clone(), "test_db".to_string(), "test_id")
            .await
            .unwrap();

        assert_eq!(result.0, StatusCode::NOT_FOUND);
        assert_json_eq!(result.1 .0, json!({ "error": "not_found" }));
    }

    #[tokio::test]
    async fn check_conflict_returns_conflict_when_document_exists() {
        let mut mock = MockDatabase::new();

        mock.expect_find_one()
            .returning(|_, _| Box::pin(async { Ok(Some(bson::doc! { "_id": "test_id" })) }));

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

        let result = check_conflict(state.clone(), "test_db".to_string(), "test_id")
            .await
            .unwrap();

        assert_eq!(result.0, StatusCode::CONFLICT);
        assert_eq!(result.1 .0, json!({ "error": "conflict" }));
    }
}

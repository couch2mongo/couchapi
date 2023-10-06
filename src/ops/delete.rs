use crate::common::IfMatch;
use crate::couchdb::maybe_write;
use crate::ops::{check_conflict, JsonWithStatusCodeResponse};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use mongodb::options::DeleteOptions;
use reqwest::Method;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

pub async fn inner_delete_item(
    state: Arc<AppState>,
    db: String,
    item: String,
    params: HashMap<String, String>,
    if_match: Option<String>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    let existing_rev = match params.get("rev") {
        Some(rev) => Some(rev.to_string()),
        None => if_match,
    }
    .ok_or((
        StatusCode::PRECONDITION_FAILED,
        Json(json!({"error": "missing rev"})),
    ))?;

    let filter = bson::doc! { "_id": item.clone(), "_rev": &existing_rev };
    let options = DeleteOptions::builder().build();
    match state.db.delete_one(db.clone(), filter, options).await {
        Ok(_) => (),
        Err(_) => {
            return match check_conflict(state, db.clone(), &item.clone()).await {
                Ok((status, json)) => Err((status, json)),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal server error", "details": e.to_string()})),
                )),
            }
        }
    };

    Ok(Json(json!({"ok": true, "id": item, "rev": &existing_rev})).into_response())
}

pub async fn delete_item(
    Extension(IfMatch(if_match)): Extension<IfMatch>,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    Path((db, item)): Path<(String, String)>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    let c = maybe_write(
        &state.couchdb_details,
        &db,
        Method::DELETE,
        None,
        &item,
        &params,
    )
    .await?;

    if let Some(r) = c {
        return Ok(r);
    }

    inner_delete_item(state, db, item, params, if_match).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::*;
    use assert_json_diff::assert_json_eq;
    use bson::doc;
    use hyper::body::to_bytes;
    use reqwest::StatusCode;
    use serde_json::Value;

    #[tokio::test]
    async fn test_delete_item() {
        let mut mock = MockDatabase::new();

        mock.expect_delete_one()
            .returning(|_, _, _| Box::pin(async { Ok(u64::try_from(1).unwrap()) }));

        let app_state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

        let db_name = "test_db".to_string();
        let item_id = "test_item".to_string();

        let result = inner_delete_item(app_state, db_name, item_id.clone()).await;

        match result {
            Ok(response) => {
                assert_eq!(response.status(), StatusCode::OK);

                let body = to_bytes(response.into_body()).await.unwrap();
                let actual_json_body: Value = serde_json::from_slice(&body).unwrap();
                let expected_json_body = json!({
                    "ok": true,
                    "id": item_id,
                    "rev": "test_rev",
                });
                assert_json_eq!(actual_json_body, expected_json_body);
            }
            Err((status_code, _json)) => {
                panic!("Expected OK, got error with status code {:?}", status_code);
            }
        };
    }

    #[tokio::test]
    async fn test_delete_item_no_rev() {
        let mock = MockDatabase::new();

        let app_state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

        let db_name = "test_db".to_string();
        let item_id = "test_item".to_string();

        let result = delete_item(
            Extension(IfMatch(None)),
            State(app_state),
            Query(HashMap::new()),
            Path((db_name, item_id.clone())),
        )
        .await;

        match result {
            Ok(response) => {
                panic!(
                    "Expected Error, got error with status code {:?}",
                    response.status()
                );
            }
            Err((status_code, _json)) => {
                assert_eq!(status_code, StatusCode::PRECONDITION_FAILED);
            }
        };
    }

    #[tokio::test]
    async fn test_delete_item_error() {
        let mut mock = MockDatabase::new();

        mock.expect_delete_one()
            .returning(|_, _, _| Box::pin(async { Err(mongodb::error::Error::custom("nothing")) }));

        mock.expect_find_one()
            .returning(|_, _| Box::pin(async { Err(mongodb::error::Error::custom("nothing")) }));

        let app_state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

        let db_name = "test_db".to_string();
        let item_id = "test_item".to_string();

        let result = inner_delete_item(app_state, db_name, item_id).await;

        match result {
            Ok(response) => {
                panic!(
                    "Expected Error, got error with status code {:?}",
                    response.status()
                );
            }
            Err((status_code, _json)) => {
                assert_eq!(status_code, StatusCode::INTERNAL_SERVER_ERROR);
            }
        };
    }

    #[tokio::test]
    async fn test_delete_item_wrong_rev() {
        let mut mock = MockDatabase::new();

        mock.expect_delete_one()
            .returning(|_, _, _| Box::pin(async { Err(mongodb::error::Error::custom("nothing")) }));

        mock.expect_find_one().returning(|_, _| {
            Box::pin(async { Ok(Some(doc! { "_id": "test_item", "_rev": "test_rev" })) })
        });

        let app_state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

        let db_name = "test_db".to_string();
        let item_id = "test_item".to_string();

        let result = inner_delete_item(app_state, db_name, item_id).await;

        match result {
            Ok(response) => {
                panic!(
                    "Expected Error, got error with status code {:?}",
                    response.status()
                );
            }
            Err((status_code, _json)) => {
                assert_eq!(status_code, StatusCode::CONFLICT);
            }
        };
    }

    async fn inner_delete_item(
        app_state: Arc<AppState>,
        db_name: String,
        item_id: String,
    ) -> Result<Response, (StatusCode, Json<Value>)> {
        delete_item(
            Extension(IfMatch(None)),
            State(app_state),
            Query({
                let mut map = HashMap::new();
                map.insert("rev".to_string(), "test_rev".to_string());
                map
            }),
            Path((db_name, item_id.clone())),
        )
        .await
    }
}

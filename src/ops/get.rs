use crate::common::IfNoneMatch;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

pub async fn get_item(
    Extension(IfNoneMatch(if_none_match)): Extension<IfNoneMatch>,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    Path((db, item)): Path<(String, String)>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let document = match state.db.find_one(db, item).await {
        Ok(d) => match d {
            Some(d) => d,
            None => {
                return Err((StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))));
            }
        },
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            ));
        }
    };

    // Emulate https://datatracker.ietf.org/doc/html/rfc7232#section-3.2
    if if_none_match.is_some() {
        return if if_none_match.as_ref().unwrap() == document.get_str("_rev").unwrap() {
            Err((StatusCode::NOT_MODIFIED, Json(json!({}))))
        } else {
            let mut r = Response::default();
            *r.status_mut() = StatusCode::PRECONDITION_FAILED;
            Ok(r)
        };
    }

    // Forces retrieving latest "leaf" revision, no matter what rev was requested. Default is false
    let latest = match params.get("latest") {
        Some(b) => matches!(b.as_str(), "true"),
        None => false,
    };

    // Forces the use of the rev parameter to match the document revision but only if latest is
    // false
    let rev = match params.get("rev") {
        Some(rev) => {
            if !latest && rev.as_str() != document.get_str("_rev").unwrap() {
                return Err((StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))));
            }
            Some(rev.clone())
        }
        None => None,
    };

    let mut json_document = Json(json!(document)).into_response();

    if let Some(rev) = document.get("_rev") {
        json_document
            .headers_mut()
            .insert("Etag", rev.to_string().parse().unwrap());
    }

    if rev.is_some() {
        // This will remove the body from the response but return the 304 as required
        *json_document.status_mut() = StatusCode::NOT_MODIFIED;
    }

    Ok(json_document)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::*;
    use assert_json_diff::assert_json_eq;
    use bson::doc;
    use hyper::body::to_bytes;
    use reqwest::StatusCode;

    #[tokio::test]
    async fn test_get_item_basic() {
        let mut mock = MockDatabase::new();

        mock.expect_find_one().returning(|_, _| {
            Box::pin(async { Ok(Some(doc! { "_id": "test_item", "_rev": "test_rev" })) })
        });

        let app_state = Arc::new(AppState { db: Box::new(mock) });

        // Assume the test data exists in MongoDB
        let db_name = "test_db".to_string();
        let item_id = "test_item".to_string();

        let result = get_item(
            Extension(IfNoneMatch(None)),
            State(app_state),
            Query(HashMap::new()),
            Path((db_name, item_id.clone())),
        )
        .await;

        match result {
            Ok(response) => {
                assert_eq!(response.status(), StatusCode::OK);

                let body = to_bytes(response.into_body()).await.unwrap();

                let actual_json_body: Value = serde_json::from_slice(&body).unwrap();
                let expected_json_body = json!({
                    "_id": item_id,
                    "_rev": "test_rev",
                });
                assert_json_eq!(actual_json_body, expected_json_body);
            }
            Err((status_code, _json)) => {
                panic!("Expected OK, got error with status code {:?}", status_code);
            }
        };
    }

    #[tokio::test]
    async fn test_get_item_not_found() {
        let mut mock = MockDatabase::new();

        mock.expect_find_one()
            .returning(|_, _| Box::pin(async { Ok(None) }));

        let app_state = Arc::new(AppState { db: Box::new(mock) });

        let db_name = "test_db".to_string();
        let item_id = "test_item".to_string();

        let result = get_item(
            Extension(IfNoneMatch(None)),
            State(app_state),
            Query(HashMap::new()),
            Path((db_name, item_id)),
        )
        .await;

        match result {
            Ok(response) => {
                panic!(
                    "Expected NOT_FOUND, got error with status code {:?}",
                    response.status()
                );
            }
            Err((status_code, json)) => {
                assert_eq!(status_code, StatusCode::NOT_FOUND);

                let body = to_bytes(json.into_response().into_body()).await.unwrap();
                let actual_json_body: Value = serde_json::from_slice(&body).unwrap();
                let expected_json_body = json!({
                    "error": "not found",
                });
                assert_json_eq!(actual_json_body, expected_json_body);
            }
        };
    }

    #[tokio::test]
    async fn test_get_item_if_none_match() {
        let mut mock = MockDatabase::new();

        mock.expect_find_one().returning(|_, _| {
            Box::pin(async { Ok(Some(doc! { "_id": "test_item", "_rev": "test_rev" })) })
        });

        let app_state = Arc::new(AppState { db: Box::new(mock) });

        let db_name = "test_db".to_string();
        let item_id = "test_item".to_string();

        let result = get_item(
            Extension(IfNoneMatch(Some("test_rev".to_string()))),
            State(app_state),
            Query(HashMap::new()),
            Path((db_name, item_id)),
        )
        .await;

        match result {
            Ok(response) => {
                panic!(
                    "Expected NOT_MODIFIED, got error with status code {:?}",
                    response.status()
                );
            }
            Err((status_code, json)) => {
                assert_eq!(status_code, StatusCode::NOT_MODIFIED);

                let body = to_bytes(json.into_response().into_body()).await.unwrap();
                let actual_json_body: Value = serde_json::from_slice(&body).unwrap();
                let expected_json_body = json!({});
                assert_json_eq!(actual_json_body, expected_json_body);
            }
        };
    }

    #[tokio::test]
    async fn test_get_item_if_none_match_different_rev() {
        let mut mock = MockDatabase::new();

        mock.expect_find_one().returning(|_, _| {
            Box::pin(async { Ok(Some(doc! { "_id": "test_item", "_rev": "test_rev" })) })
        });

        let app_state = Arc::new(AppState { db: Box::new(mock) });

        let db_name = "test_db".to_string();
        let item_id = "test_item".to_string();

        let result = get_item(
            Extension(IfNoneMatch(Some("alternative_rev".to_string()))),
            State(app_state),
            Query(HashMap::new()),
            Path((db_name, item_id)),
        )
        .await;

        match result {
            Ok(response) => {
                assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);

                let body = to_bytes(response.into_body()).await.unwrap();
                assert_eq!(body, "");
            }
            Err((status_code, _json)) => {
                panic!(
                    "Expected PRECONDITION_FAILED, got error with status code {:?}",
                    status_code
                );
            }
        };
    }
}

use crate::couchdb::maybe_write;
use crate::ops::create_update::inner_new_item;
use crate::ops::delete::inner_delete_item;
use crate::ops::JsonWithStatusCodeResponse;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use http_body_util::BodyExt;
use maplit::hashmap;
use reqwest::Method;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Docs {
    docs: Vec<Value>,
}

pub async fn bulk_docs(
    State(state): State<Arc<AppState>>,
    Path(db): Path<String>,
    Json(payload): Json<Docs>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    let p = json!(payload);

    let c = maybe_write(
        &state.couchdb_details,
        &db,
        Method::POST,
        Some(&p),
        "_bulk_docs",
        &hashmap! {},
    )
    .await?;

    if let Some(r) = c {
        return Ok(r);
    }

    let mut collected_responses: Vec<Value> = vec![];

    for doc in payload.docs {
        let delete = doc
            .get("_deleted")
            .and_then(|d| d.as_bool())
            .unwrap_or(false);

        let id = doc
            .get("_id")
            .and_then(|id| id.as_str())
            .map(|id| id.to_string());

        let response = match delete {
            true => {
                let rev = doc.get("_rev").and_then(|r| r.as_str()).ok_or((
                    StatusCode::PRECONDITION_FAILED,
                    Json(json!({"error": "missing rev"})),
                ));

                match rev {
                    Ok(r) => inner_delete_item(
                        state.clone(),
                        db.clone(),
                        id.clone().unwrap(),
                        hashmap! {
                            "rev".to_string() => r.to_string()
                        },
                        None,
                    )
                    .await
                    .map(|_| {
                        Json(json!({"ok": true, "id": id.clone().unwrap(), "rev": r.to_string()}))
                            .into_response()
                    }),
                    Err(e) => Err(e),
                }
            }
            false => {
                inner_new_item(
                    db.clone(),
                    id.clone(),
                    state.clone(),
                    hashmap! {},
                    doc.clone(),
                    None,
                )
                .await
            }
        };

        match response {
            Ok(r) => {
                let body = BodyExt::collect(r.into_body()).await.unwrap().to_bytes();
                let json: Value = serde_json::from_slice(&body).unwrap();
                collected_responses.push(json);
            }
            Err((..)) => {
                let j = json!({
                    "id": id,
                    "error": "conflict",
                    "reason": "Document update conflict."
                });

                collected_responses.push(j)
            }
        }
    }

    let response = Json(json!(collected_responses));
    let mut response = response.into_response();
    *response.status_mut() = StatusCode::CREATED;
    Ok(response)
}

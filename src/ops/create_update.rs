use crate::common::IfMatch;
use crate::couchdb::maybe_write;
use crate::ops::{check_conflict, JsonWithStatusCodeResponse};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use mongodb::options::ReplaceOptions;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

pub async fn new_item(
    Extension(IfMatch(if_match)): Extension<IfMatch>,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    Path(db): Path<String>,
    Json(payload): Json<Value>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    let c = maybe_write(
        &state.couchdb_details,
        &db,
        Method::POST,
        Some(&payload),
        &db,
        &params,
    )
    .await?;

    if c.is_some() {
        return Ok(c.unwrap());
    }

    inner_new_item(db, None, state, params, payload, if_match).await
}

pub async fn new_item_with_id(
    Extension(IfMatch(if_match)): Extension<IfMatch>,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    Path((db, item)): Path<(String, String)>,
    Json(payload): Json<Value>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    let path = format!("{}/{}", db, item);

    let c = maybe_write(
        &state.couchdb_details,
        &db,
        Method::PUT,
        Some(&payload),
        &path,
        &params,
    )
    .await?;

    if c.is_some() {
        return Ok(c.unwrap());
    }

    inner_new_item(db, Some(item), state, params, payload, if_match).await
}

pub async fn inner_new_item(
    db: String,
    item: Option<String>,
    state: Arc<AppState>,
    _params: HashMap<String, String>,
    payload: Value,
    rev_if_match: Option<String>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    // Generate an id if one wasn't provided through either the URL or the payload
    let id = match item {
        Some(i) => i,
        None => match payload.get("_id").and_then(|id| id.as_str()) {
            Some(id) => id.to_string(),
            None => {
                let mut id = Uuid::new_v4().to_string();
                id.retain(|c| c != '-');
                id
            }
        },
    };

    let existing_rev = match payload.get("_rev").and_then(|rev| rev.as_str()) {
        Some(rev) => Some(rev.to_string()),
        None => rev_if_match,
    };

    // Calculate the new 'rev' using the same formula as CouchDB - which the MD5 of the payload
    let digest = md5::compute(payload.to_string());
    let body_md5 = format!("{:x}", digest);

    // This might look confusing so to explain... If there is no existing rev, then this is a new
    // document and we set the rev to 1-<md5>. If there is an existing rev, then we split it on the
    // dash and increment the first part by 1 and then append the md5 of the body to the end.
    let new_rev = existing_rev
        .clone()
        .map_or(format!("1-{}", body_md5), |rev| {
            let rev_number = rev.split('-').next().unwrap().parse::<u64>().unwrap();
            format!("{}-{}", rev_number + 1, body_md5)
        });

    // Create the BSON document and re-insert the _id field as, insert() weirdly is an upsert.
    let mut bson_value = bson::to_bson(&payload).unwrap();
    let new_bson_document = bson_value.as_document_mut().unwrap();
    new_bson_document.insert("_rev", new_rev.clone());
    new_bson_document.insert("_id", id.clone());

    // Within the collection, replace the document with the new one but only if the _rev of the
    // document matches the existing one.
    let mut filter = bson::doc! { "_id": id.clone() };
    if let Some(rev) = existing_rev {
        filter.insert("_rev", rev);
    }

    // This allows for the insert if one doesn't exist
    let options = ReplaceOptions::builder().upsert(true).build();

    // Try and get the document in
    match state
        .db
        .replace_one(db.clone(), filter, new_bson_document.clone(), options)
        .await
    {
        Ok(_) => (),
        Err(_) => {
            // Check for the conflict to return the right error message
            return match check_conflict(state, db.clone(), &id).await {
                Ok((status, json)) => Err((status, json)),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal server error", "details": e.to_string()})),
                )),
            };
        }
    };

    // Build a response with the new id and rev
    let response = Json(json!({"ok": true, "id": id, "rev": new_rev}));
    let mut response = response.into_response();
    response
        .headers_mut()
        .insert("Location", format!("/{}", id).parse().unwrap());
    *response.status_mut() = StatusCode::CREATED;

    Ok(response)
}

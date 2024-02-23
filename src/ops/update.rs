// Copyright (c) 2024, Green Man Gaming Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::couchdb::maybe_write;
use crate::ops::create_update::inner_new_item;
use crate::ops::{get_item_from_db, JsonWithStatusCodeResponse};
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use boa_engine::property::Attribute;
use boa_engine::{Context, JsValue, Source};
use boa_runtime::Console;
use bson::Document;
use http_body_util::BodyExt;
use maplit::hashmap;
use reqwest::Method;
use serde_json::{json, Map, Value};
use std::path::PathBuf;
use std::sync::Arc;

/// Execute an update script
///
/// This method is too long at present and requires further work.
pub async fn inner_execute_update_script(
    db: String,
    design: String,
    func: String,
    document_id: Option<String>,
    state: Arc<AppState>,
    payload: Value,
) -> Result<Response, JsonWithStatusCodeResponse> {
    let updates_folder = state.updates_folder.clone().ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "no updates folder specified"})),
        )
    })?;

    let mut path = PathBuf::from(updates_folder);
    path.push(&db);
    path.push(&design);
    path.push(format!("{}.js", func));

    let path = path.as_path();
    if !path.is_file() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "update script not found"})),
        ));
    }

    let document = if let Some(document_id) = document_id.clone() {
        match get_item_from_db(state.clone(), db.clone(), document_id.to_string()).await {
            Ok(d) => Some(d),
            Err((status_code, _)) => {
                // We're actually OK here - some update handler scripts expect no document
                // to exist, and perform an upsert operation. So we don't want to short-circuit
                // here, instead catch and return None.
                if status_code == StatusCode::NOT_FOUND {
                    None
                } else {
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "error getting document"})),
                    ));
                }
            }
        }
    } else {
        None
    };

    let document_json = document.as_ref().map_or_else(|| json!({}), |d| json!(d));

    let return_value = execute_javascript(path, &document_id, &document, &document_json, &payload)?;

    let return_value_vector = if let Value::Array(v) = return_value {
        v
    } else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "return value is not an array"})),
        ));
    };

    let returned_document =
        get_returned_value(&return_value_vector, 0, "return value is not an object")?;
    let returned_response =
        get_returned_value(&return_value_vector, 1, "return value is not an object")?;

    let mut response = Response::new(String::new());

    if let Some(returned_document) = returned_document {
        let new_document_id = returned_document
            .get("_id")
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "return value has_no_id"})),
                )
            })?
            .as_str()
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "return value has_no_id"})),
                )
            })?
            .to_string();

        let item_response = inner_new_item(
            db.clone(),
            Some(new_document_id),
            state,
            hashmap! {},
            json!(returned_document),
            None,
        )
        .await?;

        let body = BodyExt::collect(item_response.into_body())
            .await
            .unwrap()
            .to_bytes();

        let body_json: Value = serde_json::from_slice(&body).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "invalid json returned"})),
            )
        })?;

        let new_rev = body_json
            .get("rev")
            .and_then(|r| r.as_str())
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "invalid json returned"})),
                )
            })?
            .to_string();

        response.headers_mut().insert(
            "x-couch-update-newrev",
            HeaderValue::from_str(&new_rev).map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "invalid json returned"})),
                )
            })?,
        );
    }

    if returned_response.is_none() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "return value is not an object"})),
        ));
    }

    *response.status_mut() = StatusCode::from_u16(
        returned_response
            .unwrap()
            .get("code")
            .unwrap_or(&Value::Null)
            .as_u64()
            .unwrap_or(200) as u16,
    )
    .unwrap();

    if let Some(json) = returned_response.unwrap().get("json") {
        *response.body_mut() = json.to_string();
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }

    if let Some(body) = returned_response.unwrap().get("body") {
        *response.body_mut() = body.as_str().unwrap().to_string();
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
    }

    if returned_response.unwrap().get("base64").is_some() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "base64 is not implemented yet"})),
        ));
    }

    // TODO(lee): this code causes a borrow check fail as the return_value_vector does not live long
    //            enough. I'm not sure how to fix this yet.
    // if let Some(headers) = returned_response.unwrap().get("headers") {
    //     if let Value::Object(headers) = headers {
    //         for (key, value) in headers {
    //             let header_string = value.as_str().ok_or_else(|| {
    //                 (
    //                     StatusCode::INTERNAL_SERVER_ERROR,
    //                     Json(json!({"error": "header value is not a string"})),
    //                 )
    //             })?;
    //             let header_value = HeaderValue::from_str(header_string).map_err(|_| {
    //                 (
    //                     StatusCode::INTERNAL_SERVER_ERROR,
    //                     Json(json!({"error": "header value is not a valid value"})),
    //                 )
    //             })?;
    //
    //             response.headers_mut().insert(key.as_str(), header_value);
    //         }
    //     }
    // }

    Ok(response.into_response())
}

fn execute_javascript(
    path: &std::path::Path,
    req_id: &Option<String>,
    document: &Option<Document>,
    document_json: &Value,
    payload: &Value,
) -> Result<Value, JsonWithStatusCodeResponse> {
    let mut context = Context::default();

    let doc_js = if let Some(_document) = &document {
        JsValue::from_json(document_json, &mut context).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })?
    } else {
        JsValue::null()
    };

    let req = json!({
        "id": req_id,
        "body": payload.to_string(),
        "uuid": uuid::Uuid::new_v4().to_string(),
    });

    let req_js = JsValue::from_json(&req, &mut context).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
    })?;

    context
        .register_global_property("req", req_js, Attribute::all())
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })?;
    context
        .register_global_property("doc", doc_js, Attribute::all())
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })?;

    let console = Console::init(&mut context);
    context
        .register_global_property(Console::NAME, console, Attribute::all())
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })?;

    let javascript_file = std::fs::read_to_string(path).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
    })?;

    let javascript_file = format!("f = {}", javascript_file);
    let javascript_file = format!("{}\n\nresult = f(doc, req)", javascript_file);

    let src = Source::from_bytes(javascript_file.as_bytes());

    context.eval(src).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
    })?;

    // Bump the result through a back n forth through JSON to ensure that we have a valid
    // JSON object at the end of the process. This will strip things like undefined etc.
    context
        .eval(Source::from_bytes(
            "result = JSON.parse(JSON.stringify(result));".as_bytes(),
        ))
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })?;

    let result = context
        .global_object()
        .get("result", &mut context)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })?;

    Ok(result.to_json(&mut context).unwrap())
}

pub async fn execute_update_script(
    State(state): State<Arc<AppState>>,
    Path((db, design, function)): Path<(String, String, String)>,
    Json(payload): Json<Value>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    let u = format!("_design/{}/_update/{}", design, function);

    let c = maybe_write(
        &state.couchdb_details,
        &db,
        Method::PUT,
        Some(&payload),
        &u,
        &hashmap! {},
    )
    .await?;

    if let Some(r) = c {
        return Ok(r);
    }

    inner_execute_update_script(db, design, function, None, state, payload).await
}

pub async fn execute_update_script_with_doc(
    State(state): State<Arc<AppState>>,
    Path((db, design, func, document_id)): Path<(String, String, String, String)>,
    Json(payload): Json<Value>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    let u = format!("_design/{}/_update/{}/{}", design, func, document_id);

    let c = maybe_write(
        &state.couchdb_details,
        &db,
        Method::PUT,
        Some(&payload),
        &u,
        &hashmap! {},
    )
    .await?;

    if let Some(r) = c {
        return Ok(r);
    }

    inner_execute_update_script(db, design, func, Some(document_id), state, payload).await
}

fn get_returned_value<'a>(
    return_value_vector: &'a [Value],
    index: usize,
    error_message: &'a str,
) -> Result<Option<&'a Map<String, Value>>, JsonWithStatusCodeResponse> {
    match return_value_vector.get(index) {
        Some(Value::Object(o)) => Ok(Some(o)),
        Some(Value::Null) => Ok(None),
        Some(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error_message})),
        )),
        None => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "return value is empty"})),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_json_diff::assert_json_eq;

    #[test]
    fn test_get_returned_value() {
        let return_value_vector = vec![
            Value::Object(Map::new()),
            Value::Null,
            Value::String("not an object".to_string()),
        ];

        // Test with valid index
        let result = get_returned_value(&return_value_vector, 0, "error message");
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Test with null value
        let result = get_returned_value(&return_value_vector, 1, "error message");
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Test with invalid value
        let result = get_returned_value(&return_value_vector, 2, "error message");
        assert!(result.is_err());
        assert_json_eq!(result.unwrap_err().1 .0, json!({"error": "error message"}));

        // Test with out of bounds index
        let result = get_returned_value(&return_value_vector, 3, "error message");
        assert!(result.is_err());
        assert_json_eq!(
            result.unwrap_err().1 .0,
            json!({"error": "return value is empty"})
        );
    }
}

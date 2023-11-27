use crate::common::IfNoneMatch;
use crate::config::DesignView;
use crate::couchdb::read_through;
use crate::ops::get_js::execute_script;
use crate::ops::{get_item_from_db, JsonWithStatusCodeResponse};
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use boa_gc::Finalize;
use bson::{doc, Bson, Document};
use hyper::body::to_bytes;
use indexmap::IndexMap;
use maplit::hashmap;
use reqwest::Method;
use serde_derive::Serialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Create a DesignView that will return all documents in the database
/// This is used for the _all_docs endpoint and should not used as a
/// general purpose way of reading a database. However, we need
/// to support it for some services.
pub fn create_all_docs_design_view() -> DesignView {
    DesignView {
        filter_insert_index: 0,
        match_fields: vec!["_id".to_string()],
        key_fields: vec!["key".to_string()],
        value_fields: vec!["rev".to_string()],
        sort_fields: None,
        reduce: None,
        aggregation: vec![r#"{
                "$project": {
                    "_id": 1,
                    "key": "$_id",
                    "rev": "$_rev"
                }
            }"#
        .to_string()],
        single_item_key_is_list: false,
        single_item_value_is_dict: true,
        break_glass_js_script: None,
        omit_null_keys_in_value: false,
    }
}

pub async fn get_item(
    Extension(IfNoneMatch(if_none_match)): Extension<IfNoneMatch>,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    Path((db, item)): Path<(String, String)>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    let document = get_item_from_db(state, db, item).await?;

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
    let latest = params
        .get("latest")
        .map(|b| b.as_str() == "true")
        .unwrap_or(false);

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

fn get_param(params: &HashMap<String, String>, key: &str, fallback_key: &str) -> Option<String> {
    params
        .get(key)
        .cloned()
        .or_else(|| params.get(fallback_key).cloned())
}

#[derive(Debug, Clone, Finalize, Serialize)]
pub struct ViewOptions {
    pub reduce: bool,
    pub group: bool,
    pub group_level: i64,
    pub include_docs: bool,
    pub descending: bool,
    pub limit: Option<i64>,
    pub skip: i64,
    pub start_key: Vec<Value>,
    pub end_key: Vec<Value>,
    pub startkey_docid: Option<String>,
    pub endkey_docid: Option<String>,
    pub keys: Vec<Value>,
}

fn extract_view_options_from_params(params: HashMap<String, String>) -> ViewOptions {
    let start_key = get_param(&params, "startkey", "start_key");
    let end_key = get_param(&params, "endkey", "end_key");

    let startkey_docid = get_param(&params, "startkey_docid", "start_key_doc_id");
    let endkey_docid = get_param(&params, "endkey_docid", "end_key_doc_id");

    let reduce: bool = params
        .get("reduce")
        .and_then(|s| s.parse().ok())
        .unwrap_or(false);

    let mut group: bool = params
        .get("group")
        .and_then(|s| s.parse().ok())
        .unwrap_or(false);

    let group_level: i64 = params
        .get("group_level")
        .and_then(|s| s.parse().ok())
        .unwrap_or({
            if group {
                999 // Set to 999 if group is true and group_level is unset
            } else {
                0 // Set to 0 otherwise
            }
        });

    // If group_level is set, then group should be true.
    if group_level != 0 {
        group = true;
    }

    let include_docs = params
        .get("include_docs")
        .cloned()
        .or_else(|| params.get("include_docs").cloned())
        .unwrap_or("false".to_string())
        == "true";

    let descending = params
        .get("descending")
        .cloned()
        .or_else(|| params.get("descending").cloned())
        .unwrap_or("false".to_string())
        == "true";

    // Optionally see if we have a Limit or Skip parameter.
    let limit = params
        .get("limit")
        .cloned()
        .and_then(|s| s.parse::<i64>().ok());

    let mut key = vec![json!(extract_key_json(params.get("key").cloned()))];
    let mut keys = extract_key_json(params.get("keys").cloned());

    if params.get("key").is_some() {
        keys.append(&mut key);
    }

    // Skip is more nuanced, we assume 0 if it's not present
    let skip = params
        .get("skip")
        .map_or(0, |v| v.parse::<i64>().unwrap_or(0));

    let start_key = extract_key_json(start_key);
    let end_key = extract_key_json(end_key);

    ViewOptions {
        reduce,
        group,
        group_level,
        include_docs,
        descending,
        limit,
        skip,
        start_key,
        end_key,
        startkey_docid,
        endkey_docid,
        keys,
    }
}

async fn inner_get_view(
    v: &DesignView,
    db: String,
    state: &AppState,
    params: HashMap<String, String>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    let view_options = extract_view_options_from_params(params);

    let pipeline = if let Some(f) = &v.break_glass_js_script {
        execute_script(f.as_str(), &view_options)?
    } else {
        create_automated_pipeline(v, &view_options).await?
    };

    let results_run = state.db.aggregate(db.clone(), pipeline).await;
    if results_run.is_err() {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": results_run.err().unwrap().to_string()})),
        ));
    }

    let results = results_run.unwrap();

    // This 'magic' takes the aggregated results and the configuration for the view
    // and creates the JSON response that CouchDB would return.
    let mut items = results
        .into_iter()
        .map(|doc| {
            let k = v
                .key_fields
                .iter()
                .map(|x| doc.get(x).unwrap_or(&Bson::Null))
                .collect::<Vec<_>>();

            let value = v
                .value_fields
                .iter()
                .map(|x| (x, doc.get(x)))
                .filter(|(_, val)| {
                    !v.omit_null_keys_in_value || val.is_some_and(|v| *v != Bson::Null)
                })
                .collect::<HashMap<_, _>>();

            // If k is only one item then we can just return the value, otherwise we need to
            // return an array of the values
            let k = if k.len() == 1 && !v.single_item_key_is_list {
                json!(k[0].clone())
            } else {
                json!(k)
            };

            // If v is only one item then we can just return the value, otherwise we need to
            // return the actual HashMap.
            let v = if value.keys().len() == 1 && !v.single_item_value_is_dict {
                // We want the only item in the list so we Collect the values into a Vec, and
                // grab the first item. This is safe because we know there is only one item.
                json!(value.values().collect::<Vec<_>>().get(0).unwrap())
            } else {
                json!(value)
            };

            json!({
                "id": json!(doc.get("_id").unwrap_or(&Bson::Null)),
                "key": k,
                "value": v,
            })
        })
        .collect::<Vec<_>>();

    // As per CouchDB documentation, include_docs is rarely sensible for views because for every
    // document returned in the index, we have to go ahead and fetch each one. MongoDB also hates
    // this. So, we emulate precisely what CouchDB would do and fetch each document individually.
    //
    // This could be optimized by using find with many IDs at once but all that does it move the
    // iterator to the server.
    if view_options.include_docs {
        for item in &mut items {
            let id = item.get("id").unwrap().as_str().unwrap();
            let doc_result = state.db.find_one(db.clone(), id.to_string()).await;
            let doc = match doc_result {
                Ok(doc) => match doc {
                    Some(doc) => doc,
                    None => doc! {},
                },
                Err(_) => doc! {},
            };
            item["doc"] = json!(doc);
        }
    }

    let count = state.db.count(db.clone()).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
    })?;

    let return_value = json!({
        "total_rows": count,
        "offset": view_options.skip,
        "rows": items,
    });

    let json_document = Json(return_value).into_response();
    Ok(json_document)
}

async fn create_automated_pipeline(
    v: &DesignView,
    view_options: &ViewOptions,
) -> Result<Vec<Document>, JsonWithStatusCodeResponse> {
    let filter = create_filter(
        v,
        &view_options.keys,
        &view_options.start_key,
        &view_options.end_key,
        &view_options.startkey_docid,
        &view_options.endkey_docid,
        view_options.descending,
    );

    let mut original_pipeline = extract_pipeline_bson(
        v,
        view_options.reduce | view_options.group,
        view_options.group_level,
    )?;

    if !filter.is_empty() {
        match original_pipeline.get_mut(v.filter_insert_index) {
            Some(doc) if doc.get("$match").is_some() => {
                let match_entry = doc.get_mut("$match").and_then(Bson::as_document_mut);

                if match_entry.is_none() {
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "expected $match to be a Document"})),
                    ));
                }

                let unwrapped_match_entry = match_entry.unwrap();

                for (k, v) in filter.iter() {
                    if k == "$and" {
                        let new_doc = doc! { "$and": v };

                        unwrapped_match_entry.extend(new_doc.into_iter());
                        continue;
                    }

                    let entry = unwrapped_match_entry
                        .entry(k.clone())
                        .or_insert_with(|| Bson::Document(doc! {}));
                    if let Some(entry_doc) = entry.as_document_mut() {
                        if let Some(v_doc) = v.as_document() {
                            entry_doc.extend(v_doc.clone().into_iter());
                        } else {
                            return Err((
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({"error": "expected value in filter to be a Document"})),
                            ));
                        }
                    } else {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"error": "expected entry in filter to be a Document"})),
                        ));
                    }
                }
            }
            _ => {
                let insert_index = std::cmp::min(v.filter_insert_index, original_pipeline.len());
                original_pipeline.insert(insert_index, doc! { "$match": filter });
            }
        }
    }

    if view_options.descending {
        for doc in &mut original_pipeline {
            if let Some(sort) = doc.get_mut("$sort").and_then(Bson::as_document_mut) {
                let fields = v.sort_fields.as_ref().unwrap_or(v.match_fields.as_ref());
                for field in fields {
                    if let Some(field) = sort.get_mut(field) {
                        if let Some(v) = field.as_i64() {
                            *field = Bson::Int64(-v);
                        }
                    }
                }
                sort.extend(doc! { "_id": -1 });
            }
        }
    }

    let mut pipeline = original_pipeline.clone();
    pipeline.push(doc! { "$skip": view_options.skip });

    // Add the $limit to the skipped_pipeline but only if the limit variable is set
    if let Some(lim) = view_options.limit {
        pipeline.push(doc! { "$limit": lim });
    }

    let pipeline_json = serde_json::to_string(&pipeline.clone()).unwrap();
    info!("pipeline '{}'", pipeline_json);

    Ok(pipeline)
}

fn extract_pipeline_bson(
    v: &DesignView,
    reduce: bool,
    group_level: i64,
) -> Result<Vec<Document>, JsonWithStatusCodeResponse> {
    let dv = v.clone();
    let it = if !reduce {
        dv.aggregation.iter()
    } else {
        let key_fields_length = dv.key_fields.len().to_string();
        let lookup_key = if group_level == 999 {
            key_fields_length
        } else {
            group_level.to_string()
        };

        dv.reduce
            .as_ref()
            .ok_or((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "expected reduce_view to be a Some"})),
            ))?
            .get(&lookup_key)
            .ok_or((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "expected reduce_view at group_level to be a Some"})),
            ))?
            .aggregation
            .iter()
    };

    it.map(|item| {
        serde_json::from_str(item.as_str())
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": e.to_string()})),
                )
            })
            .and_then(|j: Value| {
                bson::to_document(&j).map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": e.to_string()})),
                    )
                })
            })
    })
    .collect()
}

fn create_filter(
    v: &DesignView,
    keys: &[Value],
    start_key: &[Value],
    end_key: &[Value],
    start_key_doc_id: &Option<String>,
    end_key_doc_id: &Option<String>,
    flipped: bool,
) -> Document {
    let mut filter: Document = doc! {};

    match keys.len() {
        0 => {
            for (i, v) in v.match_fields.iter().enumerate() {
                let start = start_key.get(i).unwrap_or_else(|| &json!(null));
                let end = end_key.get(i).unwrap_or_else(|| &json!(null));

                let (start, end) = match flipped {
                    true => (bson::to_bson(end).ok(), bson::to_bson(start).ok()),
                    false => (bson::to_bson(start).ok(), bson::to_bson(end).ok()),
                };

                if start == end && start.is_some() {
                    filter.insert(v.clone(), doc! {"$eq": start.unwrap()});
                    continue;
                }

                let field = start
                    .filter(|val| *val != Bson::Null && *val != Bson::Document(Document::new()))
                    .map(|start_val| doc! {"$gte": &start_val})
                    .into_iter()
                    .chain(
                        // Only add the $lte condition if end is not null or an empty Document
                        end.filter(|val| {
                            *val != Bson::Null && *val != Bson::Document(Document::new())
                        })
                        .map(|end_val| doc! {"$lte": &end_val}),
                    )
                    .fold(doc! {}, |mut acc, val| {
                        acc.extend(val);
                        acc
                    });

                if !field.is_empty() {
                    filter.insert(v.clone(), field);
                }
            }

            if let Some(start_key_doc_id) = start_key_doc_id {
                if !start_key_doc_id.is_empty() {
                    filter.insert(
                        "_id",
                        doc! {
                            "$gte": start_key_doc_id
                        },
                    );
                }
            }

            if let Some(end_key_doc_id) = end_key_doc_id {
                if !end_key_doc_id.is_empty() {
                    filter.insert(
                        "_id",
                        doc! {
                            "$lte": end_key_doc_id
                        },
                    );
                }
            }
        }
        _ => {
            map_keys(v, keys, &mut filter);
        }
    }

    filter
}

fn map_keys(v: &DesignView, keys: &[Value], filter: &mut Document) {
    // Convert keys to Bson
    let vec_keys = keys
        .iter()
        .map(|key| {
            key.as_array()
                .unwrap_or(&vec![key.clone()])
                .iter()
                .map(|v| bson::to_bson(&v).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    // Generate '$and' conditions for each key
    let and_conditions: Vec<Document> = vec_keys
        .into_iter()
        .map(|values| {
            let map: IndexMap<String, Bson> =
                v.match_fields.clone().into_iter().zip(values).collect();
            let doc_and: Document = map.into_iter().collect();
            doc! { "$and": [doc_and] }
        })
        .collect();

    // Insert into the filter
    filter.insert("$and", vec![doc! { "$or": and_conditions }]);
}

pub async fn get_view(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    Path((db, design, view)): Path<(String, String, String)>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    let actual_view = extract_view_from_views(&state, db.as_str(), design.as_str(), view.as_str());
    if actual_view.is_err() {
        if state.couchdb_details.is_some()
            && state
                .couchdb_details
                .as_ref()
                .unwrap()
                .should_read_through(&db)
        {
            let couchdb_details = state.couchdb_details.as_ref().unwrap();
            let mapped_db = couchdb_details.map_for_db(db.as_str());

            let path = format!("{}/_design/{}/_view/{}", mapped_db, design, view);
            return read_through(couchdb_details, Method::GET, None, &path, &params).await;
        }

        return Err(actual_view.err().unwrap());
    }

    inner_get_view(actual_view.unwrap(), db.to_string(), state.as_ref(), params).await
}

fn extract_view_from_views<'a>(
    state: &'a Arc<AppState>,
    db: &'a str,
    design: &'a str,
    view: &'a str,
) -> Result<&'a DesignView, (StatusCode, Json<Value>)> {
    if state.views.is_none() {
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            Json(json!({"error": "not implemented"})),
        ));
    }

    let views = state.views.as_ref().unwrap();

    let design_mapping = match views.get(db) {
        Some(design_mapping) => design_mapping,
        None => {
            return Err((StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))));
        }
    };

    let view_group = match design_mapping.view_groups.get(design) {
        Some(view) => view,
        None => {
            return Err((StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))));
        }
    };

    let actual_view = match view_group.get(view) {
        Some(view) => view,
        None => {
            return Err((StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))));
        }
    };

    Ok(actual_view)
}

pub async fn post_get_view(
    State(state): State<Arc<AppState>>,
    Path((db, design, view)): Path<(String, String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(payload): Json<Value>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let mut payload_map = convert_payload(payload.clone());
    payload_map.extend(params);

    let actual_view = extract_view_from_views(&state, db.as_str(), design.as_str(), view.as_str());
    if actual_view.is_err() {
        if state.couchdb_details.is_some()
            && state
                .couchdb_details
                .as_ref()
                .unwrap()
                .should_read_through(&db)
        {
            let couchdb_details = state.couchdb_details.as_ref().unwrap();
            let mapped_db = couchdb_details.map_for_db(db.as_str());

            let path = format!("{}/_design/{}/_view/{}", mapped_db, design, view);
            return read_through(
                couchdb_details,
                Method::POST,
                Some(&payload),
                &path,
                &hashmap! {},
            )
            .await;
        }

        return Err(actual_view.err().unwrap());
    }

    inner_get_view(
        actual_view.unwrap(),
        db.to_string(),
        state.as_ref(),
        payload_map,
    )
    .await
}

pub async fn post_multi_query(
    State(state): State<Arc<AppState>>,
    Path((db, design, view)): Path<(String, String, String)>,
    Query(params): Query<HashMap<String, String>>,
    Json(payload): Json<Value>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let actual_view = extract_view_from_views(&state, db.as_str(), design.as_str(), view.as_str());

    if actual_view.is_err() {
        if state.couchdb_details.is_some()
            && state
                .couchdb_details
                .as_ref()
                .unwrap()
                .should_read_through(&db)
        {
            let couchdb_details = state.couchdb_details.as_ref().unwrap();
            let mapped_db = couchdb_details.map_for_db(db.as_str());

            let path = format!("{}/_design/{}/_view/{}/queries", mapped_db, design, view);
            return read_through(
                couchdb_details,
                Method::POST,
                Some(&payload),
                &path,
                &hashmap! {},
            )
            .await;
        }

        return Err(actual_view.err().unwrap());
    }

    let actual_view = actual_view.unwrap();

    let queries = payload
        .get("queries")
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "expected payload to contain a queries field"})),
            )
        })?
        .clone();

    match queries {
        Value::Array(payload) => {
            let mut results = Vec::new();
            for p in payload {
                let mut payload_map = convert_payload(p);
                payload_map.extend(params.clone());

                let result =
                    inner_get_view(actual_view, db.clone(), state.as_ref(), payload_map).await;
                results.push(result);
            }
            let mut json_results = Vec::new();
            for r in results {
                match r {
                    Ok(response) => {
                        let body = to_bytes(response.into_body()).await.unwrap();
                        let actual_json_body: Value = serde_json::from_slice(&body).unwrap();
                        json_results.push(actual_json_body);
                    }
                    Err((status_code, _json)) => {
                        json_results.push(json!({"error": status_code.to_string()}));
                    }
                }
            }

            let results = json!({"results": json_results});
            Ok(Json(results).into_response())
        }
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "expected payload to be an array"})),
        )),
    }
}

/// all_docs is an implementation of the CouchDB _all_docs API. It does this by creating a view
/// that returns the _id and _rev fields of every document in the collection. You really should
/// not use all_docs in a production environment, but we do. We use aggregation rather than find
/// because we want to re-use the same code as get_view. Behind the scenes we rely on MongoDB
/// to optimize the aggregation pipeline.
pub async fn all_docs(
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    Path(db): Path<String>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    inner_get_view(&create_all_docs_design_view(), db, state.as_ref(), params).await
}

pub async fn post_all_docs(
    State(state): State<Arc<AppState>>,
    Path(db): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    Json(payload): Json<Value>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let mut payload_map = convert_payload(payload);
    payload_map.extend(params);

    inner_get_view(
        &create_all_docs_design_view(),
        db,
        state.as_ref(),
        payload_map,
    )
    .await
}

fn convert_payload(payload: Value) -> HashMap<String, String> {
    match payload.as_object() {
        Some(object) => object
            .iter()
            .map(|(k, v)| match v {
                Value::String(s) => (k.clone(), s.clone()),
                _ => (k.clone(), v.to_string()),
            })
            .collect(),
        None => HashMap::new(),
    }
}

/// extract_key_json takes a string and attempts to parse it as JSON. If it's not valid JSON, it
/// will return a single element array with the string as the only element. If it is valid JSON,
/// it will return the parsed JSON as a Vec<Value>.
fn extract_key_json(key: Option<String>) -> Vec<Value> {
    match key {
        Some(key) => match serde_json::from_str::<Value>(key.as_str()) {
            Ok(value) => match value {
                Value::Array(value) => value,
                _ => vec![value],
            },
            Err(_) => vec![Value::String(key)],
        },
        None => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DesignMapping;
    use crate::db::*;
    use assert_json_diff::assert_json_eq;
    use bson::doc;
    use hyper::body::to_bytes;
    use maplit::hashmap;
    use reqwest::StatusCode;

    #[tokio::test]
    async fn test_get_item_basic() {
        let mut mock = MockDatabase::new();

        mock.expect_find_one().returning(|_, _| {
            Box::pin(async { Ok(Some(doc! { "_id": "test_item", "_rev": "test_rev" })) })
        });

        let app_state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

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

        let app_state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

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

        let app_state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

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

        let app_state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

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

    #[test]
    fn test_extract_view_from_views_none_views() {
        let mock = MockDatabase::new();

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: None,
            updates_folder: None,
            couchdb_details: None,
        });

        let result = extract_view_from_views(&state, "db", "design", "view");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_view_from_views_no_database() {
        let mock = MockDatabase::new();

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: Some(HashMap::new()),
            updates_folder: None,
            couchdb_details: None,
        });

        let result = extract_view_from_views(&state, "db", "design", "view");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_view_from_views_no_design() {
        let mock = MockDatabase::new();

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: Some(hashmap! {
                "db".into() => DesignMapping { view_groups: HashMap::new() }
            }),
            updates_folder: None,
            couchdb_details: None,
        });

        let result = extract_view_from_views(&state, "db", "design", "view");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_view_from_views_no_view() {
        let mock = MockDatabase::new();

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: Some(hashmap! {
                "db".into() => DesignMapping { view_groups: hashmap! {
                    "design".into() => HashMap::new()
                } }
            }),
            updates_folder: None,
            couchdb_details: None,
        });

        let result = extract_view_from_views(&state, "db", "design", "view");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_view_from_views_success() {
        let design_view = DesignView {
            match_fields: vec![],
            sort_fields: None,
            aggregation: vec![],
            key_fields: vec![],
            value_fields: vec![],
            filter_insert_index: 0,
            reduce: None,
            single_item_key_is_list: false,
            single_item_value_is_dict: false,
            break_glass_js_script: None,
            omit_null_keys_in_value: false,
        };

        let mock = MockDatabase::new();

        let state = Arc::new(AppState {
            db: Box::new(mock),
            views: Some(hashmap! {
                "db".into() => DesignMapping { view_groups: hashmap! {
                    "design".into() => hashmap! {
                        "view".into() => design_view.clone()
                    }
                } }
            }),
            updates_folder: None,
            couchdb_details: None,
        });

        let result = extract_view_from_views(&state, "db", "design", "view");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), &design_view);
    }

    #[test]
    fn test_extract_key_json_none() {
        let result = extract_key_json(None);
        assert!(result.is_empty());
    }

    #[test]
    fn test_extract_key_json_not_json() {
        let result = extract_key_json(Some("not_json".into()));
        assert_eq!(result, vec![Value::String("not_json".into())]);
    }

    #[test]
    fn test_extract_key_json_json_not_array() {
        let result = extract_key_json(Some("\"valid_json\"".into()));
        assert_eq!(result, vec![Value::String("valid_json".into())]);
    }

    #[test]
    fn test_extract_key_json_json_array() {
        let result = extract_key_json(Some("[\"value1\", \"value2\"]".into()));
        assert_eq!(
            result,
            vec![
                Value::String("value1".into()),
                Value::String("value2".into())
            ]
        );
    }

    #[test]
    fn test_convert_payload_object_string_values() {
        let payload = json!({ "key1": "value1", "key2": "value2" });
        let expected = hashmap! {
            "key1".to_string() => "value1".to_string(),
            "key2".to_string() => "value2".to_string()
        };

        let result = convert_payload(payload);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_convert_payload_object_non_string_values() {
        let payload = json!({ "key1": 123, "key2": true });
        let expected = hashmap! {
            "key1".to_string() => "123".to_string(),
            "key2".to_string() => "true".to_string()
        };

        let result = convert_payload(payload);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_convert_payload_non_object() {
        let payload = json!("just a string");
        let expected = HashMap::new();

        let result = convert_payload(payload);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_get_param() {
        let mut params = HashMap::new();
        params.insert("key1".to_string(), "value1".to_string());
        params.insert("key2".to_string(), "value2".to_string());

        // Test with primary key present
        let value = get_param(&params, "key1", "key3");
        assert_eq!(value, Some("value1".to_string()));

        // Test with only fallback key present
        let value = get_param(&params, "key3", "key2");
        assert_eq!(value, Some("value2".to_string()));

        // Test with neither keys present
        let value = get_param(&params, "key3", "key4");
        assert_eq!(value, None);
    }

    #[test]
    fn test_extract_view_options_from_params() {
        let mut params = HashMap::new();
        params.insert("key".to_string(), "[1, 2]".to_string());

        let check = vec![json!(vec![1, 2])];

        let result = extract_view_options_from_params(params);
        assert_eq!(result.keys, check);

        let mut params = HashMap::new();
        params.insert("keys".to_string(), "[1]".to_string());

        let check = vec![json!(1)];

        let result = extract_view_options_from_params(params);
        assert_eq!(result.keys, check);
    }

    #[test]
    fn test_create_filter_no_keys() {
        let design_view = DesignView {
            match_fields: vec!["field1".to_string(), "field2".to_string()],
            sort_fields: None,
            aggregation: vec![],
            key_fields: vec![],
            value_fields: vec![],
            filter_insert_index: 0,
            reduce: None,
            single_item_key_is_list: false,
            single_item_value_is_dict: false,
            break_glass_js_script: None,
            omit_null_keys_in_value: false,
        };

        let keys = vec![];

        let start_key = vec![json!("start1"), json!("start2")];

        let end_key = vec![json!("end1"), json!("end2")];

        let result = create_filter(
            &design_view,
            &keys,
            &start_key,
            &end_key,
            &None,
            &None,
            false,
        );

        let expected = doc! {
            "field1": {
                "$gte": "start1",
                "$lte": "end1",
            },
            "field2": {
                "$gte": "start2",
                "$lte": "end2",
            }
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_create_filter_with_only_start_key() {
        let design_view = DesignView {
            match_fields: vec!["field1".to_string()],
            sort_fields: None,
            aggregation: vec![],
            key_fields: vec![],
            value_fields: vec![],
            filter_insert_index: 0,
            reduce: None,
            single_item_key_is_list: false,
            single_item_value_is_dict: false,
            break_glass_js_script: None,
            omit_null_keys_in_value: false,
        };

        let keys = vec![];
        let start_key = vec![json!("start1")];
        let end_key = vec![];

        let result = create_filter(
            &design_view,
            &keys,
            &start_key,
            &end_key,
            &None,
            &None,
            false,
        );

        let expected = doc! {
            "field1": {
                "$gte": "start1",
            }
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_create_filter_no_keys_but_numbers() {
        let design_view = DesignView {
            match_fields: vec!["field1".to_string(), "field2".to_string()],
            sort_fields: None,
            aggregation: vec![],
            key_fields: vec![],
            value_fields: vec![],
            filter_insert_index: 0,
            reduce: None,
            single_item_key_is_list: false,
            single_item_value_is_dict: false,
            break_glass_js_script: None,
            omit_null_keys_in_value: false,
        };

        let keys = vec![];

        let start_key = vec![json!("start1"), json!(1)];

        let end_key = vec![json!("end1"), json!(2)];

        let result = create_filter(
            &design_view,
            &keys,
            &start_key,
            &end_key,
            &None,
            &None,
            false,
        );

        let expected = doc! {
            "field1": {
                "$gte": "start1",
                "$lte": "end1",
            },
            "field2": {
                "$gte": Bson::Int64(1),
                "$lte": Bson::Int64(2),
            }
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_create_filter_with_keys() {
        // NOTE: For some reason create_filter() can return the order of the $and
        // differently. Not sure if this is a BSON issue of not. It ultimately does not matter
        // though. The test should probably reflect this.

        let design_view = DesignView {
            match_fields: vec!["field1".to_string(), "field2".to_string()],
            sort_fields: None,
            aggregation: vec![],
            key_fields: vec![],
            value_fields: vec![],
            filter_insert_index: 0,
            reduce: None,
            single_item_key_is_list: false,
            single_item_value_is_dict: false,
            break_glass_js_script: None,
            omit_null_keys_in_value: false,
        };

        let keys = vec![json![vec![json!("key1"), json!("key2")]]];

        let start_key = vec![];
        let end_key = vec![];

        let result = create_filter(
            &design_view,
            &keys,
            &start_key,
            &end_key,
            &None,
            &None,
            false,
        );

        let expected = doc! {
            "$and": [
                { "$or": [
                    { "$and": [
                        {"field1": "key1", "field2": "key2"}
                    ] }
                ] }
            ]
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_create_filter_with_single_key() {
        // NOTE: For some reason create_filter() can return the order of the $and
        // differently. Not sure if this is a BSON issue of not. It ultimately does not matter
        // though. The test should probably reflect this.

        let design_view = DesignView {
            match_fields: vec!["field1".to_string()],
            sort_fields: None,
            aggregation: vec![],
            key_fields: vec![],
            value_fields: vec![],
            filter_insert_index: 0,
            reduce: None,
            single_item_key_is_list: false,
            single_item_value_is_dict: false,
            break_glass_js_script: None,
            omit_null_keys_in_value: false,
        };

        let keys = vec![json!("key1"), json!("key2")];

        let start_key = vec![];
        let end_key = vec![];

        let result = create_filter(
            &design_view,
            &keys,
            &start_key,
            &end_key,
            &None,
            &None,
            false,
        );

        let expected = doc! {
            "$and": [
                {
                    "$or": [
                        {
                            "$and": [
                                {
                                    "field1": "key1"
                                }
                            ]
                        },
                        {
                            "$and": [
                                {
                                    "field1": "key2"
                                }
                            ]
                        }
                    ]
                }
            ]
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_create_filter_with_single_key_int() {
        let design_view = DesignView {
            match_fields: vec!["field1".to_string()],
            sort_fields: None,
            aggregation: vec![],
            key_fields: vec![],
            value_fields: vec![],
            filter_insert_index: 0,
            reduce: None,
            single_item_key_is_list: false,
            single_item_value_is_dict: false,
            break_glass_js_script: None,
            omit_null_keys_in_value: false,
        };

        let keys = vec![json!(1), json!(2)];

        let mut filter: Document = doc! {};

        map_keys(&design_view, &keys, &mut filter);

        let expected = doc! {
            "$and": [ {
                "$or": [
                    { "$and": [
                        {
                        "field1": Bson::Int64(1)
                        }
                    ] },
                    { "$and": [
                        {
                        "field1": Bson::Int64(2)
                        }
                    ] }
                ]

            } ]
        };

        assert_eq!(filter, expected);
    }

    #[test]
    fn test_create_filter_with_dual_key() {
        let design_view = DesignView {
            match_fields: vec!["field1".to_string(), "field2".to_string()],
            sort_fields: None,
            aggregation: vec![],
            key_fields: vec![],
            value_fields: vec![],
            filter_insert_index: 0,
            reduce: None,
            single_item_key_is_list: false,
            single_item_value_is_dict: false,
            break_glass_js_script: None,
            omit_null_keys_in_value: false,
        };

        let key = vec![json!(1), json!(2)];
        let keys = vec![json!(key)];

        let mut filter: Document = doc! {};

        map_keys(&design_view, &keys, &mut filter);

        let expected = doc! {
            "$and": [ {
                "$or": [
                    { "$and": [
                        {
                            "field1": Bson::Int64(1),
                            "field2": Bson::Int64(2)
                        }
                    ] }
                ]
            } ]
        };

        assert_eq!(filter, expected);
    }

    #[test]
    fn test_create_filter_partial_key() {
        let design_view = DesignView {
            match_fields: vec!["field1".to_string(), "field2".to_string()],
            sort_fields: None,
            aggregation: vec![],
            key_fields: vec![],
            value_fields: vec![],
            filter_insert_index: 0,
            reduce: None,
            single_item_key_is_list: false,
            single_item_value_is_dict: false,
            break_glass_js_script: None,
            omit_null_keys_in_value: false,
        };

        let keys = vec![];

        let start_key = vec![json!("start1"), json!("start2")];

        let end_key = vec![json!("end1"), json!({})];

        let result = create_filter(
            &design_view,
            &keys,
            &start_key,
            &end_key,
            &None,
            &None,
            false,
        );

        let expected = doc! {
            "field1": {
                "$gte": "start1",
                "$lte": "end1",
            },
            "field2": {
                "$gte": "start2",
            },
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_invalid_json_in_aggregation() {
        let design_view = DesignView {
            match_fields: vec!["field1".to_string(), "field2".to_string()],
            sort_fields: None,
            aggregation: vec!["{".to_string()],
            key_fields: vec![],
            value_fields: vec![],
            filter_insert_index: 0,
            reduce: None,
            single_item_key_is_list: false,
            single_item_value_is_dict: false,
            break_glass_js_script: None,
            omit_null_keys_in_value: false,
        };

        let v = extract_pipeline_bson(&design_view, false, 0);
        assert!(v.is_err());
    }

    #[test]
    fn test_valid_json_in_aggregation() {
        let design_view = DesignView {
            match_fields: vec!["field1".to_string(), "field2".to_string()],
            sort_fields: None,
            aggregation: vec!["{}".to_string()],
            key_fields: vec![],
            value_fields: vec![],
            filter_insert_index: 0,
            reduce: None,
            single_item_key_is_list: false,
            single_item_value_is_dict: false,
            break_glass_js_script: None,
            omit_null_keys_in_value: false,
        };

        let v = extract_pipeline_bson(&design_view, false, 0);
        assert!(v.is_ok());
        assert_eq!(v.unwrap().len(), 1);
    }
}

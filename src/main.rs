mod common;
mod config;

use crate::common::{
    add_if_match,
    add_if_none_match,
    add_server_header,
    always_add_must_revalidate,
    not_implemented_handler,
    IfMatch,
    IfNoneMatch,
};
use crate::config::Settings;
use axum::extract::{Json, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{middleware, Extension, Router};
use bson::Document;
use clap::{command, Parser};
use mongodb::options::{DeleteOptions, ReplaceOptions};
use mongodb::Collection;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;
use uuid::Uuid;

struct AppState {
    db: mongodb::Database,
}

#[derive(Parser, Debug)]
#[command(author = None, version = None, about = "CouchDB to MongoDB Streamer", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let config_file = args.config;

    let s = Settings::new(Some(config_file.to_string()));
    match s {
        Ok(_) => {}
        Err(e) => {
            panic!("unable to load config: {}", e);
        }
    }

    let unwrapped_settings = s.unwrap();
    unwrapped_settings.configure_logging();

    let db = unwrapped_settings
        .get_mongodb_database()
        .await
        .expect("unable to connect to mongodb");
    let s = Arc::new(AppState { db });

    let app = Router::new()
        .route("/:db/_:view", get(not_implemented_handler))

        // Get a document
        .route("/:db/:item", get(get_item)
            .put(new_item_with_id).delete(delete_item))

        // Post a document without the ID (usually it's in the document or we
        // generate it)
        .route("/:db", post(new_item))

        .route_layer(middleware::from_fn(add_if_none_match))
        .route_layer(middleware::from_fn(add_if_match))

        // This magic sets up logging to look like normal request logging.
        .layer(TraceLayer::new_for_http()
            .make_span_with(DefaultMakeSpan::new()
                .level(Level::INFO))
            .on_response(DefaultOnResponse::new()
                .level(Level::INFO)))

        // Add standard headers.
        .layer(middleware::from_fn(always_add_must_revalidate))
        .layer(middleware::from_fn(add_server_header))

        // Add state
        .with_state(s);

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

async fn get_item(
    Extension(IfNoneMatch(if_none_match)): Extension<IfNoneMatch>,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    Path((db, item)): Path<(String, String)>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let collection = state.db.collection::<Document>(db.as_str());
    let document_id = bson::doc! { "_id": item };

    let document = match collection.find_one(document_id, None).await {
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

async fn new_item(
    Extension(IfMatch(if_match)): Extension<IfMatch>,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    Path(db): Path<String>,
    Json(payload): Json<Value>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    inner_new_item(db, None, state, params, payload, if_match).await
}

async fn new_item_with_id(
    Extension(IfMatch(if_match)): Extension<IfMatch>,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    Path((db, item)): Path<(String, String)>,
    Json(payload): Json<Value>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    inner_new_item(db, Some(item), state, params, payload, if_match).await
}

async fn inner_new_item(
    db: String,
    item: Option<String>,
    state: Arc<AppState>,
    _params: HashMap<String, String>,
    payload: Value,
    rev_if_match: Option<String>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let collection = state.db.collection::<Document>(db.as_str());

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
    match collection
        .replace_one(filter, new_bson_document.clone(), options)
        .await
    {
        Ok(_) => (),
        Err(_) => {
            // Check for the conflict to return the right error message
            return match check_conflict(collection.clone(), &id).await {
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

/// check_conflict checks to see if the document exists and if it does, returns a 409
/// conflict error.
async fn check_conflict(
    collection: Collection<Document>,
    id: &str,
) -> Result<(StatusCode, Json<Value>), Box<dyn Error>> {
    // Grab the document to determine if it exists or not
    let document = match collection.find_one(bson::doc! { "_id": id }, None).await {
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

async fn delete_item(
    Extension(IfMatch(if_match)): Extension<IfMatch>,
    State(state): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
    Path((db, item)): Path<(String, String)>,
) -> Result<Response, (StatusCode, Json<Value>)> {
    let existing_rev = match params.get("rev") {
        Some(rev) => Some(rev.to_string()),
        None => if_match,
    };

    if existing_rev.is_none() {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            Json(json!({"error": "missing rev"})),
        ));
    }

    let collection = state.db.collection::<Document>(db.as_str());
    let filter = bson::doc! { "_id": item.clone(), "_rev": existing_rev.clone() };
    let options = DeleteOptions::builder().build();
    match collection.delete_one(filter, options).await {
        Ok(_) => (),
        Err(_) => {
            return match check_conflict(collection.clone(), &item).await {
                Ok((status, json)) => Err((status, json)),
                Err(e) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal server error", "details": e.to_string()})),
                )),
            }
        }
    };

    Ok(Json(json!({"ok": true, "id": item, "rev": existing_rev.unwrap()})).into_response())
}

mod common;
mod config;
mod db;
mod ops;
mod state;

use crate::common::{
    add_if_match,
    add_if_none_match,
    add_server_header,
    always_add_must_revalidate,
};
use crate::config::Settings;
use crate::db::MongoDB;
use crate::ops::create_update::{new_item, new_item_with_id};
use crate::ops::delete::delete_item;
use crate::ops::get::{all_docs, get_item, get_view, post_all_docs, post_get_view};
use crate::ops::update::{execute_update_script, execute_update_script_with_doc};
use crate::state::AppState;
use axum::extract::{Json, Path, State};
use axum::routing::{get, post, put};
use axum::{middleware, Router};
use clap::{command, Parser};
use serde_json::{json, Value};
use std::error::Error;
use std::sync::Arc;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

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

    let settings = Settings::new(Some(config_file.to_string()));
    match settings {
        Ok(_) => {}
        Err(e) => {
            panic!("unable to load config: {}", e);
        }
    }

    // TODO(lee) make this not mutable... it's just easier while it's late at night
    let mut unwrapped_settings = settings.unwrap();
    unwrapped_settings.configure_logging();
    unwrapped_settings.maybe_add_views_from_files();

    let db = unwrapped_settings
        .get_mongodb_database()
        .await
        .expect("unable to connect to mongodb");
    let state = Arc::new(AppState {
        db: Box::new(MongoDB { db }),
        views: unwrapped_settings.views,
        updates_folder: unwrapped_settings.updates_folder,
    });

    let app = Router::new()
        .route("/:db/_design/:design/_view/:view", post(post_get_view).get(get_view))
        .route("/:db/_design/:design/_update/:function", put(execute_update_script))
        .route("/:db/_design/:design/_update/:function/:document_id", put(execute_update_script_with_doc))
        .route("/:db/_all_docs", post(post_all_docs).get(all_docs))

        // Get a document
        .route("/:db/:item", get(get_item)
            .put(new_item_with_id).delete(delete_item))

        // Post a document without the ID (usually it's in the document or we
        // generate it)
        .route("/:db", post(new_item).get(db_info))
        .route("/", get(server_info))

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
        .with_state(state);

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

async fn server_info(State(state): State<Arc<AppState>>) -> Json<Value> {
    let version_info = match state.db.get_version().await {
        Ok(v) => json!(v),
        Err(_) => {
            json!({
                "error": "unable to get version info"
            })
        }
    };

    // Return a fake amount of data so that libraries like pycouchdb can work
    Json(json!({
        "couchdb": "FakeCouchDB",
        "version": "3.1.1",
        "git_sha": "ce596c0ea",
        "uuid": "a7a9d4c9-6f4c-4f0c-8b1e-9c4e2d9e7e4a",
        "features": [
            "access-ready",
            "partitioned",
            "pluggable-storage-engines",
            "reshard",
            "scheduler"
        ],
        "vendor": {
            "name": "Green Man Gaming"
        },
        "mongo_details": version_info,
    }))
}

async fn db_info(Path(db): Path<String>) -> Json<Value> {
    Json(json!({
        "db_name": db,
        "doc_count": 0,
        "doc_del_count": 0,
        "update_seq": 0,
        "purge_seq": 0,
        "compact_running": false,
        "disk_size": 0,
        "data_size": 0,
        "instance_start_time": "0"
    }))
}

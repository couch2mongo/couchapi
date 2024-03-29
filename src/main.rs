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

// libcurl crashes on Sonoma unless CoreServices is linked in
// See https://github.com/nabijaczleweli/cargo-update/issues/240 and https://github.com/mitsuhiko/rye/issues/428
#[cfg_attr(target_os = "macos", link(name = "CoreServices", kind = "framework"))]
extern "C" {}

mod common;
mod config;
mod couchdb;
mod db;
mod metrics;
mod ops;
mod state;

use crate::common::{
    add_content_type_if_needed,
    add_if_match,
    add_if_none_match,
    add_server_header,
    always_add_must_revalidate,
    log_response_if_error,
    print_request_response,
};
use crate::config::Settings;
use crate::db::MongoDB;
use crate::ops::bulk::bulk_docs;
use crate::ops::create_update::{new_item, new_item_with_id};
use crate::ops::delete::delete_item;
use crate::ops::get::{
    all_docs,
    get_item,
    get_view,
    post_all_docs,
    post_get_view,
    post_multi_query,
};
use crate::ops::update::{execute_update_script, execute_update_script_with_doc};
use crate::ops::JsonWithStatusCodeResponse;
use crate::state::AppState;
use axum::body::Body;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::ServiceExt;
use axum::{middleware, Router};
use clap::{command, Parser};
use serde_json::{json, Value};
use std::error::Error;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::normalize_path::{NormalizePath, NormalizePathLayer};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tower_layer::Layer;
use tracing::{instrument, warn, Level};

#[derive(Parser, Debug)]
#[command(author = None, version = None, about = "CouchDB Emulation API for MongoDB", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[instrument]
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

    if let Some(couchdb_present) = &unwrapped_settings.couchdb_settings {
        warn!(
            read_only = couchdb_present.read_only,
            read_through = couchdb_present.read_through,
            "CouchDB settings present, so some functionality will differ"
        );

        if let Some(ro_databases) = &couchdb_present.read_only_databases {
            warn!(
                databases = ro_databases.join(", "),
                "Read-only databases configured"
            );
        }

        if let Some(rt_databases) = &couchdb_present.read_through_databases {
            warn!(
                databases = rt_databases.join(", "),
                "Read-through databases configured"
            );
        }

        if let Some(mappings) = &couchdb_present.mappings {
            for (k, v) in mappings {
                warn!(couchdb = k, mongodb = v, "Mapping");
            }
        }
    }

    let db = unwrapped_settings
        .get_mongodb_database()
        .await
        .expect("unable to connect to mongodb");

    let state = Arc::new(AppState {
        db: Box::new(MongoDB { db }),
        views: unwrapped_settings.views,
        updates_folder: unwrapped_settings.updates_folder,
        couchdb_details: unwrapped_settings.couchdb_settings,
    });

    metrics_prometheus::install();

    let mut router = Router::new()
        .route("/:db/_design/:design/_view/:view",
               post(post_get_view)
                   .get(get_view)
                   .layer(middleware::from_fn(metrics::add_view_metrics))
        )
        .route("/:db/_design/:design/_view/:view/queries",
               post(post_multi_query)
                   .layer(middleware::from_fn(metrics::add_view_metrics))
        )

        .route("/:db/_design/:design/_update/:function",
               put(execute_update_script)
                   .post(execute_update_script)
                   .layer(middleware::from_fn(metrics::add_update_metrics))
        )
        .route("/:db/_design/:design/_update/:function/:document_id",
               put(execute_update_script_with_doc)
                   .post(execute_update_script_with_doc)
                   .layer(middleware::from_fn(metrics::add_update_metrics))
        )

        .route("/:db/_bulk_docs", post(bulk_docs))
        .route("/:db/_all_docs", post(post_all_docs).get(all_docs))

        // Get a document
        .route("/:db/:item", get(get_item)
            .put(new_item_with_id).delete(delete_item))

        // Post a document without the ID (usually it's in the document or we
        // generate it)
        .route("/:db", post(new_item).get(db_info))

        .layer(middleware::from_fn(metrics::add_table_metrics))

        .route("/metrics", get(metrics::collect_metrics))
        .route("/", get(server_info))

        .route_layer(middleware::from_fn(add_if_none_match))
        .route_layer(middleware::from_fn(add_if_match))

        .layer(RequestDecompressionLayer::new())

        // This magic sets up logging to look like normal request logging.
        .layer(TraceLayer::new_for_http()
            .make_span_with(DefaultMakeSpan::new()
                .level(Level::INFO))
            .on_response(DefaultOnResponse::new()
                .level(Level::INFO)))

        .layer(middleware::from_fn(add_content_type_if_needed))

        // Add standard headers.
        .layer(middleware::from_fn(always_add_must_revalidate))
        .layer(middleware::from_fn(add_server_header))

        .layer(middleware::from_fn(log_response_if_error));

    if unwrapped_settings.debug_requests {
        router = router.layer(middleware::from_fn(print_request_response));
    }

    let app = NormalizePathLayer::trim_trailing_slash().layer(router.with_state(state));

    let listener = TcpListener::bind(&unwrapped_settings.listen_address)
        .await
        .unwrap();
    axum::serve(
        listener,
        <NormalizePath<Router> as ServiceExt<hyper::Request<Body>>>::into_make_service(app),
    )
    .await
    .unwrap();

    Ok(())
}

async fn server_info(
    State(state): State<Arc<AppState>>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    let version_result = state.db.get_version().await;

    // Handle the results for the first task
    let version_info = version_result
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })
        .map(|v| json!(v))?;

    // Return a fake amount of data so that libraries like pycouchdb can work
    Ok(Json(json!({
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
    .into_response())
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

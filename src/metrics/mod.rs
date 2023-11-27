use axum::extract::Path;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use prometheus::{Encoder, TextEncoder};
use std::time::Instant;

pub async fn add_table_metrics<B>(
    Path((db,)): Path<(String,)>,
    req: Request<B>,
    next: Next<B>,
) -> Response {
    let labels = [("db", db)];
    metrics::increment_counter!("couchapi_table_operations_total", &labels);
    next.run(req).await
}

pub async fn add_view_metrics<B>(
    Path((db, design, view)): Path<(String, String, String)>,
    req: Request<B>,
    next: Next<B>,
) -> Response {
    let start = Instant::now();
    let method = req.method().clone();

    let res = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();
    let status = res.status().as_u16().to_string();
    let labels = [
        ("method", method.to_string()),
        ("db", db),
        ("design", design),
        ("view", view),
        ("status", status),
    ];

    metrics::increment_counter!("couchapi_table_view_operations_total", &labels);
    metrics::histogram!(
        "couchapi_table_view_operations_duration_seconds",
        latency,
        &labels,
    );

    res
}

pub async fn add_update_metrics<B>(
    Path((db, design, function)): Path<(String, String, String)>,
    req: Request<B>,
    next: Next<B>,
) -> Response {
    let start = Instant::now();
    let method = req.method().clone();

    let res = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();
    let status = res.status().as_u16().to_string();
    let labels = [
        ("method", method.to_string()),
        ("db", db),
        ("design", design),
        ("function", function),
        ("status", status),
    ];

    metrics::increment_counter!("couchapi_table_update_function_operations_total", &labels);
    metrics::histogram!(
        "couchapi_table_update_function_operations_duration_seconds",
        latency,
        &labels,
    );

    res
}

pub async fn collect_metrics() -> String {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();

    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    String::from_utf8(buffer).unwrap()
}

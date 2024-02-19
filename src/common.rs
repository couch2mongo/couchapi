use axum::body::Body;
use axum::extract;
use axum::http;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http_body_util::BodyExt;
use tracing::warn;

/// Common middleware for all requests.

/// not_implemented_handler is a placeholder for unimplemented routes.
#[allow(dead_code)]
pub async fn not_implemented_handler<B>(_: Request<B>) -> Response {
    let mut r: Response = Response::default();
    *r.status_mut() = StatusCode::NOT_IMPLEMENTED;
    r
}

/// Add a `must-revalidate` Cache-Control header to every response.
/// ref: https://docs.couchdb.org/en/stable/api/basics.html#response-headers
pub async fn always_add_must_revalidate(req: Request<Body>, next: Next) -> Response {
    let mut res = next.run(req).await;
    res.headers_mut()
        .insert("Cache-Control", "must-revalidate".parse().unwrap());
    res
}

/// Add a `Server` header to every response.
pub async fn add_server_header(req: Request<Body>, next: Next) -> Response {
    let mut res = next.run(req).await;
    res.headers_mut().insert(
        "Server",
        "CouchDB to MongoDB Emulator Proxy".parse().unwrap(),
    );
    res
}

async fn buffer_and_log<B>(body: B) -> Result<Bytes, (StatusCode, String)>
where
    B: axum::body::HttpBody<Data = Bytes>,
    B::Error: std::fmt::Display,
{
    let bytes = match BodyExt::collect(body).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("failed to read body: {}", err),
            ));
        }
    };

    let collected_bytes = bytes.to_bytes();

    if let Ok(body) = std::str::from_utf8(&collected_bytes) {
        if !body.is_empty() {
            warn!(body = body, "response log");
        }
    }

    Ok(collected_bytes)
}

pub async fn log_response_if_error(req: Request<Body>, next: Next) -> Response {
    let res = next.run(req).await;

    if res.status().is_server_error() {
        let (res_parts, res_body) = res.into_parts();

        // Print response
        let bytes = buffer_and_log(res_body).await;
        let res = Response::from_parts(res_parts, Body::from(bytes.unwrap()));

        return res.into_response();
    }

    res
}

#[derive(Clone)]
pub struct IfNoneMatch(pub Option<String>);

/// Extract the `If-None-Match` header from the request and store it in the request extensions.
pub async fn add_if_none_match(mut req: Request<Body>, next: Next) -> Result<Response, StatusCode> {
    // We deal with borrowing first as it's just easier - I promise you. It's because otherwise
    // we hit https://doc.rust-lang.org/error_codes/E0502.html.
    let headers = req.headers().clone();
    let extensions_mut = req.extensions_mut();

    let if_none_match = headers
        .get(http::header::IF_NONE_MATCH)
        .and_then(|h| h.to_str().ok());

    extensions_mut.insert(IfNoneMatch(if_none_match.map(|s| s.to_string())));

    Ok(next.run(req).await)
}

#[derive(Clone)]
pub struct IfMatch(pub Option<String>);

/// Extract the `If-Match` header from the request and store it in the request extensions.
pub async fn add_if_match(mut req: Request<Body>, next: Next) -> Result<Response, StatusCode> {
    // We deal with borrowing first as it's just easier - I promise you. It's because otherwise
    // we hit https://doc.rust-lang.org/error_codes/E0502.html.
    let headers = req.headers().clone();
    let extensions_mut = req.extensions_mut();

    let if_match = headers
        .get(http::header::IF_MATCH)
        .and_then(|h| h.to_str().ok());

    extensions_mut.insert(IfMatch(if_match.map(|s| s.to_string())));

    Ok(next.run(req).await)
}

pub async fn add_content_type_if_needed(
    mut req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let headers = req.headers_mut();
    let empty_existing = http::HeaderValue::from_static("");

    if !headers.contains_key(http::header::CONTENT_TYPE)
        || headers[http::header::CONTENT_TYPE] != "application/json"
    {
        let existing = headers
            .get(http::header::CONTENT_TYPE)
            .unwrap_or(&empty_existing);
        tracing::debug!(existing = existing.to_str().unwrap(), "Adding content type");
        headers.insert(
            http::header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );
    }

    Ok(next.run(req).await)
}

pub async fn print_request_response(
    req: extract::Request,
    next: Next,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (parts, body) = req.into_parts();

    // Convert headers to JSON and print to log
    let headers = parts.clone().headers;
    let mut headers_map = serde_json::Map::new();
    for (key, value) in headers.iter() {
        let key = key.as_str().to_string();
        let value = value.to_str().unwrap().to_string();
        headers_map.insert(key, serde_json::Value::String(value));
    }
    let headers_json = serde_json::Value::Object(headers_map);
    tracing::debug!(headers = headers_json.to_string(), "request headers");

    let bytes = buffer_and_print("request", body).await?;
    let req = Request::from_parts(parts, Body::from(bytes));

    let res = next.run(req).await;

    let (parts, body) = res.into_parts();
    let bytes = buffer_and_print("response", body).await?;
    let res = Response::from_parts(parts, Body::from(bytes));

    Ok(res)
}

async fn buffer_and_print<B>(direction: &str, body: B) -> Result<Bytes, (StatusCode, String)>
where
    B: axum::body::HttpBody<Data = Bytes>,
    B::Error: std::fmt::Display,
{
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("failed to read {direction} body: {err}"),
            ));
        }
    };

    if let Ok(body) = std::str::from_utf8(&bytes) {
        tracing::debug!("{direction} body = {body:?}");
    }

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use axum::{middleware, Extension, Router};
    use reqwest::header::HeaderValue;
    use tokio::net::TcpListener;

    // Basic handler for testing
    async fn handler() -> &'static str {
        "OK"
    }

    #[tokio::test]
    async fn test_not_implemented_handler() {
        let req: Request<Body> = Request::default();
        let res = not_implemented_handler(req).await;
        assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn test_always_add_must_revalidate() {
        let app = Router::new()
            .route("/", get(handler))
            .layer(middleware::from_fn(always_add_must_revalidate));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        let res = client.get(format!("http://{}", addr)).send().await.unwrap();
        assert_eq!(res.headers()["Cache-Control"], "must-revalidate");
    }

    // Test add_server_header
    #[tokio::test]
    async fn test_add_server_header() {
        let app = Router::new()
            .route("/", get(handler))
            .layer(middleware::from_fn(add_server_header));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        let res = client.get(format!("http://{}", addr)).send().await.unwrap();
        assert_eq!(
            res.headers()["Server"],
            HeaderValue::from_static("CouchDB to MongoDB Emulator Proxy")
        );
    }

    async fn if_none_match_handler(Extension(if_none_match): Extension<IfNoneMatch>) -> String {
        if_none_match.0.unwrap_or_default()
    }

    #[tokio::test]
    async fn test_add_if_none_match() {
        let app = Router::new()
            .route("/", get(if_none_match_handler))
            .route_layer(middleware::from_fn(add_if_none_match));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        let res = client
            .get(format!("http://{}", addr))
            .header("If-None-Match", "\"12345\"")
            .send()
            .await
            .unwrap();

        let text = res.text().await.unwrap();
        assert_eq!(text, "\"12345\"");
    }

    async fn if_match_handler(Extension(if_match): Extension<IfMatch>) -> String {
        if_match.0.unwrap_or_default()
    }

    #[tokio::test]
    async fn test_add_if_match() {
        let app = Router::new()
            .route("/", get(if_match_handler))
            .route_layer(middleware::from_fn(add_if_match));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        let res = client
            .get(format!("http://{}", addr))
            .header("If-Match", "\"12345\"")
            .send()
            .await
            .unwrap();

        let text = res.text().await.unwrap();
        assert_eq!(text, "\"12345\"");
    }
}

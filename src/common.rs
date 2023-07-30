use axum::http;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

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
pub async fn always_add_must_revalidate<B>(req: Request<B>, next: Next<B>) -> Response {
    let mut res = next.run(req).await;
    res.headers_mut()
        .insert("Cache-Control", "must-revalidate".parse().unwrap());
    res
}

/// Add a `Server` header to every response.
pub async fn add_server_header<B>(req: Request<B>, next: Next<B>) -> Response {
    let mut res = next.run(req).await;
    res.headers_mut().insert(
        "Server",
        "CouchDB to MongoDB Emulator Proxy".parse().unwrap(),
    );
    res
}

#[derive(Clone)]
pub struct IfNoneMatch(pub Option<String>);

/// Extract the `If-None-Match` header from the request and store it in the request extensions.
pub async fn add_if_none_match<B>(
    mut req: Request<B>,
    next: Next<B>,
) -> Result<Response, StatusCode> {
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
pub async fn add_if_match<B>(mut req: Request<B>, next: Next<B>) -> Result<Response, StatusCode> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use axum::{middleware, Extension, Router, Server};
    use http::header::HeaderValue;
    use std::net::TcpListener;

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
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async {
            Server::from_tcp(listener)
                .unwrap()
                .serve(app.into_make_service())
                .await
                .unwrap()
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
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async {
            Server::from_tcp(listener)
                .unwrap()
                .serve(app.into_make_service())
                .await
                .unwrap()
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

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async {
            Server::from_tcp(listener)
                .unwrap()
                .serve(app.into_make_service())
                .await
                .unwrap()
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

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async {
            Server::from_tcp(listener)
                .unwrap()
                .serve(app.into_make_service())
                .await
                .unwrap()
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

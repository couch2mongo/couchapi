use axum::http;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

/// Common middleware for all requests.

/// not_implemented_handler is a placeholder for unimplemented routes.
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

use crate::config::CouchDb;
use crate::ops::JsonWithStatusCodeResponse;
use axum::response::{IntoResponse, Response};
use axum::Json;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::{info, instrument, warn};
use url::Url;

#[instrument]
pub async fn read_through(
    couchdb_details: &CouchDb,
    method: Method,
    json_payload: Option<&Value>,
    path: &str,
    params: &HashMap<String, String>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    warn!(path = path, "read_through required");

    let mut url = Url::parse(&couchdb_details.url).unwrap();
    url.set_path(path);

    inner_couch(
        method,
        json_payload,
        &url,
        params,
        maybe_auth(couchdb_details),
    )
    .await
}

fn maybe_auth(couchdb_details: &CouchDb) -> Option<(&str, &str)> {
    if let (Some(username), Some(password)) = (&couchdb_details.username, &couchdb_details.password)
    {
        return Some((username, password));
    }

    None
}

#[instrument]
async fn inner_couch(
    method: Method,
    json_payload: Option<&Value>,
    url: &Url,
    params: &HashMap<String, String>,
    auth_details: Option<(&str, &str)>,
) -> Result<Response, JsonWithStatusCodeResponse> {
    // We do this as a warning as we want to know this happened
    warn!(url = url.to_string(), "inner_couch");

    let client = reqwest::Client::new();
    let mut req = client.request(method, url.clone()).query(params);

    if auth_details.is_some() {
        let (username, password) = auth_details.unwrap();
        req = req.basic_auth(username, Some(password));
    }

    if let Some(json_payload) = json_payload {
        req = req.json(json_payload);
    }

    // Try and send the request
    let result = req.send().await.map_err(|e| {
        (
            hyper::StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal server error", "details": e.to_string()})),
        )
    })?;

    // Now try and build the response
    let header_map = result.headers().clone();
    let status_code = hyper::StatusCode::from_u16(result.status().as_u16()).unwrap();
    let b = result
        .bytes()
        .await
        .map_err(|e| {
            (
                hyper::StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal server error", "details": e.to_string()})),
            )
        })?
        .clone()
        .to_vec();

    let s = String::from_utf8(b.clone()).map_err(|e| {
        (
            hyper::StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal server error", "details": e.to_string()})),
        )
    })?;

    let mut r = s.into_response();
    *r.status_mut() = status_code;

    header_map.iter().for_each(|(k, v)| {
        if k.as_str().to_ascii_lowercase() == "transfer-encoding" {
            info!("Skipping transfer-encoding header");
            return;
        }

        r.headers_mut().insert(
            hyper::header::HeaderName::from_bytes(k.as_str().as_bytes()).unwrap(),
            hyper::header::HeaderValue::from_str(v.to_str().unwrap()).unwrap(),
        );
    });

    // Add our own special header so we know we did a read-through in the response
    r.headers_mut()
        .insert("X-Fake-CouchDb-Read-Through", "true".parse().unwrap());

    Ok(r)
}

#[instrument]
pub async fn maybe_write(
    couchdb_details: &Option<CouchDb>,
    mongodb_db: &str,
    method: Method,
    json_payload: Option<&Value>,
    path: &str,
    params: &HashMap<String, String>,
) -> Result<Option<Response>, JsonWithStatusCodeResponse> {
    // Just return if there are no couchdb details
    if couchdb_details.is_none() {
        return Ok(None);
    }

    let couchdb_details = couchdb_details.as_ref().unwrap();

    if !couchdb_details.is_read_only(mongodb_db) {
        return Ok(None);
    }

    let mapped_db_name = couchdb_details.map_for_db(mongodb_db);
    let full_path = format!("{}/{}", mapped_db_name, path);

    // Create the URL from the couch config and the path
    let mut url = Url::parse(&couchdb_details.url).unwrap();
    url.set_path(full_path.as_str());

    inner_couch(
        method,
        json_payload,
        &url,
        params,
        maybe_auth(couchdb_details),
    )
    .await
    .map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use httpmock::Method::GET;
    use httpmock::MockServer;
    use hyper::StatusCode;

    #[tokio::test]
    async fn test_inner_couch_success() {
        let server = MockServer::start_async().await;

        let mock = server
            .mock_async(|when, then| {
                when.method(GET).path("/test");
                then.status(200).body("success");
            })
            .await;

        let url = Url::parse(&server.base_url())
            .unwrap()
            .join("/test")
            .unwrap();

        let method = Method::GET;
        let params = HashMap::new();
        let response = inner_couch(method, None, &url, &params, None).await;

        assert!(Result::is_ok(&response));

        let response = response.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body();
        let b = BodyExt::collect(body).await.unwrap().to_bytes();
        let s = String::from_utf8(b.to_vec()).unwrap();
        assert_eq!(s, "success");

        mock.assert_async().await;
    }
}

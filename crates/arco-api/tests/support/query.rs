#![allow(dead_code)]

use anyhow::{Context, Result};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use tower::ServiceExt;

use arco_api::server::ServerBuilder;

pub fn test_router() -> axum::Router {
    ServerBuilder::new().debug(true).build().test_router()
}

pub async fn seed_catalog(router: axum::Router) -> Result<axum::Router> {
    let (status, _): (_, serde_json::Value) = helpers::post_json(
        router.clone(),
        "/api/v1/namespaces",
        serde_json::json!({
            "name": "analytics",
            "description": "Analytics namespace"
        }),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _): (_, serde_json::Value) = helpers::post_json(
        router.clone(),
        "/api/v1/namespaces/analytics/tables",
        serde_json::json!({
            "name": "events",
            "description": "Event stream",
            "columns": [
                {"name": "event_id", "data_type": "STRING", "nullable": false},
                {"name": "event_type", "data_type": "STRING", "nullable": false}
            ]
        }),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _): (_, serde_json::Value) = helpers::post_json(
        router.clone(),
        "/api/v1/lineage/edges",
        serde_json::json!({
            "edges": [
                {
                    "source_id": "table-source-1",
                    "target_id": "table-target-1",
                    "edge_type": "derives_from",
                    "run_id": "run-123"
                }
            ]
        }),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    Ok(router)
}

pub mod helpers {
    use super::*;
    use serde::de::DeserializeOwned;

    pub fn make_request(
        method: Method,
        uri: &str,
        body: Option<serde_json::Value>,
    ) -> Result<Request<Body>> {
        make_request_with_headers(method, uri, body, &[])
    }

    pub fn make_request_with_headers(
        method: Method,
        uri: &str,
        body: Option<serde_json::Value>,
        headers: &[(&str, &str)],
    ) -> Result<Request<Body>> {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("X-Tenant-Id", "test-tenant")
            .header("X-Workspace-Id", "test-workspace")
            .header(header::CONTENT_TYPE, "application/json");

        for (key, value) in headers {
            builder = builder.header(*key, *value);
        }

        let body = match body {
            Some(v) => Body::from(serde_json::to_vec(&v).context("serialize request body")?),
            None => Body::empty(),
        };

        builder.body(body).context("build request")
    }

    async fn send(
        router: axum::Router,
        request: Request<Body>,
    ) -> Result<axum::response::Response> {
        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        Ok(response)
    }

    async fn response_body(
        response: axum::response::Response,
    ) -> Result<(StatusCode, axum::body::Bytes)> {
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .context("read response body")?;
        Ok((status, body))
    }

    pub async fn get_json<T: DeserializeOwned>(
        router: axum::Router,
        uri: &str,
    ) -> Result<(StatusCode, T)> {
        let request = make_request(Method::GET, uri, None)?;
        let response = send(router, request).await?;
        let (status, body) = response_body(response).await?;
        let json = serde_json::from_slice(&body).with_context(|| {
            format!(
                "parse JSON response (status={status}): {}",
                String::from_utf8_lossy(&body)
            )
        })?;
        Ok((status, json))
    }

    pub async fn post_json<T: DeserializeOwned>(
        router: axum::Router,
        uri: &str,
        body: serde_json::Value,
    ) -> Result<(StatusCode, T)> {
        let request = make_request(Method::POST, uri, Some(body))?;
        let response = send(router, request).await?;
        let (status, body) = response_body(response).await?;
        let json = serde_json::from_slice(&body).with_context(|| {
            format!(
                "parse JSON response (status={status}): {}",
                String::from_utf8_lossy(&body)
            )
        })?;
        Ok((status, json))
    }
}

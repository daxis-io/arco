//! Orchestration API tests for create/idempotency behavior.

use anyhow::{Context, Result};
use axum::body::Body;
use axum::http::{Method, StatusCode, header};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tower::ServiceExt;

use arco_api::server::ServerBuilder;

fn test_router() -> axum::Router {
    ServerBuilder::new().debug(true).build().test_router()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateBackfillResponse {
    backfill_id: String,
    accepted_event_id: String,
    accepted_at: String,
}

#[derive(Debug, Deserialize)]
struct ApiErrorResponse {
    error: Option<String>,
    message: String,
    code: String,
}

mod helpers {
    use super::*;

    pub fn make_request_with_headers(
        method: Method,
        uri: &str,
        body: Option<serde_json::Value>,
        headers: &[(&str, &str)],
    ) -> Result<axum::http::Request<Body>> {
        let mut builder = axum::http::Request::builder()
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
        request: axum::http::Request<Body>,
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

    pub async fn post_json_with_headers<T: DeserializeOwned>(
        router: axum::Router,
        uri: &str,
        body: serde_json::Value,
        headers: &[(&str, &str)],
    ) -> Result<(StatusCode, T)> {
        let request = make_request_with_headers(Method::POST, uri, Some(body), headers)?;
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

#[tokio::test]
async fn test_create_backfill_accepts_range_selector() -> Result<()> {
    let router = test_router();

    let body = serde_json::json!({
        "assetSelection": ["analytics.daily_sales"],
        "partitionSelector": {
            "type": "range",
            "start": "2025-01-01",
            "end": "2025-01-10"
        },
        "clientRequestId": "req_01"
    });

    let (status, response): (_, CreateBackfillResponse) = helpers::post_json_with_headers(
        router,
        "/api/v1/workspaces/test-workspace/backfills",
        body,
        &[("Idempotency-Key", "idem_01")],
    )
    .await?;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(!response.backfill_id.is_empty());
    assert!(!response.accepted_event_id.is_empty());
    assert!(!response.accepted_at.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_create_backfill_rejects_filter_without_bounds() -> Result<()> {
    let router = test_router();

    let body = serde_json::json!({
        "assetSelection": ["analytics.daily_sales"],
        "partitionSelector": {
            "type": "filter",
            "filters": {
                "start": "2025-01-01"
            }
        },
        "clientRequestId": "req_filter_invalid"
    });

    let (status, error): (_, ApiErrorResponse) = helpers::post_json_with_headers(
        router,
        "/api/v1/workspaces/test-workspace/backfills",
        body,
        &[("Idempotency-Key", "idem_filter_invalid")],
    )
    .await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error.code, "BAD_REQUEST");
    assert_eq!(error.message, "filter selector requires start/end bounds");
    assert!(error.error.is_none() || error.error.as_deref() == Some("bad_request"));

    Ok(())
}

#[tokio::test]
async fn test_create_backfill_rejects_filter_with_unsupported_keys() -> Result<()> {
    let router = test_router();

    let body = serde_json::json!({
        "assetSelection": ["analytics.daily_sales"],
        "partitionSelector": {
            "type": "filter",
            "filters": {
                "start": "2025-01-01",
                "end": "2025-01-03",
                "region": "us-*"
            }
        },
        "clientRequestId": "req_filter_extra_key"
    });

    let (status, error): (_, ApiErrorResponse) = helpers::post_json_with_headers(
        router,
        "/api/v1/workspaces/test-workspace/backfills",
        body,
        &[("Idempotency-Key", "idem_filter_extra_key")],
    )
    .await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error.code, "BAD_REQUEST");
    assert_eq!(
        error.message,
        "filter selector supports only start/end bounds"
    );
    assert!(error.error.is_none() || error.error.as_deref() == Some("bad_request"));

    Ok(())
}

#[tokio::test]
async fn test_create_backfill_rejects_range_with_invalid_date_format() -> Result<()> {
    let router = test_router();

    let body = serde_json::json!({
        "assetSelection": ["analytics.daily_sales"],
        "partitionSelector": {
            "type": "range",
            "start": "2025/01/01",
            "end": "2025-01-10"
        },
        "clientRequestId": "req_range_bad_format"
    });

    let (status, error): (_, ApiErrorResponse) = helpers::post_json_with_headers(
        router,
        "/api/v1/workspaces/test-workspace/backfills",
        body,
        &[("Idempotency-Key", "idem_range_bad_format")],
    )
    .await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error.code, "BAD_REQUEST");
    assert_eq!(
        error.message,
        "partition selector bounds must use YYYY-MM-DD"
    );
    assert!(error.error.is_none() || error.error.as_deref() == Some("bad_request"));

    Ok(())
}

#[tokio::test]
async fn test_create_backfill_rejects_range_with_start_after_end() -> Result<()> {
    let router = test_router();

    let body = serde_json::json!({
        "assetSelection": ["analytics.daily_sales"],
        "partitionSelector": {
            "type": "range",
            "start": "2025-01-10",
            "end": "2025-01-01"
        },
        "clientRequestId": "req_range_bad_order"
    });

    let (status, error): (_, ApiErrorResponse) = helpers::post_json_with_headers(
        router,
        "/api/v1/workspaces/test-workspace/backfills",
        body,
        &[("Idempotency-Key", "idem_range_bad_order")],
    )
    .await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error.code, "BAD_REQUEST");
    assert_eq!(
        error.message,
        "partition selector start must be less than or equal to end"
    );
    assert!(error.error.is_none() || error.error.as_deref() == Some("bad_request"));

    Ok(())
}

#[tokio::test]
async fn test_create_backfill_idempotent_on_key() -> Result<()> {
    let router = test_router();

    let body = serde_json::json!({
        "assetSelection": ["analytics.daily_sales"],
        "partitionSelector": {
            "type": "explicit",
            "partitions": ["2025-01-01", "2025-01-02"]
        },
        "chunkSize": 5,
        "maxConcurrentRuns": 2
    });

    let (status, first): (_, CreateBackfillResponse) = helpers::post_json_with_headers(
        router.clone(),
        "/api/v1/workspaces/test-workspace/backfills",
        body.clone(),
        &[("Idempotency-Key", "idem_02")],
    )
    .await?;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (status, second): (_, CreateBackfillResponse) = helpers::post_json_with_headers(
        router,
        "/api/v1/workspaces/test-workspace/backfills",
        body,
        &[("Idempotency-Key", "idem_02")],
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first.backfill_id, second.backfill_id);
    assert_eq!(first.accepted_event_id, second.accepted_event_id);
    assert!(!first.accepted_at.is_empty());
    assert_eq!(first.accepted_at, second.accepted_at);

    Ok(())
}

#[tokio::test]
async fn test_create_backfill_conflicts_on_payload_mismatch() -> Result<()> {
    let router = test_router();

    let body = serde_json::json!({
        "assetSelection": ["analytics.daily_sales"],
        "partitionSelector": {
            "type": "explicit",
            "partitions": ["2025-01-01", "2025-01-02"]
        }
    });

    let (status, _): (_, CreateBackfillResponse) = helpers::post_json_with_headers(
        router.clone(),
        "/api/v1/workspaces/test-workspace/backfills",
        body,
        &[("Idempotency-Key", "idem_03")],
    )
    .await?;
    assert_eq!(status, StatusCode::ACCEPTED);

    let mismatch = serde_json::json!({
        "assetSelection": ["analytics.daily_sales"],
        "partitionSelector": {
            "type": "explicit",
            "partitions": ["2025-01-03"]
        }
    });

    let (status, error): (_, ApiErrorResponse) = helpers::post_json_with_headers(
        router,
        "/api/v1/workspaces/test-workspace/backfills",
        mismatch,
        &[("Idempotency-Key", "idem_03")],
    )
    .await?;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error.code, "CONFLICT");

    Ok(())
}

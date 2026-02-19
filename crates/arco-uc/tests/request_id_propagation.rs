//! Request ID propagation checks across UC route groups.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arco_core::storage::MemoryBackend;
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use serde_json::json;
use tower::ServiceExt;

fn test_router() -> axum::Router {
    let backend = Arc::new(MemoryBackend::new());
    let state = UnityCatalogState::new(backend);
    unity_catalog_router(state)
}

async fn send(
    router: &axum::Router,
    method: Method,
    uri: &str,
    body: Option<serde_json::Value>,
    request_id: &str,
    include_scope: bool,
) -> StatusCode {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("X-Request-Id", request_id);

    if include_scope {
        builder = builder
            .header("X-Tenant-Id", "tenant1")
            .header("X-Workspace-Id", "workspace1");
    }

    let request = if let Some(body) = body {
        builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .expect("request")
    } else {
        builder.body(Body::empty()).expect("request")
    };

    let response = router.clone().oneshot(request).await.expect("response");
    let echoed = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok());
    assert_eq!(echoed, Some(request_id));
    response.status()
}

#[tokio::test]
async fn request_id_is_echoed_for_uc_route_groups() {
    let router = test_router();

    // Public route should still echo request id.
    let status = send(
        &router,
        Method::GET,
        "/openapi.json",
        None,
        "req-openapi",
        false,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let status = send(
        &router,
        Method::POST,
        "/catalogs",
        Some(json!({"name": "main"})),
        "req-catalog-create",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let status = send(
        &router,
        Method::POST,
        "/schemas",
        Some(json!({"name": "default", "catalog_name": "main"})),
        "req-schema-create",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let status = send(
        &router,
        Method::POST,
        "/tables",
        Some(json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "default",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "columns": [],
            "storage_location": "gs://bucket/main/default/events"
        })),
        "req-table-create",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let status = send(
        &router,
        Method::GET,
        "/permissions/table/main.default.events",
        None,
        "req-permissions",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let status = send(
        &router,
        Method::POST,
        "/temporary-path-credentials",
        Some(json!({"url": "gs://bucket/path", "operation": "PATH_READ"})),
        "req-credentials",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let status = send(
        &router,
        Method::GET,
        "/delta/preview/commits",
        Some(json!({
            "table_id": "018f8c4b-a1de-7d57-b0d8-d98f1ef2443a",
            "table_uri": "gs://bucket/path",
            "start_version": 0
        })),
        "req-delta",
        true,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

//! Request ID propagation checks across UC route groups.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arco_catalog::authz::compiler::{CompiledPermissionRow, CompiledPermissionSet};
use arco_catalog::authz::privileges::Privilege;
use arco_core::storage::MemoryBackend;
use arco_uc::context::UnityCatalogRequestContext;
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use serde_json::json;
use tower::ServiceExt;

fn test_router() -> axum::Router {
    let backend = Arc::new(MemoryBackend::new());
    let state =
        UnityCatalogState::new(backend).with_compiled_permissions(create_table_permissions());
    unity_catalog_router(state)
}

/// Principal that the harness authorizes for table DDL.
///
/// `POST /tables` requires `CREATE_TABLE` on the target schema, so every
/// request the harness sends carries a trusted principal and the state carries
/// a compiled view granting that principal the privilege on the schemas these
/// tests use. Requests without both are denied 403 — that is the fail-closed
/// posture under test, not harness noise.
const TEST_PRINCIPAL: &str = "user_test";

fn create_table_permissions() -> CompiledPermissionSet {
    let mut rows = Vec::new();
    for catalog in ["main", "main2"] {
        for schema in ["analytics", "default"] {
            let object_id = format!("{catalog}.{schema}");
            rows.push(CompiledPermissionRow {
                principal_id: TEST_PRINCIPAL.to_string(),
                object_id: object_id.clone(),
                object_type: "SCHEMA".to_string(),
                privilege: Privilege::CreateTable,
                source: "grant".to_string(),
                source_grant_id: Some(format!("grant_{catalog}_{schema}")),
                source_principal_id: TEST_PRINCIPAL.to_string(),
                source_object_id: object_id.clone(),
                inheritance_path: object_id,
                grant_option: false,
                group_snapshot_version: "groups-test".to_string(),
            });
        }
    }
    CompiledPermissionSet::new("event_test", "groups-test", true, rows)
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

    let mut request = if let Some(body) = body {
        builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .expect("request")
    } else {
        builder.body(Body::empty()).expect("request")
    };
    if include_scope {
        request.extensions_mut().insert(UnityCatalogRequestContext {
            tenant: "tenant1".to_string(),
            workspace: "workspace1".to_string(),
            request_id: request_id.to_string(),
            user_id: Some(TEST_PRINCIPAL.to_string()),
            idempotency_key: None,
        });
    }

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
    assert_eq!(status, StatusCode::FORBIDDEN);

    let status = send(
        &router,
        Method::POST,
        "/temporary-path-credentials",
        Some(json!({"url": "gs://bucket/path", "operation": "PATH_READ"})),
        "req-credentials",
        true,
    )
    .await;
    // The harness now sends an authenticated principal (table DDL requires
    // one), so vending gets past the unauthenticated-principal denial and
    // fails closed on the missing storage-governance projection instead.
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

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

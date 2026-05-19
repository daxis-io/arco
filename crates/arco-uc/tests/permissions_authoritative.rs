//! Task 3 coverage for UC permissions backed by compiled Arco permissions.

use std::sync::Arc;

use arco_catalog::authz::compiler::{CompiledPermissionRow, CompiledPermissionSet};
use arco_catalog::authz::privileges::Privilege;
use arco_core::storage::MemoryBackend;
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn get_permissions_returns_compiled_assignments_and_honors_principal_filter() {
    let state = UnityCatalogState::new(Arc::new(MemoryBackend::new()))
        .with_compiled_permissions(compiled_permissions());
    let app = unity_catalog_router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/permissions/table/table_orders")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = json_body(response).await;
    let assignments = payload["privilege_assignments"]
        .as_array()
        .expect("assignments");
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0]["principal"], "user_alice");
    assert_eq!(assignments[0]["privileges"][0], "SELECT");
    assert_eq!(assignments[0]["inherited_from"], "catalog_sales");

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/permissions/table/table_orders?principal=user_bob")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = json_body(response).await;
    assert_eq!(
        payload["privilege_assignments"].as_array().map(Vec::len),
        Some(0)
    );
}

#[tokio::test]
async fn patch_permissions_remains_unsupported_until_writer_backed_persistence_exists() {
    let state = UnityCatalogState::new(Arc::new(MemoryBackend::new()))
        .with_compiled_permissions(compiled_permissions());
    let app = unity_catalog_router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/permissions/table/table_orders")
                .header("content-type", "application/json")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::from(
                    serde_json::json!({
                        "changes": [
                            {
                                "principal": "user_alice",
                                "remove": ["SELECT"]
                            },
                            {
                                "principal": "user_bob",
                                "add": ["SELECT"]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/permissions/table/table_orders")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = json_body(response).await;
    let assignments = payload["privilege_assignments"]
        .as_array()
        .expect("assignments");
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0]["principal"], "user_alice");
    assert_eq!(assignments[0]["privileges"][0], "SELECT");
}

fn compiled_permissions() -> CompiledPermissionSet {
    CompiledPermissionSet::new(
        "event_004",
        "groups-rev-7",
        true,
        vec![CompiledPermissionRow {
            principal_id: "user_alice".to_string(),
            object_id: "table_orders".to_string(),
            object_type: "TABLE".to_string(),
            privilege: Privilege::Select,
            source: "grant".to_string(),
            source_grant_id: Some("grant_catalog_select".to_string()),
            source_principal_id: "group_data".to_string(),
            source_object_id: "catalog_sales".to_string(),
            inheritance_path: "catalog_sales/schema_retail/table_orders".to_string(),
            grant_option: false,
            group_snapshot_version: "groups-rev-7".to_string(),
        }],
    )
}

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&body).expect("json payload")
}

//! Discovery endpoint behavior tests for the Unity Catalog facade.

use std::sync::Arc;

use arco_catalog::{
    CatalogWriter, ColumnDefinition, RegisterTableInSchemaRequest, Tier1Compactor, WriteOptions,
};
use arco_core::ScopedStorage;
use arco_core::storage::MemoryBackend;
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

struct SeededRouter {
    app: axum::Router,
    table_id: String,
    table_uri: String,
}

async fn seeded_router() -> SeededRouter {
    let backend = Arc::new(MemoryBackend::new());
    let scoped =
        ScopedStorage::new(backend.clone(), "tenant1", "workspace1").expect("scoped storage");

    let compactor = Arc::new(Tier1Compactor::new(scoped.clone()));
    let writer = CatalogWriter::new(scoped)
        .with_sync_compactor(compactor)
        // Tier-1 DDL now validates fencing token expiration; keep TTL comfortably above
        // any in-process compaction/IO jitter to avoid flakiness.
        .with_lock_policy(std::time::Duration::from_secs(5), 3);
    writer.initialize().await.expect("initialize");

    writer
        .create_catalog(
            "analytics",
            Some("Analytics catalog"),
            WriteOptions::default(),
        )
        .await
        .expect("create catalog");
    writer
        .create_schema(
            "analytics",
            "sales",
            Some("Sales schema"),
            WriteOptions::default(),
        )
        .await
        .expect("create schema");
    let table = writer
        .register_table_in_schema(
            "analytics",
            "sales",
            RegisterTableInSchemaRequest {
                name: "orders".to_string(),
                description: Some("Orders table".to_string()),
                location: Some("gs://arco-test/tenant1/workspace1/orders".to_string()),
                format: Some("delta".to_string()),
                columns: vec![
                    ColumnDefinition {
                        name: "order_id".to_string(),
                        data_type: "LONG".to_string(),
                        is_nullable: false,
                        description: None,
                    },
                    ColumnDefinition {
                        name: "amount".to_string(),
                        data_type: "DOUBLE".to_string(),
                        is_nullable: true,
                        description: None,
                    },
                ],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register table");

    let state = UnityCatalogState::new(backend);
    SeededRouter {
        app: unity_catalog_router(state),
        table_id: table.id,
        table_uri: table
            .location
            .unwrap_or_else(|| "gs://arco-test/tenant1/workspace1/orders".to_string()),
    }
}

#[tokio::test]
async fn test_get_catalogs_returns_success() {
    let seeded = seeded_router().await;
    let app = seeded.app;
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/catalogs")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_get_table_by_full_name_returns_success() {
    let seeded = seeded_router().await;
    let app = seeded.app;
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/tables/analytics.sales.orders")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_delete_catalog_native_table_returns_not_supported() {
    let seeded = seeded_router().await;
    let app = seeded.app;
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/tables/analytics.sales.orders")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(
        payload
            .pointer("/error/error_code")
            .and_then(serde_json::Value::as_str),
        Some("NOT_SUPPORTED")
    );
}

#[tokio::test]
async fn test_get_permissions_returns_success() {
    let seeded = seeded_router().await;
    let app = seeded.app;
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/permissions/table/analytics.sales.orders")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_post_temporary_path_credentials_unknown_operation_is_bad_request() {
    let seeded = seeded_router().await;
    let app = seeded.app;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/temporary-path-credentials")
                .header("content-type", "application/json")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::from(
                    json!({
                        "url": "gs://bucket/path",
                        "operation": "UNKNOWN_PATH_OPERATION"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_permissions_principal_filter_returns_empty_assignments() {
    let seeded = seeded_router().await;
    let app = seeded.app;
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/permissions/table/analytics.sales.orders?principal=someone-else")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json payload");
    assert_eq!(
        payload
            .get("privilege_assignments")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len),
        Some(0)
    );
}

#[tokio::test]
async fn test_delta_commit_invalid_table_id_is_bad_request() {
    let seeded = seeded_router().await;
    let app = seeded.app;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/delta/preview/commits")
                .header("content-type", "application/json")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .header("Idempotency-Key", Uuid::now_v7().to_string())
                .body(Body::from(
                    json!({
                        "table_id": "not-a-uuid",
                        "table_uri": "gs://bucket/path",
                        "commit_info": {
                            "version": 0,
                            "timestamp": 1,
                            "file_name": "00000000000000000000.json",
                            "file_size": 1,
                            "file_modification_timestamp": 1
                        }
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_delta_get_commits_unknown_table_returns_not_found() {
    let seeded = seeded_router().await;
    let app = seeded.app;
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/delta/preview/commits")
                .header("content-type", "application/json")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::from(
                    json!({
                        "table_id": "018f8c4b-a1de-7d57-b0d8-d98f1ef2443a",
                        "table_uri": "gs://bucket/path",
                        "start_version": 0
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delta_commit_and_get_commits_roundtrip() {
    let seeded = seeded_router().await;
    let table_id = seeded.table_id.clone();
    let table_uri = seeded.table_uri.clone();
    let table_id_for_get = seeded.table_id.clone();
    let table_uri_for_get = seeded.table_uri.clone();
    let table_id_for_backfill = seeded.table_id.clone();
    let table_uri_for_backfill = seeded.table_uri.clone();
    let table_id_for_final_get = seeded.table_id.clone();
    let table_uri_for_final_get = seeded.table_uri.clone();
    let app = seeded.app;

    let commit_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/delta/preview/commits")
                .header("content-type", "application/json")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .header("Idempotency-Key", Uuid::now_v7().to_string())
                .body(Body::from(
                    json!({
                        "table_id": table_id,
                        "table_uri": table_uri,
                        "commit_info": {
                            "version": 0,
                            "timestamp": 1,
                            "file_name": "00000000000000000000.json",
                            "file_size": 64,
                            "file_modification_timestamp": 1
                        }
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(commit_response.status(), StatusCode::OK);

    let get_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/delta/preview/commits")
                .header("content-type", "application/json")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::from(
                    json!({
                        "table_id": table_id_for_get,
                        "table_uri": table_uri_for_get,
                        "start_version": 0
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(get_response.status(), StatusCode::OK);
    let get_body = to_bytes(get_response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let payload: serde_json::Value = serde_json::from_slice(&get_body).expect("json payload");
    assert_eq!(
        payload
            .get("latest_table_version")
            .and_then(serde_json::Value::as_i64),
        Some(0)
    );
    assert_eq!(
        payload
            .get("commits")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len),
        Some(1)
    );

    let backfill_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/delta/preview/commits")
                .header("content-type", "application/json")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::from(
                    json!({
                        "table_id": table_id_for_backfill,
                        "table_uri": table_uri_for_backfill,
                        "latest_backfilled_version": 0
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(backfill_response.status(), StatusCode::OK);

    let get_after_backfill = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/delta/preview/commits")
                .header("content-type", "application/json")
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::from(
                    json!({
                        "table_id": table_id_for_final_get,
                        "table_uri": table_uri_for_final_get,
                        "start_version": 0
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(get_after_backfill.status(), StatusCode::OK);
    let backfilled_body = to_bytes(get_after_backfill.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let backfilled_payload: serde_json::Value =
        serde_json::from_slice(&backfilled_body).expect("json payload");
    assert_eq!(
        backfilled_payload
            .get("commits")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len),
        Some(0)
    );
}

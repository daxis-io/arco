//! Ensures new UC-like catalog APIs support idempotent retries.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arco_api::config::{Config, Posture};
use arco_api::routes;
use arco_api::server::AppState;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use tower::ServiceExt as _;
use uuid::Uuid;

#[derive(Debug, serde::Deserialize)]
struct CatalogResponse {
    id: String,
    name: String,
}

#[derive(Debug, serde::Deserialize)]
struct SchemaResponse {
    id: String,
    catalog: String,
    name: String,
}

#[derive(Debug, serde::Deserialize)]
struct TableResponse {
    id: String,
    catalog: String,
    schema: String,
    name: String,
}

fn make_test_state() -> Arc<AppState> {
    let config = Config {
        debug: true,
        posture: Posture::Dev,
        ..Config::default()
    };
    Arc::new(AppState::with_memory_storage(config))
}

#[tokio::test]
async fn create_catalog_is_idempotent_with_idempotency_key() {
    let state = make_test_state();
    let app = routes::api_v1_routes().with_state(Arc::clone(&state));

    let tenant = "acme";
    let workspace = "analytics";

    let idempotency_key = Uuid::now_v7().to_string();
    let body = serde_json::to_vec(&serde_json::json!({
        "name": "analytics",
        "description": null,
    }))
    .unwrap();

    let request = Request::builder()
        .method("POST")
        .uri("/catalogs")
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("Idempotency-Key", idempotency_key.clone())
        .body(Body::from(body.clone()))
        .unwrap();
    let resp = app.clone().oneshot(request).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let created: CatalogResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(created.name, "analytics");

    let replay = Request::builder()
        .method("POST")
        .uri("/catalogs")
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("Idempotency-Key", idempotency_key)
        .body(Body::from(body))
        .unwrap();
    let replay_resp = app.oneshot(replay).await.unwrap();
    assert_eq!(replay_resp.status(), StatusCode::CREATED);
    let replay_bytes = axum::body::to_bytes(replay_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let replayed: CatalogResponse = serde_json::from_slice(&replay_bytes).unwrap();
    assert_eq!(replayed.id, created.id);
    assert_eq!(replayed.name, created.name);
}

#[tokio::test]
async fn create_schema_is_idempotent_with_idempotency_key() {
    let state = make_test_state();
    let app = routes::api_v1_routes().with_state(Arc::clone(&state));

    let tenant = "acme";
    let workspace = "analytics";

    let create_catalog = Request::builder()
        .method("POST")
        .uri("/catalogs")
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({ "name": "analytics" })).unwrap(),
        ))
        .unwrap();
    let catalog_resp = app.clone().oneshot(create_catalog).await.unwrap();
    assert_eq!(catalog_resp.status(), StatusCode::CREATED);

    let idempotency_key = Uuid::now_v7().to_string();
    let body = serde_json::to_vec(&serde_json::json!({
        "name": "sales",
        "description": null,
    }))
    .unwrap();

    let request = Request::builder()
        .method("POST")
        .uri("/catalogs/analytics/schemas")
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("Idempotency-Key", idempotency_key.clone())
        .body(Body::from(body.clone()))
        .unwrap();
    let resp = app.clone().oneshot(request).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let created: SchemaResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(created.catalog, "analytics");
    assert_eq!(created.name, "sales");

    let replay = Request::builder()
        .method("POST")
        .uri("/catalogs/analytics/schemas")
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("Idempotency-Key", idempotency_key)
        .body(Body::from(body))
        .unwrap();
    let replay_resp = app.oneshot(replay).await.unwrap();
    assert_eq!(replay_resp.status(), StatusCode::CREATED);
    let replay_bytes = axum::body::to_bytes(replay_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let replayed: SchemaResponse = serde_json::from_slice(&replay_bytes).unwrap();
    assert_eq!(replayed.id, created.id);
    assert_eq!(replayed.catalog, created.catalog);
    assert_eq!(replayed.name, created.name);
}

#[tokio::test]
async fn register_table_in_schema_is_idempotent_with_idempotency_key() {
    let state = make_test_state();
    let app = routes::api_v1_routes().with_state(Arc::clone(&state));

    let tenant = "acme";
    let workspace = "analytics";

    let create_catalog = Request::builder()
        .method("POST")
        .uri("/catalogs")
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({ "name": "analytics" })).unwrap(),
        ))
        .unwrap();
    let catalog_resp = app.clone().oneshot(create_catalog).await.unwrap();
    assert_eq!(catalog_resp.status(), StatusCode::CREATED);

    let create_schema = Request::builder()
        .method("POST")
        .uri("/catalogs/analytics/schemas")
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({ "name": "sales" })).unwrap(),
        ))
        .unwrap();
    let schema_resp = app.clone().oneshot(create_schema).await.unwrap();
    assert_eq!(schema_resp.status(), StatusCode::CREATED);

    let idempotency_key = Uuid::now_v7().to_string();
    let body = serde_json::to_vec(&serde_json::json!({
        "name": "orders",
        "description": null,
        "location": "warehouse/orders.parquet",
        "format": "parquet",
        "columns": [],
    }))
    .unwrap();

    let request = Request::builder()
        .method("POST")
        .uri("/catalogs/analytics/schemas/sales/tables")
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("Idempotency-Key", idempotency_key.clone())
        .body(Body::from(body.clone()))
        .unwrap();
    let resp = app.clone().oneshot(request).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let created: TableResponse = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(created.catalog, "analytics");
    assert_eq!(created.schema, "sales");
    assert_eq!(created.name, "orders");

    let replay = Request::builder()
        .method("POST")
        .uri("/catalogs/analytics/schemas/sales/tables")
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("Idempotency-Key", idempotency_key)
        .body(Body::from(body))
        .unwrap();
    let replay_resp = app.oneshot(replay).await.unwrap();
    assert_eq!(replay_resp.status(), StatusCode::CREATED);
    let replay_bytes = axum::body::to_bytes(replay_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let replayed: TableResponse = serde_json::from_slice(&replay_bytes).unwrap();
    assert_eq!(replayed.id, created.id);
    assert_eq!(replayed.catalog, created.catalog);
    assert_eq!(replayed.schema, created.schema);
    assert_eq!(replayed.name, created.name);
}

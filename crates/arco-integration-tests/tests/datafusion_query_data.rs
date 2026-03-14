//! Ensures the query-data endpoint can resolve registered Parquet tables.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arco_api::config::{Config, Posture};
use arco_api::routes;
use arco_api::server::AppState;
use arco_catalog::write_options::WriteOptions;
use arco_catalog::{CatalogWriter, RegisterTableInSchemaRequest, Tier1Compactor};
use arco_core::ScopedStorage;
use arco_core::storage::{StorageBackend, WritePrecondition};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use tower::ServiceExt as _;

fn make_parquet_bytes() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .expect("batch");

    let mut buffer = Vec::new();
    {
        let mut writer =
            ArrowWriter::try_new(&mut buffer, Arc::clone(&schema), None).expect("arrow writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }

    buffer
}

#[tokio::test]
async fn query_data_can_select_from_registered_parquet_table() {
    let config = Config {
        debug: true,
        posture: Posture::Dev,
        ..Config::default()
    };

    let state = Arc::new(AppState::with_memory_storage(config));

    let backend: Arc<dyn StorageBackend> = state.storage_backend().expect("backend");
    let storage = ScopedStorage::new(backend, "acme", "analytics").expect("scoped storage");

    let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
    writer.initialize().await.expect("init");

    writer
        .create_catalog("analytics", None, WriteOptions::default())
        .await
        .expect("create catalog");
    writer
        .create_schema("analytics", "sales", None, WriteOptions::default())
        .await
        .expect("create schema");
    writer
        .register_table_in_schema(
            "analytics",
            "sales",
            RegisterTableInSchemaRequest {
                name: "orders".to_string(),
                description: None,
                location: Some("warehouse/orders.parquet".to_string()),
                format: Some("parquet".to_string()),
                columns: vec![],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register table");

    storage
        .put_raw(
            "warehouse/orders.parquet",
            Bytes::from(make_parquet_bytes()),
            WritePrecondition::None,
        )
        .await
        .expect("put parquet");

    let app = routes::api_v1_routes().with_state(state);

    let req = Request::builder()
        .method("POST")
        .uri("/query-data?format=json")
        .header("Content-Type", "application/json")
        .header("X-Tenant-Id", "acme")
        .header("X-Workspace-Id", "analytics")
        .body(Body::from(
            r#"{"sql":"SELECT id FROM analytics.sales.orders WHERE id >= 2 ORDER BY id"}"#,
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default(),
        "application/json"
    );

    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let rows = json.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.first().expect("row 0")["id"], 2);
    assert_eq!(rows.get(1).expect("row 1")["id"], 3);
}

#[tokio::test]
async fn query_data_does_not_fail_when_other_registered_tables_are_missing() {
    let config = Config {
        debug: true,
        posture: Posture::Dev,
        ..Config::default()
    };

    let state = Arc::new(AppState::with_memory_storage(config));

    let backend: Arc<dyn StorageBackend> = state.storage_backend().expect("backend");
    let storage = ScopedStorage::new(backend, "acme", "analytics").expect("scoped storage");

    let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
    writer.initialize().await.expect("init");

    writer
        .create_catalog("analytics", None, WriteOptions::default())
        .await
        .expect("create catalog");
    writer
        .create_schema("analytics", "sales", None, WriteOptions::default())
        .await
        .expect("create schema");
    writer
        .register_table_in_schema(
            "analytics",
            "sales",
            RegisterTableInSchemaRequest {
                name: "orders".to_string(),
                description: None,
                location: Some("warehouse/orders.parquet".to_string()),
                format: Some("parquet".to_string()),
                columns: vec![],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register orders");
    writer
        .register_table_in_schema(
            "analytics",
            "sales",
            RegisterTableInSchemaRequest {
                name: "broken".to_string(),
                description: None,
                location: Some("warehouse/missing.parquet".to_string()),
                format: Some("parquet".to_string()),
                columns: vec![],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register broken");

    storage
        .put_raw(
            "warehouse/orders.parquet",
            Bytes::from(make_parquet_bytes()),
            WritePrecondition::None,
        )
        .await
        .expect("put parquet");

    let app = routes::api_v1_routes().with_state(state);

    let req = Request::builder()
        .method("POST")
        .uri("/query-data?format=json")
        .header("Content-Type", "application/json")
        .header("X-Tenant-Id", "acme")
        .header("X-Workspace-Id", "analytics")
        .body(Body::from(
            r#"{"sql":"SELECT id FROM analytics.sales.orders WHERE id >= 2 ORDER BY id"}"#,
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let rows = json.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.first().expect("row 0")["id"], 2);
    assert_eq!(rows.get(1).expect("row 1")["id"], 3);
}

#[tokio::test]
async fn query_endpoint_rejects_non_read_sql() {
    let config = Config {
        debug: true,
        posture: Posture::Dev,
        ..Config::default()
    };

    let state = Arc::new(AppState::with_memory_storage(config));
    let app = routes::api_v1_routes().with_state(state);

    let req = Request::builder()
        .method("POST")
        .uri("/query?format=json")
        .header("Content-Type", "application/json")
        .header("X-Tenant-Id", "acme")
        .header("X-Workspace-Id", "analytics")
        .body(Body::from(r#"{"sql":"CREATE TABLE denied(id INT)"}"#))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        json["message"]
            .as_str()
            .unwrap_or_default()
            .contains("Only SELECT/CTE queries are supported")
    );
}

#[tokio::test]
async fn query_data_endpoint_rejects_non_read_sql() {
    let config = Config {
        debug: true,
        posture: Posture::Dev,
        ..Config::default()
    };

    let state = Arc::new(AppState::with_memory_storage(config));
    let app = routes::api_v1_routes().with_state(state);

    let req = Request::builder()
        .method("POST")
        .uri("/query-data?format=json")
        .header("Content-Type", "application/json")
        .header("X-Tenant-Id", "acme")
        .header("X-Workspace-Id", "analytics")
        .body(Body::from(
            r#"{"sql":"DELETE FROM analytics.sales.orders"}"#,
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        json["message"]
            .as_str()
            .unwrap_or_default()
            .contains("Only SELECT/CTE queries are supported")
    );
}

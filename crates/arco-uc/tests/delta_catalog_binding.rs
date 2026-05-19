//! Catalog-bound validation for UC Delta preview commit routes.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use arco_catalog::{
    CatalogWriter, ColumnDefinition, RegisterTableInSchemaRequest, Tier1Compactor, WriteOptions,
};
use arco_core::ScopedStorage;
use arco_core::storage::{MemoryBackend, WritePrecondition};
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use bytes::Bytes;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

const TENANT: &str = "tenant1";
const WORKSPACE: &str = "workspace1";

#[derive(Debug)]
struct UcResponse {
    status: StatusCode,
    body: Value,
    body_text: String,
    request_id: Option<String>,
}

struct Harness {
    router: Router,
    storage: ScopedStorage,
}

fn make_harness() -> Harness {
    let backend = Arc::new(MemoryBackend::new());
    let state = UnityCatalogState::new(backend.clone());
    let router = unity_catalog_router(state);
    let storage = ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage");
    Harness { router, storage }
}

async fn initialize_catalog(storage: &ScopedStorage) -> Result<(), String> {
    let writer = catalog_writer(storage);
    writer
        .initialize()
        .await
        .map_err(|err| format!("initialize catalog: {err}"))?;
    writer
        .create_catalog("prod", Some("production"), WriteOptions::default())
        .await
        .map_err(|err| format!("create catalog: {err}"))?;
    writer
        .create_schema(
            "prod",
            "analytics",
            Some("analytics"),
            WriteOptions::default(),
        )
        .await
        .map_err(|err| format!("create schema: {err}"))?;
    Ok(())
}

fn catalog_writer(storage: &ScopedStorage) -> CatalogWriter {
    let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
    CatalogWriter::new(storage.clone()).with_sync_compactor(compactor)
}

async fn seed_table(
    storage: &ScopedStorage,
    name: &str,
    location: &str,
    format: &str,
    table_type: Option<&str>,
) -> Result<arco_catalog::Table, String> {
    catalog_writer(storage)
        .register_table_in_schema(
            "prod",
            "analytics",
            RegisterTableInSchemaRequest {
                name: name.to_string(),
                description: None,
                location: Some(location.to_string()),
                format: Some(format.to_string()),
                table_type: table_type.map(str::to_string),
                properties: None,
                columns: vec![ColumnDefinition {
                    name: "id".to_string(),
                    data_type: "STRING".to_string(),
                    is_nullable: false,
                    ordinal: 0,
                    description: None,
                }],
            },
            WriteOptions::default(),
        )
        .await
        .map_err(|err| format!("seed table: {err}"))
}

async fn uc_request(
    router: &Router,
    method: Method,
    uri: &str,
    body: Value,
    idempotency_key: Option<&str>,
    request_id: Option<&str>,
) -> Result<UcResponse, String> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE);

    if let Some(idempotency_key) = idempotency_key {
        builder = builder.header("Idempotency-Key", idempotency_key);
    }
    if let Some(request_id) = request_id {
        builder = builder.header("X-Request-Id", request_id);
    }

    let req = builder
        .body(Body::from(body.to_string()))
        .map_err(|err| format!("build request: {err}"))?;

    let response = router
        .clone()
        .oneshot(req)
        .await
        .map_err(|err| format!("route request: {err}"))?;

    let status = response.status();
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .map_err(|err| format!("read response body: {err}"))?;
    let body_text = String::from_utf8(body.to_vec()).map_err(|err| format!("utf8 body: {err}"))?;
    let body = if body_text.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&body_text).map_err(|err| format!("parse response body: {err}"))?
    };

    Ok(UcResponse {
        status,
        body,
        body_text,
        request_id,
    })
}

fn commit_info(version: i64) -> Value {
    json!({
        "version": version,
        "timestamp": 1,
        "file_name": format!("{version:020}.json"),
        "file_size": 42,
        "file_modification_timestamp": 1
    })
}

fn assert_error(response: &UcResponse, status: StatusCode, code: &str, request_id: &str) {
    assert_eq!(response.status, status);
    assert_eq!(
        response
            .body
            .pointer("/error/error_code")
            .and_then(Value::as_str),
        Some(code)
    );
    assert_eq!(response.request_id.as_deref(), Some(request_id));
}

async fn assert_no_delta_side_effects(
    storage: &ScopedStorage,
    table_id: Uuid,
) -> Result<(), String> {
    for prefix in [
        format!("delta/staging/{table_id}/"),
        format!("delta/coordinator/{table_id}"),
        format!("uc/delta_preview/{table_id}"),
        format!("tables/{table_id}/_delta_log/"),
    ] {
        let paths = storage
            .list(&prefix)
            .await
            .map_err(|err| format!("list {prefix}: {err}"))?;
        assert!(
            paths.is_empty(),
            "unexpected side effects under {prefix}: {paths:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn uc_delta_preview_rejects_unknown_table_before_existing_state() -> Result<(), String> {
    let harness = make_harness();
    let table_id = Uuid::now_v7();
    harness
        .storage
        .put_raw(
            &format!("delta/coordinator/{table_id}.json"),
            Bytes::from_static(br#"{"latest_version":0,"inflight":null}"#),
            WritePrecondition::DoesNotExist,
        )
        .await
        .map_err(|err| format!("seed coordinator: {err}"))?;

    let get = uc_request(
        &harness.router,
        Method::GET,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path",
            "start_version": 0
        }),
        None,
        Some("req-unknown-get"),
    )
    .await?;
    assert_error(&get, StatusCode::NOT_FOUND, "NOT_FOUND", "req-unknown-get");

    let post = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path",
            "commit_info": commit_info(0)
        }),
        Some(&Uuid::now_v7().to_string()),
        Some("req-unknown-post"),
    )
    .await?;
    assert_error(
        &post,
        StatusCode::NOT_FOUND,
        "NOT_FOUND",
        "req-unknown-post",
    );

    let staged_paths = harness
        .storage
        .list(&format!("delta/staging/{table_id}/"))
        .await
        .map_err(|err| format!("list staged: {err}"))?;
    assert!(
        staged_paths.is_empty(),
        "unknown table must not stage payloads"
    );

    Ok(())
}

#[tokio::test]
async fn uc_delta_preview_rejects_non_delta_and_unmanaged_tables_without_side_effects()
-> Result<(), String> {
    let harness = make_harness();
    initialize_catalog(&harness.storage).await?;
    let non_delta = seed_table(
        &harness.storage,
        "orders_iceberg",
        "warehouse/prod/analytics/orders_iceberg",
        "iceberg",
        Some("MANAGED"),
    )
    .await?;
    let external_delta = seed_table(
        &harness.storage,
        "orders_external_delta",
        "warehouse/prod/analytics/orders_external_delta",
        "delta",
        Some("EXTERNAL"),
    )
    .await?;

    for (table, request_id) in [
        (&non_delta, "req-non-delta"),
        (&external_delta, "req-external-delta"),
    ] {
        let table_id =
            Uuid::parse_str(&table.id).map_err(|err| format!("parse table id: {err}"))?;
        let response = uc_request(
            &harness.router,
            Method::POST,
            "/delta/preview/commits",
            json!({
                "table_id": table.id,
                "table_uri": table.location,
                "commit_info": commit_info(0)
            }),
            Some(&Uuid::now_v7().to_string()),
            Some(request_id),
        )
        .await?;
        assert_error(&response, StatusCode::CONFLICT, "CONFLICT", request_id);
        assert_no_delta_side_effects(&harness.storage, table_id).await?;
    }

    Ok(())
}

#[tokio::test]
async fn uc_delta_preview_rejects_table_uri_mismatch_against_catalog_location() -> Result<(), String>
{
    let harness = make_harness();
    initialize_catalog(&harness.storage).await?;
    let table = seed_table(
        &harness.storage,
        "orders_managed_delta",
        "gs://safe-bucket/tenant=tenant1/workspace=workspace1/warehouse/prod/analytics/orders",
        "delta",
        Some("MANAGED"),
    )
    .await?;
    let table_id = Uuid::parse_str(&table.id).map_err(|err| format!("parse table id: {err}"))?;

    let response = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        json!({
            "table_id": table.id,
            "table_uri": "gs://caller-secret/tenant=tenant1/workspace=workspace1/warehouse/prod/analytics/orders",
            "commit_info": commit_info(0)
        }),
        Some(&Uuid::now_v7().to_string()),
        Some("req-uri-mismatch"),
    )
    .await?;
    assert_error(
        &response,
        StatusCode::BAD_REQUEST,
        "BAD_REQUEST",
        "req-uri-mismatch",
    );
    assert!(
        !response.body_text.contains("safe-bucket")
            && !response.body_text.contains("caller-secret"),
        "table_uri mismatch must not echo raw URIs: {}",
        response.body_text
    );
    assert_no_delta_side_effects(&harness.storage, table_id).await?;

    Ok(())
}

//! Integration tests for Unity Catalog catalog/schema/table CRUD endpoints.

use std::sync::Arc;

use arco_catalog::CatalogReader;
use arco_core::ScopedStorage;
use arco_core::storage::{MemoryBackend, WritePrecondition, WriteResult};
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

fn test_router() -> Router {
    let backend = Arc::new(MemoryBackend::new());
    let state = UnityCatalogState::new(backend);
    unity_catalog_router(state)
}

fn test_harness() -> (Router, Arc<MemoryBackend>) {
    let backend = Arc::new(MemoryBackend::new());
    let state = UnityCatalogState::new(backend.clone());
    (unity_catalog_router(state), backend)
}

fn scoped_storage(
    backend: Arc<MemoryBackend>,
    tenant: &str,
    workspace: &str,
) -> Result<ScopedStorage, String> {
    ScopedStorage::new(backend, tenant, workspace).map_err(|err| format!("scoped storage: {err}"))
}

fn preview_catalog_path(name: &str) -> String {
    format!("unity-catalog-preview/catalogs/{name}.json")
}

fn preview_schema_path(catalog_name: &str, schema_name: &str) -> String {
    format!("unity-catalog-preview/schemas/{catalog_name}/{schema_name}.json")
}

fn preview_table_path(catalog_name: &str, schema_name: &str, table_name: &str) -> String {
    format!("unity-catalog-preview/tables/{catalog_name}/{schema_name}/{table_name}.json")
}

async fn write_preview_json(
    storage: &ScopedStorage,
    path: &str,
    payload: Value,
) -> Result<(), String> {
    let bytes =
        serde_json::to_vec(&payload).map_err(|err| format!("serialize preview json: {err}"))?;
    match storage
        .put_raw(path, bytes.into(), WritePrecondition::None)
        .await
        .map_err(|err| format!("write preview json: {err}"))?
    {
        WriteResult::Success { .. } => Ok(()),
        WriteResult::PreconditionFailed { current_version } => Err(format!(
            "unexpected preview write precondition failure at {path}: {current_version}"
        )),
    }
}

async fn uc_request(
    router: &Router,
    method: Method,
    uri: &str,
    tenant: &str,
    workspace: &str,
    body: Option<Value>,
) -> Result<(StatusCode, Value), String> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace);

    let req = if let Some(payload) = body {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        let bytes =
            serde_json::to_vec(&payload).map_err(|err| format!("serialize request body: {err}"))?;
        builder
            .body(Body::from(bytes))
            .map_err(|err| format!("build request: {err}"))?
    } else {
        builder
            .body(Body::empty())
            .map_err(|err| format!("build request: {err}"))?
    };

    let response = router
        .clone()
        .oneshot(req)
        .await
        .map_err(|err| format!("route request: {err}"))?;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .map_err(|err| format!("read response body: {err}"))?;

    let parsed = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).map_err(|err| format!("parse response body: {err}"))?
    };
    Ok((status, parsed))
}

async fn uc_request_with_idempotency(
    router: &Router,
    method: Method,
    uri: &str,
    tenant: &str,
    workspace: &str,
    idempotency_key: &str,
    body: Option<Value>,
) -> Result<(StatusCode, Value), String> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("Idempotency-Key", idempotency_key);

    let req = if let Some(payload) = body {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        let bytes =
            serde_json::to_vec(&payload).map_err(|err| format!("serialize request body: {err}"))?;
        builder
            .body(Body::from(bytes))
            .map_err(|err| format!("build request: {err}"))?
    } else {
        builder
            .body(Body::empty())
            .map_err(|err| format!("build request: {err}"))?
    };

    let response = router
        .clone()
        .oneshot(req)
        .await
        .map_err(|err| format!("route request: {err}"))?;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .map_err(|err| format!("read response body: {err}"))?;

    let parsed = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).map_err(|err| format!("parse response body: {err}"))?
    };
    Ok((status, parsed))
}

async fn uc_request_without_scope(
    router: &Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> Result<(StatusCode, Value), String> {
    let mut builder = Request::builder().method(method).uri(uri);

    let req = if let Some(payload) = body {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        let bytes =
            serde_json::to_vec(&payload).map_err(|err| format!("serialize request body: {err}"))?;
        builder
            .body(Body::from(bytes))
            .map_err(|err| format!("build request: {err}"))?
    } else {
        builder
            .body(Body::empty())
            .map_err(|err| format!("build request: {err}"))?
    };

    let response = router
        .clone()
        .oneshot(req)
        .await
        .map_err(|err| format!("route request: {err}"))?;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .map_err(|err| format!("read response body: {err}"))?;

    let parsed = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).map_err(|err| format!("parse response body: {err}"))?
    };
    Ok((status, parsed))
}

#[tokio::test]
async fn create_and_list_catalogs() -> Result<(), String> {
    let router = test_router();

    let (create_status, created) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "main",
            "comment": "primary catalog"
        })),
    )
    .await?;
    assert_eq!(create_status, StatusCode::OK);
    assert_eq!(created.get("name").and_then(Value::as_str), Some("main"));

    let (list_status, listed) = uc_request(
        &router,
        Method::GET,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(list_status, StatusCode::OK);

    let catalogs = listed
        .get("catalogs")
        .and_then(Value::as_array)
        .ok_or_else(|| "catalogs should be an array".to_string())?;
    assert_eq!(catalogs.len(), 1);
    assert_eq!(
        catalogs[0].get("name").and_then(Value::as_str),
        Some("main")
    );
    Ok(())
}

#[tokio::test]
async fn uc_catalog_create_is_visible_through_authoritative_reader_without_preview_object()
-> Result<(), String> {
    let (router, backend) = test_harness();

    let (create_status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "main",
            "comment": "primary catalog"
        })),
    )
    .await?;
    assert_eq!(create_status, StatusCode::OK);

    let storage = scoped_storage(backend, "tenant_a", "workspace_a")?;
    let reader = CatalogReader::new(storage.clone());
    let catalog = reader
        .get_catalog("main")
        .await
        .map_err(|err| format!("read catalog from authoritative path: {err}"))?;
    assert!(
        catalog.is_some(),
        "catalog should exist in authoritative catalog state"
    );
    assert!(
        storage
            .head_raw(&preview_catalog_path("main"))
            .await
            .map_err(|err| format!("head preview catalog object: {err}"))?
            .is_none(),
        "preview catalog object should not be created for authoritative UC CRUD"
    );

    Ok(())
}

#[tokio::test]
async fn create_and_list_schemas_scoped_to_catalog() -> Result<(), String> {
    let router = test_router();

    let (catalog_status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(catalog_status, StatusCode::OK);

    let (create_status, created) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(create_status, StatusCode::OK);
    assert_eq!(
        created.get("full_name").and_then(Value::as_str),
        Some("main.analytics")
    );

    let (list_status, listed) = uc_request(
        &router,
        Method::GET,
        "/schemas?catalog_name=main",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(list_status, StatusCode::OK);
    let schemas = listed
        .get("schemas")
        .and_then(Value::as_array)
        .ok_or_else(|| "schemas should be an array".to_string())?;
    assert_eq!(schemas.len(), 1);
    assert_eq!(
        schemas[0].get("full_name").and_then(Value::as_str),
        Some("main.analytics")
    );
    Ok(())
}

#[tokio::test]
async fn uc_schema_create_is_visible_through_authoritative_reader_without_preview_object()
-> Result<(), String> {
    let (router, backend) = test_harness();

    let (catalog_status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(catalog_status, StatusCode::OK);

    let (create_status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(create_status, StatusCode::OK);

    let storage = scoped_storage(backend, "tenant_a", "workspace_a")?;
    let reader = CatalogReader::new(storage.clone());
    let schemas = reader
        .list_schemas("main")
        .await
        .map_err(|err| format!("read schemas from authoritative path: {err}"))?;
    assert!(
        schemas.iter().any(|schema| schema.name == "analytics"),
        "schema should exist in authoritative catalog state"
    );
    assert!(
        storage
            .head_raw(&preview_schema_path("main", "analytics"))
            .await
            .map_err(|err| format!("head preview schema object: {err}"))?
            .is_none(),
        "preview schema object should not be created for authoritative UC CRUD"
    );

    Ok(())
}

#[tokio::test]
async fn create_and_list_tables_scoped_to_catalog_and_schema() -> Result<(), String> {
    let router = test_router();

    let (catalog_status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(catalog_status, StatusCode::OK);

    let (schema_status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(schema_status, StatusCode::OK);

    let (create_status, created) = uc_request(
        &router,
        Method::POST,
        "/tables",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "analytics",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": [],
            "storage_location": "s3://bucket/main/analytics/events"
        })),
    )
    .await?;
    assert_eq!(create_status, StatusCode::OK);
    assert_eq!(
        created.get("full_name").and_then(Value::as_str),
        Some("main.analytics.events")
    );

    let (list_status, listed) = uc_request(
        &router,
        Method::GET,
        "/tables?catalog_name=main&schema_name=analytics",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(list_status, StatusCode::OK);
    let tables = listed
        .get("tables")
        .and_then(Value::as_array)
        .ok_or_else(|| "tables should be an array".to_string())?;
    assert_eq!(tables.len(), 1);
    assert_eq!(
        tables[0].get("full_name").and_then(Value::as_str),
        Some("main.analytics.events")
    );
    Ok(())
}

#[tokio::test]
async fn uc_table_create_is_visible_through_authoritative_reader_without_preview_object()
-> Result<(), String> {
    let (router, backend) = test_harness();

    let (catalog_status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(catalog_status, StatusCode::OK);

    let (schema_status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(schema_status, StatusCode::OK);

    let (create_status, _) = uc_request(
        &router,
        Method::POST,
        "/tables",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "analytics",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": [],
            "storage_location": "s3://bucket/main/analytics/events"
        })),
    )
    .await?;
    assert_eq!(create_status, StatusCode::OK);

    let storage = scoped_storage(backend, "tenant_a", "workspace_a")?;
    let reader = CatalogReader::new(storage.clone());
    let table = reader
        .get_table_in_schema("main", "analytics", "events")
        .await
        .map_err(|err| format!("read table from authoritative path: {err}"))?;
    assert!(
        table.is_some(),
        "table should exist in authoritative catalog state"
    );
    assert!(
        storage
            .head_raw(&preview_table_path("main", "analytics", "events"))
            .await
            .map_err(|err| format!("head preview table object: {err}"))?
            .is_none(),
        "preview table object should not be created for authoritative UC CRUD"
    );

    Ok(())
}

#[tokio::test]
async fn create_catalog_replays_with_same_idempotency_key() -> Result<(), String> {
    let (router, backend) = test_harness();
    let idempotency_key = Uuid::now_v7().to_string();
    let payload = json!({
        "name": "main",
        "comment": "primary catalog"
    });

    let (first_status, first_body) = uc_request_with_idempotency(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        &idempotency_key,
        Some(payload.clone()),
    )
    .await?;
    assert_eq!(first_status, StatusCode::OK);

    let (second_status, second_body) = uc_request_with_idempotency(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        &idempotency_key,
        Some(payload),
    )
    .await?;
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(second_body, first_body);

    let reader = CatalogReader::new(scoped_storage(backend, "tenant_a", "workspace_a")?);
    let catalogs = reader
        .list_catalogs()
        .await
        .map_err(|err| format!("list catalogs after idempotent replay: {err}"))?;
    assert_eq!(catalogs.len(), 1);
    assert_eq!(catalogs[0].name, "main");

    Ok(())
}

#[tokio::test]
async fn create_catalog_conflicts_when_idempotency_key_is_reused_for_different_payload()
-> Result<(), String> {
    let router = test_router();
    let idempotency_key = Uuid::now_v7().to_string();

    let (first_status, _) = uc_request_with_idempotency(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        &idempotency_key,
        Some(json!({
            "name": "main",
            "comment": "primary catalog"
        })),
    )
    .await?;
    assert_eq!(first_status, StatusCode::OK);

    let (second_status, second_body) = uc_request_with_idempotency(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        &idempotency_key,
        Some(json!({
            "name": "secondary",
            "comment": "different catalog"
        })),
    )
    .await?;
    assert_eq!(second_status, StatusCode::CONFLICT);
    assert_eq!(
        second_body
            .get("error")
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str),
        Some("Idempotency-Key already used with different request body")
    );

    Ok(())
}

#[tokio::test]
async fn create_schema_replays_with_same_idempotency_key() -> Result<(), String> {
    let (router, backend) = test_harness();

    let (catalog_status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(catalog_status, StatusCode::OK);

    let idempotency_key = Uuid::now_v7().to_string();
    let payload = json!({
        "name": "analytics",
        "catalog_name": "main",
        "comment": "reporting schema"
    });

    let (first_status, first_body) = uc_request_with_idempotency(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        &idempotency_key,
        Some(payload.clone()),
    )
    .await?;
    assert_eq!(first_status, StatusCode::OK);

    let (second_status, second_body) = uc_request_with_idempotency(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        &idempotency_key,
        Some(payload),
    )
    .await?;
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(second_body, first_body);

    let reader = CatalogReader::new(scoped_storage(backend, "tenant_a", "workspace_a")?);
    let schemas = reader
        .list_schemas("main")
        .await
        .map_err(|err| format!("list schemas after idempotent replay: {err}"))?;
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, "analytics");

    Ok(())
}

#[tokio::test]
async fn create_table_replays_with_same_idempotency_key() -> Result<(), String> {
    let (router, backend) = test_harness();

    let (catalog_status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(catalog_status, StatusCode::OK);

    let (schema_status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(schema_status, StatusCode::OK);

    let idempotency_key = Uuid::now_v7().to_string();
    let payload = json!({
        "name": "events",
        "catalog_name": "main",
        "schema_name": "analytics",
        "table_type": "EXTERNAL",
        "data_source_format": "DELTA",
        "columns": [],
        "storage_location": "s3://bucket/main/analytics/events",
        "comment": "tenant events"
    });

    let (first_status, first_body) = uc_request_with_idempotency(
        &router,
        Method::POST,
        "/tables",
        "tenant_a",
        "workspace_a",
        &idempotency_key,
        Some(payload.clone()),
    )
    .await?;
    assert_eq!(first_status, StatusCode::OK);

    let (second_status, second_body) = uc_request_with_idempotency(
        &router,
        Method::POST,
        "/tables",
        "tenant_a",
        "workspace_a",
        &idempotency_key,
        Some(payload),
    )
    .await?;
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(second_body, first_body);

    let reader = CatalogReader::new(scoped_storage(backend, "tenant_a", "workspace_a")?);
    let table = reader
        .get_table_in_schema("main", "analytics", "events")
        .await
        .map_err(|err| format!("get table after idempotent replay: {err}"))?;
    assert!(table.is_some());

    Ok(())
}

#[tokio::test]
async fn preview_only_catalog_schema_and_table_objects_are_ignored() -> Result<(), String> {
    let (router, backend) = test_harness();
    let storage = scoped_storage(backend, "tenant_a", "workspace_a")?;

    write_preview_json(
        &storage,
        &preview_catalog_path("main"),
        json!({"name": "main", "comment": "preview only"}),
    )
    .await?;
    write_preview_json(
        &storage,
        &preview_schema_path("main", "analytics"),
        json!({
            "name": "analytics",
            "catalog_name": "main",
            "full_name": "main.analytics"
        }),
    )
    .await?;
    write_preview_json(
        &storage,
        &preview_table_path("main", "analytics", "events"),
        json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "analytics",
            "full_name": "main.analytics.events"
        }),
    )
    .await?;

    let (catalogs_status, catalogs_body) = uc_request(
        &router,
        Method::GET,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(catalogs_status, StatusCode::OK);
    assert_eq!(
        catalogs_body
            .get("catalogs")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0),
        "preview-only catalog object must not appear in authoritative list results"
    );

    let (catalog_status, _) = uc_request(
        &router,
        Method::GET,
        "/catalogs/main",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(catalog_status, StatusCode::NOT_FOUND);

    let (schema_status, _) = uc_request(
        &router,
        Method::GET,
        "/schemas/main.analytics",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(schema_status, StatusCode::NOT_FOUND);

    let (table_status, _) = uc_request(
        &router,
        Method::GET,
        "/tables/main.analytics.events",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(table_status, StatusCode::NOT_FOUND);

    Ok(())
}

#[tokio::test]
async fn catalog_get_delete_and_not_found() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::GET,
        "/catalogs/main",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.get("name").and_then(Value::as_str), Some("main"));

    let (status, body) = uc_request(
        &router,
        Method::DELETE,
        "/catalogs/main",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Value::Object(serde_json::Map::new()));

    let (status, body) = uc_request(
        &router,
        Method::GET,
        "/catalogs/main",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("NOT_FOUND")
    );

    let (status, body) = uc_request(
        &router,
        Method::DELETE,
        "/catalogs/main",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("NOT_FOUND")
    );
    Ok(())
}

#[tokio::test]
async fn schema_get_delete_and_not_found() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::GET,
        "/schemas/main.analytics",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.get("full_name").and_then(Value::as_str),
        Some("main.analytics")
    );

    let (status, body) = uc_request(
        &router,
        Method::DELETE,
        "/schemas/main.analytics",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Value::Object(serde_json::Map::new()));

    let (status, body) = uc_request(
        &router,
        Method::GET,
        "/schemas/main.analytics",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("NOT_FOUND")
    );

    let (status, body) = uc_request(
        &router,
        Method::DELETE,
        "/schemas/main.analytics",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("NOT_FOUND")
    );
    Ok(())
}

#[tokio::test]
async fn catalog_patch_updates_authoritative_comment() -> Result<(), String> {
    let (router, backend) = test_harness();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main", "comment": "before"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::PATCH,
        "/catalogs/main",
        "tenant_a",
        "workspace_a",
        Some(json!({"comment": "after"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.get("comment").and_then(Value::as_str), Some("after"));

    let storage = scoped_storage(backend, "tenant_a", "workspace_a")?;
    let reader = CatalogReader::new(storage);
    let catalog = reader
        .get_catalog("main")
        .await
        .map_err(|err| format!("read updated catalog: {err}"))?
        .ok_or_else(|| "catalog should exist after patch".to_string())?;
    assert_eq!(catalog.description.as_deref(), Some("after"));

    Ok(())
}

#[tokio::test]
async fn schema_patch_updates_authoritative_comment() -> Result<(), String> {
    let (router, backend) = test_harness();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main",
            "comment": "before"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::PATCH,
        "/schemas/main.analytics",
        "tenant_a",
        "workspace_a",
        Some(json!({"comment": "after"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.get("comment").and_then(Value::as_str), Some("after"));

    let storage = scoped_storage(backend, "tenant_a", "workspace_a")?;
    let reader = CatalogReader::new(storage);
    let schema = reader
        .list_schemas("main")
        .await
        .map_err(|err| format!("read updated schemas: {err}"))?
        .into_iter()
        .find(|candidate| candidate.name == "analytics")
        .ok_or_else(|| "schema should exist after patch".to_string())?;
    assert_eq!(schema.description.as_deref(), Some("after"));

    Ok(())
}

#[tokio::test]
async fn force_delete_catalog_cascades_authoritative_state() -> Result<(), String> {
    let (router, backend) = test_harness();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/tables",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "analytics",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": [],
            "storage_location": "s3://bucket/main/analytics/events"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::DELETE,
        "/catalogs/main?force=true",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected force-delete body: {body:?}"
    );

    let storage = scoped_storage(backend, "tenant_a", "workspace_a")?;
    let reader = CatalogReader::new(storage);
    assert!(
        reader
            .get_catalog("main")
            .await
            .map_err(|err| format!("read catalog after force delete: {err}"))?
            .is_none(),
        "catalog should be removed by force delete"
    );

    Ok(())
}

#[tokio::test]
async fn table_get_delete_and_not_found() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/tables",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "analytics",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": [],
            "storage_location": "s3://bucket/main/analytics/events"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::GET,
        "/tables/main.analytics.events",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body.get("full_name").and_then(Value::as_str),
        Some("main.analytics.events")
    );

    let (status, body) = uc_request(
        &router,
        Method::DELETE,
        "/tables/main.analytics.events",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Value::Object(serde_json::Map::new()));

    let (status, body) = uc_request(
        &router,
        Method::GET,
        "/tables/main.analytics.events",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("NOT_FOUND")
    );

    let (status, body) = uc_request(
        &router,
        Method::DELETE,
        "/tables/main.analytics.events",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("NOT_FOUND")
    );
    Ok(())
}

#[tokio::test]
async fn object_get_delete_is_tenant_workspace_isolated() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::GET,
        "/catalogs/main",
        "tenant_b",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("NOT_FOUND")
    );

    let (status, body) = uc_request(
        &router,
        Method::DELETE,
        "/catalogs/main",
        "tenant_a",
        "workspace_b",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("NOT_FOUND")
    );

    let (status, body) = uc_request(
        &router,
        Method::GET,
        "/catalogs/main",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.get("name").and_then(Value::as_str), Some("main"));
    Ok(())
}

#[tokio::test]
async fn deny_by_default_without_scope_headers() -> Result<(), String> {
    let router = test_router();

    let (status, body) = uc_request_without_scope(&router, Method::GET, "/catalogs", None).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("UNAUTHORIZED")
    );

    let (status, body) = uc_request_without_scope(
        &router,
        Method::POST,
        "/delta/preview/commits",
        Some(json!({
            "table_id": "018f8c4b-a1de-7d57-b0d8-d98f1ef2443a",
            "table_uri": "gs://bucket/path",
            "latest_backfilled_version": 0
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("UNAUTHORIZED")
    );
    Ok(())
}

#[tokio::test]
async fn duplicate_creates_return_conflict() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("CONFLICT")
    );

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("CONFLICT")
    );

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/tables",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "analytics",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": [],
            "storage_location": "s3://bucket/main/analytics/events"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::POST,
        "/tables",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "analytics",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": [],
            "storage_location": "s3://bucket/main/analytics/events"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("CONFLICT")
    );
    Ok(())
}

#[tokio::test]
async fn tenant_workspace_isolation() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_b",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, list_a) = uc_request(
        &router,
        Method::GET,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        list_a
            .get("catalogs")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );

    let (status, list_b) = uc_request(
        &router,
        Method::GET,
        "/catalogs",
        "tenant_a",
        "workspace_b",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        list_b
            .get("catalogs")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );

    let (status, list_other_tenant) = uc_request(
        &router,
        Method::GET,
        "/catalogs",
        "tenant_b",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        list_other_tenant
            .get("catalogs")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(0)
    );
    Ok(())
}

#[tokio::test]
async fn list_schemas_does_not_leak_prefix_collisions() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main2"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main2"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, listed) = uc_request(
        &router,
        Method::GET,
        "/schemas?catalog_name=main",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    let schemas = listed
        .get("schemas")
        .and_then(Value::as_array)
        .ok_or_else(|| "schemas should be an array".to_string())?;
    assert!(schemas.is_empty(), "unexpected schema leak: {schemas:?}");
    Ok(())
}

#[tokio::test]
async fn create_table_requires_all_uc_required_fields() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::POST,
        "/tables",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "analytics"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("BAD_REQUEST")
    );
    Ok(())
}

#[tokio::test]
async fn list_tables_caps_max_results() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    for index in 0..60 {
        let table_name = format!("events_{index:02}");
        let (status, _) = uc_request(
            &router,
            Method::POST,
            "/tables",
            "tenant_a",
            "workspace_a",
            Some(json!({
                "name": table_name,
                "catalog_name": "main",
                "schema_name": "analytics",
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "columns": [],
                "storage_location": format!("s3://bucket/main/analytics/{table_name}")
            })),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
    }

    let (status, listed) = uc_request(
        &router,
        Method::GET,
        "/tables?catalog_name=main&schema_name=analytics&max_results=1000",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    let tables = listed
        .get("tables")
        .and_then(Value::as_array)
        .ok_or_else(|| "tables should be an array".to_string())?;
    assert_eq!(tables.len(), 50);
    assert_eq!(
        listed.get("next_page_token").and_then(Value::as_str),
        Some("50")
    );
    Ok(())
}

#[tokio::test]
async fn list_catalogs_omits_next_page_token_when_no_more_results() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, listed) = uc_request(
        &router,
        Method::GET,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert!(
        listed.get("next_page_token").is_none(),
        "next_page_token should be omitted when no more results: {listed:?}"
    );
    Ok(())
}

#[tokio::test]
async fn list_catalogs_rejects_negative_max_results() -> Result<(), String> {
    let router = test_router();

    let (status, body) = uc_request(
        &router,
        Method::GET,
        "/catalogs?max_results=-1",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("BAD_REQUEST")
    );
    Ok(())
}

#[tokio::test]
async fn list_schemas_rejects_invalid_page_token_before_catalog_lookup() -> Result<(), String> {
    let router = test_router();

    let (status, body) = uc_request(
        &router,
        Method::GET,
        "/schemas?catalog_name=missing&page_token=not-a-number",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("BAD_REQUEST")
    );
    Ok(())
}

#[tokio::test]
async fn list_tables_rejects_invalid_max_results_before_parent_lookup() -> Result<(), String> {
    let router = test_router();

    let (status, body) = uc_request(
        &router,
        Method::GET,
        "/tables?catalog_name=missing&schema_name=missing&max_results=-1",
        "tenant_a",
        "workspace_a",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("BAD_REQUEST")
    );
    Ok(())
}

#[tokio::test]
async fn create_table_rejects_invalid_table_type_value() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::POST,
        "/tables",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "analytics",
            "table_type": "INVALID",
            "data_source_format": "DELTA",
            "columns": [],
            "storage_location": "s3://bucket/main/analytics/events"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("BAD_REQUEST")
    );
    Ok(())
}

#[tokio::test]
async fn create_table_rejects_invalid_data_source_format_value() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::POST,
        "/tables",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "analytics",
            "table_type": "EXTERNAL",
            "data_source_format": "TSV",
            "columns": [],
            "storage_location": "s3://bucket/main/analytics/events"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("BAD_REQUEST")
    );
    Ok(())
}

#[tokio::test]
async fn create_table_rejects_non_object_columns() -> Result<(), String> {
    let router = test_router();

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/catalogs",
        "tenant_a",
        "workspace_a",
        Some(json!({"name": "main"})),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = uc_request(
        &router,
        Method::POST,
        "/schemas",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "analytics",
            "catalog_name": "main"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = uc_request(
        &router,
        Method::POST,
        "/tables",
        "tenant_a",
        "workspace_a",
        Some(json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "analytics",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": [1, {"name": "good"}],
            "storage_location": "s3://bucket/main/analytics/events"
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        body.pointer("/error/error_code").and_then(Value::as_str),
        Some("BAD_REQUEST")
    );
    Ok(())
}

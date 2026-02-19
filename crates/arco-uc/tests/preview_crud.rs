//! Integration tests for Unity Catalog preview CRUD interoperability endpoints.

use std::sync::Arc;

use arco_core::storage::MemoryBackend;
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;

fn test_router() -> Router {
    let backend = Arc::new(MemoryBackend::new());
    let state = UnityCatalogState::new(backend);
    unity_catalog_router(state)
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
            "data_source_format": "DELTA"
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
            "schema_name": "analytics"
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

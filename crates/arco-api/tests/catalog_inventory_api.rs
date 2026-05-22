//! Catalog inventory API coverage.

use anyhow::{Context, Result};
use axum::http::{Method, StatusCode};
use tower::ServiceExt;

#[path = "support/query.rs"]
mod support;

use support::{helpers, test_router_with_backend};

#[tokio::test]
async fn catalog_inventory_reports_manifest_identity_and_safe_counts() -> Result<()> {
    let (router, _backend) = test_router_with_backend();
    seed_workspace(
        router.clone(),
        "test-workspace",
        "analytics",
        "bronze",
        "events",
    )
    .await?;

    let response = helpers::make_request(Method::GET, "/api/v1/catalog/inventory", None)?;
    let response = router.oneshot(response).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    let payload: serde_json::Value = serde_json::from_slice(&body).with_context(|| {
        format!(
            "parse inventory response: {}",
            String::from_utf8_lossy(&body)
        )
    })?;

    assert_eq!(payload["manifest_type"], "catalog_inventory_snapshot");
    assert_eq!(payload["version"], 1);
    assert_eq!(payload["catalog_snapshot_version"], 3);

    let manifest_id = payload["catalog_manifest_id"]
        .as_str()
        .context("catalog_manifest_id missing")?;
    assert_eq!(manifest_id.len(), 20);
    assert!(manifest_id.bytes().all(|byte| byte.is_ascii_digit()));

    assert!(
        payload["published_at"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "published_at should be present"
    );

    assert_eq!(payload["counts"]["catalogs"], 1);
    assert_eq!(payload["counts"]["schemas"], 1);
    assert_eq!(payload["counts"]["tables"], 1);
    assert_eq!(payload["counts"]["columns"], 2);

    let families = payload["object_families"]
        .as_array()
        .context("object_families missing")?;
    assert_eq!(families.len(), 4);
    assert!(families.iter().any(|family| {
        family["name"] == "tables"
            && family["available"] == true
            && family["row_count"] == 1
            && family["query_handle"] == "system.catalog.tables"
    }));

    let raw = String::from_utf8_lossy(&body);
    assert!(!raw.contains("manifest_path"));
    assert!(!raw.contains("snapshot_path"));
    assert!(!raw.contains(".parquet"));
    assert!(!raw.contains("credential"));
    assert!(!raw.contains("grant"));

    Ok(())
}

#[tokio::test]
async fn catalog_inventory_is_scoped_to_request_workspace() -> Result<()> {
    let (router, _backend) = test_router_with_backend();
    seed_workspace(
        router.clone(),
        "test-workspace",
        "analytics",
        "bronze",
        "events",
    )
    .await?;
    seed_workspace(
        router.clone(),
        "other-workspace",
        "finance",
        "silver",
        "payments",
    )
    .await?;

    let request = helpers::make_request(Method::GET, "/api/v1/catalog/inventory", None)?;
    let response = router
        .clone()
        .oneshot(request)
        .await
        .map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    let payload: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(payload["counts"]["catalogs"], 1);
    assert_eq!(payload["counts"]["schemas"], 1);
    assert_eq!(payload["counts"]["tables"], 1);
    assert_eq!(payload["counts"]["columns"], 2);

    let request = helpers::make_request_with_scope(
        Method::GET,
        "/api/v1/catalog/inventory",
        "test-tenant",
        "other-workspace",
        None,
    )?;
    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    let payload: serde_json::Value = serde_json::from_slice(&body)?;

    assert_eq!(payload["counts"]["catalogs"], 1);
    assert_eq!(payload["counts"]["schemas"], 1);
    assert_eq!(payload["counts"]["tables"], 1);
    assert_eq!(payload["counts"]["columns"], 2);

    Ok(())
}

#[tokio::test]
async fn catalog_inventory_reports_missing_snapshot_as_not_found() -> Result<()> {
    let (router, _backend) = test_router_with_backend();

    let request = helpers::make_request(Method::GET, "/api/v1/catalog/inventory", None)?;
    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    let payload: serde_json::Value = serde_json::from_slice(&body).with_context(|| {
        format!(
            "parse missing-snapshot response: {}",
            String::from_utf8_lossy(&body)
        )
    })?;
    assert_eq!(payload["code"], "NOT_FOUND");
    assert!(
        payload["message"]
            .as_str()
            .is_some_and(|message| message.contains("root.manifest.json")),
        "missing snapshot error should identify the missing root manifest"
    );

    Ok(())
}

async fn seed_workspace(
    router: axum::Router,
    workspace: &str,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<()> {
    post_json_in_workspace(
        router.clone(),
        workspace,
        "/api/v1/catalogs",
        serde_json::json!({
            "name": catalog,
            "description": format!("{catalog} catalog")
        }),
    )
    .await?;

    post_json_in_workspace(
        router.clone(),
        workspace,
        &format!("/api/v1/catalogs/{catalog}/schemas"),
        serde_json::json!({
            "name": schema,
            "description": format!("{schema} schema")
        }),
    )
    .await?;

    post_json_in_workspace(
        router,
        workspace,
        &format!("/api/v1/catalogs/{catalog}/schemas/{schema}/tables"),
        serde_json::json!({
            "name": table,
            "description": format!("{table} table"),
            "columns": [
                {"name": "id", "data_type": "STRING", "nullable": false},
                {"name": "event_time", "data_type": "TIMESTAMP", "nullable": true}
            ]
        }),
    )
    .await?;

    Ok(())
}

async fn post_json_in_workspace(
    router: axum::Router,
    workspace: &str,
    uri: &str,
    body: serde_json::Value,
) -> Result<()> {
    let request =
        helpers::make_request_with_scope(Method::POST, uri, "test-tenant", workspace, Some(body))?;
    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::CREATED);
    Ok(())
}

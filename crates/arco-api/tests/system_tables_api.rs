//! Query API coverage for tenant-visible `system.*` tables.

use anyhow::Result;
use axum::http::{Method, StatusCode};
use tower::ServiceExt;

#[path = "support/query.rs"]
mod support;

use support::{helpers, seed_catalog, test_router};

#[tokio::test]
async fn query_can_select_from_system_catalog_namespaces() -> Result<()> {
    let router = seed_catalog(test_router()).await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT name FROM system.catalog.namespaces ORDER BY name"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn query_can_select_from_system_lineage_edges() -> Result<()> {
    let router = seed_catalog(test_router()).await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT count(*) AS edge_count FROM system.lineage.edges"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

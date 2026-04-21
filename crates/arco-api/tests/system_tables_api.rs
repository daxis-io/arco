//! Query API coverage for tenant-visible `system.*` tables.

use anyhow::Result;
use axum::http::{Method, StatusCode};
use tower::ServiceExt;

use arco_catalog::CatalogReader;
use arco_core::storage::StorageBackend;
use arco_core::{CatalogDomain, ScopedStorage};
use arco_flow::orchestration::compactor::{OrchestrationManifest, OrchestrationManifestPointer};
use arco_flow::orchestration_manifest_pointer_path;

#[path = "support/query.rs"]
mod support;

use support::{
    helpers, seed_catalog, seed_orchestration_router, seed_orchestration_router_with_l0_only,
    seed_orchestration_storage, test_router, test_router_with_backend,
};

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

#[tokio::test]
async fn query_can_select_from_system_orchestration_runs() -> Result<()> {
    let router = seed_orchestration_router().await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT run_id FROM system.orchestration.runs ORDER BY run_id"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn query_can_select_from_system_orchestration_partition_status() -> Result<()> {
    let router = seed_orchestration_router().await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT asset_key, stale_reason_code FROM system.orchestration.partition_status ORDER BY asset_key"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn query_can_select_from_system_orchestration_runs_when_state_is_only_in_l0() -> Result<()> {
    let router = seed_orchestration_router_with_l0_only().await?;

    let (status, rows): (_, Vec<serde_json::Value>) = helpers::post_json(
        router,
        "/api/v1/query?format=json",
        serde_json::json!({
            "sql": "SELECT run_id FROM system.orchestration.runs ORDER BY run_id"
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("run_id"),
        Some(&serde_json::Value::String("run_01".to_string()))
    );
    Ok(())
}

#[tokio::test]
async fn query_can_select_from_empty_system_orchestration_schedule_ticks() -> Result<()> {
    let router = seed_orchestration_router().await?;

    let (status, rows): (_, Vec<serde_json::Value>) = helpers::post_json(
        router,
        "/api/v1/query?format=json",
        serde_json::json!({
            "sql": "SELECT count(*) AS tick_count FROM system.orchestration.schedule_ticks"
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .get("tick_count")
            .and_then(serde_json::Value::as_u64),
        Some(0)
    );
    Ok(())
}

#[tokio::test]
async fn query_catalog_tables_do_not_require_unrelated_lineage_artifacts() -> Result<()> {
    let (router, backend) = test_router_with_backend();
    let router = seed_catalog(router).await?;
    let storage_backend: std::sync::Arc<dyn StorageBackend> = backend.clone();
    let storage = ScopedStorage::new(storage_backend, "test-tenant", "test-workspace")?;
    let reader = CatalogReader::new(storage.clone());
    let lineage_paths = reader.get_mintable_paths(CatalogDomain::Lineage).await?;
    assert!(!lineage_paths.is_empty());
    for path in lineage_paths {
        storage.delete(&path).await?;
    }

    let (status, rows): (_, Vec<serde_json::Value>) = helpers::post_json(
        router,
        "/api/v1/query?format=json",
        serde_json::json!({
            "sql": "SELECT name FROM catalog.namespaces ORDER BY name"
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("name"),
        Some(&serde_json::Value::String("analytics".to_string()))
    );
    Ok(())
}

#[tokio::test]
async fn query_system_catalog_tables_do_not_require_unrelated_orchestration_artifacts() -> Result<()>
{
    let (router, backend) = test_router_with_backend();
    let router = seed_catalog(router).await?;
    let storage_backend: std::sync::Arc<dyn StorageBackend> = backend;
    let storage = ScopedStorage::new(storage_backend, "test-tenant", "test-workspace")?;
    seed_orchestration_storage(&storage, true).await?;

    let pointer_bytes = storage
        .get_raw(orchestration_manifest_pointer_path())
        .await?;
    let pointer: OrchestrationManifestPointer = serde_json::from_slice(&pointer_bytes)?;
    let manifest_bytes = storage.get_raw(&pointer.manifest_path).await?;
    let manifest: OrchestrationManifest = serde_json::from_slice(&manifest_bytes)?;
    let runs_path = manifest
        .base_snapshot
        .tables
        .runs
        .as_ref()
        .map(|artifact| artifact.path().to_string())
        .expect("seeded base snapshot runs path");
    storage.delete(&runs_path).await?;

    let (status, rows): (_, Vec<serde_json::Value>) = helpers::post_json(
        router,
        "/api/v1/query?format=json",
        serde_json::json!({
            "sql": "SELECT name FROM system.catalog.namespaces ORDER BY name"
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("name"),
        Some(&serde_json::Value::String("analytics".to_string()))
    );
    Ok(())
}

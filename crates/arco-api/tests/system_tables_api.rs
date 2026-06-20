//! Query API coverage for tenant-visible `system.*` tables.

use anyhow::Result;
use axum::http::{Method, StatusCode};
use bytes::Bytes;
use chrono::Utc;
use tower::ServiceExt;

use arco_catalog::CatalogReader;
use arco_core::storage::StorageBackend;
use arco_core::{CatalogDomain, ScopedStorage, WritePrecondition};
use arco_flow::orchestration::compactor::{
    CatalogRunIndexRow, OrchestrationManifest, OrchestrationManifestPointer, RunState,
    TableArtifact, TaskState, write_catalog_run_index,
};
use arco_flow::orchestration_manifest_pointer_path;

#[path = "support/query.rs"]
mod support;

use support::{
    helpers, seed_catalog, seed_orchestration_router, seed_orchestration_router_with_l0_only,
    seed_orchestration_storage, test_router, test_router_with_backend,
};

const ORCHESTRATION_SYSTEM_TABLES: &[&str] = &[
    "runs",
    "tasks",
    "catalog_run_index",
    "dep_satisfaction",
    "timers",
    "dispatch_outbox",
    "sensor_state",
    "sensor_evals",
    "partition_status",
    "schedule_definitions",
    "schedule_state",
    "schedule_ticks",
    "backfills",
    "backfill_chunks",
    "run_key_conflicts",
];

const DEFERRED_CATALOG_PRODUCT_SYSTEM_TABLES: &[(&str, &str)] = &[
    ("access", "grants"),
    ("access", "compiled_permissions"),
    ("access", "audit"),
    ("access", "auth_denies"),
    ("access", "credential_mints"),
    ("storage", "credentials"),
    ("storage", "external_locations"),
    ("storage", "managed_roots"),
    ("storage", "workspace_bindings"),
    ("catalog", "volumes"),
    ("catalog", "functions"),
    ("catalog", "registered_models"),
    ("catalog", "model_versions"),
    ("governance", "attachments"),
];

fn catalog_run_index_row(
    org_id: &str,
    workspace_id: &str,
    run_id: &str,
    task_key: &str,
    asset_key: &str,
) -> CatalogRunIndexRow {
    let now = Utc::now();
    let (target_namespace, target_table) = asset_key
        .split_once('.')
        .map_or((None, Some(asset_key.to_string())), |(namespace, table)| {
            (Some(namespace.to_string()), Some(table.to_string()))
        });

    CatalogRunIndexRow {
        schema_version: 1,
        org_id: org_id.to_string(),
        workspace_id: workspace_id.to_string(),
        run_id: run_id.to_string(),
        task_key: task_key.to_string(),
        plan_id: "plan_01".to_string(),
        run_key: None,
        kind: Some("materialization".to_string()),
        reference_id: None,
        source_type: Some("delta".to_string()),
        run_status: RunState::Succeeded,
        cancel_requested: false,
        task_status: TaskState::Succeeded,
        asset_key: Some(asset_key.to_string()),
        target_namespace,
        target_table,
        partition_key: None,
        attempt: 1,
        attempt_id: Some(format!("{run_id}_{task_key}_attempt_01")),
        requires_visible_output: false,
        materialization_id: Some(format!("{run_id}_{task_key}_mat_01")),
        output_visibility_state: None,
        published_at: Some(now),
        publish_error: None,
        delta_table: Some(asset_key.to_string()),
        delta_version: Some(1),
        delta_partition: None,
        execution_lineage_ref: None,
        started_at: Some(now),
        last_heartbeat_at: None,
        triggered_at: now,
        completed_at: Some(now),
        updated_at: now,
        code_version: None,
        error_message: None,
        row_version: format!("{run_id}_{task_key}_row_01"),
    }
}

async fn seed_catalog_run_index_manifest_with_multiple_orgs(storage: &ScopedStorage) -> Result<()> {
    let current_org_path =
        "state/orchestration/base/base_catalog_run_index/catalog_run_index/test-tenant.parquet";
    let other_org_path =
        "state/orchestration/base/base_catalog_run_index/catalog_run_index/other-tenant.parquet";

    let current_row = catalog_run_index_row(
        "test-tenant",
        "test-workspace",
        "run_current",
        "extract",
        "analytics.daily",
    );
    let other_row = catalog_run_index_row(
        "other-tenant",
        "test-workspace",
        "run_other",
        "extract",
        "analytics.other",
    );

    storage
        .put_raw(
            current_org_path,
            write_catalog_run_index(&[current_row])?,
            WritePrecondition::DoesNotExist,
        )
        .await?;
    storage
        .put_raw(
            other_org_path,
            write_catalog_run_index(&[other_row])?,
            WritePrecondition::DoesNotExist,
        )
        .await?;

    let mut manifest = OrchestrationManifest::new("01KSN3SYSTEMTABLECATALOG");
    manifest.manifest_id = "00000000000000000000".to_string();
    manifest.base_snapshot.snapshot_id = Some("base_catalog_run_index".to_string());
    manifest.base_snapshot.published_at = Utc::now();
    manifest
        .base_snapshot
        .tables
        .catalog_run_index_by_org
        .insert(
            "test-tenant".to_string(),
            TableArtifact::legacy(current_org_path),
        );
    manifest
        .base_snapshot
        .tables
        .catalog_run_index_by_org
        .insert(
            "other-tenant".to_string(),
            TableArtifact::legacy(other_org_path),
        );

    let manifest_path = format!(
        "state/orchestration/manifests/{}.json",
        manifest.manifest_id
    );
    storage
        .put_raw(
            &manifest_path,
            Bytes::from(serde_json::to_vec(&manifest)?),
            WritePrecondition::DoesNotExist,
        )
        .await?;

    let pointer = OrchestrationManifestPointer {
        manifest_id: manifest.manifest_id,
        manifest_path,
        epoch: 0,
        parent_pointer_hash: None,
        updated_at: Utc::now(),
    };
    storage
        .put_raw(
            orchestration_manifest_pointer_path(),
            Bytes::from(serde_json::to_vec(&pointer)?),
            WritePrecondition::DoesNotExist,
        )
        .await?;

    Ok(())
}

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
async fn query_can_select_from_system_catalog_commits() -> Result<()> {
    let router = seed_catalog(test_router()).await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT commit_ulid, snapshot_version FROM system.catalog.commits ORDER BY published_at DESC"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn query_does_not_expose_manifest_paths_in_system_catalog_commits() -> Result<()> {
    let router = seed_catalog(test_router()).await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT manifest_path FROM system.catalog.commits"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn query_does_not_expose_system_search_token_postings() -> Result<()> {
    let router = seed_catalog(test_router()).await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT * FROM system.search.token_postings"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn query_does_not_expose_legacy_search_token_postings() -> Result<()> {
    let router = seed_catalog(test_router()).await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT * FROM search.token_postings"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn query_does_not_expose_internal_orchestration_idempotency_keys() -> Result<()> {
    let router = seed_orchestration_router().await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT * FROM system.orchestration.idempotency_keys"
        })),
    )?;

    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn query_does_not_expose_deferred_catalog_product_system_tables() -> Result<()> {
    let router = seed_catalog(test_router()).await?;

    for (schema, table) in DEFERRED_CATALOG_PRODUCT_SYSTEM_TABLES {
        let request = helpers::make_request(
            Method::POST,
            "/api/v1/query?format=json",
            Some(serde_json::json!({
                "sql": format!("SELECT * FROM system.{schema}.{table}")
            })),
        )?;

        let response = router
            .clone()
            .oneshot(request)
            .await
            .map_err(|err| match err {})?;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "system.{schema}.{table} must stay unavailable until its authoritative projection exists"
        );
    }

    Ok(())
}

#[tokio::test]
async fn query_system_catalog_tables_are_scoped_to_request_workspace() -> Result<()> {
    let (router, _backend) = test_router_with_backend();
    seed_catalog_in_workspace(router.clone(), "test-workspace", "analytics", "events").await?;
    seed_catalog_in_workspace(router.clone(), "other-workspace", "finance", "payments").await?;

    let (status, rows): (_, Vec<serde_json::Value>) = helpers::post_json(
        router,
        "/api/v1/query?format=json",
        serde_json::json!({
            "sql": "SELECT name FROM system.catalog.tables ORDER BY name"
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    let table_names: Vec<&str> = rows
        .iter()
        .filter_map(|row| row.get("name").and_then(serde_json::Value::as_str))
        .collect();
    assert_eq!(table_names, vec!["events"]);
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
async fn query_can_select_count_from_every_system_orchestration_table() -> Result<()> {
    let router = seed_orchestration_router().await?;

    for table in ORCHESTRATION_SYSTEM_TABLES {
        let request = helpers::make_request(
            Method::POST,
            "/api/v1/query?format=json",
            Some(serde_json::json!({
                "sql": format!(
                    "SELECT count(*) AS row_count FROM system.orchestration.{table}"
                )
            })),
        )?;

        let response = router
            .clone()
            .oneshot(request)
            .await
            .map_err(|err| match err {})?;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "system.orchestration.{table} should be queryable"
        );
    }

    Ok(())
}

#[tokio::test]
async fn query_catalog_run_index_reads_only_request_tenant_artifact() -> Result<()> {
    let (router, backend) = test_router_with_backend();
    let storage_backend: std::sync::Arc<dyn StorageBackend> = backend;
    let storage = ScopedStorage::new(storage_backend, "test-tenant", "test-workspace")?;
    seed_catalog_run_index_manifest_with_multiple_orgs(&storage).await?;

    let (status, rows): (_, Vec<serde_json::Value>) = helpers::post_json(
        router,
        "/api/v1/query?format=json",
        serde_json::json!({
            "sql": "SELECT org_id, workspace_id, run_id, task_key, target_namespace, target_table FROM system.orchestration.catalog_run_index ORDER BY org_id"
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("org_id"),
        Some(&serde_json::Value::String("test-tenant".to_string()))
    );
    assert_eq!(
        rows[0].get("workspace_id"),
        Some(&serde_json::Value::String("test-workspace".to_string()))
    );
    assert_eq!(
        rows[0].get("run_id"),
        Some(&serde_json::Value::String("run_current".to_string()))
    );
    assert_eq!(
        rows[0].get("target_namespace"),
        Some(&serde_json::Value::String("analytics".to_string()))
    );
    assert_eq!(
        rows[0].get("target_table"),
        Some(&serde_json::Value::String("daily".to_string()))
    );
    Ok(())
}

#[tokio::test]
async fn query_exposes_system_orchestration_runs_when_state_is_only_in_l0() -> Result<()> {
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

async fn seed_catalog_in_workspace(
    router: axum::Router,
    workspace: &str,
    namespace: &str,
    table: &str,
) -> Result<()> {
    let request = helpers::make_request_with_scope(
        Method::POST,
        "/api/v1/namespaces",
        "test-tenant",
        workspace,
        Some(serde_json::json!({
            "name": namespace,
            "description": format!("{namespace} namespace")
        })),
    )?;
    let response = router
        .clone()
        .oneshot(request)
        .await
        .map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::CREATED);

    let request = helpers::make_request_with_scope(
        Method::POST,
        &format!("/api/v1/namespaces/{namespace}/tables"),
        "test-tenant",
        workspace,
        Some(serde_json::json!({
            "name": table,
            "description": format!("{table} table"),
            "columns": [
                {"name": "id", "data_type": "STRING", "nullable": false}
            ]
        })),
    )?;
    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    assert_eq!(response.status(), StatusCode::CREATED);

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

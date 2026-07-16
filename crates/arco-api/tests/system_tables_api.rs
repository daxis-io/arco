//! Query API coverage for tenant-visible `system.*` tables.

use anyhow::Result;
use axum::http::{Method, StatusCode};
use bytes::Bytes;
use chrono::{TimeZone as _, Utc};
use tower::ServiceExt;

use std::io::Cursor;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arco_api::server::ServerBuilder;
use arco_catalog::CatalogReader;
use arco_catalog::manifest::{CatalogDomainManifest, DomainManifestPointer, SnapshotFile};
use arco_catalog::parquet_util::{
    WorkspaceSnapshotCatalogRecord, workspace_snapshot_schema, write_workspace_snapshots,
};
use arco_catalog::workspace_snapshot::RetentionStatus;
use arco_core::storage::{ObjectMeta, StorageBackend, WriteResult};
use arco_core::{CatalogDomain, CatalogPaths, MemoryBackend, ScopedStorage, WritePrecondition};
use arco_flow::orchestration::compactor::{
    CatalogRunIndexRow, OrchestrationManifest, OrchestrationManifestPointer, RunState,
    TableArtifact, TaskState, write_catalog_run_index,
};
use arco_flow::orchestration_manifest_pointer_path;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

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

const SNAPSHOT_ID: &str = "snap_01ARZ3NDEKTSV4RRFFQ69G5FAV";
const PARENT_SNAPSHOT_ID: &str = "snap_01ARZ3NDEKTSV4RRFFQ69G5FAW";

#[derive(Debug, Default)]
struct DenyListBackend {
    inner: MemoryBackend,
    deny_list: AtomicBool,
}

impl DenyListBackend {
    fn deny_list(&self) {
        self.deny_list.store(true, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl StorageBackend for DenyListBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        if self.deny_list.load(Ordering::SeqCst) {
            return Err(arco_core::Error::Internal {
                message: format!("list forbidden during snapshot query: {prefix}"),
            });
        }
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

fn snapshot_projection_bytes() -> Result<Bytes> {
    Ok(write_workspace_snapshots(&[
        WorkspaceSnapshotCatalogRecord::new(
            SNAPSHOT_ID,
            1,
            Utc.timestamp_opt(1_700_000_000, 0)
                .single()
                .expect("created timestamp"),
            Utc.timestamp_opt(1_800_000_000, 0)
                .single()
                .expect("retained timestamp"),
            RetentionStatus::Active,
            2,
            Some(PARENT_SNAPSHOT_ID.to_string()),
            true,
        )?,
    ])?)
}

fn snapshot_projection_with_extra_column() -> Result<Bytes> {
    let mut fields = workspace_snapshot_schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        "authority_manifest",
        DataType::Utf8,
        true,
    )));
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::new_empty(schema.clone());
    let mut cursor = Cursor::new(Vec::new());
    let mut writer = ArrowWriter::try_new(&mut cursor, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(Bytes::from(cursor.into_inner()))
}

async fn install_snapshot_projection(
    storage: &ScopedStorage,
    bytes: Bytes,
    selected: bool,
) -> Result<String> {
    let pointer_bytes = storage
        .get_raw(&CatalogPaths::domain_manifest_pointer(
            CatalogDomain::Catalog,
        ))
        .await?;
    let pointer: DomainManifestPointer = serde_json::from_slice(&pointer_bytes)?;
    let manifest_bytes = storage.get_raw(&pointer.manifest_path).await?;
    let mut manifest: CatalogDomainManifest = serde_json::from_slice(&manifest_bytes)?;
    let snapshot = manifest.snapshot.as_mut().expect("seeded catalog snapshot");
    let path = format!("{}/snapshots.parquet", snapshot.path.trim_end_matches('/'));
    storage
        .put_raw(&path, bytes.clone(), WritePrecondition::None)
        .await?;
    if selected {
        snapshot.add_file(SnapshotFile {
            path: "snapshots.parquet".to_string(),
            checksum_sha256: "11".repeat(32),
            byte_size: u64::try_from(bytes.len()).expect("projection size"),
            row_count: 1,
            position_range: None,
        });
        storage
            .put_raw(
                &pointer.manifest_path,
                Bytes::from(serde_json::to_vec(&manifest)?),
                WritePrecondition::None,
            )
            .await?;
    }
    Ok(path)
}

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
async fn snapshot_system_table_exposes_only_the_exact_safe_manifest_selected_schema() -> Result<()>
{
    let (router, backend) = test_router_with_backend();
    let router = seed_catalog(router).await?;
    let backend: Arc<dyn StorageBackend> = backend;
    let storage = ScopedStorage::new(backend, "test-tenant", "test-workspace")?;
    install_snapshot_projection(&storage, snapshot_projection_bytes()?, true).await?;

    let (status, rows): (_, Vec<serde_json::Value>) = helpers::post_json(
        router.clone(),
        "/api/v1/query?format=json",
        serde_json::json!({
            "sql": "SELECT * FROM system.catalog.snapshots"
        }),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(1, rows.len());
    let mut keys = rows[0]
        .as_object()
        .expect("row object")
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "created_at",
            "domain_count",
            "has_legacy_compatibility",
            "parent_snapshot_id",
            "record_version",
            "retained_until",
            "retention_status",
            "snapshot_id",
        ]
    );
    assert_eq!(rows[0]["snapshot_id"], SNAPSHOT_ID);
    assert_eq!(rows[0]["created_at"].as_i64(), Some(1_700_000_000_000));

    for forbidden in [
        "authority_manifest",
        "checkpoint_path",
        "creator_identity",
        "sha256",
        "relative_path",
        "archive_start_sequence",
        "relocation_root",
    ] {
        let request = helpers::make_request(
            Method::POST,
            "/api/v1/query?format=json",
            Some(serde_json::json!({
                "sql": format!("SELECT {forbidden} FROM system.catalog.snapshots")
            })),
        )?;
        let response = router
            .clone()
            .oneshot(request)
            .await
            .map_err(|error| match error {})?;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "must omit {forbidden}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn snapshot_system_table_ignores_a_physically_present_unselected_projection() -> Result<()> {
    let (router, backend) = test_router_with_backend();
    let router = seed_catalog(router).await?;
    let backend: Arc<dyn StorageBackend> = backend;
    let storage = ScopedStorage::new(backend, "test-tenant", "test-workspace")?;
    install_snapshot_projection(&storage, snapshot_projection_bytes()?, false).await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT * FROM system.catalog.snapshots"
        })),
    )?;
    let response = router
        .oneshot(request)
        .await
        .map_err(|error| match error {})?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn snapshot_system_table_rejects_selected_projection_with_extra_columns() -> Result<()> {
    let (router, backend) = test_router_with_backend();
    let router = seed_catalog(router).await?;
    let backend: Arc<dyn StorageBackend> = backend;
    let storage = ScopedStorage::new(backend, "test-tenant", "test-workspace")?;
    install_snapshot_projection(&storage, snapshot_projection_with_extra_column()?, true).await?;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT * FROM system.catalog.snapshots"
        })),
    )?;
    let response = router
        .oneshot(request)
        .await
        .map_err(|error| match error {})?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    Ok(())
}

#[tokio::test]
async fn snapshot_system_table_selected_projection_requires_no_storage_listing() -> Result<()> {
    let backend = Arc::new(DenyListBackend::default());
    let storage_backend: Arc<dyn StorageBackend> = backend.clone();
    let router = ServerBuilder::new()
        .debug(true)
        .storage_backend(storage_backend.clone())
        .build()
        .test_router();
    let router = seed_catalog(router).await?;
    let storage = ScopedStorage::new(storage_backend, "test-tenant", "test-workspace")?;
    install_snapshot_projection(&storage, snapshot_projection_bytes()?, true).await?;
    backend.deny_list();

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/query?format=json",
        Some(serde_json::json!({
            "sql": "SELECT snapshot_id FROM system.catalog.snapshots"
        })),
    )?;
    let response = router
        .oneshot(request)
        .await
        .map_err(|error| match error {})?;
    assert_eq!(response.status(), StatusCode::OK);
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
    let storage_backend: Arc<dyn StorageBackend> = backend;
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
    let storage_backend: Arc<dyn StorageBackend> = backend.clone();
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
    let storage_backend: Arc<dyn StorageBackend> = backend;
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

//! Engine-style smoke suites for UC facade and Arco-native Delta APIs.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arco_api::config::{Config, Posture};
use arco_api::server::ServerBuilder;
use arco_catalog::CatalogReader;
use arco_catalog::metastore::events::{
    CatalogObjectRecord, GrantRecord, LifecycleState, MetastoreEvent, MetastoreMutation,
    PrincipalKind, PrincipalRecord,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_core::storage::{MemoryBackend, StorageBackend};
use arco_core::{ControlPlaneScope, ScopedStorage};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use serde::Deserialize;
use serde_json::{Value, json};
use tower::ServiceExt as _;
use uuid::Uuid;

const SMOKE_PRINCIPAL: &str = "user_engine_smoke";

fn test_config() -> Config {
    let mut config = Config::default();
    config.debug = true;
    config.posture = Posture::Dev;
    config.unity_catalog.enabled = true;
    config
}

/// Seeds the production permission source with the exact schema authority the
/// smoke flow exercises. The table route is intentionally fail-closed now, so
/// an authenticated test principal must not inherit DDL authority implicitly.
async fn seed_create_table_authority(
    backend: &Arc<dyn StorageBackend>,
    tenant: &str,
    workspace: &str,
    schema_id: &str,
) {
    let scope = ControlPlaneScope::workspace_alias(tenant, workspace).expect("control scope");
    let storage = ScopedStorage::new(Arc::clone(backend), tenant, workspace)
        .expect("scoped metastore storage");
    let ledger = MetastoreLedger::new(storage);
    for event in [
        MetastoreEvent::new_scoped(
            &scope,
            "event_engine_smoke_principal",
            1,
            MetastoreMutation::PrincipalUpserted(PrincipalRecord {
                principal_id: SMOKE_PRINCIPAL.to_string(),
                name: SMOKE_PRINCIPAL.to_string(),
                principal_kind: PrincipalKind::User,
                owner: "test-owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1,
                properties: BTreeMap::new(),
            }),
        ),
        MetastoreEvent::new_scoped(
            &scope,
            "event_engine_smoke_schema",
            2,
            MetastoreMutation::CatalogObjectUpserted(CatalogObjectRecord {
                object_id: schema_id.to_string(),
                object_type: "SCHEMA".to_string(),
                qualified_name: schema_id.to_string(),
                owner: "test-owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 2,
                properties: BTreeMap::new(),
            }),
        ),
        MetastoreEvent::new_scoped(
            &scope,
            "event_engine_smoke_grant",
            3,
            MetastoreMutation::GrantUpserted(GrantRecord {
                grant_id: "grant_engine_smoke_create_table".to_string(),
                object_id: schema_id.to_string(),
                object_type: "SCHEMA".to_string(),
                principal_id: SMOKE_PRINCIPAL.to_string(),
                privilege: "CREATE_TABLE".to_string(),
                owner: "test-owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 3,
                properties: BTreeMap::new(),
            }),
        ),
    ] {
        ledger.append_event(&event).await.expect("seed authority");
    }
}

#[derive(Debug)]
struct JsonResponse {
    status: StatusCode,
    body: Value,
}

async fn send_json(
    router: &axum::Router,
    method: Method,
    uri: &str,
    tenant: &str,
    workspace: &str,
    body: Value,
    extra_headers: Option<BTreeMap<&str, String>>,
) -> JsonResponse {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("X-User-Id", SMOKE_PRINCIPAL);

    if let Some(extra_headers) = extra_headers {
        for (key, value) in extra_headers {
            builder = builder.header(key, value);
        }
    }

    let request = builder
        .body(Body::from(body.to_string()))
        .expect("build request");
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("route response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("read body");
    let body = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).expect("parse json body")
    };

    JsonResponse { status, body }
}

#[derive(Debug, Deserialize)]
struct StagedCommit {
    staged_path: String,
    staged_version: String,
}

#[derive(Debug, Deserialize)]
struct NativeCommit {
    version: i64,
}

#[derive(Debug, Deserialize)]
struct NativeTable {
    id: String,
}

async fn run_engine_flow(engine: &str) {
    let tenant = "acme";
    let workspace = "analytics";
    let catalog_name = format!("{engine}_catalog");
    let schema_name = format!("{engine}_schema");
    let schema_id = format!("{catalog_name}.{schema_name}");
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    seed_create_table_authority(&backend, tenant, workspace, &schema_id).await;
    let server = ServerBuilder::new()
        .config(test_config())
        .storage_backend(backend.clone())
        .build();
    let router = server.test_router();

    let uc_prefix = "/api/2.1/unity-catalog";
    let native_prefix = "/api/v1";

    let table_name = format!("{engine}_events");
    let full_table_name = format!("{catalog_name}.{schema_name}.{table_name}");
    let storage_location = format!("gs://smoke/{tenant}/{workspace}/{engine}/events");

    let create_catalog = send_json(
        &router,
        Method::POST,
        &format!("{uc_prefix}/catalogs"),
        tenant,
        workspace,
        json!({ "name": &catalog_name }),
        None,
    )
    .await;
    assert_eq!(create_catalog.status, StatusCode::OK);

    let create_schema = send_json(
        &router,
        Method::POST,
        &format!("{uc_prefix}/schemas"),
        tenant,
        workspace,
        json!({
            "name": &schema_name,
            "catalog_name": &catalog_name
        }),
        None,
    )
    .await;
    assert_eq!(create_schema.status, StatusCode::OK);

    let create_table = send_json(
        &router,
        Method::POST,
        &format!("{uc_prefix}/tables"),
        tenant,
        workspace,
        json!({
            "name": &table_name,
            "catalog_name": &catalog_name,
            "schema_name": &schema_name,
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "columns": [],
            "storage_location": &storage_location,
            "comment": format!("{engine} smoke table")
        }),
        None,
    )
    .await;
    assert_eq!(create_table.status, StatusCode::OK);

    let discover = send_json(
        &router,
        Method::GET,
        &format!("{uc_prefix}/tables/{full_table_name}"),
        tenant,
        workspace,
        json!({}),
        None,
    )
    .await;
    assert_eq!(discover.status, StatusCode::OK);

    let catalog_storage =
        ScopedStorage::new(backend.clone(), tenant, workspace).expect("scoped storage");
    let uc_table = CatalogReader::new(catalog_storage)
        .get_table_in_schema(&catalog_name, &schema_name, &table_name)
        .await
        .expect("read created UC table")
        .expect("created UC table");
    let uc_table_id = uc_table.id;
    let uc_table_uri = uc_table
        .location
        .unwrap_or_else(|| storage_location.clone());

    let mut uc_headers = BTreeMap::new();
    uc_headers.insert("Idempotency-Key", Uuid::now_v7().to_string());
    uc_headers.insert("X-Request-Id", format!("smoke-{engine}-uc"));

    let uc_commit = send_json(
        &router,
        Method::POST,
        &format!("{uc_prefix}/delta/preview/commits"),
        tenant,
        workspace,
        json!({
            "table_id": &uc_table_id,
            "table_uri": &uc_table_uri,
            "commit_info": {
                "version": 0,
                "timestamp": 1,
                "file_name": "00000000000000000000.json",
                "file_size": 32,
                "file_modification_timestamp": 1
            }
        }),
        Some(uc_headers),
    )
    .await;
    assert_eq!(uc_commit.status, StatusCode::OK);

    let uc_get_commits = send_json(
        &router,
        Method::GET,
        &format!("{uc_prefix}/delta/preview/commits"),
        tenant,
        workspace,
        json!({
            "table_id": &uc_table_id,
            "table_uri": &uc_table_uri,
            "start_version": 0
        }),
        None,
    )
    .await;
    assert_eq!(uc_get_commits.status, StatusCode::OK);
    assert_eq!(
        uc_get_commits
            .body
            .get("commits")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );

    let native_namespace = format!("{engine}_ns");
    let native_create_namespace = send_json(
        &router,
        Method::POST,
        &format!("{native_prefix}/namespaces"),
        tenant,
        workspace,
        json!({
            "name": &native_namespace
        }),
        None,
    )
    .await;
    assert_eq!(native_create_namespace.status, StatusCode::CREATED);

    let native_table_name = format!("{engine}_native_events");
    let native_create_table = send_json(
        &router,
        Method::POST,
        &format!("{native_prefix}/namespaces/{native_namespace}/tables"),
        tenant,
        workspace,
        json!({
            "name": &native_table_name,
            "columns": []
        }),
        None,
    )
    .await;
    assert_eq!(native_create_table.status, StatusCode::CREATED);
    let native_table: NativeTable =
        serde_json::from_value(native_create_table.body).expect("native table response");

    let native_stage = send_json(
        &router,
        Method::POST,
        &format!(
            "{native_prefix}/delta/tables/{}/commits/stage",
            native_table.id
        ),
        tenant,
        workspace,
        json!({
            "payload": format!("{{\"commitInfo\":{{\"engine\":\"{engine}\",\"ts\":1}}}}\\n")
        }),
        None,
    )
    .await;
    assert_eq!(native_stage.status, StatusCode::OK);

    let staged: StagedCommit = serde_json::from_value(native_stage.body).expect("stage response");

    let mut native_headers = BTreeMap::new();
    native_headers.insert("Idempotency-Key", Uuid::now_v7().to_string());
    let native_commit = send_json(
        &router,
        Method::POST,
        &format!("{native_prefix}/delta/tables/{}/commits", native_table.id),
        tenant,
        workspace,
        json!({
            "read_version": -1,
            "staged_path": staged.staged_path,
            "staged_version": staged.staged_version
        }),
        Some(native_headers),
    )
    .await;
    assert_eq!(native_commit.status, StatusCode::OK);
    let committed: NativeCommit =
        serde_json::from_value(native_commit.body).expect("native commit response");
    assert_eq!(committed.version, 0);
}

#[tokio::test]
async fn spark_engine_smoke_uc_and_native_delta() {
    run_engine_flow("spark").await;
}

#[tokio::test]
async fn delta_rs_engine_smoke_uc_and_native_delta() {
    run_engine_flow("delta-rs").await;
}

#[tokio::test]
async fn pyspark_engine_smoke_uc_and_native_delta() {
    run_engine_flow("pyspark").await;
}

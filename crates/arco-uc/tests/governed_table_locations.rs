//! #358 sibling-route closure: UC `POST /tables` validates the client
//! storage_location against published storage governance.
//!
//! The Iceberg REST surface already enforces governed locations for
//! create/register; this suite proves the UC facade's table-creation channel
//! enforces the same rules — governed scopes deny foreign locations with a
//! typed 400 (stale projections deny closed with 503) while ungoverned scopes
//! preserve current behavior unchanged.

use std::collections::BTreeMap;
use std::sync::Arc;

use arco_catalog::metastore::events::{
    ExternalLocationRecord, LifecycleState, MetastoreEvent, MetastoreMutation,
    StorageCredentialRecord, WorkspaceBindingRecord,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::metastore::projections::ProjectionRegistry;
use arco_catalog::metastore::publish::publish_current_metastore_projection;
use arco_core::storage::{MemoryBackend, StorageBackend};
use arco_core::{ControlPlaneScope, ScopedStorage};
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;

const TENANT: &str = "tenant1";
const WORKSPACE: &str = "workspace1";

#[tokio::test]
async fn ungoverned_scope_preserves_storage_location_behavior() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let app = router(Arc::clone(&backend));
    seed_catalog_and_schema(&app).await;

    let (status, body) =
        create_table(&app, "events", "gs://attacker-bucket/warehouse/events").await;

    assert_eq!(
        status,
        StatusCode::OK,
        "without storage governance the client storage_location must pass through: {body}"
    );
}

#[tokio::test]
async fn governed_scope_rejects_foreign_storage_location() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let app = router(Arc::clone(&backend));
    seed_catalog_and_schema(&app).await;
    seed_and_publish_governance(&backend).await;

    let (status, body) =
        create_table(&app, "events", "gs://attacker-bucket/warehouse/events").await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "got: {body}");
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("not governed"),
        "expected governed-path denial, got: {body}"
    );
}

#[tokio::test]
async fn governed_scope_accepts_location_under_bound_authority() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let app = router(Arc::clone(&backend));
    seed_catalog_and_schema(&app).await;
    seed_and_publish_governance(&backend).await;

    let (status, body) = create_table(&app, "events", "gs://bucket/warehouse/orders/events").await;

    assert_eq!(
        status,
        StatusCode::OK,
        "a location under the bound external location must be accepted: {body}"
    );
}

#[tokio::test]
async fn governed_scope_stale_projection_denies_closed() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let app = router(Arc::clone(&backend));
    seed_catalog_and_schema(&app).await;
    seed_and_publish_governance(&backend).await;
    // A newer metastore commit without republication leaves the projection
    // stale; table creation must deny closed instead of enforcing from
    // pre-revocation state.
    let scope = ControlPlaneScope::workspace_alias(TENANT, WORKSPACE).expect("scope");
    MetastoreLedger::new(scoped(&backend))
        .append_event(&MetastoreEvent::new_scoped(
            &scope,
            "event_stale_004",
            4,
            MetastoreMutation::StorageCredentialUpserted(StorageCredentialRecord {
                credential_id: "cred_02".to_string(),
                name: "lakehouse-standby".to_string(),
                cloud: "gcs".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_005,
                properties: BTreeMap::new(),
                secret_material_ref: None,
                encrypted_payload: None,
            }),
        ))
        .await
        .expect("append stale event");

    let (status, body) = create_table(&app, "events", "gs://bucket/warehouse/orders/events").await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "got: {body}");
}

fn router(backend: Arc<dyn StorageBackend>) -> Router {
    unity_catalog_router(UnityCatalogState::new(backend))
}

fn scoped(backend: &Arc<dyn StorageBackend>) -> ScopedStorage {
    ScopedStorage::new(Arc::clone(backend), TENANT, WORKSPACE).expect("scoped storage")
}

async fn seed_catalog_and_schema(app: &Router) {
    let (status, body) = uc_request(app, "POST", "/catalogs", json!({"name": "main"})).await;
    assert_eq!(status, StatusCode::OK, "create catalog: {body}");
    let (status, body) = uc_request(
        app,
        "POST",
        "/schemas",
        json!({"name": "analytics", "catalog_name": "main"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create schema: {body}");
}

async fn create_table(app: &Router, name: &str, storage_location: &str) -> (StatusCode, Value) {
    uc_request(
        app,
        "POST",
        "/tables",
        json!({
            "name": name,
            "catalog_name": "main",
            "schema_name": "analytics",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": [],
            "storage_location": storage_location
        }),
    )
    .await
}

async fn uc_request(app: &Router, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("route UC request");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let parsed = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("json payload")
    };
    (status, parsed)
}

async fn seed_and_publish_governance(backend: &Arc<dyn StorageBackend>) {
    let scope = ControlPlaneScope::workspace_alias(TENANT, WORKSPACE).expect("scope");
    let events = vec![
        MetastoreEvent::new_scoped(
            &scope,
            "event_001",
            1,
            MetastoreMutation::StorageCredentialUpserted(StorageCredentialRecord {
                credential_id: "cred_01".to_string(),
                name: "lakehouse-prod".to_string(),
                cloud: "gcs".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_000,
                properties: BTreeMap::new(),
                secret_material_ref: None,
                encrypted_payload: None,
            }),
        ),
        MetastoreEvent::new_scoped(
            &scope,
            "event_002",
            2,
            MetastoreMutation::ExternalLocationUpserted(ExternalLocationRecord {
                location_id: "loc_orders".to_string(),
                name: "orders".to_string(),
                url: "gs://bucket/warehouse/orders/".to_string(),
                credential_id: "cred_01".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_001,
                properties: BTreeMap::new(),
            }),
        ),
        MetastoreEvent::new_scoped(
            &scope,
            "event_003",
            3,
            MetastoreMutation::WorkspaceBindingUpserted(WorkspaceBindingRecord {
                binding_id: "binding_orders".to_string(),
                workspace_id: WORKSPACE.to_string(),
                object_id: "loc_orders".to_string(),
                object_type: "EXTERNAL_LOCATION".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_002,
                properties: BTreeMap::new(),
            }),
        ),
    ];
    let storage = scoped(backend);
    let ledger = MetastoreLedger::new(storage.clone());
    for event in events {
        ledger.append_event(&event).await.expect("append event");
    }
    publish_current_metastore_projection(&storage, &ProjectionRegistry::default())
        .await
        .expect("publish storage governance projection");
}

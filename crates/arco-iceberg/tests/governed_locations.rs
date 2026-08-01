//! Storage-governance enforcement for client-supplied Iceberg table locations
//! (#358).
//!
//! When a storage-governance projection is published for the tenant/workspace
//! scope, `create_table` and `register_table` must validate client-supplied
//! locations against the governed-path model. When storage governance has
//! never been enabled for the scope, current behavior is preserved.

use std::collections::BTreeMap;
use std::sync::Arc;

use arco_catalog::metastore::events::{
    ExternalLocationRecord, LifecycleState, MetastoreEvent, MetastoreMutation,
    StorageCredentialRecord, WorkspaceBindingRecord,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::metastore::projections::ProjectionRegistry;
use arco_catalog::metastore::publish::publish_current_metastore_projection;
use arco_catalog::write_options::WriteOptions;
use arco_catalog::{CatalogWriter, Tier1Compactor};
use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition};
use arco_core::{ControlPlaneScope, ScopedStorage};
use arco_iceberg::router::iceberg_router;
use arco_iceberg::state::{IcebergConfig, IcebergState, Tier1CompactorFactory};
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

const TENANT: &str = "tenant1";
const WORKSPACE: &str = "workspace1";

#[tokio::test]
async fn ungoverned_scope_preserves_client_location_behavior() {
    let backend = setup_catalog().await;
    let app = iceberg_router(crud_state(Arc::clone(&backend)));

    let response = app
        .oneshot(create_table_request(
            "events",
            Some("gs://attacker-bucket/warehouse/events"),
        ))
        .await
        .expect("response");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "without storage governance the client location must pass through unchanged: {}",
        body_string(response).await
    );
}

#[tokio::test]
async fn governed_scope_rejects_foreign_bucket_location() {
    let backend = setup_catalog().await;
    seed_and_publish_governance(&backend).await;
    let app = iceberg_router(crud_state(Arc::clone(&backend)));

    let response = app
        .oneshot(create_table_request(
            "events",
            Some("gs://attacker-bucket/warehouse/events"),
        ))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = body_string(response).await;
    assert!(
        body.contains("not governed"),
        "expected governed-path denial, got: {body}"
    );
}

#[tokio::test]
async fn governed_scope_rejects_other_tenant_prefix_alias() {
    let backend = setup_catalog().await;
    seed_and_publish_governance(&backend).await;
    let app = iceberg_router(crud_state(Arc::clone(&backend)));

    // The bucket matches the governed one, but the path is outside every
    // declared authority (an aliased "tenant=victim" advertisement).
    let response = app
        .oneshot(create_table_request(
            "events",
            Some("gs://bucket/tenant=victim/workspace=prod/warehouse/t"),
        ))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = body_string(response).await;
    assert!(
        body.contains("not governed"),
        "expected governed-path denial, got: {body}"
    );
}

#[tokio::test]
async fn governed_scope_rejects_unparseable_client_location() {
    let backend = setup_catalog().await;
    seed_and_publish_governance(&backend).await;
    let app = iceberg_router(crud_state(Arc::clone(&backend)));

    let response = app
        .oneshot(create_table_request(
            "events",
            Some("warehouse/relative/events"),
        ))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = body_string(response).await;
    assert!(
        body.contains("Invalid table location"),
        "expected governed-URI parse denial, got: {body}"
    );
}

#[tokio::test]
async fn governed_scope_accepts_location_under_bound_external_location() {
    let backend = setup_catalog().await;
    seed_and_publish_governance(&backend).await;
    let app = iceberg_router(crud_state(Arc::clone(&backend)));

    let response = app
        .oneshot(create_table_request(
            "events",
            Some("gs://bucket/warehouse/orders/events"),
        ))
        .await
        .expect("response");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a location under the bound external location must be accepted: {}",
        body_string(response).await
    );
}

#[tokio::test]
async fn governed_scope_default_location_still_works() {
    let backend = setup_catalog().await;
    seed_and_publish_governance(&backend).await;
    let app = iceberg_router(crud_state(Arc::clone(&backend)));

    let response = app
        .oneshot(create_table_request("events", None))
        .await
        .expect("response");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "server-derived default locations are not client-controlled: {}",
        body_string(response).await
    );
}

#[tokio::test]
async fn governed_scope_stale_projection_denies_closed() {
    let backend = setup_catalog().await;
    seed_and_publish_governance(&backend).await;
    // A newer metastore commit without republication leaves the projection
    // stale; location validation must deny closed rather than enforce from
    // pre-revocation state.
    append_governance_event(
        &backend,
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
        "event_stale_004",
        4,
    )
    .await;
    let app = iceberg_router(crud_state(Arc::clone(&backend)));

    let response = app
        .oneshot(create_table_request(
            "events",
            Some("gs://bucket/warehouse/orders/events"),
        ))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body_string(response).await;
    assert!(
        body.contains("Storage governance state unavailable"),
        "expected deny-closed stale projection error, got: {body}"
    );
}

/// F3: a location-bearing table property is a client-controlled location
/// channel; under governance a foreign `write.data.path` is rejected with a
/// typed 400 naming the offending property.
#[tokio::test]
async fn governed_scope_rejects_foreign_write_data_path_property() {
    let backend = setup_catalog().await;
    seed_and_publish_governance(&backend).await;
    let app = iceberg_router(crud_state(Arc::clone(&backend)));

    let response = app
        .oneshot(create_table_request_with_properties(
            "events",
            Some("gs://bucket/warehouse/orders/events"),
            serde_json::json!({
                "write.data.path": "gs://attacker-bucket/exfil/data"
            }),
        ))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = body_string(response).await;
    assert!(
        body.contains("write.data.path") && body.contains("not governed"),
        "expected governed-path denial naming the offending property, got: {body}"
    );
}

/// F3: benign properties pass through untouched, and location-bearing
/// properties pointing inside the governed authority are accepted.
#[tokio::test]
async fn governed_scope_accepts_benign_and_governed_location_properties() {
    let backend = setup_catalog().await;
    seed_and_publish_governance(&backend).await;
    let app = iceberg_router(crud_state(Arc::clone(&backend)));

    let response = app
        .oneshot(create_table_request_with_properties(
            "events",
            Some("gs://bucket/warehouse/orders/events"),
            serde_json::json!({
                "write.parquet.compression-codec": "zstd",
                "commit.retry.num-retries": "4",
                "write.data.path": "gs://bucket/warehouse/orders/events/data"
            }),
        ))
        .await
        .expect("response");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "benign properties and governed location-bearing properties must pass: {}",
        body_string(response).await
    );
}

/// F3: when governance is not configured for the scope, location-bearing
/// properties are untouched and current behavior is preserved.
#[tokio::test]
async fn ungoverned_scope_ignores_location_bearing_properties() {
    let backend = setup_catalog().await;
    let app = iceberg_router(crud_state(Arc::clone(&backend)));

    let response = app
        .oneshot(create_table_request_with_properties(
            "events",
            None,
            serde_json::json!({
                "write.data.path": "gs://attacker-bucket/exfil/data"
            }),
        ))
        .await
        .expect("response");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "without storage governance the properties must pass through unchanged: {}",
        body_string(response).await
    );
}

#[tokio::test]
async fn governed_scope_register_table_rejects_ungoverned_metadata_location() {
    let backend = setup_catalog().await;
    seed_and_publish_governance(&backend).await;
    put_registerable_metadata(&backend, "gs://attacker-bucket/warehouse/evil").await;
    let app = iceberg_router(crud_state(Arc::clone(&backend)));

    let response = app
        .oneshot(register_table_request(
            "evil",
            "warehouse/staged/metadata/v1.metadata.json",
        ))
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = body_string(response).await;
    assert!(
        body.contains("not governed"),
        "expected governed-path denial for metadata-file location, got: {body}"
    );
}

#[tokio::test]
async fn governed_scope_register_table_accepts_governed_metadata_location() {
    let backend = setup_catalog().await;
    seed_and_publish_governance(&backend).await;
    put_registerable_metadata(&backend, "gs://bucket/warehouse/orders/registered").await;
    let app = iceberg_router(crud_state(Arc::clone(&backend)));

    let response = app
        .oneshot(register_table_request(
            "registered",
            "warehouse/staged/metadata/v1.metadata.json",
        ))
        .await
        .expect("response");

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a governed metadata-file location must be accepted: {}",
        body_string(response).await
    );
}

async fn setup_catalog() -> Arc<dyn StorageBackend> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let scoped = scoped(&backend);
    let compactor = Arc::new(Tier1Compactor::new(scoped.clone()));
    let writer = CatalogWriter::new(scoped).with_sync_compactor(compactor);
    writer.initialize().await.expect("init catalog");
    writer
        .create_namespace("sales", None, WriteOptions::default())
        .await
        .expect("create namespace");
    backend
}

fn scoped(backend: &Arc<dyn StorageBackend>) -> ScopedStorage {
    ScopedStorage::new(Arc::clone(backend), TENANT, WORKSPACE).expect("scoped storage")
}

fn crud_state(backend: Arc<dyn StorageBackend>) -> IcebergState {
    let config = IcebergConfig {
        allow_write: true,
        allow_table_crud: true,
        ..Default::default()
    };
    IcebergState::with_config(backend, config)
        .with_compactor_factory(Arc::new(Tier1CompactorFactory))
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

async fn append_governance_event(
    backend: &Arc<dyn StorageBackend>,
    mutation: MetastoreMutation,
    event_id: &str,
    sequence: u64,
) {
    let scope = ControlPlaneScope::workspace_alias(TENANT, WORKSPACE).expect("scope");
    MetastoreLedger::new(scoped(backend))
        .append_event(&MetastoreEvent::new_scoped(
            &scope, event_id, sequence, mutation,
        ))
        .await
        .expect("append governance event");
}

async fn put_registerable_metadata(backend: &Arc<dyn StorageBackend>, location: &str) {
    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": 1_700_000_000_000i64,
        "last-column-id": 1,
        "current-schema-id": 0,
        "schemas": [{
            "schema-id": 0,
            "type": "struct",
            "fields": [{"id": 1, "name": "id", "type": "long", "required": true}]
        }],
        "current-snapshot-id": null,
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "properties": {},
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 0,
        "refs": {},
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}]
    });
    scoped(backend)
        .put_raw(
            "warehouse/staged/metadata/v1.metadata.json",
            bytes::Bytes::from(serde_json::to_vec(&metadata).expect("serialize metadata")),
            WritePrecondition::None,
        )
        .await
        .expect("stage metadata file");
}

fn create_table_request(name: &str, location: Option<&str>) -> Request<Body> {
    create_table_request_with_properties(name, location, serde_json::json!({}))
}

fn create_table_request_with_properties(
    name: &str,
    location: Option<&str>,
    properties: serde_json::Value,
) -> Request<Body> {
    let mut body = serde_json::json!({
        "name": name,
        "schema": {
            "schema-id": 0,
            "type": "struct",
            "fields": [{"id": 1, "name": "id", "type": "long", "required": true}]
        },
        "properties": properties
    });
    if let Some(location) = location {
        body["location"] = serde_json::Value::String(location.to_string());
    }
    json_request("POST", "/v1/arco/namespaces/sales/tables", &body)
}

fn register_table_request(name: &str, metadata_location: &str) -> Request<Body> {
    json_request(
        "POST",
        "/v1/arco/namespaces/sales/register",
        &serde_json::json!({
            "name": name,
            "metadata-location": metadata_location
        }),
    )
}

fn json_request(method: &str, uri: &str, body: &serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .body(Body::from(body.to_string()))
        .expect("request")
}

async fn body_string(response: axum::response::Response) -> String {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    String::from_utf8_lossy(&body).to_string()
}

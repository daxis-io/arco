//! #358 sibling-route closure: UC `POST /tables` validates the client
//! `storage_location` against published storage governance.
//!
//! The Iceberg REST surface already enforces governed locations for
//! create/register; this suite proves the UC facade's table-creation channel
//! enforces the same rules — governed scopes deny foreign locations with a
//! typed 400 (stale projections deny closed with 503) while ungoverned scopes
//! preserve current behavior unchanged.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "route integration tests use panic-based assertions and direct JSON fixture indexing"
)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arco_catalog::authz::compiler::{CompiledPermissionRow, CompiledPermissionSet};
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::metastore::events::{
    ExternalLocationRecord, LifecycleState, MetastoreEvent, MetastoreMutation,
    StorageCredentialRecord, WorkspaceBindingRecord,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::metastore::projections::ProjectionRegistry;
use arco_catalog::metastore::publish::publish_current_metastore_projection;
use arco_core::storage::{MemoryBackend, StorageBackend};
use arco_core::{ControlPlaneScope, ScopedStorage};
use arco_uc::context::UnityCatalogRequestContext;
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
    unity_catalog_router(
        UnityCatalogState::new(backend).with_compiled_permissions(create_table_permissions()),
    )
}

/// Principal that the harness authorizes for table DDL.
///
/// `POST /tables` requires `CREATE_TABLE` on the target schema, so every
/// request the harness sends carries a trusted principal and the state carries
/// a compiled view granting that principal the privilege on the schemas these
/// tests use. Requests without both are denied 403 — that is the fail-closed
/// posture under test, not harness noise.
const TEST_PRINCIPAL: &str = "user_test";

fn create_table_permissions() -> CompiledPermissionSet {
    let mut rows = Vec::new();
    for catalog in ["main", "main2"] {
        for schema in ["analytics", "default"] {
            let object_id = format!("{catalog}.{schema}");
            rows.push(CompiledPermissionRow {
                principal_id: TEST_PRINCIPAL.to_string(),
                object_id: object_id.clone(),
                object_type: "SCHEMA".to_string(),
                privilege: Privilege::CreateTable,
                source: "grant".to_string(),
                source_grant_id: Some(format!("grant_{catalog}_{schema}")),
                source_principal_id: TEST_PRINCIPAL.to_string(),
                source_object_id: object_id.clone(),
                inheritance_path: object_id,
                grant_option: false,
                group_snapshot_version: "groups-test".to_string(),
            });
        }
    }
    CompiledPermissionSet::new("event_test", "groups-test", true, rows)
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
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    request.extensions_mut().insert(UnityCatalogRequestContext {
        tenant: TENANT.to_string(),
        workspace: WORKSPACE.to_string(),
        request_id: "request-governed-table-locations".to_string(),
        user_id: Some(TEST_PRINCIPAL.to_string()),
        idempotency_key: None,
    });
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

// ---------------------------------------------------------------------------
// #358: `properties` is a second client-controlled location channel on this
// route. It was persisted verbatim into `RegisterTableInSchemaRequest`, so a
// governed `storage_location` was enough to smuggle a foreign data path.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn governed_scope_rejects_foreign_write_data_path_property() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let app = router(Arc::clone(&backend));
    seed_catalog_and_schema(&app).await;
    seed_and_publish_governance(&backend).await;

    let (status, body) = create_table_with_properties(
        &app,
        "events",
        "gs://bucket/warehouse/orders/events",
        json!({"write.data.path": "gs://attacker-bucket/exfil/data"}),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "got: {body}");
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("write.data.path") && message.contains("not governed"),
        "expected a denial naming the offending property, got: {message}"
    );
    assert_table_absent(&app, "events").await;
}

#[tokio::test]
async fn governed_scope_rejects_foreign_metadata_and_object_storage_path_properties() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let app = router(Arc::clone(&backend));
    seed_catalog_and_schema(&app).await;
    seed_and_publish_governance(&backend).await;

    for (index, property) in ["write.metadata.path", "write.object-storage.path"]
        .into_iter()
        .enumerate()
    {
        let (status, body) = create_table_with_properties(
            &app,
            &format!("events{index}"),
            "gs://bucket/warehouse/orders/events",
            json!({ property: "gs://attacker-bucket/exfil/objects" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{property}: {body}");
        let message = body["error"]["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(property) && message.contains("not governed"),
            "expected a denial naming {property}, got: {message}"
        );
        assert_table_absent(&app, &format!("events{index}")).await;
    }
}

#[tokio::test]
async fn governed_scope_accepts_governed_location_bearing_properties() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let app = router(Arc::clone(&backend));
    seed_catalog_and_schema(&app).await;
    seed_and_publish_governance(&backend).await;

    let (status, body) = create_table_with_properties(
        &app,
        "events",
        "gs://bucket/warehouse/orders/events",
        json!({
            "write.data.path": "gs://bucket/warehouse/orders/events/data",
            "delta.appendOnly": "true"
        }),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "governed property locations and benign properties must pass: {body}"
    );
}

#[tokio::test]
async fn ungoverned_scope_ignores_location_bearing_properties() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let app = router(Arc::clone(&backend));
    seed_catalog_and_schema(&app).await;

    let (status, body) = create_table_with_properties(
        &app,
        "events",
        "gs://attacker-bucket/warehouse/events",
        json!({"write.data.path": "gs://attacker-bucket/exfil/data"}),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::OK,
        "without storage governance the properties must pass through unchanged: {body}"
    );
}

#[tokio::test]
async fn governed_scope_stale_projection_denies_property_validation_closed() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let app = router(Arc::clone(&backend));
    seed_catalog_and_schema(&app).await;
    seed_and_publish_governance(&backend).await;

    let scope = ControlPlaneScope::workspace_alias(TENANT, WORKSPACE).expect("scope");
    MetastoreLedger::new(scoped(&backend))
        .append_event(&MetastoreEvent::new_scoped(
            &scope,
            "event_stale_property_004",
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

    let (status, body) = create_table_with_properties(
        &app,
        "events",
        "gs://bucket/warehouse/orders/events",
        json!({"write.data.path": "gs://bucket/warehouse/orders/events/data"}),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "got: {body}");
    assert_table_absent(&app, "events").await;
}

/// S2: a duplicate-slash spelling of the governed prefix is a different
/// physical object prefix, so it is rejected instead of resolving to the
/// declaring authority — through the location channel and the property
/// channel alike.
#[tokio::test]
async fn governed_scope_rejects_duplicate_slash_location_aliases() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let app = router(Arc::clone(&backend));
    seed_catalog_and_schema(&app).await;
    seed_and_publish_governance(&backend).await;

    let (status, body) = create_table(&app, "alias", "gs://bucket/warehouse//orders/events").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "got: {body}");

    let (status, body) = create_table(&app, "alias2", "gs://bucket//warehouse/orders/events").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "got: {body}");

    let (status, body) = create_table_with_properties(
        &app,
        "alias3",
        "gs://bucket/warehouse/orders/events",
        json!({"write.data.path": "gs://bucket/warehouse//orders/events/data"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "got: {body}");
}

/// S3: two principals in the same workspace, only one granted `CREATE_TABLE`.
/// The lesser principal must receive an indistinguishable 403 for governed and
/// ungoverned candidate locations alike — no governance oracle — and must
/// mutate no catalog state.
#[tokio::test]
async fn unauthorized_principal_cannot_distinguish_governed_from_ungoverned_locations() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let app = router(Arc::clone(&backend));
    seed_catalog_and_schema(&app).await;
    seed_and_publish_governance(&backend).await;

    // The granted principal can tell the two apart: that is the oracle the
    // lesser principal must not have.
    let (governed_status, _) =
        create_table(&app, "granted", "gs://bucket/warehouse/orders/granted").await;
    assert_eq!(governed_status, StatusCode::OK);
    let (ungoverned_status, _) =
        create_table(&app, "granted2", "gs://attacker-bucket/warehouse/x").await;
    assert_eq!(ungoverned_status, StatusCode::BAD_REQUEST);

    let mut responses = Vec::new();
    for (index, candidate) in [
        "gs://bucket/warehouse/orders/probe",
        "gs://attacker-bucket/warehouse/probe",
        "gs://bucket/warehouse//orders/probe",
        "not-a-uri",
    ]
    .into_iter()
    .enumerate()
    {
        let (status, body) = create_table_as(
            &app,
            "user_intruder",
            &format!("probe{index}"),
            candidate,
            json!({}),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "candidate {candidate} must be denied before governance is consulted: {body}"
        );
        responses.push(
            body["error"]["message"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        );
    }
    let first = responses.first().cloned().unwrap_or_default();
    assert!(
        responses.iter().all(|message| message == &first),
        "denial bodies must be indistinguishable across candidates: {responses:?}"
    );
    assert!(
        !first.contains("governed") && !first.contains("gs://"),
        "the 403 must not disclose governance state or the candidate path: {first}"
    );

    for index in 0..4 {
        assert_table_absent(&app, &format!("probe{index}")).await;
    }
}

async fn create_table_with_properties(
    app: &Router,
    name: &str,
    storage_location: &str,
    properties: Value,
) -> (StatusCode, Value) {
    create_table_as(app, TEST_PRINCIPAL, name, storage_location, properties).await
}

async fn create_table_as(
    app: &Router,
    principal: &str,
    name: &str,
    storage_location: &str,
    properties: Value,
) -> (StatusCode, Value) {
    let mut body = json!({
        "name": name,
        "catalog_name": "main",
        "schema_name": "analytics",
        "table_type": "EXTERNAL",
        "data_source_format": "DELTA",
        "columns": [],
        "storage_location": storage_location
    });
    if let Some(object) = properties.as_object() {
        if !object.is_empty() {
            body["properties"] = properties;
        }
    }
    uc_request_as(app, principal, "POST", "/tables", body).await
}

async fn uc_request_as(
    app: &Router,
    principal: &str,
    method: &str,
    uri: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    request.extensions_mut().insert(UnityCatalogRequestContext {
        tenant: TENANT.to_string(),
        workspace: WORKSPACE.to_string(),
        request_id: "request-governed-table-locations".to_string(),
        user_id: Some(principal.to_string()),
        idempotency_key: None,
    });
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

async fn assert_table_absent(app: &Router, name: &str) {
    let mut request = Request::builder()
        .method("GET")
        .uri(format!("/tables/main.analytics.{name}"))
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .body(Body::empty())
        .expect("request");
    request.extensions_mut().insert(UnityCatalogRequestContext {
        tenant: TENANT.to_string(),
        workspace: WORKSPACE.to_string(),
        request_id: "request-governed-table-locations".to_string(),
        user_id: Some(TEST_PRINCIPAL.to_string()),
        idempotency_key: None,
    });
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("route UC request");
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "a denied create must not have registered table {name}"
    );
}

//! Task 5 coverage for UC credential vending over native storage governance.

use std::collections::BTreeMap;
use std::sync::Arc;

use arco_catalog::authz::compiler::{CompiledPermissionRow, CompiledPermissionSet};
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::metastore::events::{
    ExternalLocationRecord, LifecycleState, MetastoreEvent, MetastoreMutation,
    StorageCredentialRecord, WorkspaceBindingRecord,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::{CatalogWriter, RegisterTableRequest, Tier1Compactor, WriteOptions};
use arco_core::audit::{AuditAction, AuditEmitter, TestAuditSink};
use arco_core::storage::MemoryBackend;
use arco_core::{ControlPlaneScope, ScopedStorage};
use arco_uc::context::UnityCatalogRequestContext;
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn temporary_path_credentials_are_governed_scoped_redacted_and_audited() {
    let backend = Arc::new(MemoryBackend::new());
    seed_storage_governance(backend.clone(), true).await;
    let state = state_with_permissions(
        backend,
        vec![permission_row(
            "loc_orders",
            "EXTERNAL_LOCATION",
            Privilege::ReadFiles,
        )],
    );
    let app = unity_catalog_router(state);

    let response = app
        .oneshot(json_request(
            "POST",
            "/temporary-path-credentials",
            serde_json::json!({
                "url": "gs://bucket/warehouse/orders/day=1/",
                "operation": "READ",
                "requested_ttl_seconds": 7200
            }),
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = json_body(response).await;

    assert_eq!(payload["decision"], "allow");
    assert_eq!(payload["reason_code"], "allowed");
    assert_eq!(payload["provider"], "gcs");
    assert_eq!(payload["credential_kind"], "scoped_bearer");
    assert_eq!(payload["max_ttl_seconds"], 3600);
    assert_eq!(
        payload["authorized_path_prefixes"][0],
        "gs://bucket/warehouse/orders/day=1/"
    );
    assert!(
        payload["audit_event_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert!(
        payload["credentials"]
            .as_array()
            .is_some_and(|items| items.len() == 1)
    );
    assert_eq!(
        payload["credentials"][0]["prefix"],
        "gs://bucket/warehouse/orders/day=1/"
    );

    let serialized = serde_json::to_string(&payload).expect("serialize credential response");
    assert!(!serialized.contains("secret://cred/01"));
    assert!(!serialized.contains("encrypted-token"));
    assert!(!serialized.contains("arco.scoped_credential"));
}

#[tokio::test]
async fn temporary_path_credentials_deny_unbound_paths_and_emit_audit_decision() {
    let backend = Arc::new(MemoryBackend::new());
    seed_storage_governance(backend.clone(), false).await;
    let state = state_with_permissions(
        backend,
        vec![permission_row(
            "loc_orders",
            "EXTERNAL_LOCATION",
            Privilege::ReadFiles,
        )],
    );
    let app = unity_catalog_router(state);

    let response = app
        .oneshot(json_request(
            "POST",
            "/temporary-path-credentials",
            serde_json::json!({
                "url": "gs://bucket/warehouse/orders/day=1/",
                "operation": "READ",
                "requested_ttl_seconds": 300
            }),
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let payload = json_body(response).await;
    assert_eq!(payload["error"]["error_code"], "FORBIDDEN");
    assert!(payload["error"]["message"].as_str().is_some_and(|message| {
        message.contains("path_not_governed") || message.contains("workspace")
    }));
}

#[tokio::test]
async fn temporary_path_credentials_emit_redacted_allow_and_deny_audit_events() {
    let backend = Arc::new(MemoryBackend::new());
    seed_storage_governance(backend.clone(), true).await;
    let sink = Arc::new(TestAuditSink::new());
    let state = state_with_permissions(
        backend,
        vec![permission_row(
            "loc_orders",
            "EXTERNAL_LOCATION",
            Privilege::ReadFiles,
        )],
    )
    .with_audit_emitter(AuditEmitter::with_test_sink(sink.clone()));
    let app = unity_catalog_router(state);

    let response = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/temporary-path-credentials",
            serde_json::json!({
                "url": "gs://bucket/warehouse/orders/day=1/",
                "operation": "READ",
                "requested_ttl_seconds": 7200
            }),
        ))
        .await
        .expect("allow response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(json_request(
            "POST",
            "/temporary-path-credentials",
            serde_json::json!({
                "url": "gs://bucket/warehouse/orders/day=1/",
                "operation": "DELETE",
                "requested_ttl_seconds": 300
            }),
        ))
        .await
        .expect("deny response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let allow_events = sink.find_by_action(AuditAction::CredVendAllow);
    let deny_events = sink.find_by_action(AuditAction::CredVendDeny);
    assert_eq!(allow_events.len(), 1);
    assert_eq!(deny_events.len(), 1);
    assert_eq!(allow_events[0].decision_reason, "allowed");
    assert_eq!(deny_events[0].decision_reason, "unsupported_operation");

    let serialized = serde_json::to_string(&sink.events()).expect("serialize audit events");
    assert!(!serialized.contains("secret://cred/01"));
    assert!(!serialized.contains("encrypted-token"));
    assert!(!serialized.contains("arco.scoped_credential"));
}

#[tokio::test]
async fn temporary_path_credentials_deny_when_compiled_permissions_are_unavailable() {
    let backend = Arc::new(MemoryBackend::new());
    seed_storage_governance(backend.clone(), true).await;
    let sink = Arc::new(TestAuditSink::new());
    let state = UnityCatalogState::new(backend)
        .with_audit_emitter(AuditEmitter::with_test_sink(sink.clone()));
    let app = unity_catalog_router(state);

    let response = app
        .oneshot(json_request(
            "POST",
            "/temporary-path-credentials",
            serde_json::json!({
                "url": "gs://bucket/warehouse/orders/day=1/",
                "operation": "READ",
                "requested_ttl_seconds": 300
            }),
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let payload = json_body(response).await;
    assert!(
        payload["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("permissions_unavailable"))
    );

    let deny_events = sink.find_by_action(AuditAction::CredVendDeny);
    assert_eq!(deny_events.len(), 1);
    assert_eq!(deny_events[0].decision_reason, "permissions_unavailable");
}

#[tokio::test]
async fn temporary_path_credentials_ignore_spoofable_principal_headers() {
    let backend = Arc::new(MemoryBackend::new());
    seed_storage_governance(backend.clone(), true).await;
    let state = state_with_permissions(
        backend,
        vec![permission_row(
            "loc_orders",
            "EXTERNAL_LOCATION",
            Privilege::ReadFiles,
        )],
    );
    let app = unity_catalog_router(state);

    let response = app
        .oneshot(spoofed_header_json_request(
            "POST",
            "/temporary-path-credentials",
            serde_json::json!({
                "url": "gs://bucket/warehouse/orders/day=1/",
                "operation": "READ",
                "requested_ttl_seconds": 300
            }),
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let payload = json_body(response).await;
    assert!(
        payload["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("unauthenticated_principal"))
    );
}

#[tokio::test]
async fn temporary_table_credentials_resolve_table_location_through_native_catalog() {
    let backend = Arc::new(MemoryBackend::new());
    seed_storage_governance(backend.clone(), true).await;
    let table_id = seed_catalog_table(backend.clone()).await;
    let sink = Arc::new(TestAuditSink::new());
    let state = state_with_permissions(
        backend,
        vec![permission_row(&table_id, "TABLE", Privilege::Select)],
    )
    .with_audit_emitter(AuditEmitter::with_test_sink(sink.clone()));
    let app = unity_catalog_router(state);

    let response = app
        .oneshot(json_request(
            "POST",
            "/temporary-table-credentials",
            serde_json::json!({
                "table_id": table_id,
                "operation": "READ",
                "requested_ttl_seconds": 7200
            }),
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = json_body(response).await;

    assert_eq!(payload["decision"], "allow");
    assert_eq!(payload["reason_code"], "allowed");
    assert_eq!(payload["provider"], "gcs");
    assert_eq!(payload["credential_kind"], "scoped_bearer");
    assert_eq!(payload["max_ttl_seconds"], 3600);
    assert_eq!(
        payload["authorized_path_prefixes"][0],
        "gs://bucket/warehouse/orders/"
    );

    let audit_events = sink.find_by_action(AuditAction::CredVendAllow);
    assert_eq!(audit_events.len(), 1);
    assert_eq!(audit_events[0].decision_reason, "allowed");

    let serialized_response =
        serde_json::to_string(&payload).expect("serialize credential response");
    let serialized_audit = serde_json::to_string(&sink.events()).expect("serialize audit events");
    assert!(!serialized_response.contains("secret://cred/01"));
    assert!(!serialized_response.contains("encrypted-token"));
    assert!(!serialized_response.contains("arco.scoped_credential"));
    assert!(!serialized_audit.contains("secret://cred/01"));
    assert!(!serialized_audit.contains("encrypted-token"));
    assert!(!serialized_audit.contains("arco.scoped_credential"));
}

fn state_with_permissions(
    backend: Arc<MemoryBackend>,
    rows: Vec<CompiledPermissionRow>,
) -> UnityCatalogState {
    UnityCatalogState::new(backend).with_compiled_permissions(CompiledPermissionSet::new(
        "event_003",
        "groups-rev-7",
        true,
        rows,
    ))
}

fn permission_row(
    object_id: impl Into<String>,
    object_type: impl Into<String>,
    privilege: Privilege,
) -> CompiledPermissionRow {
    let object_id = object_id.into();
    CompiledPermissionRow {
        principal_id: "user_alice".to_string(),
        object_id: object_id.clone(),
        object_type: object_type.into(),
        privilege,
        source: "grant".to_string(),
        source_grant_id: Some("grant_credential_access".to_string()),
        source_principal_id: "user_alice".to_string(),
        source_object_id: object_id.clone(),
        inheritance_path: object_id,
        grant_option: false,
        group_snapshot_version: "groups-rev-7".to_string(),
    }
}

async fn seed_catalog_table(backend: Arc<MemoryBackend>) -> String {
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1").expect("scoped storage");
    let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
    let writer = CatalogWriter::new(storage).with_sync_compactor(compactor);
    writer.initialize().await.expect("initialize catalog");
    writer
        .create_namespace("analytics", None, WriteOptions::default())
        .await
        .expect("create namespace");
    writer
        .register_table(
            RegisterTableRequest {
                namespace: "analytics".to_string(),
                name: "orders".to_string(),
                description: None,
                location: Some("gs://bucket/warehouse/orders/".to_string()),
                format: Some("delta".to_string()),
                columns: Vec::new(),
            },
            WriteOptions::default(),
        )
        .await
        .expect("register table")
        .id
}

async fn seed_storage_governance(backend: Arc<MemoryBackend>, include_binding: bool) {
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1").expect("scoped storage");
    let ledger = MetastoreLedger::new(storage);
    let scope = ControlPlaneScope::workspace_alias("tenant1", "workspace1").expect("scope");
    for event in storage_events(&scope, include_binding) {
        ledger.append_event(&event).await.expect("append event");
    }
}

fn storage_events(scope: &ControlPlaneScope, include_binding: bool) -> Vec<MetastoreEvent> {
    let mut events = vec![
        MetastoreEvent::new_scoped(
            scope,
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
                secret_material_ref: Some("secret://cred/01".to_string()),
                encrypted_payload: Some("encrypted-token".to_string()),
            }),
        ),
        MetastoreEvent::new_scoped(
            scope,
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
    ];
    if include_binding {
        events.push(MetastoreEvent::new_scoped(
            scope,
            "event_003",
            3,
            MetastoreMutation::WorkspaceBindingUpserted(WorkspaceBindingRecord {
                binding_id: "binding_orders".to_string(),
                workspace_id: "workspace1".to_string(),
                object_id: "loc_orders".to_string(),
                object_type: "EXTERNAL_LOCATION".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_002,
                properties: BTreeMap::new(),
            }),
        ));
    }
    events
}

fn json_request(method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
    let mut request = spoofed_header_json_request(method, uri, body);
    request.extensions_mut().insert(UnityCatalogRequestContext {
        tenant: "tenant1".to_string(),
        workspace: "workspace1".to_string(),
        request_id: "request-credential-vending".to_string(),
        user_id: Some("user_alice".to_string()),
        idempotency_key: None,
    });
    request
}

fn spoofed_header_json_request(method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("X-Tenant-Id", "tenant1")
        .header("X-Workspace-Id", "workspace1")
        .header("X-User-Id", "user_alice")
        .body(Body::from(body.to_string()))
        .expect("request")
}

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&body).expect("json payload")
}

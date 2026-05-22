//! Task 4 coverage for storage-governance UC route gating.

use std::sync::Arc;

use arco_catalog::authz::compiler::{CompiledPermissionRow, CompiledPermissionSet};
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_core::ScopedStorage;
use arco_core::storage::MemoryBackend;
use arco_uc::context::UnityCatalogRequestContext;
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn storage_credential_routes_are_ledger_backed_and_redacted() {
    let state = state_with_metastore_manage(Arc::new(MemoryBackend::new()));
    let app = unity_catalog_router(state);

    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            serde_json::json!({
                "credential_id": "cred_01",
                "name": "lakehouse-prod",
                "cloud": "gcs",
                "owner": "owner",
                "secret_material_ref": "secret://cred/01",
                "encrypted_payload": "encrypted-token"
            }),
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let payload = json_body(response).await;
    assert_eq!(payload["credential_id"], "cred_01");
    assert_eq!(payload["name"], "lakehouse-prod");
    let serialized = serde_json::to_string(&payload).expect("serialize create response");
    assert!(!serialized.contains("secret://cred/01"));
    assert!(!serialized.contains("encrypted-token"));

    let response = app
        .clone()
        .oneshot(trusted_empty_request("GET", "/storage-credentials/cred_01"))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = json_body(response).await;
    assert_eq!(payload["credential_id"], "cred_01");
    let serialized = serde_json::to_string(&payload).expect("serialize get response");
    assert!(!serialized.contains("secret://cred/01"));
    assert!(!serialized.contains("encrypted-token"));

    let response = app
        .clone()
        .oneshot(trusted_empty_request("GET", "/storage-credentials"))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = json_body(response).await;
    let credentials = payload["storage_credentials"]
        .as_array()
        .expect("credential list");
    assert_eq!(credentials.len(), 1);
    let serialized = serde_json::to_string(&payload).expect("serialize list response");
    assert!(!serialized.contains("secret://cred/01"));
    assert!(!serialized.contains("encrypted-token"));
}

#[tokio::test]
async fn external_location_routes_use_ledger_backed_storage_governance() {
    let state = state_with_metastore_manage(Arc::new(MemoryBackend::new()));
    let app = unity_catalog_router(state);

    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            serde_json::json!({
                "credential_id": "cred_01",
                "name": "lakehouse-prod",
                "cloud": "gcs",
                "owner": "owner",
                "secret_material_ref": "secret://cred/01",
                "encrypted_payload": "encrypted-token"
            }),
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/external-locations",
            serde_json::json!({
                "location_id": "loc_orders",
                "name": "orders",
                "url": "gs://bucket/warehouse/orders",
                "credential_id": "cred_01",
                "owner": "owner"
            }),
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let payload = json_body(response).await;
    assert_eq!(payload["location_id"], "loc_orders");
    assert_eq!(payload["url"], "gs://bucket/warehouse/orders/");

    let response = app
        .clone()
        .oneshot(trusted_empty_request(
            "GET",
            "/external-locations/loc_orders",
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = json_body(response).await;
    assert_eq!(payload["location_id"], "loc_orders");
    assert_eq!(payload["credential_id"], "cred_01");

    let response = app
        .oneshot(trusted_json_request(
            "POST",
            "/external-locations",
            serde_json::json!({
                "location_id": "loc_orders_child",
                "name": "orders-child",
                "url": "gs://bucket/warehouse/orders/day=1",
                "credential_id": "cred_01",
                "owner": "owner"
            }),
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn storage_governance_routes_append_scoped_metastore_events() {
    let backend = Arc::new(MemoryBackend::new());
    let state = state_with_metastore_manage(backend.clone());
    let app = unity_catalog_router(state);

    let response = app
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            serde_json::json!({
                "credential_id": "cred_01",
                "name": "lakehouse-prod",
                "cloud": "gcs",
                "owner": "owner"
            }),
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let storage = ScopedStorage::new(backend, "tenant1", "workspace1").expect("scoped storage");
    let ledger = MetastoreLedger::new(storage);
    let events = ledger.load_events().await.expect("load events");
    assert_eq!(events.len(), 1);
    let scope = events[0].scope.as_ref().expect("durable event scope");
    assert_eq!(scope.tenant_id, "tenant1");
    assert_eq!(scope.workspace_id, "workspace1");
    assert_eq!(scope.metastore_id, "workspace1");
}

#[tokio::test]
async fn storage_credential_create_does_not_persist_request_secret_material() {
    let backend = Arc::new(MemoryBackend::new());
    let state = state_with_metastore_manage(backend.clone());
    let app = unity_catalog_router(state);

    let response = app
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            serde_json::json!({
                "credential_id": "cred_01",
                "name": "lakehouse-prod",
                "cloud": "gcs",
                "owner": "owner",
                "secret_material_ref": "secret://cred/01",
                "encrypted_payload": "encrypted-token"
            }),
        ))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let storage = ScopedStorage::new(backend, "tenant1", "workspace1").expect("scoped storage");
    let ledger = MetastoreLedger::new(storage);
    let events = ledger.load_events().await.expect("load events");
    let serialized = serde_json::to_string(&events).expect("serialize events");

    assert!(!serialized.contains("secret://cred/01"));
    assert!(!serialized.contains("encrypted-token"));
    assert!(!serialized.contains("secret_material_ref"));
    assert!(!serialized.contains("encrypted_payload"));
}

#[tokio::test]
async fn storage_governance_routes_deny_when_permissions_are_unavailable() {
    let state = UnityCatalogState::new(Arc::new(MemoryBackend::new()));
    let app = unity_catalog_router(state);

    let response = app
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            serde_json::json!({
                "credential_id": "cred_01",
                "name": "lakehouse-prod",
                "cloud": "gcs",
                "owner": "owner"
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
}

#[tokio::test]
async fn storage_governance_routes_ignore_spoofable_principal_headers() {
    let state = state_with_metastore_manage(Arc::new(MemoryBackend::new()));
    let app = unity_catalog_router(state);

    let response = app
        .oneshot(json_request(
            "POST",
            "/storage-credentials",
            serde_json::json!({
                "credential_id": "cred_01",
                "name": "lakehouse-prod",
                "cloud": "gcs",
                "owner": "owner"
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

fn state_with_metastore_manage(backend: Arc<MemoryBackend>) -> UnityCatalogState {
    UnityCatalogState::new(backend).with_compiled_permissions(CompiledPermissionSet::new(
        "event_admin",
        "groups-rev-7",
        true,
        vec![CompiledPermissionRow {
            principal_id: "user_alice".to_string(),
            object_id: "workspace1".to_string(),
            object_type: "METASTORE".to_string(),
            privilege: Privilege::Manage,
            source: "grant".to_string(),
            source_grant_id: Some("grant_metastore_manage".to_string()),
            source_principal_id: "user_alice".to_string(),
            source_object_id: "workspace1".to_string(),
            inheritance_path: "workspace1".to_string(),
            grant_option: false,
            group_snapshot_version: "groups-rev-7".to_string(),
        }],
    ))
}

fn trusted_json_request(method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
    let mut request = json_request(method, uri, body);
    request.extensions_mut().insert(trusted_context());
    request
}

fn trusted_empty_request(method: &str, uri: &str) -> Request<Body> {
    let mut request = empty_request(method, uri);
    request.extensions_mut().insert(trusted_context());
    request
}

fn trusted_context() -> UnityCatalogRequestContext {
    UnityCatalogRequestContext {
        tenant: "tenant1".to_string(),
        workspace: "workspace1".to_string(),
        request_id: "request-storage-governance".to_string(),
        user_id: Some("user_alice".to_string()),
        idempotency_key: None,
    }
}

fn json_request(method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
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

fn empty_request(method: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("X-Tenant-Id", "tenant1")
        .header("X-Workspace-Id", "workspace1")
        .body(Body::empty())
        .expect("request")
}

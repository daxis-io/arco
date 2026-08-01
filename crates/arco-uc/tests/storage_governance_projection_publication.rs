//! Production storage-governance projection publication (#362) and the
//! revocation-freshness budget at the UC route surface.
//!
//! The UC governance routes are the only production committers of the
//! metastore ledger, so they publish the projection commit-synchronously and
//! self-heal a stale projection on the next authorized admin request. These
//! tests drive the real route handlers end to end: commit through the
//! production path, assert the projection is published, and assert credential
//! vending stops returning 503 and enforces the newly published state.

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arco_catalog::authz::compiler::{CompiledPermissionRow, CompiledPermissionSet};
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::metastore::events::{
    ExternalLocationRecord, LifecycleState, MetastoreEvent, MetastoreMutation,
    WorkspaceBindingRecord,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::metastore::publish::load_published_storage_governance;
use arco_core::error::Result as CoreResult;
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_core::{ControlPlaneScope, ScopedStorage};
use arco_uc::context::UnityCatalogRequestContext;
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use tower::ServiceExt;

/// Committing storage-governance changes through the production UC routes
/// publishes the metastore projection, so deployed vending serves decisions
/// from published state instead of an eternal 503 (#362).
#[tokio::test]
async fn production_governance_commits_publish_projection_and_unblock_vending() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let state = governed_state(Arc::clone(&backend), "unpublished");
    let app = unity_catalog_router(state.clone());

    // Deny-closed baseline: nothing committed, nothing published.
    let response = app
        .clone()
        .oneshot(vending_request())
        .await
        .expect("vending response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    // Production path: commit a storage credential and an external location.
    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            credential_body(),
        ))
        .await
        .expect("credential response");
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/external-locations",
            location_body(),
        ))
        .await
        .expect("location response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let payload = json_body(response).await;
    let location_watermark = payload["ledger_watermark"]
        .as_str()
        .expect("location ledger watermark")
        .to_string();

    // The projection is published at the exact commit watermark.
    let scoped = scoped(&backend);
    let published = load_published_storage_governance(&scoped)
        .await
        .expect("projection must be published by the production commit path");
    assert_eq!(published.ledger_watermark, location_watermark);
    assert_eq!(published.state.list_external_locations().len(), 1);

    // Vending now serves from the published projection: no longer 503. The
    // request is denied 403 (permissions watermark mismatch / unbound path),
    // which is enforcement of the published state, not unavailability.
    let response = app
        .clone()
        .oneshot(vending_request())
        .await
        .expect("vending response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // Bind the location to the workspace (no production route exists for
    // bindings yet, so append the event directly; the projection is now
    // stale and vending denies closed again).
    append_binding_event(&backend, "event_binding_003", 3).await;
    let response = app
        .clone()
        .oneshot(vending_request())
        .await
        .expect("vending response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    // Self-heal: the next authorized admin request republishes even though the
    // mutation itself is rejected as a duplicate.
    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            credential_body(),
        ))
        .await
        .expect("duplicate credential response");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let published = load_published_storage_governance(&scoped)
        .await
        .expect("self-healed projection");
    assert_eq!(published.ledger_watermark, "event_binding_003");

    // With compiled permissions at the published watermark, vending allows the
    // governed path: the deployed decision comes from the published state.
    set_permissions_at_watermark(&state, "event_binding_003");
    let response = app
        .clone()
        .oneshot(vending_request())
        .await
        .expect("vending response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = json_body(response).await;
    assert_eq!(payload["decision"], "allow");
    assert_eq!(
        payload["authorized_path_prefixes"][0],
        "gs://bucket/warehouse/orders/day=1/"
    );
}

/// Revocation-freshness budget at the route surface: a revocation followed by
/// a stale projection denies closed (503, staleness half = 0), and once the
/// projection is fresh the revoked scope cannot be vended (TTL half bounds the
/// residual exposure to already-minted credentials).
#[tokio::test]
async fn revocation_denies_closed_while_stale_and_cannot_vend_once_fresh() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let state = governed_state(Arc::clone(&backend), "unpublished");
    let app = unity_catalog_router(state.clone());

    // Establish a governed, bound, published location through the production
    // path plus the binding event and a self-heal republish.
    for (uri, body) in [
        ("/storage-credentials", credential_body()),
        ("/external-locations", location_body()),
    ] {
        let response = app
            .clone()
            .oneshot(trusted_json_request("POST", uri, body))
            .await
            .expect("governance commit");
        assert_eq!(response.status(), StatusCode::CREATED);
    }
    append_binding_event(&backend, "event_binding_003", 3).await;
    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            credential_body(),
        ))
        .await
        .expect("self-heal request");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    set_permissions_at_watermark(&state, "event_binding_003");

    let response = app
        .clone()
        .oneshot(vending_request())
        .await
        .expect("vending response");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "governed path must vend before revocation"
    );

    // Revocation commits to the ledger; the projection is now stale.
    append_revocation_event(&backend, "event_revoked_004", 4).await;

    // Revocation + stale projection => deny closed (503), never vend.
    let response = app
        .clone()
        .oneshot(vending_request())
        .await
        .expect("vending response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    // Republish through the production self-heal path.
    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            credential_body(),
        ))
        .await
        .expect("self-heal request");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let published = load_published_storage_governance(&scoped(&backend))
        .await
        .expect("republished projection");
    assert_eq!(published.ledger_watermark, "event_revoked_004");

    // Revocation + fresh projection => the revoked scope cannot be vended.
    set_permissions_at_watermark(&state, "event_revoked_004");
    let response = app
        .clone()
        .oneshot(vending_request())
        .await
        .expect("vending response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

/// A publication failure after a durable commit fails loud (503) and the next
/// authorized admin request heals the projection, so no sequence of API calls
/// leaves vending deny-closed forever.
#[tokio::test]
async fn failed_publication_fails_loud_and_next_admin_request_heals() {
    let flaky = Arc::new(PublicationFaultBackend::new(Arc::new(MemoryBackend::new())));
    let backend: Arc<dyn StorageBackend> = flaky.clone();
    let state = governed_state(Arc::clone(&backend), "unpublished");
    let app = unity_catalog_router(state.clone());

    // Publication writes fail; the commit is durable but the route reports the
    // deny-closed consequence loudly.
    flaky.fail_publication_writes(true);
    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            credential_body(),
        ))
        .await
        .expect("credential response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = json_body(response).await;
    assert!(payload["error"]["message"].as_str().is_some_and(|message| {
        message.contains("storage_governance_projection_publication_failed")
            && message.contains("/storage-governance/projection/republish")
    }));

    let scoped = scoped(&backend);
    let events = MetastoreLedger::new(scoped.clone())
        .load_events()
        .await
        .expect("ledger events");
    assert_eq!(events.len(), 1, "the metastore commit must remain durable");
    assert!(
        load_published_storage_governance(&scoped).await.is_err(),
        "projection must still be unpublished after the failed publication"
    );

    // Publication recovers; the retried admin request is a duplicate (409) but
    // the self-heal republishes before validation.
    flaky.fail_publication_writes(false);
    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            credential_body(),
        ))
        .await
        .expect("retried credential response");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let published = load_published_storage_governance(&scoped)
        .await
        .expect("healed projection");
    assert_eq!(published.ledger_watermark, events[0].event_id);
    assert_eq!(published.state.list_storage_credentials().len(), 1);
}

/// Path-canonicalization poison-chain regression: a percent-bearing external
/// location URL (`100%25-complete`) previously published fine (201) but
/// persisted a canonical string with a bare `%` that failed every subsequent
/// projection load and metastore replay — permanent 503 for all vending on
/// the scope with no API recovery. The canonical form is now a parse fixed
/// point, so publish → load → replay → vend keeps working.
#[tokio::test]
async fn percent_bearing_location_url_round_trips_through_publish_load_and_vend() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let state = governed_state(Arc::clone(&backend), "unpublished");
    let app = unity_catalog_router(state.clone());

    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            credential_body(),
        ))
        .await
        .expect("credential response");
    assert_eq!(response.status(), StatusCode::CREATED);

    // The reviewer's poison input: a location URL with an escaped literal '%'.
    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/external-locations",
            serde_json::json!({
                "location_id": "loc_orders",
                "name": "orders",
                "url": "gs://bucket/warehouse/100%25-complete",
                "credential_id": "cred_01",
                "owner": "owner"
            }),
        ))
        .await
        .expect("location response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let payload = json_body(response).await;
    // The persisted canonical form re-encodes the literal '%' so it stays a
    // parse fixed point instead of an unreadable bare escape.
    assert_eq!(payload["url"], "gs://bucket/warehouse/100%25-complete/");

    // LOAD side of the chain: the published projection re-parses.
    let scoped = scoped(&backend);
    let published = load_published_storage_governance(&scoped)
        .await
        .expect("projection load must not be poisoned by the percent location");
    assert_eq!(
        published
            .state
            .get_external_location("loc_orders")
            .expect("percent-bearing location present")
            .path
            .canonical_uri(),
        "gs://bucket/warehouse/100%25-complete/"
    );

    // Replay side of the chain: subsequent governance POSTs (which rebuild
    // state via from_metastore_state) keep working.
    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/external-locations",
            serde_json::json!({
                "location_id": "loc_customers",
                "name": "customers",
                "url": "gs://bucket/warehouse/customers",
                "credential_id": "cred_01",
                "owner": "owner"
            }),
        ))
        .await
        .expect("follow-up location response");
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "metastore replay after the percent-bearing commit must keep working"
    );

    // Vend side of the chain: bind the location, republish, and vend under
    // the percent-bearing governed path.
    append_binding_event(&backend, "event_binding_004", 4).await;
    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            credential_body(),
        ))
        .await
        .expect("self-heal request");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    set_permissions_at_watermark(&state, "event_binding_004");

    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/temporary-path-credentials",
            serde_json::json!({
                "url": "gs://bucket/warehouse/100%25-complete/day=1/",
                "operation": "READ",
                "requested_ttl_seconds": 300
            }),
        ))
        .await
        .expect("vending response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = json_body(response).await;
    assert_eq!(payload["decision"], "allow");
    assert_eq!(
        payload["authorized_path_prefixes"][0],
        "gs://bucket/warehouse/100%25-complete/day=1/"
    );
}

/// #362 recovery path: after a failed commit-synchronous publication, the
/// admin republish route heals the stale projection without appending any
/// ledger event, so recovery is not contingent on a future governance POST.
#[tokio::test]
async fn republish_route_heals_projection_without_appending_ledger_events() {
    let flaky = Arc::new(PublicationFaultBackend::new(Arc::new(MemoryBackend::new())));
    let backend: Arc<dyn StorageBackend> = flaky.clone();
    let state = governed_state(Arc::clone(&backend), "unpublished");
    let app = unity_catalog_router(state.clone());

    // Durable commit whose synchronous publication fails.
    flaky.fail_publication_writes(true);
    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-credentials",
            credential_body(),
        ))
        .await
        .expect("credential response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let scoped = scoped(&backend);
    let events = MetastoreLedger::new(scoped.clone())
        .load_events()
        .await
        .expect("ledger events");
    assert_eq!(events.len(), 1, "the metastore commit must remain durable");
    assert!(
        load_published_storage_governance(&scoped).await.is_err(),
        "projection must be stale after the failed publication"
    );

    // Recovery: the republish route publishes at the current watermark and
    // appends no ledger event.
    flaky.fail_publication_writes(false);
    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-governance/projection/republish",
            serde_json::json!({}),
        ))
        .await
        .expect("republish response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = json_body(response).await;
    assert_eq!(payload["status"], "published");
    assert_eq!(payload["ledger_watermark"], events[0].event_id.as_str());

    let events_after = MetastoreLedger::new(scoped.clone())
        .load_events()
        .await
        .expect("ledger events after republish");
    assert_eq!(
        events_after.len(),
        1,
        "the republish route must not append ledger events"
    );
    let published = load_published_storage_governance(&scoped)
        .await
        .expect("healed projection");
    assert_eq!(published.ledger_watermark, events[0].event_id);
    assert_eq!(published.state.list_storage_credentials().len(), 1);

    // Idempotent: a second republish reports the pointer as already current.
    let response = app
        .clone()
        .oneshot(trusted_json_request(
            "POST",
            "/storage-governance/projection/republish",
            serde_json::json!({}),
        ))
        .await
        .expect("second republish response");
    assert_eq!(response.status(), StatusCode::OK);
    let payload = json_body(response).await;
    assert_eq!(payload["status"], "already_current");
}

fn scoped(backend: &Arc<dyn StorageBackend>) -> ScopedStorage {
    ScopedStorage::new(Arc::clone(backend), "tenant1", "workspace1").expect("scoped storage")
}

async fn append_binding_event(backend: &Arc<dyn StorageBackend>, event_id: &str, sequence: u64) {
    let scope = ControlPlaneScope::workspace_alias("tenant1", "workspace1").expect("scope");
    let event = MetastoreEvent::new_scoped(
        &scope,
        event_id,
        sequence,
        MetastoreMutation::WorkspaceBindingUpserted(WorkspaceBindingRecord {
            binding_id: "binding_orders".to_string(),
            workspace_id: "workspace1".to_string(),
            object_id: "loc_orders".to_string(),
            object_type: "EXTERNAL_LOCATION".to_string(),
            owner: "owner".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 1_800_000_000_002,
            properties: std::collections::BTreeMap::new(),
        }),
    );
    MetastoreLedger::new(scoped(backend))
        .append_event(&event)
        .await
        .expect("append binding event");
}

async fn append_revocation_event(backend: &Arc<dyn StorageBackend>, event_id: &str, sequence: u64) {
    let scope = ControlPlaneScope::workspace_alias("tenant1", "workspace1").expect("scope");
    let event = MetastoreEvent::new_scoped(
        &scope,
        event_id,
        sequence,
        MetastoreMutation::ExternalLocationUpserted(ExternalLocationRecord {
            location_id: "loc_orders".to_string(),
            name: "orders".to_string(),
            url: "gs://bucket/warehouse/orders/".to_string(),
            credential_id: "cred_01".to_string(),
            owner: "owner".to_string(),
            lifecycle_state: LifecycleState::Deleted,
            updated_at_ms: 1_800_000_000_009,
            properties: std::collections::BTreeMap::new(),
        }),
    );
    MetastoreLedger::new(scoped(backend))
        .append_event(&event)
        .await
        .expect("append revocation event");
}

fn governed_state(backend: Arc<dyn StorageBackend>, watermark: &str) -> UnityCatalogState {
    UnityCatalogState::new(backend)
        .with_compiled_permissions(permission_set_at_watermark(watermark))
}

fn set_permissions_at_watermark(state: &UnityCatalogState, watermark: &str) {
    let permissions = state
        .compiled_permissions
        .as_ref()
        .expect("compiled permissions configured");
    *permissions.write().expect("permissions lock") = permission_set_at_watermark(watermark);
}

fn permission_set_at_watermark(watermark: &str) -> CompiledPermissionSet {
    CompiledPermissionSet::new(
        watermark,
        "groups-rev-7",
        true,
        vec![
            permission_row("workspace1", "METASTORE", Privilege::Manage),
            permission_row("loc_orders", "EXTERNAL_LOCATION", Privilege::ReadFiles),
        ],
    )
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
        source_grant_id: Some("grant_storage_governance".to_string()),
        source_principal_id: "user_alice".to_string(),
        source_object_id: object_id.clone(),
        inheritance_path: object_id,
        grant_option: false,
        group_snapshot_version: "groups-rev-7".to_string(),
    }
}

fn credential_body() -> serde_json::Value {
    serde_json::json!({
        "credential_id": "cred_01",
        "name": "lakehouse-prod",
        "cloud": "gcs",
        "owner": "owner"
    })
}

fn location_body() -> serde_json::Value {
    serde_json::json!({
        "location_id": "loc_orders",
        "name": "orders",
        "url": "gs://bucket/warehouse/orders",
        "credential_id": "cred_01",
        "owner": "owner"
    })
}

fn vending_request() -> Request<Body> {
    trusted_json_request(
        "POST",
        "/temporary-path-credentials",
        serde_json::json!({
            "url": "gs://bucket/warehouse/orders/day=1/",
            "operation": "READ",
            "requested_ttl_seconds": 300
        }),
    )
}

fn trusted_json_request(method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("X-Tenant-Id", "tenant1")
        .header("X-Workspace-Id", "workspace1")
        .header("X-User-Id", "user_alice")
        .body(Body::from(body.to_string()))
        .expect("request");
    request.extensions_mut().insert(UnityCatalogRequestContext {
        tenant: "tenant1".to_string(),
        workspace: "workspace1".to_string(),
        request_id: "request-projection-publication".to_string(),
        user_id: Some("user_alice".to_string()),
        idempotency_key: None,
    });
    request
}

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&body).expect("json payload")
}

/// Backend that fails projection publication writes on demand while leaving
/// ledger writes untouched.
struct PublicationFaultBackend {
    inner: Arc<dyn StorageBackend>,
    fail_publication_writes: AtomicBool,
}

impl std::fmt::Debug for PublicationFaultBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublicationFaultBackend")
            .field(
                "fail_publication_writes",
                &self.fail_publication_writes.load(Ordering::SeqCst),
            )
            .finish_non_exhaustive()
    }
}

impl PublicationFaultBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            fail_publication_writes: AtomicBool::new(false),
        }
    }

    fn fail_publication_writes(&self, fail: bool) {
        self.fail_publication_writes.store(fail, Ordering::SeqCst);
    }

    fn is_publication_path(path: &str) -> bool {
        path.contains("manifests/metastore_projection") || path.contains("snapshots/metastore/")
    }
}

impl StorageBackend for PublicationFaultBackend {
    fn get<'life0, 'life1, 'async_trait>(
        &'life0 self,
        path: &'life1 str,
    ) -> Pin<Box<dyn Future<Output = CoreResult<Bytes>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move { self.inner.get(path).await })
    }

    fn get_range<'life0, 'life1, 'async_trait>(
        &'life0 self,
        path: &'life1 str,
        range: Range<u64>,
    ) -> Pin<Box<dyn Future<Output = CoreResult<Bytes>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move { self.inner.get_range(path, range).await })
    }

    fn put<'life0, 'life1, 'async_trait>(
        &'life0 self,
        path: &'life1 str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Pin<Box<dyn Future<Output = CoreResult<WriteResult>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move {
            if self.fail_publication_writes.load(Ordering::SeqCst)
                && Self::is_publication_path(path)
            {
                return Err(arco_core::Error::Storage {
                    message: format!("injected publication write failure: {path}"),
                    source: None,
                });
            }
            self.inner.put(path, data, precondition).await
        })
    }

    fn delete<'life0, 'life1, 'async_trait>(
        &'life0 self,
        path: &'life1 str,
    ) -> Pin<Box<dyn Future<Output = CoreResult<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move { self.inner.delete(path).await })
    }

    fn list<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: &'life1 str,
    ) -> Pin<Box<dyn Future<Output = CoreResult<Vec<ObjectMeta>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move { self.inner.list(prefix).await })
    }

    fn head<'life0, 'life1, 'async_trait>(
        &'life0 self,
        path: &'life1 str,
    ) -> Pin<Box<dyn Future<Output = CoreResult<Option<ObjectMeta>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move { self.inner.head(path).await })
    }

    fn signed_url<'life0, 'life1, 'async_trait>(
        &'life0 self,
        path: &'life1 str,
        expiry: Duration,
    ) -> Pin<Box<dyn Future<Output = CoreResult<String>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: Sync + 'async_trait,
    {
        Box::pin(async move { self.inner.signed_url(path, expiry).await })
    }
}

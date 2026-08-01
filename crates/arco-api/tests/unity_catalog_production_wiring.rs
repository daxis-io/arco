//! Production wiring of the Unity Catalog facade's authorization view.
//!
//! The UC facade's authorized routes evaluate a compiled permission view. The
//! deployed server used to construct `UnityCatalogState::new`, whose
//! `compiled_permissions` is `None`, so `require_authz` short-circuited to
//! `permissions_unavailable` and every authorized UC route — including the
//! storage-governance projection republish recovery route — was permanently
//! deny-closed in production, even for a METASTORE `Manage` administrator.
//! The recovery handler existed but could not be reached.
//!
//! These tests drive the real `ServerBuilder` router (the same
//! `create_router` path the deployed server uses, including the UC auth
//! middleware and the mounted UC service) rather than injecting a permission
//! view into a hand-built `UnityCatalogState`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arco_api::config::Config;
use arco_api::server::ServerBuilder;
use arco_catalog::metastore::events::{
    GrantRecord, LifecycleState, MetastoreEvent, MetastoreMutation, PrincipalKind, PrincipalRecord,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::metastore::publish::load_published_storage_governance;
use arco_core::error::Result as CoreResult;
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_core::{ControlPlaneScope, ScopedStorage};
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use tower::ServiceExt;

const TENANT: &str = "tenant1";
const WORKSPACE: &str = "workspace1";
const ADMIN: &str = "user_admin";
const NON_ADMIN: &str = "user_intruder";

/// End-to-end #362 recovery through the production wiring: a durable
/// governance commit whose synchronous publication fails leaves the projection
/// stale, and a METASTORE `Manage` administrator heals it by calling the
/// mounted republish route — 200, exact-watermark recovery, no new ledger
/// events.
#[tokio::test]
async fn metastore_admin_can_republish_through_the_production_router() {
    let flaky = Arc::new(PublicationFaultBackend::new(Arc::new(MemoryBackend::new())));
    let backend: Arc<dyn StorageBackend> = flaky.clone();
    seed_metastore_admin(&backend).await;
    let router = production_router(Arc::clone(&backend));

    // A durable governance commit whose publication fails leaves the
    // projection behind the ledger.
    flaky.fail_publication_writes(true);
    let (status, _) = uc_request(
        &router,
        ADMIN,
        "POST",
        "/api/2.1/unity-catalog/storage-credentials",
        serde_json::json!({
            "credential_id": "cred_01",
            "name": "lakehouse-prod",
            "cloud": "gcs",
            "owner": "owner"
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a failed publication must fail loud"
    );

    let scoped = scoped(&backend);
    let events = MetastoreLedger::new(scoped.clone())
        .load_events()
        .await
        .expect("ledger events");
    assert_eq!(
        events.len(),
        3,
        "the seeded grant/principal events plus the durable credential commit"
    );
    assert!(
        load_published_storage_governance(&scoped).await.is_err(),
        "the projection must be stale after the failed publication"
    );

    // Recovery through the production route, as the administrator.
    flaky.fail_publication_writes(false);
    let (status, payload) = uc_request(
        &router,
        ADMIN,
        "POST",
        "/api/2.1/unity-catalog/storage-governance/projection/republish",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a METASTORE Manage administrator must be able to invoke republish through the \
         production wiring: {payload}"
    );
    assert_eq!(payload["status"], "published");
    let latest = events
        .iter()
        .max_by_key(|event| event.sequence)
        .expect("latest event");
    assert_eq!(payload["ledger_watermark"], latest.event_id.as_str());

    let published = load_published_storage_governance(&scoped)
        .await
        .expect("healed projection");
    assert_eq!(published.ledger_watermark, latest.event_id);
    let events_after = MetastoreLedger::new(scoped)
        .load_events()
        .await
        .expect("ledger events after republish");
    assert_eq!(
        events_after.len(),
        events.len(),
        "republish must append no ledger events"
    );
}

/// The same production route denies a principal without METASTORE `Manage`.
#[tokio::test]
async fn non_manage_principal_is_denied_republish_through_the_production_router() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    seed_metastore_admin(&backend).await;
    let router = production_router(Arc::clone(&backend));

    let (status, payload) = uc_request(
        &router,
        NON_ADMIN,
        "POST",
        "/api/2.1/unity-catalog/storage-governance/projection/republish",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a principal without METASTORE Manage must be denied: {payload}"
    );
}

/// Fail-closed baseline: with no grant in the scope's ledger, the compiled
/// view authorizes nobody, so the recovery route stays denied rather than
/// falling open.
#[tokio::test]
async fn empty_metastore_scope_authorizes_nobody_for_republish() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let router = production_router(Arc::clone(&backend));

    let (status, _) = uc_request(
        &router,
        ADMIN,
        "POST",
        "/api/2.1/unity-catalog/storage-governance/projection/republish",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

fn production_router(backend: Arc<dyn StorageBackend>) -> Router {
    let mut config = Config {
        debug: true,
        ..Config::default()
    };
    config.unity_catalog.enabled = true;
    ServerBuilder::new()
        .config(config)
        .storage_backend(backend)
        .build()
        .test_router()
}

fn scoped(backend: &Arc<dyn StorageBackend>) -> ScopedStorage {
    ScopedStorage::new(Arc::clone(backend), TENANT, WORKSPACE).expect("scoped storage")
}

/// Seeds the scope's metastore ledger with an active principal and a MANAGE
/// grant on the scope's METASTORE securable — the authoritative state the
/// production permission source compiles into an administrator.
async fn seed_metastore_admin(backend: &Arc<dyn StorageBackend>) {
    let scope = ControlPlaneScope::workspace_alias(TENANT, WORKSPACE).expect("scope");
    let ledger = MetastoreLedger::new(scoped(backend));
    ledger
        .append_event(&MetastoreEvent::new_scoped(
            &scope,
            "event_principal_001",
            1,
            MetastoreMutation::PrincipalUpserted(PrincipalRecord {
                principal_id: ADMIN.to_string(),
                name: ADMIN.to_string(),
                principal_kind: PrincipalKind::User,
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_000,
                properties: BTreeMap::new(),
            }),
        ))
        .await
        .expect("append principal event");
    ledger
        .append_event(&MetastoreEvent::new_scoped(
            &scope,
            "event_grant_002",
            2,
            MetastoreMutation::GrantUpserted(GrantRecord {
                grant_id: "grant_metastore_admin".to_string(),
                object_id: scope.metastore_id().to_string(),
                object_type: "METASTORE".to_string(),
                principal_id: ADMIN.to_string(),
                privilege: "MANAGE".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_001,
                properties: BTreeMap::new(),
            }),
        ))
        .await
        .expect("append grant event");
    // Publish at the seeded watermark so the governance route's pre-commit
    // self-heal is a no-op and the injected fault lands on the *post-commit*
    // publication, reproducing "durable commit, failed publication".
    arco_catalog::metastore::publish::publish_current_metastore_projection(
        &scoped(backend),
        &arco_catalog::metastore::projections::ProjectionRegistry::default(),
    )
    .await
    .expect("publish seeded projection");
}

async fn uc_request(
    router: &Router,
    principal: &str,
    method: &str,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .header("X-User-Id", principal)
        .body(Body::from(body.to_string()))
        .expect("request");
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("route production request");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let payload = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, payload)
}

/// Backend that fails writes to projection publication paths on demand.
struct PublicationFaultBackend {
    inner: Arc<dyn StorageBackend>,
    fail_publication_writes: AtomicBool,
}

impl std::fmt::Debug for PublicationFaultBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublicationFaultBackend").finish()
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

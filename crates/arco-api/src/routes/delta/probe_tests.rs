use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arco_catalog::managed_delta::ManagedDeltaStorage;
use arco_catalog::metastore::events::{
    CatalogObjectRecord, GrantRecord, LifecycleState, ManagedRootRecord, MetastoreEvent,
    MetastoreMutation, PrincipalKind, PrincipalRecord,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::metastore::projections::ProjectionRegistry;
use arco_catalog::metastore::publish::publish_current_metastore_projection;
use arco_core::ControlPlaneScope;
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use axum::body::{Body, to_bytes};
use axum::http::Request;
use serde_json::{Value, json};
use tokio::sync::Notify;
use tower::ServiceExt;

use super::*;
use crate::config::Config;

const ROOT: &str = "s3://analytical-bucket/approved";
const PREFIX: &str = "approved/tenant=tenant1/workspace=workspace1/managed-delta/probes";

#[derive(Default)]
struct InterleavedProbes {
    data: MemoryBackend,
    lose_every_write: bool,
    writes: AtomicUsize,
    heads: AtomicUsize,
    paused_after_head: Notify,
    deleted: Notify,
}

#[async_trait::async_trait]
impl StorageBackend for InterleavedProbes {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.data.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.data.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        let result = self.data.put(path, data, precondition).await?;
        if self.lose_every_write {
            self.data.delete(path).await?;
        }
        Ok(result)
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.data.delete(path).await?;
        self.deleted.notify_one();
        Ok(())
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.data.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        let result = self.data.head(path).await?;
        if !self.lose_every_write && self.heads.fetch_add(1, Ordering::SeqCst) == 0 {
            self.paused_after_head.notify_one();
            self.deleted.notified().await;
        }
        Ok(result)
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.data.signed_url(path, expiry).await
    }
}

async fn router(data: Arc<dyn StorageBackend>) -> anyhow::Result<Router> {
    let config = Config {
        debug: true,
        ..Config::default()
    };
    let mut state = AppState::with_memory_storage(config);
    state.managed_delta_storage = Some(ManagedDeltaStorage::open(ROOT, |_| Ok(data))?);
    let scope = ControlPlaneScope::workspace_alias("tenant1", "workspace1")?;
    let catalog = ScopedStorage::new(state.storage_backend()?, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(catalog.clone())?;
    for (sequence, mutation) in [
        MetastoreMutation::PrincipalUpserted(PrincipalRecord {
            principal_id: "owner".to_string(),
            name: "owner".to_string(),
            principal_kind: PrincipalKind::User,
            owner: "owner".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 1,
            properties: BTreeMap::new(),
        }),
        MetastoreMutation::CatalogObjectUpserted(CatalogObjectRecord {
            object_id: "turntable.verification".to_string(),
            object_type: "SCHEMA".to_string(),
            qualified_name: "turntable.verification".to_string(),
            owner: "admin".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 2,
            properties: BTreeMap::new(),
        }),
        MetastoreMutation::GrantUpserted(GrantRecord {
            grant_id: "owner_create_table".to_string(),
            object_id: "turntable.verification".to_string(),
            object_type: "SCHEMA".to_string(),
            principal_id: "owner".to_string(),
            privilege: "CREATE_TABLE".to_string(),
            owner: "admin".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 3,
            properties: BTreeMap::new(),
            identity_cut: None,
        }),
        MetastoreMutation::ManagedRootUpserted(ManagedRootRecord {
            root_id: "probe_root".to_string(),
            name: "probe_root".to_string(),
            workspace_id: "workspace1".to_string(),
            url: format!("{ROOT}/{}/", catalog.scope().prefix()),
            owner: "owner".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 4,
            properties: BTreeMap::new(),
        }),
    ]
    .into_iter()
    .enumerate()
    {
        let event = MetastoreEvent::new_scoped(
            &scope,
            format!("probe_{sequence}"),
            sequence as u64 + 1,
            mutation,
        );
        ledger.append_event(&event).await?;
    }
    publish_current_metastore_projection(&catalog, &ProjectionRegistry::default()).await?;
    Ok(Router::new()
        .nest("/api/v1", routes())
        .with_state(Arc::new(state)))
}

async fn send_probe(
    router: Router,
    operation_id: Uuid,
    owner: &str,
) -> anyhow::Result<(StatusCode, axum::http::HeaderMap, Value)> {
    let request = Request::post("/api/v1/managed-delta/storage")
        .header("content-type", "application/json")
        .header("x-tenant-id", "tenant1")
        .header("x-workspace-id", "workspace1")
        .header("x-user-id", owner)
        .body(Body::from(
            json!({"operation_id": operation_id}).to_string(),
        ))?;
    let response = router.oneshot(request).await?;
    let (parts, body) = response.into_parts();
    let body = to_bytes(body, 4096).await?;
    let decoded = serde_json::from_slice(&body).unwrap_or_else(|_| json!(body.as_ref()));
    Ok((parts.status, parts.headers, decoded))
}

#[tokio::test]
async fn managed_delta_storage_probe_recovers_when_overlapping_retry_deletes_before_get()
-> anyhow::Result<()> {
    let data = Arc::new(InterleavedProbes::default());
    let router = router(data.clone()).await?;
    let operation_id = Uuid::now_v7();
    let expected_key = format!("{PREFIX}/{operation_id}");

    assert_eq!(
        send_probe(router.clone(), operation_id, "ungranted")
            .await?
            .0,
        StatusCode::FORBIDDEN,
    );
    assert!(data.data.list("").await?.is_empty());
    data.data
        .put(
            &expected_key,
            Bytes::from_static(b"arco-managed-delta-storage-probe-v1"),
            WritePrecondition::DoesNotExist,
        )
        .await?;

    tokio::time::timeout(Duration::from_secs(5), async {
        let responses: [_; 2] =
            tokio::try_join!(send_probe(router.clone(), operation_id, "owner"), async {
                data.paused_after_head.notified().await;
                assert!(data.data.head(&expected_key).await?.is_some());
                send_probe(router, operation_id, "owner").await
            })?
            .into();
        for (status, _, receipt) in responses {
            assert_eq!(status, StatusCode::OK, "{receipt}");
            assert_eq!(receipt, json!({"state":"passed"}));
        }
        Ok::<(), anyhow::Error>(())
    })
    .await??;
    assert_eq!(data.writes.load(Ordering::SeqCst), 3);
    assert!(data.data.list("").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn managed_delta_storage_probe_returns_backpressure_after_three_misses() -> anyhow::Result<()>
{
    let data = Arc::new(InterleavedProbes {
        lose_every_write: true,
        ..InterleavedProbes::default()
    });
    let router = router(data.clone()).await?;
    let (status, headers, receipt) = send_probe(router, Uuid::now_v7(), "owner").await?;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{receipt}");
    assert_eq!(
        receipt.get("code").and_then(Value::as_str),
        Some("MANAGED_STORAGE_PROBE_BUSY")
    );
    assert_eq!(
        headers
            .get(axum::http::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );
    assert_eq!(data.writes.load(Ordering::SeqCst), 3);
    assert!(data.data.list("").await?.is_empty());
    Ok(())
}

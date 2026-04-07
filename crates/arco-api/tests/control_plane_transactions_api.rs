//! Integration tests for control-plane transaction commit and lookup APIs.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use bytes::Bytes;
use prost::Message;
use tower::ServiceExt;

use arco_api::server::{Server, ServerBuilder};
use arco_core::catalog_event::CatalogEvent;
use arco_core::catalog_paths::{CatalogDomain, CatalogPaths};
use arco_core::control_plane_transactions::{
    CatalogTxRecord, ControlPlaneIdempotencyRecord, ControlPlaneTxPaths, ControlPlaneTxStatus,
    OrchestrationTxRecord, RootTxManifest, RootTxReceipt, RootTxRecord,
};
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_core::{ControlPlaneTxDomain, ScopedStorage};
use arco_proto::catalog_ddl_operation::Op as CatalogDdlOp;
use arco_proto::{
    ApplyCatalogDdlRequest, ApplyCatalogDdlResponse, CatalogColumnDefinition, CatalogDdlOperation,
    CommitOrchestrationBatchRequest, CommitOrchestrationBatchResponse,
    CommitRootTransactionRequest, CommitRootTransactionResponse, CreateNamespaceOp, DomainMutation,
    GetCatalogTransactionRequest, GetCatalogTransactionResponse,
    GetOrchestrationTransactionRequest, GetOrchestrationTransactionResponse,
    GetRootTransactionRequest, GetRootTransactionResponse, OrchestrationBatchSpec,
    OrchestrationEventEnvelope, RegisterTableOp, RequestHeader, TenantId, TransactionDomain,
    TransactionStatus, WorkspaceId, domain_mutation,
};

const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";
const TENANT: &str = "test-tenant";
const WORKSPACE: &str = "test-workspace";

fn test_router() -> axum::Router {
    ServerBuilder::new().debug(true).build().test_router()
}

fn test_router_with_backend(backend: Arc<dyn StorageBackend>) -> axum::Router {
    test_router_with_config_backend(arco_api::config::Config::default(), backend)
}

fn test_router_with_config_backend(
    mut config: arco_api::config::Config,
    backend: Arc<dyn StorageBackend>,
) -> axum::Router {
    config.debug = true;
    Server::with_storage_backend(config, backend).test_router()
}

fn request_header(idempotency_key: &str, request_id: &str) -> RequestHeader {
    RequestHeader {
        tenant_id: Some(TenantId {
            value: TENANT.to_string(),
        }),
        workspace_id: Some(WorkspaceId {
            value: WORKSPACE.to_string(),
        }),
        trace_parent: String::new(),
        idempotency_key: idempotency_key.to_string(),
        request_time: None,
        request_id: request_id.to_string(),
    }
}

fn protobuf_request<T: Message>(
    path: &str,
    message: &T,
    idempotency_key: &str,
    request_id: &str,
) -> Result<Request<Body>> {
    Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .header("Idempotency-Key", idempotency_key)
        .header("X-Request-Id", request_id)
        .header(header::CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)
        .body(Body::from(message.encode_to_vec()))
        .context("build protobuf request")
}

async fn post_protobuf<TReq, TResp>(
    router: axum::Router,
    path: &str,
    message: &TReq,
    idempotency_key: &str,
    request_id: &str,
) -> Result<(StatusCode, TResp)>
where
    TReq: Message,
    TResp: Message + Default,
{
    let request = protobuf_request(path, message, idempotency_key, request_id)?;
    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = axum::body::to_bytes(response.into_body(), 256 * 1024)
        .await
        .context("read protobuf response body")?;
    assert_eq!(status, StatusCode::OK, "expected success, body={body:?}");
    assert_eq!(content_type, CONTENT_TYPE_PROTOBUF);
    let decoded = TResp::decode(body.as_ref()).context("decode protobuf response")?;
    Ok((status, decoded))
}

async fn post_error_json<TReq: Message>(
    router: axum::Router,
    path: &str,
    message: &TReq,
    idempotency_key: &str,
    request_id: &str,
) -> Result<(StatusCode, serde_json::Value)> {
    let request = protobuf_request(path, message, idempotency_key, request_id)?;
    let response = router.oneshot(request).await.map_err(|err| match err {})?;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 256 * 1024)
        .await
        .context("read error response body")?;
    let json = serde_json::from_slice(&body).with_context(|| {
        format!(
            "parse JSON error response (status={status}): {}",
            String::from_utf8_lossy(&body)
        )
    })?;
    Ok((status, json))
}

fn catalog_create_namespace_request(
    idempotency_key: &str,
    request_id: &str,
    name: &str,
) -> ApplyCatalogDdlRequest {
    ApplyCatalogDdlRequest {
        header: Some(request_header(idempotency_key, request_id)),
        ddl: Some(CatalogDdlOperation {
            op: Some(CatalogDdlOp::CreateNamespace(CreateNamespaceOp {
                name: name.to_string(),
                description: Some("control-plane transaction test".to_string()),
                properties: Default::default(),
            })),
        }),
        require_visible: Some(true),
    }
}

fn orchestration_request(
    idempotency_key: &str,
    request_id: &str,
    run_id: &str,
) -> CommitOrchestrationBatchRequest {
    CommitOrchestrationBatchRequest {
        header: Some(request_header(idempotency_key, request_id)),
        events: vec![OrchestrationEventEnvelope {
            event_id: "01JTXORCH000000000000000001".to_string(),
            event_type: "RunTriggered".to_string(),
            event_version: 1,
            timestamp: Some(prost_types::Timestamp {
                seconds: 1_776_000_000,
                nanos: 0,
            }),
            source: format!("arco-flow/{TENANT}/{WORKSPACE}"),
            idempotency_key: format!("event:{run_id}"),
            correlation_id: Some(run_id.to_string()),
            causation_id: None,
            payload_json: serde_json::to_vec(&serde_json::json!({
                "run_id": run_id,
                "plan_id": format!("plan-{run_id}"),
                "trigger": {
                    "type": "manual",
                    "user_id": "tester"
                },
                "root_assets": [],
                "labels": {}
            }))
            .expect("serialize orchestration payload"),
        }],
        require_visible: Some(true),
        allow_inline_merge: Some(false),
    }
}

fn root_request(
    idempotency_key: &str,
    request_id: &str,
    namespace: &str,
    run_id: &str,
) -> CommitRootTransactionRequest {
    CommitRootTransactionRequest {
        header: Some(request_header(idempotency_key, request_id)),
        mutations: vec![
            DomainMutation {
                kind: Some(domain_mutation::Kind::Catalog(CatalogDdlOperation {
                    op: Some(CatalogDdlOp::CreateNamespace(CreateNamespaceOp {
                        name: namespace.to_string(),
                        description: Some("root transaction namespace".to_string()),
                        properties: Default::default(),
                    })),
                })),
            },
            DomainMutation {
                kind: Some(domain_mutation::Kind::Orchestration(
                    OrchestrationBatchSpec {
                        events: orchestration_request(idempotency_key, request_id, run_id).events,
                        allow_inline_merge: Some(false),
                    },
                )),
            },
        ],
    }
}

fn scoped_storage(backend: Arc<dyn StorageBackend>) -> ScopedStorage {
    ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage")
}

async fn load_catalog_tx_record(
    backend: Arc<dyn StorageBackend>,
    tx_id: &str,
) -> Result<CatalogTxRecord> {
    let storage = scoped_storage(backend);
    let bytes = storage
        .get_raw(&ControlPlaneTxPaths::record(
            ControlPlaneTxDomain::Catalog,
            tx_id,
        ))
        .await?;
    serde_json::from_slice(bytes.as_ref()).context("decode catalog tx record")
}

async fn load_orchestration_tx_record(
    backend: Arc<dyn StorageBackend>,
    tx_id: &str,
) -> Result<OrchestrationTxRecord> {
    let storage = scoped_storage(backend);
    let bytes = storage
        .get_raw(&ControlPlaneTxPaths::record(
            ControlPlaneTxDomain::Orchestration,
            tx_id,
        ))
        .await?;
    serde_json::from_slice(bytes.as_ref()).context("decode orchestration tx record")
}

async fn load_root_tx_record(
    backend: Arc<dyn StorageBackend>,
    tx_id: &str,
) -> Result<RootTxRecord> {
    let storage = scoped_storage(backend);
    let bytes = storage
        .get_raw(&ControlPlaneTxPaths::record(
            ControlPlaneTxDomain::Root,
            tx_id,
        ))
        .await?;
    serde_json::from_slice(bytes.as_ref()).context("decode root tx record")
}

async fn load_root_super_manifest(
    backend: Arc<dyn StorageBackend>,
    tx_id: &str,
) -> Result<RootTxManifest> {
    let storage = scoped_storage(backend);
    let bytes = storage
        .get_raw(&ControlPlaneTxPaths::root_super_manifest(tx_id))
        .await?;
    serde_json::from_slice(bytes.as_ref()).context("decode root super-manifest")
}

async fn load_root_commit_receipt(
    backend: Arc<dyn StorageBackend>,
    commit_id: &str,
) -> Result<Option<RootTxReceipt>> {
    let storage = scoped_storage(backend);
    match storage
        .get_raw(&ControlPlaneTxPaths::root_commit_receipt(commit_id))
        .await
    {
        Ok(bytes) => serde_json::from_slice(bytes.as_ref())
            .map(Some)
            .context("decode root commit receipt"),
        Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn load_idempotency_record(
    backend: Arc<dyn StorageBackend>,
    domain: ControlPlaneTxDomain,
    idempotency_key: &str,
) -> Result<ControlPlaneIdempotencyRecord> {
    let storage = scoped_storage(backend);
    let bytes = storage
        .get_raw(&ControlPlaneTxPaths::idempotency(domain, idempotency_key))
        .await?;
    serde_json::from_slice(bytes.as_ref()).context("decode idempotency record")
}

async fn load_catalog_ledger_event_source(
    backend: Arc<dyn StorageBackend>,
    event_id: &str,
) -> Result<String> {
    let storage = scoped_storage(backend);
    let bytes = storage
        .get_raw(&CatalogPaths::ledger_event(
            CatalogDomain::Catalog,
            event_id,
        ))
        .await?;
    let envelope: CatalogEvent<serde_json::Value> =
        serde_json::from_slice(bytes.as_ref()).context("decode catalog ledger event")?;
    Ok(envelope.source)
}

#[derive(Debug)]
struct FailPathBackend {
    inner: MemoryBackend,
    fail_path: String,
    remaining_failures: std::sync::atomic::AtomicUsize,
}

impl FailPathBackend {
    fn new(fail_path: impl Into<String>, failures: usize) -> Self {
        Self {
            inner: MemoryBackend::new(),
            fail_path: fail_path.into(),
            remaining_failures: std::sync::atomic::AtomicUsize::new(failures),
        }
    }
}

#[async_trait]
impl StorageBackend for FailPathBackend {
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
        use std::sync::atomic::Ordering;

        if path == self.fail_path && matches!(&precondition, WritePrecondition::None) {
            let remaining = self.remaining_failures.load(Ordering::SeqCst);
            if remaining > 0 {
                self.remaining_failures.fetch_sub(1, Ordering::SeqCst);
                return Err(arco_core::Error::storage(format!(
                    "injected failure for {path}"
                )));
            }
        }

        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[derive(Debug)]
struct FailPrefixBackend {
    inner: MemoryBackend,
    fail_prefix: String,
    remaining_failures: std::sync::atomic::AtomicUsize,
}

impl FailPrefixBackend {
    fn new(fail_prefix: impl Into<String>, failures: usize) -> Self {
        Self {
            inner: MemoryBackend::new(),
            fail_prefix: fail_prefix.into(),
            remaining_failures: std::sync::atomic::AtomicUsize::new(failures),
        }
    }
}

#[async_trait]
impl StorageBackend for FailPrefixBackend {
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
        use std::sync::atomic::Ordering;

        if path.starts_with(&self.fail_prefix) {
            let remaining = self.remaining_failures.load(Ordering::SeqCst);
            if remaining > 0 {
                self.remaining_failures.fetch_sub(1, Ordering::SeqCst);
                return Err(arco_core::Error::storage(format!(
                    "injected failure for {path} with precondition {precondition:?}"
                )));
            }
        }

        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[derive(Debug)]
struct FailPrefixOnNoneBackend {
    inner: MemoryBackend,
    fail_prefix: String,
    remaining_failures: std::sync::atomic::AtomicUsize,
}

impl FailPrefixOnNoneBackend {
    fn new(fail_prefix: impl Into<String>, failures: usize) -> Self {
        Self {
            inner: MemoryBackend::new(),
            fail_prefix: fail_prefix.into(),
            remaining_failures: std::sync::atomic::AtomicUsize::new(failures),
        }
    }
}

#[async_trait]
impl StorageBackend for FailPrefixOnNoneBackend {
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
        use std::sync::atomic::Ordering;

        if path.starts_with(&self.fail_prefix) && matches!(&precondition, WritePrecondition::None) {
            let remaining = self.remaining_failures.load(Ordering::SeqCst);
            if remaining > 0 {
                self.remaining_failures.fetch_sub(1, Ordering::SeqCst);
                return Err(arco_core::Error::storage(format!(
                    "injected failure for {path} with precondition {precondition:?}"
                )));
            }
        }

        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[derive(Debug)]
struct FailRootSuperManifestBackend {
    inner: MemoryBackend,
    remaining_failures: std::sync::atomic::AtomicUsize,
}

impl FailRootSuperManifestBackend {
    fn new(failures: usize) -> Self {
        Self {
            inner: MemoryBackend::new(),
            remaining_failures: std::sync::atomic::AtomicUsize::new(failures),
        }
    }
}

#[async_trait]
impl StorageBackend for FailRootSuperManifestBackend {
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
        use std::sync::atomic::Ordering;

        if path.contains("/transactions/root/") && path.ends_with(".manifest.json") {
            let remaining = self.remaining_failures.load(Ordering::SeqCst);
            if remaining > 0 {
                self.remaining_failures.fetch_sub(1, Ordering::SeqCst);
                return Err(arco_core::Error::storage(format!(
                    "injected failure for {path} with precondition {precondition:?}"
                )));
            }
        }

        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[tokio::test]
async fn apply_catalog_ddl_returns_visible_receipt_and_persists_lookup_record() -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let router = test_router_with_backend(backend.clone());
    let request = catalog_create_namespace_request("idem-cat-01", "req-cat-01", "analytics");

    let (_status, response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &request,
        "idem-cat-01",
        "req-cat-01",
    )
    .await?;

    let receipt = response.receipt.context("catalog receipt missing")?;
    assert!(!receipt.tx_id.is_empty());
    assert!(!receipt.event_id.is_empty());
    assert!(!receipt.commit_id.is_empty());
    assert!(!receipt.manifest_id.is_empty());
    assert!(!receipt.pointer_version.is_empty());
    assert_eq!(
        receipt.read_token,
        format!("catalog:{}", receipt.manifest_id)
    );
    assert!(!response.repair_pending);

    let lookup = GetCatalogTransactionRequest {
        header: Some(request_header("idem-cat-lookup-01", "req-cat-lookup-01")),
        tx_id: receipt.tx_id.clone(),
    };
    let (_status, lookup_response): (_, GetCatalogTransactionResponse) = post_protobuf(
        router,
        "/api/v1/transactions/getCatalogTransaction",
        &lookup,
        "idem-cat-lookup-01",
        "req-cat-lookup-01",
    )
    .await?;
    let status = lookup_response.status.context("catalog status missing")?;
    assert_eq!(status.status, TransactionStatus::Visible as i32);
    assert_eq!(status.request_id, "req-cat-01");
    assert_eq!(status.idempotency_key, "idem-cat-01");
    assert!(status.request_hash.starts_with("sha256:"));
    assert!(!status.lock_path.is_empty());
    assert!(status.fencing_token > 0);
    assert!(status.prepared_at.is_some());
    assert!(status.visible_at.is_some());
    assert!(!status.repair_pending);
    assert_eq!(
        status.result.as_ref().map(|result| result.tx_id.as_str()),
        Some(receipt.tx_id.as_str())
    );

    let stored = load_catalog_tx_record(backend, &receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert_eq!(stored.request_id, "req-cat-01");
    assert_eq!(stored.idempotency_key, "idem-cat-01");
    assert_eq!(
        stored.result.as_ref().map(|result| result.tx_id.as_str()),
        Some(receipt.tx_id.as_str())
    );

    Ok(())
}

#[tokio::test]
async fn apply_catalog_ddl_replays_same_idempotency_key_and_rejects_hash_conflicts() -> Result<()> {
    let router = test_router();
    let first =
        catalog_create_namespace_request("idem-cat-replay-01", "req-cat-replay-01", "bronze");
    let second =
        catalog_create_namespace_request("idem-cat-replay-01", "req-cat-replay-02", "bronze");
    let conflicting =
        catalog_create_namespace_request("idem-cat-replay-01", "req-cat-replay-03", "silver");

    let (_status, first_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &first,
        "idem-cat-replay-01",
        "req-cat-replay-01",
    )
    .await?;
    let (_status, replay_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &second,
        "idem-cat-replay-01",
        "req-cat-replay-02",
    )
    .await?;

    let first_tx = first_response
        .receipt
        .context("first receipt missing")?
        .tx_id;
    let replay_tx = replay_response
        .receipt
        .context("replay receipt missing")?
        .tx_id;
    assert_eq!(first_tx, replay_tx, "idempotent replay must reuse tx_id");

    let (status, error) = post_error_json(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &conflicting,
        "idem-cat-replay-01",
        "req-cat-replay-03",
    )
    .await?;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error["code"], "CONFLICT");

    Ok(())
}

#[tokio::test]
async fn commit_orchestration_batch_returns_visible_receipt_and_persists_lookup_record()
-> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let router = test_router_with_backend(backend.clone());
    let request = orchestration_request("idem-orch-01", "req-orch-01", "run-orch-01");

    let (_status, response): (_, CommitOrchestrationBatchResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/commitOrchestrationBatch",
        &request,
        "idem-orch-01",
        "req-orch-01",
    )
    .await?;

    let receipt = response.receipt.context("orchestration receipt missing")?;
    assert!(!receipt.tx_id.is_empty());
    assert!(!receipt.commit_id.is_empty());
    assert!(!receipt.manifest_id.is_empty());
    assert!(!receipt.revision_ulid.is_empty());
    assert!(!receipt.pointer_version.is_empty());
    assert_eq!(receipt.events_processed, 1);
    assert_eq!(
        receipt.read_token,
        format!("orchestration:{}", receipt.manifest_id)
    );
    assert!(!response.repair_pending);

    let lookup = GetOrchestrationTransactionRequest {
        header: Some(request_header("idem-orch-lookup-01", "req-orch-lookup-01")),
        tx_id: receipt.tx_id.clone(),
    };
    let (_status, lookup_response): (_, GetOrchestrationTransactionResponse) = post_protobuf(
        router,
        "/api/v1/transactions/getOrchestrationTransaction",
        &lookup,
        "idem-orch-lookup-01",
        "req-orch-lookup-01",
    )
    .await?;
    let status = lookup_response
        .status
        .context("orchestration status missing")?;
    assert_eq!(status.status, TransactionStatus::Visible as i32);
    assert_eq!(status.request_id, "req-orch-01");
    assert_eq!(status.idempotency_key, "idem-orch-01");
    assert!(status.request_hash.starts_with("sha256:"));
    assert!(!status.lock_path.is_empty());
    assert!(status.fencing_token > 0);
    assert!(status.prepared_at.is_some());
    assert!(status.visible_at.is_some());
    assert!(!status.repair_pending);
    assert_eq!(
        status.result.as_ref().map(|result| result.tx_id.as_str()),
        Some(receipt.tx_id.as_str())
    );

    let stored = load_orchestration_tx_record(backend, &receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert_eq!(stored.request_id, "req-orch-01");
    assert_eq!(stored.idempotency_key, "idem-orch-01");
    assert_eq!(
        stored.result.as_ref().map(|result| result.tx_id.as_str()),
        Some(receipt.tx_id.as_str())
    );

    Ok(())
}

#[tokio::test]
async fn commit_orchestration_batch_replays_same_idempotency_key_and_rejects_hash_conflicts()
-> Result<()> {
    let router = test_router();
    let first = orchestration_request("idem-orch-replay-01", "req-orch-replay-01", "run-replay-01");
    let second =
        orchestration_request("idem-orch-replay-01", "req-orch-replay-02", "run-replay-01");
    let conflicting =
        orchestration_request("idem-orch-replay-01", "req-orch-replay-03", "run-replay-02");

    let (_status, first_response): (_, CommitOrchestrationBatchResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/commitOrchestrationBatch",
        &first,
        "idem-orch-replay-01",
        "req-orch-replay-01",
    )
    .await?;
    let (_status, replay_response): (_, CommitOrchestrationBatchResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/commitOrchestrationBatch",
        &second,
        "idem-orch-replay-01",
        "req-orch-replay-02",
    )
    .await?;

    let first_tx = first_response
        .receipt
        .context("first orchestration receipt missing")?
        .tx_id;
    let replay_tx = replay_response
        .receipt
        .context("replay orchestration receipt missing")?
        .tx_id;
    assert_eq!(first_tx, replay_tx, "idempotent replay must reuse tx_id");

    let (status, error) = post_error_json(
        router,
        "/api/v1/transactions/commitOrchestrationBatch",
        &conflicting,
        "idem-orch-replay-01",
        "req-orch-replay-03",
    )
    .await?;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error["code"], "CONFLICT");

    Ok(())
}

#[tokio::test]
async fn commit_root_transaction_returns_visible_receipt_and_persists_lookup_record() -> Result<()>
{
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let router = test_router_with_backend(backend.clone());
    let request = root_request(
        "idem-root-01",
        "req-root-01",
        "root-analytics",
        "run-root-01",
    );

    let (_status, response): (_, CommitRootTransactionResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/commitRootTransaction",
        &request,
        "idem-root-01",
        "req-root-01",
    )
    .await?;

    let receipt = response.receipt.context("root receipt missing")?;
    assert!(!receipt.tx_id.is_empty());
    assert!(!receipt.root_commit_id.is_empty());
    assert_eq!(receipt.read_token, format!("root:{}", receipt.tx_id));
    assert_eq!(
        receipt.super_manifest_path,
        format!("transactions/root/{}.manifest.json", receipt.tx_id)
    );
    assert!(receipt.visible_at.is_some());
    assert_eq!(receipt.domain_commits.len(), 2);
    assert!(!response.repair_pending);

    let catalog_commit = receipt
        .domain_commits
        .iter()
        .find(|commit| commit.domain == TransactionDomain::Catalog as i32)
        .context("catalog domain commit missing")?;
    assert!(!catalog_commit.tx_id.is_empty());
    assert!(!catalog_commit.commit_id.is_empty());
    assert_eq!(
        catalog_commit.manifest_path,
        format!("manifests/catalog/{}.json", catalog_commit.manifest_id)
    );
    assert_eq!(
        catalog_commit.read_token,
        format!("catalog:{}", catalog_commit.manifest_id)
    );

    let orchestration_commit = receipt
        .domain_commits
        .iter()
        .find(|commit| commit.domain == TransactionDomain::Orchestration as i32)
        .context("orchestration domain commit missing")?;
    assert!(!orchestration_commit.tx_id.is_empty());
    assert!(!orchestration_commit.commit_id.is_empty());
    assert_eq!(
        orchestration_commit.manifest_path,
        format!(
            "state/orchestration/manifests/{}.json",
            orchestration_commit.manifest_id
        )
    );
    assert_eq!(
        orchestration_commit.read_token,
        format!("orchestration:{}", orchestration_commit.manifest_id)
    );

    let lookup = GetRootTransactionRequest {
        header: Some(request_header("idem-root-lookup-01", "req-root-lookup-01")),
        tx_id: receipt.tx_id.clone(),
    };
    let (_status, lookup_response): (_, GetRootTransactionResponse) = post_protobuf(
        router,
        "/api/v1/transactions/getRootTransaction",
        &lookup,
        "idem-root-lookup-01",
        "req-root-lookup-01",
    )
    .await?;

    let status = lookup_response.status.context("root status missing")?;
    assert_eq!(status.status, TransactionStatus::Visible as i32);
    assert_eq!(status.request_id, "req-root-01");
    assert_eq!(status.idempotency_key, "idem-root-01");
    assert!(status.request_hash.starts_with("sha256:"));
    assert_eq!(status.lock_path, "locks/root.lock.json");
    assert!(status.fencing_token > 0);
    assert!(status.prepared_at.is_some());
    assert!(status.visible_at.is_some());
    assert_eq!(status.super_manifest_path, receipt.super_manifest_path);
    assert_eq!(status.domains.len(), 2);
    assert!(!status.repair_pending);
    assert_eq!(
        status.result.as_ref().map(|result| result.tx_id.as_str()),
        Some(receipt.tx_id.as_str())
    );

    let stored = load_root_tx_record(backend.clone(), &receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert_eq!(stored.request_id, "req-root-01");
    assert_eq!(stored.idempotency_key, "idem-root-01");
    assert_eq!(
        stored
            .result
            .as_ref()
            .map(|result| result.read_token.as_str()),
        Some(receipt.read_token.as_str())
    );

    let super_manifest = load_root_super_manifest(backend.clone(), &receipt.tx_id).await?;
    assert_eq!(super_manifest.tx_id, receipt.tx_id);
    assert_eq!(super_manifest.fencing_token, stored.fencing_token);
    assert_eq!(
        super_manifest
            .domains
            .get(&ControlPlaneTxDomain::Catalog)
            .map(|domain| domain.manifest_path.as_str()),
        Some(catalog_commit.manifest_path.as_str())
    );
    assert_eq!(
        super_manifest
            .domains
            .get(&ControlPlaneTxDomain::Orchestration)
            .map(|domain| domain.manifest_path.as_str()),
        Some(orchestration_commit.manifest_path.as_str())
    );

    let root_commit_receipt = load_root_commit_receipt(backend, &receipt.root_commit_id)
        .await?
        .context("root commit receipt missing")?;
    assert_eq!(root_commit_receipt.tx_id, receipt.tx_id);
    assert_eq!(root_commit_receipt.root_commit_id, receipt.root_commit_id);
    assert_eq!(
        root_commit_receipt.super_manifest_path,
        receipt.super_manifest_path
    );
    assert_eq!(root_commit_receipt.read_token, receipt.read_token);

    Ok(())
}

#[tokio::test]
async fn commit_root_transaction_replays_same_idempotency_key() -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let router = test_router_with_backend(backend.clone());
    let first = root_request(
        "idem-root-replay-01",
        "req-root-replay-01",
        "replay-root",
        "run-root-replay-01",
    );
    let second = root_request(
        "idem-root-replay-01",
        "req-root-replay-02",
        "replay-root",
        "run-root-replay-01",
    );

    let (_status, first_response): (_, CommitRootTransactionResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/commitRootTransaction",
        &first,
        "idem-root-replay-01",
        "req-root-replay-01",
    )
    .await?;
    let (_status, replay_response): (_, CommitRootTransactionResponse) = post_protobuf(
        router,
        "/api/v1/transactions/commitRootTransaction",
        &second,
        "idem-root-replay-01",
        "req-root-replay-02",
    )
    .await?;

    let first_receipt = first_response
        .receipt
        .context("first root receipt missing")?;
    let replay_receipt = replay_response
        .receipt
        .context("replay root receipt missing")?;
    assert_eq!(
        first_receipt.tx_id, replay_receipt.tx_id,
        "idempotent replay must reuse root tx_id"
    );
    assert_eq!(first_receipt.root_commit_id, replay_receipt.root_commit_id);
    assert_eq!(first_receipt.read_token, replay_receipt.read_token);
    assert_eq!(
        first_receipt.super_manifest_path,
        replay_receipt.super_manifest_path
    );

    let idem =
        load_idempotency_record(backend, ControlPlaneTxDomain::Root, "idem-root-replay-01").await?;
    assert_eq!(idem.tx_id, first_receipt.tx_id);
    assert!(idem.visible_at.is_some());
    assert!(idem.tx_record.is_some());

    Ok(())
}

#[tokio::test]
async fn commit_root_transaction_propagates_repair_pending_when_root_commit_receipt_write_fails()
-> Result<()> {
    let fail_prefix = format!("tenant={TENANT}/workspace={WORKSPACE}/commits/root/");
    let backend: Arc<dyn StorageBackend> = Arc::new(FailPrefixBackend::new(fail_prefix, 1));
    let router = test_router_with_backend(backend.clone());
    let request = root_request(
        "idem-root-repair-01",
        "req-root-repair-01",
        "repair-root",
        "run-root-repair-01",
    );

    let (_status, response): (_, CommitRootTransactionResponse) = post_protobuf(
        router,
        "/api/v1/transactions/commitRootTransaction",
        &request,
        "idem-root-repair-01",
        "req-root-repair-01",
    )
    .await?;

    let receipt = response
        .receipt
        .context("root repair-pending receipt missing")?;
    assert!(response.repair_pending);

    let stored = load_root_tx_record(backend.clone(), &receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert!(stored.repair_pending);

    let root_commit_receipt = load_root_commit_receipt(backend, &receipt.root_commit_id).await?;
    assert!(root_commit_receipt.is_none());

    Ok(())
}

#[tokio::test]
async fn commit_root_transaction_retries_with_fresh_tx_id_after_super_manifest_failure()
-> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(FailRootSuperManifestBackend::new(1));
    let router = test_router_with_backend(backend.clone());
    let first = root_request(
        "idem-root-retry-01",
        "req-root-retry-01",
        "retry-root",
        "run-root-retry-01",
    );

    let (status, _) = post_error_json(
        router.clone(),
        "/api/v1/transactions/commitRootTransaction",
        &first,
        "idem-root-retry-01",
        "req-root-retry-01",
    )
    .await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let first_root_idem = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Root,
        "idem-root-retry-01",
    )
    .await?;
    let first_root_record = load_root_tx_record(backend.clone(), &first_root_idem.tx_id).await?;
    assert_eq!(first_root_record.status, ControlPlaneTxStatus::Aborted);

    let first_catalog_participant = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Catalog,
        "root:idem-root-retry-01:catalog",
    )
    .await?;
    let first_orchestration_participant = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Orchestration,
        "root:idem-root-retry-01:orchestration",
    )
    .await?;
    assert!(first_catalog_participant.visible_at.is_some());
    assert!(first_orchestration_participant.visible_at.is_some());

    let second = root_request(
        "idem-root-retry-01",
        "req-root-retry-02",
        "retry-root",
        "run-root-retry-01",
    );
    let (_status, response): (_, CommitRootTransactionResponse) = post_protobuf(
        router,
        "/api/v1/transactions/commitRootTransaction",
        &second,
        "idem-root-retry-01",
        "req-root-retry-02",
    )
    .await?;

    let receipt = response.receipt.context("root retry receipt missing")?;
    assert_ne!(receipt.tx_id, first_root_idem.tx_id);

    let catalog_commit = receipt
        .domain_commits
        .iter()
        .find(|commit| commit.domain == TransactionDomain::Catalog as i32)
        .context("retried catalog participant missing")?;
    assert_eq!(catalog_commit.tx_id, first_catalog_participant.tx_id);

    let orchestration_commit = receipt
        .domain_commits
        .iter()
        .find(|commit| commit.domain == TransactionDomain::Orchestration as i32)
        .context("retried orchestration participant missing")?;
    assert_eq!(
        orchestration_commit.tx_id,
        first_orchestration_participant.tx_id
    );

    Ok(())
}

#[tokio::test]
async fn apply_catalog_ddl_retries_same_key_after_missing_tx_record_is_stale() -> Result<()> {
    let fail_prefix = format!("tenant={TENANT}/workspace={WORKSPACE}/transactions/catalog/");
    let backend: Arc<dyn StorageBackend> = Arc::new(FailPrefixBackend::new(fail_prefix, 1));
    let mut config = arco_api::config::Config::default();
    config.idempotency_stale_timeout_secs = 0;
    let router = test_router_with_config_backend(config, backend.clone());

    let first = catalog_create_namespace_request(
        "idem-cat-stale-missing-01",
        "req-cat-stale-missing-01",
        "stale-missing",
    );
    let (status, _) = post_error_json(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &first,
        "idem-cat-stale-missing-01",
        "req-cat-stale-missing-01",
    )
    .await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let second = catalog_create_namespace_request(
        "idem-cat-stale-missing-01",
        "req-cat-stale-missing-02",
        "stale-missing",
    );
    let (_status, response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &second,
        "idem-cat-stale-missing-01",
        "req-cat-stale-missing-02",
    )
    .await?;

    assert!(response.receipt.is_some());

    let idem = load_idempotency_record(
        backend,
        ControlPlaneTxDomain::Catalog,
        "idem-cat-stale-missing-01",
    )
    .await?;
    assert!(idem.visible_at.is_some());

    Ok(())
}

#[tokio::test]
async fn apply_catalog_ddl_replays_from_cached_visible_record_after_finalize_write_failure()
-> Result<()> {
    let fail_prefix = format!("tenant={TENANT}/workspace={WORKSPACE}/transactions/catalog/");
    let backend: Arc<dyn StorageBackend> = Arc::new(FailPrefixOnNoneBackend::new(fail_prefix, 1));
    let router = test_router_with_backend(backend.clone());

    let first = catalog_create_namespace_request(
        "idem-cat-finalize-cache-01",
        "req-cat-finalize-cache-01",
        "cached-visible",
    );
    let (status, _) = post_error_json(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &first,
        "idem-cat-finalize-cache-01",
        "req-cat-finalize-cache-01",
    )
    .await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let idem = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Catalog,
        "idem-cat-finalize-cache-01",
    )
    .await?;
    assert!(idem.visible_at.is_some());
    assert!(idem.tx_record.is_some());

    let second = catalog_create_namespace_request(
        "idem-cat-finalize-cache-01",
        "req-cat-finalize-cache-02",
        "cached-visible",
    );
    let (_status, response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &second,
        "idem-cat-finalize-cache-01",
        "req-cat-finalize-cache-02",
    )
    .await?;

    let receipt = response
        .receipt
        .context("catalog cached replay receipt missing")?;
    assert_eq!(receipt.tx_id, idem.tx_id);

    let stored = load_catalog_tx_record(backend, &receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert_eq!(
        stored.result.as_ref().map(|result| result.tx_id.as_str()),
        Some(receipt.tx_id.as_str())
    );

    Ok(())
}

#[tokio::test]
async fn commit_orchestration_batch_marks_failed_attempt_aborted_and_retries_same_key() -> Result<()>
{
    let fail_prefix = format!("tenant={TENANT}/workspace={WORKSPACE}/ledger/orchestration/");
    let backend: Arc<dyn StorageBackend> = Arc::new(FailPrefixBackend::new(fail_prefix, 1));
    let mut config = arco_api::config::Config::default();
    config.idempotency_stale_timeout_secs = 0;
    let router = test_router_with_config_backend(config, backend.clone());

    let first = orchestration_request(
        "idem-orch-stale-prepared-01",
        "req-orch-stale-prepared-01",
        "run-stale-prepared-01",
    );
    let (status, _) = post_error_json(
        router.clone(),
        "/api/v1/transactions/commitOrchestrationBatch",
        &first,
        "idem-orch-stale-prepared-01",
        "req-orch-stale-prepared-01",
    )
    .await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let first_idem = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Orchestration,
        "idem-orch-stale-prepared-01",
    )
    .await?;
    let aborted = load_orchestration_tx_record(backend.clone(), &first_idem.tx_id).await?;
    assert_eq!(aborted.status, ControlPlaneTxStatus::Aborted);

    let second = orchestration_request(
        "idem-orch-stale-prepared-01",
        "req-orch-stale-prepared-02",
        "run-stale-prepared-01",
    );
    let (_status, response): (_, CommitOrchestrationBatchResponse) = post_protobuf(
        router,
        "/api/v1/transactions/commitOrchestrationBatch",
        &second,
        "idem-orch-stale-prepared-01",
        "req-orch-stale-prepared-02",
    )
    .await?;

    let receipt = response
        .receipt
        .context("orchestration retry receipt missing")?;
    assert_ne!(receipt.tx_id, first_idem.tx_id);
    assert!(!receipt.tx_id.is_empty());

    Ok(())
}

#[tokio::test]
async fn apply_catalog_ddl_rejects_unsupported_namespace_and_table_properties() -> Result<()> {
    let router = test_router();

    let namespace_request = ApplyCatalogDdlRequest {
        header: Some(request_header("idem-cat-props-01", "req-cat-props-01")),
        ddl: Some(CatalogDdlOperation {
            op: Some(CatalogDdlOp::CreateNamespace(CreateNamespaceOp {
                name: "props-ns".to_string(),
                description: Some("namespace properties unsupported".to_string()),
                properties: [("owner".to_string(), "platform".to_string())]
                    .into_iter()
                    .collect(),
            })),
        }),
        require_visible: Some(true),
    };
    let (status, error) = post_error_json(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &namespace_request,
        "idem-cat-props-01",
        "req-cat-props-01",
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["code"], "BAD_REQUEST");

    let table_request = ApplyCatalogDdlRequest {
        header: Some(request_header("idem-cat-props-02", "req-cat-props-02")),
        ddl: Some(CatalogDdlOperation {
            op: Some(CatalogDdlOp::RegisterTable(RegisterTableOp {
                namespace: "analytics".to_string(),
                name: "events".to_string(),
                description: Some("table properties unsupported".to_string()),
                location: None,
                format: None,
                columns: vec![CatalogColumnDefinition {
                    name: "id".to_string(),
                    data_type: "STRING".to_string(),
                    is_nullable: false,
                    description: None,
                }],
                properties: [("quality".to_string(), "gold".to_string())]
                    .into_iter()
                    .collect(),
            })),
        }),
        require_visible: Some(true),
    };
    let (status, error) = post_error_json(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &table_request,
        "idem-cat-props-02",
        "req-cat-props-02",
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["code"], "BAD_REQUEST");

    Ok(())
}

#[tokio::test]
async fn commit_orchestration_batch_rejects_unsupported_allow_inline_merge() -> Result<()> {
    let router = test_router();
    let mut request =
        orchestration_request("idem-orch-inline-01", "req-orch-inline-01", "run-inline-01");
    request.allow_inline_merge = Some(true);

    let (status, error) = post_error_json(
        router,
        "/api/v1/transactions/commitOrchestrationBatch",
        &request,
        "idem-orch-inline-01",
        "req-orch-inline-01",
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["code"], "BAD_REQUEST");

    Ok(())
}

#[tokio::test]
async fn apply_catalog_ddl_propagates_actor_to_catalog_ledger_events() -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let router = test_router_with_backend(backend.clone());
    let request =
        catalog_create_namespace_request("idem-cat-actor-01", "req-cat-actor-01", "actor");

    let (_status, response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &request,
        "idem-cat-actor-01",
        "req-cat-actor-01",
    )
    .await?;

    let receipt = response.receipt.context("catalog actor receipt missing")?;
    let source = load_catalog_ledger_event_source(backend, &receipt.event_id).await?;
    assert_eq!(source, format!("api:{TENANT}"));

    Ok(())
}

#[tokio::test]
async fn apply_catalog_ddl_propagates_repair_pending_when_legacy_mirror_write_fails() -> Result<()>
{
    let fail_path =
        format!("tenant={TENANT}/workspace={WORKSPACE}/manifests/catalog.manifest.json");
    let backend: Arc<dyn StorageBackend> = Arc::new(FailPathBackend::new(fail_path, 1));
    let router = test_router_with_backend(backend.clone());
    let request =
        catalog_create_namespace_request("idem-cat-repair-01", "req-cat-repair-01", "repairing");

    let (_status, response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &request,
        "idem-cat-repair-01",
        "req-cat-repair-01",
    )
    .await?;

    let receipt = response.receipt.context("catalog repair receipt missing")?;
    assert!(response.repair_pending);

    let stored = load_catalog_tx_record(backend, &receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert!(stored.repair_pending);

    Ok(())
}

#[tokio::test]
async fn commit_orchestration_batch_propagates_repair_pending_when_legacy_mirror_write_fails()
-> Result<()> {
    let fail_path =
        format!("tenant={TENANT}/workspace={WORKSPACE}/state/orchestration/manifest.json");
    let backend: Arc<dyn StorageBackend> = Arc::new(FailPathBackend::new(fail_path, 1));
    let router = test_router_with_backend(backend.clone());
    let request =
        orchestration_request("idem-orch-repair-01", "req-orch-repair-01", "run-repair-01");

    let (_status, response): (_, CommitOrchestrationBatchResponse) = post_protobuf(
        router,
        "/api/v1/transactions/commitOrchestrationBatch",
        &request,
        "idem-orch-repair-01",
        "req-orch-repair-01",
    )
    .await?;

    let receipt = response
        .receipt
        .context("orchestration repair receipt missing")?;
    assert!(response.repair_pending);

    let stored = load_orchestration_tx_record(backend, &receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert!(stored.repair_pending);

    Ok(())
}

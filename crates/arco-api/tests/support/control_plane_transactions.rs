#![allow(dead_code)]

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
use arco_core::ScopedStorage;
use arco_core::control_plane_transactions::{
    CatalogTxRecord, ControlPlaneIdempotencyRecord, ControlPlaneTxDomain, ControlPlaneTxPaths,
    OrchestrationTxRecord, RootTxRecord,
};
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_proto::arco::catalog::v1::{
    CatalogDdlOperation, ColumnDefinition, CreateCatalogOp, CreateSchemaOp, DropTableOp,
    RegisterTableOp, RenameTableOp, UpdateTableOp, catalog_ddl_operation,
};
use arco_proto::arco::common::v1::TableFormat;
use arco_proto::arco::controlplane::v1::{
    ApplyCatalogDdlRequest, CommitOrchestrationBatchRequest, CommitRootTransactionRequest,
    DomainMutation, OrchestrationBatchSpec, domain_mutation,
};
use arco_proto::arco::orchestration::v1::{
    ManualTrigger, OrchestrationEventEnvelope, RunTriggered, TriggerInfo,
    orchestration_event_envelope, trigger_info,
};

pub const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";
pub const TENANT: &str = "test-tenant";
pub const WORKSPACE: &str = "test-workspace";

pub fn test_router() -> axum::Router {
    ServerBuilder::new().debug(true).build().test_router()
}

pub fn test_router_with_backend(backend: Arc<dyn StorageBackend>) -> axum::Router {
    test_router_with_config_backend(arco_api::config::Config::default(), backend)
}

pub fn test_router_with_config_backend(
    mut config: arco_api::config::Config,
    backend: Arc<dyn StorageBackend>,
) -> axum::Router {
    config.debug = true;
    Server::with_storage_backend(config, backend).test_router()
}

pub fn protobuf_request<T: Message>(
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

pub async fn post_protobuf<TReq, TResp>(
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

pub async fn post_error_json<TReq: Message>(
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

pub fn catalog_create_namespace_request(
    idempotency_key: &str,
    request_id: &str,
    name: &str,
) -> ApplyCatalogDdlRequest {
    catalog_create_schema_request(idempotency_key, request_id, "default", name)
}

pub fn catalog_create_catalog_request(
    idempotency_key: &str,
    request_id: &str,
    name: &str,
) -> ApplyCatalogDdlRequest {
    let _ = idempotency_key;
    let _ = request_id;
    ApplyCatalogDdlRequest {
        ddl: Some(CatalogDdlOperation {
            op: Some(catalog_ddl_operation::Op::CreateCatalog(CreateCatalogOp {
                name: name.to_string(),
                description: Some("control-plane transaction catalog".to_string()),
            })),
        }),
    }
}

pub fn catalog_create_schema_request(
    idempotency_key: &str,
    request_id: &str,
    catalog_name: &str,
    schema_name: &str,
) -> ApplyCatalogDdlRequest {
    let _ = idempotency_key;
    let _ = request_id;
    ApplyCatalogDdlRequest {
        ddl: Some(CatalogDdlOperation {
            op: Some(catalog_ddl_operation::Op::CreateSchema(CreateSchemaOp {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                description: Some("control-plane transaction test".to_string()),
            })),
        }),
    }
}

pub fn catalog_register_table_request(
    idempotency_key: &str,
    request_id: &str,
    namespace: &str,
    name: &str,
) -> ApplyCatalogDdlRequest {
    catalog_register_table_in_schema_request(
        idempotency_key,
        request_id,
        "default",
        namespace,
        name,
    )
}

pub fn catalog_register_table_in_schema_request(
    idempotency_key: &str,
    request_id: &str,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
) -> ApplyCatalogDdlRequest {
    catalog_register_table_in_schema_request_with_columns(
        idempotency_key,
        request_id,
        catalog_name,
        schema_name,
        table_name,
        vec![ColumnDefinition {
            name: "id".to_string(),
            data_type: "STRING".to_string(),
            is_nullable: false,
            ordinal: 0,
            description: None,
        }],
    )
}

pub fn catalog_register_table_in_schema_request_with_columns(
    idempotency_key: &str,
    request_id: &str,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    columns: Vec<ColumnDefinition>,
) -> ApplyCatalogDdlRequest {
    let _ = idempotency_key;
    let _ = request_id;
    ApplyCatalogDdlRequest {
        ddl: Some(CatalogDdlOperation {
            op: Some(catalog_ddl_operation::Op::RegisterTable(RegisterTableOp {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                table_name: table_name.to_string(),
                description: Some("control-plane transaction table".to_string()),
                location: None,
                format: TableFormat::Unspecified as i32,
                columns,
            })),
        }),
    }
}

pub fn catalog_alter_table_request(
    idempotency_key: &str,
    request_id: &str,
    namespace: &str,
    name: &str,
    description: Option<&str>,
) -> ApplyCatalogDdlRequest {
    let _ = idempotency_key;
    let _ = request_id;
    ApplyCatalogDdlRequest {
        ddl: Some(CatalogDdlOperation {
            op: Some(catalog_ddl_operation::Op::UpdateTable(UpdateTableOp {
                catalog_name: "default".to_string(),
                schema_name: namespace.to_string(),
                table_name: name.to_string(),
                description: description.map(str::to_string),
                location: None,
                format: None,
            })),
        }),
    }
}

pub fn catalog_drop_table_request(
    idempotency_key: &str,
    request_id: &str,
    namespace: &str,
    name: &str,
) -> ApplyCatalogDdlRequest {
    let _ = idempotency_key;
    let _ = request_id;
    ApplyCatalogDdlRequest {
        ddl: Some(CatalogDdlOperation {
            op: Some(catalog_ddl_operation::Op::DropTable(DropTableOp {
                catalog_name: "default".to_string(),
                schema_name: namespace.to_string(),
                table_name: name.to_string(),
            })),
        }),
    }
}

pub fn catalog_rename_table_request(
    idempotency_key: &str,
    request_id: &str,
    catalog_name: &str,
    schema_name: &str,
    old_table_name: &str,
    new_table_name: &str,
) -> ApplyCatalogDdlRequest {
    let _ = idempotency_key;
    let _ = request_id;
    ApplyCatalogDdlRequest {
        ddl: Some(CatalogDdlOperation {
            op: Some(catalog_ddl_operation::Op::RenameTable(RenameTableOp {
                catalog_name: catalog_name.to_string(),
                schema_name: schema_name.to_string(),
                old_table_name: old_table_name.to_string(),
                new_table_name: new_table_name.to_string(),
            })),
        }),
    }
}

pub fn orchestration_request(
    idempotency_key: &str,
    request_id: &str,
    run_id: &str,
) -> CommitOrchestrationBatchRequest {
    orchestration_request_with_event_id(
        idempotency_key,
        request_id,
        run_id,
        "01JTXORCH000000000000000001",
    )
}

pub fn orchestration_request_with_event_id(
    idempotency_key: &str,
    _request_id: &str,
    run_id: &str,
    event_id: &str,
) -> CommitOrchestrationBatchRequest {
    let _ = idempotency_key;
    CommitOrchestrationBatchRequest {
        events: vec![OrchestrationEventEnvelope {
            event_id: event_id.to_string(),
            event_version: 1,
            timestamp: Some(prost_types::Timestamp {
                seconds: 1_776_000_000,
                nanos: 0,
            }),
            source: format!("arco-flow/{TENANT}/{WORKSPACE}"),
            idempotency_key: format!("event:{run_id}"),
            correlation_id: Some(run_id.to_string()),
            causation_id: None,
            event: Some(orchestration_event_envelope::Event::RunTriggered(
                RunTriggered {
                    run_id: run_id.to_string(),
                    plan_id: format!("plan-{run_id}"),
                    trigger: Some(TriggerInfo {
                        trigger: Some(trigger_info::Trigger::Manual(ManualTrigger {
                            user_id: "tester".to_string(),
                            request_id: None,
                        })),
                    }),
                    root_assets: Vec::new(),
                    run_key: Some(format!("manual:{run_id}")),
                    labels: Default::default(),
                    code_version: None,
                },
            )),
        }],
    }
}

pub fn root_request(
    idempotency_key: &str,
    request_id: &str,
    namespace: &str,
    run_id: &str,
) -> CommitRootTransactionRequest {
    let _ = idempotency_key;
    CommitRootTransactionRequest {
        mutations: vec![
            DomainMutation {
                kind: Some(domain_mutation::Kind::Catalog(CatalogDdlOperation {
                    op: Some(catalog_ddl_operation::Op::CreateSchema(CreateSchemaOp {
                        catalog_name: "default".to_string(),
                        schema_name: namespace.to_string(),
                        description: Some("root transaction namespace".to_string()),
                    })),
                })),
            },
            DomainMutation {
                kind: Some(domain_mutation::Kind::Orchestration(
                    OrchestrationBatchSpec {
                        events: orchestration_request(idempotency_key, request_id, run_id).events,
                    },
                )),
            },
        ],
    }
}

pub fn scoped_storage(backend: Arc<dyn StorageBackend>) -> ScopedStorage {
    ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage")
}

pub async fn load_catalog_tx_record(
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

pub async fn load_orchestration_tx_record(
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

pub async fn load_root_tx_record(
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

pub async fn load_idempotency_record(
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

#[derive(Debug)]
pub struct FailPrefixOnNoneBackend {
    inner: MemoryBackend,
    fail_prefix: String,
    remaining_failures: std::sync::atomic::AtomicUsize,
}

impl FailPrefixOnNoneBackend {
    pub fn new(fail_prefix: impl Into<String>, failures: usize) -> Self {
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
pub struct FailReadsAfterTriggerPutBackend {
    inner: MemoryBackend,
    trigger_path: String,
    trigger_after_puts: usize,
    fail_get_paths: Vec<String>,
    fail_head_paths: Vec<String>,
    trigger_seen: std::sync::atomic::AtomicBool,
    trigger_puts_seen: std::sync::atomic::AtomicUsize,
    remaining_failures: std::sync::atomic::AtomicUsize,
}

impl FailReadsAfterTriggerPutBackend {
    pub fn new(
        trigger_path: impl Into<String>,
        trigger_after_puts: usize,
        fail_get_paths: Vec<String>,
        fail_head_paths: Vec<String>,
        failures: usize,
    ) -> Self {
        Self {
            inner: MemoryBackend::new(),
            trigger_path: trigger_path.into(),
            trigger_after_puts,
            fail_get_paths,
            fail_head_paths,
            trigger_seen: std::sync::atomic::AtomicBool::new(false),
            trigger_puts_seen: std::sync::atomic::AtomicUsize::new(0),
            remaining_failures: std::sync::atomic::AtomicUsize::new(failures),
        }
    }

    fn should_fail_path(&self, path: &str, candidates: &[String]) -> bool {
        use std::sync::atomic::Ordering;

        self.trigger_seen.load(Ordering::SeqCst)
            && candidates.iter().any(|candidate| candidate == path)
            && self.remaining_failures.load(Ordering::SeqCst) > 0
    }

    fn consume_failure(&self, path: &str) -> arco_core::Error {
        use std::sync::atomic::Ordering;

        self.remaining_failures.fetch_sub(1, Ordering::SeqCst);
        arco_core::Error::storage(format!(
            "injected post-visibility readback failure for {path}"
        ))
    }
}

#[async_trait]
impl StorageBackend for FailReadsAfterTriggerPutBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        if self.should_fail_path(path, &self.fail_get_paths) {
            return Err(self.consume_failure(path));
        }

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

        let result = self.inner.put(path, data, precondition).await;
        if path == self.trigger_path && matches!(result, Ok(WriteResult::Success { .. })) {
            let puts_seen = self.trigger_puts_seen.fetch_add(1, Ordering::SeqCst) + 1;
            if puts_seen >= self.trigger_after_puts {
                self.trigger_seen.store(true, Ordering::SeqCst);
            }
        }
        result
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        if self.should_fail_path(path, &self.fail_head_paths) {
            return Err(self.consume_failure(path));
        }

        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

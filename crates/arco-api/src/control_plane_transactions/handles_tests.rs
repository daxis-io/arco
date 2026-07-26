use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{TimeZone as _, Utc};
use sha2::{Digest as _, Sha256};
use tokio::sync::Notify;

use arco_core::FlowPaths;
use arco_core::catalog_paths::{CatalogDomain, CatalogPaths};
use arco_core::control_plane_transactions::{
    CatalogTxReceipt, ControlPlaneHandleFailureCategory, ControlPlaneHandleRecord,
    ControlPlaneHandleScope, ControlPlaneHandleStatus, ControlPlaneIdempotencyRecord,
    ControlPlaneTxDomain, ControlPlaneTxKind, ControlPlaneTxPaths, ControlPlaneTxRecord,
    ControlPlaneTxStatus, OrchestrationTxReceipt, RootTxManifest, RootTxReceipt,
};
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_proto::arco::catalog::v1::{
    CatalogDdlOperation, CatalogObjectLifecycleState, ColumnDefinition as ProtoColumnDefinition,
    CreateCatalogOp, CreateSchemaOp, MetastoreMutation, RegisterTableOp, StorageCredential,
    TableFormat, catalog_ddl_operation, metastore_mutation,
};
use arco_proto::arco::common::v1::{PartitionDimension, PartitionKey};
use arco_proto::arco::controlplane::v1::ApplyCatalogDdlRequest;
use arco_proto::arco::controlplane::v1::{DomainMutation, OrchestrationBatchSpec, domain_mutation};
use arco_proto::arco::orchestration::v1::{
    ManualTrigger, OrchestrationEventEnvelope, RunTriggered, TaskCallbackOutput, TaskFinished,
    TaskOutcome, TriggerInfo, orchestration_event_envelope, trigger_info,
};

use super::ControlPlaneTransactionService;
use super::handles::{ControlPlaneTransactionHandleService, guard_legacy_handle_identity};
use crate::config::Config;
use crate::context::RequestContext;
use crate::server::AppState;

const TENANT: &str = "tenant-handle-tests";
const WORKSPACE: &str = "workspace-handle-tests";

#[derive(Debug)]
struct PutFailure {
    contains: String,
    skip: usize,
}

#[derive(Debug)]
struct PutGate {
    target: PutGateTarget,
    skip: usize,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    attempted: Option<Arc<Mutex<Option<Bytes>>>>,
}

#[derive(Debug)]
struct HeadGate {
    target: PutGateTarget,
    skip: usize,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    observed_path: Arc<Mutex<Option<String>>>,
}

#[derive(Debug)]
enum PutGateTarget {
    Contains(String),
    TransactionRecord(ControlPlaneTxDomain),
}

impl PutGateTarget {
    fn matches(&self, path: &str) -> bool {
        match self {
            Self::Contains(contains) => path.contains(contains),
            Self::TransactionRecord(domain) => {
                let prefix = format!("transactions/{}/", domain.as_str());
                path.rsplit_once(&prefix)
                    .map(|(_, suffix)| suffix)
                    .and_then(|suffix| suffix.strip_suffix(".json"))
                    .is_some_and(|tx_id| {
                        ulid::Ulid::from_string(tx_id)
                            .is_ok_and(|parsed| parsed.to_string() == tx_id)
                    })
            }
        }
    }
}

#[derive(Debug)]
struct NoListFaultBackend {
    inner: MemoryBackend,
    put_failure: Mutex<Option<PutFailure>>,
    put_after_write_failure: Mutex<Option<PutFailure>>,
    put_corruption: Mutex<Option<String>>,
    put_gate: Mutex<Option<PutGate>>,
    head_gate: Mutex<Option<HeadGate>>,
    list_calls: AtomicUsize,
}

impl NoListFaultBackend {
    fn new() -> Self {
        Self {
            inner: MemoryBackend::new(),
            put_failure: Mutex::new(None),
            put_after_write_failure: Mutex::new(None),
            put_corruption: Mutex::new(None),
            put_gate: Mutex::new(None),
            head_gate: Mutex::new(None),
            list_calls: AtomicUsize::new(0),
        }
    }

    fn fail_next_matching_put(&self, contains: impl Into<String>, skip: usize) {
        *self.put_failure.lock().expect("put failure mutex") = Some(PutFailure {
            contains: contains.into(),
            skip,
        });
    }

    fn fail_after_next_matching_put(&self, contains: impl Into<String>, skip: usize) {
        *self
            .put_after_write_failure
            .lock()
            .expect("post-write failure mutex") = Some(PutFailure {
            contains: contains.into(),
            skip,
        });
    }

    fn clear_failure(&self) {
        *self.put_failure.lock().expect("put failure mutex") = None;
        *self
            .put_after_write_failure
            .lock()
            .expect("post-write failure mutex") = None;
    }

    fn corrupt_next_matching_put(&self, contains: impl Into<String>) {
        *self.put_corruption.lock().expect("put corruption mutex") = Some(contains.into());
    }

    fn gate_next_matching_put(&self, contains: impl Into<String>) -> (Arc<Notify>, Arc<Notify>) {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *self.put_gate.lock().expect("put gate mutex") = Some(PutGate {
            target: PutGateTarget::Contains(contains.into()),
            skip: 0,
            entered: entered.clone(),
            release: release.clone(),
            attempted: None,
        });
        (entered, release)
    }

    fn gate_matching_put_after(
        &self,
        contains: impl Into<String>,
        skip: usize,
    ) -> (Arc<Notify>, Arc<Notify>, Arc<Mutex<Option<Bytes>>>) {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let attempted = Arc::new(Mutex::new(None));
        *self.put_gate.lock().expect("put gate mutex") = Some(PutGate {
            target: PutGateTarget::Contains(contains.into()),
            skip,
            entered: entered.clone(),
            release: release.clone(),
            attempted: Some(attempted.clone()),
        });
        (entered, release, attempted)
    }

    fn gate_transaction_record_put_after(
        &self,
        domain: ControlPlaneTxDomain,
        skip: usize,
    ) -> (Arc<Notify>, Arc<Notify>, Arc<Mutex<Option<Bytes>>>) {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let attempted = Arc::new(Mutex::new(None));
        *self.put_gate.lock().expect("put gate mutex") = Some(PutGate {
            target: PutGateTarget::TransactionRecord(domain),
            skip,
            entered: entered.clone(),
            release: release.clone(),
            attempted: Some(attempted.clone()),
        });
        (entered, release, attempted)
    }

    fn gate_transaction_record_head_after(
        &self,
        domain: ControlPlaneTxDomain,
        skip: usize,
    ) -> (Arc<Notify>, Arc<Notify>, Arc<Mutex<Option<String>>>) {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let observed_path = Arc::new(Mutex::new(None));
        *self.head_gate.lock().expect("head gate mutex") = Some(HeadGate {
            target: PutGateTarget::TransactionRecord(domain),
            skip,
            entered: entered.clone(),
            release: release.clone(),
            observed_path: observed_path.clone(),
        });
        (entered, release, observed_path)
    }

    fn list_calls(&self) -> usize {
        self.list_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl StorageBackend for NoListFaultBackend {
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
        let should_fail = {
            let mut guard = self.put_failure.lock().expect("put failure mutex");
            let should_fail = match guard.as_mut() {
                Some(rule) if path.contains(&rule.contains) && rule.skip == 0 => true,
                Some(rule) if path.contains(&rule.contains) => {
                    rule.skip = rule.skip.saturating_sub(1);
                    false
                }
                _ => false,
            };
            if should_fail {
                guard.take();
            }
            should_fail
        };
        if should_fail {
            return Err(arco_core::Error::storage(
                "injected handle test put failure",
            ));
        }
        let gate = {
            let mut guard = self.put_gate.lock().expect("put gate mutex");
            match guard.as_mut() {
                Some(gate) if gate.target.matches(path) && gate.skip == 0 => guard.take(),
                Some(gate) if gate.target.matches(path) => {
                    gate.skip = gate.skip.saturating_sub(1);
                    None
                }
                _ => None,
            }
        };
        if let Some(gate) = gate {
            if let Some(attempted) = gate.attempted {
                *attempted.lock().expect("put gate attempted bytes mutex") = Some(data.clone());
            }
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        let data = {
            let mut guard = self.put_corruption.lock().expect("put corruption mutex");
            if guard
                .as_ref()
                .is_some_and(|contains| path.contains(contains))
            {
                guard.take();
                Bytes::from_static(br#"{"corrupt":true}"#)
            } else {
                data
            }
        };
        let result = self.inner.put(path, data, precondition).await?;
        let should_fail_after = {
            let mut guard = self
                .put_after_write_failure
                .lock()
                .expect("post-write failure mutex");
            let should_fail = match guard.as_mut() {
                Some(rule) if path.contains(&rule.contains) && rule.skip == 0 => true,
                Some(rule) if path.contains(&rule.contains) => {
                    rule.skip = rule.skip.saturating_sub(1);
                    false
                }
                _ => false,
            };
            if should_fail {
                guard.take();
            }
            should_fail
        };
        if should_fail_after {
            return Err(arco_core::Error::storage(
                "injected handle test post-write failure",
            ));
        }
        Ok(result)
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, _prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        Err(arco_core::Error::storage(
            "list is forbidden in durable handle operations",
        ))
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        let gate = {
            let mut guard = self.head_gate.lock().expect("head gate mutex");
            match guard.as_mut() {
                Some(gate) if gate.target.matches(path) && gate.skip == 0 => guard.take(),
                Some(gate) if gate.target.matches(path) => {
                    gate.skip = gate.skip.saturating_sub(1);
                    None
                }
                _ => None,
            }
        };
        if let Some(gate) = gate {
            *gate
                .observed_path
                .lock()
                .expect("head gate observed path mutex") = Some(path.to_string());
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

fn instant(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("test instant")
}

fn request_context() -> RequestContext {
    RequestContext {
        tenant: TENANT.to_string(),
        workspace: WORKSPACE.to_string(),
        user_id: Some("user-handle-tests".to_string()),
        groups: vec!["group:reviewers".to_string()],
        request_id: "request-handle-tests".to_string(),
        idempotency_key: None,
    }
}

fn service(backend: Arc<dyn StorageBackend>) -> (AppState, RequestContext) {
    (AppState::new(Config::default(), backend), request_context())
}

fn create_catalog(name: &str) -> CatalogDdlOperation {
    CatalogDdlOperation {
        op: Some(catalog_ddl_operation::Op::CreateCatalog(CreateCatalogOp {
            catalog: name.to_string(),
            description: Some("staged through a typed handle".to_string()),
        })),
    }
}

fn create_schema(catalog: &str, schema: &str) -> CatalogDdlOperation {
    CatalogDdlOperation {
        op: Some(catalog_ddl_operation::Op::CreateSchema(CreateSchemaOp {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            description: Some("staged through a typed handle".to_string()),
        })),
    }
}

fn register_table(location: &str) -> CatalogDdlOperation {
    CatalogDdlOperation {
        op: Some(catalog_ddl_operation::Op::RegisterTable(RegisterTableOp {
            catalog: "default".to_string(),
            schema: "default".to_string(),
            table: "secret_probe".to_string(),
            description: None,
            location: Some(location.to_string()),
            format: Some(TableFormat::Delta as i32),
            columns: Vec::new(),
        })),
    }
}

fn credential_mutation() -> DomainMutation {
    DomainMutation {
        kind: Some(domain_mutation::Kind::Metastore(MetastoreMutation {
            op: Some(metastore_mutation::Op::StorageCredential(
                StorageCredential {
                    credential_id: "credential-secret".to_string(),
                    name: "production-secret".to_string(),
                    cloud: "aws".to_string(),
                    owner: "group:data-platform".to_string(),
                    lifecycle_state: CatalogObjectLifecycleState::Active as i32,
                    ..Default::default()
                },
            )),
        })),
    }
}

fn catalog_domain_mutation(name: &str) -> DomainMutation {
    DomainMutation {
        kind: Some(domain_mutation::Kind::Catalog(create_catalog(name))),
    }
}

fn orchestration_batch(run_id: &str, event_id: &str) -> OrchestrationBatchSpec {
    OrchestrationBatchSpec {
        events: vec![OrchestrationEventEnvelope {
            event_id: event_id.to_string(),
            event_version: 1,
            timestamp: Some(prost_types::Timestamp {
                seconds: 1_784_000_000,
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
                            user_id: "handle-reviewer".to_string(),
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

fn orchestration_event_path(event_id: &str) -> String {
    let timestamp_ms = ulid::Ulid::from_string(event_id)
        .expect("canonical event ULID")
        .timestamp_ms();
    let timestamp_ms = i64::try_from(timestamp_ms).expect("event timestamp fits i64");
    let date = chrono::DateTime::<Utc>::from_timestamp_millis(timestamp_ms)
        .expect("event timestamp")
        .format("%Y-%m-%d")
        .to_string();
    FlowPaths::orchestration_event_path(&date, event_id)
}

fn task_finished_batch(source: &str, output_path: Option<&str>) -> OrchestrationBatchSpec {
    OrchestrationBatchSpec {
        events: vec![OrchestrationEventEnvelope {
            event_id: "01J00000000000000000000001".to_string(),
            event_version: 1,
            timestamp: Some(prost_types::Timestamp {
                seconds: 1_784_000_000,
                nanos: 0,
            }),
            source: source.to_string(),
            idempotency_key: "event:handle-location:task-finished".to_string(),
            correlation_id: Some("run-handle-location".to_string()),
            causation_id: None,
            event: Some(orchestration_event_envelope::Event::TaskFinished(
                TaskFinished {
                    run_id: "run-handle-location".to_string(),
                    task_key: "extract".to_string(),
                    attempt: 1,
                    attempt_id: "attempt-handle-location".to_string(),
                    worker_id: "worker-handle-location".to_string(),
                    outcome: TaskOutcome::Succeeded as i32,
                    callback_output: output_path.map(|path| TaskCallbackOutput {
                        output_path: Some(path.to_string()),
                        ..Default::default()
                    }),
                    error: None,
                    metrics: None,
                    cancelled_during_phase: None,
                    asset_key: None,
                    partition_key: None,
                    code_version: None,
                },
            )),
        }],
    }
}

async fn force_committing(
    backend: Arc<dyn StorageBackend>,
    handle_id: &str,
    now: chrono::DateTime<Utc>,
) -> ControlPlaneHandleRecord {
    let storage = request_context()
        .scoped_storage(backend)
        .expect("scoped storage");
    let path = ControlPlaneTxPaths::handle_record(handle_id).expect("handle path");
    let metadata = storage
        .head_raw(&path)
        .await
        .expect("head handle")
        .expect("handle metadata");
    let bytes = storage.get_raw(&path).await.expect("get handle");
    let mut record =
        ControlPlaneHandleRecord::from_json_slice(bytes.as_ref()).expect("decode handle");
    record.status = ControlPlaneHandleStatus::Committing;
    record.revision += 1;
    record.updated_at = now;
    record.committing_at = Some(now);
    record.validate().expect("valid committing record");
    let result = storage
        .put_raw(
            &path,
            Bytes::from(record.to_json_vec().expect("encode handle")),
            WritePrecondition::MatchesVersion(metadata.version),
        )
        .await
        .expect("force committing CAS");
    assert!(matches!(result, WriteResult::Success { .. }));
    record
}

async fn put_json<T: serde::Serialize>(backend: Arc<dyn StorageBackend>, path: &str, value: &T) {
    let storage = request_context()
        .scoped_storage(backend)
        .expect("scoped storage");
    let bytes = serde_json::to_vec(value).expect("encode seeded JSON");
    let result = storage
        .put_raw(path, Bytes::from(bytes), WritePrecondition::DoesNotExist)
        .await
        .expect("seed JSON");
    assert!(matches!(result, WriteResult::Success { .. }));
}

async fn raw_object_snapshot(
    backend: Arc<dyn StorageBackend>,
    path: &str,
) -> Option<(Bytes, String)> {
    let storage = request_context()
        .scoped_storage(backend)
        .expect("scoped storage");
    let metadata = storage.head_raw(path).await.expect("head raw object")?;
    let bytes = storage.get_raw(path).await.expect("read raw object");
    Some((bytes, metadata.version))
}

#[derive(Clone, Copy)]
enum SeededExactPredecessor {
    Missing,
    Prepared,
    Aborted,
    ExactVisibleMarkerOnly,
}

impl SeededExactPredecessor {
    const fn caches_visible_record(self) -> bool {
        !matches!(self, Self::ExactVisibleMarkerOnly)
    }

    fn exact_record(
        self,
        visible: &ControlPlaneTxRecord<serde_json::Value>,
    ) -> Option<ControlPlaneTxRecord<serde_json::Value>> {
        match self {
            Self::Missing => None,
            Self::Prepared | Self::Aborted => {
                let mut record = visible.clone();
                record.status = match self {
                    Self::Prepared => ControlPlaneTxStatus::Prepared,
                    Self::Aborted => ControlPlaneTxStatus::Aborted,
                    Self::Missing | Self::ExactVisibleMarkerOnly => unreachable!(),
                };
                record.visible_at = None;
                record.result = None;
                Some(record)
            }
            Self::ExactVisibleMarkerOnly => Some(visible.clone()),
        }
    }
}

async fn seed_aborted_catalog_claim(
    backend: Arc<dyn StorageBackend>,
    identity: &str,
    operation: &CatalogDdlOperation,
    now: chrono::DateTime<Utc>,
) -> String {
    let tx_id = ulid::Ulid::new().to_string();
    let request_hash = catalog_request_hash(operation);
    put_json(
        backend.clone(),
        &ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Catalog, identity),
        &ControlPlaneIdempotencyRecord {
            tx_id: tx_id.clone(),
            kind: ControlPlaneTxKind::CatalogDdl,
            request_id: identity.to_string(),
            idempotency_key: identity.to_string(),
            request_hash: request_hash.clone(),
            created_at: now,
            visible_at: None,
            tx_record: None,
        },
    )
    .await;
    put_json(
        backend,
        &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &tx_id),
        &ControlPlaneTxRecord::<serde_json::Value> {
            tx_id: tx_id.clone(),
            kind: ControlPlaneTxKind::CatalogDdl,
            status: ControlPlaneTxStatus::Aborted,
            repair_pending: false,
            request_id: identity.to_string(),
            idempotency_key: identity.to_string(),
            request_hash,
            lock_path: CatalogPaths::domain_lock(CatalogDomain::Catalog),
            fencing_token: 0,
            prepared_at: now,
            visible_at: None,
            durable_append: None,
            result: None,
        },
    )
    .await;
    tx_id
}

async fn rewrite_staged_and_rebind(
    backend: Arc<dyn StorageBackend>,
    handle_id: &str,
    ordinal: u64,
    rewrite: impl FnOnce(&mut serde_json::Value),
) {
    let storage = request_context()
        .scoped_storage(backend)
        .expect("scoped storage");
    let mutation_path =
        ControlPlaneTxPaths::handle_mutation(handle_id, ordinal).expect("mutation path");
    let mut mutation: serde_json::Value = serde_json::from_slice(
        storage
            .get_raw(&mutation_path)
            .await
            .expect("read mutation")
            .as_ref(),
    )
    .expect("decode mutation");
    rewrite(&mut mutation);
    let mutation_bytes = serde_json::to_vec(&mutation).expect("encode rewritten mutation");
    let rebound_digest = format!("sha256:{:x}", Sha256::digest(&mutation_bytes));
    storage
        .put_raw(
            &mutation_path,
            Bytes::from(mutation_bytes.clone()),
            WritePrecondition::None,
        )
        .await
        .expect("rewrite mutation");

    let handle_path = ControlPlaneTxPaths::handle_record(handle_id).expect("handle path");
    let metadata = storage
        .head_raw(&handle_path)
        .await
        .expect("head handle")
        .expect("handle metadata");
    let mut handle = ControlPlaneHandleRecord::from_json_slice(
        storage
            .get_raw(&handle_path)
            .await
            .expect("read handle")
            .as_ref(),
    )
    .expect("decode handle");
    let index = usize::try_from(ordinal - 1).expect("ordinal index");
    handle
        .mutation_refs
        .get_mut(index)
        .expect("mutation reference")
        .sha256
        .clone_from(&rebound_digest);
    storage
        .put_raw(
            &handle_path,
            Bytes::from(handle.to_json_vec().expect("encode rebound handle")),
            WritePrecondition::MatchesVersion(metadata.version),
        )
        .await
        .expect("rebind handle digest");

    let authority_path = format!("transactions/handles/{handle_id}/identities/{ordinal:020}.json");
    let mut authority: serde_json::Value = serde_json::from_slice(
        storage
            .get_raw(&authority_path)
            .await
            .expect("read identity authority")
            .as_ref(),
    )
    .expect("decode identity authority");
    authority["handle_intent"]["mutation_ref"]["sha256"] = serde_json::json!(rebound_digest);
    storage
        .put_raw(
            &authority_path,
            Bytes::from(serde_json::to_vec(&authority).expect("encode rebound authority")),
            WritePrecondition::None,
        )
        .await
        .expect("rebind identity authority digest");
}

async fn replace_staged_bytes_and_rebind(
    backend: Arc<dyn StorageBackend>,
    handle_id: &str,
    ordinal: u64,
    mutation_bytes: Vec<u8>,
) {
    let storage = request_context()
        .scoped_storage(backend)
        .expect("scoped storage");
    let mutation_path =
        ControlPlaneTxPaths::handle_mutation(handle_id, ordinal).expect("mutation path");
    let rebound_digest = format!("sha256:{:x}", Sha256::digest(&mutation_bytes));
    storage
        .put_raw(
            &mutation_path,
            Bytes::from(mutation_bytes),
            WritePrecondition::None,
        )
        .await
        .expect("replace mutation bytes");

    let handle_path = ControlPlaneTxPaths::handle_record(handle_id).expect("handle path");
    let metadata = storage
        .head_raw(&handle_path)
        .await
        .expect("head handle")
        .expect("handle metadata");
    let mut handle = ControlPlaneHandleRecord::from_json_slice(
        storage
            .get_raw(&handle_path)
            .await
            .expect("read handle")
            .as_ref(),
    )
    .expect("decode handle");
    let index = usize::try_from(ordinal - 1).expect("ordinal index");
    handle.mutation_refs[index]
        .sha256
        .clone_from(&rebound_digest);
    storage
        .put_raw(
            &handle_path,
            Bytes::from(handle.to_json_vec().expect("encode rebound handle")),
            WritePrecondition::MatchesVersion(metadata.version),
        )
        .await
        .expect("rebind handle digest");

    let authority_path = format!("transactions/handles/{handle_id}/identities/{ordinal:020}.json");
    let mut authority: serde_json::Value = serde_json::from_slice(
        storage
            .get_raw(&authority_path)
            .await
            .expect("read identity authority")
            .as_ref(),
    )
    .expect("decode identity authority");
    authority["handle_intent"]["mutation_ref"]["sha256"] = serde_json::json!(rebound_digest);
    storage
        .put_raw(
            &authority_path,
            Bytes::from(
                arco_core::canonical_json::to_canonical_bytes(&authority)
                    .expect("encode rebound authority"),
            ),
            WritePrecondition::None,
        )
        .await
        .expect("rebind identity authority digest");
}

async fn rewrite_handle(
    backend: Arc<dyn StorageBackend>,
    handle_id: &str,
    rewrite: impl FnOnce(&mut ControlPlaneHandleRecord),
) -> ControlPlaneHandleRecord {
    let storage = request_context()
        .scoped_storage(backend)
        .expect("scoped storage");
    let path = ControlPlaneTxPaths::handle_record(handle_id).expect("handle path");
    let metadata = storage
        .head_raw(&path)
        .await
        .expect("head handle")
        .expect("handle metadata");
    let mut record = ControlPlaneHandleRecord::from_json_slice(
        storage.get_raw(&path).await.expect("get handle").as_ref(),
    )
    .expect("decode handle");
    rewrite(&mut record);
    record.validate().expect("valid rewritten handle");
    let result = storage
        .put_raw(
            &path,
            Bytes::from(record.to_json_vec().expect("encode handle")),
            WritePrecondition::MatchesVersion(metadata.version),
        )
        .await
        .expect("rewrite handle");
    assert!(matches!(result, WriteResult::Success { .. }));
    record
}

fn catalog_request_hash(operation: &CatalogDdlOperation) -> String {
    super::CatalogMutation::from_proto(operation)
        .expect("catalog mutation")
        .request_hash()
        .expect("catalog request hash")
}

async fn seed_visible_catalog_claim(
    backend: Arc<dyn StorageBackend>,
    participant: &arco_core::control_plane_transactions::ControlPlaneHandleParticipant,
    marker_request_id: &str,
    record_request_id: &str,
    request_hash: &str,
    receipt_tx_id: Option<&str>,
    now: chrono::DateTime<Utc>,
) -> String {
    let tx_id = ulid::Ulid::new().to_string();
    let commit_id = ulid::Ulid::new().to_string();
    let receipt = CatalogTxReceipt {
        tx_id: receipt_tx_id.unwrap_or(&tx_id).to_string(),
        event_id: ulid::Ulid::new().to_string(),
        commit_id: commit_id.clone(),
        manifest_id: "00000000000000000001".to_string(),
        snapshot_version: 1,
        pointer_version: "seeded-pointer-version".to_string(),
        read_token: "catalog:seeded-manifest".to_string(),
        visible_at: now,
    };
    let marker = ControlPlaneIdempotencyRecord {
        tx_id: tx_id.clone(),
        kind: ControlPlaneTxKind::CatalogDdl,
        request_id: marker_request_id.to_string(),
        idempotency_key: participant.idempotency_key.clone(),
        request_hash: request_hash.to_string(),
        created_at: now,
        visible_at: Some(now),
        tx_record: None,
    };
    let record = ControlPlaneTxRecord::<serde_json::Value> {
        tx_id: tx_id.clone(),
        kind: ControlPlaneTxKind::CatalogDdl,
        status: ControlPlaneTxStatus::Visible,
        repair_pending: false,
        request_id: record_request_id.to_string(),
        idempotency_key: participant.idempotency_key.clone(),
        request_hash: request_hash.to_string(),
        lock_path: "locks/catalog.lock.json".to_string(),
        fencing_token: 1,
        prepared_at: now,
        visible_at: Some(now),
        durable_append: None,
        result: Some(serde_json::to_value(&receipt).expect("receipt value")),
    };
    put_json(
        backend.clone(),
        &ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Catalog,
            &participant.idempotency_key,
        ),
        &marker,
    )
    .await;
    put_json(
        backend.clone(),
        &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &tx_id),
        &record,
    )
    .await;
    put_json(
        backend,
        &CatalogPaths::commit(CatalogDomain::Catalog, &commit_id),
        &serde_json::json!({ "seeded": true }),
    )
    .await;
    tx_id
}

#[tokio::test]
async fn create_returns_review_token_once_and_persists_only_its_verifier() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, ctx) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct handle service");
    let now = instant(1_784_000_000);

    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");

    assert!(created.handle.handle_id.starts_with("hdl_"));
    assert!(created.review_token.expose().starts_with("review_"));
    let token_entropy = created
        .review_token
        .expose()
        .strip_prefix("review_")
        .expect("review prefix");
    assert_eq!(token_entropy.len(), 64);
    for uuid in [&token_entropy[..32], &token_entropy[32..]] {
        let uuid = uuid::Uuid::parse_str(uuid).expect("review UUID");
        assert_eq!(uuid.get_version_num(), 4);
        assert_eq!(uuid.get_variant(), uuid::Variant::RFC4122);
    }
    assert_eq!(created.handle.status, ControlPlaneHandleStatus::Open);
    assert_eq!(
        created.handle.expires_at,
        now + chrono::Duration::minutes(10)
    );
    let debug = format!("{created:?}");
    assert!(!debug.contains(created.review_token.expose()));
    assert!(debug.contains("<redacted>"));

    let storage = ctx.scoped_storage(backend).expect("scoped storage");
    let path = ControlPlaneTxPaths::handle_record(&created.handle.handle_id)
        .expect("canonical handle path");
    let bytes = storage.get_raw(&path).await.expect("stored handle");
    assert!(!String::from_utf8_lossy(&bytes).contains(created.review_token.expose()));
    assert!(String::from_utf8_lossy(&bytes).contains("sha256:"));

    let fetched = handles
        .get_handle(&created.handle.handle_id)
        .await
        .expect("get direct-addressed handle");
    assert_eq!(fetched, created.handle);
}

#[tokio::test]
async fn create_adopts_a_byte_identical_handle_after_an_ambiguous_write_without_listing() {
    let backend = Arc::new(NoListFaultBackend::new());
    backend.fail_after_next_matching_put("/handle.json", 0);
    let (state, ctx) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct handle service");
    let now = instant(1_784_000_050);

    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("adopt the exact persisted handle after an ambiguous create write");

    let path = ControlPlaneTxPaths::handle_record(&created.handle.handle_id)
        .expect("canonical handle path");
    let storage = ctx.scoped_storage(backend.clone()).expect("scoped storage");
    let bytes = storage.get_raw(&path).await.expect("stored handle");
    let persisted =
        ControlPlaneHandleRecord::from_json_slice(bytes.as_ref()).expect("persisted handle");
    assert_eq!(persisted, created.handle);

    let staged = handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("ambiguous_create"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("returned one-time token still authenticates the adopted handle");
    assert_eq!(staged.mutation_refs.len(), 1);
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn typed_staging_is_immutable_retryable_and_prepare_freezes_participants() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, ctx) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct handle service");
    let now = instant(1_784_000_100);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");

    let staged = handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("analytics"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage typed catalog mutation");
    assert_eq!(staged.mutation_refs.len(), 1);
    assert_eq!(staged.mutation_refs[0].ordinal, 1);

    let retry = handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("analytics"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("byte-identical retry");
    assert_eq!(retry, staged);

    let conflict = handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("different"),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect_err("same ordinal with different bytes must conflict");
    assert!(conflict.message().contains("conflict"));

    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("prepare handle");
    assert_eq!(prepared.status, ControlPlaneHandleStatus::Prepared);
    assert_eq!(prepared.participants.len(), 1);
    assert_eq!(prepared.participants[0].ordinal, 1);
    assert!(
        prepared.participants[0]
            .idempotency_key
            .contains(&created.handle.handle_id)
    );

    let storage = ctx.scoped_storage(backend).expect("scoped storage");
    let mutation_path = ControlPlaneTxPaths::handle_mutation(&created.handle.handle_id, 1)
        .expect("canonical mutation path");
    let bytes = storage.get_raw(&mutation_path).await.expect("staged bytes");
    let wire = String::from_utf8_lossy(&bytes);
    assert!(wire.contains("catalog"));
    assert!(!wire.contains(created.review_token.expose()));
}

#[tokio::test]
async fn staging_rejects_credentials_opaque_root_mutations_and_secret_bearing_locations() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend);
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_200);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");

    let credential_error = handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![credential_mutation()],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("credential mutation must be rejected");
    assert!(credential_error.message().contains("unsupported"));

    let uri_error = handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            register_table("s3://access:secret@bucket/table?token=plaintext"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("secret-bearing location must be rejected");
    assert!(uri_error.message().contains("credential"));

    for location in [
        "https://bucket.example/table?AWSAccessKeyId=plaintext",
        "https://bucket.example/table?api%5fkey=plaintext",
        "https://bucket.example/table?benign=plaintext",
        "https://bucket.example/table#token=plaintext",
        "https://user%3Asecret@bucket.example/table",
        "http:a:b@www.example.com",
        "s3:access:secret@bucket/table",
        "mailto:user:secret@example.com",
        "//user:secret@bucket.example/table",
        r"https:\\user:secret@bucket.example/table",
    ] {
        let error = handles
            .stage_catalog(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                register_table(location),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("query, fragment, and userinfo locations must fail closed");
        assert!(error.message().contains("credential"));
    }

    for batch in [
        task_finished_batch(
            "https://user:secret@events.example/source?token=plaintext",
            None,
        ),
        task_finished_batch(
            &format!("arco-flow/{TENANT}/{WORKSPACE}"),
            Some("s3://access:secret@bucket/output?token=plaintext"),
        ),
    ] {
        let error = handles
            .stage_orchestration(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                batch,
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("secret-bearing orchestration locations must fail before staging");
        assert!(error.message().contains("credential"));
    }

    let invalid_contract = handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog(""),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("invalid catalog DDL must fail before immutable staging");
    assert!(invalid_contract.message().contains("catalog"));

    let mut reflected = create_catalog("review_token_reflection");
    let Some(catalog_ddl_operation::Op::CreateCatalog(operation)) = reflected.op.as_mut() else {
        panic!("create catalog fixture");
    };
    operation.description = Some(created.review_token.expose().to_string());
    let reflection_error = handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            reflected,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("plaintext review token must never enter immutable staged bytes");
    assert!(
        !reflection_error
            .message()
            .contains(created.review_token.expose())
    );

    assert!(
        handles
            .get_handle(&created.handle.handle_id)
            .await
            .expect("get unchanged handle")
            .mutation_refs
            .is_empty()
    );
}

#[tokio::test]
async fn any_existing_low_level_claim_blocks_abort_and_expiry_terminalization() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_250);

    for (name, expire) in [("claimed_abort", false), ("claimed_expiry", true)] {
        let created = handles
            .create_handle(Duration::from_secs(5), now)
            .await
            .expect("create claimed handle");
        let operation = create_catalog(name);
        handles
            .stage_catalog(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                operation.clone(),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect("stage claimed handle");
        let prepared = handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare claimed handle");
        let participant = &prepared.participants[0];
        seed_visible_catalog_claim(
            backend.clone(),
            participant,
            &participant.request_id,
            &participant.request_id,
            &catalog_request_hash(&operation),
            None,
            now + chrono::Duration::seconds(3),
        )
        .await;

        let terminal = if expire {
            handles
                .expire_handle(
                    &prepared.handle_id,
                    created.review_token.expose(),
                    now + chrono::Duration::seconds(5),
                )
                .await
        } else {
            handles
                .abort_handle(
                    &prepared.handle_id,
                    created.review_token.expose(),
                    now + chrono::Duration::seconds(4),
                )
                .await
        };
        terminal.expect_err("a low-level claim makes terminal pre-visibility proof unsafe");
        assert_eq!(
            handles
                .get_handle(&prepared.handle_id)
                .await
                .expect("repair-required claimed handle")
                .status,
            ControlPlaneHandleStatus::RepairRequired
        );
    }
}

#[tokio::test]
async fn durable_handle_ownership_fences_claim_creation_racing_abort() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct fenced handle service");
    let now = instant(1_784_000_275);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create fenced handle");
    let operation = create_catalog("abort_claim_fence");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            operation.clone(),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage fenced mutation");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare fenced handle");
    let participant = prepared.participants[0].clone();
    let handle_path =
        ControlPlaneTxPaths::handle_record(&prepared.handle_id).expect("fenced handle path");
    let (entered, release) = backend.gate_next_matching_put(handle_path);

    let abort_handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct abort service");
    let abort = abort_handles.abort_handle(
        &prepared.handle_id,
        created.review_token.expose(),
        now + chrono::Duration::seconds(3),
    );
    let race_claim = async {
        entered.notified().await;
        let mut claim_context = ctx.clone();
        claim_context.request_id.clone_from(&participant.request_id);
        claim_context.idempotency_key = Some(participant.idempotency_key.clone());
        let result = ControlPlaneTransactionService::new(&state, claim_context)
            .expect("construct racing legacy service")
            .apply_catalog_ddl(ApplyCatalogDdlRequest {
                ddl: Some(operation),
            })
            .await;
        release.notify_one();
        result
    };
    let (terminal, claim) = tokio::join!(abort, race_claim);
    assert_eq!(
        terminal.expect("abort wins the handle CAS").status,
        ControlPlaneHandleStatus::Aborted
    );
    claim.expect_err("a legacy caller cannot claim a real durable-handle identity");
    let scoped = ctx.scoped_storage(erased).expect("scoped storage");
    assert!(
        scoped
            .head_raw(&ControlPlaneTxPaths::idempotency(
                participant.domain,
                &participant.idempotency_key,
            ))
            .await
            .expect("head fenced marker")
            .is_none()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn ttl_abort_and_terminal_transitions_fail_closed() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend);
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_300);
    let expired = handles
        .create_handle(Duration::from_secs(5), now)
        .await
        .expect("create expiring handle");

    let record = handles
        .expire_handle(
            &expired.handle.handle_id,
            expired.review_token.expose(),
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect("expire at boundary");
    assert_eq!(record.status, ControlPlaneHandleStatus::Expired);
    let stage_error = handles
        .stage_catalog(
            &record.handle_id,
            expired.review_token.expose(),
            1,
            create_catalog("too_late"),
            now + chrono::Duration::seconds(6),
        )
        .await
        .expect_err("expired handle cannot be revived");
    assert!(stage_error.message().contains("terminal"));

    let abortable = handles
        .create_handle(Duration::from_secs(60), now)
        .await
        .expect("create abortable handle");
    let aborted = handles
        .abort_handle(
            &abortable.handle.handle_id,
            abortable.review_token.expose(),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("abort open handle");
    assert_eq!(aborted.status, ControlPlaneHandleStatus::Aborted);
    assert_eq!(
        handles
            .abort_handle(
                &aborted.handle_id,
                abortable.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("idempotent abort")
            .status,
        ControlPlaneHandleStatus::Aborted
    );
    assert_eq!(
        handles
            .abort_handle(
                &aborted.handle_id,
                abortable.review_token.expose(),
                now + chrono::Duration::seconds(60),
            )
            .await
            .expect("idempotent abort remains terminal after TTL evaluation")
            .status,
        ControlPlaneHandleStatus::Aborted
    );

    let late_abort = handles
        .create_handle(Duration::from_secs(5), now)
        .await
        .expect("create late-abort handle");
    handles
        .stage_catalog(
            &late_abort.handle.handle_id,
            late_abort.review_token.expose(),
            1,
            create_catalog("expires_instead_of_aborts"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage late-abort handle");
    handles
        .prepare_handle(
            &late_abort.handle.handle_id,
            late_abort.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare late-abort handle");
    let expired_instead = handles
        .abort_handle(
            &late_abort.handle.handle_id,
            late_abort.review_token.expose(),
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect("abort at TTL must normalize to expiry");
    assert_eq!(expired_instead.status, ControlPlaneHandleStatus::Expired);

    let prepared_retry = handles
        .create_handle(Duration::from_secs(5), now)
        .await
        .expect("create prepared-retry handle");
    handles
        .stage_catalog(
            &prepared_retry.handle.handle_id,
            prepared_retry.review_token.expose(),
            1,
            create_catalog("prepared_retry_expires"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage prepared-retry handle");
    handles
        .prepare_handle(
            &prepared_retry.handle.handle_id,
            prepared_retry.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare prepared-retry handle");
    handles
        .prepare_handle(
            &prepared_retry.handle.handle_id,
            prepared_retry.review_token.expose(),
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect_err("prepared retry at TTL must expire instead of returning PREPARED");
    assert_eq!(
        handles
            .get_handle(&prepared_retry.handle.handle_id)
            .await
            .expect("expired prepared retry")
            .status,
        ControlPlaneHandleStatus::Expired
    );
}

#[tokio::test]
async fn legacy_different_request_cannot_preclaim_real_handle_identity() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend);
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_350);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");
    let reviewed = create_catalog("reviewed_catalog");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            reviewed,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage reviewed mutation");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare reviewed mutation");
    let participant = &prepared.participants[0];

    let mut direct_ctx = request_context();
    direct_ctx.request_id.clone_from(&participant.request_id);
    direct_ctx.idempotency_key = Some(participant.idempotency_key.clone());
    ControlPlaneTransactionService::new(&state, direct_ctx)
        .expect("construct direct transaction service")
        .apply_catalog_ddl(ApplyCatalogDdlRequest {
            ddl: Some(create_catalog("different_preclaim")),
        })
        .await
        .expect_err("real handle namespace rejects a different legacy request");

    let visible = handles
        .commit_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("reviewed handle remains able to claim its frozen identity");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
}

#[tokio::test]
async fn mismatched_marker_record_and_receipt_identity_fail_closed() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_355);

    for (case, marker_wrong, record_wrong, receipt_wrong) in [
        ("marker", true, false, false),
        ("record", false, true, false),
        ("receipt", false, false, true),
    ] {
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create identity-corruption handle");
        let operation = create_catalog(&format!("identity_{case}"));
        handles
            .stage_catalog(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                operation.clone(),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect("stage identity-corruption handle");
        let prepared = handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare identity-corruption handle");
        force_committing(
            backend.clone(),
            &prepared.handle_id,
            now + chrono::Duration::seconds(3),
        )
        .await;
        let participant = &prepared.participants[0];
        let wrong = format!("wrong-{case}");
        seed_visible_catalog_claim(
            backend.clone(),
            participant,
            if marker_wrong {
                &wrong
            } else {
                &participant.request_id
            },
            if record_wrong {
                &wrong
            } else {
                &participant.request_id
            },
            &catalog_request_hash(&operation),
            receipt_wrong.then_some("00000000000000000000000000"),
            now + chrono::Duration::seconds(3),
        )
        .await;

        handles
            .recover_handle(
                &prepared.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(4),
            )
            .await
            .expect_err("mismatched low-level ownership must not be adopted");
        assert_eq!(
            handles
                .get_handle(&prepared.handle_id)
                .await
                .expect("repair-required identity handle")
                .status,
            ControlPlaneHandleStatus::RepairRequired
        );
    }
}

#[tokio::test]
async fn cached_visible_and_exact_record_results_must_match_before_adoption() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_356);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create cached-record mismatch handle");
    let operation = create_catalog("cached_record_mismatch");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            operation.clone(),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage cached-record mismatch handle");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare cached-record mismatch handle");
    force_committing(
        backend.clone(),
        &prepared.handle_id,
        now + chrono::Duration::seconds(3),
    )
    .await;

    let participant = &prepared.participants[0];
    let tx_id = ulid::Ulid::new().to_string();
    let exact_receipt = CatalogTxReceipt {
        tx_id: tx_id.clone(),
        event_id: ulid::Ulid::new().to_string(),
        commit_id: ulid::Ulid::new().to_string(),
        manifest_id: "00000000000000000001".to_string(),
        snapshot_version: 1,
        pointer_version: "exact-pointer".to_string(),
        read_token: "catalog:exact".to_string(),
        visible_at: now,
    };
    let exact_record = ControlPlaneTxRecord::<serde_json::Value> {
        tx_id: tx_id.clone(),
        kind: ControlPlaneTxKind::CatalogDdl,
        status: ControlPlaneTxStatus::Visible,
        repair_pending: false,
        request_id: participant.request_id.clone(),
        idempotency_key: participant.idempotency_key.clone(),
        request_hash: participant.request_hash.clone(),
        lock_path: "locks/catalog.lock.json".to_string(),
        fencing_token: 1,
        prepared_at: now,
        visible_at: Some(now),
        durable_append: None,
        result: Some(serde_json::to_value(&exact_receipt).expect("exact receipt value")),
    };
    let mut cached_record = exact_record.clone();
    let mut cached_receipt = exact_receipt;
    cached_receipt.commit_id = ulid::Ulid::new().to_string();
    cached_record.result =
        Some(serde_json::to_value(cached_receipt).expect("cached receipt value"));
    let marker = ControlPlaneIdempotencyRecord {
        tx_id: tx_id.clone(),
        kind: ControlPlaneTxKind::CatalogDdl,
        request_id: participant.request_id.clone(),
        idempotency_key: participant.idempotency_key.clone(),
        request_hash: participant.request_hash.clone(),
        created_at: now,
        visible_at: Some(now),
        tx_record: Some(serde_json::to_value(cached_record).expect("cached record value")),
    };
    put_json(
        backend.clone(),
        &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &tx_id),
        &exact_record,
    )
    .await;
    put_json(
        backend,
        &ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Catalog,
            &participant.idempotency_key,
        ),
        &marker,
    )
    .await;

    handles
        .recover_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("divergent cached and exact results must fail closed");
    assert_eq!(
        handles
            .get_handle(&prepared.handle_id)
            .await
            .expect("repair-required divergent handle")
            .status,
        ControlPlaneHandleStatus::RepairRequired
    );
}

#[tokio::test]
async fn cached_visible_marker_is_materialized_at_the_exact_record_before_adoption() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, ctx) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct handle service");
    let now = instant(1_784_000_357);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create cached-only handle");
    let operation = create_catalog("cached_only_visible");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            operation,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage cached-only handle");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare cached-only handle");
    let seeded = handles
        .commit_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("seed complete cached-only catalog authority");
    let participant = &seeded.participants[0];
    let tx_id = participant
        .tx_id
        .clone()
        .expect("seeded cached-only transaction ID");
    let marker_path = ControlPlaneTxPaths::idempotency(
        ControlPlaneTxDomain::Catalog,
        &participant.idempotency_key,
    );
    let storage = ctx.scoped_storage(backend.clone()).expect("scoped storage");
    let marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        storage
            .get_raw(&marker_path)
            .await
            .expect("read seeded cached-only marker")
            .as_ref(),
    )
    .expect("decode seeded cached-only marker");
    let cached: ControlPlaneTxRecord<serde_json::Value> =
        serde_json::from_value(marker.tx_record.expect("seeded marker cache"))
            .expect("decode seeded cached-only record");
    storage
        .delete(&ControlPlaneTxPaths::record(
            ControlPlaneTxDomain::Catalog,
            &tx_id,
        ))
        .await
        .expect("remove exact record while retaining complete cached authority");
    rewrite_handle(backend.clone(), &seeded.handle_id, |record| {
        record.status = ControlPlaneHandleStatus::Committing;
        record.revision += 1;
        record.updated_at = now + chrono::Duration::seconds(4);
        record.visible_at = None;
        record.participants[0].receipt_path = None;
    })
    .await;

    let visible = handles
        .recover_handle(
            &seeded.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect("materialize exact transaction record before adoption");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    let exact: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        ctx.scoped_storage(backend)
            .expect("scoped storage")
            .get_raw(&ControlPlaneTxPaths::record(
                ControlPlaneTxDomain::Catalog,
                &tx_id,
            ))
            .await
            .expect("materialized exact record")
            .as_ref(),
    )
    .expect("decode materialized exact record");
    assert_eq!(exact, cached);
}

#[tokio::test]
async fn corrupt_catalog_audit_artifact_never_becomes_handle_evidence() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_358);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create corrupt-audit handle");
    let operation = create_catalog("corrupt_audit");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            operation.clone(),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage corrupt-audit handle");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare corrupt-audit handle");
    force_committing(
        backend.clone(),
        &prepared.handle_id,
        now + chrono::Duration::seconds(3),
    )
    .await;
    let participant = &prepared.participants[0];
    seed_visible_catalog_claim(
        backend,
        participant,
        &participant.request_id,
        &participant.request_id,
        &catalog_request_hash(&operation),
        None,
        now + chrono::Duration::seconds(3),
    )
    .await;

    handles
        .recover_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("unverifiable catalog audit bytes must fail closed");
    assert_eq!(
        handles
            .get_handle(&prepared.handle_id)
            .await
            .expect("repair-required corrupt-audit handle")
            .status,
        ControlPlaneHandleStatus::RepairRequired
    );
}

#[tokio::test]
async fn marker_without_a_transaction_record_requires_repair_without_ambiguous_identity() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_357);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create missing-record handle");
    let operation = create_catalog("marker_without_record");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            operation.clone(),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage missing-record handle");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare missing-record handle");
    force_committing(
        backend.clone(),
        &prepared.handle_id,
        now + chrono::Duration::seconds(3),
    )
    .await;
    let participant = &prepared.participants[0];
    put_json(
        backend,
        &ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Catalog,
            &participant.idempotency_key,
        ),
        &ControlPlaneIdempotencyRecord {
            tx_id: ulid::Ulid::new().to_string(),
            kind: participant.kind,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: catalog_request_hash(&operation),
            created_at: now,
            visible_at: None,
            tx_record: None,
        },
    )
    .await;

    handles
        .recover_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("marker without exact transaction record requires repair");
    let repair = handles
        .get_handle(&prepared.handle_id)
        .await
        .expect("repair missing-record handle");
    assert_eq!(repair.status, ControlPlaneHandleStatus::RepairRequired);
    assert!(repair.participants[0].tx_id.is_none());
    assert!(repair.participants[0].low_level_status.is_none());
}

#[tokio::test]
async fn forged_cached_visible_evidence_is_reinspected_before_final_visibility() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_360);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("forged_cached_visible"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage catalog");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare handle");
    rewrite_handle(backend.clone(), &created.handle.handle_id, |record| {
        record.status = ControlPlaneHandleStatus::Committing;
        record.revision += 1;
        record.updated_at = now + chrono::Duration::seconds(3);
        record.committing_at = Some(record.updated_at);
        record.participants[0].tx_id = Some("00000000000000000000000000".to_string());
        record.participants[0].low_level_status = Some(ControlPlaneTxStatus::Visible);
    })
    .await;

    handles
        .recover_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("handle-local VISIBLE evidence requires exact low-level proof");
    assert_ne!(
        handles
            .get_handle(&created.handle.handle_id)
            .await
            .expect("get forged handle")
            .status,
        ControlPlaneHandleStatus::Visible
    );

    let terminal = rewrite_handle(backend, &created.handle.handle_id, |record| {
        record.status = ControlPlaneHandleStatus::Visible;
        record.revision += 1;
        record.updated_at = now + chrono::Duration::seconds(5);
        record.visible_at = Some(record.updated_at);
        record.failure_category = None;
    })
    .await;
    handles
        .commit_handle(
            &terminal.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(6),
        )
        .await
        .expect_err("terminal VISIBLE commit replay must recheck exact low-level proof");
    handles
        .recover_handle(
            &terminal.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(7),
        )
        .await
        .expect_err("terminal VISIBLE recovery replay must recheck exact low-level proof");
    assert_eq!(
        handles
            .get_handle(&terminal.handle_id)
            .await
            .expect("terminal forged handle remains immutable"),
        terminal
    );
}

#[tokio::test]
async fn revision_capacity_is_reserved_before_low_level_execution_and_repair_retries() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_370);

    let exhausted = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create exhausted handle");
    handles
        .stage_catalog(
            &exhausted.handle.handle_id,
            exhausted.review_token.expose(),
            1,
            create_catalog("must_not_execute"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage exhausted handle");
    let prepared = handles
        .prepare_handle(
            &exhausted.handle.handle_id,
            exhausted.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare exhausted handle");
    let prepared = rewrite_handle(erased.clone(), &prepared.handle_id, |record| {
        record.revision = u64::MAX - 3;
    })
    .await;
    handles
        .commit_handle(
            &prepared.handle_id,
            exhausted.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("insufficient recovery revisions must fail before execution");
    let idempotency_path = ControlPlaneTxPaths::idempotency(
        ControlPlaneTxDomain::Catalog,
        &prepared.participants[0].idempotency_key,
    );
    assert!(
        request_context()
            .scoped_storage(erased.clone())
            .expect("scoped storage")
            .head_raw(&idempotency_path)
            .await
            .expect("head idempotency marker")
            .is_none(),
        "revision exhaustion must not allow a low-level claim"
    );

    let boundary = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create boundary handle");
    let boundary_token = boundary.review_token.expose().to_string();
    handles
        .stage_catalog(
            &boundary.handle.handle_id,
            &boundary_token,
            1,
            create_catalog("boundary_recovery"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage boundary handle");
    let boundary = handles
        .prepare_handle(
            &boundary.handle.handle_id,
            &boundary_token,
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare boundary handle");
    let boundary = rewrite_handle(erased.clone(), &boundary.handle_id, |record| {
        record.revision = u64::MAX - 4;
    })
    .await;
    let marker_path = ControlPlaneTxPaths::idempotency(
        ControlPlaneTxDomain::Catalog,
        &boundary.participants[0].idempotency_key,
    );
    backend.fail_next_matching_put(marker_path.clone(), 0);
    handles
        .commit_handle(
            &boundary.handle_id,
            &boundary_token,
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("first failure enters repair");
    let repair = handles
        .get_handle(&boundary.handle_id)
        .await
        .expect("repair handle");
    assert_eq!(repair.revision, u64::MAX - 2);

    backend.fail_next_matching_put(marker_path, 0);
    handles
        .recover_handle(
            &boundary.handle_id,
            &boundary_token,
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("repeated failure remains repair-required");
    assert_eq!(
        handles
            .get_handle(&boundary.handle_id)
            .await
            .expect("unchanged repair handle")
            .revision,
        repair.revision,
        "equivalent repair failures must not burn reserved revisions"
    );

    backend.clear_failure();
    let visible = handles
        .recover_handle(
            &boundary.handle_id,
            &boundary_token,
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect("reserved revisions permit recovery");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(visible.revision, u64::MAX);
}

#[tokio::test]
async fn commit_reuses_existing_transaction_executor_and_is_idempotent() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend);
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_400);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("visible_catalog"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage catalog");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare handle");

    let visible = handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("commit through existing executor");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(visible.visible_participant_count(), 1);
    assert!(visible.participants[0].tx_id.is_some());

    let replay = handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect("visible replay");
    assert_eq!(replay, visible);
}

#[tokio::test]
async fn committing_before_low_level_claim_recovers_after_service_reconstruction_without_listing() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_500);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("recover_before_claim"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage catalog");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare handle");
    drop(handles);
    force_committing(
        erased.clone(),
        &created.handle.handle_id,
        now + chrono::Duration::seconds(3),
    )
    .await;

    let reconstructed = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("reconstruct service");
    let visible = reconstructed
        .recover_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect("recover committing handle");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(visible.visible_participant_count(), 1);
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn partial_multi_mutation_visibility_requires_recovery_and_never_replays_visible_participant()
{
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_600);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("partial_first"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage first catalog");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            2,
            create_catalog("partial_second"),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("stage second catalog");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("prepare handle");
    let second_idempotency_path = ControlPlaneTxPaths::idempotency(
        ControlPlaneTxDomain::Catalog,
        &prepared.participants[1].idempotency_key,
    );
    backend.fail_next_matching_put(second_idempotency_path, 0);

    let error = handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("second participant failure must require recovery");
    assert!(error.message().contains("recovery"));
    let repair = handles
        .get_handle(&created.handle.handle_id)
        .await
        .expect("get repair handle");
    assert_eq!(repair.status, ControlPlaneHandleStatus::RepairRequired);
    assert_eq!(repair.visible_participant_count(), 1);
    assert!(
        handles
            .abort_handle(
                &repair.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(5),
            )
            .await
            .is_err()
    );
    let first_tx = repair.participants[0].tx_id.clone();

    backend.clear_failure();
    let visible = handles
        .recover_handle(
            &repair.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(6),
        )
        .await
        .expect("recover second participant");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(visible.visible_participant_count(), 2);
    assert_eq!(visible.participants[0].tx_id, first_tx);
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn visible_low_level_before_handle_receipt_cas_is_adopted_by_recovery() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased);
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_700);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("visible_before_handle_cas"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage catalog");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare handle");
    let handle_path =
        ControlPlaneTxPaths::handle_record(&created.handle.handle_id).expect("handle path");
    backend.fail_next_matching_put(handle_path, 1);

    handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("handle evidence CAS response must be lost");
    let uncertain = handles
        .get_handle(&created.handle.handle_id)
        .await
        .expect("get uncertain handle");
    assert_eq!(uncertain.status, ControlPlaneHandleStatus::Committing);
    assert!(
        handles
            .abort_handle(
                &uncertain.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(4),
            )
            .await
            .is_err()
    );

    let recovered = handles
        .recover_handle(
            &uncertain.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect("adopt visible low-level evidence");
    assert_eq!(recovered.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(recovered.visible_participant_count(), 1);
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn root_aborted_never_proves_abortable_when_its_catalog_child_is_visible() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_800);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![catalog_domain_mutation("root_child_visible")],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage root");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare root handle");
    // The first root-namespace put is the prepared root record; fail the next
    // one, which is the tx-scoped root super-manifest after the catalog child
    // is already visible.
    backend.fail_next_matching_put("transactions/root/", 1);

    handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("root super-manifest failure must require recovery");
    let repair = handles
        .get_handle(&created.handle.handle_id)
        .await
        .expect("get repair handle");
    assert_eq!(repair.status, ControlPlaneHandleStatus::RepairRequired);
    assert_ne!(
        repair.participants[0].low_level_status,
        Some(ControlPlaneTxStatus::Aborted),
        "root ABORTED cannot hide an already-visible catalog child"
    );
    let child_key = format!("root:{}:catalog", prepared.participants[0].idempotency_key);
    let child_path = ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Catalog, &child_key);
    let scoped = request_context()
        .scoped_storage(erased)
        .expect("scoped storage");
    let child_before: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        scoped
            .get_raw(&child_path)
            .await
            .expect("visible root child marker")
            .as_ref(),
    )
    .expect("decode root child marker");
    let root_path = ControlPlaneTxPaths::idempotency(
        ControlPlaneTxDomain::Root,
        &prepared.participants[0].idempotency_key,
    );
    let root_before: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        scoped
            .get_raw(&root_path)
            .await
            .expect("aborted root marker")
            .as_ref(),
    )
    .expect("decode aborted root marker");

    let visible = handles
        .recover_handle(
            &repair.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect("recover root using existing child receipt");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(
        visible.participants[0].tx_id.as_deref(),
        Some(root_before.tx_id.as_str())
    );
    let child_after: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        scoped
            .get_raw(&child_path)
            .await
            .expect("recovered root child marker")
            .as_ref(),
    )
    .expect("decode recovered child marker");
    assert_eq!(child_after.tx_id, child_before.tx_id);
    let root_after: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        scoped
            .get_raw(&root_path)
            .await
            .expect("recovered root marker")
            .as_ref(),
    )
    .expect("decode recovered root marker");
    assert_eq!(root_after.tx_id, root_before.tx_id);
    let root_record: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        scoped
            .get_raw(&ControlPlaneTxPaths::record(
                ControlPlaneTxDomain::Root,
                &root_before.tx_id,
            ))
            .await
            .expect("recovered root record")
            .as_ref(),
    )
    .expect("decode recovered root record");
    assert_eq!(root_record.status, ControlPlaneTxStatus::Visible);
    assert_eq!(root_record.tx_id, root_before.tx_id);
    assert_eq!(
        root_record.result.as_ref().expect("root receipt")["txId"],
        root_before.tx_id
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn legacy_callers_cannot_claim_real_handle_owned_root_child_identities() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct root-child ownership service");
    let now = instant(1_784_000_825);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create root-child ownership handle");
    let operation = create_catalog("root_child_reserved_identity");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![DomainMutation {
                kind: Some(domain_mutation::Kind::Catalog(operation.clone())),
            }],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage root-child ownership mutation");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare root-child ownership handle");
    let root = &prepared.participants[0];
    let child_key = format!("root:{}:catalog", root.idempotency_key);
    let mut child_context = ctx.clone();
    child_context.request_id.clone_from(&root.request_id);
    child_context.idempotency_key = Some(child_key.clone());
    ControlPlaneTransactionService::new(&state, child_context)
        .expect("construct legacy root-child service")
        .apply_catalog_ddl(ApplyCatalogDdlRequest {
            ddl: Some(operation),
        })
        .await
        .expect_err("a legacy caller cannot claim a real handle-owned root child");
    let scoped = ctx.scoped_storage(erased).expect("scoped storage");
    assert!(
        scoped
            .head_raw(&ControlPlaneTxPaths::idempotency(
                ControlPlaneTxDomain::Catalog,
                &child_key,
            ))
            .await
            .expect("head root-child marker")
            .is_none()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn aborted_handle_owned_root_child_is_never_replaced_during_recovery() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct aborted root-child service");
    let now = instant(1_784_000_850);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create aborted root-child handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![catalog_domain_mutation("aborted_root_child")],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage aborted root-child mutation");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare aborted root-child handle");
    backend.fail_next_matching_put("transactions/root/", 1);
    handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("root failure after child visibility must require repair");

    let root = &prepared.participants[0];
    let child_key = format!("root:{}:catalog", root.idempotency_key);
    let child_marker_path =
        ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Catalog, &child_key);
    let scoped = ctx.scoped_storage(erased).expect("scoped storage");
    let mut marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        scoped
            .get_raw(&child_marker_path)
            .await
            .expect("read root-child marker")
            .as_ref(),
    )
    .expect("decode root-child marker");
    let original_child_tx_id = marker.tx_id.clone();
    marker.visible_at = None;
    marker.tx_record = None;
    scoped
        .put_raw(
            &child_marker_path,
            Bytes::from(serde_json::to_vec(&marker).expect("encode aborted child marker")),
            WritePrecondition::None,
        )
        .await
        .expect("rewrite aborted child marker");
    let child_record_path =
        ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &original_child_tx_id);
    let mut child_record: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        scoped
            .get_raw(&child_record_path)
            .await
            .expect("read root-child record")
            .as_ref(),
    )
    .expect("decode root-child record");
    child_record.status = ControlPlaneTxStatus::Aborted;
    child_record.visible_at = None;
    child_record.result = None;
    child_record.durable_append = None;
    scoped
        .put_raw(
            &child_record_path,
            Bytes::from(serde_json::to_vec(&child_record).expect("encode aborted child record")),
            WritePrecondition::None,
        )
        .await
        .expect("rewrite aborted child record");

    handles
        .recover_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("an aborted non-root child must remain repair-required");
    let marker_after: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        scoped
            .get_raw(&child_marker_path)
            .await
            .expect("read retained root-child marker")
            .as_ref(),
    )
    .expect("decode retained root-child marker");
    assert_eq!(marker_after.tx_id, original_child_tx_id);
    let record_after: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        scoped
            .get_raw(&child_record_path)
            .await
            .expect("read retained root-child record")
            .as_ref(),
    )
    .expect("decode retained root-child record");
    assert_eq!(record_after.status, ControlPlaneTxStatus::Aborted);
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn aborted_handle_owned_orchestration_root_child_is_never_replaced() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct aborted orchestration-child service");
    let now = instant(1_784_000_860);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create aborted orchestration-child handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![DomainMutation {
                kind: Some(domain_mutation::Kind::Orchestration(orchestration_batch(
                    "run-aborted-root-child",
                    "01K00000000000000000000002",
                ))),
            }],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage aborted orchestration-child mutation");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare aborted orchestration-child handle");
    backend.fail_next_matching_put("transactions/root/", 1);
    handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("root failure after orchestration child must require repair");

    let root = &prepared.participants[0];
    let child_key = format!("root:{}:orchestration", root.idempotency_key);
    let child_marker_path =
        ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Orchestration, &child_key);
    let scoped = ctx.scoped_storage(erased).expect("scoped storage");
    let mut marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        scoped
            .get_raw(&child_marker_path)
            .await
            .expect("read orchestration child marker")
            .as_ref(),
    )
    .expect("decode orchestration child marker");
    let original_child_tx_id = marker.tx_id.clone();
    marker.visible_at = None;
    marker.tx_record = None;
    scoped
        .put_raw(
            &child_marker_path,
            Bytes::from(serde_json::to_vec(&marker).expect("encode aborted child marker")),
            WritePrecondition::None,
        )
        .await
        .expect("rewrite orchestration child marker");
    let child_record_path =
        ControlPlaneTxPaths::record(ControlPlaneTxDomain::Orchestration, &original_child_tx_id);
    let mut record: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        scoped
            .get_raw(&child_record_path)
            .await
            .expect("read orchestration child record")
            .as_ref(),
    )
    .expect("decode orchestration child record");
    record.status = ControlPlaneTxStatus::Aborted;
    record.visible_at = None;
    record.result = None;
    record.durable_append = None;
    scoped
        .put_raw(
            &child_record_path,
            Bytes::from(serde_json::to_vec(&record).expect("encode aborted child record")),
            WritePrecondition::None,
        )
        .await
        .expect("rewrite orchestration child record");

    handles
        .recover_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("an aborted orchestration child must remain repair-required");
    let marker_after: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        scoped
            .get_raw(&child_marker_path)
            .await
            .expect("read retained orchestration child marker")
            .as_ref(),
    )
    .expect("decode retained orchestration child marker");
    assert_eq!(marker_after.tx_id, original_child_tx_id);
    let record_after: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        scoped
            .get_raw(&child_record_path)
            .await
            .expect("read retained orchestration child record")
            .as_ref(),
    )
    .expect("decode retained orchestration child record");
    assert_eq!(record_after.status, ControlPlaneTxStatus::Aborted);
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn visible_root_handle_requires_exact_super_manifest_and_child_authority() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct exact root-authority service");
    let now = instant(1_784_000_875);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create exact root-authority handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![
                catalog_domain_mutation("exact_root_authority"),
                DomainMutation {
                    kind: Some(domain_mutation::Kind::Orchestration(orchestration_batch(
                        "run-exact-root-authority",
                        "01K00000000000000000000001",
                    ))),
                },
            ],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage exact root-authority mutation");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare exact root-authority handle");
    let visible = handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("commit exact root-authority handle");
    let root_tx_id = visible.participants[0]
        .tx_id
        .as_deref()
        .expect("visible root transaction ID");
    let super_manifest_path = ControlPlaneTxPaths::root_super_manifest(root_tx_id);
    let child_key = format!("root:{}:catalog", prepared.participants[0].idempotency_key);
    let child_marker_path =
        ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Catalog, &child_key);
    let scoped = ctx.scoped_storage(erased).expect("scoped storage");
    let root_record: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        scoped
            .get_raw(&ControlPlaneTxPaths::record(
                ControlPlaneTxDomain::Root,
                root_tx_id,
            ))
            .await
            .expect("read frozen root transaction")
            .as_ref(),
    )
    .expect("decode frozen root transaction");
    assert!(
        root_record.repair_pending,
        "frozen root visibility must retain explicit deferred-audit repair authority"
    );
    let super_manifest_bytes = scoped
        .get_raw(&super_manifest_path)
        .await
        .expect("read root super-manifest");
    let child_marker_bytes = scoped
        .get_raw(&child_marker_path)
        .await
        .expect("read root child marker");

    scoped
        .delete(&super_manifest_path)
        .await
        .expect("delete root super-manifest");
    handles
        .commit_handle(
            &visible.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("missing root super-manifest must invalidate terminal visibility");
    scoped
        .put_raw(
            &super_manifest_path,
            super_manifest_bytes.clone(),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("restore root super-manifest");

    scoped
        .delete(&child_marker_path)
        .await
        .expect("delete root child marker");
    handles
        .recover_handle(
            &visible.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect_err("missing root child authority must invalidate terminal visibility");
    scoped
        .put_raw(
            &child_marker_path,
            child_marker_bytes,
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("restore root child marker");

    let mut divergent: RootTxManifest =
        serde_json::from_slice(super_manifest_bytes.as_ref()).expect("decode root manifest");
    divergent.fencing_token = divergent.fencing_token.saturating_add(1);
    scoped
        .put_raw(
            &super_manifest_path,
            Bytes::from(serde_json::to_vec(&divergent).expect("encode divergent root manifest")),
            WritePrecondition::None,
        )
        .await
        .expect("diverge root super-manifest");
    handles
        .commit_handle(
            &visible.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(6),
        )
        .await
        .expect_err("divergent root authority must invalidate terminal visibility");
    assert_eq!(
        handles
            .get_handle(&visible.handle_id)
            .await
            .expect("visible handle remains immutable")
            .status,
        ControlPlaneHandleStatus::Visible
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn root_recovery_adopts_an_exact_manifest_after_a_lost_write_response() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_850);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create root manifest-loss handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![catalog_domain_mutation("root_manifest_lost_response")],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage root manifest-loss mutation");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare root manifest-loss handle");
    backend.fail_after_next_matching_put("transactions/root/", 1);
    handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("lost super-manifest write response requires recovery");
    let repair = handles
        .get_handle(&created.handle.handle_id)
        .await
        .expect("repair root handle");
    assert_eq!(repair.status, ControlPlaneHandleStatus::RepairRequired);
    let original_tx_id = repair.participants[0]
        .tx_id
        .clone()
        .expect("original root tx id");
    assert!(
        ctx.scoped_storage(erased)
            .expect("scoped storage")
            .head_raw(&ControlPlaneTxPaths::root_super_manifest(&original_tx_id))
            .await
            .expect("head original super-manifest")
            .is_some()
    );

    let visible = handles
        .recover_handle(
            &repair.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect("adopt exact existing super-manifest");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(
        visible.participants[0].tx_id.as_deref(),
        Some(original_tx_id.as_str())
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn concurrent_root_recovery_keeps_one_immutable_receipt_for_the_frozen_tx_id() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct handle service");
    let now = instant(1_784_000_875);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create concurrent root recovery handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![catalog_domain_mutation("root_concurrent_recovery")],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage concurrent root recovery");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare concurrent root recovery");
    backend.fail_next_matching_put("transactions/root/", 1);
    handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("initial root super-manifest write must require recovery");
    let repair = handles
        .get_handle(&created.handle.handle_id)
        .await
        .expect("repair-required root handle");
    assert_eq!(repair.status, ControlPlaneHandleStatus::RepairRequired);
    let root_tx_id = repair.participants[0]
        .tx_id
        .clone()
        .expect("frozen root tx id");
    let super_manifest_path = ControlPlaneTxPaths::root_super_manifest(&root_tx_id);
    let (entered, release) = backend.gate_next_matching_put(&super_manifest_path);
    let first_handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct first recovery service");
    let second_handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct second recovery service");
    let handle_id = repair.handle_id.clone();
    let review_token = created.review_token.expose().to_string();
    let scoped = ctx.scoped_storage(erased.clone()).expect("scoped storage");
    let root_record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, &root_tx_id);

    let first = first_handles.recover_handle(
        &handle_id,
        &review_token,
        now + chrono::Duration::seconds(4),
    );
    let coordinate_second = async {
        entered.notified().await;
        let first_rearm_version = scoped
            .head_raw(&root_record_path)
            .await
            .expect("head first rearm")
            .expect("first rearmed record")
            .version;
        let second = second_handles.recover_handle(
            &handle_id,
            &review_token,
            now + chrono::Duration::seconds(5),
        );
        tokio::pin!(second);
        let mut second_rearmed = false;
        for _ in 0..1_000 {
            tokio::select! {
                result = &mut second => panic!("second recovery returned before the root gate released: {result:?}"),
                () = tokio::task::yield_now() => {
                    let version = scoped
                        .head_raw(&root_record_path)
                        .await
                        .expect("head second rearm")
                        .expect("second rearmed record")
                        .version;
                    if version != first_rearm_version {
                        second_rearmed = true;
                        break;
                    }
                }
            }
        }
        assert!(
            second_rearmed,
            "second recovery must rearm before the first is released"
        );
        release.notify_one();
        second.await
    };
    let (first_result, second_result) = tokio::join!(first, coordinate_second);
    first_result.expect("first root recovery");
    second_result.expect("second root recovery adopts the first winner");

    let visible = handles
        .get_handle(&handle_id)
        .await
        .expect("visible concurrent root handle");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    let record: ControlPlaneTxRecord<RootTxReceipt> = serde_json::from_slice(
        scoped
            .get_raw(&root_record_path)
            .await
            .expect("final root record")
            .as_ref(),
    )
    .expect("decode final root record");
    let receipt = record.result.expect("final root receipt");
    assert_eq!(receipt.tx_id, root_tx_id);
    assert_eq!(
        visible.participants[0].receipt_path.as_deref(),
        Some(ControlPlaneTxPaths::root_commit_receipt(&receipt.root_commit_id).as_str()),
        "the handle and exact root record must keep the same immutable receipt"
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn existing_low_level_prepared_status_maps_to_repair_required() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_900);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("prepared_mapping"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage catalog");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare handle");
    force_committing(
        erased.clone(),
        &prepared.handle_id,
        now + chrono::Duration::seconds(3),
    )
    .await;

    let participant = &prepared.participants[0];
    let tx_id = ulid::Ulid::new().to_string();
    let marker = ControlPlaneIdempotencyRecord {
        tx_id: tx_id.clone(),
        kind: ControlPlaneTxKind::CatalogDdl,
        request_id: participant.request_id.clone(),
        idempotency_key: participant.idempotency_key.clone(),
        request_hash: participant.request_hash.clone(),
        created_at: now,
        visible_at: None,
        tx_record: None,
    };
    let low_record = ControlPlaneTxRecord::<serde_json::Value> {
        tx_id: tx_id.clone(),
        kind: ControlPlaneTxKind::CatalogDdl,
        status: ControlPlaneTxStatus::Prepared,
        repair_pending: true,
        request_id: participant.request_id.clone(),
        idempotency_key: participant.idempotency_key.clone(),
        request_hash: marker.request_hash.clone(),
        lock_path: "locks/catalog".to_string(),
        fencing_token: 0,
        prepared_at: now,
        visible_at: None,
        durable_append: None,
        result: None,
    };
    put_json(
        erased.clone(),
        &ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Catalog,
            &participant.idempotency_key,
        ),
        &marker,
    )
    .await;
    put_json(
        erased,
        &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &tx_id),
        &low_record,
    )
    .await;

    handles
        .recover_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("prepared low-level participant must not be reported visible");
    let repair = handles
        .get_handle(&prepared.handle_id)
        .await
        .expect("get repair handle");
    assert_eq!(repair.status, ControlPlaneHandleStatus::RepairRequired);
    assert_eq!(
        repair.participants[0].low_level_status,
        Some(ControlPlaneTxStatus::Prepared)
    );
    assert_eq!(
        repair.participants[0].tx_id.as_deref(),
        Some(tx_id.as_str())
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn clean_prepared_claim_recovery_resumes_the_frozen_transaction_id() {
    for (domain, repair_pending) in [
        (ControlPlaneTxDomain::Catalog, false),
        (ControlPlaneTxDomain::Orchestration, false),
        (ControlPlaneTxDomain::Root, false),
        (ControlPlaneTxDomain::Root, true),
    ] {
        let backend = Arc::new(NoListFaultBackend::new());
        let erased: Arc<dyn StorageBackend> = backend.clone();
        let (state, _) = service(erased.clone());
        let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
            .expect("construct clean-prepared recovery service");
        let now = instant(1_784_000_925);
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create clean-prepared handle");
        match domain {
            ControlPlaneTxDomain::Catalog => {
                handles
                    .stage_catalog(
                        &created.handle.handle_id,
                        created.review_token.expose(),
                        1,
                        create_catalog("clean_prepared_resume"),
                        now + chrono::Duration::seconds(1),
                    )
                    .await
                    .expect("stage clean-prepared catalog");
            }
            ControlPlaneTxDomain::Orchestration => {
                handles
                    .stage_orchestration(
                        &created.handle.handle_id,
                        created.review_token.expose(),
                        1,
                        orchestration_batch(
                            "clean-prepared-resume",
                            &ulid::Ulid::new().to_string(),
                        ),
                        now + chrono::Duration::seconds(1),
                    )
                    .await
                    .expect("stage clean-prepared orchestration");
            }
            ControlPlaneTxDomain::Root => {
                handles
                    .stage_root(
                        &created.handle.handle_id,
                        created.review_token.expose(),
                        1,
                        vec![catalog_domain_mutation(if repair_pending {
                            "repair_pending_root_resume"
                        } else {
                            "clean_prepared_root_resume"
                        })],
                        now + chrono::Duration::seconds(1),
                    )
                    .await
                    .expect("stage prepared root recovery");
            }
        }
        let prepared = handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare clean-prepared handle");
        force_committing(
            erased.clone(),
            &prepared.handle_id,
            now + chrono::Duration::seconds(3),
        )
        .await;
        let participant = &prepared.participants[0];
        let tx_id = ulid::Ulid::new().to_string();
        let marker = ControlPlaneIdempotencyRecord {
            tx_id: tx_id.clone(),
            kind: participant.kind,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            created_at: now,
            visible_at: None,
            tx_record: None,
        };
        let record = ControlPlaneTxRecord::<serde_json::Value> {
            tx_id: tx_id.clone(),
            kind: participant.kind,
            status: ControlPlaneTxStatus::Prepared,
            repair_pending,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            lock_path: match domain {
                ControlPlaneTxDomain::Catalog => CatalogPaths::domain_lock(CatalogDomain::Catalog),
                ControlPlaneTxDomain::Orchestration => {
                    arco_flow::orchestration_compaction_lock_path().to_string()
                }
                ControlPlaneTxDomain::Root => ControlPlaneTxPaths::root_lock(),
            },
            fencing_token: 0,
            prepared_at: now,
            visible_at: None,
            durable_append: None,
            result: None,
        };
        put_json(
            erased.clone(),
            &ControlPlaneTxPaths::idempotency(domain, &participant.idempotency_key),
            &marker,
        )
        .await;
        put_json(
            erased.clone(),
            &ControlPlaneTxPaths::record(domain, &tx_id),
            &record,
        )
        .await;

        let visible = handles
            .recover_handle(
                &prepared.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(4),
            )
            .await
            .expect("resume clean prepared transaction in place");
        assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
        assert_eq!(
            visible.participants[0].tx_id.as_deref(),
            Some(tx_id.as_str())
        );
        assert_eq!(backend.list_calls(), 0);
    }
}

#[tokio::test]
async fn catalog_publication_before_low_level_finalize_recovers_without_reapplying() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct catalog publication recovery service");
    let now = instant(1_784_000_930);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create catalog publication recovery handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("catalog_published_before_finalize"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage catalog publication recovery");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare catalog publication recovery");

    let (entered, release, attempted) = backend.gate_matching_put_after("transactions/catalog/", 0);
    let commit = handles.commit_handle(
        &prepared.handle_id,
        created.review_token.expose(),
        now + chrono::Duration::seconds(3),
    );
    let inject = async {
        entered.notified().await;
        let attempted = attempted
            .lock()
            .expect("prepared catalog record mutex")
            .clone()
            .expect("captured prepared catalog record");
        let record: ControlPlaneTxRecord<serde_json::Value> =
            serde_json::from_slice(attempted.as_ref()).expect("decode prepared catalog record");
        assert_eq!(record.status, ControlPlaneTxStatus::Prepared);
        let record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &record.tx_id);
        backend.fail_next_matching_put(record_path.clone(), 0);
        release.notify_one();
        (record.tx_id, record_path)
    };
    let (result, (tx_id, record_path)) = tokio::join!(commit, inject);
    result.expect_err("lost low-level finalize must require handle recovery");
    backend.clear_failure();

    let repair = handles
        .get_handle(&prepared.handle_id)
        .await
        .expect("load catalog publication repair handle");
    assert_eq!(repair.status, ControlPlaneHandleStatus::RepairRequired);
    let scoped = request_context()
        .scoped_storage(erased.clone())
        .expect("scoped storage");
    let exact: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        scoped
            .get_raw(&record_path)
            .await
            .expect("prepared exact record after catalog publication")
            .as_ref(),
    )
    .expect("decode prepared exact record after catalog publication");
    assert_eq!(exact.status, ControlPlaneTxStatus::Prepared);
    let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
    let pointer_before = raw_object_snapshot(erased.clone(), &pointer_path)
        .await
        .expect("catalog pointer before exact recovery");

    let visible = handles
        .recover_handle(
            &repair.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect("recover catalog publication from its exact event intent");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(
        visible.participants[0].tx_id.as_deref(),
        Some(tx_id.as_str())
    );
    assert_eq!(
        raw_object_snapshot(erased.clone(), &pointer_path)
            .await
            .expect("catalog pointer after exact recovery"),
        pointer_before,
        "catalog recovery republished a second manifest"
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn frozen_catalog_identity_rejects_a_different_prepublished_payload() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct catalog payload-binding service");
    let now = instant(1_784_000_935);
    let reviewed_operation = create_catalog("reviewed_catalog_payload");
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create catalog payload-binding handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            reviewed_operation,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage reviewed catalog payload");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare reviewed catalog payload");
    force_committing(
        erased.clone(),
        &prepared.handle_id,
        now + chrono::Duration::seconds(3),
    )
    .await;

    let participant = prepared.participants[0].clone();
    let tx_id = ulid::Ulid::new().to_string();
    put_json(
        erased.clone(),
        &ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Catalog,
            &participant.idempotency_key,
        ),
        &ControlPlaneIdempotencyRecord {
            tx_id: tx_id.clone(),
            kind: ControlPlaneTxKind::CatalogDdl,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            created_at: now,
            visible_at: None,
            tx_record: None,
        },
    )
    .await;
    put_json(
        erased.clone(),
        &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &tx_id),
        &ControlPlaneTxRecord::<serde_json::Value> {
            tx_id: tx_id.clone(),
            kind: ControlPlaneTxKind::CatalogDdl,
            status: ControlPlaneTxStatus::Prepared,
            repair_pending: false,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            lock_path: CatalogPaths::domain_lock(CatalogDomain::Catalog),
            fencing_token: 0,
            prepared_at: now,
            visible_at: None,
            durable_append: None,
            result: None,
        },
    )
    .await;

    let storage = ctx
        .scoped_storage(erased.clone())
        .expect("catalog payload-binding storage");
    let compactor = Arc::new(arco_catalog::Tier1Compactor::new(storage.clone()));
    let writer = arco_catalog::CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
    writer
        .initialize()
        .await
        .expect("initialize catalog writer");
    let identity = writer
        .authorize_frozen_catalog_transaction(
            &tx_id,
            &participant.request_hash,
            &participant.request_id,
            &participant.idempotency_key,
        )
        .await
        .expect("authorize the exact frozen catalog participant");
    writer
        .create_catalog_transaction(
            "unreviewed_catalog_payload",
            Some("must not satisfy the frozen handle"),
            arco_catalog::WriteOptions::default()
                .with_actor("ordinary-catalog-writer")
                .with_request_id(&participant.request_id)
                .with_idempotency_key(&participant.idempotency_key)
                .with_transaction_identity(identity),
        )
        .await
        .expect_err("a different payload must not publish under the reviewed identity");
    assert!(
        storage
            .head_raw(&format!("transactions/catalog/{tx_id}.intent.json"))
            .await
            .expect("head rejected catalog intent")
            .is_none(),
        "request binding must fail before an event intent is published"
    );

    let visible = handles
        .recover_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect("the exact reviewed payload remains recoverable");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(
        visible.participants[0].tx_id.as_deref(),
        Some(tx_id.as_str())
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn frozen_catalog_authorizer_rejects_a_record_corrupted_at_its_first_exact_read() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct catalog authority-race service");
    let now = instant(1_784_000_936);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create catalog authority-race handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("catalog_authority_race"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage catalog authority-race mutation");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare catalog authority-race handle");

    let (entered, release, observed_path) =
        backend.gate_transaction_record_head_after(ControlPlaneTxDomain::Catalog, 0);
    let commit = handles.commit_handle(
        &created.handle.handle_id,
        created.review_token.expose(),
        now + chrono::Duration::seconds(3),
    );
    let race_backend = backend.clone();
    let corrupt = async {
        entered.notified().await;
        let path = observed_path
            .lock()
            .expect("catalog head gate path mutex")
            .clone()
            .expect("catalog head gate path");
        let mut record: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
            race_backend
                .inner
                .get(&path)
                .await
                .expect("read clean prepared record")
                .as_ref(),
        )
        .expect("decode clean prepared record");
        record.repair_pending = true;
        race_backend
            .inner
            .put(
                &path,
                Bytes::from(serde_json::to_vec(&record).expect("encode corrupt prepared record")),
                WritePrecondition::None,
            )
            .await
            .expect("publish corrupt prepared race winner");
        release.notify_one();
        (path, record.tx_id)
    };
    let (result, (record_path, tx_id)) = tokio::join!(commit, corrupt);

    result.expect_err("a non-clean prepared race winner must fail before catalog mutation");
    let storage = ctx
        .scoped_storage(backend.clone())
        .expect("catalog post-race storage");
    let preserved: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        backend
            .inner
            .get(&record_path)
            .await
            .expect("read preserved corrupt race winner")
            .as_ref(),
    )
    .expect("decode preserved corrupt race winner");
    assert!(preserved.repair_pending);
    assert!(
        storage
            .head_raw(&format!("transactions/catalog/{tx_id}.intent.json"))
            .await
            .expect("head forbidden catalog intent")
            .is_none(),
        "authority validation must fail before transaction-owned event publication"
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn frozen_catalog_authorizer_rejects_claims_not_owned_by_the_exact_staged_mutation() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct catalog claim-set service");
    let now = instant(1_784_000_937);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create catalog claim-set handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("catalog_claim_set"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage catalog claim-set mutation");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare catalog claim-set handle");
    force_committing(
        erased.clone(),
        &prepared.handle_id,
        now + chrono::Duration::seconds(3),
    )
    .await;

    let participant = prepared.participants[0].clone();
    let tx_id = ulid::Ulid::new().to_string();
    put_json(
        erased.clone(),
        &ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Catalog,
            &participant.idempotency_key,
        ),
        &ControlPlaneIdempotencyRecord {
            tx_id: tx_id.clone(),
            kind: ControlPlaneTxKind::CatalogDdl,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            created_at: now,
            visible_at: None,
            tx_record: None,
        },
    )
    .await;
    put_json(
        erased.clone(),
        &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &tx_id),
        &ControlPlaneTxRecord::<serde_json::Value> {
            tx_id: tx_id.clone(),
            kind: ControlPlaneTxKind::CatalogDdl,
            status: ControlPlaneTxStatus::Prepared,
            repair_pending: false,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            lock_path: CatalogPaths::domain_lock(CatalogDomain::Catalog),
            fencing_token: 0,
            prepared_at: now,
            visible_at: None,
            durable_append: None,
            result: None,
        },
    )
    .await;

    let storage = ctx
        .scoped_storage(erased)
        .expect("catalog claim-set storage");
    let authority_path = ControlPlaneTxPaths::handle_identity_authority(&prepared.handle_id, 1)
        .expect("catalog claim-set authority path");
    let mut authority: serde_json::Value = serde_json::from_slice(
        storage
            .get_raw(&authority_path)
            .await
            .expect("read catalog claim-set authority")
            .as_ref(),
    )
    .expect("decode catalog claim-set authority");
    authority["handle_intent"]["claim_identities"]
        .as_array_mut()
        .expect("catalog claim set")
        .push(serde_json::json!({
            "domain": "orchestration",
            "kind": "orchestration_batch",
            "idempotency_key": format!(
                "root:{}:orchestration",
                participant.idempotency_key
            ),
        }));
    storage
        .put_raw(
            &authority_path,
            Bytes::from(
                arco_core::canonical_json::to_canonical_bytes(&authority)
                    .expect("canonical divergent catalog claim set"),
            ),
            WritePrecondition::None,
        )
        .await
        .expect("publish divergent catalog claim set");

    let writer = arco_catalog::CatalogWriter::new(storage.clone());
    writer
        .authorize_frozen_catalog_transaction(
            &tx_id,
            &participant.request_hash,
            &participant.request_id,
            &participant.idempotency_key,
        )
        .await
        .expect_err("writer must require the exact complete staged claim set");
    assert!(
        storage
            .head_raw(&format!("transactions/catalog/{tx_id}.intent.json"))
            .await
            .expect("head forbidden claim-set intent")
            .is_none()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn exact_visible_catalog_authority_issues_only_a_read_only_capability() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct exact-visible capability service");
    let now = instant(1_784_000_938);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create exact-visible capability handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("exact_visible_capability"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage exact-visible capability mutation");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare exact-visible capability handle");
    let visible = handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("seed exact-visible catalog authority");
    let participant = &visible.participants[0];
    let tx_id = participant
        .tx_id
        .as_deref()
        .expect("exact-visible catalog transaction ID");
    let storage = ctx
        .scoped_storage(erased)
        .expect("exact-visible capability storage");
    let writer = arco_catalog::CatalogWriter::new(storage);
    let identity = writer
        .authorize_frozen_catalog_transaction(
            tx_id,
            &participant.request_hash,
            &participant.request_id,
            &participant.idempotency_key,
        )
        .await
        .expect("validate exact-visible catalog authority");

    writer
        .create_catalog_transaction(
            "exact_visible_capability",
            Some("staged through a typed handle"),
            arco_catalog::WriteOptions::default()
                .with_actor("ordinary-catalog-writer")
                .with_request_id(&participant.request_id)
                .with_idempotency_key(&participant.idempotency_key)
                .with_transaction_identity(identity),
        )
        .await
        .expect_err("visible authority must never authorize another catalog mutation");
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn catalog_orphan_event_reissues_after_a_later_watermark_without_listing() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct catalog orphan recovery service");
    let now = instant(1_784_000_940);

    let initializer = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create catalog initializer");
    handles
        .stage_catalog(
            &initializer.handle.handle_id,
            initializer.review_token.expose(),
            1,
            create_catalog("catalog_orphan_initializer"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage catalog initializer");
    handles
        .prepare_handle(
            &initializer.handle.handle_id,
            initializer.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare catalog initializer");
    handles
        .commit_handle(
            &initializer.handle.handle_id,
            initializer.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("publish catalog initializer");

    let target = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create catalog orphan target");
    handles
        .stage_catalog(
            &target.handle.handle_id,
            target.review_token.expose(),
            1,
            create_catalog("catalog_orphan_reissued"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage catalog orphan target");
    let target_prepared = handles
        .prepare_handle(
            &target.handle.handle_id,
            target.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare catalog orphan target");
    let catalog_pointer = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
    backend.fail_next_matching_put(catalog_pointer, 0);
    handles
        .commit_handle(
            &target_prepared.handle_id,
            target.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("catalog manifest failure must leave an exact event intent");
    backend.clear_failure();
    let repair = handles
        .get_handle(&target_prepared.handle_id)
        .await
        .expect("load catalog orphan repair handle");
    let target_tx_id = repair.participants[0]
        .tx_id
        .clone()
        .expect("catalog orphan transaction ID");
    let intent_path = format!("transactions/catalog/{target_tx_id}.intent.json");
    let scoped = ctx.scoped_storage(erased.clone()).expect("scoped storage");
    let initial_intent: serde_json::Value = serde_json::from_slice(
        scoped
            .get_raw(&intent_path)
            .await
            .expect("initial catalog event intent")
            .as_ref(),
    )
    .expect("decode initial catalog event intent");
    assert_eq!(
        initial_intent["eventIds"]
            .as_array()
            .expect("initial event IDs")
            .len(),
        1
    );

    tokio::time::sleep(Duration::from_millis(2)).await;
    let later = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create later catalog handle");
    handles
        .stage_catalog(
            &later.handle.handle_id,
            later.review_token.expose(),
            1,
            create_catalog("catalog_after_orphan"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage later catalog mutation");
    handles
        .prepare_handle(
            &later.handle.handle_id,
            later.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare later catalog mutation");
    handles
        .commit_handle(
            &later.handle.handle_id,
            later.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("advance catalog watermark past orphan");

    let visible = handles
        .recover_handle(
            &repair.handle_id,
            target.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect("reissue the orphaned reviewed event after the later watermark");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(
        visible.participants[0].tx_id.as_deref(),
        Some(target_tx_id.as_str())
    );
    let recovered_intent: serde_json::Value = serde_json::from_slice(
        scoped
            .get_raw(&intent_path)
            .await
            .expect("recovered catalog event intent")
            .as_ref(),
    )
    .expect("decode recovered catalog event intent");
    assert_eq!(
        recovered_intent["eventIds"]
            .as_array()
            .expect("recovered event IDs")
            .len(),
        2
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn aborted_catalog_claim_never_replaces_the_frozen_transaction_id() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_000_950);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create aborted-claim handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("aborted_claim"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage aborted-claim handle");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare aborted-claim handle");
    force_committing(
        erased.clone(),
        &prepared.handle_id,
        now + chrono::Duration::seconds(3),
    )
    .await;
    let participant = &prepared.participants[0];
    let tx_id = ulid::Ulid::new().to_string();
    let marker = ControlPlaneIdempotencyRecord {
        tx_id: tx_id.clone(),
        kind: participant.kind,
        request_id: participant.request_id.clone(),
        idempotency_key: participant.idempotency_key.clone(),
        request_hash: participant.request_hash.clone(),
        created_at: now,
        visible_at: None,
        tx_record: None,
    };
    let record = ControlPlaneTxRecord::<serde_json::Value> {
        tx_id: tx_id.clone(),
        kind: participant.kind,
        status: ControlPlaneTxStatus::Aborted,
        repair_pending: false,
        request_id: participant.request_id.clone(),
        idempotency_key: participant.idempotency_key.clone(),
        request_hash: participant.request_hash.clone(),
        lock_path: "locks/catalog.lock.json".to_string(),
        fencing_token: 0,
        prepared_at: now,
        visible_at: None,
        durable_append: None,
        result: None,
    };
    let marker_path = ControlPlaneTxPaths::idempotency(
        ControlPlaneTxDomain::Catalog,
        &participant.idempotency_key,
    );
    put_json(erased.clone(), &marker_path, &marker).await;
    put_json(
        erased.clone(),
        &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &tx_id),
        &record,
    )
    .await;

    handles
        .recover_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("an aborted claim must not be replaced by a new transaction");
    let after: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        request_context()
            .scoped_storage(erased)
            .expect("scoped storage")
            .get_raw(&marker_path)
            .await
            .expect("stable aborted marker")
            .as_ref(),
    )
    .expect("decode stable aborted marker");
    assert_eq!(after.tx_id, tx_id);
    assert_eq!(
        handles
            .get_handle(&prepared.handle_id)
            .await
            .expect("repair-required aborted handle")
            .status,
        ControlPlaneHandleStatus::RepairRequired
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn staged_object_orphan_and_both_preparing_crash_points_resume_exactly() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct handle service");
    let now = instant(1_784_001_000);

    let orphaned = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create orphan test handle");
    let orphan_handle_path =
        ControlPlaneTxPaths::handle_record(&orphaned.handle.handle_id).expect("orphan handle path");
    backend.fail_next_matching_put(orphan_handle_path, 0);
    handles
        .stage_catalog(
            &orphaned.handle.handle_id,
            orphaned.review_token.expose(),
            1,
            create_catalog("orphan_retry"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("fail handle-reference CAS after immutable object write");
    let orphan_path = ControlPlaneTxPaths::handle_mutation(&orphaned.handle.handle_id, 1)
        .expect("orphan mutation path");
    let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
    assert!(
        storage
            .head_raw(&orphan_path)
            .await
            .expect("head orphan")
            .is_some()
    );
    assert!(
        handles
            .get_handle(&orphaned.handle.handle_id)
            .await
            .expect("orphan handle")
            .mutation_refs
            .is_empty()
    );
    let attached = handles
        .stage_catalog(
            &orphaned.handle.handle_id,
            orphaned.review_token.expose(),
            1,
            create_catalog("orphan_retry"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("attach byte-identical orphan retry");
    assert_eq!(attached.mutation_refs.len(), 1);

    let missing = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create missing stage handle");
    handles
        .stage_catalog(
            &missing.handle.handle_id,
            missing.review_token.expose(),
            1,
            create_catalog("preparing_missing"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage missing test");
    let missing_path = ControlPlaneTxPaths::handle_mutation(&missing.handle.handle_id, 1)
        .expect("missing mutation path");
    let missing_bytes = storage
        .get_raw(&missing_path)
        .await
        .expect("read staged bytes");
    storage
        .delete(&missing_path)
        .await
        .expect("remove staged bytes");
    handles
        .prepare_handle(
            &missing.handle.handle_id,
            missing.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect_err("missing object after PREPARING transition");
    assert_eq!(
        handles
            .get_handle(&missing.handle.handle_id)
            .await
            .expect("preparing handle")
            .status,
        ControlPlaneHandleStatus::Preparing
    );
    storage
        .put_raw(
            &missing_path,
            missing_bytes,
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("restore exact staged bytes");
    assert_eq!(
        handles
            .prepare_handle(
                &missing.handle.handle_id,
                missing.review_token.expose(),
                now + chrono::Duration::seconds(3),
            )
            .await
            .expect("resume PREPARING after exact object returns")
            .status,
        ControlPlaneHandleStatus::Prepared
    );

    let cas = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create prepare CAS handle");
    handles
        .stage_catalog(
            &cas.handle.handle_id,
            cas.review_token.expose(),
            1,
            create_catalog("preparing_cas"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage prepare CAS test");
    backend.fail_next_matching_put(
        ControlPlaneTxPaths::handle_record(&cas.handle.handle_id).expect("CAS handle path"),
        1,
    );
    handles
        .prepare_handle(
            &cas.handle.handle_id,
            cas.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect_err("fail PREPARING to PREPARED CAS after validation");
    assert_eq!(
        handles
            .get_handle(&cas.handle.handle_id)
            .await
            .expect("CAS preparing handle")
            .status,
        ControlPlaneHandleStatus::Preparing
    );
    assert_eq!(
        handles
            .prepare_handle(
                &cas.handle.handle_id,
                cas.review_token.expose(),
                now + chrono::Duration::seconds(3),
            )
            .await
            .expect("resume after prepared CAS loss")
            .status,
        ControlPlaneHandleStatus::Prepared
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn final_visible_handle_cas_loss_recovers_without_reexecuting_participant() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased);
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_001_100);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("final_cas_loss"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage catalog");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare handle");
    backend.fail_next_matching_put(
        ControlPlaneTxPaths::handle_record(&created.handle.handle_id)
            .expect("final CAS handle path"),
        2,
    );
    handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("fail only final VISIBLE handle CAS");
    let committing = handles
        .get_handle(&created.handle.handle_id)
        .await
        .expect("get committing handle");
    assert_eq!(committing.status, ControlPlaneHandleStatus::Committing);
    assert_eq!(committing.visible_participant_count(), 1);
    let tx_id = committing.participants[0].tx_id.clone();

    let recovered = handles
        .recover_handle(
            &committing.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect("retry only final handle CAS");
    assert_eq!(recovered.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(recovered.participants[0].tx_id, tx_id);
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn orchestration_staging_persists_runtime_events_and_uses_existing_executor() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct handle service");
    let now = instant(1_784_001_200);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");
    handles
        .stage_orchestration(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            orchestration_batch("run-handle", "01KXHANDLEORCHESTRATION001"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage orchestration runtime events");
    let staged = ctx
        .scoped_storage(erased)
        .expect("scoped storage")
        .get_raw(
            &ControlPlaneTxPaths::handle_mutation(&created.handle.handle_id, 1)
                .expect("orchestration staged path"),
        )
        .await
        .expect("staged orchestration bytes");
    let wire = String::from_utf8_lossy(&staged);
    assert!(wire.contains("orchestration"));
    assert!(wire.contains("run_triggered"));
    assert!(!wire.contains("protoHex"));
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare orchestration");
    let visible = handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("commit orchestration through existing executor");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(
        visible.participants[0].kind,
        ControlPlaneTxKind::OrchestrationBatch
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn orchestration_durable_append_recovery_finishes_the_same_transaction() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_001_350);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create orchestration recovery handle");
    let mut reviewed_batch = orchestration_batch("run-handle-repair", "01KXHANDLEREPAIR0000000001");
    let Some(orchestration_event_envelope::Event::RunTriggered(run)) =
        reviewed_batch.events[0].event.as_mut()
    else {
        panic!("run-triggered recovery fixture");
    };
    run.labels = BTreeMap::from([
        ("zeta".to_string(), "last".to_string()),
        ("alpha".to_string(), "first".to_string()),
        ("middle".to_string(), "between".to_string()),
    ]);
    handles
        .stage_orchestration(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            reviewed_batch,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage orchestration recovery");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare orchestration recovery");

    backend.fail_next_matching_put("state/orchestration/manifests/", 0);
    handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("post-append compaction failure must require recovery");
    let repair = handles
        .get_handle(&created.handle.handle_id)
        .await
        .expect("repair-required orchestration handle");
    assert_eq!(repair.status, ControlPlaneHandleStatus::RepairRequired);
    assert_eq!(
        repair.participants[0].low_level_status,
        Some(ControlPlaneTxStatus::Prepared)
    );
    let original_tx_id = repair.participants[0]
        .tx_id
        .clone()
        .expect("repair transaction id");
    let storage = request_context()
        .scoped_storage(erased.clone())
        .expect("scoped orchestration recovery storage");
    let repair_record: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        storage
            .get_raw(&ControlPlaneTxPaths::record(
                ControlPlaneTxDomain::Orchestration,
                &original_tx_id,
            ))
            .await
            .expect("read repair-pending orchestration record")
            .as_ref(),
    )
    .expect("decode repair-pending orchestration record");
    let event_path = repair_record
        .durable_append
        .as_ref()
        .and_then(|append| append.event_paths.first())
        .expect("repair-pending event path");
    let legacy_event: serde_json::Value = serde_json::from_slice(
        storage
            .get_raw(event_path)
            .await
            .expect("read canonical event before legacy rewrite")
            .as_ref(),
    )
    .expect("decode canonical event before legacy rewrite");
    storage
        .put_raw(
            event_path,
            Bytes::from(
                serde_json::to_vec_pretty(&legacy_event).expect("encode equivalent legacy event"),
            ),
            WritePrecondition::None,
        )
        .await
        .expect("rewrite event using an equivalent legacy encoding");

    backend.clear_failure();
    let (recovered_state, _) = service(erased);
    let recovered_handles =
        ControlPlaneTransactionHandleService::new(&recovered_state, request_context())
            .expect("reconstruct orchestration recovery service");
    let visible = recovered_handles
        .recover_handle(
            &repair.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect("repair durable append through the existing executor");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(
        visible.participants[0].tx_id.as_deref(),
        Some(original_tx_id.as_str())
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn orchestration_durable_append_repair_rejects_unreviewed_event_paths() {
    for nested_root in [false, true] {
        let backend = Arc::new(NoListFaultBackend::new());
        let erased: Arc<dyn StorageBackend> = backend.clone();
        let (state, ctx) = service(erased.clone());
        let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
            .expect("construct durable-append binding service");
        let now = instant(1_784_001_360);

        let unrelated_event_id = ulid::Ulid::new().to_string();
        let unrelated = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create unrelated orchestration handle");
        handles
            .stage_orchestration(
                &unrelated.handle.handle_id,
                unrelated.review_token.expose(),
                1,
                orchestration_batch("unrelated-durable-append", &unrelated_event_id),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect("stage unrelated orchestration event");
        handles
            .prepare_handle(
                &unrelated.handle.handle_id,
                unrelated.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare unrelated orchestration event");
        handles
            .commit_handle(
                &unrelated.handle.handle_id,
                unrelated.review_token.expose(),
                now + chrono::Duration::seconds(3),
            )
            .await
            .expect("publish unrelated orchestration event");

        let reviewed_event_id = ulid::Ulid::new().to_string();
        let target = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create durable-append binding target");
        if nested_root {
            handles
                .stage_root(
                    &target.handle.handle_id,
                    target.review_token.expose(),
                    1,
                    vec![DomainMutation {
                        kind: Some(domain_mutation::Kind::Orchestration(orchestration_batch(
                            "reviewed-root-append",
                            &reviewed_event_id,
                        ))),
                    }],
                    now + chrono::Duration::seconds(1),
                )
                .await
                .expect("stage reviewed root orchestration event");
        } else {
            handles
                .stage_orchestration(
                    &target.handle.handle_id,
                    target.review_token.expose(),
                    1,
                    orchestration_batch("reviewed-direct-append", &reviewed_event_id),
                    now + chrono::Duration::seconds(1),
                )
                .await
                .expect("stage reviewed direct orchestration event");
        }
        let prepared = handles
            .prepare_handle(
                &target.handle.handle_id,
                target.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare durable-append binding target");
        backend.fail_next_matching_put("state/orchestration/manifests/", 0);
        handles
            .commit_handle(
                &prepared.handle_id,
                target.review_token.expose(),
                now + chrono::Duration::seconds(3),
            )
            .await
            .expect_err("seed repair-required durable append");
        backend.clear_failure();
        let repair = handles
            .get_handle(&prepared.handle_id)
            .await
            .expect("load durable-append repair handle");
        assert_eq!(repair.status, ControlPlaneHandleStatus::RepairRequired);

        let child_key = if nested_root {
            format!(
                "root:{}:orchestration",
                prepared.participants[0].idempotency_key
            )
        } else {
            prepared.participants[0].idempotency_key.clone()
        };
        let marker_path =
            ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Orchestration, &child_key);
        let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
        let marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
            storage
                .get_raw(&marker_path)
                .await
                .expect("durable-append marker")
                .as_ref(),
        )
        .expect("decode durable-append marker");
        let record_path =
            ControlPlaneTxPaths::record(ControlPlaneTxDomain::Orchestration, &marker.tx_id);
        let mut record: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
            storage
                .get_raw(&record_path)
                .await
                .expect("durable-append exact record")
                .as_ref(),
        )
        .expect("decode durable-append exact record");
        let durable = record
            .durable_append
            .as_mut()
            .expect("repair record has durable append");
        durable.event_paths = vec![orchestration_event_path(&unrelated_event_id)];
        storage
            .put_raw(
                &record_path,
                Bytes::from(serde_json::to_vec(&record).expect("encode corrupt durable append")),
                WritePrecondition::None,
            )
            .await
            .expect("replace durable append with unreviewed event path");
        let marker_before = raw_object_snapshot(erased.clone(), &marker_path)
            .await
            .expect("marker before unreviewed repair");
        let record_before = raw_object_snapshot(erased.clone(), &record_path)
            .await
            .expect("record before unreviewed repair");
        let pointer_path = arco_flow::orchestration_manifest_pointer_path();
        let pointer_before = raw_object_snapshot(erased.clone(), pointer_path)
            .await
            .expect("pointer before unreviewed repair");

        handles
            .recover_handle(
                &repair.handle_id,
                target.review_token.expose(),
                now + chrono::Duration::seconds(4),
            )
            .await
            .expect_err("unreviewed durable append must fail before compaction");
        assert_eq!(
            raw_object_snapshot(erased.clone(), &marker_path)
                .await
                .expect("marker after unreviewed repair"),
            marker_before
        );
        assert_eq!(
            raw_object_snapshot(erased.clone(), &record_path)
                .await
                .expect("record after unreviewed repair"),
            record_before
        );
        assert_eq!(
            raw_object_snapshot(erased.clone(), pointer_path)
                .await
                .expect("pointer after unreviewed repair"),
            pointer_before
        );
        assert_eq!(backend.list_calls(), 0);
    }
}

#[tokio::test]
async fn malformed_prepared_visibility_evidence_blocks_orchestration_repair_before_mutation() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct malformed repair service");
    let now = instant(1_784_001_375);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create malformed repair handle");
    handles
        .stage_orchestration(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            orchestration_batch("run-malformed-repair", "01KXHANDLEMALFORMED00001"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage malformed repair mutation");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare malformed repair handle");
    backend.fail_next_matching_put("state/orchestration/manifests/", 0);
    handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("durable append ambiguity must require repair");
    backend.clear_failure();
    let repair = handles
        .get_handle(&created.handle.handle_id)
        .await
        .expect("get malformed repair handle");
    let tx_id = repair.participants[0]
        .tx_id
        .as_deref()
        .expect("repair transaction ID");
    let record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Orchestration, tx_id);
    let scoped = ctx.scoped_storage(erased).expect("scoped storage");
    let mut record: ControlPlaneTxRecord<OrchestrationTxReceipt> = serde_json::from_slice(
        scoped
            .get_raw(&record_path)
            .await
            .expect("read repair record")
            .as_ref(),
    )
    .expect("decode repair record");
    record.visible_at = Some(now + chrono::Duration::seconds(3));
    record.result = Some(OrchestrationTxReceipt {
        tx_id: tx_id.to_string(),
        commit_id: ulid::Ulid::new().to_string(),
        manifest_id: "malformed-manifest".to_string(),
        revision_ulid: ulid::Ulid::new().to_string(),
        delta_id: "malformed-delta".to_string(),
        pointer_version: "malformed-version".to_string(),
        events_processed: 1,
        read_token: "orchestration:malformed-manifest".to_string(),
        visible_at: now + chrono::Duration::seconds(3),
    });
    let malformed_bytes =
        Bytes::from(serde_json::to_vec(&record).expect("encode malformed record"));
    scoped
        .put_raw(
            &record_path,
            malformed_bytes.clone(),
            WritePrecondition::None,
        )
        .await
        .expect("persist malformed repair record");
    let pointer_path = "state/orchestration/manifests/current.json";
    let pointer_before = scoped
        .head_raw(pointer_path)
        .await
        .expect("head pointer before malformed recovery")
        .map(|metadata| metadata.version);

    handles
        .recover_handle(
            &repair.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("malformed non-visible evidence must fail before repair mutation");
    assert_eq!(
        scoped
            .get_raw(&record_path)
            .await
            .expect("read unchanged malformed record"),
        malformed_bytes
    );
    assert_eq!(
        scoped
            .head_raw(pointer_path)
            .await
            .expect("head pointer after malformed recovery")
            .map(|metadata| metadata.version),
        pointer_before
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn concurrent_orchestration_repair_adopts_one_immutable_visible_receipt() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct concurrent repair service");
    let now = instant(1_784_001_400);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create concurrent repair handle");
    handles
        .stage_orchestration(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            orchestration_batch("run-concurrent-repair", "01KXHANDLECONCURRENT001"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage concurrent repair mutation");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare concurrent repair handle");
    backend.fail_next_matching_put("state/orchestration/manifests/", 0);
    handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("seed orchestration repair state");
    backend.clear_failure();
    let repair = handles
        .get_handle(&created.handle.handle_id)
        .await
        .expect("get concurrent repair handle");
    let tx_id = repair.participants[0]
        .tx_id
        .clone()
        .expect("concurrent repair transaction ID");
    let (entered, release) = backend.gate_next_matching_put("commits/orchestration/");
    let first_handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct first repair service");
    let second_handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct second repair service");
    let first = first_handles.recover_handle(
        &repair.handle_id,
        created.review_token.expose(),
        now + chrono::Duration::seconds(4),
    );
    let coordinate_second = async {
        entered.notified().await;
        let second = second_handles.recover_handle(
            &repair.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(5),
        );
        tokio::pin!(second);
        let result = match tokio::time::timeout(Duration::from_millis(200), &mut second).await {
            Ok(result) => result,
            Err(_) => {
                release.notify_one();
                return second.await;
            }
        };
        release.notify_one();
        result
    };
    let (first_result, second_result) = tokio::join!(first, coordinate_second);
    assert!(
        first_result.is_ok() || second_result.is_ok(),
        "at least one repair caller must publish the immutable winner"
    );

    let final_handle = handles
        .get_handle(&repair.handle_id)
        .await
        .expect("get concurrent repair winner");
    assert_eq!(final_handle.status, ControlPlaneHandleStatus::Visible);
    let scoped = ctx.scoped_storage(erased).expect("scoped storage");
    let exact: ControlPlaneTxRecord<OrchestrationTxReceipt> = serde_json::from_slice(
        scoped
            .get_raw(&ControlPlaneTxPaths::record(
                ControlPlaneTxDomain::Orchestration,
                &tx_id,
            ))
            .await
            .expect("read exact repair winner")
            .as_ref(),
    )
    .expect("decode exact repair winner");
    let receipt = exact.result.expect("visible repair receipt");
    assert_eq!(receipt.commit_id, tx_id);
    assert_eq!(
        final_handle.participants[0].receipt_path.as_deref(),
        Some(ControlPlaneTxPaths::orchestration_commit_receipt(&receipt.commit_id).as_str())
    );
    handles
        .commit_handle(
            &final_handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(6),
        )
        .await
        .expect("terminal visible replay exact-adopts the immutable repair winner");
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn orchestration_receipt_conflict_must_match_the_reviewed_manifest_authority() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct receipt-authority service");
    let now = instant(1_784_001_410);

    let unrelated = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create unrelated receipt handle");
    handles
        .stage_orchestration(
            &unrelated.handle.handle_id,
            unrelated.review_token.expose(),
            1,
            orchestration_batch(
                "unrelated-receipt-authority",
                &ulid::Ulid::new().to_string(),
            ),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage unrelated receipt authority");
    handles
        .prepare_handle(
            &unrelated.handle.handle_id,
            unrelated.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare unrelated receipt authority");
    let unrelated_visible = handles
        .commit_handle(
            &unrelated.handle.handle_id,
            unrelated.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("publish unrelated receipt authority");
    let unrelated_tx_id = unrelated_visible.participants[0]
        .tx_id
        .as_deref()
        .expect("unrelated transaction ID");
    let scoped = ctx.scoped_storage(erased.clone()).expect("scoped storage");
    let unrelated_record: ControlPlaneTxRecord<OrchestrationTxReceipt> = serde_json::from_slice(
        scoped
            .get_raw(&ControlPlaneTxPaths::record(
                ControlPlaneTxDomain::Orchestration,
                unrelated_tx_id,
            ))
            .await
            .expect("read unrelated exact receipt")
            .as_ref(),
    )
    .expect("decode unrelated exact receipt");
    let unrelated_receipt = unrelated_record.result.expect("unrelated receipt");

    let target = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create target receipt handle");
    handles
        .stage_orchestration(
            &target.handle.handle_id,
            target.review_token.expose(),
            1,
            orchestration_batch("target-receipt-authority", &ulid::Ulid::new().to_string()),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage target receipt authority");
    let prepared = handles
        .prepare_handle(
            &target.handle.handle_id,
            target.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare target receipt authority");
    let participant = &prepared.participants[0];
    let marker_path = ControlPlaneTxPaths::idempotency(
        ControlPlaneTxDomain::Orchestration,
        &participant.idempotency_key,
    );
    let (entered, release, attempted) =
        backend.gate_matching_put_after("commits/orchestration/", 0);
    let commit = handles.commit_handle(
        &prepared.handle_id,
        target.review_token.expose(),
        now + chrono::Duration::seconds(3),
    );
    let poison = async {
        entered.notified().await;
        let attempted = attempted
            .lock()
            .expect("attempted receipt mutex")
            .clone()
            .expect("captured target receipt");
        let mut poisoned: OrchestrationTxReceipt =
            serde_json::from_slice(attempted.as_ref()).expect("decode target receipt");
        poisoned.manifest_id = unrelated_receipt.manifest_id.clone();
        poisoned.revision_ulid = unrelated_receipt.revision_ulid.clone();
        poisoned.delta_id = unrelated_receipt.delta_id.clone();
        poisoned.pointer_version = unrelated_receipt.pointer_version.clone();
        poisoned.events_processed = unrelated_receipt.events_processed;
        poisoned.read_token = unrelated_receipt.read_token.clone();
        let receipt_path = ControlPlaneTxPaths::orchestration_commit_receipt(&poisoned.commit_id);
        scoped
            .put_raw(
                &receipt_path,
                Bytes::from(serde_json::to_vec(&poisoned).expect("encode poisoned receipt")),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("seed poisoned deterministic receipt");
        let marker = raw_object_snapshot(erased.clone(), &marker_path)
            .await
            .expect("claim-only target marker");
        let exact_path =
            ControlPlaneTxPaths::record(ControlPlaneTxDomain::Orchestration, &poisoned.tx_id);
        let exact = raw_object_snapshot(erased.clone(), &exact_path)
            .await
            .expect("prepared target record");
        let poisoned_snapshot = raw_object_snapshot(erased.clone(), &receipt_path)
            .await
            .expect("poisoned receipt snapshot");
        release.notify_one();
        (receipt_path, poisoned_snapshot, exact_path, exact, marker)
    };
    let (result, (receipt_path, poisoned, exact_path, exact, marker)) =
        tokio::join!(commit, poison);
    result.expect_err("unrelated receipt authority must fail closed");
    assert_eq!(
        raw_object_snapshot(erased.clone(), &marker_path)
            .await
            .expect("marker after poisoned receipt"),
        marker
    );
    assert_eq!(
        raw_object_snapshot(erased.clone(), &exact_path)
            .await
            .expect("exact record after poisoned receipt"),
        exact
    );
    assert_eq!(
        raw_object_snapshot(erased.clone(), &receipt_path)
            .await
            .expect("poisoned receipt after conflict"),
        poisoned
    );
    assert_ne!(
        handles
            .get_handle(&prepared.handle_id)
            .await
            .expect("target handle after poisoned receipt")
            .status,
        ControlPlaneHandleStatus::Visible
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn orchestration_staging_uses_canonical_bytes_for_equivalent_map_orders() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, ctx) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct handle service");
    let now = instant(1_784_001_250);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create canonical handle");

    let mut first = orchestration_batch("run-canonical", "01KXHANDLECANONICAL00001");
    let Some(orchestration_event_envelope::Event::RunTriggered(first_run)) =
        first.events[0].event.as_mut()
    else {
        panic!("run-triggered fixture");
    };
    first_run.labels = BTreeMap::from([
        ("zeta".to_string(), "last".to_string()),
        ("alpha".to_string(), "first".to_string()),
    ]);
    handles
        .stage_orchestration(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            first,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage first map order");

    let path = ControlPlaneTxPaths::handle_mutation(&created.handle.handle_id, 1)
        .expect("canonical staged path");
    let storage = ctx.scoped_storage(backend).expect("scoped storage");
    let first_bytes = storage.get_raw(&path).await.expect("first staged bytes");
    let value: serde_json::Value =
        serde_json::from_slice(&first_bytes).expect("decode first staged bytes");
    assert_eq!(
        first_bytes.as_ref(),
        arco_core::canonical_json::to_canonical_bytes(&value)
            .expect("canonical staged value")
            .as_slice(),
        "immutable staged bytes must use canonical JSON"
    );

    let mut second = orchestration_batch("run-canonical", "01KXHANDLECANONICAL00001");
    let Some(orchestration_event_envelope::Event::RunTriggered(second_run)) =
        second.events[0].event.as_mut()
    else {
        panic!("run-triggered fixture");
    };
    second_run.labels = BTreeMap::from([
        ("alpha".to_string(), "first".to_string()),
        ("zeta".to_string(), "last".to_string()),
    ]);
    handles
        .stage_orchestration(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            second,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("equivalent map order is a byte-identical retry");
    assert_eq!(
        storage.get_raw(&path).await.expect("retried staged bytes"),
        first_bytes
    );
}

#[tokio::test]
async fn wrong_review_token_gates_every_mutating_lifecycle_operation_without_echo() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend);
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_001_300);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create handle");
    let wrong = "review_plaintext_wrong_secret";
    for error in [
        handles
            .stage_catalog(
                &created.handle.handle_id,
                wrong,
                1,
                create_catalog("wrong_token"),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("stage token gate"),
        handles
            .prepare_handle(
                &created.handle.handle_id,
                wrong,
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("prepare token gate"),
        handles
            .commit_handle(
                &created.handle.handle_id,
                wrong,
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("commit token gate"),
        handles
            .recover_handle(
                &created.handle.handle_id,
                wrong,
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("recover token gate"),
        handles
            .abort_handle(
                &created.handle.handle_id,
                wrong,
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("abort token gate"),
        handles
            .expire_handle(
                &created.handle.handle_id,
                wrong,
                now + chrono::Duration::seconds(601),
            )
            .await
            .expect_err("expire token gate"),
    ] {
        assert_eq!(error.code(), "FORBIDDEN");
        assert!(!error.message().contains(wrong));
    }
    let unchanged = handles
        .get_handle(&created.handle.handle_id)
        .await
        .expect("unchanged handle");
    assert_eq!(unchanged, created.handle);
}

#[tokio::test]
async fn corrupt_handle_decode_never_echoes_a_review_token_value() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, ctx) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct corrupt handle decode service");
    let now = instant(1_784_001_325);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create corrupt handle decode fixture");
    let token = created.review_token.expose().to_string();
    let mut value = serde_json::to_value(&created.handle).expect("encode handle value");
    value["status"] = serde_json::Value::String(token.clone());
    let bytes = arco_core::canonical_json::to_canonical_bytes(&value)
        .expect("canonical corrupt handle bytes");
    let path = ControlPlaneTxPaths::handle_record(&created.handle.handle_id).expect("handle path");
    ctx.scoped_storage(backend)
        .expect("scoped storage")
        .put_raw(&path, Bytes::from(bytes), WritePrecondition::None)
        .await
        .expect("write canonical corrupt handle");

    let error = handles
        .prepare_handle(
            &created.handle.handle_id,
            &token,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("corrupt status must fail closed");
    assert!(!error.message().contains(&token));
}

#[tokio::test]
async fn malformed_review_token_cannot_match_a_forged_empty_candidate_verifier() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, ctx) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct malformed-token handle service");
    let now = instant(1_784_001_350);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create malformed-token handle");
    rewrite_handle(backend.clone(), &created.handle.handle_id, |record| {
        record.review_token_verifier = format!("sha256:{:x}", Sha256::digest(b""));
    })
    .await;
    let mutation_path =
        ControlPlaneTxPaths::handle_mutation(&created.handle.handle_id, 1).expect("mutation path");

    let error = handles
        .stage_catalog(
            &created.handle.handle_id,
            "",
            1,
            create_catalog("malformed_review_token"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("empty candidate must fail wire validation before verifier matching");

    assert_eq!(error.code(), "FORBIDDEN");
    assert!(
        ctx.scoped_storage(backend)
            .expect("scoped storage")
            .head_raw(&mutation_path)
            .await
            .expect("head forbidden mutation")
            .is_none()
    );
    assert!(
        handles
            .get_handle(&created.handle.handle_id)
            .await
            .expect("unchanged malformed-token handle")
            .mutation_refs
            .is_empty()
    );
}

#[tokio::test]
async fn invalid_persisted_orchestration_partition_key_never_reflects_review_token() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct partition-redaction handle service");
    let now = instant(1_784_001_375);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create partition-redaction handle");
    handles
        .stage_orchestration(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            task_finished_batch(&format!("arco-flow/{TENANT}/{WORKSPACE}"), None),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage valid orchestration event");
    rewrite_staged_and_rebind(erased, &created.handle.handle_id, 1, |wire| {
        wire["mutation"]["events"][0]["data"]["partition_key"] =
            serde_json::json!(created.review_token.expose());
    })
    .await;

    let error = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect_err("invalid persisted partition key must fail safe validation");

    assert!(!error.message().contains(created.review_token.expose()));
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn incoming_malformed_orchestration_partition_never_reflects_or_writes_review_token() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct incoming partition-redaction service");
    let now = instant(1_784_001_380);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create incoming partition-redaction handle");
    let mut batch = task_finished_batch(&format!("arco-flow/{TENANT}/{WORKSPACE}"), None);
    let Some(orchestration_event_envelope::Event::TaskFinished(event)) =
        batch.events[0].event.as_mut()
    else {
        panic!("task-finished fixture");
    };
    event.partition_key = Some(PartitionKey {
        dimensions: vec![PartitionDimension {
            name: created.review_token.expose().to_string(),
            value: None,
        }],
    });

    let error = handles
        .stage_orchestration(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            batch,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("malformed incoming partition must fail before staging");

    assert_eq!(error.message(), "orchestration handle mutation is invalid");
    assert!(!error.message().contains(created.review_token.expose()));
    let storage = ctx.scoped_storage(erased).expect("scoped storage");
    let mutation_path =
        ControlPlaneTxPaths::handle_mutation(&created.handle.handle_id, 1).expect("mutation path");
    let authority_path = format!(
        "transactions/handles/{}/identities/{:020}.json",
        created.handle.handle_id, 1
    );
    assert!(
        storage
            .head_raw(&mutation_path)
            .await
            .expect("head forbidden staged mutation")
            .is_none()
    );
    assert!(
        storage
            .head_raw(&authority_path)
            .await
            .expect("head forbidden identity authority")
            .is_none()
    );
    assert!(
        handles
            .get_handle(&created.handle.handle_id)
            .await
            .expect("unchanged incoming partition handle")
            .mutation_refs
            .is_empty()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn direct_handle_rejects_duplicate_orchestration_event_paths_before_staging() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct duplicate-event handle service");
    let now = instant(1_784_001_385);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create duplicate-event handle");
    let mut batch = orchestration_batch(
        "duplicate-direct-orchestration-event",
        &ulid::Ulid::new().to_string(),
    );
    batch.events.push(batch.events[0].clone());

    let error = handles
        .stage_orchestration(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            batch,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("duplicate event path must fail before direct staging");

    assert_eq!(
        error.message(),
        "duplicate orchestration event path is unsupported"
    );
    let mutation_path =
        ControlPlaneTxPaths::handle_mutation(&created.handle.handle_id, 1).expect("mutation path");
    assert!(
        ctx.scoped_storage(erased)
            .expect("scoped storage")
            .head_raw(&mutation_path)
            .await
            .expect("head rejected direct mutation")
            .is_none()
    );
    assert!(
        handles
            .get_handle(&created.handle.handle_id)
            .await
            .expect("unchanged direct handle")
            .mutation_refs
            .is_empty()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn root_handle_rejects_duplicate_orchestration_event_paths_before_staging() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct duplicate-event root handle service");
    let now = instant(1_784_001_390);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create duplicate-event root handle");
    let mut batch = orchestration_batch(
        "duplicate-root-orchestration-event",
        &ulid::Ulid::new().to_string(),
    );
    batch.events.push(batch.events[0].clone());
    let Some(orchestration_event_envelope::Event::RunTriggered(event)) =
        batch.events[1].event.as_mut()
    else {
        panic!("run-triggered fixture");
    };
    event.plan_id = "divergent-duplicate-plan".to_string();

    let error = handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![DomainMutation {
                kind: Some(domain_mutation::Kind::Orchestration(batch)),
            }],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("duplicate event path must fail before root staging");

    assert_eq!(
        error.message(),
        "duplicate orchestration event path is unsupported"
    );
    let mutation_path =
        ControlPlaneTxPaths::handle_mutation(&created.handle.handle_id, 1).expect("mutation path");
    assert!(
        ctx.scoped_storage(erased)
            .expect("scoped storage")
            .head_raw(&mutation_path)
            .await
            .expect("head rejected root mutation")
            .is_none()
    );
    assert!(
        handles
            .get_handle(&created.handle.handle_id)
            .await
            .expect("unchanged root handle")
            .mutation_refs
            .is_empty()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn staged_v1_wire_accepts_additive_fields_and_rejects_versions_and_opaque_tags() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct handle service");
    let now = instant(1_784_001_400);

    let additive = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create additive handle");
    handles
        .stage_catalog(
            &additive.handle.handle_id,
            additive.review_token.expose(),
            1,
            create_catalog("additive_wire"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage additive handle");
    rewrite_staged_and_rebind(erased.clone(), &additive.handle.handle_id, 1, |wire| {
        wire.as_object_mut().expect("staged object").insert(
            "future_field".to_string(),
            serde_json::json!({ "safe": true }),
        );
        wire["mutation"]["operation"]
            .as_object_mut()
            .expect("catalog operation object")
            .insert(
                "future_operation_field".to_string(),
                serde_json::json!({ "safe": true }),
            );
    })
    .await;
    assert_eq!(
        handles
            .prepare_handle(
                &additive.handle.handle_id,
                additive.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("additive v1 field remains compatible")
            .status,
        ControlPlaneHandleStatus::Prepared
    );
    assert_eq!(
        handles
            .commit_handle(
                &additive.handle.handle_id,
                additive.review_token.expose(),
                now + chrono::Duration::seconds(3),
            )
            .await
            .expect("nested additive v1 operation field remains executable")
            .status,
        ControlPlaneHandleStatus::Visible
    );
    assert_eq!(
        handles
            .recover_handle(
                &additive.handle.handle_id,
                additive.review_token.expose(),
                now + chrono::Duration::seconds(4),
            )
            .await
            .expect("nested additive v1 operation field remains recoverable")
            .status,
        ControlPlaneHandleStatus::Visible
    );

    let version = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create version handle");
    handles
        .stage_catalog(
            &version.handle.handle_id,
            version.review_token.expose(),
            1,
            create_catalog("version_wire"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage version handle");
    rewrite_staged_and_rebind(erased.clone(), &version.handle.handle_id, 1, |wire| {
        wire["version"] = serde_json::json!(2);
    })
    .await;
    let version_error = handles
        .prepare_handle(
            &version.handle.handle_id,
            version.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect_err("unsupported staged version must fail closed");
    assert!(version_error.message().contains("unsupported version"));
    assert_eq!(
        handles
            .get_handle(&version.handle.handle_id)
            .await
            .expect("version handle")
            .status,
        ControlPlaneHandleStatus::Preparing
    );

    let invalid_contract = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create persisted-contract handle");
    handles
        .stage_catalog(
            &invalid_contract.handle.handle_id,
            invalid_contract.review_token.expose(),
            1,
            create_catalog("persisted_contract"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage persisted-contract handle");
    rewrite_staged_and_rebind(
        erased.clone(),
        &invalid_contract.handle.handle_id,
        1,
        |wire| {
            wire["mutation"]["operation"]["catalog"] = serde_json::json!("");
        },
    )
    .await;
    handles
        .prepare_handle(
            &invalid_contract.handle.handle_id,
            invalid_contract.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect_err("persisted invalid DDL must fail revalidation before prepare");

    let opaque = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create opaque handle");
    handles
        .stage_catalog(
            &opaque.handle.handle_id,
            opaque.review_token.expose(),
            1,
            create_catalog("opaque_wire"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage opaque handle");
    rewrite_staged_and_rebind(erased, &opaque.handle.handle_id, 1, |wire| {
        wire["mutation"]["mutation_type"] = serde_json::json!("opaque_payload");
        wire["mutation"]["protoHex"] = serde_json::json!("736563726574");
    })
    .await;
    let opaque_error = handles
        .prepare_handle(
            &opaque.handle.handle_id,
            opaque.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect_err("opaque staged tag must fail closed");
    assert!(opaque_error.message().contains("corrupt"));
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn staged_v1_nested_additive_column_fields_remain_executable_and_recoverable() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct additive-column handle service");
    let now = instant(1_784_001_405);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create additive-column handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_schema("default", "default"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage required default schema");
    let mut operation = register_table("warehouse/additive-column");
    let Some(catalog_ddl_operation::Op::RegisterTable(register)) = operation.op.as_mut() else {
        panic!("register-table fixture");
    };
    register.table = "additive_column".to_string();
    register.columns.push(ProtoColumnDefinition {
        name: "id".to_string(),
        data_type: "INT64".to_string(),
        is_nullable: false,
        ordinal: 0,
        description: Some("stable known column".to_string()),
    });
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            2,
            operation,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage additive-column mutation");
    rewrite_staged_and_rebind(erased, &created.handle.handle_id, 2, |wire| {
        wire["mutation"]["operation"]["columns"][0]
            .as_object_mut()
            .expect("staged column object")
            .insert(
                "future_column_field".to_string(),
                serde_json::json!({ "safe": true }),
            );
    })
    .await;
    assert_eq!(
        handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare nested additive column")
            .status,
        ControlPlaneHandleStatus::Prepared
    );
    assert_eq!(
        handles
            .commit_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(3),
            )
            .await
            .expect("commit nested additive column")
            .status,
        ControlPlaneHandleStatus::Visible
    );
    assert_eq!(
        handles
            .recover_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(4),
            )
            .await
            .expect("recover nested additive column")
            .status,
        ControlPlaneHandleStatus::Visible
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn visible_repair_pending_divergence_converges_true_in_both_write_orders() {
    for (case, stored_pending, cached_pending) in [
        ("marker_first", false, true),
        ("record_fallback", true, false),
    ] {
        let backend = Arc::new(NoListFaultBackend::new());
        let erased: Arc<dyn StorageBackend> = backend.clone();
        let (state, ctx) = service(erased.clone());
        let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
            .expect("construct repair-join handle service");
        let now = instant(1_784_001_500);
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create repair-join handle");
        handles
            .stage_orchestration(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                orchestration_batch(
                    &format!("run-repair-join-{case}"),
                    &ulid::Ulid::new().to_string(),
                ),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect("stage repair-join orchestration");
        let prepared = handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare repair-join handle");
        let committed = handles
            .commit_handle(
                &prepared.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(3),
            )
            .await
            .expect("publish complete orchestration authority fixture");
        let participant = &committed.participants[0];
        let tx_id = participant
            .tx_id
            .clone()
            .expect("visible orchestration transaction ID");
        let marker_path = ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Orchestration,
            &participant.idempotency_key,
        );
        let record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Orchestration, &tx_id);
        let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
        let mut marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
            storage
                .get_raw(&marker_path)
                .await
                .expect("read successful orchestration marker")
                .as_ref(),
        )
        .expect("decode successful orchestration marker");
        let base: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
            storage
                .get_raw(&record_path)
                .await
                .expect("read successful orchestration record")
                .as_ref(),
        )
        .expect("decode successful orchestration record");
        let mut stored = base.clone();
        stored.repair_pending = stored_pending;
        let mut cached = base;
        cached.repair_pending = cached_pending;
        marker.tx_record = Some(serde_json::to_value(&cached).expect("cached record"));
        let marker_bytes = Bytes::from(serde_json::to_vec(&marker).expect("encode marker"));
        let record_bytes = Bytes::from(serde_json::to_vec(&stored).expect("encode record"));
        let writes = if case == "marker_first" {
            vec![
                (marker_path.as_str(), marker_bytes),
                (record_path.as_str(), record_bytes),
            ]
        } else {
            vec![
                (record_path.as_str(), record_bytes),
                (marker_path.as_str(), marker_bytes),
            ]
        };
        for (path, bytes) in writes {
            let result = storage
                .put_raw(path, bytes, WritePrecondition::None)
                .await
                .expect("write repair-pending divergence");
            assert!(matches!(result, WriteResult::Success { .. }));
        }
        let recovering = rewrite_handle(erased.clone(), &committed.handle_id, |record| {
            record.status = ControlPlaneHandleStatus::Committing;
            record.revision += 1;
            record.updated_at += chrono::Duration::seconds(1);
            record.visible_at = None;
        })
        .await;

        let visible = handles
            .recover_handle(
                &recovering.handle_id,
                created.review_token.expose(),
                recovering.updated_at + chrono::Duration::seconds(1),
            )
            .await
            .expect("repair-pending copies converge before adoption");
        assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);

        let scoped = request_context()
            .scoped_storage(erased)
            .expect("scoped storage");
        let joined_marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
            scoped
                .get_raw(&marker_path)
                .await
                .expect("joined marker")
                .as_ref(),
        )
        .expect("decode joined marker");
        let joined_cached: ControlPlaneTxRecord<serde_json::Value> =
            serde_json::from_value(joined_marker.tx_record.expect("joined cached record"))
                .expect("decode joined cached record");
        let joined_stored: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
            scoped
                .get_raw(&record_path)
                .await
                .expect("joined exact record")
                .as_ref(),
        )
        .expect("decode joined exact record");
        assert_eq!(joined_cached, joined_stored);
        assert!(joined_stored.repair_pending);
        assert_eq!(backend.list_calls(), 0);
    }
}

#[tokio::test]
async fn frozen_participant_with_missing_marker_never_allocates_a_replacement_id() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct missing-marker handle service");
    let now = instant(1_784_001_510);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create missing-marker handle");
    let operation = create_catalog("missing_frozen_marker");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            operation,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage missing-marker handle");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare missing-marker handle");
    force_committing(
        erased.clone(),
        &prepared.handle_id,
        now + chrono::Duration::seconds(3),
    )
    .await;
    let frozen_tx_id = ulid::Ulid::new().to_string();
    let repair = rewrite_handle(erased.clone(), &prepared.handle_id, |record| {
        record.status = ControlPlaneHandleStatus::RepairRequired;
        record.revision += 1;
        record.updated_at = now + chrono::Duration::seconds(4);
        record.failure_category = Some(ControlPlaneHandleFailureCategory::ParticipantAborted);
        record.participants[0].tx_id = Some(frozen_tx_id.clone());
        record.participants[0].low_level_status = Some(ControlPlaneTxStatus::Aborted);
    })
    .await;
    let participant = &repair.participants[0];
    let exact_path = ControlPlaneTxPaths::record(participant.domain, &frozen_tx_id);
    put_json(
        erased.clone(),
        &exact_path,
        &ControlPlaneTxRecord::<serde_json::Value> {
            tx_id: frozen_tx_id.clone(),
            kind: participant.kind,
            status: ControlPlaneTxStatus::Aborted,
            repair_pending: false,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            lock_path: "locks/catalog.lock.json".to_string(),
            fencing_token: 0,
            prepared_at: now,
            visible_at: None,
            durable_append: None,
            result: None,
        },
    )
    .await;
    let marker_path =
        ControlPlaneTxPaths::idempotency(participant.domain, &participant.idempotency_key);

    handles
        .recover_handle(
            &repair.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect_err("missing frozen marker must fail closed");
    let after = handles
        .get_handle(&repair.handle_id)
        .await
        .expect("repair handle remains");
    assert_eq!(after.status, ControlPlaneHandleStatus::RepairRequired);
    assert_eq!(
        after.participants[0].tx_id.as_deref(),
        Some(frozen_tx_id.as_str())
    );
    let scoped = request_context()
        .scoped_storage(erased)
        .expect("scoped storage");
    assert!(
        scoped
            .head_raw(&marker_path)
            .await
            .expect("head marker")
            .is_none()
    );
    let exact_after: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        scoped
            .get_raw(&exact_path)
            .await
            .expect("frozen exact record")
            .as_ref(),
    )
    .expect("decode frozen exact record");
    assert_eq!(exact_after.tx_id, frozen_tx_id);
    assert_eq!(exact_after.status, ControlPlaneTxStatus::Aborted);
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn exact_aborted_root_without_child_claim_recovers_in_place() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct aborted-root handle service");
    let now = instant(1_784_001_520);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create aborted-root handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![catalog_domain_mutation("root_aborted_before_child")],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage aborted-root handle");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare aborted-root handle");
    force_committing(
        erased.clone(),
        &prepared.handle_id,
        now + chrono::Duration::seconds(3),
    )
    .await;
    let participant = &prepared.participants[0];
    let root_tx_id = ulid::Ulid::new().to_string();
    let marker = ControlPlaneIdempotencyRecord {
        tx_id: root_tx_id.clone(),
        kind: participant.kind,
        request_id: participant.request_id.clone(),
        idempotency_key: participant.idempotency_key.clone(),
        request_hash: participant.request_hash.clone(),
        created_at: now,
        visible_at: None,
        tx_record: None,
    };
    put_json(
        erased.clone(),
        &ControlPlaneTxPaths::idempotency(participant.domain, &participant.idempotency_key),
        &marker,
    )
    .await;
    put_json(
        erased.clone(),
        &ControlPlaneTxPaths::record(participant.domain, &root_tx_id),
        &ControlPlaneTxRecord::<serde_json::Value> {
            tx_id: root_tx_id.clone(),
            kind: participant.kind,
            status: ControlPlaneTxStatus::Aborted,
            repair_pending: false,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            lock_path: ControlPlaneTxPaths::root_lock(),
            fencing_token: 0,
            prepared_at: now,
            visible_at: None,
            durable_append: None,
            result: None,
        },
    )
    .await;

    let visible = handles
        .recover_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect("aborted root reuses its exact transaction ID");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(
        visible.participants[0].tx_id.as_deref(),
        Some(root_tx_id.as_str())
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn handle_shaped_public_key_keeps_legacy_aborted_retry_behavior() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let now = instant(1_784_001_530);
    let identity = format!("handle:hdl_{}:mutation:{:020}", ulid::Ulid::new(), 1);
    let operation = create_catalog("legacy_handle_shaped_key");
    let request_hash = catalog_request_hash(&operation);
    let old_tx_id = ulid::Ulid::new().to_string();
    let marker_path = ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Catalog, &identity);
    put_json(
        erased.clone(),
        &marker_path,
        &ControlPlaneIdempotencyRecord {
            tx_id: old_tx_id.clone(),
            kind: ControlPlaneTxKind::CatalogDdl,
            request_id: identity.clone(),
            idempotency_key: identity.clone(),
            request_hash: request_hash.clone(),
            created_at: now,
            visible_at: None,
            tx_record: None,
        },
    )
    .await;
    put_json(
        erased.clone(),
        &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &old_tx_id),
        &ControlPlaneTxRecord::<serde_json::Value> {
            tx_id: old_tx_id.clone(),
            kind: ControlPlaneTxKind::CatalogDdl,
            status: ControlPlaneTxStatus::Aborted,
            repair_pending: false,
            request_id: identity.clone(),
            idempotency_key: identity.clone(),
            request_hash,
            lock_path: "locks/catalog.lock.json".to_string(),
            fencing_token: 0,
            prepared_at: now,
            visible_at: None,
            durable_append: None,
            result: None,
        },
    )
    .await;
    let mut context = request_context();
    context.request_id = identity.clone();
    context.idempotency_key = Some(identity);
    let response = ControlPlaneTransactionService::new(&state, context)
        .expect("construct legacy public service")
        .apply_catalog_ddl(ApplyCatalogDdlRequest {
            ddl: Some(operation),
        })
        .await
        .expect("legacy retry replaces its aborted attempt");
    let receipt = response.receipt.expect("legacy retry receipt");
    assert_ne!(receipt.tx_id, old_tx_id);
    let scoped = request_context()
        .scoped_storage(erased)
        .expect("scoped storage");
    let after: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        scoped
            .get_raw(&marker_path)
            .await
            .expect("legacy replacement marker")
            .as_ref(),
    )
    .expect("decode legacy replacement marker");
    assert_eq!(after.tx_id, receipt.tx_id);
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn frozen_root_handle_wrapper_never_replaces_an_aborted_claim() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased.clone());
    let now = instant(1_784_001_535);
    let identity = format!("handle:hdl_{}:mutation:{:020}", ulid::Ulid::new(), 1);
    let request = arco_proto::arco::controlplane::v1::CommitRootTransactionRequest {
        mutations: vec![catalog_domain_mutation("frozen_root_claim")],
    };
    let meta = super::ResolvedRequestMetadata {
        tenant: TENANT.to_string(),
        workspace: WORKSPACE.to_string(),
        request_id: identity.clone(),
        idempotency_key: identity.clone(),
    };
    let mutations = request
        .mutations
        .iter()
        .map(super::RootMutation::from_proto)
        .collect::<Result<Vec<_>, _>>()
        .expect("decode frozen root mutations");
    let request_hash = super::root_request_hash(&mutations, &meta).expect("frozen root hash");
    let old_tx_id = ulid::Ulid::new().to_string();
    let marker_path = ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Root, &identity);
    put_json(
        erased.clone(),
        &marker_path,
        &ControlPlaneIdempotencyRecord {
            tx_id: old_tx_id.clone(),
            kind: ControlPlaneTxKind::RootCommit,
            request_id: identity.clone(),
            idempotency_key: identity.clone(),
            request_hash: request_hash.clone(),
            created_at: now,
            visible_at: None,
            tx_record: None,
        },
    )
    .await;
    put_json(
        erased.clone(),
        &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, &old_tx_id),
        &ControlPlaneTxRecord::<serde_json::Value> {
            tx_id: old_tx_id.clone(),
            kind: ControlPlaneTxKind::RootCommit,
            status: ControlPlaneTxStatus::Aborted,
            repair_pending: false,
            request_id: identity.clone(),
            idempotency_key: identity.clone(),
            request_hash,
            lock_path: ControlPlaneTxPaths::root_lock(),
            fencing_token: 0,
            prepared_at: now,
            visible_at: None,
            durable_append: None,
            result: None,
        },
    )
    .await;
    let mut context = request_context();
    context.request_id = identity.clone();
    context.idempotency_key = Some(identity);
    ControlPlaneTransactionService::new(&state, context)
        .expect("construct frozen root service")
        .commit_root_transaction_for_handle(request)
        .await
        .expect_err("frozen root claim cannot allocate a replacement transaction ID");
    let scoped = request_context()
        .scoped_storage(erased)
        .expect("scoped storage");
    let after: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        scoped
            .get_raw(&marker_path)
            .await
            .expect("frozen root marker")
            .as_ref(),
    )
    .expect("decode frozen root marker");
    assert_eq!(after.tx_id, old_tx_id);
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn post_lock_root_visibility_must_match_the_frozen_request_hash() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend.clone());
    let service = ControlPlaneTransactionService::new(&state, request_context())
        .expect("construct post-lock root service");
    let now = instant(1_784_001_540);
    let tx_id = ulid::Ulid::new().to_string();
    let identity = "handle:hdl_01KXHASHBOUND0000000000000:mutation:00000000000000000001";
    let expected_hash = format!("sha256:{}", "1".repeat(64));
    let replacement_hash = format!("sha256:{}", "2".repeat(64));
    let receipt = RootTxReceipt {
        tx_id: tx_id.clone(),
        root_commit_id: ulid::Ulid::new().to_string(),
        super_manifest_path: ControlPlaneTxPaths::root_super_manifest(&tx_id),
        domain_commits: Vec::new(),
        read_token: format!("root:{tx_id}"),
        visible_at: now,
    };
    let record = ControlPlaneTxRecord::<RootTxReceipt> {
        tx_id: tx_id.clone(),
        kind: ControlPlaneTxKind::RootCommit,
        status: ControlPlaneTxStatus::Visible,
        repair_pending: false,
        request_id: identity.to_string(),
        idempotency_key: identity.to_string(),
        request_hash: replacement_hash.clone(),
        lock_path: ControlPlaneTxPaths::root_lock(),
        fencing_token: 1,
        prepared_at: now,
        visible_at: Some(now),
        durable_append: None,
        result: Some(receipt),
    };
    let marker_path = ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Root, identity);
    put_json(
        backend.clone(),
        &marker_path,
        &ControlPlaneIdempotencyRecord {
            tx_id: tx_id.clone(),
            kind: ControlPlaneTxKind::RootCommit,
            request_id: identity.to_string(),
            idempotency_key: identity.to_string(),
            request_hash: replacement_hash,
            created_at: now,
            visible_at: Some(now),
            tx_record: Some(serde_json::to_value(&record).expect("cached root record")),
        },
    )
    .await;
    put_json(
        backend,
        &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, &tx_id),
        &record,
    )
    .await;
    let meta = super::ResolvedRequestMetadata {
        tenant: TENANT.to_string(),
        workspace: WORKSPACE.to_string(),
        request_id: identity.to_string(),
        idempotency_key: identity.to_string(),
    };

    service
        .resolve_visible_root_recovery(
            &meta,
            &tx_id,
            &marker_path,
            &expected_hash,
            super::VisibleMarkerPolicy::DeferredForHandleValidation,
        )
        .await
        .expect_err("post-lock adoption must remain bound to the frozen request hash");
}

#[tokio::test]
async fn executor_response_is_reinspected_before_corrupt_audit_can_publish_handle_visible() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased);
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct corrupt-response handle service");
    let now = instant(1_784_001_550);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create corrupt-response handle");
    handles
        .stage_orchestration(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            orchestration_batch("run-corrupt-audit", &ulid::Ulid::new().to_string()),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage corrupt-response orchestration");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare corrupt-response handle");
    backend.corrupt_next_matching_put("commits/orchestration/");

    handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("corrupt audit bytes must block handle visibility immediately");
    assert_eq!(
        handles
            .get_handle(&created.handle.handle_id)
            .await
            .expect("repair-required corrupt-response handle")
            .status,
        ControlPlaneHandleStatus::RepairRequired
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn real_handle_does_not_reserve_an_unstaged_legacy_ordinal() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct exact ownership service");
    let now = instant(1_784_001_600);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create exact ownership handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("owned_ordinal_one"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage only ordinal one");

    let identity = format!("handle:{}:mutation:{:020}", created.handle.handle_id, 2);
    let operation = create_catalog("legacy_unstaged_ordinal");
    let old_tx_id = seed_aborted_catalog_claim(backend, &identity, &operation, now).await;
    let mut context = request_context();
    context.request_id.clone_from(&identity);
    context.idempotency_key = Some(identity);
    let response = ControlPlaneTransactionService::new(&state, context)
        .expect("construct unstaged legacy service")
        .apply_catalog_ddl(ApplyCatalogDdlRequest {
            ddl: Some(operation),
        })
        .await
        .expect("unstaged ordinal keeps legacy aborted-retry behavior");
    assert_ne!(
        response.receipt.expect("replacement receipt").tx_id,
        old_tx_id
    );
}

#[tokio::test]
async fn root_handle_does_not_reserve_a_child_domain_it_did_not_stage() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct root ownership service");
    let now = instant(1_784_001_610);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create root ownership handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![DomainMutation {
                kind: Some(domain_mutation::Kind::Orchestration(orchestration_batch(
                    "run-only-orchestration-child",
                    &ulid::Ulid::new().to_string(),
                ))),
            }],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage orchestration-only root");

    let identity = format!(
        "root:handle:{}:mutation:{:020}:catalog",
        created.handle.handle_id, 1
    );
    let operation = create_catalog("legacy_absent_root_child");
    let old_tx_id = seed_aborted_catalog_claim(backend, &identity, &operation, now).await;
    let mut context = request_context();
    context.request_id.clone_from(&identity);
    context.idempotency_key = Some(identity);
    let response = ControlPlaneTransactionService::new(&state, context)
        .expect("construct absent-child legacy service")
        .apply_catalog_ddl(ApplyCatalogDdlRequest {
            ddl: Some(operation),
        })
        .await
        .expect("unstaged root child keeps legacy aborted-retry behavior");
    assert_ne!(
        response.receipt.expect("replacement receipt").tx_id,
        old_tx_id
    );
}

#[tokio::test]
async fn staging_refuses_an_ordinal_already_claimed_by_legacy_execution() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct stage-claim serialization service");
    let now = instant(1_784_001_615);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create stage-claim handle");
    let identity = format!("handle:{}:mutation:{:020}", created.handle.handle_id, 1);
    let operation = create_catalog("legacy_claim_before_stage");
    seed_aborted_catalog_claim(backend, &identity, &operation, now).await;

    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            operation,
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("staging must not take an identity already claimed by legacy execution");
    assert!(
        handles
            .get_handle(&created.handle.handle_id)
            .await
            .expect("read unmodified stage-claim handle")
            .mutation_refs
            .is_empty()
    );
}

#[tokio::test]
async fn persisted_handle_intent_survives_stage_crash_and_service_reconstruction() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let now = instant(1_784_001_616);
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct durable-intent service");
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create durable-intent handle");
    let handle_id = created.handle.handle_id.clone();
    let review_token = created.review_token.expose().to_string();
    let handle_path = ControlPlaneTxPaths::handle_record(&handle_id).expect("handle path");
    let operation = create_catalog("durable_intent_after_crash");
    backend.fail_next_matching_put(&handle_path, 0);

    handles
        .stage_catalog(
            &handle_id,
            &review_token,
            1,
            operation.clone(),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("handle-reference CAS failure simulates a crash after durable intent");
    assert!(
        handles
            .get_handle(&handle_id)
            .await
            .expect("read handle after interrupted stage")
            .mutation_refs
            .is_empty(),
        "the interrupted stage must stop before its handle reference is visible"
    );

    let rebuilt_handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("reconstruct handle service after interrupted stage");
    let identity = format!("handle:{handle_id}:mutation:{:020}", 1);
    let mut legacy_ctx = ctx.clone();
    legacy_ctx.request_id.clone_from(&identity);
    legacy_ctx.idempotency_key = Some(identity.clone());
    ControlPlaneTransactionService::new(&state, legacy_ctx)
        .expect("reconstruct legacy service after interrupted stage")
        .apply_catalog_ddl(ApplyCatalogDdlRequest {
            ddl: Some(operation.clone()),
        })
        .await
        .expect_err("persisted handle intent must outlive the staging service and block legacy");

    let staged = rebuilt_handles
        .stage_catalog(
            &handle_id,
            &review_token,
            1,
            operation,
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("identical retry completes the interrupted stage");
    assert_eq!(staged.mutation_refs.len(), 1);

    let authority_path = format!("transactions/handles/{handle_id}/identities/{:020}.json", 1);
    let authority: serde_json::Value = serde_json::from_slice(
        ctx.scoped_storage(erased.clone())
            .expect("scoped storage")
            .get_raw(&authority_path)
            .await
            .expect("get durable identity authority")
            .as_ref(),
    )
    .expect("decode durable identity authority");
    assert!(
        authority
            .get("handle_intent")
            .is_some_and(|value| !value.is_null())
    );
    assert_eq!(
        authority
            .get("legacy_reservations")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len),
        Some(0)
    );
    assert!(
        ctx.scoped_storage(erased)
            .expect("scoped storage")
            .head_raw(&ControlPlaneTxPaths::idempotency(
                ControlPlaneTxDomain::Catalog,
                &identity,
            ))
            .await
            .expect("head losing legacy marker")
            .is_none()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn persisted_handle_intent_never_recreates_missing_staged_bytes() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let now = instant(1_784_001_616);
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct missing-crash-stage service");
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create missing-crash-stage handle");
    let handle_id = created.handle.handle_id.clone();
    let review_token = created.review_token.expose().to_string();
    let handle_path = ControlPlaneTxPaths::handle_record(&handle_id).expect("handle path");
    let staged_path =
        ControlPlaneTxPaths::handle_mutation(&handle_id, 1).expect("canonical staged path");
    let operation = create_catalog("missing_stage_after_intent");
    backend.fail_next_matching_put(&handle_path, 0);
    handles
        .stage_catalog(
            &handle_id,
            &review_token,
            1,
            operation.clone(),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("interrupt stage after durable intent");
    ctx.scoped_storage(erased.clone())
        .expect("scoped storage")
        .delete(&staged_path)
        .await
        .expect("delete staged bytes retained by intent");

    ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("reconstruct missing-crash-stage service")
        .stage_catalog(
            &handle_id,
            &review_token,
            1,
            operation,
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect_err("an existing intent cannot recreate missing staged bytes from caller input");
    assert!(
        ctx.scoped_storage(erased)
            .expect("scoped storage")
            .head_raw(&staged_path)
            .await
            .expect("head missing staged bytes after rejected retry")
            .is_none(),
        "missing staged authority must fail before replacement"
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn canonical_legacy_reservation_can_predate_the_handle_and_blocks_later_stage() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let now = instant(1_784_001_616);
    let handle_id = format!("hdl_{}", ulid::Ulid::new());
    let identity = format!("handle:{handle_id}:mutation:{:020}", 1);
    let authority_path = format!("transactions/handles/{handle_id}/identities/{:020}.json", 1);
    let legacy_service = ControlPlaneTransactionService::new(&state, ctx.clone())
        .expect("construct pre-handle legacy service");

    assert!(
        guard_legacy_handle_identity(
            &legacy_service,
            ControlPlaneTxDomain::Catalog,
            ControlPlaneTxKind::OrchestrationBatch,
            &identity,
        )
        .await
        .expect("wrong domain-kind tuple retains legacy behavior")
        .is_none()
    );
    assert!(
        ctx.scoped_storage(erased.clone())
            .expect("scoped storage")
            .head_raw(&authority_path)
            .await
            .expect("head authority after mismatched tuple")
            .is_none(),
        "syntax alone must not create an authority record"
    );
    assert!(
        guard_legacy_handle_identity(
            &legacy_service,
            ControlPlaneTxDomain::Catalog,
            ControlPlaneTxKind::CatalogDdl,
            &identity,
        )
        .await
        .expect("persist canonical pre-handle legacy reservation")
        .is_some()
    );

    let review_token = "review_prehandle_reservation";
    let scope = ControlPlaneHandleScope::new(TENANT, WORKSPACE).expect("handle scope");
    let handle = ControlPlaneHandleRecord::new(
        handle_id.clone(),
        scope,
        now,
        now + chrono::Duration::minutes(10),
        format!("sha256:{:x}", Sha256::digest(review_token.as_bytes())),
    )
    .expect("construct handle after legacy reservation");
    let handle_path = ControlPlaneTxPaths::handle_record(&handle_id).expect("handle path");
    let write = ctx
        .scoped_storage(erased.clone())
        .expect("scoped storage")
        .put_raw(
            &handle_path,
            Bytes::from(handle.to_json_vec().expect("encode handle")),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("publish handle after legacy reservation");
    assert!(matches!(write, WriteResult::Success { .. }));

    ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct post-reservation handle service")
        .stage_catalog(
            &handle_id,
            review_token,
            1,
            create_catalog("prehandle_legacy_wins"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("durable legacy reservation must block the later staged intent");
    let authority: serde_json::Value = serde_json::from_slice(
        ctx.scoped_storage(erased)
            .expect("scoped storage")
            .get_raw(&authority_path)
            .await
            .expect("get pre-handle authority")
            .as_ref(),
    )
    .expect("decode pre-handle authority");
    assert!(authority.get("handle_intent").is_none());
    assert_eq!(
        authority
            .get("legacy_reservations")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len),
        Some(1)
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn staging_and_legacy_claim_serialize_one_unused_handle_ordinal() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let state = Arc::new(state);
    let handles = ControlPlaneTransactionHandleService::new(state.as_ref(), ctx.clone())
        .expect("construct stage-claim race service");
    let now = instant(1_784_001_617);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create stage-claim race handle");
    let handle_id = created.handle.handle_id.clone();
    let review_token = created.review_token.expose().to_string();
    let operation = create_catalog("stage_wins_identity_race");
    let identity = format!("handle:{handle_id}:mutation:{:020}", 1);
    let handle_path = ControlPlaneTxPaths::handle_record(&handle_id).expect("handle path");
    let (entered, release) = backend.gate_next_matching_put(handle_path);

    let stage_state = state.clone();
    let stage_ctx = ctx.clone();
    let stage_handle_id = handle_id.clone();
    let stage_token = review_token.clone();
    let stage_operation = operation.clone();
    let stage = tokio::spawn(async move {
        ControlPlaneTransactionHandleService::new(stage_state.as_ref(), stage_ctx)
            .expect("construct racing stage service")
            .stage_catalog(
                &stage_handle_id,
                &stage_token,
                1,
                stage_operation,
                now + chrono::Duration::seconds(1),
            )
            .await
    });
    entered.notified().await;

    let legacy_state = state.clone();
    let mut legacy_ctx = ctx.clone();
    legacy_ctx.request_id.clone_from(&identity);
    legacy_ctx.idempotency_key = Some(identity.clone());
    let legacy = tokio::spawn(async move {
        ControlPlaneTransactionService::new(legacy_state.as_ref(), legacy_ctx)
            .expect("construct racing legacy service")
            .apply_catalog_ddl(ApplyCatalogDdlRequest {
                ddl: Some(operation),
            })
            .await
    });
    let authority_path = format!("transactions/handles/{handle_id}/identities/{:020}.json", 1);
    let authority: serde_json::Value = serde_json::from_slice(
        ctx.scoped_storage(erased.clone())
            .expect("scoped storage")
            .get_raw(&authority_path)
            .await
            .expect("get racing identity authority")
            .as_ref(),
    )
    .expect("decode racing identity authority");
    assert!(
        authority
            .get("handle_intent")
            .is_some_and(|value| !value.is_null())
    );
    tokio::time::timeout(Duration::from_secs(5), legacy)
        .await
        .expect("legacy claim resolves from durable intent before handle CAS resumes")
        .expect("join legacy task")
        .expect_err("legacy claim loses after staged ownership becomes durable");
    release.notify_one();
    let staged = stage
        .await
        .expect("join stage task")
        .expect("staging wins the serialized identity");
    assert_eq!(staged.mutation_refs.len(), 1);
    assert!(
        ctx.scoped_storage(erased)
            .expect("scoped storage")
            .head_raw(&ControlPlaneTxPaths::idempotency(
                ControlPlaneTxDomain::Catalog,
                &identity,
            ))
            .await
            .expect("head losing legacy marker")
            .is_none()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn referenced_handle_identity_with_missing_authority_fails_closed() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct missing-authority service");
    let now = instant(1_784_001_618);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create missing-authority handle");
    let operation = create_catalog("missing_identity_authority");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            operation.clone(),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage before deleting authority");
    let authority_path = format!(
        "transactions/handles/{}/identities/{:020}.json",
        created.handle.handle_id, 1
    );
    ctx.scoped_storage(erased.clone())
        .expect("scoped storage")
        .delete(&authority_path)
        .await
        .expect("delete referenced identity authority");

    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            operation,
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect_err("a referenced mutation cannot recreate missing identity authority");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("prepare must validate every referenced identity authority");
    assert_eq!(
        handles
            .get_handle(&created.handle.handle_id)
            .await
            .expect("read handle after rejected prepare")
            .status,
        ControlPlaneHandleStatus::Preparing,
        "authority corruption must stop before PREPARED or low-level execution"
    );
    assert!(
        ctx.scoped_storage(erased)
            .expect("scoped storage")
            .head_raw(&authority_path)
            .await
            .expect("head missing authority after rejected retry")
            .is_none(),
        "missing authority must fail before any replacement write"
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn referenced_handle_identity_with_overlapping_authority_fails_before_prepared() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct overlapping-authority service");
    let now = instant(1_784_001_619);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create overlapping-authority handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("overlapping_identity_authority"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage before corrupting authority");
    let authority_path = format!(
        "transactions/handles/{}/identities/{:020}.json",
        created.handle.handle_id, 1
    );
    let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
    let mut authority: serde_json::Value = serde_json::from_slice(
        storage
            .get_raw(&authority_path)
            .await
            .expect("read identity authority")
            .as_ref(),
    )
    .expect("decode identity authority");
    let claim = authority["handle_intent"]["claim_identities"][0].clone();
    let idempotency_key = claim["idempotency_key"]
        .as_str()
        .expect("claim idempotency key")
        .to_string();
    authority["legacy_reservations"] = serde_json::json!([claim]);
    storage
        .put_raw(
            &authority_path,
            Bytes::from(serde_json::to_vec(&authority).expect("encode corrupt authority")),
            WritePrecondition::None,
        )
        .await
        .expect("write overlapping authority");

    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect_err("overlapping owners must invalidate lifecycle authority");
    assert_eq!(
        handles
            .get_handle(&created.handle.handle_id)
            .await
            .expect("read handle after corrupt authority")
            .status,
        ControlPlaneHandleStatus::Preparing
    );
    assert!(
        storage
            .head_raw(&ControlPlaneTxPaths::idempotency(
                ControlPlaneTxDomain::Catalog,
                &idempotency_key,
            ))
            .await
            .expect("head low-level marker after corrupt authority")
            .is_none()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn referenced_missing_staged_identity_fails_closed_before_legacy_claim() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct missing-stage ownership service");
    let now = instant(1_784_001_620);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create missing-stage handle");
    let operation = create_catalog("missing_owned_stage");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            operation.clone(),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage owned mutation");
    let identity = format!("handle:{}:mutation:{:020}", created.handle.handle_id, 1);
    let staged_path = ControlPlaneTxPaths::handle_mutation(&created.handle.handle_id, 1)
        .expect("canonical staged path");
    ctx.scoped_storage(erased.clone())
        .expect("scoped storage")
        .delete(&staged_path)
        .await
        .expect("delete referenced stage");

    let mut context = ctx;
    context.request_id.clone_from(&identity);
    context.idempotency_key = Some(identity.clone());
    let error = ControlPlaneTransactionService::new(&state, context)
        .expect("construct missing-stage legacy service")
        .apply_catalog_ddl(ApplyCatalogDdlRequest {
            ddl: Some(operation),
        })
        .await
        .expect_err("missing referenced stage must fail as corrupt authority");
    assert!(error.message().contains("missing"));
    assert!(
        request_context()
            .scoped_storage(erased)
            .expect("scoped storage")
            .head_raw(&ControlPlaneTxPaths::idempotency(
                ControlPlaneTxDomain::Catalog,
                &identity,
            ))
            .await
            .expect("head forbidden legacy marker")
            .is_none()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn referenced_corrupt_staged_identity_fails_closed_before_legacy_claim() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct corrupt-stage ownership service");
    let now = instant(1_784_001_630);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create corrupt-stage handle");
    let operation = create_catalog("corrupt_owned_stage");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            operation.clone(),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage owned mutation");
    let identity = format!("handle:{}:mutation:{:020}", created.handle.handle_id, 1);
    let staged_path = ControlPlaneTxPaths::handle_mutation(&created.handle.handle_id, 1)
        .expect("canonical staged path");
    ctx.scoped_storage(erased.clone())
        .expect("scoped storage")
        .put_raw(
            &staged_path,
            Bytes::from_static(br#"{"corrupt":true}"#),
            WritePrecondition::None,
        )
        .await
        .expect("corrupt referenced stage");

    let mut context = ctx;
    context.request_id.clone_from(&identity);
    context.idempotency_key = Some(identity.clone());
    let error = ControlPlaneTransactionService::new(&state, context)
        .expect("construct corrupt-stage legacy service")
        .apply_catalog_ddl(ApplyCatalogDdlRequest {
            ddl: Some(operation),
        })
        .await
        .expect_err("corrupt referenced stage must fail before claim");
    assert!(error.message().contains("corrupt"));
    assert!(
        request_context()
            .scoped_storage(erased)
            .expect("scoped storage")
            .head_raw(&ControlPlaneTxPaths::idempotency(
                ControlPlaneTxDomain::Catalog,
                &identity,
            ))
            .await
            .expect("head forbidden legacy marker")
            .is_none()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn exact_handle_get_rejects_a_record_copied_beneath_another_handle_path() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, ctx) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct path-bound handle service");
    let now = instant(1_784_001_640);
    let first = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create addressed handle");
    let second = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create copied handle");
    let storage = ctx.scoped_storage(backend).expect("scoped storage");
    let first_path =
        ControlPlaneTxPaths::handle_record(&first.handle.handle_id).expect("first handle path");
    let second_path =
        ControlPlaneTxPaths::handle_record(&second.handle.handle_id).expect("second handle path");
    let second_bytes = storage
        .get_raw(&second_path)
        .await
        .expect("read copied handle");
    storage
        .put_raw(&first_path, second_bytes, WritePrecondition::None)
        .await
        .expect("copy record beneath wrong path");

    handles
        .get_handle(&first.handle.handle_id)
        .await
        .expect_err("path-addressed get must reject another handle record");
}

#[tokio::test]
async fn wrong_path_handle_record_cannot_redirect_a_lifecycle_cas() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, ctx) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct path-bound CAS service");
    let now = instant(1_784_001_650);
    let first = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create addressed CAS handle");
    let second = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create CAS target handle");
    let storage = ctx.scoped_storage(backend).expect("scoped storage");
    let first_path =
        ControlPlaneTxPaths::handle_record(&first.handle.handle_id).expect("first handle path");
    let second_path =
        ControlPlaneTxPaths::handle_record(&second.handle.handle_id).expect("second handle path");
    let second_before = storage
        .get_raw(&second_path)
        .await
        .expect("read CAS target before corruption");
    storage
        .put_raw(&first_path, second_before.clone(), WritePrecondition::None)
        .await
        .expect("copy CAS target beneath wrong path");

    handles
        .abort_handle(
            &first.handle.handle_id,
            second.review_token.expose(),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect_err("wrong-path record must not redirect abort to embedded handle ID");
    assert_eq!(
        storage
            .get_raw(&second_path)
            .await
            .expect("read unchanged CAS target"),
        second_before
    );
}

#[tokio::test]
async fn typed_invalid_cached_catalog_visibility_never_mutates_exact_or_marker() {
    for predecessor in [
        SeededExactPredecessor::Missing,
        SeededExactPredecessor::Prepared,
        SeededExactPredecessor::Aborted,
        SeededExactPredecessor::ExactVisibleMarkerOnly,
    ] {
        let backend = Arc::new(NoListFaultBackend::new());
        let erased: Arc<dyn StorageBackend> = backend.clone();
        let (state, _) = service(erased.clone());
        let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
            .expect("construct catalog prevalidation service");
        let now = instant(1_784_001_655);
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create catalog prevalidation handle");
        handles
            .stage_catalog(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                create_catalog("typed_invalid_cached_catalog"),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect("stage catalog prevalidation mutation");
        let prepared = handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare catalog prevalidation handle");
        force_committing(
            erased.clone(),
            &prepared.handle_id,
            now + chrono::Duration::seconds(3),
        )
        .await;
        let participant = &prepared.participants[0];
        let tx_id = ulid::Ulid::new().to_string();
        let receipt = CatalogTxReceipt {
            tx_id: tx_id.clone(),
            event_id: ulid::Ulid::new().to_string(),
            commit_id: ulid::Ulid::new().to_string(),
            manifest_id: "00000000000000000001".to_string(),
            snapshot_version: 1,
            pointer_version: "typed-invalid-catalog".to_string(),
            read_token: "catalog:00000000000000000001".to_string(),
            visible_at: now,
        };
        let cached = ControlPlaneTxRecord::<serde_json::Value> {
            tx_id: tx_id.clone(),
            kind: participant.kind,
            status: ControlPlaneTxStatus::Visible,
            repair_pending: false,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            lock_path: CatalogPaths::domain_lock(CatalogDomain::Catalog),
            fencing_token: 0,
            prepared_at: now,
            visible_at: Some(now),
            durable_append: None,
            result: Some(serde_json::to_value(&receipt).expect("catalog receipt value")),
        };
        let marker_path = ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Catalog,
            &participant.idempotency_key,
        );
        let record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &tx_id);
        put_json(
            erased.clone(),
            &marker_path,
            &ControlPlaneIdempotencyRecord {
                tx_id,
                kind: participant.kind,
                request_id: participant.request_id.clone(),
                idempotency_key: participant.idempotency_key.clone(),
                request_hash: participant.request_hash.clone(),
                created_at: now,
                visible_at: Some(now),
                tx_record: predecessor
                    .caches_visible_record()
                    .then(|| serde_json::to_value(&cached).expect("cached catalog record")),
            },
        )
        .await;
        if let Some(exact) = predecessor.exact_record(&cached) {
            put_json(erased.clone(), &record_path, &exact).await;
        }
        let marker_before = raw_object_snapshot(erased.clone(), &marker_path).await;
        let exact_before = raw_object_snapshot(erased.clone(), &record_path).await;

        handles
            .recover_handle(
                &prepared.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(4),
            )
            .await
            .expect_err("typed-invalid cached catalog authority must fail before reconciliation");

        assert_eq!(
            raw_object_snapshot(erased.clone(), &marker_path).await,
            marker_before,
            "catalog marker bytes and version must remain unchanged"
        );
        assert_eq!(
            raw_object_snapshot(erased.clone(), &record_path).await,
            exact_before,
            "catalog exact record must remain byte/version-identical or absent"
        );
        assert_ne!(
            handles
                .get_handle(&prepared.handle_id)
                .await
                .expect("catalog handle remains readable")
                .status,
            ControlPlaneHandleStatus::Visible
        );
        assert_eq!(backend.list_calls(), 0);
    }
}

#[tokio::test]
async fn typed_invalid_cached_orchestration_visibility_never_mutates_exact_or_marker() {
    for predecessor in [
        SeededExactPredecessor::Missing,
        SeededExactPredecessor::Prepared,
        SeededExactPredecessor::Aborted,
        SeededExactPredecessor::ExactVisibleMarkerOnly,
    ] {
        let backend = Arc::new(NoListFaultBackend::new());
        let erased: Arc<dyn StorageBackend> = backend.clone();
        let (state, _) = service(erased.clone());
        let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
            .expect("construct orchestration prevalidation service");
        let now = instant(1_784_001_656);
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create orchestration prevalidation handle");
        handles
            .stage_orchestration(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                orchestration_batch(
                    "typed-invalid-cached-orchestration",
                    &ulid::Ulid::new().to_string(),
                ),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect("stage orchestration prevalidation mutation");
        let prepared = handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare orchestration prevalidation handle");
        force_committing(
            erased.clone(),
            &prepared.handle_id,
            now + chrono::Duration::seconds(3),
        )
        .await;
        let participant = &prepared.participants[0];
        let tx_id = ulid::Ulid::new().to_string();
        let receipt = OrchestrationTxReceipt {
            tx_id: tx_id.clone(),
            commit_id: ulid::Ulid::new().to_string(),
            manifest_id: "00000000000000000001".to_string(),
            revision_ulid: ulid::Ulid::new().to_string(),
            delta_id: ulid::Ulid::new().to_string(),
            pointer_version: "typed-invalid-orchestration".to_string(),
            events_processed: 1,
            read_token: "orchestration:00000000000000000001".to_string(),
            visible_at: now,
        };
        let cached = ControlPlaneTxRecord::<serde_json::Value> {
            tx_id: tx_id.clone(),
            kind: participant.kind,
            status: ControlPlaneTxStatus::Visible,
            repair_pending: false,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            lock_path: "locks/orchestration.compaction.lock.json".to_string(),
            fencing_token: 1,
            prepared_at: now,
            visible_at: Some(now),
            durable_append: None,
            result: Some(serde_json::to_value(&receipt).expect("orchestration receipt value")),
        };
        let marker_path = ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Orchestration,
            &participant.idempotency_key,
        );
        let record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Orchestration, &tx_id);
        put_json(
            erased.clone(),
            &marker_path,
            &ControlPlaneIdempotencyRecord {
                tx_id,
                kind: participant.kind,
                request_id: participant.request_id.clone(),
                idempotency_key: participant.idempotency_key.clone(),
                request_hash: participant.request_hash.clone(),
                created_at: now,
                visible_at: Some(now),
                tx_record: predecessor
                    .caches_visible_record()
                    .then(|| serde_json::to_value(&cached).expect("cached orchestration record")),
            },
        )
        .await;
        if let Some(exact) = predecessor.exact_record(&cached) {
            put_json(erased.clone(), &record_path, &exact).await;
        }
        let marker_before = raw_object_snapshot(erased.clone(), &marker_path).await;
        let exact_before = raw_object_snapshot(erased.clone(), &record_path).await;

        handles
            .recover_handle(
                &prepared.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(4),
            )
            .await
            .expect_err(
                "typed-invalid cached orchestration authority must fail before reconciliation",
            );

        assert_eq!(
            raw_object_snapshot(erased.clone(), &marker_path).await,
            marker_before,
            "orchestration marker bytes and version must remain unchanged"
        );
        assert_eq!(
            raw_object_snapshot(erased.clone(), &record_path).await,
            exact_before,
            "orchestration exact record must remain byte/version-identical or absent"
        );
        assert_ne!(
            handles
                .get_handle(&prepared.handle_id)
                .await
                .expect("orchestration handle remains readable")
                .status,
            ControlPlaneHandleStatus::Visible
        );
        assert_eq!(backend.list_calls(), 0);
    }
}

#[tokio::test]
async fn typed_invalid_cached_root_visibility_never_mutates_exact_or_marker() {
    for predecessor in [
        SeededExactPredecessor::Missing,
        SeededExactPredecessor::Prepared,
        SeededExactPredecessor::Aborted,
        SeededExactPredecessor::ExactVisibleMarkerOnly,
    ] {
        let backend = Arc::new(NoListFaultBackend::new());
        let erased: Arc<dyn StorageBackend> = backend.clone();
        let (state, _) = service(erased.clone());
        let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
            .expect("construct root prevalidation service");
        let now = instant(1_784_001_657);
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create root prevalidation handle");
        handles
            .stage_root(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                vec![catalog_domain_mutation("typed_invalid_cached_root")],
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect("stage root prevalidation mutation");
        let prepared = handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare root prevalidation handle");
        force_committing(
            erased.clone(),
            &prepared.handle_id,
            now + chrono::Duration::seconds(3),
        )
        .await;
        let participant = &prepared.participants[0];
        let tx_id = ulid::Ulid::new().to_string();
        let receipt = RootTxReceipt {
            tx_id: tx_id.clone(),
            root_commit_id: ulid::Ulid::new().to_string(),
            super_manifest_path: "transactions/root/not-canonical.json".to_string(),
            domain_commits: Vec::new(),
            read_token: format!("root:{tx_id}"),
            visible_at: now,
        };
        let cached = ControlPlaneTxRecord::<serde_json::Value> {
            tx_id: tx_id.clone(),
            kind: participant.kind,
            status: ControlPlaneTxStatus::Visible,
            repair_pending: false,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            lock_path: ControlPlaneTxPaths::root_lock(),
            fencing_token: 1,
            prepared_at: now,
            visible_at: Some(now),
            durable_append: None,
            result: Some(serde_json::to_value(&receipt).expect("root receipt value")),
        };
        let marker_path = ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Root,
            &participant.idempotency_key,
        );
        let record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, &tx_id);
        put_json(
            erased.clone(),
            &marker_path,
            &ControlPlaneIdempotencyRecord {
                tx_id,
                kind: participant.kind,
                request_id: participant.request_id.clone(),
                idempotency_key: participant.idempotency_key.clone(),
                request_hash: participant.request_hash.clone(),
                created_at: now,
                visible_at: Some(now),
                tx_record: predecessor
                    .caches_visible_record()
                    .then(|| serde_json::to_value(&cached).expect("cached root record")),
            },
        )
        .await;
        if let Some(exact) = predecessor.exact_record(&cached) {
            put_json(erased.clone(), &record_path, &exact).await;
        }
        let marker_before = raw_object_snapshot(erased.clone(), &marker_path).await;
        let exact_before = raw_object_snapshot(erased.clone(), &record_path).await;

        handles
            .recover_handle(
                &prepared.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(4),
            )
            .await
            .expect_err("typed-invalid cached root authority must fail before reconciliation");

        assert_eq!(
            raw_object_snapshot(erased.clone(), &marker_path).await,
            marker_before,
            "root marker bytes and version must remain unchanged"
        );
        assert_eq!(
            raw_object_snapshot(erased.clone(), &record_path).await,
            exact_before,
            "root exact record must remain byte/version-identical or absent"
        );
        assert_ne!(
            handles
                .get_handle(&prepared.handle_id)
                .await
                .expect("root handle remains readable")
                .status,
            ControlPlaneHandleStatus::Visible
        );
        assert_eq!(backend.list_calls(), 0);
    }
}

#[tokio::test]
async fn direct_catalog_visible_authority_rejects_overflow_and_wrong_read_tokens() {
    for (case, manifest_id, read_token) in [
        (
            "overflow",
            "99999999999999999999",
            "catalog:99999999999999999999",
        ),
        (
            "read_token",
            "00000000000000000001",
            "catalog:wrong-manifest",
        ),
    ] {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let (state, _) = service(backend.clone());
        let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
            .expect("construct direct catalog authority service");
        let now = instant(1_784_001_660);
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create direct catalog authority handle");
        handles
            .stage_catalog(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                create_catalog(&format!("direct_catalog_{case}")),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect("stage direct catalog authority mutation");
        let prepared = handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare direct catalog authority handle");
        force_committing(
            backend.clone(),
            &prepared.handle_id,
            now + chrono::Duration::seconds(3),
        )
        .await;
        let participant = &prepared.participants[0];
        let tx_id = ulid::Ulid::new().to_string();
        let receipt = CatalogTxReceipt {
            tx_id: tx_id.clone(),
            event_id: ulid::Ulid::new().to_string(),
            commit_id: ulid::Ulid::new().to_string(),
            manifest_id: manifest_id.to_string(),
            snapshot_version: 1,
            pointer_version: "direct-catalog-authority".to_string(),
            read_token: read_token.to_string(),
            visible_at: now,
        };
        let record = ControlPlaneTxRecord::<serde_json::Value> {
            tx_id: tx_id.clone(),
            kind: participant.kind,
            status: ControlPlaneTxStatus::Visible,
            repair_pending: false,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            lock_path: CatalogPaths::domain_lock(CatalogDomain::Catalog),
            fencing_token: 1,
            prepared_at: now,
            visible_at: Some(now),
            durable_append: None,
            result: Some(serde_json::to_value(&receipt).expect("catalog receipt value")),
        };
        put_json(
            backend.clone(),
            &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &tx_id),
            &record,
        )
        .await;
        put_json(
            backend,
            &ControlPlaneTxPaths::idempotency(
                ControlPlaneTxDomain::Catalog,
                &participant.idempotency_key,
            ),
            &ControlPlaneIdempotencyRecord {
                tx_id,
                kind: participant.kind,
                request_id: participant.request_id.clone(),
                idempotency_key: participant.idempotency_key.clone(),
                request_hash: participant.request_hash.clone(),
                created_at: now,
                visible_at: Some(now),
                tx_record: Some(serde_json::to_value(record).expect("cached catalog record")),
            },
        )
        .await;

        handles
            .recover_handle(
                &prepared.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(4),
            )
            .await
            .expect_err("noncanonical direct catalog authority must fail closed");
    }
}

#[tokio::test]
async fn direct_catalog_visible_authority_requires_its_immutable_manifest() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct direct catalog manifest service");
    let now = instant(1_784_001_665);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create direct catalog manifest handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("direct_catalog_manifest"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage direct catalog manifest mutation");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare direct catalog manifest handle");
    let visible = handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("commit direct catalog manifest handle");
    let participant = &visible.participants[0];
    let tx_id = participant
        .tx_id
        .as_deref()
        .expect("direct catalog transaction ID");
    let storage = ctx.scoped_storage(erased).expect("scoped storage");
    let record: ControlPlaneTxRecord<CatalogTxReceipt> = serde_json::from_slice(
        storage
            .get_raw(&ControlPlaneTxPaths::record(
                ControlPlaneTxDomain::Catalog,
                tx_id,
            ))
            .await
            .expect("read direct catalog exact record")
            .as_ref(),
    )
    .expect("decode direct catalog exact record");
    let receipt = record.result.expect("direct catalog receipt");
    let manifest_path =
        CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, &receipt.manifest_id);
    storage
        .delete(&manifest_path)
        .await
        .expect("remove immutable catalog manifest");

    handles
        .commit_handle(
            &visible.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("missing immutable catalog manifest must invalidate terminal visibility");
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn direct_catalog_visible_authority_binds_manifest_to_its_transaction_intent() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct catalog intent-binding service");
    let now = instant(1_784_001_666);

    let mut visible_handles = Vec::new();
    for (ordinal, name) in [
        "catalog_intent_binding_first",
        "catalog_intent_binding_second",
    ]
    .into_iter()
    .enumerate()
    {
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create catalog intent-binding handle");
        handles
            .stage_catalog(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                create_catalog(name),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect("stage catalog intent-binding mutation");
        handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare catalog intent-binding handle");
        let visible = handles
            .commit_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(
                    i64::try_from(ordinal).expect("ordinal fits i64") + 3,
                ),
            )
            .await
            .expect("commit catalog intent-binding handle");
        visible_handles.push((visible, created.review_token.expose().to_string()));
    }

    let storage = ctx.scoped_storage(erased).expect("scoped storage");
    let first = &visible_handles[0].0;
    let second = &visible_handles[1].0;
    let first_participant = &first.participants[0];
    let second_participant = &second.participants[0];
    let first_tx_id = first_participant
        .tx_id
        .as_deref()
        .expect("first catalog transaction ID");
    let second_tx_id = second_participant
        .tx_id
        .as_deref()
        .expect("second catalog transaction ID");
    let first_record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, first_tx_id);
    let second_record_path =
        ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, second_tx_id);
    let mut first_record: ControlPlaneTxRecord<CatalogTxReceipt> = serde_json::from_slice(
        storage
            .get_raw(&first_record_path)
            .await
            .expect("read first catalog exact record")
            .as_ref(),
    )
    .expect("decode first catalog exact record");
    let second_record: ControlPlaneTxRecord<CatalogTxReceipt> = serde_json::from_slice(
        storage
            .get_raw(&second_record_path)
            .await
            .expect("read second catalog exact record")
            .as_ref(),
    )
    .expect("decode second catalog exact record");
    let mut transplanted = second_record.result.expect("second catalog receipt");
    transplanted.tx_id = first_tx_id.to_string();
    first_record.fencing_token = second_record.fencing_token;
    first_record.visible_at = Some(transplanted.visible_at);
    first_record.result = Some(transplanted);
    storage
        .put_raw(
            &first_record_path,
            Bytes::from(serde_json::to_vec(&first_record).expect("encode transplanted exact")),
            WritePrecondition::None,
        )
        .await
        .expect("write transplanted catalog exact");
    let first_marker_path = ControlPlaneTxPaths::idempotency(
        ControlPlaneTxDomain::Catalog,
        &first_participant.idempotency_key,
    );
    let mut first_marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        storage
            .get_raw(&first_marker_path)
            .await
            .expect("read first catalog marker")
            .as_ref(),
    )
    .expect("decode first catalog marker");
    first_marker.visible_at = first_record.visible_at;
    first_marker.tx_record =
        Some(serde_json::to_value(&first_record).expect("cache transplanted exact"));
    storage
        .put_raw(
            &first_marker_path,
            Bytes::from(serde_json::to_vec(&first_marker).expect("encode transplanted marker")),
            WritePrecondition::None,
        )
        .await
        .expect("write transplanted catalog marker");

    handles
        .commit_handle(
            &first.handle_id,
            &visible_handles[0].1,
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect_err("another transaction's valid manifest must not satisfy this frozen intent");
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn direct_orchestration_visible_authority_requires_its_audit_receipt() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (state, _) = service(backend.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct direct orchestration authority service");
    let now = instant(1_784_001_670);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create direct orchestration authority handle");
    handles
        .stage_orchestration(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            orchestration_batch(
                "run-direct-orchestration-authority",
                &ulid::Ulid::new().to_string(),
            ),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage direct orchestration authority mutation");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare direct orchestration authority handle");
    force_committing(
        backend.clone(),
        &prepared.handle_id,
        now + chrono::Duration::seconds(3),
    )
    .await;
    let participant = &prepared.participants[0];
    let tx_id = ulid::Ulid::new().to_string();
    let receipt = OrchestrationTxReceipt {
        tx_id: tx_id.clone(),
        commit_id: ulid::Ulid::new().to_string(),
        manifest_id: "00000000000000000001".to_string(),
        revision_ulid: ulid::Ulid::new().to_string(),
        delta_id: ulid::Ulid::new().to_string(),
        pointer_version: "direct-orchestration-authority".to_string(),
        events_processed: 1,
        read_token: "orchestration:00000000000000000001".to_string(),
        visible_at: now,
    };
    let record = ControlPlaneTxRecord::<serde_json::Value> {
        tx_id: tx_id.clone(),
        kind: participant.kind,
        status: ControlPlaneTxStatus::Visible,
        repair_pending: false,
        request_id: participant.request_id.clone(),
        idempotency_key: participant.idempotency_key.clone(),
        request_hash: participant.request_hash.clone(),
        lock_path: "locks/orchestration.compaction.lock.json".to_string(),
        fencing_token: 1,
        prepared_at: now,
        visible_at: Some(now),
        durable_append: None,
        result: Some(serde_json::to_value(&receipt).expect("orchestration receipt value")),
    };
    put_json(
        backend.clone(),
        &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Orchestration, &tx_id),
        &record,
    )
    .await;
    put_json(
        backend,
        &ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Orchestration,
            &participant.idempotency_key,
        ),
        &ControlPlaneIdempotencyRecord {
            tx_id,
            kind: participant.kind,
            request_id: participant.request_id.clone(),
            idempotency_key: participant.idempotency_key.clone(),
            request_hash: participant.request_hash.clone(),
            created_at: now,
            visible_at: Some(now),
            tx_record: Some(serde_json::to_value(record).expect("cached orchestration record")),
        },
    )
    .await;

    handles
        .recover_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("missing direct orchestration audit must fail closed");
}

#[tokio::test]
async fn child_claim_without_root_marker_is_durably_retained_for_every_entry_point() {
    for mode in ["abort", "expire", "commit"] {
        let backend = Arc::new(NoListFaultBackend::new());
        let erased: Arc<dyn StorageBackend> = backend.clone();
        let (state, _) = service(erased.clone());
        let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
            .expect("construct orphan-child recovery service");
        let now = instant(1_784_001_680);
        let created = handles
            .create_handle(Duration::from_secs(5), now)
            .await
            .expect("create orphan-child handle");
        let operation = create_catalog(&format!("orphan_child_{mode}"));
        handles
            .stage_root(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                vec![DomainMutation {
                    kind: Some(domain_mutation::Kind::Catalog(operation.clone())),
                }],
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect("stage orphan-child root mutation");
        let prepared = handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare orphan-child handle");
        let root = &prepared.participants[0];
        let child_identity = format!("root:{}:catalog", root.idempotency_key);
        let child_tx_id = ulid::Ulid::new().to_string();
        let child_hash = catalog_request_hash(&operation);
        put_json(
            erased.clone(),
            &ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Catalog, &child_identity),
            &ControlPlaneIdempotencyRecord {
                tx_id: child_tx_id.clone(),
                kind: ControlPlaneTxKind::CatalogDdl,
                request_id: root.request_id.clone(),
                idempotency_key: child_identity.clone(),
                request_hash: child_hash.clone(),
                created_at: now,
                visible_at: None,
                tx_record: None,
            },
        )
        .await;
        put_json(
            erased,
            &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &child_tx_id),
            &ControlPlaneTxRecord::<serde_json::Value> {
                tx_id: child_tx_id,
                kind: ControlPlaneTxKind::CatalogDdl,
                status: ControlPlaneTxStatus::Prepared,
                repair_pending: false,
                request_id: root.request_id.clone(),
                idempotency_key: child_identity,
                request_hash: child_hash,
                lock_path: CatalogPaths::domain_lock(CatalogDomain::Catalog),
                fencing_token: 0,
                prepared_at: now,
                visible_at: None,
                durable_append: None,
                result: None,
            },
        )
        .await;

        let result = match mode {
            "abort" => {
                handles
                    .abort_handle(
                        &prepared.handle_id,
                        created.review_token.expose(),
                        now + chrono::Duration::seconds(3),
                    )
                    .await
            }
            "expire" => {
                handles
                    .expire_handle(
                        &prepared.handle_id,
                        created.review_token.expose(),
                        now + chrono::Duration::seconds(5),
                    )
                    .await
            }
            "commit" => {
                handles
                    .commit_handle(
                        &prepared.handle_id,
                        created.review_token.expose(),
                        now + chrono::Duration::seconds(3),
                    )
                    .await
            }
            _ => unreachable!(),
        };
        result.expect_err("orphan child claim requires explicit recovery");
        let retained = handles
            .get_handle(&prepared.handle_id)
            .await
            .expect("read retained orphan-child handle");
        assert_eq!(retained.status, ControlPlaneHandleStatus::RepairRequired);
        assert!(retained.participants[0].tx_id.is_none());
        assert!(retained.participants[0].low_level_status.is_none());
        assert_eq!(backend.list_calls(), 0);
    }
}

#[tokio::test]
async fn stage_wrappers_authenticate_before_parsing_malformed_payloads() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct stage authentication service");
    let now = instant(1_784_001_690);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create stage authentication handle");
    let wrong = format!("review_{}", "0".repeat(64));

    let errors = [
        handles
            .stage_catalog(
                &created.handle.handle_id,
                &wrong,
                1,
                create_catalog(""),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("catalog must authenticate before contract parsing"),
        handles
            .stage_orchestration(
                &created.handle.handle_id,
                &wrong,
                1,
                OrchestrationBatchSpec::default(),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("orchestration must authenticate before contract parsing"),
        handles
            .stage_root(
                &created.handle.handle_id,
                &wrong,
                1,
                vec![DomainMutation::default()],
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("root must authenticate before contract parsing"),
    ];
    for error in errors {
        assert_eq!(error.code(), "FORBIDDEN");
        assert!(!error.message().contains(&wrong));
    }

    let storage = ctx.scoped_storage(erased).expect("scoped storage");
    assert_eq!(
        storage
            .get_raw(
                &ControlPlaneTxPaths::handle_record(&created.handle.handle_id)
                    .expect("handle path"),
            )
            .await
            .expect("unchanged handle"),
        Bytes::from(created.handle.to_json_vec().expect("canonical handle"))
    );
    assert!(
        storage
            .head_raw(
                &ControlPlaneTxPaths::handle_mutation(&created.handle.handle_id, 1)
                    .expect("mutation path"),
            )
            .await
            .expect("head forbidden stage")
            .is_none()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn staged_locations_reject_blank_and_absolute_filesystem_paths() {
    for location in [
        "",
        "/",
        "/var/private/artifact",
        "file:///etc/passwd",
        "C:/private/artifact",
    ] {
        let backend = Arc::new(NoListFaultBackend::new());
        let erased: Arc<dyn StorageBackend> = backend.clone();
        let (state, _) = service(erased);
        let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
            .expect("construct location validation service");
        let now = instant(1_784_001_691);
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create location validation handle");

        handles
            .stage_catalog(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                register_table(location),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("catalog absolute filesystem location must fail");
        handles
            .stage_orchestration(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                task_finished_batch(location, None),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("event source absolute filesystem location must fail");
        handles
            .stage_orchestration(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                task_finished_batch(&format!("arco-flow/{TENANT}/{WORKSPACE}"), Some(location)),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect_err("callback absolute filesystem location must fail");
        assert!(
            handles
                .get_handle(&created.handle.handle_id)
                .await
                .expect("unchanged location handle")
                .mutation_refs
                .is_empty()
        );
        assert_eq!(backend.list_calls(), 0);
    }
}

#[tokio::test]
async fn only_recovery_may_drive_a_repair_required_handle() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, _) = service(erased);
    let handles = ControlPlaneTransactionHandleService::new(&state, request_context())
        .expect("construct repair mode service");
    let now = instant(1_784_001_692);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create repair mode handle");
    handles
        .stage_catalog(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            create_catalog("repair_mode"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage repair mode mutation");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare repair mode handle");
    rewrite_handle(backend.clone(), &created.handle.handle_id, |record| {
        record.status = ControlPlaneHandleStatus::RepairRequired;
        record.revision += 1;
        record.updated_at = now + chrono::Duration::seconds(3);
        record.committing_at = Some(record.updated_at);
        record.failure_category = Some(ControlPlaneHandleFailureCategory::ParticipantUncertain);
    })
    .await;

    handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("commit must not exit repair-required");
    assert_eq!(
        handles
            .get_handle(&created.handle.handle_id)
            .await
            .expect("repair handle remains repair-required")
            .status,
        ControlPlaneHandleStatus::RepairRequired
    );
    let visible = handles
        .recover_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect("recovery may exit repair-required");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn staged_mutation_and_identity_authority_reads_require_canonical_bytes() {
    let now = instant(1_784_001_693);

    let additive_backend = Arc::new(NoListFaultBackend::new());
    let additive_erased: Arc<dyn StorageBackend> = additive_backend.clone();
    let (additive_state, additive_ctx) = service(additive_erased.clone());
    let additive_handles =
        ControlPlaneTransactionHandleService::new(&additive_state, additive_ctx.clone())
            .expect("construct additive canonical service");
    let additive = additive_handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create additive canonical handle");
    additive_handles
        .stage_catalog(
            &additive.handle.handle_id,
            additive.review_token.expose(),
            1,
            create_catalog("canonical_additive"),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage additive canonical mutation");
    rewrite_staged_and_rebind(
        additive_erased.clone(),
        &additive.handle.handle_id,
        1,
        |value| value["future_v1_field"] = serde_json::json!({"safe": true}),
    )
    .await;
    let authority_path = format!(
        "transactions/handles/{}/identities/{:020}.json",
        additive.handle.handle_id, 1
    );
    let additive_storage = additive_ctx
        .scoped_storage(additive_erased)
        .expect("scoped additive storage");
    let mut authority: serde_json::Value = serde_json::from_slice(
        additive_storage
            .get_raw(&authority_path)
            .await
            .expect("read additive authority")
            .as_ref(),
    )
    .expect("decode additive authority");
    authority["future_v1_field"] = serde_json::json!({"safe": true});
    additive_storage
        .put_raw(
            &authority_path,
            Bytes::from(
                arco_core::canonical_json::to_canonical_bytes(&authority)
                    .expect("canonical additive authority"),
            ),
            WritePrecondition::None,
        )
        .await
        .expect("write canonical additive authority");
    additive_handles
        .prepare_handle(
            &additive.handle.handle_id,
            additive.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("canonical additive v1 fields remain readable");
    assert_eq!(additive_backend.list_calls(), 0);

    for target in ["mutation", "authority"] {
        let backend = Arc::new(NoListFaultBackend::new());
        let erased: Arc<dyn StorageBackend> = backend.clone();
        let (state, ctx) = service(erased.clone());
        let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
            .expect("construct noncanonical authority service");
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create noncanonical authority handle");
        handles
            .stage_catalog(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                create_catalog(&format!("noncanonical_{target}")),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect("stage noncanonical authority fixture");
        let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
        if target == "mutation" {
            let mutation_path = ControlPlaneTxPaths::handle_mutation(&created.handle.handle_id, 1)
                .expect("mutation path");
            let value: serde_json::Value = serde_json::from_slice(
                storage
                    .get_raw(&mutation_path)
                    .await
                    .expect("read canonical mutation")
                    .as_ref(),
            )
            .expect("decode canonical mutation");
            replace_staged_bytes_and_rebind(
                erased.clone(),
                &created.handle.handle_id,
                1,
                serde_json::to_vec_pretty(&value).expect("pretty mutation"),
            )
            .await;
        } else {
            let path = format!(
                "transactions/handles/{}/identities/{:020}.json",
                created.handle.handle_id, 1
            );
            let value: serde_json::Value = serde_json::from_slice(
                storage
                    .get_raw(&path)
                    .await
                    .expect("read canonical authority")
                    .as_ref(),
            )
            .expect("decode canonical authority");
            storage
                .put_raw(
                    &path,
                    Bytes::from(serde_json::to_vec_pretty(&value).expect("pretty authority")),
                    WritePrecondition::None,
                )
                .await
                .expect("write noncanonical authority");
        }
        handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect_err("noncanonical private authority must fail closed");
        assert_eq!(backend.list_calls(), 0);
    }
}

#[tokio::test]
async fn fresh_finalize_typed_invalid_exact_winner_never_repairs_a_handle_marker() {
    for case in ["catalog", "orchestration", "root", "root_child"] {
        let backend = Arc::new(NoListFaultBackend::new());
        let erased: Arc<dyn StorageBackend> = backend.clone();
        let (state, ctx) = service(erased.clone());
        let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
            .expect("construct fresh-finalize race service");
        let now = instant(1_784_001_693);
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create fresh-finalize race handle");
        match case {
            "catalog" => {
                handles
                    .stage_catalog(
                        &created.handle.handle_id,
                        created.review_token.expose(),
                        1,
                        create_catalog("fresh_finalize_catalog_race"),
                        now + chrono::Duration::seconds(1),
                    )
                    .await
                    .expect("stage catalog fresh-finalize race");
            }
            "orchestration" => {
                handles
                    .stage_orchestration(
                        &created.handle.handle_id,
                        created.review_token.expose(),
                        1,
                        orchestration_batch(
                            "fresh-finalize-orchestration-race",
                            &ulid::Ulid::new().to_string(),
                        ),
                        now + chrono::Duration::seconds(1),
                    )
                    .await
                    .expect("stage orchestration fresh-finalize race");
            }
            "root" | "root_child" => {
                handles
                    .stage_root(
                        &created.handle.handle_id,
                        created.review_token.expose(),
                        1,
                        vec![catalog_domain_mutation(&format!(
                            "fresh_finalize_{case}_race"
                        ))],
                        now + chrono::Duration::seconds(1),
                    )
                    .await
                    .expect("stage root fresh-finalize race");
            }
            _ => unreachable!("enumerated fresh-finalize race case"),
        }
        let prepared = handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare fresh-finalize race handle");
        let participant = &prepared.participants[0];
        let (domain, idempotency_key, skip) = match case {
            "catalog" => (
                ControlPlaneTxDomain::Catalog,
                participant.idempotency_key.clone(),
                1,
            ),
            "orchestration" => (
                ControlPlaneTxDomain::Orchestration,
                participant.idempotency_key.clone(),
                1,
            ),
            "root" => (
                ControlPlaneTxDomain::Root,
                participant.idempotency_key.clone(),
                1,
            ),
            "root_child" => (
                ControlPlaneTxDomain::Catalog,
                format!("root:{}:catalog", participant.idempotency_key),
                1,
            ),
            _ => unreachable!("enumerated fresh-finalize race case"),
        };
        let marker_path = ControlPlaneTxPaths::idempotency(domain, &idempotency_key);
        let (entered, release, attempted) = backend.gate_transaction_record_put_after(domain, skip);
        let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
        let commit = handles.commit_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        );
        let race = async {
            entered.notified().await;
            let attempted = attempted
                .lock()
                .expect("attempted exact bytes mutex")
                .clone()
                .expect("captured visible exact write");
            let mut invalid: ControlPlaneTxRecord<serde_json::Value> =
                serde_json::from_slice(attempted.as_ref()).expect("decode visible exact candidate");
            assert_eq!(invalid.status, ControlPlaneTxStatus::Visible);
            invalid.fencing_token = 0;
            let record_path = ControlPlaneTxPaths::record(domain, &invalid.tx_id);
            storage
                .put_raw(
                    &record_path,
                    Bytes::from(serde_json::to_vec(&invalid).expect("encode invalid winner")),
                    WritePrecondition::None,
                )
                .await
                .expect("publish fresh-finalize invalid exact winner");
            let marker = raw_object_snapshot(erased.clone(), &marker_path)
                .await
                .expect("claim-only marker before finalize resumes");
            let exact = raw_object_snapshot(erased.clone(), &record_path)
                .await
                .expect("invalid exact winner snapshot");
            let root_tx_id = match case {
                "root" => Some(invalid.tx_id.clone()),
                "root_child" => {
                    let root_marker_path = ControlPlaneTxPaths::idempotency(
                        ControlPlaneTxDomain::Root,
                        &participant.idempotency_key,
                    );
                    let root_marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
                        storage
                            .get_raw(&root_marker_path)
                            .await
                            .expect("root marker during child finalize race")
                            .as_ref(),
                    )
                    .expect("decode root marker during child finalize race");
                    Some(root_marker.tx_id)
                }
                _ => None,
            };
            let root_manifest = if let Some(root_tx_id) = root_tx_id.as_ref() {
                let path = ControlPlaneTxPaths::root_super_manifest(root_tx_id);
                let snapshot = raw_object_snapshot(erased.clone(), &path).await;
                Some((path, snapshot))
            } else {
                None
            };
            let root_audit = (case == "root")
                .then(|| {
                    invalid
                        .result
                        .as_ref()
                        .and_then(|result| result.get("rootCommitId"))
                        .and_then(serde_json::Value::as_str)
                        .map(ControlPlaneTxPaths::root_commit_receipt)
                })
                .flatten();
            let root_audit = if let Some(path) = root_audit {
                Some((
                    path.clone(),
                    raw_object_snapshot(erased.clone(), &path).await,
                ))
            } else {
                None
            };
            release.notify_one();
            (marker, exact, record_path, root_manifest, root_audit)
        };
        let (result, (marker_before, exact_winner, record_path, root_manifest, root_audit)) =
            tokio::join!(commit, race);
        result.expect_err("typed-invalid fresh-finalize winner must fail handle commit");
        assert_eq!(
            raw_object_snapshot(erased.clone(), &marker_path)
                .await
                .expect("marker after fresh-finalize race"),
            marker_before,
            "{case} marker changed after typed-invalid fresh-finalize winner"
        );
        assert_eq!(
            raw_object_snapshot(erased.clone(), &record_path)
                .await
                .expect("exact after fresh-finalize race"),
            exact_winner,
            "{case} typed-invalid exact winner was rewritten"
        );
        if let Some((path, before)) = root_manifest {
            assert_eq!(
                raw_object_snapshot(erased.clone(), &path).await,
                before,
                "{case} published root manifest authority after a typed-invalid exact winner"
            );
        }
        if let Some((path, before)) = root_audit {
            assert_eq!(
                raw_object_snapshot(erased.clone(), &path).await,
                before,
                "{case} published root audit authority after a typed-invalid exact winner"
            );
        }
        assert_eq!(backend.list_calls(), 0);
    }
}

#[tokio::test]
async fn root_preflight_rejects_orchestration_child_without_immutable_manifest_before_root_authority()
 {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct root orchestration preflight service");
    let now = instant(1_784_001_693);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create root orchestration preflight handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![DomainMutation {
                kind: Some(domain_mutation::Kind::Orchestration(orchestration_batch(
                    "root-orchestration-preflight",
                    &ulid::Ulid::new().to_string(),
                ))),
            }],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage root orchestration preflight mutation");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare root orchestration preflight handle");
    let root_participant = &prepared.participants[0];
    let root_marker_path = ControlPlaneTxPaths::idempotency(
        ControlPlaneTxDomain::Root,
        &root_participant.idempotency_key,
    );
    let child_marker_path = ControlPlaneTxPaths::idempotency(
        ControlPlaneTxDomain::Orchestration,
        &format!("root:{}:orchestration", root_participant.idempotency_key),
    );
    let (entered, release, attempted) =
        backend.gate_transaction_record_put_after(ControlPlaneTxDomain::Orchestration, 1);
    let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
    let commit = handles.commit_handle(
        &prepared.handle_id,
        created.review_token.expose(),
        now + chrono::Duration::seconds(3),
    );
    let race = async {
        entered.notified().await;
        let attempted = attempted
            .lock()
            .expect("attempted orchestration exact bytes mutex")
            .clone()
            .expect("captured orchestration visible exact write");
        let child_record: ControlPlaneTxRecord<OrchestrationTxReceipt> =
            serde_json::from_slice(attempted.as_ref())
                .expect("decode orchestration visible exact candidate");
        let child_receipt = child_record
            .result
            .as_ref()
            .expect("orchestration visible exact result");
        let child_record_path =
            ControlPlaneTxPaths::record(ControlPlaneTxDomain::Orchestration, &child_record.tx_id);
        storage
            .put_raw(&child_record_path, attempted, WritePrecondition::None)
            .await
            .expect("publish orchestration exact winner");
        let manifest_path = format!(
            "state/orchestration/manifests/{}.json",
            child_receipt.manifest_id
        );
        storage
            .delete(&manifest_path)
            .await
            .expect("remove immutable child manifest before root preflight");

        let child_marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
            storage
                .get_raw(&child_marker_path)
                .await
                .expect("read orchestration child marker")
                .as_ref(),
        )
        .expect("decode orchestration child marker");
        assert_eq!(child_marker.tx_id, child_record.tx_id);
        let root_marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
            storage
                .get_raw(&root_marker_path)
                .await
                .expect("read root marker before preflight resumes")
                .as_ref(),
        )
        .expect("decode root marker before preflight resumes");
        let root_record_path =
            ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, &root_marker.tx_id);
        let root_manifest_path = ControlPlaneTxPaths::root_super_manifest(&root_marker.tx_id);
        let root_marker_before = raw_object_snapshot(erased.clone(), &root_marker_path)
            .await
            .expect("root marker snapshot before preflight resumes");
        let root_record_before = raw_object_snapshot(erased.clone(), &root_record_path)
            .await
            .expect("root prepared record before preflight resumes");
        let root_manifest_before = raw_object_snapshot(erased.clone(), &root_manifest_path).await;
        release.notify_one();
        (
            manifest_path,
            root_record_path,
            root_manifest_path,
            root_marker_before,
            root_record_before,
            root_manifest_before,
        )
    };
    let (
        result,
        (
            child_manifest_path,
            root_record_path,
            root_manifest_path,
            root_marker_before,
            root_record_before,
            root_manifest_before,
        ),
    ) = tokio::join!(commit, race);
    result.expect_err("missing child manifest must fail before root authority publication");
    assert_eq!(
        raw_object_snapshot(erased.clone(), &root_marker_path)
            .await
            .expect("root marker after rejected child"),
        root_marker_before
    );
    let root_record_before: ControlPlaneTxRecord<serde_json::Value> =
        serde_json::from_slice(root_record_before.0.as_ref())
            .expect("decode root prepared record before rejected child");
    let root_record_after: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        raw_object_snapshot(erased.clone(), &root_record_path)
            .await
            .expect("root record after rejected child")
            .0
            .as_ref(),
    )
    .expect("decode root record after rejected child");
    assert_eq!(root_record_after.tx_id, root_record_before.tx_id);
    assert_eq!(
        root_record_after.request_hash,
        root_record_before.request_hash
    );
    assert_eq!(root_record_after.status, ControlPlaneTxStatus::Aborted);
    assert_eq!(root_record_after.fencing_token, 0);
    assert!(root_record_after.visible_at.is_none());
    assert!(root_record_after.result.is_none());
    assert_eq!(
        raw_object_snapshot(erased.clone(), &root_manifest_path).await,
        root_manifest_before
    );
    assert!(
        raw_object_snapshot(erased.clone(), &child_manifest_path)
            .await
            .is_none()
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn concurrent_typed_invalid_exact_winner_never_repairs_a_handle_marker() {
    for domain in [
        ControlPlaneTxDomain::Catalog,
        ControlPlaneTxDomain::Orchestration,
        ControlPlaneTxDomain::Root,
    ] {
        let backend = Arc::new(NoListFaultBackend::new());
        let erased: Arc<dyn StorageBackend> = backend.clone();
        let (state, ctx) = service(erased.clone());
        let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
            .expect("construct exact-winner race service");
        let now = instant(1_784_001_694);
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create exact-winner race handle");
        match domain {
            ControlPlaneTxDomain::Catalog => {
                handles
                    .stage_catalog(
                        &created.handle.handle_id,
                        created.review_token.expose(),
                        1,
                        create_catalog("exact_winner_race"),
                        now + chrono::Duration::seconds(1),
                    )
                    .await
                    .expect("stage catalog race mutation");
            }
            ControlPlaneTxDomain::Orchestration => {
                handles
                    .stage_orchestration(
                        &created.handle.handle_id,
                        created.review_token.expose(),
                        1,
                        orchestration_batch("exact-winner-race", &ulid::Ulid::new().to_string()),
                        now + chrono::Duration::seconds(1),
                    )
                    .await
                    .expect("stage orchestration race mutation");
            }
            ControlPlaneTxDomain::Root => {
                handles
                    .stage_root(
                        &created.handle.handle_id,
                        created.review_token.expose(),
                        1,
                        vec![catalog_domain_mutation("exact_winner_root_race")],
                        now + chrono::Duration::seconds(1),
                    )
                    .await
                    .expect("stage root race mutation");
            }
        }
        let prepared = handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare exact-winner race handle");
        if domain == ControlPlaneTxDomain::Orchestration {
            backend.fail_next_matching_put("state/orchestration/manifests/", 0);
            handles
                .commit_handle(
                    &prepared.handle_id,
                    created.review_token.expose(),
                    now + chrono::Duration::seconds(3),
                )
                .await
                .expect_err("seed durable-append recovery predecessor");
            backend.clear_failure();

            let repair = handles
                .get_handle(&prepared.handle_id)
                .await
                .expect("read durable-append recovery predecessor");
            assert_eq!(repair.status, ControlPlaneHandleStatus::RepairRequired);
            let participant = repair.participants[0].clone();
            let tx_id = participant
                .tx_id
                .clone()
                .expect("repair-pending orchestration transaction ID");
            let marker_path =
                ControlPlaneTxPaths::idempotency(domain, &participant.idempotency_key);
            let record_path = ControlPlaneTxPaths::record(domain, &tx_id);
            let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
            let marker_before = raw_object_snapshot(erased.clone(), &marker_path)
                .await
                .expect("marker before orchestration recovery race");
            let (entered, release, attempted) =
                backend.gate_matching_put_after(record_path.clone(), 0);
            let mut recovery = Box::pin(handles.recover_handle(
                &repair.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(4),
            ));
            tokio::select! {
                result = &mut recovery => {
                    panic!(
                        "orchestration recovery ended before the exact-record write: {result:?}"
                    );
                }
                entered_result =
                    tokio::time::timeout(Duration::from_secs(5), entered.notified()) => {
                    entered_result.unwrap_or_else(
                        |_| panic!(
                            "orchestration recovery never reached the exact-record write"
                        ),
                    );
                }
            }
            let attempted = attempted
                .lock()
                .expect("orchestration attempted bytes mutex")
                .clone()
                .expect("captured orchestration exact-record write");
            let mut invalid_winner: ControlPlaneTxRecord<serde_json::Value> =
                serde_json::from_slice(attempted.as_ref())
                    .expect("decode orchestration visible exact candidate");
            assert_eq!(invalid_winner.status, ControlPlaneTxStatus::Visible);
            invalid_winner.fencing_token = 0;
            storage
                .put_raw(
                    &record_path,
                    Bytes::from(
                        serde_json::to_vec(&invalid_winner)
                            .expect("encode orchestration invalid winner"),
                    ),
                    WritePrecondition::None,
                )
                .await
                .expect("publish concurrent orchestration invalid exact winner");
            let invalid_snapshot = raw_object_snapshot(erased.clone(), &record_path)
                .await
                .expect("orchestration invalid exact winner snapshot");
            release.notify_one();
            recovery
                .await
                .expect_err("typed-invalid orchestration winner must fail recovery");
            assert_eq!(
                raw_object_snapshot(erased.clone(), &marker_path)
                    .await
                    .expect("orchestration marker after exact-winner race"),
                marker_before
            );
            assert_eq!(
                raw_object_snapshot(erased.clone(), &record_path)
                    .await
                    .expect("orchestration exact winner after recovery"),
                invalid_snapshot
            );
            assert_eq!(backend.list_calls(), 0);
            continue;
        }
        let visible = handles
            .commit_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(3),
            )
            .await
            .expect("seed typed-valid visible authority");
        let participant = visible.participants[0].clone();
        let tx_id = participant.tx_id.clone().expect("visible transaction ID");
        let marker_path = ControlPlaneTxPaths::idempotency(domain, &participant.idempotency_key);
        let record_path = ControlPlaneTxPaths::record(domain, &tx_id);
        let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
        let mut predecessor_marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
            storage
                .get_raw(&marker_path)
                .await
                .expect("read visible marker before predecessor rewrite")
                .as_ref(),
        )
        .expect("decode visible marker before predecessor rewrite");
        predecessor_marker.visible_at = None;
        predecessor_marker.tx_record = None;
        storage
            .put_raw(
                &marker_path,
                Bytes::from(
                    serde_json::to_vec(&predecessor_marker).expect("encode prepared predecessor"),
                ),
                WritePrecondition::None,
            )
            .await
            .expect("replace marker with prepared predecessor");
        let valid: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
            storage
                .get_raw(&record_path)
                .await
                .expect("read valid exact record")
                .as_ref(),
        )
        .expect("decode valid exact record");
        let mut predecessor = valid.clone();
        predecessor.status = ControlPlaneTxStatus::Prepared;
        predecessor.repair_pending = false;
        predecessor.fencing_token = 0;
        predecessor.visible_at = None;
        predecessor.durable_append = None;
        predecessor.result = None;
        storage
            .put_raw(
                &record_path,
                Bytes::from(serde_json::to_vec(&predecessor).expect("encode predecessor")),
                WritePrecondition::None,
            )
            .await
            .expect("replace exact record with prepared predecessor");
        rewrite_handle(erased.clone(), &visible.handle_id, |record| {
            record.status = ControlPlaneHandleStatus::Committing;
            record.revision += 1;
            record.updated_at = now + chrono::Duration::seconds(4);
            record.visible_at = None;
            record.participants[0].low_level_status = Some(ControlPlaneTxStatus::Prepared);
            record.participants[0].receipt_path = None;
        })
        .await;
        let marker_before = raw_object_snapshot(erased.clone(), &marker_path)
            .await
            .expect("marker before exact-winner race");
        let (entered, release) = backend.gate_next_matching_put(record_path.clone());
        let mut recovery = Box::pin(handles.recover_handle(
            &visible.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(5),
        ));
        tokio::select! {
            result = &mut recovery => {
                panic!("{domain} recovery ended before the exact-record write: {result:?}");
            }
            entered_result =
                tokio::time::timeout(Duration::from_secs(5), entered.notified()) => {
                entered_result.unwrap_or_else(
                    |_| panic!("{domain} recovery never reached the exact-record write"),
                );
            }
        }
        let mut invalid_winner = valid;
        invalid_winner.fencing_token = 0;
        storage
            .put_raw(
                &record_path,
                Bytes::from(serde_json::to_vec(&invalid_winner).expect("encode invalid winner")),
                WritePrecondition::None,
            )
            .await
            .expect("publish concurrent invalid exact winner");
        let invalid_snapshot = raw_object_snapshot(erased.clone(), &record_path)
            .await
            .expect("invalid exact winner snapshot");
        release.notify_one();
        let result = recovery.await;
        result.expect_err("typed-invalid exact CAS winner must fail recovery");
        assert_eq!(
            raw_object_snapshot(erased.clone(), &marker_path)
                .await
                .expect("marker after exact-winner race"),
            marker_before,
            "{domain} marker changed after typed-invalid exact winner"
        );
        assert_eq!(
            raw_object_snapshot(erased.clone(), &record_path)
                .await
                .expect("exact winner after recovery"),
            invalid_snapshot,
            "{domain} exact race winner was rewritten"
        );
        assert_eq!(backend.list_calls(), 0);
    }
}

#[tokio::test]
async fn missing_audit_repair_pending_divergence_joins_before_typed_validation() {
    for domain in [
        ControlPlaneTxDomain::Orchestration,
        ControlPlaneTxDomain::Root,
    ] {
        let pending_pairs = if domain == ControlPlaneTxDomain::Root {
            vec![(false, true), (true, false), (false, false)]
        } else {
            vec![(false, true), (true, false)]
        };
        for (stored_pending, cached_pending) in pending_pairs {
            let backend = Arc::new(NoListFaultBackend::new());
            let erased: Arc<dyn StorageBackend> = backend.clone();
            let (state, ctx) = service(erased.clone());
            let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
                .expect("construct joined repair authority service");
            let now = instant(1_784_001_695);
            let created = handles
                .create_handle(Duration::from_secs(600), now)
                .await
                .expect("create joined repair handle");
            match domain {
                ControlPlaneTxDomain::Orchestration => {
                    handles
                        .stage_orchestration(
                            &created.handle.handle_id,
                            created.review_token.expose(),
                            1,
                            orchestration_batch(
                                "joined-repair-audit",
                                &ulid::Ulid::new().to_string(),
                            ),
                            now + chrono::Duration::seconds(1),
                        )
                        .await
                        .expect("stage joined orchestration mutation");
                }
                ControlPlaneTxDomain::Root => {
                    handles
                        .stage_root(
                            &created.handle.handle_id,
                            created.review_token.expose(),
                            1,
                            vec![catalog_domain_mutation("joined_repair_root")],
                            now + chrono::Duration::seconds(1),
                        )
                        .await
                        .expect("stage joined root mutation");
                }
                ControlPlaneTxDomain::Catalog => unreachable!(),
            }
            handles
                .prepare_handle(
                    &created.handle.handle_id,
                    created.review_token.expose(),
                    now + chrono::Duration::seconds(2),
                )
                .await
                .expect("prepare joined repair handle");
            let visible = handles
                .commit_handle(
                    &created.handle.handle_id,
                    created.review_token.expose(),
                    now + chrono::Duration::seconds(3),
                )
                .await
                .expect("seed visible joined repair authority");
            let participant = visible.participants[0].clone();
            let tx_id = participant.tx_id.clone().expect("joined repair tx id");
            let marker_path =
                ControlPlaneTxPaths::idempotency(domain, &participant.idempotency_key);
            let record_path = ControlPlaneTxPaths::record(domain, &tx_id);
            let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
            let mut exact: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
                storage
                    .get_raw(&record_path)
                    .await
                    .expect("read joined repair exact")
                    .as_ref(),
            )
            .expect("decode joined repair exact");
            let audit_path = match domain {
                ControlPlaneTxDomain::Orchestration => {
                    let receipt: OrchestrationTxReceipt =
                        serde_json::from_value(exact.result.clone().expect("orchestration result"))
                            .expect("decode orchestration result");
                    ControlPlaneTxPaths::orchestration_commit_receipt(&receipt.commit_id)
                }
                ControlPlaneTxDomain::Root => {
                    let receipt: RootTxReceipt =
                        serde_json::from_value(exact.result.clone().expect("root result"))
                            .expect("decode root result");
                    ControlPlaneTxPaths::root_commit_receipt(&receipt.root_commit_id)
                }
                ControlPlaneTxDomain::Catalog => unreachable!(),
            };
            storage
                .delete(&audit_path)
                .await
                .expect("remove audit to require repair state");
            exact.repair_pending = stored_pending;
            storage
                .put_raw(
                    &record_path,
                    Bytes::from(serde_json::to_vec(&exact).expect("encode stored divergence")),
                    WritePrecondition::None,
                )
                .await
                .expect("write stored divergence");
            let mut cached = exact.clone();
            cached.repair_pending = cached_pending;
            let mut marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
                storage
                    .get_raw(&marker_path)
                    .await
                    .expect("read joined repair marker")
                    .as_ref(),
            )
            .expect("decode joined repair marker");
            marker.tx_record = Some(serde_json::to_value(&cached).expect("cached divergence"));
            storage
                .put_raw(
                    &marker_path,
                    Bytes::from(serde_json::to_vec(&marker).expect("encode marker divergence")),
                    WritePrecondition::None,
                )
                .await
                .expect("write marker divergence");
            rewrite_handle(erased.clone(), &visible.handle_id, |record| {
                record.status = ControlPlaneHandleStatus::Committing;
                record.revision += 1;
                record.updated_at = now + chrono::Duration::seconds(4);
                record.visible_at = None;
                record.participants[0].receipt_path = None;
            })
            .await;

            if !stored_pending && !cached_pending {
                handles
                    .recover_handle(
                        &visible.handle_id,
                        created.review_token.expose(),
                        now + chrono::Duration::seconds(5),
                    )
                    .await
                    .expect_err("missing root audit without repair state must fail closed");
                assert!(
                    storage
                        .head_raw(&audit_path)
                        .await
                        .expect("head forbidden audit repair")
                        .is_none()
                );
                assert_eq!(backend.list_calls(), 0);
                continue;
            }

            let recovered = handles
                .recover_handle(
                    &visible.handle_id,
                    created.review_token.expose(),
                    now + chrono::Duration::seconds(5),
                )
                .await
                .expect("joined repair state authorizes missing audit recovery");
            assert_eq!(recovered.status, ControlPlaneHandleStatus::Visible);
            let joined_exact: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
                storage
                    .get_raw(&record_path)
                    .await
                    .expect("read joined exact")
                    .as_ref(),
            )
            .expect("decode joined exact");
            let joined_marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
                storage
                    .get_raw(&marker_path)
                    .await
                    .expect("read joined marker")
                    .as_ref(),
            )
            .expect("decode joined marker");
            let joined_cached: ControlPlaneTxRecord<serde_json::Value> =
                serde_json::from_value(joined_marker.tx_record.expect("joined cached record"))
                    .expect("decode joined cached record");
            let expected_pending = stored_pending || cached_pending;
            assert_eq!(joined_exact.repair_pending, expected_pending);
            assert_eq!(joined_cached.repair_pending, expected_pending);
            if domain == ControlPlaneTxDomain::Root {
                assert!(
                    storage
                        .head_raw(&audit_path)
                        .await
                        .expect("head repaired audit")
                        .is_some(),
                    "missing root audit must be reconstructed after exact authority converges"
                );
            }
            assert_eq!(backend.list_calls(), 0);
        }
    }
}

#[tokio::test]
async fn participant_evidence_cas_adopts_terminal_winners_without_redriving_them() {
    for winner_status in [
        ControlPlaneHandleStatus::RepairRequired,
        ControlPlaneHandleStatus::Visible,
    ] {
        let backend = Arc::new(NoListFaultBackend::new());
        let erased: Arc<dyn StorageBackend> = backend.clone();
        let (state, ctx) = service(erased.clone());
        let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
            .expect("construct participant evidence race service");
        let now = instant(1_784_001_696);
        let created = handles
            .create_handle(Duration::from_secs(600), now)
            .await
            .expect("create participant evidence race handle");
        handles
            .stage_catalog(
                &created.handle.handle_id,
                created.review_token.expose(),
                1,
                create_catalog("participant_evidence_race"),
                now + chrono::Duration::seconds(1),
            )
            .await
            .expect("stage participant evidence race");
        let prepared = handles
            .prepare_handle(
                &created.handle.handle_id,
                created.review_token.expose(),
                now + chrono::Duration::seconds(2),
            )
            .await
            .expect("prepare participant evidence race");
        force_committing(
            erased.clone(),
            &prepared.handle_id,
            now + chrono::Duration::seconds(3),
        )
        .await;
        let handle_path =
            ControlPlaneTxPaths::handle_record(&prepared.handle_id).expect("handle path");
        let marker_path = ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Catalog,
            &prepared.participants[0].idempotency_key,
        );
        let (entered, release) = backend.gate_next_matching_put(handle_path.clone());
        let commit = handles.commit_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        );
        let race = async {
            entered.notified().await;
            let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
            let marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
                storage
                    .get_raw(&marker_path)
                    .await
                    .expect("visible low-level marker")
                    .as_ref(),
            )
            .expect("decode visible low-level marker");
            let winner = rewrite_handle(erased.clone(), &prepared.handle_id, |record| {
                record.status = winner_status;
                record.revision += 1;
                record.updated_at = now + chrono::Duration::seconds(4);
                record.participants[0].tx_id = Some(marker.tx_id.clone());
                record.participants[0].low_level_status = Some(ControlPlaneTxStatus::Visible);
                record.participants[0].receipt_path = None;
                match winner_status {
                    ControlPlaneHandleStatus::RepairRequired => {
                        record.failure_category =
                            Some(ControlPlaneHandleFailureCategory::ParticipantUncertain);
                    }
                    ControlPlaneHandleStatus::Visible => {
                        record.visible_at = Some(record.updated_at);
                        record.failure_category = None;
                    }
                    _ => unreachable!(),
                }
            })
            .await;
            let snapshot = raw_object_snapshot(erased.clone(), &handle_path)
                .await
                .expect("terminal CAS winner snapshot");
            release.notify_one();
            (winner, snapshot)
        };
        let (commit_result, (winner, snapshot)) = tokio::join!(commit, race);
        match winner_status {
            ControlPlaneHandleStatus::RepairRequired => {
                commit_result.expect_err("ordinary commit must stop at repair-required winner");
            }
            ControlPlaneHandleStatus::Visible => {
                let adopted = commit_result.expect("visible winner is safely adopted");
                assert_eq!(adopted, winner);
            }
            _ => unreachable!(),
        }
        assert_eq!(
            raw_object_snapshot(erased.clone(), &handle_path)
                .await
                .expect("terminal winner after blocked drive"),
            snapshot,
            "terminal handle winner was rewritten"
        );
        let durable = handles
            .get_handle(&prepared.handle_id)
            .await
            .expect("read terminal handle winner");
        assert_eq!(durable.revision, winner.revision);
        assert_eq!(durable.visible_at, winner.visible_at);
        if winner_status == ControlPlaneHandleStatus::RepairRequired {
            let recovered = handles
                .recover_handle(
                    &prepared.handle_id,
                    created.review_token.expose(),
                    now + chrono::Duration::seconds(5),
                )
                .await
                .expect("recovery may drive repair winner");
            assert_eq!(recovered.status, ControlPlaneHandleStatus::Visible);
        }
        assert_eq!(backend.list_calls(), 0);
    }
}

#[tokio::test]
async fn root_recovery_resumes_a_repair_pending_orchestration_child_with_frozen_ids() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct nested orchestration recovery service");
    let now = instant(1_784_001_697);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create nested orchestration recovery handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![DomainMutation {
                kind: Some(domain_mutation::Kind::Orchestration(orchestration_batch(
                    "nested-root-repair",
                    &ulid::Ulid::new().to_string(),
                ))),
            }],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage nested orchestration root");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare nested orchestration root");
    let child_key = format!(
        "root:{}:orchestration",
        prepared.participants[0].idempotency_key
    );

    backend.fail_next_matching_put("state/orchestration/manifests/", 0);
    handles
        .commit_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("nested durable append failure must require recovery");
    backend.clear_failure();
    let repair = handles
        .get_handle(&prepared.handle_id)
        .await
        .expect("read nested repair handle");
    assert_eq!(repair.status, ControlPlaneHandleStatus::RepairRequired);
    assert_eq!(
        repair.participants[0].low_level_status,
        Some(ControlPlaneTxStatus::Prepared)
    );
    let original_root_tx_id = repair.participants[0]
        .tx_id
        .clone()
        .expect("frozen root transaction ID");
    let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
    let child_before: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        storage
            .get_raw(&ControlPlaneTxPaths::idempotency(
                ControlPlaneTxDomain::Orchestration,
                &child_key,
            ))
            .await
            .expect("nested child marker")
            .as_ref(),
    )
    .expect("decode nested child marker");
    let child_record_before: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        storage
            .get_raw(&ControlPlaneTxPaths::record(
                ControlPlaneTxDomain::Orchestration,
                &child_before.tx_id,
            ))
            .await
            .expect("nested child record")
            .as_ref(),
    )
    .expect("decode nested child record");
    assert_eq!(child_record_before.status, ControlPlaneTxStatus::Prepared);
    assert!(child_record_before.repair_pending);
    assert!(child_record_before.durable_append.is_some());

    let visible = handles
        .recover_handle(
            &repair.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect("recover nested orchestration append in place");
    assert_eq!(visible.status, ControlPlaneHandleStatus::Visible);
    assert_eq!(
        visible.participants[0].tx_id.as_deref(),
        Some(original_root_tx_id.as_str())
    );
    let root_marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        storage
            .get_raw(&ControlPlaneTxPaths::idempotency(
                ControlPlaneTxDomain::Root,
                &prepared.participants[0].idempotency_key,
            ))
            .await
            .expect("recovered root marker")
            .as_ref(),
    )
    .expect("decode recovered root marker");
    let child_after: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        storage
            .get_raw(&ControlPlaneTxPaths::idempotency(
                ControlPlaneTxDomain::Orchestration,
                &child_key,
            ))
            .await
            .expect("recovered child marker")
            .as_ref(),
    )
    .expect("decode recovered child marker");
    assert_eq!(root_marker.tx_id, original_root_tx_id);
    assert_eq!(child_after.tx_id, child_before.tx_id);
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn root_recovery_rejects_invalid_existing_manifest_before_claim_mutation() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct invalid manifest recovery service");
    let now = instant(1_784_001_698);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create invalid manifest handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![catalog_domain_mutation("invalid_existing_manifest")],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage invalid manifest root");
    let prepared = handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare invalid manifest root");
    backend.fail_next_matching_put("transactions/root/", 1);
    handles
        .commit_handle(
            &prepared.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect_err("seed recoverable root claim before manifest publication");
    backend.clear_failure();
    let repair = handles
        .get_handle(&prepared.handle_id)
        .await
        .expect("read invalid manifest repair handle");
    assert_eq!(repair.status, ControlPlaneHandleStatus::RepairRequired);
    let root_tx_id = repair.participants[0]
        .tx_id
        .clone()
        .expect("frozen root transaction ID");
    let marker_path = ControlPlaneTxPaths::idempotency(
        ControlPlaneTxDomain::Root,
        &prepared.participants[0].idempotency_key,
    );
    let record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, &root_tx_id);
    let manifest_path = ControlPlaneTxPaths::root_super_manifest(&root_tx_id);
    put_json(
        erased.clone(),
        &manifest_path,
        &RootTxManifest {
            tx_id: root_tx_id,
            fencing_token: 0,
            published_at: now + chrono::Duration::seconds(3),
            domains: BTreeMap::from([(
                ControlPlaneTxDomain::Catalog,
                arco_core::control_plane_transactions::RootTxManifestDomain {
                    manifest_id: "00000000000000000001".to_string(),
                    manifest_path: "manifests/catalog/00000000000000000001.json".to_string(),
                    commit_id: ulid::Ulid::new().to_string(),
                },
            )]),
        },
    )
    .await;
    let marker_before = raw_object_snapshot(erased.clone(), &marker_path)
        .await
        .expect("root marker before invalid adoption");
    let record_before = raw_object_snapshot(erased.clone(), &record_path)
        .await
        .expect("root record before invalid adoption");

    handles
        .recover_handle(
            &repair.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(4),
        )
        .await
        .expect_err("zero-fence existing manifest must fail before root finalization");
    assert_eq!(
        raw_object_snapshot(erased.clone(), &marker_path)
            .await
            .expect("root marker after invalid adoption"),
        marker_before
    );
    assert_eq!(
        raw_object_snapshot(erased.clone(), &record_path)
            .await
            .expect("root record after invalid adoption"),
        record_before
    );

    let root_record: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        &raw_object_snapshot(erased.clone(), &record_path)
            .await
            .expect("root record before divergent-reference adoption")
            .0,
    )
    .expect("decode root predecessor chronology");
    ctx.scoped_storage(erased.clone())
        .expect("scoped storage")
        .delete(&manifest_path)
        .await
        .expect("replace zero-fence manifest fixture");
    put_json(
        erased.clone(),
        &manifest_path,
        &RootTxManifest {
            tx_id: root_record.tx_id.clone(),
            fencing_token: 1,
            published_at: root_record.prepared_at + chrono::Duration::seconds(1),
            domains: BTreeMap::from([(
                ControlPlaneTxDomain::Catalog,
                arco_core::control_plane_transactions::RootTxManifestDomain {
                    manifest_id: "00000000000000000999".to_string(),
                    manifest_path: "manifests/catalog/00000000000000000999.json".to_string(),
                    commit_id: ulid::Ulid::new().to_string(),
                },
            )]),
        },
    )
    .await;
    let marker_before_divergent = raw_object_snapshot(erased.clone(), &marker_path)
        .await
        .expect("root marker before divergent-reference adoption");
    let record_before_divergent = raw_object_snapshot(erased.clone(), &record_path)
        .await
        .expect("root record before divergent-reference adoption");
    let manifest_before_divergent = raw_object_snapshot(erased.clone(), &manifest_path)
        .await
        .expect("divergent manifest before adoption");
    handles
        .recover_handle(
            &repair.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect_err("divergent manifest references must fail before root rearm");
    assert_eq!(
        raw_object_snapshot(erased.clone(), &marker_path)
            .await
            .expect("root marker after divergent-reference adoption"),
        marker_before_divergent
    );
    assert_eq!(
        raw_object_snapshot(erased.clone(), &record_path)
            .await
            .expect("root record after divergent-reference adoption"),
        record_before_divergent
    );
    assert_eq!(
        raw_object_snapshot(erased.clone(), &manifest_path)
            .await
            .expect("manifest after divergent-reference adoption"),
        manifest_before_divergent
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn visible_root_chronology_regression_fails_closed_without_repair() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct root chronology service");
    let now = instant(1_784_001_699);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create root chronology handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![catalog_domain_mutation("root_chronology")],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage root chronology mutation");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare root chronology handle");
    let visible = handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("seed visible root chronology authority");
    let participant = &visible.participants[0];
    let tx_id = participant.tx_id.as_ref().expect("root transaction ID");
    let marker_path =
        ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Root, &participant.idempotency_key);
    let record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, tx_id);
    let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
    let mut marker: ControlPlaneIdempotencyRecord = serde_json::from_slice(
        storage
            .get_raw(&marker_path)
            .await
            .expect("root chronology marker")
            .as_ref(),
    )
    .expect("decode root chronology marker");
    let mut record: ControlPlaneTxRecord<RootTxReceipt> = serde_json::from_slice(
        storage
            .get_raw(&record_path)
            .await
            .expect("root chronology record")
            .as_ref(),
    )
    .expect("decode root chronology record");
    record.prepared_at = record.visible_at.expect("root visible_at") + chrono::Duration::seconds(1);
    marker.tx_record = Some(serde_json::to_value(&record).expect("cached chronology record"));
    storage
        .put_raw(
            &record_path,
            Bytes::from(serde_json::to_vec(&record).expect("encode chronology record")),
            WritePrecondition::None,
        )
        .await
        .expect("write chronology-regressed exact root");
    storage
        .put_raw(
            &marker_path,
            Bytes::from(serde_json::to_vec(&marker).expect("encode chronology marker")),
            WritePrecondition::None,
        )
        .await
        .expect("write chronology-regressed root marker");
    rewrite_handle(erased.clone(), &visible.handle_id, |handle| {
        handle.status = ControlPlaneHandleStatus::Committing;
        handle.revision += 1;
        handle.updated_at = now + chrono::Duration::seconds(4);
        handle.visible_at = None;
    })
    .await;
    let marker_before = raw_object_snapshot(erased.clone(), &marker_path)
        .await
        .expect("chronology marker before recovery");
    let record_before = raw_object_snapshot(erased.clone(), &record_path)
        .await
        .expect("chronology record before recovery");

    handles
        .recover_handle(
            &visible.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(5),
        )
        .await
        .expect_err("root visibility cannot precede preparation");
    assert_eq!(
        raw_object_snapshot(erased.clone(), &marker_path)
            .await
            .expect("chronology marker after recovery"),
        marker_before
    );
    assert_eq!(
        raw_object_snapshot(erased.clone(), &record_path)
            .await
            .expect("chronology record after recovery"),
        record_before
    );
    assert_eq!(backend.list_calls(), 0);
}

#[tokio::test]
async fn concurrent_typed_invalid_root_child_blocks_root_marker_repair() {
    let backend = Arc::new(NoListFaultBackend::new());
    let erased: Arc<dyn StorageBackend> = backend.clone();
    let (state, ctx) = service(erased.clone());
    let handles = ControlPlaneTransactionHandleService::new(&state, ctx.clone())
        .expect("construct root-child race service");
    let now = instant(1_784_001_699);
    let created = handles
        .create_handle(Duration::from_secs(600), now)
        .await
        .expect("create root-child race handle");
    handles
        .stage_root(
            &created.handle.handle_id,
            created.review_token.expose(),
            1,
            vec![catalog_domain_mutation("root_child_exact_race")],
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("stage root-child race");
    handles
        .prepare_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(2),
        )
        .await
        .expect("prepare root-child race");
    let visible = handles
        .commit_handle(
            &created.handle.handle_id,
            created.review_token.expose(),
            now + chrono::Duration::seconds(3),
        )
        .await
        .expect("seed valid visible root and child");
    let root_participant = visible.participants[0].clone();
    let root_tx_id = root_participant.tx_id.clone().expect("root transaction ID");
    let root_marker_path = ControlPlaneTxPaths::idempotency(
        ControlPlaneTxDomain::Root,
        &root_participant.idempotency_key,
    );
    let root_record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, &root_tx_id);
    let storage = ctx.scoped_storage(erased.clone()).expect("scoped storage");
    let valid_root: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        storage
            .get_raw(&root_record_path)
            .await
            .expect("valid root exact")
            .as_ref(),
    )
    .expect("decode valid root exact");
    let root_receipt: RootTxReceipt =
        serde_json::from_value(valid_root.result.clone().expect("root result"))
            .expect("decode root receipt");
    let child_tx_id = root_receipt.domain_commits[0].tx_id.clone();
    let child_key = format!("root:{}:catalog", root_participant.idempotency_key);
    let child_marker_path =
        ControlPlaneTxPaths::idempotency(ControlPlaneTxDomain::Catalog, &child_key);
    let child_record_path =
        ControlPlaneTxPaths::record(ControlPlaneTxDomain::Catalog, &child_tx_id);
    let valid_child: ControlPlaneTxRecord<serde_json::Value> = serde_json::from_slice(
        storage
            .get_raw(&child_record_path)
            .await
            .expect("valid child exact")
            .as_ref(),
    )
    .expect("decode valid child exact");
    let mut root_predecessor = valid_root.clone();
    root_predecessor.status = ControlPlaneTxStatus::Prepared;
    root_predecessor.repair_pending = false;
    root_predecessor.visible_at = None;
    root_predecessor.result = None;
    storage
        .put_raw(
            &root_record_path,
            Bytes::from(serde_json::to_vec(&root_predecessor).expect("root predecessor")),
            WritePrecondition::None,
        )
        .await
        .expect("write root predecessor");
    rewrite_handle(erased.clone(), &visible.handle_id, |record| {
        record.status = ControlPlaneHandleStatus::Committing;
        record.revision += 1;
        record.updated_at = now + chrono::Duration::seconds(4);
        record.visible_at = None;
    })
    .await;
    let root_marker_before = raw_object_snapshot(erased.clone(), &root_marker_path)
        .await
        .expect("root marker before child race");
    let child_marker_before = raw_object_snapshot(erased.clone(), &child_marker_path)
        .await
        .expect("child marker before child race");
    let (entered, release) = backend.gate_next_matching_put(root_record_path.clone());
    let recovery = handles.recover_handle(
        &visible.handle_id,
        created.review_token.expose(),
        now + chrono::Duration::seconds(5),
    );
    let race = async {
        entered.notified().await;
        let mut invalid_child = valid_child;
        invalid_child.fencing_token = 0;
        storage
            .put_raw(
                &child_record_path,
                Bytes::from(serde_json::to_vec(&invalid_child).expect("invalid child")),
                WritePrecondition::None,
            )
            .await
            .expect("publish invalid child race winner");
        let child_snapshot = raw_object_snapshot(erased.clone(), &child_record_path)
            .await
            .expect("invalid child race snapshot");
        release.notify_one();
        child_snapshot
    };
    let (result, child_snapshot) = tokio::join!(recovery, race);
    result.expect_err("actual root winner must re-preflight every child before marker repair");
    assert_eq!(
        raw_object_snapshot(erased.clone(), &root_marker_path)
            .await
            .expect("root marker after child race"),
        root_marker_before
    );
    assert_eq!(
        raw_object_snapshot(erased.clone(), &child_marker_path)
            .await
            .expect("child marker after child race"),
        child_marker_before
    );
    assert_eq!(
        raw_object_snapshot(erased.clone(), &child_record_path)
            .await
            .expect("child exact after root rejection"),
        child_snapshot
    );
    assert_eq!(backend.list_calls(), 0);
}

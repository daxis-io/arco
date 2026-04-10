//! Integration tests for control-plane transaction commit and lookup APIs.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use axum::http::StatusCode;
use bytes::Bytes;

use arco_catalog::CatalogReader;
use arco_core::ControlPlaneTxDomain;
use arco_core::catalog_event::CatalogEvent;
use arco_core::catalog_paths::{CatalogDomain, CatalogPaths};
use arco_core::control_plane_transactions::{
    ControlPlaneTxPaths, ControlPlaneTxStatus, RootTxManifest, RootTxReceipt,
};
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_proto::arco::catalog::v1::{CatalogDdlOperation, ColumnDefinition};
use arco_proto::arco::controlplane::v1::{
    ApplyCatalogDdlRequest, ApplyCatalogDdlResponse, CommitOrchestrationBatchRequest,
    CommitOrchestrationBatchResponse, CommitRootTransactionResponse, GetCatalogTransactionRequest,
    GetCatalogTransactionResponse, GetOrchestrationTransactionRequest,
    GetOrchestrationTransactionResponse, GetRootTransactionRequest, GetRootTransactionResponse,
    TransactionDomain, TransactionStatus, domain_mutation,
};
#[path = "support/control_plane_transactions.rs"]
mod support;

use support::{
    TENANT, WORKSPACE, catalog_alter_table_request, catalog_create_catalog_request,
    catalog_create_namespace_request, catalog_create_schema_request, catalog_drop_table_request,
    catalog_register_table_in_schema_request,
    catalog_register_table_in_schema_request_with_columns, catalog_register_table_request,
    catalog_rename_table_request, load_catalog_tx_record, load_idempotency_record,
    load_orchestration_tx_record, load_root_tx_record, orchestration_request,
    orchestration_request_with_event_id, post_error_json, post_protobuf, root_request,
    scoped_storage, test_router, test_router_with_backend, test_router_with_config_backend,
};

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

async fn load_orchestration_commit_receipt(
    backend: Arc<dyn StorageBackend>,
    commit_id: &str,
) -> Result<Option<arco_core::control_plane_transactions::OrchestrationTxReceipt>> {
    let storage = scoped_storage(backend);
    match storage
        .get_raw(&ControlPlaneTxPaths::orchestration_commit_receipt(
            commit_id,
        ))
        .await
    {
        Ok(bytes) => serde_json::from_slice(bytes.as_ref())
            .map(Some)
            .context("decode orchestration commit receipt"),
        Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
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
async fn apply_catalog_ddl_registers_alters_and_drops_tables_via_transaction_layer() -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let router = test_router_with_backend(backend.clone());

    let create_namespace =
        catalog_create_namespace_request("idem-cat-table-ns-01", "req-cat-table-ns-01", "ops");
    let (_status, create_namespace_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &create_namespace,
        "idem-cat-table-ns-01",
        "req-cat-table-ns-01",
    )
    .await?;
    assert!(
        create_namespace_response.receipt.is_some(),
        "namespace receipt missing"
    );

    let register = catalog_register_table_request(
        "idem-cat-table-reg-01",
        "req-cat-table-reg-01",
        "ops",
        "events",
    );
    let (_status, register_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &register,
        "idem-cat-table-reg-01",
        "req-cat-table-reg-01",
    )
    .await?;
    let register_receipt = register_response
        .receipt
        .context("register table receipt missing")?;
    let register_record = load_catalog_tx_record(backend.clone(), &register_receipt.tx_id).await?;
    assert_eq!(register_record.status, ControlPlaneTxStatus::Visible);

    let alter = catalog_alter_table_request(
        "idem-cat-table-alt-01",
        "req-cat-table-alt-01",
        "ops",
        "events",
        Some("updated description"),
    );
    let (_status, alter_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &alter,
        "idem-cat-table-alt-01",
        "req-cat-table-alt-01",
    )
    .await?;
    let alter_receipt = alter_response
        .receipt
        .context("alter table receipt missing")?;
    let alter_record = load_catalog_tx_record(backend.clone(), &alter_receipt.tx_id).await?;
    assert_eq!(alter_record.status, ControlPlaneTxStatus::Visible);

    let drop = catalog_drop_table_request(
        "idem-cat-table-drop-01",
        "req-cat-table-drop-01",
        "ops",
        "events",
    );
    let (_status, drop_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &drop,
        "idem-cat-table-drop-01",
        "req-cat-table-drop-01",
    )
    .await?;
    let drop_receipt = drop_response
        .receipt
        .context("drop table receipt missing")?;
    let drop_record = load_catalog_tx_record(backend, &drop_receipt.tx_id).await?;
    assert_eq!(drop_record.status, ControlPlaneTxStatus::Visible);

    Ok(())
}

#[tokio::test]
async fn apply_catalog_ddl_creates_catalogs_and_renames_tables_via_transaction_layer() -> Result<()>
{
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let router = test_router_with_backend(backend.clone());

    let create_catalog =
        catalog_create_catalog_request("idem-cat-uc-cat-01", "req-cat-uc-cat-01", "governed");
    let (_status, create_catalog_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &create_catalog,
        "idem-cat-uc-cat-01",
        "req-cat-uc-cat-01",
    )
    .await?;
    assert!(
        create_catalog_response.receipt.is_some(),
        "catalog receipt missing"
    );

    let create_schema = catalog_create_schema_request(
        "idem-cat-uc-schema-01",
        "req-cat-uc-schema-01",
        "governed",
        "bronze",
    );
    let (_status, create_schema_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &create_schema,
        "idem-cat-uc-schema-01",
        "req-cat-uc-schema-01",
    )
    .await?;
    assert!(
        create_schema_response.receipt.is_some(),
        "schema receipt missing"
    );

    let register = catalog_register_table_in_schema_request(
        "idem-cat-uc-reg-01",
        "req-cat-uc-reg-01",
        "governed",
        "bronze",
        "events",
    );
    let (_status, register_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &register,
        "idem-cat-uc-reg-01",
        "req-cat-uc-reg-01",
    )
    .await?;
    let register_receipt = register_response
        .receipt
        .context("register table receipt missing")?;

    let rename = catalog_rename_table_request(
        "idem-cat-uc-rename-01",
        "req-cat-uc-rename-01",
        "governed",
        "bronze",
        "events",
        "events_curated",
    );
    let (_status, rename_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &rename,
        "idem-cat-uc-rename-01",
        "req-cat-uc-rename-01",
    )
    .await?;
    let rename_receipt = rename_response
        .receipt
        .context("rename table receipt missing")?;

    let reader = CatalogReader::new(scoped_storage(backend.clone()));
    let catalog = reader
        .get_catalog("governed")
        .await?
        .context("created catalog missing from reader")?;
    assert_eq!(catalog.name, "governed");

    let old_table = reader
        .get_table_in_schema("governed", "bronze", "events")
        .await?;
    assert!(
        old_table.is_none(),
        "old table name should no longer resolve"
    );

    let renamed_table = reader
        .get_table_in_schema("governed", "bronze", "events_curated")
        .await?
        .context("renamed table missing from reader")?;
    assert_eq!(renamed_table.name, "events_curated");

    let register_record = load_catalog_tx_record(backend.clone(), &register_receipt.tx_id).await?;
    assert_eq!(register_record.status, ControlPlaneTxStatus::Visible);

    let rename_record = load_catalog_tx_record(backend, &rename_receipt.tx_id).await?;
    assert_eq!(rename_record.status, ControlPlaneTxStatus::Visible);

    Ok(())
}

#[tokio::test]
async fn apply_catalog_ddl_preserves_proto_column_ordinals_via_transaction_layer() -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let router = test_router_with_backend(backend.clone());

    let create_namespace = catalog_create_namespace_request(
        "idem-cat-ordinal-ns-01",
        "req-cat-ordinal-ns-01",
        "ordinals",
    );
    let (_status, create_namespace_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &create_namespace,
        "idem-cat-ordinal-ns-01",
        "req-cat-ordinal-ns-01",
    )
    .await?;
    assert!(
        create_namespace_response.receipt.is_some(),
        "namespace receipt missing"
    );

    let register = catalog_register_table_in_schema_request_with_columns(
        "idem-cat-ordinal-reg-01",
        "req-cat-ordinal-reg-01",
        "default",
        "ordinals",
        "events",
        vec![
            ColumnDefinition {
                name: "event_ts".to_string(),
                data_type: "TIMESTAMP".to_string(),
                is_nullable: false,
                ordinal: 10,
                description: Some("event time".to_string()),
            },
            ColumnDefinition {
                name: "event_id".to_string(),
                data_type: "STRING".to_string(),
                is_nullable: false,
                ordinal: 5,
                description: Some("event identifier".to_string()),
            },
        ],
    );
    let (_status, register_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &register,
        "idem-cat-ordinal-reg-01",
        "req-cat-ordinal-reg-01",
    )
    .await?;
    assert!(
        register_response.receipt.is_some(),
        "register table receipt missing"
    );

    let reader = CatalogReader::new(scoped_storage(backend));
    let table = reader
        .get_table_in_schema("default", "ordinals", "events")
        .await?
        .context("registered table missing from reader")?;
    let columns = reader.get_columns(&table.id).await?;
    let ordinals = columns
        .iter()
        .map(|column| (column.name.as_str(), column.ordinal))
        .collect::<Vec<_>>();
    assert_eq!(ordinals, vec![("event_id", 5), ("event_ts", 10)]);

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
    assert_ne!(receipt.commit_id, receipt.revision_ulid);
    assert!(!receipt.pointer_version.is_empty());
    assert_eq!(receipt.events_processed, 1);
    assert_eq!(
        receipt.read_token,
        format!("orchestration:{}", receipt.manifest_id)
    );
    assert!(!response.repair_pending);

    let lookup = GetOrchestrationTransactionRequest {
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

    let stored = load_orchestration_tx_record(backend.clone(), &receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert_eq!(stored.request_id, "req-orch-01");
    assert_eq!(stored.idempotency_key, "idem-orch-01");
    assert_eq!(
        stored.result.as_ref().map(|result| result.tx_id.as_str()),
        Some(receipt.tx_id.as_str())
    );

    let commit_receipt = load_orchestration_commit_receipt(backend, &receipt.commit_id)
        .await?
        .context("orchestration commit receipt missing")?;
    assert_eq!(commit_receipt.tx_id, receipt.tx_id);
    assert_eq!(commit_receipt.commit_id, receipt.commit_id);
    assert_eq!(commit_receipt.revision_ulid, receipt.revision_ulid);

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

    let first_receipt = first_response
        .receipt
        .context("first orchestration receipt missing")?;
    let replay_receipt = replay_response
        .receipt
        .context("replay orchestration receipt missing")?;
    assert_eq!(
        first_receipt.tx_id, replay_receipt.tx_id,
        "idempotent replay must reuse tx_id"
    );
    assert_eq!(first_receipt.commit_id, replay_receipt.commit_id);
    assert_ne!(first_receipt.commit_id, first_receipt.revision_ulid);

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
async fn commit_root_transaction_replays_same_idempotency_key_and_keeps_audit_receipt_stable()
-> Result<()> {
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
    let first_audit_receipt =
        load_root_commit_receipt(backend.clone(), &first_receipt.root_commit_id)
            .await?
            .context("first replay audit receipt missing")?;
    let replay_audit_receipt =
        load_root_commit_receipt(backend.clone(), &replay_receipt.root_commit_id)
            .await?
            .context("replay audit receipt missing")?;
    assert_eq!(first_audit_receipt.tx_id, first_receipt.tx_id);
    assert_eq!(
        first_audit_receipt.root_commit_id,
        first_receipt.root_commit_id
    );
    assert_eq!(replay_audit_receipt.tx_id, replay_receipt.tx_id);
    assert_eq!(
        replay_audit_receipt.root_commit_id,
        replay_receipt.root_commit_id
    );
    assert_eq!(first_audit_receipt, replay_audit_receipt);

    let idem =
        load_idempotency_record(backend, ControlPlaneTxDomain::Root, "idem-root-replay-01").await?;
    assert_eq!(idem.tx_id, first_receipt.tx_id);
    assert!(idem.visible_at.is_some());
    assert!(idem.tx_record.is_some());

    Ok(())
}

#[tokio::test]
async fn commit_root_transaction_exposes_pinned_catalog_and_orchestration_reads() -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let router = test_router_with_backend(backend.clone());
    let request = root_request(
        "idem-root-read-01",
        "req-root-read-01",
        "root-pinned-catalog",
        "run-root-read-01",
    );

    let (_status, response): (_, CommitRootTransactionResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/commitRootTransaction",
        &request,
        "idem-root-read-01",
        "req-root-read-01",
    )
    .await?;

    let receipt = response
        .receipt
        .context("root pinned read receipt missing")?;
    let storage = scoped_storage(backend.clone());
    let reader = CatalogReader::new(storage.clone());
    let compactor = MicroCompactor::new(storage);

    let pinned_namespaces = reader
        .list_namespaces_for_root_token(&receipt.read_token)
        .await?;
    assert_eq!(
        pinned_namespaces
            .iter()
            .map(|namespace| namespace.name.as_str())
            .collect::<Vec<_>>(),
        vec!["root-pinned-catalog"]
    );

    let (_pinned_manifest, pinned_state) = compactor
        .load_state_for_root_token(&receipt.read_token)
        .await?;
    assert!(pinned_state.runs.contains_key("run-root-read-01"));

    let current_catalog = catalog_create_namespace_request(
        "idem-root-read-cat-current-01",
        "req-root-read-cat-current-01",
        "current-only-catalog",
    );
    let (_status, current_catalog_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &current_catalog,
        "idem-root-read-cat-current-01",
        "req-root-read-cat-current-01",
    )
    .await?;
    assert!(current_catalog_response.receipt.is_some());

    let current_orchestration = orchestration_request(
        "idem-root-read-orch-current-01",
        "req-root-read-orch-current-01",
        "run-current-read-01",
    );
    let current_orchestration = CommitOrchestrationBatchRequest {
        events: orchestration_request_with_event_id(
            "idem-root-read-orch-current-01",
            "req-root-read-orch-current-01",
            "run-current-read-01",
            "01JTXORCH000000000000000002",
        )
        .events,
        ..current_orchestration
    };
    let (_status, current_orchestration_response): (_, CommitOrchestrationBatchResponse) =
        post_protobuf(
            router,
            "/api/v1/transactions/commitOrchestrationBatch",
            &current_orchestration,
            "idem-root-read-orch-current-01",
            "req-root-read-orch-current-01",
        )
        .await?;
    assert!(current_orchestration_response.receipt.is_some());

    let mut current_namespaces = reader.list_namespaces().await?;
    current_namespaces.sort_by(|left, right| left.name.cmp(&right.name));
    assert_eq!(
        current_namespaces
            .iter()
            .map(|namespace| namespace.name.as_str())
            .collect::<Vec<_>>(),
        vec!["current-only-catalog", "root-pinned-catalog"]
    );

    let (_current_manifest, current_state) = compactor.load_state().await?;
    assert!(current_state.runs.contains_key("run-root-read-01"));
    assert!(current_state.runs.contains_key("run-current-read-01"));

    let pinned_namespaces_after_current = reader
        .list_namespaces_for_root_token(&receipt.read_token)
        .await?;
    assert_eq!(
        pinned_namespaces_after_current
            .iter()
            .map(|namespace| namespace.name.as_str())
            .collect::<Vec<_>>(),
        vec!["root-pinned-catalog"]
    );

    let (_pinned_manifest_after_current, pinned_state_after_current) = compactor
        .load_state_for_root_token(&receipt.read_token)
        .await?;
    assert!(
        pinned_state_after_current
            .runs
            .contains_key("run-root-read-01")
    );
    assert!(
        !pinned_state_after_current
            .runs
            .contains_key("run-current-read-01")
    );

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
async fn commit_root_transaction_rejects_empty_orchestration_participant() -> Result<()> {
    let router = test_router();
    let mut request = root_request(
        "idem-root-empty-orch-01",
        "req-root-empty-orch-01",
        "empty-orch-root",
        "run-root-empty-orch-01",
    );
    let orchestration = request
        .mutations
        .get_mut(1)
        .and_then(|mutation| mutation.kind.as_mut())
        .and_then(|kind| match kind {
            domain_mutation::Kind::Orchestration(spec) => Some(spec),
            _ => None,
        })
        .context("root request orchestration mutation missing")?;
    orchestration.events.clear();

    let (status, error) = post_error_json(
        router,
        "/api/v1/transactions/commitRootTransaction",
        &request,
        "idem-root-empty-orch-01",
        "req-root-empty-orch-01",
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["code"], "BAD_REQUEST");

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
async fn apply_catalog_ddl_rejects_missing_catalog_operation() -> Result<()> {
    let router = test_router();
    let request = ApplyCatalogDdlRequest {
        ddl: Some(CatalogDdlOperation { op: None }),
    };
    let (status, error) = post_error_json(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &request,
        "idem-cat-props-01",
        "req-cat-props-01",
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["code"], "BAD_REQUEST");

    Ok(())
}

#[tokio::test]
async fn commit_orchestration_batch_rejects_missing_event_payload() -> Result<()> {
    let router = test_router();
    let mut request =
        orchestration_request("idem-orch-inline-01", "req-orch-inline-01", "run-inline-01");
    request.events[0].event = None;

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
async fn apply_catalog_ddl_does_not_create_legacy_catalog_manifest_mirror() -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
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
    assert!(
        !response.repair_pending,
        "catalog visible commits should not report repair_pending after legacy side-effect removal"
    );

    let stored = load_catalog_tx_record(backend.clone(), &receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert!(!stored.repair_pending);
    assert!(
        backend
            .head(&format!(
                "tenant={TENANT}/workspace={WORKSPACE}/{}",
                CatalogPaths::domain_manifest(CatalogDomain::Catalog)
            ))
            .await?
            .is_none(),
        "catalog legacy mutable manifest should not be created"
    );

    Ok(())
}

#[tokio::test]
async fn commit_orchestration_batch_does_not_create_legacy_orchestration_manifest_mirror()
-> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
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
    assert!(
        !response.repair_pending,
        "orchestration visible commits should not report repair_pending after legacy side-effect removal"
    );

    let stored = load_orchestration_tx_record(backend.clone(), &receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert!(!stored.repair_pending);
    assert!(
        backend
            .head(&format!(
                "tenant={TENANT}/workspace={WORKSPACE}/state/orchestration/manifest.json"
            ))
            .await?
            .is_none(),
        "orchestration legacy mutable manifest should not be created"
    );

    Ok(())
}

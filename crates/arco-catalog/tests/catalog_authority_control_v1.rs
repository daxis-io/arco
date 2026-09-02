//! Catalog authority contracts for the first `control/v1` metastore pilot.

#![allow(
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::unwrap_used,
    reason = "contract tests keep setup and end-to-end transactional assertions explicit"
)]

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arco_catalog::state_store::projection_outbox_acks::{
    PROJECTION_OUTBOX_ACK_DOMAIN, ProjectionOutboxAckWriter, ProjectionOutboxWorker,
};
use arco_catalog::{
    ArcoStateReader, CATALOG_PARQUET_PROJECTION_CONSUMER_ID, CatalogAuthorityBinding,
    CatalogAuthorityBindings, CatalogAuthorityKind, CatalogListRequest, CatalogPatch,
    CatalogProjectionMaterializer, CatalogProjectionNotifier, ColumnDefinition,
    ControlCatalogAuthority, ControlMvpProjectionOutboxRecord, ControlMvpStateStore,
    ProjectionIntentV1, RegisterTableInSchemaRequest, SchemaPatch, StateScope, TxnOptions,
    WriteOptions,
};
use arco_core::storage::{ListPage, ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{MemoryBackend, ScopedStorage};
use async_trait::async_trait;
use bytes::Bytes;

struct FailProjectionPutBackend {
    inner: MemoryBackend,
    fail: AtomicBool,
}

struct LoseAcceptedCatalogHeadResponseBackend {
    inner: MemoryBackend,
    lose_next: AtomicBool,
}

#[derive(Default)]
struct RecordingProjectionNotifier {
    calls: AtomicUsize,
    fail: AtomicBool,
}

impl CatalogProjectionNotifier for RecordingProjectionNotifier {
    fn notify(&self, _intent: &ProjectionIntentV1) -> arco_catalog::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            Err(arco_catalog::CatalogError::Storage {
                message: "injected post-commit projection notification failure".to_string(),
            })
        } else {
            Ok(())
        }
    }
}

impl LoseAcceptedCatalogHeadResponseBackend {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryBackend::new(),
            lose_next: AtomicBool::new(false),
        })
    }

    fn arm(&self) {
        self.lose_next.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for LoseAcceptedCatalogHeadResponseBackend {
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
        let result = self.inner.put(path, data, precondition).await?;
        if path.ends_with("/control/v1/domains/catalog/head/current.json")
            && matches!(result, WriteResult::Success { .. })
            && self.lose_next.swap(false, Ordering::SeqCst)
        {
            return Err(arco_core::Error::storage(
                "injected lost response after catalog head acceptance",
            ));
        }
        Ok(result)
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> arco_core::Result<ListPage> {
        self.inner.list_page(prefix, start_after, limit).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

impl FailProjectionPutBackend {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryBackend::new(),
            fail: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl StorageBackend for FailProjectionPutBackend {
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
        if self.fail.load(Ordering::SeqCst) && path.contains("control/v1/projections/") {
            return Err(arco_core::Error::storage(
                "injected projection artifact failure with credential detail redacted",
            ));
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> arco_core::Result<ListPage> {
        self.inner.list_page(prefix, start_after, limit).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

fn scoped_storage() -> ScopedStorage {
    ScopedStorage::new(
        Arc::new(MemoryBackend::new()),
        "synthetic-tenant",
        "synthetic-workspace",
    )
    .expect("scoped storage")
}

fn scope() -> StateScope {
    StateScope::new("synthetic-tenant", "synthetic-workspace", "catalog")
}

#[test]
fn bindings_default_to_legacy_and_select_only_the_exact_pilot_root() {
    let bindings = CatalogAuthorityBindings::new([CatalogAuthorityBinding::control_v1(
        "synthetic-tenant",
        "synthetic-workspace",
    )])
    .expect("valid bindings");

    assert_eq!(
        CatalogAuthorityKind::ControlV1,
        bindings.resolve("synthetic-tenant", "synthetic-workspace")
    );
    assert_eq!(
        CatalogAuthorityKind::Legacy,
        bindings.resolve("synthetic-tenant", "other-workspace")
    );
    assert_eq!(
        CatalogAuthorityKind::Legacy,
        bindings.resolve("other-tenant", "synthetic-workspace")
    );
}

#[test]
fn duplicate_root_bindings_fail_closed() {
    let error = CatalogAuthorityBindings::new([
        CatalogAuthorityBinding::control_v1("synthetic-tenant", "synthetic-workspace"),
        CatalogAuthorityBinding::legacy("synthetic-tenant", "synthetic-workspace"),
    ])
    .expect_err("duplicate binding must fail");

    assert!(
        error
            .to_string()
            .contains("duplicate catalog authority binding")
    );
}

#[tokio::test]
async fn committed_catalog_mutations_notify_best_effort_without_rolling_back_on_failure() {
    let storage = scoped_storage();
    let notifier = Arc::new(RecordingProjectionNotifier::default());
    let authority = ControlCatalogAuthority::new(storage, scope())
        .expect("control authority")
        .with_projection_notifier(notifier.clone());

    authority
        .create_catalog("notified", None, WriteOptions::default())
        .await
        .expect("successful notification leaves commit successful");
    assert_eq!(1, notifier.calls.load(Ordering::SeqCst));

    notifier.fail.store(true, Ordering::SeqCst);
    let created = authority
        .create_catalog("notify-failed", None, WriteOptions::default())
        .await
        .expect("notification failure must not revoke committed authority");
    assert_eq!("notify-failed", created.name);
    assert_eq!(2, notifier.calls.load(Ordering::SeqCst));
    assert!(
        authority
            .get_catalog("notify-failed")
            .await
            .expect("authority read")
            .is_some(),
        "the durable commit remains visible when best-effort notification fails"
    );
}

#[tokio::test]
async fn catalog_pages_are_bounded_name_ordered_and_pinned_across_head_advancement() {
    let storage = scoped_storage();
    let authority =
        ControlCatalogAuthority::new(storage.clone(), scope()).expect("control authority");
    for name in ["a", "aa", "b", "c", "d"] {
        authority
            .create_catalog(name, None, WriteOptions::default())
            .await
            .expect("create catalog");
    }

    let first = authority
        .list_catalogs_page(CatalogListRequest::new(2).expect("request"))
        .await
        .expect("first page");
    assert_eq!(
        vec!["a", "aa"],
        first
            .items()
            .iter()
            .map(|catalog| catalog.name.as_str())
            .collect::<Vec<_>>()
    );
    let token = first.next_page_token().expect("continuation").to_string();
    let pointer: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw("control/v1/domains/catalog/head/current.json")
            .await
            .expect("catalog head"),
    )
    .expect("catalog head JSON");
    let manifest_id = pointer["manifest_id"].as_str().expect("manifest id");
    assert!(
        !token.contains(manifest_id),
        "sealed protocol continuation must not expose the retained manifest"
    );
    let mut tampered = token.clone().into_bytes();
    let last = tampered.last_mut().expect("non-empty continuation");
    *last = if *last == b'A' { b'B' } else { b'A' };
    let tampered = String::from_utf8(tampered).expect("ASCII continuation");
    authority
        .list_catalogs_page(
            CatalogListRequest::new(2)
                .expect("request")
                .with_page_token(tampered),
        )
        .await
        .expect_err("tampered sealed continuation must fail closed");

    authority
        .create_catalog("ab", None, WriteOptions::default())
        .await
        .expect("advance authority");
    let second = authority
        .list_catalogs_page(
            CatalogListRequest::new(2)
                .expect("request")
                .with_page_token(token),
        )
        .await
        .expect("pinned second page");
    assert_eq!(
        vec!["b", "c"],
        second
            .items()
            .iter()
            .map(|catalog| catalog.name.as_str())
            .collect::<Vec<_>>()
    );
    assert!(
        second.items().iter().all(|catalog| catalog.name != "ab"),
        "a continuation must retain the original authority cut"
    );
}

#[tokio::test]
async fn schema_pages_resolve_parent_renames_and_recreation_at_the_retained_cut() {
    let authority =
        ControlCatalogAuthority::new(scoped_storage(), scope()).expect("control authority");

    authority
        .create_catalog("schema-parent", None, WriteOptions::default())
        .await
        .expect("create schema parent");
    for name in ["a", "b", "c"] {
        authority
            .create_schema("schema-parent", name, None, WriteOptions::default())
            .await
            .expect("create schema");
    }
    let first = authority
        .list_schemas_page(
            "schema-parent",
            CatalogListRequest::new(1).expect("request"),
        )
        .await
        .expect("first schema page");
    let renamed_parent_token = first.next_page_token().expect("schema token").to_string();
    authority
        .patch_catalog(
            "schema-parent",
            CatalogPatch {
                new_name: Some("schema-parent-renamed".to_string()),
                ..CatalogPatch::default()
            },
            WriteOptions::default(),
        )
        .await
        .expect("rename catalog parent");
    let second = authority
        .list_schemas_page(
            "schema-parent",
            CatalogListRequest::new(1)
                .expect("request")
                .with_page_token(renamed_parent_token),
        )
        .await
        .expect("schema page after parent rename");
    assert_eq!(
        vec!["b"],
        second
            .items()
            .iter()
            .map(|v| v.name.as_str())
            .collect::<Vec<_>>()
    );

    authority
        .create_catalog("recreated-catalog", None, WriteOptions::default())
        .await
        .expect("create catalog");
    for name in ["old-a", "old-b"] {
        authority
            .create_schema("recreated-catalog", name, None, WriteOptions::default())
            .await
            .expect("create old schema");
    }
    let first = authority
        .list_schemas_page(
            "recreated-catalog",
            CatalogListRequest::new(1).expect("request"),
        )
        .await
        .expect("first old schema page");
    let recreated_parent_token = first.next_page_token().expect("schema token").to_string();
    authority
        .delete_catalog("recreated-catalog", true, WriteOptions::default())
        .await
        .expect("delete catalog");
    authority
        .create_catalog("recreated-catalog", None, WriteOptions::default())
        .await
        .expect("recreate catalog");
    authority
        .create_schema(
            "recreated-catalog",
            "new-only",
            None,
            WriteOptions::default(),
        )
        .await
        .expect("create replacement schema");
    let second = authority
        .list_schemas_page(
            "recreated-catalog",
            CatalogListRequest::new(1)
                .expect("request")
                .with_page_token(recreated_parent_token),
        )
        .await
        .expect("schema page after parent recreation");
    assert_eq!(
        vec!["old-b"],
        second
            .items()
            .iter()
            .map(|v| v.name.as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn table_pages_resolve_parent_renames_and_recreation_at_the_retained_cut() {
    let authority =
        ControlCatalogAuthority::new(scoped_storage(), scope()).expect("control authority");
    authority
        .create_catalog("schema-parent-renamed", None, WriteOptions::default())
        .await
        .expect("create catalog");
    authority
        .create_schema(
            "schema-parent-renamed",
            "table-parent",
            None,
            WriteOptions::default(),
        )
        .await
        .expect("create table parent");
    for name in ["a", "b", "c"] {
        authority
            .register_table_in_schema(
                "schema-parent-renamed",
                "table-parent",
                RegisterTableInSchemaRequest {
                    name: name.to_string(),
                    description: None,
                    location: None,
                    format: Some("parquet".to_string()),
                    table_type: None,
                    properties: None,
                    columns: Vec::new(),
                },
                WriteOptions::default(),
            )
            .await
            .expect("register table");
    }
    let first = authority
        .list_tables_page(
            "schema-parent-renamed",
            "table-parent",
            CatalogListRequest::new(1).expect("request"),
        )
        .await
        .expect("first table page");
    let renamed_schema_token = first.next_page_token().expect("table token").to_string();
    authority
        .patch_schema_in_catalog(
            "schema-parent-renamed",
            "table-parent",
            SchemaPatch {
                new_name: Some("table-parent-renamed".to_string()),
                ..SchemaPatch::default()
            },
            WriteOptions::default(),
        )
        .await
        .expect("rename schema parent");
    let second = authority
        .list_tables_page(
            "schema-parent-renamed",
            "table-parent",
            CatalogListRequest::new(1)
                .expect("request")
                .with_page_token(renamed_schema_token),
        )
        .await
        .expect("table page after schema rename");
    assert_eq!(
        vec!["b"],
        second
            .items()
            .iter()
            .map(|v| v.name.as_str())
            .collect::<Vec<_>>()
    );

    authority
        .create_schema(
            "schema-parent-renamed",
            "recreated-schema",
            None,
            WriteOptions::default(),
        )
        .await
        .expect("create schema");
    for name in ["old-a", "old-b"] {
        authority
            .register_table_in_schema(
                "schema-parent-renamed",
                "recreated-schema",
                RegisterTableInSchemaRequest {
                    name: name.to_string(),
                    description: None,
                    location: None,
                    format: Some("parquet".to_string()),
                    table_type: None,
                    properties: None,
                    columns: Vec::new(),
                },
                WriteOptions::default(),
            )
            .await
            .expect("register old table");
    }
    let first = authority
        .list_tables_page(
            "schema-parent-renamed",
            "recreated-schema",
            CatalogListRequest::new(1).expect("request"),
        )
        .await
        .expect("first old table page");
    let recreated_schema_token = first.next_page_token().expect("table token").to_string();
    authority
        .delete_schema_in_catalog(
            "schema-parent-renamed",
            "recreated-schema",
            true,
            WriteOptions::default(),
        )
        .await
        .expect("delete schema");
    authority
        .create_schema(
            "schema-parent-renamed",
            "recreated-schema",
            None,
            WriteOptions::default(),
        )
        .await
        .expect("recreate schema");
    authority
        .register_table_in_schema(
            "schema-parent-renamed",
            "recreated-schema",
            RegisterTableInSchemaRequest {
                name: "new-only".to_string(),
                description: None,
                location: None,
                format: Some("parquet".to_string()),
                table_type: None,
                properties: None,
                columns: Vec::new(),
            },
            WriteOptions::default(),
        )
        .await
        .expect("register replacement table");
    let second = authority
        .list_tables_page(
            "schema-parent-renamed",
            "recreated-schema",
            CatalogListRequest::new(1)
                .expect("request")
                .with_page_token(recreated_schema_token),
        )
        .await
        .expect("table page after schema recreation");
    assert_eq!(
        vec!["old-b"],
        second
            .items()
            .iter()
            .map(|v| v.name.as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn materializer_publishes_parquet_before_ack_and_recovers_by_anti_entropy() {
    let backend = FailProjectionPutBackend::new();
    let storage = ScopedStorage::new(backend.clone(), "synthetic-tenant", "synthetic-workspace")
        .expect("storage");
    let authority = ControlCatalogAuthority::new(storage.clone(), scope())
        .expect("control authority")
        .with_projection_notifier(Arc::new(RecordingProjectionNotifier::default()));
    authority
        .create_catalog("analytics", None, WriteOptions::default())
        .await
        .expect("create catalog");

    backend.fail.store(true, Ordering::SeqCst);
    let failed = CatalogProjectionMaterializer::new(storage.clone())
        .expect("materializer")
        .drain_once()
        .await
        .expect_err("artifact failure must prevent acknowledgement");
    assert!(failed.to_string().contains("projection artifact failure"));
    let failed_status = CatalogProjectionMaterializer::new(storage.clone())
        .expect("restart")
        .status()
        .await
        .expect("status")
        .expect("failure status");
    assert_eq!(
        Some("retryable:CATALOG_PROJECTION_FAILED"),
        failed_status.failure_state()
    );
    assert!(
        !failed_status
            .failure_state()
            .expect("failure")
            .contains("credential"),
        "durable failure status must be redacted"
    );
    let backlog = ProjectionOutboxWorker::new(
        storage.clone(),
        "catalog",
        CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
    )
    .expect("worker")
    .backlog()
    .await
    .expect("backlog");
    assert_eq!(None, backlog.latest_projected_sequence);
    assert_eq!(1, backlog.pending_record_ids.len());

    backend.fail.store(false, Ordering::SeqCst);
    let restarted = CatalogProjectionMaterializer::new(storage.clone()).expect("restart");
    let drained = restarted.drain_once().await.expect("anti-entropy retry");
    assert_eq!(1, drained.drained_record_ids.len());
    let success = restarted
        .status()
        .await
        .expect("status")
        .expect("success status");
    assert_eq!(None, success.failure_state());
    let manifest_path = success
        .artifact_manifest_path()
        .expect("published artifact manifest");
    assert!(
        storage
            .head_raw(manifest_path)
            .await
            .expect("manifest head")
            .is_some(),
        "acknowledged projection must have a visible materialized manifest"
    );
    let backlog = ProjectionOutboxWorker::new(
        storage.clone(),
        "catalog",
        CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
    )
    .expect("worker")
    .backlog()
    .await
    .expect("backlog");
    assert_eq!(
        success.applied_authority_sequence(),
        backlog.latest_projected_sequence
    );
    assert!(backlog.pending_record_ids.is_empty());
}

#[tokio::test]
async fn malformed_catalog_projection_intent_is_quarantined_without_blocking_later_work() {
    let storage = scoped_storage();
    let store = ControlMvpStateStore::new(storage.clone(), scope()).expect("control store");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "malformed-catalog-intent",
        Bytes::from_static(b"{}"),
    ))
    .expect("stage malformed projection intent");
    let malformed_token = txn
        .commit()
        .await
        .expect("commit malformed intent")
        .into_state_token();
    let malformed_sequence = malformed_token.logical_sequence();

    let incompatible = ProjectionIntentV1::new(
        "wrong-kind-intent",
        "unsupported-catalog-projection",
        &malformed_token,
        b"{}",
    )
    .expect("well-formed incompatible intent");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin incompatible transaction");
    txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "wrong-kind-intent",
        Bytes::from(serde_json::to_vec(&incompatible).expect("encode incompatible intent")),
    ))
    .expect("stage incompatible projection intent");
    let incompatible_sequence = txn
        .commit()
        .await
        .expect("commit incompatible intent")
        .into_state_token()
        .logical_sequence();

    ControlCatalogAuthority::new(storage.clone(), scope())
        .expect("authority")
        .with_projection_notifier(Arc::new(RecordingProjectionNotifier::default()))
        .create_catalog("after-poison", None, WriteOptions::default())
        .await
        .expect("commit valid intent after poison");

    let materializer = CatalogProjectionMaterializer::new(storage.clone()).expect("materializer");
    let report = materializer
        .drain_once()
        .await
        .expect("quarantine must not block later valid work");
    assert_eq!(
        vec!["malformed-catalog-intent", "wrong-kind-intent"],
        report.quarantined_record_ids
    );
    assert_eq!(1, report.drained_record_ids.len());
    let status = materializer
        .status()
        .await
        .expect("status")
        .expect("materialized status");
    assert_eq!(None, status.failure_state());
    let ack_writer = ProjectionOutboxAckWriter::new(
        storage.clone(),
        StateScope::new(
            "synthetic-tenant",
            "synthetic-workspace",
            PROJECTION_OUTBOX_ACK_DOMAIN,
        ),
    )
    .expect("ack writer");
    let quarantine = ack_writer
        .projection_quarantine(CATALOG_PARQUET_PROJECTION_CONSUMER_ID, malformed_sequence)
        .await
        .expect("read quarantine")
        .expect("durable quarantine");
    assert_eq!("malformed-catalog-intent", quarantine.source_record_id());
    assert_eq!("INVALID_PROJECTION_INTENT", quarantine.failure_code());
    let incompatible_quarantine = ack_writer
        .projection_quarantine(
            CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
            incompatible_sequence,
        )
        .await
        .expect("read incompatible quarantine")
        .expect("durable incompatible quarantine");
    assert_eq!(
        "wrong-kind-intent",
        incompatible_quarantine.source_record_id()
    );
    assert_eq!(
        "INCOMPATIBLE_PROJECTION_INTENT",
        incompatible_quarantine.failure_code()
    );
    let backlog = ProjectionOutboxWorker::new(
        storage.clone(),
        "catalog",
        CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
    )
    .expect("worker")
    .backlog()
    .await
    .expect("backlog");
    assert_eq!(
        status.applied_authority_sequence(),
        backlog.latest_projected_sequence
    );
    assert_eq!(
        vec![
            "malformed-catalog-intent".to_string(),
            "wrong-kind-intent".to_string()
        ],
        backlog.pending_record_ids
    );

    let restarted = CatalogProjectionMaterializer::new(storage).expect("restart");
    let retry = restarted.drain_once().await.expect("restart drain");
    assert_eq!(
        vec!["malformed-catalog-intent", "wrong-kind-intent"],
        retry.quarantined_record_ids
    );
    assert_eq!(1, retry.already_acknowledged);
    assert!(retry.drained_record_ids.is_empty());
}

#[tokio::test]
async fn control_authority_keeps_stable_objects_indexes_columns_and_projection_intents_atomic() {
    let storage = scoped_storage();
    let authority =
        ControlCatalogAuthority::new(storage.clone(), scope()).expect("control catalog authority");

    let catalog = authority
        .create_catalog(
            "analytics",
            Some("pilot"),
            WriteOptions::with_idempotency("create-catalog"),
        )
        .await
        .expect("create catalog");
    let schema = authority
        .create_schema(
            "analytics",
            "sales",
            Some("sales schema"),
            WriteOptions::with_idempotency("create-schema"),
        )
        .await
        .expect("create schema");
    let table = authority
        .register_table_in_schema(
            "analytics",
            "sales",
            RegisterTableInSchemaRequest {
                name: "orders".to_string(),
                description: Some("orders table".to_string()),
                location: Some("s3://pilot/orders".to_string()),
                format: Some("delta".to_string()),
                table_type: Some("EXTERNAL".to_string()),
                properties: None,
                columns: vec![
                    ColumnDefinition {
                        name: "order_id".to_string(),
                        data_type: "BIGINT".to_string(),
                        is_nullable: false,
                        ordinal: 0,
                        description: None,
                    },
                    ColumnDefinition {
                        name: "amount".to_string(),
                        data_type: "DECIMAL(18,2)".to_string(),
                        is_nullable: true,
                        ordinal: 1,
                        description: None,
                    },
                ],
            },
            WriteOptions::with_idempotency("register-table"),
        )
        .await
        .expect("register table");

    let renamed = authority
        .rename_table(
            "analytics",
            "sales",
            "orders",
            "orders_v2",
            WriteOptions::with_idempotency("rename-table"),
        )
        .await
        .expect("rename table");
    assert_eq!(table.id, renamed.id);
    assert_eq!(
        catalog.id,
        authority
            .get_catalog("analytics")
            .await
            .unwrap()
            .unwrap()
            .id
    );
    assert_eq!(
        schema.id,
        authority
            .get_schema("analytics", "sales")
            .await
            .unwrap()
            .unwrap()
            .id
    );
    assert!(
        authority
            .get_table("analytics", "sales", "orders")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        renamed.id,
        authority
            .get_table("analytics", "sales", "orders_v2")
            .await
            .unwrap()
            .unwrap()
            .id
    );
    let columns = authority.get_columns(&renamed.id).await.expect("columns");
    assert_eq!(
        vec![(0, "order_id"), (1, "amount")],
        columns
            .iter()
            .map(|column| (column.ordinal, column.name.as_str()))
            .collect::<Vec<_>>()
    );

    let store = ControlMvpStateStore::new(storage, scope()).expect("control store");
    let outbox = store
        .current_projection_outbox()
        .await
        .expect("projection outbox");
    assert_eq!(4, outbox.len());
    assert!(
        outbox
            .iter()
            .all(|record| record.origin_sequence().is_some())
    );

    let object_rows = store
        .scan(arco_catalog::ScanRequest::new(b"\x01"))
        .await
        .expect("object rows");
    let index_rows = store
        .scan(arco_catalog::ScanRequest::new(b"\x02"))
        .await
        .expect("index rows");
    assert!(!object_rows.entries().is_empty());
    assert!(!index_rows.entries().is_empty());
}

#[tokio::test]
async fn idempotency_replay_is_exact_and_mismatched_reuse_conflicts() {
    let authority =
        ControlCatalogAuthority::new(scoped_storage(), scope()).expect("control authority");
    let first = authority
        .create_catalog(
            "analytics",
            Some("first"),
            WriteOptions::with_idempotency("same-key"),
        )
        .await
        .expect("first create");
    let replay = authority
        .create_catalog(
            "analytics",
            Some("first"),
            WriteOptions::with_idempotency("same-key"),
        )
        .await
        .expect("exact replay");
    assert_eq!(first.id, replay.id);

    let error = authority
        .create_catalog(
            "different",
            Some("different request"),
            WriteOptions::with_idempotency("same-key"),
        )
        .await
        .expect_err("mismatched reuse must fail");
    assert!(error.to_string().contains("idempotency"));
}

#[tokio::test]
async fn accepted_head_with_lost_response_reconciles_one_logical_catalog_mutation() {
    let backend = LoseAcceptedCatalogHeadResponseBackend::new();
    let storage = ScopedStorage::new(backend.clone(), "synthetic-tenant", "synthetic-workspace")
        .expect("scoped storage");
    let authority =
        ControlCatalogAuthority::new(storage.clone(), scope()).expect("control authority");

    backend.arm();
    let first = authority
        .create_catalog(
            "analytics",
            Some("lost response"),
            WriteOptions::with_idempotency("lost-response-create"),
        )
        .await
        .expect("accepted transaction must reconcile after its response is lost");
    let replay = authority
        .create_catalog(
            "analytics",
            Some("lost response"),
            WriteOptions::with_idempotency("lost-response-create"),
        )
        .await
        .expect("idempotent replay");
    assert_eq!(first.id, replay.id);
    assert_eq!(first.name, replay.name);
    assert_eq!(first.description, replay.description);
    assert_eq!(first.created_at, replay.created_at);
    assert_eq!(first.updated_at, replay.updated_at);
    assert_eq!(1, authority.list_catalogs().await.expect("catalogs").len());

    let store = ControlMvpStateStore::new(storage, scope()).expect("control store");
    let receipts = store
        .scan(arco_catalog::ScanRequest::new(b"\x03"))
        .await
        .expect("idempotency receipts");
    let audit = store
        .scan(arco_catalog::ScanRequest::new(b"\x04"))
        .await
        .expect("audit records");
    let outbox = store
        .current_projection_outbox()
        .await
        .expect("projection intents");
    assert_eq!(1, receipts.entries().len());
    assert_eq!(1, audit.entries().len());
    assert_eq!(1, outbox.len());
    assert_eq!(Some(1), outbox[0].origin_sequence());
}

#[tokio::test]
async fn renames_keep_stable_parent_ids_and_cascades_remove_every_index_and_column() {
    let authority =
        ControlCatalogAuthority::new(scoped_storage(), scope()).expect("control authority");
    let catalog = authority
        .create_catalog("old", None, WriteOptions::default())
        .await
        .unwrap();
    let schema = authority
        .create_schema("old", "old_schema", None, WriteOptions::default())
        .await
        .unwrap();
    let table = authority
        .register_table_in_schema(
            "old",
            "old_schema",
            RegisterTableInSchemaRequest {
                name: "old_table".to_string(),
                description: None,
                location: Some("s3://pilot/old".to_string()),
                format: Some("parquet".to_string()),
                table_type: None,
                properties: None,
                columns: vec![ColumnDefinition {
                    name: "id".to_string(),
                    data_type: "BIGINT".to_string(),
                    is_nullable: false,
                    ordinal: 0,
                    description: None,
                }],
            },
            WriteOptions::default(),
        )
        .await
        .unwrap();

    let renamed_catalog = authority
        .patch_catalog(
            "old",
            CatalogPatch {
                new_name: Some("new".to_string()),
                ..CatalogPatch::default()
            },
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let renamed_schema = authority
        .patch_schema_in_catalog(
            "new",
            "old_schema",
            SchemaPatch {
                new_name: Some("new_schema".to_string()),
                ..SchemaPatch::default()
            },
            WriteOptions::default(),
        )
        .await
        .unwrap();

    assert_eq!(catalog.id, renamed_catalog.id);
    assert_eq!(schema.id, renamed_schema.id);
    assert_eq!(
        table.id,
        authority
            .get_table("new", "new_schema", "old_table")
            .await
            .unwrap()
            .unwrap()
            .id
    );

    authority
        .delete_catalog("new", true, WriteOptions::default())
        .await
        .expect("cascade catalog delete");
    assert!(authority.list_catalogs().await.unwrap().is_empty());
    assert!(authority.get_columns(&table.id).await.unwrap().is_empty());
    assert!(
        authority
            .get_table("new", "new_schema", "old_table")
            .await
            .unwrap()
            .is_none()
    );
}

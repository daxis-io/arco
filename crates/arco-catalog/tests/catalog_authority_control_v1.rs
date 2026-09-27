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

#[cfg(feature = "test-utils")]
use arco_catalog::catalog_authority::{BoundedCatalogTestCommand, CatalogProjectionNotifierV2};
use arco_catalog::parquet_util::{CatalogAuditRow, read_audit_records};
use arco_catalog::state_store::projection_outbox_acks::{
    PROJECTION_OUTBOX_ACK_DOMAIN, PROJECTION_OUTBOX_TRIM_BINDING_KEY, ProjectionOutboxAckWriter,
    ProjectionOutboxBacklog, ProjectionOutboxWorker,
};
use arco_catalog::{
    ArcoStateAdmin, ArcoStateReader, CATALOG_AUDIT_PROJECTION_PREFIX,
    CATALOG_PARQUET_PROJECTION_CONSUMER_ID, CatalogAuthority, CatalogAuthorityBinding,
    CatalogAuthorityBindings, CatalogAuthorityKind, CatalogError, CatalogListRequest, CatalogPatch,
    CatalogProjectionMaterializer, CatalogProjectionNotifier, ColumnDefinition,
    ControlCatalogAuthority, ControlMvpMaintenanceOutcome, ControlMvpProjectionOutboxRecord,
    ControlMvpStateStore, DurableAuthorityBinding, DurableMaintenanceWorker, MaintenanceStatus,
    ProjectionIntentV1, PurgedCounts, RegisterTableInSchemaRequest, SchemaPatch, StateScope,
    TxnOptions, WriteOptions, catalog_audit_artifact_path,
};
use arco_core::storage::{ListPage, ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{AuthorityRoot, MemoryBackend, ScopedStorage};
use async_trait::async_trait;
use bytes::Bytes;

struct FailProjectionPutBackend {
    inner: MemoryBackend,
    /// Fails every put under `control/v1/projections/`.
    fail: AtomicBool,
    /// Fails only puts under the catalog audit projection prefix.
    fail_audit: AtomicBool,
    /// Fails every put to the projection-outbox ack domain, which holds both
    /// the materialization status and the acknowledgements.
    fail_acks: AtomicBool,
    /// Counts put attempts under the catalog audit projection prefix.
    audit_puts: AtomicUsize,
}

struct LoseAcceptedCatalogHeadResponseBackend {
    inner: MemoryBackend,
    lose_next: AtomicBool,
    gate_next: AtomicBool,
    paused: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    attempts: std::sync::Mutex<Vec<Bytes>>,
}

#[derive(Default)]
struct RecordingProjectionNotifier {
    calls: AtomicUsize,
    fail: AtomicBool,
}

#[cfg(feature = "test-utils")]
#[derive(Default)]
struct NoopProjectionNotifierV2;

#[cfg(feature = "test-utils")]
impl CatalogProjectionNotifierV2 for NoopProjectionNotifierV2 {
    fn notify(
        &self,
        _: &arco_catalog::state_store::ProjectionIntentV2,
    ) -> arco_catalog::Result<()> {
        Ok(())
    }
}

impl CatalogProjectionNotifier for RecordingProjectionNotifier {
    fn notify(&self, _intent: &ProjectionIntentV1) -> arco_catalog::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            Err(CatalogError::Storage {
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
            gate_next: AtomicBool::new(false),
            paused: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
            attempts: std::sync::Mutex::new(Vec::new()),
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
        if path.ends_with("/control/v1/domains/catalog/head/current.json") {
            self.attempts.lock().unwrap().push(data.clone());
            if self.gate_next.swap(false, Ordering::SeqCst) {
                self.paused.notify_one();
                self.resume.notified().await;
            }
        }
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
            fail_audit: AtomicBool::new(false),
            fail_acks: AtomicBool::new(false),
            audit_puts: AtomicUsize::new(0),
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
        let audit = path.contains(CATALOG_AUDIT_PROJECTION_PREFIX);
        if audit {
            self.audit_puts.fetch_add(1, Ordering::SeqCst);
        }
        if self.fail.load(Ordering::SeqCst) && path.contains("control/v1/projections/") {
            return Err(arco_core::Error::storage(
                "injected projection artifact failure with credential detail redacted",
            ));
        }
        if self.fail_audit.load(Ordering::SeqCst) && audit {
            return Err(arco_core::Error::storage("injected audit artifact failure"));
        }
        if self.fail_acks.load(Ordering::SeqCst)
            && path.contains(&format!(
                "control/v1/domains/{PROJECTION_OUTBOX_ACK_DOMAIN}/"
            ))
        {
            return Err(arco_core::Error::storage(
                "injected projection acknowledgement failure",
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

/// Counts projection manifest publications and, when armed, holds the first
/// one until the test releases it so a burst of authority commits lands while
/// exactly one drain pass is in flight.
struct GatedProjectionManifestBackend {
    inner: MemoryBackend,
    manifest_puts: AtomicUsize,
    hold_first: AtomicBool,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl GatedProjectionManifestBackend {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryBackend::new(),
            manifest_puts: AtomicUsize::new(0),
            hold_first: AtomicBool::new(false),
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        })
    }

    fn manifest_puts(&self) -> usize {
        self.manifest_puts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl StorageBackend for GatedProjectionManifestBackend {
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
        if path.contains("control/v1/projections/") && path.ends_with("/manifest.json") {
            self.manifest_puts.fetch_add(1, Ordering::SeqCst);
            if self.hold_first.swap(false, Ordering::SeqCst) {
                self.reached.notify_one();
                self.release.notified().await;
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

/// Commits eight catalog mutations while the first post-commit drain is held
/// at its manifest publication, releases it, and waits until every intent is
/// acknowledged. Returns the backlog observed at settle time.
async fn commit_burst_and_settle(
    backend: &GatedProjectionManifestBackend,
    storage: &ScopedStorage,
    authority: &ControlCatalogAuthority,
) {
    backend.hold_first.store(true, Ordering::SeqCst);
    authority
        .create_catalog("burst-0", None, WriteOptions::default())
        .await
        .expect("first burst commit");
    tokio::time::timeout(Duration::from_secs(10), backend.reached.notified())
        .await
        .expect("the post-commit drain must reach its first manifest publication");
    for index in 1..8 {
        authority
            .create_catalog(&format!("burst-{index}"), None, WriteOptions::default())
            .await
            .expect("burst commit");
    }
    backend.release.notify_one();

    let worker = ProjectionOutboxWorker::new(
        storage.clone(),
        "catalog",
        CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
    )
    .expect("worker");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let backlog = worker.backlog().await.expect("backlog");
        if backlog.pending_record_ids.is_empty()
            && backlog.latest_projected_sequence == Some(8)
            && backlog.committed_sequence == Some(8)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "post-commit drains did not settle: {backlog:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
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

/// The materializer's trim is the fixed-consumer path: it trims exactly the
/// intents its drain acknowledged, installs no binding metadata in the catalog
/// root (which would make every later drain refuse the root), and leaves the
/// following drain working and empty.
#[tokio::test]
async fn materializer_trim_removes_drained_intents_without_binding_the_catalog_root() {
    let storage = scoped_storage();
    let authority = ControlCatalogAuthority::new(storage.clone(), scope())
        .expect("control authority")
        .with_projection_notifier(Arc::new(RecordingProjectionNotifier::default()));
    for name in ["analytics", "finance", "ops"] {
        authority
            .create_catalog(name, None, WriteOptions::default())
            .await
            .expect("create catalog");
    }
    let source = ControlMvpStateStore::new(storage.clone(), scope()).expect("catalog store");
    assert_eq!(
        3,
        source
            .current_projection_outbox()
            .await
            .expect("outbox")
            .len()
    );
    let worker = ProjectionOutboxWorker::new(
        storage.clone(),
        "catalog",
        CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
    )
    .expect("worker");
    let seeded_sequence = worker
        .backlog()
        .await
        .expect("backlog")
        .committed_sequence
        .expect("seeded catalog has committed state");

    let materializer = CatalogProjectionMaterializer::new(storage.clone()).expect("materializer");
    let drained = materializer.drain_once().await.expect("drain");
    assert_eq!(3, drained.drained_record_ids.len());
    assert_eq!(
        Some(seeded_sequence),
        worker.backlog().await.expect("backlog").committed_sequence,
        "the fixed drain commits nothing to the catalog"
    );

    let trimmed = materializer.trim_once().await.expect("trim");
    assert_eq!(drained.drained_record_ids, trimmed.trimmed_record_ids);
    assert_eq!(drained.drained_event_ids, trimmed.trimmed_event_ids);
    assert_eq!(
        Some(seeded_sequence + 1),
        trimmed.trim_sequence,
        "the trim is the only catalog commit after the seed"
    );
    assert!(
        source
            .current_projection_outbox()
            .await
            .expect("outbox after trim")
            .is_empty()
    );
    assert_eq!(
        None,
        source
            .get(PROJECTION_OUTBOX_TRIM_BINDING_KEY)
            .await
            .expect("binding read"),
        "the materializer's trim must not bind the catalog root"
    );
    assert_eq!(None, worker.bound_consumer().await.expect("bound consumer"));

    let again = materializer.drain_once().await.expect("drain after trim");
    assert!(again.drained_record_ids.is_empty());
    assert_eq!(0, again.already_acknowledged);
    assert_eq!(
        Some(seeded_sequence),
        again.latest_projected_sequence,
        "the projection watermark survives ack retirement"
    );
    let idle = materializer.trim_once().await.expect("idle trim");
    assert!(idle.trimmed_record_ids.is_empty());
    assert_eq!(None, idle.trim_sequence);
    assert_eq!(
        trimmed.trim_sequence,
        worker.backlog().await.expect("backlog").committed_sequence,
        "an idle trim commits nothing"
    );
}

/// A burst of commits must not fan out into concurrent drains that all
/// materialize the same intents and race on the ack-root pointer: with the
/// default notifier, every intent is materialized exactly once and acked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_commits_materialize_each_projection_intent_exactly_once() {
    let backend = GatedProjectionManifestBackend::new();
    let storage =
        ScopedStorage::new(backend.clone(), "burst-tenant", "burst-exactly-once").expect("storage");
    let authority = ControlCatalogAuthority::new(
        storage.clone(),
        StateScope::new("burst-tenant", "burst-exactly-once", "catalog"),
    )
    .expect("control authority");

    commit_burst_and_settle(&backend, &storage, &authority).await;

    assert_eq!(
        8,
        backend.manifest_puts(),
        "serialized drains publish each projection manifest exactly once"
    );
}

/// The process-local wake-up serializes drains per root and coalesces the
/// wake-ups that arrive while a drain is running into one further pass.
#[cfg(feature = "test-utils")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_commits_coalesce_into_at_most_three_drain_passes() {
    use arco_catalog::catalog_authority::CatalogProjectionDrainNotifier;

    let backend = GatedProjectionManifestBackend::new();
    let storage =
        ScopedStorage::new(backend.clone(), "burst-tenant", "burst-coalesce").expect("storage");
    let notifier = Arc::new(CatalogProjectionDrainNotifier::new(storage.clone()));
    let authority = ControlCatalogAuthority::new(
        storage.clone(),
        StateScope::new("burst-tenant", "burst-coalesce", "catalog"),
    )
    .expect("control authority")
    .with_projection_notifier(notifier.clone());

    commit_burst_and_settle(&backend, &storage, &authority).await;

    let passes = notifier.drain_passes();
    assert!(
        (1..=3).contains(&passes),
        "eight wake-ups must coalesce into at most three drain passes, got {passes}"
    );
    assert_eq!(8, backend.manifest_puts());
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
    .await
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
    .await
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
    // The later valid intent is applied, but the quarantined events remain
    // unacknowledged backlog that only an operator resolves, so the newest
    // terminal state stays visible instead of being cleared by that success.
    assert_eq!(
        Some("terminal:INCOMPATIBLE_PROJECTION_INTENT"),
        status.failure_state()
    );
    assert!(status.applied_authority_sequence() > Some(incompatible_sequence));
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

/// The audit artifact path of a V1 catalog intent in the outbox, derived
/// independently of the materializer from the intent's source sequence and
/// the UTC day of its audit record's `occurredAtMs`; also pins the public
/// path builder to it.
fn expected_audit_path(record: &ControlMvpProjectionOutboxRecord) -> String {
    let intent: ProjectionIntentV1 =
        serde_json::from_slice(record.payload()).expect("projection intent envelope");
    let occurred_at_ms = intent_audit_record(record)
        .get("occurredAtMs")
        .and_then(serde_json::Value::as_i64)
        .expect("occurredAtMs");
    let day = chrono::DateTime::from_timestamp_millis(occurred_at_ms)
        .expect("occurrence within chrono's range")
        .format("%Y-%m-%d");
    let path = format!(
        "control/v1/projections/catalog-audit/dt={day}/{:020}-{}.parquet",
        intent.source_logical_sequence(),
        intent.intent_id()
    );
    assert_eq!(
        catalog_audit_artifact_path(&intent).expect("audit artifact path"),
        path
    );
    path
}

/// The audit row a V1 catalog intent in the outbox must project to.
fn expected_audit_row(record: &ControlMvpProjectionOutboxRecord) -> CatalogAuditRow {
    let intent: ProjectionIntentV1 =
        serde_json::from_slice(record.payload()).expect("projection intent envelope");
    let audit = intent_audit_record(record);
    let text = |field: &str| {
        audit
            .get(field)
            .and_then(serde_json::Value::as_str)
            .expect(field)
            .to_string()
    };
    CatalogAuditRow {
        record_version: 1,
        operation_id: record.record_id().to_string(),
        operation_family: text("operationFamily"),
        request_digest: text("requestDigest"),
        actor: text("actor"),
        occurred_at_ms: audit
            .get("occurredAtMs")
            .and_then(serde_json::Value::as_i64)
            .expect("occurredAtMs"),
        logical_sequence: record.origin_sequence().expect("committed origin sequence"),
        authority_manifest_id: Some(intent.source_authority_manifest_id().to_string()),
        logical_commit_id: None,
    }
}

/// Every object under the catalog audit projection prefix, sorted.
async fn audit_artifacts(storage: &ScopedStorage) -> Vec<String> {
    let mut paths = storage
        .list(CATALOG_AUDIT_PROJECTION_PREFIX)
        .await
        .expect("list audit projection")
        .into_iter()
        .map(|path| path.as_str().to_string())
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

async fn read_audit_artifact(storage: &ScopedStorage, path: &str) -> Vec<CatalogAuditRow> {
    read_audit_records(&storage.get_raw(path).await.expect("audit artifact bytes"))
        .expect("audit artifact decodes")
}

fn catalog_worker(storage: &ScopedStorage) -> ProjectionOutboxWorker {
    ProjectionOutboxWorker::new(
        storage.clone(),
        "catalog",
        CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
    )
    .expect("worker")
}

#[tokio::test]
async fn materializer_writes_one_audit_artifact_per_intent() {
    let storage = scoped_storage();
    let authority = ControlCatalogAuthority::new(storage.clone(), scope())
        .expect("control authority")
        .with_projection_notifier(Arc::new(RecordingProjectionNotifier::default()));
    authority
        .create_catalog("c", None, WriteOptions::default())
        .await
        .expect("create catalog");
    authority
        .create_schema("c", "s", None, WriteOptions::default())
        .await
        .expect("create schema");
    authority
        .patch_catalog(
            "c",
            CatalogPatch {
                description: Some(Some("patched".to_string())),
                ..CatalogPatch::default()
            },
            WriteOptions::default(),
        )
        .await
        .expect("patch catalog");
    let store = ControlMvpStateStore::new(storage.clone(), scope()).expect("control store");
    let outbox = store
        .current_projection_outbox()
        .await
        .expect("projection intents");
    assert_eq!(3, outbox.len());
    assert!(
        audit_artifacts(&storage).await.is_empty(),
        "nothing is projected before a drain"
    );

    let drained = CatalogProjectionMaterializer::new(storage.clone())
        .expect("materializer")
        .drain_once()
        .await
        .expect("drain");
    assert_eq!(3, drained.drained_record_ids.len());
    let mut expected = outbox.iter().map(expected_audit_path).collect::<Vec<_>>();
    expected.sort();
    assert_eq!(
        expected,
        audit_artifacts(&storage).await,
        "exactly one audit artifact per intent, in the day partition of its occurrence"
    );
    let mut families = Vec::new();
    for record in &outbox {
        let rows = read_audit_artifact(&storage, &expected_audit_path(record)).await;
        assert_eq!(vec![expected_audit_row(record)], rows);
        families.push(rows[0].operation_family.clone());
    }
    families.sort();
    assert_eq!(
        vec!["create_catalog", "create_schema", "patch_catalog"],
        families
    );
}

/// The audit artifact is durable before the acknowledgement: a failed
/// artifact put leaves the intent pending, and the snapshot manifest (written
/// after the audit artifact) is not published either.
#[tokio::test]
async fn audit_artifact_is_written_before_acknowledgement() {
    let backend = FailProjectionPutBackend::new();
    let storage = ScopedStorage::new(backend.clone(), "synthetic-tenant", "synthetic-workspace")
        .expect("storage");
    ControlCatalogAuthority::new(storage.clone(), scope())
        .expect("control authority")
        .with_projection_notifier(Arc::new(RecordingProjectionNotifier::default()))
        .create_catalog("analytics", None, WriteOptions::default())
        .await
        .expect("create catalog");
    let store = ControlMvpStateStore::new(storage.clone(), scope()).expect("control store");
    let record = store
        .current_projection_outbox()
        .await
        .expect("projection intents")
        .into_iter()
        .next()
        .expect("one intent");
    let intent: ProjectionIntentV1 =
        serde_json::from_slice(record.payload()).expect("projection intent envelope");
    let audit_path = expected_audit_path(&record);
    let snapshot_directory = format!(
        "control/v1/projections/catalog-parquet/{:020}-{}/",
        intent.source_logical_sequence(),
        intent.source_authority_manifest_id()
    );
    let worker = catalog_worker(&storage);
    let assert_unacknowledged = |backlog: ProjectionOutboxBacklog| {
        assert_eq!(
            vec![record.record_id().to_string()],
            backlog.pending_record_ids
        );
        assert_eq!(None, backlog.latest_projected_sequence);
    };

    // Every projection put fails: the first snapshot file fails, so the audit
    // put is never attempted, and nothing is acknowledged.
    backend.fail.store(true, Ordering::SeqCst);
    CatalogProjectionMaterializer::new(storage.clone())
        .expect("materializer")
        .drain_once()
        .await
        .expect_err("an artifact failure must prevent acknowledgement");
    backend.fail.store(false, Ordering::SeqCst);
    assert_eq!(0, backend.audit_puts.load(Ordering::SeqCst));
    assert!(storage.head_raw(&audit_path).await.expect("head").is_none());
    assert_unacknowledged(worker.backlog().await.expect("backlog"));

    // Only the audit put fails: the snapshot files land (they are written
    // first), but neither the snapshot manifest nor the acknowledgement does.
    backend.fail_audit.store(true, Ordering::SeqCst);
    let failed = CatalogProjectionMaterializer::new(storage.clone())
        .expect("materializer")
        .drain_once()
        .await
        .expect_err("an audit artifact failure must prevent acknowledgement");
    backend.fail_audit.store(false, Ordering::SeqCst);
    assert!(
        failed.to_string().contains("audit artifact failure"),
        "unexpected error: {failed}"
    );
    assert_eq!(1, backend.audit_puts.load(Ordering::SeqCst));
    assert!(storage.head_raw(&audit_path).await.expect("head").is_none());
    assert!(
        storage
            .head_raw(&format!("{snapshot_directory}catalogs.parquet"))
            .await
            .expect("head")
            .is_some(),
        "the snapshot files precede the audit artifact"
    );
    assert!(
        storage
            .head_raw(&format!("{snapshot_directory}manifest.json"))
            .await
            .expect("head")
            .is_none(),
        "the snapshot manifest follows the audit artifact"
    );
    assert_unacknowledged(worker.backlog().await.expect("backlog"));
    let status = CatalogProjectionMaterializer::new(storage.clone())
        .expect("materializer")
        .status()
        .await
        .expect("status")
        .expect("failure status");
    assert_eq!(
        Some("retryable:CATALOG_PROJECTION_FAILED"),
        status.failure_state()
    );

    // The retry publishes the artifact, then acknowledges.
    let retried = CatalogProjectionMaterializer::new(storage.clone())
        .expect("materializer")
        .drain_once()
        .await
        .expect("anti-entropy retry");
    assert_eq!(
        vec![record.record_id().to_string()],
        retried.drained_record_ids
    );
    assert_eq!(2, backend.audit_puts.load(Ordering::SeqCst));
    assert_eq!(vec![audit_path.clone()], audit_artifacts(&storage).await);
    assert_eq!(
        vec![expected_audit_row(&record)],
        read_audit_artifact(&storage, &audit_path).await
    );
    let backlog = worker.backlog().await.expect("backlog");
    assert!(backlog.pending_record_ids.is_empty());
    assert_eq!(record.origin_sequence(), backlog.latest_projected_sequence);
}

/// At-least-once redelivery: the acknowledgement is lost after the artifacts
/// were published, so the next drain materializes the same intent again and
/// its does-not-exist audit write finds the identical file and accepts it.
#[tokio::test]
async fn redelivered_intent_rewrites_an_identical_audit_artifact() {
    let backend = FailProjectionPutBackend::new();
    let storage = ScopedStorage::new(backend.clone(), "synthetic-tenant", "synthetic-workspace")
        .expect("storage");
    ControlCatalogAuthority::new(storage.clone(), scope())
        .expect("control authority")
        .with_projection_notifier(Arc::new(RecordingProjectionNotifier::default()))
        .create_catalog("analytics", None, WriteOptions::default())
        .await
        .expect("create catalog");
    let store = ControlMvpStateStore::new(storage.clone(), scope()).expect("control store");
    let record = store
        .current_projection_outbox()
        .await
        .expect("projection intents")
        .into_iter()
        .next()
        .expect("one intent");
    let audit_path = expected_audit_path(&record);

    backend.fail_acks.store(true, Ordering::SeqCst);
    CatalogProjectionMaterializer::new(storage.clone())
        .expect("materializer")
        .drain_once()
        .await
        .expect_err("a lost acknowledgement fails the drain after publication");
    backend.fail_acks.store(false, Ordering::SeqCst);
    let published = storage
        .get_raw(&audit_path)
        .await
        .expect("the artifact was published before the lost acknowledgement");
    assert_eq!(1, backend.audit_puts.load(Ordering::SeqCst));
    assert_eq!(
        vec![record.record_id().to_string()],
        catalog_worker(&storage)
            .backlog()
            .await
            .expect("backlog")
            .pending_record_ids,
        "the unacknowledged intent is redelivered"
    );

    let restarted = CatalogProjectionMaterializer::new(storage.clone()).expect("restart");
    let redelivered = restarted
        .drain_once()
        .await
        .expect("the redelivery accepts the identical artifact");
    assert_eq!(
        vec![record.record_id().to_string()],
        redelivered.drained_record_ids
    );
    assert!(
        redelivered.quarantined_record_ids.is_empty(),
        "an identical rewrite is not a precondition failure"
    );
    assert_eq!(
        2,
        backend.audit_puts.load(Ordering::SeqCst),
        "the redelivery attempted the does-not-exist write again"
    );
    assert_eq!(
        published,
        storage.get_raw(&audit_path).await.expect("artifact bytes")
    );
    assert_eq!(vec![audit_path], audit_artifacts(&storage).await);
    let status = restarted
        .status()
        .await
        .expect("status")
        .expect("success status");
    assert_eq!(None, status.failure_state());
    assert!(
        catalog_worker(&storage)
            .backlog()
            .await
            .expect("backlog")
            .pending_record_ids
            .is_empty()
    );
}

/// An existing audit artifact with different bytes is never overwritten: the
/// materializer fails closed exactly as it does for a divergent snapshot
/// manifest, so the intent is quarantined and the manifest is not published.
#[tokio::test]
async fn a_divergent_audit_artifact_fails_closed_without_acknowledging() {
    let storage = scoped_storage();
    ControlCatalogAuthority::new(storage.clone(), scope())
        .expect("control authority")
        .with_projection_notifier(Arc::new(RecordingProjectionNotifier::default()))
        .create_catalog("analytics", None, WriteOptions::default())
        .await
        .expect("create catalog");
    let store = ControlMvpStateStore::new(storage.clone(), scope()).expect("control store");
    let record = store
        .current_projection_outbox()
        .await
        .expect("projection intents")
        .into_iter()
        .next()
        .expect("one intent");
    let intent: ProjectionIntentV1 =
        serde_json::from_slice(record.payload()).expect("projection intent envelope");
    let audit_path = expected_audit_path(&record);
    let snapshot_directory = format!(
        "control/v1/projections/catalog-parquet/{:020}-{}/",
        intent.source_logical_sequence(),
        intent.source_authority_manifest_id()
    );
    let foreign = Bytes::from_static(b"not the projected audit record");
    storage
        .put_raw(
            &audit_path,
            foreign.clone(),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("seed a divergent artifact");

    let materializer = CatalogProjectionMaterializer::new(storage.clone()).expect("materializer");
    let report = materializer
        .drain_once()
        .await
        .expect("a quarantine is not a drain failure");
    assert_eq!(
        vec![record.record_id().to_string()],
        report.quarantined_record_ids
    );
    assert!(report.drained_record_ids.is_empty());
    assert_eq!(
        foreign,
        storage.get_raw(&audit_path).await.expect("artifact bytes"),
        "the divergent artifact is left untouched"
    );
    assert!(
        storage
            .head_raw(&format!("{snapshot_directory}catalogs.parquet"))
            .await
            .expect("head")
            .is_some(),
        "the snapshot files landed, so the quarantine came from the audit step"
    );
    assert!(
        storage
            .head_raw(&format!("{snapshot_directory}manifest.json"))
            .await
            .expect("head")
            .is_none(),
        "the snapshot manifest follows the audit artifact"
    );
    assert_eq!(
        Some("terminal:INCOMPATIBLE_PROJECTION_INTENT"),
        materializer
            .status()
            .await
            .expect("status")
            .expect("quarantine status")
            .failure_state()
    );
    let quarantine = ProjectionOutboxAckWriter::new(
        storage.clone(),
        StateScope::new(
            "synthetic-tenant",
            "synthetic-workspace",
            PROJECTION_OUTBOX_ACK_DOMAIN,
        ),
    )
    .expect("ack writer")
    .projection_quarantine(
        CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
        record.origin_sequence().expect("committed origin sequence"),
    )
    .await
    .expect("read quarantine")
    .expect("durable quarantine");
    assert_eq!(record.record_id(), quarantine.source_record_id());
    assert_eq!("INCOMPATIBLE_PROJECTION_INTENT", quarantine.failure_code());
    assert_eq!(
        vec![record.record_id().to_string()],
        catalog_worker(&storage)
            .backlog()
            .await
            .expect("backlog")
            .pending_record_ids,
        "a quarantined intent is never acknowledged"
    );
}

#[tokio::test]
async fn an_intent_whose_payload_is_not_an_audit_record_is_quarantined_without_an_artifact() {
    let storage = scoped_storage();
    let store = ControlMvpStateStore::new(storage.clone(), scope()).expect("control store");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.stage_projection_intent(
        "x",
        CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
        Bytes::from_static(b"junk"),
    )
    .await
    .expect("stage a well-formed intent with a junk payload");
    let junk_sequence = txn
        .commit()
        .await
        .expect("commit junk intent")
        .into_state_token()
        .logical_sequence();

    let materializer = CatalogProjectionMaterializer::new(storage.clone()).expect("materializer");
    let report = materializer
        .drain_once()
        .await
        .expect("a quarantine is not a drain failure");
    assert_eq!(vec!["x"], report.quarantined_record_ids);
    assert!(report.drained_record_ids.is_empty());
    let failure_state = materializer
        .status()
        .await
        .expect("status")
        .expect("quarantine status")
        .failure_state()
        .map(str::to_string);
    assert!(
        failure_state
            .as_deref()
            .is_some_and(|state| state.starts_with("terminal:")),
        "expected a terminal quarantine, got {failure_state:?}"
    );
    let quarantine = ProjectionOutboxAckWriter::new(
        storage.clone(),
        StateScope::new(
            "synthetic-tenant",
            "synthetic-workspace",
            PROJECTION_OUTBOX_ACK_DOMAIN,
        ),
    )
    .expect("ack writer")
    .projection_quarantine(CATALOG_PARQUET_PROJECTION_CONSUMER_ID, junk_sequence)
    .await
    .expect("read quarantine")
    .expect("durable quarantine");
    assert_eq!("x", quarantine.source_record_id());
    assert_eq!("INCOMPATIBLE_PROJECTION_INTENT", quarantine.failure_code());
    assert!(audit_artifacts(&storage).await.is_empty());
    assert!(
        storage
            .list("control/v1/projections/")
            .await
            .expect("list projections")
            .is_empty(),
        "the payload is rejected before any snapshot file is written"
    );

    ControlCatalogAuthority::new(storage.clone(), scope())
        .expect("control authority")
        .with_projection_notifier(Arc::new(RecordingProjectionNotifier::default()))
        .create_catalog("after-junk", None, WriteOptions::default())
        .await
        .expect("commit a real mutation after the junk intent");
    let record = store
        .current_projection_outbox()
        .await
        .expect("projection intents")
        .into_iter()
        .find(|record| record.record_id() != "x")
        .expect("the real mutation's intent");
    let report = materializer
        .drain_once()
        .await
        .expect("the junk intent does not block later work");
    assert_eq!(
        vec![record.record_id().to_string()],
        report.drained_record_ids
    );
    assert_eq!(vec!["x"], report.quarantined_record_ids);
    let audit_path = expected_audit_path(&record);
    assert_eq!(vec![audit_path.clone()], audit_artifacts(&storage).await);
    assert_eq!(
        vec![expected_audit_row(&record)],
        read_audit_artifact(&storage, &audit_path).await
    );
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

/// Decodes the catalog audit record carried by a V1 projection intent in the
/// outbox.
fn intent_audit_record(record: &ControlMvpProjectionOutboxRecord) -> serde_json::Value {
    let intent: ProjectionIntentV1 =
        serde_json::from_slice(record.payload()).expect("projection intent envelope");
    assert_eq!(
        intent.projection_kind(),
        CATALOG_PARQUET_PROJECTION_CONSUMER_ID
    );
    let audit: serde_json::Value =
        serde_json::from_slice(intent.payload()).expect("audit record json");
    assert_eq!(audit.get("version"), Some(&serde_json::json!(1)));
    audit
}

/// Returns the `operationFamily` of the audit record a V1 projection intent
/// in the outbox carries.
fn intent_audit_family(record: &ControlMvpProjectionOutboxRecord) -> String {
    intent_audit_record(record)
        .get("operationFamily")
        .and_then(serde_json::Value::as_str)
        .expect("operation family")
        .to_string()
}

#[tokio::test]
async fn shared_idempotency_key_across_families_keeps_every_intent_and_no_audit_row() {
    let storage = scoped_storage();
    let authority =
        ControlCatalogAuthority::new(storage.clone(), scope()).expect("control authority");
    authority
        .create_catalog("c", None, WriteOptions::default())
        .await
        .expect("create catalog");
    authority
        .create_schema("c", "s", None, WriteOptions::with_idempotency("shared-key"))
        .await
        .expect("create schema with the shared key");
    authority
        .register_table_in_schema(
            "c",
            "s",
            RegisterTableInSchemaRequest {
                name: "t".to_string(),
                description: None,
                location: None,
                format: Some("parquet".to_string()),
                table_type: None,
                properties: None,
                columns: Vec::new(),
            },
            WriteOptions::with_idempotency("shared-key"),
        )
        .await
        .expect("a different operation family may reuse the same idempotency key");

    let store = ControlMvpStateStore::new(storage, scope()).expect("control store");
    let audit = store
        .scan(arco_catalog::ScanRequest::new(b"\x04"))
        .await
        .expect("audit key scan");
    assert!(
        audit.entries().is_empty(),
        "audit records are projection-only since retention step 3; found {} tag-4 rows",
        audit.entries().len()
    );
    let outbox = store
        .current_projection_outbox()
        .await
        .expect("projection intents");
    assert_eq!(3, outbox.len());
    let mut families = outbox.iter().map(intent_audit_family).collect::<Vec<_>>();
    families.sort();
    assert_eq!(
        vec!["create_catalog", "create_schema", "register_table"],
        families,
        "every mutation keeps its own audit intent when a key is shared across families"
    );
}

/// Drives one `RetentionHorizon` job through the kernel at `now` and returns
/// the publication outcome.
async fn publish_retention_horizon_at(
    storage: ScopedStorage,
    now: chrono::DateTime<chrono::Utc>,
) -> ControlMvpMaintenanceOutcome {
    let worker =
        DurableMaintenanceWorker::new(storage, scope(), DurableAuthorityBinding::new([7; 32]))
            .expect("maintenance worker");
    let plan = worker
        .prepare_horizon_at(now)
        .await
        .expect("horizon preflight")
        .expect("an expired receipt makes the horizon eligible");
    let job_id = plan.job_id().clone();
    let mut progress = worker.start_at(&plan, now).await.expect("start horizon");
    while progress.status == MaintenanceStatus::Active {
        progress = worker
            .advance_at(&job_id, now)
            .await
            .expect("advance horizon");
    }
    assert_eq!(progress.status, MaintenanceStatus::ReadyToPublish);
    worker
        .publish_at(&job_id, now)
        .await
        .expect("publish horizon")
        .expect("the horizon publishes over an uncontended head")
}

/// Observation used after the purge: with no receipt left, the keyed replay
/// is no longer short-circuited and the adapter re-executes `create_catalog`,
/// which now collides with the catalog the first application created. The
/// first application's intent is still retained (nothing trims it here), but
/// the command's name check runs before the commit records are staged, so the
/// catalog conflict fires before the projection-intent id could collide. The
/// failed re-execution commits nothing, so the receipt prefix stays empty.
#[tokio::test]
async fn receipts_expire_after_the_retention_window_and_the_replay_reapplies() {
    let storage = scoped_storage();
    let authority =
        ControlCatalogAuthority::new(storage.clone(), scope()).expect("control authority");
    let request = || {
        authority.create_catalog(
            "analytics",
            Some("first"),
            WriteOptions::with_idempotency("create-analytics"),
        )
    };
    let first = request().await.expect("first create");
    let replay = request()
        .await
        .expect("exact replay within the retention window");
    assert_eq!(first.id, replay.id);
    assert_eq!(first.created_at, replay.created_at);
    assert_eq!(first.updated_at, replay.updated_at);

    let store = ControlMvpStateStore::new(storage.clone(), scope()).expect("control store");
    let receipts = store
        .scan(arco_catalog::ScanRequest::new(b"\x03"))
        .await
        .expect("receipt scan");
    assert_eq!(
        1,
        receipts.entries().len(),
        "one keyed mutation, one receipt"
    );

    let after_retention = chrono::Utc::now() + chrono::Duration::hours(26);
    let outcome = publish_retention_horizon_at(storage.clone(), after_retention).await;
    assert_eq!(
        outcome.purged_counts(),
        Some(PurgedCounts {
            expired_rows: 1,
            tombstones: 0,
        }),
        "the horizon purges exactly the expired receipt"
    );
    let receipts = store
        .scan(arco_catalog::ScanRequest::new(b"\x03"))
        .await
        .expect("receipt scan after the horizon");
    assert!(
        receipts.entries().is_empty(),
        "the purged receipt is no longer visible"
    );

    let reapplied = request()
        .await
        .expect_err("without a receipt the same request re-executes and collides");
    assert!(
        matches!(
            &reapplied,
            CatalogError::AlreadyExists { entity, name }
                if entity == "catalog" && name == "analytics"
        ),
        "expected the non-idempotent catalog name conflict, got {reapplied:?}"
    );
    let receipts = store
        .scan(arco_catalog::ScanRequest::new(b"\x03"))
        .await
        .expect("receipt scan after the re-execution");
    assert!(
        receipts.entries().is_empty(),
        "a failed re-execution commits no receipt"
    );
    assert_eq!(1, authority.list_catalogs().await.expect("catalogs").len());
}

/// Keyed operation ids are deterministic per (family, idempotency key), and
/// the projection intent id is the operation id. Once the receipt is purged,
/// a keyed replay re-executes under the same id: while the first intent is
/// still retained in the outbox, staging fails closed with a projection-intent
/// conflict before anything commits; once the materializer drains and trims
/// that intent, the replay commits and stages a fresh outbox incarnation of
/// the same id at a higher origin sequence. Each incarnation gets its own
/// audit artifact: the projection identity is `(operation_id,
/// logical_sequence)`, not the operation id alone.
#[tokio::test]
async fn a_purged_receipt_lets_a_keyed_patch_reapply_once_its_intent_is_trimmed() {
    let storage = scoped_storage();
    let authority = ControlCatalogAuthority::new(storage.clone(), scope())
        .expect("control authority")
        .with_projection_notifier(Arc::new(RecordingProjectionNotifier::default()));
    authority
        .create_catalog("c", None, WriteOptions::default())
        .await
        .expect("unkeyed create");
    let patch = || {
        authority.patch_catalog(
            "c",
            CatalogPatch {
                description: Some(Some("patched".to_string())),
                ..CatalogPatch::default()
            },
            WriteOptions::with_idempotency("k"),
        )
    };
    let first = patch().await.expect("keyed patch");

    let store = ControlMvpStateStore::new(storage.clone(), scope()).expect("control store");
    let receipt_count = || async {
        store
            .scan(arco_catalog::ScanRequest::new(b"\x03"))
            .await
            .expect("receipt scan")
            .entries()
            .len()
    };
    assert_eq!(
        2,
        receipt_count().await,
        "unkeyed mutations write a receipt too (keyed by their random operation id)"
    );
    let outbox = store
        .current_projection_outbox()
        .await
        .expect("projection intents");
    assert_eq!(2, outbox.len());
    let first_intent = outbox
        .iter()
        .find(|record| intent_audit_family(record) == "patch_catalog")
        .expect("the keyed patch's intent");
    let operation_id = first_intent.record_id().to_string();
    let first_origin = first_intent
        .origin_sequence()
        .expect("committed intent carries its origin sequence");
    let first_audit_path = expected_audit_path(first_intent);
    let first_audit_row = expected_audit_row(first_intent);

    let after_retention = chrono::Utc::now() + chrono::Duration::hours(26);
    let outcome = publish_retention_horizon_at(storage.clone(), after_retention).await;
    assert_eq!(
        outcome.purged_counts(),
        Some(PurgedCounts {
            expired_rows: 2,
            tombstones: 0,
        }),
        "the horizon purges both receipts: every receipt carries the expiry hint"
    );
    assert_eq!(0, receipt_count().await);

    let blocked = patch()
        .await
        .expect_err("the retained intent blocks a re-execution under the same id");
    assert!(
        matches!(
            &blocked,
            CatalogError::AlreadyExists { entity, name }
                if entity == "projection intent" && name == &operation_id
        ),
        "expected the projection-intent id conflict, got {blocked:?}"
    );
    assert_eq!(
        0,
        receipt_count().await,
        "a replay that fails closed commits no receipt"
    );

    let materializer = CatalogProjectionMaterializer::new(storage.clone()).expect("materializer");
    let drained = materializer.drain_once().await.expect("drain");
    assert_eq!(2, drained.drained_record_ids.len());
    let trimmed = materializer.trim_once().await.expect("trim");
    assert_eq!(drained.drained_record_ids, trimmed.trimmed_record_ids);
    assert!(
        store
            .current_projection_outbox()
            .await
            .expect("outbox after trim")
            .is_empty()
    );

    let reapplied = patch()
        .await
        .expect("once the intent is trimmed the keyed patch re-executes");
    assert_eq!(first.id, reapplied.id);
    assert_eq!(Some("patched".to_string()), reapplied.description);
    assert_eq!(
        1,
        receipt_count().await,
        "the re-execution writes one receipt"
    );
    let outbox = store
        .current_projection_outbox()
        .await
        .expect("outbox after the re-execution");
    assert_eq!(1, outbox.len());
    assert_eq!(operation_id, outbox[0].record_id());
    assert_eq!("patch_catalog", intent_audit_family(&outbox[0]));
    let second_origin = outbox[0]
        .origin_sequence()
        .expect("committed intent carries its origin sequence");
    // create (1) and patch (2) committed; the horizon publication keeps the
    // logical sequence, the trim commits 3, and the re-execution commits 4.
    assert_eq!(2, first_origin, "the original patch intent's incarnation");
    assert_eq!(
        store
            .current_state_token()
            .await
            .expect("head after the re-execution")
            .logical_sequence(),
        second_origin,
        "the re-staged intent is a fresh incarnation at the re-execution's sequence"
    );
    assert_eq!(4, second_origin);

    // Both incarnations are audited, each under its own sequence (and the day
    // partition of its own occurrence).
    let second_audit_path = expected_audit_path(&outbox[0]);
    let second_audit_row = expected_audit_row(&outbox[0]);
    let redrained = materializer.drain_once().await.expect("drain re-execution");
    assert_eq!(vec![operation_id.clone()], redrained.drained_record_ids);
    let suffix = format!("-{operation_id}.parquet");
    let operation_artifacts = audit_artifacts(&storage)
        .await
        .into_iter()
        .filter(|path| path.ends_with(&suffix))
        .collect::<Vec<_>>();
    let mut expected_paths = vec![first_audit_path.clone(), second_audit_path.clone()];
    expected_paths.sort();
    assert_eq!(
        expected_paths, operation_artifacts,
        "one audit artifact per (operation_id, logical_sequence)"
    );
    assert_eq!(2, first_audit_row.logical_sequence);
    assert_eq!(4, second_audit_row.logical_sequence);
    assert_eq!(
        vec![first_audit_row],
        read_audit_artifact(&storage, &first_audit_path).await
    );
    assert_eq!(
        vec![second_audit_row],
        read_audit_artifact(&storage, &second_audit_path).await
    );
}

/// The horizon purges a receipt only when its expiry (`occurredAtMs` + 24 h)
/// lies strictly before the horizon clock minus the one-hour skew margin:
/// not at `occurredAtMs` + 25 h, but one millisecond later.
#[tokio::test]
async fn a_receipt_becomes_purge_eligible_strictly_after_twenty_five_hours() {
    let storage = scoped_storage();
    let authority = ControlCatalogAuthority::new(storage.clone(), scope())
        .expect("control authority")
        .with_projection_notifier(Arc::new(RecordingProjectionNotifier::default()));
    authority
        .create_catalog("analytics", None, WriteOptions::with_idempotency("k"))
        .await
        .expect("keyed create");
    let store = ControlMvpStateStore::new(storage.clone(), scope()).expect("control store");
    let outbox = store
        .current_projection_outbox()
        .await
        .expect("projection intents");
    assert_eq!(1, outbox.len());
    let occurred_at_ms = intent_audit_record(&outbox[0])
        .get("occurredAtMs")
        .and_then(serde_json::Value::as_i64)
        .expect("audit occurredAtMs");
    let occurred_at =
        chrono::DateTime::from_timestamp_millis(occurred_at_ms).expect("valid occurredAtMs");
    let boundary = occurred_at + chrono::Duration::hours(25);

    let worker = DurableMaintenanceWorker::new(
        storage.clone(),
        scope(),
        DurableAuthorityBinding::new([7; 32]),
    )
    .expect("maintenance worker");
    assert!(
        worker
            .prepare_horizon_at(boundary)
            .await
            .expect("horizon preflight at the boundary")
            .is_none(),
        "expiry == cutoff is not strictly before it: nothing is eligible"
    );
    assert!(
        worker
            .prepare_horizon_at(boundary + chrono::Duration::milliseconds(1))
            .await
            .expect("horizon preflight past the boundary")
            .is_some(),
        "one millisecond past the boundary the receipt is eligible"
    );
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
        .expect("audit key scan");
    let outbox = store
        .current_projection_outbox()
        .await
        .expect("projection intents");
    assert_eq!(1, receipts.entries().len());
    assert!(
        audit.entries().is_empty(),
        "audit records are projection-only since retention step 3"
    );
    assert_eq!(1, outbox.len());
    assert_eq!(Some(1), outbox[0].origin_sequence());
    assert_eq!("create_catalog", intent_audit_family(&outbox[0]));
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

#[tokio::test]
async fn gate4_cas_loss_reexecutes_decisions_and_regenerates_receipt_response_and_intent() {
    let backend = LoseAcceptedCatalogHeadResponseBackend::new();
    let storage =
        ScopedStorage::new(backend.clone(), "synthetic-tenant", "synthetic-workspace").unwrap();
    let authority = ControlCatalogAuthority::new(storage.clone(), scope()).unwrap();
    authority
        .create_catalog(
            "analytics",
            Some("before"),
            WriteOptions::with_idempotency("seed"),
        )
        .await
        .unwrap();
    backend.gate_next.store(true, Ordering::SeqCst);
    let properties =
        std::collections::BTreeMap::from([("pending-property".to_string(), "value".to_string())]);
    let pending = authority.patch_catalog(
        "analytics",
        CatalogPatch {
            properties: Some(Some(properties)),
            ..CatalogPatch::default()
        },
        WriteOptions::with_idempotency("pending"),
    );
    tokio::pin!(pending);
    tokio::select! {
        result = &mut pending => panic!("pending command finished before the CAS gate: {result:?}"),
        () = backend.paused.notified() => {},
    }
    authority
        .patch_catalog(
            "analytics",
            CatalogPatch {
                description: Some(Some("concurrent".to_string())),
                ..CatalogPatch::default()
            },
            WriteOptions::with_idempotency("concurrent"),
        )
        .await
        .unwrap();
    backend.resume.notify_one();
    let response = pending.await.unwrap();
    assert_eq!(
        response.description.as_deref(),
        Some("concurrent"),
        "retry must rebuild its response from the winning base"
    );
    let repeated = authority
        .patch_catalog(
            "analytics",
            CatalogPatch {
                properties: Some(Some(std::collections::BTreeMap::from([(
                    "pending-property".to_string(),
                    "value".to_string(),
                )]))),
                ..CatalogPatch::default()
            },
            WriteOptions::with_idempotency("pending"),
        )
        .await
        .unwrap();
    assert_eq!(
        repeated.description, response.description,
        "receipt contains the regenerated response"
    );
    let store = ControlMvpStateStore::new(storage, scope()).unwrap();
    let token = store.current_state_token().await.unwrap();
    assert_eq!(token.logical_sequence(), 3);
    let records = store.current_projection_outbox().await.unwrap();
    assert_eq!(records.len(), 3);
    let latest = records.last().unwrap();
    assert_eq!(latest.origin_sequence(), Some(3));
    let intent: ProjectionIntentV1 = serde_json::from_slice(latest.payload()).unwrap();
    assert_eq!(
        intent.source_authority_manifest_id(),
        token.authority_manifest_id()
    );
    let attempts = {
        let attempts = backend.attempts.lock().unwrap();
        assert_eq!(attempts.len(), 4);
        attempts
            .iter()
            .map(|b| serde_json::from_slice::<serde_json::Value>(b).unwrap())
            .collect::<Vec<_>>()
    };
    let manifests = attempts
        .iter()
        .map(|v| v["manifest_id"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        manifests.len(),
        4,
        "the lost attempt cannot reuse candidate objects"
    );
    let receipts = store
        .scan(arco_catalog::ScanRequest::new(b"\x03"))
        .await
        .unwrap();
    assert_eq!(receipts.entries().len(), 3);
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn bounded_v2_frozen_patch_reexecutes_after_competing_cas_with_same_identity() {
    let backend = LoseAcceptedCatalogHeadResponseBackend::new();
    let storage = ScopedStorage::new(backend.clone(), "synthetic-tenant", "synthetic-workspace")
        .expect("storage");
    let authority = ControlCatalogAuthority::new_synthetic_bounded(
        storage.clone(),
        scope(),
        Arc::new(NoopProjectionNotifierV2),
    )
    .expect("bounded authority");
    authority
        .create_catalog_v2(
            "analytics",
            Some("before"),
            WriteOptions::with_idempotency("seed"),
        )
        .await
        .expect("seed catalog");
    let command = authority
        .prepare_synthetic_bounded_command(
            BoundedCatalogTestCommand::PatchCatalog {
                name: "analytics".to_string(),
                patch: CatalogPatch {
                    properties: Some(Some(std::collections::BTreeMap::from([(
                        "pending-property".to_string(),
                        "value".to_string(),
                    )]))),
                    ..CatalogPatch::default()
                },
            },
            WriteOptions::with_idempotency("bounded-pending").with_request_id("bounded-request"),
        )
        .expect("freeze command before CAS");
    let operation_id = command.operation_id().to_string();
    let request_digest = command.request_digest().to_string();
    let occurred_at_ms = command.occurred_at_ms();
    backend.gate_next.store(true, Ordering::SeqCst);
    let pending = authority.execute_prepared_synthetic_bounded_command(command.clone());
    tokio::pin!(pending);
    tokio::select! {
        result = &mut pending => panic!("bounded command finished before the CAS gate: {result:?}"),
        () = backend.paused.notified() => {},
    }
    authority
        .patch_catalog(
            "analytics",
            CatalogPatch {
                description: Some(Some("concurrent".to_string())),
                ..CatalogPatch::default()
            },
            WriteOptions::with_idempotency("bounded-concurrent"),
        )
        .await
        .expect("competing V2 patch");
    backend.resume.notify_one();
    pending.await.expect("frozen bounded retry");
    let store = ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope())
        .expect("bounded store");
    let published = store
        .export_published_logical_v2_current()
        .await
        .expect("published frozen logical operation");
    assert_eq!(published["operation"]["operationId"], operation_id);
    assert_eq!(published["operation"]["requestDigest"], request_digest);
    let catalog = authority
        .get_catalog("analytics")
        .await
        .expect("catalog lookup")
        .expect("catalog exists");
    assert_eq!(catalog.description.as_deref(), Some("concurrent"));
    assert_eq!(
        catalog.properties.as_ref().unwrap()["pending-property"],
        "value"
    );
    assert_eq!(catalog.updated_at, occurred_at_ms);
    let token_before_replay = store.current_state_token().await.expect("published token");
    authority
        .execute_prepared_synthetic_bounded_command(command)
        .await
        .expect("exact frozen replay");
    assert!(!operation_id.is_empty());
    assert_eq!(request_digest.len(), 64);
    assert_eq!(
        store
            .current_state_token()
            .await
            .expect("token after exact replay"),
        token_before_replay,
    );
    assert_eq!(backend.attempts.lock().unwrap().len(), 4);
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn bounded_v2_recovers_accepted_head_after_lost_response() {
    let backend = LoseAcceptedCatalogHeadResponseBackend::new();
    let storage = ScopedStorage::new(backend.clone(), "synthetic-tenant", "synthetic-workspace")
        .expect("storage");
    let authority = ControlCatalogAuthority::new_synthetic_bounded(
        storage.clone(),
        scope(),
        Arc::new(NoopProjectionNotifierV2),
    )
    .expect("bounded authority");
    authority
        .create_catalog_v2(
            "analytics",
            Some("before"),
            WriteOptions::with_idempotency("seed"),
        )
        .await
        .expect("seed catalog");
    backend.arm();
    authority
        .patch_catalog(
            "analytics",
            CatalogPatch {
                description: Some(Some("after".to_string())),
                ..CatalogPatch::default()
            },
            WriteOptions::with_idempotency("lost-response"),
        )
        .await
        .expect("accepted V2 HEAD must reconcile to committed outcome");
    let store = ControlMvpStateStore::new_synthetic_bounded(storage, scope()).expect("store");
    let token = store.current_state_token().await.expect("published token");
    assert_eq!(token.logical_sequence(), 2);
    assert_eq!(backend.attempts.lock().unwrap().len(), 2);
    authority
        .patch_catalog(
            "analytics",
            CatalogPatch {
                description: Some(Some("after".to_string())),
                ..CatalogPatch::default()
            },
            WriteOptions::with_idempotency("lost-response"),
        )
        .await
        .expect("idempotent V2 replay");
    assert_eq!(
        store.current_state_token().await.expect("replay token"),
        token
    );
    assert_eq!(backend.attempts.lock().unwrap().len(), 2);
}

#[test]
fn bindings_distinguish_equal_textual_workspace_and_metastore_roots() {
    let bindings = CatalogAuthorityBindings::new([
        CatalogAuthorityBinding::control_v1("acme", "lakehouse"),
        CatalogAuthorityBinding::new(
            "acme",
            AuthorityRoot::Metastore {
                metastore_id: "lakehouse".to_string(),
            },
            CatalogAuthorityKind::ControlV1,
        ),
    ])
    .expect("distinct root families");

    assert_eq!(
        CatalogAuthorityKind::ControlV1,
        bindings.resolve_root(
            "acme",
            &AuthorityRoot::Workspace {
                workspace_id: "lakehouse".to_string()
            }
        )
    );
    assert_eq!(
        CatalogAuthorityKind::ControlV1,
        bindings.resolve_root(
            "acme",
            &AuthorityRoot::Metastore {
                metastore_id: "lakehouse".to_string()
            }
        )
    );
    assert_eq!(
        CatalogAuthorityKind::Legacy,
        bindings.resolve("acme", "other")
    );
}

#[test]
fn catalog_bindings_reject_non_catalog_roots() {
    let error = CatalogAuthorityBindings::new([CatalogAuthorityBinding::new(
        "acme",
        AuthorityRoot::TenantIdentity,
        CatalogAuthorityKind::ControlV1,
    )])
    .expect_err("identity is not a catalog authority root");
    assert!(error.to_string().contains("workspace and metastore"));
}

#[tokio::test]
async fn control_v1_bound_rejects_a_metastore_scope_without_a_metastore_binding() {
    let bindings =
        CatalogAuthorityBindings::new([CatalogAuthorityBinding::control_v1("acme", "lakehouse")])
            .expect("workspace binding");

    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "acme", "lakehouse").expect("storage");
    let metastore_scope = StateScope::metastore("acme", "lakehouse", "catalog");

    assert!(
        CatalogAuthority::control_v1_bound(storage, metastore_scope, &bindings).is_err(),
        "a workspace binding must not authorize a metastore root"
    );
}

/// Reports a fresh pointer HEAD version for the first `unstable_remaining`
/// pointer `head()` calls, then passes the real version through.
struct UnstablePointerHeadBackend {
    inner: MemoryBackend,
    unstable_remaining: AtomicUsize,
    counter: AtomicUsize,
}

impl UnstablePointerHeadBackend {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryBackend::new(),
            unstable_remaining: AtomicUsize::new(0),
            counter: AtomicUsize::new(0),
        })
    }

    fn arm(&self, calls: usize) {
        self.unstable_remaining.store(calls, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for UnstablePointerHeadBackend {
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
        let mut meta = self.inner.head(path).await?;
        if path.ends_with("/control/v1/domains/catalog/head/current.json")
            && let Some(meta) = &mut meta
            && self
                .unstable_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        {
            meta.version = format!("unstable-{}", self.counter.fetch_add(1, Ordering::SeqCst));
        }
        Ok(meta)
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

#[tokio::test]
async fn head_pin_conflicts_retry_inside_the_catalog_budget() {
    let backend = UnstablePointerHeadBackend::new();
    let storage = ScopedStorage::new(backend.clone(), "synthetic-tenant", "synthetic-workspace")
        .expect("scoped storage");
    let authority = ControlCatalogAuthority::new(storage, scope()).expect("control authority");
    authority
        .create_catalog("seed", None, WriteOptions::default())
        .await
        .expect("seed mutation publishes the pointer");

    // The pin reads HEAD before and after the pointer body for three attempts;
    // six fresh versions exhaust that budget exactly once.
    backend.arm(6);
    let created = authority
        .create_catalog("after-unstable-head", None, WriteOptions::default())
        .await
        .expect("a transient head-pin conflict must be retried inside the catalog budget");
    assert_eq!("after-unstable-head", created.name);
    assert_eq!(
        0,
        backend.unstable_remaining.load(Ordering::SeqCst),
        "the unstable head fault must fire"
    );
    assert!(
        authority
            .get_catalog("after-unstable-head")
            .await
            .expect("authority read")
            .is_some(),
        "the retried mutation must be durable"
    );
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn bounded_head_pin_conflicts_retry_inside_the_catalog_budget() {
    let backend = UnstablePointerHeadBackend::new();
    let storage = ScopedStorage::new(backend.clone(), "synthetic-tenant", "synthetic-workspace")
        .expect("scoped storage");
    let authority = ControlCatalogAuthority::new_synthetic_bounded(
        storage,
        scope(),
        Arc::new(NoopProjectionNotifierV2),
    )
    .expect("bounded authority");
    authority
        .create_catalog_v2("seed", None, WriteOptions::with_idempotency("seed"))
        .await
        .expect("seed mutation publishes the pointer");

    // Format 8 classifies pin exhaustion as an ambiguous outcome; at begin
    // nothing has been written, so the authority must still retry it.
    backend.arm(6);
    let created = authority
        .create_catalog_v2(
            "after-unstable-head",
            None,
            WriteOptions::with_idempotency("after-unstable-head"),
        )
        .await
        .expect("a transient head-pin conflict must be retried inside the bounded budget");
    assert_eq!("after-unstable-head", created.name);
    assert_eq!(
        0,
        backend.unstable_remaining.load(Ordering::SeqCst),
        "the unstable head fault must fire"
    );
    assert!(
        authority
            .get_catalog("after-unstable-head")
            .await
            .expect("authority read")
            .is_some(),
        "the retried bounded mutation must be durable"
    );
}

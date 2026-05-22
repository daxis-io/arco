#![allow(clippy::expect_used)]
#![allow(missing_docs)]

#[path = "../../arco-core/tests/support/scripted_backend.rs"]
mod scripted_backend;
#[path = "../../arco-core/tests/support/spy_backend.rs"]
mod spy_backend;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arco_catalog::manifest::{CatalogDomainManifest, DomainManifestPointer};
use arco_catalog::parquet_util::{
    NamespaceRecord, TableRecord, read_commits, write_catalogs, write_columns, write_namespaces,
    write_tables,
};
use arco_catalog::writer::{CatalogTransactionCommit, SchemaPatch};
use arco_catalog::{
    CatalogReader, CatalogWriter, ColumnDefinition, RegisterTableInSchemaRequest,
    RegisterTableRequest, TablePatch, Tier1Compactor, Tier1Writer, WriteOptions,
};
use arco_core::control_plane_transactions::{
    ControlPlaneTxDomain, ControlPlaneTxKind, ControlPlaneTxPaths, ControlPlaneTxStatus,
    DomainCommit, RootTxManifest, RootTxManifestDomain, RootTxReceipt, RootTxRecord,
};
use arco_core::lock::LockInfo;
use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};
use bytes::Bytes;
use chrono::Utc;
use scripted_backend::{PreconditionMatcher, ScriptedBackend, ScriptedEffect, ScriptedRule};
use spy_backend::{SpyBackend, SpyOp};

const TENANT: &str = "test-tenant";
const WORKSPACE: &str = "test-workspace";

fn scoped_storage<B>(backend: Arc<B>) -> ScopedStorage
where
    B: StorageBackend,
{
    ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage")
}

fn catalog_writer(storage: ScopedStorage) -> CatalogWriter {
    CatalogWriter::new(storage.clone()).with_sync_compactor(Arc::new(Tier1Compactor::new(storage)))
}

fn test_column(name: &str) -> ColumnDefinition {
    ColumnDefinition {
        name: name.to_string(),
        data_type: "STRING".to_string(),
        is_nullable: false,
        ordinal: 0,
        description: None,
    }
}

async fn assert_visible_commit(storage: &ScopedStorage, commit: &CatalogTransactionCommit) {
    let (pointer, manifest, pointer_version) = current_catalog_pointer(storage).await;
    assert!(
        !commit.event_id.is_empty(),
        "transaction helper must return a ledger event identifier"
    );
    assert_eq!(commit.manifest_id, pointer.manifest_id);
    assert_eq!(commit.snapshot_version, manifest.snapshot_version);
    assert_eq!(commit.pointer_version, pointer_version);
    assert_eq!(
        manifest.last_commit_id.as_deref(),
        Some(commit.commit_id.as_str()),
        "receipt commit id must match the visible immutable manifest head"
    );
    assert_eq!(
        commit.lock_path,
        CatalogPaths::domain_lock(CatalogDomain::Catalog)
    );
    assert!(
        commit.fencing_token > 0,
        "transaction helper must report the held fencing token"
    );

    let commits_path = CatalogPaths::snapshot_file(
        CatalogDomain::Catalog,
        manifest.snapshot_version,
        "commits.parquet",
    );
    let commit_bytes = storage
        .get_raw(&commits_path)
        .await
        .expect("catalog commits snapshot bytes");
    let commit_rows = read_commits(&commit_bytes).expect("parse catalog commits snapshot");
    let latest = commit_rows
        .last()
        .expect("visible catalog snapshot must include at least one commit row");
    assert_eq!(latest.commit_ulid, commit.commit_id);
    assert_eq!(
        latest.snapshot_version,
        i64::try_from(manifest.snapshot_version).expect("snapshot version fits in i64")
    );
    assert_eq!(
        latest.watermark_event_id,
        manifest.watermark_event_id.clone()
    );
    assert_eq!(
        latest.fencing_token,
        i64::try_from(commit.fencing_token).expect("fencing token fits in i64")
    );
    assert_eq!(latest.published_at, manifest.updated_at.timestamp_millis());
}

async fn current_catalog_pointer(
    storage: &ScopedStorage,
) -> (DomainManifestPointer, CatalogDomainManifest, String) {
    let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
    let pointer_bytes = storage
        .get_raw(&pointer_path)
        .await
        .expect("catalog pointer bytes");
    let pointer: DomainManifestPointer =
        serde_json::from_slice(&pointer_bytes).expect("parse catalog pointer");
    let manifest_bytes = storage
        .get_raw(&pointer.manifest_path)
        .await
        .expect("catalog manifest bytes");
    let manifest: CatalogDomainManifest =
        serde_json::from_slice(&manifest_bytes).expect("parse catalog manifest");
    let version = storage
        .head_raw(&pointer_path)
        .await
        .expect("catalog pointer head")
        .expect("catalog pointer exists")
        .version;
    (pointer, manifest, version)
}

async fn seed_root_token_for_catalog(
    storage: &ScopedStorage,
    tx_id: &str,
    pointer: &DomainManifestPointer,
    manifest: &CatalogDomainManifest,
) {
    let visible_at = Utc::now();
    let super_manifest_path = ControlPlaneTxPaths::root_super_manifest(tx_id);
    let root_manifest = RootTxManifest {
        tx_id: tx_id.to_string(),
        fencing_token: 1,
        published_at: visible_at,
        domains: BTreeMap::from([(
            ControlPlaneTxDomain::Catalog,
            RootTxManifestDomain {
                manifest_id: pointer.manifest_id.clone(),
                manifest_path: pointer.manifest_path.clone(),
                commit_id: manifest
                    .last_commit_id
                    .clone()
                    .expect("catalog manifest commit id"),
            },
        )]),
    };
    storage
        .put_raw(
            &super_manifest_path,
            Bytes::from(serde_json::to_vec(&root_manifest).expect("serialize root manifest")),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("write root super-manifest");

    let root_record = RootTxRecord {
        tx_id: tx_id.to_string(),
        kind: ControlPlaneTxKind::RootCommit,
        status: ControlPlaneTxStatus::Visible,
        repair_pending: false,
        request_id: "req-catalog-root-token".to_string(),
        idempotency_key: "idem-catalog-root-token".to_string(),
        request_hash: "sha256:catalog-root-token".to_string(),
        lock_path: ControlPlaneTxPaths::root_lock(),
        fencing_token: 1,
        prepared_at: visible_at,
        visible_at: Some(visible_at),
        result: Some(RootTxReceipt {
            tx_id: tx_id.to_string(),
            root_commit_id: "01JROOTTOKENCOMMIT0000000001".to_string(),
            super_manifest_path: super_manifest_path.clone(),
            domain_commits: vec![DomainCommit {
                domain: ControlPlaneTxDomain::Catalog,
                tx_id: "01JCATTOKENPARTICIPANT000001".to_string(),
                commit_id: manifest
                    .last_commit_id
                    .clone()
                    .expect("catalog manifest commit id"),
                manifest_id: pointer.manifest_id.clone(),
                manifest_path: pointer.manifest_path.clone(),
                read_token: format!("catalog:{}", pointer.manifest_id),
            }],
            read_token: format!("root:{tx_id}"),
            visible_at,
        }),
    };
    storage
        .put_raw(
            &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, tx_id),
            Bytes::from(serde_json::to_vec(&root_record).expect("serialize root tx record")),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("write root tx record");
}

async fn seed_legacy_default_namespace_snapshot(
    storage: &ScopedStorage,
    namespace: &str,
    table_name: &str,
) {
    let now = Utc::now().timestamp_millis();
    let namespace_id = "018f0000-0000-7000-8000-000000000777".to_string();
    let table_id = "018f0000-0000-7000-8000-000000000888".to_string();
    let snapshot_version = 1;
    let snapshot_dir = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, snapshot_version);

    storage
        .put_raw(
            &CatalogPaths::snapshot_file(
                CatalogDomain::Catalog,
                snapshot_version,
                "catalogs.parquet",
            ),
            write_catalogs(&[]).expect("serialize legacy catalogs"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("write legacy catalogs snapshot");
    storage
        .put_raw(
            &CatalogPaths::snapshot_file(
                CatalogDomain::Catalog,
                snapshot_version,
                "namespaces.parquet",
            ),
            write_namespaces(&[NamespaceRecord {
                id: namespace_id.clone(),
                catalog_id: None,
                name: namespace.to_string(),
                description: Some("legacy default namespace".to_string()),
                properties_json: None,
                storage_root: None,
                created_at: now,
                updated_at: now,
            }])
            .expect("serialize legacy namespaces"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("write legacy namespaces snapshot");
    storage
        .put_raw(
            &CatalogPaths::snapshot_file(
                CatalogDomain::Catalog,
                snapshot_version,
                "tables.parquet",
            ),
            write_tables(&[TableRecord {
                id: table_id,
                namespace_id,
                name: table_name.to_string(),
                description: Some("legacy table".to_string()),
                location: None,
                format: Some("iceberg".to_string()),
                table_type: None,
                properties_json: None,
                created_at: now,
                updated_at: now,
            }])
            .expect("serialize legacy tables"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("write legacy tables snapshot");
    storage
        .put_raw(
            &CatalogPaths::snapshot_file(
                CatalogDomain::Catalog,
                snapshot_version,
                "columns.parquet",
            ),
            write_columns(&[]).expect("serialize legacy columns"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("write legacy columns snapshot");

    let tier1 = Tier1Writer::new(storage.clone());
    let guard = tier1
        .acquire_lock(Duration::from_secs(30), 1)
        .await
        .expect("catalog lock");
    tier1
        .update_locked(&guard, |manifest| {
            manifest.snapshot_version = snapshot_version;
            manifest.snapshot_path = snapshot_dir.clone();
            manifest.snapshot = None;
            manifest.updated_at = Utc::now();
            Ok(())
        })
        .await
        .expect("publish legacy snapshot head");
    guard.release().await.expect("release catalog lock");
}

#[tokio::test]
async fn ordinary_catalog_reads_never_list_storage() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let spy = Arc::new(SpyBackend::new(inner));
    let storage = scoped_storage(spy.clone());
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");
    writer
        .create_namespace("analytics", None, WriteOptions::default())
        .await
        .expect("create namespace");
    let table = writer
        .register_table(
            RegisterTableRequest {
                namespace: "analytics".to_string(),
                name: "events".to_string(),
                description: None,
                location: None,
                format: None,
                columns: vec![test_column("event_id")],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register table");

    let reader = CatalogReader::new(storage);
    spy.clear_ops();
    spy.set_fail_on_list(true);

    let namespaces = reader.list_namespaces().await.expect("list namespaces");
    assert_eq!(namespaces.len(), 1);
    assert!(
        reader
            .get_namespace("analytics")
            .await
            .expect("get namespace")
            .is_some()
    );
    let tables = reader.list_tables("analytics").await.expect("list tables");
    assert_eq!(tables.len(), 1);
    assert!(
        reader
            .get_table("analytics", "events")
            .await
            .expect("get table")
            .is_some()
    );
    assert!(
        reader
            .get_table_by_id(&table.id)
            .await
            .expect("get table by id")
            .is_some()
    );
    let columns = reader.get_columns(&table.id).await.expect("get columns");
    assert_eq!(columns.len(), 1);
    reader
        .get_freshness(CatalogDomain::Catalog)
        .await
        .expect("get freshness");

    assert!(
        spy.ops().iter().all(|op| !matches!(op, SpyOp::List { .. })),
        "ordinary catalog reads must not call list(): {:?}",
        spy.ops()
    );
}

#[tokio::test]
async fn root_token_catalog_reads_never_list_storage() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let spy = Arc::new(SpyBackend::new(inner));
    let storage = scoped_storage(spy.clone());
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");
    writer
        .create_namespace("bronze", None, WriteOptions::default())
        .await
        .expect("create bronze namespace");

    let (pointer, manifest, _) = current_catalog_pointer(&storage).await;
    seed_root_token_for_catalog(&storage, "01JROOTCATTOKEN000000000001", &pointer, &manifest).await;

    let reader = CatalogReader::new(storage);
    spy.clear_ops();
    spy.set_fail_on_list(true);

    let pinned = reader
        .list_namespaces_for_root_token("root:01JROOTCATTOKEN000000000001")
        .await
        .expect("pinned namespace read");
    assert_eq!(pinned.len(), 1);

    assert!(
        spy.ops().iter().all(|op| !matches!(op, SpyOp::List { .. })),
        "root-token catalog reads must not call list(): {:?}",
        spy.ops()
    );
}

#[tokio::test]
async fn tier1_writer_rejects_stale_fencing_and_leaves_visible_head_unchanged() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = scoped_storage(backend.clone());
    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("initialize");

    let guard = writer
        .acquire_lock(Duration::from_secs(30), 1)
        .await
        .expect("catalog lock");

    let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
    let pointer_before = storage
        .get_raw(&pointer_path)
        .await
        .expect("pointer bytes before stale bump");
    let pointer_meta = storage
        .head_raw(&pointer_path)
        .await
        .expect("pointer head before stale bump")
        .expect("pointer exists");
    let mut pointer: DomainManifestPointer =
        serde_json::from_slice(&pointer_before).expect("parse pointer");
    pointer.epoch = guard.fencing_token().sequence() + 1;
    storage
        .put_raw(
            &pointer_path,
            Bytes::from(serde_json::to_vec(&pointer).expect("serialize pointer bump")),
            WritePrecondition::MatchesVersion(pointer_meta.version.clone()),
        )
        .await
        .expect("bump pointer epoch");

    let head_after_bump = storage
        .head_raw(&pointer_path)
        .await
        .expect("head after bump")
        .expect("pointer exists")
        .version;
    let bytes_after_bump = storage
        .get_raw(&pointer_path)
        .await
        .expect("pointer after bump");

    let error = writer
        .update_locked(&guard, |manifest| {
            manifest.writer_session_id = Some("stale-guard".to_string());
            Ok(())
        })
        .await
        .expect_err("stale writer guard must be rejected");
    assert!(
        error.to_string().contains("stale epoch"),
        "unexpected stale writer error: {error}"
    );

    let head_after = storage
        .head_raw(&pointer_path)
        .await
        .expect("head after stale write")
        .expect("pointer exists")
        .version;
    let bytes_after = storage
        .get_raw(&pointer_path)
        .await
        .expect("pointer bytes after stale write");
    assert_eq!(head_after, head_after_bump);
    assert_eq!(bytes_after, bytes_after_bump);
}

#[tokio::test]
async fn tier1_sync_compaction_rejects_stale_fencing_and_leaves_visible_head_unchanged() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = scoped_storage(backend.clone());
    let writer = Tier1Writer::new(storage.clone());
    writer.initialize().await.expect("initialize");

    let guard = writer
        .acquire_lock(Duration::from_secs(30), 1)
        .await
        .expect("catalog lock");
    let namespace = NamespaceRecord {
        id: "018f0000-0000-7000-8000-000000000123".to_string(),
        catalog_id: None,
        name: "stale-fence".to_string(),
        description: None,
        properties_json: None,
        storage_root: None,
        created_at: 1_710_000_000_000,
        updated_at: 1_710_000_000_000,
    };
    let event_id = writer
        .append_ledger_event(
            &guard,
            CatalogDomain::Catalog,
            &arco_catalog::CatalogDdlEvent::NamespaceCreated { namespace },
            "test",
        )
        .await
        .expect("append ledger event");
    let event_path = format!("ledger/catalog/{event_id}.json");

    let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
    let head_before = storage
        .head_raw(&pointer_path)
        .await
        .expect("pointer head before takeover")
        .expect("pointer exists")
        .version;
    let bytes_before = storage
        .get_raw(&pointer_path)
        .await
        .expect("pointer bytes before takeover");

    let replacement = LockInfo::new(
        "new-holder",
        Duration::from_secs(30),
        guard.fencing_token().sequence() + 1,
    );
    let replacement_write = backend
        .put(
            &storage.lock(CatalogDomain::Catalog),
            Bytes::from(serde_json::to_vec(&replacement).expect("serialize replacement lock")),
            WritePrecondition::MatchesVersion(guard.version().to_string()),
        )
        .await
        .expect("replacement lock write");
    assert!(matches!(replacement_write, WriteResult::Success { .. }));

    let compactor = Tier1Compactor::new(storage.clone());
    let error = compactor
        .sync_compact(
            "catalog",
            vec![event_path],
            guard.fencing_token().sequence(),
        )
        .await
        .expect_err("stale fencing token must be rejected");
    assert!(
        error.to_string().contains("stale fencing token"),
        "unexpected sync compaction error: {error}"
    );

    let head_after = storage
        .head_raw(&pointer_path)
        .await
        .expect("pointer head after stale sync compaction")
        .expect("pointer exists")
        .version;
    let bytes_after = storage
        .get_raw(&pointer_path)
        .await
        .expect("pointer bytes after stale sync compaction");
    assert_eq!(head_after, head_before);
    assert_eq!(bytes_after, bytes_before);
}

#[tokio::test]
async fn default_catalog_rename_transaction_bridges_legacy_null_catalog_namespaces() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = scoped_storage(backend);
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");
    seed_legacy_default_namespace_snapshot(&storage, "legacy", "old_name").await;

    writer
        .rename_table_in_schema_transaction(
            "default",
            "legacy",
            "old_name",
            "new_name",
            WriteOptions::default(),
        )
        .await
        .expect("default catalog rename should bridge legacy null catalog_id namespaces");

    let reader = CatalogReader::new(storage);
    assert!(
        reader
            .get_table("legacy", "old_name")
            .await
            .expect("lookup old table")
            .is_none()
    );
    let renamed = reader
        .get_table("legacy", "new_name")
        .await
        .expect("lookup renamed table")
        .expect("renamed table exists");
    assert_eq!(renamed.name, "new_name");
}

#[tokio::test]
async fn default_catalog_schema_patch_bridges_legacy_null_catalog_namespaces() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = scoped_storage(backend);
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");
    seed_legacy_default_namespace_snapshot(&storage, "legacy", "old_name").await;

    writer
        .patch_schema_in_catalog(
            "default",
            "legacy",
            SchemaPatch {
                description: Some(Some("patched legacy default schema".to_string())),
                new_name: Some("gold".to_string()),
                properties: Some(Some(BTreeMap::from([(
                    "classification".to_string(),
                    "restricted".to_string(),
                )]))),
                storage_root: Some(Some("s3://bucket/schemas/gold".to_string())),
            },
            WriteOptions::default(),
        )
        .await
        .expect("default catalog schema patch should bridge legacy null catalog_id namespaces");

    let reader = CatalogReader::new(storage);
    let default_catalog = reader
        .get_catalog("default")
        .await
        .expect("get default catalog")
        .expect("default catalog exists");
    let patched = reader
        .list_schemas("default")
        .await
        .expect("list default schemas")
        .into_iter()
        .find(|schema| schema.name == "gold")
        .expect("patched schema exists");
    assert_eq!(
        patched.catalog_id.as_deref(),
        Some(default_catalog.id.as_str())
    );
    assert_eq!(
        patched.description.as_deref(),
        Some("patched legacy default schema")
    );
    assert_eq!(
        patched
            .properties
            .as_ref()
            .and_then(|properties| properties.get("classification"))
            .map(String::as_str),
        Some("restricted")
    );
    assert_eq!(
        patched.storage_root.as_deref(),
        Some("s3://bucket/schemas/gold")
    );
    assert!(
        reader
            .list_schemas("default")
            .await
            .expect("list schemas after patch")
            .into_iter()
            .all(|schema| schema.name != "legacy"),
        "old schema name must disappear after authoritative patch"
    );
}

#[tokio::test]
async fn default_catalog_transaction_helpers_publish_visible_round_trip_state() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = scoped_storage(backend);
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");

    let schema_commit = writer
        .create_schema_transaction(
            "default",
            "analytics",
            Some("default schema"),
            WriteOptions::default(),
        )
        .await
        .expect("create default schema transaction");
    assert_visible_commit(&storage, &schema_commit).await;

    let reader = CatalogReader::new(storage.clone());
    let default_catalog = reader
        .get_catalog("default")
        .await
        .expect("get default catalog")
        .expect("default catalog exists");
    let namespace = reader
        .get_namespace("analytics")
        .await
        .expect("get default namespace")
        .expect("default namespace exists");
    assert_eq!(
        namespace.catalog_id.as_deref(),
        Some(default_catalog.id.as_str())
    );

    let register_commit = writer
        .register_table_in_schema_transaction(
            "default",
            "analytics",
            RegisterTableInSchemaRequest {
                name: "events".to_string(),
                description: Some("event log".to_string()),
                location: None,
                format: Some("iceberg".to_string()),
                table_type: None,
                properties: None,
                columns: vec![test_column("event_id")],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register default table transaction");
    assert_visible_commit(&storage, &register_commit).await;

    let table = reader
        .get_table("analytics", "events")
        .await
        .expect("get default table")
        .expect("default table exists");
    assert_eq!(table.description.as_deref(), Some("event log"));
    assert_eq!(
        reader
            .get_table_in_schema("default", "analytics", "events")
            .await
            .expect("get table through uc-like reader")
            .map(|table| table.id),
        Some(table.id.clone())
    );
    let columns = reader
        .get_columns(&table.id)
        .await
        .expect("get default table columns");
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name, "event_id");

    let update_commit = writer
        .update_table_in_schema_transaction(
            "default",
            "analytics",
            "events",
            TablePatch {
                description: Some(Some("patched default table".to_string())),
                ..TablePatch::default()
            },
            WriteOptions::default(),
        )
        .await
        .expect("update default table transaction");
    assert_visible_commit(&storage, &update_commit).await;

    let updated = reader
        .get_table("analytics", "events")
        .await
        .expect("get updated default table")
        .expect("updated default table exists");
    assert_eq!(
        updated.description.as_deref(),
        Some("patched default table")
    );

    let rename_commit = writer
        .rename_table_in_schema_transaction(
            "default",
            "analytics",
            "events",
            "events_v2",
            WriteOptions::default(),
        )
        .await
        .expect("rename default table transaction");
    assert_visible_commit(&storage, &rename_commit).await;

    assert!(
        reader
            .get_table("analytics", "events")
            .await
            .expect("lookup old default table")
            .is_none()
    );
    let renamed = reader
        .get_table("analytics", "events_v2")
        .await
        .expect("lookup renamed default table")
        .expect("renamed default table exists");
    assert_eq!(renamed.name, "events_v2");

    let drop_commit = writer
        .drop_table_in_schema_transaction(
            "default",
            "analytics",
            "events_v2",
            WriteOptions::default(),
        )
        .await
        .expect("drop default table transaction");
    assert_visible_commit(&storage, &drop_commit).await;
    assert!(
        reader
            .get_table("analytics", "events_v2")
            .await
            .expect("lookup dropped default table")
            .is_none()
    );
}

#[tokio::test]
async fn named_catalog_transaction_helpers_round_trip_and_preserve_catalog_scope() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = scoped_storage(backend);
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");

    let default_schema_commit = writer
        .create_schema_transaction("default", "analytics", None, WriteOptions::default())
        .await
        .expect("create default analytics schema");
    assert_visible_commit(&storage, &default_schema_commit).await;

    let default_register_commit = writer
        .register_table_in_schema_transaction(
            "default",
            "analytics",
            RegisterTableInSchemaRequest {
                name: "events".to_string(),
                description: Some("default events".to_string()),
                location: None,
                format: Some("iceberg".to_string()),
                table_type: None,
                properties: None,
                columns: vec![test_column("event_id")],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register default analytics table");
    assert_visible_commit(&storage, &default_register_commit).await;

    let catalog_commit = writer
        .create_catalog_transaction("warehouse", Some("named catalog"), WriteOptions::default())
        .await
        .expect("create named catalog transaction");
    assert_visible_commit(&storage, &catalog_commit).await;

    let schema_commit = writer
        .create_schema_transaction(
            "warehouse",
            "analytics",
            Some("warehouse analytics schema"),
            WriteOptions::default(),
        )
        .await
        .expect("create named catalog schema transaction");
    assert_visible_commit(&storage, &schema_commit).await;

    let register_commit = writer
        .register_table_in_schema_transaction(
            "warehouse",
            "analytics",
            RegisterTableInSchemaRequest {
                name: "events".to_string(),
                description: Some("warehouse events".to_string()),
                location: None,
                format: Some("iceberg".to_string()),
                table_type: None,
                properties: None,
                columns: vec![test_column("warehouse_event_id")],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register named catalog table transaction");
    assert_visible_commit(&storage, &register_commit).await;

    let reader = CatalogReader::new(storage.clone());
    let warehouse_catalog = reader
        .get_catalog("warehouse")
        .await
        .expect("get warehouse catalog")
        .expect("warehouse catalog exists");
    let warehouse_schema = reader
        .list_schemas("warehouse")
        .await
        .expect("list warehouse schemas")
        .into_iter()
        .find(|namespace| namespace.name == "analytics")
        .expect("warehouse analytics schema exists");
    assert_eq!(
        warehouse_schema.catalog_id.as_deref(),
        Some(warehouse_catalog.id.as_str())
    );

    let default_table = reader
        .get_table_in_schema("default", "analytics", "events")
        .await
        .expect("get default scoped table")
        .expect("default scoped table exists");
    let warehouse_table = reader
        .get_table_in_schema("warehouse", "analytics", "events")
        .await
        .expect("get warehouse scoped table")
        .expect("warehouse scoped table exists");
    assert_ne!(default_table.id, warehouse_table.id);
    assert_eq!(default_table.description.as_deref(), Some("default events"));
    assert_eq!(
        warehouse_table.description.as_deref(),
        Some("warehouse events")
    );

    let update_commit = writer
        .update_table_in_schema_transaction(
            "warehouse",
            "analytics",
            "events",
            TablePatch {
                description: Some(Some("warehouse events patched".to_string())),
                ..TablePatch::default()
            },
            WriteOptions::default(),
        )
        .await
        .expect("update named catalog table transaction");
    assert_visible_commit(&storage, &update_commit).await;
    assert_eq!(
        reader
            .get_table_in_schema("warehouse", "analytics", "events")
            .await
            .expect("get patched warehouse table")
            .expect("patched warehouse table exists")
            .description
            .as_deref(),
        Some("warehouse events patched")
    );
    assert_eq!(
        reader
            .get_table_in_schema("default", "analytics", "events")
            .await
            .expect("re-read default scoped table")
            .expect("default scoped table still exists")
            .description
            .as_deref(),
        Some("default events")
    );

    let rename_commit = writer
        .rename_table_in_schema_transaction(
            "warehouse",
            "analytics",
            "events",
            "events_curated",
            WriteOptions::default(),
        )
        .await
        .expect("rename named catalog table transaction");
    assert_visible_commit(&storage, &rename_commit).await;
    assert!(
        reader
            .get_table_in_schema("warehouse", "analytics", "events")
            .await
            .expect("lookup old warehouse table")
            .is_none()
    );
    assert!(
        reader
            .get_table_in_schema("default", "analytics", "events")
            .await
            .expect("lookup default table after warehouse rename")
            .is_some()
    );

    let drop_commit = writer
        .drop_table_in_schema_transaction(
            "warehouse",
            "analytics",
            "events_curated",
            WriteOptions::default(),
        )
        .await
        .expect("drop named catalog table transaction");
    assert_visible_commit(&storage, &drop_commit).await;
    assert!(
        reader
            .get_table_in_schema("warehouse", "analytics", "events_curated")
            .await
            .expect("lookup dropped warehouse table")
            .is_none()
    );
    assert!(
        reader
            .get_table_in_schema("default", "analytics", "events")
            .await
            .expect("default table after warehouse drop")
            .is_some()
    );
}

#[tokio::test]
async fn catalog_reader_fails_closed_when_visible_catalog_snapshot_file_is_missing() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = scoped_storage(backend);
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");

    writer
        .create_catalog(
            "broken_visible",
            Some("visible pointer with missing catalogs snapshot"),
            WriteOptions::default(),
        )
        .await
        .expect("create visible catalog");

    let (_pointer, manifest, _pointer_version) = current_catalog_pointer(&storage).await;
    let catalogs_path = CatalogPaths::snapshot_file(
        CatalogDomain::Catalog,
        manifest.snapshot_version,
        "catalogs.parquet",
    );
    storage
        .delete(&catalogs_path)
        .await
        .expect("delete visible catalogs snapshot file");

    let error = CatalogReader::new(storage)
        .list_catalogs()
        .await
        .expect_err("reader must fail closed when visible snapshot file is missing");
    let message = error.to_string();
    assert!(
        message.contains("catalogs.parquet") || message.contains("not found"),
        "missing visible snapshot should surface a storage error, got: {message}"
    );
}

#[tokio::test]
async fn stale_update_event_replay_does_not_rollback_table_metadata() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = scoped_storage(backend);
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");

    writer
        .create_schema_transaction("default", "replay", None, WriteOptions::default())
        .await
        .expect("create replay schema");
    writer
        .register_table_in_schema_transaction(
            "default",
            "replay",
            RegisterTableInSchemaRequest {
                name: "events".to_string(),
                description: Some("initial".to_string()),
                location: None,
                format: None,
                table_type: None,
                properties: None,
                columns: vec![test_column("event_id")],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register replay table");

    let first_update = writer
        .update_table_in_schema_transaction(
            "default",
            "replay",
            "events",
            TablePatch {
                description: Some(Some("first update".to_string())),
                ..TablePatch::default()
            },
            WriteOptions::default(),
        )
        .await
        .expect("first table update");
    writer
        .update_table_in_schema_transaction(
            "default",
            "replay",
            "events",
            TablePatch {
                description: Some(Some("second update".to_string())),
                ..TablePatch::default()
            },
            WriteOptions::default(),
        )
        .await
        .expect("second table update");

    let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
    let pointer_version_before = storage
        .head_raw(&pointer_path)
        .await
        .expect("pointer head before stale replay")
        .expect("catalog pointer exists")
        .version;
    let stale_event_path =
        CatalogPaths::ledger_event(CatalogDomain::Catalog, &first_update.event_id);
    let tier1 = Tier1Writer::new(storage.clone());
    let guard = tier1
        .acquire_lock(Duration::from_secs(30), 1)
        .await
        .expect("catalog replay lock");
    let result = Tier1Compactor::new(storage.clone())
        .sync_compact(
            "catalog",
            vec![stale_event_path],
            guard.fencing_token().sequence(),
        )
        .await
        .expect("stale event replay should be a no-op");
    guard.release().await.expect("release replay lock");
    assert_eq!(result.events_processed, 0);

    let pointer_version_after = storage
        .head_raw(&pointer_path)
        .await
        .expect("pointer head after stale replay")
        .expect("catalog pointer exists")
        .version;
    assert_eq!(pointer_version_after, pointer_version_before);

    let table = CatalogReader::new(storage)
        .get_table_in_schema("default", "replay", "events")
        .await
        .expect("read replay table")
        .expect("replay table exists");
    assert_eq!(table.description.as_deref(), Some("second update"));
}

#[tokio::test]
async fn create_catalog_returns_success_when_lock_release_fails_after_visible_publish() {
    let backend = Arc::new(ScriptedBackend::new());
    let storage = scoped_storage(backend.clone());
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");
    backend.add_rule(ScriptedRule::put_after(
        format!(
            "tenant={TENANT}/workspace={WORKSPACE}/{}",
            CatalogPaths::domain_lock(CatalogDomain::Catalog)
        ),
        PreconditionMatcher::MatchesVersion,
        1,
        1,
        ScriptedEffect::Error("injected post-publish lock release failure".to_string()),
    ));

    let catalog = writer
        .create_catalog(
            "release_visible_catalog",
            Some("release failure should not hide visible catalog"),
            WriteOptions::with_idempotency("018f0f87-8e78-7c21-8e91-5370f332bb01"),
        )
        .await
        .expect("visible create_catalog should survive lock release failure");
    assert_eq!(catalog.name, "release_visible_catalog");

    let replay = writer
        .create_catalog(
            "release_visible_catalog",
            Some("release failure should not hide visible catalog"),
            WriteOptions::with_idempotency("018f0f87-8e78-7c21-8e91-5370f332bb01"),
        )
        .await
        .expect("idempotent replay should return visible catalog");
    assert_eq!(replay.id, catalog.id);
}

#[tokio::test]
async fn catalog_transaction_helper_returns_commit_when_lock_release_fails_after_publish() {
    let backend = Arc::new(ScriptedBackend::new());
    let storage = scoped_storage(backend.clone());
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");
    writer
        .create_schema_transaction(
            "default",
            "release_failure_seed",
            Some("seed default catalog before injecting release failure"),
            WriteOptions::default(),
        )
        .await
        .expect("seed default catalog");
    backend.add_rule(ScriptedRule::put_after(
        format!(
            "tenant={TENANT}/workspace={WORKSPACE}/{}",
            CatalogPaths::domain_lock(CatalogDomain::Catalog)
        ),
        PreconditionMatcher::MatchesVersion,
        1,
        1,
        ScriptedEffect::Error("injected post-publish lock release failure".to_string()),
    ));

    let commit = writer
        .create_schema_transaction(
            "default",
            "release_failure",
            Some("release failure should not hide the visible commit"),
            WriteOptions::default(),
        )
        .await
        .expect("visible catalog transaction should survive lock release failure");

    assert_visible_commit(&storage, &commit).await;
    let schema = CatalogReader::new(storage)
        .get_namespace("release_failure")
        .await
        .expect("read namespace")
        .expect("namespace published despite release failure");
    assert_eq!(schema.name, "release_failure");
}

#[tokio::test]
async fn pointer_cas_loss_leaves_visible_head_unchanged_even_if_new_snapshot_persists() {
    let backend = Arc::new(ScriptedBackend::new());
    let storage = scoped_storage(backend.clone());
    let writer = Tier1Writer::new(storage.clone()).with_cas_retries(1);
    writer.initialize().await.expect("initialize");

    let guard = writer
        .acquire_lock(Duration::from_secs(30), 1)
        .await
        .expect("catalog lock");
    let (_pointer_before, _manifest_before, pointer_version_before) =
        current_catalog_pointer(&storage).await;

    backend.add_rule(ScriptedRule::put(
        format!(
            "tenant={TENANT}/workspace={WORKSPACE}/{}",
            "manifests/catalog.pointer.json"
        ),
        PreconditionMatcher::MatchesVersion,
        1,
        ScriptedEffect::PreconditionFailed {
            current_version: pointer_version_before.clone(),
        },
    ));

    let error = writer
        .update_locked(&guard, |manifest| {
            manifest.writer_session_id = Some("cas-loss".to_string());
            Ok(())
        })
        .await
        .expect_err("pointer CAS loss must fail the write");
    assert!(
        error.to_string().contains("lost CAS race"),
        "unexpected pointer CAS loss error: {error}"
    );

    let (_pointer_after, _manifest_after, pointer_version_after) =
        current_catalog_pointer(&storage).await;
    assert_eq!(pointer_version_after, pointer_version_before);

    let immutable_manifests = storage
        .list("manifests/catalog/")
        .await
        .expect("list immutable manifests");
    assert!(
        immutable_manifests.len() >= 2,
        "losing snapshot publish may persist an immutable manifest artifact"
    );
}

#[tokio::test]
async fn snapshot_file_collision_fails_publication_instead_of_reusing_existing_bytes() {
    let backend = Arc::new(ScriptedBackend::new());
    let storage = scoped_storage(backend.clone());
    let writer = catalog_writer(storage.clone());
    writer.initialize().await.expect("initialize");

    backend.add_rule(ScriptedRule::put(
        format!(
            "tenant={TENANT}/workspace={WORKSPACE}/{}",
            CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1)
        ),
        PreconditionMatcher::DoesNotExist,
        1,
        ScriptedEffect::PreconditionFailed {
            current_version: "existing-stale-snapshot-file".to_string(),
        },
    ));

    let error = writer
        .create_schema_transaction(
            "default",
            "snapshot_collision",
            Some("snapshot collision must fail publication"),
            WriteOptions::default(),
        )
        .await
        .expect_err("snapshot file collision must fail publication");

    assert!(
        error.to_string().contains("snapshot file already exists"),
        "unexpected snapshot collision error: {error}"
    );
}

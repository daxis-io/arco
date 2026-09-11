//! Request construction is inside measurements. Frozen storage images are shared
//! byte-for-byte with the Gate 6 baseline; fixture setup and maintenance are outside.
// Keep frozen fixture orchestration and assertions together. All indexed sizes
// and lowercase object extensions are established by the explicit fixture.
#![allow(
    clippy::indexing_slicing,
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::case_sensitive_file_extension_comparisons
)]
use super::{
    Arc, ArcoStateAdmin, BTreeMap, Bytes, CatalogError, ControlMvpStateStore, CountingBackend,
    MemoryBackend, Ordering, Recorder, ScopedStorage, StateScope, StorageBackend,
    WritePrecondition, durable_maintenance,
};
use arco_catalog::catalog_authority::projection_measurement;
use arco_catalog::parquet_util::{self, CatalogRecord, ColumnRecord, NamespaceRecord, TableRecord};
use arco_catalog::state_store::projection_outbox_acks::{
    PROJECTION_OUTBOX_ACK_DOMAIN, ProjectionOutboxWorker,
};
use arco_catalog::{
    CATALOG_PARQUET_PROJECTION_CONSUMER_ID, CatalogAuthority, CatalogAuthorityBinding,
    CatalogAuthorityBindings, CatalogListRequest, CatalogProjectionMaterializer,
    CatalogProjectionNotifier, ColumnDefinition, ControlCatalogAuthority, DurableAuthorityBinding,
    DurableMaintenanceWorker, ProjectionIntentV1, RegisterTableInSchemaRequest, TablePatch,
    WriteOptions,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

struct Quiet;
impl CatalogProjectionNotifier for Quiet {
    fn notify(&self, _: &ProjectionIntentV1) -> arco_catalog::Result<()> {
        Ok(())
    }
}
fn scope(domain: &str) -> StateScope {
    StateScope::new("runtime-tenant", "runtime-workspace", domain)
}
fn storage(backend: Arc<CountingBackend>) -> ScopedStorage {
    ScopedStorage::new(backend, "runtime-tenant", "runtime-workspace").unwrap()
}
fn bindings() -> CatalogAuthorityBindings {
    CatalogAuthorityBindings::new_with_continuation_key(
        [CatalogAuthorityBinding::control_v1(
            "runtime-tenant",
            "runtime-workspace",
        )],
        &[42; 32],
    )
    .unwrap()
}
fn authority(storage: &ScopedStorage) -> ControlCatalogAuthority {
    ControlCatalogAuthority::new(storage.clone(), scope("catalog"))
        .unwrap()
        .with_projection_notifier(Arc::new(Quiet))
}
fn request(storage: &ScopedStorage, bindings: &CatalogAuthorityBindings) -> CatalogAuthority {
    CatalogAuthority::control_v1_bound(storage.clone(), scope("catalog"), bindings).unwrap()
}
async fn maintenance(storage: &ScopedStorage) {
    for domain in ["catalog", PROJECTION_OUTBOX_ACK_DOMAIN] {
        let worker = DurableMaintenanceWorker::new(
            storage.clone(),
            scope(domain),
            DurableAuthorityBinding::new([17; 32]),
        )
        .unwrap();
        Box::pin(durable_maintenance::consolidate_pending(&worker))
            .await
            .unwrap();
    }
}
#[derive(Serialize, Deserialize)]
struct Fixture {
    catalog: CatalogRecord,
    schema: NamespaceRecord,
    tables: Vec<TableRecord>,
    columns: Vec<ColumnRecord>,
    pending: Vec<(u64, String, Vec<TableRecord>)>,
    objects: Vec<(String, Vec<u8>)>,
}
async fn fixture(backend: &Arc<CountingBackend>, tables: usize, backlog: usize) -> Fixture {
    let path = std::env::var("ARCO_RUNTIME_FIXTURES").ok().map(|dir| {
        std::path::PathBuf::from(dir).join(format!("tables-{tables}-backlog-{backlog}.json"))
    });
    if let Some(path) = path.as_ref().filter(|path| path.exists()) {
        let fixture: Fixture = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        for (path, data) in &fixture.objects {
            backend
                .inner
                .put(
                    path,
                    Bytes::copy_from_slice(data),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
        }
        return fixture;
    }
    let storage = storage(backend.clone());
    let writer = authority(&storage);
    let catalog = CatalogRecord::try_from(
        &writer
            .create_catalog("catalog", None, WriteOptions::default())
            .await
            .unwrap(),
    )
    .unwrap();
    let schema = NamespaceRecord::try_from(
        &writer
            .create_schema("catalog", "schema", None, WriteOptions::default())
            .await
            .unwrap(),
    )
    .unwrap();
    let mut fixture = Fixture {
        catalog,
        schema,
        tables: Vec::new(),
        columns: Vec::new(),
        pending: Vec::new(),
        objects: Vec::new(),
    };
    for i in 0..tables {
        let table = writer
            .register_table_in_schema(
                "catalog",
                "schema",
                RegisterTableInSchemaRequest {
                    name: format!("table{i:04}"),
                    description: Some("initial".into()),
                    location: None,
                    format: Some("parquet".into()),
                    table_type: None,
                    properties: None,
                    columns: (0..4)
                        .map(|ordinal| ColumnDefinition {
                            name: format!("column{ordinal}"),
                            data_type: "STRING".into(),
                            is_nullable: true,
                            ordinal,
                            description: None,
                        })
                        .collect(),
                },
                WriteOptions::default(),
            )
            .await
            .unwrap();
        fixture.columns.extend(
            writer
                .get_columns(&table.id)
                .await
                .unwrap()
                .iter()
                .map(ColumnRecord::from),
        );
        fixture.tables.push(TableRecord::try_from(&table).unwrap());
        if (i + 1) % 4 == 0 {
            CatalogProjectionMaterializer::new(storage.clone())
                .unwrap()
                .drain_once()
                .await
                .unwrap();
            maintenance(&storage).await;
        }
    }
    CatalogProjectionMaterializer::new(storage.clone())
        .unwrap()
        .drain_once()
        .await
        .unwrap();
    maintenance(&storage).await;
    let source = ControlMvpStateStore::new(storage.clone(), scope("catalog")).unwrap();
    for i in 0..backlog {
        let index = i % tables;
        let table = writer
            .update_table_in_schema(
                "catalog",
                "schema",
                &format!("table{index:04}"),
                TablePatch {
                    description: Some(Some(format!("update-{i:04}"))),
                    ..TablePatch::default()
                },
                WriteOptions::default(),
            )
            .await
            .unwrap();
        fixture.tables[index] = TableRecord::try_from(&table).unwrap();
        let token = source.current_state_token().await.unwrap();
        fixture.pending.push((
            token.logical_sequence(),
            token.authority_manifest_id().into(),
            fixture.tables.clone(),
        ));
    }
    maintenance(&storage).await;
    for object in backend.inner.list("").await.unwrap() {
        fixture.objects.push((
            object.path.clone(),
            backend.inner.get(&object.path).await.unwrap().to_vec(),
        ));
    }
    fixture.objects.sort_by(|a, b| a.0.cmp(&b.0));
    if let Some(path) = path {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, serde_json::to_vec(&fixture).unwrap()).unwrap();
    }
    fixture
}
fn sorted<T: Serialize>(items: &[T]) -> Vec<String> {
    let mut rows: Vec<_> = items
        .iter()
        .map(|item| serde_json::to_string(item).unwrap())
        .collect();
    rows.sort();
    rows
}
fn check_tables(actual: &[arco_catalog::Table], expected: &[TableRecord]) {
    let rows: Vec<_> = actual
        .iter()
        .map(|t| TableRecord::try_from(t).unwrap())
        .collect();
    assert_eq!(sorted(&rows), sorted(expected));
}
fn operations(recorder: Recorder) -> Vec<serde_json::Value> {
    recorder
        .finish()
        .into_iter()
        .map(|operation| {
            let mut value = serde_json::to_value(&operation).unwrap();
            value["latency_samples_micros"] = serde_json::json!(operation.durations);
            value
        })
        .collect()
}

pub async fn runtime(
    statistics: fn(&CatalogAuthorityBindings) -> serde_json::Value,
    cold: usize,
    warm: usize,
) -> serde_json::Value {
    let mut reports = Vec::new();
    for tables in [8, 64] {
        let backend = Arc::new(CountingBackend::new(1));
        let fixture = Box::pin(fixture(&backend, tables, 0)).await;
        let storage = storage(backend.clone());
        let mut recorder = Recorder::default();
        let mut retained = bindings();
        for phase in ["cold_point", "cold_scan", "warm_point", "warm_scan"] {
            for _ in 0..if phase.starts_with("cold") {
                cold
            } else {
                warm
            } {
                if phase.starts_with("cold") {
                    retained = bindings();
                    let initial = statistics(&retained);
                    assert!(initial.is_null() || initial.as_array().unwrap().is_empty());
                }
                // Registry configuration is server startup; request authority construction,
                // fresh authority resolution, reader opening and copies are inside the poll.
                let result = recorder
                    .measure(phase, &backend, 0, false, async {
                        let authority = request(&storage, &retained);
                        if phase.ends_with("point") {
                            vec![
                                authority
                                    .get_table("catalog", "schema", "table0000")
                                    .await
                                    .unwrap()
                                    .unwrap(),
                            ]
                        } else {
                            authority.list_tables("catalog", "schema").await.unwrap()
                        }
                    })
                    .await;
                check_tables(
                    &result,
                    if phase.ends_with("point") {
                        &fixture.tables[..1]
                    } else {
                        &fixture.tables
                    },
                );
            }
        }
        let digest = hex::encode(Sha256::digest(serde_json::to_vec(&fixture).unwrap()));
        reports.push(
            serde_json::json!({"tables":tables,"fixture_sha256":digest,"parity":true,
            "cold_fresh_starts":{"cold_point":cold,"cold_scan":cold},
            "operations":operations(recorder),"cache":statistics(&retained)}),
        );
    }
    serde_json::json!(reports)
}

async fn verify_projection(storage: &ScopedStorage, fixture: &Fixture) -> Vec<serde_json::Value> {
    let mut proofs = Vec::new();
    for (sequence, manifest, tables) in &fixture.pending {
        let directory =
            format!("control/v1/projections/catalog-parquet/{sequence:020}-{manifest}/");
        let info: arco_catalog::manifest::SnapshotInfo = serde_json::from_slice(
            &storage
                .get_raw(&format!("{directory}manifest.json"))
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(info.version, *sequence);
        assert_eq!(info.path, directory);
        let mut total_rows = 0;
        let mut total_bytes = 0;
        for file in &info.files {
            let data = storage
                .get_raw(&format!("{directory}{}", file.path))
                .await
                .unwrap();
            assert_eq!(hex::encode(Sha256::digest(&data)), file.checksum_sha256);
            assert_eq!(data.len() as u64, file.byte_size);
            let rows = match file.path.as_str() {
                "catalogs.parquet" => {
                    let rows = parquet_util::read_catalogs(&data).unwrap();
                    assert_eq!(rows, vec![fixture.catalog.clone()]);
                    rows.len()
                }
                "namespaces.parquet" => {
                    let rows = parquet_util::read_namespaces(&data).unwrap();
                    assert_eq!(rows, vec![fixture.schema.clone()]);
                    rows.len()
                }
                "tables.parquet" => {
                    let rows = parquet_util::read_tables(&data).unwrap();
                    assert_eq!(sorted(&rows), sorted(tables));
                    rows.len()
                }
                "columns.parquet" => {
                    let rows = parquet_util::read_columns(&data).unwrap();
                    assert_eq!(sorted(&rows), sorted(&fixture.columns));
                    rows.len()
                }
                "commits.parquet" => {
                    let rows = parquet_util::read_commits(&data).unwrap();
                    assert!(rows.is_empty());
                    0
                }
                _ => panic!("unexpected projection file"),
            };
            assert_eq!(rows as u64, file.row_count);
            total_rows += file.row_count;
            total_bytes += file.byte_size;
        }
        assert_eq!(info.files.len(), 5);
        assert_eq!(info.total_rows, total_rows);
        assert_eq!(info.total_bytes, total_bytes);
        proofs.push(serde_json::json!({"sequence":sequence,"manifest":manifest,"tables":tables.len(),"columns":fixture.columns.len(),"parity":true,"bytes":total_bytes}));
    }
    proofs
}

// A drain can make durable progress before acknowledgement-domain backpressure.
// Keep demanded maintenance and replay in total catch-up work; do not change admission.
async fn drain_with_maintenance(
    storage: &ScopedStorage,
) -> (
    arco_catalog::state_store::projection_outbox_acks::ProjectionOutboxDrainReport,
    usize,
) {
    for retries in 0..4 {
        match CatalogProjectionMaterializer::new(storage.clone())
            .unwrap()
            .drain_once()
            .await
        {
            Ok(report) => return (report, retries),
            Err(CatalogError::MaintenanceBackpressure { .. }) => {
                projection_measurement::phase("projection-maintenance", maintenance(storage)).await;
            }
            Err(error) => panic!("projection drain failed: {error}"),
        }
    }
    panic!("projection drain exhausted four maintenance passes")
}

pub async fn projection(tables: usize, backlog: usize, interrupt: bool) -> serde_json::Value {
    let backend = Arc::new(CountingBackend::new(1));
    let fixture = Box::pin(fixture(&backend, tables, backlog)).await;
    let storage = storage(backend.clone());
    let worker = ProjectionOutboxWorker::new(
        storage.clone(),
        "catalog",
        CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
    )
    .unwrap();
    assert_eq!(
        worker.backlog().await.unwrap().pending_record_ids.len(),
        backlog
    );
    let mut recorder = Recorder::default();
    let mut stages = BTreeMap::new();
    let mut published_before_retry = None;
    if interrupt {
        // Publish five Parquet objects and the manifest, then fail the first status PUT.
        // This exercises recovery after immutable publication but before acknowledgement.
        backend.fail_put_countdown.store(7, Ordering::SeqCst);
        projection_measurement::start();
        let failed = recorder
            .measure("interrupted", &backend, 0, false, async {
                CatalogProjectionMaterializer::new(storage.clone())
                    .unwrap()
                    .drain_once()
                    .await
            })
            .await;
        stages.insert("interrupted", projection_measurement::finish());
        assert!(failed.is_err());
        let (sequence, manifest, _) = &fixture.pending[0];
        let path = format!(
            "control/v1/projections/catalog-parquet/{sequence:020}-{manifest}/manifest.json"
        );
        published_before_retry = Some((path.clone(), storage.get_raw(&path).await.unwrap()));
        assert_eq!(
            worker.backlog().await.unwrap().pending_record_ids.len(),
            backlog
        );
    }
    projection_measurement::start();
    let (report, maintenance_retries) = recorder
        .measure(
            "drain",
            &backend,
            0,
            false,
            drain_with_maintenance(&storage),
        )
        .await;
    stages.insert("drain", projection_measurement::finish());
    assert!(report.drained_record_ids.len() <= backlog);
    assert!(report.quarantined_record_ids.is_empty());
    if let Some((path, bytes)) = published_before_retry {
        assert_eq!(storage.get_raw(&path).await.unwrap(), bytes);
    }
    let proofs = verify_projection(&storage, &fixture).await;
    assert!(
        worker
            .backlog()
            .await
            .unwrap()
            .pending_record_ids
            .is_empty()
    );
    let status = CatalogProjectionMaterializer::new(storage.clone())
        .unwrap()
        .status()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        status.applied_authority_sequence(),
        Some(fixture.pending.last().unwrap().0)
    );
    assert_eq!(status.failure_state(), None);
    projection_measurement::start();
    for _ in 0..3 {
        let repeat = recorder
            .measure("repeat_notification", &backend, 0, false, async {
                CatalogProjectionMaterializer::new(storage.clone())
                    .unwrap()
                    .drain_once()
                    .await
            })
            .await
            .unwrap();
        assert!(repeat.drained_record_ids.is_empty());
        assert!(repeat.quarantined_record_ids.is_empty());
    }
    stages.insert("repeat_notification", projection_measurement::finish());
    serde_json::json!({"tables":tables,"backlog":backlog,"interrupted":interrupt,"stages":stages,
        "fixture_sha256":hex::encode(Sha256::digest(serde_json::to_vec(&fixture).unwrap())),
        "operations":operations(recorder),"watermarks":proofs,"parity":true,"maintenance_retries":maintenance_retries})
}

pub async fn fresh_authority_and_retained_pagination() {
    let backend = Arc::new(CountingBackend::new(1));
    let fixture = Box::pin(fixture(&backend, 8, 0)).await;
    let storage = storage(backend.clone());
    let bindings = bindings();
    let first = request(&storage, &bindings)
        .list_tables_page("catalog", "schema", CatalogListRequest::new(1).unwrap())
        .await
        .unwrap();
    let token = first.next_page_token().unwrap().to_string();
    authority(&storage)
        .update_table_in_schema(
            "catalog",
            "schema",
            "table0001",
            TablePatch {
                description: Some(Some("advanced".into())),
                ..TablePatch::default()
            },
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let next = request(&storage, &bindings)
        .get_table("catalog", "schema", "table0001")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.description.as_deref(), Some("advanced"));
    let pinned = request(&storage, &bindings)
        .list_tables_page(
            "catalog",
            "schema",
            CatalogListRequest::new(1).unwrap().with_page_token(token),
        )
        .await
        .unwrap();
    check_tables(pinned.items(), &fixture.tables[1..2]);
}

pub async fn warm_cache_cannot_revive_missing_or_replaced_objects() {
    for replacement in [None, Some(Bytes::from_static(b"corrupt"))] {
        let backend = Arc::new(CountingBackend::new(1));
        let fixture = Box::pin(fixture(&backend, 8, 0)).await;
        let storage = storage(backend.clone());
        let bindings = bindings();
        request(&storage, &bindings)
            .list_tables("catalog", "schema")
            .await
            .unwrap();
        // All current source segments are read by the full catalog reconstruction.
        let paths: Vec<_> = fixture
            .objects
            .iter()
            .filter(|(path, _)| path.contains("/catalog/") && path.ends_with(".arrow"))
            .map(|(path, _)| path)
            .collect();
        assert!(!paths.is_empty());
        for path in paths {
            if let Some(data) = &replacement {
                backend
                    .inner
                    .put(path, data.clone(), WritePrecondition::None)
                    .await
                    .unwrap();
            } else {
                backend.inner.delete(path).await.unwrap();
            }
        }
        let warm = request(&storage, &bindings)
            .list_tables("catalog", "schema")
            .await;
        let direct_bindings = bindings_for_other_backend(&storage);
        let direct = request(&storage, &direct_bindings)
            .list_tables("catalog", "schema")
            .await;
        assert!(warm.is_err());
        assert_eq!(
            warm.unwrap_err().to_string(),
            direct.unwrap_err().to_string()
        );
    }
}
fn bindings_for_other_backend(storage: &ScopedStorage) -> CatalogAuthorityBindings {
    let bindings = bindings();
    let other = ScopedStorage::new(
        Arc::new(MemoryBackend::new()),
        storage.tenant_id(),
        storage.workspace_id(),
    )
    .unwrap();
    request(&other, &bindings);
    bindings
}

pub async fn registry_cancellation_keeps_shared_reservations_bounded() {
    let backend = Arc::new(CountingBackend::new(1));
    let fixture = Box::pin(fixture(&backend, 8, 0)).await;
    let storage = storage(backend.clone());
    let bindings = CatalogAuthorityBindings::new([
        CatalogAuthorityBinding::control_v1("runtime-tenant", "runtime-workspace"),
        CatalogAuthorityBinding::control_v1("runtime-tenant", "other"),
        CatalogAuthorityBinding::legacy("runtime-tenant", "legacy"),
    ])
    .unwrap();
    backend.pause_cache_payload.store(true, Ordering::SeqCst);
    let mut requests: Vec<_> = (0..32)
        .map(|_| {
            Box::pin(async {
                request(&storage, &bindings)
                    .list_tables("catalog", "schema")
                    .await
            })
        })
        .collect();
    for request in &mut requests {
        assert!(futures::poll!(request).is_pending());
    }
    let paused = bindings.test_read_cache_statistics().pop().unwrap();
    assert!(paused.active_loads > 0);
    assert!(paused.participants >= 32);
    requests.drain(..16);
    assert!(bindings.test_read_cache_statistics()[0].active_loads > 0);
    drop(requests);
    let released = bindings.test_read_cache_statistics().pop().unwrap();
    assert_eq!(released.active_loads, 0);
    assert_eq!(released.participants, 0);
    assert_eq!(released.metadata.reserved_bytes, 0);
    assert_eq!(released.decoded.reserved_bytes, 0);
    backend.pause_cache_payload.store(false, Ordering::SeqCst);
    backend.cache_payload_release.notify_waiters();
    let results = futures::future::join_all((0..32).map(|_| async {
        request(&storage, &bindings)
            .list_tables("catalog", "schema")
            .await
            .unwrap()
    }))
    .await;
    for result in results {
        check_tables(&result, &fixture.tables);
    }
    let final_stats = bindings.test_read_cache_statistics().pop().unwrap();
    assert_eq!(final_stats.underestimates, 0);
    assert_eq!(final_stats.metadata.capacity_bytes, 16 * 1024 * 1024);
    assert_eq!(final_stats.decoded.capacity_bytes, 64 * 1024 * 1024);
    for pool in [final_stats.metadata, final_stats.decoded] {
        assert!(pool.high_water_bytes <= pool.capacity_bytes);
    }
}

pub async fn conflicting_publication_is_still_quarantined() {
    let backend = Arc::new(CountingBackend::new(1));
    let fixture = Box::pin(fixture(&backend, 8, 1)).await;
    let storage = storage(backend.clone());
    backend.fail_put_countdown.store(7, Ordering::SeqCst);
    assert!(
        CatalogProjectionMaterializer::new(storage.clone())
            .unwrap()
            .drain_once()
            .await
            .is_err()
    );
    let (sequence, manifest, _) = &fixture.pending[0];
    let path =
        format!("control/v1/projections/catalog-parquet/{sequence:020}-{manifest}/manifest.json");
    let mut info: arco_catalog::manifest::SnapshotInfo =
        serde_json::from_slice(&storage.get_raw(&path).await.unwrap()).unwrap();
    info.total_rows += 1;
    let corrupted = Bytes::from(serde_json::to_vec(&info).unwrap());
    storage
        .put_raw(&path, corrupted.clone(), WritePrecondition::None)
        .await
        .unwrap();
    let report = CatalogProjectionMaterializer::new(storage.clone())
        .unwrap()
        .drain_once()
        .await
        .unwrap();
    assert!(report.drained_record_ids.is_empty());
    assert_eq!(report.quarantined_record_ids.len(), 1);
    assert_eq!(storage.get_raw(&path).await.unwrap(), corrupted);
    let worker =
        ProjectionOutboxWorker::new(storage, "catalog", CATALOG_PARQUET_PROJECTION_CONSUMER_ID)
            .unwrap();
    assert_eq!(worker.backlog().await.unwrap().pending_record_ids.len(), 1);
}

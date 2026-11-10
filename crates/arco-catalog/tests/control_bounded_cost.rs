//! Frozen authority-8 catalog workload shapes and explicit cost-lane entry points.
#![cfg(feature = "test-utils")]
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]

#[path = "../benches/support/control_cost.rs"]
#[allow(dead_code)]
mod control_cost;

use std::future::Future;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use arco_catalog::catalog_authority::{
    BoundedCatalogTestCommand, CatalogProjectionNotifierV2, PreparedBoundedCatalogMutation,
    bounded_capacity_v2_intent, bounded_capacity_v2_record_pair,
};
use arco_catalog::state_store::SyntheticKvEntry;
use arco_catalog::state_store::{CandidateRecoveryV2, ProjectionIntentV2};
use arco_catalog::workspace_restore::{
    OmittedDomainPolicy, RestoreWorkspaceToSnapshot, WorkspaceRestoreService,
    WorkspaceRestoreStatus,
};
use arco_catalog::workspace_snapshot::WorkspaceScope;
use arco_catalog::workspace_snapshot_service::{
    CreateWorkspaceSnapshotRequest, EventArchiveCapture, EventArchiveProvider,
    ProjectionWatermarkCut, ProjectionWatermarkProvider, RestoreSource, WorkspaceCaptureIo,
    WorkspaceDomainBinding, WorkspaceDomainRegistry, WorkspaceSnapshotService,
};
use arco_catalog::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, CatalogPatch, ColumnDefinition,
    ControlMvpReadCacheConfig, ControlMvpReadCacheStatistics, ControlMvpStateStore,
    RegisterTableInSchemaRequest, StateScope, TxnOptions, WriteOptions,
};
use arco_catalog::{
    CatalogError, ControlCatalogAuthority, ControlMvpRestoreParticipant, DurableAuthorityBinding,
};
use arco_core::{MemoryBackend, ScopedStorage};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};

const OPERATIONS_PER_SAMPLE: u64 = 200;
const REPETITIONS: u64 = 5;
const DECLARED_BLOCK_BOUND: u64 = 16;

async fn measure_catalog_request<T>(
    backend: &control_cost::CountingBackend,
    request: impl Future<Output = T>,
) -> (
    T,
    control_cost::RequestAllocations,
    control_cost::BackendCounts,
) {
    let prior_nested = backend.set_nested_allocation_measurement(false);
    let (result, allocations) = control_cost::measure_request_allocations(request).await;
    backend.set_nested_allocation_measurement(prior_nested);
    let counters = backend.take();
    (result, allocations, counters)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct FixtureAxis {
    retained_rows: u64,
    active_records: u64,
}

const FIXTURE_AXES: [FixtureAxis; 6] = [
    FixtureAxis {
        retained_rows: 4_096,
        active_records: 128,
    },
    FixtureAxis {
        retained_rows: 65_536,
        active_records: 128,
    },
    FixtureAxis {
        retained_rows: 1_048_576,
        active_records: 128,
    },
    FixtureAxis {
        retained_rows: 2_419_200,
        active_records: 128,
    },
    FixtureAxis {
        retained_rows: 4_096,
        active_records: 4_096,
    },
    FixtureAxis {
        retained_rows: 4_096,
        active_records: 65_536,
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scenario {
    CreateCatalog,
    CreateSchema,
    RegisterTable,
    PatchCatalog,
    RenameTable,
    DropTable,
    IdempotentReplay,
    StaleGenerationConflict,
    OrderedOutboxPage,
    ExactTrim,
}

impl Scenario {
    const fn label(self) -> &'static str {
        match self {
            Self::CreateCatalog => "create-catalog",
            Self::CreateSchema => "create-schema",
            Self::RegisterTable => "register-table",
            Self::PatchCatalog => "patch-catalog",
            Self::RenameTable => "rename-table",
            Self::DropTable => "drop-table",
            Self::IdempotentReplay => "idempotent-replay",
            Self::StaleGenerationConflict => "stale-generation-conflict",
            Self::OrderedOutboxPage => "ordered-outbox-page",
            Self::ExactTrim => "exact-trim",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CacheMode {
    Disabled,
    Default,
    Pressure,
}

impl CacheMode {
    const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Default => "default",
            Self::Pressure => "pressure",
        }
    }
}

const CACHE_MODES: [CacheMode; 3] = [CacheMode::Disabled, CacheMode::Default, CacheMode::Pressure];

const SCENARIOS: [Scenario; 10] = [
    Scenario::CreateCatalog,
    Scenario::CreateSchema,
    Scenario::RegisterTable,
    Scenario::PatchCatalog,
    Scenario::RenameTable,
    Scenario::DropTable,
    Scenario::IdempotentReplay,
    Scenario::StaleGenerationConflict,
    Scenario::OrderedOutboxPage,
    Scenario::ExactTrim,
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct FrozenCatalogOperation {
    scenario: Scenario,
    ordinal: u64,
    operation_id: String,
    idempotency_key: String,
    catalog: String,
    schema: String,
    table: String,
}

impl FrozenCatalogOperation {
    fn new(
        scenario: Scenario,
        axis: usize,
        cache: CacheMode,
        repetition: u64,
        ordinal: u64,
    ) -> Self {
        let prefix = format!(
            "{}-a{axis}-{}-r{repetition:02}-n{ordinal:03}",
            scenario.label(),
            cache.label(),
        );
        Self {
            scenario,
            ordinal,
            operation_id: format!("bounded-{prefix}"),
            idempotency_key: format!("bounded-idem-{prefix}"),
            catalog: format!("catalog-{prefix}"),
            schema: format!("schema-{prefix}"),
            table: format!("table-{prefix}"),
        }
    }
}

fn frozen_operations(
    scenario: Scenario,
    axis: usize,
    cache: CacheMode,
    repetition: u64,
) -> impl Iterator<Item = FrozenCatalogOperation> {
    (1..=OPERATIONS_PER_SAMPLE)
        .map(move |ordinal| FrozenCatalogOperation::new(scenario, axis, cache, repetition, ordinal))
}

/// Streams the ordered KV predecessor rows for explicit synthetic genesis.
///
/// Receipt keys precede audit keys by the production record key tags. Each row
/// is encoded by the catalog authority's production V2 record encoder.
struct V2PredecessorRows {
    next: u64,
    retained_rows: u64,
}

impl V2PredecessorRows {
    fn new(retained_rows: u64) -> Self {
        Self {
            next: 1,
            retained_rows,
        }
    }
}

impl Iterator for V2PredecessorRows {
    type Item = SyntheticKvEntry;

    fn next(&mut self) -> Option<Self::Item> {
        let ordinal = self.next;
        if ordinal > self.retained_rows {
            return None;
        }
        self.next += 1;
        let pair_count = self.retained_rows / 2;
        let source_ordinal = if ordinal <= pair_count {
            ordinal
        } else {
            ordinal - pair_count
        };
        let (receipt, audit) = bounded_capacity_v2_record_pair(source_ordinal)
            .expect("frozen production V2 fixture record");
        let (key, value) = if ordinal <= pair_count {
            receipt
        } else {
            audit
        };
        Some(SyntheticKvEntry {
            key,
            generation: source_ordinal,
            value: Some(value),
        })
    }
}

fn restore_backend_summary(counts: &control_cost::BackendCounts) -> serde_json::Value {
    serde_json::json!({
        "logical_storage_calls": counts.logical_storage_calls,
        "modeled_s3_no_retry_requests": counts.get_attempts + counts.range_get_attempts + counts.head_attempts
            + counts.put_attempts + counts.list_attempts + counts.list_page_attempts + counts.delete_attempts + counts.precondition_failures,
        "provider_model": "successful nonempty native ranges use one GET; conditional conflicts add one HEAD; failed-range fallback/SDK retries/LIST wire metadata require transport accounting",
        "get_attempts": counts.get_attempts,
        "range_get_attempts": counts.range_get_attempts,
        "head_attempts": counts.head_attempts,
        "head_missing": counts.head_missing,
        "head_failures": counts.head_failures,
        "put_attempts": counts.put_attempts,
        "head_cas_attempts": counts.head_cas_attempts,
        "precondition_failures": counts.precondition_failures,
        "delete_attempts": counts.delete_attempts,
        "list_attempts": counts.list_attempts,
        "list_page_attempts": counts.list_page_attempts,
        "head_metadata_bytes": counts.head_metadata_bytes,
        "list_returned_objects": counts.list_returned_objects,
        "list_metadata_bytes": counts.list_metadata_bytes,
        "sha256_helper_calls": counts.sha256_helper_calls,
        "sha256_helper_bytes": counts.sha256_helper_bytes,
        "gc_probe_batches": counts.gc_probe_batches,
        "gc_probe_requests": counts.gc_probe_requests,
        "gc_head_batches": counts.gc_head_batches,
        "gc_head_requests": counts.gc_head_requests,
        "metadata_decode_calls": counts.metadata_decode_calls,
        "object_io": if std::env::var_os("ARCO_GC_OBJECT_TRACE").is_some() { Some(&counts.object_io) } else { None },
        "object_write_attempt_bytes": if std::env::var_os("ARCO_GC_OBJECT_TRACE").is_some() { Some(&counts.object_writes) } else { None },
        "read_bytes": counts.read_bytes,
        "write_attempt_bytes": counts.write_attempt_bytes,
        "decoded_rows": counts.decoded_rows,
        "selected_blocks": counts.selected_blocks,
        "block_decode_bytes": counts.block_decode_bytes,
        "metadata_decode_bytes": counts.metadata_decode_bytes,
        "object_class_io": counts.object_class_io,
    })
}

struct EmptyRestoreCapture;

#[async_trait]
impl ProjectionWatermarkProvider for EmptyRestoreCapture {
    async fn capture(
        &self,
        _: &arco_catalog::workspace_snapshot::DomainAuthorityReference,
    ) -> arco_catalog::Result<ProjectionWatermarkCut> {
        ProjectionWatermarkCut::new(vec![], vec![], vec![])
    }

    async fn capture_bounded(
        &self,
        authority: &arco_catalog::workspace_snapshot::DomainAuthorityReference,
        _: &mut WorkspaceCaptureIo<'_>,
    ) -> arco_catalog::Result<ProjectionWatermarkCut> {
        ProjectionWatermarkProvider::capture(self, authority).await
    }
}

#[async_trait]
impl EventArchiveProvider for EmptyRestoreCapture {
    async fn capture(
        &self,
        authority: &arco_catalog::workspace_snapshot::DomainAuthorityReference,
    ) -> arco_catalog::Result<EventArchiveCapture> {
        EventArchiveCapture::new(
            arco_catalog::workspace_snapshot::DomainEventArchive::empty(authority.domain())?,
            vec![],
        )
    }

    async fn capture_bounded(
        &self,
        authority: &arco_catalog::workspace_snapshot::DomainAuthorityReference,
        _: &mut WorkspaceCaptureIo<'_>,
    ) -> arco_catalog::Result<EventArchiveCapture> {
        EventArchiveProvider::capture(self, authority).await
    }
}

#[tokio::test]
#[ignore = "manual frozen Step 3 large streaming qualification"]
async fn durable_restore_streams_frozen_receipts_outbox_and_disjoint_target() {
    Box::pin(run_frozen_restore_workload(FrozenWorkloadProbe::Restore)).await;
}

#[tokio::test]
#[ignore = "manual Step 4 closure capacity gate; active GC remains disabled"]
async fn authority8_gc_closure_on_frozen_restore_workload() {
    Box::pin(run_frozen_restore_workload(FrozenWorkloadProbe::Closure)).await;
}

#[tokio::test]
#[ignore = "manual full frozen mark slice; active GC remains disabled"]
async fn authority8_gc_mark_on_frozen_restore_workload() {
    Box::pin(run_frozen_restore_workload(FrozenWorkloadProbe::Mark)).await;
}

#[tokio::test]
#[ignore = "manual frozen marked GC planning; active deletion remains disabled"]
async fn authority8_marked_gc_plan_on_frozen_restore_workload() {
    Box::pin(run_frozen_restore_workload(FrozenWorkloadProbe::MarkedGc)).await;
}

#[tokio::test]
#[ignore = "manual unchanged frozen complete sweep; no active deletion"]
async fn authority8_gc_complete_sweep_on_frozen_restore_workload() {
    Box::pin(run_frozen_restore_workload(FrozenWorkloadProbe::Sweep)).await;
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "independent graph and inventory axes keep the complete oracle setup visible"
)]
async fn authority8_sweep_matches_independent_inventory_oracle_and_has_flat_page_cost() {
    for rows in [0_u64, 512, 65_536] {
        for orphan_count in [0_u64, 1, 600] {
            let backend = Arc::new(control_cost::CountingBackend::new(1));
            let storage =
                ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
            let store = ControlMvpStateStore::new_synthetic_bounded(
                storage.clone(),
                StateScope::new("tenant", "workspace", "catalog"),
            )
            .expect("store");
            store
                .install_synthetic_genesis(
                    "sweep-oracle",
                    1,
                    rows,
                    (0..rows).map(|n| SyntheticKvEntry {
                        key: n.to_be_bytes().to_vec(),
                        generation: 1,
                        value: Some(vec![42; 80]),
                    }),
                    0,
                    [],
                )
                .await
                .expect("fixture");
            if rows == 65_536 && orphan_count == 1 {
                let mut txn = store
                    .begin_control_txn(TxnOptions::default())
                    .await
                    .expect("path copy");
                txn.set_logical_operation_v2("overlap", "test", &"a1".repeat(32))
                    .expect("operation");
                txn.put(&0_u64.to_be_bytes(), Bytes::from_static(b"changed"))
                    .await
                    .expect("write");
                txn.commit_v2().await.expect("overlapping retained root");
            }
            let mut expected = std::collections::BTreeSet::new();
            for n in 0..orphan_count {
                let path = if n % 2 == 0 {
                    store.paths().state_object(&format!("000-orphan-{n:06}"))
                } else {
                    format!(
                        "control/directory/v1/domains/catalog/pages/{}",
                        hex::encode(Sha256::digest(n.to_le_bytes()))
                    )
                };
                storage
                    .put_raw(
                        &path,
                        Bytes::from_static(b"orphan"),
                        arco_core::WritePrecondition::DoesNotExist,
                    )
                    .await
                    .expect("orphan");
                expected.insert(path);
            }
            // All published historical roots remain protected; the oracle tracks inserted orphans independently.
            backend.take();
            let worker = arco_catalog::ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
            let seed = worker
                .begin_gc_mark_at(Utc::now() + ChronoDuration::days(40))
                .await
                .expect("seed");
            let mut root_pages = 0;
            let mut cursor = None;
            loop {
                cursor = worker
                    .advance_gc_mark(&seed, cursor.as_ref())
                    .await
                    .expect("schedule")
                    .continuation;
                root_pages += 1;
                if cursor.is_none() {
                    break;
                }
            }
            let mut planning = worker
                .begin_marked_gc_plan(&seed, [], None)
                .await
                .expect("admit");
            let mut actual = std::collections::BTreeSet::new();
            let mut inventory_pages = 0;
            let mut warm_calls = 0;
            let mut ready = None;
            loop {
                let plan = if let Some(plan) = ready.take() {
                    plan
                } else {
                    loop {
                        let result = worker
                            .advance_marked_gc_plan(&mut planning)
                            .await
                            .expect("verify/filter");
                        let (bytes, ops) = planning.last_admission();
                        assert!(bytes <= 64 * 1024 * 1024 && ops <= 4096);
                        if let Some(plan) = result {
                            break plan;
                        }
                    }
                };
                inventory_pages += 1;
                for candidate in plan.candidates() {
                    assert!(actual.insert(candidate.path().to_owned()));
                }
                if plan.continuation().is_none() {
                    break;
                }
                backend.take();
                let next = worker
                    .next_marked_gc_plan_page(&mut planning, [])
                    .await
                    .expect("reuse verified sweep");
                let cost = backend.take();
                let page_calls = cost.logical_storage_calls;
                assert!(
                    page_calls <= 24 + next.candidates().len() as u64,
                    "one complete warm page must use one freshness pair: {page_calls}"
                );
                ready = Some(next);
                warm_calls += page_calls;
                for class in [
                    "physical_descriptor",
                    "directory",
                    "directory_page",
                    "external_fence",
                    "data",
                ] {
                    if let Some(io) = cost.object_class_io.get(class) {
                        assert_eq!(
                            io.range_get_attempts + io.full_get_attempts,
                            0,
                            "warm page replayed {class}"
                        );
                    }
                }
                assert_eq!(planning.verified_pages(), root_pages);
            }
            assert_eq!(actual, expected, "rows={rows}, orphans={orphan_count}");
            assert!(
                worker
                    .continue_marked_gc_plan(&mut planning, [])
                    .await
                    .is_err()
            );
            if orphan_count == 600 {
                assert!(inventory_pages >= 4 && warm_calls > 0);
            }
        }
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "valid snapshot and unchanged pin are both essential to this freshness regression"
)]
async fn authority8_verified_sweep_refences_workspace_record_with_unchanged_pin() {
    let storage =
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").expect("storage");
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = Arc::new(
        ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope.clone())
            .expect("store")
            .with_durable_authority_binding(DurableAuthorityBinding::new([83; 32])),
    );
    store
        .install_synthetic_genesis(
            "record-fence",
            1,
            1,
            [SyntheticKvEntry {
                key: b"live".to_vec(),
                generation: 1,
                value: Some(vec![42; 80]),
            }],
            0,
            [],
        )
        .await
        .expect("fixture");
    let binding = WorkspaceDomainBinding::new(
        scope,
        store.clone(),
        store.clone(),
        Arc::new(EmptyRestoreCapture),
        Arc::new(EmptyRestoreCapture),
    )
    .expect("binding");
    let registry = WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        vec![binding],
    )
    .expect("registry");
    let now = Utc::now();
    let snapshot_id = format!("snap_{}", ulid::Ulid::from(884_u128));
    let pin_id = format!("pin_{}", ulid::Ulid::from(885_u128));
    WorkspaceSnapshotService::new(storage.clone(), registry)
        .expect("service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::days(20),
                None,
            )
            .expect("request"),
        )
        .await
        .expect("snapshot");
    let worker = arco_catalog::ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
    let seed = worker
        .begin_gc_mark_at(now + ChronoDuration::days(8))
        .await
        .expect("seed");
    let mut cursor = None;
    loop {
        cursor = worker
            .advance_gc_mark(&seed, cursor.as_ref())
            .await
            .expect("mark")
            .continuation;
        if cursor.is_none() {
            break;
        }
    }
    let mut planning = worker
        .begin_marked_gc_plan(&seed, [], None)
        .await
        .expect("admit");
    loop {
        if worker
            .advance_marked_gc_plan(&mut planning)
            .await
            .expect("verify")
            .is_some()
        {
            break;
        }
    }
    let path =
        arco_catalog::workspace_snapshot::snapshot_record_path(&snapshot_id).expect("record path");
    let pins_before = storage.list_meta("retention/pins/").await.expect("pins");
    let bytes = storage.get_raw(&path).await.expect("record");
    storage
        .put_raw(&path, bytes, arco_core::WritePrecondition::None)
        .await
        .expect("mutate record");
    let pins_after = storage.list_meta("retention/pins/").await.expect("pins");
    assert_eq!(
        pins_before
            .iter()
            .map(|m| (&m.path, &m.version))
            .collect::<Vec<_>>(),
        pins_after
            .iter()
            .map(|m| (&m.path, &m.version))
            .collect::<Vec<_>>()
    );
    assert!(
        matches!(
            worker.continue_marked_gc_plan(&mut planning, []).await,
            Err(CatalogError::CasFailed { .. })
        ),
        "unchanged pin selector cannot excuse a changed authenticated root record"
    );
}

// A cold complete proof must read each physical descriptor at most once.
#[tokio::test]
async fn authority8_marking_reads_each_descriptor_once() {
    let backend = Arc::new(control_cost::CountingBackend::new(1));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = ControlMvpStateStore::new_synthetic_bounded(
        storage,
        StateScope::new("tenant", "workspace", "catalog"),
    )
    .expect("store");
    store
        .install_synthetic_genesis(
            "gc-io-red",
            1,
            65_536,
            (0_u64..65_536).map(|n| SyntheticKvEntry {
                key: n.to_be_bytes().to_vec(),
                generation: 1,
                value: Some(vec![42; 80]),
            }),
            0,
            [],
        )
        .await
        .expect("fixture");
    let setup = backend.take();
    let descriptors = setup.object_class_io["physical_descriptor"].put_attempts;
    let worker = arco_catalog::ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
    let seed = worker.begin_gc_mark_at(Utc::now()).await.expect("seed");
    let mut cursor = None;
    loop {
        cursor = worker
            .advance_gc_mark(&seed, cursor.as_ref())
            .await
            .expect("mark")
            .continuation;
        if cursor.is_none() {
            break;
        }
    }
    backend
        .gc_probe_yield
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut verifier = worker
        .begin_gc_mark_verification(&seed)
        .await
        .expect("verifier");
    while !verifier.is_complete() {
        worker
            .advance_gc_mark_verification(&mut verifier)
            .await
            .expect("verify");
    }
    let cost = backend.take();
    assert!(
        cost.object_class_io["physical_descriptor"].range_get_attempts <= descriptors,
        "cold authentication repeated descriptor reads: {:?}, unique fixture descriptors: {descriptors}",
        cost.object_class_io["physical_descriptor"]
    );
    assert_eq!(cost.delete_attempts, 0);
    assert_eq!(
        cost.gc_probe_requests, descriptors,
        "concurrency work must be measured at its owner"
    );
    assert!(cost.gc_probe_batches > 0 && cost.gc_probe_requests <= 8 * cost.gc_probe_batches);
    assert_eq!(cost.gc_head_requests, 2 * cost.gc_head_batches);
    let parallel = backend
        .gc_max_probes
        .load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        (2..=8).contains(&parallel),
        "authenticated probes must overlap within the fixed bound: {parallel}"
    );
}

#[allow(
    clippy::print_stdout,
    clippy::too_many_lines,
    reason = "manual frozen recovery keeps cold and warm invocation receipts together"
)]
async fn qualify_frozen_gc_restarts(
    storage: &ScopedStorage,
    backend: &control_cost::CountingBackend,
    seed: &str,
    cursor: &str,
    orphan: &str,
) {
    for restart in 0..2 {
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .expect("cold restart store")
        .with_durable_authority_binding(DurableAuthorityBinding::new([83; 32]));
        let worker = arco_catalog::ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
        let unit_started = Instant::now();
        let mut planning = worker
            .begin_marked_gc_plan(seed, [], Some(cursor))
            .await
            .expect("restart admission");
        let elapsed_us = unit_started.elapsed().as_micros();
        let mut calls = 0;
        let cost = backend.take();
        calls += cost.logical_storage_calls;
        println!(
            "step4-restart-admission {}",
            serde_json::json!({"restart":restart,"elapsed_us":elapsed_us,"admitted_bytes":planning.last_admission().0,
                "charged_operations":planning.last_admission().1,"backend":restore_backend_summary(&cost)})
        );
        assert_eq!(planning.verified_pages(), 0);
        loop {
            let unit_started = Instant::now();
            let (result, allocations, cost) =
                measure_catalog_request(backend, worker.advance_marked_gc_plan(&mut planning))
                    .await;
            let elapsed_us = unit_started.elapsed().as_micros();
            let result = result.expect("restart verify");
            let (bytes, operations) = planning.last_admission();
            assert!(
                bytes <= 64 * 1024 * 1024
                    && operations <= 4096
                    && cost.logical_storage_calls <= operations as u64
            );
            calls += cost.logical_storage_calls;
            println!(
                "step4-restart-unit {}",
                serde_json::json!({"restart":restart,"elapsed_us":elapsed_us,"allocation_peak_upper_bound":allocations.peak_owned_bytes,
                "observed_net_poll_peak_bytes":allocations.net_poll_peak_bytes,"admitted_bytes":bytes,
                "charged_operations":operations,"backend":restore_backend_summary(&cost)})
            );
            if let Some(plan) = result {
                assert_eq!(
                    plan.candidates()
                        .iter()
                        .map(arco_catalog::state_store::ControlMvpGcCandidate::path)
                        .collect::<Vec<_>>(),
                    vec![orphan]
                );
                break;
            }
        }
        assert!(
            calls <= 99_365,
            "restart is one cold verification, independent of inventory pages"
        );
        for page in 0..2 {
            let unit_started = Instant::now();
            let (plan, allocations, cost) = measure_catalog_request(
                backend,
                worker.next_marked_gc_plan_page(&mut planning, []),
            )
            .await;
            let plan = plan.expect("atomic warm restart page");
            let elapsed_us = unit_started.elapsed().as_micros();
            let (bytes, operations) = planning.last_admission();
            assert!(bytes <= 64 * 1024 * 1024 && operations <= 4096);
            assert!(plan.candidates().is_empty());
            assert!(cost.logical_storage_calls <= 24);
            assert!(
                cost.object_class_io
                    .iter()
                    .filter(|(class, _)| matches!(
                        class.as_str(),
                        "physical_descriptor"
                            | "directory"
                            | "directory_page"
                            | "external_fence"
                            | "data"
                    ))
                    .all(|(_, io)| io.range_get_attempts + io.full_get_attempts == 0)
            );
            println!(
                "step4-restart-warm-page {}",
                serde_json::json!({"restart":restart,"page":page,"elapsed_us":elapsed_us,"allocation_peak_upper_bound":allocations.peak_owned_bytes,"observed_net_poll_peak_bytes":allocations.net_poll_peak_bytes,"admitted_bytes":bytes,
                "charged_operations":operations,"backend":restore_backend_summary(&cost)})
            );
        }
        println!(
            "step4-restart-complete {}",
            serde_json::json!({"restart":restart,"cold_calls":calls})
        );
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FrozenWorkloadProbe {
    Restore,
    Closure,
    Mark,
    MarkedGc,
    Sweep,
}

#[allow(
    clippy::too_many_lines,
    clippy::tuple_array_conversions,
    clippy::cognitive_complexity,
    clippy::print_stdout,
    clippy::panic,
    reason = "manual qualification keeps fixture construction, raw receipts, and paired read assertions together"
)]
async fn run_frozen_restore_workload(probe: FrozenWorkloadProbe) {
    const RETAINED_ROWS: u64 = 2_419_200;
    const TARGET_ROWS: u64 = 1_048_576;
    const ACTIVE_OUTBOX: u64 = 65_536;
    let retained_rows = std::env::var("ARCO_STEP3_RETAINED_ROWS")
        .map_or(RETAINED_ROWS, |n| n.parse().expect("retained rows"));
    let target_rows = std::env::var("ARCO_STEP3_TARGET_ROWS")
        .map_or(TARGET_ROWS, |n| n.parse().expect("target rows"));
    let active_outbox = std::env::var("ARCO_STEP3_ACTIVE_OUTBOX")
        .map_or(ACTIVE_OUTBOX, |n| n.parse().expect("active outbox"));
    if probe != FrozenWorkloadProbe::Restore {
        assert_eq!(
            (retained_rows, target_rows, active_outbox),
            (RETAINED_ROWS, TARGET_ROWS, ACTIVE_OUTBOX),
            "the Step 4 capacity gate requires the complete frozen fixture"
        );
    }
    assert!(
        retained_rows > 0
            && retained_rows % 2 == 0
            && target_rows > 0
            && (4..=retained_rows / 2 + 1).contains(&active_outbox)
    );
    let backend = Arc::new(control_cost::CountingBackend::new(1));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = Arc::new(
        ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope.clone())
            .expect("synthetic authority 8")
            .with_durable_authority_binding(DurableAuthorityBinding::new([83; 32])),
    );
    let fixture_started = Instant::now();
    let source = store
        .install_synthetic_genesis(
            "step3-large-source",
            retained_rows / 2,
            retained_rows,
            V2PredecessorRows::new(retained_rows),
            0,
            Vec::new(),
        )
        .await
        .expect("source genesis");
    let registry = || {
        let binding = WorkspaceDomainBinding::new(
            scope.clone(),
            store.clone(),
            store.clone(),
            Arc::new(EmptyRestoreCapture),
            Arc::new(EmptyRestoreCapture),
        )
        .expect("source binding")
        .with_restore_participant(Arc::new(ControlMvpRestoreParticipant::new(
            store.as_ref().clone(),
        )))
        .expect("restore binding");
        WorkspaceDomainRegistry::new(
            WorkspaceScope::new("tenant", "workspace").expect("scope"),
            vec![binding],
        )
        .expect("registry")
    };
    let snapshot_id = format!("snap_{}", ulid::Ulid::from(830_u128));
    let pin_id = format!("pin_{}", ulid::Ulid::from(831_u128));
    let now = Utc::now();
    WorkspaceSnapshotService::new(storage.clone(), registry())
        .expect("snapshot service")
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::days(2),
                None,
            )
            .expect("snapshot request"),
        )
        .await
        .expect("retained snapshot");
    storage
        .delete("control/v1/domains/catalog/head/current.json")
        .await
        .expect("synthetic target reset");
    let target_entries = (0..target_rows).map(|ordinal| {
        let mut key = vec![255];
        key.extend_from_slice(&ordinal.to_be_bytes());
        SyntheticKvEntry {
            key,
            generation: retained_rows / 2 + 1,
            value: Some(vec![0x42; 80]),
        }
    });
    let target_sequence = retained_rows / 2 + 1;
    let target_intents = (1..=active_outbox).map(|ordinal| {
        bounded_capacity_v2_intent(scope.clone(), ordinal, 0).expect("target outbox intent")
    });
    store
        .install_synthetic_genesis(
            "step3-large-target",
            target_sequence,
            target_rows,
            target_entries,
            active_outbox,
            target_intents,
        )
        .await
        .expect("target genesis");
    let orphan = store.paths().state_object("000-marked-gc-orphan");
    if matches!(
        probe,
        FrozenWorkloadProbe::MarkedGc | FrozenWorkloadProbe::Sweep
    ) {
        storage
            .put_raw(
                &orphan,
                Bytes::from_static(b"orphan"),
                arco_core::WritePrecondition::DoesNotExist,
            )
            .await
            .expect("eligible frozen orphan");
    }
    let fixture_counts = backend.take();
    println!(
        "step3-large-fixture {}",
        serde_json::json!({
            "retained_rows": retained_rows,
            "target_rows": target_rows,
            "active_outbox": active_outbox,
            "source_manifest": source.authority_manifest_id(),
            "fixture_elapsed_ms": fixture_started.elapsed().as_millis(),
            "backend": restore_backend_summary(&fixture_counts),
        })
    );

    if matches!(
        probe,
        FrozenWorkloadProbe::Mark | FrozenWorkloadProbe::MarkedGc | FrozenWorkloadProbe::Sweep
    ) {
        let worker = arco_catalog::ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
        let started = Instant::now();
        let (seed, seed_bytes, seed_operations) = worker
            .begin_gc_mark_at_with_admission(now + ChronoDuration::days(40))
            .await
            .expect("frozen mark seed");
        let seed_elapsed_us = started.elapsed().as_micros();
        let seed_cost = backend.take();
        assert!(
            seed_cost.logical_storage_calls <= seed_operations as u64 && seed_operations <= 4096
        );
        assert!(seed_bytes <= 64 * 1024 * 1024);
        assert!(seed_cost.read_bytes <= 64 * 1024 * 1024);
        assert_eq!(seed_cost.delete_attempts, 0);
        assert_eq!(seed_cost.head_cas_attempts, 0);
        println!(
            "step4-frozen-mark-seed {}",
            serde_json::json!({
                "seed": seed, "elapsed_us":seed_elapsed_us,"admitted_bytes":seed_bytes,"charged_operations":seed_operations,
                "backend": restore_backend_summary(&seed_cost),
            })
        );
        let mut total_calls = seed_cost.logical_storage_calls;
        let mut total_reads = seed_cost.read_bytes;
        let mut total_writes = seed_cost.write_attempt_bytes;
        let mut cursor = None;
        let mut pages = 0_u64;
        let mut max_calls = 0_u64;
        let mut max_bytes = 0_u64;
        loop {
            let unit_started = Instant::now();
            let page = worker
                .advance_gc_mark(&seed, cursor.as_ref())
                .await
                .expect("bounded frozen mark page");
            let elapsed_us = unit_started.elapsed().as_micros();
            let cost = backend.take();
            total_calls += cost.logical_storage_calls;
            total_reads += cost.read_bytes;
            total_writes += cost.write_attempt_bytes;
            assert!(cost.logical_storage_calls <= 4096);
            assert!(cost.read_bytes <= 64 * 1024 * 1024);
            assert!(page.charged_operations <= 4096);
            assert!(page.admitted_bytes <= 64 * 1024 * 1024);
            assert!(cost.logical_storage_calls <= page.charged_operations as u64);
            assert_eq!(cost.decoded_rows, 0);
            assert_eq!(
                cost.object_class_io
                    .get("data")
                    .map_or(0, |io| io.full_get_attempts + io.range_get_attempts),
                0
            );
            assert_eq!(cost.delete_attempts, 0);
            assert_eq!(cost.head_cas_attempts, 0);
            max_calls = max_calls.max(cost.logical_storage_calls);
            max_bytes = max_bytes.max(cost.read_bytes);
            pages += 1;
            println!(
                "step4-frozen-mark-page {}",
                serde_json::json!({
                    "ordinal": pages - 1, "elapsed_us":elapsed_us,"sha256": page.page_sha256,
                "admitted_bytes": page.admitted_bytes, "charged_operations": page.charged_operations,
                    "backend": restore_backend_summary(&cost),
                })
            );
            cursor = page.continuation;
            if cursor.is_none() {
                break;
            }
        }
        if matches!(
            probe,
            FrozenWorkloadProbe::MarkedGc | FrozenWorkloadProbe::Sweep
        ) {
            let inventory_cursor =
                format!("v8:authority:{}/segments/l1/", store.paths().base_prefix());
            let unit_started = Instant::now();
            let mut planning = worker
                .begin_marked_gc_plan(
                    &seed,
                    [],
                    (probe == FrozenWorkloadProbe::MarkedGc).then_some(inventory_cursor.as_str()),
                )
                .await
                .expect("bounded frozen candidate inventory");
            let elapsed_us = unit_started.elapsed().as_micros();
            let cost = backend.take();
            total_calls += cost.logical_storage_calls;
            total_reads += cost.read_bytes;
            total_writes += cost.write_attempt_bytes;
            let (bytes, operations) = planning.last_admission();
            assert!(cost.logical_storage_calls <= operations as u64 && operations <= 4096);
            assert!(cost.read_bytes <= 64 * 1024 * 1024 && bytes <= 64 * 1024 * 1024);
            assert_eq!(cost.delete_attempts, 0);
            assert_eq!(cost.head_cas_attempts, 0);
            println!(
                "step4-frozen-marked-gc-admission {}",
                serde_json::json!({"elapsed_us":elapsed_us,"admitted_bytes": bytes, "charged_operations": operations, "backend": restore_backend_summary(&cost)})
            );
            let mut inventory_pages = 0_u64;
            let mut candidates = std::collections::BTreeSet::new();
            let mut ready = None;
            loop {
                let result = if let Some(plan) = ready.take() {
                    Some(plan)
                } else {
                    let unit_started = Instant::now();
                    let (result, allocations, cost) = measure_catalog_request(
                        &backend,
                        worker.advance_marked_gc_plan(&mut planning),
                    )
                    .await;
                    let elapsed_us = unit_started.elapsed().as_micros();
                    let result = result.expect("bounded independent marked candidate filtering");
                    total_calls += cost.logical_storage_calls;
                    total_reads += cost.read_bytes;
                    total_writes += cost.write_attempt_bytes;
                    let (bytes, operations) = planning.last_admission();
                    assert!(cost.logical_storage_calls <= operations as u64 && operations <= 4096);
                    assert!(cost.read_bytes <= 64 * 1024 * 1024 && bytes <= 64 * 1024 * 1024);
                    assert_eq!(cost.decoded_rows, 0);
                    assert_eq!(
                        cost.object_class_io
                            .get("data")
                            .map_or(0, |io| io.full_get_attempts + io.range_get_attempts),
                        0
                    );
                    assert_eq!(cost.delete_attempts, 0);
                    assert_eq!(cost.head_cas_attempts, 0);
                    max_calls = max_calls.max(cost.logical_storage_calls);
                    max_bytes = max_bytes.max(cost.read_bytes);
                    println!(
                        "step4-frozen-marked-gc-verified-page {}",
                        serde_json::json!({"completed_roots": planning.verified_pages(), "elapsed_us":elapsed_us,"allocation_bytes": allocations.bytes,
                        "allocation_peak_upper_bound": allocations.peak_owned_bytes,
                        "observed_net_poll_peak_bytes": allocations.net_poll_peak_bytes, "admitted_bytes": bytes, "charged_operations": operations, "backend": restore_backend_summary(&cost)})
                    );
                    result
                };
                if let Some(plan) = result {
                    assert_eq!(planning.verified_pages(), pages);
                    inventory_pages += 1;
                    for candidate in plan.candidates() {
                        assert!(candidates.insert(candidate.path().to_owned()));
                    }
                    if probe == FrozenWorkloadProbe::Sweep && plan.continuation().is_some() {
                        let unit_started = Instant::now();
                        ready = Some(
                            worker
                                .next_marked_gc_plan_page(&mut planning, [])
                                .await
                                .expect("next verified sweep page"),
                        );
                        let elapsed_us = unit_started.elapsed().as_micros();
                        let cost = backend.take();
                        total_calls += cost.logical_storage_calls;
                        total_reads += cost.read_bytes;
                        total_writes += cost.write_attempt_bytes;
                        let (bytes, operations) = planning.last_admission();
                        assert!(
                            cost.logical_storage_calls <= 512
                                && operations <= 4096
                                && bytes <= 64 * 1024 * 1024
                        );
                        println!(
                            "step4-frozen-sweep-inventory {}",
                            serde_json::json!({"page": inventory_pages,"elapsed_us":elapsed_us,
                            "admitted_bytes":bytes,"charged_operations":operations,"backend":restore_backend_summary(&cost)})
                        );
                        continue;
                    }
                    assert_eq!(
                        candidates,
                        std::collections::BTreeSet::from([orphan.clone()])
                    );
                    let proof_inventory = storage
                        .list_meta(&format!(
                            "{}/gc-marks/v1/{seed}/",
                            store.paths().base_prefix()
                        ))
                        .await
                        .expect("proof footprint");
                    let proof_bytes = proof_inventory
                        .iter()
                        .map(|object| object.size)
                        .sum::<u64>();
                    assert!(proof_bytes <= 128 * 1024, "persistent proof footprint");
                    let diagnostic = backend.take();
                    println!(
                        "step4-proof-footprint {}",
                        serde_json::json!({"persisted_bytes":proof_bytes,
                        "objects":proof_inventory.len(),"diagnostic_backend":restore_backend_summary(&diagnostic)})
                    );
                    println!(
                        "step4-frozen-marked-gc-complete {}",
                        serde_json::json!({
                            "seed": seed, "pages":pages,"inventory_pages":inventory_pages,"candidate_count":candidates.len(),
                            "total_calls":total_calls,"total_read_bytes":total_reads,"proof_write_attempt_bytes":total_writes,
                            "elapsed_ms":started.elapsed().as_millis(),"max_calls":max_calls,"max_read_bytes":max_bytes,
                            "active_authority8_delete":false,"complete_sweep":probe == FrozenWorkloadProbe::Sweep,
                        })
                    );
                    assert!(
                        total_calls
                            <= if probe == FrozenWorkloadProbe::Sweep {
                                250_000
                            } else {
                                99_365
                            },
                        "frozen request target"
                    );
                    assert!(
                        total_reads
                            <= if probe == FrozenWorkloadProbe::Sweep {
                                200
                            } else {
                                150
                            } * 1024
                                * 1024,
                        "metadata read ceiling"
                    );
                    assert!(total_writes <= 256 * 1024, "proof-write ceiling");
                    break;
                }
                assert!(
                    planning.verified_pages() < pages,
                    "no plan from partial or unending coverage"
                );
            }
            if probe == FrozenWorkloadProbe::Sweep {
                qualify_frozen_gc_restarts(&storage, &backend, &seed, &inventory_cursor, &orphan)
                    .await;
            }
            return;
        }
        let mut verifier = worker
            .begin_gc_mark_verification(&seed)
            .await
            .expect("independent frozen verifier");
        let cost = backend.take();
        assert!(cost.logical_storage_calls <= 4096);
        assert_eq!(cost.delete_attempts, 0);
        assert_eq!(cost.head_cas_attempts, 0);
        while !verifier.is_complete() {
            worker
                .advance_gc_mark_verification(&mut verifier)
                .await
                .expect("independent bounded frozen coverage");
            let cost = backend.take();
            assert!(cost.logical_storage_calls <= 4096);
            assert!(cost.read_bytes <= 64 * 1024 * 1024);
            let (bytes, operations) = verifier.last_admission();
            assert!(operations <= 4096);
            assert!(bytes <= 64 * 1024 * 1024);
            assert!(cost.logical_storage_calls <= operations as u64);
            assert_eq!(cost.decoded_rows, 0);
            assert_eq!(
                cost.object_class_io
                    .get("data")
                    .map_or(0, |io| io.full_get_attempts + io.range_get_attempts),
                0
            );
            assert_eq!(cost.delete_attempts, 0);
            assert_eq!(cost.head_cas_attempts, 0);
            max_calls = max_calls.max(cost.logical_storage_calls);
            max_bytes = max_bytes.max(cost.read_bytes);
            println!(
                "step4-frozen-mark-verified-page {}",
                serde_json::json!({
                    "ordinal": verifier.verified_pages() - 1,
                "admitted_bytes": bytes, "charged_operations": operations,
                    "backend": restore_backend_summary(&cost),
                })
            );
        }
        assert_eq!(verifier.verified_pages(), pages);
        println!(
            "step4-frozen-mark-complete {}",
            serde_json::json!({
                "seed": seed, "pages": pages, "elapsed_ms": started.elapsed().as_millis(),
                "max_calls": max_calls, "max_read_bytes": max_bytes,
                "active_authority8_delete": false,
            })
        );
        return;
    }
    if probe == FrozenWorkloadProbe::Closure {
        let worker = arco_catalog::ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
        let started = Instant::now();
        let result = worker
            .plan_gc_page_at(now + ChronoDuration::days(40), Vec::new(), None)
            .await;
        let counters = backend.take();
        println!(
            "step4-frozen-closure {}",
            serde_json::json!({
                "elapsed_ms": started.elapsed().as_millis(),
                "backend": restore_backend_summary(&counters),
                "error": result.as_ref().err().map(ToString::to_string),
            })
        );
        let plan = result.expect("frozen current and retained closure must fit a bounded GC page");
        assert!(
            plan.candidates().is_empty(),
            "every fixture object is protected"
        );
        return;
    }

    let restore_id = format!("rst_{}", ulid::Ulid::from(832_u128));
    let service =
        WorkspaceRestoreService::new(storage.clone(), registry()).expect("restore service");
    let request = RestoreWorkspaceToSnapshot::new(
        &restore_id,
        RestoreSource::snapshot(snapshot_id, pin_id).expect("source"),
        WorkspaceScope::new("tenant", "workspace").expect("scope"),
        now,
        OmittedDomainPolicy::Reject,
    )
    .expect("restore request");
    let mut candidate_id = None;
    let mut prior_selector = None;
    let mut head_sends = 0;
    let mut terminal_sent = false;
    for invocation in 0..32_768_u64 {
        let started = Instant::now();
        let (outcome, allocations, backend_counts) = measure_catalog_request(&backend, async {
            if invocation == 0 {
                service.restore_workspace_to_snapshot(&request).await
            } else {
                service.recover_restore(&restore_id).await
            }
        })
        .await;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                let journal = storage
                    .get_raw(&format!("transactions/restores/{restore_id}/journal.json"))
                    .await;
                println!(
                    "step3-large-error {}",
                    serde_json::json!({
                        "invocation": invocation,
                        "error": error.to_string(),
                        "journal": journal.map(|bytes| String::from_utf8_lossy(&bytes).to_string()).ok(),
                        "backend": restore_backend_summary(&backend_counts),
                    })
                );
                panic!("bounded restore invocation: {error}");
            }
        };
        head_sends += backend_counts.head_cas_attempts;
        println!(
            "step3-large-unit {}",
            serde_json::json!({
                "invocation": invocation,
                "status": format!("{:?}", outcome.status()),
                "elapsed_ms": started.elapsed().as_millis(),
                "allocation_bytes_upper_bound": allocations.peak_owned_bytes,
                "net_poll_peak_bytes_diagnostic": allocations.net_poll_peak_bytes,
                "backend": restore_backend_summary(&backend_counts),
            })
        );
        if outcome.status() == WorkspaceRestoreStatus::Visible {
            assert_eq!(head_sends, 1, "one original-condition HEAD send");
            assert!(outcome.read_manifest().is_some());
            let token = store.current_state_token().await.expect("current");
            assert_eq!(
                store
                    .get(&[255, 0, 0, 0, 0, 0, 0, 0, 0])
                    .await
                    .expect("target key"),
                None
            );
            assert_eq!(token.logical_sequence(), target_sequence + 1);
            for ordinal in [1, retained_rows / 4, retained_rows / 2] {
                let (receipt, audit) = bounded_capacity_v2_record_pair(ordinal).expect("V2 pair");
                for (key, expected) in [receipt, audit] {
                    let actual = store.get(&key).await.expect("source key").expect("value");
                    assert_eq!(actual.as_ref(), expected.as_slice());
                }
            }
            let page = store
                .projection_outbox_page_v2(Some(token), None, 4)
                .await
                .expect("visible outbox page");
            assert_eq!(page.records().len(), 4);
            for (index, intent) in page.records().iter().enumerate() {
                assert_eq!(
                    intent.intent_id(),
                    format!("intent-{:020}-00000000000000000000", index + 1)
                );
            }
            return;
        }
        assert_eq!(outcome.status(), WorkspaceRestoreStatus::Applying);
        assert!(outcome.read_manifest().is_none());
        assert!(
            !terminal_sent,
            "visible candidate did not finalize on the next call"
        );
        if backend_counts.head_cas_attempts == 1 {
            terminal_sent = true;
            continue;
        }
        assert_eq!(backend_counts.head_cas_attempts, 0);
        if candidate_id.is_none() {
            let attempt: serde_json::Value = serde_json::from_slice(
                &storage
                    .get_raw(&format!(
                        "transactions/restores/{restore_id}/attempts/00000000000000000001.plan.json"
                    ))
                    .await
                    .expect("selected attempt"),
            )
            .expect("selected attempt JSON");
            candidate_id = Some(
                attempt["participants"][0]["plan"]["candidate_id"]
                    .as_str()
                    .expect("Plan7 candidate")
                    .to_owned(),
            );
        }
        let selector = storage
            .get_raw(&format!(
                "control/v1/domains/catalog/restore/v7/{}/selector.json",
                candidate_id.as_deref().expect("candidate")
            ))
            .await
            .expect("selected progress");
        assert!(
            prior_selector.as_deref() != Some(selector.as_ref()),
            "restore selector did not advance at invocation {invocation}"
        );
        prior_selector = Some(selector.to_vec());
    }
    panic!("large durable restore exceeded 32,768 resumable invocations");
}

#[test]
fn frozen_matrix_and_predecessor_rows_are_exact_and_ordered() {
    assert_eq!(FIXTURE_AXES.len(), 6);
    assert_eq!(FIXTURE_AXES[0].retained_rows, FIXTURE_AXES[4].retained_rows);
    assert_ne!(
        FIXTURE_AXES[0].active_records,
        FIXTURE_AXES[4].active_records
    );
    assert_eq!(REPETITIONS * OPERATIONS_PER_SAMPLE, 1_000);
    assert_eq!(SCENARIOS.len(), 10);
    assert_eq!(CACHE_MODES.len(), 3);

    let operations =
        frozen_operations(Scenario::CreateCatalog, 0, CacheMode::Default, 1).collect::<Vec<_>>();
    assert_eq!(
        operations.len(),
        usize::try_from(OPERATIONS_PER_SAMPLE).expect("operation count")
    );
    assert!(
        operations
            .windows(2)
            .all(|pair| pair[0].operation_id < pair[1].operation_id)
    );
    assert!(
        operations
            .windows(2)
            .all(|pair| pair[0].catalog < pair[1].catalog)
    );
    let distinct_scenario = frozen_operations(Scenario::CreateSchema, 0, CacheMode::Default, 1)
        .next()
        .expect("first scenario operation");
    let distinct_axis = frozen_operations(Scenario::CreateCatalog, 1, CacheMode::Default, 1)
        .next()
        .expect("first axis operation");
    let distinct_cache = frozen_operations(Scenario::CreateCatalog, 0, CacheMode::Pressure, 1)
        .next()
        .expect("first cache operation");
    assert_ne!(operations[0].operation_id, distinct_scenario.operation_id);
    assert_ne!(operations[0].operation_id, distinct_axis.operation_id);
    assert_ne!(operations[0].operation_id, distinct_cache.operation_id);

    let rows = V2PredecessorRows::new(6).collect::<Vec<_>>();
    assert_eq!(rows.len(), 6);
    assert!(rows.windows(2).all(|pair| pair[0].key < pair[1].key));
    let receipt: serde_json::Value =
        serde_json::from_slice(rows[0].value.as_deref().unwrap()).expect("receipt JSON");
    let audit: serde_json::Value =
        serde_json::from_slice(rows[3].value.as_deref().unwrap()).expect("audit JSON");
    assert_eq!(receipt["logicalSequence"], 1);
    assert_eq!(audit["logicalSequence"], 1);
    assert!(receipt.get("authorityManifestId").is_none());
    assert!(audit.get("authorityManifestId").is_none());
}

#[tokio::test]
async fn streamed_production_v2_predecessor_requires_authenticated_genesis_publication() {
    let storage = ScopedStorage::new(
        Arc::new(MemoryBackend::new()),
        "synthetic-tenant",
        "synthetic-workspace",
    )
    .expect("storage");
    let scope = StateScope::new("synthetic-tenant", "synthetic-workspace", "catalog");
    let store = ControlMvpStateStore::new_synthetic_bounded(storage, scope.clone()).expect("store");
    let intents = (1..=3)
        .map(|origin| bounded_capacity_v2_intent(scope.clone(), origin, 0))
        .collect::<Result<Vec<_>, _>>()
        .expect("fixed historical intents");

    let token = store
        .install_synthetic_genesis(
            "bounded-cost-predecessor",
            3,
            6,
            V2PredecessorRows::new(6),
            3,
            intents,
        )
        .await
        .expect("synthetic genesis must publish");
    let first_page = store
        .projection_outbox_page_v2(Some(token), None, 1)
        .await
        .expect("authenticated bounded delivery page");
    assert_eq!(first_page.records().len(), 1);
    let intent = first_page.records()[0].clone();
    assert_eq!(
        intent.intent_id(),
        "intent-00000000000000000001-00000000000000000000"
    );
    let mut trim = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("bounded trim transaction");
    trim.set_logical_operation_v2("fixture-trim", "exact_trim", &"44".repeat(32))
        .expect("frozen trim identity");
    trim.trim_projection_intents_v2(&[intent])
        .await
        .expect("exact live V2 incarnation");
    trim.commit_v2()
        .await
        .expect("bounded exact trim publication");
}

#[derive(Default, Debug, Serialize)]
struct CandidateInventoryClass {
    candidates: usize,
    envelope_bytes: u64,
    manifest_bytes: u64,
}

#[derive(Debug, Serialize)]
struct PreparedCandidateInventory {
    attached: CandidateInventoryClass,
    unattached: CandidateInventoryClass,
    current_root: serde_json::Value,
}

fn require_manifest_scope(value: &serde_json::Value, scope: &StateScope) {
    assert_eq!(value["scope"]["tenant_id"], scope.tenant_id());
    assert_eq!(
        value["scope"]["workspace_id"].as_str(),
        scope.workspace_id()
    );
    assert_eq!(value["scope"]["domain"], scope.domain());
}

fn manifest_path(id: &str) -> String {
    assert!(!id.is_empty() && id.len() <= 1024);
    assert!(
        id.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    );
    format!("control/v1/domains/catalog/manifests/{id}.json")
}

// This test-only inventory walks short private fixture ancestry, outside measured requests.
#[allow(clippy::too_many_lines)]
async fn prepared_candidate_inventory(
    storage: &ScopedStorage,
    scope: &StateScope,
) -> PreparedCandidateInventory {
    let store = ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope.clone())
        .expect("inventory store");
    let current_root = store
        .bounded_root_inventory_v2()
        .await
        .expect("authenticated current root");
    let mut attached = std::collections::BTreeSet::new();
    let mut manifest_id = current_root["manifestId"]
        .as_str()
        .expect("manifest ID")
        .to_string();
    let mut digest = current_root["manifestSha256"]
        .as_str()
        .expect("manifest digest")
        .to_string();
    loop {
        assert!(
            attached.len() < 32,
            "private fixture ancestry exceeded declared inventory bound"
        );
        assert!(
            attached.insert(manifest_id.clone()),
            "cyclic manifest ancestry"
        );
        let bytes = storage
            .get_raw(&manifest_path(&manifest_id))
            .await
            .expect("ancestor manifest");
        assert_eq!(format!("{:x}", Sha256::digest(&bytes)), digest);
        let manifest: serde_json::Value = serde_json::from_slice(&bytes).expect("manifest JSON");
        assert_eq!(manifest["format_version"], 8);
        assert_eq!(manifest["manifest_id"], manifest_id);
        require_manifest_scope(&manifest, scope);
        let Some(parent) = manifest["parent_manifest_id"].as_str() else {
            assert!(manifest["parent_manifest_sha256"].is_null());
            break;
        };
        manifest_id = parent.to_string();
        digest = manifest["parent_manifest_sha256"]
            .as_str()
            .expect("parent digest")
            .to_string();
    }
    let candidates = storage
        .list_meta("control/v1/domains/catalog/prepared")
        .await
        .expect("private candidates");
    let mut inventory = PreparedCandidateInventory {
        attached: CandidateInventoryClass::default(),
        unattached: CandidateInventoryClass::default(),
        current_root,
    };
    for candidate in candidates {
        let path = candidate.path.to_string();
        let id = path
            .strip_prefix("control/v1/domains/catalog/prepared/")
            .and_then(|path| path.strip_suffix(".json"))
            .expect("prepared candidate path");
        let envelope_bytes = storage.get_raw(&path).await.expect("candidate envelope");
        assert_eq!(envelope_bytes.len() as u64, candidate.size);
        let envelope: serde_json::Value =
            serde_json::from_slice(&envelope_bytes).expect("envelope JSON");
        assert_eq!(envelope["candidate_id"], id);
        assert_eq!(envelope["encoding_version"], 1);
        assert_eq!(
            envelope["record_type"],
            "arco_control_bounded_prepared_candidate"
        );
        assert_eq!(
            envelope["candidate_head"]["manifest"],
            envelope["candidate_manifest"]
        );
        require_manifest_scope(&envelope, scope);
        let candidate_manifest = manifest_path(id);
        assert_eq!(envelope["candidate_manifest"]["path"], candidate_manifest);
        let bytes = storage
            .get_raw(&candidate_manifest)
            .await
            .expect("candidate manifest");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            envelope["candidate_manifest"]["sha256"]
                .as_str()
                .expect("candidate manifest digest")
        );
        let manifest: serde_json::Value =
            serde_json::from_slice(&bytes).expect("candidate manifest JSON");
        assert_eq!(manifest["manifest_id"], id);
        assert_eq!(manifest["format_version"], 8);
        require_manifest_scope(&manifest, scope);
        let class = if attached.contains(id) {
            &mut inventory.attached
        } else {
            &mut inventory.unattached
        };
        class.candidates += 1;
        class.envelope_bytes += envelope_bytes.len() as u64;
        class.manifest_bytes += bytes.len() as u64;
    }
    inventory
}

#[tokio::test]
async fn prepared_candidates_restore_classifies_attached_and_unattached_before_inventory_fix() {
    let backend = Arc::new(control_cost::CountingBackend::new(1));
    let storage =
        ScopedStorage::new(backend.clone(), "candidate-classifier", "fixture").expect("storage");
    let scope = StateScope::new("candidate-classifier", "fixture", "catalog");
    let store =
        ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope.clone()).expect("store");
    let genesis = store
        .install_synthetic_genesis(
            "candidate-genesis",
            1,
            2,
            V2PredecessorRows::new(2),
            0,
            Vec::new(),
        )
        .await
        .expect("genesis");
    let head_path = "control/v1/domains/catalog/head/current.json";
    let genesis_head = storage.get_raw(head_path).await.expect("genesis HEAD");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("transaction");
    txn.set_logical_operation_v2("candidate-mutation", "candidate_test", &"aa".repeat(32))
        .expect("logical operation");
    txn.put(b"candidate-key", Bytes::from_static(b"candidate-value"))
        .await
        .expect("mutation");
    let committed = txn.commit_v2().await.expect("mutation commit");
    let version = storage
        .head_raw(head_path)
        .await
        .expect("HEAD meta")
        .expect("HEAD exists")
        .version;
    backend.replace_head_for_bounded_fixture(
        "tenant=candidate-classifier/workspace=fixture/control/v1/domains/catalog/head/current.json",
        &version,
        genesis_head,
    ).await.expect("restore genesis HEAD");
    assert!(
        store
            .read_at(committed.token().clone())
            .await
            .expect("retained committed token")
            .get(b"candidate-key")
            .await
            .expect("retained read")
            .is_some()
    );
    let inventory = prepared_candidate_inventory(&storage, &scope).await;
    assert_eq!(
        (
            inventory.attached.candidates,
            inventory.unattached.candidates
        ),
        (1, 1)
    );
    assert!(inventory.unattached.envelope_bytes > 0 && inventory.unattached.manifest_bytes > 0);
    assert_eq!(inventory.current_root["rootRows"]["kv"], 2);
    assert_eq!(inventory.current_root["rootRows"]["activeId"], 0);
    assert_eq!(inventory.current_root["rootRows"]["deliveryOrder"], 0);
    let restarted = ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope.clone())
        .expect("restarted store");
    assert!(matches!(
        restarted
            .reconcile_candidate_v2(committed.token().authority_manifest_id())
            .await
            .expect("restart candidate"),
        CandidateRecoveryV2::Unresolved
    ));
    assert_eq!(genesis.logical_sequence(), 1);
}

#[derive(Default)]
struct NoopV2Notifier;

impl CatalogProjectionNotifierV2 for NoopV2Notifier {
    fn notify(&self, _: &ProjectionIntentV2) -> arco_catalog::Result<()> {
        Ok(())
    }
}

#[derive(Serialize)]
struct BoundedCostSample<'a> {
    kind: &'static str,
    source_identity: &'a str,
    axis: FixtureAxis,
    cache: &'static str,
    scenario: &'static str,
    repetition: u64,
    ordinal: u64,
    operation_id: &'a str,
    idempotency_key: &'a str,
    request_digest: &'a str,
    occurred_at_ms: i64,
    elapsed_micros: u128,
    request_allocations: &'a control_cost::RequestAllocations,
    backend: &'a control_cost::BackendCounts,
    verification_allocations: Option<&'a control_cost::RequestAllocations>,
    verification_backend: Option<&'a control_cost::BackendCounts>,
    published_logical: Option<&'a serde_json::Value>,
    head_before: &'a HeadInventory,
    head_after: &'a HeadInventory,
    store_cache_before: Option<&'a ControlMvpReadCacheStatistics>,
    store_cache_after: Option<&'a ControlMvpReadCacheStatistics>,
    authority_cache_before: Option<&'a ControlMvpReadCacheStatistics>,
    authority_cache_after: Option<&'a ControlMvpReadCacheStatistics>,
    reset_count: u64,
    result: &'static str,
}

#[derive(Clone, Serialize)]
struct HeadInventory {
    version: String,
    bytes: usize,
    sha256: String,
}

#[derive(Serialize)]
struct PhaseSample<'a> {
    kind: &'static str,
    source_identity: &'a str,
    phase: &'static str,
    axis: FixtureAxis,
    cache: &'static str,
    scenario: &'static str,
    repetition: u64,
    ordinal: Option<u64>,
    backend: &'a control_cost::BackendCounts,
    head: &'a HeadInventory,
    reset: Option<&'a control_cost::FixtureHeadReset>,
}

#[derive(Serialize)]
struct SummarySample<'a> {
    kind: &'static str,
    source_identity: &'a str,
    axis: FixtureAxis,
    cache: &'static str,
    scenario: &'static str,
    repetition: u64,
    operations: u64,
    final_head: &'a HeadInventory,
}

#[derive(Serialize)]
struct PrivateArtifactInventory {
    kind: &'static str,
    source_identity: String,
    axis: FixtureAxis,
    object_count: usize,
    object_bytes: u64,
    prepared_candidate_objects: usize,
    candidate_manifest_objects: usize,
    descriptor_objects: usize,
    prepared_candidates: PreparedCandidateInventory,
    inventory_backend: control_cost::BackendCounts,
    semantics: &'static str,
}

fn append_jsonl<T: Serialize>(write_jsonl: bool, sample: &T) {
    if !write_jsonl {
        return;
    }
    let path = std::env::var("ARCO_BOUNDED_COST_JSONL").expect("bounded cost JSONL output path");
    let mut output = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("cost JSONL output");
    writeln!(
        output,
        "{}",
        serde_json::to_string(sample).expect("JSON sample")
    )
    .expect("write JSONL");
}

async fn head_inventory(storage: &ScopedStorage, head_path: &str) -> HeadInventory {
    let meta = storage
        .head_raw(head_path)
        .await
        .expect("fixture HEAD metadata")
        .expect("fixture HEAD exists");
    let bytes = storage
        .get_raw(head_path)
        .await
        .expect("fixture HEAD bytes");
    HeadInventory {
        version: meta.version,
        bytes: bytes.len(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    }
}

fn cache_statistics(store: &ControlMvpStateStore) -> Option<ControlMvpReadCacheStatistics> {
    store.read_cache().map(|cache| cache.statistics())
}

fn actual_depth(proof: &serde_json::Value) -> u64 {
    let depths = proof
        .get("actualRootDepths")
        .and_then(serde_json::Value::as_object)
        .expect("published proof must include actualRootDepths");
    ["kv", "activeId", "deliveryOrder"]
        .into_iter()
        .map(|role| {
            depths
                .get(role)
                .and_then(serde_json::Value::as_u64)
                .expect("published proof root depth")
        })
        .max()
        .expect("three published root depths")
}

fn assert_bounded_cost_contract(
    counts: &control_cost::BackendCounts,
    allocations: &control_cost::RequestAllocations,
    proof: &serde_json::Value,
    scenario: Scenario,
) {
    let depth = actual_depth(proof);
    if scenario != Scenario::OrderedOutboxPage {
        assert!(
            proof
                .get("operation")
                .is_some_and(serde_json::Value::is_object),
            "{} missing published operation",
            scenario.label()
        );
    }
    assert!(
        proof
            .get("logicalCommitId")
            .is_some_and(serde_json::Value::is_string),
        "{} missing published logical identity",
        scenario.label()
    );
    assert!(
        counts.selected_blocks <= DECLARED_BLOCK_BOUND,
        "{} selected {} blocks",
        scenario.label(),
        counts.selected_blocks
    );
    assert!(counts.rewritten_blocks <= DECLARED_BLOCK_BOUND.saturating_mul(2));
    assert!(counts.block_decode_calls <= DECLARED_BLOCK_BOUND.saturating_mul(4));
    if !matches!(
        scenario,
        Scenario::IdempotentReplay | Scenario::OrderedOutboxPage
    ) {
        assert!(
            counts.transition_proof_rows > 0,
            "{} did not record bounded commit proof rows",
            scenario.label()
        );
        assert!(
            counts
                .phases
                .values()
                .any(|phase| phase.transition_proof_rows > 0),
            "{} did not record bounded commit proof rows in a named phase",
            scenario.label()
        );
    }
    assert_eq!(0, counts.streaming_builder_inputs);
    assert_eq!(0, counts.replayed_rows);
    assert_eq!(0, counts.full_checksum_calls);
    assert!(allocations.peak_owned_bytes <= 64 * 1024 * 1024);
    let directory = counts
        .object_class_io
        .get("directory_page")
        .cloned()
        .unwrap_or_default();
    let ceiling = 4_u64
        .saturating_mul(depth)
        .saturating_mul(DECLARED_BLOCK_BOUND.saturating_add(1));
    assert!(directory.full_get_attempts + directory.range_get_attempts <= ceiling);
    assert!(directory.put_attempts <= ceiling);
}

fn fixture_scope(axis: usize) -> StateScope {
    StateScope::new("bounded-cost", format!("axis-{axis}"), "catalog")
}

fn fixture_storage(backend: Arc<control_cost::CountingBackend>, axis: usize) -> ScopedStorage {
    ScopedStorage::new(backend, "bounded-cost", format!("axis-{axis}")).expect("fixture storage")
}

async fn restore_fixture_head(
    backend: &control_cost::CountingBackend,
    storage: &ScopedStorage,
    axis: usize,
    head_path: &str,
    bytes: Bytes,
) -> control_cost::FixtureHeadReset {
    let version = storage
        .head_raw(head_path)
        .await
        .expect("fixture HEAD metadata")
        .expect("fixture HEAD exists")
        .version;
    backend
        .replace_head_for_bounded_fixture(
            &format!("tenant=bounded-cost/workspace=axis-{axis}/{head_path}"),
            &version,
            bytes,
        )
        .await
        .expect("restore fixture HEAD")
}

fn catalog_request(name: &str) -> RegisterTableInSchemaRequest {
    RegisterTableInSchemaRequest {
        name: name.to_string(),
        description: Some("bounded-cost".to_string()),
        location: Some("s3://bounded-cost/table".to_string()),
        format: Some("delta".to_string()),
        table_type: Some("EXTERNAL".to_string()),
        properties: None,
        columns: vec![ColumnDefinition {
            name: "id".to_string(),
            data_type: "BIGINT".to_string(),
            is_nullable: false,
            ordinal: 0,
            description: None,
        }],
    }
}

fn cache_config(mode: CacheMode) -> Option<ControlMvpReadCacheConfig> {
    match mode {
        CacheMode::Disabled => None,
        CacheMode::Default => Some(ControlMvpReadCacheConfig::default()),
        CacheMode::Pressure => Some(ControlMvpReadCacheConfig {
            metadata_bytes: 1024 * 1024,
            decoded_bytes: 4 * 1024 * 1024,
        }),
    }
}

async fn bootstrap(
    authority: &ControlCatalogAuthority,
    scenario: Scenario,
) -> Result<(), CatalogError> {
    if !matches!(
        scenario,
        Scenario::CreateCatalog | Scenario::IdempotentReplay
    ) {
        authority
            .create_catalog_v2(
                "base",
                None,
                WriteOptions::with_idempotency("bootstrap-catalog"),
            )
            .await?;
    }
    if matches!(
        scenario,
        Scenario::RegisterTable | Scenario::RenameTable | Scenario::DropTable
    ) {
        authority
            .create_schema(
                "base",
                "main",
                None,
                WriteOptions::with_idempotency("bootstrap-schema"),
            )
            .await?;
    }
    if matches!(scenario, Scenario::RenameTable | Scenario::DropTable) {
        authority
            .register_table_in_schema(
                "base",
                "main",
                catalog_request("base-table"),
                WriteOptions::with_idempotency("bootstrap-table"),
            )
            .await?;
    }
    Ok(())
}

async fn actual_scenario(
    authority: &ControlCatalogAuthority,
    store: &ControlMvpStateStore,
    operation: &FrozenCatalogOperation,
    prepared: Option<PreparedBoundedCatalogMutation>,
) -> Result<(), CatalogError> {
    match operation.scenario {
        Scenario::CreateCatalog
        | Scenario::CreateSchema
        | Scenario::RegisterTable
        | Scenario::PatchCatalog
        | Scenario::RenameTable
        | Scenario::DropTable
        | Scenario::IdempotentReplay => {
            authority
                .execute_prepared_synthetic_bounded_command(
                    prepared.expect("frozen catalog command"),
                )
                .await?;
        }
        Scenario::StaleGenerationConflict => {
            let mut stale = store.begin_control_txn(TxnOptions::default()).await?;
            stale
                .put(b"bounded/stale", Bytes::from_static(b"first"))
                .await?;
            let mut winner = store.begin_control_txn(TxnOptions::default()).await?;
            winner
                .put(b"bounded/stale", Bytes::from_static(b"winner"))
                .await?;
            winner.set_logical_operation_v2(
                &operation.operation_id,
                "stale_generation_conflict",
                &"11".repeat(32),
            )?;
            winner.commit_v2().await?;
            stale
                .put(b"bounded/stale", Bytes::from_static(b"stale"))
                .await?;
            stale.set_logical_operation_v2(
                &format!("{}-stale", operation.operation_id),
                "stale_generation_conflict",
                &"22".repeat(32),
            )?;
            assert!(matches!(
                stale.commit_v2().await,
                Err(CatalogError::CasFailed { .. } | CatalogError::PreconditionFailed { .. })
            ));
        }
        Scenario::OrderedOutboxPage => {
            let page = store.projection_outbox_page_v2(None, None, 1).await?;
            assert_eq!(page.records().len(), 1);
        }
        Scenario::ExactTrim => {
            let page = store.projection_outbox_page_v2(None, None, 1).await?;
            let intent = page.records().first().expect("fixture intent").clone();
            let mut txn = store.begin_control_txn(TxnOptions::default()).await?;
            txn.set_logical_operation_v2(&operation.operation_id, "exact_trim", &"33".repeat(32))?;
            txn.trim_projection_intents_v2(&[intent]).await?;
            txn.commit_v2().await?;
        }
    }
    Ok(())
}

fn fixture_uuid(operation: &FrozenCatalogOperation, tag: u16) -> String {
    format!(
        "01900000-{:04x}-7{tag:03x}-8000-{:012x}",
        operation.ordinal, operation.ordinal
    )
}

fn frozen_catalog_command(operation: &FrozenCatalogOperation) -> Option<BoundedCatalogTestCommand> {
    let id = fixture_uuid(operation, 1);
    match operation.scenario {
        Scenario::CreateCatalog | Scenario::IdempotentReplay => {
            Some(BoundedCatalogTestCommand::CreateCatalog {
                id,
                name: operation.catalog.clone(),
            })
        }
        Scenario::CreateSchema => Some(BoundedCatalogTestCommand::CreateSchema {
            id,
            catalog: "base".to_string(),
            name: operation.schema.clone(),
        }),
        Scenario::RegisterTable => Some(BoundedCatalogTestCommand::RegisterTable {
            id,
            column_id: fixture_uuid(operation, 2),
            catalog: "base".to_string(),
            schema: "main".to_string(),
            request: catalog_request(&operation.table),
        }),
        Scenario::PatchCatalog => Some(BoundedCatalogTestCommand::PatchCatalog {
            name: "base".to_string(),
            patch: CatalogPatch {
                description: Some(Some(operation.operation_id.clone())),
                ..CatalogPatch::default()
            },
        }),
        Scenario::RenameTable => Some(BoundedCatalogTestCommand::RenameTable {
            catalog: "base".to_string(),
            schema: "main".to_string(),
            name: "base-table".to_string(),
            new_name: operation.table.clone(),
        }),
        Scenario::DropTable => Some(BoundedCatalogTestCommand::DropTable {
            catalog: "base".to_string(),
            schema: "main".to_string(),
            name: "base-table".to_string(),
        }),
        Scenario::StaleGenerationConflict | Scenario::OrderedOutboxPage | Scenario::ExactTrim => {
            None
        }
    }
}

#[tokio::test]
async fn prepared_bounded_command_freezes_identity_before_execution() {
    let _fixed_inputs = arco_core::test_inputs::FixedInputs::scoped();
    let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "frozen-command", "fixture")
        .expect("storage");
    let authority = ControlCatalogAuthority::new_synthetic_bounded(
        storage,
        StateScope::new("frozen-command", "fixture", "catalog"),
        Arc::new(NoopV2Notifier),
    )
    .expect("authority")
    .with_synthetic_bounded_read_cache(None)
    .expect("disabled cache");
    let command = authority
        .prepare_synthetic_bounded_command(
            BoundedCatalogTestCommand::CreateCatalog {
                id: "01900000-0001-7001-8000-000000000001".to_string(),
                name: "frozen".to_string(),
            },
            WriteOptions::with_idempotency("frozen-command").with_request_id("frozen-request"),
        )
        .expect("prepare before execution");
    let operation_id = command.operation_id().to_string();
    let digest = command.request_digest().to_string();
    let occurred_at_ms = command.occurred_at_ms();
    authority
        .execute_prepared_synthetic_bounded_command(command.clone())
        .await
        .expect("first interpreter execution");
    authority
        .execute_prepared_synthetic_bounded_command(command)
        .await
        .expect("idempotent interpreter replay");
    assert!(!operation_id.is_empty());
    assert_eq!(digest.len(), 64);
    assert!(occurred_at_ms > 0);
    assert!(
        authority
            .get_catalog("frozen")
            .await
            .expect("catalog lookup")
            .is_some()
    );
}

// FixedInputs is thread-local; this sequential current-thread fixture keeps every phase explicit.
#[allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::future_not_send,
    clippy::large_futures
)]
async fn run_matrix(axes: &[FixtureAxis], repetitions: u64, operations: u64, write_jsonl: bool) {
    let _fixed_inputs = arco_core::test_inputs::FixedInputs::scoped();
    let source_identity = if write_jsonl {
        std::env::var("ARCO_BOUNDED_COST_SOURCE_IDENTITY")
            .expect("full matrix requires ARCO_BOUNDED_COST_SOURCE_IDENTITY")
    } else {
        "smoke".to_string()
    };
    for (axis_index, axis) in axes.iter().copied().enumerate() {
        let backend = Arc::new(control_cost::CountingBackend::new(1));
        let storage = fixture_storage(backend.clone(), axis_index);
        let scope = fixture_scope(axis_index);
        let fixture_store =
            ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope.clone())
                .expect("synthetic fixture store");
        let sequence = (axis.retained_rows / 2).max(axis.active_records);
        let intents = (1..=axis.active_records)
            .map(|origin| bounded_capacity_v2_intent(scope.clone(), origin, 0))
            .collect::<Result<Vec<_>, _>>()
            .expect("fixture intents");
        fixture_store
            .install_synthetic_genesis(
                &format!("bounded-cost-a{axis_index}"),
                sequence,
                axis.retained_rows,
                V2PredecessorRows::new(axis.retained_rows),
                axis.active_records,
                intents,
            )
            .await
            .expect("fixture genesis");
        let head_path = "control/v1/domains/catalog/head/current.json";
        let genesis = storage
            .get_raw(head_path)
            .await
            .expect("fixture HEAD bytes");
        let construction_head = head_inventory(&storage, head_path).await;
        let construction = backend.take();
        append_jsonl(
            write_jsonl,
            &PhaseSample {
                kind: "phase",
                source_identity: &source_identity,
                phase: "fixture-construction",
                axis,
                cache: "none",
                scenario: "none",
                repetition: 0,
                ordinal: None,
                backend: &construction,
                head: &construction_head,
                reset: None,
            },
        );

        for cache in CACHE_MODES {
            for scenario in SCENARIOS {
                for repetition in 1..=repetitions {
                    let reset = restore_fixture_head(
                        &backend,
                        &storage,
                        axis_index,
                        head_path,
                        genesis.clone(),
                    )
                    .await;
                    let reset_head = head_inventory(&storage, head_path).await;
                    let reset_counts = backend.take();
                    append_jsonl(
                        write_jsonl,
                        &PhaseSample {
                            kind: "phase",
                            source_identity: &source_identity,
                            phase: "fixture-reset",
                            axis,
                            cache: cache.label(),
                            scenario: scenario.label(),
                            repetition,
                            ordinal: None,
                            backend: &reset_counts,
                            head: &reset_head,
                            reset: Some(&reset),
                        },
                    );
                    let authority = ControlCatalogAuthority::new_synthetic_bounded(
                        storage.clone(),
                        scope.clone(),
                        Arc::new(NoopV2Notifier),
                    )
                    .expect("synthetic catalog authority")
                    .with_synthetic_bounded_read_cache(cache_config(cache))
                    .expect("catalog cache config");
                    bootstrap(&authority, scenario)
                        .await
                        .expect("scenario bootstrap");
                    let saved_head = storage
                        .get_raw(head_path)
                        .await
                        .expect("bootstrap HEAD bytes");
                    let bootstrap_head = head_inventory(&storage, head_path).await;
                    let bootstrap_counts = backend.take();
                    append_jsonl(
                        write_jsonl,
                        &PhaseSample {
                            kind: "phase",
                            source_identity: &source_identity,
                            phase: "bootstrap",
                            axis,
                            cache: cache.label(),
                            scenario: scenario.label(),
                            repetition,
                            ordinal: None,
                            backend: &bootstrap_counts,
                            head: &bootstrap_head,
                            reset: None,
                        },
                    );
                    let mut final_head = bootstrap_head;

                    for operation in frozen_operations(scenario, axis_index, cache, repetition)
                        .take(usize::try_from(operations).expect("operation count"))
                    {
                        // A fresh authority/store makes cache state empty after every private fixture reset.
                        let request_store = ControlMvpStateStore::new_synthetic_bounded(
                            storage.clone(),
                            scope.clone(),
                        )
                        .expect("request store");
                        let store = cache_config(cache).map_or_else(
                            || request_store.clone().without_read_cache(),
                            |config| {
                                request_store
                                    .clone()
                                    .with_read_cache_config(config)
                                    .expect("cache config")
                            },
                        );
                        drop(request_store);
                        let authority = ControlCatalogAuthority::new_synthetic_bounded(
                            storage.clone(),
                            scope.clone(),
                            Arc::new(NoopV2Notifier),
                        )
                        .expect("request authority")
                        .with_synthetic_bounded_read_cache(cache_config(cache))
                        .expect("catalog cache config");
                        let prepared = frozen_catalog_command(&operation).map(|command| {
                            authority
                                .prepare_synthetic_bounded_command(
                                    command,
                                    WriteOptions::with_idempotency(
                                        operation.idempotency_key.clone(),
                                    )
                                    .with_request_id(operation.operation_id.clone()),
                                )
                                .expect("freeze catalog command before timing")
                        });
                        if operation.scenario == Scenario::IdempotentReplay {
                            authority
                                .execute_prepared_synthetic_bounded_command(
                                    prepared.clone().expect("idempotent seed command"),
                                )
                                .await
                                .expect("idempotent seed outside timing");
                        }
                        let frozen_operation_id = prepared.as_ref().map_or_else(
                            || operation.operation_id.clone(),
                            |command| command.operation_id().to_string(),
                        );
                        let frozen_digest = prepared.as_ref().map_or_else(
                            || {
                                match scenario {
                                    Scenario::StaleGenerationConflict => "11",
                                    Scenario::ExactTrim => "33",
                                    _ => "00",
                                }
                                .repeat(32)
                            },
                            |command| command.request_digest().to_string(),
                        );
                        let frozen_occurred_at_ms = prepared
                            .as_ref()
                            .map_or(0, PreparedBoundedCatalogMutation::occurred_at_ms);
                        // Warm-up is explicitly outside the request counters.
                        let _ = authority.get_catalog("base").await;
                        let warmup = backend.take();
                        let warmup_head = head_inventory(&storage, head_path).await;
                        let warmup_inventory = backend.take();
                        append_jsonl(
                            write_jsonl,
                            &PhaseSample {
                                kind: "phase",
                                source_identity: &source_identity,
                                phase: "warmup",
                                axis,
                                cache: cache.label(),
                                scenario: scenario.label(),
                                repetition,
                                ordinal: Some(operation.ordinal),
                                backend: &warmup,
                                head: &warmup_head,
                                reset: None,
                            },
                        );
                        let _ = warmup_inventory;
                        let head_before = head_inventory(&storage, head_path).await;
                        let _start_inventory = backend.take();
                        let store_cache_before = cache_statistics(&store);
                        let authority_cache_before =
                            authority.synthetic_bounded_read_cache_statistics();
                        let started = Instant::now();
                        let prepared_for_request = &prepared;
                        let (request_result, allocations, counts) =
                            measure_catalog_request(&backend, async {
                                actual_scenario(
                                    &authority,
                                    &store,
                                    &operation,
                                    prepared_for_request.clone(),
                                )
                                .await
                            })
                            .await;
                        let elapsed_micros = started.elapsed().as_micros();
                        let (verification_result, verification_allocations, verification_counts) =
                            if request_result.is_ok() {
                                let (result, allocations, counts) = measure_catalog_request(
                                    &backend,
                                    store.export_published_logical_v2_current(),
                                )
                                .await;
                                (Some(result), Some(allocations), Some(counts))
                            } else {
                                (None, None, None)
                            };
                        let proof = verification_result
                            .as_ref()
                            .and_then(|result| result.as_ref().ok());
                        let head_after = head_inventory(&storage, head_path).await;
                        let _end_inventory = backend.take();
                        let store_cache_after = cache_statistics(&store);
                        let authority_cache_after =
                            authority.synthetic_bounded_read_cache_statistics();
                        let result_label = match (&request_result, &verification_result) {
                            (Ok(()), Some(Ok(_))) => "ok",
                            (Ok(()), Some(Err(_))) => "verification-error",
                            (Ok(()), None) => "verification-missing",
                            (Err(_), _) => "request-error",
                        };
                        if write_jsonl {
                            let sample = BoundedCostSample {
                                kind: "operation",
                                source_identity: &source_identity,
                                axis,
                                cache: cache.label(),
                                scenario: scenario.label(),
                                repetition,
                                ordinal: operation.ordinal,
                                operation_id: &frozen_operation_id,
                                idempotency_key: &operation.idempotency_key,
                                request_digest: &frozen_digest,
                                occurred_at_ms: frozen_occurred_at_ms,
                                elapsed_micros,
                                request_allocations: &allocations,
                                backend: &counts,
                                verification_allocations: verification_allocations.as_ref(),
                                verification_backend: verification_counts.as_ref(),
                                published_logical: proof,
                                head_before: &head_before,
                                head_after: &head_after,
                                store_cache_before: store_cache_before.as_ref(),
                                store_cache_after: store_cache_after.as_ref(),
                                authority_cache_before: authority_cache_before.as_ref(),
                                authority_cache_after: authority_cache_after.as_ref(),
                                reset_count: reset.reset_count,
                                result: result_label,
                            };
                            append_jsonl(true, &sample);
                        }
                        request_result.expect("bounded scenario request");
                        let proof = verification_result
                            .expect("successful request must export a published logical proof")
                            .expect("published logical proof export");
                        if scenario != Scenario::OrderedOutboxPage {
                            assert_eq!(
                                proof["operation"]["operationId"].as_str(),
                                Some(frozen_operation_id.as_str()),
                                "sample must identify the authenticated published operation"
                            );
                            assert_eq!(
                                proof["operation"]["requestDigest"].as_str(),
                                Some(frozen_digest.as_str()),
                                "sample must record the authenticated published request digest"
                            );
                        }
                        assert_bounded_cost_contract(&counts, &allocations, &proof, scenario);
                        let final_reset = restore_fixture_head(
                            &backend,
                            &storage,
                            axis_index,
                            head_path,
                            saved_head.clone(),
                        )
                        .await;
                        final_head = head_inventory(&storage, head_path).await;
                        let final_reset_counts = backend.take();
                        append_jsonl(
                            write_jsonl,
                            &PhaseSample {
                                kind: "phase",
                                source_identity: &source_identity,
                                phase: "operation-reset",
                                axis,
                                cache: cache.label(),
                                scenario: scenario.label(),
                                repetition,
                                ordinal: Some(operation.ordinal),
                                backend: &final_reset_counts,
                                head: &final_head,
                                reset: Some(&final_reset),
                            },
                        );
                    }
                    append_jsonl(
                        write_jsonl,
                        &SummarySample {
                            kind: "summary",
                            source_identity: &source_identity,
                            axis,
                            cache: cache.label(),
                            scenario: scenario.label(),
                            repetition,
                            operations,
                            final_head: &final_head,
                        },
                    );
                }
            }
        }
        let _prior_inventory_phase = backend.take();
        let prepared_candidates = prepared_candidate_inventory(&storage, &scope).await;
        let artifacts = storage
            .list_meta("")
            .await
            .expect("private artifact inventory");
        let prepared_candidate_objects = artifacts
            .iter()
            .filter(|artifact| artifact.path.to_string().contains("/prepared/"))
            .count();
        let candidate_manifest_objects = artifacts
            .iter()
            .filter(|artifact| artifact.path.to_string().contains("/manifests/"))
            .count();
        let descriptor_objects = artifacts
            .iter()
            .filter(|artifact| artifact.path.to_string().contains("/physical/descriptors/"))
            .count();
        append_jsonl(
            write_jsonl,
            &PrivateArtifactInventory {
                kind: "private-artifact-inventory",
                source_identity: source_identity.clone(),
                axis,
                object_count: artifacts.len(),
                object_bytes: artifacts.iter().map(|artifact| artifact.size).sum(),
                prepared_candidate_objects,
                candidate_manifest_objects,
                descriptor_objects,
                prepared_candidates,
                inventory_backend: backend.take(),
                semantics: "prepared candidates are classified against authenticated current HEAD manifest ancestry; class bytes count only candidate envelopes and manifests, not shared transitive objects or GC eligibility; overall physical totals retain all objects",
            },
        );
    }
}

#[tokio::test]
// The measured request is stack-pinned inside this bounded test harness frame.
#[allow(clippy::large_futures)]
async fn bounded_catalog_cost_smoke_executes_all_frozen_scenarios() {
    run_matrix(
        &[FixtureAxis {
            retained_rows: 4,
            active_records: 4,
        }],
        1,
        1,
        false,
    )
    .await;
}

#[tokio::test]
#[ignore = "explicit frozen authority-8 cost matrix; run only after prerequisite counters are proven"]
// The measured request is stack-pinned inside this bounded test harness frame.
#[allow(clippy::large_futures)]
async fn bounded_catalog_cost_full_matrix_executes_all_frozen_configurations() {
    run_matrix(&FIXTURE_AXES, REPETITIONS, OPERATIONS_PER_SAMPLE, true).await;
}

#[tokio::test]
#[ignore = "explicit real-axis authority-8 preflight; emits JSONL evidence"]
// The measured request is stack-pinned inside this bounded test harness frame.
#[allow(clippy::large_futures)]
async fn bounded_catalog_cost_real_axis_preflight() {
    run_matrix(&FIXTURE_AXES[..1], 1, 1, true).await;
}

#[tokio::test]
#[ignore = "explicit one-request-per-scenario preflight across all six frozen axes"]
#[allow(clippy::large_futures)]
async fn bounded_catalog_cost_all_axes_preflight() {
    run_matrix(&FIXTURE_AXES, 1, 1, true).await;
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "valid restore records and pin chains jointly exercise one GC invoice"
)]
async fn authority8_gc_invocation_bounds_pin_chains_and_restore_inventory() {
    use arco_catalog::workspace_restore::{
        restore_attempt_plan_path, restore_journal_path, restore_read_manifest_path,
        restore_request_path,
    };
    use arco_catalog::workspace_snapshot::{
        RetentionPinLatest, RetentionPinRevision, RetentionTarget, encode_retention_pin_latest,
        encode_retention_pin_revision, retention_pin_latest_path, retention_pin_revision_path,
    };
    use arco_core::storage::WritePrecondition;

    let backend = Arc::new(control_cost::CountingBackend::new(1));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
    let store = ControlMvpStateStore::new_synthetic_bounded(
        storage.clone(),
        StateScope::new("tenant", "workspace", "catalog"),
    )
    .unwrap();
    let source = store
        .install_synthetic_genesis(
            "gc-invoice-source",
            1,
            1,
            [SyntheticKvEntry {
                key: b"live".to_vec(),
                generation: 1,
                value: Some(b"old".to_vec()),
            }],
            0,
            [],
        )
        .await
        .unwrap();
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    txn.set_logical_operation_v2("gc-invoice-current", "kv", &"aa".repeat(32))
        .unwrap();
    txn.put(b"live", Bytes::from_static(b"new")).await.unwrap();
    txn.commit_v2().await.unwrap();
    let now = Utc::now();
    // Expired pins still require authenticating every selected predecessor.
    for ordinal in 0..4_u128 {
        let pin_id = format!("pin_{}", ulid::Ulid::from(900 + ordinal));
        let target = format!("snap_{}", ulid::Ulid::from(950 + ordinal));
        let mut revision = RetentionPinRevision::new(
            &pin_id,
            1,
            RetentionTarget::snapshot(&target).unwrap(),
            now,
            now + ChronoDuration::days(1),
            None,
        )
        .unwrap();
        let mut last = Vec::new();
        for index in 1..=1024 {
            if index > 1 {
                revision = revision
                    .renew(
                        index,
                        now + ChronoDuration::days(1)
                            + ChronoDuration::seconds(i64::try_from(index).unwrap()),
                        now,
                    )
                    .unwrap();
            }
            last = encode_retention_pin_revision(&revision).unwrap();
            storage
                .put_raw(
                    &retention_pin_revision_path(&pin_id, index).unwrap(),
                    Bytes::copy_from_slice(&last),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
        }
        let latest = RetentionPinLatest::new(
            &pin_id,
            1024,
            retention_pin_revision_path(&pin_id, 1024).unwrap(),
            format!("sha256:{}", hex::encode(Sha256::digest(&last))),
        )
        .unwrap();
        storage
            .put_raw(
                &retention_pin_latest_path(&pin_id).unwrap(),
                Bytes::from(encode_retention_pin_latest(&latest).unwrap()),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
    }
    // Canonical Applying journals for another domain must be scanned, not denied
    // merely for being present. They consume the same invoice as the pin chains.
    for ordinal in 0..129_u128 {
        let id = format!("rst_{}", ulid::Ulid::from(1000 + ordinal));
        let path = restore_journal_path(&id).unwrap();
        let journal = serde_json::json!({
            "record_type": "workspace_restore_journal", "version": 1,
            "restore_id": id, "revision": 1, "status": "APPLYING",
            "scope": WorkspaceScope::new("tenant", "workspace").unwrap(),
            "request_sha256": format!("sha256:{}", "11".repeat(32)),
            "request_path": restore_request_path(&id).unwrap(),
            "aggregate_attempt": 1, "attempt_path": restore_attempt_plan_path(&id, 1).unwrap(),
            "attempt_sha256": format!("sha256:{}", "22".repeat(32)),
            "required_domains": ["other"], "participants": [{"domain": "other", "participant_attempt": 1, "plan_sha256": format!("sha256:{}", "33".repeat(32)), "evidence": null}],
            "omitted_domains": [], "failure_category": null,
            "read_manifest_path": restore_read_manifest_path(&id).unwrap(),
            "finalized_at": null, "read_manifest_sha256": null
        });
        storage
            .put_raw(
                &path,
                Bytes::from(serde_jcs::to_vec(&journal).unwrap()),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
    }
    let orphan = store.paths().state_object(&"ee".repeat(32));
    storage
        .put_raw(
            &orphan,
            Bytes::from_static(b"orphan"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    backend.take();
    let result = arco_catalog::ControlMvpMaintenanceWorker::new_synthetic_bounded(&store)
        .plan_gc_at(now + ChronoDuration::days(40), Vec::new())
        .await;
    let counts = backend.take();
    println!(
        "gc-invocation {}",
        serde_json::json!({"calls": counts.logical_storage_calls, "read_bytes": counts.read_bytes, "head_calls": counts.head_attempts, "range_calls": counts.range_get_attempts, "list_calls": counts.list_page_attempts, "writes": counts.put_attempts, "deletes": counts.delete_attempts, "head_cas": counts.head_cas_attempts})
    );
    assert!(
        matches!(result, Err(CatalogError::MaintenanceBackpressure { .. })),
        "the whole invocation must reject oversized pin/restore inventory; got {result:?}"
    );
    assert!(
        counts.logical_storage_calls <= 4096,
        "actual backend calls exceeded admission: {}",
        counts.logical_storage_calls
    );
    assert_eq!(counts.put_attempts, 0);
    assert_eq!(counts.delete_attempts, 0);
    assert_eq!(counts.head_cas_attempts, 0);
    assert!(storage.head_raw(&orphan).await.unwrap().is_some());
    assert_eq!(
        store.get(b"live").await.unwrap(),
        Some(Bytes::from_static(b"new"))
    );
    assert_eq!(
        store
            .read_at(source)
            .await
            .unwrap()
            .get(b"live")
            .await
            .unwrap(),
        Some(Bytes::from_static(b"old"))
    );
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one active pin chain binds the snapshot, retained cut and GC probe"
)]
async fn authority8_gc_bounded_pin_inventory_preserves_snapshot_and_current_reads() {
    use arco_catalog::workspace_snapshot::{
        RetentionPinLatest, decode_retention_pin_latest, decode_retention_pin_revision,
        encode_retention_pin_latest, encode_retention_pin_revision, retention_pin_latest_path,
        retention_pin_revision_path,
    };
    use arco_core::storage::WritePrecondition;
    let backend = Arc::new(control_cost::CountingBackend::new(1));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = Arc::new(
        ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope.clone())
            .unwrap()
            .with_durable_authority_binding(DurableAuthorityBinding::new([93; 32])),
    );
    let source = store
        .install_synthetic_genesis(
            "gc-active-pin-source",
            1,
            1,
            [SyntheticKvEntry {
                key: b"live".to_vec(),
                generation: 1,
                value: Some(b"old".to_vec()),
            }],
            0,
            [],
        )
        .await
        .unwrap();
    let binding = WorkspaceDomainBinding::new(
        scope,
        store.clone(),
        store.clone(),
        Arc::new(EmptyRestoreCapture),
        Arc::new(EmptyRestoreCapture),
    )
    .unwrap();
    let registry = WorkspaceDomainRegistry::new(
        WorkspaceScope::new("tenant", "workspace").unwrap(),
        vec![binding],
    )
    .unwrap();
    let snapshot_id = format!("snap_{}", ulid::Ulid::from(930_u128));
    let pin_id = format!("pin_{}", ulid::Ulid::from(931_u128));
    let now = Utc::now();
    WorkspaceSnapshotService::new(storage.clone(), registry)
        .unwrap()
        .create_snapshot(
            &CreateWorkspaceSnapshotRequest::new(
                &snapshot_id,
                &pin_id,
                now,
                now + ChronoDuration::days(14),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let selector_path = retention_pin_latest_path(&pin_id).unwrap();
    let selector_head = storage.head_raw(&selector_path).await.unwrap().unwrap();
    let selected =
        decode_retention_pin_latest(&storage.get_raw(&selector_path).await.unwrap()).unwrap();
    let initial =
        decode_retention_pin_revision(&storage.get_raw(selected.revision_path()).await.unwrap())
            .unwrap();
    let release = initial.release(2, now + ChronoDuration::days(13)).unwrap();
    let bytes = encode_retention_pin_revision(&release).unwrap();
    let path = retention_pin_revision_path(&pin_id, 2).unwrap();
    storage
        .put_raw(
            &path,
            Bytes::copy_from_slice(&bytes),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    let selected = RetentionPinLatest::new(
        &pin_id,
        2,
        path,
        format!("sha256:{}", hex::encode(Sha256::digest(&bytes))),
    )
    .unwrap();
    storage
        .put_raw(
            &selector_path,
            Bytes::from(encode_retention_pin_latest(&selected).unwrap()),
            WritePrecondition::MatchesVersion(selector_head.version),
        )
        .await
        .unwrap();
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    txn.set_logical_operation_v2("gc-active-pin-current", "kv", &"bb".repeat(32))
        .unwrap();
    txn.put(b"live", Bytes::from_static(b"new")).await.unwrap();
    txn.commit_v2().await.unwrap();
    let orphan = store.paths().state_object(&"ee".repeat(32));
    storage
        .put_raw(
            &orphan,
            Bytes::from_static(b"orphan"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    backend.take();
    let plan = arco_catalog::ControlMvpMaintenanceWorker::new_synthetic_bounded(&store)
        .plan_gc_at(now + ChronoDuration::days(8), Vec::new())
        .await
        .unwrap();
    let counts = backend.take();
    assert_eq!(
        plan.candidates()
            .iter()
            .map(arco_catalog::state_store::ControlMvpGcCandidate::path)
            .collect::<Vec<_>>(),
        vec![orphan.as_str()]
    );
    assert!(counts.logical_storage_calls <= 4096);
    assert_eq!(counts.put_attempts, 0);
    assert_eq!(counts.delete_attempts, 0);
    assert_eq!(
        store.get(b"live").await.unwrap(),
        Some(Bytes::from_static(b"new"))
    );
    assert_eq!(
        store
            .read_at(source)
            .await
            .unwrap()
            .get(b"live")
            .await
            .unwrap(),
        Some(Bytes::from_static(b"old"))
    );
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the same GC probe covers pin corruption and maintenance lifecycle boundaries"
)]
async fn authority8_gc_bounded_pin_inventory_fails_closed_on_unverifiable_roots() {
    use arco_catalog::workspace_snapshot::{
        RetentionPinLatest, RetentionPinRevision, RetentionTarget, encode_retention_pin_latest,
        encode_retention_pin_revision, retention_pin_latest_path, retention_pin_revision_path,
    };
    use arco_core::storage::WritePrecondition;
    for scenario in [
        "active_maintenance",
        "expired_maintenance",
        "corrupt_predecessor",
        "oversized_chain",
    ] {
        let backend = Arc::new(control_cost::CountingBackend::new(1));
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        store
            .install_synthetic_genesis(
                "gc-pin-negative",
                1,
                1,
                [SyntheticKvEntry {
                    key: b"live".to_vec(),
                    generation: 1,
                    value: Some(b"value".to_vec()),
                }],
                0,
                [],
            )
            .await
            .unwrap();
        let now = Utc::now();
        let pin_id = format!("pin_{}", ulid::Ulid::from(940_u128));
        let maintenance = scenario.ends_with("maintenance");
        let created = if scenario == "active_maintenance" {
            now + ChronoDuration::days(39)
        } else {
            now
        };
        let target = if scenario.ends_with("maintenance") {
            RetentionTarget::Maintenance(format!("catalog/{}", "aa".repeat(32)))
        } else {
            RetentionTarget::snapshot(format!("snap_{}", ulid::Ulid::from(941_u128))).unwrap()
        };
        let mut pin = RetentionPinRevision::new(
            &pin_id,
            1,
            target,
            created,
            created + ChronoDuration::days(if maintenance { 8 } else { 1 }),
            None,
        )
        .unwrap();
        let mut last = Vec::new();
        let revision_count = if maintenance { 1 } else { 3 };
        for ordinal in 1..=revision_count {
            if ordinal > 1 {
                pin = pin
                    .renew(
                        ordinal,
                        pin.retained_until() + ChronoDuration::seconds(1),
                        now,
                    )
                    .unwrap();
            }
            last = encode_retention_pin_revision(&pin).unwrap();
            storage
                .put_raw(
                    &retention_pin_revision_path(&pin_id, ordinal).unwrap(),
                    Bytes::copy_from_slice(&last),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
        }
        let selected_revision = if scenario == "oversized_chain" {
            1025
        } else {
            revision_count
        };
        let selector = RetentionPinLatest::new(
            &pin_id,
            selected_revision,
            retention_pin_revision_path(&pin_id, selected_revision).unwrap(),
            format!("sha256:{}", hex::encode(Sha256::digest(&last))),
        )
        .unwrap();
        storage
            .put_raw(
                &retention_pin_latest_path(&pin_id).unwrap(),
                Bytes::from(encode_retention_pin_latest(&selector).unwrap()),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        if scenario == "corrupt_predecessor" {
            let path = retention_pin_revision_path(&pin_id, 2).unwrap();
            let head = storage.head_raw(&path).await.unwrap().unwrap();
            storage
                .put_raw(
                    &path,
                    Bytes::from_static(b"corrupt middle revision"),
                    WritePrecondition::MatchesVersion(head.version),
                )
                .await
                .unwrap();
        }
        let orphan = store.paths().state_object(&"ee".repeat(32));
        storage
            .put_raw(
                &orphan,
                Bytes::from_static(b"orphan"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        backend.take();
        let result = arco_catalog::ControlMvpMaintenanceWorker::new_synthetic_bounded(&store)
            .plan_gc_at(now + ChronoDuration::days(40), Vec::new())
            .await;
        let counts = backend.take();
        if scenario == "expired_maintenance" {
            assert_eq!(
                result.unwrap().candidates().len(),
                1,
                "expired pins do not require job loading"
            );
        } else if scenario == "active_maintenance" {
            assert!(
                matches!(result, Err(CatalogError::UnsupportedOperation { .. })),
                "a bounded reader must not dispatch to the unbounded maintenance loader: {result:?}"
            );
        } else {
            assert!(
                matches!(result, Err(CatalogError::Validation { .. })),
                "unverifiable pin chain must deny the page: {result:?}"
            );
        }
        assert!(counts.logical_storage_calls <= 4096);
        assert_eq!(counts.put_attempts, 0);
        assert_eq!(counts.delete_attempts, 0);
        assert_eq!(counts.head_cas_attempts, 0);
        assert!(storage.head_raw(&orphan).await.unwrap().is_some());
        assert_eq!(
            store.get(b"live").await.unwrap(),
            Some(Bytes::from_static(b"value"))
        );
    }
}

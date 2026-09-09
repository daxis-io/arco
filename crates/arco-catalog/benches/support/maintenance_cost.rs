//! Gate 5 measurements against the frozen eager algorithm.
#![allow(clippy::print_stderr, clippy::indexing_slicing)] // Acceptance fixtures use asserted JSON shapes and emit progress.
use super::{
    Arc, ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, BTreeMap, Bytes, CatalogError, Contents,
    ControlMvpMaintenanceWorker, ControlMvpStateStore, CountingBackend, Ordering, ScanRequest,
    ScopedStorage, StateScope, TxnOptions, measure_allocations, scaling_key,
};

async fn all_contents(reader: &dyn ArcoStateReader) -> Contents {
    let mut contents = BTreeMap::new();
    let mut continuation = None;
    loop {
        let mut request = ScanRequest::new(b"").with_limits(1024, 4 * 1024 * 1024, 64);
        if let Some(token) = continuation {
            request = request.with_token(token);
        }
        let page = reader.scan(request).await.unwrap();
        for entry in page.entries() {
            assert!(
                contents
                    .insert(entry.key().to_vec(), entry.value().bytes().clone())
                    .is_none()
            );
        }
        continuation = page.continuation().cloned();
        if continuation.is_none() {
            return contents;
        }
    }
}

fn checkpoint_sample(sample: &serde_json::Value) {
    use std::io::Write;
    if let Ok(path) = std::env::var("ARCO_EAGER_MAINTENANCE_JOURNAL") {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        serde_json::to_writer(&mut file, sample).unwrap();
        writeln!(file).unwrap();
    }
}

#[allow(
    clippy::indexing_slicing,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]
async fn maintenance_case(
    target: usize,
    owners: usize,
    suffix: usize,
    case: &str,
    durable: bool,
) -> serde_json::Value {
    let backend = Arc::new(CountingBackend::new(1));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
    let rows = match case {
        "empty" | "keyless" => 0,
        "bloom_disabled" => 104_859,
        _ => owners * 55,
    };
    let outbox_rows = if matches!(case, "outbox" | "keyless") {
        32
    } else {
        0
    };
    let worker = ControlMvpMaintenanceWorker::new(storage.clone(), scope.clone())
        .unwrap()
        .with_test_segment_sizing((rows + outbox_rows).max(1).div_ceil(owners), target)
        .unwrap();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    for ordinal in 0..rows {
        let key = scaling_key(ordinal);
        if matches!(case, "tombstones" | "bloom_disabled") {
            tx.delete(&key).await.unwrap();
        } else {
            let size = if case == "oversized" && ordinal == 0 {
                300 * 1024
            } else {
                1024
            };
            tx.put(&key, Bytes::from(vec![42; size])).await.unwrap();
        }
    }
    if matches!(case, "outbox" | "keyless") {
        for ordinal in 0..32 {
            tx.stage_projection_intent(
                format!("event-{ordinal}"),
                "projection",
                Bytes::from_static(b"payload"),
            )
            .await
            .unwrap();
        }
    }
    tx.commit().await.unwrap();
    worker.test_eager_consolidate(true).await.unwrap().unwrap();
    for _ in 0..suffix {
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        if rows > 0 {
            tx.put(&scaling_key(rows / 2), Bytes::from_static(b"suffix"))
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
    }
    let before = store.current_state_token().await.unwrap();
    let source: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(
                &store
                    .paths()
                    .manifest_object(before.authority_manifest_id()),
            )
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        source["payload"]["tx_refs"].as_array().unwrap().len(),
        suffix
    );
    let actual_owners = source["payload"]["base_states"].as_array().unwrap().len();
    if case == "kv" {
        assert_eq!(actual_owners, owners);
    }
    let expected = all_contents(&store).await;
    let live_rows = if matches!(case, "tombstones" | "bloom_disabled") {
        usize::from(suffix > 0)
    } else {
        rows
    };
    assert_eq!(expected.len(), live_rows);
    let expected_outbox = store.current_projection_outbox().await.unwrap();
    if durable {
        let maintenance = arco_catalog::DurableMaintenanceWorker::new(
            storage.clone(),
            scope,
            arco_catalog::DurableAuthorityBinding::new([21; 32]),
        )
        .unwrap()
        .with_test_segment_sizing((rows + outbox_rows).max(1).div_ceil(owners), target)
        .unwrap();
        let measured = durable_job_cost(&maintenance, &backend).await;
        assert_eq!(
            store
                .current_state_token()
                .await
                .unwrap()
                .logical_sequence(),
            before.logical_sequence()
        );
        assert_eq!(all_contents(&store).await, expected);
        assert_eq!(
            store.current_projection_outbox().await.unwrap(),
            expected_outbox
        );
        return compare_durable_case(target, actual_owners, suffix, case, measured);
    }
    backend.take();
    let (normal, normal_allocations) =
        measure_allocations(worker.test_eager_consolidate(false)).await;
    let normal = normal.unwrap();
    let normal_cost = backend.take();
    assert_eq!(normal.is_some(), suffix >= 16);
    let forced = if normal.is_none() {
        assert_eq!(normal_cost.put_attempts, 0);
        let (result, allocations) = measure_allocations(worker.test_eager_consolidate(true)).await;
        result.unwrap().unwrap();
        Some(serde_json::json!({"cost":backend.take(), "allocations":allocations}))
    } else {
        None
    };
    let after = store.current_state_token().await.unwrap();
    assert_eq!(before.logical_sequence(), after.logical_sequence());
    assert_eq!(all_contents(&store).await, expected);
    assert_eq!(
        store.current_projection_outbox().await.unwrap(),
        expected_outbox
    );
    serde_json::json!({"case":case, "target_bytes":target, "owning_l1_shards":actual_owners,
        "rows":rows, "l0_suffix":suffix, "normal":{"did_rewrite":normal.is_some(),
        "cost":normal_cost, "allocations":normal_allocations}, "forced_test_only":forced})
}

async fn eager_case(target: usize, owners: usize, suffix: usize, case: &str) -> serde_json::Value {
    Box::pin(maintenance_case(target, owners, suffix, case, false)).await
}

pub async fn run_eager_matrix() -> serde_json::Value {
    let mut samples = Vec::new();
    for target in [32, 64, 128, 256] {
        for owners in [1, 4, 16, 64] {
            for suffix in [0, 1, 8, 16, 31] {
                eprintln!("Gate 5 eager: target={target} KiB owners={owners} suffix={suffix}");
                let sample = Box::pin(eager_case(target * 1024, owners, suffix, "kv")).await;
                checkpoint_sample(&sample);
                samples.push(sample);
            }
        }
    }
    for case in [
        "empty",
        "tombstones",
        "oversized",
        "bloom_disabled",
        "outbox",
        "keyless",
    ] {
        eprintln!("Gate 5 eager: exceptional={case}");
        let sample = Box::pin(eager_case(64 * 1024, 1, 1, case)).await;
        checkpoint_sample(&sample);
        samples.push(sample);
    }
    let known_reference_defect = eager_outbox_ordering_defect().await;
    serde_json::json!({"reference_base":"745ed92e25ff7b4ec85aa0be57b02450642fda59",
        "reference":"original eager algorithm, explicit force only changes admission",
        "accounting":"whole-operation totals include child phases; do not sum them",
        "allocations":"cumulative allocator bytes during future polls, not RSS",
        "scope":"MemoryBackend; uninterrupted baseline; concurrency and fault schedules measured separately",
        "samples":samples, "known_reference_defect":known_reference_defect})
}

pub async fn eager_fixture_smoke() {
    for case in [
        "kv",
        "empty",
        "tombstones",
        "oversized",
        "outbox",
        "keyless",
    ] {
        Box::pin(eager_case(64 * 1024, 1, 1, case)).await;
    }
    eager_outbox_ordering_defect().await;
}

pub async fn eager_disabled_source() -> serde_json::Value {
    let backend = Arc::new(CountingBackend::new(1));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
    let worker = ControlMvpMaintenanceWorker::new(storage.clone(), scope).unwrap();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    let id = tx.tx_id().to_string();
    for ordinal in 0..104_859 {
        tx.delete(&scaling_key(ordinal)).await.unwrap();
    }
    let before = tx.commit().await.unwrap();
    let index: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&store.paths().segment_index(&id))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(index.get("bloomMode").unwrap(), "Disabled");
    backend.take();
    let (result, allocations) = measure_allocations(worker.test_eager_consolidate(true)).await;
    let result = result.unwrap().unwrap();
    let cost = backend.take();
    assert_eq!(
        result.selected_token().logical_sequence(),
        before.logical_sequence()
    );
    assert!(all_contents(&store).await.is_empty());
    serde_json::json!({"case":"disabled_filter_in_selected_L0_source", "rows":104_859,
        "l0_suffix":1, "owning_l1_shards":0, "cost":cost, "allocations":allocations,
        "admission":"explicit test-only forced eager rewrite"})
}

async fn eager_outbox_ordering_defect() -> serde_json::Value {
    let backend = Arc::new(CountingBackend::new(1));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
    let worker = ControlMvpMaintenanceWorker::new(storage, scope)
        .unwrap()
        .with_test_segment_sizing(1, 64 * 1024)
        .unwrap();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    for ordinal in 0..12 {
        tx.stage_projection_outbox(arco_catalog::ControlMvpProjectionOutboxRecord::new(
            format!("event-{ordinal}"),
            Bytes::from_static(b"payload"),
        ))
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    backend.take();
    let (result, allocations) = measure_allocations(worker.test_eager_consolidate(true)).await;
    let error = result.unwrap_err();
    assert!(
        matches!(&error, CatalogError::InvariantViolation { message }
        if message == "discontinuous L1 shard sequence or ordinals")
    );
    let cost = backend.take();
    assert_eq!(cost.put_attempts, 0);
    serde_json::json!({"case":"original_eager_outbox_id_order_crosses_logical_order",
        "rows":12, "rows_per_shard":1, "error":error.to_string(), "cost":cost,
        "allocations":allocations, "status":"known reference defect, no successful rewrite denominator"})
}

/// Barrier schedules consume no wall-clock sleeps and preserve each invocation.
pub async fn run_eager_schedules() -> serde_json::Value {
    let mut samples = Vec::new();
    for schedule in [
        "none",
        "one_commit",
        "three_commits",
        "segment_put",
        "index_put",
        "manifest_put",
        "head_put",
        "lost_head_response",
    ] {
        let backend = Arc::new(CountingBackend::new(1));
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
        let worker = ControlMvpMaintenanceWorker::new(storage, scope).unwrap();
        for sequence in 0..16 {
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            tx.put(b"key", Bytes::from(vec![sequence; 1024]))
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        backend.take();
        let concurrent = match schedule {
            "one_commit" => 1,
            "three_commits" => 3,
            _ => 0,
        };
        let fail_at = match schedule {
            "segment_put" => 1,
            "index_put" => 2,
            "manifest_put" => 3,
            "head_put" => 4,
            _ => 0,
        };
        backend.pause_l1.store(concurrent > 0, Ordering::SeqCst);
        backend.fail_put_countdown.store(fail_at, Ordering::SeqCst);
        backend
            .lose_head_response
            .store(schedule == "lost_head_response", Ordering::SeqCst);
        // Counters in a concurrent sample intentionally include logical commits;
        // its entire operation is reported, never presented as isolated rewrite cost.
        let ((result, ()), allocations) = measure_allocations(async {
            tokio::join!(worker.test_eager_consolidate(false), async {
                if concurrent > 0 {
                    backend.l1_entered.notified().await;
                    for _ in 0..concurrent {
                        let mut tx = store
                            .begin_control_txn(TxnOptions::default())
                            .await
                            .unwrap();
                        tx.put(b"later", Bytes::from_static(b"preserved"))
                            .await
                            .unwrap();
                        tx.commit().await.unwrap();
                    }
                    backend.l1_release.notify_one();
                }
            })
        })
        .await;
        let first = backend.take();
        let error = result.as_ref().err().map(ToString::to_string);
        assert_eq!(result.is_err(), fail_at > 0);
        let retry = if result.is_err() {
            let (result, allocations) =
                measure_allocations(worker.test_eager_consolidate(false)).await;
            result.unwrap().unwrap();
            Some(serde_json::json!({"cost":backend.take(), "allocations":allocations}))
        } else {
            None
        };
        assert_eq!(
            store.get(b"key").await.unwrap().unwrap(),
            Bytes::from(vec![15; 1024])
        );
        if concurrent > 0 {
            assert!(store.get(b"later").await.unwrap().is_some());
        }
        samples.push(
            serde_json::json!({"schedule":schedule, "concurrent_commits":concurrent,
            "first":{"cost":first,"allocations":allocations,"error":error}, "retry":retry}),
        );
    }
    serde_json::json!({"reference":"original eager", "samples":samples,
        "concurrent_accounting":"whole-operation includes logical commit work; not isolated maintenance cost"})
}

#[derive(Default, serde::Serialize)]
struct DurableCost {
    total: super::BackendCounts,
    allocations: super::Allocations,
    phases: Vec<serde_json::Value>,
    violations: Vec<String>,
}

impl DurableCost {
    async fn measure<T>(
        &mut self,
        name: &str,
        backend: &Arc<CountingBackend>,
        operation: impl Future<Output = T>,
    ) -> T {
        let (result, allocations) = Box::pin(measure_allocations(operation)).await;
        let mut counts = backend.take();
        // Each invocation explicitly reports even phases that did no work.
        for phase in MAINTENANCE_PHASES {
            counts.phases.entry((*phase).into()).or_default();
        }
        self.total.add(&counts);
        self.allocations.count += allocations.count;
        self.allocations.bytes += allocations.bytes;
        self.phases
            .push(serde_json::json!({"phase":name,"cost":counts,"allocations":allocations}));
        result
    }
}

#[allow(clippy::too_many_lines)] // Keep the measured job boundaries together.
async fn durable_job_cost(
    worker: &arco_catalog::DurableMaintenanceWorker,
    backend: &Arc<CountingBackend>,
) -> DurableCost {
    let now = chrono::Utc::now();
    backend.take();
    let mut cost = DurableCost::default();
    let normal = cost
        .measure("prepare_normal", backend, Box::pin(worker.prepare_at(now)))
        .await
        .unwrap();
    let plan = match normal {
        Some(plan) => plan,
        None => cost
            .measure(
                "prepare_forced_test_only",
                backend,
                Box::pin(worker.test_prepare_forced_at(now)),
            )
            .await
            .unwrap()
            .unwrap(),
    };
    let id = plan.job_id().clone();
    let mut progress = cost
        .measure(
            "start_pinning",
            backend,
            Box::pin(worker.start_at(&plan, now)),
        )
        .await
        .unwrap();
    for _ in 0..256 {
        if progress.status == arco_catalog::MaintenanceStatus::ReadyToPublish {
            break;
        }
        let prior = progress.completed;
        progress = cost
            .measure("advance", backend, Box::pin(worker.advance_at(&id, now)))
            .await
            .unwrap();
        assert_eq!(progress.completed, prior + 1);
        let measured = cost.phases.last().unwrap();
        let counts = &measured["cost"];
        if counts["put_attempts"].as_u64().unwrap() != 4 {
            cost.violations.push(
                "advance must issue two output PUTs, one receipt PUT and one selector CAS".into(),
            );
        }
        let reads = &counts["object_reads"]["data"];
        if reads["ranges"].as_u64().unwrap_or(0) > 64
            || reads["returned_bytes"].as_u64().unwrap_or(0) > 64 * 1024 * 1024
        {
            cost.violations
                .push("selected advance data exceeds budget".into());
        }
    }
    assert_eq!(
        progress.status,
        arco_catalog::MaintenanceStatus::ReadyToPublish
    );
    cost.measure("resume", backend, Box::pin(worker.resume_at(&id, now)))
        .await
        .unwrap();
    let resumed = &cost.phases.last().unwrap()["cost"];
    if resumed["object_reads"]["data"]["returned_bytes"]
        .as_u64()
        .unwrap_or(0)
        != 0
    {
        cost.violations
            .push("resume reconstructed source data".into());
    }
    let requests = [
        "get_attempts",
        "range_get_attempts",
        "head_attempts",
        "put_attempts",
        "list_attempts",
        "list_page_attempts",
        "delete_attempts",
    ]
    .iter()
    .map(|name| resumed[*name].as_u64().unwrap())
    .sum::<u64>();
    if requests > 1280 || resumed["read_bytes"].as_u64().unwrap() > 192 * 1024 * 1024 {
        cost.violations
            .push("resume metadata budget exceeded".into());
    }
    cost.measure(
        "publication",
        backend,
        Box::pin(worker.publish_at(&id, now)),
    )
    .await
    .unwrap()
    .unwrap();
    let published = &cost.phases.last().unwrap()["cost"];
    if published["head_cas_attempts"].as_u64().unwrap() != 1 {
        cost.violations
            .push("publication HEAD submission count differs from one".into());
    }
    if published["object_writes"]
        .as_object()
        .unwrap()
        .keys()
        .any(|path| path.contains("/segments/l1/") || path.contains("/indexes/maintenance-"))
    {
        cost.violations
            .push("publication rewrote completed output".into());
    }
    cost
}

fn l1_bytes(cost: &serde_json::Value) -> u64 {
    cost["object_writes"]
        .as_object()
        .unwrap()
        .iter()
        .filter(|(path, _)| path.contains("/segments/l1/") || path.contains("/indexes/"))
        .map(|(_, bytes)| bytes.as_u64().unwrap())
        .sum()
}

fn hash_bytes(cost: &serde_json::Value) -> u64 {
    // Root, validation and full-checksum counters are children of the SHA helper.
    // Witness framing, Bloom probes and retention codecs hash directly. Frozen eager
    // reports omit Bloom/retention bytes, so their denominator is a lower bound.
    cost["sha256_helper_bytes"].as_u64().unwrap()
        + cost["witness_hash_bytes"].as_u64().unwrap()
        + cost["bloom_hash_bytes"].as_u64().unwrap_or(0)
        + cost["retention_hash_bytes"].as_u64().unwrap_or(0)
}

fn compare_durable_case(
    target: usize,
    owners: usize,
    suffix: usize,
    case: &str,
    mut measured: DurableCost,
) -> serde_json::Value {
    let baseline: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../docs/reports/2026-09-07-gate5-eager-baseline.json"
    ))
    .unwrap();
    let baseline = baseline["samples"]
        .as_array()
        .unwrap()
        .iter()
        .find(|sample| {
            sample["case"] == case
                && sample["target_bytes"] == target
                && sample["owning_l1_shards"] == owners
                && sample["l0_suffix"] == suffix
        })
        .unwrap();
    let eager = if baseline["forced_test_only"].is_null() {
        &baseline["normal"]
    } else {
        &baseline["forced_test_only"]
    };
    let eager_cost = &eager["cost"];
    let total = serde_json::to_value(&measured.total).unwrap();
    let output_bytes = l1_bytes(&total);
    let candidate_data = total["object_writes"]
        .as_object()
        .unwrap()
        .iter()
        .filter(|(path, _)| path.contains("/segments/l1/"))
        .map(|(_, bytes)| bytes.as_u64().unwrap())
        .sum::<u64>();
    let source_data = eager_cost["object_reads"]["data"]["returned_bytes"]
        .as_u64()
        .unwrap_or(0);
    let construction_data = measured
        .phases
        .iter()
        .filter(|phase| phase["phase"] == "advance")
        .map(|phase| {
            phase["cost"]["object_reads"]["data"]["returned_bytes"]
                .as_u64()
                .unwrap_or(0)
        })
        .sum::<u64>();
    let data_reads = total["object_reads"]["data"]["returned_bytes"]
        .as_u64()
        .unwrap_or(0);
    for (passed, message) in [
        (
            output_bytes <= 2 * l1_bytes(eager_cost),
            "L1 bytes exceed 2x eager",
        ),
        (
            measured.allocations.bytes <= 8 * eager["allocations"]["bytes"].as_u64().unwrap(),
            "cumulative allocations exceed 8x eager",
        ),
        (
            hash_bytes(&total) <= 8 * hash_bytes(eager_cost),
            "hashing exceeds 8x eager",
        ),
        (
            construction_data <= 2 * source_data,
            "construction data exceeds 2x source",
        ),
        (
            data_reads <= 4 * source_data + candidate_data,
            "data ledger exceeds 4x eager source plus candidate",
        ),
    ] {
        if !passed {
            measured.violations.push(message.into());
        }
    }
    serde_json::json!({"case":case,"target_bytes":target,"owning_l1_shards":owners,"l0_suffix":suffix,
        "source_data_bytes":source_data,"candidate_data_bytes":candidate_data,"construction_data_bytes":construction_data,
        "eager_l1_bytes":l1_bytes(eager_cost),"new_l1_bytes":output_bytes,"eager_allocations":eager["allocations"],
        "eager_hash_bytes":hash_bytes(eager_cost),"new_hash_bytes":hash_bytes(&total),"measured":measured})
}

pub async fn durable_fixture_smoke() {
    for case in ["kv", "empty", "outbox", "keyless"] {
        let sample = Box::pin(maintenance_case(64 * 1024, 1, 1, case, true)).await;
        eprintln!("durable fixture: {sample}");
        assert!(
            sample["measured"]["violations"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
}

pub async fn run_durable_matrix() -> serde_json::Value {
    let mut samples = Vec::new();
    for target in [32, 64, 128, 256] {
        for owners in [1, 4, 16, 64] {
            for suffix in [0, 1, 8, 16, 31] {
                eprintln!("Gate 5 durable: target={target} KiB owners={owners} suffix={suffix}");
                samples.push(
                    Box::pin(maintenance_case(target * 1024, owners, suffix, "kv", true)).await,
                );
            }
        }
    }
    for case in [
        "empty",
        "tombstones",
        "oversized",
        "bloom_disabled",
        "outbox",
        "keyless",
    ] {
        samples.push(Box::pin(maintenance_case(64 * 1024, 1, 1, case, true)).await);
    }
    serde_json::json!({"reference_base":"745ed92e25ff7b4ec85aa0be57b02450642fda59", "samples":samples,
        "accounting":"operation totals include nested phases; operation allocation measurements partition the workload; report serialization excluded; cumulative allocations are not RSS; unit digests use witness framing counters",
        "scope":"MemoryBackend uninterrupted full matrix; forced admission explicitly named"})
}

/// Completed-output reuse across interruptions, restarts and consumed publish cuts.
#[allow(clippy::too_many_lines, clippy::cognitive_complexity)] // Explicit fault schedule table.
pub async fn run_durable_schedules() -> serde_json::Value {
    use arco_catalog::{DurableAuthorityBinding, DurableMaintenanceWorker, MaintenanceStatus};
    let mut samples = Vec::new();
    for schedule in [
        "none",
        "one_commit",
        "three_commits",
        "advance_put_1",
        "advance_put_2",
        "advance_put_3",
        "advance_put_4",
        "publish_put_1",
        "publish_put_2",
        "publish_put_3",
        "publish_put_4",
        "publish_put_5",
        "lost_head_response",
        "cas_loss",
        "cancel_unfinished_output",
    ] {
        let backend = Arc::new(CountingBackend::new(1));
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
        for sequence in 0..16 {
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            if sequence == 0 {
                for key in 0..8 {
                    tx.put(&[key], Bytes::from(vec![key; 1024])).await.unwrap();
                }
            }
            tx.commit().await.unwrap();
        }
        let worker = DurableMaintenanceWorker::new(
            storage.clone(),
            scope.clone(),
            DurableAuthorityBinding::new([22; 32]),
        )
        .unwrap()
        .with_test_segment_sizing(2, 8 * 1024)
        .unwrap();
        backend.take();
        let now = chrono::Utc::now();
        let mut cost = DurableCost::default();
        let plan = cost
            .measure("prepare", &backend, Box::pin(worker.prepare_at(now)))
            .await
            .unwrap()
            .unwrap();
        let id = plan.job_id().clone();
        cost.measure(
            "start_pinning",
            &backend,
            Box::pin(worker.start_at(&plan, now)),
        )
        .await
        .unwrap();
        cost.measure(
            "first_complete_unit",
            &backend,
            Box::pin(worker.advance_at(&id, now)),
        )
        .await
        .unwrap();
        let completed_objects: Vec<String> = cost
            .total
            .object_writes
            .keys()
            .filter(|path| path.contains("/segments/l1/") || path.contains("/indexes/maintenance-"))
            .cloned()
            .collect();
        assert_eq!(completed_objects.len(), 2);
        let concurrent = if schedule == "three_commits" {
            3
        } else {
            usize::from(schedule == "one_commit")
        };
        for _ in 0..concurrent {
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            tx.put(b"later", Bytes::from_static(b"retained suffix"))
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        let concurrent_cost = backend.take();
        if let Some(ordinal) = schedule.strip_prefix("advance_put_") {
            backend
                .fail_put_countdown
                .store(ordinal.parse().unwrap(), Ordering::SeqCst);
            let result = cost
                .measure(
                    "interrupted_advance",
                    &backend,
                    Box::pin(worker.advance_at(&id, now)),
                )
                .await;
            assert!(result.is_err());
        }
        if schedule == "cancel_unfinished_output" {
            backend.pause_l1.store(true, Ordering::SeqCst);
            cost.measure("cancel_before_unfinished_output", &backend, async {
                let mut work = Box::pin(worker.advance_at(&id, now));
                tokio::select! { result = &mut work => panic!("advance did not pause: {result:?}"), () = backend.l1_entered.notified() => {} }
                drop(work);
            }).await;
            assert_eq!(cost.phases.last().unwrap()["cost"]["put_attempts"], 0);
        }
        let restarted =
            DurableMaintenanceWorker::new(storage, scope, DurableAuthorityBinding::new([22; 32]))
                .unwrap();
        for _ in 0..3 {
            let progress = cost
                .measure(
                    "repeated_resume",
                    &backend,
                    Box::pin(restarted.resume_at(&id, now)),
                )
                .await
                .unwrap();
            assert_eq!(progress.completed, 1);
            let counts = &cost.phases.last().unwrap()["cost"];
            assert_eq!(counts["put_attempts"], 0);
            assert_eq!(
                counts["object_reads"]["data"]["returned_bytes"]
                    .as_u64()
                    .unwrap_or(0),
                0
            );
        }
        for _ in 0..256 {
            let progress = cost
                .measure(
                    "remaining_advance",
                    &backend,
                    Box::pin(restarted.advance_at(&id, now)),
                )
                .await
                .unwrap();
            if progress.status == MaintenanceStatus::ReadyToPublish {
                break;
            }
        }
        if let Some(ordinal) = schedule.strip_prefix("publish_put_") {
            backend
                .fail_put_countdown
                .store(ordinal.parse().unwrap(), Ordering::SeqCst);
        }
        backend
            .lose_head_response
            .store(schedule == "lost_head_response", Ordering::SeqCst);
        backend
            .pause_publication_manifest
            .store(schedule == "cas_loss", Ordering::SeqCst);
        let publication = async {
            tokio::join!(restarted.publish_at(&id, now), async {
                if schedule == "cas_loss" {
                    backend.l1_entered.notified().await;
                    let mut tx = store
                        .begin_control_txn(TxnOptions::default())
                        .await
                        .unwrap();
                    tx.put(b"cas-loss", Bytes::from_static(b"preserved"))
                        .await
                        .unwrap();
                    tx.commit().await.unwrap();
                    backend.l1_release.notify_one();
                }
            })
            .0
        };
        let first = cost
            .measure(
                "publication_including_concurrent_commit_if_any",
                &backend,
                Box::pin(publication),
            )
            .await;
        let first_error = first.as_ref().err().map(ToString::to_string);
        let mut published = first.is_ok_and(|outcome| outcome.is_some());
        if schedule.starts_with("publish_put_") {
            assert!(first_error.is_some());
        }
        if schedule == "cas_loss" {
            assert!(!published && first_error.is_none());
        }
        for _ in 0..16 {
            if published {
                break;
            }
            published = cost
                .measure(
                    "publication_retry_or_reconcile",
                    &backend,
                    Box::pin(restarted.publish_at(&id, now)),
                )
                .await
                .unwrap()
                .is_some();
        }
        assert!(published);
        for phase in cost.phases.iter().skip(3) {
            for path in &completed_objects {
                assert!(
                    phase["cost"]["object_writes"][path].is_null(),
                    "completed object rewritten: {schedule} {path}"
                );
            }
        }
        if concurrent > 0 {
            assert_eq!(
                store.get(b"later").await.unwrap(),
                Some(Bytes::from_static(b"retained suffix"))
            );
        }
        if schedule == "cas_loss" {
            assert_eq!(
                store.get(b"cas-loss").await.unwrap(),
                Some(Bytes::from_static(b"preserved"))
            );
        }
        for key in 0..8 {
            assert_eq!(
                store.get(&[key]).await.unwrap(),
                Some(Bytes::from(vec![key; 1024]))
            );
        }
        samples.push(serde_json::json!({"schedule":schedule,"completed_output_objects":completed_objects,"first_publication_error":first_error,"concurrent_commits":concurrent,"concurrent_commit_cost":concurrent_cost,"measured":cost}));
    }
    serde_json::json!({"samples":samples,"accounting":"each invocation charged separately; completed objects never PUT again; interrupted unfinished objects may be retried; CAS-loss first invocation includes separately identified logical commit; cumulative allocations include cancellation probes and exclude spawned work"})
}

/// Explicit terminal and reclamation costs, separate from uninterrupted rewrite ratios.
#[allow(clippy::too_many_lines, clippy::cognitive_complexity)] // Explicit measured lifecycle schedules.
pub async fn run_durable_lifecycle() -> serde_json::Value {
    use arco_catalog::{DurableAuthorityBinding, DurableMaintenanceWorker, MaintenanceStatus};
    let mut samples = Vec::new();
    for schedule in [
        "active_abandon",
        "reclamation_invalidation",
        "expired_collection",
    ] {
        let backend = Arc::new(CountingBackend::new(1));
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
        for sequence in 0..16 {
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            if sequence == 0 {
                for key in 0..8 {
                    tx.put(&[key], Bytes::from(vec![key; 1024])).await.unwrap();
                }
            }
            tx.commit().await.unwrap();
        }
        storage
            .put_raw(
                &store.paths().state_object("lifecycle-orphan"),
                Bytes::from_static(b"orphan"),
                arco_core::WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        let worker = DurableMaintenanceWorker::new(
            storage.clone(),
            scope.clone(),
            DurableAuthorityBinding::new([27; 32]),
        )
        .unwrap()
        .with_test_segment_sizing(2, 8 * 1024)
        .unwrap();
        let collector = ControlMvpMaintenanceWorker::new(storage.clone(), scope).unwrap();
        backend.take();
        let now = chrono::Utc::now();
        let mut cost = DurableCost::default();
        let plan = cost
            .measure("prepare", &backend, Box::pin(worker.prepare_at(now)))
            .await
            .unwrap()
            .unwrap();
        let id = plan.job_id().clone();
        cost.measure(
            "start_pinning",
            &backend,
            Box::pin(worker.start_at(&plan, now)),
        )
        .await
        .unwrap();
        let progress = cost
            .measure("advance", &backend, Box::pin(worker.advance_at(&id, now)))
            .await
            .unwrap();
        assert_eq!(progress.status, MaintenanceStatus::Active);
        let outputs: Vec<String> = cost
            .total
            .object_writes
            .keys()
            .filter(|path| path.contains("/segments/l1/") || path.contains("/indexes/"))
            .cloned()
            .collect();
        assert_eq!(outputs.len(), 2);
        if schedule == "active_abandon" {
            let terminal = cost
                .measure(
                    "abandonment",
                    &backend,
                    Box::pin(worker.abandon_at(&id, now)),
                )
                .await
                .unwrap();
            assert_eq!(terminal.status, MaintenanceStatus::Abandoned);
            assert_eq!(cost.phases.last().unwrap()["cost"]["put_attempts"], 2);
        }
        if schedule == "reclamation_invalidation" {
            let page = cost
                .measure(
                    "GC_live_pin",
                    &backend,
                    Box::pin(collector.collect_gc_at(
                        now + chrono::Duration::days(7) + chrono::Duration::hours(1),
                        Vec::new(),
                    )),
                )
                .await
                .unwrap();
            assert!(page.objects_deleted() > 0);
            let error = cost
                .measure(
                    "invalidation",
                    &backend,
                    Box::pin(worker.advance_at(&id, now)),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, CatalogError::PreconditionFailed { .. }));
            assert_eq!(cost.phases.last().unwrap()["cost"]["put_attempts"], 0);
            cost.measure(
                "abandon_invalidated",
                &backend,
                Box::pin(worker.abandon_at(&id, now)),
            )
            .await
            .unwrap();
        }
        if schedule == "expired_collection" {
            assert!(
                cost.measure(
                    "expired_resume",
                    &backend,
                    Box::pin(worker.resume_at(&id, now + chrono::Duration::hours(24)))
                )
                .await
                .is_err()
            );
            assert_eq!(cost.phases.last().unwrap()["cost"]["put_attempts"], 0);
        }
        let mut cursor = None;
        let mut deleted = 0;
        loop {
            let page = cost
                .measure(
                    "GC_expired_page",
                    &backend,
                    Box::pin(collector.collect_gc_page_at(
                        now + chrono::Duration::days(9),
                        Vec::new(),
                        cursor.as_deref(),
                    )),
                )
                .await
                .unwrap();
            deleted += page.objects_deleted();
            cursor = page.continuation().map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        assert!(deleted > 0);
        assert!(cost.total.retention_hash_bytes > 0);
        assert!(cost.allocations.bytes > 0);
        // Final postconditions are outside measured operations; discard their counters.
        for path in &outputs {
            let relative = path
                .strip_prefix("tenant=tenant/workspace=workspace/")
                .unwrap();
            assert!(storage.head_raw(relative).await.unwrap().is_none());
        }
        for key in 0..8 {
            assert_eq!(
                store.get(&[key]).await.unwrap().unwrap(),
                Bytes::from(vec![key; 1024])
            );
        }
        backend.take();
        samples.push(serde_json::json!({"schedule":schedule,"completed_units_before_terminal":progress.completed,
            "planned_units":progress.total,"discarded_output_objects":outputs,"expired_objects_deleted":deleted,"measured":cost}));
    }
    serde_json::json!({"samples":samples,"accounting":"Lifecycle totals include children. GC pages, abandonment, invalidation and expiry rejection are measured separately. Setup and final postcondition probes are excluded. No eager ratio is claimed for terminal-only operations."})
}

/// Job-wide admission rejects the 257th indivisible unit before any artifact PUT.
pub async fn durable_plan_capacity_rejects_without_puts() {
    use arco_catalog::{DurableAuthorityBinding, DurableMaintenanceWorker};
    let backend = Arc::new(CountingBackend::new(1));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
    for sequence in 0..16 {
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        if sequence == 0 {
            for key in 0_u16..257 {
                tx.put(&key.to_be_bytes(), Bytes::from_static(b"value"))
                    .await
                    .unwrap();
            }
        }
        tx.commit().await.unwrap();
    }
    let worker =
        DurableMaintenanceWorker::new(storage, scope, DurableAuthorityBinding::new([28; 32]))
            .unwrap()
            .with_test_segment_sizing(1, 8 * 1024)
            .unwrap();
    backend.take();
    let error = worker.prepare_at(chrono::Utc::now()).await.err().unwrap();
    assert!(
        matches!(error, CatalogError::MaintenanceBackpressure { ref message }
        if message == "maintenance plan exceeds job capacity")
    );
    let rejected = backend.take();
    assert_eq!(rejected.put_attempts, 0);
    assert!(rejected.object_writes.is_empty());
    assert_eq!(rejected.maintenance_render_rows, 257);
}

const MAINTENANCE_PHASES: &[&str] = &[
    "maintenance-selection",
    "maintenance-source-metadata",
    "maintenance-selected-data-reads",
    "maintenance-output-rendering-validation",
    "maintenance-completed-output-reuse",
    "maintenance-final-equivalence",
    "maintenance-start-preflight",
    "maintenance-start-pinning",
    "maintenance-preflight-reconstruction",
    "maintenance-preflight-sizing",
    "maintenance-resume",
    "maintenance-progress-authentication",
    "maintenance-plan-authentication",
    "maintenance-advance",
    "maintenance-construction",
    "maintenance-ancestry-compatibility",
    "maintenance-publish-source-reconstruction",
    "maintenance-candidate-reconstruction",
    "maintenance-manifest-HEAD-rendering",
    "maintenance-immutable-write",
    "maintenance-publication",
    "maintenance-publication-reconciliation",
    "maintenance-attempt-authentication",
    "maintenance-progress-CAS",
    "maintenance-HEAD-CAS",
    "maintenance-root-recovery",
    "maintenance-abandonment",
    "maintenance-GC-root",
    "maintenance-GC-pins",
];

#[tokio::test]
async fn phase_partitions_detect_selected_read_amplification_and_report_zero_work() {
    let mut samples = Vec::new();
    for repetitions in [1, 2] {
        let backend = Arc::new(CountingBackend::new(1));
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let store = ControlMvpStateStore::new(storage.clone(), scope.clone()).unwrap();
        for ordinal in 0..16 {
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            if ordinal == 0 {
                tx.put(b"key", Bytes::from_static(b"value")).await.unwrap();
            }
            tx.commit().await.unwrap();
        }
        let worker = arco_catalog::DurableMaintenanceWorker::new(
            storage,
            scope,
            arco_catalog::DurableAuthorityBinding::new([17; 32]),
        )
        .unwrap();
        let now = chrono::Utc::now();
        let plan = worker.prepare_at(now).await.unwrap().unwrap();
        worker.start_at(&plan, now).await.unwrap();
        backend.take();
        backend
            .selected_read_repetitions
            .store(repetitions, Ordering::SeqCst);
        let mut measured = DurableCost::default();
        measured
            .measure("advance", &backend, worker.advance_at(plan.job_id(), now))
            .await
            .unwrap();
        let counts = measured.total;
        assert_eq!(
            counts.read_bytes,
            counts.phases.values().map(|p| p.read_bytes).sum::<u64>()
        );
        assert_eq!(
            counts.requests(),
            counts
                .phases
                .values()
                .map(super::BackendCounts::requests)
                .sum::<u64>()
        );
        assert_eq!(
            counts.sha256_helper_bytes,
            counts
                .phases
                .values()
                .map(|p| p.sha256_helper_bytes)
                .sum::<u64>()
        );
        assert_eq!(
            counts.full_checksum_bytes,
            counts
                .phases
                .values()
                .map(|p| p.full_checksum_bytes)
                .sum::<u64>()
        );
        assert!(counts.phases["maintenance-source-metadata"].read_bytes > 0);
        assert!(counts.phases["maintenance-selected-data-reads"].read_bytes > 0);
        assert_eq!(
            counts.phases["maintenance-output-rendering-validation"].maintenance_render_rows,
            1
        );
        assert_eq!(
            counts.phases["maintenance-completed-output-reuse"].requests(),
            0
        );
        assert_eq!(
            counts.phases["maintenance-final-equivalence"].full_checksum_bytes,
            0
        );
        samples.push(counts);
    }
    let normal = &samples[0];
    let amplified = &samples[1];
    assert_eq!(
        amplified.phases["maintenance-selected-data-reads"].read_bytes,
        2 * normal.phases["maintenance-selected-data-reads"].read_bytes
    );
    assert_eq!(
        amplified.phases["maintenance-source-metadata"].read_bytes,
        normal.phases["maintenance-source-metadata"].read_bytes
    );
    assert_eq!(
        amplified.phases["maintenance-output-rendering-validation"].maintenance_render_rows,
        normal.phases["maintenance-output-rendering-validation"].maintenance_render_rows
    );
}

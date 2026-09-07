//! Shared deterministic workload and `StorageBackend`-boundary accounting.
//! No provider latency, pricing, or cache evidence is inferred.
#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::ops::Range;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

use arco_catalog::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, CatalogError, CheckpointOptions,
    ControlMvpMaintenanceWorker, ControlMvpStateStore, ScanRequest, StateScope, StateToken,
    TxnOptions,
};
use arco_core::storage::{ListPage, ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{MemoryBackend, ScopedStorage};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Duration as ChronoDuration, Utc};
use serde::Serialize;

#[derive(Clone, Copy, Serialize)]
pub struct Profile {
    pub setup_commits: usize,
    pub samples: usize,
}

impl Profile {
    pub const fn smoke() -> Self {
        Self {
            setup_commits: 48,
            samples: 8,
        }
    }
    pub const fn benchmark() -> Self {
        Self {
            setup_commits: 256,
            samples: 200,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct BackendCounts {
    pub sha256_helper_calls: u64,
    pub sha256_helper_bytes: u64,
    pub canonical_root_hash_calls: u64,
    pub canonical_root_hash_bytes: u64,
    pub rendered_state_validation_calls: u64,
    pub rendered_state_validation_bytes: u64,
    pub rendered_transaction_validation_calls: u64,
    pub rendered_transaction_validation_bytes: u64,
    pub object_reads: BTreeMap<String, ObjectReadCounts>,
    pub requested_ranges: BTreeMap<String, u64>,
    pub logical_storage_calls: u64,
    pub get_attempts: u64,
    pub range_get_attempts: u64,
    pub head_attempts: u64,
    pub put_attempts: u64,
    pub list_attempts: u64,
    pub list_page_attempts: u64,
    pub delete_attempts: u64,
    pub head_cas_attempts: u64,
    pub precondition_failures: u64,
    pub read_bytes: u64,
    pub write_attempt_bytes: u64,
    pub immutable_write_attempt_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ObjectReadCounts {
    pub full_reads: u64,
    pub ranges: u64,
    pub returned_bytes: u64,
    pub failures: u64,
}

// Authority paths have canonical lowercase extensions.
#[allow(clippy::case_sensitive_file_extension_comparisons)]
fn object_class(path: &str) -> &'static str {
    if path.contains("/manifests/") {
        "manifest"
    } else if path.contains("/indexes/") || path.ends_with(".index.json") {
        "directory"
    } else if path.ends_with(".arrow") {
        "data"
    } else if path.contains("/transactions/") {
        "transaction"
    } else {
        "other"
    }
}

impl BackendCounts {
    pub fn requests(&self) -> u64 {
        self.get_attempts
            + self.range_get_attempts
            + self.head_attempts
            + self.put_attempts
            + self.list_attempts
            + self.list_page_attempts
            + self.delete_attempts
    }

    fn add(&mut self, other: &Self) {
        self.canonical_root_hash_calls += other.canonical_root_hash_calls;
        self.canonical_root_hash_bytes += other.canonical_root_hash_bytes;
        self.rendered_state_validation_calls += other.rendered_state_validation_calls;
        self.rendered_state_validation_bytes += other.rendered_state_validation_bytes;
        self.rendered_transaction_validation_calls += other.rendered_transaction_validation_calls;
        self.rendered_transaction_validation_bytes += other.rendered_transaction_validation_bytes;
        self.sha256_helper_calls += other.sha256_helper_calls;
        self.sha256_helper_bytes += other.sha256_helper_bytes;
        for (class, read) in &other.object_reads {
            let entry = self.object_reads.entry(class.clone()).or_default();
            entry.full_reads += read.full_reads;
            entry.ranges += read.ranges;
            entry.returned_bytes += read.returned_bytes;
            entry.failures += read.failures;
        }
        for (range, count) in &other.requested_ranges {
            *self.requested_ranges.entry(range.clone()).or_default() += count;
        }
        self.logical_storage_calls += other.logical_storage_calls;
        self.get_attempts += other.get_attempts;
        self.range_get_attempts += other.range_get_attempts;
        self.head_attempts += other.head_attempts;
        self.put_attempts += other.put_attempts;
        self.list_attempts += other.list_attempts;
        self.list_page_attempts += other.list_page_attempts;
        self.delete_attempts += other.delete_attempts;
        self.head_cas_attempts += other.head_cas_attempts;
        self.precondition_failures += other.precondition_failures;
        self.read_bytes += other.read_bytes;
        self.write_attempt_bytes += other.write_attempt_bytes;
        self.immutable_write_attempt_bytes += other.immutable_write_attempt_bytes;
    }
}

struct CountingBackend {
    inner: MemoryBackend,
    counts: Mutex<BackendCounts>,
    get_repetitions: usize,
    lose_head_response: AtomicBool,
}

impl CountingBackend {
    fn new(get_repetitions: usize) -> Self {
        Self {
            inner: MemoryBackend::new(),
            counts: Mutex::new(BackendCounts::default()),
            get_repetitions,
            lose_head_response: AtomicBool::new(false),
        }
    }
    fn count(&self, update: impl FnOnce(&mut BackendCounts)) {
        update(&mut self.counts.lock().unwrap());
    }
    fn take(&self) -> BackendCounts {
        let result = std::mem::take(&mut *self.counts.lock().unwrap());
        #[cfg(feature = "test-utils")]
        let result = {
            let mut result = result;
            let (calls, bytes) = ControlMvpStateStore::take_test_authentication_work();
            result.sha256_helper_calls = calls;
            result.sha256_helper_bytes = bytes;
            let [
                root_calls,
                root_bytes,
                state_calls,
                state_bytes,
                tx_calls,
                tx_bytes,
            ] = ControlMvpStateStore::take_test_integrity_work();
            result.canonical_root_hash_calls = root_calls;
            result.canonical_root_hash_bytes = root_bytes;
            result.rendered_state_validation_calls = state_calls;
            result.rendered_state_validation_bytes = state_bytes;
            result.rendered_transaction_validation_calls = tx_calls;
            result.rendered_transaction_validation_bytes = tx_bytes;
            result
        };
        result
    }
}

#[async_trait]
impl StorageBackend for CountingBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.count(|count| count.logical_storage_calls += 1);
        let mut result = Err(arco_core::Error::storage("GET probe has no repetitions"));
        for _ in 0..self.get_repetitions {
            self.count(|count| count.get_attempts += 1);
            result = self.inner.get(path).await;
            self.count(|count| {
                let read = count
                    .object_reads
                    .entry(object_class(path).to_string())
                    .or_default();
                read.full_reads += 1;
                match &result {
                    Ok(bytes) => read.returned_bytes += bytes.len() as u64,
                    Err(_) => read.failures += 1,
                }
            });
            if let Ok(bytes) = &result {
                self.count(|count| count.read_bytes += u64::try_from(bytes.len()).unwrap());
            }
        }
        result
    }
    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.count(|count| {
            count.logical_storage_calls += 1;
            count.range_get_attempts += 1;
        });
        let range_key = format!("{}:{}:{}", object_class(path), range.start, range.end);
        let result = self.inner.get_range(path, range).await;
        self.count(|count| {
            *count.requested_ranges.entry(range_key).or_default() += 1;
            let read = count
                .object_reads
                .entry(object_class(path).to_string())
                .or_default();
            read.ranges += 1;
            match &result {
                Ok(bytes) => read.returned_bytes += bytes.len() as u64,
                Err(_) => read.failures += 1,
            }
        });
        if let Ok(bytes) = &result {
            self.count(|count| count.read_bytes += u64::try_from(bytes.len()).unwrap());
        }
        result
    }
    async fn put(
        &self,
        path: &str,
        bytes: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        let head = path.ends_with("/head/current.json");
        self.count(|count| {
            count.logical_storage_calls += 1;
            count.put_attempts += 1;
            count.write_attempt_bytes += u64::try_from(bytes.len()).unwrap();
            if head {
                count.head_cas_attempts += 1;
            } else if matches!(precondition, WritePrecondition::DoesNotExist) {
                count.immutable_write_attempt_bytes += u64::try_from(bytes.len()).unwrap();
            }
        });
        let result = self.inner.put(path, bytes, precondition).await?;
        if matches!(result, WriteResult::PreconditionFailed { .. }) {
            self.count(|count| count.precondition_failures += 1);
        }
        if head
            && matches!(result, WriteResult::Success { .. })
            && self.lose_head_response.swap(false, Ordering::SeqCst)
        {
            return Err(arco_core::Error::storage(
                "injected lost HEAD publication response",
            ));
        }
        Ok(result)
    }
    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.count(|count| {
            count.logical_storage_calls += 1;
            count.head_attempts += 1;
        });
        self.inner.head(path).await
    }
    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.count(|count| {
            count.logical_storage_calls += 1;
            count.list_attempts += 1;
        });
        self.inner.list(prefix).await
    }
    async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> arco_core::Result<ListPage> {
        self.count(|count| {
            count.logical_storage_calls += 1;
            count.list_page_attempts += 1;
        });
        self.inner.list_page(prefix, start_after, limit).await
    }
    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.count(|count| {
            count.logical_storage_calls += 1;
            count.delete_attempts += 1;
        });
        self.inner.delete(path).await
    }
    async fn signed_url(&self, _path: &str, _expiry: Duration) -> arco_core::Result<String> {
        Err(arco_core::Error::storage(
            "signed URLs are outside the cost workload",
        ))
    }
}

/// Exact ratio; a missing ratio means the workload has no payload denominator.
#[derive(Debug, Default, Serialize)]
pub struct Ratio {
    pub numerator: u64,
    pub denominator: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct Allocations {
    pub count: u64,
    pub bytes: u64,
}

// Measure each future poll on its executing thread. Counting across await
// suspension would also count unrelated executor work. Spawned work is excluded;
// this deterministic MemoryBackend workload executes on the calling thread.
pub async fn measure_allocations<T>(operation: impl Future<Output = T>) -> (T, Allocations) {
    let mut operation = pin!(operation);
    let mut allocations = Allocations::default();
    let value = poll_fn(|context| {
        let mut result = Poll::Pending;
        let info = allocation_counter::measure(|| {
            result = operation.as_mut().poll(context);
        });
        allocations.count += info.count_total;
        allocations.bytes += info.bytes_total;
        result
    })
    .await;
    (value, allocations)
}

#[derive(Debug, Default, Serialize)]
pub struct OperationCost {
    pub phase: String,
    pub samples: u64,
    pub backend: BackendCounts,
    /// Payload bytes requested or returned by the logical state operation.
    pub logical_payload_bytes: u64,
    pub abandoned_immutable_bytes: u64,
    pub discarded_candidates: u64,
    /// The current engine reruns candidates; it has no output-preserving rebase.
    pub reused_candidates: u64,
    pub requests_per_operation: Ratio,
    pub read_amplification: Option<Ratio>,
    pub write_amplification: Option<Ratio>,
    pub allocations: Allocations,
    pub latency_p50_micros: u128,
    pub latency_p99_micros: u128,
    #[serde(skip)]
    durations: Vec<u128>,
}

#[derive(Serialize)]
pub struct CostReport {
    pub format_version: u32,
    pub profile: Profile,
    pub backend_kind: &'static str,
    pub request_accounting: &'static str,
    pub allocation_accounting: &'static str,
    pub cache_accounting: &'static str,
    pub provider_qualification: &'static str,
    pub operations: Vec<OperationCost>,
    pub validated_historical_tokens: usize,
    pub expected_backpressure_observed: bool,
}

#[derive(Default)]
struct Recorder {
    operations: BTreeMap<String, OperationCost>,
}

impl Recorder {
    fn measure<'a, T: 'a>(
        &'a mut self,
        phase: &'a str,
        backend: &'a CountingBackend,
        logical_bytes: usize,
        abandoned: bool,
        operation: impl Future<Output = T> + 'a,
    ) -> impl Future<Output = T> + 'a {
        // Harness-owned future storage is outside the measured operation polls.
        let operation = Box::pin(operation);
        async move {
            backend.take();
            let started = Instant::now();
            let (value, allocations) = measure_allocations(operation).await;
            let elapsed = started.elapsed().as_micros();
            let counts = backend.take();
            let entry = self.operations.entry(phase.to_string()).or_default();
            entry.phase = phase.to_string();
            entry.samples += 1;
            entry.logical_payload_bytes += u64::try_from(logical_bytes).unwrap();
            entry.allocations.count += allocations.count;
            entry.allocations.bytes += allocations.bytes;
            if abandoned {
                entry.abandoned_immutable_bytes += counts.immutable_write_attempt_bytes;
                entry.discarded_candidates += 1;
            }
            entry.backend.add(&counts);
            entry.durations.push(elapsed);
            value
        }
    }
    fn finish(self) -> Vec<OperationCost> {
        self.operations
            .into_values()
            .map(|mut entry| {
                entry.durations.sort_unstable();
                let percentile = |percent: usize| {
                    *entry
                        .durations
                        .get(
                            (entry.durations.len() * percent)
                                .div_ceil(100)
                                .saturating_sub(1),
                        )
                        .expect("recorded sample")
                };
                entry.latency_p50_micros = percentile(50);
                entry.latency_p99_micros = percentile(99);
                entry.requests_per_operation = Ratio {
                    numerator: entry.backend.requests(),
                    denominator: entry.samples,
                };
                if entry.logical_payload_bytes > 0 {
                    entry.read_amplification = Some(Ratio {
                        numerator: entry.backend.read_bytes,
                        denominator: entry.logical_payload_bytes,
                    });
                    entry.write_amplification = Some(Ratio {
                        numerator: entry.backend.write_attempt_bytes,
                        denominator: entry.logical_payload_bytes,
                    });
                }
                entry
            })
            .collect()
    }
}

type Contents = BTreeMap<Vec<u8>, Bytes>;

fn scoped(backend: Arc<CountingBackend>) -> ScopedStorage {
    ScopedStorage::new(backend, "bench-tenant", "bench-workspace").unwrap()
}
fn scope() -> StateScope {
    StateScope::new("bench-tenant", "bench-workspace", "catalog")
}
fn store(backend: Arc<CountingBackend>) -> ControlMvpStateStore {
    ControlMvpStateStore::new(scoped(backend), scope()).unwrap()
}

async fn read_contents(reader: &dyn ArcoStateReader) -> Contents {
    let mut contents = BTreeMap::new();
    let mut continuation = None;
    loop {
        let mut request = ScanRequest::new(b"catalog/").with_limits(5, 64 * 1024, 32);
        if let Some(token) = continuation {
            request = request.with_token(token);
        }
        let page = reader.scan(request).await.unwrap();
        for pair in page.entries() {
            assert!(
                contents
                    .insert(pair.key().to_vec(), pair.value().bytes().clone())
                    .is_none()
            );
        }
        continuation = page.continuation().cloned();
        if continuation.is_none() {
            return contents;
        }
    }
}

async fn maintain(
    recorder: &mut Recorder,
    backend: &CountingBackend,
    worker: &ControlMvpMaintenanceWorker,
    contents: &Contents,
) {
    recorder
        .measure(
            "maintenance",
            backend,
            contents
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum(),
            false,
            async {
                worker
                    .consolidate_pending()
                    .await
                    .unwrap()
                    .expect("maintenance threshold reached");
            },
        )
        .await;
}

async fn verify_history(
    recorder: &mut Recorder,
    backend: &CountingBackend,
    state: &ControlMvpStateStore,
    history: &[(StateToken, Contents)],
) {
    for (token, expected) in history {
        let logical_bytes = expected
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        recorder
            .measure(
                "retained_validation",
                backend,
                logical_bytes,
                false,
                async {
                    let reader = state.read_at(token.clone()).await.unwrap();
                    assert_eq!(
                        read_contents(reader.as_ref()).await,
                        *expected,
                        "retained token contents changed"
                    );
                },
            )
            .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn setup_and_reads(
    profile: Profile,
    recorder: &mut Recorder,
    backend: &CountingBackend,
    state: &ControlMvpStateStore,
    worker: &ControlMvpMaintenanceWorker,
    expected: &mut Contents,
    history: &mut Vec<(StateToken, Contents)>,
    pending: &mut usize,
) {
    for ordinal in 0..profile.setup_commits + profile.samples {
        let key = format!("catalog/table-{:04}", ordinal % 16).into_bytes();
        let value = Bytes::from(format!("revision-{ordinal}"));
        let mut txn = recorder
            .measure(
                "begin",
                backend,
                0,
                false,
                state.begin_control_txn(TxnOptions::default()),
            )
            .await
            .unwrap();
        recorder
            .measure(
                "stage_write",
                backend,
                key.len() + value.len(),
                false,
                txn.put(&key, value.clone()),
            )
            .await
            .unwrap();
        let phase = if ordinal < profile.setup_commits {
            "setup_commit"
        } else {
            "commit"
        };
        let outcome = recorder
            .measure(phase, backend, key.len() + value.len(), false, txn.commit())
            .await
            .unwrap();
        expected.insert(key, value);
        history.push((outcome.state_token().clone(), expected.clone()));
        *pending += 1;
        if *pending == 16 {
            maintain(recorder, backend, worker, &*expected).await;
            *pending = 0;
        }
    }
    for _ in 0..profile.samples {
        for (key, value) in &*expected {
            let actual = recorder
                .measure("point_read", backend, value.len(), false, state.get(key))
                .await
                .unwrap();
            assert_eq!(actual.as_ref(), Some(value));
        }
        let bytes = expected
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        let actual = recorder
            .measure("scan", backend, bytes, false, read_contents(state))
            .await;
        assert_eq!(actual, *expected);
    }
}

#[allow(clippy::too_many_lines)]
pub async fn run(profile: Profile) -> CostReport {
    assert!(profile.setup_commits >= 32 && profile.samples > 0);
    let backend = Arc::new(CountingBackend::new(1));
    let state = store(backend.clone());
    let worker = ControlMvpMaintenanceWorker::new(scoped(backend.clone()), scope()).unwrap();
    let mut recorder = Recorder::default();
    let mut expected = Contents::new();
    let mut history = Vec::new();
    let mut pending = 0;

    setup_and_reads(
        profile,
        &mut recorder,
        &backend,
        &state,
        &worker,
        &mut expected,
        &mut history,
        &mut pending,
    )
    .await;

    // Deterministic contention: both candidates start from the exact same HEAD.
    let key = b"catalog/contended";
    let mut winner = state
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    let mut loser = state
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    winner
        .put(key, Bytes::from_static(b"winner"))
        .await
        .unwrap();
    loser.put(key, Bytes::from_static(b"loser")).await.unwrap();
    recorder
        .measure(
            "contention_winner",
            &backend,
            key.len() + 6,
            false,
            winner.commit(),
        )
        .await
        .unwrap();
    assert!(matches!(
        recorder
            .measure(
                "contention_loser",
                &backend,
                key.len() + 5,
                true,
                loser.commit()
            )
            .await,
        Err(CatalogError::CasFailed { .. })
    ));
    // Rerun the entire command, including its read and recomputed response.
    recorder
        .measure("conflict_retry", &backend, key.len() + 7, false, async {
            let mut txn = state
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            assert_eq!(
                txn.get(key).await.unwrap().unwrap().bytes(),
                &Bytes::from_static(b"winner")
            );
            txn.put(key, Bytes::from_static(b"retried")).await.unwrap();
            txn.commit().await.unwrap();
        })
        .await;
    expected.insert(key.to_vec(), Bytes::from_static(b"retried"));
    pending += 2;
    if pending >= 16 {
        maintain(&mut recorder, &backend, &worker, &expected).await;
        pending = 0;
    }

    let mut txn = state
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    txn.put(b"catalog/reconciled", Bytes::from_static(b"visible once"))
        .await
        .unwrap();
    backend.lose_head_response.store(true, Ordering::SeqCst);
    recorder
        .measure(
            "ambiguous_commit_reconciliation",
            &backend,
            b"catalog/reconciled".len() + b"visible once".len(),
            false,
            txn.commit(),
        )
        .await
        .unwrap();
    expected.insert(
        b"catalog/reconciled".to_vec(),
        Bytes::from_static(b"visible once"),
    );
    pending += 1;
    while pending < 16 {
        let txn = state
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        recorder
            .measure("maintenance_fill_commit", &backend, 0, false, txn.commit())
            .await
            .unwrap();
        pending += 1;
    }
    maintain(&mut recorder, &backend, &worker, &expected).await;
    verify_history(&mut recorder, &backend, &state, &history).await;

    verify_retention_and_recovery(
        &mut recorder,
        &backend,
        &state,
        &worker,
        &expected,
        &history,
    )
    .await;

    verify_backpressure(&mut recorder).await;

    CostReport {
        format_version: 1,
        profile,
        backend_kind: "MemoryBackend",
        request_accounting: "StorageBackend API attempts; no network transport or retries",
        allocation_accounting: "Rust allocator calls during future polls; excludes spawned work",
        cache_accounting: "disabled; no cache hits",
        provider_qualification: "not_run",
        operations: recorder.finish(),
        validated_historical_tokens: history.len(),
        expected_backpressure_observed: true,
    }
}

pub async fn probe_backend_accounting(get_repetitions: usize) -> BackendCounts {
    let backend = CountingBackend::new(get_repetitions);
    backend
        .put(
            "object",
            Bytes::from_static(b"12345678"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    backend
        .put(
            "object",
            Bytes::from_static(b"12345678"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    backend.get("object").await.unwrap();
    assert!(backend.get("missing").await.is_err());
    backend.get_range("object", 2..5).await.unwrap();
    backend.head("object").await.unwrap();
    backend.list("").await.unwrap();
    backend.list_page("", None, 1).await.unwrap();
    backend.delete("object").await.unwrap();
    backend.take()
}

async fn verify_backpressure(recorder: &mut Recorder) {
    // Backpressure is a separate workload with maintenance deliberately absent.
    let pressure_backend = Arc::new(CountingBackend::new(1));
    let pressure = store(pressure_backend.clone());
    for _ in 0..31 {
        pressure
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let before = pressure.current_state_token().await.unwrap();
    let txn = pressure
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    assert!(matches!(
        recorder
            .measure(
                "expected_backpressure",
                &pressure_backend,
                0,
                false,
                txn.commit()
            )
            .await,
        Err(CatalogError::MaintenanceBackpressure { .. })
    ));
    assert_eq!(pressure.current_state_token().await.unwrap(), before);
}

async fn verify_retention_and_recovery(
    recorder: &mut Recorder,
    backend: &Arc<CountingBackend>,
    state: &ControlMvpStateStore,
    worker: &ControlMvpMaintenanceWorker,
    expected: &Contents,
    history: &[(StateToken, Contents)],
) {
    let checkpoint = recorder
        .measure(
            "checkpoint",
            backend,
            0,
            false,
            state.checkpoint(CheckpointOptions::default()),
        )
        .await
        .unwrap();
    let checkpoint_reader = state.read_checkpoint(checkpoint).await.unwrap();
    assert_eq!(read_contents(checkpoint_reader.as_ref()).await, *expected);
    let orphan = state.paths().tx_object("cost-harness-orphan");
    scoped(backend.clone())
        .put_raw(
            &orphan,
            Bytes::from_static(b"orphan"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    recorder
        .measure("gc", backend, 0, false, async {
            let mut cursor = None;
            let mut deleted = 0;
            loop {
                let outcome = worker
                    .collect_gc_page_at(
                        Utc::now() + ChronoDuration::days(8),
                        Vec::new(),
                        cursor.as_deref(),
                    )
                    .await
                    .unwrap();
                deleted += outcome.objects_deleted();
                cursor = outcome.continuation().map(ToOwned::to_owned);
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(deleted, 1);
        })
        .await;
    verify_history(recorder, backend, state, history).await;
    recorder
        .measure("recovery", backend, 0, false, async {
            let reopened = store(backend.clone());
            let txn = reopened
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            drop(txn);
            assert_eq!(read_contents(&reopened).await, *expected);
        })
        .await;
}

#[cfg(feature = "test-utils")]
#[derive(Serialize)]
pub struct ScalingSample {
    pub target_bytes: usize,
    pub rows: usize,
    pub l1_segments: usize,
    pub l0_suffix: usize,
    pub blocks: usize,
    pub encoded_data_bytes: usize,
    pub index_bytes: usize,
    pub pages: usize,
    pub operations: BTreeMap<String, BackendCounts>,
    pub allocations: BTreeMap<String, Allocations>,
}

#[cfg(feature = "test-utils")]
fn scaling_key(ordinal: usize) -> Vec<u8> {
    format!("{:032}", ordinal * 2).into_bytes()
}

#[cfg(feature = "test-utils")]
#[allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::indexing_slicing
)]
async fn scaling_fixture(
    rows: usize,
    segments: usize,
    target: usize,
    suffix: usize,
) -> ScalingSample {
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
            for ordinal in 0..rows {
                tx.put(&scaling_key(ordinal), Bytes::from(vec![42; 1024]))
                    .await
                    .unwrap();
            }
        }
        tx.commit().await.unwrap();
    }
    let worker = ControlMvpMaintenanceWorker::new(storage.clone(), scope)
        .unwrap()
        .with_test_segment_sizing(rows.div_ceil(segments), target)
        .unwrap();
    backend.take();
    let (maintenance, maintenance_allocations) =
        measure_allocations(worker.consolidate_pending()).await;
    maintenance.unwrap().unwrap();
    let maintenance_cost = backend.take();
    for _ in 0..suffix {
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        tx.put(&scaling_key(rows / 2), Bytes::from(vec![43; 1024]))
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let token = store.current_state_token().await.unwrap();
    let paths = arco_catalog::ControlMvpPaths::new("catalog");
    let manifest: serde_json::Value = serde_json::from_slice(
        &storage
            .get_raw(&paths.manifest_object(token.authority_manifest_id()))
            .await
            .unwrap(),
    )
    .unwrap();
    let refs = manifest["payload"]["base_states"].as_array().unwrap();
    assert_eq!(refs.len(), segments);
    let mut sample = ScalingSample {
        target_bytes: target,
        rows,
        l1_segments: segments,
        l0_suffix: suffix,
        blocks: 0,
        encoded_data_bytes: 0,
        index_bytes: 0,
        pages: 0,
        operations: BTreeMap::new(),
        allocations: BTreeMap::new(),
    };
    sample
        .operations
        .insert("maintenance".to_string(), maintenance_cost);
    sample
        .allocations
        .insert("maintenance".to_string(), maintenance_allocations);
    for reference in refs {
        sample.encoded_data_bytes +=
            usize::try_from(reference["segment_size_bytes"].as_u64().unwrap()).unwrap();
        sample.index_bytes +=
            usize::try_from(reference["index_size_bytes"].as_u64().unwrap()).unwrap();
        let index: serde_json::Value = serde_json::from_slice(
            &storage
                .get_raw(&paths.segment_index(reference["state_id"].as_str().unwrap()))
                .await
                .unwrap(),
        )
        .unwrap();
        sample.blocks += index["blocks"].as_array().unwrap().len();
    }
    backend.take();
    let (reader, allocations) = measure_allocations(store.read_at(token)).await;
    let reader = reader.unwrap();
    sample
        .operations
        .insert("reader_open".to_string(), backend.take());
    sample
        .allocations
        .insert("reader_open".to_string(), allocations);
    let (value, allocations) = measure_allocations(reader.get(&scaling_key(rows / 2))).await;
    assert_eq!(value.unwrap().unwrap().len(), 1024);
    let point = backend.take();
    let data = point.object_reads.get("data").unwrap();
    assert_eq!(data.full_reads, 0);
    assert_eq!(data.ranges, (suffix + 1) as u64);
    let directories = point.object_reads.get("directory").unwrap();
    assert_eq!(directories.full_reads, 0);
    assert_eq!(directories.ranges, (suffix + 1) as u64);
    let transactions = point
        .object_reads
        .get("transaction")
        .map_or(0, |read| read.full_reads + read.ranges);
    assert!(transactions + directories.ranges <= (2 * suffix + 1) as u64);
    if suffix == 0 && segments == 1 && sample.blocks >= 16 {
        assert!(data.returned_bytes * sample.blocks as u64 <= 2 * sample.encoded_data_bytes as u64);
    }
    sample.operations.insert("pinned_point".to_string(), point);
    sample
        .allocations
        .insert("pinned_point".to_string(), allocations);
    if suffix == 0 {
        assert!(
            reader
                .get(b"outside-manifest-bounds")
                .await
                .unwrap()
                .is_none()
        );
        let outside = backend.take();
        assert!(
            !outside.object_reads.contains_key("directory")
                && !outside.object_reads.contains_key("data")
        );
        sample
            .operations
            .insert("outside_bounds".to_string(), outside);
        // Fixed odd key between present even keys. Find the first actual Bloom-negative.
        for probe in 0..100 {
            let absent = format!("{:032}", probe * 2 + 1);
            assert!(reader.get(absent.as_bytes()).await.unwrap().is_none());
            let miss = backend.take();
            if !miss.object_reads.contains_key("data") {
                assert_eq!(miss.object_reads["directory"].ranges, 1);
                sample.operations.insert("bloom_negative".to_string(), miss);
                break;
            }
        }
        assert!(sample.operations.contains_key("bloom_negative"));
    }
    let mut continuation = None;
    let mut observed = Vec::new();
    let mut scan_counts = BackendCounts::default();
    loop {
        let mut request = ScanRequest::new(b"").with_limits(113, 256 * 1024, 64);
        if let Some(cursor) = continuation.take() {
            request = request.with_token(cursor);
        }
        let page = reader.scan(request).await.unwrap();
        sample.pages += 1;
        scan_counts.add(&backend.take());
        observed.extend(page.entries().iter().map(|entry| entry.key().to_vec()));
        continuation = page.continuation().cloned();
        if continuation.is_none() {
            break;
        }
        assert!(sample.pages <= rows + 1);
    }
    assert_eq!(observed, (0..rows).map(scaling_key).collect::<Vec<_>>());
    let data = &scan_counts.object_reads["data"];
    assert_eq!(data.full_reads, 0);
    if suffix == 0 {
        assert!(
            data.ranges <= (sample.blocks + sample.pages - 1) as u64,
            "scan reads {} blocks for {} blocks / {} pages",
            data.ranges,
            sample.blocks,
            sample.pages
        );
    }
    sample
        .operations
        .insert("complete_scan".to_string(), scan_counts);
    let (_, allocations) = measure_allocations(store.get(&scaling_key(rows / 2))).await;
    sample
        .operations
        .insert("current_point".to_string(), backend.take());
    sample
        .allocations
        .insert("current_point".to_string(), allocations);
    let (txn, allocations) =
        measure_allocations(store.begin_control_txn(TxnOptions::default())).await;
    Box::new(txn.unwrap()).rollback().await.unwrap();
    sample
        .operations
        .insert("eager_begin".to_string(), backend.take());
    sample
        .allocations
        .insert("eager_begin".to_string(), allocations);
    assert!(
        sample.allocations["pinned_point"].bytes
            <= sample.operations["pinned_point"].read_bytes * 12 + 128 * 1024
    );
    if rows >= 4096 {
        assert!(sample.allocations["pinned_point"].bytes < sample.allocations["eager_begin"].bytes);
    }
    sample
}

#[cfg(feature = "test-utils")]
#[allow(clippy::indexing_slicing)]
pub async fn run_scaling() -> serde_json::Value {
    let mut samples = Vec::new();
    for target in [32, 64, 128, 256] {
        samples.push(scaling_fixture(4096, 1, target * 1024, 0).await);
    }
    let small = &samples[1];
    let large = &samples[3];
    assert!(small.encoded_data_bytes * 100 <= large.encoded_data_bytes * 115);
    let data = |sample: &ScalingSample, name: &str| {
        sample.operations[name].object_reads["data"].returned_bytes
    };
    assert!(data(small, "complete_scan") * 100 <= data(large, "complete_scan") * 115);
    assert!(data(small, "pinned_point") * 100 <= data(large, "pinned_point") * 40);
    assert!(
        small.operations["complete_scan"].object_reads["data"].ranges * 2
            <= large.operations["complete_scan"].object_reads["data"].ranges * 9 + 2
    );
    assert!(small.index_bytes <= large.index_bytes * 3);
    for blocks in [1, 4, 16, 64] {
        let sample = scaling_fixture(blocks * 55, 1, 64 * 1024, 0).await;
        assert_eq!(sample.blocks, blocks);
        samples.push(sample);
    }
    for segments in [1, 4, 16, 64] {
        samples.push(scaling_fixture(4096, segments, 64 * 1024, 0).await);
    }
    for suffix in [0, 1, 8, 16, 31] {
        samples.push(scaling_fixture(256, 1, 64 * 1024, suffix).await);
    }
    let exceptional = Box::pin(exceptional_scaling_costs()).await;
    serde_json::json!({"samples": samples, "exceptional_cases": exceptional, "backend": "MemoryBackend API calls, not provider traffic", "authentication": "thread-local SHA-256 helper input bytes/calls; excludes Bloom probe hashing", "allocation_bound": "pinned point cumulative allocations <= 12 * returned metadata and selected data bytes + 128 KiB; eager begin measured separately"})
}

#[cfg(feature = "test-utils")]
#[allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::indexing_slicing
)]
async fn exceptional_scaling_costs() -> serde_json::Value {
    let mut cases = Vec::new();
    for oversized in [true, false] {
        let backend = Arc::new(CountingBackend::new(1));
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        let mut tx = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        let tx_id = tx.tx_id().to_string();
        if oversized {
            tx.put(&scaling_key(0), Bytes::from(vec![42; 300 * 1024]))
                .await
                .unwrap();
            tx.put(&scaling_key(1), Bytes::from(vec![42; 1024]))
                .await
                .unwrap();
        } else {
            for ordinal in 0..104_859 {
                tx.delete(&scaling_key(ordinal)).await.unwrap();
            }
        }
        backend.take();
        let (token, write_allocations) = measure_allocations(tx.commit()).await;
        let token = token.unwrap();
        let writes = backend.take();
        let index_bytes = storage
            .get_raw(&store.paths().segment_index(&tx_id))
            .await
            .unwrap();
        let index: serde_json::Value = serde_json::from_slice(&index_bytes).unwrap();
        if oversized {
            assert_eq!(index["blocks"][0]["rowCount"], 1);
            assert!(index["blocks"][0]["length"].as_u64().unwrap() > 256 * 1024);
        } else {
            assert_eq!(index["bloomMode"], "Disabled");
            assert_eq!(index["bloomBitsHex"], "");
        }
        let reader = store.read_at(token.into_state_token()).await.unwrap();
        backend.take();
        let (value, allocations) = measure_allocations(reader.get(&scaling_key(0))).await;
        assert_eq!(value.unwrap().is_some(), oversized);
        cases.push(serde_json::json!({"case": if oversized { "oversized_row" } else { "disabled_filter" }, "directory_bytes":index_bytes.len(), "blocks":index["blocks"].as_array().unwrap().len(), "commit":writes, "commit_allocations":write_allocations, "point":backend.take(), "point_allocations":allocations}));
    }
    let backend = Arc::new(CountingBackend::new(1));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
    let store =
        ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
            .unwrap();
    let mut first = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    first
        .stage_projection_intent("projection", "test", Bytes::from_static(b"payload"))
        .unwrap();
    first.commit().await.unwrap();
    for _ in 0..2 {
        store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap()
            .commit()
            .await
            .unwrap();
    }
    let records = store.current_projection_outbox().await.unwrap();
    backend.take();
    let (resolved, allocations) =
        measure_allocations(store.resolve_test_projection_source(&records[0])).await;
    assert_eq!(resolved.unwrap().logical_sequence(), 1);
    let counts = backend.take();
    assert_eq!(counts.object_reads["manifest"].ranges, 3);
    cases.push(serde_json::json!({"case":"projection_source_resolution", "manifests":3, "reads":counts, "allocations":allocations}));
    serde_json::Value::Array(cases)
}

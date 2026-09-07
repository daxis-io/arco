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
        std::mem::take(&mut *self.counts.lock().unwrap())
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
        let result = self.inner.get_range(path, range).await;
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

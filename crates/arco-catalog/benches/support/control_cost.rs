//! Shared deterministic workload and `StorageBackend`-boundary accounting.
//! No provider latency, pricing, or cache evidence is inferred.
// Measurement fixtures deliberately remain on one thread for allocation accounting and fixed inputs.
#![allow(clippy::future_not_send)]
#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::ops::Range;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

#[path = "read_cache_cost.rs"]
#[allow(dead_code)]
pub mod read_cache_cost;

#[path = "durable_maintenance.rs"]
mod durable_maintenance;
#[cfg(feature = "test-utils")]
#[path = "maintenance_cost.rs"]
#[allow(dead_code)] // Shared benchmark module; acceptance tests call these entry points.
mod maintenance_cost;
#[cfg(feature = "test-utils")]
#[allow(unused_imports)] // The benchmark and acceptance test use different entry points.
pub use maintenance_cost::{
    durable_fixture_smoke, durable_plan_capacity_rejects_without_puts, eager_disabled_source,
    eager_fixture_smoke, run_durable_lifecycle, run_durable_matrix, run_durable_schedules,
    run_eager_matrix, run_eager_schedules,
};

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
    pub decoder_scratch_allocation_calls: u64,
    pub decoder_scratch_allocation_bytes: u64,
    pub request_copy_allocation_calls: u64,
    pub request_copy_allocation_bytes: u64,
    pub error_fanout_allocation_calls: u64,
    pub error_fanout_allocation_bytes: u64,
    pub decoded_row_allocation_calls: u64,
    pub decoded_row_allocation_bytes: u64,
    pub json_decode_allocation_calls: u64,
    pub json_decode_allocation_bytes: u64,
    pub cache_copy_allocation_calls: u64,
    pub cache_copy_allocation_bytes: u64,
    pub backend_allocation_calls: u64,
    pub backend_allocation_bytes: u64,
    pub block_decode_calls: u64,
    pub block_decode_bytes: u64,
    pub metadata_decode_calls: u64,
    pub metadata_decode_bytes: u64,

    pub phases: BTreeMap<String, BackendCounts>,
    pub replayed_rows: u64,
    pub maintenance_render_rows: u64,
    pub full_checksum_calls: u64,
    pub full_checksum_bytes: u64,
    pub digest_validation_hash_calls: u64,
    pub digest_validation_hash_bytes: u64,
    pub retention_hash_calls: u64,
    pub retention_hash_bytes: u64,
    pub bloom_hash_calls: u64,
    pub bloom_hash_bytes: u64,
    pub witness_hash_calls: u64,
    pub witness_hash_bytes: u64,
    pub sha256_helper_calls: u64,
    pub sha256_helper_bytes: u64,
    pub canonical_root_hash_calls: u64,
    pub canonical_root_hash_bytes: u64,
    pub rendered_state_validation_calls: u64,
    pub rendered_state_validation_bytes: u64,
    pub rendered_transaction_validation_calls: u64,
    pub rendered_transaction_validation_bytes: u64,
    pub object_reads: BTreeMap<String, ObjectReadCounts>,
    pub object_writes: BTreeMap<String, u64>,
    pub requested_ranges: BTreeMap<String, u64>,
    pub logical_storage_calls: u64,
    pub get_attempts: u64,
    pub range_get_attempts: u64,
    pub head_attempts: u64,
    /// Returned `ObjectMeta` struct plus owned string capacities; not wire bytes.
    pub head_metadata_bytes: u64,
    pub head_missing: u64,
    pub head_failures: u64,
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
        self.decoder_scratch_allocation_calls += other.decoder_scratch_allocation_calls;
        self.decoder_scratch_allocation_bytes += other.decoder_scratch_allocation_bytes;
        self.request_copy_allocation_calls += other.request_copy_allocation_calls;
        self.request_copy_allocation_bytes += other.request_copy_allocation_bytes;
        self.error_fanout_allocation_calls += other.error_fanout_allocation_calls;
        self.error_fanout_allocation_bytes += other.error_fanout_allocation_bytes;
        self.decoded_row_allocation_calls += other.decoded_row_allocation_calls;
        self.decoded_row_allocation_bytes += other.decoded_row_allocation_bytes;
        self.json_decode_allocation_calls += other.json_decode_allocation_calls;
        self.json_decode_allocation_bytes += other.json_decode_allocation_bytes;
        self.cache_copy_allocation_calls += other.cache_copy_allocation_calls;
        self.cache_copy_allocation_bytes += other.cache_copy_allocation_bytes;
        self.backend_allocation_calls += other.backend_allocation_calls;
        self.backend_allocation_bytes += other.backend_allocation_bytes;
        self.block_decode_calls += other.block_decode_calls;
        self.block_decode_bytes += other.block_decode_bytes;
        self.metadata_decode_calls += other.metadata_decode_calls;
        self.metadata_decode_bytes += other.metadata_decode_bytes;

        self.replayed_rows += other.replayed_rows;
        self.maintenance_render_rows += other.maintenance_render_rows;
        self.full_checksum_calls += other.full_checksum_calls;
        self.full_checksum_bytes += other.full_checksum_bytes;
        self.digest_validation_hash_calls += other.digest_validation_hash_calls;
        self.digest_validation_hash_bytes += other.digest_validation_hash_bytes;
        self.retention_hash_calls += other.retention_hash_calls;
        self.retention_hash_bytes += other.retention_hash_bytes;
        self.bloom_hash_calls += other.bloom_hash_calls;
        self.bloom_hash_bytes += other.bloom_hash_bytes;
        self.witness_hash_calls += other.witness_hash_calls;
        self.witness_hash_bytes += other.witness_hash_bytes;
        for (name, phase) in &other.phases {
            self.phases.entry(name.clone()).or_default().add(phase);
        }
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
        for (path, bytes) in &other.object_writes {
            *self.object_writes.entry(path.clone()).or_default() += bytes;
        }
        for (range, count) in &other.requested_ranges {
            *self.requested_ranges.entry(range.clone()).or_default() += count;
        }
        self.logical_storage_calls += other.logical_storage_calls;
        self.get_attempts += other.get_attempts;
        self.range_get_attempts += other.range_get_attempts;
        self.head_attempts += other.head_attempts;
        self.head_metadata_bytes += other.head_metadata_bytes;
        self.head_missing += other.head_missing;
        self.head_failures += other.head_failures;
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
    #[cfg(feature = "test-utils")]
    selected_read_repetitions: AtomicUsize,
    lose_head_response: AtomicBool,
    pause_l1: AtomicBool,
    pause_publication_manifest: AtomicBool,
    l1_entered: tokio::sync::Notify,
    l1_release: tokio::sync::Notify,
    fail_put_countdown: AtomicUsize,
}

impl CountingBackend {
    fn new(get_repetitions: usize) -> Self {
        Self {
            inner: MemoryBackend::new(),
            counts: Mutex::new(BackendCounts::default()),
            get_repetitions,
            #[cfg(feature = "test-utils")]
            selected_read_repetitions: AtomicUsize::new(1),
            lose_head_response: AtomicBool::new(false),
            pause_l1: AtomicBool::new(false),
            pause_publication_manifest: AtomicBool::new(false),
            l1_entered: tokio::sync::Notify::new(),
            l1_release: tokio::sync::Notify::new(),
            fail_put_countdown: AtomicUsize::new(0),
        }
    }
    fn count(&self, update: impl Fn(&mut BackendCounts)) {
        let mut counts = self.counts.lock().unwrap();
        update(&mut counts);
        #[cfg(feature = "test-utils")]
        {
            update(
                counts
                    .phases
                    .entry(ControlMvpStateStore::test_cost_phase().to_string())
                    .or_default(),
            );
        }
    }
    fn take(&self) -> BackendCounts {
        let result = std::mem::take(&mut *self.counts.lock().unwrap());
        #[cfg(feature = "test-utils")]
        let result = {
            let mut result = result;
            let (calls, bytes) = ControlMvpStateStore::take_test_authentication_work();
            for (name, work) in ControlMvpStateStore::take_test_phase_work() {
                result.decoder_scratch_allocation_calls += work[24];
                result.decoder_scratch_allocation_bytes += work[25];
                result.request_copy_allocation_calls += work[26];
                result.request_copy_allocation_bytes += work[27];
                result.error_fanout_allocation_calls += work[28];
                result.error_fanout_allocation_bytes += work[29];
                result.decoded_row_allocation_calls += work[30];
                result.decoded_row_allocation_bytes += work[31];
                result.json_decode_allocation_calls += work[32];
                result.json_decode_allocation_bytes += work[33];
                result.cache_copy_allocation_calls += work[34];
                result.cache_copy_allocation_bytes += work[35];
                result.block_decode_calls += work[20];
                result.block_decode_bytes += work[21];
                result.metadata_decode_calls += work[22];
                result.metadata_decode_bytes += work[23];

                result.full_checksum_bytes += work[12];
                result.digest_validation_hash_calls += work[13];
                result.digest_validation_hash_bytes += work[14];
                result.replayed_rows += work[8];
                result.maintenance_render_rows += work[19];
                result.full_checksum_calls += work[9];
                result.retention_hash_calls += work[17];
                result.retention_hash_bytes += work[18];
                result.bloom_hash_calls += work[15];
                result.bloom_hash_bytes += work[16];
                result.witness_hash_calls += work[10];
                result.witness_hash_bytes += work[11];
                {
                    let phase = result.phases.entry(name.to_string()).or_default();
                    phase.decoder_scratch_allocation_calls = work[24];
                    phase.decoder_scratch_allocation_bytes = work[25];
                    phase.request_copy_allocation_calls = work[26];
                    phase.request_copy_allocation_bytes = work[27];
                    phase.error_fanout_allocation_calls = work[28];
                    phase.error_fanout_allocation_bytes = work[29];
                    phase.decoded_row_allocation_calls = work[30];
                    phase.decoded_row_allocation_bytes = work[31];
                    phase.json_decode_allocation_calls = work[32];
                    phase.json_decode_allocation_bytes = work[33];
                    phase.cache_copy_allocation_calls = work[34];
                    phase.cache_copy_allocation_bytes = work[35];
                    phase.block_decode_calls = work[20];
                    phase.block_decode_bytes = work[21];
                    phase.metadata_decode_calls = work[22];
                    phase.metadata_decode_bytes = work[23];

                    phase.sha256_helper_calls = work[0];
                    phase.sha256_helper_bytes = work[1];
                    phase.canonical_root_hash_calls = work[2];
                    phase.canonical_root_hash_bytes = work[3];
                    phase.rendered_state_validation_calls = work[4];
                    phase.rendered_state_validation_bytes = work[5];
                    phase.rendered_transaction_validation_calls = work[6];
                    phase.rendered_transaction_validation_bytes = work[7];
                    phase.full_checksum_bytes = work[12];
                    phase.digest_validation_hash_calls = work[13];
                    phase.digest_validation_hash_bytes = work[14];
                    phase.replayed_rows = work[8];
                    phase.maintenance_render_rows = work[19];
                    phase.full_checksum_calls = work[9];
                    phase.retention_hash_calls = work[17];
                    phase.retention_hash_bytes = work[18];
                    phase.bloom_hash_calls = work[15];
                    phase.bloom_hash_bytes = work[16];
                    phase.witness_hash_calls = work[10];
                    phase.witness_hash_bytes = work[11];
                }
            }
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
            let (value, allocations) = measure_allocations(self.inner.get(path)).await;
            result = value;
            self.count(|count| {
                count.backend_allocation_calls += allocations.count;
                count.backend_allocation_bytes += allocations.bytes;
            });
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
        self.count(|count| count.logical_storage_calls += 1);
        let repetitions = 1;
        #[cfg(feature = "test-utils")]
        let repetitions =
            if ControlMvpStateStore::test_cost_phase() == "maintenance-selected-data-reads" {
                self.selected_read_repetitions.load(Ordering::SeqCst)
            } else {
                repetitions
            };
        let range_key = format!("{path}:{}:{}", range.start, range.end);
        let mut result = Err(arco_core::Error::storage("range probe has no repetitions"));
        for _ in 0..repetitions {
            self.count(|count| count.range_get_attempts += 1);
            let (value, allocations) =
                measure_allocations(self.inner.get_range(path, range.clone())).await;
            result = value;
            self.count(|count| {
                count.backend_allocation_calls += allocations.count;
                count.backend_allocation_bytes += allocations.bytes;
            });
            self.count(|count| {
                *count.requested_ranges.entry(range_key.clone()).or_default() += 1;
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
        }
        result
    }
    async fn put(
        &self,
        path: &str,
        bytes: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        if (path.contains("/segments/l1/") && self.pause_l1.swap(false, Ordering::SeqCst))
            || (path.contains("/manifests/maintenance-")
                && self
                    .pause_publication_manifest
                    .swap(false, Ordering::SeqCst))
        {
            self.l1_entered.notify_one();
            self.l1_release.notified().await;
        }
        let head = path.ends_with("/head/current.json");
        self.count(|count| {
            count.logical_storage_calls += 1;
            count.put_attempts += 1;
            count.write_attempt_bytes += u64::try_from(bytes.len()).unwrap();
            *count.object_writes.entry(path.to_string()).or_default() += bytes.len() as u64;
            if head {
                count.head_cas_attempts += 1;
            } else if matches!(precondition, WritePrecondition::DoesNotExist) {
                count.immutable_write_attempt_bytes += u64::try_from(bytes.len()).unwrap();
            }
        });
        if self
            .fail_put_countdown
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok_and(|left| left == 1)
        {
            return Err(arco_core::Error::storage(
                "injected pre-application PUT interruption",
            ));
        }
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
        let (result, allocations) = measure_allocations(self.inner.head(path)).await;
        self.count(|count| {
            count.backend_allocation_calls += allocations.count;
            count.backend_allocation_bytes += allocations.bytes;
            match &result {
                Ok(Some(meta)) => {
                    count.head_metadata_bytes += (size_of::<ObjectMeta>()
                        + meta.path.capacity()
                        + meta.version.capacity()
                        + meta.etag.as_ref().map_or(0, String::capacity))
                        as u64;
                }
                Ok(None) => count.head_missing += 1,
                Err(_) => count.head_failures += 1,
            }
        });
        result
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
    pub store_cache: serde_json::Value,
    pub maintenance_cache: serde_json::Value,
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
    ControlMvpStateStore::new(scoped(backend), scope())
        .map(configured_store)
        .unwrap()
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
    worker: &arco_catalog::DurableMaintenanceWorker,
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
                durable_maintenance::consolidate_pending(worker)
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
    worker: &arco_catalog::DurableMaintenanceWorker,
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
    #[cfg(feature = "test-utils")]
    let _fixed_inputs = arco_core::test_inputs::FixedInputs::scoped();
    assert!(profile.setup_commits >= 32 && profile.samples > 0);
    let backend = Arc::new(CountingBackend::new(1));
    let state = store(backend.clone());
    let worker = ControlMvpMaintenanceWorker::new(scoped(backend.clone()), scope()).unwrap();
    let durable_worker = configured_worker(
        arco_catalog::DurableMaintenanceWorker::new(
            scoped(backend.clone()),
            scope(),
            arco_catalog::DurableAuthorityBinding::new([17; 32]),
        )
        .unwrap(),
    );
    let mut recorder = Recorder::default();
    let mut expected = Contents::new();
    let mut history = Vec::new();
    let mut pending = 0;

    setup_and_reads(
        profile,
        &mut recorder,
        &backend,
        &state,
        &durable_worker,
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
        .measure(
            "full_command_retry",
            &backend,
            key.len() + 7,
            false,
            async {
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
            },
        )
        .await;
    expected.insert(key.to_vec(), Bytes::from_static(b"retried"));
    pending += 2;
    if pending >= 16 {
        maintain(&mut recorder, &backend, &durable_worker, &expected).await;
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
    maintain(&mut recorder, &backend, &durable_worker, &expected).await;
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
        store_cache: store_statistics(&state),
        maintenance_cache: worker_statistics(&durable_worker),
        format_version: 1,
        profile,
        backend_kind: "MemoryBackend",
        request_accounting: "StorageBackend API attempts; no network transport or retries",
        allocation_accounting: "Rust allocator calls during future polls; excludes spawned work",
        cache_accounting: "explicit store/maintenance mode; null statistics means disabled",
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
                        input_now() + ChronoDuration::days(8),
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
            assert!(deleted >= 1);
            assert!(
                scoped(backend.clone())
                    .head(&orphan)
                    .await
                    .unwrap()
                    .is_none()
            );
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
    lazy: bool,
) -> ScalingSample {
    scaling_fixture_with_store(rows, segments, target, suffix, lazy)
        .await
        .0
}

#[cfg(feature = "test-utils")]
#[allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::indexing_slicing
)]
async fn scaling_fixture_with_store(
    rows: usize,
    segments: usize,
    target: usize,
    suffix: usize,
    lazy: bool,
) -> (ScalingSample, ControlMvpStateStore, Arc<CountingBackend>) {
    #[cfg(feature = "test-utils")]
    let _fixed_inputs = arco_core::test_inputs::FixedInputs::scoped();
    let backend = Arc::new(CountingBackend::new(1));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
    let scope = StateScope::new("tenant", "workspace", "catalog");
    // Gate 5 construction admits at most 64 source blocks per output unit.
    // Keep this output-layout fixture's source below that bound, so the requested
    // owner count measures output sizing rather than selected-input splitting.
    let store = ControlMvpStateStore::new(storage.clone(), scope.clone())
        .map(configured_store)
        .unwrap()
        .with_test_segment_sizing(
            rows,
            (rows.div_ceil(segments) * 512).clamp(8192, 256 * 1024),
        )
        .unwrap();
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
    let worker = configured_worker(
        arco_catalog::DurableMaintenanceWorker::new(
            storage.clone(),
            scope,
            arco_catalog::DurableAuthorityBinding::new([17; 32]),
        )
        .unwrap()
        .with_test_segment_sizing(rows.div_ceil(segments), target)
        .unwrap(),
    );
    backend.take();
    let (maintenance, maintenance_allocations) =
        measure_allocations(Box::pin(durable_maintenance::consolidate_pending(&worker))).await;
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
        measure_allocations(store.begin_eager_reference(TxnOptions::default())).await;
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
    if lazy {
        Box::pin(lazy_transaction_costs(&store, &backend, &mut sample)).await;
    }
    (sample, store, backend)
}

#[cfg(feature = "test-utils")]
#[allow(clippy::indexing_slicing)]
pub async fn run_scaling() -> serde_json::Value {
    let _fixed_inputs = arco_core::test_inputs::FixedInputs::scoped();
    let mut samples = Vec::new();
    for target in [32, 64, 128, 256] {
        samples.push(Box::pin(scaling_fixture(4096, 1, target * 1024, 0, false)).await);
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
        let sample = Box::pin(scaling_fixture(blocks * 55, 1, 64 * 1024, 0, false)).await;
        assert_eq!(sample.blocks, blocks);
        samples.push(sample);
    }
    for segments in [1, 4, 16, 64] {
        samples.push(Box::pin(scaling_fixture(4096, segments, 64 * 1024, 0, false)).await);
    }
    for suffix in [0, 1, 8, 16, 31] {
        samples.push(Box::pin(scaling_fixture(256, 1, 64 * 1024, suffix, false)).await);
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
        .map(configured_store)
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
            .unwrap()
            .without_read_cache();
    let mut first = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    first
        .stage_projection_intent("projection", "test", Bytes::from_static(b"payload"))
        .await
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

#[cfg(feature = "test-utils")]
#[allow(
    clippy::too_many_lines,
    clippy::indexing_slicing,
    clippy::cognitive_complexity,
    reason = "explicit executable acceptance table"
)]
async fn lazy_transaction_costs(
    store: &ControlMvpStateStore,
    backend: &Arc<CountingBackend>,
    sample: &mut ScalingSample,
) {
    backend.take();
    let (txn, allocations) =
        measure_allocations(store.begin_control_txn(TxnOptions::default())).await;
    let mut txn = txn.unwrap();
    let begin = backend.take();
    assert_eq!(begin.head_attempts, 1);
    assert_eq!(begin.get_attempts + begin.range_get_attempts, 2);
    for class in ["data", "transaction", "directory"] {
        assert!(!begin.object_reads.contains_key(class));
    }
    assert_eq!(begin.replayed_rows, 0);
    assert_eq!(begin.full_checksum_calls, 0);
    assert_eq!(
        begin.rendered_state_validation_calls + begin.rendered_transaction_validation_calls,
        0
    );
    assert!(allocations.bytes <= begin.read_bytes * 12 + 128 * 1024);
    sample.operations.insert("lazy_begin".to_string(), begin);
    sample
        .allocations
        .insert("lazy_begin".to_string(), allocations);
    for (name, key) in [
        ("first_point", scaling_key(sample.rows / 2)),
        ("repeated_point", scaling_key(sample.rows / 2)),
        ("different_point", scaling_key(0)),
    ] {
        let (value, allocations) = measure_allocations(txn.get(&key)).await;
        assert!(value.unwrap().is_some());
        let counts = backend.take();
        if name == "repeated_point" {
            assert_eq!(counts.requests(), 0);
        } else {
            assert!(
                counts.object_reads.get("data").map_or(0, |v| v.ranges)
                    <= (sample.l0_suffix + 1) as u64
            );
            assert_eq!(
                counts.object_reads.get("data").map_or(0, |v| v.full_reads),
                0
            );
            let metadata = ["directory", "transaction"]
                .iter()
                .map(|c| {
                    counts
                        .object_reads
                        .get(*c)
                        .map_or(0, |v| v.full_reads + v.ranges)
                })
                .sum::<u64>();
            assert!(metadata <= (2 * sample.l0_suffix + 1) as u64);
            assert!(allocations.bytes <= counts.read_bytes * 12 + 128 * 1024);
        }
        sample.operations.insert(name.to_string(), counts);
        sample.allocations.insert(name.to_string(), allocations);
    }
    let lazy_bytes =
        sample.operations["lazy_begin"].read_bytes + sample.operations["first_point"].read_bytes;
    let lazy_allocations =
        sample.allocations["lazy_begin"].bytes + sample.allocations["first_point"].bytes;
    if sample.rows == 4096 && sample.l0_suffix == 0 {
        assert!(lazy_bytes * 10 <= sample.operations["eager_begin"].read_bytes);
        assert!(lazy_allocations * 10 <= sample.allocations["eager_begin"].bytes);
    }
    backend.take();
    let ((), allocations) = measure_allocations(async {
        let mut abandoned = store
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        abandoned.get(&scaling_key(sample.rows / 2)).await.unwrap();
        Box::new(abandoned).rollback().await.unwrap();
    })
    .await;
    let total = backend.take();
    if sample.rows == 4096 && sample.l0_suffix == 0 {
        assert!(total.read_bytes * 10 <= sample.operations["eager_begin"].read_bytes);
        assert!(allocations.bytes * 10 <= sample.allocations["eager_begin"].bytes);
    }
    sample
        .operations
        .insert("begin_point_rollback_total".to_string(), total);
    sample
        .allocations
        .insert("begin_point_rollback_total".to_string(), allocations);
    let range = arco_catalog::KeyRange::new(Vec::new(), vec![255]);
    for name in ["witness_capture", "witness_reuse"] {
        let (result, allocations) =
            measure_allocations(txn.read_set(&[], std::slice::from_ref(&range))).await;
        result.unwrap();
        let counts = backend.take();
        if name == "witness_reuse" {
            assert_eq!(counts.requests(), 0);
        }
        sample.operations.insert(name.to_string(), counts);
        sample.allocations.insert(name.to_string(), allocations);
    }
    let ((), allocations) = measure_allocations(async {
        txn.put(&scaling_key(0), Bytes::new()).await.unwrap();
        txn.delete(&scaling_key(1)).await.unwrap();
        txn.put(b"new-overlay-key", Bytes::from_static(b"staged"))
            .await
            .unwrap();
    })
    .await;
    sample
        .operations
        .insert("staging".to_string(), backend.take());
    sample
        .allocations
        .insert("staging".to_string(), allocations);
    let mut cursor = None;
    let mut pages = 0;
    let mut scan = BackendCounts::default();
    let mut keys = Vec::new();
    loop {
        let mut request = ScanRequest::new(b"").with_limits(113, 256 * 1024, 64);
        if let Some(c) = cursor.take() {
            request = request.with_token(c);
        }
        let page = txn.scan(request).await.unwrap();
        pages += 1;
        scan.add(&backend.take());
        keys.extend(page.entries().iter().map(|v| v.key().to_vec()));
        cursor = page.continuation().cloned();
        if cursor.is_none() {
            break;
        }
    }
    let mut expected = (0..sample.rows)
        .filter(|n| *n != 1)
        .map(scaling_key)
        .collect::<Vec<_>>();
    expected.push(b"new-overlay-key".to_vec());
    expected.sort();
    assert_eq!(keys, expected);
    let distinct = sample.blocks + sample.l0_suffix;
    assert_eq!(scan.object_reads["data"].full_reads, 0);
    assert!(
        scan.object_reads["data"].ranges
            <= (distinct + (sample.l0_suffix + 1) * (pages - 1)) as u64
    );
    sample.operations.insert("overlay_scan".to_string(), scan);
    Box::new(txn).rollback().await.unwrap();
    // Duplicate lookup with an absent ID authenticates outbox blocks only.
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    backend.take();
    tx.stage_projection_outbox(arco_catalog::ControlMvpProjectionOutboxRecord::new(
        "missing-outbox-id",
        Bytes::new(),
    ))
    .await
    .unwrap();
    let lookup = backend.take();
    assert_eq!(
        lookup
            .object_reads
            .get("data")
            .map_or(0, |v| v.full_reads + v.ranges),
        0,
        "KV-only fixture must not read KV blocks for outbox lookup"
    );
    assert!(
        lookup
            .object_reads
            .values()
            .map(|v| v.full_reads + v.ranges)
            .sum::<u64>()
            <= (sample.l1_segments + 2 * sample.l0_suffix) as u64
    );
    sample
        .operations
        .insert("outbox_id_lookup".to_string(), lookup);
    Box::new(tx).rollback().await.unwrap();
    // Matched immutable fixtures make both lifecycle attempts successful (or
    // both intentionally hit the retained 32-L0 capacity gate).
    for eager in [true, false] {
        let copy = Arc::new(CountingBackend::new(1));
        let mut objects = backend.inner.list("").await.unwrap();
        objects.sort_by(|a, b| a.path.cmp(&b.path));
        for object in objects {
            copy.inner
                .put(
                    &object.path,
                    backend.inner.get(&object.path).await.unwrap(),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
        }
        let copy_store = ControlMvpStateStore::new(
            ScopedStorage::new(copy.clone(), "tenant", "workspace").unwrap(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .map(configured_store)
        .unwrap();
        copy.take();
        let (tx, allocation) = measure_allocations(async {
            if eager {
                copy_store
                    .begin_eager_reference(TxnOptions::default())
                    .await
            } else {
                copy_store.begin_control_txn(TxnOptions::default()).await
            }
        })
        .await;
        let prefix = if eager {
            "eager_no_read"
        } else {
            "lazy_no_read"
        };
        let mut total = copy.take();
        sample
            .operations
            .insert(format!("{prefix}_begin"), total.clone());
        sample
            .allocations
            .insert(format!("{prefix}_begin"), allocation);
        let (result, allocation) = measure_allocations(tx.unwrap().commit()).await;
        if sample.l0_suffix == 31 {
            assert!(matches!(
                result,
                Err(CatalogError::MaintenanceBackpressure { .. })
            ));
        } else {
            result.unwrap();
        }
        let commit = copy.take();
        total.add(&commit);
        sample.operations.insert(format!("{prefix}_commit"), commit);
        sample.operations.insert(format!("{prefix}_total"), total);
        sample
            .allocations
            .insert(format!("{prefix}_commit"), allocation);
        sample.allocations.insert(
            format!("{prefix}_total"),
            Allocations {
                count: sample.allocations[&format!("{prefix}_begin")].count
                    + sample.allocations[&format!("{prefix}_commit")].count,
                bytes: sample.allocations[&format!("{prefix}_begin")].bytes
                    + sample.allocations[&format!("{prefix}_commit")].bytes,
            },
        );
    }
    let data = |name: &str| {
        sample.operations[name]
            .object_reads
            .get("data")
            .map_or(0, |v| v.returned_bytes)
    };
    assert!(data("lazy_no_read_total") <= data("eager_no_read_total"));
    assert!(sample.operations["lazy_no_read_commit"].full_checksum_calls > 0);
    assert!(sample.operations["lazy_no_read_commit"].replayed_rows > 0);
}

#[cfg(feature = "test-utils")]
pub async fn run_lazy_scaling() -> serde_json::Value {
    let _fixed_inputs = arco_core::test_inputs::FixedInputs::scoped();
    let mut samples = Vec::new();
    for target in [32, 64, 128, 256] {
        samples.push(Box::pin(scaling_fixture(4096, 1, target * 1024, 0, true)).await);
    }
    for blocks in [1, 4, 16, 64] {
        samples.push(Box::pin(scaling_fixture(blocks * 55, 1, 64 * 1024, 0, true)).await);
    }
    for segments in [1, 4, 16, 64] {
        samples.push(Box::pin(scaling_fixture(4096, segments, 64 * 1024, 0, true)).await);
    }
    for suffix in [0, 1, 8, 16, 31] {
        samples.push(Box::pin(scaling_fixture(256, 1, 64 * 1024, suffix, true)).await);
    }
    let exceptional = Box::pin(lazy_exceptional_costs()).await;
    serde_json::json!({"exceptional_cases":exceptional,"samples":samples,"acceptance":"all executable assertions passed","counter_nesting":"phases partition totals; canonical hashes are included in SHA helper work; rendered validation includes replay/checksum work; cumulative allocations are allocator requests, not RSS","reference":"real test-only eager snapshot begin and pre-Gate-4 reads/preconditions; matched independent MemoryBackend fixtures for no-read lifecycle","no_read_suffix_31":"both lifecycles fail capacity before first PUT, as required"})
}

#[cfg(all(test, feature = "test-utils"))]
#[tokio::test]
async fn independent_fixture_copies_have_identical_publication_work() {
    let mut expected = None;
    for _ in 0..4 {
        let sample = scaling_fixture(64, 1, 64 * 1024, 0, true).await;
        let writes = &sample
            .operations
            .get("eager_no_read_commit")
            .unwrap()
            .object_writes;
        if let Some(expected) = &expected {
            assert_eq!(writes, expected);
        } else {
            expected = Some(writes.clone());
        }
    }
}

#[cfg(feature = "test-utils")]
#[allow(clippy::indexing_slicing)]
async fn lazy_exceptional_costs() -> serde_json::Value {
    let backend = Arc::new(CountingBackend::new(1));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap();
    let store = ControlMvpStateStore::new(
        storage.clone(),
        StateScope::new("tenant", "workspace", "catalog"),
    )
    .unwrap()
    .without_read_cache()
    .with_checkpoint_interval(std::num::NonZeroU64::new(1).unwrap())
    .with_test_segment_sizing(8, 8192)
    .unwrap();
    backend.take();
    let (tx, allocation) =
        measure_allocations(store.begin_control_txn(TxnOptions::default())).await;
    let mut tx = tx.unwrap();
    let genesis = backend.take();
    assert_eq!(genesis.head_attempts, 1);
    assert_eq!(genesis.get_attempts + genesis.range_get_attempts, 0);
    assert_eq!(
        genesis.replayed_rows
            + genesis.full_checksum_calls
            + genesis.rendered_state_validation_calls
            + genesis.rendered_transaction_validation_calls,
        0
    );
    let genesis = serde_json::json!({"backend":genesis,"allocations":allocation});
    tx.put(b"only-kv-key", Bytes::from_static(b"kv"))
        .await
        .unwrap();
    for n in 0..32 {
        tx.stage_projection_outbox(arco_catalog::ControlMvpProjectionOutboxRecord::new(
            format!("record-{n:02}"),
            Bytes::from(vec![42; 1024]),
        ))
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap()
        .commit()
        .await
        .unwrap();
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    backend.take();
    let (result, allocation) = measure_allocations(tx.stage_projection_outbox(
        arco_catalog::ControlMvpProjectionOutboxRecord::new("record-16", Bytes::new()),
    ))
    .await;
    assert!(matches!(result, Err(CatalogError::AlreadyExists { .. })));
    let lookup = backend.take();
    assert_eq!(lookup.object_reads["data"].ranges, 1);
    assert_eq!(lookup.object_reads["data"].full_reads, 0);
    // 32 outbox rows plus one KV row are five owners, four are keyless.
    assert!(lookup.object_reads["directory"].ranges <= 6);
    assert!(
        lookup.object_reads["transaction"].ranges + lookup.object_reads["transaction"].full_reads
            <= 1
    );
    serde_json::json!({"genesis":genesis,"keyless_outbox_id":{"backend":lookup,"allocations":allocation,"expected_selected_outbox_blocks":1,"expected_kv_blocks":0}})
}

fn configured_worker(
    worker: arco_catalog::DurableMaintenanceWorker,
) -> arco_catalog::DurableMaintenanceWorker {
    match std::env::var("ARCO_MAINTENANCE_CACHE_MODE").as_deref() {
        Ok("enabled") => worker,
        Ok("pressure") => worker
            .with_read_cache_config(arco_catalog::ControlMvpReadCacheConfig {
                metadata_bytes: 1024 * 1024,
                decoded_bytes: 4 * 1024 * 1024,
            })
            .unwrap(),
        _ => worker.without_read_cache(),
    }
}
fn worker_statistics(worker: &arco_catalog::DurableMaintenanceWorker) -> serde_json::Value {
    worker
        .read_cache()
        .map_or(serde_json::Value::Null, |cache| {
            serde_json::to_value(cache.statistics()).unwrap()
        })
}

fn configured_store(store: ControlMvpStateStore) -> ControlMvpStateStore {
    match std::env::var("ARCO_STATE_READ_CACHE_MODE").as_deref() {
        Ok("enabled") => store,
        Ok("pressure") => store
            .with_read_cache_config(arco_catalog::ControlMvpReadCacheConfig {
                metadata_bytes: 1024 * 1024,
                decoded_bytes: 4 * 1024 * 1024,
            })
            .unwrap(),
        _ => store.without_read_cache(),
    }
}
fn store_statistics(store: &ControlMvpStateStore) -> serde_json::Value {
    store.read_cache().map_or(serde_json::Value::Null, |cache| {
        serde_json::to_value(cache.statistics()).unwrap()
    })
}

fn input_now() -> chrono::DateTime<Utc> {
    #[cfg(feature = "test-utils")]
    {
        arco_core::test_inputs::now()
    }
    #[cfg(not(feature = "test-utils"))]
    {
        Utc::now()
    }
}

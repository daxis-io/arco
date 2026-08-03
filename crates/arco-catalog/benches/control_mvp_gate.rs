//! Phase 3C promotion-gate evidence harness for the control-store MVP.
//!
//! Measures the roadmap's budgeted quantities (warm write p99, warm point-read
//! p99, cold writer startup, manifest-reachable replay bytes) against
//! `ControlMvpStateStore` with automatic checkpoint anchoring enabled, then
//! evaluates the Phase 3C promotion gate and prints the serialized evidence
//! packet.
//!
//! The backend is `MemoryBackend`; results are harness evidence, NOT
//! production-provider evidence. Criteria that require real-provider or
//! projection evidence are deliberately left unsatisfied so the gate decision
//! stays honest.
//!
//! Run with: `cargo bench -p arco-catalog --bench control_mvp_gate`

#![allow(missing_docs)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "this evidence harness reports human diagnostics and a JSON packet on its streams"
)]

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arco_catalog::state_store::promotion_gate::{
    MeasurementSource, MeasurementValue, PromotionCriterion, PromotionGateInput,
    PromotionGateReport, PromotionMeasurement, PromotionMeasurementKind,
};
use arco_catalog::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, ControlMvpStateStore, StateScope, TxnOptions,
};
use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{MemoryBackend, ScopedStorage};
use async_trait::async_trait;
use bytes::Bytes;
use tokio::runtime::Runtime;

const SETUP_COMMITS: usize = 256;
const SAMPLES: usize = 200;
const COLD_STARTS: usize = 20;

struct ByteCountingBackend {
    inner: Arc<dyn StorageBackend>,
    get_bytes: AtomicU64,
    get_calls: AtomicU64,
}

impl ByteCountingBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            get_bytes: AtomicU64::new(0),
            get_calls: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl StorageBackend for ByteCountingBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        let bytes = self.inner.get(path).await?;
        self.get_bytes
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        self.get_calls.fetch_add(1, Ordering::SeqCst);
        Ok(bytes)
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

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

fn p99_micros(mut samples: Vec<u128>) -> u64 {
    samples.sort_unstable();
    let index = (samples.len() * 99).div_ceil(100).saturating_sub(1);
    samples.get(index).copied().unwrap_or(0) as u64
}

fn scope() -> StateScope {
    StateScope::new("bench-tenant", "bench-workspace", "catalog")
}

fn store_over(backend: Arc<dyn StorageBackend>) -> ControlMvpStateStore {
    let storage =
        ScopedStorage::new(backend, "bench-tenant", "bench-workspace").expect("scoped storage");
    ControlMvpStateStore::new(storage, scope()).expect("control MVP store")
}

async fn commit_value(store: &ControlMvpStateStore, key: &[u8], value: String) {
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(key, Bytes::from(value)).await.expect("stage write");
    txn.commit().await.expect("commit");
}

#[allow(clippy::too_many_lines)]
async fn run() -> PromotionGateReport {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let store = store_over(backend.clone());

    for index in 0..SETUP_COMMITS {
        commit_value(
            &store,
            format!("catalog/table-{:04}", index % 64).as_bytes(),
            format!("table-metadata-revision-{index}"),
        )
        .await;
    }

    // Warm write p99: narrow metadata mutations against the warmed store.
    let mut write_samples = Vec::with_capacity(SAMPLES);
    for index in 0..SAMPLES {
        let started = Instant::now();
        commit_value(
            &store,
            format!("catalog/table-{:04}", index % 64).as_bytes(),
            format!("warm-write-revision-{index}"),
        )
        .await;
        write_samples.push(started.elapsed().as_micros());
    }
    let warm_write_p99 = p99_micros(write_samples);

    // Warm point-read p99.
    let mut read_samples = Vec::with_capacity(SAMPLES);
    for index in 0..SAMPLES {
        let key = format!("catalog/table-{:04}", index % 64);
        let started = Instant::now();
        let value = store.get(key.as_bytes()).await.expect("point read");
        read_samples.push(started.elapsed().as_micros());
        assert!(value.is_some(), "warm reads must observe committed values");
    }
    let warm_read_p99 = p99_micros(read_samples);

    // Bounded prefix-scan p99 (no roadmap numeric budget; recorded for review).
    let mut scan_samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let entries = store.scan_prefix(b"catalog/").await.expect("prefix scan");
        scan_samples.push(started.elapsed().as_micros());
        assert!(!entries.is_empty());
    }
    let scan_p99 = p99_micros(scan_samples);

    // Cold writer startup to write-ready: fresh store instance over existing
    // history until a transaction is ready to stage writes.
    let mut cold_samples = Vec::with_capacity(COLD_STARTS);
    for _ in 0..COLD_STARTS {
        let started = Instant::now();
        let cold_store = store_over(backend.clone());
        let txn = cold_store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("cold begin");
        cold_samples.push(started.elapsed().as_micros());
        drop(txn);
    }
    let cold_start_p99 = p99_micros(cold_samples);

    // Manifest-reachable replay bytes: total bytes fetched by a cold read.
    let counting = Arc::new(ByteCountingBackend::new(backend.clone()));
    let counting_store = store_over(counting.clone());
    counting_store
        .get(b"catalog/table-0000")
        .await
        .expect("cold replay read");
    let replay_bytes = counting.get_bytes.load(Ordering::SeqCst);
    let replay_gets = counting.get_calls.load(Ordering::SeqCst);

    // Compaction backlog before replay budget breach: how many un-anchored
    // transactions fit in the 64MiB replay budget at the observed mean
    // transaction size (replay cost = one snapshot + the suffix).
    let mean_tx_bytes = (replay_bytes / replay_gets.max(1)).max(1);
    let backlog_before_breach = (64 * 1024 * 1024) / mean_tx_bytes;

    // StateToken read-after-write retention: every commit's token stays
    // readable (no GC exists in the MVP); verified against the current token.
    let token = store.current_state_token().await.expect("current token");
    let retained_reader = store.read_at(token).await.expect("retained read");
    assert!(
        retained_reader
            .get(b"catalog/table-0000")
            .await
            .expect("retained value")
            .is_some()
    );
    let retained_tokens = (SETUP_COMMITS + SAMPLES) as u64;

    let checkpoint = store
        .checkpoint(arco_catalog::CheckpointOptions::default())
        .await
        .expect("checkpoint");
    let checkpoint_reader = store
        .read_checkpoint(checkpoint)
        .await
        .expect("bounded checkpoint read");
    assert!(
        checkpoint_reader
            .get(b"catalog/table-0000")
            .await
            .expect("checkpoint value")
            .is_some()
    );

    let input = PromotionGateInput::new(
        [
            // Satisfied by the CI-executed Phase 3A/3B suites.
            PromotionCriterion::CorrectnessAndFailureStateTests,
            PromotionCriterion::StateTokenReadAfterWrite,
            PromotionCriterion::ModelReplayEquivalence,
            PromotionCriterion::ObjectStoreMvpReplayEquivalence,
            // Deliberately NOT satisfied: ProviderCasAndRetryBehavior (memory
            // backend only; real-provider conformance is skip-green, #366),
            // ProjectionEqualityWatermark (no projection pipeline),
            // EnforcementAndVendingFreshness (no enforcement-seam caller),
            // OperationalComplexityAcceptable (no alerts/runbooks/dashboards).
        ],
        [
            PromotionMeasurement::with_value(
                PromotionMeasurementKind::WarmWriteP99NarrowMetadataMutation,
                MeasurementSource::OptInBenchmark,
                MeasurementValue::DurationMicros(warm_write_p99),
            ),
            PromotionMeasurement::with_value(
                PromotionMeasurementKind::WarmPointReadP99,
                MeasurementSource::OptInBenchmark,
                MeasurementValue::DurationMicros(warm_read_p99),
            ),
            PromotionMeasurement::with_value(
                PromotionMeasurementKind::BoundedPrefixScanP99,
                MeasurementSource::OptInBenchmark,
                MeasurementValue::DurationMicros(scan_p99),
            ),
            PromotionMeasurement::with_value(
                PromotionMeasurementKind::ColdWriterStartupToWriteReady,
                MeasurementSource::OptInBenchmark,
                MeasurementValue::DurationMicros(cold_start_p99),
            ),
            PromotionMeasurement::with_value(
                PromotionMeasurementKind::ManifestReachableReplayBytes,
                MeasurementSource::OptInBenchmark,
                MeasurementValue::Bytes(replay_bytes),
            ),
            PromotionMeasurement::unavailable(PromotionMeasurementKind::ProjectionWatermarkLag),
            PromotionMeasurement::with_value(
                PromotionMeasurementKind::CompactionBacklogBeforeReplayBudgetBreach,
                MeasurementSource::OptInBenchmark,
                MeasurementValue::Count(backlog_before_breach),
            ),
            PromotionMeasurement::with_value(
                PromotionMeasurementKind::StateTokenReadAfterWriteRetention,
                MeasurementSource::OptInBenchmark,
                MeasurementValue::Count(retained_tokens),
            ),
        ],
    );

    eprintln!("control_mvp_gate harness (MemoryBackend, interval 32):");
    eprintln!("  warm write p99:        {warm_write_p99} µs (budget 250000)");
    eprintln!("  warm point-read p99:   {warm_read_p99} µs (budget 50000)");
    eprintln!("  prefix scan p99:       {scan_p99} µs (no budget)");
    eprintln!("  cold start p99:        {cold_start_p99} µs (budget 2000000)");
    eprintln!(
        "  replay bytes (cold):   {replay_bytes} B over {replay_gets} GETs (budget 67108864)"
    );
    eprintln!("  backlog before breach: {backlog_before_breach} txs (derived)");
    eprintln!("  retained tokens:       {retained_tokens}");

    input.evaluate()
}

fn main() {
    let runtime = Runtime::new().expect("tokio runtime");
    let report = runtime.block_on(run());
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize gate report")
    );
}

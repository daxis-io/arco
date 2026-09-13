//! Explicit oversized-singleton authority-8 evidence lane.
#![cfg(feature = "test-utils")]
#![allow(clippy::expect_used, clippy::panic)]

#[path = "../benches/support/control_cost.rs"]
#[allow(dead_code)]
mod control_cost;

use std::future::Future;
use std::io::Write;
use std::sync::Arc;

use arco_catalog::state_store::SyntheticKvEntry;
use arco_catalog::{
    ArcoStateReader, ArcoStateTxn, ControlMvpReadCacheConfig, ControlMvpStateStore, StateScope,
    TxnOptions,
};
use arco_core::ScopedStorage;
use bytes::Bytes;
use serde::Serialize;

const PAYLOADS: [usize; 3] = [300 * 1024, 8 * 1024 * 1024, 60 * 1024 * 1024];
const SCRATCH_OVERHEAD_BYTES: u64 = 16 * 1024 * 1024;
const SCRATCH_PAYLOAD_MULTIPLIER: u64 = 24;
const SEGMENT_LIMIT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy)]
enum CacheMode {
    Disabled,
    Default,
    Pressure,
}

impl CacheMode {
    const ALL: [Self; 3] = [Self::Disabled, Self::Default, Self::Pressure];

    const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Default => "default",
            Self::Pressure => "pressure",
        }
    }

    fn config(self) -> Option<ControlMvpReadCacheConfig> {
        match self {
            Self::Disabled => None,
            Self::Default => Some(ControlMvpReadCacheConfig::default()),
            Self::Pressure => Some(ControlMvpReadCacheConfig {
                metadata_bytes: 1024 * 1024,
                decoded_bytes: 4 * 1024 * 1024,
            }),
        }
    }
}

#[derive(Serialize)]
struct SingletonEvidence<'a> {
    source_fingerprint: &'a str,
    phase: &'static str,
    command_result: &'a str,
    scratch_ceiling_result: &'static str,
    payload_bytes: usize,
    cache: &'a str,
    request_allocations: &'a control_cost::RequestAllocations,
    backend: &'a control_cost::BackendCounts,
    encoded_singleton_segment_bytes: u64,
    encoded_segment_limit_bytes: u64,
    scratch_upper_bound_bytes: u64,
}

fn payload(bytes: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_u32;
    (0..bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            let [low, ..] = state.to_le_bytes();
            low
        })
        .collect()
}

async fn measure_request<T>(
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
    (result, allocations, backend.take())
}

fn append_evidence(path: &str, evidence: &SingletonEvidence<'_>) {
    let mut output = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open singleton JSONL evidence");
    serde_json::to_writer(&mut output, evidence).expect("encode singleton evidence");
    output
        .write_all(b"\n")
        .expect("terminate singleton evidence");
}

#[tokio::test]
#[ignore = "explicit authority-8 oversized-singleton evidence lane"]
// Keep fixture construction visibly separate from request measurement and retained reads.
#[allow(clippy::too_many_lines)]
async fn oversized_singletons_preserve_retained_payloads_within_declared_scratch_ceiling() {
    let source_fingerprint = std::env::var("ARCO_SOURCE_FINGERPRINT")
        .expect("ARCO_SOURCE_FINGERPRINT records the measured source identity");
    let evidence_path = std::env::var("ARCO_BOUNDED_SINGLETON_JSONL")
        .expect("ARCO_BOUNDED_SINGLETON_JSONL records append-only JSONL evidence");

    for payload_bytes in PAYLOADS {
        for cache in CacheMode::ALL {
            // Fixture generation is deliberately outside the measured request.
            let expected_payload = payload(payload_bytes);
            let backend = Arc::new(control_cost::CountingBackend::new(1));
            let storage = ScopedStorage::new(
                backend.clone(),
                format!("singleton-{payload_bytes}-{}", cache.label()),
                "workspace",
            )
            .expect("storage");
            let scope = StateScope::new(
                format!("singleton-{payload_bytes}-{}", cache.label()),
                "workspace",
                "catalog",
            );
            let store = ControlMvpStateStore::new_synthetic_bounded(storage.clone(), scope)
                .expect("authority-8 store");
            let store = match cache.config() {
                Some(config) => store.with_read_cache_config(config).expect("cache config"),
                None => store,
            };
            let fixture_id = format!("singleton-{}-{}", payload_bytes, cache.label());
            let key = b"singleton/payload".to_vec();

            // Fixture creation is deliberately excluded from the ordinary-mutation
            // measurement. It installs the immutable singleton predecessor only.
            let token = store
                .install_synthetic_genesis(
                    &fixture_id,
                    1,
                    1,
                    [SyntheticKvEntry {
                        key: key.clone(),
                        value: Some(expected_payload.clone()),
                        generation: 1,
                    }],
                    0,
                    std::iter::empty(),
                )
                .await
                .expect("oversized singleton fixture genesis");
            let _fixture_counters = backend.take();

            let segment_prefix = "control/v1/domains/catalog/segments/l1/";
            let segments = storage
                .list_meta(segment_prefix)
                .await
                .expect("list singleton segments")
                .into_iter()
                .filter(|meta| {
                    std::path::Path::new(meta.path.as_str()).extension()
                        == Some(std::ffi::OsStr::new("arrow"))
                })
                .collect::<Vec<_>>();
            assert_eq!(segments.len(), 1, "one singleton segment per fixture");
            let encoded_size = segments[0].size;
            assert!(encoded_size <= SEGMENT_LIMIT_BYTES, "{encoded_size}");
            let _fixture_metadata_counters = backend.take();

            let (outcome, allocations, counters) = measure_request(&backend, async {
                let mut commit = store.begin_control_txn(TxnOptions::default()).await?;
                commit.set_logical_operation_v2(
                    &format!("singleton-commit-{}-{}", payload_bytes, cache.label()),
                    "catalog",
                    &"a5".repeat(32),
                )?;
                // This owned clone is inside the request. Replacing the same key
                // forces the bounded path to decode and locally rewrite its singleton.
                commit
                    .put(&key, Bytes::from(expected_payload.clone()))
                    .await?;
                commit.commit_v2().await
            })
            .await;
            let result = match &outcome {
                Ok(_) => "ok".to_owned(),
                Err(error) => format!("error:{error}"),
            };
            let scratch_upper_bound = SCRATCH_OVERHEAD_BYTES
                .saturating_add(SCRATCH_PAYLOAD_MULTIPLIER.saturating_mul(payload_bytes as u64));
            let scratch_ceiling_result = if allocations.peak_owned_bytes <= scratch_upper_bound {
                "passed"
            } else {
                "failed_scratch_upper_bound"
            };
            append_evidence(
                &evidence_path,
                &SingletonEvidence {
                    source_fingerprint: &source_fingerprint,
                    phase: "ordinary_singleton_rewrite",
                    command_result: &result,
                    scratch_ceiling_result,
                    payload_bytes,
                    cache: cache.label(),
                    request_allocations: &allocations,
                    backend: &counters,
                    encoded_singleton_segment_bytes: encoded_size,
                    encoded_segment_limit_bytes: SEGMENT_LIMIT_BYTES,
                    scratch_upper_bound_bytes: scratch_upper_bound,
                },
            );
            outcome.expect("ordinary oversized singleton rewrite");
            assert!(
                allocations.peak_owned_bytes <= scratch_upper_bound,
                "{} > {scratch_upper_bound}",
                allocations.peak_owned_bytes
            );
            let retained = store
                .read_at(token)
                .await
                .expect("retained singleton reader");
            let observed = retained
                .get(&key)
                .await
                .expect("retained singleton read")
                .expect("retained singleton value");
            assert_eq!(observed.as_ref(), expected_payload.as_slice());
            assert!(
                counters.selected_blocks > 0,
                "selected singleton block missing"
            );
            assert!(
                counters.rewritten_blocks > 0,
                "rewritten singleton block missing"
            );
            assert!(
                counters.transition_proof_rows > 0,
                "singleton transition proof rows missing"
            );
        }
    }
}

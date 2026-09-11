#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
use super::*;
use crate::{ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, StateToken, TxnOptions};
use arco_core::storage::StorageBackend;
use arco_core::{MemoryBackend, ScopedStorage};

fn store() -> ControlMvpStateStore {
    ControlMvpStateStore::new(
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap(),
        StateScope::new("tenant", "workspace", "catalog"),
    )
    .unwrap()
}

#[test]
fn handles_retain_backend_and_reject_other_scopes() {
    let original = store();
    let handle = original.read_cache().unwrap();
    assert!(original.clone().with_read_cache(handle.clone()).is_ok());
    assert!(matches!(
        store().with_read_cache(handle.clone()),
        Err(CatalogError::Validation { .. })
    ));
    let other = ControlMvpStateStore::new(
        original.retention.clone(),
        StateScope::new("tenant", "workspace", "other"),
    )
    .unwrap();
    assert!(other.with_read_cache(handle).is_err());
    assert!(original.without_read_cache().read_cache().is_none());
}

#[test]
fn reservations_and_evicted_leases_remain_bounded() {
    let cache = store().read_cache().unwrap();
    let request = Request::test("one", Pool::Decoded, 8192);
    let reservation = cache.reserve(&request).unwrap();
    assert_eq!(cache.statistics().decoded.reserved_bytes, 8192);
    let lease = reservation
        .finish(request.key, Value::Block(Vec::new()), true)
        .unwrap();
    cache.insert(lease.clone());
    assert!(cache.statistics().decoded.resident_bytes >= 4096);
    cache.evict(Pool::Decoded);
    assert_eq!(cache.statistics().decoded.resident_bytes, 0);
    assert!(cache.statistics().decoded.live_evicted_bytes >= 4096);
    drop(lease);
    let stats = cache.statistics();
    assert_eq!(stats.decoded.live_evicted_bytes, 0);
    assert_eq!(stats.decoded.reserved_bytes, 0);
    assert_eq!(stats.decoded.live_records, 0);
}

#[test]
fn completed_insert_declines_are_counted_and_leases_stay_charged() {
    for collision in [true, false] {
        let cache = store().read_cache().unwrap();
        let make_entry = || {
            let request = Request::test("insert-decline", Pool::Decoded, 8192);
            cache
                .reserve(&request)
                .unwrap()
                .finish(request.key, Value::Block(Vec::new()), true)
                .unwrap()
        };
        let resident = collision.then(|| cache.insert(make_entry()));
        let completed = make_entry();
        if !collision {
            lock(&cache.0.directory).generation = u64::MAX;
        }
        let selected = cache.insert(completed.clone());
        assert_eq!(cache.statistics().declined, 1);
        assert_eq!(cache.statistics().fallbacks, 1);
        assert_eq!(
            cache.statistics().decoded.live_evicted_bytes,
            completed.charge
        );
        assert!(!completed.resident.load(Ordering::Relaxed));
        if let Some(resident) = resident {
            assert!(Arc::ptr_eq(&selected, &resident));
        } else {
            assert!(Arc::ptr_eq(&selected, &completed));
        }
        drop(selected);
        drop(completed);
        assert_eq!(cache.statistics().decoded.live_evicted_bytes, 0);
        assert_eq!(cache.statistics().decoded.reserved_bytes, 0);
    }
}

#[tokio::test]
async fn same_key_participants_cancel_without_poisoning_or_leaking() {
    use futures::{future::join_all, poll};
    use std::task::Poll;
    let cache = store().read_cache().unwrap();
    let release = Arc::new(tokio::sync::Notify::new());
    let mut callers = Vec::new();
    for _ in 0..32 {
        let request = Request::test("shared", Pool::Decoded, 8192);
        let release = release.clone();
        callers.push(Box::pin(cache.load(request, move || async move {
            release.notified().await;
            Ok((Value::Block(Vec::new()), true))
        })));
    }
    for caller in &mut callers {
        assert!(matches!(poll!(caller.as_mut()), Poll::Pending));
    }
    assert_eq!(cache.statistics().loads, 1);
    assert_eq!(cache.statistics().participants, 32);
    assert!(
        cache
            .load(Request::test("shared", Pool::Decoded, 8192), || async {
                panic!("the thirty-third participant must use direct fallback")
            })
            .await
            .unwrap()
            .is_none()
    );
    drop(callers.remove(0));
    assert_eq!(cache.statistics().participants, 31);
    release.notify_one();
    for result in join_all(callers).await {
        assert!(result.unwrap().is_some());
    }
    assert_eq!(cache.statistics().participants, 0);
    assert_eq!(cache.statistics().active_loads, 0);
    assert_eq!(cache.statistics().decoded.reserved_bytes, 0);
    let mut abandoned = Box::pin(cache.load(
        Request::test("abandoned", Pool::Decoded, 8192),
        || async {
            std::future::pending::<()>().await;
            Ok((Value::Block(Vec::new()), true))
        },
    ));
    assert!(matches!(poll!(abandoned.as_mut()), Poll::Pending));
    drop(abandoned);
    assert_eq!(cache.statistics().active_loads, 0);
    assert_eq!(cache.statistics().decoded.reserved_bytes, 0);
    let unpolled = cache.load(
        Request::test("never-polled", Pool::Decoded, 8192),
        || async { Ok((Value::Block(Vec::new()), true)) },
    );
    drop(unpolled);
    assert_eq!(cache.statistics().active_loads, 0);
    assert!(lock(&cache.0.directory).flights.iter().all(Option::is_none));
}

#[tokio::test]
async fn failures_preserve_variants_and_allow_retry() {
    let cache = store().read_cache().unwrap();
    let result = cache
        .load(Request::test("failed", Pool::Decoded, 8192), || async {
            Err(CatalogError::RequestFailed {
                http_status: 418,
                message: "exact failure".into(),
            })
        })
        .await;
    assert!(
        matches!(result, Err(CatalogError::RequestFailed { http_status: 418, message }) if message == "exact failure")
    );
    assert_eq!(cache.statistics().active_loads, 0);
    assert_eq!(cache.statistics().decoded.reserved_bytes, 0);
    assert!(
        cache
            .load(Request::test("failed", Pool::Decoded, 8192), || async {
                Ok((Value::Block(Vec::new()), true))
            })
            .await
            .unwrap()
            .is_some()
    );
}

async fn seed(store: &ControlMvpStateStore) -> (String, StateToken) {
    let mut tx = store
        .begin_control_txn(TxnOptions::default())
        .await
        .unwrap();
    let id = tx.tx_id().to_string();
    for n in 0..64_u32 {
        tx.put(&n.to_be_bytes(), Bytes::from(vec![42; 1024]))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    (id, store.current_state_token().await.unwrap())
}

#[tokio::test]
async fn warm_readers_do_not_revive_reclaimed_or_replaced_objects() {
    for object in ["segment", "directory", "transaction"] {
        for replace in [false, true] {
            let store = store();
            let (id, token) = seed(&store).await;
            let reader = store.read_at(token).await.unwrap();
            assert!(reader.get(&0_u32.to_be_bytes()).await.unwrap().is_some());
            assert!(reader.get(&0_u32.to_be_bytes()).await.unwrap().is_some());
            let mut tx = store
                .begin_control_txn(TxnOptions::default())
                .await
                .unwrap();
            let path = match object {
                "segment" => store.paths.l0_segment_object(&id),
                "directory" => store.paths.segment_index(&id),
                _ => store.paths.tx_object(&id),
            };
            if replace {
                store
                    .retention
                    .put_raw(
                        &path,
                        Bytes::from_static(b"corrupt"),
                        arco_core::storage::WritePrecondition::None,
                    )
                    .await
                    .unwrap();
            } else {
                store.retention.delete(&path).await.unwrap();
            }
            assert!(
                reader.get(&0_u32.to_be_bytes()).await.is_err(),
                "{object} replace={replace}"
            );
            assert!(tx.get(&0_u32.to_be_bytes()).await.is_err());
            assert!(tx.commit().await.is_err());
        }
    }
}

#[tokio::test]
async fn selective_blocks_cannot_mint_complete_proof() {
    let store = store();
    let (id, _) = seed(&store).await;
    let cache = store.read_cache().unwrap();
    let before = cache.statistics();
    store.get(&0_u32.to_be_bytes()).await.unwrap();
    assert!(cache.statistics().loads > before.loads);
    assert!(
        !lock(&cache.0.directory)
            .entries
            .values()
            .any(|e| matches!(e.value, Value::Certificate(_)))
    );
    let pointer = store.load_pointer().await.unwrap();
    let manifest = store.load_manifest_for_pointer(&pointer).await.unwrap();
    let tx = store.load_tx_metadata(&manifest.tx_refs[0]).await.unwrap();
    assert_eq!(tx.tx_id, id);
    assert_eq!(
        store
            .load_l0_segment_rows(&tx.l0_segment)
            .await
            .unwrap()
            .len(),
        64
    );
    assert!(
        lock(&cache.0.directory)
            .entries
            .values()
            .any(|e| matches!(e.value, Value::Certificate(_)))
    );
    let loaded = cache.statistics().loads;
    store.load_l0_segment_rows(&tx.l0_segment).await.unwrap();
    assert_eq!(cache.statistics().loads, loaded);
    cache.evict(Pool::Decoded);
    store.load_l0_segment_rows(&tx.l0_segment).await.unwrap();
    assert!(cache.statistics().loads > loaded);
}

#[tokio::test]
async fn oversized_completion_is_charged_until_last_lease() {
    let cache = store().read_cache().unwrap();
    let entry = cache
        .load(
            Request::test("oversized", Pool::Decoded, 12 * MIB),
            || async {
                Ok((
                    Value::Block(vec![ControlMvpSegmentRow {
                        record_kind: SEGMENT_RECORD_KV,
                        key: vec![0; 9 * MIB],
                        value: None,
                        generation: 1,
                        tombstone: true,
                        logical_sequence: 1,
                        logical_ordinal: 0,
                        origin_sequence: None,
                    }]),
                    true,
                ))
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cache.statistics().decoded.resident_bytes, 0);
    assert!(cache.statistics().decoded.live_evicted_bytes > 9 * MIB);
    assert_eq!(cache.statistics().declined, 1);
    drop(entry);
    assert_eq!(cache.statistics().decoded.live_evicted_bytes, 0);
}

#[tokio::test]
async fn eight_distinct_loads_and_256_participants_are_hard_limits() {
    use futures::poll;
    let cache = store().read_cache().unwrap();
    let mut participants = Vec::new();
    for key in 0..8 {
        for _ in 0..32 {
            let request = Request::test(&format!("key-{key}"), Pool::Decoded, 8192);
            let mut participant = Box::pin(cache.load(request, || async {
                std::future::pending::<()>().await;
                Ok((Value::Block(Vec::new()), true))
            }));
            assert!(poll!(participant.as_mut()).is_pending());
            participants.push(participant);
        }
    }
    let stats = cache.statistics();
    assert_eq!(stats.active_loads, 8);
    assert_eq!(stats.participants, 256);
    for key in ["ninth", "key-0"] {
        assert!(
            cache
                .load(Request::test(key, Pool::Decoded, 8192), || async {
                    panic!("fallback must not start cache-owned loader")
                })
                .await
                .unwrap()
                .is_none()
        );
    }
    drop(participants);
    assert_eq!(cache.statistics().participants, 0);
    assert_eq!(cache.statistics().active_loads, 0);
    assert_eq!(cache.statistics().decoded.reserved_bytes, 0);
}

#[derive(Default)]
struct VersionBackend {
    inner: MemoryBackend,
    mode: AtomicUsize,
    heads: AtomicUsize,
    directory_reads: AtomicUsize,
    directory_gets: AtomicUsize,
    directory_bytes: AtomicUsize,
    head_bytes: AtomicUsize,
    backend_allocation_bytes: AtomicUsize,
    backend_allocation_count: AtomicUsize,
    live_buffers: Arc<AtomicUsize>,
    pause: AtomicBool,
    release: tokio::sync::Notify,
}
impl VersionBackend {
    async fn measured_backend<T>(&self, operation: impl Future<Output = T>) -> T {
        let mut operation = std::pin::pin!(operation);
        std::future::poll_fn(|context| {
            let mut result = std::task::Poll::Pending;
            let allocations =
                allocation_counter::measure(|| result = operation.as_mut().poll(context));
            self.backend_allocation_bytes.fetch_add(
                usize::try_from(allocations.bytes_total).unwrap(),
                Ordering::Relaxed,
            );
            self.backend_allocation_count.fetch_add(
                usize::try_from(allocations.count_total).unwrap(),
                Ordering::Relaxed,
            );
            result
        })
        .await
    }
}

#[async_trait::async_trait]
impl StorageBackend for VersionBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        if path.contains("/indexes/") {
            self.directory_gets.fetch_add(1, Ordering::Relaxed);
        }
        self.measured_backend(self.inner.get(path))
            .await
            .map(|bytes| {
                if path.contains("/indexes/") {
                    self.directory_bytes
                        .fetch_add(bytes.len(), Ordering::Relaxed);
                }
                self.owned_backend_slice(bytes)
            })
    }
    async fn get_range(&self, path: &str, range: std::ops::Range<u64>) -> arco_core::Result<Bytes> {
        if path.contains("/indexes/") {
            self.directory_reads.fetch_add(1, Ordering::Relaxed);
        }
        if self.pause.load(Ordering::Relaxed)
            && (path.contains("/indexes/") || self.mode.load(Ordering::Relaxed) == 9)
        {
            self.release.notified().await;
        }
        self.measured_backend(self.inner.get_range(path, range))
            .await
            .map(|bytes| {
                if path.contains("/indexes/") {
                    self.directory_bytes
                        .fetch_add(bytes.len(), Ordering::Relaxed);
                }
                self.owned_backend_slice(bytes)
            })
    }
    async fn head(&self, path: &str) -> arco_core::Result<Option<arco_core::storage::ObjectMeta>> {
        let mut meta = self.measured_backend(self.inner.head(path)).await?;
        if path.contains("/indexes/") {
            let observation = self.heads.fetch_add(1, Ordering::Relaxed);
            let mode = self.mode.load(Ordering::Relaxed);
            if mode == 2 || (mode == 3 && observation % 2 == 1) {
                return Err(arco_core::Error::storage("injected directory HEAD failure"));
            }
            if let Some(meta) = &mut meta {
                match mode {
                    1 => meta.version.clear(),
                    4 if observation % 2 == 1 => meta.version.push_str("changed"),
                    5 => meta.size += 1,
                    7 => meta.version.reserve(16 * MIB),
                    8 => meta.version = "v".repeat(400 * 1024),
                    _ => (),
                }
                self.head_bytes.fetch_add(
                    size_of::<arco_core::storage::ObjectMeta>()
                        + meta.path.capacity()
                        + meta.version.capacity()
                        + meta.etag.as_ref().map_or(0, String::capacity),
                    Ordering::Relaxed,
                );
            }
        }
        Ok(meta)
    }
    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: arco_core::storage::WritePrecondition,
    ) -> arco_core::Result<arco_core::storage::WriteResult> {
        self.inner.put(path, data, precondition).await
    }
    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }
    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<arco_core::storage::ObjectMeta>> {
        self.inner.list(prefix).await
    }
    async fn list_page(
        &self,
        prefix: &str,
        start: Option<&str>,
        limit: usize,
    ) -> arco_core::Result<arco_core::storage::ListPage> {
        self.inner.list_page(prefix, start, limit).await
    }
    async fn signed_url(
        &self,
        path: &str,
        expiry: std::time::Duration,
    ) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

fn version_store() -> (Arc<VersionBackend>, ControlMvpStateStore) {
    let backend = Arc::new(VersionBackend::default());
    let store = ControlMvpStateStore::new(
        ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap(),
        StateScope::new("tenant", "workspace", "catalog"),
    )
    .unwrap();
    (backend, store)
}

#[tokio::test]
async fn version_captures_cannot_bypass_metadata_capacity() {
    use futures::poll;
    let (backend, store) = version_store();
    let (id, token) = seed(&store).await;
    let store = store
        .with_read_cache_config(ControlMvpReadCacheConfig {
            metadata_bytes: MIB,
            decoded_bytes: 4 * MIB,
        })
        .unwrap();
    let cache = store.read_cache().unwrap();
    let path = store.paths.segment_index(&id);
    let directory = store.storage.head(&path).await.unwrap().unwrap();
    backend.mode.store(7, Ordering::Relaxed);
    let version = store
        .cache_version(&cache, &path, directory.size)
        .await
        .unwrap();
    assert_eq!(version.capacity(), version.len());

    backend.mode.store(8, Ordering::Relaxed);
    let reader = store.read_at(token).await.unwrap();
    backend.pause.store(true, Ordering::Relaxed);
    let key = 0_u32.to_be_bytes();
    let mut read = Box::pin(reader.get(&key));
    assert!(poll!(read.as_mut()).is_pending());
    let stats = cache.statistics();
    assert_eq!(stats.active_loads, 0);
    assert_eq!(stats.metadata.reserved_bytes, 0);
    assert!(stats.fallbacks > 0);
    backend.release.notify_one();
    assert!(read.await.unwrap().is_some());
}

#[tokio::test]
async fn unusable_or_changed_head_observations_return_valid_data_without_admission() {
    for mode in 1..=5 {
        let (backend, store) = version_store();
        let (_, token) = seed(&store).await;
        backend.mode.store(mode, Ordering::Relaxed);
        backend.heads.store(0, Ordering::Relaxed);
        let reader = store.read_at(token).await.unwrap();
        for _ in 0..2 {
            assert!(reader.get(&0_u32.to_be_bytes()).await.unwrap().is_some());
        }
        let cache = store.read_cache().unwrap();
        assert!(
            !lock(&cache.0.directory)
                .entries
                .values()
                .any(|e| matches!(e.value, Value::Directory(..)))
        );
        assert!(cache.statistics().fallbacks >= 2);
        assert_eq!(cache.statistics().metadata.reserved_bytes, 0);
    }
}

#[tokio::test]
async fn actual_directory_misses_coalesce_but_head_observations_do_not() {
    use futures::poll;
    let (backend, store) = version_store();
    let (_, token) = seed(&store).await;
    let reader = store.read_at(token).await.unwrap();
    backend.pause.store(true, Ordering::Relaxed);
    backend.heads.store(0, Ordering::Relaxed);
    backend.directory_reads.store(0, Ordering::Relaxed);
    let key = 0_u32.to_be_bytes();
    let mut callers = Box::pin(futures::future::join_all((0..32).map(|_| reader.get(&key))));
    assert!(poll!(callers.as_mut()).is_pending());
    assert_eq!(backend.heads.load(Ordering::Relaxed), 32);
    assert_eq!(backend.directory_reads.load(Ordering::Relaxed), 1);
    assert_eq!(store.read_cache().unwrap().statistics().coalesced, 31);
    backend.release.notify_one();
    for value in callers.await {
        assert!(value.unwrap().is_some());
    }
    assert_eq!(backend.heads.load(Ordering::Relaxed), 33);
    assert_eq!(backend.directory_reads.load(Ordering::Relaxed), 1);
    assert_eq!(store.read_cache().unwrap().statistics().participants, 0);
}

#[tokio::test]
async fn durable_namespaces_and_reopened_handles_are_independent() {
    let store = store();
    seed(&store).await;
    let cache = store.read_cache().unwrap();
    let mut first = store.clone();
    first.cache_namespace = Some(super::super::DurableAuthorityBinding::new([7; 32]));
    first.get(&0_u32.to_be_bytes()).await.unwrap();
    let loaded = cache.statistics().loads;
    let mut second = store.clone();
    second.cache_namespace = Some(super::super::DurableAuthorityBinding::new([8; 32]));
    second.get(&0_u32.to_be_bytes()).await.unwrap();
    assert!(cache.statistics().loads > loaded);
    let reopened = ControlMvpStateStore::new(store.retention.clone(), store.scope.clone()).unwrap();
    assert_eq!(reopened.read_cache().unwrap().statistics().loads, 0);
    let shared = reopened.with_read_cache(cache).unwrap();
    assert!(shared.read_cache().unwrap().statistics().loads > 0);
}

#[test]
fn configuration_rejects_unfunded_administration_and_zero_disables() {
    let original = store();
    assert!(
        original
            .clone()
            .with_read_cache_config(ControlMvpReadCacheConfig {
                metadata_bytes: 1,
                decoded_bytes: MIB,
            })
            .is_err()
    );
    for (metadata_bytes, decoded_bytes) in [(0, MIB), (MIB, 0)] {
        assert!(
            original
                .clone()
                .with_read_cache_config(ControlMvpReadCacheConfig {
                    metadata_bytes,
                    decoded_bytes,
                })
                .unwrap()
                .read_cache()
                .is_none()
        );
    }
    let small = original
        .with_read_cache_config(ControlMvpReadCacheConfig {
            metadata_bytes: 16 * 1024,
            decoded_bytes: 1,
        })
        .unwrap();
    let cache = small.read_cache().unwrap();
    assert!(
        cache
            .reserve(&Request::test("too-large", Pool::Decoded, 8192))
            .is_none()
    );
    assert!(cache.statistics().decoded.high_water_bytes <= 1);
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // All current typed variants share the same 32-waiter failure schedule.
async fn failed_shared_load_fans_out_all_fields_and_retries() {
    use futures::{future::join_all, poll};
    use std::task::Poll;
    let cases: [fn() -> CatalogError; 15] = [
        || CatalogError::Storage {
            message: "Storage-message".into(),
        },
        || CatalogError::Serialization {
            message: "Serialization-message".into(),
        },
        || CatalogError::Parquet {
            message: "Parquet-message".into(),
        },
        || CatalogError::Validation {
            message: "Validation-message".into(),
        },
        || CatalogError::PreconditionFailed {
            message: "PreconditionFailed-message".into(),
        },
        || CatalogError::CasFailed {
            message: "CasFailed-message".into(),
        },
        || CatalogError::StaleWriterEpoch {
            message: "StaleWriterEpoch-message".into(),
        },
        || CatalogError::AmbiguousAuthorityOutcome {
            message: "AmbiguousAuthorityOutcome-message".into(),
        },
        || CatalogError::MaintenanceBackpressure {
            message: "MaintenanceBackpressure-message".into(),
        },
        || CatalogError::UnsupportedAuthorityFormat {
            message: "UnsupportedAuthorityFormat-message".into(),
        },
        || CatalogError::InvariantViolation {
            message: "InvariantViolation-message".into(),
        },
        || CatalogError::UnsupportedOperation {
            message: "UnsupportedOperation-message".into(),
        },
        || CatalogError::AlreadyExists {
            entity: "existing-entity".into(),
            name: "existing-name".into(),
        },
        || CatalogError::NotFound {
            entity: "missing-entity".into(),
            name: "missing-name".into(),
        },
        || CatalogError::RequestFailed {
            http_status: 429,
            message: "request-message".into(),
        },
    ];
    for make_error in cases {
        let expected = make_error();
        let expected_variant = std::mem::discriminant(&expected);
        let expected_fields = format!("{expected:?}");
        let cache = store().read_cache().unwrap();
        let release = Arc::new(tokio::sync::Notify::new());
        let mut callers = Vec::new();
        for _ in 0..32 {
            let release = release.clone();
            callers.push(Box::pin(cache.load(
                Request::test("failed-burst", Pool::Metadata, 8192),
                move || async move {
                    release.notified().await;
                    Err(make_error())
                },
            )));
        }
        for caller in &mut callers {
            assert!(matches!(poll!(caller.as_mut()), Poll::Pending));
        }
        release.notify_one();
        for result in join_all(callers).await {
            let error = result.err().expect("shared failure");
            assert_eq!(std::mem::discriminant(&error), expected_variant);
            assert_eq!(format!("{error:?}"), expected_fields);
        }
        let stats = cache.statistics();
        assert_eq!((stats.loads, stats.coalesced, stats.failures), (1, 31, 1));
        assert_eq!(
            (
                stats.active_loads,
                stats.participants,
                stats.metadata.reserved_bytes
            ),
            (0, 0, 0)
        );
        assert!(
            cache
                .load(
                    Request::test("failed-burst", Pool::Metadata, 8192),
                    || async { Ok((Value::Unavailable, false)) }
                )
                .await
                .unwrap()
                .is_some()
        );
        let stats = cache.statistics();
        assert_eq!(stats.active_loads, 0);
        assert_eq!(stats.participants, 0);
        for pool in [stats.metadata, stats.decoded] {
            assert_eq!(pool.reserved_bytes, 0);
            assert_eq!(pool.participant_bytes, 0);
            assert_eq!(pool.live_evicted_bytes, 0);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_completion_and_cancellation_release_every_flight() {
    let cache = store().read_cache().unwrap();
    for round in 0..128 {
        let mut callers = Vec::new();
        for caller in 0..16 {
            let cache = cache.clone();
            callers.push(tokio::spawn(async move {
                cache
                    .load(
                        Request::test(&format!("race-{}", caller % 4), Pool::Decoded, 8192),
                        move || async move {
                            tokio::task::yield_now().await;
                            Ok((Value::Block(Vec::new()), round % 2 == 0))
                        },
                    )
                    .await
            }));
        }
        for (index, caller) in callers.into_iter().enumerate() {
            if index % 3 == 0 {
                caller.abort();
            }
            if let Ok(result) = caller.await {
                result.unwrap();
            }
        }
        while cache.evict_locked(Pool::Decoded, &mut lock(&cache.0.directory)) {}
        let stats = cache.statistics();
        assert_eq!(
            (
                stats.active_loads,
                stats.participants,
                stats.decoded.reserved_bytes,
                stats.decoded.live_evicted_bytes
            ),
            (0, 0, 0, 0)
        );
        assert!(stats.high_water_loads <= 8);
        assert!(stats.decoded.high_water_bytes <= stats.decoded.capacity_bytes);
    }
    assert!(lock(&cache.0.directory).flights.iter().all(Option::is_none));
}

#[test]
#[cfg(feature = "test-utils")]
fn allocation_classes_count_owned_copies_and_scratch_separately() {
    cost::take();
    let copied = cost::allocated(26, || vec![7_u8; 8192]);
    let scratch = cost::allocated(24, || vec![3_u8; 4096]);
    std::hint::black_box((&copied, &scratch));
    let work = cost::take();
    assert_eq!(work["request"][25], 4096);
    assert_eq!(work["request"][27], 8192);
    assert_eq!(work["request"][29], 0);
}

struct BackendAllocation {
    bytes: Vec<u8>,
    live: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for BackendAllocation {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for BackendAllocation {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}
impl VersionBackend {
    fn owned_backend_slice(&self, bytes: Bytes) -> Bytes {
        if self.mode.load(Ordering::Relaxed) != 6 {
            return bytes;
        }
        let mut owned = vec![0; 16 * MIB + bytes.len()];
        owned[..bytes.len()].copy_from_slice(&bytes);
        self.live_buffers.fetch_add(1, Ordering::Relaxed);
        Bytes::from_owner(BackendAllocation {
            bytes: owned,
            live: self.live_buffers.clone(),
        })
        .slice(..bytes.len())
    }
}
#[tokio::test]
async fn backend_slices_never_retain_their_oversized_owner_in_cache() {
    let (backend, store) = version_store();
    let (_, token) = seed(&store).await;
    backend.mode.store(6, Ordering::Relaxed);
    let reader = store.read_at(token).await.unwrap();
    for _ in 0..2 {
        assert!(reader.get(&0_u32.to_be_bytes()).await.unwrap().is_some());
        assert_eq!(backend.live_buffers.load(Ordering::Relaxed), 0);
    }
    let stats = store.read_cache().unwrap().statistics();
    assert_eq!(stats.underestimates, 0);
    assert!(stats.metadata.high_water_bytes < MIB);
}

#[tokio::test]
async fn leased_empty_records_cannot_bypass_either_record_limit() {
    let cache = store().read_cache().unwrap();
    for pool in [Pool::Metadata, Pool::Decoded] {
        let mut leases = Vec::new();
        for ordinal in 0..pool.records() {
            leases.push(
                cache
                    .load(
                        Request::test(&format!("record-{ordinal}"), pool, 8192),
                        || async { Ok((Value::Block(Vec::new()), true)) },
                    )
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert!(
            cache
                .load(Request::test("over-record-limit", pool, 8192), || async {
                    panic!("record limit must decline before loader construction")
                })
                .await
                .unwrap()
                .is_none()
        );
        let mut stats = cache.statistics();
        let stats = pool.stats(&mut stats);
        assert_eq!(stats.live_records, pool.records());
        assert_eq!(stats.high_water_records, pool.records());
        assert!(stats.high_water_bytes <= stats.capacity_bytes);
        drop(leases);
        while cache.evict_locked(pool, &mut lock(&cache.0.directory)) {}
        assert_eq!(pool.stats(&mut cache.statistics()).live_records, 0);
    }
}

#[test]
fn handle_owns_backend_identity_until_its_last_clone_drops() {
    let store = store();
    let backend = Arc::downgrade(store.retention.backend());
    let handle = store.read_cache().unwrap();
    let last = handle.clone();
    drop(store);
    drop(handle);
    assert!(backend.upgrade().is_some());
    drop(last);
    assert!(backend.upgrade().is_none());
}

#[tokio::test]
#[cfg(feature = "test-utils")]
async fn shared_work_keeps_the_initiating_phase_after_initiator_cancellation() {
    use futures::poll;
    let (backend, store) = version_store();
    seed(&store).await;
    let pointer = store.load_pointer().await.unwrap();
    let manifest = store.load_manifest_for_pointer(&pointer).await.unwrap();
    let tx = store.load_tx_metadata(&manifest.tx_refs[0]).await.unwrap();
    backend.pause.store(true, Ordering::Relaxed);
    backend.directory_reads.store(0, Ordering::Relaxed);
    cost::take();
    let mut origin = Box::pin(cost::phase(
        "cache-origin",
        store.cached_directory(&tx.l0_segment),
    ));
    assert!(poll!(origin.as_mut()).is_pending());
    let mut waiter = Box::pin(cost::phase(
        "cache-waiter",
        store.cached_directory(&tx.l0_segment),
    ));
    assert!(poll!(waiter.as_mut()).is_pending());
    drop(origin);
    backend.release.notify_one();
    waiter.await.unwrap();
    let work = cost::take();
    assert_eq!(work["cache-origin"][22], 1);
    assert_eq!(work["cache-waiter"][22], 0);
    assert_eq!(backend.directory_reads.load(Ordering::Relaxed), 1);
    assert_eq!(store.read_cache().unwrap().statistics().participants, 0);
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn long_suffix_cold_allocations_stay_within_twice_disabled() {
    let writer = store().without_read_cache();
    seed(&writer).await;
    let key = 0_u32.to_be_bytes();
    let expected = Bytes::from(vec![42; 1024]);
    for _ in 0..30 {
        let mut tx = writer
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        tx.put(&key, expected.clone()).await.unwrap();
        tx.commit().await.unwrap();
    }
    let token = writer.current_state_token().await.unwrap();
    let direct = writer.read_at(token.clone()).await.unwrap();
    let cached = writer
        .clone()
        .with_read_cache_config(ControlMvpReadCacheConfig::default())
        .unwrap();
    let reader = cached.read_at(token).await.unwrap();
    // Both reads use the same immutable fixture and allocator instrumentation.
    futures::executor::block_on(async {});
    let disabled = allocation_counter::measure(|| {
        assert_eq!(
            futures::executor::block_on(direct.get(&key)).unwrap(),
            Some(expected.clone())
        );
    });
    let enabled = allocation_counter::measure(|| {
        assert_eq!(
            futures::executor::block_on(reader.get(&key)).unwrap(),
            Some(expected.clone())
        );
    });
    assert!(
        enabled.bytes_total <= 2 * disabled.bytes_total,
        "cold suffix allocations: enabled={}, disabled={}",
        enabled.bytes_total,
        disabled.bytes_total
    );
}

#[test]
fn default_constructor_does_not_silently_disable_unfunded_cache() {
    let metadata_bytes = ControlMvpReadCacheConfig::default().metadata_bytes;
    let domain = "x".repeat(metadata_bytes);
    let result = ControlMvpStateStore::new(
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap(),
        StateScope::new("tenant", "workspace", domain),
    );
    assert!(matches!(
        result,
        Err(CatalogError::Validation { message })
            if message == "read cache metadata capacity cannot fund administration"
    ));
}

#[test]
fn reference_validation_preserves_ascii_and_unicode_policy() {
    let original_id = |value: &str| {
        !value.trim().is_empty()
            && !matches!(value, "." | "..")
            && !value.contains(['/', '\\', '%'])
            && !value.chars().any(char::is_control)
    };
    let original_digest = |value: &str| {
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    for character in (0..=255).filter_map(char::from_u32).chain([
        '\u{2003}',
        '\u{2028}',
        '\u{3000}',
        '\u{feff}',
        '\u{1f642}',
    ]) {
        for value in [character.to_string(), format!("a{character}b")] {
            assert_eq!(integrity::valid_immutable_id(&value), original_id(&value));
        }
        for position in 0..64 {
            let mut digest = "a".repeat(64);
            digest.replace_range(position..=position, &character.to_string());
            assert_eq!(valid_raw_digest(&digest), original_digest(&digest));
        }
    }
    for value in ["", ".", "..", " ", " a ", "\u{2003}\u{3000}"] {
        assert_eq!(integrity::valid_immutable_id(value), original_id(value));
        assert_eq!(valid_raw_digest(value), original_digest(value));
    }
}

#[tokio::test]
async fn large_owner_captures_cannot_bypass_decoded_capacity() {
    use futures::poll;
    let (backend, store) = version_store();
    seed(&store).await;
    let pointer = store.load_pointer().await.unwrap();
    let manifest = store.load_manifest_for_pointer(&pointer).await.unwrap();
    let tx = store.load_tx_metadata(&manifest.tx_refs[0]).await.unwrap();
    let mut owner = tx.l0_segment;
    let segment = store.load_complete_segment(&owner).await.unwrap();
    let (_, mut index) = store.load_segment_index_direct(&owner).await.unwrap();
    owner.segment_id = "s".repeat(256 * 1024);
    index.segment_id = owner.segment_id.clone();
    let directory = serde_json::to_vec(&index).unwrap();
    owner.index_size_bytes = directory.len() as u64;
    owner.index_checksum_sha256 = super::super::sha256_hex(&directory);
    for (path, bytes) in [
        (segment_path(&store, &owner), segment),
        (
            store.paths.segment_index(&owner.segment_id),
            Bytes::from(directory),
        ),
    ] {
        store
            .retention
            .put_raw(&path, bytes, arco_core::storage::WritePrecondition::None)
            .await
            .unwrap();
    }
    // The large identity is valid under the existing authenticated directory limits.
    let (_, index) = store.load_segment_index_direct(&owner).await.unwrap();
    let block = &index.blocks[0];
    let expected = store.load_block_direct(&owner, block).await.unwrap();
    let store = store
        .with_read_cache_config(ControlMvpReadCacheConfig {
            metadata_bytes: MIB,
            decoded_bytes: 1400 * 1024,
        })
        .unwrap();
    let cache = store.read_cache().unwrap();
    let path = segment_path(&store, &owner);
    let version = store.storage.head(&path).await.unwrap().unwrap().version;
    let request = store
        .cache_request("block", &path, &(&owner, block), &version, Pool::Decoded, 0)
        .unwrap();
    let retained_floor = key_charge(&request.key) + reference_charge(&owner) + string(&path);
    assert!(retained_floor > 1400 * 1024);
    drop(request);
    backend.mode.store(9, Ordering::Relaxed);
    backend.pause.store(true, Ordering::Relaxed);
    let mut read = Box::pin(store.cached_block(&owner, block));
    assert!(poll!(read.as_mut()).is_pending());
    // The key plus the separately owned identity/path copies cannot fit this pool.
    assert_eq!(cache.statistics().active_loads, 0);
    assert_eq!(cache.statistics().decoded.reserved_bytes, 0);
    assert!(cache.statistics().fallbacks > 0);
    backend.release.notify_one();
    assert_eq!(read.await.unwrap(), expected);

    // At a paused payload read, the cache owns the key, loader and scoped paths.
    // Measure retained allocations before any backend bytes or decoder scratch exist.
    let store = store
        .with_read_cache_config(ControlMvpReadCacheConfig::default())
        .unwrap();
    let cache = store.read_cache().unwrap();
    let mut read = Box::pin(store.cached_block(&owner, block));
    let allocations = allocation_counter::measure(|| {
        let mut context = std::task::Context::from_waker(futures::task::noop_waker_ref());
        assert!(Future::poll(read.as_mut(), &mut context).is_pending());
    });
    let stats = cache.statistics();
    assert_eq!(stats.active_loads, 1);
    assert!(allocations.bytes_current > 0);
    assert!(
        usize::try_from(allocations.bytes_current).unwrap()
            <= stats.decoded.reserved_bytes + stats.decoded.participant_bytes,
        "live loader bytes {} exceed reserved {} plus participants {}",
        allocations.bytes_current,
        stats.decoded.reserved_bytes,
        stats.decoded.participant_bytes
    );
    backend.release.notify_one();
    assert_eq!(read.await.unwrap(), expected);
}

#[tokio::test]
#[cfg(feature = "test-utils")]
#[allow(clippy::too_many_lines)] // Five real I/O schedules share the same measured setup.
async fn overlapping_directory_loads_report_coalescing_and_saturation() {
    let _inputs = arco_core::test_inputs::FixedInputs::scoped();
    let (backend, writer) = version_store();
    let writer = writer.without_read_cache();
    for value in 0..9_u8 {
        let mut tx = writer
            .begin_control_txn(TxnOptions::default())
            .await
            .unwrap();
        tx.put(b"key", Bytes::from(vec![value])).await.unwrap();
        tx.commit().await.unwrap();
    }
    let pointer = writer.load_pointer().await.unwrap();
    let manifest = writer.load_manifest_for_pointer(&pointer).await.unwrap();
    let mut owners = Vec::new();
    let mut expected = Vec::new();
    for reference in &manifest.tx_refs {
        let owner = writer
            .load_tx_metadata_direct(reference)
            .await
            .unwrap()
            .l0_segment;
        expected.push(writer.load_segment_index_direct(&owner).await.unwrap());
        owners.push(owner);
    }
    assert_eq!(owners.len(), 9);
    let mut reports = Vec::new();
    for repetition in 1..=5 {
        for (callers, distinct, saturated) in [
            (1, false, false),
            (8, false, false),
            (32, false, false),
            (9, true, false),
            (257, true, true),
        ] {
            let owner_for = |caller: usize| {
                if saturated {
                    (caller / 32).min(8)
                } else if distinct {
                    caller
                } else {
                    0
                }
            };
            let store = writer
                .clone()
                .with_read_cache_config(ControlMvpReadCacheConfig::default())
                .unwrap();
            let cache = store.read_cache().unwrap();
            backend.heads.store(0, Ordering::Relaxed);
            backend.head_bytes.store(0, Ordering::Relaxed);
            backend.backend_allocation_bytes.store(0, Ordering::Relaxed);
            backend.backend_allocation_count.store(0, Ordering::Relaxed);
            backend.directory_reads.store(0, Ordering::Relaxed);
            backend.directory_gets.store(0, Ordering::Relaxed);
            backend.directory_bytes.store(0, Ordering::Relaxed);
            backend.pause.store(true, Ordering::Relaxed);
            let _ = cost::take();
            let started = std::time::Instant::now();
            let mut reads = Box::pin(cost::phase(
                "cache-concurrency",
                futures::future::join_all(
                    (0..callers).map(|caller| store.cached_directory(&owners[owner_for(caller)])),
                ),
            ));
            let initial = allocation_counter::measure(|| {
                let mut context = std::task::Context::from_waker(futures::task::noop_waker_ref());
                for _ in 0..=callers {
                    assert!(reads.as_mut().poll(&mut context).is_pending());
                    if backend.heads.load(Ordering::Relaxed) == callers {
                        break;
                    }
                }
            });
            let paused = cache.statistics();
            let physical = if distinct { 9 } else { 1 };
            let admitted = if distinct { 8 } else { 1 };
            assert_eq!(backend.heads.load(Ordering::Relaxed), callers);
            assert_eq!(backend.directory_reads.load(Ordering::Relaxed), physical);
            assert_eq!(paused.active_loads, admitted);
            assert_eq!(
                paused.participants,
                if saturated {
                    256
                } else if distinct {
                    8
                } else {
                    callers
                }
            );
            assert_eq!(
                paused.coalesced,
                if saturated {
                    248
                } else if distinct {
                    0
                } else {
                    u64::try_from(callers).unwrap() - 1
                }
            );
            assert_eq!(paused.fallbacks, u64::from(distinct));
            backend.pause.store(false, Ordering::Relaxed);
            backend.release.notify_waiters();
            let mut allocation_bytes = initial.bytes_total;
            let mut allocation_count = initial.count_total;
            let values = std::future::poll_fn(|context| {
                let mut result = std::task::Poll::Pending;
                let allocations =
                    allocation_counter::measure(|| result = reads.as_mut().poll(context));
                allocation_bytes += allocations.bytes_total;
                allocation_count += allocations.count_total;
                result
            })
            .await;
            let elapsed_micros = started.elapsed().as_micros();
            for (caller, value) in values.into_iter().enumerate() {
                let value = value.unwrap();
                let expected = &expected[owner_for(caller)];
                assert_eq!(value.0, expected.0);
                assert_eq!(
                    serde_json::to_value(value.1).unwrap(),
                    serde_json::to_value(&expected.1).unwrap()
                );
            }
            let final_stats = cache.statistics();
            assert_eq!(backend.heads.load(Ordering::Relaxed), callers + admitted);
            assert_eq!(backend.directory_reads.load(Ordering::Relaxed), physical);
            assert_eq!(final_stats.active_loads, 0);
            assert_eq!(final_stats.participants, 0);
            assert_eq!(final_stats.metadata.reserved_bytes, 0);
            assert_eq!(final_stats.underestimates, 0);
            assert_eq!(backend.directory_gets.load(Ordering::Relaxed), 0);
            let work = cost::take();
            assert_eq!(
                work.values().map(|work| work[22]).sum::<u64>(),
                u64::try_from(physical).unwrap()
            );
            reports.push(serde_json::json!({
                "repetition": repetition, "distinct_objects": distinct, "saturated_participants": saturated, "caller_demands": callers,
                "directory_get_attempts": backend.directory_gets.load(Ordering::Relaxed), "directory_range_attempts": backend.directory_reads.load(Ordering::Relaxed),
                "directory_range_bytes": backend.directory_bytes.load(Ordering::Relaxed),
                "directory_head_attempts": backend.heads.load(Ordering::Relaxed),
                "directory_head_metadata_bytes": backend.head_bytes.load(Ordering::Relaxed),
                "cache_at_pause": paused, "cache_final": final_stats,
                "elapsed_micros_including_barrier": elapsed_micros,
                "poll_allocations": {"count": allocation_count, "bytes": allocation_bytes},
                "backend_poll_allocations": {
                    "count": backend.backend_allocation_count.load(Ordering::Relaxed),
                    "bytes": backend.backend_allocation_bytes.load(Ordering::Relaxed)
                },
                "phase_work_slots_0_to_35": work.into_iter().map(|(phase, work)| (phase, work.to_vec())).collect::<BTreeMap<_,_>>(),
                "result_parity": true
            }));
        }
    }
    if let Ok(path) = std::env::var("ARCO_READ_CACHE_CONCURRENCY_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&reports).unwrap()).unwrap();
    }
}

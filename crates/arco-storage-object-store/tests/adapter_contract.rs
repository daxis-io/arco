//! Contract tests for provider-neutral object-store protocol translation.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]

use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};
use arco_storage_object_store::{ObjectStoreBackend, no_automatic_request_retries};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    DynObjectStore, GetOptions, GetResult, ListResult, MultipartUpload, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};

#[derive(Debug, Default)]
struct NotFoundOnWriteStore {
    inner: InMemory,
}

#[derive(Debug)]
struct MockProviderClient {
    inner: InMemory,
    retry_one_transient_get: bool,
    fail_put_ambiguously: bool,
    get_attempts: AtomicUsize,
    put_attempts: AtomicUsize,
}

impl MockProviderClient {
    fn new(retry_one_transient_get: bool, fail_put_ambiguously: bool) -> Self {
        Self {
            inner: InMemory::new(),
            retry_one_transient_get,
            fail_put_ambiguously,
            get_attempts: AtomicUsize::new(0),
            put_attempts: AtomicUsize::new(0),
        }
    }
}

impl fmt::Display for MockProviderClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("mock-provider-client")
    }
}

#[async_trait]
impl ObjectStore for MockProviderClient {
    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put_attempts.fetch_add(1, Ordering::SeqCst);
        if self.fail_put_ambiguously {
            return Err(object_store::Error::Generic {
                store: "mock-provider-client",
                source: Box::new(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "response lost after conditional put",
                )),
            });
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        options: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.get_attempts.fetch_add(1, Ordering::SeqCst);
        if self.retry_one_transient_get && self.get_attempts.load(Ordering::SeqCst) == 1 {
            // Model the provider client consuming a first 503 and retrying the
            // safe GET internally before returning to the Arco adapter.
            self.get_attempts.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &ObjectStorePath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'_, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectStorePath, to: &ObjectStorePath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

impl fmt::Display for NotFoundOnWriteStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("not-found-on-write")
    }
}

#[async_trait]
impl ObjectStore for NotFoundOnWriteStore {
    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        _payload: PutPayload,
        _options: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(object_store::Error::NotFound {
            path: location.to_string(),
            source: Box::new(io::Error::new(io::ErrorKind::NotFound, "missing root")),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        options: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &ObjectStorePath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'_, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectStorePath, to: &ObjectStorePath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn ordered_memory_adapter() -> ObjectStoreBackend {
    let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
    ObjectStoreBackend::new_with_ordered_listing(store, None)
}

#[test]
fn provider_request_policy_does_not_retry_ambiguous_writes() {
    assert_eq!(no_automatic_request_retries().max_retries, 0);
}

#[tokio::test]
async fn safe_reads_retry_while_conditional_writes_use_the_single_attempt_client() {
    let primary = Arc::new(MockProviderClient::new(true, false));
    primary
        .inner
        .put_opts(
            &ObjectStorePath::from("authority/head.json"),
            Bytes::from_static(b"visible").into(),
            PutOptions::default(),
        )
        .await
        .unwrap();
    let conditional = Arc::new(MockProviderClient::new(false, true));
    let primary_store: Arc<DynObjectStore> = primary.clone();
    let conditional_store: Arc<DynObjectStore> = conditional.clone();
    let backend = ObjectStoreBackend::new_with_conditional_write_store(
        primary_store,
        conditional_store,
        None,
    );

    assert_eq!(
        backend.get("authority/head.json").await.unwrap(),
        Bytes::from_static(b"visible")
    );
    assert_eq!(primary.get_attempts.load(Ordering::SeqCst), 2);

    let write = backend
        .put(
            "authority/head.json",
            Bytes::from_static(b"candidate"),
            WritePrecondition::MatchesVersion("opaque-version".to_owned()),
        )
        .await;
    assert!(write.is_err());
    assert_eq!(conditional.put_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(primary.put_attempts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn translates_create_compare_and_swap_range_and_delete() {
    let backend = ordered_memory_adapter();
    let first = backend
        .put(
            "authority/head.json",
            Bytes::from_static(b"version-one"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("create");
    let WriteResult::Success { version: first } = first else {
        panic!("initial create must succeed");
    };

    let duplicate = backend
        .put(
            "authority/head.json",
            Bytes::from_static(b"duplicate"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("duplicate is a typed result");
    assert!(matches!(
        duplicate,
        WriteResult::PreconditionFailed { current_version } if current_version == first
    ));
    assert_eq!(
        backend
            .get_range("authority/head.json", 8..64)
            .await
            .unwrap(),
        Bytes::from_static(b"one")
    );

    let update = backend
        .put(
            "authority/head.json",
            Bytes::from_static(b"version-two"),
            WritePrecondition::MatchesVersion(first.clone()),
        )
        .await
        .expect("matching update");
    let WriteResult::Success { version: second } = update else {
        panic!("matching update must succeed");
    };
    assert_ne!(first, second);

    let stale = backend
        .put(
            "authority/head.json",
            Bytes::from_static(b"stale"),
            WritePrecondition::MatchesVersion(first),
        )
        .await
        .expect("stale update is a typed result");
    assert!(matches!(
        stale,
        WriteResult::PreconditionFailed { current_version } if current_version == second
    ));

    backend.delete("authority/head.json").await.unwrap();
    backend.delete("authority/head.json").await.unwrap();
    assert!(backend.head("authority/head.json").await.unwrap().is_none());

    let recreated = backend
        .put(
            "authority/head.json",
            Bytes::from_static(b"version-three"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("recreate");
    let WriteResult::Success { version: recreated } = recreated else {
        panic!("recreate must succeed");
    };
    assert_ne!(second, recreated);

    let pre_delete_token = backend
        .put(
            "authority/head.json",
            Bytes::from_static(b"stale-after-recreate"),
            WritePrecondition::MatchesVersion(second),
        )
        .await
        .expect("stale post-recreate update is a typed result");
    assert!(matches!(
        pre_delete_token,
        WriteResult::PreconditionFailed { current_version } if current_version == recreated
    ));
}

#[tokio::test]
async fn ordered_paging_is_exclusive_and_defers_insertions_before_the_cursor() {
    let backend = ordered_memory_adapter();
    for path in ["ledger/a.json", "ledger/c.json"] {
        backend
            .put(path, Bytes::from_static(b"event"), WritePrecondition::None)
            .await
            .unwrap();
    }
    let first = backend.list_page("ledger/", None, 2).await.unwrap();
    assert_eq!(first.next_start_after.as_deref(), Some("ledger/c.json"));

    for path in ["ledger/b.json", "ledger/d.json"] {
        backend
            .put(path, Bytes::from_static(b"event"), WritePrecondition::None)
            .await
            .unwrap();
    }
    let second = backend
        .list_page("ledger/", first.next_start_after.as_deref(), 2)
        .await
        .unwrap();
    assert_eq!(
        second
            .objects
            .iter()
            .map(|object| object.path.as_str())
            .collect::<Vec<_>>(),
        ["ledger/d.json"]
    );
    assert!(second.next_start_after.is_none());
}

#[tokio::test]
async fn generic_adapter_fails_closed_for_bounded_listing() {
    let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
    let backend = ObjectStoreBackend::new(store, None);
    let empty = backend.list_page("ledger/", None, 0).await.unwrap();
    assert!(empty.objects.is_empty());
    assert!(empty.next_start_after.is_none());
    let error = backend.list_page("ledger/", None, 1).await.unwrap_err();
    assert!(error.to_string().contains("unsupported"));
}

#[tokio::test]
async fn non_cas_not_found_writes_remain_operational_errors() {
    let store: Arc<DynObjectStore> = Arc::new(NotFoundOnWriteStore::default());
    let backend = ObjectStoreBackend::new(store, None);
    for precondition in [WritePrecondition::DoesNotExist, WritePrecondition::None] {
        let write = backend
            .put(
                "missing-root/head.json",
                Bytes::from_static(b"v1"),
                precondition,
            )
            .await;
        assert!(
            write.is_err(),
            "NotFound must not become a CAS loser: {write:?}"
        );
    }
}

#![allow(clippy::expect_used)]
#![allow(missing_docs)]

#[path = "support/barrier_backend.rs"]
mod barrier_backend;

use std::fmt;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use arco_core::storage::{ObjectStoreBackend, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{MemoryBackend, ObjectMeta};
use async_trait::async_trait;
use barrier_backend::{BarrierBackend, BarrierMatch};
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectStorePath;
use object_store::{
    DynObjectStore, GetOptions, GetResult, ListResult, MultipartUpload, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult,
};
use ulid::Ulid;

#[derive(Debug, Default)]
struct NotFoundOnWriteStore {
    inner: InMemory,
}

impl fmt::Display for NotFoundOnWriteStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("not-found-on-write")
    }
}

#[async_trait]
impl ObjectStore for NotFoundOnWriteStore {
    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(object_store::Error::NotFound {
            path: location.to_string(),
            source: Box::new(io::Error::new(io::ErrorKind::NotFound, "missing root")),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
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

async fn assert_storage_conformance(name: &str, backend: Arc<dyn StorageBackend>) {
    let path = format!("conformance/{name}/{}/head.json", Ulid::new());

    let first = backend
        .put(
            &path,
            Bytes::from_static(b"v1"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("create-if-absent write");
    let first_version = match first {
        WriteResult::Success { version } => version,
        WriteResult::PreconditionFailed { .. } => {
            panic!("initial create-if-absent write must succeed")
        }
    };
    assert!(!first_version.is_empty(), "version token must be non-empty");

    let duplicate = backend
        .put(
            &path,
            Bytes::from_static(b"v1-duplicate"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("duplicate create-if-absent write");
    assert!(
        matches!(
            duplicate,
            WriteResult::PreconditionFailed { ref current_version } if current_version == &first_version
        ),
        "immutable create-if-absent writes must reject overwrite attempts"
    );

    let second = backend
        .put(
            &path,
            Bytes::from_static(b"v2"),
            WritePrecondition::MatchesVersion(first_version.clone()),
        )
        .await
        .expect("cas write");
    let second_version = match second {
        WriteResult::Success { version } => version,
        WriteResult::PreconditionFailed { .. } => panic!("current-version CAS must succeed"),
    };
    assert_ne!(
        first_version, second_version,
        "successful CAS must advance the visible version token"
    );

    let stale = backend
        .put(
            &path,
            Bytes::from_static(b"v3"),
            WritePrecondition::MatchesVersion(first_version.clone()),
        )
        .await
        .expect("stale cas write");
    assert!(
        matches!(
            stale,
            WriteResult::PreconditionFailed { ref current_version } if current_version == &second_version
        ),
        "stale CAS must fail with the current visible version"
    );

    let head = backend
        .head(&path)
        .await
        .expect("head after CAS")
        .expect("visible object");
    assert_head_version(&head, &second_version);

    backend.delete(&path).await.expect("delete before recreate");
    assert!(
        backend
            .head(&path)
            .await
            .expect("head after delete")
            .is_none(),
        "delete must remove the visible object"
    );

    let recreated = backend
        .put(
            &path,
            Bytes::from_static(b"v4"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("recreate after delete");
    let recreated_version = match recreated {
        WriteResult::Success { version } => version,
        WriteResult::PreconditionFailed { .. } => panic!("recreate after delete must succeed"),
    };
    assert_ne!(
        recreated_version, first_version,
        "{name}: delete/recreate must not recycle an earlier version token"
    );
    assert_ne!(
        recreated_version, second_version,
        "{name}: delete/recreate must not recycle the deleted head version token"
    );

    let stale_after_recreate = backend
        .put(
            &path,
            Bytes::from_static(b"stale-after-recreate"),
            WritePrecondition::MatchesVersion(first_version.clone()),
        )
        .await
        .expect("stale CAS after recreate");
    assert!(
        matches!(
            stale_after_recreate,
            WriteResult::PreconditionFailed { ref current_version } if current_version == &recreated_version
        ),
        "{name}: CAS with a pre-delete token must fail against the recreated object"
    );

    let malformed = backend
        .put(
            &path,
            Bytes::from_static(b"malformed-token"),
            WritePrecondition::MatchesVersion("not-a-backend-version-token".to_string()),
        )
        .await
        .expect("malformed CAS token");
    assert!(
        matches!(
            malformed,
            WriteResult::PreconditionFailed { ref current_version } if current_version == &recreated_version
        ),
        "{name}: malformed CAS tokens must fail closed with the current visible version"
    );
    let head_after_malformed = backend
        .head(&path)
        .await
        .expect("head after malformed token")
        .expect("visible object after malformed token");
    assert_head_version(&head_after_malformed, &recreated_version);

    let missing_path = format!("{path}.missing");
    let missing_cas = backend
        .put(
            &missing_path,
            Bytes::from_static(b"missing-cas"),
            WritePrecondition::MatchesVersion(recreated_version.clone()),
        )
        .await
        .expect("missing-object CAS");
    assert!(
        matches!(
            missing_cas,
            WriteResult::PreconditionFailed { ref current_version } if current_version.is_empty()
        ),
        "{name}: CAS against a missing object must fail without inventing a visible version"
    );
    assert!(
        backend
            .head(&missing_path)
            .await
            .expect("head after missing-object CAS")
            .is_none(),
        "{name}: failed missing-object CAS must not create the object"
    );

    assert_exact_one_cas_winner(name, backend, &path, &recreated_version).await;
}

fn assert_head_version(head: &ObjectMeta, expected_version: &str) {
    assert_eq!(head.version, expected_version);
    assert!(!head.version.is_empty(), "head version must be non-empty");
}

async fn assert_exact_one_cas_winner(
    name: &str,
    backend: Arc<dyn StorageBackend>,
    seed_path: &str,
    current_version: &str,
) {
    let race_path = format!("{seed_path}.race");
    let seed = backend
        .put(
            &race_path,
            Bytes::from_static(b"seed"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("seed race path");
    let race_version = match seed {
        WriteResult::Success { version } => version,
        WriteResult::PreconditionFailed { .. } => panic!("seed write must succeed"),
    };
    assert_ne!(
        race_version, current_version,
        "race path version should be independent of the control path"
    );

    let barrier_backend: Arc<dyn StorageBackend> = Arc::new(BarrierBackend::new(
        backend.clone(),
        2,
        BarrierMatch::matches_version(race_path.clone()),
    ));

    let left = {
        let backend = barrier_backend.clone();
        let race_path = race_path.clone();
        let race_version = race_version.clone();
        tokio::spawn(async move {
            backend
                .put(
                    &race_path,
                    Bytes::from_static(b"left"),
                    WritePrecondition::MatchesVersion(race_version),
                )
                .await
                .expect("left CAS result")
        })
    };
    let right = {
        let backend = barrier_backend;
        let race_path = race_path.clone();
        let race_version = race_version.clone();
        tokio::spawn(async move {
            backend
                .put(
                    &race_path,
                    Bytes::from_static(b"right"),
                    WritePrecondition::MatchesVersion(race_version),
                )
                .await
                .expect("right CAS result")
        })
    };

    let left = left.await.expect("left task");
    let right = right.await.expect("right task");

    let results = [left.clone(), right.clone()];
    let winner_versions: Vec<String> = results
        .iter()
        .filter_map(|result| match result {
            WriteResult::Success { version } => Some(version.clone()),
            WriteResult::PreconditionFailed { .. } => None,
        })
        .collect();
    assert_eq!(
        winner_versions.len(),
        1,
        "{name}: exactly one concurrent CAS write must win"
    );

    let winner_version = winner_versions[0].clone();
    let loser = if matches!(left, WriteResult::PreconditionFailed { .. }) {
        left
    } else {
        right
    };
    assert!(
        matches!(
            loser,
            WriteResult::PreconditionFailed { ref current_version } if current_version == &winner_version
        ),
        "{name}: loser must observe the winner as the new visible head"
    );

    let head = backend
        .head(&race_path)
        .await
        .expect("head after race")
        .expect("race path exists");
    assert_eq!(
        head.version, winner_version,
        "{name}: visible head must be old-or-winner only"
    );
}

fn object_store_memory_backend() -> Arc<dyn StorageBackend> {
    let store: Arc<DynObjectStore> = Arc::new(InMemory::new());
    Arc::new(ObjectStoreBackend::new(store, None))
}

fn object_store_local_backend() -> (Arc<dyn StorageBackend>, PathBuf) {
    let root = std::env::temp_dir().join(format!("arco-storage-conformance-{}", Ulid::new()));
    std::fs::create_dir_all(&root).expect("create local object store root");
    let store: Arc<DynObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&root).expect("local filesystem store"));
    (Arc::new(ObjectStoreBackend::new(store, None)), root)
}

#[tokio::test]
async fn memory_backend_satisfies_storage_conformance() {
    assert_storage_conformance("memory", Arc::new(MemoryBackend::new())).await;
}

#[tokio::test]
async fn object_store_memory_backend_satisfies_storage_conformance() {
    assert_storage_conformance("object-store-memory", object_store_memory_backend()).await;
}

#[tokio::test]
async fn object_store_backend_preserves_non_cas_not_found_write_errors() {
    let store: Arc<DynObjectStore> = Arc::new(NotFoundOnWriteStore::default());
    let backend = ObjectStoreBackend::new(store, None);

    let create = backend
        .put(
            "missing-root/create.json",
            Bytes::from_static(b"v1"),
            WritePrecondition::DoesNotExist,
        )
        .await;
    assert!(
        create.is_err(),
        "create write NotFound must remain an operational error, got {create:?}"
    );

    let unconditional = backend
        .put(
            "missing-root/unconditional.json",
            Bytes::from_static(b"v1"),
            WritePrecondition::None,
        )
        .await;
    assert!(
        unconditional.is_err(),
        "unconditional write NotFound must remain an operational error, got {unconditional:?}"
    );
}

#[tokio::test]
async fn object_store_local_backend_is_not_a_cas_conformance_substitute() {
    let (backend, root) = object_store_local_backend();
    let path = format!("local-cas-negative/{}/head.json", Ulid::new());
    let seed = backend
        .put(
            &path,
            Bytes::from_static(b"v1"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("seed local object");
    let WriteResult::Success { version } = seed else {
        panic!("seed local object must succeed");
    };

    let update = backend
        .put(
            &path,
            Bytes::from_static(b"v2"),
            WritePrecondition::MatchesVersion(version),
        )
        .await;
    assert!(
        format!("{update:?}").contains("NotImplemented"),
        "object_store::local must not be treated as production CAS conformance"
    );
    std::fs::remove_dir_all(root).expect("remove local object store root");
}

#[tokio::test]
#[ignore = "requires ARCO_TEST_GCS_BUCKET and cloud credentials"]
async fn gcs_backend_satisfies_storage_conformance() {
    let bucket = std::env::var("ARCO_TEST_GCS_BUCKET")
        .expect("ARCO_TEST_GCS_BUCKET must be set for the GCS conformance test");
    let backend: Arc<dyn StorageBackend> =
        Arc::new(ObjectStoreBackend::gcs(&bucket).expect("gcs backend"));
    assert_storage_conformance("gcs", backend).await;
}

#[tokio::test]
#[ignore = "requires ARCO_TEST_S3_BUCKET and cloud credentials"]
async fn s3_backend_satisfies_storage_conformance() {
    let bucket = std::env::var("ARCO_TEST_S3_BUCKET")
        .expect("ARCO_TEST_S3_BUCKET must be set for the S3 conformance test");
    let backend: Arc<dyn StorageBackend> =
        Arc::new(ObjectStoreBackend::s3(&bucket).expect("s3 backend"));
    assert_storage_conformance("s3", backend).await;
}

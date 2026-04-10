#![allow(clippy::expect_used)]
#![allow(missing_docs)]

#[path = "support/barrier_backend.rs"]
mod barrier_backend;

use std::sync::Arc;

use arco_core::storage::{ObjectStoreBackend, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{MemoryBackend, ObjectMeta};
use barrier_backend::{BarrierBackend, BarrierMatch};
use bytes::Bytes;
use object_store::DynObjectStore;
use object_store::memory::InMemory;
use ulid::Ulid;

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

    assert_exact_one_cas_winner(name, backend, &path, &second_version).await;
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

#[tokio::test]
async fn memory_backend_satisfies_storage_conformance() {
    assert_storage_conformance("memory", Arc::new(MemoryBackend::new())).await;
}

#[tokio::test]
async fn object_store_memory_backend_satisfies_storage_conformance() {
    assert_storage_conformance("object-store-memory", object_store_memory_backend()).await;
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

#![allow(clippy::expect_used)]
#![allow(missing_docs)]
// Test-target lint scope (#331): conformance helpers signal failure by
// panicking, mirroring the allow-*-in-tests policy in clippy.toml.
#![allow(clippy::panic, clippy::indexing_slicing)]
// Advisory lint scope for test code (#331): the pedantic/nursery lints below
// conflict with test ergonomics here; production code keeps them active.
#![allow(clippy::cognitive_complexity, clippy::too_many_lines)]

#[path = "support/barrier_backend.rs"]
mod barrier_backend;
#[path = "support/spy_backend.rs"]
mod spy_backend;

use std::sync::Arc;

use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};
use arco_core::{MemoryBackend, ObjectMeta};
use barrier_backend::{BarrierBackend, BarrierMatch};
use bytes::Bytes;
use spy_backend::{SpyBackend, SpyOp};
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

#[tokio::test]
async fn memory_backend_satisfies_storage_conformance() {
    assert_storage_conformance("memory", Arc::new(MemoryBackend::new())).await;
}

#[tokio::test]
async fn spy_backend_records_failed_get_attempts() {
    let spy = SpyBackend::new(Arc::new(MemoryBackend::new()));

    let result = spy.get("missing/object.json").await;

    assert!(result.is_err(), "missing object read should fail");
    let ops = spy.ops();
    assert!(
        matches!(
            ops.as_slice(),
            [SpyOp::Get { path, byte_len }]
                if path == "missing/object.json" && *byte_len == 0
        ),
        "spy should record failed get attempts with zero bytes: {ops:?}"
    );
}

#[tokio::test]
async fn spy_backend_records_failed_get_range_attempts() {
    let spy = SpyBackend::new(Arc::new(MemoryBackend::new()));

    let result = spy.get_range("missing/object.json", 2..7).await;

    assert!(result.is_err(), "missing range read should fail");
    let ops = spy.ops();
    assert!(
        matches!(
            ops.as_slice(),
            [SpyOp::GetRange {
                path,
                start: 2,
                end: 7,
                byte_len
            }] if path == "missing/object.json" && *byte_len == 0
        ),
        "spy should record failed get_range attempts with zero bytes: {ops:?}"
    );
}

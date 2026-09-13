//! Shared live-provider conformance assertions included by provider test targets.

#![allow(
    clippy::cognitive_complexity,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines,
    clippy::unwrap_used
)]

use std::sync::Arc;

use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use bytes::Bytes;
use tokio::sync::Barrier;
use ulid::Ulid;

/// Verifies the conditional-write semantics required by Arco authority heads.
pub async fn assert_storage_conformance(name: &str, backend: Arc<dyn StorageBackend>) {
    let path = format!("conformance/{name}/{}/head.json", Ulid::new());
    let first = backend
        .put(
            &path,
            Bytes::from_static(b"v1"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("create-if-absent write");
    let WriteResult::Success {
        version: first_version,
    } = first
    else {
        panic!("initial create-if-absent write must succeed");
    };
    assert!(!first_version.is_empty());

    let duplicate = backend
        .put(
            &path,
            Bytes::from_static(b"duplicate"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("duplicate create");
    assert!(matches!(
        duplicate,
        WriteResult::PreconditionFailed { ref current_version }
            if current_version == &first_version
    ));

    let second = backend
        .put(
            &path,
            Bytes::from_static(b"v2"),
            WritePrecondition::MatchesVersion(first_version.clone()),
        )
        .await
        .expect("matching CAS");
    let WriteResult::Success {
        version: second_version,
    } = second
    else {
        panic!("matching CAS must succeed");
    };
    assert_ne!(first_version, second_version);

    let stale = backend
        .put(
            &path,
            Bytes::from_static(b"stale"),
            WritePrecondition::MatchesVersion(first_version.clone()),
        )
        .await
        .expect("stale CAS");
    assert!(matches!(
        stale,
        WriteResult::PreconditionFailed { ref current_version }
            if current_version == &second_version
    ));
    assert_head_version(
        &backend.head(&path).await.unwrap().expect("visible head"),
        &second_version,
    );

    backend.delete(&path).await.expect("delete");
    assert!(backend.head(&path).await.unwrap().is_none());
    let recreated = backend
        .put(
            &path,
            Bytes::from_static(b"v3"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("recreate");
    let WriteResult::Success {
        version: recreated_version,
    } = recreated
    else {
        panic!("recreate must succeed");
    };
    assert_ne!(recreated_version, first_version);
    assert_ne!(recreated_version, second_version);

    for token in [first_version, "not-a-backend-version-token".to_string()] {
        let rejected = backend
            .put(
                &path,
                Bytes::from_static(b"rejected"),
                WritePrecondition::MatchesVersion(token),
            )
            .await
            .expect("invalid CAS token is a typed loser");
        assert!(matches!(
            rejected,
            WriteResult::PreconditionFailed { ref current_version }
                if current_version == &recreated_version
        ));
    }

    let missing = format!("{path}.missing");
    let missing_cas = backend
        .put(
            &missing,
            Bytes::from_static(b"missing"),
            WritePrecondition::MatchesVersion(recreated_version.clone()),
        )
        .await
        .expect("missing-object CAS");
    assert!(matches!(
        missing_cas,
        WriteResult::PreconditionFailed { ref current_version } if current_version.is_empty()
    ));
    assert!(backend.head(&missing).await.unwrap().is_none());

    assert_exactly_one_concurrent_cas_winner(name, backend, &path).await;
}

/// Verifies the bounded exclusive-cursor contract for ordered providers.
pub async fn assert_bounded_list_conformance(name: &str, backend: &Arc<dyn StorageBackend>) {
    const LIMIT: usize = 3;
    let prefix = format!("conformance/{name}/{}/paged/", Ulid::new());
    let expected = (0..6)
        .map(|index| format!("{prefix}object-{index:02}.json"))
        .collect::<Vec<_>>();
    for path in &expected {
        assert!(matches!(
            backend
                .put(
                    path,
                    Bytes::from_static(b"page"),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .expect("seed page object"),
            WriteResult::Success { .. }
        ));
    }

    let mut cursor = None;
    let mut seen = Vec::new();
    loop {
        let page = backend
            .list_page(&prefix, cursor.as_deref(), LIMIT)
            .await
            .expect("bounded list page");
        assert!(page.objects.len() <= LIMIT);
        if let Some(cursor) = cursor.as_deref() {
            assert!(page.objects.iter().all(|meta| meta.path.as_str() > cursor));
        }
        seen.extend(page.objects.into_iter().map(|meta| meta.path));
        let Some(next) = page.next_start_after else {
            break;
        };
        cursor = Some(next);
    }
    assert_eq!(seen, expected);
    for path in expected {
        backend.delete(&path).await.expect("delete page object");
    }
}

fn assert_head_version(head: &ObjectMeta, expected: &str) {
    assert_eq!(head.version, expected);
    assert!(!head.version.is_empty());
}

async fn assert_exactly_one_concurrent_cas_winner(
    name: &str,
    backend: Arc<dyn StorageBackend>,
    seed_path: &str,
) {
    let path = format!("{seed_path}.race");
    let seed = backend
        .put(
            &path,
            Bytes::from_static(b"seed"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("seed race");
    let WriteResult::Success { version } = seed else {
        panic!("race seed must succeed");
    };
    let barrier = Arc::new(Barrier::new(3));
    let contender = |payload: &'static [u8]| {
        let backend = backend.clone();
        let barrier = barrier.clone();
        let path = path.clone();
        let version = version.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            backend
                .put(
                    &path,
                    Bytes::from_static(payload),
                    WritePrecondition::MatchesVersion(version),
                )
                .await
                .expect("concurrent CAS")
        })
    };
    let left = contender(b"left");
    let right = contender(b"right");
    barrier.wait().await;
    let left = left.await.expect("left task");
    let right = right.await.expect("right task");
    let winner_versions = [&left, &right]
        .into_iter()
        .filter_map(|result| match result {
            WriteResult::Success { version } => Some(version.clone()),
            WriteResult::PreconditionFailed { .. } => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(winner_versions.len(), 1, "{name}: exactly one CAS winner");
    let winner = &winner_versions[0];
    let loser = if matches!(left, WriteResult::PreconditionFailed { .. }) {
        left
    } else {
        right
    };
    assert!(matches!(
        loser,
        WriteResult::PreconditionFailed { current_version } if current_version == *winner
    ));
    assert_head_version(
        &backend.head(&path).await.unwrap().expect("race head"),
        winner,
    );
}

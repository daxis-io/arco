//! Synthetic tenant identity-root admission contracts.
#![cfg(feature = "test-utils")]

use std::sync::Arc;

use arco_catalog::state_store::identity_probe::{IdentityStore, SyntheticIdentityMutation};
use arco_catalog::{
    ArcoStateReader, CheckpointOptions, ControlMvpMaintenanceWorker, ScanRequest, StateScope,
};
use arco_core::storage::{StorageBackend, WritePrecondition};
use arco_core::{IdentityStorage, MemoryBackend};
use bytes::Bytes;
use chrono::{Duration, Utc};

#[tokio::test]
async fn identity_commit_is_scoped_and_survives_restart() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = IdentityStorage::new(backend.clone(), "acme").unwrap();
    let store = IdentityStore::new(storage.clone()).unwrap();
    let token = store
        .commit(SyntheticIdentityMutation::put(b"principal/a", b"active"))
        .await
        .unwrap();
    assert_eq!(
        IdentityStore::new(storage)
            .unwrap()
            .read_at(token.clone())
            .await
            .unwrap()
            .get(b"principal/a")
            .await
            .unwrap(),
        Some(Bytes::from_static(b"active"))
    );
    assert_eq!(
        token.scope(),
        &StateScope::tenant_identity("acme", "identity")
    );
    let paths = backend.list("tenant=acme/").await.unwrap();
    assert!(!paths.is_empty());
    assert!(
        paths
            .iter()
            .all(|object| object.path.starts_with("tenant=acme/identity/"))
    );
    assert!(
        backend
            .head("tenant=acme/identity/control/v1/domains/identity/head/current.json")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        backend
            .head("tenant=acme/workspace=identity/control/v1/domains/identity/head/current.json")
            .await
            .unwrap()
            .is_none()
    );
    let other = IdentityStore::new(IdentityStorage::new(backend, "other").unwrap()).unwrap();
    assert!(other.read_at(token).await.is_err());
}

#[tokio::test]
async fn historical_read_and_scan_cursor_stay_on_identity_root() {
    let backend = Arc::new(MemoryBackend::new());
    let store = IdentityStore::new(IdentityStorage::new(backend.clone(), "acme").unwrap()).unwrap();
    let first = store
        .commit(SyntheticIdentityMutation::put(b"p/a", b"old"))
        .await
        .unwrap();
    store
        .commit(SyntheticIdentityMutation::put(b"p/b", b"new"))
        .await
        .unwrap();
    assert_eq!(
        store
            .read_at(first.clone())
            .await
            .unwrap()
            .get(b"p/a")
            .await
            .unwrap(),
        Some(Bytes::from_static(b"old"))
    );
    let first_page = store
        .scan(ScanRequest::new(b"p/").with_limits(1, 1024, 64))
        .await
        .unwrap();
    let cursor = first_page.continuation().unwrap().clone();
    let second_page = store
        .scan(
            ScanRequest::new(b"p/")
                .with_limits(1, 1024, 64)
                .with_token(cursor.clone()),
        )
        .await
        .unwrap();
    assert_eq!(second_page.entries().len(), 1);

    let other =
        IdentityStore::new(IdentityStorage::new(backend.clone(), "other").unwrap()).unwrap();
    assert!(
        other
            .scan(
                ScanRequest::new(b"p/")
                    .with_limits(1, 1024, 64)
                    .with_token(cursor)
            )
            .await
            .is_err()
    );
    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    assert!(
        backend
            .head("tenant=acme/identity/retention/coordination/mutation-epoch.json")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        backend
            .head("tenant=acme/workspace=identity/retention/coordination/mutation-epoch.json")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .read_checkpoint(checkpoint)
            .await
            .unwrap()
            .get(b"p/a")
            .await
            .unwrap(),
        Some(Bytes::from_static(b"old"))
    );
    let workspace = arco_catalog::ControlMvpStateStore::new(
        arco_core::ScopedStorage::new(backend.clone(), "acme", "acme").unwrap(),
        StateScope::new("acme", "acme", "identity"),
    )
    .unwrap();
    assert_eq!(workspace.get(b"p/a").await.unwrap(), None);
    assert!(workspace.read_at(first.clone()).await.is_err());
    let request = arco_core::ControlPlaneScope::new("acme", "acme", "acme").unwrap();
    let metastore = arco_core::ScopedStorage::new_metastore_scoped(backend, &request).unwrap();
    let metastore = arco_catalog::ControlMvpStateStore::new(
        metastore,
        StateScope::metastore("acme", "acme", "identity"),
    )
    .unwrap();
    assert_eq!(metastore.get(b"p/a").await.unwrap(), None);
    assert!(metastore.read_at(first).await.is_err());
}

#[tokio::test]
async fn workspace_gc_cannot_reach_identity_objects() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        IdentityStore::new(IdentityStorage::new(backend.clone(), "acme").unwrap()).unwrap();
    identity
        .commit(SyntheticIdentityMutation::put(b"p/a", b"one"))
        .await
        .unwrap();
    let before = backend.list("tenant=acme/identity/").await.unwrap();
    let worker = ControlMvpMaintenanceWorker::new(
        arco_core::ScopedStorage::new(backend.clone(), "acme", "acme").unwrap(),
        StateScope::new("acme", "acme", "identity"),
    )
    .unwrap();
    let future = Utc::now() + Duration::days(40);
    let plan = worker.plan_gc_at(future, []).await.unwrap();
    assert!(plan.candidates().is_empty());
    worker.collect_gc_at(future, []).await.unwrap();
    let after = backend.list("tenant=acme/identity/").await.unwrap();
    assert_eq!(before.len(), after.len());
    for object in before {
        assert!(
            after
                .iter()
                .any(|candidate| candidate.path == object.path
                    && candidate.version == object.version)
        );
    }
}

#[tokio::test]
async fn identity_retained_history_survives_gc_until_exact_deadline() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = IdentityStorage::new(backend.clone(), "acme").unwrap();
    let store = IdentityStore::new(storage.clone()).unwrap();
    let first = store
        .commit(SyntheticIdentityMutation::put(b"p/a", b"old"))
        .await
        .unwrap();
    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .unwrap();
    let deadline = Utc::now() + Duration::days(90);
    let state_ref = store.protect_state_token(&first, deadline).await.unwrap();
    let checkpoint_ref = store
        .protect_checkpoint_token(&checkpoint, deadline)
        .await
        .unwrap();
    store
        .commit(SyntheticIdentityMutation::put(b"p/a", b"new"))
        .await
        .unwrap();

    let retained_at = Utc::now() + Duration::days(40);
    store.collect_gc_at(retained_at).await.unwrap();
    let restarted = IdentityStore::new(storage).unwrap();
    for reference in [&state_ref, &checkpoint_ref] {
        assert_eq!(
            restarted
                .resolve_reference_at(reference, retained_at)
                .await
                .unwrap()
                .get(b"p/a")
                .await
                .unwrap(),
            Some(Bytes::from_static(b"old"))
        );
    }
    assert_eq!(
        restarted
            .read_checkpoint(checkpoint)
            .await
            .unwrap()
            .get(b"p/a")
            .await
            .unwrap(),
        Some(Bytes::from_static(b"old"))
    );

    let expired_at = Utc::now() + Duration::days(95);
    assert!(
        restarted
            .resolve_reference_at(&state_ref, expired_at)
            .await
            .is_err()
    );
    let mut cursor = None;
    loop {
        let page = restarted
            .collect_gc_page_at(expired_at, cursor.as_deref())
            .await
            .unwrap();
        cursor = page.continuation().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert!(
        backend
            .head(&format!(
                "tenant=acme/identity/{}",
                state_ref.manifest_path()
            ))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        restarted.get(b"p/a").await.unwrap(),
        Some(Bytes::from_static(b"new"))
    );
    assert!(
        backend
            .list("tenant=acme/identity/retention/identity-references/")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn malformed_identity_reference_blocks_gc_before_any_delete() {
    let backend = Arc::new(MemoryBackend::new());
    let store = IdentityStore::new(IdentityStorage::new(backend.clone(), "acme").unwrap()).unwrap();
    store
        .commit(SyntheticIdentityMutation::put(b"p/a", b"live"))
        .await
        .unwrap();
    let orphan = "tenant=acme/identity/control/v1/domains/identity/transactions/orphan.json";
    backend
        .put(
            orphan,
            Bytes::from_static(b"orphan"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    backend
        .put(
            "tenant=acme/identity/retention/identity-references/forged.json",
            Bytes::from_static(b"{}"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();

    assert!(
        store
            .collect_gc_at(Utc::now() + Duration::days(40))
            .await
            .is_err()
    );
    assert!(backend.head(orphan).await.unwrap().is_some());
}

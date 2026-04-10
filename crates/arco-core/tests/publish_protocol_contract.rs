#![allow(clippy::expect_used)]
#![allow(missing_docs)]

#[path = "support/barrier_backend.rs"]
mod barrier_backend;
#[path = "support/scripted_backend.rs"]
mod scripted_backend;

use std::sync::Arc;

use arco_core::CatalogDomain;
use arco_core::publish::{
    SnapshotPointerDurability, SnapshotPointerPublishOutcome, publish_snapshot_pointer_transaction,
};
use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};
use arco_core::{CatalogPaths, ScopedStorage};
use barrier_backend::{BarrierBackend, BarrierMatch};
use bytes::Bytes;
use scripted_backend::{PreconditionMatcher, ScriptedBackend, ScriptedEffect, ScriptedRule};

async fn scoped_storage(backend: Arc<dyn StorageBackend>) -> ScopedStorage {
    ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage")
}

#[tokio::test]
async fn visible_publish_race_keeps_pointer_at_old_or_winner_only() {
    let backend: Arc<dyn StorageBackend> = Arc::new(BarrierBackend::new(
        Arc::new(arco_core::MemoryBackend::new()),
        2,
        BarrierMatch::matches_version(CatalogPaths::domain_manifest_pointer(
            CatalogDomain::Catalog,
        )),
    ));
    let storage = scoped_storage(backend.clone()).await;
    let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);

    let seed = storage
        .put_raw(
            &pointer_path,
            Bytes::from_static(
                br#"{"manifestId":"seed","manifestPath":"manifests/catalog/seed.json","epoch":0}"#,
            ),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("seed pointer");
    let seed_version = match seed {
        WriteResult::Success { version } => version,
        WriteResult::PreconditionFailed { .. } => panic!("seed pointer must succeed"),
    };

    let left = {
        let storage = storage.clone();
        let pointer_path = pointer_path.clone();
        let seed_version = seed_version.clone();
        tokio::spawn(async move {
            publish_snapshot_pointer_transaction(
                &storage,
                "manifests/catalog/left.json",
                Bytes::from_static(br#"{"manifestId":"left"}"#),
                &pointer_path,
                Bytes::from_static(
                    br#"{"manifestId":"left","manifestPath":"manifests/catalog/left.json","epoch":1}"#,
                ),
                Some(seed_version.as_str()),
                None,
                SnapshotPointerDurability::Visible,
                async { Ok(()) },
            )
            .await
        })
    };
    let right = {
        let storage = storage.clone();
        let pointer_path = pointer_path.clone();
        let seed_version = seed_version.clone();
        tokio::spawn(async move {
            publish_snapshot_pointer_transaction(
                &storage,
                "manifests/catalog/right.json",
                Bytes::from_static(br#"{"manifestId":"right"}"#),
                &pointer_path,
                Bytes::from_static(
                    br#"{"manifestId":"right","manifestPath":"manifests/catalog/right.json","epoch":1}"#,
                ),
                Some(seed_version.as_str()),
                None,
                SnapshotPointerDurability::Visible,
                async { Ok(()) },
            )
            .await
        })
    };

    let left = left.await.expect("left task");
    let right = right.await.expect("right task");

    let successes = [left.as_ref(), right.as_ref()]
        .iter()
        .filter(|result| matches!(result, Ok(SnapshotPointerPublishOutcome::Visible { .. })))
        .count();
    assert_eq!(
        successes, 1,
        "exactly one pointer CAS publish must become visible"
    );

    let head = storage
        .head_raw(&pointer_path)
        .await
        .expect("pointer head")
        .expect("pointer exists");
    assert_ne!(
        head.version, seed_version,
        "visible pointer head must advance to the winner"
    );
}

#[tokio::test]
async fn persisted_publish_conflict_returns_persisted_not_visible_without_advancing_pointer() {
    let backend: Arc<dyn StorageBackend> = Arc::new(arco_core::MemoryBackend::new());
    let storage = scoped_storage(backend).await;
    let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
    storage
        .put_raw(
            &pointer_path,
            Bytes::from_static(
                br#"{"manifestId":"seed","manifestPath":"manifests/catalog/seed.json","epoch":0}"#,
            ),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("seed pointer");
    let head_before = storage
        .head_raw(&pointer_path)
        .await
        .expect("head before")
        .expect("pointer exists");

    let outcome = publish_snapshot_pointer_transaction(
        &storage,
        "manifests/catalog/persisted.json",
        Bytes::from_static(br#"{"manifestId":"persisted"}"#),
        &pointer_path,
        Bytes::from_static(
            br#"{"manifestId":"persisted","manifestPath":"manifests/catalog/persisted.json","epoch":1}"#,
        ),
        Some("stale-version"),
        None,
        SnapshotPointerDurability::Persisted,
        async { Ok(()) },
    )
    .await
    .expect("persisted publish outcome");

    assert_eq!(outcome, SnapshotPointerPublishOutcome::PersistedNotVisible);
    let head_after = storage
        .head_raw(&pointer_path)
        .await
        .expect("head after")
        .expect("pointer exists");
    assert_eq!(
        head_after.version, head_before.version,
        "persisted-not-visible publish must leave the visible pointer unchanged"
    );
}

#[tokio::test]
async fn immutable_snapshot_write_rejects_overwrite_attempts() {
    let backend = Arc::new(ScriptedBackend::new());
    let storage = scoped_storage(backend.clone()).await;
    let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
    storage
        .put_raw(
            &pointer_path,
            Bytes::from_static(
                br#"{"manifestId":"seed","manifestPath":"manifests/catalog/seed.json","epoch":0}"#,
            ),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("seed pointer");

    backend.add_rule(ScriptedRule::put(
        "tenant=tenant/workspace=workspace/manifests/catalog/reused.json",
        PreconditionMatcher::DoesNotExist,
        1,
        ScriptedEffect::PreconditionFailed {
            current_version: "existing-version".to_string(),
        },
    ));

    let error = publish_snapshot_pointer_transaction(
        &storage,
        "manifests/catalog/reused.json",
        Bytes::from_static(br#"{"manifestId":"reused"}"#),
        &pointer_path,
        Bytes::from_static(
            br#"{"manifestId":"reused","manifestPath":"manifests/catalog/reused.json","epoch":1}"#,
        ),
        Some("stale-version"),
        None,
        SnapshotPointerDurability::Visible,
        async { Ok(()) },
    )
    .await
    .expect_err("immutable snapshot overwrite must fail");

    assert!(
        error
            .to_string()
            .contains("immutable manifest snapshot already exists"),
        "unexpected error: {error}"
    );
}

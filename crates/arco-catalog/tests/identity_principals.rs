//! Test-only tenant principal semantics over the identity authority.
#![cfg(feature = "test-utils")]

use std::collections::BTreeSet;
use std::sync::Arc;

use arco_catalog::metastore::events::PrincipalKind;
use arco_catalog::state_store::identity_probe::{
    IdentityMutation, IdentityOrigin, IdentityState, PrincipalIdentityStore,
};
use arco_catalog::{ArcoStateTxn, ControlMvpStateStore, StateScope, TxnOptions};
use arco_core::{IdentityStorage, MemoryBackend, ScopedStorage};
use bytes::Bytes;

#[tokio::test]
async fn replay_and_restart_preserve_creation_disable_and_membership_revisions() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = IdentityStorage::new(backend.clone(), "tenant-a").unwrap();
    let store = PrincipalIdentityStore::new(storage.clone()).unwrap();
    let origin = Some(IdentityOrigin {
        workspace_id: Some("workspace-a".into()),
        metastore_id: Some("metastore-a".into()),
    });
    let (group, _) = store
        .commit(
            IdentityMutation::Create {
                name: "editors".into(),
                kind: PrincipalKind::Group,
            },
            None,
        )
        .await
        .unwrap();
    let (created, created_token) = store
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            origin.clone(),
        )
        .await
        .unwrap();
    assert_ne!(group.principal_id, created.principal_id);
    assert_eq!(created.tenant_id, "tenant-a");
    assert_eq!(created.origin, origin);
    let id = created.principal_id.clone();
    let (revised, _) = store
        .commit(
            IdentityMutation::ReviseMembership {
                principal_id: id.clone(),
                expected_revision: 0,
                group_ids: [group.principal_id.clone()].into(),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(revised.sequence, created.sequence + 1);
    let (disabled, _) = store
        .commit(
            IdentityMutation::Disable {
                principal_id: id.clone(),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(disabled.sequence, revised.sequence + 1);
    let restarted = PrincipalIdentityStore::new(storage).unwrap();
    let replayed = restarted.replay().await.unwrap();
    assert_eq!(replayed.principals[&id].membership_revision, 1);
    assert_eq!(
        replayed.principals[&id].group_ids,
        [group.principal_id].into()
    );
    assert!(!replayed.principals[&id].active);
    assert!(restarted.replay_at(created_token).await.unwrap().principals[&id].active);
    assert_eq!(replayed.events.last(), Some(&disabled));
    let mut rebuilt = IdentityState::default();
    for event in replayed.events.clone() {
        rebuilt.apply_event(event).unwrap();
    }
    assert_eq!(rebuilt, replayed);
    let mut recycled = created;
    recycled.sequence = disabled.sequence + 1;
    recycled.event_id = format!("identity-{}", recycled.sequence);
    assert!(rebuilt.apply_event(recycled).is_err());
}

#[tokio::test]
async fn rejected_transitions_do_not_advance_authority() {
    let backend = Arc::new(MemoryBackend::new());
    let store =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "a").unwrap()).unwrap();
    let (created, _) = store
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let id = created.principal_id;
    assert!(
        store
            .commit(
                IdentityMutation::ReviseMembership {
                    principal_id: id.clone(),
                    expected_revision: 1,
                    group_ids: BTreeSet::default(),
                },
                None
            )
            .await
            .is_err()
    );
    assert!(
        store
            .commit(
                IdentityMutation::ReviseMembership {
                    principal_id: id.clone(),
                    expected_revision: 0,
                    group_ids: ["missing-group".into()].into(),
                },
                None
            )
            .await
            .is_err()
    );
    store
        .commit(
            IdentityMutation::Disable {
                principal_id: id.clone(),
            },
            None,
        )
        .await
        .unwrap();
    assert!(
        store
            .commit(
                IdentityMutation::Disable {
                    principal_id: id.clone()
                },
                None
            )
            .await
            .is_err()
    );
    assert!(
        store
            .commit(
                IdentityMutation::ReviseMembership {
                    principal_id: id.clone(),
                    expected_revision: 0,
                    group_ids: BTreeSet::default(),
                },
                None
            )
            .await
            .is_err()
    );
    let (another, _) = store
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    assert_ne!(another.principal_id, id);
}

#[tokio::test]
async fn foreign_workspace_and_tenant_tokens_are_rejected() {
    let backend = Arc::new(MemoryBackend::new());
    let store =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "a").unwrap()).unwrap();
    let (_, token) = store
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let workspace = ControlMvpStateStore::new(
        ScopedStorage::new(backend.clone(), "a", "workspace-a").unwrap(),
        StateScope::new("a", "workspace-a", "principals"),
    )
    .unwrap();
    let mut txn = workspace
        .begin_control_txn(TxnOptions::new(None))
        .await
        .unwrap();
    txn.put(b"marker", Bytes::from_static(b"workspace"))
        .await
        .unwrap();
    let workspace_token = txn.commit().await.unwrap().into_state_token();
    assert!(store.replay_at(workspace_token).await.is_err());
    let other = PrincipalIdentityStore::new(IdentityStorage::new(backend, "b").unwrap()).unwrap();
    assert!(other.replay_at(token).await.is_err());
    assert!(other.replay().await.unwrap().principals.is_empty());
}

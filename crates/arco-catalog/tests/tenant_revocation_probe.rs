//! Test-only named-cut revocation across two metastore roots.
#![cfg(feature = "test-utils")]
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    reason = "the integration proof keeps both authority roots and exact token assertions together"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arco_catalog::authz::compiler::SecurableObject;
use arco_catalog::authz::decision::{AuthzRequest, DecisionOutcome};
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::authz::tenant_probe::{METASTORE_AUTHZ_STATE_KEY, TenantAuthorizationProbe};
use arco_catalog::metastore::events::{GrantRecord, LifecycleState, PrincipalKind};
use arco_catalog::metastore::replay::MetastoreState;
use arco_catalog::state_store::identity_probe::{IdentityMutation, PrincipalIdentityStore};
use arco_catalog::{
    ArcoStateReader, ArcoStateTxn, ControlMvpStateStore, StateScope, StateToken, TxnOptions,
};
use arco_core::{ControlPlaneScope, IdentityStorage, MemoryBackend, ScopedStorage};
use bytes::Bytes;
use chrono::Utc;

#[tokio::test]
async fn disable_denies_cached_and_recompiled_permissions_in_both_metastores() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = IdentityStorage::new(backend.clone(), "tenant-a").unwrap();
    let identity = PrincipalIdentityStore::new(storage.clone()).unwrap();
    let (user, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let (group, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "readers".into(),
                kind: PrincipalKind::Group,
            },
            None,
        )
        .await
        .unwrap();
    identity
        .commit(
            IdentityMutation::ReviseMembership {
                principal_id: user.principal_id.clone(),
                expected_revision: 0,
                group_ids: [group.principal_id.clone()].into(),
            },
            None,
        )
        .await
        .unwrap();

    let first = metastore(backend.clone(), "first");
    let second = metastore(backend, "second");
    let first_token = publish(&first, &grant_state(&user.principal_id)).await;
    let second_token = publish(&second, &grant_state(&group.principal_id)).await;
    let request = AuthzRequest::new(&user.principal_id, "table", "TABLE", Privilege::Select);
    let first_probe = TenantAuthorizationProbe::new(&identity, &first);
    let second_probe = TenantAuthorizationProbe::new(&identity, &second);
    let first_cut = first_probe.compile_current().await.unwrap();
    let second_cut = second_probe.compile_current().await.unwrap();
    assert_eq!(first_cut.metastore_token(), &first_token);
    assert_eq!(second_cut.metastore_token(), &second_token);
    assert_eq!(first_cut.identity_token(), second_cut.identity_token());
    assert_eq!(
        first_probe.evaluate(&first_cut, &request).await.outcome,
        DecisionOutcome::Allow
    );
    assert_eq!(
        second_probe.evaluate(&second_cut, &request).await.outcome,
        DecisionOutcome::Allow
    );

    let concurrent_grant = grant_state(&user.principal_id);
    let (disabled, _) = tokio::join!(
        async {
            let result = identity
                .commit(
                    IdentityMutation::Disable {
                        principal_id: user.principal_id.clone(),
                    },
                    None,
                )
                .await;
            (result, Utc::now())
        },
        publish(&first, &concurrent_grant),
    );
    let (disabled_result, disable_observed_at) = disabled;
    let (_, disable_token) = disabled_result.unwrap();
    for (metastore_id, probe, cut) in [
        ("first", &first_probe, &first_cut),
        ("second", &second_probe, &second_cut),
    ] {
        let denied = probe.evaluate(cut, &request).await;
        println!(
            "revocation receipt: metastore={metastore_id} cut_identity={:?} cut_metastore={:?} disable_identity={:?} disable_observed_at={} denied_at={} reason={}",
            cut.identity_token(),
            cut.metastore_token(),
            disable_token,
            disable_observed_at.to_rfc3339(),
            Utc::now().to_rfc3339(),
            denied.reason_code,
        );
        assert_eq!(denied.outcome, DecisionOutcome::Deny);
        assert_eq!(denied.reason_code, "stale_authority_cut");
        let refreshed = probe.compile_current().await.unwrap();
        assert_eq!(refreshed.identity_token(), &disable_token);
        assert_eq!(
            probe.evaluate(&refreshed, &request).await.outcome,
            DecisionOutcome::Deny
        );
    }

    // Historical catalog data remains readable, but cannot select old identity authority.
    assert_eq!(
        first
            .read_at(first_token)
            .await
            .unwrap()
            .get(b"catalog/history-probe")
            .await
            .unwrap(),
        Some(Bytes::from_static(b"historical catalog data"))
    );
    let restarted = PrincipalIdentityStore::new(storage).unwrap();
    let restarted_probe = TenantAuthorizationProbe::new(&restarted, &second);
    assert_eq!(
        restarted_probe
            .evaluate(&second_cut, &request)
            .await
            .outcome,
        DecisionOutcome::Deny
    );
}

#[tokio::test]
async fn membership_revision_invalidates_a_cached_cut() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (user, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let (group, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "readers".into(),
                kind: PrincipalKind::Group,
            },
            None,
        )
        .await
        .unwrap();
    let store = metastore(backend, "first");
    publish(&store, &grant_state(&group.principal_id)).await;
    let request = AuthzRequest::new(&user.principal_id, "table", "TABLE", Privilege::Select);
    let probe = TenantAuthorizationProbe::new(&identity, &store);
    let before = probe.compile_current().await.unwrap();
    assert_eq!(
        probe.evaluate(&before, &request).await.outcome,
        DecisionOutcome::Deny
    );

    identity
        .commit(
            IdentityMutation::ReviseMembership {
                principal_id: user.principal_id.clone(),
                expected_revision: 0,
                group_ids: [group.principal_id.clone()].into(),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        probe.evaluate(&before, &request).await.reason_code,
        "stale_authority_cut"
    );
    let member_cut = probe.compile_current().await.unwrap();
    assert_eq!(
        probe.evaluate(&member_cut, &request).await.outcome,
        DecisionOutcome::Allow
    );

    identity
        .commit(
            IdentityMutation::ReviseMembership {
                principal_id: user.principal_id.clone(),
                expected_revision: 1,
                group_ids: BTreeSet::default(),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        probe.evaluate(&member_cut, &request).await.reason_code,
        "stale_authority_cut"
    );
    let removed = probe.compile_current().await.unwrap();
    assert_eq!(
        probe.evaluate(&removed, &request).await.outcome,
        DecisionOutcome::Deny
    );
}

#[tokio::test]
async fn foreign_or_advanced_metastore_cut_is_rejected() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (user, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let store = metastore(backend.clone(), "first");
    publish(&store, &grant_state(&user.principal_id)).await;
    let request = AuthzRequest::new(&user.principal_id, "table", "TABLE", Privilege::Select);
    let probe = TenantAuthorizationProbe::new(&identity, &store);
    let cut = probe.compile_current().await.unwrap();
    assert_eq!(
        probe.evaluate(&cut, &request).await.outcome,
        DecisionOutcome::Allow
    );

    publish(&store, &MetastoreState::empty()).await;
    assert_eq!(
        probe.evaluate(&cut, &request).await.reason_code,
        "stale_authority_cut"
    );
    assert_eq!(
        probe
            .evaluate(&probe.compile_current().await.unwrap(), &request)
            .await
            .outcome,
        DecisionOutcome::Deny
    );

    let foreign = metastore(backend.clone(), "other");
    publish(&foreign, &grant_state(&user.principal_id)).await;
    let foreign_probe = TenantAuthorizationProbe::new(&identity, &foreign);
    assert_eq!(
        foreign_probe.evaluate(&cut, &request).await.outcome,
        DecisionOutcome::Deny
    );
    let workspace = ControlMvpStateStore::new(
        ScopedStorage::new(backend.clone(), "tenant-a", "workspace").unwrap(),
        StateScope::new("tenant-a", "workspace", "catalog"),
    )
    .unwrap();
    publish(&workspace, &grant_state(&user.principal_id)).await;
    assert!(
        TenantAuthorizationProbe::new(&identity, &workspace)
            .compile_current()
            .await
            .is_err()
    );
    let other_identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend, "tenant-b").unwrap()).unwrap();
    other_identity
        .commit(
            IdentityMutation::Create {
                name: "bob".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let foreign_tenant_probe = TenantAuthorizationProbe::new(&other_identity, &store);
    assert!(foreign_tenant_probe.compile_current().await.is_err());
}

#[tokio::test]
async fn missing_or_unreadable_authority_denies() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = IdentityStorage::new(backend.clone(), "tenant-a").unwrap();
    let identity = PrincipalIdentityStore::new(storage).unwrap();
    let store = metastore(backend, "first");
    let probe = TenantAuthorizationProbe::new(&identity, &store);
    assert!(probe.compile_current().await.is_err());

    let (user, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let mut txn = store
        .begin_control_txn(TxnOptions::new(None))
        .await
        .unwrap();
    txn.put(b"other", Bytes::from_static(b"unrelated"))
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert!(probe.compile_current().await.is_err());
    let cut = {
        publish(&store, &grant_state(&user.principal_id)).await;
        probe.compile_current().await.unwrap()
    };
    let missing_identity = PrincipalIdentityStore::new(
        IdentityStorage::new(Arc::new(MemoryBackend::new()), "tenant-a").unwrap(),
    )
    .unwrap();
    let missing_probe = TenantAuthorizationProbe::new(&missing_identity, &store);
    let request = AuthzRequest::new(&user.principal_id, "table", "TABLE", Privilege::Select);
    assert_eq!(
        missing_probe.evaluate(&cut, &request).await.outcome,
        DecisionOutcome::Deny
    );
}

fn metastore(backend: Arc<MemoryBackend>, id: &str) -> ControlMvpStateStore {
    let request = ControlPlaneScope::new("tenant-a", "workspace", id).unwrap();
    let storage = ScopedStorage::new_metastore_scoped(backend, &request).unwrap();
    ControlMvpStateStore::new(storage, StateScope::metastore("tenant-a", id, "catalog")).unwrap()
}

fn grant_state(principal_id: &str) -> MetastoreState {
    let mut state = MetastoreState::empty();
    state.ledger_watermark = Some("grant-1".into());
    state.grants.insert(
        "grant-1".into(),
        GrantRecord {
            grant_id: "grant-1".into(),
            object_id: "table".into(),
            object_type: "TABLE".into(),
            principal_id: principal_id.into(),
            privilege: "SELECT".into(),
            owner: "admin".into(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 0,
            properties: BTreeMap::new(),
            identity_cut: None,
        },
    );
    state
}

async fn publish(store: &ControlMvpStateStore, state: &MetastoreState) -> StateToken {
    let securables = [SecurableObject::new(
        "table",
        "TABLE",
        None,
        "unassigned-owner",
    )];
    let mut txn = store
        .begin_control_txn(TxnOptions::new(None))
        .await
        .unwrap();
    txn.put(
        METASTORE_AUTHZ_STATE_KEY,
        Bytes::from(serde_json::to_vec(&(state, securables)).unwrap()),
    )
    .await
    .unwrap();
    txn.put(
        b"catalog/history-probe",
        Bytes::from_static(b"historical catalog data"),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap().into_state_token()
}

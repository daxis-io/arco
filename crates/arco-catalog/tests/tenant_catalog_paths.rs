//! Test-only tenant identity admission against native grants and catalog reads.
#![cfg(feature = "test-utils")]
#![allow(clippy::too_many_lines, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arco_catalog::authz::decision::{AuthzRequest, DecisionOutcome};
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::authz::tenant_probe::TenantCatalogProbe;
use arco_catalog::manifest::{CatalogDomainManifest, DomainManifestPointer};
use arco_catalog::metastore::events::{
    CatalogObjectRecord, GrantRecord, LifecycleState, MetastoreEvent, MetastoreMutation,
    PrincipalKind,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::state_store::identity_probe::{IdentityMutation, PrincipalIdentityStore};
use arco_catalog::{
    CatalogReader, CatalogWriter, ColumnDefinition, RegisterTableRequest, Tier1Compactor,
    WriteOptions,
};
use arco_core::control_plane_transactions::{
    ControlPlaneTxDomain, ControlPlaneTxKind, ControlPlaneTxPaths, ControlPlaneTxStatus,
    RootTxManifest, RootTxManifestDomain, RootTxReceipt, RootTxRecord,
};
use arco_core::storage::WritePrecondition;
use arco_core::{
    CatalogDomain, CatalogPaths, ControlPlaneScope, IdentityStorage, MemoryBackend, ScopedStorage,
};
use bytes::Bytes;
use chrono::Utc;

#[tokio::test]
async fn native_grants_and_catalog_reads_follow_current_identity_in_two_metastores() {
    let backend = Arc::new(MemoryBackend::new());
    let identity_storage = IdentityStorage::new(backend.clone(), "tenant-a").unwrap();
    let identity = PrincipalIdentityStore::new(identity_storage.clone()).unwrap();
    let (principal, created_at) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();

    let first_storage = metastore_storage(backend.clone(), "first");
    let second_storage = metastore_storage(backend, "second");
    let first_table = seed_table_and_object(&first_storage).await;
    let second_table = seed_table_and_object(&second_storage).await;
    let first = TenantCatalogProbe::new(&identity, first_storage.clone()).unwrap();
    let second = TenantCatalogProbe::new(&identity, second_storage.clone()).unwrap();
    first
        .append_grant(
            "same-grant-event",
            2,
            grant(&principal.principal_id, &first_table),
        )
        .await
        .unwrap();
    second
        .append_grant(
            "same-grant-event",
            2,
            grant(&principal.principal_id, &second_table),
        )
        .await
        .unwrap();

    for (storage, table_id) in [
        (&first_storage, &first_table),
        (&second_storage, &second_table),
    ] {
        let grant = MetastoreLedger::new(storage.clone())
            .unwrap()
            .replay()
            .await
            .unwrap()
            .grants
            .into_values()
            .next()
            .unwrap();
        let evidence = grant.identity_cut.unwrap();
        assert_eq!(evidence.tenant_id, "tenant-a");
        assert_eq!(
            evidence.identity_manifest_id,
            created_at.authority_manifest_id()
        );
        assert_eq!(evidence.membership_revision, 0);
        assert_eq!(grant.object_id, *table_id);
    }

    let first_cut = first.compile_current().await.unwrap();
    let second_cut = second.compile_current().await.unwrap();
    assert_ne!(
        first_cut.metastore_token().scope(),
        second_cut.metastore_token().scope()
    );
    let first_request = request(&principal.principal_id, &first_table);
    let second_request = request(&principal.principal_id, &second_table);
    assert_eq!(
        second.evaluate(&first_cut, &first_request).await.outcome,
        DecisionOutcome::Deny,
        "a cut from another metastore cannot authorize a matching watermark"
    );
    assert!(
        first
            .read_table(&first_cut, &first_request)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        second
            .read_table(&second_cut, &second_request)
            .await
            .unwrap()
            .is_some()
    );

    identity
        .commit(
            IdentityMutation::Disable {
                principal_id: principal.principal_id.clone(),
            },
            None,
        )
        .await
        .unwrap();
    assert!(first.read_table(&first_cut, &first_request).await.is_err());
    assert!(
        second
            .read_table(&second_cut, &second_request)
            .await
            .is_err()
    );
    assert!(
        first
            .read_table(&first.compile_current().await.unwrap(), &first_request)
            .await
            .is_err()
    );
    assert!(
        second
            .read_table(&second.compile_current().await.unwrap(), &second_request)
            .await
            .is_err()
    );

    let restarted = PrincipalIdentityStore::new(identity_storage).unwrap();
    let reopened = TenantCatalogProbe::new(&restarted, first_storage).unwrap();
    assert!(
        reopened
            .read_table(&first_cut, &first_request)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn concurrent_disable_and_grant_never_restore_catalog_access() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    let (disable, append) = tokio::join!(
        identity.commit(
            IdentityMutation::Disable {
                principal_id: principal.principal_id.clone(),
            },
            None,
        ),
        probe.append_grant("racing-grant", 2, grant(&principal.principal_id, &table_id)),
    );
    disable.unwrap();
    let replayed = MetastoreLedger::new(storage)
        .unwrap()
        .replay()
        .await
        .unwrap();
    if append.is_ok() {
        assert!(replayed.grants.contains_key("grant"));
    }
    if let Some(grant) = replayed.grants.get("grant") {
        assert!(grant.identity_cut.is_some());
    }
    if let Ok(cut) = probe.compile_current().await {
        assert!(
            probe
                .read_table(&cut, &request(&principal.principal_id, &table_id))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn foreign_tenant_and_workspace_roots_cannot_admit_grants() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-b").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "bob".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend.clone(), "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    assert!(
        probe
            .append_grant(
                "foreign-grant",
                2,
                grant(&principal.principal_id, &table_id)
            )
            .await
            .is_err()
    );
    assert!(
        MetastoreLedger::new(storage)
            .unwrap()
            .replay()
            .await
            .unwrap()
            .grants
            .is_empty()
    );
    let workspace = ScopedStorage::new(backend, "tenant-b", "workspace").unwrap();
    assert!(TenantCatalogProbe::new(&identity, workspace).is_err());
}

#[tokio::test]
async fn admission_only_accepts_active_grant_creation() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    let mut disabled = grant(&principal.principal_id, &table_id);
    disabled.lifecycle_state = LifecycleState::Disabled;
    assert!(
        probe
            .append_grant("disabled-grant", 2, disabled)
            .await
            .is_err()
    );
    assert!(
        MetastoreLedger::new(storage)
            .unwrap()
            .replay()
            .await
            .unwrap()
            .grants
            .is_empty()
    );
}

#[tokio::test]
async fn unwitnessed_native_ledger_advance_cannot_compile_a_new_cut() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    probe
        .append_grant("grant", 2, grant(&principal.principal_id, &table_id))
        .await
        .unwrap();
    let cut = probe.compile_current().await.unwrap();
    let scope = ControlPlaneScope::new("tenant-a", "workspace", "first").unwrap();
    MetastoreLedger::new(storage)
        .unwrap()
        .append_event(&MetastoreEvent::new_scoped(
            &scope,
            "unwitnessed-object",
            3,
            MetastoreMutation::CatalogObjectUpserted(CatalogObjectRecord {
                object_id: "unwitnessed".into(),
                object_type: "TABLE".into(),
                qualified_name: "default.unwitnessed".into(),
                owner: "unassigned-owner".into(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 0,
                properties: BTreeMap::new(),
            }),
        ))
        .await
        .unwrap();
    let request = request(&principal.principal_id, &table_id);
    assert_eq!(
        probe.evaluate(&cut, &request).await.outcome,
        DecisionOutcome::Deny
    );
    assert!(probe.compile_current().await.is_err());
}

#[tokio::test]
async fn prepared_grant_recovers_after_restart_without_reauthorizing_a_disabled_principal() {
    let backend = Arc::new(MemoryBackend::new());
    let identity_storage = IdentityStorage::new(backend.clone(), "tenant-a").unwrap();
    let identity = PrincipalIdentityStore::new(identity_storage.clone()).unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    let prepared = probe
        .prepare_grant(
            "interrupted-grant",
            2,
            grant(&principal.principal_id, &table_id),
        )
        .await
        .unwrap();
    MetastoreLedger::new(storage.clone())
        .unwrap()
        .append_event(&prepared)
        .await
        .unwrap();
    assert!(probe.compile_current().await.is_err());

    identity
        .commit(
            IdentityMutation::Disable {
                principal_id: principal.principal_id.clone(),
            },
            None,
        )
        .await
        .unwrap();
    let restarted_identity = PrincipalIdentityStore::new(identity_storage).unwrap();
    let restarted = TenantCatalogProbe::new(&restarted_identity, storage).unwrap();
    let watermark = restarted.reconcile_prepared_grant().await.unwrap();
    assert_eq!(watermark.event_id, "interrupted-grant");
    let cut = restarted.compile_current().await.unwrap();
    assert!(
        restarted
            .read_table(&cut, &request(&principal.principal_id, &table_id))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn recovery_rejects_absent_or_changed_event_and_preserves_denial() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    let mut prepared = probe
        .prepare_grant(
            "interrupted-grant",
            2,
            grant(&principal.principal_id, &table_id),
        )
        .await
        .unwrap();
    assert!(
        probe
            .prepare_grant("second-grant", 3, grant(&principal.principal_id, &table_id))
            .await
            .is_err(),
        "one pending grant fences another admission attempt"
    );
    assert!(probe.reconcile_prepared_grant().await.is_err());
    assert!(probe.compile_current().await.is_err());

    if let MetastoreMutation::GrantUpserted(ref mut grant) = prepared.mutation {
        grant.privilege = "MODIFY".into();
    }
    MetastoreLedger::new(storage)
        .unwrap()
        .append_event(&prepared)
        .await
        .unwrap();
    assert!(probe.reconcile_prepared_grant().await.is_err());
    assert!(probe.compile_current().await.is_err());
}

#[tokio::test]
async fn absent_prepared_grant_can_be_aborted_after_restart_without_reusing_its_slot() {
    let backend = Arc::new(MemoryBackend::new());
    let identity_storage = IdentityStorage::new(backend.clone(), "tenant-a").unwrap();
    let identity = PrincipalIdentityStore::new(identity_storage.clone()).unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    let prepared = probe
        .prepare_grant("lost-grant", 2, grant(&principal.principal_id, &table_id))
        .await
        .unwrap();
    assert!(probe.compile_current().await.is_err());

    let restarted_identity = PrincipalIdentityStore::new(identity_storage).unwrap();
    let restarted = TenantCatalogProbe::new(&restarted_identity, storage.clone()).unwrap();
    let aborted = restarted.abort_prepared_grant().await.unwrap();
    assert_eq!(aborted.event_id, "lost-grant");
    assert_eq!(aborted.sequence, 2);
    assert!(
        MetastoreLedger::new(storage.clone())
            .unwrap()
            .append_event(&prepared)
            .await
            .is_err()
    );
    assert!(
        MetastoreLedger::new(storage.clone())
            .unwrap()
            .replay()
            .await
            .unwrap()
            .grants
            .is_empty()
    );
    assert!(restarted.compile_current().await.is_ok());

    restarted
        .append_grant("next-grant", 3, grant(&principal.principal_id, &table_id))
        .await
        .unwrap();
    let cut = restarted.compile_current().await.unwrap();
    assert!(
        restarted
            .read_table(&cut, &request(&principal.principal_id, &table_id))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn abort_refuses_a_grant_that_already_won_the_native_event_slot() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    let prepared = probe
        .prepare_grant(
            "prepared-grant",
            2,
            grant(&principal.principal_id, &table_id),
        )
        .await
        .unwrap();
    MetastoreLedger::new(storage)
        .unwrap()
        .append_event(&prepared)
        .await
        .unwrap();
    assert!(probe.abort_prepared_grant().await.is_err());
    assert!(probe.compile_current().await.is_err());
    probe.reconcile_prepared_grant().await.unwrap();
    assert!(probe.compile_current().await.is_ok());
}

#[tokio::test]
async fn appended_abort_recovers_its_witness_after_restart() {
    let backend = Arc::new(MemoryBackend::new());
    let identity_storage = IdentityStorage::new(backend.clone(), "tenant-a").unwrap();
    let identity = PrincipalIdentityStore::new(identity_storage.clone()).unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    let mut prepared = probe
        .prepare_grant(
            "interrupted-abort",
            2,
            grant(&principal.principal_id, &table_id),
        )
        .await
        .unwrap();
    prepared.mutation = MetastoreMutation::GrantAdmissionAborted {
        grant_id: "grant".into(),
    };
    MetastoreLedger::new(storage.clone())
        .unwrap()
        .append_event(&prepared)
        .await
        .unwrap();
    assert!(probe.compile_current().await.is_err());

    let restarted_identity = PrincipalIdentityStore::new(identity_storage).unwrap();
    let restarted = TenantCatalogProbe::new(&restarted_identity, storage.clone()).unwrap();
    restarted.abort_prepared_grant().await.unwrap();
    assert!(restarted.compile_current().await.is_ok());
    assert!(
        MetastoreLedger::new(storage)
            .unwrap()
            .replay()
            .await
            .unwrap()
            .grants
            .is_empty()
    );
}

#[tokio::test]
async fn abort_completes_a_partial_native_append_without_an_event() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    let prepared = probe
        .prepare_grant(
            "partial-grant",
            2,
            grant(&principal.principal_id, &table_id),
        )
        .await
        .unwrap();
    storage
        .put_raw(
            "ledger/metastore-sequences/00000000000000000002.event_id",
            Bytes::from_static(b"partial-grant"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    storage
        .put_raw(
            "ledger/metastore-latest/pending.json",
            Bytes::from_static(br#"{"event_id":"partial-grant","sequence":2}"#),
            WritePrecondition::None,
        )
        .await
        .unwrap();
    assert!(
        MetastoreLedger::new(storage.clone())
            .unwrap()
            .latest_watermark()
            .await
            .is_err()
    );
    probe.abort_prepared_grant().await.unwrap();
    assert!(probe.compile_current().await.is_ok());
    assert!(
        MetastoreLedger::new(storage)
            .unwrap()
            .append_event(&prepared)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn racing_late_grant_and_abort_leave_one_replayable_winner() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    let prepared = probe
        .prepare_grant("racing-grant", 2, grant(&principal.principal_id, &table_id))
        .await
        .unwrap();
    let ledger = MetastoreLedger::new(storage.clone()).unwrap();
    let (late_grant, abort) =
        tokio::join!(ledger.append_event(&prepared), probe.abort_prepared_grant(),);
    let winner = ledger
        .load_events()
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.event_id == "racing-grant")
        .unwrap();
    match winner.mutation {
        MetastoreMutation::GrantUpserted(_) => {
            assert!(late_grant.is_ok());
            assert!(abort.is_err());
            probe.reconcile_prepared_grant().await.unwrap();
            assert!(ledger.replay().await.unwrap().grants.contains_key("grant"));
        }
        MetastoreMutation::GrantAdmissionAborted { grant_id } => {
            assert_eq!(grant_id, "grant");
            assert!(late_grant.is_err());
            if abort.is_err() {
                probe.abort_prepared_grant().await.unwrap();
            }
            assert!(ledger.replay().await.unwrap().grants.is_empty());
        }
        other => panic!("unexpected native event winner: {other:?}"),
    }
    assert!(probe.compile_current().await.is_ok());
}

#[tokio::test]
async fn preparation_rejects_reused_event_ids_and_stale_sequences_without_blocking_next_grant() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage).unwrap();
    probe
        .append_grant("first-grant", 2, grant(&principal.principal_id, &table_id))
        .await
        .unwrap();
    assert!(
        probe
            .prepare_grant("first-grant", 3, grant(&principal.principal_id, &table_id))
            .await
            .is_err()
    );
    assert!(
        probe
            .prepare_grant(
                "stale-sequence",
                2,
                grant(&principal.principal_id, &table_id)
            )
            .await
            .is_err()
    );
    probe
        .append_grant("next-grant", 3, grant(&principal.principal_id, &table_id))
        .await
        .unwrap();
    assert!(probe.compile_current().await.is_ok());
}

#[tokio::test]
async fn preparation_rejects_an_aborted_event_id_and_an_orphan_sequence_reservation() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    probe
        .prepare_grant(
            "aborted-grant",
            2,
            grant(&principal.principal_id, &table_id),
        )
        .await
        .unwrap();
    probe.abort_prepared_grant().await.unwrap();
    assert!(
        probe
            .prepare_grant(
                "aborted-grant",
                3,
                grant(&principal.principal_id, &table_id)
            )
            .await
            .is_err()
    );
    storage
        .put_raw(
            "ledger/metastore-sequences/00000000000000000003.event_id",
            Bytes::from_static(b"other-writer"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    assert!(
        probe
            .prepare_grant("new-grant", 3, grant(&principal.principal_id, &table_id))
            .await
            .is_err()
    );
    assert!(probe.compile_current().await.is_ok());
}

#[tokio::test]
async fn independent_probe_writers_prepare_only_one_native_grant() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let first = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    let second = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    let (one, two) = tokio::join!(
        first.prepare_grant("writer-one", 2, grant(&principal.principal_id, &table_id)),
        second.prepare_grant("writer-two", 2, grant(&principal.principal_id, &table_id)),
    );
    assert_ne!(one.is_ok(), two.is_ok());
    let prepared = one.or(two).unwrap();
    MetastoreLedger::new(storage)
        .unwrap()
        .append_event(&prepared)
        .await
        .unwrap();
    first.reconcile_prepared_grant().await.unwrap();
    assert!(second.compile_current().await.is_ok());
    second
        .append_grant("next-grant", 3, grant(&principal.principal_id, &table_id))
        .await
        .unwrap();
}

#[tokio::test]
async fn later_admitted_grant_cannot_witness_a_direct_ledger_grant() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    let admitted = probe
        .append_grant("first-grant", 2, grant(&principal.principal_id, &table_id))
        .await
        .unwrap();
    let mut forged = grant(&principal.principal_id, &table_id);
    forged.grant_id = "direct-grant".into();
    forged.privilege = "MODIFY".into();
    forged.identity_cut = Some(admitted);
    let scope = ControlPlaneScope::new("tenant-a", "workspace", "first").unwrap();
    MetastoreLedger::new(storage)
        .unwrap()
        .append_event(&MetastoreEvent::new_scoped(
            &scope,
            "direct-grant",
            3,
            MetastoreMutation::GrantUpserted(forged),
        ))
        .await
        .unwrap();

    assert!(
        probe
            .append_grant("later-grant", 4, grant(&principal.principal_id, &table_id))
            .await
            .is_err()
    );
    assert!(probe.compile_current().await.is_err());
}

#[tokio::test]
async fn foreign_scoped_native_event_cannot_compile_permissions() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    probe
        .append_grant("grant", 2, grant(&principal.principal_id, &table_id))
        .await
        .unwrap();
    assert!(probe.compile_current().await.is_ok());
    let ledger = MetastoreLedger::new(storage.clone()).unwrap();
    let mut object_event = ledger
        .load_events()
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.event_id == "object")
        .unwrap();
    object_event.scope.as_mut().unwrap().tenant_id = "foreign".into();
    storage
        .put_raw(
            "ledger/metastore/object.json",
            Bytes::from(serde_json::to_vec_pretty(&object_event).unwrap()),
            WritePrecondition::None,
        )
        .await
        .unwrap();
    assert!(probe.compile_current().await.is_err());
}

#[tokio::test]
async fn membership_revision_refreshes_native_grant_decisions() {
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
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage).unwrap();
    probe
        .append_grant("grant", 2, grant(&group.principal_id, &table_id))
        .await
        .unwrap();
    let request = request(&user.principal_id, &table_id);
    let before = probe.compile_current().await.unwrap();
    assert!(probe.read_table(&before, &request).await.is_err());

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
    let member = probe.compile_current().await.unwrap();
    assert!(probe.read_table(&member, &request).await.unwrap().is_some());

    identity
        .commit(
            IdentityMutation::ReviseMembership {
                principal_id: user.principal_id.clone(),
                expected_revision: 1,
                group_ids: BTreeSet::new(),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        probe.evaluate(&member, &request).await.reason_code,
        "stale_authority_cut"
    );
    let removed = probe.compile_current().await.unwrap();
    assert!(probe.read_table(&removed, &request).await.is_err());
}

#[tokio::test]
async fn unvalidated_native_owner_does_not_bypass_grant_admission() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (owner, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "owner".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let (grantee, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "grantee".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let scope = ControlPlaneScope::new("tenant-a", "workspace", "first").unwrap();
    MetastoreLedger::new(storage.clone())
        .unwrap()
        .append_event(&MetastoreEvent::new_scoped(
            &scope,
            "owner-update",
            2,
            MetastoreMutation::CatalogObjectUpserted(CatalogObjectRecord {
                object_id: table_id.clone(),
                object_type: "TABLE".into(),
                qualified_name: "default.events".into(),
                owner: owner.principal_id.clone(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1,
                properties: BTreeMap::new(),
            }),
        ))
        .await
        .unwrap();
    let probe = TenantCatalogProbe::new(&identity, storage).unwrap();
    probe
        .append_grant("grant", 3, grant(&grantee.principal_id, &table_id))
        .await
        .unwrap();
    let cut = probe.compile_current().await.unwrap();
    assert!(
        probe
            .read_table(&cut, &request(&owner.principal_id, &table_id))
            .await
            .is_err()
    );
    assert!(
        probe
            .read_table(&cut, &request(&grantee.principal_id, &table_id))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn historical_catalog_token_keeps_data_but_cannot_reauthorize_disabled_principal() {
    let backend = Arc::new(MemoryBackend::new());
    let identity =
        PrincipalIdentityStore::new(IdentityStorage::new(backend.clone(), "tenant-a").unwrap())
            .unwrap();
    let (principal, _) = identity
        .commit(
            IdentityMutation::Create {
                name: "alice".into(),
                kind: PrincipalKind::User,
            },
            None,
        )
        .await
        .unwrap();
    let storage = metastore_storage(backend, "first");
    let table_id = seed_table_and_object(&storage).await;
    let root_token = seed_historical_catalog_token(&storage).await;
    let probe = TenantCatalogProbe::new(&identity, storage.clone()).unwrap();
    probe
        .append_grant("grant", 2, grant(&principal.principal_id, &table_id))
        .await
        .unwrap();
    let cut = probe.compile_current().await.unwrap();
    let request = request(&principal.principal_id, &table_id);
    assert!(
        probe
            .read_table_at(&cut, &request, &root_token)
            .await
            .unwrap()
            .is_some()
    );

    identity
        .commit(
            IdentityMutation::Disable {
                principal_id: principal.principal_id.clone(),
            },
            None,
        )
        .await
        .unwrap();
    assert!(
        CatalogReader::new(storage)
            .get_table_by_id_for_root_token(&root_token, &table_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        probe
            .read_table_at(&cut, &request, &root_token)
            .await
            .is_err()
    );
}

fn metastore_storage(backend: Arc<MemoryBackend>, id: &str) -> ScopedStorage {
    let scope = ControlPlaneScope::new("tenant-a", "workspace", id).unwrap();
    ScopedStorage::new_metastore_scoped(backend, &scope).unwrap()
}

async fn seed_table_and_object(storage: &ScopedStorage) -> String {
    let writer = CatalogWriter::new(storage.clone())
        .with_sync_compactor(Arc::new(Tier1Compactor::new(storage.clone())));
    writer.initialize().await.unwrap();
    writer
        .create_namespace("default", None, WriteOptions::default())
        .await
        .unwrap();
    let table = writer
        .register_table(
            RegisterTableRequest {
                namespace: "default".into(),
                name: "events".into(),
                description: None,
                location: None,
                format: None,
                columns: vec![ColumnDefinition {
                    name: "id".into(),
                    data_type: "STRING".into(),
                    is_nullable: false,
                    ordinal: 0,
                    description: None,
                }],
            },
            WriteOptions::default(),
        )
        .await
        .unwrap();
    MetastoreLedger::new(storage.clone())
        .unwrap()
        .append_event(&MetastoreEvent::new_scoped(
            &ControlPlaneScope::new(
                storage.tenant_id(),
                storage.workspace_id(),
                storage.scope().metastore_id().unwrap(),
            )
            .unwrap(),
            "object",
            1,
            MetastoreMutation::CatalogObjectUpserted(CatalogObjectRecord {
                object_id: table.id.clone(),
                object_type: "TABLE".into(),
                qualified_name: "default.events".into(),
                owner: "unassigned-owner".into(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 0,
                properties: BTreeMap::new(),
            }),
        ))
        .await
        .unwrap();
    table.id
}

fn grant(principal_id: &str, table_id: &str) -> GrantRecord {
    GrantRecord {
        grant_id: "grant".into(),
        object_id: table_id.into(),
        object_type: "TABLE".into(),
        principal_id: principal_id.into(),
        privilege: "SELECT".into(),
        owner: "admin".into(),
        lifecycle_state: LifecycleState::Active,
        updated_at_ms: 0,
        properties: BTreeMap::new(),
        identity_cut: None,
    }
}

fn request(principal_id: &str, table_id: &str) -> AuthzRequest {
    AuthzRequest::new(principal_id, table_id, "TABLE", Privilege::Select)
}

async fn seed_historical_catalog_token(storage: &ScopedStorage) -> String {
    let pointer_bytes = storage
        .get_raw(&CatalogPaths::domain_manifest_pointer(
            CatalogDomain::Catalog,
        ))
        .await
        .unwrap();
    let pointer: DomainManifestPointer = serde_json::from_slice(&pointer_bytes).unwrap();
    let manifest_bytes = storage.get_raw(&pointer.manifest_path).await.unwrap();
    let manifest: CatalogDomainManifest = serde_json::from_slice(&manifest_bytes).unwrap();
    let tx_id = "01JIDENTITYHISTORY00000000001";
    let super_manifest_path = ControlPlaneTxPaths::root_super_manifest(tx_id);
    let now = Utc::now();
    let root_manifest = RootTxManifest {
        tx_id: tx_id.into(),
        fencing_token: 1,
        published_at: now,
        domains: BTreeMap::from([(
            ControlPlaneTxDomain::Catalog,
            RootTxManifestDomain {
                manifest_id: pointer.manifest_id,
                manifest_path: pointer.manifest_path,
                commit_id: manifest.last_commit_id.unwrap(),
            },
        )]),
    };
    storage
        .put_raw(
            &super_manifest_path,
            Bytes::from(serde_json::to_vec(&root_manifest).unwrap()),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    let read_token = format!("root:{tx_id}");
    let root_record = RootTxRecord {
        tx_id: tx_id.into(),
        kind: ControlPlaneTxKind::RootCommit,
        status: ControlPlaneTxStatus::Visible,
        repair_pending: false,
        request_id: "historical-catalog-test".into(),
        idempotency_key: "historical-catalog-test".into(),
        request_hash: "sha256:historical-catalog-test".into(),
        lock_path: ControlPlaneTxPaths::root_lock(),
        fencing_token: 1,
        prepared_at: now,
        visible_at: Some(now),
        durable_append: None,
        result: Some(RootTxReceipt {
            tx_id: tx_id.into(),
            root_commit_id: "01JIDENTITYHISTORYCOMMIT000001".into(),
            super_manifest_path,
            domain_commits: vec![],
            read_token: read_token.clone(),
            visible_at: now,
        }),
    };
    storage
        .put_raw(
            &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, tx_id),
            Bytes::from(serde_json::to_vec(&root_record).unwrap()),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    read_token
}

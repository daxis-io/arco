//! Tests for the native metastore replay and projection kernel.

#![allow(clippy::expect_used)]

#[path = "../../arco-core/tests/support/spy_backend.rs"]
mod spy_backend;

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arco_catalog::metastore::events::{
    ExternalLocationRecord, GrantRecord, LifecycleState, ManagedRootRecord, MetastoreEvent,
    MetastoreMutation, PrincipalKind, PrincipalRecord, StorageCredentialRecord,
    WorkspaceBindingRecord,
};
use arco_catalog::metastore::ledger::{MetastoreLedger, MetastoreLedgerWatermark};
use arco_catalog::metastore::projections::{
    METASTORE_OBJECTS_PROJECTION, ProjectionRegistry, ProjectionSet, STORAGE_GOVERNANCE_PROJECTION,
    build_projection_set, write_metastore_objects,
};
use arco_catalog::metastore::publish::{
    PointerPublishResult, PublishedProjectionSet, PublishedStorageGovernanceCache,
    complete_pointer_publication, load_published_storage_governance,
    publish_metastore_projection_set,
};
use arco_catalog::metastore::replay::replay_events;
use arco_catalog::{CatalogError, Result};
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_core::{ControlPlaneScope, ScopedStorage};
use async_trait::async_trait;
use bytes::Bytes;
use spy_backend::{SpyBackend, SpyOp};

#[test]
fn metastore_replay_is_deterministic_and_indexes_names_by_stable_id() {
    let events = sample_events();
    let mut reversed = events.clone();
    reversed.reverse();

    let state = replay_events(events.iter()).expect("replay should succeed");
    let reversed_state = replay_events(reversed.iter()).expect("replay should sort by sequence");

    assert_eq!(
        state.deterministic_digest(),
        reversed_state.deterministic_digest()
    );
    assert_eq!(
        state
            .principal_id_by_name("alice@example.com")
            .expect("principal name index"),
        "principal_01"
    );
    assert_eq!(state.principals.len(), 1);
    assert_eq!(state.grants.len(), 1);
    assert_eq!(state.storage_credentials.len(), 1);
}

#[test]
fn projection_registry_allowlists_only_current_kernel_projection() {
    let registry = ProjectionRegistry::default();

    assert!(registry.contains(METASTORE_OBJECTS_PROJECTION));
    assert!(registry.contains(STORAGE_GOVERNANCE_PROJECTION));
    assert!(!registry.contains("volumes.parquet"));
    assert!(!registry.contains("functions.parquet"));
    assert!(!registry.contains("registered_models.parquet"));
    assert!(!registry.contains("governance_attachments.parquet"));
    assert_eq!(
        registry.enabled_files(),
        vec![METASTORE_OBJECTS_PROJECTION, STORAGE_GOVERNANCE_PROJECTION]
    );
}

#[test]
fn projections_include_schema_version_watermark_and_redact_secrets() {
    let state = replay_events(sample_events().iter()).expect("replay should succeed");
    let registry = ProjectionRegistry::default();

    let set =
        build_projection_set(&state, &registry, "event_003").expect("projection set should build");
    let file = set
        .file(METASTORE_OBJECTS_PROJECTION)
        .expect("metastore object projection should be present");

    assert_eq!(file.schema_version, 2);
    assert_eq!(file.ledger_watermark, "event_003");
    assert_eq!(file.rows.len(), 3);
    assert!(file.rows.iter().any(|row| row.object_id == "cred_01"));
    assert!(file.rows.iter().all(|row| row.properties_json.is_none()));

    let schema_fields = file.schema_field_names();
    assert!(schema_fields.contains(&"schema_version"));
    assert!(schema_fields.contains(&"ledger_watermark"));
    assert!(!schema_fields.contains(&"secret_material_ref"));
    assert!(!schema_fields.contains(&"encrypted_payload"));

    let serialized_rows = serde_json::to_string(&file.rows).expect("rows should serialize");
    assert!(!serialized_rows.contains("secret://"));
    assert!(!serialized_rows.contains("encrypted-secret"));
    assert!(!serialized_rows.contains("secret-token"));
}

#[test]
fn storage_governance_replays_typed_authoritative_state() {
    let state = replay_events(storage_governance_events().iter()).expect("replay should succeed");

    assert_eq!(state.storage_credentials.len(), 1);
    assert_eq!(state.external_locations.len(), 1);
    assert_eq!(state.managed_roots.len(), 1);
    assert_eq!(state.workspace_bindings.len(), 1);
    assert_eq!(state.ledger_watermark.as_deref(), Some("event_004"));

    let location = state
        .external_locations
        .get("loc_orders")
        .expect("external location");
    assert_eq!(location.url, "gs://bucket/warehouse/orders/");
    assert_eq!(location.credential_id, "cred_01");

    let root = state.managed_roots.get("root_main").expect("managed root");
    assert_eq!(root.workspace_id, "workspace1");
    assert_eq!(root.url, "gs://bucket/managed/");
}

#[test]
fn storage_governance_projection_is_safe_redacted_and_parquet_serializable() -> Result<()> {
    let state = replay_events(storage_governance_events().iter()).expect("replay should succeed");
    let registry = ProjectionRegistry::default();

    let set = build_projection_set(&state, &registry, "event_004")?;
    let file = set
        .file(STORAGE_GOVERNANCE_PROJECTION)
        .expect("storage governance projection should be present");

    assert_eq!(file.schema_version, 1);
    assert_eq!(file.ledger_watermark, "event_004");
    assert_eq!(file.rows.len(), 4);
    assert!(file.rows.iter().all(|row| row.properties_json.is_none()));

    let schema_fields = file.schema_field_names();
    assert!(schema_fields.contains(&"schema_version"));
    assert!(schema_fields.contains(&"ledger_watermark"));
    assert!(schema_fields.contains(&"object_id"));
    assert!(schema_fields.contains(&"object_type"));
    assert!(schema_fields.contains(&"url"));
    assert!(!schema_fields.contains(&"secret_material_ref"));
    assert!(!schema_fields.contains(&"encrypted_payload"));

    let serialized_rows = serde_json::to_string(&file.rows).expect("rows should serialize");
    assert!(serialized_rows.contains("external_location"));
    assert!(serialized_rows.contains("managed_root"));
    assert!(serialized_rows.contains("workspace_binding"));
    assert!(!serialized_rows.contains("secret://"));
    assert!(!serialized_rows.contains("encrypted-secret"));
    assert!(!serialized_rows.contains("secret-token"));

    let _parquet = file.write_parquet()?;
    Ok(())
}

#[test]
fn storage_governance_pointer_publication_is_all_or_nothing() {
    let previous = PublishedProjectionSet::new(
        "manifest_old",
        "event_001",
        build_projection_set(
            &replay_events(sample_events().iter()).expect("sample replay"),
            &ProjectionRegistry::new([METASTORE_OBJECTS_PROJECTION]),
            "event_001",
        )
        .expect("previous projection"),
    );
    let candidate = PublishedProjectionSet::new(
        "manifest_new",
        "event_004",
        build_projection_set(
            &replay_events(storage_governance_events().iter()).expect("storage replay"),
            &ProjectionRegistry::default(),
            "event_004",
        )
        .expect("candidate projection"),
    );

    let failed = complete_pointer_publication(
        previous.clone(),
        candidate.clone(),
        PointerPublishResult::CasFailed,
    );
    let published =
        complete_pointer_publication(previous, candidate, PointerPublishResult::Published);

    assert!(
        failed
            .projections
            .file(STORAGE_GOVERNANCE_PROJECTION)
            .is_none()
    );
    assert!(
        published
            .projections
            .file(STORAGE_GOVERNANCE_PROJECTION)
            .is_some()
    );
}

#[tokio::test]
async fn metastore_ledger_persists_storage_governance_events_and_replays_state() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage);

    for event in scoped_storage_governance_events(&test_scope()) {
        ledger.append_event(&event).await?;
    }

    let events = ledger.load_events().await?;
    assert_eq!(events.len(), 4);

    let state = ledger.replay().await?;
    assert_eq!(state.storage_credentials.len(), 1);
    assert_eq!(state.external_locations.len(), 1);
    assert_eq!(state.managed_roots.len(), 1);
    assert_eq!(state.workspace_bindings.len(), 1);
    Ok(())
}

#[tokio::test]
async fn metastore_ledger_rejects_unscoped_durable_events() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage);
    let event = sample_events().remove(0);

    let err = ledger
        .append_event(&event)
        .await
        .expect_err("durable ledger writes must require scope");

    assert!(err.to_string().contains("scope"), "unexpected error: {err}");
    Ok(())
}

#[tokio::test]
async fn metastore_ledger_rejects_duplicate_event_id_with_different_payload() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage);
    let scope = test_scope();
    let first = scoped_storage_credential_event(&scope, "event_001", 1, "cred_01");
    let conflicting = scoped_storage_credential_event(&scope, "event_001", 1, "cred_02");

    ledger.append_event(&first).await?;
    let err = ledger
        .append_event(&conflicting)
        .await
        .expect_err("same event id with different payload must conflict");

    assert!(
        err.to_string().contains("event_001"),
        "unexpected error: {err}"
    );
    let events = ledger.load_events().await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], first);
    Ok(())
}

#[tokio::test]
async fn metastore_ledger_rejects_duplicate_sequence_for_different_events() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage);
    let scope = test_scope();
    let first = scoped_storage_credential_event(&scope, "event_001", 1, "cred_01");
    let conflicting = scoped_storage_credential_event(&scope, "event_002", 1, "cred_02");

    ledger.append_event(&first).await?;
    let err = ledger
        .append_event(&conflicting)
        .await
        .expect_err("different events must not share a replay sequence");

    assert!(
        err.to_string().contains("sequence"),
        "unexpected error: {err}"
    );
    let events = ledger.load_events().await?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], first);
    Ok(())
}

#[tokio::test]
async fn metastore_ledger_does_not_reserve_sequence_for_conflicting_event_id() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage);
    let scope = test_scope();
    let first = scoped_storage_credential_event(&scope, "event_001", 1, "cred_01");
    let conflicting = scoped_storage_credential_event(&scope, "event_001", 2, "cred_02");
    let next = scoped_storage_credential_event(&scope, "event_002", 2, "cred_03");

    ledger.append_event(&first).await?;
    assert!(ledger.append_event(&conflicting).await.is_err());
    ledger.append_event(&next).await?;

    let events = ledger.load_events().await?;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0], first);
    assert_eq!(events[1], next);
    Ok(())
}

#[tokio::test]
async fn metastore_ledger_next_sequence_skips_orphan_sequence_reservations() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    storage
        .put_raw(
            "ledger/metastore-sequences/00000000000000000001.event_id",
            Bytes::from_static(b"event_orphan"),
            WritePrecondition::DoesNotExist,
        )
        .await?;
    let ledger = MetastoreLedger::new(storage);

    assert_eq!(ledger.next_sequence().await?, 2);
    Ok(())
}

#[tokio::test]
async fn metastore_ledger_latest_watermark_tracks_latest_persisted_event() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage);
    let scope = test_scope();

    assert!(ledger.latest_watermark().await?.is_none());

    ledger
        .append_event(&scoped_storage_credential_event(
            &scope,
            "event_001",
            1,
            "cred_01",
        ))
        .await?;
    ledger
        .append_event(&scoped_principal_event(
            &scope,
            "event_005",
            5,
            "principal_01",
        ))
        .await?;

    let watermark = ledger
        .latest_watermark()
        .await?
        .expect("non-empty ledger should have a latest watermark");

    assert_eq!(watermark.event_id, "event_005");
    assert_eq!(watermark.sequence, 5);
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_accepts_empty_manifest_for_empty_ledger() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;

    publish_metastore_projection_set(&storage, &ProjectionSet { files: Vec::new() }, 0).await?;

    let published = load_published_storage_governance(&storage).await?;

    assert_eq!(published.ledger_watermark, "empty");
    assert!(published.state.list_storage_credentials().is_empty());
    assert!(published.state.list_external_locations().is_empty());
    assert!(published.state.list_managed_roots().is_empty());
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_requires_latest_ledger_watermark_after_authz_event()
-> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();

    for event in scoped_storage_governance_events(&scope) {
        ledger.append_event(&event).await?;
    }
    let state_at_storage_event = ledger.replay().await?;
    let projection_at_storage_event = build_projection_set(
        &state_at_storage_event,
        &ProjectionRegistry::default(),
        "event_004",
    )?;
    publish_metastore_projection_set(&storage, &projection_at_storage_event, 4).await?;

    ledger
        .append_event(&scoped_principal_event(
            &scope,
            "event_005",
            5,
            "principal_01",
        ))
        .await?;

    let err = load_published_storage_governance(&storage)
        .await
        .expect_err("older projection must deny after any newer metastore event");
    assert_projection_stale(err);
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_denies_when_latest_watermark_update_is_pending() -> Result<()>
{
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();

    for event in scoped_storage_governance_events(&scope) {
        ledger.append_event(&event).await?;
    }
    let state_at_storage_event = ledger.replay().await?;
    let projection_at_storage_event = build_projection_set(
        &state_at_storage_event,
        &ProjectionRegistry::default(),
        "event_004",
    )?;
    publish_metastore_projection_set(&storage, &projection_at_storage_event, 4).await?;

    write_metastore_event_with_pending_watermark(
        &storage,
        &scoped_storage_credential_event(&scope, "event_005", 5, "cred_02"),
    )
    .await?;

    let err = load_published_storage_governance(&storage)
        .await
        .expect_err("pending latest-watermark update must fail closed");
    assert_latest_watermark_pending(err);
    Ok(())
}

#[tokio::test]
async fn metastore_ledger_missing_latest_marker_repairs_to_real_latest_on_old_idempotent_append()
-> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();
    let first = scoped_storage_credential_event(&scope, "event_001", 1, "cred_01");
    let latest = scoped_principal_event(&scope, "event_005", 5, "principal_01");

    ledger.append_event(&first).await?;
    ledger.append_event(&latest).await?;
    storage
        .delete("ledger/metastore-latest/watermark.json")
        .await?;

    ledger.append_event(&first).await?;
    let watermark = ledger
        .latest_watermark()
        .await?
        .expect("latest watermark should be repaired");
    assert_eq!(watermark.event_id, "event_005");
    assert_eq!(watermark.sequence, 5);
    Ok(())
}

#[tokio::test]
async fn metastore_ledger_does_not_advance_latest_over_lower_pending_sequence() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();
    let first = scoped_storage_credential_event(&scope, "event_001", 1, "cred_01");
    let blocked = scoped_storage_credential_event(&scope, "event_005", 5, "cred_05");
    let later = scoped_principal_event(&scope, "event_006", 6, "principal_06");

    ledger.append_event(&first).await?;
    storage
        .put_raw(
            "ledger/metastore-sequences/00000000000000000005.event_id",
            Bytes::copy_from_slice(blocked.event_id.as_bytes()),
            WritePrecondition::DoesNotExist,
        )
        .await?;
    let pending = MetastoreLedgerWatermark {
        event_id: blocked.event_id.clone(),
        sequence: blocked.sequence,
    };
    let pending_bytes = serde_json::to_vec_pretty(&pending).expect("pending watermark serializes");
    storage
        .put_raw(
            "ledger/metastore-latest/pending.json",
            Bytes::from(pending_bytes),
            WritePrecondition::None,
        )
        .await?;

    let err = ledger
        .append_event(&later)
        .await
        .expect_err("later append must not advance latest over lower pending sequence");
    assert_latest_watermark_pending(err);
    let err = ledger
        .latest_watermark()
        .await
        .expect_err("lower pending sequence must keep readers fail-closed");
    assert_latest_watermark_pending(err);
    Ok(())
}

#[tokio::test]
async fn metastore_ledger_recovers_stale_pending_after_event_write_failure() -> Result<()> {
    let backend = Arc::new(FailOncePutBackend::new(Arc::new(MemoryBackend::new())));
    let storage = ScopedStorage::new(backend.clone(), "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();
    let first = scoped_storage_credential_event(&scope, "event_001", 1, "cred_01");
    let failed = scoped_storage_credential_event(&scope, "event_002", 2, "cred_02");
    let retry = scoped_storage_credential_event(&scope, "event_002_retry", 2, "cred_02_retry");

    ledger.append_event(&first).await?;
    backend.fail_put_once("ledger/metastore/event_002.json");
    let err = ledger
        .append_event(&failed)
        .await
        .expect_err("event write failure should leave a recoverable pending marker");
    assert!(matches!(err, CatalogError::Storage { .. }));
    assert!(
        storage
            .head_raw("ledger/metastore-sequences/00000000000000000002.event_id")
            .await?
            .is_none(),
        "failed append should release the sequence reservation"
    );

    ledger.append_event(&retry).await?;
    let watermark = ledger
        .latest_watermark()
        .await?
        .expect("retry should repair stale pending marker");
    assert_eq!(watermark.event_id, retry.event_id);
    assert_eq!(watermark.sequence, retry.sequence);
    Ok(())
}

#[tokio::test]
async fn metastore_ledger_corrupt_latest_marker_fails_closed() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();

    ledger
        .append_event(&scoped_storage_credential_event(
            &scope,
            "event_001",
            1,
            "cred_01",
        ))
        .await?;
    storage
        .put_raw(
            "ledger/metastore-latest/watermark.json",
            Bytes::from_static(b"not-json"),
            WritePrecondition::None,
        )
        .await?;

    let err = ledger
        .latest_watermark()
        .await
        .expect_err("corrupt latest marker must fail closed");
    assert!(matches!(err, CatalogError::Serialization { .. }));
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_uses_ledger_when_sidecar_marker_is_missing() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();

    for event in scoped_storage_governance_events(&scope) {
        ledger.append_event(&event).await?;
    }
    let state_at_storage_event = ledger.replay().await?;
    let projection_at_storage_event = build_projection_set(
        &state_at_storage_event,
        &ProjectionRegistry::default(),
        "event_004",
    )?;
    publish_metastore_projection_set(&storage, &projection_at_storage_event, 4).await?;

    write_metastore_event_without_projection_sidecar(
        &storage,
        &scoped_storage_credential_event(&scope, "event_005", 5, "cred_02"),
    )
    .await?;

    let err = load_published_storage_governance(&storage)
        .await
        .expect_err("ledger event without sidecar marker must still make projection stale");
    assert_projection_stale(err);
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_accepts_latest_non_storage_event_watermark() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();

    for event in scoped_storage_governance_events(&scope) {
        ledger.append_event(&event).await?;
    }
    ledger
        .append_event(&scoped_principal_event(
            &scope,
            "event_005",
            5,
            "principal_01",
        ))
        .await?;

    let latest_state = ledger.replay().await?;
    let latest_projection =
        build_projection_set(&latest_state, &ProjectionRegistry::default(), "event_005")?;
    publish_metastore_projection_set(&storage, &latest_projection, 5).await?;

    let published = load_published_storage_governance(&storage).await?;

    assert_eq!(published.ledger_watermark, "event_005");
    assert_eq!(published.state.list_external_locations().len(), 1);
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_cache_reuses_hot_state() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let spy = Arc::new(SpyBackend::new(backend));
    let storage = ScopedStorage::new(spy.clone(), "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();

    for event in scoped_storage_governance_events(&scope) {
        ledger.append_event(&event).await?;
    }
    let state = ledger.replay().await?;
    let projection = build_projection_set(&state, &ProjectionRegistry::default(), "event_004")?;
    publish_metastore_projection_set(&storage, &projection, 4).await?;

    let cache = PublishedStorageGovernanceCache::default();
    let first = cache.load(&storage).await?;
    spy.clear_ops();
    let second = cache.load(&storage).await?;

    assert_eq!(first.ledger_watermark, second.ledger_watermark);
    let parquet_gets = spy
        .ops()
        .into_iter()
        .filter(|op| {
            matches!(
                op,
                SpyOp::Get { path } if path.ends_with("storage_governance.parquet")
            )
        })
        .collect::<Vec<_>>();
    assert!(
        parquet_gets.is_empty(),
        "hot load should reuse cached projection state"
    );
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_cache_hot_hit_does_not_read_ledger_events() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let spy = Arc::new(SpyBackend::new(backend));
    let storage = ScopedStorage::new(spy.clone(), "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();

    for event in scoped_storage_governance_events(&scope) {
        ledger.append_event(&event).await?;
    }
    let state = ledger.replay().await?;
    let projection = build_projection_set(&state, &ProjectionRegistry::default(), "event_004")?;
    publish_metastore_projection_set(&storage, &projection, 4).await?;

    let cache = PublishedStorageGovernanceCache::default();
    cache.load(&storage).await?;
    spy.clear_ops();
    cache.load(&storage).await?;

    let ledger_event_ops = spy
        .ops()
        .into_iter()
        .filter(|op| match op {
            SpyOp::List { prefix } => {
                prefix.ends_with("ledger/metastore/")
                    || prefix.ends_with("ledger/metastore-sequences/")
            }
            SpyOp::Get { path } => path.contains("/ledger/metastore/"),
            _ => false,
        })
        .collect::<Vec<_>>();
    assert!(
        ledger_event_ops.is_empty(),
        "hot freshness check must not list/read ledger event files or scan sequence markers: {ledger_event_ops:?}"
    );
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_cache_is_scoped_by_tenant_workspace() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage_a = ScopedStorage::new(backend.clone(), "tenant1", "workspace1")?;
    let storage_b = ScopedStorage::new(backend, "tenant2", "workspace2")?;
    let scope_a = ControlPlaneScope::workspace_alias("tenant1", "workspace1")?;
    let scope_b = ControlPlaneScope::workspace_alias("tenant2", "workspace2")?;

    publish_storage_governance_projection(
        &storage_a,
        scoped_storage_governance_events_for_scope(
            &scope_a,
            "workspace1",
            "gs://bucket-a/warehouse/orders/",
            "gs://bucket-a/managed/",
        ),
    )
    .await?;
    publish_storage_governance_projection(
        &storage_b,
        scoped_storage_governance_events_for_scope(
            &scope_b,
            "workspace2",
            "gs://bucket-b/warehouse/orders/",
            "gs://bucket-b/managed/",
        ),
    )
    .await?;

    let cache = PublishedStorageGovernanceCache::default();
    let first = cache.load(&storage_a).await?;
    let second = cache.load(&storage_b).await?;

    assert_eq!(first.ledger_watermark, "event_004");
    assert_eq!(second.ledger_watermark, "event_004");
    let second_location = second
        .state
        .get_external_location("loc_orders")
        .expect("scope B external location");
    assert_eq!(
        second_location.path.canonical_uri(),
        "gs://bucket-b/warehouse/orders/"
    );
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_cache_validates_hot_manifest_file_shape() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;

    let mut manifest = publish_storage_governance_projection(
        &storage,
        scoped_storage_governance_events(&test_scope()),
    )
    .await?;

    let cache = PublishedStorageGovernanceCache::default();
    cache.load(&storage).await?;

    manifest
        .files
        .retain(|file| file.file_name != STORAGE_GOVERNANCE_PROJECTION);
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).expect("manifest should serialize");
    storage
        .put_raw(
            "manifests/metastore_projection/00000000000000000004.json",
            Bytes::from(manifest_bytes),
            WritePrecondition::None,
        )
        .await?;

    let err = cache
        .load(&storage)
        .await
        .expect_err("hot cache must not bypass missing storage-governance file");
    assert_projection_missing(err);
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_cache_validates_hot_projection_object_exists() -> Result<()>
{
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;

    publish_storage_governance_projection(
        &storage,
        scoped_storage_governance_events(&test_scope()),
    )
    .await?;

    let cache = PublishedStorageGovernanceCache::default();
    cache.load(&storage).await?;
    storage.delete(&storage_governance_snapshot_path(4)).await?;

    let err = cache
        .load(&storage)
        .await
        .expect_err("hot cache must not bypass a missing projection object");
    assert_projection_unavailable(err);
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_cache_rejects_same_id_manifest_metadata_change() -> Result<()>
{
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;

    let mut manifest = publish_storage_governance_projection(
        &storage,
        scoped_storage_governance_events(&test_scope()),
    )
    .await?;

    let cache = PublishedStorageGovernanceCache::default();
    cache.load(&storage).await?;

    let file = manifest
        .files
        .iter_mut()
        .find(|file| file.file_name == STORAGE_GOVERNANCE_PROJECTION)
        .expect("storage-governance manifest file");
    file.row_count = file.row_count.saturating_add(1);
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).expect("manifest should serialize");
    storage
        .put_raw(
            "manifests/metastore_projection/00000000000000000004.json",
            Bytes::from(manifest_bytes),
            WritePrecondition::None,
        )
        .await?;

    let err = cache
        .load(&storage)
        .await
        .expect_err("hot cache must not reuse state across changed manifest metadata");
    assert_projection_unsupported(err);
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_cache_reloads_same_identity_object_replacement() -> Result<()>
{
    let backend = Arc::new(CacheRaceBackend::new(Arc::new(MemoryBackend::new())));
    let storage = ScopedStorage::new(backend.clone(), "tenant1", "workspace1")?;

    publish_storage_governance_projection(
        &storage,
        scoped_storage_governance_events(&test_scope()),
    )
    .await?;
    let path = storage_governance_snapshot_path(4);
    let original_meta = storage
        .head_raw(&path)
        .await?
        .expect("storage-governance projection object");
    backend.pin_identity(&original_meta.path, &original_meta);

    let cache = PublishedStorageGovernanceCache::default();
    let first = cache.load(&storage).await?;
    assert_eq!(
        first
            .state
            .get_external_location("loc_orders")
            .expect("original external location")
            .path
            .canonical_uri(),
        "gs://bucket/warehouse/orders/"
    );

    tokio::time::sleep(Duration::from_millis(1)).await;
    let replacement =
        storage_governance_projection_bytes_with_external_url("gs://bucket/warehouse/returns/")?;
    storage
        .put_raw(&path, replacement, WritePrecondition::None)
        .await?;

    let second = cache.load(&storage).await?;
    assert_eq!(
        second
            .state
            .get_external_location("loc_orders")
            .expect("replacement external location")
            .path
            .canonical_uri(),
        "gs://bucket/warehouse/returns/"
    );
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_cache_revalidates_after_refresh_wait() -> Result<()> {
    let backend = Arc::new(CacheRaceBackend::new(Arc::new(MemoryBackend::new())));
    let storage = ScopedStorage::new(backend.clone(), "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();

    for event in scoped_storage_governance_events(&scope) {
        ledger.append_event(&event).await?;
    }
    let state = ledger.replay().await?;
    let projection = build_projection_set(&state, &ProjectionRegistry::default(), "event_004")?;
    publish_metastore_projection_set(&storage, &projection, 4).await?;
    let storage_governance_backend_path = storage
        .head_raw(&storage_governance_snapshot_path(4))
        .await?
        .expect("storage-governance projection object")
        .path;

    let cache = Arc::new(PublishedStorageGovernanceCache::default());
    backend.block_get_once(&storage_governance_backend_path);

    let first_cache = Arc::clone(&cache);
    let first_storage = storage.clone();
    let first = tokio::spawn(async move { first_cache.load(&first_storage).await });
    backend.wait_for_blocked_get().await;

    let second_cache = Arc::clone(&cache);
    let second_storage = storage.clone();
    let second = tokio::spawn(async move { second_cache.load(&second_storage).await });
    backend.wait_for_storage_governance_heads(4).await;

    ledger
        .append_event(&scoped_principal_event(
            &scope,
            "event_005",
            5,
            "principal_01",
        ))
        .await?;
    let latest_state = ledger.replay().await?;
    let latest_projection =
        build_projection_set(&latest_state, &ProjectionRegistry::default(), "event_005")?;
    publish_metastore_projection_set(&storage, &latest_projection, 5).await?;

    backend.release_blocked_get();
    let first = first.await.expect("first cache load task")?;
    let second = second.await.expect("second cache load task")?;

    assert_eq!(first.ledger_watermark, "event_004");
    assert_eq!(second.ledger_watermark, "event_005");
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_denies_manifest_ahead_of_ledger() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();

    for event in scoped_storage_governance_events(&scope) {
        ledger.append_event(&event).await?;
    }
    let state = ledger.replay().await?;
    let projection = build_projection_set(&state, &ProjectionRegistry::default(), "event_004")?;
    publish_metastore_projection_set(&storage, &projection, 5).await?;

    let err = load_published_storage_governance(&storage)
        .await
        .expect_err("manifest sequence ahead of ledger must deny closed");
    assert_projection_stale(err);
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_denies_corrupt_projection_file() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();

    for event in scoped_storage_governance_events(&scope) {
        ledger.append_event(&event).await?;
    }
    let state = ledger.replay().await?;
    let projection = build_projection_set(&state, &ProjectionRegistry::default(), "event_004")?;
    publish_metastore_projection_set(&storage, &projection, 4).await?;

    storage
        .put_raw(
            &storage_governance_snapshot_path(4),
            Bytes::from_static(b"not a parquet file"),
            WritePrecondition::None,
        )
        .await?;

    let err = load_published_storage_governance(&storage)
        .await
        .expect_err("corrupt storage-governance projection must deny closed");
    assert_projection_unavailable(err);
    Ok(())
}

#[tokio::test]
async fn storage_governance_projection_denies_row_watermark_mismatch() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();

    for event in scoped_storage_governance_events(&scope) {
        ledger.append_event(&event).await?;
    }
    let state = ledger.replay().await?;
    let projection = build_projection_set(&state, &ProjectionRegistry::default(), "event_004")?;
    publish_metastore_projection_set(&storage, &projection, 4).await?;

    let mut rows = projection
        .file(STORAGE_GOVERNANCE_PROJECTION)
        .expect("storage-governance projection")
        .rows
        .clone();
    for row in &mut rows {
        row.ledger_watermark = "event_003".to_string();
    }
    storage
        .put_raw(
            &storage_governance_snapshot_path(4),
            write_metastore_objects(&rows)?,
            WritePrecondition::None,
        )
        .await?;

    let err = load_published_storage_governance(&storage)
        .await
        .expect_err("row watermark mismatch must deny closed");
    assert_projection_unsupported(err);
    Ok(())
}

#[tokio::test]
async fn stale_projection_publish_does_not_roll_back_pointer() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1")?;
    let ledger = MetastoreLedger::new(storage.clone());
    let scope = test_scope();

    for event in scoped_storage_governance_events(&scope) {
        ledger.append_event(&event).await?;
    }
    let state_at_storage_event = ledger.replay().await?;
    let old_projection = build_projection_set(
        &state_at_storage_event,
        &ProjectionRegistry::default(),
        "event_004",
    )?;
    publish_metastore_projection_set(&storage, &old_projection, 4).await?;

    ledger
        .append_event(&scoped_principal_event(
            &scope,
            "event_005",
            5,
            "principal_01",
        ))
        .await?;
    let latest_state = ledger.replay().await?;
    let latest_projection =
        build_projection_set(&latest_state, &ProjectionRegistry::default(), "event_005")?;
    publish_metastore_projection_set(&storage, &latest_projection, 5).await?;

    let err = publish_metastore_projection_set(&storage, &old_projection, 4)
        .await
        .expect_err("stale publisher must not move pointer backward");
    assert!(
        matches!(
            err,
            CatalogError::PreconditionFailed { .. } | CatalogError::CasFailed { .. }
        ),
        "unexpected stale publish error: {err:?}"
    );

    let published = load_published_storage_governance(&storage).await?;
    assert_eq!(published.ledger_watermark, "event_005");
    Ok(())
}

#[test]
fn projection_specs_hide_storage_governance_from_tenant_visibility() {
    let registry = ProjectionRegistry::default();
    let specs = registry.specs();

    let storage_governance = specs
        .iter()
        .find(|spec| spec.file_name == STORAGE_GOVERNANCE_PROJECTION)
        .expect("storage-governance projection spec");
    let metastore_objects = specs
        .iter()
        .find(|spec| spec.file_name == METASTORE_OBJECTS_PROJECTION)
        .expect("metastore object projection spec");

    assert!(!storage_governance.tenant_visible);
    assert!(metastore_objects.tenant_visible);
}

#[test]
fn pointer_publication_is_all_or_nothing() {
    let previous = PublishedProjectionSet::empty("manifest_old", "event_001");
    let candidate = PublishedProjectionSet::empty("manifest_new", "event_003");

    let failed = complete_pointer_publication(
        previous.clone(),
        candidate.clone(),
        PointerPublishResult::CasFailed,
    );
    let published =
        complete_pointer_publication(previous, candidate, PointerPublishResult::Published);

    assert_eq!(failed.manifest_id, "manifest_old");
    assert_eq!(failed.ledger_watermark, "event_001");
    assert_eq!(published.manifest_id, "manifest_new");
    assert_eq!(published.ledger_watermark, "event_003");
}

#[derive(Debug, Clone)]
struct PinnedIdentity {
    size: u64,
    version: String,
    etag: Option<String>,
}

struct CacheRaceBackend {
    inner: Arc<dyn StorageBackend>,
    pinned_identities: std::sync::Mutex<BTreeMap<String, PinnedIdentity>>,
    block_get_path: std::sync::Mutex<Option<String>>,
    blocked_get_seen: AtomicBool,
    blocked_get: tokio::sync::Notify,
    release_get_seen: AtomicBool,
    release_get: tokio::sync::Notify,
    storage_governance_heads: AtomicUsize,
    storage_governance_head_notify: tokio::sync::Notify,
}

impl std::fmt::Debug for CacheRaceBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheRaceBackend")
            .field(
                "storage_governance_heads",
                &self.storage_governance_heads.load(Ordering::SeqCst),
            )
            .finish_non_exhaustive()
    }
}

impl CacheRaceBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            pinned_identities: std::sync::Mutex::new(BTreeMap::new()),
            block_get_path: std::sync::Mutex::new(None),
            blocked_get_seen: AtomicBool::new(false),
            blocked_get: tokio::sync::Notify::new(),
            release_get_seen: AtomicBool::new(false),
            release_get: tokio::sync::Notify::new(),
            storage_governance_heads: AtomicUsize::new(0),
            storage_governance_head_notify: tokio::sync::Notify::new(),
        }
    }

    fn pin_identity(&self, path: &str, meta: &ObjectMeta) {
        self.pinned_identities
            .lock()
            .expect("pinned identity lock")
            .insert(
                path.to_string(),
                PinnedIdentity {
                    size: meta.size,
                    version: meta.version.clone(),
                    etag: meta.etag.clone(),
                },
            );
    }

    fn block_get_once(&self, path: &str) {
        *self.block_get_path.lock().expect("block get lock") = Some(path.to_string());
    }

    async fn wait_for_blocked_get(&self) {
        while !self.blocked_get_seen.load(Ordering::SeqCst) {
            self.blocked_get.notified().await;
        }
    }

    fn release_blocked_get(&self) {
        self.release_get_seen.store(true, Ordering::SeqCst);
        self.release_get.notify_one();
    }

    async fn wait_for_storage_governance_heads(&self, expected: usize) {
        while self.storage_governance_heads.load(Ordering::SeqCst) < expected {
            self.storage_governance_head_notify.notified().await;
        }
    }
}

#[async_trait]
impl StorageBackend for CacheRaceBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        let should_block = {
            let mut block_get_path = self.block_get_path.lock().expect("block get lock");
            if block_get_path.as_deref() == Some(path) {
                block_get_path.take();
                true
            } else {
                false
            }
        };
        if should_block {
            self.blocked_get_seen.store(true, Ordering::SeqCst);
            self.blocked_get.notify_one();
            while !self.release_get_seen.load(Ordering::SeqCst) {
                self.release_get.notified().await;
            }
        }
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        let mut meta = self.inner.head(path).await?;
        if path.ends_with(STORAGE_GOVERNANCE_PROJECTION) {
            self.storage_governance_heads.fetch_add(1, Ordering::SeqCst);
            self.storage_governance_head_notify.notify_waiters();
        }
        if let Some(meta) = meta.as_mut() {
            if let Some(identity) = self
                .pinned_identities
                .lock()
                .expect("pinned identity lock")
                .get(path)
            {
                meta.size = identity.size;
                meta.version.clone_from(&identity.version);
                meta.etag.clone_from(&identity.etag);
            }
        }
        Ok(meta)
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

struct FailOncePutBackend {
    inner: Arc<dyn StorageBackend>,
    fail_put_suffix: std::sync::Mutex<Option<String>>,
}

impl std::fmt::Debug for FailOncePutBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailOncePutBackend").finish_non_exhaustive()
    }
}

impl FailOncePutBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            fail_put_suffix: std::sync::Mutex::new(None),
        }
    }

    fn fail_put_once(&self, suffix: &str) {
        *self.fail_put_suffix.lock().expect("fail-put lock") = Some(suffix.to_string());
    }
}

#[async_trait]
impl StorageBackend for FailOncePutBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        let should_fail = {
            let mut fail_put_suffix = self.fail_put_suffix.lock().expect("fail-put lock");
            let should_fail = fail_put_suffix
                .as_ref()
                .is_some_and(|suffix| path.ends_with(suffix));
            if should_fail {
                fail_put_suffix.take();
            }
            should_fail
        };
        if should_fail {
            return Err(arco_core::Error::Storage {
                message: format!("injected put failure: {path}"),
                source: None,
            });
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

fn storage_governance_projection_bytes_with_external_url(url: &str) -> Result<Bytes> {
    let events = scoped_storage_governance_events_for_scope(
        &test_scope(),
        "workspace1",
        url,
        "gs://bucket/managed/",
    );
    let state = replay_events(events.iter())?;
    let projection = build_projection_set(&state, &ProjectionRegistry::default(), "event_004")?;
    projection
        .file(STORAGE_GOVERNANCE_PROJECTION)
        .expect("storage-governance projection")
        .write_parquet()
}

fn storage_governance_events() -> Vec<MetastoreEvent> {
    vec![
        MetastoreEvent::new(
            "event_001",
            1,
            MetastoreMutation::StorageCredentialUpserted(StorageCredentialRecord {
                credential_id: "cred_01".to_string(),
                name: "lakehouse-prod".to_string(),
                cloud: "gcs".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_000,
                properties: sensitive_properties(),
            }),
        ),
        MetastoreEvent::new(
            "event_002",
            2,
            MetastoreMutation::ExternalLocationUpserted(ExternalLocationRecord {
                location_id: "loc_orders".to_string(),
                name: "orders".to_string(),
                url: "gs://bucket/warehouse/orders/".to_string(),
                credential_id: "cred_01".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_001,
                properties: sensitive_properties(),
            }),
        ),
        MetastoreEvent::new(
            "event_003",
            3,
            MetastoreMutation::ManagedRootUpserted(ManagedRootRecord {
                root_id: "root_main".to_string(),
                name: "main".to_string(),
                workspace_id: "workspace1".to_string(),
                url: "gs://bucket/managed/".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_002,
                properties: sensitive_properties(),
            }),
        ),
        MetastoreEvent::new(
            "event_004",
            4,
            MetastoreMutation::WorkspaceBindingUpserted(WorkspaceBindingRecord {
                binding_id: "binding_orders".to_string(),
                workspace_id: "workspace1".to_string(),
                object_id: "loc_orders".to_string(),
                object_type: "EXTERNAL_LOCATION".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_003,
                properties: sensitive_properties(),
            }),
        ),
    ]
}

fn scoped_storage_governance_events(scope: &ControlPlaneScope) -> Vec<MetastoreEvent> {
    storage_governance_events()
        .into_iter()
        .map(|event| {
            MetastoreEvent::new_scoped(scope, event.event_id, event.sequence, event.mutation)
        })
        .collect()
}

fn scoped_storage_governance_events_for_scope(
    scope: &ControlPlaneScope,
    workspace_id: &str,
    external_url: &str,
    managed_url: &str,
) -> Vec<MetastoreEvent> {
    vec![
        MetastoreEvent::new_scoped(
            scope,
            "event_001",
            1,
            MetastoreMutation::StorageCredentialUpserted(StorageCredentialRecord {
                credential_id: "cred_01".to_string(),
                name: "lakehouse-prod".to_string(),
                cloud: "gcs".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_000,
                properties: sensitive_properties(),
            }),
        ),
        MetastoreEvent::new_scoped(
            scope,
            "event_002",
            2,
            MetastoreMutation::ExternalLocationUpserted(ExternalLocationRecord {
                location_id: "loc_orders".to_string(),
                name: "orders".to_string(),
                url: external_url.to_string(),
                credential_id: "cred_01".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_001,
                properties: sensitive_properties(),
            }),
        ),
        MetastoreEvent::new_scoped(
            scope,
            "event_003",
            3,
            MetastoreMutation::ManagedRootUpserted(ManagedRootRecord {
                root_id: "root_main".to_string(),
                name: "main".to_string(),
                workspace_id: workspace_id.to_string(),
                url: managed_url.to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_002,
                properties: sensitive_properties(),
            }),
        ),
        MetastoreEvent::new_scoped(
            scope,
            "event_004",
            4,
            MetastoreMutation::WorkspaceBindingUpserted(WorkspaceBindingRecord {
                binding_id: "binding_orders".to_string(),
                workspace_id: workspace_id.to_string(),
                object_id: "loc_orders".to_string(),
                object_type: "EXTERNAL_LOCATION".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_003,
                properties: sensitive_properties(),
            }),
        ),
    ]
}

async fn publish_storage_governance_projection(
    storage: &ScopedStorage,
    events: Vec<MetastoreEvent>,
) -> Result<arco_catalog::metastore::publish::MetastoreProjectionManifest> {
    let ledger = MetastoreLedger::new(storage.clone());
    for event in events {
        ledger.append_event(&event).await?;
    }
    let state = ledger.replay().await?;
    let projection = build_projection_set(&state, &ProjectionRegistry::default(), "event_004")?;
    publish_metastore_projection_set(storage, &projection, 4).await
}

fn test_scope() -> ControlPlaneScope {
    ControlPlaneScope::workspace_alias("tenant1", "workspace1").expect("test scope")
}

async fn write_metastore_event_without_projection_sidecar(
    storage: &ScopedStorage,
    event: &MetastoreEvent,
) -> Result<()> {
    let event_bytes = serde_json::to_vec_pretty(event).expect("event should serialize");
    storage
        .put_raw(
            &format!("ledger/metastore/{}.json", event.event_id),
            Bytes::from(event_bytes),
            WritePrecondition::DoesNotExist,
        )
        .await?;
    storage
        .put_raw(
            &format!("ledger/metastore-sequences/{:020}.event_id", event.sequence),
            Bytes::copy_from_slice(event.event_id.as_bytes()),
            WritePrecondition::DoesNotExist,
        )
        .await?;
    let latest = MetastoreLedgerWatermark {
        event_id: event.event_id.clone(),
        sequence: event.sequence,
    };
    let latest_bytes = serde_json::to_vec_pretty(&latest).expect("latest watermark serializes");
    storage
        .put_raw(
            "ledger/metastore-latest/watermark.json",
            Bytes::from(latest_bytes),
            WritePrecondition::None,
        )
        .await?;
    Ok(())
}

async fn write_metastore_event_with_pending_watermark(
    storage: &ScopedStorage,
    event: &MetastoreEvent,
) -> Result<()> {
    let event_bytes = serde_json::to_vec_pretty(event).expect("event should serialize");
    storage
        .put_raw(
            &format!("ledger/metastore/{}.json", event.event_id),
            Bytes::from(event_bytes),
            WritePrecondition::DoesNotExist,
        )
        .await?;
    storage
        .put_raw(
            &format!("ledger/metastore-sequences/{:020}.event_id", event.sequence),
            Bytes::copy_from_slice(event.event_id.as_bytes()),
            WritePrecondition::DoesNotExist,
        )
        .await?;
    let pending = MetastoreLedgerWatermark {
        event_id: event.event_id.clone(),
        sequence: event.sequence,
    };
    let pending_bytes = serde_json::to_vec_pretty(&pending).expect("pending watermark serializes");
    storage
        .put_raw(
            "ledger/metastore-latest/pending.json",
            Bytes::from(pending_bytes),
            WritePrecondition::None,
        )
        .await?;
    Ok(())
}

fn assert_latest_watermark_pending(err: CatalogError) {
    assert!(
        matches!(
            &err,
            CatalogError::InvariantViolation { message }
                if message == "metastore latest watermark update is pending"
        ),
        "unexpected latest watermark pending error: {err:?}"
    );
}

fn assert_projection_stale(err: CatalogError) {
    assert!(
        matches!(
            &err,
            CatalogError::RequestFailed {
                http_status: 503,
                message
            } if message == "storage_governance_projection_stale"
        ),
        "unexpected projection freshness error: {err:?}"
    );
}

fn assert_projection_missing(err: CatalogError) {
    assert!(
        matches!(
            &err,
            CatalogError::RequestFailed {
                http_status: 503,
                message
            } if message == "storage_governance_projection_missing"
        ),
        "unexpected projection missing error: {err:?}"
    );
}

fn assert_projection_unavailable(err: CatalogError) {
    assert!(
        matches!(
            &err,
            CatalogError::RequestFailed {
                http_status: 503,
                message
            } if message == "storage_governance_projection_unavailable"
        ),
        "unexpected projection availability error: {err:?}"
    );
}

fn assert_projection_unsupported(err: CatalogError) {
    assert!(
        matches!(
            &err,
            CatalogError::RequestFailed {
                http_status: 503,
                message
            } if message == "storage_governance_projection_unsupported"
        ),
        "unexpected projection support error: {err:?}"
    );
}

fn storage_governance_snapshot_path(sequence: u64) -> String {
    format!("snapshots/metastore/v{sequence}/{STORAGE_GOVERNANCE_PROJECTION}")
}

fn scoped_storage_credential_event(
    scope: &ControlPlaneScope,
    event_id: &str,
    sequence: u64,
    credential_id: &str,
) -> MetastoreEvent {
    MetastoreEvent::new_scoped(
        scope,
        event_id,
        sequence,
        MetastoreMutation::StorageCredentialUpserted(StorageCredentialRecord {
            credential_id: credential_id.to_string(),
            name: format!("{credential_id}-name"),
            cloud: "gcs".to_string(),
            owner: "owner".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 1_800_000_000_000 + i64::try_from(sequence).expect("sequence fits"),
            properties: BTreeMap::new(),
        }),
    )
}

fn scoped_principal_event(
    scope: &ControlPlaneScope,
    event_id: &str,
    sequence: u64,
    principal_id: &str,
) -> MetastoreEvent {
    MetastoreEvent::new_scoped(
        scope,
        event_id,
        sequence,
        MetastoreMutation::PrincipalUpserted(PrincipalRecord {
            principal_id: principal_id.to_string(),
            name: format!("{principal_id}@example.com"),
            principal_kind: PrincipalKind::User,
            owner: "metastore-admin".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 1_800_000_000_000 + i64::try_from(sequence).expect("sequence fits"),
            properties: BTreeMap::new(),
        }),
    )
}

fn sample_events() -> Vec<MetastoreEvent> {
    vec![
        MetastoreEvent::new(
            "event_001",
            1,
            MetastoreMutation::PrincipalUpserted(PrincipalRecord {
                principal_id: "principal_01".to_string(),
                name: "alice@example.com".to_string(),
                principal_kind: PrincipalKind::User,
                owner: "metastore-admin".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_000,
                properties: sensitive_properties(),
            }),
        ),
        MetastoreEvent::new(
            "event_002",
            2,
            MetastoreMutation::GrantUpserted(GrantRecord {
                grant_id: "grant_01".to_string(),
                object_id: "table_01".to_string(),
                object_type: "TABLE".to_string(),
                principal_id: "principal_01".to_string(),
                privilege: "SELECT".to_string(),
                owner: "metastore-admin".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_001,
                properties: sensitive_properties(),
            }),
        ),
        MetastoreEvent::new(
            "event_003",
            3,
            MetastoreMutation::StorageCredentialUpserted(StorageCredentialRecord {
                credential_id: "cred_01".to_string(),
                name: "lakehouse-prod".to_string(),
                cloud: "aws".to_string(),
                owner: "group:data-platform".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_002,
                properties: sensitive_properties(),
            }),
        ),
    ]
}

fn sensitive_properties() -> BTreeMap<String, String> {
    BTreeMap::from([("api_token".to_string(), "secret-token".to_string())])
}

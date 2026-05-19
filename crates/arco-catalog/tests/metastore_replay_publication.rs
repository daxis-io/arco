//! Tests for the native metastore replay and projection kernel.

#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use arco_catalog::Result;
use arco_catalog::metastore::events::{
    ExternalLocationRecord, GrantRecord, LifecycleState, ManagedRootRecord, MetastoreEvent,
    MetastoreMutation, PrincipalKind, PrincipalRecord, StorageCredentialRecord,
    WorkspaceBindingRecord,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::metastore::projections::{
    METASTORE_OBJECTS_PROJECTION, ProjectionRegistry, STORAGE_GOVERNANCE_PROJECTION,
    build_projection_set,
};
use arco_catalog::metastore::publish::{
    PointerPublishResult, PublishedProjectionSet, complete_pointer_publication,
};
use arco_catalog::metastore::replay::replay_events;
use arco_core::storage::{MemoryBackend, WritePrecondition};
use arco_core::{ControlPlaneScope, ScopedStorage};
use bytes::Bytes;

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
                secret_material_ref: Some("secret://credential/cred_01".to_string()),
                encrypted_payload: Some("encrypted-secret".to_string()),
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

fn test_scope() -> ControlPlaneScope {
    ControlPlaneScope::workspace_alias("tenant1", "workspace1").expect("test scope")
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
            secret_material_ref: Some(format!("secret://credential/{credential_id}")),
            encrypted_payload: Some("encrypted-secret".to_string()),
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
                secret_material_ref: Some("secret://credential/cred_01".to_string()),
                encrypted_payload: Some("encrypted-secret".to_string()),
            }),
        ),
    ]
}

fn sensitive_properties() -> BTreeMap<String, String> {
    BTreeMap::from([("api_token".to_string(), "secret-token".to_string())])
}

//! Task 4 coverage for storage governance and path ownership.

use arco_catalog::Result;
use arco_catalog::metastore::events::{
    ExternalLocationRecord, LifecycleState, ManagedRootRecord, MetastoreEvent, MetastoreMutation,
    StorageCredentialRecord, WorkspaceBindingRecord,
};
use arco_catalog::metastore::replay::replay_events;
use arco_catalog::storage_governance::bindings::WorkspaceBinding;
use arco_catalog::storage_governance::credentials::{CredentialSecret, StorageCredentialMetadata};
use arco_catalog::storage_governance::external_locations::ExternalLocation;
use arco_catalog::storage_governance::managed_roots::ManagedRoot;
use arco_catalog::storage_governance::path_normalization::GovernedPath;
use arco_catalog::storage_governance::{PathAuthorityKind, PathDecision, StorageGovernanceState};
use std::collections::BTreeMap;

#[test]
fn governed_paths_canonicalize_cloud_and_local_uris() {
    let gcs = GovernedPath::parse("gs://Lake-Bucket//warehouse/%66acts/")
        .expect("gcs path")
        .canonical_uri();
    assert_eq!(gcs, "gs://lake-bucket/warehouse/facts/");

    let s3 = GovernedPath::parse("s3://Prod-Bucket//Team//Orders")
        .expect("s3 path")
        .canonical_uri();
    assert_eq!(s3, "s3://prod-bucket/Team/Orders/");

    let azure = GovernedPath::parse("abfss://Container@Account.dfs.core.windows.net//root/table")
        .expect("azure path")
        .canonical_uri();
    assert_eq!(
        azure,
        "abfss://container@account.dfs.core.windows.net/root/table/"
    );

    let local = GovernedPath::parse("file:///tmp//arco/dev")
        .expect("local path")
        .canonical_uri();
    assert_eq!(local, "file:///tmp/arco/dev/");
}

#[test]
fn governed_paths_reject_dot_segments_and_bad_percent_encoding() {
    assert!(GovernedPath::parse("gs://bucket/root/../escape").is_err());
    assert!(GovernedPath::parse("gs://bucket/root/%2e%2e/escape").is_err());
    assert!(GovernedPath::parse("gs://bucket/root/%2e%2e%2fescape").is_err());
    assert!(GovernedPath::parse("gs://bucket/root/safe%2Fescape").is_err());
    assert!(GovernedPath::parse("gs://bucket/root/safe%5cescape").is_err());
    assert!(GovernedPath::parse("gs://bucket/root/%zz").is_err());
}

#[test]
fn storage_credentials_are_redacted_in_metadata_views() {
    let mut state = StorageGovernanceState::default();
    state
        .create_storage_credential(
            StorageCredentialMetadata::new("cred_01", "lakehouse-prod", "gcs", "owner"),
            CredentialSecret::new("secret://cred/01", "encrypted-token"),
        )
        .expect("create credential");

    let credential = state
        .get_storage_credential("cred_01")
        .expect("credential")
        .expect("present");
    assert_eq!(credential.credential_id, "cred_01");
    assert_eq!(credential.name, "lakehouse-prod");
    let serialized = serde_json::to_string(&credential).expect("serialize metadata");
    assert!(!serialized.contains("secret://"));
    assert!(!serialized.contains("encrypted-token"));
}

#[test]
fn storage_governance_rejects_overlapping_locations_and_roots() -> Result<()> {
    let mut state = seeded_state();
    state
        .create_external_location(ExternalLocation::new(
            "loc_orders",
            "orders",
            "gs://bucket/warehouse/orders",
            "cred_01",
            "owner",
        )?)
        .expect("create external location");

    let sibling = state.create_external_location(ExternalLocation::new(
        "loc_customers",
        "customers",
        "gs://bucket/warehouse/customers",
        "cred_01",
        "owner",
    )?);
    assert!(sibling.is_ok());

    let child = state.create_external_location(ExternalLocation::new(
        "loc_orders_child",
        "orders-child",
        "gs://bucket/warehouse/orders/2026",
        "cred_01",
        "owner",
    )?);
    assert!(child.is_err());

    let parent_root = state.create_managed_root(ManagedRoot::new(
        "root_warehouse",
        "warehouse",
        "workspace1",
        "gs://bucket/warehouse",
        "owner",
    )?);
    assert!(parent_root.is_err());
    Ok(())
}

#[test]
fn path_ownership_enforces_workspace_bindings() -> Result<()> {
    let mut state = seeded_state();
    state
        .create_external_location(ExternalLocation::new(
            "loc_orders",
            "orders",
            "gs://bucket/warehouse/orders",
            "cred_01",
            "owner",
        )?)
        .expect("create external location");
    state
        .bind_workspace(WorkspaceBinding::new(
            "binding_01",
            "workspace1",
            "loc_orders",
            "EXTERNAL_LOCATION",
            "owner",
        ))
        .expect("bind workspace");

    let decision = state
        .authority_for_path(
            "workspace1",
            "gs://bucket/warehouse/orders/day=1/file.parquet",
        )
        .expect("authority");
    assert_eq!(
        decision,
        PathDecision::owned("loc_orders", PathAuthorityKind::ExternalLocation)
    );

    assert!(
        state
            .authority_for_path(
                "workspace2",
                "gs://bucket/warehouse/orders/day=1/file.parquet"
            )
            .is_err()
    );

    Ok(())
}

#[test]
fn path_ownership_rejects_workspace_binding_with_wrong_object_type() -> Result<()> {
    let mut state = seeded_state();
    state
        .create_external_location(ExternalLocation::new(
            "loc_orders",
            "orders",
            "gs://bucket/warehouse/orders",
            "cred_01",
            "owner",
        )?)
        .expect("create external location");

    let binding = state.bind_workspace(WorkspaceBinding::new(
        "binding_wrong_type",
        "workspace1",
        "loc_orders",
        "MANAGED_ROOT",
        "owner",
    ));

    assert!(binding.is_err());
    assert!(
        state
            .authority_for_path(
                "workspace1",
                "gs://bucket/warehouse/orders/day=1/file.parquet"
            )
            .is_err()
    );
    Ok(())
}

#[test]
fn path_ownership_uses_replayed_metastore_storage_governance_state() -> Result<()> {
    let metastore = replay_events(storage_governance_events().iter())?;
    let state = StorageGovernanceState::from_metastore_state(&metastore)?;

    let location_decision = state.authority_for_path(
        "workspace1",
        "gs://bucket/warehouse/orders/day=1/file.parquet",
    )?;
    assert_eq!(
        location_decision,
        PathDecision::owned("loc_orders", PathAuthorityKind::ExternalLocation)
    );

    let root_decision = state.authority_for_path(
        "workspace1",
        "gs://bucket/managed/table/_delta_log/000.json",
    )?;
    assert_eq!(
        root_decision,
        PathDecision::owned("root_main", PathAuthorityKind::ManagedRoot)
    );

    assert!(
        state
            .authority_for_path("workspace2", "gs://bucket/warehouse/orders/file.parquet")
            .is_err()
    );

    let listed_credentials = state.list_storage_credentials();
    let serialized = serde_json::to_string(&listed_credentials).expect("serialize credentials");
    assert!(!serialized.contains("secret://"));
    assert!(!serialized.contains("encrypted-token"));
    Ok(())
}

fn seeded_state() -> StorageGovernanceState {
    let mut state = StorageGovernanceState::default();
    state
        .create_storage_credential(
            StorageCredentialMetadata::new("cred_01", "lakehouse-prod", "gcs", "owner"),
            CredentialSecret::new("secret://cred/01", "encrypted-token"),
        )
        .expect("create credential");
    state
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
                properties: BTreeMap::new(),
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
                properties: BTreeMap::new(),
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
                properties: BTreeMap::new(),
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
                properties: BTreeMap::new(),
            }),
        ),
    ]
}

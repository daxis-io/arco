//! Task 4 coverage for storage governance and path ownership.

// Test-target lint scope (#331): tests and their helpers signal failure by
// panicking. clippy.toml scopes the restriction lints out of #[test] fns;
// this header extends the same policy to this file's shared helpers.
#![allow(clippy::expect_used)]

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

/// Property: for every parse-accepted input, the canonical URI is a fixed
/// point of `GovernedPath::parse` — re-parsing succeeds, yields an equal
/// governed path, and canonicalization is idempotent. The corpus is derived
/// from the grammar `parse` accepts: percent-escaped literals (including `%25`
/// and attempts around `%2F`), spaces, unicode (raw and encoded), `+`, `?`,
/// `#`, the full literal `pchar` set, all supported schemes, and degenerate
/// authority-only shapes.
#[test]
fn canonical_uris_are_parse_fixed_points_over_adversarial_corpus() {
    let corpus = [
        "gs://bucket/warehouse/orders",
        "gs://bucket/warehouse/orders/",
        "gs://bucket/warehouse/100%25-complete",
        "gs://bucket/warehouse/100%2525-complete",
        "gs://Bucket//warehouse//%66acts/",
        "gs://bucket/warehouse/day%3D01",
        "gs://bucket/warehouse/with%20space",
        "gs://bucket/warehouse/with space",
        "gs://bucket/warehouse/a+b",
        "gs://bucket/warehouse/%C3%BCber",
        "gs://bucket/warehouse/über",
        "gs://bucket/warehouse/query?like",
        "gs://bucket/warehouse/frag#ment",
        "gs://bucket/warehouse/tilde~_.-!$&'()*,;=:@",
        "gs://bucket/warehouse/...",
        "s3://Prod-Bucket//Team//Orders",
        "abfss://Container@Account.dfs.core.windows.net//root/table",
        "file:///tmp//arco/dev",
        "file:///tmp/100%25/space here",
        "gs://bucket",
        "gs://bucket/",
    ];

    for raw in corpus {
        let parsed = GovernedPath::parse(raw)
            .unwrap_or_else(|error| panic!("corpus entry {raw:?} must parse: {error:?}"));
        let canonical = parsed.canonical_uri();
        let reparsed = GovernedPath::parse(&canonical).unwrap_or_else(|error| {
            panic!("canonical URI {canonical:?} from {raw:?} must re-parse: {error:?}")
        });
        assert_eq!(
            reparsed, parsed,
            "canonical URI {canonical:?} from {raw:?} must be a parse fixed point"
        );
        assert_eq!(
            reparsed.canonical_uri(),
            canonical,
            "canonicalization must be idempotent for {raw:?}"
        );
        // The persistence-boundary guard accepts every well-formed canonical
        // URI and returns the identical string.
        assert_eq!(
            parsed
                .persistable_canonical_uri()
                .unwrap_or_else(|error| panic!("persistence guard must accept {raw:?}: {error:?}")),
            canonical
        );
    }
}

/// Regression for the persistent-poison chain: a location URL carrying a
/// percent-escaped literal `%` must canonicalize to a string that re-parses
/// (`%25` re-encoded), never to a bare `%` that fails every future replay.
#[test]
fn percent_literals_are_reencoded_into_round_trip_safe_canonical_uris() {
    let path =
        GovernedPath::parse("gs://bucket/warehouse/100%25-complete").expect("percent literal");
    assert_eq!(
        path.canonical_uri(),
        "gs://bucket/warehouse/100%25-complete/"
    );
    assert_eq!(
        GovernedPath::parse(&path.canonical_uri()).expect("canonical URI re-parses"),
        path
    );
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
                secret_material_ref: Some("secret://credential/cred_01".to_string()),
                encrypted_payload: Some("encrypted-token".to_string()),
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

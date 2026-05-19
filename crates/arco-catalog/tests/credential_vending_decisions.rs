//! Task 5 coverage for credential vending decisions.

use std::time::Duration;

use arco_catalog::Result;
use arco_catalog::credential_vending::{
    CredentialDecision, CredentialOperation, CredentialVendingEngine, CredentialVendingRequest,
};
use arco_catalog::metastore::events::LifecycleState;
use arco_catalog::storage_governance::StorageGovernanceState;
use arco_catalog::storage_governance::bindings::WorkspaceBinding;
use arco_catalog::storage_governance::credentials::{CredentialSecret, StorageCredentialMetadata};
use arco_catalog::storage_governance::external_locations::ExternalLocation;

#[test]
fn credential_vending_allows_governed_gcs_path_with_clamped_ttl_and_audit_id() -> Result<()> {
    let state = seeded_state()?;
    let engine = CredentialVendingEngine::default();

    let decision = engine.decide_path(
        &state,
        CredentialVendingRequest {
            principal_id: "user_alice".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-allow".to_string(),
            operation: CredentialOperation::Read,
            requested_path: "gs://bucket/warehouse/orders/day=1/".to_string(),
            requested_ttl: Duration::from_secs(7200),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
        },
    )?;

    assert_eq!(decision.decision, CredentialDecision::Allow);
    assert_eq!(decision.reason_code, "allowed");
    assert_eq!(decision.provider, Some("gcs".to_string()));
    assert_eq!(decision.credential_kind, Some("scoped_bearer".to_string()));
    assert_eq!(
        decision.authorized_path_prefixes,
        vec!["gs://bucket/warehouse/orders/day=1/"]
    );
    assert_eq!(decision.max_ttl, Duration::from_secs(3600));
    assert!(!decision.audit_event_id.is_empty());

    let debug = format!("{decision:?}");
    assert!(!debug.contains("secret://"));
    assert!(!debug.contains("encrypted-token"));
    Ok(())
}

#[test]
fn credential_vending_denies_ungoverned_paths_with_audit_id() -> Result<()> {
    let state = seeded_state()?;
    let engine = CredentialVendingEngine::default();

    let decision = engine.decide_path(
        &state,
        CredentialVendingRequest {
            principal_id: "user_alice".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-deny".to_string(),
            operation: CredentialOperation::Read,
            requested_path: "gs://bucket/unowned/orders/".to_string(),
            requested_ttl: Duration::from_secs(300),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
        },
    )?;

    assert_eq!(decision.decision, CredentialDecision::Deny);
    assert_eq!(decision.reason_code, "path_not_governed");
    assert!(decision.authorized_path_prefixes.is_empty());
    assert_eq!(decision.max_ttl, Duration::from_secs(300));
    assert!(!decision.audit_event_id.is_empty());
    Ok(())
}

#[test]
fn credential_vending_denies_unsupported_operations_closed() -> Result<()> {
    let state = seeded_state()?;
    let engine = CredentialVendingEngine::default();

    let decision = engine.decide_path(
        &state,
        CredentialVendingRequest {
            principal_id: "user_alice".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-delete".to_string(),
            operation: CredentialOperation::Delete,
            requested_path: "gs://bucket/warehouse/orders/day=1/".to_string(),
            requested_ttl: Duration::from_secs(300),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
        },
    )?;

    assert_eq!(decision.decision, CredentialDecision::Deny);
    assert_eq!(decision.reason_code, "unsupported_operation");
    assert!(decision.authorized_path_prefixes.is_empty());
    assert!(!decision.audit_event_id.is_empty());
    Ok(())
}

#[test]
fn credential_vending_denies_external_locations_backed_by_disabled_credentials() -> Result<()> {
    let mut state = StorageGovernanceState::default();
    let mut credential =
        StorageCredentialMetadata::new("cred_01", "lakehouse-prod", "gcs", "owner");
    credential.lifecycle_state = LifecycleState::Disabled;
    state.create_storage_credential(
        credential,
        CredentialSecret::new("secret://cred/01", "encrypted-token"),
    )?;
    state.create_external_location(ExternalLocation::new(
        "loc_orders",
        "orders",
        "gs://bucket/warehouse/orders",
        "cred_01",
        "owner",
    )?)?;
    state.bind_workspace(WorkspaceBinding::new(
        "binding_01",
        "workspace1",
        "loc_orders",
        "EXTERNAL_LOCATION",
        "owner",
    ))?;
    let engine = CredentialVendingEngine::default();

    let decision = engine.decide_path(
        &state,
        CredentialVendingRequest {
            principal_id: "user_alice".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-disabled".to_string(),
            operation: CredentialOperation::Read,
            requested_path: "gs://bucket/warehouse/orders/day=1/".to_string(),
            requested_ttl: Duration::from_secs(300),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
        },
    )?;

    assert_eq!(decision.decision, CredentialDecision::Deny);
    assert_eq!(decision.reason_code, "storage_credential_not_active");
    assert!(decision.provider.is_none());
    assert!(decision.authorized_path_prefixes.is_empty());
    Ok(())
}

fn seeded_state() -> Result<StorageGovernanceState> {
    let mut state = StorageGovernanceState::default();
    state.create_storage_credential(
        StorageCredentialMetadata::new("cred_01", "lakehouse-prod", "gcs", "owner"),
        CredentialSecret::new("secret://cred/01", "encrypted-token"),
    )?;
    state.create_external_location(ExternalLocation::new(
        "loc_orders",
        "orders",
        "gs://bucket/warehouse/orders",
        "cred_01",
        "owner",
    )?)?;
    state.bind_workspace(WorkspaceBinding::new(
        "binding_01",
        "workspace1",
        "loc_orders",
        "EXTERNAL_LOCATION",
        "owner",
    ))?;
    Ok(state)
}

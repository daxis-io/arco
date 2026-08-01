//! Task 5 coverage for credential vending decisions.

use std::time::Duration;

use chrono::Utc;

use arco_catalog::Result;
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::credential_vending::{
    CredentialDecision, CredentialOperation, CredentialVendingAuthorization,
    CredentialVendingEngine, CredentialVendingRequest, MAX_CREDENTIAL_TTL,
    MAX_PROJECTION_STALENESS, REVOCATION_FRESHNESS_BUDGET, REVOCATION_FRESHNESS_BUDGET_SECS,
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
        &CredentialVendingRequest {
            principal_id: "user_alice".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-allow".to_string(),
            operation: CredentialOperation::Read,
            requested_path: "gs://bucket/warehouse/orders/day=1/".to_string(),
            requested_ttl: Duration::from_secs(7200),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
            freshness_observed_at: Utc::now(),
            authorization: Some(path_authorization("event_004")),
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
    // Clamped to MAX_CREDENTIAL_TTL measured from the freshness observation,
    // not from the decision instant: a 7200s request is cut to at most 3600s,
    // minus whatever elapsed since the watermark was observed.
    assert!(decision.max_ttl <= Duration::from_secs(3600));
    assert!(decision.max_ttl > Duration::from_secs(3599));
    assert!(!decision.audit_event_id.is_empty());

    let debug = format!("{decision:?}");
    assert!(!debug.contains("secret://"));
    assert!(!debug.contains("encrypted-token"));
    Ok(())
}

#[test]
fn credential_vending_denies_without_authorization_context() -> Result<()> {
    let state = seeded_state()?;
    let engine = CredentialVendingEngine::default();

    let decision = engine.decide_path(
        &state,
        &CredentialVendingRequest {
            principal_id: "user_mallory".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-no-authz".to_string(),
            operation: CredentialOperation::Read,
            requested_path: "gs://bucket/warehouse/orders/day=1/".to_string(),
            requested_ttl: Duration::from_secs(300),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
            freshness_observed_at: Utc::now(),
            authorization: None,
        },
    )?;

    assert_eq!(decision.decision, CredentialDecision::Deny);
    assert_eq!(decision.reason_code, "authorization_required");
    assert!(decision.authorized_path_prefixes.is_empty());
    Ok(())
}

#[test]
fn credential_vending_denies_stale_authorization_watermark() -> Result<()> {
    let state = seeded_state()?;
    let engine = CredentialVendingEngine::default();

    let decision = engine.decide_path(
        &state,
        &CredentialVendingRequest {
            principal_id: "user_alice".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-stale-authz".to_string(),
            operation: CredentialOperation::Read,
            requested_path: "gs://bucket/warehouse/orders/day=1/".to_string(),
            requested_ttl: Duration::from_secs(300),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
            freshness_observed_at: Utc::now(),
            authorization: Some(path_authorization("event_003")),
        },
    )?;

    assert_eq!(decision.decision, CredentialDecision::Deny);
    assert_eq!(decision.reason_code, "stale_projection");
    assert!(decision.provider.is_none());
    assert!(decision.credential_kind.is_none());
    assert!(decision.authorized_object_id.is_none());
    assert!(decision.authorized_path_prefixes.is_empty());
    assert!(!decision.audit_event_id.is_empty());
    Ok(())
}

#[test]
fn credential_vending_denies_unsupported_authorization_object_type() -> Result<()> {
    let state = seeded_state()?;
    let engine = CredentialVendingEngine::default();
    let mut authorization = path_authorization("event_004");
    authorization.object_type = "CATALOG".to_string();
    authorization.privilege = Privilege::ReadFiles;

    let decision = engine.decide_path(
        &state,
        &CredentialVendingRequest {
            principal_id: "user_alice".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-unsupported-authz-object".to_string(),
            operation: CredentialOperation::Read,
            requested_path: "gs://bucket/warehouse/orders/day=1/".to_string(),
            requested_ttl: Duration::from_secs(300),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
            freshness_observed_at: Utc::now(),
            authorization: Some(authorization),
        },
    )?;

    assert_eq!(decision.decision, CredentialDecision::Deny);
    assert_eq!(
        decision.reason_code,
        "authorization_unsupported_object_type"
    );
    assert!(decision.authorized_path_prefixes.is_empty());
    Ok(())
}

#[test]
fn credential_vending_denies_ungoverned_paths_with_audit_id() -> Result<()> {
    let state = seeded_state()?;
    let engine = CredentialVendingEngine::default();

    let decision = engine.decide_path(
        &state,
        &CredentialVendingRequest {
            principal_id: "user_alice".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-deny".to_string(),
            operation: CredentialOperation::Read,
            requested_path: "gs://bucket/unowned/orders/".to_string(),
            requested_ttl: Duration::from_secs(300),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
            freshness_observed_at: Utc::now(),
            authorization: Some(path_authorization("event_004")),
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
        &CredentialVendingRequest {
            principal_id: "user_alice".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-delete".to_string(),
            operation: CredentialOperation::Delete,
            requested_path: "gs://bucket/warehouse/orders/day=1/".to_string(),
            requested_ttl: Duration::from_secs(300),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
            freshness_observed_at: Utc::now(),
            authorization: Some(path_authorization("event_004")),
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
        &CredentialVendingRequest {
            principal_id: "user_alice".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-disabled".to_string(),
            operation: CredentialOperation::Read,
            requested_path: "gs://bucket/warehouse/orders/day=1/".to_string(),
            requested_ttl: Duration::from_secs(300),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
            freshness_observed_at: Utc::now(),
            authorization: Some(path_authorization("event_004")),
        },
    )?;

    assert_eq!(decision.decision, CredentialDecision::Deny);
    assert_eq!(decision.reason_code, "storage_credential_not_active");
    assert!(decision.provider.is_none());
    assert!(decision.authorized_path_prefixes.is_empty());
    Ok(())
}

/// Revocation-freshness budget arithmetic (roadmap Phase 6 required test).
///
/// The worst-case duration a revoked authorization can still be honored is the
/// projection-staleness bound the vending path enforces (zero: exact-watermark
/// equality, see `metastore::publish`) plus the maximum vended credential TTL
/// (the engine clamp). The documented budget is 3600 seconds.
#[test]
fn revocation_freshness_budget_is_projection_staleness_plus_max_ttl() -> Result<()> {
    let engine = CredentialVendingEngine::default();

    // The budget is the sum of its two named halves.
    assert_eq!(
        REVOCATION_FRESHNESS_BUDGET,
        MAX_PROJECTION_STALENESS + MAX_CREDENTIAL_TTL
    );
    // The documented worst-case number: 0s staleness + 3600s max TTL.
    assert_eq!(MAX_PROJECTION_STALENESS, Duration::ZERO);
    assert_eq!(MAX_CREDENTIAL_TTL, Duration::from_secs(3600));
    assert_eq!(REVOCATION_FRESHNESS_BUDGET_SECS, 3600);
    // The default engine's exposure budget equals the documented budget.
    assert_eq!(
        engine.revocation_exposure_budget(),
        REVOCATION_FRESHNESS_BUDGET
    );
    assert_eq!(engine.max_ttl(), MAX_CREDENTIAL_TTL);

    // Adversarial TTL requests cannot widen the TTL half of the budget: even a
    // week-long requested TTL is clamped to MAX_CREDENTIAL_TTL on allow.
    let decision = engine.decide_path(
        &seeded_state()?,
        &CredentialVendingRequest {
            principal_id: "user_alice".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-budget-clamp".to_string(),
            operation: CredentialOperation::Read,
            requested_path: "gs://bucket/warehouse/orders/day=1/".to_string(),
            requested_ttl: Duration::from_secs(7 * 24 * 3600),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
            freshness_observed_at: Utc::now(),
            authorization: Some(path_authorization("event_004")),
        },
    )?;
    assert_eq!(decision.decision, CredentialDecision::Allow);
    // The TTL half is now anchored to the freshness observation, so an allow
    // decided `elapsed` after the observation is clamped to
    // `MAX_CREDENTIAL_TTL - elapsed`: never above the budget, and no lower
    // than the budget minus the in-test decision latency.
    assert!(decision.max_ttl <= MAX_CREDENTIAL_TTL);
    assert!(decision.max_ttl > MAX_CREDENTIAL_TTL - Duration::from_secs(1));
    Ok(())
}

/// A revoked external location visible in fresh state can never be vended:
/// once the revocation is replayed into the decision state, the revoked scope
/// no longer resolves to a path authority and vending denies.
#[test]
fn revoked_external_location_with_fresh_state_cannot_be_vended() -> Result<()> {
    use arco_catalog::metastore::events::{
        ExternalLocationRecord, MetastoreEvent, MetastoreMutation, StorageCredentialRecord,
        WorkspaceBindingRecord,
    };
    use arco_catalog::metastore::replay::replay_events;

    let events = vec![
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
                properties: std::collections::BTreeMap::new(),
                secret_material_ref: None,
                encrypted_payload: None,
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
                properties: std::collections::BTreeMap::new(),
            }),
        ),
        MetastoreEvent::new(
            "event_003",
            3,
            MetastoreMutation::WorkspaceBindingUpserted(WorkspaceBindingRecord {
                binding_id: "binding_orders".to_string(),
                workspace_id: "workspace1".to_string(),
                object_id: "loc_orders".to_string(),
                object_type: "EXTERNAL_LOCATION".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_002,
                properties: std::collections::BTreeMap::new(),
            }),
        ),
        // Revocation: the external location transitions to Deleted.
        MetastoreEvent::new(
            "event_004",
            4,
            MetastoreMutation::ExternalLocationUpserted(ExternalLocationRecord {
                location_id: "loc_orders".to_string(),
                name: "orders".to_string(),
                url: "gs://bucket/warehouse/orders/".to_string(),
                credential_id: "cred_01".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Deleted,
                updated_at_ms: 1_800_000_000_003,
                properties: std::collections::BTreeMap::new(),
            }),
        ),
    ];
    let state = StorageGovernanceState::from_metastore_state(&replay_events(events.iter())?)?;
    let engine = CredentialVendingEngine::default();

    let decision = engine.decide_path(
        &state,
        &CredentialVendingRequest {
            principal_id: "user_alice".to_string(),
            groups_snapshot_version: "groups-rev-1".to_string(),
            workspace_id: "workspace1".to_string(),
            request_id: "request-revoked".to_string(),
            operation: CredentialOperation::Read,
            requested_path: "gs://bucket/warehouse/orders/day=1/".to_string(),
            requested_ttl: Duration::from_secs(300),
            client_kind: "uc".to_string(),
            catalog_snapshot_version: "event_004".to_string(),
            freshness_observed_at: Utc::now(),
            authorization: Some(path_authorization("event_004")),
        },
    )?;

    assert_eq!(decision.decision, CredentialDecision::Deny);
    assert_eq!(decision.reason_code, "path_not_governed");
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

fn path_authorization(permission_ledger_watermark: &str) -> CredentialVendingAuthorization {
    CredentialVendingAuthorization {
        principal_id: "user_alice".to_string(),
        object_id: "loc_orders".to_string(),
        object_type: "EXTERNAL_LOCATION".to_string(),
        privilege: Privilege::ReadFiles,
        permission_ledger_watermark: permission_ledger_watermark.to_string(),
        path_authority_object_id: "loc_orders".to_string(),
        path_authority_object_type: "EXTERNAL_LOCATION".to_string(),
    }
}

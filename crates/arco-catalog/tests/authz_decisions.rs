//! Task 3 coverage for authorization decisions and explain-access output.

use arco_catalog::authz::compiler::{CompiledPermissionRow, CompiledPermissionSet};
use arco_catalog::authz::decision::{AuthzDecision, AuthzRequest, DecisionOutcome};
use arco_catalog::authz::explain::explain_access;
use arco_catalog::authz::privileges::Privilege;

#[test]
fn authz_decision_allows_from_compiled_permission_and_records_evidence() {
    let compiled = compiled_permissions(true);
    let request = AuthzRequest::new("user_alice", "table_orders", "TABLE", Privilege::Select)
        .with_request_id("req-allow");
    let decision = AuthzDecision::evaluate(&request, &compiled);

    assert_eq!(decision.outcome, DecisionOutcome::Allow);
    assert_eq!(decision.reason_code, "direct_or_inherited_grant");
    assert_eq!(
        decision.group_snapshot_version.as_deref(),
        Some("groups-rev-7")
    );
    assert_eq!(decision.evidence.grant_ids, vec!["grant_catalog_select"]);
}

#[test]
fn authz_decision_denies_by_default_for_missing_grants() {
    let compiled = compiled_permissions(true);
    let request = AuthzRequest::new("user_bob", "table_orders", "TABLE", Privilege::Select)
        .with_request_id("req-deny");
    let decision = AuthzDecision::evaluate(&request, &compiled);

    assert_eq!(decision.outcome, DecisionOutcome::Deny);
    assert_eq!(decision.reason_code, "absence_of_allow");
    assert!(decision.reason_message_safe.contains("no compiled allow"));
}

#[test]
fn authz_decision_denies_stale_or_unknown_compiled_permissions() {
    let stale = compiled_permissions(false);
    let request = AuthzRequest::new("user_alice", "table_orders", "TABLE", Privilege::Select)
        .with_request_id("req-stale");
    let decision = AuthzDecision::evaluate(&request, &stale);
    assert_eq!(decision.outcome, DecisionOutcome::Deny);
    assert_eq!(decision.reason_code, "stale_projection");

    let compiled = compiled_permissions(true);
    let request = AuthzRequest::new("user_alice", "unknown_01", "CONNECTION", Privilege::Select)
        .with_request_id("req-unknown");
    let decision = AuthzDecision::evaluate(&request, &compiled);
    assert_eq!(decision.outcome, DecisionOutcome::Deny);
    assert_eq!(decision.reason_code, "unknown_object_type");
}

#[test]
fn explain_access_matches_enforcement_decision() {
    let compiled = compiled_permissions(true);
    let request = AuthzRequest::new("user_bob", "table_orders", "TABLE", Privilege::Select)
        .with_request_id("req-explain");
    let decision = AuthzDecision::evaluate(&request, &compiled);
    let explanation = explain_access(&request, &compiled);

    assert_eq!(explanation.decision.outcome, decision.outcome);
    assert_eq!(explanation.decision.reason_code, decision.reason_code);
    assert_eq!(explanation.request_id, "req-explain");
}

fn compiled_permissions(fresh: bool) -> CompiledPermissionSet {
    CompiledPermissionSet::new(
        "event_004",
        "groups-rev-7",
        fresh,
        vec![CompiledPermissionRow {
            principal_id: "user_alice".to_string(),
            object_id: "table_orders".to_string(),
            object_type: "TABLE".to_string(),
            privilege: Privilege::Select,
            source: "grant".to_string(),
            source_grant_id: Some("grant_catalog_select".to_string()),
            source_principal_id: "group_data".to_string(),
            source_object_id: "catalog_sales".to_string(),
            inheritance_path: "catalog_sales/schema_retail/table_orders".to_string(),
            grant_option: false,
            group_snapshot_version: "groups-rev-7".to_string(),
        }],
    )
}

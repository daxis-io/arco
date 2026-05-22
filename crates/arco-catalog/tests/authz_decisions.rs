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

#[test]
fn authorization_index_matches_scan_semantics_with_many_irrelevant_rows() {
    let mut rows = Vec::new();
    for i in 0..1_000 {
        rows.push(CompiledPermissionRow {
            principal_id: format!("principal_{i}"),
            object_id: format!("object_{i}"),
            object_type: "TABLE".to_string(),
            privilege: Privilege::Select,
            source: "grant".to_string(),
            source_grant_id: Some(format!("grant_{i}")),
            source_principal_id: format!("principal_{i}"),
            source_object_id: format!("object_{i}"),
            inheritance_path: format!("object_{i}"),
            grant_option: false,
            group_snapshot_version: "groups_1".to_string(),
        });
    }
    rows.push(CompiledPermissionRow {
        principal_id: "alice".to_string(),
        object_id: "table_1".to_string(),
        object_type: "TABLE".to_string(),
        privilege: Privilege::Select,
        source: "grant".to_string(),
        source_grant_id: Some("grant_allow".to_string()),
        source_principal_id: "alice".to_string(),
        source_object_id: "table_1".to_string(),
        inheritance_path: "table_1".to_string(),
        grant_option: false,
        group_snapshot_version: "groups_1".to_string(),
    });

    let compiled = CompiledPermissionSet::new("event_1", "groups_1", true, rows);
    let request = AuthzRequest::new("alice", "table_1", "TABLE", Privilege::Select);
    let decision = AuthzDecision::evaluate(&request, &compiled);

    assert_eq!(decision.outcome, DecisionOutcome::Allow);
    assert_eq!(decision.evidence.grant_ids, vec!["grant_allow"]);
}

#[test]
fn authorization_index_matches_object_type_case_insensitively() {
    let compiled = CompiledPermissionSet::new(
        "event_1",
        "groups_1",
        true,
        vec![CompiledPermissionRow {
            principal_id: "alice".to_string(),
            object_id: "table_1".to_string(),
            object_type: "table".to_string(),
            privilege: Privilege::Select,
            source: "grant".to_string(),
            source_grant_id: Some("grant_lowercase_table".to_string()),
            source_principal_id: "alice".to_string(),
            source_object_id: "table_1".to_string(),
            inheritance_path: "table_1".to_string(),
            grant_option: false,
            group_snapshot_version: "groups_1".to_string(),
        }],
    );

    let request = AuthzRequest::new("alice", "table_1", "TABLE", Privilege::Select);
    let decision = AuthzDecision::evaluate(&request, &compiled);

    assert_eq!(decision.outcome, DecisionOutcome::Allow);
    assert_eq!(decision.evidence.grant_ids, vec!["grant_lowercase_table"]);
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

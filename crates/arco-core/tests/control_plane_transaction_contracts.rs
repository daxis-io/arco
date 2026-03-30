//! Contract tests for shared control-plane transaction request and response types.
//!
//! These assertions lock in the ADR-034 PI-1 wire contract:
//! canonical fenced fields are serialized, legacy orchestration `epoch`
//! payloads remain accepted in compatibility mode, and responses surface
//! visibility plus repair state.

#![allow(clippy::expect_used)]

use arco_core::orchestration_compaction::{
    OrchestrationCompactRequest, OrchestrationCompactionResponse, OrchestrationRebuildRequest,
};
use arco_core::sync_compact::{SyncCompactRequest, SyncCompactResponse, VisibilityStatus};

#[test]
fn sync_compact_request_serializes_canonical_lock_fields() {
    let request = SyncCompactRequest {
        domain: "catalog".to_string(),
        event_paths: vec!["ledger/catalog/01JTEST.json".to_string()],
        fencing_token: 42,
        lock_path: Some("locks/catalog.lock.json".to_string()),
        request_id: Some("req_01".to_string()),
    };

    let json = serde_json::to_value(&request).expect("serialize");
    assert_eq!(json["fencing_token"], 42);
    assert_eq!(json["lock_path"], "locks/catalog.lock.json");
    assert!(json.get("epoch").is_none());
}

#[test]
fn sync_compact_response_includes_visibility_and_repair_status() {
    let response = SyncCompactResponse {
        manifest_version: "obj-version-7".to_string(),
        commit_ulid: "01JTESTCOMMIT".to_string(),
        events_processed: 1,
        snapshot_version: 7,
        visibility_status: VisibilityStatus::Visible,
        repair_pending: true,
    };

    let json = serde_json::to_value(&response).expect("serialize");
    assert_eq!(json["visibility_status"], "visible");
    assert_eq!(json["repair_pending"], true);
}

#[test]
fn orchestration_compact_request_accepts_legacy_epoch_alias() {
    let request: OrchestrationCompactRequest = serde_json::from_str(
        r#"{
            "event_paths": ["ledger/orchestration/2026-03-28/01JTEST.json"],
            "epoch": 9,
            "lock_path": "locks/orchestration.compaction.lock.json",
            "request_id": "req_legacy"
        }"#,
    )
    .expect("deserialize");

    assert_eq!(request.fencing_token, Some(9));
    assert_eq!(
        request.lock_path.as_deref(),
        Some("locks/orchestration.compaction.lock.json")
    );
    assert_eq!(request.request_id.as_deref(), Some("req_legacy"));
}

#[test]
fn orchestration_rebuild_request_serializes_canonical_fencing_field() {
    let request = OrchestrationRebuildRequest {
        rebuild_manifest_path: "state/orchestration/rebuilds/rebuild-01.json".to_string(),
        fencing_token: Some(7),
        lock_path: Some("locks/orchestration.compaction.lock.json".to_string()),
        request_id: Some("req_rebuild".to_string()),
    };

    let json = serde_json::to_value(&request).expect("serialize");
    assert_eq!(json["fencing_token"], 7);
    assert_eq!(
        json["lock_path"],
        "locks/orchestration.compaction.lock.json"
    );
    assert!(json.get("epoch").is_none());
}

#[test]
fn orchestration_compaction_response_includes_repair_pending() {
    let response = OrchestrationCompactionResponse {
        events_processed: 2,
        delta_id: Some("01JDELTA".to_string()),
        manifest_revision: "01JMANIFEST".to_string(),
        visibility_status: VisibilityStatus::Visible,
        repair_pending: true,
    };

    let json = serde_json::to_value(&response).expect("serialize");
    assert_eq!(json["visibility_status"], "visible");
    assert_eq!(json["repair_pending"], true);
}

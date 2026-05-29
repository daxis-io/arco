//! Worker contract round-trip tests.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::path::PathBuf;

use arco_worker_contract::{
    CallbackErrorResponse, HeartbeatRequest, HeartbeatResponse, TaskCompletedRequest,
    TaskStartedRequest, WorkerDispatchEnvelope, callback_task_id, deterministic_attempt_id,
    parse_callback_task_id,
};
use serde::de::DeserializeOwned;
use serde_json::json;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("worker_protocol")
        .join(name)
}

fn fixture_value(name: &str) -> serde_json::Value {
    let fixture = fs::read_to_string(fixture_path(name)).expect("read fixture");
    serde_json::from_str(&fixture).expect("parse fixture")
}

fn assert_fixture_roundtrip<T>(name: &str)
where
    T: DeserializeOwned + serde::Serialize,
{
    let value = fixture_value(name);
    let parsed: T = serde_json::from_value(value.clone()).expect("deserialize fixture");
    let serialized = serde_json::to_value(parsed).expect("serialize fixture");
    assert_eq!(serialized, value, "{name}");
}

#[test]
fn canonical_worker_dispatch_envelope_matches_fixture() {
    assert_fixture_roundtrip::<WorkerDispatchEnvelope>("worker_dispatch_envelope.json");
}

#[test]
fn worker_dispatch_envelope_accepts_legacy_snake_case_fixture() {
    let value = fixture_value("worker_dispatch_envelope_legacy_snake_case.json");
    let parsed: WorkerDispatchEnvelope =
        serde_json::from_value(value).expect("legacy fixture should deserialize");

    assert_eq!(parsed.task_id, "analytics.daily_sales");
    assert_eq!(parsed.task_key, "analytics.daily_sales");
    assert_eq!(parsed.execution_location_id.as_deref(), Some("local-dev"));

    let serialized = serde_json::to_value(parsed).expect("serialize canonical envelope");
    assert!(serialized.get("taskId").is_some());
    assert!(serialized.get("callbackBaseUrl").is_some());
    assert!(serialized.get("task_key").is_none());
}

#[test]
fn callback_request_fixtures_roundtrip() {
    assert_fixture_roundtrip::<TaskStartedRequest>("task_started_request.json");
    assert_fixture_roundtrip::<HeartbeatRequest>("heartbeat_request.json");
    assert_fixture_roundtrip::<HeartbeatResponse>("heartbeat_response.json");
    assert_fixture_roundtrip::<TaskCompletedRequest>("task_completed_request.json");
    assert_fixture_roundtrip::<CallbackErrorResponse>("callback_error_payload.json");
}

#[test]
fn callback_task_id_roundtrips_run_and_task_key() {
    let task_id = callback_task_id("run-123", "analytics.daily_sales");
    assert_eq!(task_id, "ct1_cnVuLTEyMwBhbmFseXRpY3MuZGFpbHlfc2FsZXM");

    let parsed = parse_callback_task_id(&task_id).expect("parse callback task id");
    assert_eq!(parsed.run_id, "run-123");
    assert_eq!(parsed.task_key, "analytics.daily_sales");
}

#[test]
fn callback_task_id_rejects_legacy_task_key() {
    let err = parse_callback_task_id("analytics.daily_sales").expect_err("legacy task key");
    assert!(err.to_string().contains("ct1_"));
}

#[test]
fn deterministic_attempt_id_matches_existing_contract() {
    let dispatch_id = "dispatch:run-123:analytics.daily_sales:1";
    assert_eq!(
        deterministic_attempt_id(dispatch_id),
        deterministic_attempt_id(dispatch_id)
    );
    assert!(deterministic_attempt_id(dispatch_id).starts_with("att_"));
}

#[test]
fn heartbeat_progress_rejects_values_above_one_hundred() {
    let err = serde_json::from_value::<HeartbeatRequest>(json!({
        "attempt": 1,
        "attemptId": "att-123",
        "workerId": "worker-1",
        "progressPct": 101
    }))
    .expect_err("invalid progress");

    assert!(err.to_string().contains("progressPct"));
}

#[test]
fn completed_request_accepts_legacy_result_alias() {
    let parsed = serde_json::from_value::<TaskCompletedRequest>(json!({
        "attempt": 1,
        "attemptId": "att-123",
        "workerId": "worker-1",
        "outcome": "SUCCEEDED",
        "result": {
            "rowCount": 1
        }
    }))
    .expect("legacy result alias");

    assert_eq!(parsed.output.expect("output").row_count, Some(1));
}

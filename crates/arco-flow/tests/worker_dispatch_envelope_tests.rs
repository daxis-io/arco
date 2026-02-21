//! Worker dispatch envelope contract tests.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use arco_flow::orchestration::worker_contract::WorkerDispatchEnvelope;
use chrono::{TimeZone, Utc};
use serde_json::json;

fn sample_envelope() -> WorkerDispatchEnvelope {
    WorkerDispatchEnvelope {
        tenant_id: "tenant-a".to_string(),
        workspace_id: "workspace-b".to_string(),
        run_id: "run-123".to_string(),
        task_key: "analytics.daily_sales".to_string(),
        attempt: 2,
        attempt_id: "01HQ123ATT".to_string(),
        dispatch_id: "dispatch:run-123:analytics.daily_sales:2".to_string(),
        worker_queue: "default-queue".to_string(),
        callback_base_url: "https://api.arco.dev".to_string(),
        task_token: "jwt-token".to_string(),
        token_expires_at: Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("timestamp"),
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string()),
        payload: json!({
            "asset": "analytics.daily_sales",
            "partition": "date=2026-01-01"
        }),
    }
}

#[test]
fn worker_dispatch_envelope_round_trips_json() {
    let envelope = sample_envelope();

    let json = serde_json::to_string(&envelope).expect("serialize envelope");
    let parsed: WorkerDispatchEnvelope = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(parsed.tenant_id, "tenant-a");
    assert_eq!(parsed.workspace_id, "workspace-b");
    assert_eq!(parsed.run_id, "run-123");
    assert_eq!(parsed.task_key, "analytics.daily_sales");
    assert_eq!(parsed.attempt, 2);
    assert_eq!(parsed.attempt_id, "01HQ123ATT");
    assert_eq!(
        parsed.dispatch_id,
        "dispatch:run-123:analytics.daily_sales:2"
    );
    assert_eq!(parsed.worker_queue, "default-queue");
    assert_eq!(parsed.callback_base_url, "https://api.arco.dev");
    assert_eq!(parsed.task_token, "jwt-token");
    assert_eq!(
        parsed.traceparent.as_deref().expect("traceparent present"),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    );
    assert_eq!(parsed.payload["asset"], "analytics.daily_sales");
}

#[test]
fn worker_dispatch_envelope_rejects_missing_required_fields() {
    let invalid = json!({
        "tenant_id": "tenant-a",
        "workspace_id": "workspace-b",
        "run_id": "run-123",
        "task_key": "analytics.daily_sales",
        "attempt": 2,
        "attempt_id": "01HQ123ATT",
        "dispatch_id": "dispatch:run-123:analytics.daily_sales:2",
        "worker_queue": "default-queue",
        "task_token": "jwt-token",
        "token_expires_at": "2026-01-01T00:00:00Z",
        "payload": {}
    });

    let err = serde_json::from_value::<WorkerDispatchEnvelope>(invalid)
        .expect_err("missing callback_base_url must fail");

    assert!(err.to_string().contains("callback_base_url"));
}

#[test]
fn worker_dispatch_envelope_omits_traceparent_when_absent() {
    let mut envelope = sample_envelope();
    envelope.traceparent = None;

    let value = serde_json::to_value(&envelope).expect("serialize envelope");

    assert!(value.get("traceparent").is_none());
}

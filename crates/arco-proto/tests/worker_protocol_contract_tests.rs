//! Generated worker protocol contract tests.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;

use arco_proto::arco::orchestration::v1::{
    WorkerCallbackError, WorkerDispatchEnvelope, WorkerHeartbeatRequest, WorkerHeartbeatResponse,
    WorkerOutcome, WorkerTaskCompletedRequest, WorkerTaskCompletedResponse, WorkerTaskError,
    WorkerTaskMetrics, WorkerTaskOutput, WorkerTaskStartedRequest, WorkerTaskStartedResponse,
};
use prost::Message;
use prost_types::{Struct, Timestamp, Value, value};

fn timestamp(seconds: i64) -> Option<Timestamp> {
    Some(Timestamp { seconds, nanos: 0 })
}

fn payload_struct() -> Option<Struct> {
    let mut fields = BTreeMap::new();
    fields.insert(
        "partition".to_string(),
        Value {
            kind: Some(value::Kind::StringValue("date=2026-01-01".to_string())),
        },
    );
    Some(Struct { fields })
}

#[test]
fn worker_dispatch_envelope_proto_roundtrips_wire_format() {
    let envelope = WorkerDispatchEnvelope {
        tenant_id: "tenant-a".to_string(),
        workspace_id: "workspace-b".to_string(),
        task_id: arco_worker_contract::callback_task_id("run-123", "analytics.daily_sales"),
        task_key: "analytics.daily_sales".to_string(),
        run_id: "run-123".to_string(),
        attempt: 1,
        attempt_id: "att-123".to_string(),
        dispatch_id: "dispatch:run-123:analytics.daily_sales:1".to_string(),
        execution_location_id: Some("local-dev".to_string()),
        worker_queue: "default-queue".to_string(),
        callback_base_url: "https://callbacks.example".to_string(),
        task_token: "token".to_string(),
        token_expires_at: timestamp(1_767_225_600),
        traceparent: None,
        payload: payload_struct(),
    };

    let encoded = envelope.encode_to_vec();
    let decoded = WorkerDispatchEnvelope::decode(encoded.as_slice()).expect("decode envelope");

    assert_eq!(decoded.task_id, envelope.task_id);
    assert_eq!(decoded.task_key, "analytics.daily_sales");
    assert_eq!(decoded.execution_location_id.as_deref(), Some("local-dev"));
    let partition = decoded
        .payload
        .and_then(|payload| payload.fields.get("partition").cloned())
        .and_then(|value| value.kind)
        .expect("partition");
    assert!(matches!(
        partition,
        value::Kind::StringValue(value) if value == "date=2026-01-01"
    ));
}

#[test]
fn callback_request_response_messages_roundtrip_wire_format() {
    let started = WorkerTaskStartedRequest {
        attempt: 1,
        attempt_id: "att-123".to_string(),
        worker_id: "worker-1".to_string(),
        traceparent: None,
        started_at: timestamp(1_767_225_601),
    };
    let decoded =
        WorkerTaskStartedRequest::decode(started.encode_to_vec().as_slice()).expect("started");
    assert_eq!(decoded.attempt_id, "att-123");

    let started_response = WorkerTaskStartedResponse {
        acknowledged: true,
        server_time: timestamp(1_767_225_602),
    };
    let decoded = WorkerTaskStartedResponse::decode(started_response.encode_to_vec().as_slice())
        .expect("started response");
    assert!(decoded.acknowledged);

    let heartbeat = WorkerHeartbeatRequest {
        attempt: 1,
        attempt_id: "att-123".to_string(),
        worker_id: "worker-1".to_string(),
        traceparent: None,
        heartbeat_at: timestamp(1_767_225_605),
        progress_pct: Some(45),
        message: Some("loading".to_string()),
    };
    let decoded =
        WorkerHeartbeatRequest::decode(heartbeat.encode_to_vec().as_slice()).expect("heartbeat");
    assert_eq!(decoded.progress_pct, Some(45));

    let heartbeat_response = WorkerHeartbeatResponse {
        acknowledged: true,
        should_cancel: false,
        cancel_reason: None,
        server_time: timestamp(1_767_225_606),
    };
    let decoded = WorkerHeartbeatResponse::decode(heartbeat_response.encode_to_vec().as_slice())
        .expect("heartbeat response");
    assert!(!decoded.should_cancel);
}

#[test]
fn worker_heartbeat_contract_rejects_out_of_range_progress() {
    let heartbeat = WorkerHeartbeatRequest {
        attempt: 1,
        attempt_id: "att-123".to_string(),
        worker_id: "worker-1".to_string(),
        traceparent: None,
        heartbeat_at: None,
        progress_pct: Some(101),
        message: None,
    };

    assert!(heartbeat.validate_contract().is_err());
}

#[test]
fn worker_callback_contract_rejects_zero_attempt() {
    let started = WorkerTaskStartedRequest {
        attempt: 0,
        attempt_id: "att-123".to_string(),
        worker_id: "worker-1".to_string(),
        traceparent: None,
        started_at: None,
    };

    assert!(started.validate_contract().is_err());
}

#[test]
fn completed_and_error_messages_roundtrip_wire_format() {
    let completed = WorkerTaskCompletedRequest {
        attempt: 1,
        attempt_id: "att-123".to_string(),
        worker_id: "worker-1".to_string(),
        traceparent: None,
        outcome: WorkerOutcome::Failed as i32,
        completed_at: timestamp(1_767_225_610),
        output: Some(WorkerTaskOutput {
            materialization_id: Some("mat-1".to_string()),
            row_count: Some(10),
            byte_size: None,
            output_path: None,
            delta_table: None,
            delta_version: None,
            delta_partition: None,
            output_visibility_state: 0,
            published_at: None,
            publish_error: None,
        }),
        error: Some(WorkerTaskError {
            category: 3,
            message: "transient object storage failure".to_string(),
            stack_trace: None,
            retryable: Some(true),
        }),
        metrics: Some(WorkerTaskMetrics {
            cpu_time_ms: Some(12),
            peak_memory_bytes: Some(4096),
            io_read_bytes: Some(1024),
            io_write_bytes: Some(2048),
        }),
        cancelled_during_phase: None,
        partial_progress: None,
    };

    let decoded = WorkerTaskCompletedRequest::decode(completed.encode_to_vec().as_slice())
        .expect("completed");
    assert_eq!(decoded.outcome, WorkerOutcome::Failed as i32);
    assert_eq!(
        decoded.metrics.expect("metrics").peak_memory_bytes,
        Some(4096)
    );

    let response = WorkerTaskCompletedResponse {
        acknowledged: true,
        final_state: "FAILED".to_string(),
        server_time: timestamp(1_767_225_611),
    };
    let decoded = WorkerTaskCompletedResponse::decode(response.encode_to_vec().as_slice())
        .expect("completed response");
    assert_eq!(decoded.final_state, "FAILED");

    let error = WorkerCallbackError {
        error: "attempt_id_mismatch".to_string(),
        message: "callback attempt id is stale".to_string(),
        state: Some("RUNNING".to_string()),
        expected_attempt: Some(1),
        received_attempt: Some(1),
        expected_attempt_id: Some("att-123".to_string()),
        received_attempt_id: Some("att-456".to_string()),
    };
    let decoded =
        WorkerCallbackError::decode(error.encode_to_vec().as_slice()).expect("callback error");
    assert_eq!(decoded.expected_attempt_id.as_deref(), Some("att-123"));
}

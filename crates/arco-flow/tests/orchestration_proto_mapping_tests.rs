#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
#![allow(missing_docs)]

use chrono::{TimeZone, Utc};

use arco_flow::orchestration::callbacks::{
    ErrorCategory as CallbackErrorCategory, TaskError as CallbackTaskError,
    TaskMetrics as CallbackTaskMetrics, TaskOutput as CallbackTaskOutput,
};
use arco_flow::orchestration::compactor::fold::{
    RunRow, RunState as FoldRunState, TaskRow, TaskState as FoldTaskState,
};
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData, TaskOutcome};
use arco_flow::orchestration::proto::{
    event_from_proto_envelope, event_to_proto_envelope, fold_run_state_to_proto,
    fold_task_state_to_proto, task_row_to_proto_execution,
};
use arco_proto::arco::orchestration::v1::{
    OutputVisibilityState, RunState as ProtoRunState, TaskState as ProtoTaskState,
    orchestration_event_envelope,
};

fn base_task_row(state: FoldTaskState) -> TaskRow {
    TaskRow {
        run_id: "run-01".to_string(),
        task_key: "extract".to_string(),
        state,
        attempt: 1,
        attempt_id: Some("attempt-01".to_string()),
        started_at: Some(Utc.with_ymd_and_hms(2026, 4, 9, 12, 0, 5).unwrap()),
        completed_at: Some(Utc.with_ymd_and_hms(2026, 4, 9, 12, 1, 5).unwrap()),
        error_message: None,
        deps_total: 0,
        deps_satisfied_count: 0,
        max_attempts: 3,
        heartbeat_timeout_sec: 300,
        last_heartbeat_at: Some(Utc.with_ymd_and_hms(2026, 4, 9, 12, 0, 45).unwrap()),
        ready_at: Some(Utc.with_ymd_and_hms(2026, 4, 9, 12, 0, 0).unwrap()),
        asset_key: Some("default.raw.events".to_string()),
        partition_key: Some("date=d:2026-04-09".to_string()),
        requires_visible_output: false,
        materialization_id: None,
        output_visibility_state: None,
        published_at: None,
        publish_error: None,
        delta_table: None,
        delta_version: None,
        delta_partition: None,
        execution_lineage_ref: None,
        row_version: "01JTESTTASKROW".to_string(),
    }
}

#[test]
fn task_finished_proto_round_trip_preserves_typed_callback_payloads_without_public_json_escape_hatch()
 {
    let event = OrchestrationEvent::new(
        "tenant-01",
        "workspace-01",
        OrchestrationEventData::TaskFinished {
            run_id: "run-01".to_string(),
            task_key: "extract".to_string(),
            attempt: 1,
            attempt_id: "attempt-01".to_string(),
            worker_id: "worker-01".to_string(),
            outcome: TaskOutcome::Succeeded,
            materialization_id: Some("mat-01".to_string()),
            error_message: Some("boom".to_string()),
            output: Some(CallbackTaskOutput {
                materialization_id: Some("mat-01".to_string()),
                row_count: Some(25),
                byte_size: Some(512),
                output_path: Some("s3://bucket/out/part-000.parquet".to_string()),
                delta_table: Some("default.raw.events".to_string()),
                delta_version: Some(7),
                delta_partition: Some("date=d:2026-04-09".to_string()),
                output_visibility_state: None,
                published_at: None,
                publish_error: None,
            }),
            error: Some(CallbackTaskError {
                category: CallbackErrorCategory::UserCode,
                message: "boom".to_string(),
                stack_trace: Some("trace".to_string()),
                retryable: Some(false),
            }),
            metrics: Some(CallbackTaskMetrics {
                cpu_time_ms: Some(42),
                peak_memory_bytes: Some(1024),
                io_read_bytes: Some(2048),
                io_write_bytes: Some(4096),
            }),
            cancelled_during_phase: None,
            partial_progress_json: Some("{\"percent\":50}".to_string()),
            asset_key: Some("default.raw.events".to_string()),
            partition_key: Some("date=d:2026-04-09".to_string()),
            code_version: Some("git:abc123".to_string()),
        },
    );

    let envelope = event_to_proto_envelope(&event).expect("event should map to proto");
    envelope
        .validate_contract()
        .expect("mapped proto event should satisfy the orchestration contract");

    let orchestration_event_envelope::Event::TaskFinished(task_finished) =
        envelope.event.as_ref().expect("task_finished payload")
    else {
        panic!("expected task_finished payload");
    };

    let callback_output = task_finished
        .callback_output
        .as_ref()
        .expect("callback_output should be present");
    assert_eq!(
        callback_output.delta_table.as_deref(),
        Some("default.raw.events")
    );
    assert_eq!(callback_output.delta_version, Some(7));

    let roundtrip = event_from_proto_envelope("tenant-01", "workspace-01", &envelope)
        .expect("proto event should map back to runtime");

    let OrchestrationEventData::TaskFinished {
        output,
        error,
        metrics,
        partial_progress_json,
        ..
    } = roundtrip.data
    else {
        panic!("expected task_finished runtime event");
    };

    assert_eq!(
        output
            .as_ref()
            .and_then(|value| value.delta_table.as_deref())
            .unwrap(),
        "default.raw.events"
    );
    assert_eq!(
        output.as_ref().and_then(|value| value.delta_version),
        Some(7)
    );
    assert_eq!(
        error.as_ref().map(|value| value.message.as_str()),
        Some("boom")
    );
    assert_eq!(
        metrics.as_ref().and_then(|value| value.cpu_time_ms),
        Some(42)
    );
    assert!(
        partial_progress_json.is_none(),
        "public proto round-trip should not preserve internal partial progress blobs"
    );
}

#[test]
fn task_finished_proto_round_trip_preserves_synthetic_failure_error_message() {
    let event = OrchestrationEvent::new(
        "tenant-01",
        "workspace-01",
        OrchestrationEventData::TaskFinished {
            run_id: "run-02".to_string(),
            task_key: "extract".to_string(),
            attempt: 3,
            attempt_id: "attempt-03".to_string(),
            worker_id: "arco_flow_automation_reconciler".to_string(),
            outcome: TaskOutcome::Failed,
            materialization_id: None,
            error_message: Some("heartbeat timed out".to_string()),
            output: None,
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress_json: None,
            asset_key: Some("default.raw.events".to_string()),
            partition_key: Some("date=d:2026-04-10".to_string()),
            code_version: Some("git:def456".to_string()),
        },
    );

    let envelope = event_to_proto_envelope(&event).expect("event should map to proto");
    let roundtrip = event_from_proto_envelope("tenant-01", "workspace-01", &envelope)
        .expect("proto event should map back to runtime");

    let OrchestrationEventData::TaskFinished {
        error_message,
        error,
        ..
    } = roundtrip.data
    else {
        panic!("expected task_finished runtime event");
    };

    assert_eq!(error_message.as_deref(), Some("heartbeat timed out"));
    assert!(
        error.is_none(),
        "synthetic failure should stay message-only"
    );
}

#[test]
fn task_finished_proto_round_trip_preserves_top_level_materialization_id_without_callback_output() {
    let event = OrchestrationEvent::new(
        "tenant-01",
        "workspace-01",
        OrchestrationEventData::TaskFinished {
            run_id: "run-03".to_string(),
            task_key: "extract".to_string(),
            attempt: 1,
            attempt_id: "attempt-01".to_string(),
            worker_id: "worker-03".to_string(),
            outcome: TaskOutcome::Succeeded,
            materialization_id: Some("mat-03".to_string()),
            error_message: None,
            output: None,
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress_json: None,
            asset_key: Some("default.raw.events".to_string()),
            partition_key: Some("date=d:2026-04-11".to_string()),
            code_version: Some("git:ghi789".to_string()),
        },
    );

    let envelope = event_to_proto_envelope(&event).expect("event should map to proto");
    let roundtrip = event_from_proto_envelope("tenant-01", "workspace-01", &envelope)
        .expect("proto event should map back to runtime");

    let OrchestrationEventData::TaskFinished {
        materialization_id,
        output,
        ..
    } = roundtrip.data
    else {
        panic!("expected task_finished runtime event");
    };

    assert_eq!(materialization_id.as_deref(), Some("mat-03"));
    assert_eq!(
        output
            .as_ref()
            .and_then(|value| value.materialization_id.as_deref()),
        Some("mat-03")
    );
}

#[test]
fn fold_state_mapping_tracks_public_proto_states_without_old_api_collapsing() {
    let pending_run = RunRow {
        run_id: "run-01".to_string(),
        plan_id: "plan-01".to_string(),
        state: FoldRunState::Triggered,
        run_key: None,
        labels: Default::default(),
        code_version: None,
        cancel_requested: false,
        tasks_total: 1,
        tasks_completed: 0,
        tasks_succeeded: 0,
        tasks_failed: 0,
        tasks_skipped: 0,
        tasks_cancelled: 0,
        triggered_at: Utc.with_ymd_and_hms(2026, 4, 9, 12, 0, 0).unwrap(),
        completed_at: None,
        row_version: "01JTESTRUNROW".to_string(),
    };
    assert_eq!(
        fold_run_state_to_proto(&pending_run),
        ProtoRunState::Pending
    );

    let cancelling_run = RunRow {
        cancel_requested: true,
        state: FoldRunState::Running,
        ..pending_run.clone()
    };
    assert_eq!(
        fold_run_state_to_proto(&cancelling_run),
        ProtoRunState::Cancelling
    );

    assert_eq!(
        fold_task_state_to_proto(&base_task_row(FoldTaskState::Blocked)),
        ProtoTaskState::Pending
    );
    assert_eq!(
        fold_task_state_to_proto(&base_task_row(FoldTaskState::Ready)),
        ProtoTaskState::Ready
    );
    assert_eq!(
        fold_task_state_to_proto(&base_task_row(FoldTaskState::Dispatched)),
        ProtoTaskState::Dispatched
    );
    assert_eq!(
        fold_task_state_to_proto(&base_task_row(FoldTaskState::RetryWait)),
        ProtoTaskState::RetryWait
    );
}

#[test]
fn task_row_public_output_uses_task_output_visibility_contract_not_callback_payload_shape() {
    let mut task = base_task_row(FoldTaskState::Succeeded);
    task.materialization_id = Some("mat-01".to_string());
    task.delta_table = Some("default.raw.events".to_string());
    task.delta_version = Some(7);
    task.delta_partition = Some("date=d:2026-04-09".to_string());

    let execution = task_row_to_proto_execution(&task);
    let output = execution.output.expect("task execution output");

    assert_eq!(output.materialization_id.as_deref(), Some("mat-01"));
    assert_eq!(
        OutputVisibilityState::try_from(output.visibility_state).unwrap(),
        OutputVisibilityState::Pending
    );
    assert!(
        output.files.is_empty(),
        "public output must not leak callback files"
    );
    assert!(
        output.row_count.is_none(),
        "public output must not reuse callback stats"
    );
    assert!(
        output.byte_size.is_none(),
        "public output must not reuse callback stats"
    );
    assert!(output.published_at.is_none());
    assert!(output.publish_error.is_none());
    output
        .validate_contract()
        .expect("derived public output should satisfy the proto contract");
}

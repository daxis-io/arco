//! Minimal embedded worker protocol example.

use chrono::{TimeZone, Utc};
use serde_json::json;

use arco_worker_contract::{
    HeartbeatRequest, TaskCompletedRequest, TaskOutput, TaskStartedRequest, WorkerDispatchEnvelope,
    WorkerOutcome, callback_task_id, parse_callback_task_id,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run_id = "run-123";
    let task_key = "analytics.daily_sales";
    let task_id = callback_task_id(run_id, task_key);
    let token_expires_at = Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .ok_or("invalid timestamp")?;

    let envelope = WorkerDispatchEnvelope {
        tenant_id: "tenant-a".to_string(),
        workspace_id: "workspace-b".to_string(),
        task_id,
        task_key: task_key.to_string(),
        run_id: run_id.to_string(),
        attempt: 1,
        attempt_id: "att-123".to_string(),
        dispatch_id: "dispatch:run-123:analytics.daily_sales:1".to_string(),
        execution_location_id: None,
        worker_queue: "default-queue".to_string(),
        callback_base_url: "https://api.arco.dev".to_string(),
        task_token: "<task-scoped-jwt>".to_string(),
        token_expires_at,
        traceparent: None,
        payload: json!({
            "partition": "date=2026-01-01"
        }),
    };

    let wire_json = envelope.to_json()?;
    let parsed = WorkerDispatchEnvelope::from_json(&wire_json)?;
    let parsed_task_id = parse_callback_task_id(&parsed.task_id)?;
    assert_eq!(parsed_task_id.run_id, parsed.run_id);
    assert_eq!(parsed_task_id.task_key, parsed.task_key);

    let started = TaskStartedRequest {
        attempt: parsed.attempt,
        attempt_id: parsed.attempt_id.clone(),
        worker_id: "worker-1".to_string(),
        traceparent: parsed.traceparent.clone(),
        started_at: None,
    };

    let heartbeat = HeartbeatRequest {
        attempt: parsed.attempt,
        attempt_id: parsed.attempt_id.clone(),
        worker_id: "worker-1".to_string(),
        traceparent: parsed.traceparent.clone(),
        heartbeat_at: None,
        progress_pct: Some(50),
        message: Some("running".to_string()),
    };

    let completed = TaskCompletedRequest {
        attempt: parsed.attempt,
        attempt_id: parsed.attempt_id.clone(),
        worker_id: "worker-1".to_string(),
        traceparent: parsed.traceparent.clone(),
        outcome: WorkerOutcome::Succeeded,
        completed_at: None,
        output: Some(TaskOutput {
            materialization_id: Some("mat-123".to_string()),
            row_count: Some(42),
            byte_size: None,
            output_path: None,
            delta_table: None,
            delta_version: None,
            delta_partition: None,
            output_visibility_state: None,
            published_at: None,
            publish_error: None,
        }),
        error: None,
        metrics: None,
        cancelled_during_phase: None,
        partial_progress: None,
    };

    println!(
        "POST {}/api/v1/tasks/{}/started",
        parsed.callback_base_url, parsed.task_id
    );
    println!("{}", serde_json::to_string(&started)?);
    println!(
        "POST {}/api/v1/tasks/{}/heartbeat",
        parsed.callback_base_url, parsed.task_id
    );
    println!("{}", serde_json::to_string(&heartbeat)?);
    println!(
        "POST {}/api/v1/tasks/{}/completed",
        parsed.callback_base_url, parsed.task_id
    );
    println!("{}", serde_json::to_string(&completed)?);

    Ok(())
}

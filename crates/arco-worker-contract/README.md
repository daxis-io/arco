# arco-worker-contract

Versioned worker dispatch and callback protocol types for embedded Arco deployments.

This crate owns the JSON/OpenAPI worker wire contract. Runtime crates can re-export these
types, but should not redefine dispatch or callback payloads locally.

## Compatibility

- New JSON writes use canonical `camelCase`.
- Dispatch readers accept the legacy snake_case envelope during migration.
- Workers use opaque `taskId` for callbacks and `taskKey` for work selection.
- Legacy task-key callback paths are accepted only when unambiguous.
- Completed callbacks accept legacy `result` as an alias for `output`.
- Heartbeat `progressPct` is an integer from `0` through `100`.
- Callback attempts are one-indexed.

## Embedded Orchestrator Sketch

```rust
use arco_worker_contract::{WorkerDispatchEnvelope, callback_task_id};

let run_id = "run-123";
let task_key = "analytics.daily_sales";
let task_id = callback_task_id(run_id, task_key);

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
    token_expires_at: chrono::Utc::now(),
    traceparent: None,
    payload: serde_json::json!({}),
};

let body = envelope.to_json()?;
```

## Embedded Worker Sketch

```rust
use arco_worker_contract::{
    HeartbeatRequest, TaskCompletedRequest, TaskStartedRequest, WorkerDispatchEnvelope,
    WorkerOutcome,
};

let envelope = WorkerDispatchEnvelope::from_json(dispatch_body)?;
let started_url = format!(
    "{}/api/v1/tasks/{}/started",
    envelope.callback_base_url,
    envelope.task_id
);
let authorization = format!("Bearer {}", envelope.task_token);

let started = TaskStartedRequest {
    attempt: envelope.attempt,
    attempt_id: envelope.attempt_id.clone(),
    worker_id: "worker-1".to_string(),
    traceparent: envelope.traceparent.clone(),
    started_at: None,
};

let heartbeat = HeartbeatRequest {
    attempt: envelope.attempt,
    attempt_id: envelope.attempt_id.clone(),
    worker_id: "worker-1".to_string(),
    traceparent: envelope.traceparent.clone(),
    heartbeat_at: None,
    progress_pct: Some(50),
    message: Some("running".to_string()),
};

let completed = TaskCompletedRequest {
    attempt: envelope.attempt,
    attempt_id: envelope.attempt_id.clone(),
    worker_id: "worker-1".to_string(),
    traceparent: envelope.traceparent.clone(),
    outcome: WorkerOutcome::Succeeded,
    completed_at: None,
    output: None,
    error: None,
    metrics: None,
    cancelled_during_phase: None,
    partial_progress: None,
};
```

Run the compiled example:

```sh
cargo run -p arco-worker-contract --example embedded_protocol
```

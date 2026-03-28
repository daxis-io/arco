//! Smoke tests for the cloud test worker service.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use chrono::Utc;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use arco_flow::orchestration::worker_contract::WorkerDispatchEnvelope;

#[derive(Clone, Debug)]
struct CallbackRecord {
    path: String,
    authorization: Option<String>,
    tenant_id: Option<String>,
    workspace_id: Option<String>,
    body: Value,
}

#[derive(Clone)]
struct CallbackState {
    records: Arc<Mutex<Vec<CallbackRecord>>>,
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn callback_handler(
    State(state): State<CallbackState>,
    request: axum::extract::Request,
) -> StatusCode {
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("body bytes");
    let payload: Value = serde_json::from_slice(&bytes).expect("json body");
    let authorization = parts
        .headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let tenant_id = parts
        .headers
        .get("x-tenant-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let workspace_id = parts
        .headers
        .get("x-workspace-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);

    state.records.lock().await.push(CallbackRecord {
        path: parts.uri.path().to_string(),
        authorization,
        tenant_id,
        workspace_id,
        body: payload,
    });

    StatusCode::OK
}

#[tokio::test]
async fn worker_dispatch_posts_started_and_completed_callbacks() -> Result<()> {
    let callback_records = Arc::new(Mutex::new(Vec::new()));
    let callback_state = CallbackState {
        records: Arc::clone(&callback_records),
    };
    let callback_app = Router::new()
        .route("/health", get(health_handler))
        .route("/api/v1/tasks/:task_id/started", post(callback_handler))
        .route("/api/v1/tasks/:task_id/completed", post(callback_handler))
        .with_state(callback_state);

    let callback_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let callback_addr: SocketAddr = callback_listener.local_addr()?;
    let callback_server = tokio::spawn(async move {
        axum::serve(callback_listener, callback_app)
            .await
            .expect("callback server");
    });

    let worker_app = arco_flow_worker::build_app("cloud-test-worker".to_string());
    let worker_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let worker_addr: SocketAddr = worker_listener.local_addr()?;
    let worker_server = tokio::spawn(async move {
        axum::serve(worker_listener, worker_app)
            .await
            .expect("worker server");
    });

    let client = reqwest::Client::new();
    let health = client
        .get(format!("http://{worker_addr}/health"))
        .send()
        .await?;
    assert_eq!(health.status(), StatusCode::OK);

    let envelope = WorkerDispatchEnvelope {
        tenant_id: "tenant-dev".to_string(),
        workspace_id: "workspace-dev".to_string(),
        run_id: "run_01".to_string(),
        task_key: "analytics.users".to_string(),
        attempt: 1,
        attempt_id: "attempt_01".to_string(),
        dispatch_id: "dispatch:run_01:analytics.users:1".to_string(),
        worker_queue: "default-queue".to_string(),
        callback_base_url: format!("http://{callback_addr}"),
        task_token: "task-token-123".to_string(),
        token_expires_at: Utc::now(),
        traceparent: None,
        payload: json!({
            "simulate": "success",
            "deltaTable": "analytics.users",
            "deltaVersion": 42
        }),
    };

    let dispatch = client
        .post(format!("http://{worker_addr}/dispatch"))
        .json(&envelope)
        .send()
        .await?;
    assert_eq!(dispatch.status(), StatusCode::ACCEPTED);

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let records = callback_records.lock().await.clone();
    assert_eq!(records.len(), 2, "expected started and completed callbacks");

    let started = &records[0];
    assert_eq!(started.path, "/api/v1/tasks/analytics.users/started");
    assert_eq!(
        started.authorization.as_deref(),
        Some("Bearer task-token-123")
    );
    assert_eq!(started.tenant_id.as_deref(), Some("tenant-dev"));
    assert_eq!(started.workspace_id.as_deref(), Some("workspace-dev"));
    assert_eq!(started.body["attempt"], 1);
    assert_eq!(started.body["attemptId"], "attempt_01");
    assert_eq!(started.body["workerId"], "cloud-test-worker");

    let completed = &records[1];
    assert_eq!(completed.path, "/api/v1/tasks/analytics.users/completed");
    assert_eq!(
        completed.authorization.as_deref(),
        Some("Bearer task-token-123")
    );
    assert_eq!(completed.tenant_id.as_deref(), Some("tenant-dev"));
    assert_eq!(completed.workspace_id.as_deref(), Some("workspace-dev"));
    assert_eq!(completed.body["attempt"], 1);
    assert_eq!(completed.body["attemptId"], "attempt_01");
    assert_eq!(completed.body["workerId"], "cloud-test-worker");
    assert_eq!(completed.body["outcome"], "SUCCEEDED");

    worker_server.abort();
    callback_server.abort();
    Ok(())
}

//! Minimal Arco cloud test worker runtime.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use reqwest::Client;

use arco_flow::orchestration::{
    TaskCompletedRequest, TaskOutput, TaskStartedRequest, WorkerDispatchEnvelope, WorkerOutcome,
};

#[derive(Clone)]
struct AppState {
    client: Client,
    worker_id: Arc<str>,
}

/// Builds the test worker Axum application.
#[must_use]
pub fn build_app(worker_id: String) -> Router {
    let state = AppState {
        client: Client::new(),
        worker_id: Arc::from(worker_id),
    };

    Router::new()
        .route("/health", get(health_handler))
        .route("/dispatch", post(dispatch_handler))
        .with_state(state)
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn dispatch_handler(
    State(state): State<AppState>,
    Json(envelope): Json<WorkerDispatchEnvelope>,
) -> StatusCode {
    tokio::spawn(async move {
        if let Err(error) = process_dispatch(state, envelope).await {
            tracing::error!(error = %error, "test worker dispatch failed");
        }
    });

    StatusCode::ACCEPTED
}

async fn process_dispatch(
    state: AppState,
    envelope: WorkerDispatchEnvelope,
) -> Result<(), reqwest::Error> {
    let started = TaskStartedRequest {
        attempt: envelope.attempt,
        attempt_id: envelope.attempt_id.clone(),
        worker_id: state.worker_id.to_string(),
        traceparent: envelope.traceparent.clone(),
        started_at: Some(Utc::now()),
    };
    post_callback(
        &state.client,
        callback_url(&envelope.callback_base_url, &envelope.task_key, "started"),
        &envelope.task_token,
        &envelope.tenant_id,
        &envelope.workspace_id,
        &started,
    )
    .await?;

    let completed = TaskCompletedRequest {
        attempt: envelope.attempt,
        attempt_id: envelope.attempt_id,
        worker_id: state.worker_id.to_string(),
        traceparent: envelope.traceparent,
        outcome: WorkerOutcome::Succeeded,
        completed_at: Some(Utc::now()),
        output: Some(TaskOutput {
            materialization_id: Some(format!("mat:{}", envelope.dispatch_id)),
            row_count: Some(1),
            byte_size: Some(128),
            output_path: Some(format!(
                "gs://arco-test/{}/{}/{}",
                envelope.tenant_id, envelope.workspace_id, envelope.task_key
            )),
            delta_table: envelope
                .payload
                .get("deltaTable")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            delta_version: envelope
                .payload
                .get("deltaVersion")
                .and_then(serde_json::Value::as_i64),
            delta_partition: envelope
                .payload
                .get("deltaPartition")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
        }),
        error: None,
        metrics: None,
        cancelled_during_phase: None,
        partial_progress: None,
    };
    post_callback(
        &state.client,
        callback_url(&envelope.callback_base_url, &envelope.task_key, "completed"),
        &envelope.task_token,
        &envelope.tenant_id,
        &envelope.workspace_id,
        &completed,
    )
    .await
}

fn callback_url(base_url: &str, task_key: &str, callback: &str) -> String {
    format!(
        "{}/api/v1/tasks/{task_key}/{callback}",
        base_url.trim_end_matches('/')
    )
}

async fn post_callback<T: serde::Serialize>(
    client: &Client,
    url: String,
    task_token: &str,
    tenant_id: &str,
    workspace_id: &str,
    payload: &T,
) -> Result<(), reqwest::Error> {
    client
        .post(url)
        .bearer_auth(task_token)
        .header("x-tenant-id", tenant_id)
        .header("x-workspace-id", workspace_id)
        .json(payload)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

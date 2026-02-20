//! Parity gate suite M1 (API): selection + run_key semantics.
//!
//! These tests are intended to be:
//! - hermetic (no external services)
//! - fast (integration-style)
//! - hard to “cheat” via doc-only changes

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use axum::body::Body;
use axum::http::{Method, StatusCode, header};
use axum::middleware;
use chrono::{Duration, Utc};
use serde::Deserialize;
use serde_json::Value;
use tower::ServiceExt;
use ulid::Ulid;

use arco_api::config::{Config, Posture};
use arco_api::context::{RequestContext, auth_middleware};
use arco_api::routes;
use arco_api::server::{AppState, ServerBuilder};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData, TaskOutcome};

fn test_router() -> axum::Router {
    ServerBuilder::new().debug(true).build().test_router()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TriggerRunResponse {
    run_id: String,
    plan_id: String,
    created: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RerunRunResponse {
    run_id: String,
    #[allow(dead_code)]
    plan_id: String,
    #[allow(dead_code)]
    state: String,
    created: bool,
    parent_run_id: String,
    rerun_kind: String,
    #[serde(default)]
    rerun_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeployManifestResponse {
    manifest_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskSummary {
    task_key: String,
    asset_key: Option<String>,
    state: String,
    #[allow(dead_code)]
    attempt: u32,
    #[serde(default)]
    execution_lineage_ref: Option<String>,
    #[serde(default)]
    retry_attribution: Option<TaskRetryAttribution>,
    #[serde(default)]
    skip_attribution: Option<TaskSkipAttribution>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskRetryAttribution {
    retries: u32,
    reason: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskSkipAttribution {
    upstream_task_key: String,
    upstream_resolution: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunResponse {
    run_id: String,
    state: String,
    tasks: Vec<TaskSummary>,
    #[serde(default)]
    labels: HashMap<String, String>,
    #[serde(default)]
    rerun_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiErrorResponse {
    message: String,
    code: String,
    #[serde(default)]
    details: Option<Value>,
}

mod helpers {
    use super::*;

    pub fn make_request(
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> Result<axum::http::Request<Body>> {
        let body = match body {
            Some(v) => Body::from(serde_json::to_vec(&v).context("serialize request body")?),
            None => Body::empty(),
        };

        axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header("X-Tenant-Id", "test-tenant")
            .header("X-Workspace-Id", "test-workspace")
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .context("build request")
    }

    pub async fn send(
        router: axum::Router,
        request: axum::http::Request<Body>,
    ) -> Result<axum::response::Response> {
        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        Ok(response)
    }

    pub async fn response_json<T: for<'de> Deserialize<'de>>(
        response: axum::response::Response,
    ) -> Result<(StatusCode, T)> {
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .context("read response body")?;

        let json = serde_json::from_slice(&body).with_context(|| {
            format!(
                "parse JSON response (status={status}): {}",
                String::from_utf8_lossy(&body)
            )
        })?;

        Ok((status, json))
    }
}

#[tokio::test]
async fn parity_m1_selection_does_not_autofill_tasks() -> Result<()> {
    let router = test_router();

    // Unsorted selection on purpose.
    let selection = vec!["analytics.orders", "analytics.users"];

    let (status, manifest): (_, DeployManifestResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/manifests",
                Some(serde_json::json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "abc123",
                    "assets": [
                        {
                            "key": { "namespace": "analytics", "name": "orders" },
                            "id": "01HQORDERS01",
                            "dependencies": []
                        },
                        {
                            "key": { "namespace": "analytics", "name": "users" },
                            "id": "01HQUSERS001",
                            "dependencies": []
                        }
                    ],
                    "schedules": []
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    let _ = manifest.manifest_id;

    let (status, created): (_, TriggerRunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/runs",
                Some(serde_json::json!({
                    "selection": selection,
                    "partitions": [],
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);

    let run_id = created.run_id;

    let (status, run): (_, RunResponse) = helpers::response_json(
        helpers::send(
            router,
            helpers::make_request(
                Method::GET,
                &format!("/api/v1/workspaces/test-workspace/runs/{run_id}"),
                None,
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(run.run_id, run_id);

    let mut actual_task_keys: Vec<String> = run.tasks.iter().map(|t| t.task_key.clone()).collect();
    actual_task_keys.sort();

    let mut expected_task_keys: Vec<String> = vec![
        "analytics.orders".to_string(),
        "analytics.users".to_string(),
    ];
    expected_task_keys.sort();

    assert_eq!(actual_task_keys, expected_task_keys);

    for task in &run.tasks {
        assert_eq!(task.asset_key.as_deref(), Some(task.task_key.as_str()));
    }

    Ok(())
}

#[tokio::test]
async fn parity_m1_deploy_rejects_invalid_manifest_asset_key() -> Result<()> {
    let router = test_router();

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/workspaces/test-workspace/manifests",
        Some(serde_json::json!({
            "manifestVersion": "1.0",
            "codeVersionId": "abc123",
            "assets": [
                {
                    "key": { "namespace": "analytics", "name": "users..daily" },
                    "id": "01HQBADKEY01",
                    "dependencies": []
                }
            ],
            "schedules": []
        })),
    )?;

    let response = helpers::send(router, request).await?;
    let (status, error): (_, ApiErrorResponse) = helpers::response_json(response).await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        error.message.contains("invalid asset key") || error.message.contains("asset key"),
        "unexpected error message: {}",
        error.message
    );

    Ok(())
}

#[tokio::test]
async fn parity_m1_run_key_is_not_reserved_on_bad_request() -> Result<()> {
    let router = test_router();

    let (status, manifest): (_, DeployManifestResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/manifests",
                Some(serde_json::json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "abc123",
                    "assets": [
                        {
                            "key": { "namespace": "analytics", "name": "orders" },
                            "id": "01HQORDERS01",
                            "dependencies": []
                        }
                    ],
                    "schedules": []
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    let _ = manifest.manifest_id;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/workspaces/test-workspace/runs",
        Some(serde_json::json!({
            "selection": ["analytics.unknown"],
            "partitions": [],
            "runKey": "rk-parity-poison-001",
            "labels": {},
        })),
    )?;

    let response = helpers::send(router.clone(), request).await?;
    let (status, _error): (_, ApiErrorResponse) = helpers::response_json(response).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, created): (_, TriggerRunResponse) = helpers::response_json(
        helpers::send(
            router,
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/runs",
                Some(serde_json::json!({
                    "selection": ["analytics.orders"],
                    "partitions": [],
                    "runKey": "rk-parity-poison-001",
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    assert!(created.created);

    Ok(())
}

#[tokio::test]
async fn parity_m1_selection_include_downstream_uses_manifest_graph() -> Result<()> {
    let router = test_router();

    let (status, manifest): (_, DeployManifestResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/manifests",
                Some(serde_json::json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "abc123",
                    "assets": [
                        {
                            "key": { "namespace": "analytics", "name": "a" },
                            "id": "01HQA000001",
                            "dependencies": []
                        },
                        {
                            "key": { "namespace": "analytics", "name": "b" },
                            "id": "01HQB000001",
                            "dependencies": [
                                {
                                    "upstreamKey": { "namespace": "analytics", "name": "a" }
                                }
                            ]
                        },
                        {
                            "key": { "namespace": "analytics", "name": "c" },
                            "id": "01HQC000001",
                            "dependencies": [
                                {
                                    "upstreamKey": { "namespace": "analytics", "name": "b" }
                                }
                            ]
                        }
                    ],
                    "schedules": []
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    let _ = manifest.manifest_id;

    let (status, created_root_only): (_, TriggerRunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/runs",
                Some(serde_json::json!({
                    "selection": ["analytics.a"],
                    "partitions": [],
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);

    let run_id = created_root_only.run_id;
    let (status, run): (_, RunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::GET,
                &format!("/api/v1/workspaces/test-workspace/runs/{run_id}"),
                None,
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::OK);

    let mut actual_task_keys: Vec<String> = run.tasks.iter().map(|t| t.task_key.clone()).collect();
    actual_task_keys.sort();
    assert_eq!(actual_task_keys, vec!["analytics.a".to_string()]);

    let (status, created_with_downstream): (_, TriggerRunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/runs",
                Some(serde_json::json!({
                    "selection": ["analytics.a"],
                    "includeDownstream": true,
                    "partitions": [],
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);

    let run_id = created_with_downstream.run_id;
    let (status, run): (_, RunResponse) = helpers::response_json(
        helpers::send(
            router,
            helpers::make_request(
                Method::GET,
                &format!("/api/v1/workspaces/test-workspace/runs/{run_id}"),
                None,
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::OK);

    let mut actual_task_keys: Vec<String> = run.tasks.iter().map(|t| t.task_key.clone()).collect();
    actual_task_keys.sort();

    let mut expected_task_keys = vec![
        "analytics.a".to_string(),
        "analytics.b".to_string(),
        "analytics.c".to_string(),
    ];
    expected_task_keys.sort();

    assert_eq!(actual_task_keys, expected_task_keys);

    Ok(())
}

#[tokio::test]
async fn parity_m1_run_key_idempotency_is_order_insensitive_for_selection() -> Result<()> {
    let router = test_router();

    let (status, manifest): (_, DeployManifestResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/manifests",
                Some(serde_json::json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "abc123",
                    "assets": [
                        {
                            "key": { "namespace": "analytics", "name": "orders" },
                            "id": "01HQORDERS01",
                            "dependencies": []
                        },
                        {
                            "key": { "namespace": "analytics", "name": "users" },
                            "id": "01HQUSERS001",
                            "dependencies": []
                        }
                    ],
                    "schedules": []
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    let _ = manifest.manifest_id;

    let (status, first): (_, TriggerRunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/runs",
                Some(serde_json::json!({
                    "selection": ["analytics.users", "analytics.orders"],
                    "partitions": [],
                    "runKey": "rk-parity-m1-001",
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    assert!(first.created);
    assert!(!first.run_id.is_empty());
    assert!(!first.plan_id.is_empty());

    // Reverse selection order; run_key should still map to the same run.
    let (status, second): (_, TriggerRunResponse) = helpers::response_json(
        helpers::send(
            router,
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/runs",
                Some(serde_json::json!({
                    "selection": ["analytics.orders", "analytics.users"],
                    "partitions": [],
                    "runKey": "rk-parity-m1-001",
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert!(!second.created);
    assert_eq!(second.run_id, first.run_id);
    assert_eq!(second.plan_id, first.plan_id);

    Ok(())
}

#[tokio::test]
async fn parity_m1_run_key_conflicts_on_payload_mismatch() -> Result<()> {
    let router = test_router();

    let (status, manifest): (_, DeployManifestResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/manifests",
                Some(serde_json::json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "abc123",
                    "assets": [
                        {
                            "key": { "namespace": "analytics", "name": "orders" },
                            "id": "01HQORDERS01",
                            "dependencies": []
                        },
                        {
                            "key": { "namespace": "analytics", "name": "users" },
                            "id": "01HQUSERS001",
                            "dependencies": []
                        }
                    ],
                    "schedules": []
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    let _ = manifest.manifest_id;

    let (status, first): (_, TriggerRunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/runs",
                Some(serde_json::json!({
                    "selection": ["analytics.users"],
                    "partitions": [],
                    "runKey": "rk-parity-m1-002",
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    assert!(first.created);

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/workspaces/test-workspace/runs",
        Some(serde_json::json!({
            "selection": ["analytics.orders"],
            "partitions": [],
            "runKey": "rk-parity-m1-002",
            "labels": {},
        })),
    )?;

    let response = helpers::send(router, request).await?;
    let (status, error): (_, ApiErrorResponse) = helpers::response_json(response).await?;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error.code, "CONFLICT");
    assert!(
        error.message.contains("run_key"),
        "expected conflict error to mention run_key"
    );
    let details = error
        .details
        .as_ref()
        .expect("run_key conflicts should expose diagnostics payload");
    assert_eq!(
        details.get("conflictType").and_then(Value::as_str),
        Some("RUN_KEY_FINGERPRINT_MISMATCH")
    );
    assert_eq!(
        details.get("runKey").and_then(Value::as_str),
        Some("rk-parity-m1-002")
    );
    assert!(
        details
            .get("existingFingerprint")
            .and_then(Value::as_str)
            .is_some(),
        "existing fingerprint should be included for operators"
    );
    assert!(
        details
            .get("requestedFingerprint")
            .and_then(Value::as_str)
            .is_some(),
        "requested fingerprint should be included for operators"
    );

    Ok(())
}

#[tokio::test]
async fn parity_m1_rejects_invalid_asset_keys() -> Result<()> {
    let router = test_router();

    let (status, manifest): (_, DeployManifestResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/manifests",
                Some(serde_json::json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "abc123",
                    "assets": [
                        {
                            "key": { "namespace": "analytics", "name": "orders" },
                            "id": "01HQORDERS01",
                            "dependencies": []
                        }
                    ],
                    "schedules": []
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    let _ = manifest.manifest_id;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/workspaces/test-workspace/runs",
        Some(serde_json::json!({
            "selection": ["analytics/"],
            "partitions": [],
            "labels": {},
        })),
    )?;

    let response = helpers::send(router, request).await?;
    let (status, error): (_, ApiErrorResponse) = helpers::response_json(response).await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        error.message.contains("invalid asset key") || error.message.contains("asset key"),
        "unexpected error message: {}",
        error.message
    );

    Ok(())
}

#[tokio::test]
async fn parity_m1_rejects_unknown_assets() -> Result<()> {
    let router = test_router();

    let (status, manifest): (_, DeployManifestResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/manifests",
                Some(serde_json::json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "abc123",
                    "assets": [
                        {
                            "key": { "namespace": "analytics", "name": "orders" },
                            "id": "01HQORDERS01",
                            "dependencies": []
                        }
                    ],
                    "schedules": []
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    let _ = manifest.manifest_id;

    let request = helpers::make_request(
        Method::POST,
        "/api/v1/workspaces/test-workspace/runs",
        Some(serde_json::json!({
            "selection": ["analytics.unknown"],
            "partitions": [],
            "labels": {},
        })),
    )?;

    let response = helpers::send(router, request).await?;
    let (status, error): (_, ApiErrorResponse) = helpers::response_json(response).await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        error.message.contains("unknown asset keys"),
        "unexpected error message: {}",
        error.message
    );

    Ok(())
}

fn test_state_and_router() -> Result<(Arc<AppState>, axum::Router)> {
    let mut config = Config::default();
    config.debug = true;
    config.posture = Posture::Dev;

    let state = Arc::new(AppState::with_memory_storage(config));
    let auth_layer = middleware::from_fn_with_state(state.clone(), auth_middleware);

    let router = axum::Router::new()
        .nest("/api/v1", routes::api_v1_routes().layer(auth_layer))
        .with_state(state.clone());

    Ok((state, router))
}

fn test_ctx() -> RequestContext {
    RequestContext {
        tenant: "test-tenant".to_string(),
        workspace: "test-workspace".to_string(),
        user_id: Some("user@example.com".to_string()),
        groups: vec![],
        request_id: "req_test".to_string(),
        idempotency_key: None,
    }
}

async fn append_events_and_compact(
    state: &Arc<AppState>,
    ctx: &RequestContext,
    events: Vec<OrchestrationEvent>,
) -> Result<()> {
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let ledger = LedgerWriter::new(storage.clone());

    let mut event_paths = Vec::with_capacity(events.len());
    for event in events {
        let path = LedgerWriter::event_path(&event);
        ledger.append(event).await.map_err(|e| anyhow!("{e}"))?;
        event_paths.push(path);
    }

    let compactor = MicroCompactor::new(storage);
    compactor
        .compact_events(event_paths)
        .await
        .map_err(|e| anyhow!("{e}"))?;

    Ok(())
}

async fn load_fold_state(
    state: &Arc<AppState>,
    ctx: &RequestContext,
) -> Result<arco_flow::orchestration::compactor::FoldState> {
    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;
    let compactor = MicroCompactor::new(storage);
    let (_manifest, fold_state) = compactor.load_state().await.map_err(|e| anyhow!("{e}"))?;
    Ok(fold_state)
}

#[tokio::test]
async fn parity_m1_rerun_from_failure_plans_only_unsucceeded_tasks() -> Result<()> {
    let (state, router) = test_state_and_router()?;

    let (status, _manifest): (_, DeployManifestResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/manifests",
                Some(serde_json::json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "abc123",
                    "assets": [
                        {
                            "key": { "namespace": "analytics", "name": "a" },
                            "id": "01HQA000001",
                            "dependencies": []
                        },
                        {
                            "key": { "namespace": "analytics", "name": "b" },
                            "id": "01HQB000001",
                            "dependencies": [
                                {
                                    "upstreamKey": { "namespace": "analytics", "name": "a" }
                                }
                            ]
                        },
                        {
                            "key": { "namespace": "analytics", "name": "c" },
                            "id": "01HQC000001",
                            "dependencies": [
                                {
                                    "upstreamKey": { "namespace": "analytics", "name": "b" }
                                }
                            ]
                        }
                    ],
                    "schedules": []
                })),
            )?,
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, created): (_, TriggerRunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/runs",
                Some(serde_json::json!({
                    "selection": ["analytics.a"],
                    "includeDownstream": true,
                    "partitions": [],
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let parent_run_id = created.run_id;

    let ctx = test_ctx();
    let base = Utc::now();

    let a_attempt_id = Ulid::new().to_string();
    let b_attempt_id = Ulid::new().to_string();

    append_events_and_compact(
        &state,
        &ctx,
        vec![
            {
                let mut event = OrchestrationEvent::new(
                    &ctx.tenant,
                    &ctx.workspace,
                    OrchestrationEventData::TaskStarted {
                        run_id: parent_run_id.clone(),
                        task_key: "analytics.a".to_string(),
                        attempt: 1,
                        attempt_id: a_attempt_id.clone(),
                        worker_id: "worker-1".to_string(),
                    },
                );
                event.timestamp = base;
                event
            },
            {
                let mut event = OrchestrationEvent::new(
                    &ctx.tenant,
                    &ctx.workspace,
                    OrchestrationEventData::TaskFinished {
                        run_id: parent_run_id.clone(),
                        task_key: "analytics.a".to_string(),
                        attempt: 1,
                        attempt_id: a_attempt_id.clone(),
                        worker_id: "worker-1".to_string(),
                        outcome: TaskOutcome::Succeeded,
                        materialization_id: None,
                        error_message: None,
                        output: None,
                        error: None,
                        metrics: None,
                        cancelled_during_phase: None,
                        partial_progress: None,
                        asset_key: Some("analytics.a".to_string()),
                        partition_key: None,
                        code_version: None,
                    },
                );
                event.timestamp = base + Duration::milliseconds(1);
                event
            },
            {
                let mut event = OrchestrationEvent::new(
                    &ctx.tenant,
                    &ctx.workspace,
                    OrchestrationEventData::TaskStarted {
                        run_id: parent_run_id.clone(),
                        task_key: "analytics.b".to_string(),
                        attempt: 3,
                        attempt_id: b_attempt_id.clone(),
                        worker_id: "worker-1".to_string(),
                    },
                );
                event.timestamp = base + Duration::milliseconds(2);
                event
            },
            {
                let mut event = OrchestrationEvent::new(
                    &ctx.tenant,
                    &ctx.workspace,
                    OrchestrationEventData::TaskFinished {
                        run_id: parent_run_id.clone(),
                        task_key: "analytics.b".to_string(),
                        attempt: 3,
                        attempt_id: b_attempt_id.clone(),
                        worker_id: "worker-1".to_string(),
                        outcome: TaskOutcome::Failed,
                        materialization_id: None,
                        error_message: Some("boom".to_string()),
                        output: None,
                        error: None,
                        metrics: None,
                        cancelled_during_phase: None,
                        partial_progress: None,
                        asset_key: Some("analytics.b".to_string()),
                        partition_key: None,
                        code_version: None,
                    },
                );
                event.timestamp = base + Duration::milliseconds(3);
                event
            },
        ],
    )
    .await?;

    let (status, parent): (_, RunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::GET,
                &format!("/api/v1/workspaces/test-workspace/runs/{parent_run_id}"),
                None,
            )?,
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(parent.state, "FAILED");

    let mut parent_states: HashMap<String, String> = parent
        .tasks
        .iter()
        .map(|t| (t.task_key.clone(), t.state.clone()))
        .collect();
    assert_eq!(
        parent_states.remove("analytics.a").as_deref(),
        Some("SUCCEEDED")
    );
    assert_eq!(
        parent_states.remove("analytics.b").as_deref(),
        Some("FAILED")
    );
    assert_eq!(
        parent_states.remove("analytics.c").as_deref(),
        Some("SKIPPED")
    );

    let failed_task = parent
        .tasks
        .iter()
        .find(|task| task.task_key == "analytics.b")
        .expect("expected failed task in parent run");
    let retry_attribution = failed_task
        .retry_attribution
        .as_ref()
        .expect("failed task with attempt=3 should carry retry attribution");
    assert_eq!(retry_attribution.retries, 2);
    assert_eq!(retry_attribution.reason, "prior_attempt_failed");

    let skipped_task = parent
        .tasks
        .iter()
        .find(|task| task.task_key == "analytics.c")
        .expect("expected skipped task in parent run");
    let skip_attribution = skipped_task
        .skip_attribution
        .as_ref()
        .expect("skipped task should carry upstream attribution");
    assert_eq!(skip_attribution.upstream_task_key, "analytics.b");
    assert_eq!(skip_attribution.upstream_resolution, "FAILED");
    assert!(
        skipped_task.execution_lineage_ref.is_none(),
        "skipped tasks should not expose execution lineage refs"
    );

    let (status, rerun): (_, RerunRunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                &format!("/api/v1/workspaces/test-workspace/runs/{parent_run_id}/rerun"),
                Some(serde_json::json!({
                    "mode": "fromFailure",
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(rerun.parent_run_id, parent_run_id);
    assert_eq!(rerun.rerun_kind, "FROM_FAILURE");
    assert_eq!(
        rerun.rerun_reason.as_deref(),
        Some("from_failure_unsucceeded_tasks")
    );

    let rerun_run_id = rerun.run_id;

    let (status, rerun_run): (_, RunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::GET,
                &format!("/api/v1/workspaces/test-workspace/runs/{rerun_run_id}"),
                None,
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        rerun_run
            .labels
            .get("arco.parent_run_id")
            .map(String::as_str),
        Some(parent_run_id.as_str())
    );
    assert_eq!(
        rerun_run.labels.get("arco.rerun.kind").map(String::as_str),
        Some("FROM_FAILURE")
    );
    assert_eq!(
        rerun_run.rerun_reason.as_deref(),
        Some("from_failure_unsucceeded_tasks")
    );

    let mut task_keys: Vec<String> = rerun_run.tasks.iter().map(|t| t.task_key.clone()).collect();
    task_keys.sort();
    assert_eq!(
        task_keys,
        vec!["analytics.b".to_string(), "analytics.c".to_string()]
    );

    let fold_state = load_fold_state(&state, &ctx).await?;
    let rerun_tasks: Vec<_> = fold_state
        .tasks
        .values()
        .filter(|row| row.run_id == rerun_run_id)
        .collect();
    assert_eq!(rerun_tasks.len(), 2);

    let rerun_edges: Vec<_> = fold_state
        .dep_satisfaction
        .values()
        .filter(|row| row.run_id == rerun_run_id)
        .collect();

    assert!(rerun_edges.iter().any(|row| {
        row.upstream_task_key == "analytics.b" && row.downstream_task_key == "analytics.c"
    }));
    assert!(!rerun_edges.iter().any(|row| {
        row.upstream_task_key == "analytics.a" || row.downstream_task_key == "analytics.a"
    }));

    let (status, rerun_again): (_, RerunRunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                &format!("/api/v1/workspaces/test-workspace/runs/{parent_run_id}/rerun"),
                Some(serde_json::json!({
                    "mode": "fromFailure",
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert!(!rerun_again.created);
    assert_eq!(rerun_again.run_id, rerun_run_id);

    Ok(())
}

#[tokio::test]
async fn parity_m1_rerun_subset_respects_include_downstream() -> Result<()> {
    let (_state, router) = test_state_and_router()?;

    let (status, _manifest): (_, DeployManifestResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/manifests",
                Some(serde_json::json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "abc123",
                    "assets": [
                        {
                            "key": { "namespace": "analytics", "name": "a" },
                            "id": "01HQA000001",
                            "dependencies": []
                        },
                        {
                            "key": { "namespace": "analytics", "name": "b" },
                            "id": "01HQB000001",
                            "dependencies": [
                                {
                                    "upstreamKey": { "namespace": "analytics", "name": "a" }
                                }
                            ]
                        },
                        {
                            "key": { "namespace": "analytics", "name": "c" },
                            "id": "01HQC000001",
                            "dependencies": [
                                {
                                    "upstreamKey": { "namespace": "analytics", "name": "b" }
                                }
                            ]
                        }
                    ],
                    "schedules": []
                })),
            )?,
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, created): (_, TriggerRunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/runs",
                Some(serde_json::json!({
                    "selection": ["analytics.a"],
                    "includeDownstream": true,
                    "partitions": [],
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    let parent_run_id = created.run_id;

    let (status, rerun_downstream): (_, RerunRunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                &format!("/api/v1/workspaces/test-workspace/runs/{parent_run_id}/rerun"),
                Some(serde_json::json!({
                    "mode": "subset",
                    "selection": ["analytics.b"],
                    "includeDownstream": true,
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(rerun_downstream.rerun_kind, "SUBSET");

    let (status, rerun_run): (_, RunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::GET,
                &format!(
                    "/api/v1/workspaces/test-workspace/runs/{}",
                    rerun_downstream.run_id
                ),
                None,
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    let mut keys: Vec<String> = rerun_run.tasks.iter().map(|t| t.task_key.clone()).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec!["analytics.b".to_string(), "analytics.c".to_string()]
    );

    let (status, rerun_root_only): (_, RerunRunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                &format!("/api/v1/workspaces/test-workspace/runs/{parent_run_id}/rerun"),
                Some(serde_json::json!({
                    "mode": "subset",
                    "selection": ["analytics.b"],
                    "includeDownstream": false,
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);

    let (status, rerun_run): (_, RunResponse) = helpers::response_json(
        helpers::send(
            router,
            helpers::make_request(
                Method::GET,
                &format!(
                    "/api/v1/workspaces/test-workspace/runs/{}",
                    rerun_root_only.run_id
                ),
                None,
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    let mut keys: Vec<String> = rerun_run.tasks.iter().map(|t| t.task_key.clone()).collect();
    keys.sort();
    assert_eq!(keys, vec!["analytics.b".to_string()]);

    Ok(())
}

#[tokio::test]
async fn parity_m1_rerun_from_failure_rejects_succeeded_parent() -> Result<()> {
    let (state, router) = test_state_and_router()?;

    let (status, _manifest): (_, DeployManifestResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/manifests",
                Some(serde_json::json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "abc123",
                    "assets": [
                        {
                            "key": { "namespace": "analytics", "name": "a" },
                            "id": "01HQA000001",
                            "dependencies": []
                        }
                    ],
                    "schedules": []
                })),
            )?,
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, created): (_, TriggerRunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/runs",
                Some(serde_json::json!({
                    "selection": ["analytics.a"],
                    "partitions": [],
                    "labels": {},
                })),
            )?,
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let parent_run_id = created.run_id;

    let ctx = test_ctx();
    let base = Utc::now();
    let attempt_id = Ulid::new().to_string();

    append_events_and_compact(
        &state,
        &ctx,
        vec![
            {
                let mut event = OrchestrationEvent::new(
                    &ctx.tenant,
                    &ctx.workspace,
                    OrchestrationEventData::TaskStarted {
                        run_id: parent_run_id.clone(),
                        task_key: "analytics.a".to_string(),
                        attempt: 1,
                        attempt_id: attempt_id.clone(),
                        worker_id: "worker-1".to_string(),
                    },
                );
                event.timestamp = base;
                event
            },
            {
                let mut event = OrchestrationEvent::new(
                    &ctx.tenant,
                    &ctx.workspace,
                    OrchestrationEventData::TaskFinished {
                        run_id: parent_run_id.clone(),
                        task_key: "analytics.a".to_string(),
                        attempt: 1,
                        attempt_id: attempt_id.clone(),
                        worker_id: "worker-1".to_string(),
                        outcome: TaskOutcome::Succeeded,
                        materialization_id: None,
                        error_message: None,
                        output: None,
                        error: None,
                        metrics: None,
                        cancelled_during_phase: None,
                        partial_progress: None,
                        asset_key: Some("analytics.a".to_string()),
                        partition_key: None,
                        code_version: None,
                    },
                );
                event.timestamp = base + Duration::milliseconds(1);
                event
            },
        ],
    )
    .await?;

    let (status, parent): (_, RunResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::GET,
                &format!("/api/v1/workspaces/test-workspace/runs/{parent_run_id}"),
                None,
            )?,
        )
        .await?,
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(parent.state, "SUCCEEDED");

    let response = helpers::send(
        router,
        helpers::make_request(
            Method::POST,
            &format!("/api/v1/workspaces/test-workspace/runs/{parent_run_id}/rerun"),
            Some(serde_json::json!({
                "mode": "fromFailure",
                "labels": {},
            })),
        )?,
    )
    .await?;

    let (status, _error): (_, ApiErrorResponse) = helpers::response_json(response).await?;
    assert_eq!(status, StatusCode::CONFLICT);

    Ok(())
}

#[tokio::test]
async fn parity_m1_trigger_rejects_reserved_lineage_labels() -> Result<()> {
    let router = test_router();

    let (status, _manifest): (_, DeployManifestResponse) = helpers::response_json(
        helpers::send(
            router.clone(),
            helpers::make_request(
                Method::POST,
                "/api/v1/workspaces/test-workspace/manifests",
                Some(serde_json::json!({
                    "manifestVersion": "1.0",
                    "codeVersionId": "abc123",
                    "assets": [
                        {
                            "key": { "namespace": "analytics", "name": "a" },
                            "id": "01HQA000001",
                            "dependencies": []
                        }
                    ],
                    "schedules": []
                })),
            )?,
        )
        .await?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let response = helpers::send(
        router,
        helpers::make_request(
            Method::POST,
            "/api/v1/workspaces/test-workspace/runs",
            Some(serde_json::json!({
                "selection": ["analytics.a"],
                "partitions": [],
                "labels": {
                    "arco.parent_run_id": "not-a-real-run",
                    "arco.rerun.kind": "SUBSET"
                }
            })),
        )?,
    )
    .await?;

    let (status, err): (_, ApiErrorResponse) = helpers::response_json(response).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(err.message.contains("arco.parent_run_id"));

    Ok(())
}

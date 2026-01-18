//! Parity gate suite M1 (API): selection + run_key semantics.
//!
//! These tests are intended to be:
//! - hermetic (no external services)
//! - fast (integration-style)
//! - hard to “cheat” via doc-only changes

use anyhow::{Context, Result};
use axum::body::Body;
use axum::http::{Method, StatusCode, header};
use serde::Deserialize;
use tower::ServiceExt;

use arco_api::server::ServerBuilder;

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
struct DeployManifestResponse {
    manifest_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskSummary {
    task_key: String,
    asset_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunResponse {
    run_id: String,
    tasks: Vec<TaskSummary>,
}

#[derive(Debug, Deserialize)]
struct ApiErrorResponse {
    message: String,
    code: String,
}

mod helpers {
    use super::*;

    pub fn make_request(
        method: Method,
        uri: &str,
        body: Option<serde_json::Value>,
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

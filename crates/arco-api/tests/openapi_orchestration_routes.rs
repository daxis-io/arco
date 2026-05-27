//! OpenAPI coverage for orchestration read endpoints.

use anyhow::{Result, ensure};

#[test]
fn orchestration_read_endpoints_are_documented() -> Result<()> {
    let json = arco_api::openapi::openapi_json()?;
    let spec: serde_json::Value = serde_json::from_str(&json)?;

    let paths = spec
        .get("paths")
        .and_then(|value| value.as_object())
        .ok_or_else(|| anyhow::anyhow!("missing paths in OpenAPI"))?;

    for path in [
        "/api/v1/workspaces/{workspace_id}/schedules",
        "/api/v1/workspaces/{workspace_id}/schedules/{schedule_id}",
        "/api/v1/workspaces/{workspace_id}/schedules/{schedule_id}/ticks",
        "/api/v1/workspaces/{workspace_id}/sensors",
        "/api/v1/workspaces/{workspace_id}/sensors/{sensor_id}",
        "/api/v1/workspaces/{workspace_id}/sensors/{sensor_id}/evals",
        "/api/v1/workspaces/{workspace_id}/backfills",
        "/api/v1/workspaces/{workspace_id}/backfills/{backfill_id}",
        "/api/v1/workspaces/{workspace_id}/backfills/{backfill_id}/chunks",
        "/api/v1/workspaces/{workspace_id}/partitions",
        "/api/v1/workspaces/{workspace_id}/assets/{asset_key}/partitions/summary",
    ] {
        ensure!(paths.contains_key(path), "missing OpenAPI path: {path}");
    }

    Ok(())
}

#[test]
fn list_history_endpoints_do_not_document_404() -> Result<()> {
    let json = arco_api::openapi::openapi_json()?;
    let spec: serde_json::Value = serde_json::from_str(&json)?;

    let ticks = &spec["paths"]["/api/v1/workspaces/{workspace_id}/schedules/{schedule_id}/ticks"]["get"]
        ["responses"];
    ensure!(
        ticks.get("404").is_none(),
        "unexpected 404 in ticks responses"
    );

    let evals = &spec["paths"]["/api/v1/workspaces/{workspace_id}/sensors/{sensor_id}/evals"]["get"]
        ["responses"];
    ensure!(
        evals.get("404").is_none(),
        "unexpected 404 in evals responses"
    );

    Ok(())
}

#[test]
fn worker_callback_endpoints_use_contract_schemas() -> Result<()> {
    let json = arco_api::openapi::openapi_json()?;
    let spec: serde_json::Value = serde_json::from_str(&json)?;

    let paths = spec
        .get("paths")
        .and_then(|value| value.as_object())
        .ok_or_else(|| anyhow::anyhow!("missing paths in OpenAPI"))?;

    for path in [
        "/api/v1/tasks/{task_id}/started",
        "/api/v1/tasks/{task_id}/heartbeat",
        "/api/v1/tasks/{task_id}/completed",
    ] {
        ensure!(paths.contains_key(path), "missing OpenAPI path: {path}");
    }

    let schemas = &spec["components"]["schemas"];
    ensure!(
        schemas["TaskStartedRequest"]["properties"]
            .as_object()
            .is_some_and(|properties| properties.contains_key("workerId")),
        "TaskStartedRequest should expose contract camelCase fields"
    );
    ensure!(
        schemas["HeartbeatRequest"]["properties"]
            .as_object()
            .is_some_and(|properties| properties.contains_key("progressPct")),
        "HeartbeatRequest should expose contract camelCase fields"
    );
    ensure!(
        schemas["HeartbeatRequest"]["properties"]["progressPct"]["maximum"] == 100,
        "HeartbeatRequest progressPct should document the 0-100 contract"
    );
    ensure!(
        schemas["HeartbeatRequest"]["properties"]["attempt"]["minimum"] == 1,
        "HeartbeatRequest attempt should document one-indexed attempts"
    );
    ensure!(
        schemas["TaskCompletedRequest"]["properties"]
            .as_object()
            .is_some_and(|properties| properties.contains_key("outcome")),
        "TaskCompletedRequest should expose contract outcome"
    );
    ensure!(
        schemas["TaskCompletedRequest"]["properties"]["attempt"]["minimum"] == 1,
        "TaskCompletedRequest attempt should document one-indexed attempts"
    );
    ensure!(
        schemas["CallbackErrorResponse"]["properties"]
            .as_object()
            .is_some_and(|properties| properties.contains_key("expectedAttemptId")),
        "CallbackErrorResponse should expose contract camelCase fields"
    );

    Ok(())
}

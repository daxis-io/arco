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

#[test]
fn catalog_list_endpoints_document_pagination_contract() -> Result<()> {
    let json = arco_api::openapi::openapi_json()?;
    let spec: serde_json::Value = serde_json::from_str(&json)?;
    let paths = spec
        .get("paths")
        .and_then(|value| value.as_object())
        .ok_or_else(|| anyhow::anyhow!("missing paths in OpenAPI"))?;

    for path in [
        "/api/v1/catalogs",
        "/api/v1/catalogs/{catalog}/schemas",
        "/api/v1/catalogs/{catalog}/schemas/{schema}/tables",
        "/api/v1/namespaces",
        "/api/v1/namespaces/{namespace}/tables",
    ] {
        let params = paths[path]["get"]["parameters"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("missing GET parameters for {path}"))?;
        ensure!(
            params.iter().any(|param| param["name"] == "limit"),
            "{path} must document limit"
        );
        let limit = params
            .iter()
            .find(|param| param["name"] == "limit")
            .ok_or_else(|| anyhow::anyhow!("missing limit parameter for {path}"))?;
        ensure!(
            limit["schema"]["minimum"] == 1,
            "{path} limit minimum must match runtime validation"
        );
        ensure!(
            limit["schema"]["maximum"] == 500,
            "{path} limit maximum must match runtime validation"
        );
        ensure!(
            params.iter().any(|param| param["name"] == "cursor"),
            "{path} must document cursor"
        );
    }

    let schemas = &spec["components"]["schemas"];
    for schema in [
        "ListCatalogsResponse",
        "ListSchemasResponse",
        "ListSchemaTablesResponse",
        "ListNamespacesResponse",
        "ListTablesResponse",
    ] {
        ensure!(
            schemas[schema]["properties"]
                .as_object()
                .is_some_and(|properties| properties.contains_key("next_cursor")),
            "{schema} must expose next_cursor"
        );
        ensure!(
            schemas[schema]["properties"]
                .as_object()
                .is_some_and(|properties| !properties.contains_key("nextCursor")),
            "{schema} must preserve existing catalog snake_case response family"
        );
    }

    Ok(())
}

#[test]
fn api_v1_json_casing_policy_is_documented_and_pinned() -> Result<()> {
    let docs = include_str!("../../../docs/guide/src/reference/api.md");
    ensure!(
        docs.contains(
            "Existing `/api/v1` catalog, namespace, table, and Delta route families keep their snake_case JSON fields"
        ),
        "API docs must document existing snake_case compatibility"
    );
    ensure!(
        docs.contains("New native `/api/v1` route families use camelCase JSON fields"),
        "API docs must document the forward casing rule"
    );

    let json = arco_api::openapi::openapi_json()?;
    let spec: serde_json::Value = serde_json::from_str(&json)?;
    let schemas = &spec["components"]["schemas"];

    ensure!(
        schemas["TableResponse"]["properties"]
            .as_object()
            .is_some_and(|properties| properties.contains_key("created_at")),
        "existing table responses must preserve snake_case created_at"
    );
    ensure!(
        schemas["TriggerRunResponse"]["properties"]
            .as_object()
            .is_some_and(|properties| properties.contains_key("runId")),
        "orchestration responses must preserve camelCase runId"
    );
    ensure!(
        schemas["TriggerRunResponse"]["properties"]
            .as_object()
            .is_some_and(|properties| !properties.contains_key("run_id")),
        "orchestration responses must not document snake_case run_id"
    );

    Ok(())
}

#[test]
fn orchestration_write_routes_document_idempotency_key_contract() -> Result<()> {
    let docs = include_str!("../../../docs/guide/src/reference/api.md");
    ensure!(
        docs.contains("## Idempotency Policy"),
        "API docs must document route-family idempotency behavior"
    );
    ensure!(
        docs.contains("Trigger-run accepts `runKey` first and falls back to `Idempotency-Key`"),
        "API docs must document trigger-run idempotency precedence"
    );
    ensure!(
        docs.contains(
            "Backfill creation accepts `clientRequestId` first and falls back to `Idempotency-Key`"
        ),
        "API docs must document backfill idempotency precedence"
    );
    ensure!(
        docs.contains("Manifest deployment uses `Idempotency-Key` when present"),
        "API docs must document manifest idempotency behavior"
    );
    ensure!(
        docs.contains("Schedule mutations use `Idempotency-Key` as the event idempotency discriminator when present"),
        "API docs must document schedule idempotency behavior"
    );

    let json = arco_api::openapi::openapi_json()?;
    let spec: serde_json::Value = serde_json::from_str(&json)?;

    assert_header_param(
        &spec,
        "/api/v1/workspaces/{workspace_id}/runs",
        "post",
        "trigger-run",
    )?;
    assert_header_param(
        &spec,
        "/api/v1/workspaces/{workspace_id}/backfills",
        "post",
        "create-backfill",
    )?;
    assert_header_param(
        &spec,
        "/api/v1/workspaces/{workspace_id}/manifests",
        "post",
        "deploy-manifest",
    )?;
    assert_header_param(
        &spec,
        "/api/v1/workspaces/{workspace_id}/schedules/{schedule_id}",
        "put",
        "upsert-schedule",
    )?;
    assert_header_param(
        &spec,
        "/api/v1/workspaces/{workspace_id}/schedules/{schedule_id}/enable",
        "post",
        "enable-schedule",
    )?;
    assert_header_param(
        &spec,
        "/api/v1/workspaces/{workspace_id}/schedules/{schedule_id}/disable",
        "post",
        "disable-schedule",
    )?;

    Ok(())
}

fn assert_header_param(
    spec: &serde_json::Value,
    path: &str,
    method: &str,
    label: &str,
) -> Result<()> {
    let params = spec["paths"][path][method]["parameters"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("missing {label} parameters"))?;

    ensure!(
        params
            .iter()
            .any(|param| param["name"] == "Idempotency-Key" && param["in"] == "header"),
        "{label} must document Idempotency-Key header support"
    );

    Ok(())
}

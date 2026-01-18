//! Schedule E2E tests from manifest deploy through tick history.

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::body::Body;
use axum::http::{Method, StatusCode, header};
use serde::de::DeserializeOwned;
use tower::ServiceExt;

use arco_api::server::ServerBuilder;
use arco_core::ScopedStorage;
use arco_core::storage::{MemoryBackend, StorageBackend};
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::controllers::ScheduleController;
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData, TickStatus};
use arco_flow::orchestration::ledger::LedgerWriter;
use chrono::{TimeZone, Utc};

fn test_router(backend: Arc<MemoryBackend>) -> axum::Router {
    ServerBuilder::new()
        .debug(true)
        .storage_backend(backend)
        .build()
        .test_router()
}

mod helpers {
    use super::*;

    pub fn make_request_with_headers(
        method: Method,
        uri: &str,
        body: Option<serde_json::Value>,
        headers: &[(&str, &str)],
    ) -> Result<axum::http::Request<Body>> {
        let mut builder = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header("X-Tenant-Id", "test-tenant")
            .header("X-Workspace-Id", "test-workspace")
            .header(header::CONTENT_TYPE, "application/json");

        for (key, value) in headers {
            builder = builder.header(*key, *value);
        }

        let body = match body {
            Some(v) => Body::from(serde_json::to_vec(&v).context("serialize request body")?),
            None => Body::empty(),
        };

        builder.body(body).context("build request")
    }

    async fn send(
        router: axum::Router,
        request: axum::http::Request<Body>,
    ) -> Result<axum::response::Response> {
        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        Ok(response)
    }

    async fn response_body(
        response: axum::response::Response,
    ) -> Result<(StatusCode, axum::body::Bytes)> {
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 128 * 1024)
            .await
            .context("read response body")?;
        Ok((status, body))
    }

    pub async fn post_json_with_headers<T: DeserializeOwned>(
        router: axum::Router,
        uri: &str,
        body: serde_json::Value,
        headers: &[(&str, &str)],
    ) -> Result<(StatusCode, T)> {
        let request = make_request_with_headers(Method::POST, uri, Some(body), headers)?;
        let response = send(router, request).await?;
        let (status, body) = response_body(response).await?;
        let json = serde_json::from_slice(&body).with_context(|| {
            format!(
                "parse JSON response (status={status}): {}",
                String::from_utf8_lossy(&body)
            )
        })?;
        Ok((status, json))
    }

    pub async fn put_json_with_headers<T: DeserializeOwned>(
        router: axum::Router,
        uri: &str,
        body: serde_json::Value,
        headers: &[(&str, &str)],
    ) -> Result<(StatusCode, T)> {
        let request = make_request_with_headers(Method::PUT, uri, Some(body), headers)?;
        let response = send(router, request).await?;
        let (status, body) = response_body(response).await?;
        let json = serde_json::from_slice(&body).with_context(|| {
            format!(
                "parse JSON response (status={status}): {}",
                String::from_utf8_lossy(&body)
            )
        })?;
        Ok((status, json))
    }

    pub async fn get_json<T: DeserializeOwned>(
        router: axum::Router,
        uri: &str,
    ) -> Result<(StatusCode, T)> {
        let request = make_request_with_headers(Method::GET, uri, None, &[])?;
        let response = send(router, request).await?;
        let (status, body) = response_body(response).await?;
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
async fn schedule_definitions_flow_from_manifest_to_tick_history() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let router = test_router(backend.clone());

    let manifest = serde_json::json!({
        "manifestVersion": "1.0",
        "codeVersionId": "abc123",
        "assets": [{
            "key": {"namespace": "analytics", "name": "users"},
            "id": "01HQXYZ123",
            "description": "User analytics asset"
        }],
        "schedules": [{
            "id": "daily-users",
            "cron": "0 0 * * *",
            "assets": ["analytics/users"],
            "timezone": "UTC"
        }]
    });

    let (status, _deploy): (_, serde_json::Value) = helpers::post_json_with_headers(
        router.clone(),
        "/api/v1/workspaces/test-workspace/manifests",
        manifest,
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let storage = ScopedStorage::new(backend.clone(), "test-tenant", "test-workspace")?;
    let ledger = LedgerWriter::new(storage.clone());
    let compactor = MicroCompactor::new(storage.clone());

    let (_, fold_state) = compactor.load_state().await?;
    let definition = fold_state
        .schedule_definitions
        .get("daily-users")
        .context("expected schedule definition")?
        .clone();
    assert_eq!(definition.cron_expression, "0 0 * * *");
    assert_eq!(definition.timezone, "UTC");
    assert_eq!(
        definition.asset_selection,
        vec!["analytics/users".to_string()]
    );

    let now = Utc.with_ymd_and_hms(2025, 1, 15, 0, 0, 0).unwrap();
    let controller = ScheduleController::new();
    let events = controller.reconcile(&[definition], &[], now);
    assert!(
        events
            .iter()
            .any(|e| matches!(e.data, OrchestrationEventData::ScheduleTicked { .. })),
        "expected ScheduleTicked event"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e.data, OrchestrationEventData::RunRequested { .. })),
        "expected RunRequested event"
    );

    let event_paths: Vec<String> = events.iter().map(LedgerWriter::event_path).collect();
    ledger.append_all(events).await?;
    compactor.compact_events(event_paths).await?;

    let expected_tick_id = format!("daily-users:{}", now.timestamp());

    let (status, schedules): (_, serde_json::Value) = helpers::get_json(
        router.clone(),
        "/api/v1/workspaces/test-workspace/schedules",
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    let schedules = schedules
        .get("schedules")
        .and_then(|v| v.as_array())
        .context("expected schedules array")?;
    assert!(
        schedules
            .iter()
            .any(|s| s.get("scheduleId")
                == Some(&serde_json::Value::String("daily-users".to_string()))),
        "expected schedule to appear in schedule_state"
    );

    let ticks_path = "/api/v1/workspaces/test-workspace/schedules/daily-users/ticks";
    let (status, ticks): (_, serde_json::Value) = helpers::get_json(router, ticks_path).await?;
    assert_eq!(status, StatusCode::OK);

    let ticks = ticks
        .get("ticks")
        .and_then(|v| v.as_array())
        .context("expected ticks array")?;
    let tick = ticks
        .iter()
        .find(|t| t.get("tickId") == Some(&serde_json::Value::String(expected_tick_id.clone())))
        .context("expected tick row")?;
    assert_eq!(
        tick.get("tickId"),
        Some(&serde_json::Value::String(expected_tick_id))
    );
    assert!(tick.get("runId").is_some());

    Ok(())
}

#[tokio::test]
async fn deploy_manifest_idempotency_does_not_duplicate_schedule_events() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let router = test_router(backend.clone());

    let manifest = serde_json::json!({
        "manifestVersion": "1.0",
        "codeVersionId": "abc123",
        "assets": [{
            "key": {"namespace": "analytics", "name": "users"},
            "id": "01HQXYZ123",
            "description": "User analytics asset"
        }],
        "schedules": [{
            "id": "daily-users",
            "cron": "0 0 * * *",
            "assets": ["analytics/users"],
            "timezone": "UTC"
        }]
    });

    let prefix = "tenant=test-tenant/workspace=test-workspace/ledger/orchestration/";
    let before = backend.list(prefix).await?;

    let (status, _deploy): (_, serde_json::Value) = helpers::post_json_with_headers(
        router.clone(),
        "/api/v1/workspaces/test-workspace/manifests",
        manifest.clone(),
        &[("Idempotency-Key", "idem-arco-schedules-001")],
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let after_first = backend.list(prefix).await?;
    assert!(
        after_first.len() > before.len(),
        "expected schedule events to be appended"
    );

    let (status, _deploy2): (_, serde_json::Value) = helpers::post_json_with_headers(
        router,
        "/api/v1/workspaces/test-workspace/manifests",
        manifest,
        &[("Idempotency-Key", "idem-arco-schedules-001")],
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let after_second = backend.list(prefix).await?;
    assert_eq!(
        after_second.len(),
        after_first.len(),
        "expected idempotent manifest deploy to avoid schedule event bloat"
    );

    Ok(())
}

#[tokio::test]
async fn manifest_redeploy_does_not_override_operator_disabled_schedules() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let router = test_router(backend.clone());

    let manifest = serde_json::json!({
        "manifestVersion": "1.0",
        "codeVersionId": "abc123",
        "assets": [{
            "key": {"namespace": "analytics", "name": "users"},
            "id": "01HQXYZ123",
            "description": "User analytics asset"
        }],
        "schedules": [{
            "id": "daily-users",
            "cron": "0 0 * * *",
            "assets": ["analytics/users"],
            "timezone": "UTC"
        }]
    });

    let (status, _deploy): (_, serde_json::Value) = helpers::post_json_with_headers(
        router.clone(),
        "/api/v1/workspaces/test-workspace/manifests",
        manifest.clone(),
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, disabled): (_, serde_json::Value) = helpers::post_json_with_headers(
        router.clone(),
        "/api/v1/workspaces/test-workspace/schedules/daily-users/disable",
        serde_json::json!({}),
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        disabled.get("enabled"),
        Some(&serde_json::Value::Bool(false))
    );

    let (status, _deploy2): (_, serde_json::Value) = helpers::post_json_with_headers(
        router.clone(),
        "/api/v1/workspaces/test-workspace/manifests",
        manifest,
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, schedule): (_, serde_json::Value) = helpers::get_json(
        router,
        "/api/v1/workspaces/test-workspace/schedules/daily-users",
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        schedule.get("enabled"),
        Some(&serde_json::Value::Bool(false))
    );

    Ok(())
}

#[tokio::test]
async fn schedule_crud_enable_disable_roundtrips_and_appears_in_list() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let router = test_router(backend.clone());

    let schedule_id = "daily-users";

    let create_body = serde_json::json!({
        "cronExpression": "0 0 * * *",
        "timezone": "UTC",
        "catchupWindowMinutes": 60,
        "maxCatchupTicks": 3,
        "assetSelection": ["analytics/users"],
        "enabled": true
    });

    let (status, created): (_, serde_json::Value) = helpers::put_json_with_headers(
        router.clone(),
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}"),
        create_body.clone(),
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        created.get("scheduleId"),
        Some(&serde_json::Value::String(schedule_id.to_string()))
    );
    assert_eq!(created.get("enabled"), Some(&serde_json::Value::Bool(true)));

    let created_version = created
        .get("definitionVersion")
        .and_then(|v| v.as_str())
        .context("expected definitionVersion")?
        .to_string();

    let (status, created_again): (_, serde_json::Value) = helpers::put_json_with_headers(
        router.clone(),
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}"),
        create_body,
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        created_again.get("definitionVersion"),
        Some(&serde_json::Value::String(created_version.clone()))
    );

    let (status, schedule): (_, serde_json::Value) = helpers::get_json(
        router.clone(),
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        schedule.get("cronExpression"),
        Some(&serde_json::Value::String("0 0 * * *".to_string()))
    );

    let (status, disabled): (_, serde_json::Value) = helpers::post_json_with_headers(
        router.clone(),
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}/disable"),
        serde_json::json!({}),
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        disabled.get("enabled"),
        Some(&serde_json::Value::Bool(false))
    );

    let disabled_version = disabled
        .get("definitionVersion")
        .and_then(|v| v.as_str())
        .context("expected definitionVersion")?
        .to_string();
    assert_ne!(disabled_version, created_version);

    let (status, disabled_again): (_, serde_json::Value) = helpers::post_json_with_headers(
        router.clone(),
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}/disable"),
        serde_json::json!({}),
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        disabled_again.get("definitionVersion"),
        Some(&serde_json::Value::String(disabled_version.clone()))
    );

    let (status, enabled): (_, serde_json::Value) = helpers::post_json_with_headers(
        router.clone(),
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}/enable"),
        serde_json::json!({}),
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(enabled.get("enabled"), Some(&serde_json::Value::Bool(true)));

    let enabled_version = enabled
        .get("definitionVersion")
        .and_then(|v| v.as_str())
        .context("expected definitionVersion")?
        .to_string();
    assert_ne!(enabled_version, disabled_version);

    let (status, enabled_again): (_, serde_json::Value) = helpers::post_json_with_headers(
        router.clone(),
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}/enable"),
        serde_json::json!({}),
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        enabled_again.get("definitionVersion"),
        Some(&serde_json::Value::String(enabled_version.clone()))
    );

    let update_body = serde_json::json!({
        "cronExpression": "0 1 * * *",
        "timezone": "UTC",
        "catchupWindowMinutes": 60,
        "maxCatchupTicks": 3,
        "assetSelection": ["analytics/users"],
        "enabled": true
    });

    let (status, updated): (_, serde_json::Value) = helpers::put_json_with_headers(
        router.clone(),
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}"),
        update_body,
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        updated.get("cronExpression"),
        Some(&serde_json::Value::String("0 1 * * *".to_string()))
    );

    let updated_version = updated
        .get("definitionVersion")
        .and_then(|v| v.as_str())
        .context("expected definitionVersion")?
        .to_string();
    assert_ne!(updated_version, enabled_version);

    let (status, schedules): (_, serde_json::Value) =
        helpers::get_json(router, "/api/v1/workspaces/test-workspace/schedules").await?;
    assert_eq!(status, StatusCode::OK);

    let schedules = schedules
        .get("schedules")
        .and_then(|v| v.as_array())
        .context("expected schedules array")?;

    assert!(
        schedules.iter().any(|s| {
            s.get("scheduleId") == Some(&serde_json::Value::String(schedule_id.to_string()))
                && s.get("cronExpression")
                    == Some(&serde_json::Value::String("0 1 * * *".to_string()))
        }),
        "expected updated schedule to appear in list"
    );

    Ok(())
}

#[tokio::test]
async fn schedule_tick_detail_exposes_run_key_and_run_id_linkage() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let router = test_router(backend.clone());

    let schedule_id = "daily-users";

    let create_body = serde_json::json!({
        "cronExpression": "0 0 * * *",
        "timezone": "UTC",
        "catchupWindowMinutes": 60,
        "maxCatchupTicks": 3,
        "assetSelection": ["analytics/users"],
        "enabled": true
    });

    let (status, _created): (_, serde_json::Value) = helpers::put_json_with_headers(
        router.clone(),
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}"),
        create_body,
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let storage = ScopedStorage::new(backend.clone(), "test-tenant", "test-workspace")?;
    let ledger = LedgerWriter::new(storage.clone());
    let compactor = MicroCompactor::new(storage.clone());

    let (_, fold_state) = compactor.load_state().await?;
    let definition = fold_state
        .schedule_definitions
        .get(schedule_id)
        .context("expected schedule definition")?
        .clone();

    let now = Utc.with_ymd_and_hms(2025, 1, 15, 0, 0, 0).unwrap();
    let controller = ScheduleController::new();
    let events = controller.reconcile(&[definition], &[], now);

    let event_paths: Vec<String> = events.iter().map(LedgerWriter::event_path).collect();
    ledger.append_all(events).await?;
    compactor.compact_events(event_paths).await?;

    let expected_tick_id = format!("{schedule_id}:{}", now.timestamp());

    let tick_detail_path = format!(
        "/api/v1/workspaces/test-workspace/schedules/{schedule_id}/ticks/{expected_tick_id}"
    );
    let (status, tick): (_, serde_json::Value) =
        helpers::get_json(router.clone(), &tick_detail_path).await?;
    assert_eq!(status, StatusCode::OK);

    assert_eq!(
        tick.get("tickId"),
        Some(&serde_json::Value::String(expected_tick_id.clone()))
    );
    assert_eq!(
        tick.get("scheduleId"),
        Some(&serde_json::Value::String(schedule_id.to_string()))
    );
    assert!(tick.get("runKey").is_some());
    assert!(tick.get("runId").is_some());

    let ticks_path = format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}/ticks");
    let (status, ticks): (_, serde_json::Value) = helpers::get_json(router, &ticks_path).await?;
    assert_eq!(status, StatusCode::OK);

    let ticks = ticks
        .get("ticks")
        .and_then(|v| v.as_array())
        .context("expected ticks array")?;
    assert!(
        ticks
            .iter()
            .any(|t| t.get("tickId").and_then(|v| v.as_str()) == Some(expected_tick_id.as_str())),
        "expected tick to appear in history listing"
    );

    Ok(())
}

#[tokio::test]
async fn schedule_tick_detail_surfaces_skip_reason_and_error_message() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let router = test_router(backend.clone());

    let schedule_id = "daily-users";

    let create_body = serde_json::json!({
        "cronExpression": "0 0 * * *",
        "timezone": "UTC",
        "catchupWindowMinutes": 60,
        "maxCatchupTicks": 3,
        "assetSelection": ["analytics/users"],
        "enabled": false
    });

    let (status, _created): (_, serde_json::Value) = helpers::put_json_with_headers(
        router.clone(),
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}"),
        create_body,
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let storage = ScopedStorage::new(backend.clone(), "test-tenant", "test-workspace")?;
    let ledger = LedgerWriter::new(storage.clone());
    let compactor = MicroCompactor::new(storage.clone());

    let (_, fold_state) = compactor.load_state().await?;
    let definition = fold_state
        .schedule_definitions
        .get(schedule_id)
        .context("expected schedule definition")?
        .clone();

    let now = Utc.with_ymd_and_hms(2025, 1, 15, 0, 0, 0).unwrap();
    let controller = ScheduleController::new();
    let events = controller.reconcile(&[definition], &[], now);

    let event_paths: Vec<String> = events.iter().map(LedgerWriter::event_path).collect();
    ledger.append_all(events).await?;
    compactor.compact_events(event_paths).await?;

    let skipped_tick_id = format!("{schedule_id}:{}", now.timestamp());

    let (status, skipped_tick): (_, serde_json::Value) = helpers::get_json(
        router.clone(),
        &format!(
            "/api/v1/workspaces/test-workspace/schedules/{schedule_id}/ticks/{skipped_tick_id}"
        ),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        skipped_tick.get("status"),
        Some(&serde_json::Value::String("SKIPPED".to_string()))
    );
    assert_eq!(
        skipped_tick.get("skipReason"),
        Some(&serde_json::Value::String("paused".to_string()))
    );
    assert!(skipped_tick.get("errorMessage").is_none());

    let failed_scheduled_for = Utc.with_ymd_and_hms(2025, 1, 15, 0, 1, 0).unwrap();
    let failed_tick_id = format!("{schedule_id}:{}", failed_scheduled_for.timestamp());

    let mut failed_tick_event = OrchestrationEvent::new_with_timestamp(
        "test-tenant",
        "test-workspace",
        OrchestrationEventData::ScheduleTicked {
            schedule_id: schedule_id.to_string(),
            scheduled_for: failed_scheduled_for,
            tick_id: failed_tick_id.clone(),
            definition_version: "def_v1".to_string(),
            asset_selection: vec!["analytics/users".to_string()],
            partition_selection: None,
            status: TickStatus::Failed {
                error: "boom".to_string(),
            },
            run_key: None,
            request_fingerprint: None,
        },
        failed_scheduled_for,
    );
    failed_tick_event.event_id = "evt_failed_tick_01".to_string();

    let failed_path = LedgerWriter::event_path(&failed_tick_event);
    ledger.append(failed_tick_event).await?;
    compactor.compact_events(vec![failed_path]).await?;

    let (status, failed_tick): (_, serde_json::Value) = helpers::get_json(
        router,
        &format!(
            "/api/v1/workspaces/test-workspace/schedules/{schedule_id}/ticks/{failed_tick_id}"
        ),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        failed_tick.get("status"),
        Some(&serde_json::Value::String("FAILED".to_string()))
    );
    assert_eq!(
        failed_tick.get("errorMessage"),
        Some(&serde_json::Value::String("boom".to_string()))
    );
    assert!(failed_tick.get("skipReason").is_none());

    Ok(())
}

#[tokio::test]
async fn list_schedule_ticks_missing_schedule_returns_empty_list() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let router = test_router(backend);

    let (status, body): (_, serde_json::Value) = helpers::get_json(
        router,
        "/api/v1/workspaces/test-workspace/schedules/does-not-exist/ticks",
    )
    .await?;

    assert_eq!(status, StatusCode::OK);

    let ticks = body
        .get("ticks")
        .and_then(|v| v.as_array())
        .context("expected ticks array")?;
    assert!(ticks.is_empty());
    assert!(
        body.get("nextCursor").is_none()
            || body.get("nextCursor") == Some(&serde_json::Value::Null)
    );

    Ok(())
}

#[tokio::test]
async fn schedule_upsert_validates_cron_timezone_and_assets() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let router = test_router(backend);

    let schedule_id = "daily-users";

    let body = serde_json::json!({
        "cronExpression": "0 0 * * *",
        "timezone": "UTC",
        "catchupWindowMinutes": 60,
        "maxCatchupTicks": 3,
        "assetSelection": [],
        "enabled": true
    });

    let (status, _resp): (_, serde_json::Value) = helpers::put_json_with_headers(
        router.clone(),
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}"),
        body,
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let body = serde_json::json!({
        "cronExpression": "0 0 * *",
        "timezone": "UTC",
        "catchupWindowMinutes": 60,
        "maxCatchupTicks": 3,
        "assetSelection": ["analytics/users"],
        "enabled": true
    });

    let (status, _resp): (_, serde_json::Value) = helpers::put_json_with_headers(
        router.clone(),
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}"),
        body,
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let body = serde_json::json!({
        "cronExpression": "0 0 * * *",
        "timezone": "Not/AZone",
        "catchupWindowMinutes": 60,
        "maxCatchupTicks": 3,
        "assetSelection": ["analytics/users"],
        "enabled": true
    });

    let (status, _resp): (_, serde_json::Value) = helpers::put_json_with_headers(
        router,
        &format!("/api/v1/workspaces/test-workspace/schedules/{schedule_id}"),
        body,
        &[],
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    Ok(())
}

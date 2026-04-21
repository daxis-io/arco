#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use bytes::Bytes;
use chrono::Utc;
use tower::ServiceExt;
use ulid::Ulid;

use arco_api::server::ServerBuilder;
use arco_core::ScopedStorage;
use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::{
    MicroCompactor, OrchestrationManifest, OrchestrationManifestPointer,
};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskDef, TaskOutcome, TriggerInfo,
};
use arco_flow::orchestration_manifest_pointer_path;

pub fn test_router() -> axum::Router {
    ServerBuilder::new().debug(true).build().test_router()
}

pub fn test_router_with_backend() -> (axum::Router, Arc<MemoryBackend>) {
    let backend = Arc::new(MemoryBackend::new());
    let router = ServerBuilder::new()
        .debug(true)
        .storage_backend(backend.clone())
        .build()
        .test_router();
    (router, backend)
}

pub async fn seed_catalog(router: axum::Router) -> Result<axum::Router> {
    let (status, _): (_, serde_json::Value) = helpers::post_json(
        router.clone(),
        "/api/v1/namespaces",
        serde_json::json!({
            "name": "analytics",
            "description": "Analytics namespace"
        }),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _): (_, serde_json::Value) = helpers::post_json(
        router.clone(),
        "/api/v1/namespaces/analytics/tables",
        serde_json::json!({
            "name": "events",
            "description": "Event stream",
            "columns": [
                {"name": "event_id", "data_type": "STRING", "nullable": false},
                {"name": "event_type", "data_type": "STRING", "nullable": false}
            ]
        }),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _): (_, serde_json::Value) = helpers::post_json(
        router.clone(),
        "/api/v1/lineage/edges",
        serde_json::json!({
            "edges": [
                {
                    "source_id": "table-source-1",
                    "target_id": "table-target-1",
                    "edge_type": "derives_from",
                    "run_id": "run-123"
                }
            ]
        }),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    Ok(router)
}

pub async fn seed_orchestration_router() -> Result<axum::Router> {
    seed_orchestration_router_with_base_snapshot(true).await
}

pub async fn seed_orchestration_router_with_l0_only() -> Result<axum::Router> {
    seed_orchestration_router_with_base_snapshot(false).await
}

async fn seed_orchestration_router_with_base_snapshot(
    force_base_snapshot: bool,
) -> Result<axum::Router> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let router = ServerBuilder::new()
        .debug(true)
        .storage_backend(backend.clone())
        .build()
        .test_router();

    let storage = ScopedStorage::new(backend, "test-tenant", "test-workspace")?;
    seed_orchestration_storage(&storage, force_base_snapshot).await?;

    Ok(router)
}

pub async fn seed_orchestration_storage(
    storage: &ScopedStorage,
    force_base_snapshot: bool,
) -> Result<()> {
    let manifest_id = "00000000000000000000";
    let manifest_path = format!("state/orchestration/manifests/{manifest_id}.json");

    let mut manifest = OrchestrationManifest::new(Ulid::new().to_string());
    if force_base_snapshot {
        manifest.l0_limits.max_count = 1;
    }

    storage
        .put_raw(
            &manifest_path,
            Bytes::from(
                serde_json::to_vec(&manifest).context("serialize seeded orchestration manifest")?,
            ),
            WritePrecondition::DoesNotExist,
        )
        .await?;

    let pointer = OrchestrationManifestPointer {
        manifest_id: manifest.manifest_id.clone(),
        manifest_path,
        epoch: 0,
        parent_pointer_hash: None,
        updated_at: Utc::now(),
    };
    storage
        .put_raw(
            orchestration_manifest_pointer_path(),
            Bytes::from(
                serde_json::to_vec(&pointer)
                    .context("serialize seeded orchestration manifest pointer")?,
            ),
            WritePrecondition::DoesNotExist,
        )
        .await?;

    let run_id = "run_01";
    let plan_id = "plan_01";
    let task_key = "extract";
    let attempt_id = "attempt_01";

    let events = vec![
        OrchestrationEvent::new(
            "test-tenant",
            "test-workspace",
            OrchestrationEventData::RunTriggered {
                run_id: run_id.to_string(),
                plan_id: plan_id.to_string(),
                trigger: TriggerInfo::Manual {
                    user_id: "tester@example.com".to_string(),
                },
                root_assets: vec!["analytics.daily".to_string()],
                run_key: None,
                labels: HashMap::new(),
                code_version: None,
            },
        ),
        OrchestrationEvent::new(
            "test-tenant",
            "test-workspace",
            OrchestrationEventData::PlanCreated {
                run_id: run_id.to_string(),
                plan_id: plan_id.to_string(),
                tasks: vec![TaskDef {
                    key: task_key.to_string(),
                    depends_on: vec![],
                    asset_key: Some("analytics.daily".to_string()),
                    partition_key: Some("2025-01-15".to_string()),
                    max_attempts: 1,
                    heartbeat_timeout_sec: 300,
                }],
            },
        ),
        OrchestrationEvent::new(
            "test-tenant",
            "test-workspace",
            OrchestrationEventData::TaskStarted {
                run_id: run_id.to_string(),
                task_key: task_key.to_string(),
                attempt: 1,
                attempt_id: attempt_id.to_string(),
                worker_id: "worker-1".to_string(),
            },
        ),
        OrchestrationEvent::new(
            "test-tenant",
            "test-workspace",
            OrchestrationEventData::TaskFinished {
                run_id: run_id.to_string(),
                task_key: task_key.to_string(),
                attempt: 1,
                attempt_id: attempt_id.to_string(),
                worker_id: "worker-1".to_string(),
                outcome: TaskOutcome::Succeeded,
                materialization_id: Some("mat_01".to_string()),
                error_message: None,
                output: None,
                error: None,
                metrics: None,
                cancelled_during_phase: None,
                partial_progress_json: None,
                asset_key: Some("analytics.daily".to_string()),
                partition_key: Some("2025-01-15".to_string()),
                code_version: None,
            },
        ),
    ];

    let ledger = LedgerWriter::new(storage.clone());
    let paths: Vec<_> = events.iter().map(LedgerWriter::event_path).collect();
    ledger.append_all(events).await?;

    MicroCompactor::new(storage.clone())
        .compact_events(paths)
        .await?;
    Ok(())
}

pub mod helpers {
    use super::*;
    use serde::de::DeserializeOwned;

    pub fn make_request(
        method: Method,
        uri: &str,
        body: Option<serde_json::Value>,
    ) -> Result<Request<Body>> {
        make_request_with_headers(method, uri, body, &[])
    }

    pub fn make_request_with_headers(
        method: Method,
        uri: &str,
        body: Option<serde_json::Value>,
        headers: &[(&str, &str)],
    ) -> Result<Request<Body>> {
        let mut builder = Request::builder()
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
        request: Request<Body>,
    ) -> Result<axum::response::Response> {
        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        Ok(response)
    }

    async fn response_body(response: axum::response::Response) -> Result<(StatusCode, Bytes)> {
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .context("read response body")?;
        Ok((status, body))
    }

    pub async fn get_json<T: DeserializeOwned>(
        router: axum::Router,
        uri: &str,
    ) -> Result<(StatusCode, T)> {
        let request = make_request(Method::GET, uri, None)?;
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

    pub async fn post_json<T: DeserializeOwned>(
        router: axum::Router,
        uri: &str,
        body: serde_json::Value,
    ) -> Result<(StatusCode, T)> {
        let request = make_request(Method::POST, uri, Some(body))?;
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

//! API integration tests.
//!
//! Tests the complete request flow: HTTP → routes → catalog → storage.

use anyhow::{Context, Result};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use tower::ServiceExt;

use arco_api::config::{Config, CorsConfig};
use arco_api::server::{Server, ServerBuilder};

const TEST_JWT_SECRET: &str = "test-jwt-secret";

fn test_router() -> axum::Router {
    ServerBuilder::new().debug(true).build().test_router()
}

fn test_router_prod() -> axum::Router {
    let config = Config {
        debug: false,
        jwt: arco_api::config::JwtConfig {
            hs256_secret: Some(TEST_JWT_SECRET.to_string()),
            ..arco_api::config::JwtConfig::default()
        },
        ..Config::default()
    };

    Server::new(config).test_router()
}

fn test_router_with_cors(allowed_origins: Vec<String>) -> axum::Router {
    let config = Config {
        debug: true,
        cors: CorsConfig {
            allowed_origins,
            max_age_seconds: 3600,
        },
        ..Config::default()
    };

    Server::new(config).test_router()
}

#[tokio::test]
async fn test_server_uses_provided_storage_backend() -> Result<()> {
    use std::sync::Arc;

    use arco_core::storage::{MemoryBackend, StorageBackend};

    let backend = Arc::new(MemoryBackend::new());

    let objects = backend.list("").await?;
    assert!(
        objects.is_empty(),
        "expected empty storage backend before requests"
    );

    let router = ServerBuilder::new()
        .debug(true)
        .storage_backend(backend.clone())
        .build()
        .test_router();

    let (status, _): (_, serde_json::Value) = helpers::post_json(
        router,
        "/api/v1/namespaces",
        serde_json::json!({
            "name": "analytics",
            "description": "Analytics namespace"
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::CREATED);

    let objects = backend
        .list("tenant=test-tenant/workspace=test-workspace/")
        .await?;
    assert!(
        !objects.is_empty(),
        "expected writes to go to the provided backend"
    );

    Ok(())
}

mod helpers {
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

    async fn response_body(
        response: axum::response::Response,
    ) -> Result<(StatusCode, axum::body::Bytes)> {
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

    pub async fn get_text(router: axum::Router, uri: &str) -> Result<(StatusCode, String)> {
        let request = make_request(Method::GET, uri, None)?;
        let response = send(router, request).await?;
        let (status, body) = response_body(response).await?;
        let text = String::from_utf8(body.to_vec()).context("parse text response")?;
        Ok((status, text))
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

    pub async fn put_json<T: DeserializeOwned>(
        router: axum::Router,
        uri: &str,
        body: serde_json::Value,
    ) -> Result<(StatusCode, T)> {
        let request = make_request(Method::PUT, uri, Some(body))?;
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

    pub async fn delete(router: axum::Router, uri: &str) -> Result<StatusCode> {
        let request = make_request(Method::DELETE, uri, None)?;
        let response = send(router, request).await?;
        Ok(response.status())
    }
}

// ============================================================================
// Namespace Tests
// ============================================================================

mod namespaces {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct NamespaceResponse {
        id: String,
        name: String,
        description: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct ListNamespacesResponse {
        namespaces: Vec<NamespaceResponse>,
    }

    #[tokio::test]
    async fn test_namespace_crud_lifecycle() -> Result<()> {
        let router = test_router();

        // Create namespace
        let (status, ns): (_, NamespaceResponse) = helpers::post_json(
            router.clone(),
            "/api/v1/namespaces",
            serde_json::json!({
                "name": "analytics",
                "description": "Analytics namespace"
            }),
        )
        .await?;

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(ns.name, "analytics");
        assert_eq!(ns.description.as_deref(), Some("Analytics namespace"));
        assert!(!ns.id.is_empty());

        // List namespaces
        let (status, list): (_, ListNamespacesResponse) =
            helpers::get_json(router.clone(), "/api/v1/namespaces").await?;
        assert_eq!(status, StatusCode::OK);
        assert!(list.namespaces.iter().any(|n| n.name == "analytics"));

        // Get namespace by name
        let (status, ns_get): (_, NamespaceResponse) =
            helpers::get_json(router.clone(), "/api/v1/namespaces/analytics").await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ns_get.id, ns.id);

        // Delete namespace
        let status = helpers::delete(router.clone(), "/api/v1/namespaces/analytics").await?;
        assert_eq!(status, StatusCode::NO_CONTENT);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_nonexistent_namespace_returns_404() -> Result<()> {
        let router = test_router();

        let request = helpers::make_request(Method::GET, "/api/v1/namespaces/nonexistent", None)?;
        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        Ok(())
    }
}

// ============================================================================
// Table Tests
// ============================================================================

mod tables {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct ColumnResponse {
        name: String,
        data_type: String,
        nullable: bool,
        description: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct TableResponse {
        id: String,
        namespace: String,
        name: String,
        description: Option<String>,
        columns: Vec<ColumnResponse>,
    }

    #[derive(Debug, Deserialize)]
    struct ListTablesResponse {
        tables: Vec<TableResponse>,
    }

    #[tokio::test]
    async fn test_table_crud_lifecycle() -> Result<()> {
        let router = test_router();

        // First create a namespace
        let (status, _): (_, serde_json::Value) = helpers::post_json(
            router.clone(),
            "/api/v1/namespaces",
            serde_json::json!({
                "name": "sales",
                "description": "Sales data"
            }),
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);

        // Register a table
        let (status, table): (_, TableResponse) = helpers::post_json(
            router.clone(),
            "/api/v1/namespaces/sales/tables",
            serde_json::json!({
                "name": "orders",
                "description": "Customer orders",
                "columns": [
                    {"name": "order_id", "data_type": "STRING", "nullable": false},
                    {"name": "customer_id", "data_type": "STRING", "nullable": false},
                    {"name": "total", "data_type": "DECIMAL", "nullable": true, "description": "Order total"}
                ]
            }),
        )
        .await?;

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(table.name, "orders");
        assert_eq!(table.namespace, "sales");
        assert_eq!(table.columns.len(), 3);
        assert!(!table.id.is_empty());

        // Verify column details
        let order_id_col = table
            .columns
            .iter()
            .find(|c| c.name == "order_id")
            .context("order_id column missing")?;
        assert!(!order_id_col.nullable);
        assert_eq!(order_id_col.data_type, "STRING");

        let total_col = table
            .columns
            .iter()
            .find(|c| c.name == "total")
            .context("total column missing")?;
        assert!(total_col.nullable);
        assert_eq!(total_col.description.as_deref(), Some("Order total"));

        // List tables in namespace
        let (status, list): (_, ListTablesResponse) =
            helpers::get_json(router.clone(), "/api/v1/namespaces/sales/tables").await?;
        assert_eq!(status, StatusCode::OK);
        assert!(list.tables.iter().any(|t| t.name == "orders"));

        // Get table by name
        let (status, table_get): (_, TableResponse) =
            helpers::get_json(router.clone(), "/api/v1/namespaces/sales/tables/orders").await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(table_get.id, table.id);

        // Update table description
        let (status, updated): (_, TableResponse) = helpers::put_json(
            router.clone(),
            "/api/v1/namespaces/sales/tables/orders",
            serde_json::json!({
                "description": "Updated: Customer order records"
            }),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            updated.description.as_deref(),
            Some("Updated: Customer order records")
        );

        // Drop table
        let status =
            helpers::delete(router.clone(), "/api/v1/namespaces/sales/tables/orders").await?;
        assert_eq!(status, StatusCode::NO_CONTENT);

        Ok(())
    }

    #[tokio::test]
    async fn test_register_table_in_nonexistent_namespace_returns_error() -> Result<()> {
        let router = test_router();

        let request = helpers::make_request(
            Method::POST,
            "/api/v1/namespaces/nonexistent/tables",
            Some(serde_json::json!({
                "name": "test_table",
                "columns": [{"name": "id", "data_type": "STRING"}]
            })),
        )?;
        let response = router.oneshot(request).await.map_err(|err| match err {})?;

        assert!(response.status().is_client_error() || response.status().is_server_error());
        Ok(())
    }
}

// ============================================================================
// Lineage Tests
// ============================================================================

mod lineage {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct EdgeResponse {
        target_id: String,
        edge_type: String,
    }

    #[derive(Debug, Deserialize)]
    struct AddEdgesResponse {
        added: usize,
    }

    #[derive(Debug, Deserialize)]
    struct LineageResponse {
        table_id: String,
        upstream: Vec<EdgeResponse>,
        downstream: Vec<EdgeResponse>,
    }

    #[tokio::test]
    async fn test_lineage_edge_lifecycle() -> Result<()> {
        let router = test_router();

        // Add lineage edges
        let (status, result): (_, AddEdgesResponse) = helpers::post_json(
            router.clone(),
            "/api/v1/lineage/edges",
            serde_json::json!({
                "edges": [
                    {
                        "source_id": "table-source-1",
                        "target_id": "table-target-1",
                        "edge_type": "derives_from",
                        "run_id": "run-123"
                    },
                    {
                        "source_id": "table-source-2",
                        "target_id": "table-target-1",
                        "edge_type": "transforms"
                    }
                ]
            }),
        )
        .await?;

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(result.added, 2);

        // Get lineage for target table (should show upstream)
        let (status, lineage): (_, LineageResponse) =
            helpers::get_json(router.clone(), "/api/v1/lineage/table-target-1").await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(lineage.table_id, "table-target-1");
        assert_eq!(lineage.upstream.len(), 2);

        // Get lineage for source table (should show downstream)
        let (status, lineage): (_, LineageResponse) =
            helpers::get_json(router.clone(), "/api/v1/lineage/table-source-1").await?;
        assert_eq!(status, StatusCode::OK);
        let first = lineage
            .downstream
            .first()
            .context("expected at least one downstream edge")?;
        assert_eq!(first.target_id, "table-target-1");

        Ok(())
    }

    #[tokio::test]
    async fn test_lineage_edge_with_default_type() -> Result<()> {
        let router = test_router();

        // Add edge without specifying type (should default to "derives_from")
        let (status, _result): (_, AddEdgesResponse) = helpers::post_json(
            router.clone(),
            "/api/v1/lineage/edges",
            serde_json::json!({
                "edges": [
                    {
                        "source_id": "src",
                        "target_id": "tgt"
                    }
                ]
            }),
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);

        let (_status, lineage): (_, LineageResponse) =
            helpers::get_json(router.clone(), "/api/v1/lineage/tgt").await?;
        let first = lineage
            .upstream
            .first()
            .context("expected at least one upstream edge")?;
        assert_eq!(first.edge_type, "derives_from");
        Ok(())
    }
}

// ============================================================================
// Browser URL Minting Tests
// ============================================================================

mod browser {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct MintUrlsResponse {
        ttl_seconds: u64,
    }

    #[tokio::test]
    async fn test_mint_urls_rejects_path_traversal() -> Result<()> {
        let router = test_router();

        let request = helpers::make_request(
            Method::POST,
            "/api/v1/browser/urls",
            Some(serde_json::json!({
                "domain": "catalog",
                "paths": ["../../../etc/passwd"]
            })),
        )?;
        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        Ok(())
    }

    #[tokio::test]
    async fn test_mint_urls_rejects_unknown_domain() -> Result<()> {
        let router = test_router();

        let request = helpers::make_request(
            Method::POST,
            "/api/v1/browser/urls",
            Some(serde_json::json!({
                "domain": "unknown_domain",
                "paths": ["some/path"]
            })),
        )?;
        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[tokio::test]
    async fn test_mint_urls_ttl_bounded() -> Result<()> {
        let router = test_router();

        // Ensure manifests exist for get_mintable_paths().
        let (status, _): (_, serde_json::Value) = helpers::post_json(
            router.clone(),
            "/api/v1/namespaces",
            serde_json::json!({"name": "init"}),
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);

        let (status, result): (_, MintUrlsResponse) = helpers::post_json(
            router.clone(),
            "/api/v1/browser/urls",
            serde_json::json!({
                "domain": "catalog",
                "paths": [],
                "ttl_seconds": 999_999
            }),
        )
        .await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(result.ttl_seconds, 3600);
        Ok(())
    }
}

// ============================================================================
// Manifest Tests
// ============================================================================

mod manifests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct DeployManifestResponse {
        manifest_id: String,
        workspace_id: String,
        code_version_id: String,
        fingerprint: String,
        asset_count: u32,
        #[allow(dead_code)]
        deployed_at: String,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ManifestListItem {
        manifest_id: String,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ListManifestsResponse {
        manifests: Vec<ManifestListItem>,
    }

    #[tokio::test]
    async fn test_manifest_deploy_get_list_idempotent() -> Result<()> {
        let router = test_router();

        let request = serde_json::json!({
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

        let (status, deploy): (_, DeployManifestResponse) = helpers::post_json_with_headers(
            router.clone(),
            "/api/v1/workspaces/test-workspace/manifests",
            request.clone(),
            &[("Idempotency-Key", "idem-001")],
        )
        .await?;

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(deploy.workspace_id, "test-workspace");
        assert_eq!(deploy.code_version_id, "abc123");
        assert_eq!(deploy.asset_count, 1);
        assert!(!deploy.fingerprint.is_empty());

        let (status, deploy_repeat): (_, DeployManifestResponse) = helpers::post_json_with_headers(
            router.clone(),
            "/api/v1/workspaces/test-workspace/manifests",
            request,
            &[("Idempotency-Key", "idem-001")],
        )
        .await?;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(deploy_repeat.manifest_id, deploy.manifest_id);

        let get_path = format!(
            "/api/v1/workspaces/test-workspace/manifests/{}",
            deploy.manifest_id
        );
        let (status, stored): (_, serde_json::Value) =
            helpers::get_json(router.clone(), &get_path).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(stored["manifestId"], deploy.manifest_id);

        let (status, list): (_, ListManifestsResponse) = helpers::get_json(
            router.clone(),
            "/api/v1/workspaces/test-workspace/manifests",
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert!(
            list.manifests
                .iter()
                .any(|item| item.manifest_id == deploy.manifest_id)
        );

        let (status, _): (_, serde_json::Value) = helpers::get_json(
            router,
            "/api/v1/workspaces/test-workspace/manifests/does-not-exist",
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND);

        Ok(())
    }
}

// ============================================================================
// Orchestration Tests
// ============================================================================

mod orchestration {
    use super::*;
    use serde::Deserialize;
    use std::sync::Arc;

    use chrono::Utc;

    use arco_core::ScopedStorage;
    use arco_core::storage::{MemoryBackend, StorageBackend};
    use arco_flow::orchestration::LedgerWriter;
    use arco_flow::orchestration::compactor::MicroCompactor;
    use arco_flow::orchestration::controllers::ReadyDispatchController;
    use arco_flow::orchestration::controllers::{
        PollSensorResult, PubSubMessage, SensorEvaluationError, SensorEvaluator,
    };
    use arco_flow::orchestration::events::{
        OrchestrationEvent, OrchestrationEventData, RunRequest,
    };

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TriggerRunResponse {
        run_id: String,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct RunResponse {
        run_id: String,
        tasks: Vec<TaskSummary>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TaskSummary {
        task_key: String,
        attempt: u32,
        state: String,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TaskCallbackResponse {
        acknowledged: bool,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct RunLogsResponse {
        path: String,
        size_bytes: u64,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ManualSensorEvaluateResponse {
        eval_id: String,
        message_id: String,
        status: String,
        message: Option<String>,
        #[serde(default)]
        run_requests: Vec<RunRequestResponse>,
        events_written: u32,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[allow(dead_code)]
    struct RunRequestResponse {
        run_key: String,
        request_fingerprint: String,
        asset_selection: Vec<String>,
        partition_selection: Option<Vec<String>>,
    }

    #[derive(Debug)]
    struct ManualSensorEvaluator;

    impl SensorEvaluator for ManualSensorEvaluator {
        fn evaluate_push(
            &self,
            _sensor_id: &str,
            _message: &PubSubMessage,
        ) -> Result<Vec<RunRequest>, SensorEvaluationError> {
            Ok(vec![RunRequest {
                run_key: "sensor:manual:run_01".to_string(),
                request_fingerprint: "fp_manual".to_string(),
                asset_selection: vec!["analytics.summary".to_string()],
                partition_selection: None,
            }])
        }

        fn evaluate_poll(
            &self,
            _sensor_id: &str,
            _cursor_before: Option<&str>,
        ) -> Result<PollSensorResult, SensorEvaluationError> {
            Ok(PollSensorResult {
                cursor_after: None,
                run_requests: Vec::new(),
            })
        }
    }

    #[tokio::test]
    async fn test_servo_deploy_run_callbacks_and_logs() -> Result<()> {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let router = ServerBuilder::new()
            .debug(true)
            .storage_backend(backend.clone())
            .build()
            .test_router();

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
                "assets": ["analytics.users"],
                "timezone": "UTC"
            }]
        });

        let (status, _deploy): (_, serde_json::Value) = helpers::post_json_with_headers(
            router.clone(),
            "/api/v1/workspaces/test-workspace/manifests",
            manifest,
            &[("Idempotency-Key", "idem-servo-001")],
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);

        let run_request = serde_json::json!({
            "selection": ["analytics.users"],
            "partitions": []
        });
        let (status, trigger): (_, TriggerRunResponse) = helpers::post_json(
            router.clone(),
            "/api/v1/workspaces/test-workspace/runs",
            run_request,
        )
        .await?;
        assert_eq!(status, StatusCode::CREATED);

        let run_path = format!("/api/v1/workspaces/test-workspace/runs/{}", trigger.run_id);
        let (status, run): (_, RunResponse) = helpers::get_json(router.clone(), &run_path).await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(run.run_id, trigger.run_id);
        let task = run.tasks.first().context("expected run task")?;
        assert_eq!(task.attempt, 0);

        let task_key = task.task_key.clone();
        let storage = ScopedStorage::new(backend.clone(), "test-tenant", "test-workspace")?;
        let ledger = LedgerWriter::new(storage.clone());
        let compactor = MicroCompactor::new(storage.clone());
        let (manifest, fold_state) = compactor.load_state().await?;
        let ready_controller = ReadyDispatchController::with_defaults();
        let ready_actions = ready_controller.reconcile(&manifest, &fold_state);
        let ready_events: Vec<_> = ready_actions
            .into_iter()
            .filter_map(|action| action.into_event_data())
            .map(|data| OrchestrationEvent::new("test-tenant", "test-workspace", data))
            .collect();

        let (attempt, attempt_id) = ready_events
            .iter()
            .find_map(|event| match &event.data {
                OrchestrationEventData::DispatchRequested {
                    task_key: event_task_key,
                    attempt,
                    attempt_id,
                    ..
                } if event_task_key == &task_key => Some((*attempt, attempt_id.clone())),
                _ => None,
            })
            .context("expected DispatchRequested event for task")?;

        let event_paths: Vec<_> = ready_events.iter().map(LedgerWriter::event_path).collect();
        ledger.append_all(ready_events).await?;
        compactor.compact_events(event_paths).await?;

        let started_request = serde_json::json!({
            "attempt": attempt,
            "attemptId": attempt_id,
            "workerId": "worker-1",
            "startedAt": Utc::now().to_rfc3339(),
        });
        let (status, started): (_, TaskCallbackResponse) = helpers::post_json_with_headers(
            router.clone(),
            &format!("/api/v1/tasks/{task_key}/started"),
            started_request,
            &[("Authorization", "Bearer test-token")],
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert!(started.acknowledged);

        let logs_request = serde_json::json!({
            "taskKey": task_key.clone(),
            "attempt": attempt,
            "stdout": "hello stdout",
            "stderr": "hello stderr",
        });
        let (status, logs): (_, RunLogsResponse) = helpers::post_json(
            router.clone(),
            &format!(
                "/api/v1/workspaces/test-workspace/runs/{}/logs",
                trigger.run_id
            ),
            logs_request,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert!(logs.path.contains("attempt-1.log"));
        assert!(logs.size_bytes > 0);

        let completed_request = serde_json::json!({
            "attempt": attempt,
            "attemptId": attempt_id,
            "workerId": "worker-1",
            "outcome": "SUCCEEDED",
            "completedAt": Utc::now().to_rfc3339(),
        });
        let (status, completed): (_, TaskCallbackResponse) = helpers::post_json_with_headers(
            router.clone(),
            &format!("/api/v1/tasks/{task_key}/completed"),
            completed_request,
            &[("Authorization", "Bearer test-token")],
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert!(completed.acknowledged);

        let (status, finished): (_, RunResponse) =
            helpers::get_json(router.clone(), &run_path).await?;
        assert_eq!(status, StatusCode::OK);
        let finished_task = finished
            .tasks
            .iter()
            .find(|task| task.task_key == task_key)
            .context("expected completed task")?;
        assert_eq!(finished_task.state, "SUCCEEDED");

        let (status, logs_text) = helpers::get_text(
            router,
            &format!(
                "/api/v1/workspaces/test-workspace/runs/{}/logs",
                trigger.run_id
            ),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert!(logs_text.contains("=== stdout ==="));
        assert!(logs_text.contains("hello stdout"));
        assert!(logs_text.contains("=== stderr ==="));
        assert!(logs_text.contains("hello stderr"));

        Ok(())
    }

    #[tokio::test]
    async fn test_manual_sensor_evaluate_endpoint() -> Result<()> {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let router = ServerBuilder::new()
            .debug(true)
            .storage_backend(backend.clone())
            .sensor_evaluator(Arc::new(ManualSensorEvaluator))
            .build()
            .test_router();

        let request = serde_json::json!({
            "payload": {
                "bucket": "test-bucket",
                "object": "data/file.parquet"
            },
            "attributes": {
                "source": "manual"
            }
        });

        let (status, response): (_, ManualSensorEvaluateResponse) =
            helpers::post_json_with_headers(
                router,
                "/api/v1/workspaces/test-workspace/sensors/01HQTESTSENSOR/evaluate",
                request,
                &[("Idempotency-Key", "manual-eval-001")],
            )
            .await?;

        assert_eq!(status, StatusCode::OK);
        assert!(!response.eval_id.is_empty());
        assert_eq!(response.message_id, "manual-eval-001");
        assert_eq!(response.status, "triggered");
        assert!(response.message.is_none());
        assert_eq!(response.run_requests.len(), 1);
        assert_eq!(response.run_requests[0].run_key, "sensor:manual:run_01");
        assert_eq!(response.events_written, 2);

        Ok(())
    }
}

// ============================================================================
// Cross-Cutting Tests
// ============================================================================

mod cross_cutting {
    use super::*;
    use serde::Deserialize;

    const TEST_RSA_PRIVATE_KEY_PEM: &str = r#"-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTL
UTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2V
rUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8H
oGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBI
Mc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/
by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKd
WUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZ
Dpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7j
E0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5Jn
LnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSS
bYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE
8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBl
xyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY
2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp1
9m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P0
7mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZ
mY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7
MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4
t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIam
QOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA
2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ
4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gn
PYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJH
UvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8
oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==
-----END RSA PRIVATE KEY-----"#;

    const TEST_RSA_PUBLIC_KEY_PEM: &str = r#"-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4
l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2VrUyW
yj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG
/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4l
QzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/by2h
3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQAB
-----END RSA PUBLIC KEY-----"#;

    const TEST_USER_ID: &str = "test-user";

    fn make_test_jwt(tenant: &str, workspace: &str) -> Result<String> {
        use serde::Serialize;
        use std::time::{Duration, SystemTime, UNIX_EPOCH};

        #[derive(Debug, Serialize)]
        struct Claims<'a> {
            tenant: &'a str,
            workspace: &'a str,
            sub: &'a str,
            exp: u64,
        }

        let exp = SystemTime::now()
            .checked_add(Duration::from_secs(60 * 60))
            .context("compute JWT expiry")?
            .duration_since(UNIX_EPOCH)
            .context("system time before unix epoch")?
            .as_secs();

        let claims = Claims {
            tenant,
            workspace,
            sub: TEST_USER_ID,
            exp,
        };

        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes()),
        )
        .context("encode JWT")
    }

    fn make_test_jwt_without_user(tenant: &str, workspace: &str) -> Result<String> {
        use serde::Serialize;
        use std::time::{Duration, SystemTime, UNIX_EPOCH};

        #[derive(Debug, Serialize)]
        struct Claims<'a> {
            tenant: &'a str,
            workspace: &'a str,
            exp: u64,
        }

        let exp = SystemTime::now()
            .checked_add(Duration::from_secs(60 * 60))
            .context("compute JWT expiry")?
            .duration_since(UNIX_EPOCH)
            .context("system time before unix epoch")?
            .as_secs();

        let claims = Claims {
            tenant,
            workspace,
            exp,
        };

        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes()),
        )
        .context("encode JWT")
    }

    fn make_test_jwt_rs256(tenant: &str, workspace: &str) -> Result<String> {
        use serde::Serialize;
        use std::time::{Duration, SystemTime, UNIX_EPOCH};

        #[derive(Debug, Serialize)]
        struct Claims<'a> {
            tenant: &'a str,
            workspace: &'a str,
            sub: &'a str,
            exp: u64,
        }

        let exp = SystemTime::now()
            .checked_add(Duration::from_secs(60 * 60))
            .context("compute JWT expiry")?
            .duration_since(UNIX_EPOCH)
            .context("system time before unix epoch")?
            .as_secs();

        let claims = Claims {
            tenant,
            workspace,
            sub: TEST_USER_ID,
            exp,
        };

        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY_PEM.as_bytes())
                .context("load RSA private key")?,
        )
        .context("encode JWT")
    }

    #[tokio::test]
    async fn test_missing_tenant_header_returns_401() -> Result<()> {
        let router = test_router();

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/namespaces")
            .header("X-Workspace-Id", "test-workspace")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        Ok(())
    }

    #[tokio::test]
    async fn test_missing_workspace_header_returns_401() -> Result<()> {
        let router = test_router();

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/namespaces")
            .header("X-Tenant-Id", "test-tenant")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        Ok(())
    }

    #[tokio::test]
    async fn test_production_mode_requires_authorization_header() -> Result<()> {
        #[derive(Debug, Deserialize)]
        struct ErrorBody {
            code: String,
        }

        let router = test_router_prod();

        // Note: tenant/workspace headers are ignored in production mode.
        let request = helpers::make_request(Method::GET, "/api/v1/namespaces", None)?;
        let response = router.oneshot(request).await.map_err(|err| match err {})?;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .context("read response body")?;
        let error: ErrorBody = serde_json::from_slice(&body).context("parse JSON body")?;
        assert_eq!(error.code, "MISSING_AUTH");

        Ok(())
    }

    #[tokio::test]
    async fn test_production_mode_rejects_missing_user_claim() -> Result<()> {
        #[derive(Debug, Deserialize)]
        struct ErrorBody {
            code: String,
        }

        let router = test_router_prod();
        let jwt = make_test_jwt_without_user("test-tenant", "test-workspace")?;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/namespaces")
            .header(header::AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .context("read response body")?;
        let error: ErrorBody = serde_json::from_slice(&body).context("parse JSON body")?;
        assert_eq!(error.code, "INVALID_TOKEN");

        Ok(())
    }

    #[tokio::test]
    async fn test_production_mode_accepts_bearer_jwt() -> Result<()> {
        let router = test_router_prod();

        let jwt = make_test_jwt("test-tenant", "test-workspace")?;

        let body = serde_json::to_vec(&serde_json::json!({ "name": "prod_ns" }))
            .context("serialize body")?;

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/namespaces")
            .header(header::AUTHORIZATION, format!("Bearer {jwt}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert_eq!(response.status(), StatusCode::CREATED);

        Ok(())
    }

    #[tokio::test]
    async fn test_production_mode_accepts_rs256_jwt() -> Result<()> {
        let config = Config {
            debug: false,
            jwt: arco_api::config::JwtConfig {
                rs256_public_key_pem: Some(TEST_RSA_PUBLIC_KEY_PEM.to_string()),
                ..arco_api::config::JwtConfig::default()
            },
            ..Config::default()
        };
        let router = Server::new(config).test_router();

        let jwt = make_test_jwt_rs256("test-tenant", "test-workspace")?;

        let body = serde_json::to_vec(&serde_json::json!({ "name": "prod_rs_ns" }))
            .context("serialize body")?;

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/namespaces")
            .header(header::AUTHORIZATION, format!("Bearer {jwt}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert_eq!(response.status(), StatusCode::CREATED);

        Ok(())
    }

    #[tokio::test]
    async fn test_idempotency_key_accepted() -> Result<()> {
        let router = test_router();

        let body = serde_json::to_vec(&serde_json::json!({ "name": "idempotent_ns" }))
            .context("serialize body")?;

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/namespaces")
            .header("X-Tenant-Id", "test-tenant")
            .header("X-Workspace-Id", "test-workspace")
            .header("Idempotency-Key", "unique-key-123")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert!(response.status() == StatusCode::CREATED || response.status() == StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn test_cors_disabled_by_default() -> Result<()> {
        let router = test_router();

        let request = Request::builder()
            .method(Method::GET)
            .uri("/health")
            .header("Origin", "http://localhost:3000")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            !response
                .headers()
                .contains_key("access-control-allow-origin")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cors_preflight_request() -> Result<()> {
        let router = test_router_with_cors(vec!["*".to_string()]);

        // CORS preflight request
        let request = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/v1/namespaces")
            .header("Origin", "http://localhost:3000")
            .header("Access-Control-Request-Method", "POST")
            .header("Access-Control-Request-Headers", "content-type,x-tenant-id")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .contains_key("access-control-allow-origin")
        );
        assert!(
            response
                .headers()
                .contains_key("access-control-allow-methods")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_cors_headers_on_response() -> Result<()> {
        let router = test_router_with_cors(vec!["*".to_string()]);

        // Regular request with Origin header
        let request = Request::builder()
            .method(Method::GET)
            .uri("/health")
            .header("Origin", "http://localhost:3000")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .contains_key("access-control-allow-origin")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_cors_specific_origin_allows_matching_origin() -> Result<()> {
        let router = test_router_with_cors(vec!["https://allowed.example".to_string()]);

        let request = Request::builder()
            .method(Method::GET)
            .uri("/health")
            .header("Origin", "https://allowed.example")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert_eq!(response.status(), StatusCode::OK);

        let origin = response
            .headers()
            .get("access-control-allow-origin")
            .context("missing access-control-allow-origin")?;
        assert_eq!(origin, "https://allowed.example");

        Ok(())
    }

    #[tokio::test]
    async fn test_cors_specific_origin_rejects_non_matching_origin() -> Result<()> {
        let router = test_router_with_cors(vec!["https://allowed.example".to_string()]);

        let request = Request::builder()
            .method(Method::GET)
            .uri("/health")
            .header("Origin", "https://not-allowed.example")
            .body(Body::empty())
            .context("build request")?;

        let response = router.oneshot(request).await.map_err(|err| match err {})?;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            !response
                .headers()
                .contains_key("access-control-allow-origin")
        );

        Ok(())
    }
}

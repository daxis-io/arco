//! Minimal Cloud Run worker used by the live pipeline smoke path.

use std::net::SocketAddr;
use std::{io::Write as _, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::{Duration, Instant, sleep};
use uuid::Uuid;

use arco_core::ScopedStorage;
use arco_core::observability::{LogFormat, init_logging};
use arco_core::storage::{ObjectStoreBackend, StorageBackend, WritePrecondition};
use arco_flow::orchestration::worker_contract::WorkerDispatchEnvelope;

const DEFAULT_ASSET_NAMESPACE: &str = "pipeline";
const DEFAULT_ASSET_NAME: &str = "orders_smoke";
const DEFAULT_WORKER_ID: &str = "arco-flow-worker-smoke";
const DEFAULT_VERIFY_TIMEOUT_SECS: u64 = 180;
const VERIFY_POLL_SECS: u64 = 5;

#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    config: Arc<WorkerConfig>,
    storage: ScopedStorage,
}

#[derive(Clone, Debug)]
struct WorkerConfig {
    api_url: String,
    tenant_id: String,
    workspace_id: String,
    storage_bucket: String,
    worker_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorResponse {
    error: String,
}

#[derive(Debug)]
struct WorkerError(anyhow::Error);

impl IntoResponse for WorkerError {
    fn into_response(self) -> Response {
        tracing::error!(error = %self.0, "flow worker dispatch failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: self.0.to_string(),
            }),
        )
            .into_response()
    }
}

impl From<anyhow::Error> for WorkerError {
    fn from(value: anyhow::Error) -> Self {
        Self(value)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreatedTableResponse {
    id: String,
    #[allow(dead_code)]
    format: String,
}

#[derive(Debug, Deserialize)]
struct StageCommitResponse {
    staged_path: String,
    staged_version: String,
}

#[derive(Debug, Deserialize)]
struct CommitResponse {
    version: i64,
    delta_log_path: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DispatchResponse {
    run_id: String,
    task_key: String,
    table_id: String,
    delta_table: String,
    delta_version: i64,
    delta_log_path: String,
    data_path: String,
    rows: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeployManifestResponse {
    manifest_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TriggerRunResponse {
    run_id: String,
    plan_id: String,
    state: String,
    created: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DriverResponse {
    manifest_id: String,
    run_id: String,
    plan_id: String,
    state: String,
    created: bool,
    selection: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunStatusResponse {
    run_id: String,
    state: String,
    tasks: Vec<RunTaskSummary>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunTaskSummary {
    task_key: String,
    #[serde(default)]
    asset_key: Option<String>,
    state: String,
    #[serde(default)]
    delta_table: Option<String>,
    #[serde(default)]
    delta_version: Option<i64>,
    #[serde(default)]
    output_visibility_state: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct VerifyResponse {
    run_id: String,
    run_state: String,
    task_key: String,
    task_state: String,
    output_visibility_state: Option<String>,
    delta_table: String,
    delta_version: i64,
    data_path: String,
    delta_log_path: String,
    data_object_exists: bool,
    delta_log_object_exists: bool,
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn dispatch_handler(
    State(state): State<AppState>,
    Json(envelope): Json<WorkerDispatchEnvelope>,
) -> Result<Json<DispatchResponse>, WorkerError> {
    execute_dispatch(&state, envelope)
        .await
        .map(Json)
        .map_err(WorkerError::from)
}

async fn execute_dispatch(
    state: &AppState,
    envelope: WorkerDispatchEnvelope,
) -> Result<DispatchResponse> {
    validate_envelope_scope(&state.config, &envelope)?;
    post_started(&state.client, &envelope, &state.config).await?;

    match materialize_delta_table(state, &envelope).await {
        Ok(response) => {
            post_completed_success(&state.client, &envelope, &state.config, &response).await?;
            Ok(response)
        }
        Err(error) => {
            let message = error.to_string();
            let _ = post_completed_failure(&state.client, &envelope, &state.config, &message).await;
            Err(error)
        }
    }
}

fn validate_envelope_scope(config: &WorkerConfig, envelope: &WorkerDispatchEnvelope) -> Result<()> {
    if envelope.tenant_id != config.tenant_id {
        bail!(
            "tenant mismatch: envelope={} configured={}",
            envelope.tenant_id,
            config.tenant_id
        );
    }
    if envelope.workspace_id != config.workspace_id {
        bail!(
            "workspace mismatch: envelope={} configured={}",
            envelope.workspace_id,
            config.workspace_id
        );
    }
    Ok(())
}

async fn materialize_delta_table(
    state: &AppState,
    envelope: &WorkerDispatchEnvelope,
) -> Result<DispatchResponse> {
    let suffix = smoke_name_suffix(&envelope.run_id);
    let catalog = format!("pipeline_smoke_{suffix}");
    let schema = "sales";
    let table = "orders";
    let delta_table = format!("{catalog}.{schema}.{table}");
    let location = smoke_table_location(&envelope.run_id, &envelope.task_key);
    let data_file_name = format!("part-00000-{suffix}.parquet");
    let data_path = format!("{location}/{data_file_name}");
    let rows = sample_rows(&envelope.run_id, &envelope.task_key);
    let parquet = build_orders_parquet(&rows)?;
    let byte_size = u64::try_from(parquet.len()).context("parquet size overflow")?;

    state
        .storage
        .put_raw(
            &data_path,
            Bytes::from(parquet),
            WritePrecondition::DoesNotExist,
        )
        .await
        .with_context(|| format!("failed to write data file {data_path}"))?;

    let table_id = create_catalog_schema_table(
        &state.client,
        &state.config,
        &catalog,
        schema,
        table,
        &location,
    )
    .await?;

    let payload = build_initial_delta_payload(
        &table_id,
        &data_file_name,
        byte_size,
        u64::try_from(rows.len()).context("row count overflow")?,
        Utc::now().timestamp_millis(),
    );
    let staged = stage_delta_commit(&state.client, &state.config, &table_id, &payload).await?;
    let committed = commit_delta(&state.client, &state.config, &table_id, staged).await?;

    Ok(DispatchResponse {
        run_id: envelope.run_id.clone(),
        task_key: envelope.task_key.clone(),
        table_id,
        delta_table,
        delta_version: committed.version,
        delta_log_path: committed.delta_log_path,
        data_path,
        rows: u64::try_from(rows.len()).unwrap_or(u64::MAX),
    })
}

async fn create_catalog_schema_table(
    client: &reqwest::Client,
    config: &WorkerConfig,
    catalog: &str,
    schema: &str,
    table: &str,
    location: &str,
) -> Result<String> {
    let catalog_url = api_v1_url(config, "/catalogs");
    post_json(
        client,
        &catalog_url,
        config,
        None,
        Some(Uuid::now_v7().to_string()),
        &json!({ "name": catalog }),
        &[StatusCode::CREATED],
    )
    .await?;

    let schema_url = api_v1_url(config, &format!("/catalogs/{catalog}/schemas"));
    post_json(
        client,
        &schema_url,
        config,
        None,
        Some(Uuid::now_v7().to_string()),
        &json!({ "name": schema }),
        &[StatusCode::CREATED],
    )
    .await?;

    let table_url = api_v1_url(
        config,
        &format!("/catalogs/{catalog}/schemas/{schema}/tables"),
    );
    let value = post_json(
        client,
        &table_url,
        config,
        None,
        Some(Uuid::now_v7().to_string()),
        &smoke_table_registration_body(table, location),
        &[StatusCode::CREATED],
    )
    .await?;
    let created: CreatedTableResponse =
        serde_json::from_value(value).context("failed to parse create table response")?;
    Ok(created.id)
}

fn smoke_table_registration_body(table: &str, location: &str) -> Value {
    json!({
        "name": table,
        "format": "delta",
        "columns": [
            {"name": "order_id", "data_type": "long", "nullable": false},
            {"name": "customer", "data_type": "string", "nullable": false},
            {"name": "run_id", "data_type": "string", "nullable": false}
        ],
        "description": "Live Arco pipeline smoke Delta table",
        "location": location
    })
}

async fn stage_delta_commit(
    client: &reqwest::Client,
    config: &WorkerConfig,
    table_id: &str,
    payload: &str,
) -> Result<StageCommitResponse> {
    let url = api_v1_url(config, &format!("/delta/tables/{table_id}/commits/stage"));
    let value = post_json(
        client,
        &url,
        config,
        None,
        None,
        &json!({ "payload": payload }),
        &[StatusCode::OK],
    )
    .await?;
    serde_json::from_value(value).context("failed to parse stage commit response")
}

async fn commit_delta(
    client: &reqwest::Client,
    config: &WorkerConfig,
    table_id: &str,
    staged: StageCommitResponse,
) -> Result<CommitResponse> {
    let url = api_v1_url(config, &format!("/delta/tables/{table_id}/commits"));
    let value = post_json(
        client,
        &url,
        config,
        None,
        Some(Uuid::now_v7().to_string()),
        &delta_commit_request_body(&staged, -1),
        &[StatusCode::OK],
    )
    .await?;
    serde_json::from_value(value).context("failed to parse commit response")
}

fn delta_commit_request_body(staged: &StageCommitResponse, read_version: i64) -> Value {
    json!({
        "read_version": read_version,
        "staged_path": staged.staged_path,
        "staged_version": staged.staged_version
    })
}

async fn post_started(
    client: &reqwest::Client,
    envelope: &WorkerDispatchEnvelope,
    config: &WorkerConfig,
) -> Result<()> {
    let url = api_url(
        &envelope.callback_base_url,
        &task_callback_path(envelope, "started"),
    );
    post_json(
        client,
        &url,
        config,
        Some(envelope.task_token.as_str()),
        None,
        &json!({
            "attempt": envelope.attempt,
            "attemptId": envelope.attempt_id,
            "workerId": config.worker_id,
            "traceparent": envelope.traceparent,
            "startedAt": Utc::now()
        }),
        &[StatusCode::OK],
    )
    .await?;
    Ok(())
}

async fn post_completed_success(
    client: &reqwest::Client,
    envelope: &WorkerDispatchEnvelope,
    config: &WorkerConfig,
    response: &DispatchResponse,
) -> Result<()> {
    let url = api_url(
        &envelope.callback_base_url,
        &task_callback_path(envelope, "completed"),
    );
    post_json(
        client,
        &url,
        config,
        Some(envelope.task_token.as_str()),
        None,
        &json!({
            "attempt": envelope.attempt,
            "attemptId": envelope.attempt_id,
            "workerId": config.worker_id,
            "traceparent": envelope.traceparent,
            "outcome": "SUCCEEDED",
            "completedAt": Utc::now(),
            "output": {
                "materializationId": format!("mat_{}", smoke_name_suffix(&envelope.run_id)),
                "rowCount": response.rows,
                "byteSize": null,
                "outputPath": response.data_path,
                "deltaTable": response.delta_table,
                "deltaVersion": response.delta_version,
                "deltaPartition": null,
                "outputVisibilityState": "VISIBLE",
                "publishedAt": Utc::now(),
                "publishError": null
            },
            "error": null,
            "metrics": {
                "ioWriteBytes": null
            },
            "cancelledDuringPhase": null,
            "partialProgress": null
        }),
        &[StatusCode::OK],
    )
    .await?;
    Ok(())
}

async fn post_completed_failure(
    client: &reqwest::Client,
    envelope: &WorkerDispatchEnvelope,
    config: &WorkerConfig,
    message: &str,
) -> Result<()> {
    let url = api_url(
        &envelope.callback_base_url,
        &task_callback_path(envelope, "completed"),
    );
    post_json(
        client,
        &url,
        config,
        Some(envelope.task_token.as_str()),
        None,
        &json!({
            "attempt": envelope.attempt,
            "attemptId": envelope.attempt_id,
            "workerId": config.worker_id,
            "traceparent": envelope.traceparent,
            "outcome": "FAILED",
            "completedAt": Utc::now(),
            "output": null,
            "error": {
                "category": "INFRASTRUCTURE",
                "message": message,
                "retryable": false
            },
            "metrics": null,
            "cancelledDuringPhase": null,
            "partialProgress": null
        }),
        &[StatusCode::OK, StatusCode::CONFLICT, StatusCode::GONE],
    )
    .await?;
    Ok(())
}

async fn run_driver(config: WorkerConfig) -> Result<DriverResponse> {
    let client = reqwest::Client::new();
    let manifest_id = deploy_smoke_manifest(&client, &config).await?;
    let trigger = trigger_smoke_run(&client, &config).await?;
    Ok(DriverResponse {
        manifest_id,
        run_id: trigger.run_id,
        plan_id: trigger.plan_id,
        state: trigger.state,
        created: trigger.created,
        selection: vec![format!("{DEFAULT_ASSET_NAMESPACE}.{DEFAULT_ASSET_NAME}")],
    })
}

async fn run_verifier(
    config: WorkerConfig,
    storage: ScopedStorage,
    run_id: &str,
) -> Result<VerifyResponse> {
    let client = reqwest::Client::new();
    let timeout = verify_timeout();
    let start = Instant::now();
    let asset_key = format!("{DEFAULT_ASSET_NAMESPACE}.{DEFAULT_ASSET_NAME}");

    loop {
        let status = fetch_run_status(&client, &config, run_id).await?;
        let Some(task) = status.tasks.iter().find(|task| {
            task.task_key == asset_key || task.asset_key.as_deref() == Some(asset_key.as_str())
        }) else {
            bail!("run {} does not include task {asset_key}", status.run_id);
        };

        if status.state == "SUCCEEDED" && task.state == "SUCCEEDED" {
            let delta_table = task
                .delta_table
                .clone()
                .ok_or_else(|| anyhow!("task {} succeeded without deltaTable", task.task_key))?;
            let delta_version = task
                .delta_version
                .ok_or_else(|| anyhow!("task {} succeeded without deltaVersion", task.task_key))?;
            let (data_path, delta_log_path) = smoke_output_paths(&status.run_id, &task.task_key);
            let data_object_exists = storage
                .head_raw(&data_path)
                .await
                .with_context(|| format!("failed to stat data object {data_path}"))?
                .is_some();
            let delta_log_object_exists = storage
                .head_raw(&delta_log_path)
                .await
                .with_context(|| format!("failed to stat Delta log object {delta_log_path}"))?
                .is_some();

            if !data_object_exists {
                bail!("expected data object missing: {data_path}");
            }
            if !delta_log_object_exists {
                bail!("expected Delta log object missing: {delta_log_path}");
            }

            return Ok(VerifyResponse {
                run_id: status.run_id,
                run_state: status.state,
                task_key: task.task_key.clone(),
                task_state: task.state.clone(),
                output_visibility_state: task.output_visibility_state.clone(),
                delta_table,
                delta_version,
                data_path,
                delta_log_path,
                data_object_exists,
                delta_log_object_exists,
            });
        }

        if matches!(
            status.state.as_str(),
            "FAILED" | "CANCELLED" | "TIMED_OUT" | "CANCELLING"
        ) {
            bail!(
                "run {} reached terminal state {} before proof completed",
                status.run_id,
                status.state
            );
        }

        if start.elapsed() >= timeout {
            bail!(
                "verification timed out after {}s waiting for run {run_id}; last state={}",
                timeout.as_secs(),
                status.state
            );
        }
        sleep(Duration::from_secs(VERIFY_POLL_SECS)).await;
    }
}

async fn fetch_run_status(
    client: &reqwest::Client,
    config: &WorkerConfig,
    run_id: &str,
) -> Result<RunStatusResponse> {
    let url = api_v1_url(
        config,
        &format!("/workspaces/{}/runs/{run_id}", config.workspace_id),
    );
    let value = get_json(client, &url, config, &[StatusCode::OK]).await?;
    serde_json::from_value(value).context("failed to parse run status response")
}

async fn deploy_smoke_manifest(client: &reqwest::Client, config: &WorkerConfig) -> Result<String> {
    let url = api_v1_url(
        config,
        &format!("/workspaces/{}/manifests", config.workspace_id),
    );
    let value = post_json(
        client,
        &url,
        config,
        None,
        Some(Uuid::now_v7().to_string()),
        &json!({
            "manifestVersion": "1.0",
            "codeVersionId": format!("live-pipeline-smoke-{}", Utc::now().timestamp()),
            "git": {
                "repository": "https://github.com/daxis-io/arco",
                "branch": "live-cloud-smoke",
                "commitSha": "",
                "commitMessage": "live pipeline smoke",
                "author": "local-smoke",
                "dirty": true
            },
            "assets": [{
                "key": {
                    "namespace": DEFAULT_ASSET_NAMESPACE,
                    "name": DEFAULT_ASSET_NAME
                },
                "id": Uuid::now_v7().to_string(),
                "description": "Live Cloud Run pipeline smoke asset",
                "owners": ["platform"],
                "tags": {},
                "partitioning": {},
                "dependencies": [],
                "code": {"kind": "cloud_run_worker", "binary": "arco_flow_worker"},
                "checks": [],
                "execution": {"queue": "default-queue"},
                "resources": {},
                "io": {"format": "delta"},
                "transformFingerprint": "live-pipeline-smoke"
            }],
            "schedules": [],
            "deployedBy": "live-smoke",
            "metadata": {"purpose": "end-to-end-live-pipeline-proof"}
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await?;
    let response: DeployManifestResponse =
        serde_json::from_value(value).context("failed to parse deploy manifest response")?;
    Ok(response.manifest_id)
}

async fn trigger_smoke_run(
    client: &reqwest::Client,
    config: &WorkerConfig,
) -> Result<TriggerRunResponse> {
    let url = api_v1_url(config, &format!("/workspaces/{}/runs", config.workspace_id));
    let value = post_json(
        client,
        &url,
        config,
        None,
        None,
        &json!({
            "selection": [format!("{DEFAULT_ASSET_NAMESPACE}.{DEFAULT_ASSET_NAME}")],
            "includeUpstream": false,
            "includeDownstream": false,
            "partitions": [],
            "runKey": format!("live-pipeline-smoke:{}", Uuid::now_v7()),
            "labels": {
                "proof": "live_pipeline_delta_catalog",
                "driver": "arco_flow_worker"
            }
        }),
        &[StatusCode::CREATED, StatusCode::OK],
    )
    .await?;
    serde_json::from_value(value).context("failed to parse trigger run response")
}

async fn post_json(
    client: &reqwest::Client,
    url: &str,
    config: &WorkerConfig,
    bearer: Option<&str>,
    idempotency_key: Option<String>,
    body: &Value,
    accepted: &[StatusCode],
) -> Result<Value> {
    let mut request = client
        .post(url)
        .header("X-Tenant-Id", &config.tenant_id)
        .header("X-Workspace-Id", &config.workspace_id)
        .json(body);

    if let Some(token) = bearer {
        request = request.bearer_auth(token);
    }
    if let Some(key) = idempotency_key {
        request = request.header("Idempotency-Key", key);
    }

    let response = request
        .send()
        .await
        .with_context(|| format!("request failed: POST {url}"))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("failed to read response body from POST {url}"))?;

    if !accepted.contains(&status) {
        let body = String::from_utf8_lossy(&bytes);
        bail!("POST {url} returned {status}: {body}");
    }

    if bytes.is_empty() {
        return Ok(Value::Null);
    }

    serde_json::from_slice(&bytes).with_context(|| format!("failed to parse JSON from POST {url}"))
}

async fn get_json(
    client: &reqwest::Client,
    url: &str,
    config: &WorkerConfig,
    accepted: &[StatusCode],
) -> Result<Value> {
    let response = client
        .get(url)
        .header("X-Tenant-Id", &config.tenant_id)
        .header("X-Workspace-Id", &config.workspace_id)
        .send()
        .await
        .with_context(|| format!("request failed: GET {url}"))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("failed to read response body from GET {url}"))?;

    if !accepted.contains(&status) {
        let body = String::from_utf8_lossy(&bytes);
        bail!("GET {url} returned {status}: {body}");
    }

    serde_json::from_slice(&bytes).with_context(|| format!("failed to parse JSON from GET {url}"))
}

fn build_orders_parquet(rows: &[OrderRow]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.order_id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.customer.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.run_id.as_str())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .context("failed to build Arrow batch")?;

    let mut buffer = Vec::new();
    let props = WriterProperties::builder().build();
    {
        let mut writer =
            ArrowWriter::try_new(&mut buffer, schema, Some(props)).context("parquet writer")?;
        writer
            .write(&batch)
            .context("failed to write parquet batch")?;
        writer.close().context("failed to close parquet writer")?;
    }
    Ok(buffer)
}

#[derive(Debug)]
struct OrderRow {
    order_id: i64,
    customer: String,
    run_id: String,
}

fn sample_rows(run_id: &str, task_key: &str) -> Vec<OrderRow> {
    vec![
        OrderRow {
            order_id: 1001,
            customer: format!("{task_key}:north"),
            run_id: run_id.to_string(),
        },
        OrderRow {
            order_id: 1002,
            customer: format!("{task_key}:south"),
            run_id: run_id.to_string(),
        },
        OrderRow {
            order_id: 1003,
            customer: format!("{task_key}:west"),
            run_id: run_id.to_string(),
        },
    ]
}

fn build_initial_delta_payload(
    table_id: &str,
    data_file_name: &str,
    data_file_size: u64,
    row_count: u64,
    timestamp_millis: i64,
) -> String {
    let schema_string = json!({
        "type": "struct",
        "fields": [
            {"name": "order_id", "type": "long", "nullable": false, "metadata": {}},
            {"name": "customer", "type": "string", "nullable": false, "metadata": {}},
            {"name": "run_id", "type": "string", "nullable": false, "metadata": {}}
        ]
    })
    .to_string();

    let lines = [
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({
            "metaData": {
                "id": table_id,
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": timestamp_millis
            }
        }),
        json!({
            "add": {
                "path": data_file_name,
                "partitionValues": {},
                "size": data_file_size,
                "modificationTime": timestamp_millis,
                "dataChange": true,
                "stats": json!({"numRecords": row_count}).to_string()
            }
        }),
        json!({
            "commitInfo": {
                "timestamp": timestamp_millis,
                "operation": "WRITE",
                "operationParameters": {"mode": "Append"},
                "engineInfo": "arco-flow-worker live smoke"
            }
        }),
    ];

    let mut payload = lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    payload.push('\n');
    payload
}

fn smoke_table_location(run_id: &str, task_key: &str) -> String {
    format!(
        "warehouse/pipeline_smoke/{}/{}",
        sanitize_path_component(run_id),
        sanitize_path_component(task_key)
    )
}

fn smoke_output_paths(run_id: &str, task_key: &str) -> (String, String) {
    let suffix = smoke_name_suffix(run_id);
    let location = smoke_table_location(run_id, task_key);
    (
        format!("{location}/part-00000-{suffix}.parquet"),
        format!("{location}/_delta_log/00000000000000000000.json"),
    )
}

fn smoke_name_suffix(run_id: &str) -> String {
    sanitize_path_component(run_id).chars().take(16).collect()
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | '0'..='9' => ch,
            'A'..='Z' => ch.to_ascii_lowercase(),
            _ => '_',
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}

fn api_v1_url(config: &WorkerConfig, path: &str) -> String {
    api_url(&config.api_url, &format!("/api/v1{path}"))
}

fn task_callback_path(envelope: &WorkerDispatchEnvelope, phase: &str) -> String {
    format!("/api/v1/tasks/{}/{phase}", envelope.task_id)
}

fn api_url(base: &str, path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path)
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name)
        .map(|value| value.trim().to_string())
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{name} is required"))
}

fn resolve_port() -> Result<u16> {
    if let Ok(port) = std::env::var("PORT") {
        return port
            .parse::<u16>()
            .with_context(|| format!("invalid PORT: {port}"));
    }
    Ok(8080)
}

fn verify_timeout() -> Duration {
    std::env::var("ARCO_VERIFY_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map_or_else(
            || Duration::from_secs(DEFAULT_VERIFY_TIMEOUT_SECS),
            Duration::from_secs,
        )
}

fn config_from_env() -> Result<WorkerConfig> {
    Ok(WorkerConfig {
        api_url: required_env("ARCO_FLOW_API_URL")?,
        tenant_id: required_env("ARCO_FLOW_TENANT_ID")?,
        workspace_id: required_env("ARCO_FLOW_WORKSPACE_ID")?,
        storage_bucket: required_env("ARCO_STORAGE_BUCKET")?,
        worker_id: std::env::var("ARCO_FLOW_WORKER_ID")
            .unwrap_or_else(|_| DEFAULT_WORKER_ID.to_string()),
    })
}

fn storage_from_config(config: &WorkerConfig) -> Result<ScopedStorage> {
    let backend = ObjectStoreBackend::from_bucket(&config.storage_bucket)?;
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    ScopedStorage::new(
        backend,
        config.tenant_id.as_str(),
        config.workspace_id.as_str(),
    )
    .map_err(Into::into)
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging(LogFormat::Pretty);
    let config = config_from_env()?;
    let args = std::env::args().skip(1).collect::<Vec<_>>();

    if args.first().is_some_and(|arg| arg == "driver") {
        let response = run_driver(config).await?;
        print_json_response(&response)?;
        return Ok(());
    }

    let storage = storage_from_config(&config)?;
    if args.first().is_some_and(|arg| arg == "verify") {
        let run_id = args
            .get(1)
            .ok_or_else(|| anyhow!("verify requires a run_id argument"))?;
        let response = run_verifier(config, storage, run_id).await?;
        print_json_response(&response)?;
        return Ok(());
    }

    let state = AppState {
        client: reqwest::Client::new(),
        config: Arc::new(config),
        storage,
    };
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/dispatch", post(dispatch_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], resolve_port()?));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind worker listener")?;
    axum::serve(listener, app)
        .await
        .context("worker server failed")
}

fn print_json_response<T: Serialize>(response: &T) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{}", serde_json::to_string_pretty(response)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope_fixture(task_id: Option<String>) -> WorkerDispatchEnvelope {
        let task_key = "pipeline.orders_smoke".to_string();
        WorkerDispatchEnvelope {
            tenant_id: "tenant-1".to_string(),
            workspace_id: "workspace-1".to_string(),
            run_id: "run-1".to_string(),
            task_id: task_id.unwrap_or_else(|| task_key.clone()),
            task_key,
            attempt: 1,
            attempt_id: "attempt-1".to_string(),
            dispatch_id: "dispatch:run-1:pipeline.orders_smoke:1".to_string(),
            execution_location_id: None,
            worker_queue: "default-queue".to_string(),
            callback_base_url: "https://api.example".to_string(),
            task_token: "token".to_string(),
            token_expires_at: Utc::now(),
            traceparent: None,
            payload: Value::Null,
        }
    }

    #[test]
    fn task_callback_path_uses_envelope_task_id() {
        let envelope = envelope_fixture(Some("ct1_callback_id".to_string()));

        assert_eq!(
            task_callback_path(&envelope, "started"),
            "/api/v1/tasks/ct1_callback_id/started"
        );
    }

    #[test]
    fn task_callback_path_falls_back_to_task_key_for_legacy_envelopes() {
        let envelope = envelope_fixture(None);

        assert_eq!(
            task_callback_path(&envelope, "completed"),
            "/api/v1/tasks/pipeline.orders_smoke/completed"
        );
    }

    #[test]
    fn smoke_table_location_is_scoped_by_run_and_task() {
        let location = smoke_table_location("01JABCDEF1234567890", "analytics.orders");

        assert_eq!(
            location,
            "warehouse/pipeline_smoke/01jabcdef1234567890/analytics_orders"
        );
    }

    #[test]
    fn smoke_output_paths_include_data_and_initial_delta_log() {
        let (data_path, delta_log_path) =
            smoke_output_paths("01JABCDEF1234567890", "analytics.orders");

        assert_eq!(
            data_path,
            "warehouse/pipeline_smoke/01jabcdef1234567890/analytics_orders/part-00000-01jabcdef1234567.parquet"
        );
        assert_eq!(
            delta_log_path,
            "warehouse/pipeline_smoke/01jabcdef1234567890/analytics_orders/_delta_log/00000000000000000000.json"
        );
    }

    #[test]
    fn smoke_table_registration_body_uses_catalog_column_contract() {
        let body = smoke_table_registration_body(
            "orders",
            "warehouse/pipeline_smoke/run-1/pipeline_orders_smoke",
        );

        assert_eq!(body["name"], "orders");
        assert_eq!(body["format"], "delta");
        assert_eq!(body["columns"][0]["name"], "order_id");
        assert_eq!(body["columns"][0]["data_type"], "long");
        assert_eq!(body["columns"][0]["nullable"], false);
        assert!(body["columns"][0].get("type").is_none());
    }

    #[test]
    fn delta_stage_response_uses_api_snake_case_contract() {
        let staged: StageCommitResponse = serde_json::from_value(json!({
            "staged_path": "delta/_staging/commit.json",
            "staged_version": "abc123"
        }))
        .expect("stage response should parse");

        assert_eq!(staged.staged_path, "delta/_staging/commit.json");
        assert_eq!(staged.staged_version, "abc123");
    }

    #[test]
    fn delta_commit_request_body_uses_api_snake_case_contract() {
        let body = delta_commit_request_body(
            &StageCommitResponse {
                staged_path: "delta/_staging/commit.json".to_string(),
                staged_version: "abc123".to_string(),
            },
            -1,
        );

        assert_eq!(body["read_version"], -1);
        assert_eq!(body["staged_path"], "delta/_staging/commit.json");
        assert_eq!(body["staged_version"], "abc123");
        assert!(body.get("readVersion").is_none());
        assert!(body.get("stagedPath").is_none());
        assert!(body.get("stagedVersion").is_none());
    }

    #[test]
    fn delta_commit_response_uses_api_snake_case_contract() {
        let committed: CommitResponse = serde_json::from_value(json!({
            "version": 0,
            "delta_log_path": "warehouse/table/_delta_log/00000000000000000000.json"
        }))
        .expect("commit response should parse");

        assert_eq!(committed.version, 0);
        assert_eq!(
            committed.delta_log_path,
            "warehouse/table/_delta_log/00000000000000000000.json"
        );
    }

    #[test]
    fn initial_delta_payload_references_written_data_file() {
        let payload = build_initial_delta_payload(
            "table-1",
            "part-00000-01jabcdef1234567890.parquet",
            1234,
            3,
            1_789_000_000_000,
        );

        assert!(payload.contains(r#""protocol""#));
        assert!(payload.contains(r#""metaData""#));
        assert!(payload.contains(r#""add""#));
        assert!(payload.contains(r#""path":"part-00000-01jabcdef1234567890.parquet""#));
        let add_line = payload
            .lines()
            .find(|line| line.contains(r#""add""#))
            .expect("add line");
        let add: Value = serde_json::from_str(add_line).expect("parse add line");
        let stats: Value = serde_json::from_str(
            add["add"]["stats"]
                .as_str()
                .expect("stats should be encoded as a JSON string"),
        )
        .expect("parse stats");
        assert_eq!(stats["numRecords"], 3);
        assert!(payload.ends_with('\n'));
    }
}

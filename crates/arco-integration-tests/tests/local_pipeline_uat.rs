//! Local UAT proof for a realistic data-pipeline path.
//!
//! This test intentionally uses no cloud services. It exercises the same API,
//! catalog, Delta commit, object-storage, query, and orchestration callback
//! contracts that the Cloud Run smoke path uses, backed by in-memory storage.

#![allow(clippy::expect_used, clippy::too_many_lines, clippy::unwrap_used)]

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use bytes::Bytes;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tower::ServiceExt as _;
use uuid::Uuid;

use arco_api::server::ServerBuilder;
use arco_core::ScopedStorage;
use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::controllers::ReadyDispatchController;
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData};

const TENANT: &str = "test-tenant";
const WORKSPACE: &str = "test-workspace";
const CATALOG: &str = "analytics";
const SCHEMA_NAME: &str = "sales";
const PARQUET_TABLE: &str = "raw_orders";
const DELTA_TABLE: &str = "orders_delta";
const DELTA_LOCATION: &str = "warehouse/local_pipeline/orders_delta";
const DELTA_DATA_FILE: &str = "part-00000-local-uat.parquet";
const RAW_PARQUET_PATH: &str = "warehouse/local_pipeline/raw_orders.parquet";
const ASSET_KEY: &str = "pipeline.orders_smoke";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreatedTableResponse {
    id: String,
    format: String,
    location: Option<String>,
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeployManifestResponse {
    manifest_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TriggerRunResponse {
    run_id: String,
    state: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunResponse {
    run_id: String,
    state: String,
    tasks: Vec<TaskSummary>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskSummary {
    task_key: String,
    attempt: u32,
    state: String,
    delta_table: Option<String>,
    delta_version: Option<i64>,
    output_visibility_state: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskCallbackResponse {
    acknowledged: bool,
}

#[derive(Clone)]
struct TestApp {
    router: axum::Router,
    storage: ScopedStorage,
}

#[tokio::test]
async fn local_pipeline_uat_writes_delta_catalogs_queries_data_and_completes_run() {
    let app = TestApp::new();
    let rows = sample_rows();
    let parquet = build_orders_parquet(&rows);

    create_catalog_schema(&app).await;

    app.storage
        .put_raw(
            RAW_PARQUET_PATH,
            Bytes::from(parquet.clone()),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("write raw parquet data");

    let raw_table = register_table(
        &app,
        PARQUET_TABLE,
        "parquet",
        RAW_PARQUET_PATH,
        "Local UAT raw orders table",
    )
    .await;
    assert_eq!(raw_table.format, "parquet");
    assert_eq!(raw_table.location.as_deref(), Some(RAW_PARQUET_PATH));

    let raw_rows = query_json(
        &app,
        "/api/v1/query-data?format=json",
        "SELECT order_id, customer FROM analytics.sales.raw_orders WHERE order_id >= 1002 ORDER BY order_id",
    )
    .await;
    assert_eq!(raw_rows.len(), 2);
    assert_eq!(raw_rows[0]["order_id"], 1002);
    assert_eq!(raw_rows[1]["customer"], "local-west");

    let delta_table = register_table(
        &app,
        DELTA_TABLE,
        "delta",
        DELTA_LOCATION,
        "Local UAT Delta table",
    )
    .await;
    assert_eq!(delta_table.format, "delta");

    let delta_data_path = format!("{DELTA_LOCATION}/{DELTA_DATA_FILE}");
    app.storage
        .put_raw(
            &delta_data_path,
            Bytes::from(parquet),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("write Delta data parquet");

    let delta_payload = build_initial_delta_payload(
        &delta_table.id,
        DELTA_DATA_FILE,
        app.storage
            .head_raw(&delta_data_path)
            .await
            .expect("stat Delta data")
            .expect("Delta data exists")
            .size,
        rows.len() as u64,
    );
    let staged = stage_delta_commit(&app, &delta_table.id, &delta_payload).await;
    let committed = commit_delta(&app, &delta_table.id, staged).await;
    assert_eq!(committed.version, 0);
    assert_eq!(
        committed.delta_log_path,
        "warehouse/local_pipeline/orders_delta/_delta_log/00000000000000000000.json"
    );

    let delta_log = app
        .storage
        .get_raw(&committed.delta_log_path)
        .await
        .expect("read Delta log");
    let delta_log_text = String::from_utf8(delta_log.to_vec()).expect("Delta log UTF-8");
    assert!(delta_log_text.contains(r#""protocol""#));
    assert!(delta_log_text.contains(r#""metaData""#));
    assert!(delta_log_text.contains(r#""add""#));
    assert!(delta_log_text.contains(r#""operation":"WRITE""#));

    let catalog_rows = query_json(
        &app,
        "/api/v1/query?format=json",
        "SELECT name, format, location FROM system.catalog.tables WHERE name IN ('orders_delta', 'raw_orders') ORDER BY name",
    )
    .await;
    assert_eq!(catalog_rows.len(), 2);
    assert_eq!(catalog_rows[0]["name"], DELTA_TABLE);
    assert_eq!(catalog_rows[0]["format"], "delta");
    assert_eq!(catalog_rows[0]["location"], DELTA_LOCATION);
    assert_eq!(catalog_rows[1]["name"], PARQUET_TABLE);
    assert_eq!(catalog_rows[1]["format"], "parquet");

    let deploy = deploy_manifest(&app).await;
    assert!(!deploy.manifest_id.is_empty());
    let triggered = trigger_run(&app).await;
    assert_eq!(triggered.state, "PENDING");

    let run_path = format!("/api/v1/workspaces/{WORKSPACE}/runs/{}", triggered.run_id);
    let run: RunResponse = get_json(&app.router, &run_path, StatusCode::OK).await;
    assert_eq!(run.run_id, triggered.run_id);
    let task = run.tasks.first().expect("run task");
    assert_eq!(task.task_key, ASSET_KEY);
    assert_eq!(task.attempt, 0);

    let (attempt, attempt_id) = request_dispatch(&app, &task.task_key).await;
    post_task_started(&app, &task.task_key, attempt, &attempt_id).await;
    post_task_completed(
        &app,
        &task.task_key,
        attempt,
        &attempt_id,
        &format!("{CATALOG}.{SCHEMA_NAME}.{DELTA_TABLE}"),
        committed.version,
        &delta_data_path,
    )
    .await;
    compact_orchestration_ledger(&app).await;

    let finished: RunResponse = get_json(&app.router, &run_path, StatusCode::OK).await;
    assert_eq!(finished.state, "SUCCEEDED");
    let finished_task = finished
        .tasks
        .iter()
        .find(|task| task.task_key == ASSET_KEY)
        .expect("finished task");
    assert_eq!(finished_task.attempt, attempt);
    assert_eq!(finished_task.state, "SUCCEEDED");
    assert_eq!(
        finished_task.delta_table.as_deref(),
        Some("analytics.sales.orders_delta")
    );
    assert_eq!(finished_task.delta_version, Some(0));
    assert_eq!(
        finished_task.output_visibility_state.as_deref(),
        Some("VISIBLE")
    );
}

impl TestApp {
    fn new() -> Self {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let router = ServerBuilder::new()
            .debug(true)
            .storage_backend(backend.clone())
            .build()
            .test_router();
        let storage = ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage");
        Self { router, storage }
    }
}

async fn create_catalog_schema(app: &TestApp) {
    let _: Value = post_json(
        &app.router,
        "/api/v1/catalogs",
        json!({ "name": CATALOG }),
        &[],
        StatusCode::CREATED,
    )
    .await;
    let _: Value = post_json(
        &app.router,
        &format!("/api/v1/catalogs/{CATALOG}/schemas"),
        json!({ "name": SCHEMA_NAME }),
        &[],
        StatusCode::CREATED,
    )
    .await;
}

async fn register_table(
    app: &TestApp,
    name: &str,
    format: &str,
    location: &str,
    description: &str,
) -> CreatedTableResponse {
    post_json(
        &app.router,
        &format!("/api/v1/catalogs/{CATALOG}/schemas/{SCHEMA_NAME}/tables"),
        json!({
            "name": name,
            "format": format,
            "columns": [
                {"name": "order_id", "data_type": "long", "nullable": false},
                {"name": "customer", "data_type": "string", "nullable": false}
            ],
            "description": description,
            "location": location
        }),
        &[("Idempotency-Key", Uuid::now_v7().to_string().as_str())],
        StatusCode::CREATED,
    )
    .await
}

async fn stage_delta_commit(app: &TestApp, table_id: &str, payload: &str) -> StageCommitResponse {
    post_json(
        &app.router,
        &format!("/api/v1/delta/tables/{table_id}/commits/stage"),
        json!({ "payload": payload }),
        &[],
        StatusCode::OK,
    )
    .await
}

async fn commit_delta(
    app: &TestApp,
    table_id: &str,
    staged: StageCommitResponse,
) -> CommitResponse {
    post_json(
        &app.router,
        &format!("/api/v1/delta/tables/{table_id}/commits"),
        json!({
            "read_version": -1,
            "staged_path": staged.staged_path,
            "staged_version": staged.staged_version
        }),
        &[("Idempotency-Key", Uuid::now_v7().to_string().as_str())],
        StatusCode::OK,
    )
    .await
}

async fn deploy_manifest(app: &TestApp) -> DeployManifestResponse {
    post_json(
        &app.router,
        &format!("/api/v1/workspaces/{WORKSPACE}/manifests"),
        json!({
            "manifestVersion": "1.0",
            "codeVersionId": "local-pipeline-uat",
            "assets": [{
                "key": {"namespace": "pipeline", "name": "orders_smoke"},
                "id": Uuid::now_v7().to_string(),
                "description": "Local UAT data pipeline asset",
                "owners": ["platform"],
                "tags": {},
                "partitioning": {},
                "dependencies": [],
                "code": {"kind": "local_uat"},
                "checks": [],
                "execution": {"queue": "local"},
                "resources": {},
                "io": {"format": "delta"},
                "transformFingerprint": "local-pipeline-uat"
            }],
            "schedules": [],
            "deployedBy": "local-uat",
            "metadata": {"purpose": "local-pipeline-delta-catalog-proof"}
        }),
        &[("Idempotency-Key", Uuid::now_v7().to_string().as_str())],
        StatusCode::CREATED,
    )
    .await
}

async fn trigger_run(app: &TestApp) -> TriggerRunResponse {
    post_json(
        &app.router,
        &format!("/api/v1/workspaces/{WORKSPACE}/runs"),
        json!({
            "selection": [ASSET_KEY],
            "includeUpstream": false,
            "includeDownstream": false,
            "partitions": [],
            "runKey": format!("local-pipeline-uat:{}", Uuid::now_v7()),
            "labels": {"proof": "local_pipeline_delta_catalog"}
        }),
        &[],
        StatusCode::CREATED,
    )
    .await
}

async fn request_dispatch(app: &TestApp, task_key: &str) -> (u32, String) {
    let ledger = LedgerWriter::new(app.storage.clone());
    let compactor = MicroCompactor::new(app.storage.clone());
    let (manifest, fold_state) = compactor.load_state().await.expect("load flow state");
    let ready_controller = ReadyDispatchController::with_defaults();
    let ready_events: Vec<_> = ready_controller
        .reconcile(&manifest, &fold_state)
        .into_iter()
        .filter_map(|action| action.into_event_data())
        .map(|data| OrchestrationEvent::new(TENANT, WORKSPACE, data))
        .collect();

    let (attempt, attempt_id) = ready_events
        .iter()
        .find_map(|event| match &event.data {
            OrchestrationEventData::DispatchRequested {
                task_key: event_task_key,
                attempt,
                attempt_id,
                ..
            } if event_task_key == task_key => Some((*attempt, attempt_id.clone())),
            _ => None,
        })
        .expect("DispatchRequested event");

    let event_paths: Vec<_> = ready_events.iter().map(LedgerWriter::event_path).collect();
    ledger
        .append_all(ready_events)
        .await
        .expect("append dispatch");
    compactor
        .compact_events(event_paths)
        .await
        .expect("compact dispatch");
    (attempt, attempt_id)
}

async fn compact_orchestration_ledger(app: &TestApp) {
    let mut event_paths: Vec<_> = app
        .storage
        .list("ledger/orchestration")
        .await
        .expect("list orchestration ledger")
        .into_iter()
        .map(|path| path.as_str().to_string())
        .collect();
    event_paths.sort();
    MicroCompactor::new(app.storage.clone())
        .compact_events(event_paths)
        .await
        .expect("compact orchestration ledger");
}

async fn post_task_started(app: &TestApp, task_key: &str, attempt: u32, attempt_id: &str) {
    let response: TaskCallbackResponse = post_json(
        &app.router,
        &format!("/api/v1/tasks/{task_key}/started"),
        json!({
            "attempt": attempt,
            "attemptId": attempt_id,
            "workerId": "local-uat-worker",
            "startedAt": Utc::now()
        }),
        &[("Authorization", "Bearer local-uat-token")],
        StatusCode::OK,
    )
    .await;
    assert!(response.acknowledged);
}

async fn post_task_completed(
    app: &TestApp,
    task_key: &str,
    attempt: u32,
    attempt_id: &str,
    delta_table: &str,
    delta_version: i64,
    output_path: &str,
) {
    let response: TaskCallbackResponse = post_json(
        &app.router,
        &format!("/api/v1/tasks/{task_key}/completed"),
        json!({
            "attempt": attempt,
            "attemptId": attempt_id,
            "workerId": "local-uat-worker",
            "outcome": "SUCCEEDED",
            "completedAt": Utc::now(),
            "output": {
                "materializationId": format!("mat-local-uat-{delta_version}"),
                "rowCount": 3,
                "byteSize": null,
                "outputPath": output_path,
                "deltaTable": delta_table,
                "deltaVersion": delta_version,
                "deltaPartition": null,
                "outputVisibilityState": "VISIBLE",
                "publishedAt": Utc::now(),
                "publishError": null
            },
            "error": null,
            "metrics": {"ioWriteBytes": null},
            "cancelledDuringPhase": null,
            "partialProgress": null
        }),
        &[("Authorization", "Bearer local-uat-token")],
        StatusCode::OK,
    )
    .await;
    assert!(response.acknowledged);
}

async fn query_json(app: &TestApp, uri: &str, sql: &str) -> Vec<Value> {
    post_json(&app.router, uri, json!({ "sql": sql }), &[], StatusCode::OK).await
}

async fn post_json<T: DeserializeOwned>(
    router: &axum::Router,
    uri: &str,
    body: Value,
    headers: &[(&str, &str)],
    expected: StatusCode,
) -> T {
    let request = make_request(Method::POST, uri, Some(body), headers);
    send_json(router, request, expected).await
}

async fn get_json<T: DeserializeOwned>(
    router: &axum::Router,
    uri: &str,
    expected: StatusCode,
) -> T {
    let request = make_request(Method::GET, uri, None, &[]);
    send_json(router, request, expected).await
}

async fn send_json<T: DeserializeOwned>(
    router: &axum::Router,
    request: Request<Body>,
    expected: StatusCode,
) -> T {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(
        status,
        expected,
        "unexpected status body={}",
        String::from_utf8_lossy(&body)
    );
    serde_json::from_slice(&body).unwrap()
}

fn make_request(
    method: Method,
    uri: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .header(header::CONTENT_TYPE, "application/json");

    for (key, value) in headers {
        builder = builder.header(*key, *value);
    }

    let body = body.map_or_else(Body::empty, |value| {
        Body::from(serde_json::to_vec(&value).unwrap())
    });
    builder.body(body).unwrap()
}

fn build_orders_parquet(rows: &[OrderRow]) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer", DataType::Utf8, false),
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
        ],
    )
    .unwrap();

    let mut buffer = Vec::new();
    let props = WriterProperties::builder().build();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    buffer
}

fn build_initial_delta_payload(
    table_id: &str,
    data_file_name: &str,
    data_file_size: u64,
    row_count: u64,
) -> String {
    let timestamp_millis = Utc::now().timestamp_millis();
    let schema_string = json!({
        "type": "struct",
        "fields": [
            {"name": "order_id", "type": "long", "nullable": false, "metadata": {}},
            {"name": "customer", "type": "string", "nullable": false, "metadata": {}}
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
                "engineInfo": "arco local pipeline UAT"
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

#[derive(Debug)]
struct OrderRow {
    order_id: i64,
    customer: String,
}

fn sample_rows() -> Vec<OrderRow> {
    vec![
        OrderRow {
            order_id: 1001,
            customer: "local-north".to_string(),
        },
        OrderRow {
            order_id: 1002,
            customer: "local-south".to_string(),
        },
        OrderRow {
            order_id: 1003,
            customer: "local-west".to_string(),
        },
    ]
}

//! Hourly Usage publication over the production router and a real Flow completion.

#![allow(
    clippy::indexing_slicing,
    reason = "Wire assertions use fixed JSON field paths"
)]

use super::*;
use arco_core::{TaskTokenConfig, mint_task_token_for_attempt};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::accepted_plan::{ACCEPTED_PLAN_SHA256_LABEL, load_accepted_run_plan};
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use arco_flow::orchestration::flow_service::append_events_and_compact;
use arco_worker_contract::callback_task_id;

const HOUR: i64 = 1_800_000_000_000;
const USAGE: &str = "/api/v1/usage/hourly";
const STREAM: &str = "/api/2.1/unity-catalog/extensions/activity-events";
const PINS: &str = "/api/v1/event-streams/query-activity/pins";
const NATIVE_STREAM: &str = "/api/v1/event-streams/query-activity/events";
const PUBLISHER: &str = "usage_publisher";
const USAGE_READER: &str = "usage_reader";
const RECORDER: &str = "activity_recorder";
const TASK: &str = "analytics.surface_usage_hourly";
const TRANSFORM: &str = "95b6a9fab555dc8e0d535249da21f2083a5b746a3c16aab58b4db4540dece521";

#[tokio::test]
async fn production_usage_replaces_one_hour_and_keeps_the_original_receipt() {
    exercise_usage(Arc::new(MemoryBackend::new()), STORAGE_ROOT).await;
}

#[cfg(feature = "aws")]
#[tokio::test]
#[ignore = "requires fresh MinIO"]
async fn production_usage_owner_minio() {
    let root = std::env::var("ARCO_DELTA_PROOF_BUCKET").expect("a fresh MinIO bucket");
    let fixture = exercise_usage(arco_storage::from_bucket(&root).unwrap(), &root).await;
    if let Some(dir) = std::env::var_os("ARCO_DELTA_PROOF_EVIDENCE") {
        std::fs::write(
            Path::new(&dir).join("usage-pin-replay.json"),
            serde_json::to_vec(&fixture).unwrap(),
        )
        .unwrap();
    }
}

#[allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    reason = "The ordered acceptance scenario runs unchanged against memory and MinIO"
)]
async fn exercise_usage(backend: Arc<dyn StorageBackend>, root: &str) -> Value {
    let activity =
        event_streams::seed_activity_table(backend.clone(), root, WORKSPACE, RECORDER).await;
    let table_id = seed_usage_table(&activity.storage).await;
    let mut config = production_rs256_config(root);
    config.unity_catalog.activity_tables = serde_json::from_value(json!({
        (format!("{TENANT}/{WORKSPACE}")): activity.table_id,
    }))
    .unwrap();
    config.usage_bindings = serde_json::from_value(json!([{"tenant":TENANT,"workspace":WORKSPACE,
        "table_id":table_id,"transform_sha256":TRANSFORM}]))
    .unwrap();
    config.task_token = TaskTokenConfig {
        hs256_secret: "usage-local-fixture-token-signing-key-000000".into(),
        issuer: Some("flow".into()),
        audience: Some("worker".into()),
        ttl_seconds: 900,
    };
    let open = |config: Config| {
        ServerBuilder::new()
            .config(config)
            .storage_backend(backend.clone())
            .build()
            .test_router()
    };
    let mut router = open(config.clone());
    let reader = table_read_rs256_jwt(TENANT, WORKSPACE, USAGE_READER);
    let activity_reader = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let publisher = table_read_rs256_jwt(TENANT, WORKSPACE, PUBLISHER);
    let recorder = table_read_rs256_jwt(TENANT, WORKSPACE, RECORDER);
    let other = table_read_rs256_jwt(TENANT, "other", READER);
    assert_eq!(
        read_json(&router, USAGE, &reader).await,
        (
            StatusCode::OK,
            json!({
                "workspace_id":WORKSPACE,"published_version":0,"receipt":null,"rows":[],"next_offset":null
            })
        )
    );
    for slot in 1..=16 {
        let (hour, surface) = if slot == 3 {
            (HOUR + 3_600_000, "other")
        } else {
            (HOUR, "revenue")
        };
        let (status, receipt) = post_json(&router, STREAM, &recorder, json!({
            "schema_version":1,"source_event_id":Uuid::now_v7(),"occurred_at_ms":hour+slot,
            "stage":"execution","outcome":"succeeded","surface_id":surface,"release_id":"rel_revenue",
            "operation_id":format!("req_{slot}"),
        })).await;
        assert_eq!(status, StatusCode::OK, "{receipt}");
        assert_eq!(receipt["commit_version"], slot);
    }

    let request = verified_rollup(&router, &activity, &config, HOUR, 2, "revenue", 2).await;
    let (status, first) = post_json(&router, USAGE, &publisher, request.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["table_version"], 1);
    assert_eq!(first["source_delta_version"], 2);
    assert_eq!(
        post_json(&router, USAGE, &reader, request.clone()).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        read_json(&router, USAGE, &publisher).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        read_json(&router, STREAM, &reader).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        read_json(&router, USAGE, &activity_reader).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        read_json(&router, USAGE, &other).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        post_json(
            &router,
            USAGE,
            &publisher,
            json!({"run_id":request["run_id"], "attempt_id":"stale_attempt"})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );

    router = open(config.clone());
    assert_eq!(
        post_json(&router, USAGE, &publisher, request.clone()).await,
        (StatusCode::OK, first.clone())
    );
    let next_hour = HOUR + 3_600_000;
    let other_hour = verified_rollup(&router, &activity, &config, next_hour, 3, "other", 1).await;
    assert_eq!(
        post_json(&router, USAGE, &publisher, other_hour).await.1["table_version"],
        2
    );
    let pin_id = issue_stream_pin(&router, &activity_reader, &recorder, &other).await;
    let update = verify_usage_run(
        &router,
        &activity,
        &config,
        json!({"streamPinId":pin_id,"hourStartMs":HOUR,"transformSha256":TRANSFORM}),
        None,
    )
    .await;
    let (status, replacement) = post_json(&router, USAGE, &publisher, update.clone()).await;
    assert_eq!(status, StatusCode::OK, "{replacement}");
    assert_eq!(replacement["table_version"], 3);
    assert_eq!(replacement["source_delta_version"], 16);
    router = open(config.clone());
    assert_eq!(
        post_json(&router, USAGE, &publisher, update.clone()).await,
        (StatusCode::OK, replacement.clone())
    );

    let stale = verified_rollup(&router, &activity, &config, HOUR, 1, "revenue", 1).await;
    let (code, body) = post_json(&router, USAGE, &publisher, stale).await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(body["error"]["message"], "usage_source_version_stale");
    let different = verified_rollup(&router, &activity, &config, HOUR, 16, "revenue", 9).await;
    let (code, body) = post_json(&router, USAGE, &publisher, different).await;
    assert_eq!(code, StatusCode::CONFLICT);
    assert_eq!(body["error"]["message"], "usage_effect_conflict");

    let (status, page) =
        read_json(&router, &format!("{USAGE}?hour_start_ms={HOUR}"), &reader).await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["published_version"], 3);
    assert_eq!(page["receipt"], replacement);
    assert_eq!(page["rows"][0]["executed"], 15);
    let older = read_json(
        &router,
        &format!("{USAGE}?hour_start_ms={HOUR}&published_version=1"),
        &reader,
    )
    .await;
    assert_eq!(older.0, StatusCode::OK, "{}", older.1);
    assert_eq!(older.1["rows"][0]["executed"], 2);
    let latest_hour = read_json(&router, USAGE, &reader).await;
    assert_eq!(latest_hour.1["receipt"]["hour_start_ms"], next_hour);
    assert_eq!(latest_hour.1["rows"][0]["surface_id"], "other");
    let run = request["run_id"].as_str().unwrap();
    backend
        .delete(&format!(
            "tenant={TENANT}/workspace={WORKSPACE}/outputs/usage/{run}.parquet"
        ))
        .await
        .unwrap();
    assert_eq!(
        read_json(
            &router,
            &format!("{USAGE}/receipt?hour_start_ms={HOUR}&source_delta_version=2"),
            &reader
        )
        .await,
        (StatusCode::OK, first.clone())
    );
    assert_eq!(
        post_json(&router, USAGE, &publisher, request).await,
        (StatusCode::OK, first)
    );

    let paths = DeltaPaths::legacy(table_id.parse().unwrap());
    let delta = activity
        .storage
        .get_raw(&paths.delta_log_json(3).unwrap())
        .await
        .unwrap();
    let writes = std::str::from_utf8(&delta)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        writes
            .iter()
            .filter(|action| action.get("remove").is_some())
            .count(),
        1
    );
    assert_eq!(
        writes
            .iter()
            .filter(|action| action.get("add").is_some())
            .count(),
        1
    );

    config.usage_bindings = arco_uc::UsageBindings::default();
    router = open(config);
    let raw = format!("/api/v1/delta/tables/{table_id}/commits/stage");
    assert_eq!(
        post_json(&router, &raw, &publisher, json!({"payload":"{}"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let uc_raw = "/api/2.1/unity-catalog/delta/preview/commits";
    assert_eq!(post_json(&router, uc_raw, &publisher, json!({
        "table_id":table_id, "table_uri":format!("{root}/tenant={TENANT}/workspace={WORKSPACE}/{}",paths.table_root()),
        "latest_backfilled_version":3
    })).await.0, StatusCode::FORBIDDEN);
    json!({"tenant":TENANT,"workspace":WORKSPACE,"sourceTableId":activity.table_id,
        "usageTableId":table_id,"pinId":pin_id,"transformSha256":TRANSFORM,
        "publishRequest":update,"receipt":replacement})
}

async fn issue_stream_pin(router: &Router, reader: &str, recorder: &str, other: &str) -> String {
    let (status, pin) = post_json(router, PINS, reader, json!({})).await;
    assert_eq!(status, StatusCode::OK, "{pin}");
    assert_eq!(pin["publishedVersion"], 16);
    let id = pin["pinId"].as_str().unwrap();
    assert_eq!(
        post_json(router, PINS, recorder, json!({})).await.0,
        StatusCode::FORBIDDEN
    );
    let (status, receipt) = post_json(
        router,
        STREAM,
        recorder,
        json!({
            "schema_version":1,"source_event_id":Uuid::now_v7(),"occurred_at_ms":HOUR+17,
            "stage":"execution","outcome":"succeeded","surface_id":"revenue",
            "release_id":"rel_revenue","operation_id":"req_17",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{receipt}");
    assert_eq!(receipt["commit_version"], 17);
    assert_eq!(
        post_json(router, PINS, reader, json!({"publishedVersion":16})).await,
        (StatusCode::OK, pin.clone())
    );
    let path = format!("{NATIVE_STREAM}?pinId={id}&limit=8");
    assert_eq!(
        read_json(router, &path, other).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        read_json(router, &path, recorder).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        read_json(router, &format!("{path}&publishedVersion=16"), reader)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        read_json(
            router,
            &format!("{NATIVE_STREAM}?pinId={}", "0".repeat(64)),
            reader
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post_json(router, PINS, reader, json!({"publishedVersion":18}))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    id.to_string()
}

async fn materialize_pinned_hour(
    router: &Router,
    source: &TableReadFixture,
    run_id: &str,
    plan_id: &str,
) -> Vec<u8> {
    let (_, fold) = MicroCompactor::new(source.storage.clone())
        .load_state()
        .await
        .unwrap();
    let hash = &fold.runs[run_id].labels[ACCEPTED_PLAN_SHA256_LABEL];
    let plan = load_accepted_run_plan(&source.storage, run_id, plan_id, hash)
        .await
        .unwrap();
    let payload = plan.task_payloads[TASK]
        .pointer("/asset/execution/payload")
        .unwrap();
    let pin_id = payload["streamPinId"].as_str().unwrap();
    let hour = payload["hourStartMs"].as_i64().unwrap();
    let reader = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let path = format!("{NATIVE_STREAM}?pinId={pin_id}&limit=8");
    let (status, first) = read_json(router, &path, &reader).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["publishedVersion"], 16);
    let (status, second) = read_json(
        router,
        &format!("{path}&beforeVersion={}", first["nextBeforeVersion"]),
        &reader,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second["publishedVersion"], 16);
    assert!(second["nextBeforeVersion"].is_null());
    let rows = [first, second];
    let events = rows
        .iter()
        .flat_map(|page| page["events"].as_array().unwrap());
    let versions = events
        .clone()
        .map(|row| row["commitVersion"].as_i64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(versions, (1..=16).rev().collect::<Vec<_>>());
    let executions = events
        .filter(|row| {
            let event = &row["event"];
            (hour..hour + 3_600_000).contains(&event["occurredAtMs"].as_i64().unwrap())
                && event["fields"]["stage"] == "execution"
                && event["fields"]["outcome"] == "succeeded"
        })
        .map(|row| row["event"]["fields"]["operation_id"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(executions.len(), 15);
    usage_parquet_fixture(
        hour,
        16,
        "revenue",
        i64::try_from(executions.len()).unwrap(),
    )
}

const COLUMNS: [(&str, DataType, bool); 11] = [
    ("workspace_id", DataType::Utf8, false),
    ("hour_start_ms", DataType::Int64, false),
    ("surface_id", DataType::Utf8, false),
    ("release_id", DataType::Utf8, true),
    ("source_delta_version", DataType::Int64, false),
    ("admitted", DataType::Int64, false),
    ("admission_denied", DataType::Int64, false),
    ("executed", DataType::Int64, false),
    ("execution_failed", DataType::Int64, false),
    ("delivery_authorized", DataType::Int64, false),
    ("delivery_denied", DataType::Int64, false),
];

async fn seed_usage_table(storage: &ScopedStorage) -> String {
    let writer = CatalogWriter::new(storage.clone())
        .with_sync_compactor(Arc::new(Tier1Compactor::new(storage.clone())));
    let columns = COLUMNS
        .into_iter()
        .enumerate()
        .map(|(ordinal, (name, kind, nullable))| ColumnDefinition {
            name: name.into(),
            data_type: if kind == DataType::Utf8 {
                "string".into()
            } else {
                "long".into()
            },
            ordinal: i32::try_from(ordinal).unwrap(),
            is_nullable: nullable,
            description: None,
        })
        .collect();
    let table = writer
        .register_table_in_schema(
            "default",
            "analytics",
            RegisterTableInSchemaRequest {
                name: "surface_usage_hourly".into(),
                description: None,
                location: None,
                format: Some("delta".into()),
                table_type: Some("MANAGED".into()),
                properties: None,
                columns,
            },
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let scope = ControlPlaneScope::workspace_alias(TENANT, WORKSPACE).unwrap();
    let ledger = MetastoreLedger::new(storage.clone()).unwrap();
    let seq = next_fixture_sequence(&ledger).await;
    let at = 1_800_000_010_000;
    let grant = |id: &str, principal: &str, privilege: &str| {
        MetastoreMutation::GrantUpserted(GrantRecord {
            grant_id: id.into(),
            object_id: table.id.clone(),
            object_type: "TABLE".into(),
            principal_id: principal.into(),
            privilege: privilege.into(),
            owner: "owner".into(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: at,
            properties: BTreeMap::new(),
            identity_cut: None,
        })
    };
    let principal = |id: &str| {
        MetastoreMutation::PrincipalUpserted(PrincipalRecord {
            principal_id: id.into(),
            name: id.into(),
            principal_kind: PrincipalKind::ServicePrincipal,
            owner: "owner".into(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: at,
            properties: BTreeMap::new(),
        })
    };
    let records = [
        (
            "usage_table",
            MetastoreMutation::CatalogObjectUpserted(CatalogObjectRecord {
                object_id: table.id.clone(),
                object_type: "TABLE".into(),
                qualified_name: "analytics.surface_usage_hourly".into(),
                owner: "owner".into(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: at,
                properties: BTreeMap::new(),
            }),
        ),
        ("usage_publisher", principal(PUBLISHER)),
        ("usage_reader", principal(USAGE_READER)),
        (
            "usage_select",
            grant("usage_select", USAGE_READER, "SELECT"),
        ),
        ("usage_modify", grant("usage_modify", PUBLISHER, "MODIFY")),
    ];
    for (offset, (name, record)) in records.into_iter().enumerate() {
        let event = MetastoreEvent::new_scoped(&scope, name, seq + offset as u64, record);
        ledger.append_event(&event).await.unwrap();
    }
    publish_current_metastore_projection(storage, &ProjectionRegistry::default())
        .await
        .unwrap();
    table.id
}

async fn verified_rollup(
    router: &Router,
    source: &TableReadFixture,
    config: &Config,
    hour: i64,
    version: i64,
    surface: &str,
    executions: i64,
) -> Value {
    let pin = json!({"inputIdentity":{"tenant":TENANT,"workspace":WORKSPACE,"table_id":source.table_id,
        "delta_version":version,"stream_id":"query-activity"}, "inputSha256":"a".repeat(64),
        "hourStartMs":hour,"transformSha256":TRANSFORM});
    verify_usage_run(
        router,
        source,
        config,
        pin,
        Some(usage_parquet_fixture(hour, version, surface, executions)),
    )
    .await
}

async fn verify_usage_run(
    router: &Router,
    source: &TableReadFixture,
    config: &Config,
    pin: Value,
    data: Option<Vec<u8>>,
) -> Value {
    let publisher = table_read_rs256_jwt(TENANT, WORKSPACE, PUBLISHER);
    let workspace = format!("/api/v1/workspaces/{WORKSPACE}");
    let code_version = Uuid::now_v7().to_string();
    let (status, manifest) = post_json(router, &format!("{workspace}/manifests"), &publisher, json!({
        "manifestVersion":"1.0","codeVersionId":code_version,
        "assets":[{"key":{"namespace":"analytics","name":"surface_usage_hourly"},"id":"usage_asset",
            "code":{"entrypoint":"usage_rollup"},"execution":{"payload":pin}}]
    })).await;
    assert_eq!(status, StatusCode::CREATED, "{manifest}");
    let (status, run) = post_json(
        router,
        &format!("{workspace}/runs"),
        &publisher,
        json!({"selection":[TASK],"runKey":format!("usage:{code_version}")}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{run}");
    let run_id = run["runId"].as_str().unwrap();
    let attempt_id = format!("attempt_{code_version}");
    let dispatch = OrchestrationEvent::new(
        TENANT,
        WORKSPACE,
        OrchestrationEventData::DispatchRequested {
            run_id: run_id.into(),
            task_key: TASK.into(),
            attempt: 1,
            attempt_id: attempt_id.clone(),
            worker_queue: "default-queue".into(),
            dispatch_id: format!("dispatch_{code_version}"),
        },
    );
    append_events_and_compact(
        &LedgerWriter::new(source.storage.clone()),
        None,
        vec![dispatch],
    )
    .await
    .unwrap();
    let task_id = callback_task_id(run_id, TASK);
    let worker = mint_task_token_for_attempt(
        &config.task_token,
        &task_id,
        TENANT,
        WORKSPACE,
        run_id,
        1,
        &attempt_id,
        chrono::Utc::now(),
    )
    .unwrap()
    .token;
    let callback = format!("/api/v1/tasks/{task_id}");
    let (code, body) = post_json(
        router,
        &format!("{callback}/started"),
        &worker,
        json!({"attempt":1,"attemptId":attempt_id,"workerId":"usage-worker"}),
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    let req = json!({"run_id":run_id, "attempt_id":attempt_id});
    assert_eq!(
        post_json(router, USAGE, &publisher, req.clone()).await.0,
        StatusCode::CONFLICT
    );

    let data = match data {
        Some(fixture) => fixture,
        None => {
            materialize_pinned_hour(router, source, run_id, run["planId"].as_str().unwrap()).await
        }
    };
    let size = data.len();
    let hash = hex::encode(Sha256::digest(&data));
    let path = format!("outputs/usage/{run_id}.parquet");
    source
        .storage
        .put_raw(&path, Bytes::from(data), WritePrecondition::DoesNotExist)
        .await
        .unwrap();
    let meta = source.storage.head_raw(&path).await.unwrap().unwrap();
    let completion = json!({"attempt":1,"attemptId":attempt_id,"workerId":"usage-worker",
        "outcome":"SUCCEEDED","output":{"publication":{"version":1,
            "manifestId":format!("sha256:{hash}"),"objectPath":path,"objectVersion":meta.version,
            "checksumSha256":hash,"byteSize":size,"format":"parquet","schemaRef":format!("sha256:{hash}#parquet-schema")}}});
    let (code, body) = post_json(
        router,
        &format!("{callback}/completed"),
        &worker,
        completion,
    )
    .await;
    assert_eq!(code, StatusCode::OK, "{body}");
    req
}

fn usage_parquet_fixture(hour: i64, version: i64, surface: &str, executions: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(
        COLUMNS
            .map(|(name, kind, nullable)| Field::new(name, kind, nullable))
            .to_vec(),
    ));
    let str_array = |value: &str| -> ArrayRef { Arc::new(StringArray::from(vec![value])) };
    let int_array = |value| -> ArrayRef { Arc::new(Int64Array::from(vec![value])) };
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            str_array(WORKSPACE),
            int_array(hour),
            str_array(surface),
            str_array("rel_revenue"),
            int_array(version),
            int_array(0),
            int_array(0),
            int_array(executions),
            int_array(0),
            int_array(0),
            int_array(0),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.into_inner().unwrap()
}

async fn read_json(router: &Router, path: &str, token: &str) -> (StatusCode, Value) {
    let req = Request::get(path)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(req).await.unwrap();
    let status = response.status();
    (
        status,
        serde_json::from_slice(&to_bytes(response.into_body(), 65_536).await.unwrap()).unwrap(),
    )
}

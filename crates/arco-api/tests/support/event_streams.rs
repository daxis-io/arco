//! Production-router acceptance for Workspace event streams and the Logfood adapter.

#![allow(
    clippy::indexing_slicing,
    reason = "Wire-shape assertions inspect and modify explicit JSON fixtures"
)]

use super::*;
use arco_catalog::Table;

#[tokio::test]
async fn production_activity_owner_commits_and_replays_typed_events() {
    exercise_activity_owner(Arc::new(MemoryBackend::new()), STORAGE_ROOT).await;
}

#[cfg(feature = "aws")]
#[tokio::test]
#[ignore = "requires fresh MinIO"]
async fn production_activity_owner_minio() {
    let bucket = std::env::var("ARCO_DELTA_PROOF_BUCKET").expect("fresh MinIO s3:// bucket");
    let backend = arco_storage::from_bucket(&bucket).expect("real S3 adapter");
    exercise_activity_owner(backend, &bucket).await;
}

#[cfg(feature = "aws")]
#[tokio::test]
#[ignore = "requires a copy of the pre-event-stream Logfood MinIO fixture"]
async fn production_event_stream_replays_legacy_logfood_minio() {
    let bucket = std::env::var("ARCO_DELTA_PROOF_BUCKET").unwrap();
    let table_id = std::env::var("ARCO_LEGACY_ACTIVITY_TABLE_ID").unwrap();
    let expected_id = std::env::var("ARCO_LEGACY_ACTIVITY_EVENT_ID").unwrap();
    let mut config = production_rs256_config(&bucket);
    config.unity_catalog.activity_tables = serde_json::from_value(json!({
        (format!("{TENANT}/bravo")): table_id,
    }))
    .unwrap();
    let router = ServerBuilder::new()
        .config(config)
        .storage_backend(arco_storage::from_bucket(&bucket).unwrap())
        .build()
        .test_router();
    let reader = table_read_rs256_jwt(TENANT, "bravo", READER);
    let recorder = table_read_rs256_jwt(TENANT, "bravo", "activity_recorder");
    let legacy = "/api/2.1/unity-catalog/extensions/activity-events";
    let stream = "/api/v1/event-streams/query-activity/events";
    let (status, page) = get_activity(&router, stream, &reader).await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["publishedVersion"], 1);
    let event = &page["events"][0]["event"];
    assert_eq!(event["sourceEventId"], expected_id);
    let (status, legacy_page) = get_activity(&router, legacy, &reader).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page, native_page(&legacy_page));

    let (status, receipt) = post_json(&router, stream, &recorder, event.clone()).await;
    assert_eq!(status, StatusCode::OK, "{receipt}");
    assert_eq!(
        receipt,
        json!({"sourceEventId": expected_id, "commitVersion": 1})
    );
    assert_eq!(
        get_activity(&router, stream, &reader).await,
        (StatusCode::OK, page)
    );
}

#[allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    reason = "The ordered acceptance scenario runs unchanged against memory and MinIO"
)]
async fn exercise_activity_owner(backend: Arc<dyn StorageBackend>, storage_root: &str) {
    const PATH: &str = "/api/2.1/unity-catalog/extensions/activity-events";
    const STREAM: &str = "/api/v1/event-streams/query-activity/events";
    const RECORDER: &str = "activity_recorder";
    const INPUT_SENTINEL: &str = "distinct customer input 4191";
    let acme = seed_activity_table(Arc::clone(&backend), storage_root, "acme", RECORDER).await;
    let bravo = seed_activity_table(Arc::clone(&backend), storage_root, "bravo", RECORDER).await;
    let syncs = seed_sync_table(&acme.storage, "sync_events").await;
    let mut config = production_rs256_config(storage_root);
    config.unity_catalog.activity_tables = serde_json::from_value(json!({
        (format!("{TENANT}/acme")): acme.table_id,
        (format!("{TENANT}/bravo")): bravo.table_id,
    }))
    .unwrap();
    config.event_streams = serde_json::from_value(json!([sync_binding(&syncs.id)])).unwrap();
    let router = |config: &Config| {
        ServerBuilder::new()
            .config(config.clone())
            .storage_backend(Arc::clone(&backend))
            .build()
            .test_router()
    };
    let initial = router(&config);
    let recorder = table_read_rs256_jwt(TENANT, "acme", RECORDER);
    let reader = table_read_rs256_jwt(TENANT, "acme", READER);
    let sibling_reader = table_read_rs256_jwt(TENANT, "bravo", READER);
    let sibling_recorder = table_read_rs256_jwt(TENANT, "bravo", RECORDER);
    let mut event = json!({
        "schema_version": 1,
        "source_event_id": Uuid::now_v7(),
        "occurred_at_ms": 1_800_000_000_000_i64,
        "stage": "admission",
        "outcome": "admitted",
        "surface_id": "revenue",
        "release_id": "rel_revenue",
        "operation_id": "invocation_1"
    });

    let (status, receipt) = post_json(&initial, PATH, &recorder, event.clone()).await;
    assert_eq!(status, StatusCode::OK, "{receipt}");
    assert_eq!(receipt["source_event_id"], event["source_event_id"]);
    assert_eq!(receipt["commit_version"], 1);
    assert_eq!(
        post_json(&initial, STREAM, &recorder, event.clone())
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post_json(&initial, STREAM, &recorder, native_event(&event)).await,
        (
            StatusCode::OK,
            json!({"sourceEventId": receipt["source_event_id"], "commitVersion": 1})
        )
    );
    assert_eq!(
        get_activity(&initial, STREAM, &reader).await,
        (
            StatusCode::OK,
            native_page(&get_activity(&initial, PATH, &reader).await.1)
        )
    );
    exercise_configured_stream(&initial, &syncs, &event["source_event_id"], &recorder).await;
    let mut native_only = config.clone();
    native_only.unity_catalog.enabled = false;
    assert_eq!(
        get_activity(&router(&native_only), STREAM, &reader).await,
        get_activity(&initial, STREAM, &reader).await
    );

    let next_table = seed_sync_table(&acme.storage, "replacement_sync_events").await;
    let mut rebound = config.clone();
    rebound.event_streams = serde_json::from_value(json!([sync_binding(&next_table.id)])).unwrap();
    let rebound_router = router(&rebound);
    let (status, error) = get_activity(&rebound_router, SYNC_STREAM, &reader).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        error,
        json!({"code": "CONFLICT", "message": "event_stream_binding_mismatch"})
    );
    let raw = format!("/api/v1/delta/tables/{}/commits/stage", syncs.id);
    assert_eq!(
        post_json(&rebound_router, &raw, &recorder, json!({"payload":"{}"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        get_activity(&initial, PATH, &recorder).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        post_json(&initial, PATH, &reader, event.clone()).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        get_activity(&initial, PATH, &sibling_reader).await,
        (
            StatusCode::OK,
            json!({
                "events": [], "published_version": 0, "next_before_version": null,
            })
        )
    );

    let mut bravo_event = event.clone();
    bravo_event["surface_id"] = json!("bravo_sales");
    assert_eq!(
        post_json(&initial, PATH, &sibling_recorder, bravo_event)
            .await
            .0,
        StatusCode::OK
    );
    let (status, sibling_page) = get_activity(&initial, PATH, &sibling_reader).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(sibling_page["events"].as_array().unwrap().len(), 1);
    assert_eq!(sibling_page["events"][0]["workspace_id"], "bravo");
    assert_eq!(sibling_page["events"][0]["surface_id"], "bravo_sales");

    let mut execution = event.clone();
    execution["source_event_id"] = json!(Uuid::now_v7());
    execution["stage"] = json!("execution");
    execution["outcome"] = json!("succeeded");
    assert_eq!(
        post_json(&initial, PATH, &recorder, execution.clone())
            .await
            .0,
        StatusCode::OK
    );

    let current = router(&config);
    assert_eq!(
        post_json(&current, PATH, &recorder, event.clone()).await,
        (StatusCode::OK, receipt)
    );
    let mut changed = event.clone();
    changed["outcome"] = json!("denied");
    assert_eq!(
        post_json(&current, PATH, &recorder, changed).await.0,
        StatusCode::CONFLICT
    );

    let mut delivery = event.clone();
    delivery["source_event_id"] = json!(Uuid::now_v7());
    delivery["stage"] = json!("delivery");
    delivery["outcome"] = json!("denied");
    assert_eq!(
        post_json(&current, PATH, &recorder, delivery.clone())
            .await
            .0,
        StatusCode::OK
    );

    let (status, first) = get_activity(&current, &format!("{PATH}?limit=2"), &reader).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["published_version"], 3);
    assert_eq!(first["next_before_version"], 2);
    assert_eq!(
        first["events"][0]["source_event_id"],
        delivery["source_event_id"]
    );
    assert_eq!(first["events"][0]["outcome"], "denied");
    assert_eq!(
        first["events"][1]["source_event_id"],
        execution["source_event_id"]
    );
    assert_eq!(first["events"][1]["outcome"], "succeeded");
    assert_eq!(first["events"][1]["workspace_id"], "acme");

    let (status, next) = get_activity(
        &current,
        &format!("{PATH}?before_version=2&published_version=3"),
        &reader,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{next}");
    assert_eq!(next["events"].as_array().unwrap().len(), 1);
    assert_eq!(
        next["events"][0]["source_event_id"],
        event["source_event_id"]
    );
    assert!(next["next_before_version"].is_null());
    assert_eq!(
        get_activity(
            &current,
            &format!("{STREAM}?beforeVersion=2&publishedVersion=3"),
            &reader
        )
        .await,
        (StatusCode::OK, native_page(&next))
    );

    let paths = DeltaPaths::legacy(Uuid::parse_str(&acme.table_id).unwrap());
    let log = paths.delta_log_json(1).unwrap();
    let original = acme.storage.get_raw(&log).await.unwrap();
    let damaged = std::str::from_utf8(&original)
        .unwrap()
        .replace("invocation_1", "invocation_2");
    acme.storage
        .put_raw(&log, Bytes::from(damaged), WritePrecondition::None)
        .await
        .unwrap();
    assert_eq!(
        get_activity(&current, PATH, &reader).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        post_json(&current, PATH, &recorder, event.clone()).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    acme.storage
        .put_raw(&log, original, WritePrecondition::None)
        .await
        .unwrap();
    let filename = format!(
        "{}/part-{}.parquet",
        paths.table_root(),
        event["source_event_id"].as_str().unwrap()
    );
    let parquet = acme.storage.get_raw(&filename).await.unwrap();
    acme.storage
        .put_raw(
            &filename,
            Bytes::from_static(b"replaced object"),
            WritePrecondition::None,
        )
        .await
        .unwrap();
    let (status, _) = get_activity(&current, PATH, &reader).await;
    assert_eq!(status, StatusCode::CONFLICT);
    acme.storage
        .put_raw(&filename, parquet.clone(), WritePrecondition::None)
        .await
        .unwrap();
    let mut reader_batch =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(parquet)
            .unwrap()
            .build()
            .unwrap();
    let batch = reader_batch.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let workspace_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(workspace_col.value(0), "acme");
    assert!(reader_batch.next().is_none());

    let descriptor = extension_path("table-descriptors", &acme.table_id);
    let (status, snapshot) =
        post_json(&current, &descriptor, &reader, json!({"delta_version":3})).await;
    assert_eq!(status, StatusCode::OK, "{snapshot}");
    assert_eq!(snapshot["input_identity"]["delta_version"], 3);

    let rejected_id = Uuid::now_v7();
    event["source_event_id"] = json!(rejected_id);
    event["input"] = json!(INPUT_SENTINEL);
    let (status, rejected) = post_json(&current, PATH, &recorder, event).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(!rejected.to_string().contains(INPUT_SENTINEL));
    let rejected_file = format!("{}/part-{rejected_id}.parquet", paths.table_root());
    assert!(
        acme.storage
            .head_raw(&rejected_file)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        get_activity(&current, PATH, &reader).await.1["published_version"],
        3
    );

    let raw = format!("/api/v1/delta/tables/{}/commits/stage", acme.table_id);
    assert_eq!(
        post_json(&current, &raw, &recorder, json!({"payload": "{}"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );

    let preview = "/api/2.1/unity-catalog/delta/preview/commits";
    let (status, _) = post_json(&current, preview, &recorder, json!({
        "table_id": acme.table_id, "table_uri": paths.table_root(), "latest_backfilled_version": 3,
    })).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    revoke_fixture_select(&acme, READER).await;
    assert_eq!(
        get_activity(&current, PATH, &reader).await.0,
        StatusCode::FORBIDDEN
    );
}

const SYNC_STREAM: &str = "/api/v1/event-streams/integration-syncs/events";
const SYNC_RECORDER: &str = "sync_recorder";

#[test]
fn event_stream_config_rejects_ambiguous_bindings_and_reserved_fields() {
    let first = sync_binding(&Uuid::now_v7().to_string());
    let mut collision = first.clone();
    collision["stream_id"] = json!("syncs-alias");
    assert!(
        serde_json::from_value::<arco_uc::EventStreamBindings>(json!([first, collision])).is_err()
    );
    let mut collision = first.clone();
    collision["table_id"] = json!(Uuid::now_v7());
    assert!(
        serde_json::from_value::<arco_uc::EventStreamBindings>(json!([first, collision])).is_err()
    );
    let mut reserved = first;
    reserved["schema"]["fields"][0]["name"] = json!("workspace_id");
    assert!(serde_json::from_value::<arco_uc::EventStreamBindings>(json!([reserved])).is_err());
}

fn sync_binding(table_id: &str) -> Value {
    json!({
        "tenant": TENANT, "workspace": "acme", "stream_id": "integration-syncs", "table_id": table_id,
        "schema": {"profile": "custom", "version": 3, "fields": [
            {"name": "connector_id", "type": "identifier"},
            {"name": "outcome", "type": "enum", "values": ["succeeded", "failed"]},
            {"name": "records_written", "type": "integer"},
            {"name": "full_refresh", "type": "boolean", "nullable": true},
        ]}
    })
}

#[allow(
    clippy::cognitive_complexity,
    reason = "Status assertions pin the stream contract"
)]
async fn exercise_configured_stream(
    router: &Router,
    table: &Table,
    shared_id: &Value,
    activity_recorder: &str,
) {
    let recorder = table_read_rs256_jwt(TENANT, "acme", SYNC_RECORDER);
    let reader = table_read_rs256_jwt(TENANT, "acme", READER);
    let sibling = table_read_rs256_jwt(TENANT, "bravo", READER);
    let event = json!({
        "schemaVersion": 3, "sourceEventId": shared_id, "occurredAtMs": 1_800_000_000_000_i64,
        "fields": {"connector_id": "crm", "outcome": "succeeded", "records_written": 3, "full_refresh": true},
    });

    assert_eq!(
        post_json(router, SYNC_STREAM, activity_recorder, event.clone())
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        get_activity(router, SYNC_STREAM, &sibling).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        get_activity(router, SYNC_STREAM, &recorder).await.0,
        StatusCode::FORBIDDEN
    );
    let (status, error) = get_activity(router, SYNC_STREAM, "invalid-token").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(error["code"], "INVALID_TOKEN");

    let (status, receipt) = post_json(router, SYNC_STREAM, &recorder, event.clone()).await;
    assert_eq!(status, StatusCode::OK, "{receipt}");
    assert_eq!(
        receipt,
        json!({"sourceEventId": shared_id, "commitVersion": 1})
    );
    assert_eq!(
        post_json(router, SYNC_STREAM, &recorder, event.clone()).await,
        (StatusCode::OK, receipt)
    );

    let mut changed = event.clone();
    changed["fields"]["records_written"] = json!(4);
    assert_eq!(
        post_json(router, SYNC_STREAM, &recorder, changed).await.0,
        StatusCode::CONFLICT
    );
    for (field, invalid) in [
        ("input", json!("recognizable customer data")),
        ("outcome", json!("unknown")),
        ("records_written", json!("3")),
        ("workspace_id", json!("bravo")),
        ("full_refresh", json!("true")),
    ] {
        let mut bad = event.clone();
        bad["fields"][field] = invalid;
        let (status, response) = post_json(router, SYNC_STREAM, &recorder, bad).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{field}");
        assert!(!response.to_string().contains("recognizable customer data"));
    }
    let mut wrong_version = event.clone();
    wrong_version["schemaVersion"] = json!(2);
    assert_eq!(
        post_json(router, SYNC_STREAM, &recorder, wrong_version)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );

    let serialized = event.to_string();
    let duplicate = Request::post(SYNC_STREAM)
        .header(header::AUTHORIZATION, format!("Bearer {recorder}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serialized.replacen(
            "\"fields\":{",
            "\"fields\":{\"outcome\":\"failed\",",
            1,
        )))
        .unwrap();
    assert_eq!(
        router.clone().oneshot(duplicate).await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );

    let projection = json!({"workspaceId": "acme", "commitVersion": 1, "event": event});
    assert_eq!(
        get_activity(router, SYNC_STREAM, &reader).await,
        (
            StatusCode::OK,
            json!({
                "events": [projection], "publishedVersion": 1, "nextBeforeVersion": null,
            })
        )
    );

    let descriptor = extension_path("table-descriptors", &table.id);
    let (status, snapshot) =
        post_json(router, &descriptor, &reader, json!({"delta_version":1})).await;
    assert_eq!(status, StatusCode::OK, "{snapshot}");
    assert_eq!(snapshot["input_identity"]["delta_version"], 1);

    let raw = format!("/api/v1/delta/tables/{}/commits/stage", table.id);
    assert_eq!(
        post_json(router, &raw, &recorder, json!({"payload":"{}"}))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
}

fn native_event(legacy: &Value) -> Value {
    let mut fields = legacy.as_object().unwrap().clone();
    let schema_version = fields.remove("schema_version").unwrap();
    let source_event_id = fields.remove("source_event_id").unwrap();
    let occurred_at_ms = fields.remove("occurred_at_ms").unwrap();
    fields.remove("workspace_id");
    fields.remove("commit_version");
    json!({"schemaVersion": schema_version, "sourceEventId": source_event_id, "occurredAtMs": occurred_at_ms, "fields": fields})
}

fn native_page(legacy: &Value) -> Value {
    let events = legacy["events"].as_array().unwrap().iter().map(|row|
        json!({"workspaceId": row["workspace_id"], "commitVersion": row["commit_version"], "event": native_event(row)})
    ).collect::<Vec<_>>();
    json!({"events": events, "publishedVersion": legacy["published_version"], "nextBeforeVersion": legacy["next_before_version"]})
}

async fn get_activity(router: &Router, path: &str, token: &str) -> (StatusCode, Value) {
    let request = Request::get(path)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    if status.is_success() {
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    }
    let bytes = to_bytes(response.into_body(), 65_536).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn seed_activity_table(
    backend: Arc<dyn StorageBackend>,
    storage_root: &str,
    workspace: &str,
    recorder: &str,
) -> TableReadFixture {
    let storage = ScopedStorage::new(backend, TENANT, workspace).unwrap();
    let writer = table_read_catalog(&storage).await;
    let table = register_event_table(
        &writer,
        "activity_events",
        &[
            ("stage", "string", false),
            ("outcome", "string", false),
            ("surface_id", "string", false),
            ("release_id", "string", true),
            ("operation_id", "string", false),
        ],
    )
    .await;
    seed_table_read_authority(
        &storage,
        storage_root,
        &table.id,
        &table.name,
        READER,
        false,
    )
    .await;
    let scope = ControlPlaneScope::workspace_alias(TENANT, workspace).unwrap();
    let ledger = MetastoreLedger::new(storage.clone()).unwrap();
    let mutations = [
        (
            "activity_principal",
            MetastoreMutation::PrincipalUpserted(PrincipalRecord {
                principal_id: recorder.to_string(),
                name: recorder.to_string(),
                principal_kind: PrincipalKind::ServicePrincipal,
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_007,
                properties: BTreeMap::new(),
            }),
        ),
        (
            "activity_grant",
            MetastoreMutation::GrantUpserted(GrantRecord {
                grant_id: "activity_modify".to_string(),
                object_id: table.id.clone(),
                object_type: "TABLE".to_string(),
                principal_id: recorder.to_string(),
                privilege: "MODIFY".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_008,
                properties: BTreeMap::new(),
                identity_cut: None,
            }),
        ),
    ];
    for (id, mutation) in mutations {
        let sequence = next_fixture_sequence(&ledger).await;
        ledger
            .append_event(&MetastoreEvent::new_scoped(&scope, id, sequence, mutation))
            .await
            .unwrap();
    }
    publish_current_metastore_projection(&storage, &ProjectionRegistry::default())
        .await
        .unwrap();

    TableReadFixture {
        tenant: TENANT.to_string(),
        workspace: workspace.to_string(),
        table_id: table.id,
        parquet: Bytes::new(),
        storage,
    }
}

async fn seed_sync_table(storage: &ScopedStorage, name: &str) -> Table {
    let writer = CatalogWriter::new(storage.clone())
        .with_sync_compactor(Arc::new(Tier1Compactor::new(storage.clone())));
    let table = register_event_table(
        &writer,
        name,
        &[
            ("connector_id", "string", false),
            ("outcome", "string", false),
            ("records_written", "long", false),
            ("full_refresh", "boolean", true),
        ],
    )
    .await;
    let now = 1_800_000_000_020;
    let grant = |principal: &str, privilege: &str| {
        MetastoreMutation::GrantUpserted(GrantRecord {
            grant_id: format!("{name}_{privilege}"),
            object_id: table.id.clone(),
            object_type: "TABLE".to_string(),
            principal_id: principal.to_string(),
            privilege: privilege.to_string(),
            owner: "owner".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: now,
            properties: BTreeMap::new(),
            identity_cut: None,
        })
    };
    let mutations = [
        (
            "recorder",
            MetastoreMutation::PrincipalUpserted(PrincipalRecord {
                principal_id: SYNC_RECORDER.to_string(),
                name: SYNC_RECORDER.to_string(),
                principal_kind: PrincipalKind::ServicePrincipal,
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: now,
                properties: BTreeMap::new(),
            }),
        ),
        (
            "table",
            MetastoreMutation::CatalogObjectUpserted(CatalogObjectRecord {
                object_id: table.id.clone(),
                object_type: "TABLE".to_string(),
                qualified_name: format!("analytics.{name}"),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: now,
                properties: BTreeMap::new(),
            }),
        ),
        ("modify", grant(SYNC_RECORDER, "MODIFY")),
        ("select", grant(READER, "SELECT")),
    ];
    let scope =
        ControlPlaneScope::workspace_alias(storage.tenant_id(), storage.workspace_id()).unwrap();
    let ledger = MetastoreLedger::new(storage.clone()).unwrap();
    for (kind, mutation) in mutations {
        let sequence = next_fixture_sequence(&ledger).await;
        let id = format!("{name}_{kind}");
        ledger
            .append_event(&MetastoreEvent::new_scoped(&scope, id, sequence, mutation))
            .await
            .unwrap();
    }
    publish_current_metastore_projection(storage, &ProjectionRegistry::default())
        .await
        .unwrap();
    table
}

async fn register_event_table(
    writer: &CatalogWriter,
    name: &str,
    fields: &[(&str, &str, bool)],
) -> Table {
    let columns = [
        ("workspace_id", "string", false),
        ("schema_version", "integer", false),
        ("source_event_id", "string", false),
        ("occurred_at_ms", "long", false),
    ]
    .into_iter()
    .chain(fields.iter().copied())
    .enumerate()
    .map(
        |(ordinal, (name, data_type, is_nullable))| ColumnDefinition {
            name: name.to_string(),
            data_type: data_type.to_string(),
            is_nullable,
            ordinal: i32::try_from(ordinal).unwrap(),
            description: None,
        },
    )
    .collect();
    writer
        .register_table_in_schema(
            "default",
            "analytics",
            RegisterTableInSchemaRequest {
                name: name.to_string(),
                description: None,
                location: None,
                format: Some("delta".to_string()),
                table_type: Some("MANAGED".to_string()),
                properties: None,
                columns,
            },
            WriteOptions::default(),
        )
        .await
        .unwrap()
}

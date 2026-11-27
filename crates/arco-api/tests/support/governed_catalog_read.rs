#![allow(
    clippy::indexing_slicing,
    reason = "fixture tests assert the concrete JSON response shape"
)]

use super::*;

use arco_api::server::Server;
use arco_core::audit::{AuditEmitter, TestAuditSink};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const DISCOVERY: &str =
    "/api/2.1/unity-catalog/extensions/tables?catalog_name=default&schema_name=analytics";
const BROWSER: &str = "user_browser";

#[tokio::test]
async fn governed_catalog_discovery_checks_admission_and_current_privileges() {
    let backend = Arc::new(MemoryBackend::new());
    let orders = seed_table_read_fixture(Arc::clone(&backend)).await;
    let payroll = register_table(&orders.storage, STORAGE_ROOT, "payroll", &[(1, 42)]).await;
    let unapproved = register_table(&orders.storage, STORAGE_ROOT, "unapproved", &[(1, 42)]).await;
    append_grant(&orders.storage, &orders.table_id, BROWSER, "BROWSE").await;
    append_grant(&orders.storage, &unapproved, READER, "SELECT").await;
    let ids = [orders.table_id.as_str(), payroll.as_str()];
    let router = governed_router(backend.clone(), &ids);
    let reader = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let browser = table_read_rs256_jwt(TENANT, WORKSPACE, BROWSER);
    let intruder = table_read_rs256_jwt(TENANT, WORKSPACE, INTRUDER);

    for token in [&reader, &browser] {
        let response = get(&router, DISCOVERY, Some(token)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(
            read_json(response).await,
            json!({"tables": [{"table_id": orders.table_id, "name": "orders", "format": "parquet"}]})
        );
    }
    assert_eq!(
        read_json(get(&router, DISCOVERY, Some(&intruder)).await).await,
        json!({"tables": []})
    );
    assert_eq!(
        get(&router, DISCOVERY, None).await.status(),
        StatusCode::UNAUTHORIZED
    );
    for scope in [("other-tenant", WORKSPACE), (TENANT, "other-workspace")] {
        // A table with the same name and an active grant exists in the other scope.
        seed_table_read_fixture_in_scope(
            Arc::clone(&backend),
            scope.0,
            scope.1,
            None,
            "parquet",
            single_column_parquet(),
            true,
        )
        .await;
        let token = table_read_rs256_jwt(scope.0, scope.1, READER);
        assert_eq!(
            get(&router, DISCOVERY, Some(&token)).await.status(),
            StatusCode::FORBIDDEN
        );
    }

    for (table, principal) in [
        (&orders.table_id, &browser),
        (&payroll, &reader),
        (&unapproved, &reader),
    ] {
        let (status, _) = post_json(
            &router,
            &extension_path("table-descriptors", table),
            principal,
            json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    append_unrelated_principal_with_publication(&orders, false).await;
    assert_eq!(
        get(&router, DISCOVERY, Some(&reader)).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    revoke_fixture_select(&orders, READER).await;
    let restarted = governed_router(backend, &ids);
    for router in [&router, &restarted] {
        assert_eq!(
            read_json(get(router, DISCOVERY, Some(&reader)).await).await,
            json!({"tables": []})
        );
    }
}

#[tokio::test]
async fn governed_catalog_exposes_only_the_read_profile_and_requires_a_pin() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&backend)).await;
    let router = governed_router(backend, &[&fixture.table_id]);
    let jwt = table_read_rs256_jwt(TENANT, WORKSPACE, READER);

    for path in [
        "/api/v1/namespaces/analytics/tables",
        "/api/2.1/unity-catalog/tables?catalog_name=default&schema_name=analytics",
        "/api/2.1/unity-catalog/delta/v1/config",
        "/iceberg/v1/config",
        "/api/v1/event-streams/fixture-events/events",
        "/metrics",
    ] {
        assert_eq!(
            get(&router, path, Some(&jwt)).await.status(),
            StatusCode::NOT_FOUND,
            "{path}"
        );
    }
    for path in [
        "/api/v1/browser/urls",
        "/api/v1/managed-delta/ensure",
        "/api/v1/managed-delta/storage",
        "/internal/control-store/catalog-projection/urls",
        "/api/v1/event-streams/fixture-events/events",
        "/api/2.1/unity-catalog/temporary-table-credentials",
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::post(path)
                    .header(header::AUTHORIZATION, format!("Bearer {jwt}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
    let response = router
        .oneshot(table_read_bearer_request(
            &fixture.table_id,
            &jwt,
            &json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn governed_catalog_reproduces_pinned_net_revenue_with_audit_evidence() {
    exercise_join(Arc::new(MemoryBackend::new()), STORAGE_ROOT, None).await;
}

#[tokio::test]
async fn governed_catalog_reads_an_admitted_delta_version_after_append() {
    let storage = Arc::new(MemoryBackend::new());
    let (fixture, owner, paths) =
        managed_delta_fixture(storage.clone(), STORAGE_ROOT, WORKSPACE).await;
    let jwt = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let reader = governed_router(storage, &[&fixture.table_id]);
    let log = delta_orders_commit(&fixture, &paths, 0, &[1_000, -200], None).await;
    commit_delta(&owner, &fixture.table_id, &jwt, 0, &log).await;
    let (status, descriptor) = post_json(
        &reader,
        &extension_path("table-descriptors", &fixture.table_id),
        &jwt,
        json!({"delta_version": 0}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{descriptor}");
    let identity = &descriptor["input_identity"];
    let log = delta_orders_commit(&fixture, &paths, 1, &[300], None).await;
    commit_delta(&owner, &fixture.table_id, &jwt, 1, &log).await;
    assert_delta_snapshot(
        &reader,
        &fixture.table_id,
        &jwt,
        identity,
        (0, 800, 2),
        None,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires a fresh loopback S3 fixture and a proof directory"]
async fn governed_catalog_minio_pinned_join() {
    let bucket = std::env::var("ARCO_DELTA_PROOF_BUCKET").expect("fresh MinIO s3:// bucket");
    let evidence = std::env::var("ARCO_DELTA_PROOF_EVIDENCE").expect("fresh proof directory");
    exercise_join(
        arco_storage::from_bucket(&bucket).unwrap(),
        &bucket,
        Some(Path::new(&evidence)),
    )
    .await;
}

async fn exercise_join(
    backend: Arc<dyn StorageBackend>,
    storage_root: &str,
    evidence: Option<&Path>,
) {
    let storage = ScopedStorage::new(Arc::clone(&backend), TENANT, WORKSPACE).unwrap();
    let writer = table_read_catalog(&storage).await;
    let order_id = register_table(&storage, storage_root, "orders", &[(1, 1_000), (2, 500)]).await;
    let refund_id = register_table(&storage, storage_root, "refunds", &[(1, 200), (1, 100)]).await;
    seed_table_read_authority(&storage, storage_root, &order_id, "orders", READER, true).await;
    append_grant(&storage, &refund_id, READER, "SELECT").await;
    let config = governed_config(storage_root, &[&order_id, &refund_id]);
    let sink = Arc::new(TestAuditSink::new());
    let server = ServerBuilder::new()
        .config(config)
        .storage_backend(backend)
        .audit_emitter(Arc::new(AuditEmitter::with_test_sink(Arc::clone(&sink))))
        .build();
    let router = server.test_router();
    let jwt = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let discovered = read_json(get(&router, DISCOVERY, Some(&jwt)).await).await;
    assert_table_ids(&discovered, &[&order_id, &refund_id]);
    let (orders, order_pin) =
        describe_and_read(&router, &jwt, &discovered["tables"][0], evidence, &sink).await;
    let (refunds, _) =
        describe_and_read(&router, &jwt, &discovered["tables"][1], evidence, &sink).await;
    assert_eq!(net_revenue_cents(&orders, &refunds), 1_200);

    let renamed_pin = verify_rename(&server, &writer, &order_id, &jwt, &order_pin, &sink).await;

    revoke_fixture_select(
        &TableReadFixture {
            tenant: TENANT.to_string(),
            workspace: WORKSPACE.to_string(),
            table_id: order_id.clone(),
            parquet: Bytes::new(),
            storage: storage.clone(),
        },
        READER,
    )
    .await;
    let router = server.test_router();
    let denied = router
        .clone()
        .oneshot(table_read_bearer_request(
            &order_id,
            &jwt,
            &json!({"expected_snapshot_id": renamed_pin}),
        ))
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let after_revoke = read_json(get(&router, DISCOVERY, Some(&jwt)).await).await;
    assert_table_ids(&after_revoke, &[&refund_id]);

    writer
        .drop_table("analytics", "paid_orders", WriteOptions::default())
        .await
        .unwrap();
    let replacement = register_table(&storage, storage_root, "paid_orders", &[(1, 1_000)]).await;
    assert_ne!(replacement, order_id);
    append_grant(&storage, &replacement, READER, "SELECT").await;
    let after_replace = read_json(get(&router, DISCOVERY, Some(&jwt)).await).await;
    assert_table_ids(&after_replace, &[&refund_id]);
}

fn assert_table_ids(discovery: &Value, expected: &[&str]) {
    let actual = discovery["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|table| table["table_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(actual.as_slice(), expected);
}

async fn verify_rename(
    server: &Server,
    writer: &CatalogWriter,
    order_id: &str,
    jwt: &str,
    original_pin: &str,
    sink: &TestAuditSink,
) -> String {
    let renamed = writer
        .rename_table(
            "analytics",
            "orders",
            "analytics",
            "paid_orders",
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(renamed.id, order_id);
    let router = server.test_router();
    let after_rename = read_json(get(&router, DISCOVERY, Some(jwt)).await).await;
    let table = &after_rename["tables"][0];
    assert_eq!(table["name"], "paid_orders");
    assert_eq!(table["table_id"], order_id);
    let old_pin_read = router
        .clone()
        .oneshot(table_read_bearer_request(
            order_id,
            jwt,
            &json!({"expected_snapshot_id": original_pin}),
        ))
        .await
        .unwrap();
    assert_eq!(old_pin_read.status(), StatusCode::CONFLICT);
    let (_, renamed_pin) = describe_and_read(&router, jwt, table, None, sink).await;
    renamed_pin
}

async fn describe_and_read(
    router: &Router,
    jwt: &str,
    table: &Value,
    evidence: Option<&Path>,
    sink: &TestAuditSink,
) -> (Bytes, String) {
    let table_id = table["table_id"].as_str().unwrap();
    let name = table["name"].as_str().unwrap();
    let (status, descriptor) = post_json(
        router,
        &extension_path("table-descriptors", table_id),
        jwt,
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{descriptor}");
    assert_eq!(descriptor["schema"][0]["name"], "order_id");
    assert_eq!(descriptor["units"]["amount_cents"], "USD cents");
    assert_eq!(descriptor["source_data_cutoff"], "2026-11-25T00:00:00Z");
    assert_eq!(descriptor["completeness"], "unknown");
    let identity = &descriptor["input_identity"];
    let snapshot = identity["snapshot_id"].as_str().unwrap().to_string();
    let response = router
        .clone()
        .oneshot(table_read_bearer_request(
            table_id,
            jwt,
            &json!({"expected_snapshot_id": snapshot}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        table_read_header(&response, "X-Arco-Table-Read-Snapshot-Id"),
        snapshot
    );
    let audit_id = table_read_header(&response, "X-Arco-Table-Read-Authorization-Audit-Id");
    let watermark = table_read_header(&response, "X-Arco-Table-Read-Permission-Watermark");
    let event = sink
        .events()
        .into_iter()
        .find(|event| event.event_id == audit_id)
        .unwrap();
    assert_eq!(event.policy_version, watermark);
    assert_eq!(
        event.resource,
        format!("uc_table_read:{table_id}:{snapshot}")
    );
    assert_ne!(event.actor, READER);
    let bytes = to_bytes(response.into_body(), 8 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(identity["sha256"], hex::encode(Sha256::digest(&bytes)));
    if let Some(directory) = evidence {
        write_proof_json(directory, &format!("{name}-descriptor.json"), &descriptor);
        write_proof_json(
            directory,
            &format!("{name}-audit.json"),
            &serde_json::to_value(&event).unwrap(),
        );
        std::fs::write(directory.join(format!("{name}.parquet")), &bytes).unwrap();
    }
    (bytes, snapshot)
}

fn governed_router(backend: Arc<MemoryBackend>, table_ids: &[&str]) -> Router {
    ServerBuilder::new()
        .config(governed_config(STORAGE_ROOT, table_ids))
        .storage_backend(backend)
        .build()
        .test_router()
}

fn governed_config(storage_root: &str, table_ids: &[&str]) -> Config {
    // Deserialize the normal deployment configuration so the contract can be
    // exercised before this profile's configuration field exists.
    let mut config = serde_json::to_value(Config::default()).unwrap();
    config["governed_catalog_read"] = json!({
        "tenant_id": TENANT,
        "workspace_id": WORKSPACE,
        "table_ids": table_ids,
    });
    let mut config: Config = serde_json::from_value(config).unwrap();
    config.debug = false;
    config.posture = Posture::Private;
    config.storage.bucket = Some(storage_root.to_string());
    config.unity_catalog.enabled = true;
    config.unity_catalog.table_reads_enabled = true;
    config.iceberg.enabled = true;
    config.control_store_operator_endpoints = true;
    config.event_streams = serde_json::from_value(json!([{
        "tenant": TENANT,
        "workspace": WORKSPACE,
        "stream_id": "fixture-events",
        "table_id": table_ids[0],
        "schema": {
            "profile": "custom",
            "version": 1,
            "fields": [{"name": "code", "type": "identifier"}]
        }
    }]))
    .unwrap();
    config.jwt = JwtConfig {
        rs256_public_key_pem: Some(TEST_RSA_PUBLIC_KEY_PEM.to_string()),
        issuer: Some(JWT_ISSUER.to_string()),
        audience: Some(JWT_AUDIENCE.to_string()),
        ..JwtConfig::default()
    };
    config
}

async fn register_table(
    storage: &ScopedStorage,
    storage_root: &str,
    name: &str,
    rows: &[(i64, i64)],
) -> String {
    let path = format!("fixtures/{name}.parquet");
    let writer = CatalogWriter::new(storage.clone())
        .with_sync_compactor(Arc::new(Tier1Compactor::new(storage.clone())));
    let columns = ["order_id", "amount_cents"];
    let table = writer
        .register_table_in_schema(
            "default",
            "analytics",
            RegisterTableInSchemaRequest {
                name: name.to_string(),
                description: Some(format!("Paid order amounts for {name}.")),
                location: Some(format!(
                    "{storage_root}/{}/{path}",
                    storage.scope().prefix()
                )),
                format: Some("parquet".to_string()),
                table_type: Some("EXTERNAL".to_string()),
                properties: Some(BTreeMap::from([
                    (
                        "arco.dataset.grain".into(),
                        if name == "refunds" {
                            "one row per refund"
                        } else {
                            "one row per order"
                        }
                        .into(),
                    ),
                    (
                        "arco.dataset.units".into(),
                        r#"{"amount_cents":"USD cents"}"#.into(),
                    ),
                    (
                        "arco.dataset.source_data_cutoff".into(),
                        "2026-11-25T00:00:00Z".into(),
                    ),
                ])),
                columns: columns
                    .into_iter()
                    .enumerate()
                    .map(|(ordinal, name)| ColumnDefinition {
                        name: name.to_string(),
                        ordinal: i32::try_from(ordinal).unwrap(),
                        ..revenue_column()
                    })
                    .collect(),
            },
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(
        columns
            .map(|column| Field::new(column, DataType::Int64, false))
            .to_vec(),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
        ],
    )
    .unwrap();
    let mut parquet = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    parquet.write(&batch).unwrap();
    storage
        .put_raw(
            &path,
            Bytes::from(parquet.into_inner().unwrap()),
            WritePrecondition::None,
        )
        .await
        .unwrap();
    table.id
}

fn parquet_pairs(bytes: &Bytes) -> Vec<(i64, i64)> {
    let batches = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .unwrap()
        .build()
        .unwrap();
    let mut pairs = Vec::new();
    for batch in batches {
        let batch = batch.unwrap();
        let orders = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let amounts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        pairs.extend(
            orders
                .values()
                .iter()
                .copied()
                .zip(amounts.values().iter().copied()),
        );
    }
    pairs
}

fn net_revenue_cents(orders: &Bytes, refunds: &Bytes) -> i64 {
    let mut refunded_by_order = BTreeMap::<i64, i64>::new();
    for (order, cents) in parquet_pairs(refunds) {
        *refunded_by_order.entry(order).or_default() += cents;
    }
    parquet_pairs(orders)
        .into_iter()
        .map(|(order, cents)| cents - refunded_by_order.get(&order).copied().unwrap_or_default())
        .sum()
}

async fn append_grant(storage: &ScopedStorage, table_id: &str, principal: &str, privilege: &str) {
    let scope =
        ControlPlaneScope::workspace_alias(storage.tenant_id(), storage.workspace_id()).unwrap();
    let ledger = MetastoreLedger::new(storage.clone()).unwrap();
    let sequence = next_fixture_sequence(&ledger).await;
    let mut mutations = vec![
        MetastoreMutation::PrincipalUpserted(PrincipalRecord {
            principal_id: principal.to_string(),
            name: principal.to_string(),
            principal_kind: PrincipalKind::User,
            owner: "owner".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 1_800_000_000_000,
            properties: BTreeMap::new(),
        }),
        MetastoreMutation::GrantUpserted(GrantRecord {
            grant_id: format!("grant_{sequence}_{privilege}"),
            object_id: table_id.to_string(),
            object_type: "TABLE".to_string(),
            principal_id: principal.to_string(),
            privilege: privilege.to_string(),
            owner: "owner".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 1_800_000_000_000,
            properties: BTreeMap::new(),
            identity_cut: None,
        }),
    ];
    if !ledger
        .replay()
        .await
        .unwrap()
        .catalog_objects
        .contains_key(table_id)
    {
        mutations.insert(
            1,
            MetastoreMutation::CatalogObjectUpserted(CatalogObjectRecord {
                object_id: table_id.to_string(),
                object_type: "TABLE".to_string(),
                qualified_name: format!("analytics.table_{sequence}"),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_000,
                properties: BTreeMap::new(),
            }),
        );
    }
    for (sequence, mutation) in (sequence..).zip(mutations) {
        ledger
            .append_event(&MetastoreEvent::new_scoped(
                &scope,
                format!("governed_table_{sequence:03}"),
                sequence,
                mutation,
            ))
            .await
            .unwrap();
    }
    publish_current_metastore_projection(storage, &ProjectionRegistry::default())
        .await
        .unwrap();
}

async fn get(router: &Router, path: &str, token: Option<&str>) -> Response {
    let mut request = Request::get(path);
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn read_json(response: Response) -> Value {
    let bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

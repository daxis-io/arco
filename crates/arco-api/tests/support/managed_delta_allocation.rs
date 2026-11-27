//! Allocated managed Delta placement through real API and UC handlers.

use super::*;
use arco_api::server::Server;
use arco_catalog::CatalogReader;
use arco_core::flow_paths::ManagedDeltaRoot;
use arco_uc::ManagedDeltaStorage;

async fn admit_object(storage: &ScopedStorage, id: &str, kind: &str, privileges: &[&str]) {
    let scope = ControlPlaneScope::workspace_alias(storage.tenant_id(), storage.workspace_id())
        .expect("workspace scope");
    let ledger = MetastoreLedger::new(storage.clone()).expect("ledger");
    let object = MetastoreMutation::CatalogObjectUpserted(CatalogObjectRecord {
        object_id: id.to_string(),
        object_type: kind.to_string(),
        qualified_name: format!("analytics.{id}"),
        owner: READER.to_string(),
        lifecycle_state: LifecycleState::Active,
        updated_at_ms: 1_800_000_100_000,
        properties: BTreeMap::new(),
    });
    let grants = privileges.iter().map(|privilege| {
        MetastoreMutation::GrantUpserted(GrantRecord {
            grant_id: format!("grant_{id}_{privilege}"),
            object_id: id.to_string(),
            object_type: kind.to_string(),
            principal_id: READER.to_string(),
            privilege: (*privilege).to_string(),
            owner: READER.to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 1_800_000_100_001,
            properties: BTreeMap::new(),
            identity_cut: None,
        })
    });
    for mutation in std::iter::once(object).chain(grants) {
        let seq = next_fixture_sequence(&ledger).await;
        let event =
            MetastoreEvent::new_scoped(&scope, format!("allocate_fixture_{seq}"), seq, mutation);
        ledger
            .append_event(&event)
            .await
            .expect("owner fixture event");
    }
}

fn router(
    catalog: Arc<dyn StorageBackend>,
    data: Arc<dyn StorageBackend>,
    root: &ManagedDeltaRoot,
) -> Router {
    let mut config = production_rs256_config(STORAGE_ROOT);
    config.managed_delta_root = Some(root.as_uri().to_string());
    Server::with_storage_backend(config, catalog)
        .with_managed_delta_storage(ManagedDeltaStorage::new(root.clone(), data))
        .test_router()
}

async fn request(
    router: &Router,
    method: &str,
    path: &str,
    bearer: &str,
    payload: &Value,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(payload.to_string()))
        .expect("request");
    let response = router.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    (
        status,
        serde_json::from_slice(&bytes).expect("JSON response"),
    )
}

#[tokio::test]
async fn allocated_table_replays_and_uses_one_analytical_log_root() {
    let catalog = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&catalog)).await;
    let catalog: Arc<dyn StorageBackend> = catalog;
    let data: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let root = ManagedDeltaRoot::parse("s3://analytical-bucket/approved").expect("root");
    let api = router(Arc::clone(&catalog), Arc::clone(&data), &root);
    let actor = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let op_id = Uuid::now_v7().to_string();
    let payload = json!({
        "operation_id": op_id, "catalog": "default", "schema": "analytics",
        "name": "allocated_orders", "provisioning_fingerprint": "v1",
        "columns": [
            {"name": "day", "data_type": "DATE", "nullable": false},
            {"name": "amount_cents", "data_type": "LONG", "nullable": false},
            {"name": "status", "data_type": "STRING", "nullable": false},
        ],
    });
    let path = "/api/v1/managed-delta/ensure";
    let before = fixture_object_versions(&fixture).await;
    assert_eq!(
        request(&api, "POST", path, &actor, &payload).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(fixture_object_versions(&fixture).await, before);

    let schemas = CatalogReader::new(fixture.storage.clone())
        .list_schemas("default")
        .await
        .expect("default schemas");
    let schema = schemas
        .iter()
        .find(|schema| schema.name == "analytics")
        .expect("analytics schema");
    admit_object(&fixture.storage, &schema.id, "SCHEMA", &["CREATE_TABLE"]).await;

    let (status, allocated) = request(&api, "POST", path, &actor, &payload).await;
    assert_eq!(status, StatusCode::CREATED, "{allocated}");
    assert_eq!(
        request(&api, "POST", path, &actor, &payload).await,
        (status, allocated.clone())
    );
    let mut changed = payload;
    *changed
        .get_mut("provisioning_fingerprint")
        .expect("fingerprint") = json!("changed");
    assert_eq!(
        request(&api, "POST", path, &actor, &changed).await.0,
        StatusCode::CONFLICT
    );

    let id = allocated
        .get("id")
        .and_then(Value::as_str)
        .expect("table ID");
    let table_id = Uuid::parse_str(id).expect("table UUID");
    let location = root
        .table_location(TENANT, WORKSPACE, table_id)
        .expect("managed URI");
    assert_ne!(id, op_id);
    assert_eq!(
        allocated.get("location").and_then(Value::as_str),
        Some(location.as_str())
    );
    assert_eq!(
        allocated
            .pointer("/properties/arco.managed_delta_owner")
            .and_then(Value::as_str),
        Some(READER)
    );
    admit_object(&fixture.storage, id, "TABLE", &["SELECT", "MODIFY"]).await;
    assert_split_delta_commits(
        &api,
        &actor,
        &fixture.storage,
        Arc::clone(&data),
        &root,
        table_id,
        &location,
    )
    .await;
    assert_allocated_pin(
        &fixture.storage,
        Arc::clone(&catalog),
        Arc::clone(&data),
        &root,
        &actor,
        table_id,
    )
    .await;
    let wrong_root =
        ManagedDeltaRoot::parse("s3://analytical-bucket/different").expect("other root");
    let moved = router(catalog, data, &wrong_root);
    let before = fixture_object_versions(&fixture).await;
    let stage = format!("/api/v1/delta/tables/{table_id}/commits/stage");
    assert_eq!(
        post_json(&moved, &stage, &actor, json!({"payload": "{}"}))
            .await
            .0,
        StatusCode::PRECONDITION_FAILED,
    );
    let preview = "/api/2.1/unity-catalog/delta/preview/commits";
    let request_body = json!({"table_id": table_id, "table_uri": location});
    assert_eq!(
        request(&moved, "GET", preview, &actor, &request_body)
            .await
            .0,
        StatusCode::CONFLICT,
    );
    assert_eq!(fixture_object_versions(&fixture).await, before);
}

async fn assert_split_delta_commits(
    api: &Router,
    actor: &str,
    catalog: &ScopedStorage,
    data: Arc<dyn StorageBackend>,
    root: &ManagedDeltaRoot,
    table_id: Uuid,
    location: &str,
) {
    let paths = DeltaPaths::legacy(table_id);
    let logs = ScopedStorage::new(data, TENANT, WORKSPACE)
        .expect("workspace")
        .with_bucket_prefix(root.prefix())
        .expect("data prefix");
    let fixture = TableReadFixture {
        tenant: TENANT.to_string(),
        workspace: WORKSPACE.to_string(),
        table_id: table_id.to_string(),
        parquet: Bytes::new(),
        storage: logs.clone(),
    };
    let payload = delta_orders_commit(&fixture, &paths, 0, &[1000], None).await;
    commit_delta(api, &table_id.to_string(), actor, 0, &payload).await;
    let preview = "/api/2.1/unity-catalog/delta/preview/commits";
    let commit = json!({
        "table_id": table_id, "table_uri": location,
        "commit_info": {
            "version": 1, "file_name": "00000000000000000001.json", "file_size": 50,
            "timestamp": 1, "file_modification_timestamp": 1,
        }
    });
    assert_eq!(
        post_json(api, preview, actor, commit).await.0,
        StatusCode::OK
    );
    let (status, listed) = request(
        api,
        "GET",
        preview,
        actor,
        &json!({
            "table_id": table_id, "table_uri": location, "start_version": 0, "end_version": 1,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    assert_eq!(
        listed.get("latest_table_version").and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        listed
            .get("commits")
            .and_then(Value::as_array)
            .expect("commits")
            .len(),
        2
    );
    assert!(
        catalog
            .head_raw(&paths.coordinator_state())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        logs.head_raw(&paths.coordinator_state())
            .await
            .unwrap()
            .is_none()
    );
    for version in 0..=1 {
        let key = paths.delta_log_json(version).expect("log key");
        assert!(logs.head_raw(&key).await.unwrap().is_some());
        assert!(catalog.head_raw(&key).await.unwrap().is_none());
    }
}

async fn assert_allocated_pin(
    storage: &ScopedStorage,
    catalog: Arc<dyn StorageBackend>,
    data: Arc<dyn StorageBackend>,
    root: &ManagedDeltaRoot,
    actor: &str,
    table_id: Uuid,
) {
    let scope = ControlPlaneScope::workspace_alias(TENANT, WORKSPACE).expect("scope");
    let ledger = MetastoreLedger::new(storage.clone()).expect("ledger");
    let sequence = next_fixture_sequence(&ledger).await;
    ledger
        .append_event(&MetastoreEvent::new_scoped(
            &scope,
            format!("allocate_fixture_{sequence}"),
            sequence,
            MetastoreMutation::ExternalLocationUpserted(ExternalLocationRecord {
                location_id: "location_table_read".to_string(),
                name: "table-read-root".to_string(),
                url: format!("{}/tenant={TENANT}/workspace={WORKSPACE}/", root.as_uri()),
                credential_id: "credential_table_read".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_100_002,
                properties: BTreeMap::new(),
            }),
        ))
        .await
        .expect("bind analytical root");
    publish_current_metastore_projection(storage, &ProjectionRegistry::default())
        .await
        .expect("publish analytical governance");
    let mut cfg = production_rs256_config(STORAGE_ROOT);
    cfg.managed_delta_root = Some(root.as_uri().to_string());
    let serving = Server::with_storage_backend(cfg, catalog)
        .with_managed_delta_storage(ManagedDeltaStorage::new(root.clone(), data))
        .test_router();
    let id = table_id.to_string();
    let (status, described) = post_json(
        &serving,
        &extension_path("table-descriptors", &id),
        actor,
        json!({"delta_version": 0}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{described}");
    let snapshot = described
        .pointer("/input_identity/snapshot_id")
        .and_then(Value::as_str)
        .expect("pin");
    let (status, read) = post_json(
        &serving,
        &extension_path("table-reads", &id),
        actor,
        json!({"delta_version": 0, "expected_snapshot_id": snapshot}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{read}");
    assert_eq!(
        read.pointer("/input_identity/snapshot_id")
            .and_then(Value::as_str),
        Some(snapshot)
    );
    assert_eq!(
        read.get("objects")
            .and_then(Value::as_array)
            .expect("log and data")
            .len(),
        2
    );
}

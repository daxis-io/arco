//! Two-store qualification through the production JWT and catalog routers.

use super::{
    READER, TENANT, WORKSPACE, assert_delta_snapshot, commit_delta, delta_fixture_commit,
    extension_path, fixture_parquet, next_fixture_sequence, post_json, production_rs256_config,
    table_read_catalog, table_read_rs256_jwt, write_proof_json,
};
use arco_api::server::ServerBuilder;
use arco_catalog::idempotency::{
    CatalogIdempotencyMarker, CatalogOperation, DEFAULT_STALE_TIMEOUT, IdempotencyStatus,
};
use arco_catalog::managed_delta::{
    CATALOG, ManagedDeltaAllocation, ManagedDeltaStorage, SCHEMA, TABLE,
};
use arco_catalog::metastore::events::{
    CatalogObjectRecord, GrantRecord, LifecycleState, ManagedRootRecord, MetastoreEvent,
    MetastoreMutation, PrincipalKind, PrincipalRecord,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::metastore::projections::ProjectionRegistry;
use arco_catalog::metastore::publish::publish_current_metastore_projection;
use arco_catalog::{CatalogError, CatalogReader, CatalogWriter, Tier1Compactor, WriteOptions};
use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{ControlPlaneScope, DeltaPaths, ScopedStorage};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;

const PREFIX: &str = "approved/managed";
const ROOT_ID: &str = "approved_managed_delta";
const ENSURE: &str = "/api/v1/managed-delta/ensure";
const STORAGE: &str = "/api/v1/managed-delta/storage";
const PREVIEW: &str = "/api/2.1/unity-catalog/delta/preview/commits";

#[tokio::test]
async fn managed_delta_allocation_replay_does_not_adopt_recreated_table() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let uri = "s3://analytic-bucket/approved/managed";
    let catalog = seed_scope(&backend, TENANT, WORKSPACE, uri).await;
    let root = ManagedDeltaStorage::open(uri, |bucket| {
        assert_eq!(bucket, "s3://analytic-bucket");
        Ok(Arc::new(MemoryBackend::new()))
    })
    .unwrap();
    let writer = CatalogWriter::new(catalog.clone())
        .with_sync_compactor(Arc::new(Tier1Compactor::new(catalog.clone())));
    let mut intent = ManagedDeltaAllocation {
        operation_id: Uuid::now_v7(),
        owner: READER.to_string(),
        provisioning_fingerprint: "test-immutable-placement".to_string(),
    };
    let scope = ControlPlaneScope::workspace_alias(TENANT, WORKSPACE).unwrap();
    let ledger = MetastoreLedger::new(catalog.clone()).unwrap();
    let sequence = next_fixture_sequence(&ledger).await;
    let schema = format!("{CATALOG}.{SCHEMA}");
    ledger
        .append_event(&MetastoreEvent::new_scoped(
            &scope,
            "make_allocation_governance_stale",
            sequence,
            object(&schema, "SCHEMA", &schema),
        ))
        .await
        .unwrap();
    let unavailable = writer
        .allocate_managed_delta(&root, &intent, WriteOptions::default())
        .await
        .unwrap_err();
    assert_eq!(unavailable.http_status_code(), Some(503), "{unavailable}");
    let path = CatalogIdempotencyMarker::storage_path(
        CatalogOperation::RegisterTableInSchema,
        &CatalogIdempotencyMarker::hash_key(&intent.operation_id.to_string()),
    );
    let mut marker: CatalogIdempotencyMarker =
        serde_json::from_slice(&catalog.get_raw(&path).await.unwrap()).unwrap();
    assert_eq!(marker.status, IdempotencyStatus::InProgress);
    marker.started_at -= DEFAULT_STALE_TIMEOUT + chrono::TimeDelta::seconds(1);
    let write = catalog
        .put_raw(
            &path,
            serde_json::to_vec(&marker).unwrap().into(),
            WritePrecondition::None,
        )
        .await
        .unwrap();
    assert!(matches!(write, WriteResult::Success { .. }));
    publish_current_metastore_projection(&catalog, &ProjectionRegistry::default())
        .await
        .unwrap();
    let first = writer
        .allocate_managed_delta(&root, &intent, WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(
        marker.entity_id.as_deref(),
        Some(first.table_id.to_string().as_str())
    );
    writer
        .drop_table_in_schema_transaction(CATALOG, SCHEMA, TABLE, WriteOptions::default())
        .await
        .unwrap();
    let original_operation = intent.operation_id;
    intent.operation_id = Uuid::now_v7();
    let replacement = writer
        .allocate_managed_delta(&root, &intent, WriteOptions::default())
        .await
        .unwrap();
    assert_ne!(first.table_id, replacement.table_id);
    assert_ne!(first.location, replacement.location);
    intent.operation_id = original_operation;
    let error = writer
        .allocate_managed_delta(&root, &intent, WriteOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            CatalogError::RequestFailed {
                http_status: 409,
                ..
            }
        ),
        "{error}"
    );
}

#[tokio::test]
#[ignore = "requires fresh catalog and analytical S3 buckets and pinned delta-rs"]
async fn production_managed_delta_allocation_minio() {
    let bucket = std::env::var("ARCO_DELTA_PROOF_BUCKET").unwrap();
    let analytics = std::env::var("ARCO_DELTA_PROOF_ANALYTICAL_BUCKET").unwrap();
    assert_ne!(bucket, analytics);
    let root = format!("{analytics}/{PREFIX}");
    let evidence = std::env::var("ARCO_DELTA_PROOF_EVIDENCE").unwrap();
    let evidence = Path::new(&evidence);
    let catalog_backend = arco_storage::from_bucket(&bucket).unwrap();
    let data_backend = arco_storage::from_bucket(&analytics).unwrap();
    let router = router(&catalog_backend, &bucket, Some(&root));
    let catalog = seed_scope(&catalog_backend, TENANT, WORKSPACE, &root).await;
    let token = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let data = analytical_scope(&data_backend, TENANT, WORKSPACE);
    verify_storage_probe(&router, &catalog, &data, &token, evidence).await;
    let request = json!({
        "operation_id": Uuid::now_v7(), "owner": READER,
        "provisioning_fingerprint": "managed-delta-native-proof-v1"
    });
    let receipt = allocate(&router, &catalog, &token, &root, &request).await;
    let table_id = receipt.get("id").and_then(Value::as_str).unwrap();
    let location = receipt.get("location").and_then(Value::as_str).unwrap();
    write_proof_json(
        evidence,
        "allocation.json",
        &json!({
            "method": "POST", "path": ENSURE, "request": request, "status": 200, "response": receipt,
        }),
    );
    let pin = verify_two_versions(&router, &catalog, &data, table_id, &token, evidence).await;
    verify_preview(&router, table_id, location, &token, evidence).await;
    let (status, replay) = post_json(&router, ENSURE, &token, request.clone()).await;
    assert_eq!((status, &replay), (StatusCode::OK, &receipt));
    assert!(
        data.head_raw(
            &DeltaPaths::legacy(table_id.parse().unwrap())
                .delta_log_json(2)
                .unwrap()
        )
        .await
        .unwrap()
        .is_none()
    );

    let drifted = [
        self::router(
            &catalog_backend,
            &bucket,
            Some(&format!("{analytics}/different/managed")),
        ),
        self::router(
            &catalog_backend,
            &bucket,
            Some(&format!("{bucket}/{PREFIX}")),
        ),
        self::router(&catalog_backend, &bucket, None),
    ];
    verify_denials(
        &router, &drifted, &catalog, table_id, location, &token, &pin, evidence,
    )
    .await;
    publish(
        &catalog,
        [managed_root(&catalog, &root, LifecycleState::Deleted)],
    )
    .await;
    let (status, error) = post_json(&router, ENSURE, &token, request.clone()).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{error}");
    let (status, error) = post_json(
        &router,
        STORAGE,
        &token,
        json!({"operation_id": Uuid::now_v7()}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{error}");
    let stage = format!("/api/v1/delta/tables/{table_id}/commits/stage");
    let (status, error) = post_json(&router, &stage, &token, json!({"payload": "{}"})).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{error}");
    write_proof_json(evidence, "revoked-root.json", &error);
    verify_isolation(
        &router,
        &catalog_backend,
        &data_backend,
        &root,
        table_id,
        &request,
        evidence,
    )
    .await;
}

async fn verify_storage_probe(
    router: &Router,
    catalog: &ScopedStorage,
    data: &ScopedStorage,
    token: &str,
    evidence: &Path,
) {
    let operation_id = Uuid::now_v7();
    let request = json!({"operation_id": operation_id});
    let key = format!("managed-delta/probes/{operation_id}");
    let stranded = data
        .put_raw(
            &key,
            Bytes::from_static(b"arco-managed-delta-storage-probe-v1"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    assert!(matches!(stranded, WriteResult::Success { .. }));

    let [first, second]: [_; 2] = tokio::join!(
        post_json(router, STORAGE, token, request.clone()),
        post_json(router, STORAGE, token, request.clone()),
    )
    .into();
    for (status, receipt) in [&first, &second] {
        assert_eq!(*status, StatusCode::OK, "{receipt}");
        assert_eq!(*receipt, json!({"state": "passed"}));
    }
    assert!(data.head_raw(&key).await.unwrap().is_none());
    assert!(catalog.head_raw(&key).await.unwrap().is_none());
    write_proof_json(
        evidence,
        "storage-probe.json",
        &json!({
            "path": STORAGE, "request": request, "recovered_key": key,
            "responses": [first.1, second.1], "statuses": [first.0.as_u16(), second.0.as_u16()],
        }),
    );
}

fn router(backend: &Arc<dyn StorageBackend>, bucket: &str, root: Option<&str>) -> Router {
    let mut config = production_rs256_config(bucket);
    config.storage.managed_delta_root = root.map(str::to_string);
    ServerBuilder::new()
        .config(config)
        .storage_backend(Arc::clone(backend))
        .build()
        .test_router()
}

async fn seed_scope(
    backend: &Arc<dyn StorageBackend>,
    tenant: &str,
    workspace: &str,
    root: &str,
) -> ScopedStorage {
    let catalog = ScopedStorage::new(Arc::clone(backend), tenant, workspace).unwrap();
    let writer = table_read_catalog(&catalog).await;
    writer
        .create_catalog(CATALOG, None, WriteOptions::default())
        .await
        .unwrap();
    writer
        .create_schema(CATALOG, SCHEMA, None, WriteOptions::default())
        .await
        .unwrap();
    let schema = format!("{CATALOG}.{SCHEMA}");
    publish(
        &catalog,
        [
            MetastoreMutation::PrincipalUpserted(PrincipalRecord {
                principal_id: READER.to_string(),
                name: READER.to_string(),
                principal_kind: PrincipalKind::User,
                owner: READER.to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1,
                properties: BTreeMap::new(),
            }),
            object(&schema, "SCHEMA", &schema),
            grant(&schema, "SCHEMA", "CREATE_TABLE", LifecycleState::Active),
            managed_root(&catalog, root, LifecycleState::Active),
        ],
    )
    .await;
    catalog
}

fn analytical_scope(
    backend: &Arc<dyn StorageBackend>,
    tenant: &str,
    workspace: &str,
) -> ScopedStorage {
    ScopedStorage::new(Arc::clone(backend), tenant, workspace)
        .unwrap()
        .with_storage_prefix(PREFIX)
        .unwrap()
}

fn object(id: &str, kind: &str, name: &str) -> MetastoreMutation {
    MetastoreMutation::CatalogObjectUpserted(CatalogObjectRecord {
        object_id: id.to_string(),
        object_type: kind.to_string(),
        qualified_name: name.to_string(),
        owner: "catalog_administrator".to_string(),
        lifecycle_state: LifecycleState::Active,
        updated_at_ms: 1,
        properties: BTreeMap::new(),
    })
}

fn grant(
    id: &str,
    kind: &str,
    privilege: &str,
    lifecycle_state: LifecycleState,
) -> MetastoreMutation {
    MetastoreMutation::GrantUpserted(GrantRecord {
        grant_id: format!("proof_{id}_{privilege}"),
        object_id: id.to_string(),
        object_type: kind.to_string(),
        principal_id: READER.to_string(),
        privilege: privilege.to_string(),
        owner: READER.to_string(),
        lifecycle_state,
        updated_at_ms: 2,
        properties: BTreeMap::new(),
        identity_cut: None,
    })
}

fn managed_root(
    catalog: &ScopedStorage,
    root: &str,
    lifecycle_state: LifecycleState,
) -> MetastoreMutation {
    MetastoreMutation::ManagedRootUpserted(ManagedRootRecord {
        root_id: ROOT_ID.to_string(),
        name: ROOT_ID.to_string(),
        workspace_id: catalog.workspace_id().to_string(),
        url: format!("{root}/{}/", catalog.scope().prefix()),
        owner: READER.to_string(),
        lifecycle_state,
        updated_at_ms: 2,
        properties: BTreeMap::new(),
    })
}

async fn publish(catalog: &ScopedStorage, mutations: impl IntoIterator<Item = MetastoreMutation>) {
    let ledger = MetastoreLedger::new(catalog.clone()).unwrap();
    let scope =
        ControlPlaneScope::workspace_alias(catalog.tenant_id(), catalog.workspace_id()).unwrap();
    let mut sequence = next_fixture_sequence(&ledger).await;
    for mut mutation in mutations {
        match &mut mutation {
            MetastoreMutation::GrantUpserted(record) => {
                record.updated_at_ms = sequence.cast_signed();
            }
            MetastoreMutation::ManagedRootUpserted(record) => {
                record.updated_at_ms = sequence.cast_signed();
            }
            _ => {}
        }
        let event =
            MetastoreEvent::new_scoped(&scope, format!("placement_{sequence}"), sequence, mutation);
        ledger.append_event(&event).await.unwrap();
        sequence += 1;
    }
    publish_current_metastore_projection(catalog, &ProjectionRegistry::default())
        .await
        .unwrap();
}

async fn allocate(
    router: &Router,
    catalog: &ScopedStorage,
    token: &str,
    root: &str,
    request: &Value,
) -> Value {
    let foreign_owner =
        table_read_rs256_jwt(catalog.tenant_id(), catalog.workspace_id(), "other_owner");
    let (status, error) = post_json(router, ENSURE, &foreign_owner, request.clone()).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{error}");
    let [first, second]: [_; 2] = tokio::join!(
        post_json(router, ENSURE, token, request.clone()),
        post_json(router, ENSURE, token, request.clone()),
    )
    .into();
    let receipt = if first.0 == StatusCode::OK {
        first.1.clone()
    } else {
        assert_eq!(second.0, StatusCode::OK, "{first:?}, {second:?}");
        second.1.clone()
    };
    let statuses = [first.0.as_u16(), second.0.as_u16()];
    for (status, response) in [first, second] {
        if status == StatusCode::OK {
            assert_eq!(response, receipt);
        } else {
            assert_eq!(status, StatusCode::CONFLICT, "{response}");
            let replay = post_json(router, ENSURE, token, request.clone()).await;
            assert_eq!(replay, (StatusCode::OK, receipt.clone()));
        }
    }
    let table_id = receipt.get("id").and_then(Value::as_str).unwrap();
    let uuid = Uuid::parse_str(table_id).unwrap();
    assert_eq!(uuid.get_version_num(), 7);
    let location = format!("{root}/{}/tables/{table_id}/", catalog.scope().prefix());
    assert_eq!(
        receipt.get("location").and_then(Value::as_str),
        Some(location.as_str())
    );
    let tables = CatalogReader::new(catalog.clone())
        .list_tables_in_schema(CATALOG, SCHEMA)
        .await
        .unwrap();
    assert_eq!(tables.len(), 1, "expected one allocated table");
    let row = tables.first().unwrap();
    assert_eq!(row.id, table_id);
    assert_eq!(row.name, TABLE);
    assert_eq!(row.location.as_deref(), Some(location.as_str()));
    assert_eq!(row.format.as_deref(), Some("delta"));
    assert_eq!(row.table_type.as_deref(), Some("MANAGED"));
    println!(
        "{}",
        json!({
            "event": "concurrent-allocation", "tenant": catalog.tenant_id(),
            "workspace": catalog.workspace_id(), "statuses": statuses, "table_id": table_id,
            "location": location, "rows": tables.len(),
        })
    );
    let bytes = catalog
        .get_raw(&format!("delta/placements/{uuid}.json"))
        .await
        .unwrap();
    let placement: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(placement.get("allocation"), Some(request));
    assert_eq!(placement.get("location"), receipt.get("location"));
    assert_eq!(placement.get("schema_digest"), receipt.get("schema_digest"));
    publish(
        catalog,
        [
            object(table_id, "TABLE", &format!("{CATALOG}.{SCHEMA}.{TABLE}")),
            grant(table_id, "TABLE", "SELECT", LifecycleState::Active),
            grant(table_id, "TABLE", "MODIFY", LifecycleState::Active),
        ],
    )
    .await;
    receipt
}

fn data_file(start: i64, amounts: &[i64]) -> Bytes {
    fixture_parquet(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(
                start..start + i64::try_from(amounts.len()).unwrap(),
            )),
            Arc::new(Int64Array::from(amounts.to_vec())),
        ],
    )
}

async fn verify_two_versions(
    router: &Router,
    catalog: &ScopedStorage,
    data: &ScopedStorage,
    table_id: &str,
    token: &str,
    evidence: &Path,
) -> Value {
    let paths = DeltaPaths::legacy(table_id.parse().unwrap());
    assert!(
        data.list_page_meta(paths.table_root(), None, 1)
            .await
            .unwrap()
            .objects
            .is_empty()
    );
    let fields = [("id", "long"), ("amount", "long")];
    let v0 =
        delta_fixture_commit(data, &paths, 0, data_file(1, &[1000, -200]), &fields, None).await;
    commit_delta(router, table_id, token, 0, &v0).await;
    let descriptor = extension_path("table-descriptors", table_id);
    let (status, first) = post_json(router, &descriptor, token, json!({"delta_version": 0})).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let pin = first.get("input_identity").unwrap();
    let v1 = delta_fixture_commit(data, &paths, 1, data_file(3, &[300]), &fields, None).await;
    commit_delta(router, table_id, token, 1, &v1).await;
    for expected in [(0, 800, 2), (1, 1100, 3)] {
        assert_delta_snapshot(router, table_id, token, pin, expected, Some(evidence)).await;
        let log = paths.delta_log_json(expected.0).unwrap();
        assert!(data.head_raw(&log).await.unwrap().is_some());
        assert!(catalog.head_raw(&log).await.unwrap().is_none());
    }
    assert!(
        catalog
            .head_raw(&paths.coordinator_state())
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        data.head_raw(&paths.coordinator_state())
            .await
            .unwrap()
            .is_none()
    );
    pin.clone()
}

async fn verify_preview(router: &Router, id: &str, location: &str, token: &str, evidence: &Path) {
    let body = json!({"table_id": id, "table_uri": location});
    let request = Request::get(PREVIEW)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let history: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 8192).await.unwrap()).unwrap();
    assert_eq!(
        history.get("latest_table_version").and_then(Value::as_i64),
        Some(1)
    );
    assert_eq!(
        history
            .get("commits")
            .and_then(Value::as_array)
            .unwrap()
            .len(),
        2
    );
    let backfill = json!({"table_id": id, "table_uri": location, "latest_backfilled_version": 0});
    let (status, response) = post_json(router, PREVIEW, token, backfill.clone()).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    write_proof_json(
        evidence,
        "preview.json",
        &json!({
            "path": PREVIEW, "get_request": body, "get_response": history,
            "post_request": backfill, "post_response": response,
        }),
    );
}

#[allow(clippy::too_many_arguments)]
async fn verify_denials(
    router: &Router,
    drifted: &[Router; 3],
    catalog: &ScopedStorage,
    id: &str,
    location: &str,
    token: &str,
    pin: &Value,
    evidence: &Path,
) {
    let read = extension_path("table-reads", id);
    let describe = extension_path("table-descriptors", id);
    let stage = format!("/api/v1/delta/tables/{id}/commits/stage");
    let read_body =
        json!({"delta_version": 0, "expected_snapshot_id": pin.get("snapshot_id").unwrap()});
    let preview = json!({"table_id": id, "table_uri": location, "latest_backfilled_version": 1});
    let mut controls = BTreeMap::new();
    for (changed, expected, name) in [
        (&drifted[0], StatusCode::CONFLICT, "prefix_drift"),
        (&drifted[1], StatusCode::CONFLICT, "bucket_drift"),
        (
            &drifted[2],
            StatusCode::SERVICE_UNAVAILABLE,
            "allocation_config_removed",
        ),
    ] {
        for (path, body) in [
            (read.as_str(), read_body.clone()),
            (stage.as_str(), json!({"payload": "{}"})),
            (PREVIEW, preview.clone()),
        ] {
            let (status, response) = post_json(changed, path, token, body).await;
            assert_eq!(status, expected, "{name} {path}: {response}");
            controls.insert(format!("{name} {path}"), response);
        }
    }
    for (path, body, expected, name) in [
        (
            read.as_str(),
            json!({"delta_version": 0, "expected_snapshot_id": "0".repeat(64)}),
            StatusCode::CONFLICT,
            "wrong_pin",
        ),
        (
            describe.as_str(),
            json!({"delta_version": 2}),
            StatusCode::NOT_FOUND,
            "uncommitted_version",
        ),
    ] {
        let (status, error) = post_json(router, path, token, body).await;
        assert_eq!(status, expected, "{error}");
        controls.insert(name.to_string(), error);
    }
    publish(
        catalog,
        [grant(id, "TABLE", "SELECT", LifecycleState::Deleted)],
    )
    .await;
    let (status, denied) = post_json(router, &read, token, read_body).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
    controls.insert("revoked_select".to_string(), denied);
    write_proof_json(evidence, "denials.json", &json!(controls));
}

async fn verify_isolation(
    router: &Router,
    catalog_backend: &Arc<dyn StorageBackend>,
    data_backend: &Arc<dyn StorageBackend>,
    root: &str,
    original_id: &str,
    request: &Value,
    evidence: &Path,
) {
    let other_tenant = "tenant2";
    let catalog = seed_scope(catalog_backend, other_tenant, WORKSPACE, root).await;
    seed_scope(catalog_backend, TENANT, "workspace2", root).await;
    let token = table_read_rs256_jwt(other_tenant, WORKSPACE, READER);
    let receipt = allocate(router, &catalog, &token, root, request).await;
    let id = receipt.get("id").and_then(Value::as_str).unwrap();
    assert_ne!(id, original_id);
    let describe = extension_path("table-descriptors", original_id);
    for cross_scope in [
        token.clone(),
        table_read_rs256_jwt(TENANT, "workspace2", READER),
    ] {
        let (status, error) =
            post_json(router, &describe, &cross_scope, json!({"delta_version": 0})).await;
        assert!(
            matches!(status, StatusCode::FORBIDDEN | StatusCode::NOT_FOUND),
            "{error}"
        );
    }
    let mut changed = request.clone();
    changed
        .as_object_mut()
        .unwrap()
        .insert("provisioning_fingerprint".to_string(), json!("different"));
    let (status, error) = post_json(router, ENSURE, &token, changed).await;
    assert_eq!(status, StatusCode::CONFLICT, "{error}");

    let data = analytical_scope(data_backend, other_tenant, WORKSPACE);
    let paths = DeltaPaths::legacy(id.parse().unwrap());
    let log = delta_fixture_commit(
        &data,
        &paths,
        0,
        data_file(1, &[300]),
        &[("id", "long"), ("amount", "long")],
        Some(json!({
            "minReaderVersion": 3, "minWriterVersion": 7,
            "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"]
        })),
    )
    .await;
    commit_delta(router, id, &token, 0, &log).await;
    let (status, unsupported) = post_json(
        router,
        &extension_path("table-descriptors", id),
        &token,
        json!({"delta_version": 0}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{unsupported}");
    assert_eq!(
        unsupported
            .pointer("/error/message")
            .and_then(Value::as_str),
        Some("delta_read_unsupported_log")
    );
    let preview_commit = json!({
        "table_id": id, "table_uri": receipt.get("location").unwrap(),
        "commit_info": {
            "version": 1, "timestamp": 1, "file_name": "00000000000000000001.json",
            "file_size": 1, "file_modification_timestamp": 1
        }
    });
    let (status, committed) = post_json(router, PREVIEW, &token, preview_commit.clone()).await;
    assert_eq!(status, StatusCode::OK, "{committed}");
    let preview_log = paths.delta_log_json(1).unwrap();
    assert!(data.head_raw(&preview_log).await.unwrap().is_some());
    assert!(catalog.head_raw(&preview_log).await.unwrap().is_none());
    write_proof_json(
        evidence,
        "isolation.json",
        &json!({
            "other_tenant_allocation": receipt, "changed_intent": error, "unsupported_protocol": unsupported,
            "preview_commit": {"request": preview_commit, "status": status.as_u16(), "response": committed}
        }),
    );
}

//! Cross-protocol proof for the exact-root `control/v1` catalog authority.

#![allow(clippy::expect_used)]

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::{Context, Result};
use arco_api::config::{Config, ControlV1CatalogRootConfig, ControlV1CursorKeyConfig};
use arco_api::server::Server;
use arco_catalog::CatalogAuthorityBindings;
use arco_core::storage::{MemoryBackend, StorageBackend};
use arco_iceberg::{IcebergConfig, IcebergState, iceberg_router};
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use prost::Message;
use serde_json::{Value, json};
use tower::ServiceExt;
use tracing_subscriber::fmt::MakeWriter;

use arco_proto::arco::catalog::v1::{CatalogDdlOperation, CreateCatalogOp, catalog_ddl_operation};
use arco_proto::arco::controlplane::v1::ApplyCatalogDdlRequest;

const TENANT: &str = "pilot-tenant";
const WORKSPACE: &str = "pilot-workspace";
const LEGACY_WORKSPACE: &str = "legacy-workspace";

#[derive(Clone, Default)]
struct StructuredLogCapture {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl StructuredLogCapture {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.bytes.lock().expect("log capture")).into_owned()
    }
}

struct StructuredLogWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl Write for StructuredLogWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes
            .lock()
            .map_err(|_| io::Error::other("structured log capture poisoned"))?
            .extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for StructuredLogCapture {
    type Writer = StructuredLogWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        StructuredLogWriter {
            bytes: Arc::clone(&self.bytes),
        }
    }
}

fn request(method: Method, uri: &str, body: Option<Value>) -> Result<Request<Body>> {
    request_in_scope(method, uri, body, TENANT, WORKSPACE, None)
}

fn request_in_scope(
    method: Method,
    uri: &str,
    body: Option<Value>,
    tenant: &str,
    workspace: &str,
    idempotency_key: Option<&str>,
) -> Result<Request<Body>> {
    let builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header(header::CONTENT_TYPE, "application/json");
    let builder = match idempotency_key {
        Some(key) => builder.header("Idempotency-Key", key),
        None => builder,
    };
    let body = body.map_or_else(Body::empty, |value| Body::from(value.to_string()));
    builder.body(body).context("build request")
}

async fn call_request(
    router: Router,
    request: Request<Body>,
) -> Result<(StatusCode, HeaderMap, Bytes)> {
    let response = router
        .oneshot(request)
        .await
        .map_err(|error| match error {})?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .context("read response")?;
    Ok((status, headers, body))
}

async fn call(
    router: Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> Result<(StatusCode, Bytes)> {
    let (status, _, body) = call_request(router, request(method, uri, body)?).await?;
    Ok((status, body))
}

fn assert_no_internal_state_token(headers: &HeaderMap, body: &[u8]) {
    for (name, value) in headers {
        let rendered = format!("{}:{}", name.as_str(), value.to_str().unwrap_or_default());
        let lowercase = rendered.to_ascii_lowercase();
        assert!(
            !lowercase.contains("statetoken"),
            "leaked header: {rendered}"
        );
        assert!(
            !lowercase.contains("state_token"),
            "leaked header: {rendered}"
        );
        assert!(
            !lowercase.contains("authority_manifest_id"),
            "leaked header: {rendered}"
        );
    }
    let rendered = String::from_utf8_lossy(body);
    let lowercase = rendered.to_ascii_lowercase();
    assert!(!lowercase.contains("statetoken"), "leaked body: {rendered}");
    assert!(
        !lowercase.contains("state_token"),
        "leaked body: {rendered}"
    );
    assert!(
        !lowercase.contains("authority_manifest_id"),
        "leaked body: {rendered}"
    );
}

fn pilot_config() -> Config {
    Config {
        debug: true,
        catalog_control_v1_root: Some(ControlV1CatalogRootConfig {
            tenant_id: TENANT.to_string(),
            workspace_id: WORKSPACE.to_string(),
        }),
        catalog_control_v1_cursor_key: Some(ControlV1CursorKeyConfig::new(
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        )),
        ..Config::default()
    }
}

fn protocol_bindings(config: &Config) -> Arc<CatalogAuthorityBindings> {
    Arc::new(
        config
            .catalog_authority_bindings()
            .expect("valid exact pilot binding"),
    )
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn native_uc_and_iceberg_reads_resolve_one_control_v1_authority() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let config = pilot_config();
    let bindings = protocol_bindings(&config);
    let native = Server::with_storage_backend(config, backend.clone()).test_router();

    let (status, _) = call(
        native.clone(),
        Method::POST,
        "/api/v1/catalogs",
        Some(json!({"name": "default"})),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = call(
        native.clone(),
        Method::POST,
        "/api/v1/catalogs/default/schemas",
        Some(json!({"name": "sales"})),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    let (status, native_body) = call(
        native.clone(),
        Method::POST,
        "/api/v1/catalogs/default/schemas/sales/tables",
        Some(json!({
            "name": "orders",
            "format": "iceberg",
            "location": "warehouse/sales/orders",
            "columns": [{
                "name": "order_id",
                "data_type": "STRING",
                "is_nullable": false,
                "ordinal": 0
            }]
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert!(!String::from_utf8_lossy(&native_body).contains("StateToken"));

    let mut projection_status = Value::Null;
    for _ in 0..50 {
        let (status, projection_status_body) = call(
            native.clone(),
            Method::POST,
            "/api/v1/query?format=json",
            Some(json!({
                "sql": "SELECT projection_kind, applied_authority_sequence, observed_head_sequence, lag, last_success_at_ms, failure_state FROM system.catalog.projection_status"
            })),
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        projection_status = serde_json::from_slice(&projection_status_body)?;
        let row = &projection_status[0];
        if row["applied_authority_sequence"].as_u64().is_some()
            && row["applied_authority_sequence"] == row["observed_head_sequence"]
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let status_row = &projection_status[0];
    assert_eq!(status_row["projection_kind"], "catalog-parquet-v1");
    assert!(status_row["observed_head_sequence"].as_u64().is_some());
    assert_eq!(
        status_row["applied_authority_sequence"], status_row["observed_head_sequence"],
        "the default post-commit notifier must wake projection without an operator request"
    );
    assert_eq!(status_row["lag"], 0);
    assert!(
        !status_row["last_success_at_ms"].is_null(),
        "a converged projection must report its last success: {status_row}"
    );
    assert!(status_row["failure_state"].is_null());

    let (status, query_data_body) = call(
        native.clone(),
        Method::POST,
        "/api/v1/query-data?format=json",
        Some(json!({"sql": "SELECT * FROM default.sales.orders"})),
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&query_data_body).contains("only parquet tables"));

    for (uri, body) in [
        ("/api/v1/catalog/inventory", None),
        (
            "/api/v1/browser/urls",
            Some(json!({"domain": "catalog", "paths": []})),
        ),
    ] {
        let method = if body.is_some() {
            Method::POST
        } else {
            Method::GET
        };
        let (status, body) = call(native.clone(), method, uri, body).await?;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let error: Value = serde_json::from_slice(&body)?;
        assert_eq!(error["code"], "CATALOG_AUTHORITY_PROJECTION_UNAVAILABLE");
    }

    let legacy_transaction = ApplyCatalogDdlRequest {
        ddl: Some(CatalogDdlOperation {
            op: Some(catalog_ddl_operation::Op::CreateCatalog(CreateCatalogOp {
                catalog: "legacy-bypass".to_string(),
                description: None,
            })),
        }),
    };
    let response = native
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/transactions/applyCatalogDdl")
                .header("X-Tenant-Id", TENANT)
                .header("X-Workspace-Id", WORKSPACE)
                .header("X-Request-Id", "req-legacy-bypass")
                .header("Idempotency-Key", "idem-legacy-bypass")
                .header(
                    header::CONTENT_TYPE,
                    "application/x-protobuf; proto=arco.controlplane.v1.ApplyCatalogDdlRequest",
                )
                .body(Body::from(legacy_transaction.encode_to_vec()))?,
        )
        .await
        .map_err(|error| match error {})?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[header::RETRY_AFTER], "5");
    let legacy_transaction_body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
    let legacy_transaction_error: Value = serde_json::from_slice(&legacy_transaction_body)?;
    assert_eq!(
        legacy_transaction_error["code"],
        "CATALOG_AUTHORITY_LEGACY_TRANSACTION_DISABLED"
    );

    let (status, catalogs_body) = call(native, Method::GET, "/api/v1/catalogs", None).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(!String::from_utf8_lossy(&catalogs_body).contains("legacy-bypass"));

    let uc = unity_catalog_router(
        UnityCatalogState::new(backend.clone()).with_catalog_authority_bindings(bindings.clone()),
    );
    let (status, uc_body) = call(
        uc,
        Method::GET,
        "/tables?catalog_name=default&schema_name=sales",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    let uc_value: Value = serde_json::from_slice(&uc_body)?;
    assert_eq!(uc_value["tables"][0]["name"], "orders");
    assert_eq!(uc_value["tables"][0]["data_source_format"], "ICEBERG");
    assert!(!String::from_utf8_lossy(&uc_body).contains("StateToken"));

    let iceberg =
        iceberg_router(IcebergState::new(backend).with_catalog_authority_bindings(bindings));
    let (status, iceberg_body) = call(
        iceberg,
        Method::GET,
        "/v1/arco/namespaces/sales/tables",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    let iceberg_value: Value = serde_json::from_slice(&iceberg_body)?;
    assert_eq!(iceberg_value["identifiers"][0]["name"], "orders");
    assert!(!String::from_utf8_lossy(&iceberg_body).contains("StateToken"));
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn iceberg_parent_filtered_pages_retain_the_parent_cut_and_bind_the_filter() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let config = pilot_config();
    let bindings = protocol_bindings(&config);
    let native = Server::with_storage_backend(config, backend.clone()).test_router();
    let (status, _) = call(
        native.clone(),
        Method::POST,
        "/api/v1/catalogs",
        Some(json!({"name": "default"})),
    )
    .await?;
    assert_eq!(StatusCode::CREATED, status);
    for name in ["a", "a.b", "a.c", "other", "other.x", "other.y"] {
        let (status, body) = call(
            native.clone(),
            Method::POST,
            "/api/v1/catalogs/default/schemas",
            Some(json!({"name": name})),
        )
        .await?;
        assert_eq!(
            StatusCode::CREATED,
            status,
            "create {name}: {}",
            String::from_utf8_lossy(&body)
        );
    }

    let uc = unity_catalog_router(
        UnityCatalogState::new(backend.clone()).with_catalog_authority_bindings(bindings.clone()),
    );
    let iceberg = iceberg_router(
        IcebergState::with_config(
            backend,
            IcebergConfig {
                namespace_separator: ".".to_string(),
                ..IcebergConfig::default()
            },
        )
        .with_catalog_authority_bindings(bindings),
    );

    let (status, first_body) = call(
        iceberg.clone(),
        Method::GET,
        "/v1/arco/namespaces?parent=a&pageSize=1",
        None,
    )
    .await?;
    assert_eq!(StatusCode::OK, status);
    let first: Value = serde_json::from_slice(&first_body)?;
    assert_eq!(first["namespaces"], json!([["a", "b"]]));
    let cursor = first["next-page-token"]
        .as_str()
        .or_else(|| first["next_page_token"].as_str())
        .context("Iceberg parent continuation")?;

    let (status, body) = call(
        uc.clone(),
        Method::PATCH,
        "/schemas/default.a",
        Some(json!({"new_name": "renamed"})),
    )
    .await?;
    assert_eq!(
        StatusCode::OK,
        status,
        "rename parent: {}",
        String::from_utf8_lossy(&body)
    );
    let (status, second_body) = call(
        iceberg.clone(),
        Method::GET,
        &format!("/v1/arco/namespaces?parent=a&pageSize=1&pageToken={cursor}"),
        None,
    )
    .await?;
    assert_eq!(StatusCode::OK, status);
    assert_eq!(
        serde_json::from_slice::<Value>(&second_body)?["namespaces"],
        json!([["a", "c"]])
    );
    let (status, _) = call(
        iceberg.clone(),
        Method::GET,
        &format!("/v1/arco/namespaces?parent=other&pageSize=1&pageToken={cursor}"),
        None,
    )
    .await?;
    assert_eq!(
        StatusCode::BAD_REQUEST,
        status,
        "a sealed continuation cannot be replayed under a different parent filter"
    );

    let (status, first_other_body) = call(
        iceberg.clone(),
        Method::GET,
        "/v1/arco/namespaces?parent=other&pageSize=1",
        None,
    )
    .await?;
    assert_eq!(StatusCode::OK, status);
    let first_other: Value = serde_json::from_slice(&first_other_body)?;
    assert_eq!(first_other["namespaces"], json!([["other", "x"]]));
    let other_cursor = first_other["next-page-token"]
        .as_str()
        .or_else(|| first_other["next_page_token"].as_str())
        .context("Iceberg recreated-parent continuation")?;
    let (status, body) = call(
        uc.clone(),
        Method::DELETE,
        "/schemas/default.other?force=true",
        None,
    )
    .await?;
    assert_eq!(
        StatusCode::OK,
        status,
        "delete parent: {}",
        String::from_utf8_lossy(&body)
    );
    let (status, body) = call(
        uc,
        Method::POST,
        "/schemas",
        Some(json!({"name": "other", "catalog_name": "default"})),
    )
    .await?;
    assert_eq!(
        StatusCode::OK,
        status,
        "recreate parent: {}",
        String::from_utf8_lossy(&body)
    );
    let (status, second_other_body) = call(
        iceberg,
        Method::GET,
        &format!("/v1/arco/namespaces?parent=other&pageSize=1&pageToken={other_cursor}"),
        None,
    )
    .await?;
    assert_eq!(StatusCode::OK, status);
    assert_eq!(
        serde_json::from_slice::<Value>(&second_other_body)?["namespaces"],
        json!([["other", "y"]])
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn every_protocol_mutates_one_authority_while_unbound_roots_remain_legacy() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let config = pilot_config();
    let bindings = protocol_bindings(&config);
    let native = Server::with_storage_backend(config, backend.clone()).test_router();
    let uc = unity_catalog_router(
        UnityCatalogState::new(backend.clone()).with_catalog_authority_bindings(bindings.clone()),
    );
    let iceberg = iceberg_router(
        IcebergState::with_config(
            backend.clone(),
            IcebergConfig {
                allow_write: true,
                allow_namespace_crud: true,
                allow_table_crud: true,
                ..IcebergConfig::default()
            },
        )
        .with_catalog_authority_bindings(bindings),
    );

    let (status, headers, body) = call_request(
        native.clone(),
        request_in_scope(
            Method::POST,
            "/api/v1/catalogs",
            Some(json!({"name": "default"})),
            TENANT,
            WORKSPACE,
            Some("native-default-catalog"),
        )?,
    )
    .await?;
    assert_eq!(StatusCode::CREATED, status);
    assert_no_internal_state_token(&headers, &body);

    let uc_schema_request = || {
        request_in_scope(
            Method::POST,
            "/schemas",
            Some(json!({"name": "uc_owned", "catalog_name": "default"})),
            TENANT,
            WORKSPACE,
            Some("uc-create-schema"),
        )
    };
    let (first_status, first_headers, first_body) =
        call_request(uc.clone(), uc_schema_request()?).await?;
    let (replay_status, replay_headers, replay_body) =
        call_request(uc.clone(), uc_schema_request()?).await?;
    assert_eq!(
        StatusCode::OK,
        first_status,
        "UC schema create: {}",
        String::from_utf8_lossy(&first_body)
    );
    assert_eq!(first_status, replay_status);
    assert_eq!(first_body, replay_body);
    assert_no_internal_state_token(&first_headers, &first_body);
    assert_no_internal_state_token(&replay_headers, &replay_body);

    let iceberg_namespace_request = || {
        request_in_scope(
            Method::POST,
            "/v1/arco/namespaces",
            Some(json!({"namespace": ["iceberg_owned"], "properties": {}})),
            TENANT,
            WORKSPACE,
            Some("01941234-5678-7def-8abc-123456789a01"),
        )
    };
    let (first_status, first_headers, first_body) =
        call_request(iceberg.clone(), iceberg_namespace_request()?).await?;
    let (replay_status, replay_headers, replay_body) =
        call_request(iceberg.clone(), iceberg_namespace_request()?).await?;
    assert_eq!(
        StatusCode::OK,
        first_status,
        "Iceberg namespace create: {}",
        String::from_utf8_lossy(&first_body)
    );
    assert_eq!(first_status, replay_status);
    assert_eq!(
        serde_json::from_slice::<Value>(&first_body)?,
        serde_json::from_slice::<Value>(&replay_body)?
    );
    assert_no_internal_state_token(&first_headers, &first_body);
    assert_no_internal_state_token(&replay_headers, &replay_body);

    let (status, headers, body) = call_request(
        iceberg.clone(),
        request_in_scope(
            Method::POST,
            "/v1/arco/namespaces/iceberg_owned/tables",
            Some(json!({
                "name": "iceberg_table",
                "schema": {
                    "schema-id": 0,
                    "type": "struct",
                    "fields": [{"id": 1, "name": "id", "type": "long", "required": true}]
                },
                "properties": {}
            })),
            TENANT,
            WORKSPACE,
            Some("01941234-5678-7def-8abc-123456789a02"),
        )?,
    )
    .await?;
    assert_eq!(
        StatusCode::OK,
        status,
        "Iceberg table create: {}",
        String::from_utf8_lossy(&body)
    );
    assert_no_internal_state_token(&headers, &body);

    let (status, headers, body) = call_request(
        iceberg.clone(),
        request_in_scope(
            Method::POST,
            "/v1/arco/tables/rename",
            Some(json!({
                "source": {"namespace": ["iceberg_owned"], "name": "iceberg_table"},
                "destination": {"namespace": ["iceberg_owned"], "name": "iceberg_table_v2"}
            })),
            TENANT,
            WORKSPACE,
            Some("01941234-5678-7def-8abc-123456789a03"),
        )?,
    )
    .await?;
    assert_eq!(StatusCode::NO_CONTENT, status);
    assert_no_internal_state_token(&headers, &body);

    let (status, native_schemas) = call(
        native.clone(),
        Method::GET,
        "/api/v1/catalogs/default/schemas",
        None,
    )
    .await?;
    assert_eq!(StatusCode::OK, status);
    let native_schemas: Value = serde_json::from_slice(&native_schemas)?;
    let native_schema_names = native_schemas["schemas"]
        .as_array()
        .context("native schemas array")?
        .iter()
        .filter_map(|schema| schema["name"].as_str())
        .collect::<Vec<_>>();
    assert!(
        native_schema_names.contains(&"uc_owned"),
        "native route did not observe the UC-created schema: {native_schema_names:?}"
    );
    let (status, native_iceberg_tables) = call(
        native.clone(),
        Method::GET,
        "/api/v1/catalogs/default/schemas/iceberg_owned/tables",
        None,
    )
    .await?;
    assert_eq!(StatusCode::OK, status);
    assert_eq!(
        serde_json::from_slice::<Value>(&native_iceberg_tables)?["tables"][0]["name"],
        "iceberg_table_v2"
    );

    let (status, uc_sees_iceberg) = call(
        uc.clone(),
        Method::GET,
        "/tables?catalog_name=default&schema_name=iceberg_owned",
        None,
    )
    .await?;
    assert_eq!(StatusCode::OK, status);
    assert_eq!(
        serde_json::from_slice::<Value>(&uc_sees_iceberg)?["tables"][0]["name"],
        "iceberg_table_v2"
    );

    let commit_request = request_in_scope(
        Method::POST,
        "/v1/arco/namespaces/iceberg_owned/tables/iceberg_table_v2",
        Some(json!({})),
        TENANT,
        WORKSPACE,
        Some("01941234-5678-7def-8abc-123456789a04"),
    )?;
    let (status, headers, body) = call_request(iceberg.clone(), commit_request).await?;
    assert_eq!(StatusCode::SERVICE_UNAVAILABLE, status);
    assert_eq!(
        Some("5"),
        headers
            .get(header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
    );
    assert_no_internal_state_token(&headers, &body);

    let race_one = call_request(
        native.clone(),
        request_in_scope(
            Method::POST,
            "/api/v1/catalogs",
            Some(json!({"name": "race"})),
            TENANT,
            WORKSPACE,
            Some("race-one"),
        )?,
    );
    let race_two = call_request(
        native.clone(),
        request_in_scope(
            Method::POST,
            "/api/v1/catalogs",
            Some(json!({"name": "race"})),
            TENANT,
            WORKSPACE,
            Some("race-two"),
        )?,
    );
    let (race_one, race_two) = tokio::join!(race_one, race_two);
    let race_one = race_one?;
    let race_two = race_two?;
    let mut race_statuses = [race_one.0, race_two.0];
    race_statuses.sort();
    assert_eq!([StatusCode::CREATED, StatusCode::CONFLICT], race_statuses);
    assert_no_internal_state_token(&race_one.1, &race_one.2);
    assert_no_internal_state_token(&race_two.1, &race_two.2);

    let legacy_create = || {
        request_in_scope(
            Method::POST,
            "/api/v1/catalogs",
            Some(json!({"name": "legacy"})),
            TENANT,
            LEGACY_WORKSPACE,
            Some("01941234-5678-7def-8abc-123456789a05"),
        )
    };
    let (legacy_first_status, legacy_first_headers, legacy_first_body) =
        call_request(native.clone(), legacy_create()?).await?;
    let (legacy_replay_status, legacy_replay_headers, legacy_replay_body) =
        call_request(native.clone(), legacy_create()?).await?;
    assert_eq!(
        StatusCode::CREATED,
        legacy_first_status,
        "legacy create: {}",
        String::from_utf8_lossy(&legacy_first_body)
    );
    assert_eq!(legacy_first_status, legacy_replay_status);
    assert_eq!(legacy_first_body, legacy_replay_body);
    assert_no_internal_state_token(&legacy_first_headers, &legacy_first_body);
    assert_no_internal_state_token(&legacy_replay_headers, &legacy_replay_body);

    for (name, idempotency_key) in [
        ("alpha", "01941234-5678-7def-8abc-123456789a11"),
        ("bravo", "01941234-5678-7def-8abc-123456789a12"),
        ("charlie", "01941234-5678-7def-8abc-123456789a13"),
    ] {
        let (status, _, body) = call_request(
            native.clone(),
            request_in_scope(
                Method::POST,
                "/api/v1/catalogs",
                Some(json!({"name": name})),
                TENANT,
                LEGACY_WORKSPACE,
                Some(idempotency_key),
            )?,
        )
        .await?;
        assert_eq!(
            StatusCode::CREATED,
            status,
            "legacy create {name}: {}",
            String::from_utf8_lossy(&body)
        );
    }
    let (status, _, first_page_body) = call_request(
        native.clone(),
        request_in_scope(
            Method::GET,
            "/api/v1/catalogs?limit=2",
            None,
            TENANT,
            LEGACY_WORKSPACE,
            None,
        )?,
    )
    .await?;
    assert_eq!(StatusCode::OK, status);
    let first_page: Value = serde_json::from_slice(&first_page_body)?;
    let expected_legacy_cursor = URL_SAFE_NO_PAD.encode(br#"{"key":"bravo"}"#);
    assert_eq!(first_page["next_cursor"], expected_legacy_cursor);
    let (status, _, resumed_body) = call_request(
        native,
        request_in_scope(
            Method::GET,
            &format!("/api/v1/catalogs?limit=2&cursor={expected_legacy_cursor}"),
            None,
            TENANT,
            LEGACY_WORKSPACE,
            None,
        )?,
    )
    .await?;
    assert_eq!(StatusCode::OK, status);
    let resumed: Value = serde_json::from_slice(&resumed_body)?;
    assert_eq!(resumed["catalogs"][0]["name"], "charlie");
    assert_eq!(resumed["catalogs"][1]["name"], "legacy");

    let pilot_objects = backend
        .list(&format!("tenant={TENANT}/workspace={WORKSPACE}/"))
        .await?;
    assert!(pilot_objects.iter().any(|object| {
        object
            .path
            .ends_with("/control/v1/domains/catalog/head/current.json")
    }));
    assert!(
        pilot_objects
            .iter()
            .all(|object| !object.path.contains("/ledger/catalog/")),
        "the bound pilot root must not invoke the legacy catalog writer"
    );
    let legacy_objects = backend
        .list(&format!("tenant={TENANT}/workspace={LEGACY_WORKSPACE}/"))
        .await?;
    assert!(
        legacy_objects
            .iter()
            .any(|object| object.path.contains("/ledger/catalog/"))
    );
    assert!(
        legacy_objects
            .iter()
            .any(|object| object.path.ends_with("/manifests/root.manifest.json")),
        "the unbound root must retain synchronous legacy compaction"
    );
    assert!(legacy_objects.iter().all(|object| {
        !object
            .path
            .contains("/control/v1/domains/catalog/head/current.json")
    }));

    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn internal_state_token_is_absent_from_structured_protocol_logs() -> Result<()> {
    let capture = StructuredLogCapture::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(capture.clone())
        .finish();
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    let backend = Arc::new(MemoryBackend::new());
    let config = pilot_config();
    let bindings = protocol_bindings(&config);
    let native = Server::with_storage_backend(config, backend.clone()).test_router();
    let (status, headers, body) = call_request(
        native.clone(),
        request_in_scope(
            Method::POST,
            "/api/v1/catalogs",
            Some(json!({"name": "logged"})),
            TENANT,
            WORKSPACE,
            Some("logged-create"),
        )?,
    )
    .await?;
    assert_eq!(StatusCode::CREATED, status);
    assert_no_internal_state_token(&headers, &body);

    let (error_status, error_headers, error_body) = call_request(
        native.clone(),
        request_in_scope(
            Method::POST,
            "/api/v1/catalogs",
            Some(json!({"name": "logged"})),
            TENANT,
            WORKSPACE,
            Some("logged-conflict"),
        )?,
    )
    .await?;
    assert_eq!(StatusCode::CONFLICT, error_status);
    assert_no_internal_state_token(&error_headers, &error_body);

    for name in ["logged-b", "logged-c"] {
        let (status, headers, body) = call_request(
            native.clone(),
            request_in_scope(
                Method::POST,
                "/api/v1/catalogs",
                Some(json!({"name": name})),
                TENANT,
                WORKSPACE,
                Some(&format!("create-{name}")),
            )?,
        )
        .await?;
        assert_eq!(StatusCode::CREATED, status);
        assert_no_internal_state_token(&headers, &body);
    }

    let pointer_bytes = backend
        .get(&format!(
            "tenant={TENANT}/workspace={WORKSPACE}/control/v1/domains/catalog/head/current.json"
        ))
        .await?;
    let pointer: Value = serde_json::from_slice(&pointer_bytes)?;
    let manifest_id = pointer["manifest_id"]
        .as_str()
        .context("catalog pointer manifest id")?;

    let uc = unity_catalog_router(
        UnityCatalogState::new(backend).with_catalog_authority_bindings(bindings),
    );
    let (status, headers, page_body) = call_request(
        uc.clone(),
        request_in_scope(
            Method::GET,
            "/catalogs?max_results=1",
            None,
            TENANT,
            WORKSPACE,
            None,
        )?,
    )
    .await?;
    assert_eq!(StatusCode::OK, status);
    assert_no_internal_state_token(&headers, &page_body);
    assert!(
        !String::from_utf8_lossy(&page_body).contains(manifest_id),
        "sealed cursor response exposed its retained manifest"
    );
    let page: Value = serde_json::from_slice(&page_body)?;
    let cursor = page["next_page_token"]
        .as_str()
        .context("UC continuation")?;
    let (resumed_status, resumed_headers, resumed_body) = call_request(
        uc.clone(),
        request_in_scope(
            Method::GET,
            &format!("/catalogs?max_results=1&page_token={cursor}"),
            None,
            TENANT,
            WORKSPACE,
            None,
        )?,
    )
    .await?;
    assert_eq!(StatusCode::OK, resumed_status);
    assert_no_internal_state_token(&resumed_headers, &resumed_body);
    let mut tampered = cursor.as_bytes().to_vec();
    let last = tampered.last_mut().context("non-empty continuation")?;
    *last = if *last == b'A' { b'B' } else { b'A' };
    let tampered = String::from_utf8(tampered)?;
    let (tampered_status, tampered_headers, tampered_body) = call_request(
        uc,
        request_in_scope(
            Method::GET,
            &format!("/catalogs?max_results=1&page_token={tampered}"),
            None,
            TENANT,
            WORKSPACE,
            None,
        )?,
    )
    .await?;
    assert_eq!(StatusCode::BAD_REQUEST, tampered_status);
    assert_no_internal_state_token(&tampered_headers, &tampered_body);

    let logs = capture.contents();
    let lowercase = logs.to_ascii_lowercase();
    assert!(!lowercase.contains("statetoken"), "leaked logs: {logs}");
    assert!(!lowercase.contains("state_token"), "leaked logs: {logs}");
    assert!(
        !lowercase.contains("authority_manifest_id"),
        "leaked logs: {logs}"
    );
    assert!(
        !logs.contains(manifest_id),
        "authority manifest from the internal StateToken leaked into structured logs: {logs}"
    );
    Ok(())
}

#[derive(Default)]
struct CacheReadBackend {
    inner: MemoryBackend,
    payloads: std::sync::atomic::AtomicUsize,
    heads: std::sync::atomic::AtomicUsize,
}
impl CacheReadBackend {
    #[allow(clippy::case_sensitive_file_extension_comparisons)] // Canonical immutable object paths.
    fn payload(&self, path: &str) {
        if path.contains("/transactions/")
            || path.contains("/indexes/")
            || path.ends_with(".arrow")
            || path.ends_with(".index.json")
        {
            self.payloads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}
#[async_trait::async_trait]
impl StorageBackend for CacheReadBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.payload(path);
        self.inner.get(path).await
    }
    async fn get_range(&self, path: &str, range: std::ops::Range<u64>) -> arco_core::Result<Bytes> {
        self.payload(path);
        self.inner.get_range(path, range).await
    }
    async fn head(&self, path: &str) -> arco_core::Result<Option<arco_core::storage::ObjectMeta>> {
        self.heads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.head(path).await
    }
    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: arco_core::storage::WritePrecondition,
    ) -> arco_core::Result<arco_core::storage::WriteResult> {
        self.inner.put(path, data, precondition).await
    }
    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }
    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<arco_core::storage::ObjectMeta>> {
        self.inner.list(prefix).await
    }
    async fn signed_url(
        &self,
        path: &str,
        expiry: std::time::Duration,
    ) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }

    async fn list_page(
        &self,
        prefix: &str,
        continuation: Option<&str>,
        limit: usize,
    ) -> arco_core::Result<arco_core::storage::ListPage> {
        self.inner.list_page(prefix, continuation, limit).await
    }
}

#[tokio::test]
async fn native_warmth_is_reused_by_first_uc_and_iceberg_requests() -> Result<()> {
    use arco_api::server::AppState;
    use arco_catalog::{
        CatalogProjectionNotifier, ControlCatalogAuthority, ProjectionIntentV1, StateScope,
        WriteOptions,
    };
    use std::sync::atomic::Ordering;
    struct Quiet;
    impl CatalogProjectionNotifier for Quiet {
        fn notify(&self, _: &ProjectionIntentV1) -> arco_catalog::Result<()> {
            Ok(())
        }
    }
    let backend = Arc::new(CacheReadBackend::default());
    let config = pilot_config();
    let bindings = protocol_bindings(&config);
    let storage = arco_core::ScopedStorage::new(backend.clone(), TENANT, WORKSPACE)?;
    ControlCatalogAuthority::new(storage, StateScope::new(TENANT, WORKSPACE, "catalog"))?
        .with_projection_notifier(Arc::new(Quiet))
        .create_catalog("default", None, WriteOptions::default())
        .await?;
    let state = Arc::new(
        AppState::new(config, backend.clone()).with_catalog_authority_bindings(bindings.clone()),
    );
    let native = arco_api::routes::api_v1_routes()
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            arco_api::context::auth_middleware,
        ))
        .with_state(state);
    let uc = unity_catalog_router(
        UnityCatalogState::new(backend.clone()).with_catalog_authority_bindings(bindings.clone()),
    );
    let iceberg = iceberg_router(
        IcebergState::new(backend.clone()).with_catalog_authority_bindings(bindings),
    );
    let (status, _) = call(native.clone(), Method::GET, "/catalogs", None).await?;
    assert_eq!(status, StatusCode::OK);
    for (router, path) in [
        (native, "/catalogs"),
        (uc, "/catalogs"),
        (iceberg, "/v1/arco/namespaces"),
    ] {
        backend.payloads.store(0, Ordering::SeqCst);
        backend.heads.store(0, Ordering::SeqCst);
        let (status, body) = call(router, Method::GET, path, None).await?;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        assert_eq!(backend.payloads.load(Ordering::SeqCst), 0, "{path}");
        assert!(backend.heads.load(Ordering::SeqCst) > 0, "{path}");
    }
    Ok(())
}

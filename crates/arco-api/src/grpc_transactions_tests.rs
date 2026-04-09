//! Round-trip tests for gRPC control-plane transaction transport.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Server};

use super::service;
use crate::server::AppState;
use crate::{config::Posture, rate_limit::RateLimitConfig};
use arco_core::control_plane_transactions::ControlPlaneTxPaths;
use arco_core::storage::{MemoryBackend, StorageBackend};
use arco_core::{ControlPlaneTxDomain, ScopedStorage};
use arco_proto::catalog_ddl_operation::Op as CatalogDdlOp;
use arco_proto::control_plane_transaction_service_client::ControlPlaneTransactionServiceClient;
use arco_proto::domain_mutation;
use arco_proto::{
    ApplyCatalogDdlRequest, CatalogDdlOperation, CommitOrchestrationBatchRequest,
    CommitRootTransactionRequest, CreateNamespaceOp, DomainMutation, GetCatalogTransactionRequest,
    GetOrchestrationTransactionRequest, GetRootTransactionRequest, OrchestrationBatchSpec,
    OrchestrationEventEnvelope, RequestHeader, TenantId, Timestamp, TransactionStatus, WorkspaceId,
};

const TENANT: &str = "test-tenant";
const WORKSPACE: &str = "test-workspace";
const TEST_JWT_SECRET: &str = "grpc-transaction-secret";
const TEST_JWT_ISSUER: &str = "https://issuer.example";
const TEST_JWT_AUDIENCE: &str = "arco-api";
const TEST_USER_ID: &str = "grpc-tester";

fn request_header(idempotency_key: &str, request_id: &str) -> RequestHeader {
    RequestHeader {
        tenant_id: Some(TenantId {
            value: TENANT.to_string(),
        }),
        workspace_id: Some(WorkspaceId {
            value: WORKSPACE.to_string(),
        }),
        trace_parent: String::new(),
        idempotency_key: idempotency_key.to_string(),
        request_time: None,
        request_id: request_id.to_string(),
    }
}

fn catalog_request(idempotency_key: &str, request_id: &str, name: &str) -> ApplyCatalogDdlRequest {
    ApplyCatalogDdlRequest {
        header: Some(request_header(idempotency_key, request_id)),
        ddl: Some(CatalogDdlOperation {
            op: Some(CatalogDdlOp::CreateNamespace(CreateNamespaceOp {
                name: name.to_string(),
                description: Some("grpc transaction test".to_string()),
                properties: Default::default(),
            })),
        }),
        require_visible: Some(true),
    }
}

fn orchestration_request(
    idempotency_key: &str,
    request_id: &str,
    run_id: &str,
) -> CommitOrchestrationBatchRequest {
    CommitOrchestrationBatchRequest {
        header: Some(request_header(idempotency_key, request_id)),
        events: vec![OrchestrationEventEnvelope {
            event_id: "01JTXORCH000000000000000001".to_string(),
            event_type: "RunTriggered".to_string(),
            event_version: 1,
            timestamp: Some(Timestamp {
                seconds: 1_776_000_000,
                nanos: 0,
            }),
            source: format!("arco-flow/{TENANT}/{WORKSPACE}"),
            idempotency_key: format!("event:{run_id}"),
            correlation_id: Some(run_id.to_string()),
            causation_id: None,
            payload_json: serde_json::to_vec(&serde_json::json!({
                "run_id": run_id,
                "plan_id": format!("plan-{run_id}"),
                "trigger": {
                    "type": "manual",
                    "user_id": "tester"
                },
                "root_assets": [],
                "labels": {}
            }))
            .expect("serialize orchestration payload"),
        }],
        require_visible: Some(true),
        allow_inline_merge: Some(false),
    }
}

fn root_request(
    idempotency_key: &str,
    request_id: &str,
    namespace: &str,
    run_id: &str,
) -> CommitRootTransactionRequest {
    CommitRootTransactionRequest {
        header: Some(request_header(idempotency_key, request_id)),
        mutations: vec![
            DomainMutation {
                kind: Some(domain_mutation::Kind::Catalog(CatalogDdlOperation {
                    op: Some(CatalogDdlOp::CreateNamespace(CreateNamespaceOp {
                        name: namespace.to_string(),
                        description: Some("grpc root transaction test".to_string()),
                        properties: Default::default(),
                    })),
                })),
            },
            DomainMutation {
                kind: Some(domain_mutation::Kind::Orchestration(
                    OrchestrationBatchSpec {
                        events: orchestration_request(idempotency_key, request_id, run_id).events,
                        allow_inline_merge: Some(false),
                    },
                )),
            },
        ],
    }
}

fn scoped_storage(backend: Arc<dyn StorageBackend>) -> ScopedStorage {
    ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage")
}

async fn load_catalog_tx_exists(backend: Arc<dyn StorageBackend>, tx_id: &str) -> Result<bool> {
    let storage = scoped_storage(backend);
    match storage
        .get_raw(&ControlPlaneTxPaths::record(
            ControlPlaneTxDomain::Catalog,
            tx_id,
        ))
        .await
    {
        Ok(_) => Ok(true),
        Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn spawn_grpc_client(
    backend: Arc<dyn StorageBackend>,
) -> Result<(
    ControlPlaneTransactionServiceClient<Channel>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let mut config = crate::config::Config::default();
    config.debug = true;
    spawn_grpc_client_with_config(config, backend).await
}

async fn spawn_grpc_client_with_config(
    config: crate::config::Config,
    backend: Arc<dyn StorageBackend>,
) -> Result<(
    ControlPlaneTransactionServiceClient<Channel>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let state = Arc::new(AppState::new(config, backend));
    let grpc_service = service(state);

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let stream = TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(grpc_service)
            .serve_with_incoming(stream)
            .await
    });

    let endpoint = Endpoint::from_shared(format!("http://{addr}"))?;
    let channel = endpoint.connect().await?;
    Ok((ControlPlaneTransactionServiceClient::new(channel), handle))
}

fn attach_transport_metadata<T>(
    request: T,
    tenant: &str,
    workspace: &str,
    idempotency_key: &str,
    request_id: &str,
) -> tonic::Request<T> {
    let mut request = tonic::Request::new(request);
    request.metadata_mut().insert(
        "x-tenant-id",
        MetadataValue::try_from(tenant).expect("tenant metadata"),
    );
    request.metadata_mut().insert(
        "x-workspace-id",
        MetadataValue::try_from(workspace).expect("workspace metadata"),
    );
    request.metadata_mut().insert(
        "idempotency-key",
        MetadataValue::try_from(idempotency_key).expect("idem metadata"),
    );
    request.metadata_mut().insert(
        "x-request-id",
        MetadataValue::try_from(request_id).expect("request-id metadata"),
    );
    request
}

fn attach_transport_auth_metadata<T>(
    request: T,
    idempotency_key: &str,
    request_id: &str,
    authorization: &str,
) -> tonic::Request<T> {
    let mut request = tonic::Request::new(request);
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(authorization).expect("authorization metadata"),
    );
    request.metadata_mut().insert(
        "idempotency-key",
        MetadataValue::try_from(idempotency_key).expect("idem metadata"),
    );
    request.metadata_mut().insert(
        "x-request-id",
        MetadataValue::try_from(request_id).expect("request-id metadata"),
    );
    request
}

fn make_test_jwt(tenant: &str, workspace: &str) -> Result<String> {
    #[derive(Debug, Serialize)]
    struct Claims<'a> {
        tenant: &'a str,
        workspace: &'a str,
        sub: &'a str,
        iss: &'a str,
        aud: &'a str,
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
        iss: TEST_JWT_ISSUER,
        aud: TEST_JWT_AUDIENCE,
        exp,
    };

    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes()),
    )
    .context("encode JWT")
}

fn metadata_str<'a>(metadata: &'a tonic::metadata::MetadataMap, key: &str) -> Option<&'a str> {
    metadata.get(key).and_then(|value| value.to_str().ok())
}

#[tokio::test]
async fn grpc_apply_catalog_ddl_and_lookup_round_trip() -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (mut client, handle) = spawn_grpc_client(backend.clone()).await?;
    let request = catalog_request("idem-grpc-cat-01", "req-grpc-cat-01", "grpc-analytics");

    let response = client
        .apply_catalog_ddl(attach_transport_metadata(
            request,
            TENANT,
            WORKSPACE,
            "idem-grpc-cat-01",
            "req-grpc-cat-01",
        ))
        .await?
        .into_inner();
    let receipt = response.receipt.context("grpc catalog receipt missing")?;

    assert!(load_catalog_tx_exists(backend, &receipt.tx_id).await?);

    let lookup = GetCatalogTransactionRequest {
        header: Some(request_header(
            "idem-grpc-cat-lookup-01",
            "req-grpc-cat-lookup-01",
        )),
        tx_id: receipt.tx_id.clone(),
    };
    let lookup = client
        .get_catalog_transaction(attach_transport_metadata(
            lookup,
            TENANT,
            WORKSPACE,
            "idem-grpc-cat-lookup-01",
            "req-grpc-cat-lookup-01",
        ))
        .await?
        .into_inner();
    let status = lookup.status.context("grpc catalog status missing")?;
    assert_eq!(status.status, TransactionStatus::Visible as i32);

    handle.abort();
    Ok(())
}

#[tokio::test]
async fn grpc_commit_orchestration_batch_and_lookup_round_trip() -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (mut client, handle) = spawn_grpc_client(backend).await?;
    let request =
        orchestration_request("idem-grpc-orch-01", "req-grpc-orch-01", "run-grpc-orch-01");

    let response = client
        .commit_orchestration_batch(attach_transport_metadata(
            request,
            TENANT,
            WORKSPACE,
            "idem-grpc-orch-01",
            "req-grpc-orch-01",
        ))
        .await?
        .into_inner();
    let receipt = response
        .receipt
        .context("grpc orchestration receipt missing")?;
    assert_ne!(receipt.commit_id, receipt.revision_ulid);

    let lookup = GetOrchestrationTransactionRequest {
        header: Some(request_header(
            "idem-grpc-orch-lookup-01",
            "req-grpc-orch-lookup-01",
        )),
        tx_id: receipt.tx_id,
    };
    let lookup = client
        .get_orchestration_transaction(attach_transport_metadata(
            lookup,
            TENANT,
            WORKSPACE,
            "idem-grpc-orch-lookup-01",
            "req-grpc-orch-lookup-01",
        ))
        .await?
        .into_inner();
    let status = lookup.status.context("grpc orchestration status missing")?;
    assert_eq!(status.status, TransactionStatus::Visible as i32);

    handle.abort();
    Ok(())
}

#[tokio::test]
async fn grpc_commit_root_transaction_and_lookup_round_trip() -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (mut client, handle) = spawn_grpc_client(backend).await?;
    let request = root_request(
        "idem-grpc-root-01",
        "req-grpc-root-01",
        "grpc-root-namespace",
        "run-grpc-root-01",
    );

    let response = client
        .commit_root_transaction(attach_transport_metadata(
            request,
            TENANT,
            WORKSPACE,
            "idem-grpc-root-01",
            "req-grpc-root-01",
        ))
        .await?
        .into_inner();
    let receipt = response.receipt.context("grpc root receipt missing")?;
    assert!(receipt.read_token.starts_with("root:"));
    assert_eq!(receipt.domain_commits.len(), 2);

    let lookup = GetRootTransactionRequest {
        header: Some(request_header(
            "idem-grpc-root-lookup-01",
            "req-grpc-root-lookup-01",
        )),
        tx_id: receipt.tx_id,
    };
    let lookup = client
        .get_root_transaction(attach_transport_metadata(
            lookup,
            TENANT,
            WORKSPACE,
            "idem-grpc-root-lookup-01",
            "req-grpc-root-lookup-01",
        ))
        .await?
        .into_inner();
    let status = lookup.status.context("grpc root status missing")?;
    assert_eq!(status.status, TransactionStatus::Visible as i32);

    handle.abort();
    Ok(())
}

#[tokio::test]
async fn grpc_applies_rate_limits_and_returns_request_metadata() -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let config = crate::config::Config {
        debug: true,
        rate_limit: RateLimitConfig {
            enabled: true,
            default_requests_per_minute: 1,
            url_minting_requests_per_minute: 1,
            burst_size: 1,
            max_tenant_entries: 10_000,
            tenant_entry_ttl_secs: 3600,
        },
        ..crate::config::Config::default()
    };
    let (mut client, handle) = spawn_grpc_client_with_config(config, backend).await?;

    let response = client
        .apply_catalog_ddl(attach_transport_metadata(
            catalog_request("idem-grpc-limit-01", "req-grpc-limit-01", "grpc-limit-01"),
            TENANT,
            WORKSPACE,
            "idem-grpc-limit-01",
            "req-grpc-limit-01",
        ))
        .await?;
    assert_eq!(
        metadata_str(response.metadata(), "x-request-id"),
        Some("req-grpc-limit-01")
    );
    assert_eq!(
        metadata_str(response.metadata(), "x-ratelimit-limit"),
        Some("1")
    );
    assert!(
        metadata_str(response.metadata(), "x-ratelimit-remaining").is_some(),
        "expected rate limit remaining metadata on success"
    );

    let error = client
        .apply_catalog_ddl(attach_transport_metadata(
            catalog_request("idem-grpc-limit-02", "req-grpc-limit-02", "grpc-limit-02"),
            TENANT,
            WORKSPACE,
            "idem-grpc-limit-02",
            "req-grpc-limit-02",
        ))
        .await
        .expect_err("second request should be rate limited");
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(
        metadata_str(error.metadata(), "x-request-id"),
        Some("req-grpc-limit-02")
    );
    assert_eq!(
        metadata_str(error.metadata(), "x-ratelimit-limit"),
        Some("1")
    );
    assert_eq!(
        metadata_str(error.metadata(), "x-ratelimit-remaining"),
        Some("0")
    );
    assert!(
        metadata_str(error.metadata(), "retry-after").is_some(),
        "expected retry-after metadata on rate limit errors"
    );

    handle.abort();
    Ok(())
}

#[tokio::test]
async fn grpc_accepts_bearer_jwt_in_production_mode() -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let config = crate::config::Config {
        debug: false,
        posture: Posture::Private,
        jwt: crate::config::JwtConfig {
            hs256_secret: Some(TEST_JWT_SECRET.to_string()),
            issuer: Some(TEST_JWT_ISSUER.to_string()),
            audience: Some(TEST_JWT_AUDIENCE.to_string()),
            ..crate::config::JwtConfig::default()
        },
        ..crate::config::Config::default()
    };
    let (mut client, handle) = spawn_grpc_client_with_config(config, backend).await?;
    let jwt = make_test_jwt(TENANT, WORKSPACE)?;

    let response = client
        .apply_catalog_ddl(attach_transport_auth_metadata(
            catalog_request("idem-grpc-jwt-01", "req-grpc-jwt-01", "grpc-prod-auth"),
            "idem-grpc-jwt-01",
            "req-grpc-jwt-01",
            &format!("Bearer {jwt}"),
        ))
        .await?;
    let receipt = response
        .into_inner()
        .receipt
        .context("grpc JWT catalog receipt missing")?;
    assert!(!receipt.tx_id.is_empty());

    handle.abort();
    Ok(())
}

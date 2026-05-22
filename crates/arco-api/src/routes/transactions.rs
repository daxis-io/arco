//! Protobuf-aligned HTTP routes for control-plane transaction APIs.

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Request, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use prost::Message;

use arco_proto::arco::controlplane::v1::{
    ApplyCatalogDdlRequest, CommitOrchestrationBatchRequest, CommitRootTransactionRequest,
    GetCatalogTransactionRequest, GetOrchestrationTransactionRequest, GetRootTransactionRequest,
};

use crate::context::RequestContext;
use crate::control_plane_transactions::ControlPlaneTransactionService;
use crate::error::ApiError;
use crate::server::AppState;

const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";
const APPLY_CATALOG_DDL_REQUEST_PROTO: &str = "arco.controlplane.v1.ApplyCatalogDdlRequest";
const GET_CATALOG_TRANSACTION_REQUEST_PROTO: &str =
    "arco.controlplane.v1.GetCatalogTransactionRequest";
const COMMIT_ORCHESTRATION_BATCH_REQUEST_PROTO: &str =
    "arco.controlplane.v1.CommitOrchestrationBatchRequest";
const GET_ORCHESTRATION_TRANSACTION_REQUEST_PROTO: &str =
    "arco.controlplane.v1.GetOrchestrationTransactionRequest";
const COMMIT_ROOT_TRANSACTION_REQUEST_PROTO: &str =
    "arco.controlplane.v1.CommitRootTransactionRequest";
const GET_ROOT_TRANSACTION_REQUEST_PROTO: &str = "arco.controlplane.v1.GetRootTransactionRequest";
const MAX_PROTO_BODY_BYTES: usize = 1024 * 1024;

/// Creates control-plane transaction routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/transactions/applyCatalogDdl", post(apply_catalog_ddl))
        .route(
            "/transactions/getCatalogTransaction",
            post(get_catalog_transaction),
        )
        .route(
            "/transactions/commitOrchestrationBatch",
            post(commit_orchestration_batch),
        )
        .route(
            "/transactions/getOrchestrationTransaction",
            post(get_orchestration_transaction),
        )
        .route(
            "/transactions/commitRootTransaction",
            post(commit_root_transaction),
        )
        .route(
            "/transactions/getRootTransaction",
            post(get_root_transaction),
        )
}

async fn apply_catalog_ddl(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let service = ControlPlaneTransactionService::new(state.as_ref(), ctx)?;
    let request =
        decode_protobuf::<ApplyCatalogDdlRequest>(request, APPLY_CATALOG_DDL_REQUEST_PROTO).await?;
    let response = service.apply_catalog_ddl(request).await?;
    Ok(encode_protobuf_response(&response))
}

async fn get_catalog_transaction(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let service = ControlPlaneTransactionService::new(state.as_ref(), ctx)?;
    let request = decode_protobuf::<GetCatalogTransactionRequest>(
        request,
        GET_CATALOG_TRANSACTION_REQUEST_PROTO,
    )
    .await?;
    let response = service.get_catalog_transaction(request).await?;
    Ok(encode_protobuf_response(&response))
}

async fn commit_orchestration_batch(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let service = ControlPlaneTransactionService::new(state.as_ref(), ctx)?;
    let request = decode_protobuf::<CommitOrchestrationBatchRequest>(
        request,
        COMMIT_ORCHESTRATION_BATCH_REQUEST_PROTO,
    )
    .await?;
    let response = service.commit_orchestration_batch(request).await?;
    Ok(encode_protobuf_response(&response))
}

async fn get_orchestration_transaction(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let service = ControlPlaneTransactionService::new(state.as_ref(), ctx)?;
    let request = decode_protobuf::<GetOrchestrationTransactionRequest>(
        request,
        GET_ORCHESTRATION_TRANSACTION_REQUEST_PROTO,
    )
    .await?;
    let response = service.get_orchestration_transaction(request).await?;
    Ok(encode_protobuf_response(&response))
}

async fn commit_root_transaction(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let service = ControlPlaneTransactionService::new(state.as_ref(), ctx)?;
    let request = decode_protobuf::<CommitRootTransactionRequest>(
        request,
        COMMIT_ROOT_TRANSACTION_REQUEST_PROTO,
    )
    .await?;
    let response = service.commit_root_transaction(request).await?;
    Ok(encode_protobuf_response(&response))
}

async fn get_root_transaction(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let service = ControlPlaneTransactionService::new(state.as_ref(), ctx)?;
    let request =
        decode_protobuf::<GetRootTransactionRequest>(request, GET_ROOT_TRANSACTION_REQUEST_PROTO)
            .await?;
    let response = service.get_root_transaction(request).await?;
    Ok(encode_protobuf_response(&response))
}

async fn decode_protobuf<T>(
    request: Request<Body>,
    expected_proto: &'static str,
) -> Result<T, ApiError>
where
    T: Message + Default,
{
    validate_protobuf_content_type(request.headers(), expected_proto)?;
    let body = to_bytes(request.into_body(), MAX_PROTO_BODY_BYTES)
        .await
        .map_err(|error| {
            ApiError::bad_request(format!("failed to read protobuf request body: {error}"))
        })?;
    T::decode(body)
        .map_err(|error| ApiError::bad_request(format!("invalid protobuf request body: {error}")))
}

fn validate_protobuf_content_type(
    headers: &HeaderMap,
    expected_proto: &'static str,
) -> Result<(), ApiError> {
    let Some(content_type) = headers.get(header::CONTENT_TYPE) else {
        return Err(protobuf_contract_error(
            expected_proto,
            "missing Content-Type",
        ));
    };
    let content_type = content_type
        .to_str()
        .map_err(|_| protobuf_contract_error(expected_proto, "Content-Type is not valid ASCII"))?;
    let mut parts = content_type.split(';');
    let media_type = parts.next().unwrap_or_default().trim();
    if !media_type.eq_ignore_ascii_case(CONTENT_TYPE_PROTOBUF) {
        return Err(protobuf_contract_error(expected_proto, content_type));
    }

    for part in parts {
        let Some((name, value)) = part.split_once('=') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("proto") {
            let value = value.trim().trim_matches('"');
            if value == expected_proto {
                return Ok(());
            }
            return Err(protobuf_contract_error(expected_proto, content_type));
        }
    }

    Err(protobuf_contract_error(expected_proto, content_type))
}

fn protobuf_contract_error(expected_proto: &'static str, actual: &str) -> ApiError {
    ApiError::unsupported_media_type(format!(
        "protobuf HTTP requests must use Content-Type: {CONTENT_TYPE_PROTOBUF}; proto={expected_proto}; got {actual}"
    ))
}

fn encode_protobuf_response<T>(message: &T) -> Response
where
    T: Message,
{
    (
        [(header::CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)],
        message.encode_to_vec(),
    )
        .into_response()
}

//! Protobuf-aligned HTTP routes for control-plane transaction APIs.

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{Request, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use prost::Message;

use arco_proto::{
    ApplyCatalogDdlRequest, CommitOrchestrationBatchRequest, CommitRootTransactionRequest,
    GetCatalogTransactionRequest, GetOrchestrationTransactionRequest, GetRootTransactionRequest,
};

use crate::context::RequestContext;
use crate::control_plane_transactions::ControlPlaneTransactionService;
use crate::error::ApiError;
use crate::server::AppState;

const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";
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
    let request = decode_protobuf::<ApplyCatalogDdlRequest>(request).await?;
    let response = service.apply_catalog_ddl(request).await?;
    Ok(encode_protobuf_response(&response))
}

async fn get_catalog_transaction(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let service = ControlPlaneTransactionService::new(state.as_ref(), ctx)?;
    let request = decode_protobuf::<GetCatalogTransactionRequest>(request).await?;
    let response = service.get_catalog_transaction(request).await?;
    Ok(encode_protobuf_response(&response))
}

async fn commit_orchestration_batch(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let service = ControlPlaneTransactionService::new(state.as_ref(), ctx)?;
    let request = decode_protobuf::<CommitOrchestrationBatchRequest>(request).await?;
    let response = service.commit_orchestration_batch(request).await?;
    Ok(encode_protobuf_response(&response))
}

async fn get_orchestration_transaction(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let service = ControlPlaneTransactionService::new(state.as_ref(), ctx)?;
    let request = decode_protobuf::<GetOrchestrationTransactionRequest>(request).await?;
    let response = service.get_orchestration_transaction(request).await?;
    Ok(encode_protobuf_response(&response))
}

async fn commit_root_transaction(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let service = ControlPlaneTransactionService::new(state.as_ref(), ctx)?;
    let request = decode_protobuf::<CommitRootTransactionRequest>(request).await?;
    let response = service.commit_root_transaction(request).await?;
    Ok(encode_protobuf_response(&response))
}

async fn get_root_transaction(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let service = ControlPlaneTransactionService::new(state.as_ref(), ctx)?;
    let request = decode_protobuf::<GetRootTransactionRequest>(request).await?;
    let response = service.get_root_transaction(request).await?;
    Ok(encode_protobuf_response(&response))
}

async fn decode_protobuf<T>(request: Request<Body>) -> Result<T, ApiError>
where
    T: Message + Default,
{
    let body = to_bytes(request.into_body(), MAX_PROTO_BODY_BYTES)
        .await
        .map_err(|error| {
            ApiError::bad_request(format!("failed to read protobuf request body: {error}"))
        })?;
    T::decode(body)
        .map_err(|error| ApiError::bad_request(format!("invalid protobuf request body: {error}")))
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

//! gRPC transport adapters for control-plane transaction APIs.

use std::sync::Arc;
use std::time::Instant;

use axum::http::{HeaderMap, HeaderValue};
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Code, Request, Response, Status};

use arco_proto::control_plane_transaction_service_server::{
    ControlPlaneTransactionService as ControlPlaneTransactionGrpc,
    ControlPlaneTransactionServiceServer,
};
use arco_proto::{
    ApplyCatalogDdlRequest, ApplyCatalogDdlResponse, CommitOrchestrationBatchRequest,
    CommitOrchestrationBatchResponse, CommitRootTransactionRequest, CommitRootTransactionResponse,
    GetCatalogTransactionRequest, GetCatalogTransactionResponse,
    GetOrchestrationTransactionRequest, GetOrchestrationTransactionResponse,
    GetRootTransactionRequest, GetRootTransactionResponse,
};

use crate::context::{REQUEST_ID_HEADER, RequestContext, request_context_from_headers};
use crate::control_plane_transactions::ControlPlaneTransactionService;
use crate::error::ApiError;
use crate::rate_limit::RateLimitResult;
use crate::server::AppState;

const AUTHORIZATION_HEADER: &str = "authorization";
const TENANT_HEADER: &str = "x-tenant-id";
const WORKSPACE_HEADER: &str = "x-workspace-id";
const IDEMPOTENCY_HEADER: &str = "idempotency-key";
const USER_HEADER: &str = "x-user-id";
const GROUPS_HEADER: &str = "x-groups";
const RATE_LIMIT_LIMIT_HEADER: &str = "x-ratelimit-limit";
const RATE_LIMIT_REMAINING_HEADER: &str = "x-ratelimit-remaining";
const RETRY_AFTER_HEADER: &str = "retry-after";

/// Returns the generated tonic service for transaction RPCs.
#[must_use]
pub(crate) fn service(
    state: Arc<AppState>,
) -> ControlPlaneTransactionServiceServer<GrpcControlPlaneTransactionService> {
    ControlPlaneTransactionServiceServer::new(GrpcControlPlaneTransactionService { state })
}

/// Tonic transport adapter for transaction RPCs.
#[derive(Debug, Clone)]
pub(crate) struct GrpcControlPlaneTransactionService {
    state: Arc<AppState>,
}

#[derive(Debug, Clone, Copy)]
struct GrpcRateLimitAllowance {
    limit: u32,
    remaining: u32,
}

#[cfg(test)]
#[path = "grpc_transactions_tests.rs"]
mod tests;

impl GrpcControlPlaneTransactionService {
    fn request_context(
        &self,
        metadata: &MetadataMap,
        resource: &str,
    ) -> Result<RequestContext, Status> {
        let headers = headers_from_metadata(metadata);
        match request_context_from_headers(&headers, self.state.as_ref()) {
            Ok(ctx) => {
                crate::audit::emit_auth_allow(self.state.as_ref(), &ctx, resource);
                Ok(ctx)
            }
            Err(error) => {
                let request_id = error
                    .request_id()
                    .map(str::to_owned)
                    .or_else(|| {
                        headers
                            .get(REQUEST_ID_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned)
                    })
                    .unwrap_or_else(|| ulid::Ulid::new().to_string());
                let reason = if error.code() == "MISSING_AUTH" {
                    crate::audit::REASON_MISSING_TOKEN
                } else {
                    crate::audit::REASON_INVALID_TOKEN
                };
                crate::audit::emit_auth_deny(self.state.as_ref(), &request_id, resource, reason);
                Err(api_error_to_status(error))
            }
        }
    }

    async fn request_context_and_rate_limit(
        &self,
        metadata: &MetadataMap,
        resource: &str,
    ) -> Result<(RequestContext, GrpcRateLimitAllowance), Status> {
        let ctx = self.request_context(metadata, resource)?;
        match self.state.rate_limit().check_default(&ctx.tenant).await {
            RateLimitResult::Allowed { limit, remaining } => {
                Ok((ctx, GrpcRateLimitAllowance { limit, remaining }))
            }
            RateLimitResult::Limited {
                limit,
                retry_after_secs,
            } => {
                tracing::warn!(
                    tenant = %ctx.tenant,
                    path = %resource,
                    endpoint = %resource,
                    request_id = %ctx.request_id,
                    limit,
                    retry_after_secs,
                    "Rate limit exceeded"
                );
                crate::metrics::record_rate_limit_hit(resource);
                Err(rate_limit_status(&ctx.request_id, limit, retry_after_secs))
            }
        }
    }
}

fn headers_from_metadata(metadata: &MetadataMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    insert_metadata_header(
        &mut headers,
        metadata,
        AUTHORIZATION_HEADER,
        "Authorization",
    );
    insert_metadata_header(&mut headers, metadata, TENANT_HEADER, "X-Tenant-Id");
    insert_metadata_header(&mut headers, metadata, WORKSPACE_HEADER, "X-Workspace-Id");
    insert_metadata_header(
        &mut headers,
        metadata,
        IDEMPOTENCY_HEADER,
        "Idempotency-Key",
    );
    insert_metadata_header(&mut headers, metadata, REQUEST_ID_HEADER, "X-Request-Id");
    insert_metadata_header(&mut headers, metadata, USER_HEADER, "X-User-Id");
    insert_metadata_header(&mut headers, metadata, GROUPS_HEADER, "X-Groups");
    headers
}

fn insert_metadata_header(
    headers: &mut HeaderMap,
    metadata: &MetadataMap,
    metadata_key: &'static str,
    header_name: &'static str,
) {
    let Some(value) = metadata.get(metadata_key) else {
        return;
    };
    let Ok(value) = value.to_str() else {
        return;
    };
    let Ok(value) = HeaderValue::from_str(value) else {
        return;
    };
    headers.insert(header_name, value);
}

fn api_error_to_status(error: ApiError) -> Status {
    let code = match error.status() {
        axum::http::StatusCode::BAD_REQUEST | axum::http::StatusCode::UNPROCESSABLE_ENTITY => {
            Code::InvalidArgument
        }
        axum::http::StatusCode::UNAUTHORIZED => Code::Unauthenticated,
        axum::http::StatusCode::FORBIDDEN => Code::PermissionDenied,
        axum::http::StatusCode::NOT_FOUND => Code::NotFound,
        axum::http::StatusCode::CONFLICT => Code::Aborted,
        axum::http::StatusCode::PRECONDITION_FAILED | axum::http::StatusCode::NOT_ACCEPTABLE => {
            Code::FailedPrecondition
        }
        axum::http::StatusCode::TOO_MANY_REQUESTS => Code::ResourceExhausted,
        axum::http::StatusCode::REQUEST_TIMEOUT => Code::DeadlineExceeded,
        axum::http::StatusCode::NOT_IMPLEMENTED => Code::Unimplemented,
        _ => Code::Internal,
    };

    let mut metadata = MetadataMap::new();
    if let Some(request_id) = error.request_id() {
        if let Ok(value) = MetadataValue::try_from(request_id) {
            metadata.insert(REQUEST_ID_HEADER, value);
        }
    }

    Status::with_metadata(code, error.message().to_string(), metadata)
}

fn insert_ascii_metadata(metadata: &mut MetadataMap, key: &'static str, value: &str) {
    if let Ok(parsed) = MetadataValue::try_from(value) {
        metadata.insert(key, parsed);
    }
}

fn request_metadata(request_id: &str, rate_limit: Option<GrpcRateLimitAllowance>) -> MetadataMap {
    let mut metadata = MetadataMap::new();
    insert_ascii_metadata(&mut metadata, REQUEST_ID_HEADER, request_id);
    if let Some(rate_limit) = rate_limit {
        if rate_limit.limit > 0 {
            insert_ascii_metadata(
                &mut metadata,
                RATE_LIMIT_LIMIT_HEADER,
                &rate_limit.limit.to_string(),
            );
            insert_ascii_metadata(
                &mut metadata,
                RATE_LIMIT_REMAINING_HEADER,
                &rate_limit.remaining.to_string(),
            );
        }
    }
    metadata
}

fn apply_success_metadata<T>(
    response: &mut Response<T>,
    request_id: &str,
    rate_limit: GrpcRateLimitAllowance,
) {
    let metadata = response.metadata_mut();
    insert_ascii_metadata(metadata, REQUEST_ID_HEADER, request_id);
    if rate_limit.limit > 0 {
        insert_ascii_metadata(
            metadata,
            RATE_LIMIT_LIMIT_HEADER,
            &rate_limit.limit.to_string(),
        );
        insert_ascii_metadata(
            metadata,
            RATE_LIMIT_REMAINING_HEADER,
            &rate_limit.remaining.to_string(),
        );
    }
}

fn rate_limit_status(request_id: &str, limit: u32, retry_after_secs: u64) -> Status {
    let mut metadata = request_metadata(
        request_id,
        Some(GrpcRateLimitAllowance {
            limit,
            remaining: 0,
        }),
    );
    insert_ascii_metadata(
        &mut metadata,
        RETRY_AFTER_HEADER,
        &retry_after_secs.to_string(),
    );
    Status::with_metadata(
        Code::ResourceExhausted,
        format!(
            "Rate limit exceeded. Limit: {limit} requests per minute. Retry after {retry_after_secs} seconds."
        ),
        metadata,
    )
}

fn finalize_rpc<T>(
    resource: &str,
    start: Instant,
    result: Result<Response<T>, Status>,
) -> Result<Response<T>, Status> {
    let code = result
        .as_ref()
        .map_or_else(|status| status.code(), |_| Code::Ok);
    crate::metrics::record_grpc_request(resource, code, start.elapsed().as_secs_f64());
    result
}

#[async_trait::async_trait]
impl ControlPlaneTransactionGrpc for GrpcControlPlaneTransactionService {
    async fn apply_catalog_ddl(
        &self,
        request: Request<ApplyCatalogDdlRequest>,
    ) -> Result<Response<ApplyCatalogDdlResponse>, Status> {
        const RESOURCE: &str = "/arco.v1.ControlPlaneTransactionService/ApplyCatalogDdl";
        let start = Instant::now();
        let result = async {
            let (ctx, rate_limit) = self
                .request_context_and_rate_limit(request.metadata(), RESOURCE)
                .await?;
            let span = tracing::info_span!(
                "grpc_request",
                endpoint = RESOURCE,
                tenant = %ctx.tenant,
                workspace = %ctx.workspace,
                request_id = %ctx.request_id
            );
            let _guard = span.enter();
            let service = ControlPlaneTransactionService::new(self.state.as_ref(), ctx.clone())
                .map_err(api_error_to_status)?;
            let response = service
                .apply_catalog_ddl(request.into_inner())
                .await
                .map_err(api_error_to_status)?;
            let mut response = Response::new(response);
            apply_success_metadata(&mut response, &ctx.request_id, rate_limit);
            Ok(response)
        }
        .await;
        finalize_rpc(RESOURCE, start, result)
    }

    async fn get_catalog_transaction(
        &self,
        request: Request<GetCatalogTransactionRequest>,
    ) -> Result<Response<GetCatalogTransactionResponse>, Status> {
        const RESOURCE: &str = "/arco.v1.ControlPlaneTransactionService/GetCatalogTransaction";
        let start = Instant::now();
        let result = async {
            let (ctx, rate_limit) = self
                .request_context_and_rate_limit(request.metadata(), RESOURCE)
                .await?;
            let span = tracing::info_span!(
                "grpc_request",
                endpoint = RESOURCE,
                tenant = %ctx.tenant,
                workspace = %ctx.workspace,
                request_id = %ctx.request_id
            );
            let _guard = span.enter();
            let service = ControlPlaneTransactionService::new(self.state.as_ref(), ctx.clone())
                .map_err(api_error_to_status)?;
            let response = service
                .get_catalog_transaction(request.into_inner())
                .await
                .map_err(api_error_to_status)?;
            let mut response = Response::new(response);
            apply_success_metadata(&mut response, &ctx.request_id, rate_limit);
            Ok(response)
        }
        .await;
        finalize_rpc(RESOURCE, start, result)
    }

    async fn commit_orchestration_batch(
        &self,
        request: Request<CommitOrchestrationBatchRequest>,
    ) -> Result<Response<CommitOrchestrationBatchResponse>, Status> {
        const RESOURCE: &str = "/arco.v1.ControlPlaneTransactionService/CommitOrchestrationBatch";
        let start = Instant::now();
        let result = async {
            let (ctx, rate_limit) = self
                .request_context_and_rate_limit(request.metadata(), RESOURCE)
                .await?;
            let span = tracing::info_span!(
                "grpc_request",
                endpoint = RESOURCE,
                tenant = %ctx.tenant,
                workspace = %ctx.workspace,
                request_id = %ctx.request_id
            );
            let _guard = span.enter();
            let service = ControlPlaneTransactionService::new(self.state.as_ref(), ctx.clone())
                .map_err(api_error_to_status)?;
            let response = service
                .commit_orchestration_batch(request.into_inner())
                .await
                .map_err(api_error_to_status)?;
            let mut response = Response::new(response);
            apply_success_metadata(&mut response, &ctx.request_id, rate_limit);
            Ok(response)
        }
        .await;
        finalize_rpc(RESOURCE, start, result)
    }

    async fn get_orchestration_transaction(
        &self,
        request: Request<GetOrchestrationTransactionRequest>,
    ) -> Result<Response<GetOrchestrationTransactionResponse>, Status> {
        const RESOURCE: &str =
            "/arco.v1.ControlPlaneTransactionService/GetOrchestrationTransaction";
        let start = Instant::now();
        let result = async {
            let (ctx, rate_limit) = self
                .request_context_and_rate_limit(request.metadata(), RESOURCE)
                .await?;
            let span = tracing::info_span!(
                "grpc_request",
                endpoint = RESOURCE,
                tenant = %ctx.tenant,
                workspace = %ctx.workspace,
                request_id = %ctx.request_id
            );
            let _guard = span.enter();
            let service = ControlPlaneTransactionService::new(self.state.as_ref(), ctx.clone())
                .map_err(api_error_to_status)?;
            let response = service
                .get_orchestration_transaction(request.into_inner())
                .await
                .map_err(api_error_to_status)?;
            let mut response = Response::new(response);
            apply_success_metadata(&mut response, &ctx.request_id, rate_limit);
            Ok(response)
        }
        .await;
        finalize_rpc(RESOURCE, start, result)
    }

    async fn commit_root_transaction(
        &self,
        request: Request<CommitRootTransactionRequest>,
    ) -> Result<Response<CommitRootTransactionResponse>, Status> {
        const RESOURCE: &str = "/arco.v1.ControlPlaneTransactionService/CommitRootTransaction";
        let start = Instant::now();
        let result = async {
            let (ctx, rate_limit) = self
                .request_context_and_rate_limit(request.metadata(), RESOURCE)
                .await?;
            let span = tracing::info_span!(
                "grpc_request",
                endpoint = RESOURCE,
                tenant = %ctx.tenant,
                workspace = %ctx.workspace,
                request_id = %ctx.request_id
            );
            let _guard = span.enter();
            let service = ControlPlaneTransactionService::new(self.state.as_ref(), ctx.clone())
                .map_err(api_error_to_status)?;
            let response = service
                .commit_root_transaction(request.into_inner())
                .await
                .map_err(api_error_to_status)?;
            let mut response = Response::new(response);
            apply_success_metadata(&mut response, &ctx.request_id, rate_limit);
            Ok(response)
        }
        .await;
        finalize_rpc(RESOURCE, start, result)
    }

    async fn get_root_transaction(
        &self,
        request: Request<GetRootTransactionRequest>,
    ) -> Result<Response<GetRootTransactionResponse>, Status> {
        const RESOURCE: &str = "/arco.v1.ControlPlaneTransactionService/GetRootTransaction";
        let start = Instant::now();
        let result = async {
            let (ctx, rate_limit) = self
                .request_context_and_rate_limit(request.metadata(), RESOURCE)
                .await?;
            let span = tracing::info_span!(
                "grpc_request",
                endpoint = RESOURCE,
                tenant = %ctx.tenant,
                workspace = %ctx.workspace,
                request_id = %ctx.request_id
            );
            let _guard = span.enter();
            let service = ControlPlaneTransactionService::new(self.state.as_ref(), ctx.clone())
                .map_err(api_error_to_status)?;
            let response = service
                .get_root_transaction(request.into_inner())
                .await
                .map_err(api_error_to_status)?;
            let mut response = Response::new(response);
            apply_success_metadata(&mut response, &ctx.request_id, rate_limit);
            Ok(response)
        }
        .await;
        finalize_rpc(RESOURCE, start, result)
    }
}

//! Owner allocation of managed Delta table storage.

use std::collections::BTreeMap;
use std::sync::Arc;

use arco_catalog::authz::decision::{AuthzDecision, AuthzRequest, DecisionOutcome};
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::{CatalogAuthority, ManagedDeltaAllocationRequest, WriteOptions};
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::catalog_authority;
use super::tables::ColumnDefinition;
use crate::context::RequestContext;
use crate::error::{ApiError, ApiErrorBody};
use crate::server::AppState;

/// Owner intent for a table in an existing schema.
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AllocateManagedDeltaRequest {
    /// `UUIDv7` reused for identical retries.
    operation_id: String,
    /// Existing catalog.
    catalog: String,
    /// Existing schema.
    schema: String,
    /// Logical table name.
    name: String,
    /// Optional table description.
    description: Option<String>,
    /// Fingerprint of the client's provisioning intent.
    provisioning_fingerprint: String,
    /// Columns in schema order.
    columns: Vec<ColumnDefinition>,
}

/// Catalog-verified placement returned to the owner.
#[derive(Serialize, ToSchema)]
pub struct ManagedDeltaAllocationResponse {
    /// Arco-selected table UUID.
    id: String,
    /// Catalog of the allocated table.
    catalog: String,
    /// Schema of the allocated table.
    schema: String,
    /// Table name.
    name: String,
    /// Approved analytical storage URI.
    location: String,
    /// Always `MANAGED` for this operation.
    table_type: String,
    /// Always `delta` for this operation.
    format: String,
    /// Persisted root, owner, schema digest and provisioning fingerprint.
    properties: BTreeMap<String, String>,
}

pub(super) fn routes() -> Router<Arc<AppState>> {
    Router::new().route("/managed-delta/ensure", post(ensure))
}

/// Allocates a managed Delta table for a principal with schema CREATE_TABLE.
///
/// # Errors
/// Returns an error on denied allocation, invalid intent, or catalog failure.
#[utoipa::path(
    post,
    path = "/api/v1/managed-delta/ensure",
    tag = "catalogs",
    request_body = AllocateManagedDeltaRequest,
    responses(
        (status = 201, description = "Arco-owned placement allocated or replayed", body = ManagedDeltaAllocationResponse),
        (status = 400, description = "Invalid intent", body = ApiErrorBody),
        (status = 403, description = "Schema CREATE_TABLE required", body = ApiErrorBody),
        (status = 404, description = "Allocation unavailable or schema absent", body = ApiErrorBody),
        (status = 409, description = "Operation or name conflict", body = ApiErrorBody),
    ),
    security(("bearerAuth" = []))
)]
pub async fn ensure(
    ctx: RequestContext,
    State(state): State<Arc<AppState>>,
    Json(request): Json<AllocateManagedDeltaRequest>,
) -> Result<(StatusCode, Json<ManagedDeltaAllocationResponse>), ApiError> {
    catalog_authority::reject_table_commit_for_control_v1(&state, &ctx)?;
    let binding = state
        .managed_delta_storage
        .as_ref()
        .ok_or_else(|| ApiError::not_found("managed Delta allocation is disabled"))?;
    let owner = ctx
        .user_id
        .as_deref()
        .filter(|_| !state.config.debug)
        .ok_or_else(|| {
            ApiError::forbidden("managed Delta allocation requires an authenticated owner")
        })?;
    if ctx
        .idempotency_key
        .as_deref()
        .is_some_and(|key| key != request.operation_id)
    {
        return Err(ApiError::bad_request(
            "operation_id and Idempotency-Key differ",
        ));
    }
    let authority = catalog_authority::resolve_read(&state, &ctx)?;
    let schema = authority
        .get_schema(&request.catalog, &request.schema)
        .await?
        .ok_or_else(|| ApiError::not_found("schema not found"))?;
    let permissions = state
        .catalog_permissions
        .compiled_permissions(&ctx.tenant, &ctx.workspace)
        .await
        .map_err(|_| ApiError::forbidden("managed Delta allocation permissions unavailable"))?;
    let check = AuthzRequest::new(owner, schema.id, "SCHEMA", Privilege::CreateTable)
        .with_request_id(&ctx.request_id);
    if AuthzDecision::evaluate(&check, &permissions).outcome != DecisionOutcome::Allow {
        return Err(ApiError::forbidden(
            "managed Delta allocation requires schema CREATE_TABLE",
        ));
    }

    let CatalogAuthority::Legacy { writer, .. } = authority else {
        return Err(ApiError::catalog_table_commit_disabled());
    };
    let opts = WriteOptions::default()
        .with_idempotency_key(request.operation_id)
        .with_request_id(&ctx.request_id)
        .with_actor(owner);
    let columns = request
        .columns
        .into_iter()
        .enumerate()
        .map(|(ordinal, column)| {
            let ordinal = i32::try_from(ordinal)
                .map_err(|_| ApiError::bad_request("too many managed Delta columns"))?;
            Ok(arco_catalog::ColumnDefinition {
                name: column.name,
                data_type: column.data_type,
                is_nullable: column.nullable,
                ordinal,
                description: column.description,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let table = writer
        .allocate_managed_delta(
            &request.catalog,
            &request.schema,
            ManagedDeltaAllocationRequest {
                name: request.name,
                description: request.description,
                owner: owner.to_string(),
                provisioning_fingerprint: request.provisioning_fingerprint,
                columns,
            },
            binding.root(),
            opts,
        )
        .await?;
    let incomplete = || ApiError::internal("managed Delta allocation metadata is incomplete");
    Ok((
        StatusCode::CREATED,
        Json(ManagedDeltaAllocationResponse {
            id: table.id,
            catalog: request.catalog,
            schema: request.schema,
            name: table.name,
            location: table.location.ok_or_else(incomplete)?,
            table_type: table.table_type.ok_or_else(incomplete)?,
            format: table.format.ok_or_else(incomplete)?,
            properties: table.properties.ok_or_else(incomplete)?,
        }),
    ))
}

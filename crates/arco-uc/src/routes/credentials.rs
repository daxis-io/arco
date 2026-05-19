//! Temporary credential routes for the Unity Catalog facade.
//!

use arco_catalog::CatalogReader;
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::credential_vending::{
    CredentialDecision, CredentialOperation, CredentialVendingEngine, CredentialVendingRequest,
    DEFAULT_CREDENTIAL_TTL,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::storage_governance::StorageGovernanceState;
use axum::Json;
use axum::Router;
use axum::extract::{Extension, OriginalUri, State};
use axum::http::Method;
use axum::http::StatusCode;
use axum::routing::post;
use serde::Deserialize;
use serde_json::json;

use crate::audit;
use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};
use crate::routes::common::{authz_denial_reason, map_catalog_error, scoped_storage};
use crate::state::UnityCatalogState;

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[schema(title = "GenerateTemporaryTableCredentialRequestBody")]
#[serde(default)]
pub(crate) struct GenerateTemporaryTableCredentialRequestBody {
    table_id: Option<String>,
    operation: Option<String>,
    requested_ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[schema(title = "GenerateTemporaryPathCredentialRequestBody")]
pub(crate) struct GenerateTemporaryPathCredentialRequestBody {
    /// Cloud storage URL (e.g. `gs://bucket/path`).
    url: String,
    /// Requested operation.
    operation: String,
    /// Requested TTL in seconds.
    requested_ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone)]
struct CredentialAuthzTarget {
    object_id: String,
    object_type: &'static str,
    privilege: Privilege,
}

/// Temporary credential route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route(
            "/temporary-model-version-credentials",
            post(post_temporary_model_version_credentials),
        )
        .route(
            "/temporary-table-credentials",
            post(post_temporary_table_credentials),
        )
        .route(
            "/temporary-volume-credentials",
            post(post_temporary_volume_credentials),
        )
        .route(
            "/temporary-path-credentials",
            post(post_temporary_path_credentials),
        )
}

/// `POST /temporary-model-version-credentials` (known UC operation; currently unsupported).
#[utoipa::path(
    post,
    path = "/temporary-model-version-credentials",
    tag = "TemporaryCredentials",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn post_temporary_model_version_credentials(
    method: Method,
    uri: OriginalUri,
) -> UnityCatalogError {
    super::common::known_but_unsupported(&method, &uri)
}

/// Generates temporary table credentials (preview scaffolding).
///
/// # Errors
///
/// Returns [`UnityCatalogError::NotImplemented`] while preview scaffolding is active.
#[utoipa::path(
    post,
    path = "/temporary-table-credentials",
    tag = "TemporaryCredentials",
    request_body = GenerateTemporaryTableCredentialRequestBody,
    responses(
        (status = 200, description = "Successful response."),
        (status = 501, description = "Endpoint is scaffolded but not yet implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_temporary_table_credentials(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    payload: Json<GenerateTemporaryTableCredentialRequestBody>,
) -> UnityCatalogResult<(StatusCode, Json<serde_json::Value>)> {
    let Json(payload) = payload;
    let table_id = payload
        .table_id
        .ok_or_else(|| UnityCatalogError::BadRequest {
            message: "missing table_id".to_string(),
        })?;
    let operation = payload
        .operation
        .ok_or_else(|| UnityCatalogError::BadRequest {
            message: "missing operation".to_string(),
        })?;
    let Some(operation) = CredentialOperation::from_uc_operation(&operation) else {
        return Err(UnityCatalogError::BadRequest {
            message: format!("unknown table operation: {operation}"),
        });
    };

    let storage = scoped_storage(&state, &ctx)?;
    let table = CatalogReader::new(storage)
        .get_table_by_id(&table_id)
        .await
        .map_err(map_catalog_error)?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("table not found: {table_id}"),
        })?;
    let location = table
        .location
        .ok_or_else(|| UnityCatalogError::BadRequest {
            message: format!("table has no storage location: {table_id}"),
        })?;

    vend_path_credentials(
        &state,
        &ctx,
        location,
        operation,
        Some(CredentialAuthzTarget {
            object_id: table_id,
            object_type: "TABLE",
            privilege: table_privilege_for_operation(operation),
        }),
        payload
            .requested_ttl_seconds
            .map_or(DEFAULT_CREDENTIAL_TTL, std::time::Duration::from_secs),
    )
    .await
}

/// `POST /temporary-volume-credentials` (known UC operation; currently unsupported).
#[utoipa::path(
    post,
    path = "/temporary-volume-credentials",
    tag = "TemporaryCredentials",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn post_temporary_volume_credentials(
    method: Method,
    uri: OriginalUri,
) -> UnityCatalogError {
    super::common::known_but_unsupported(&method, &uri)
}

/// Generates temporary path credentials.
///
/// # Errors
///
/// Returns [`UnityCatalogError::BadRequest`] if `operation` is not recognized.
#[utoipa::path(
    post,
    path = "/temporary-path-credentials",
    tag = "TemporaryCredentials",
    request_body = GenerateTemporaryPathCredentialRequestBody,
    responses(
        (status = 200, description = "Successful response."),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_temporary_path_credentials(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Json(request): Json<GenerateTemporaryPathCredentialRequestBody>,
) -> UnityCatalogResult<(StatusCode, Json<serde_json::Value>)> {
    let Some(operation) = CredentialOperation::from_uc_operation(&request.operation) else {
        return Err(UnityCatalogError::BadRequest {
            message: format!("unknown path operation: {}", request.operation),
        });
    };

    vend_path_credentials(
        &state,
        &ctx,
        request.url,
        operation,
        None,
        request
            .requested_ttl_seconds
            .map_or(DEFAULT_CREDENTIAL_TTL, std::time::Duration::from_secs),
    )
    .await
}

async fn vend_path_credentials(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    requested_path: String,
    operation: CredentialOperation,
    authz_target: Option<CredentialAuthzTarget>,
    requested_ttl: std::time::Duration,
) -> UnityCatalogResult<(StatusCode, Json<serde_json::Value>)> {
    let storage = scoped_storage(state, ctx)?;
    let ledger = MetastoreLedger::new(storage);
    let metastore = ledger.replay().await.map_err(map_catalog_error)?;
    let storage_governance =
        StorageGovernanceState::from_metastore_state(&metastore).map_err(map_catalog_error)?;
    let decision = CredentialVendingEngine::default()
        .decide_path(
            &storage_governance,
            &CredentialVendingRequest {
                principal_id: format!("tenant:{}", ctx.tenant),
                groups_snapshot_version: "unknown".to_string(),
                workspace_id: ctx.workspace.clone(),
                request_id: ctx.request_id.clone(),
                operation,
                requested_path: requested_path.clone(),
                requested_ttl,
                client_kind: "uc".to_string(),
                catalog_snapshot_version: metastore
                    .ledger_watermark
                    .unwrap_or_else(|| "empty".to_string()),
            },
        )
        .map_err(map_catalog_error)?;

    if decision.decision == CredentialDecision::Deny {
        audit::emit_credentials_deny(state, ctx, &requested_path, &decision.reason_code);
        return Err(UnityCatalogError::Forbidden {
            message: format!(
                "credential_scope_denied:{} audit_event_id={}",
                decision.reason_code, decision.audit_event_id
            ),
        });
    }

    let authz_target = match authz_target {
        Some(target) => target,
        None => storage_authz_target(&storage_governance, &decision, operation)?,
    };
    if let Some(reason_code) = credential_authz_denial_reason(state, ctx, &authz_target) {
        audit::emit_credentials_deny(state, ctx, &requested_path, &reason_code);
        return Err(UnityCatalogError::Forbidden {
            message: format!(
                "credential_scope_denied:{} audit_event_id={}",
                reason_code, decision.audit_event_id
            ),
        });
    }

    audit::emit_credentials_allow(state, ctx, &requested_path, &decision.reason_code);

    let credentials = decision
        .authorized_path_prefixes
        .iter()
        .map(|prefix| {
            json!({
                "prefix": prefix,
                "provider": decision.provider.as_deref(),
                "credential_kind": decision.credential_kind.as_deref()
            })
        })
        .collect::<Vec<_>>();

    let payload = json!({
        "decision": decision.decision.as_str(),
        "reason_code": decision.reason_code,
        "authorized_path_prefixes": decision.authorized_path_prefixes,
        "provider": decision.provider,
        "credential_kind": decision.credential_kind,
        "max_ttl_seconds": decision.max_ttl.as_secs(),
        "audit_event_id": decision.audit_event_id,
        "credentials": credentials,
    });

    Ok((StatusCode::OK, Json(payload)))
}

fn storage_authz_target(
    state: &StorageGovernanceState,
    decision: &arco_catalog::credential_vending::CredentialVendingDecision,
    operation: CredentialOperation,
) -> UnityCatalogResult<CredentialAuthzTarget> {
    let object_id =
        decision
            .authorized_object_id
            .clone()
            .ok_or_else(|| UnityCatalogError::Internal {
                message: "missing authorized storage governance object".to_string(),
            })?;
    let object_type = if state.get_external_location(&object_id).is_some() {
        "EXTERNAL_LOCATION"
    } else if state.get_managed_root(&object_id).is_some() {
        "MANAGED_ROOT"
    } else {
        return Err(UnityCatalogError::Internal {
            message: format!("unknown authorized storage governance object: {object_id}"),
        });
    };

    Ok(CredentialAuthzTarget {
        object_id,
        object_type,
        privilege: path_privilege_for_operation(operation),
    })
}

fn credential_authz_denial_reason(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    target: &CredentialAuthzTarget,
) -> Option<String> {
    authz_denial_reason(
        state,
        ctx,
        &target.object_id,
        target.object_type,
        target.privilege,
    )
}

fn table_privilege_for_operation(operation: CredentialOperation) -> Privilege {
    match operation {
        CredentialOperation::Read | CredentialOperation::List => Privilege::Select,
        CredentialOperation::Write => Privilege::Modify,
        CredentialOperation::Manage | CredentialOperation::Delete => Privilege::Manage,
    }
}

fn path_privilege_for_operation(operation: CredentialOperation) -> Privilege {
    match operation {
        CredentialOperation::Read | CredentialOperation::List => Privilege::ReadFiles,
        CredentialOperation::Write => Privilege::WriteFiles,
        CredentialOperation::Manage | CredentialOperation::Delete => Privilege::Manage,
    }
}

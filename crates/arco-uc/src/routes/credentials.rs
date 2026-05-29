//! Temporary credential routes for the Unity Catalog facade.
//!

use arco_catalog::CatalogReader;
use arco_catalog::authz::privileges::Privilege;
use arco_catalog::credential_vending::{
    CredentialDecision, CredentialOperation, CredentialVendingAuthorization,
    CredentialVendingEngine, CredentialVendingRequest, DEFAULT_CREDENTIAL_TTL,
};
use arco_catalog::error::CatalogError;
use arco_catalog::metastore::publish::load_published_storage_governance;
use arco_catalog::storage_governance::{PathDecision, StorageGovernanceState};
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
use crate::routes::common::{
    authz_context_denial_reason_for_watermark, authz_denial_reason_for_watermark,
    map_catalog_error, scoped_storage,
};
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
    permission_ledger_watermark: String,
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

/// Generates temporary table credential decisions.
///
/// # Errors
///
/// Returns a UC error response when the table, authorization, storage-governance
/// projection, or requested operation is invalid for credential vending.
#[utoipa::path(
    post,
    path = "/temporary-table-credentials",
    tag = "TemporaryCredentials",
    request_body = GenerateTemporaryTableCredentialRequestBody,
    responses(
        (status = 200, description = "Successful response."),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Credential request denied.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Table not found.", body = UnityCatalogErrorResponse),
        (status = 503, description = "Credential scope unavailable.", body = UnityCatalogErrorResponse),
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
    let authz_target = CredentialAuthzTarget {
        object_id: table_id.clone(),
        object_type: "TABLE",
        privilege: table_privilege_for_operation(operation),
        permission_ledger_watermark: String::new(),
    };
    if let Some(reason_code) = authz_context_denial_reason_for_watermark(&state, &ctx, None) {
        audit::emit_credentials_deny(&state, &ctx, &table_id, &reason_code);
        return credential_denied(&reason_code, None);
    }

    let storage = scoped_storage(&state, &ctx)?;
    let published = load_published_storage_governance(&storage)
        .await
        .map_err(|err| credential_projection_error(&err))?;
    let catalog_snapshot_version = published.ledger_watermark.clone();
    if let Some(reason_code) = credential_authz_denial_reason_for_watermark(
        &state,
        &ctx,
        &authz_target,
        Some(&catalog_snapshot_version),
    ) {
        audit::emit_credentials_deny(&state, &ctx, &table_id, &reason_code);
        return credential_denied(&reason_code, None);
    }

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
        Some(authz_target),
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
/// Returns a UC error response when the operation is unknown, authorization is
/// denied, or the published storage-governance projection is unavailable.
#[utoipa::path(
    post,
    path = "/temporary-path-credentials",
    tag = "TemporaryCredentials",
    request_body = GenerateTemporaryPathCredentialRequestBody,
    responses(
        (status = 200, description = "Successful response."),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Credential request denied.", body = UnityCatalogErrorResponse),
        (status = 503, description = "Credential scope unavailable.", body = UnityCatalogErrorResponse),
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
    if let Some(reason_code) = authz_context_denial_reason_for_watermark(&state, &ctx, None) {
        audit::emit_credentials_deny(&state, &ctx, &request.url, &reason_code);
        return credential_denied(&reason_code, None);
    }

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
    if let Some(reason_code) = authz_context_denial_reason_for_watermark(state, ctx, None) {
        audit::emit_credentials_deny(state, ctx, &requested_path, &reason_code);
        return credential_denied(&reason_code, None);
    }
    if !supports_vended_operation(operation) {
        audit::emit_credentials_deny(state, ctx, &requested_path, "unsupported_operation");
        return credential_denied("unsupported_operation", None);
    }

    let storage = scoped_storage(state, ctx)?;
    let published = load_published_storage_governance(&storage)
        .await
        .map_err(|err| credential_projection_error(&err))?;
    let catalog_snapshot_version = published.ledger_watermark.clone();
    if let Some(reason_code) =
        authz_context_denial_reason_for_watermark(state, ctx, Some(&catalog_snapshot_version))
    {
        audit::emit_credentials_deny(state, ctx, &requested_path, &reason_code);
        return credential_denied(&reason_code, None);
    }
    let storage_governance = published.state;

    let Ok(path_decision) = storage_governance.authority_for_path(&ctx.workspace, &requested_path)
    else {
        audit::emit_credentials_deny(state, ctx, &requested_path, "path_not_governed");
        return credential_denied("access_denied", None);
    };

    let mut authz_target = match authz_target {
        Some(target) => target,
        None => storage_authz_target(&storage_governance, &path_decision, operation)?,
    };
    if let Some(reason_code) = credential_authz_denial_reason_for_watermark(
        state,
        ctx,
        &authz_target,
        Some(&catalog_snapshot_version),
    ) {
        audit::emit_credentials_deny(state, ctx, &requested_path, &reason_code);
        return credential_denied("access_denied", None);
    }
    authz_target
        .permission_ledger_watermark
        .clone_from(&catalog_snapshot_version);

    let decision = CredentialVendingEngine::default()
        .decide_path(
            &storage_governance,
            &CredentialVendingRequest {
                principal_id: ctx.user_id.clone().unwrap_or_default(),
                groups_snapshot_version: "unknown".to_string(),
                workspace_id: ctx.workspace.clone(),
                request_id: ctx.request_id.clone(),
                operation,
                requested_path: requested_path.clone(),
                requested_ttl,
                client_kind: "uc".to_string(),
                catalog_snapshot_version,
                authorization: Some(CredentialVendingAuthorization {
                    principal_id: ctx.user_id.clone().unwrap_or_default(),
                    object_id: authz_target.object_id.clone(),
                    object_type: authz_target.object_type.to_string(),
                    privilege: authz_target.privilege,
                    permission_ledger_watermark: authz_target.permission_ledger_watermark,
                    path_authority_object_id: path_decision.object_id.clone(),
                    path_authority_object_type: path_authority_object_type(
                        &storage_governance,
                        &path_decision,
                    )?,
                }),
            },
        )
        .map_err(map_catalog_error)?;

    if decision.decision == CredentialDecision::Deny {
        audit::emit_credentials_deny(state, ctx, &requested_path, &decision.reason_code);
        return credential_denied(&decision.reason_code, Some(&decision.audit_event_id));
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

fn credential_projection_error(err: &CatalogError) -> UnityCatalogError {
    let message = err.to_string();
    UnityCatalogError::ServiceUnavailable {
        message: format!("credential_scope_unavailable:{message}"),
    }
}

fn storage_authz_target(
    state: &StorageGovernanceState,
    path_decision: &PathDecision,
    operation: CredentialOperation,
) -> UnityCatalogResult<CredentialAuthzTarget> {
    let object_id = path_decision.object_id.clone();
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
        permission_ledger_watermark: String::new(),
    })
}

fn credential_authz_denial_reason_for_watermark(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    target: &CredentialAuthzTarget,
    expected_ledger_watermark: Option<&str>,
) -> Option<String> {
    authz_denial_reason_for_watermark(
        state,
        ctx,
        &target.object_id,
        target.object_type,
        target.privilege,
        expected_ledger_watermark,
    )
}

fn path_authority_object_type(
    state: &StorageGovernanceState,
    path_decision: &PathDecision,
) -> UnityCatalogResult<String> {
    if state
        .get_external_location(&path_decision.object_id)
        .is_some()
    {
        Ok("EXTERNAL_LOCATION".to_string())
    } else if state.get_managed_root(&path_decision.object_id).is_some() {
        Ok("MANAGED_ROOT".to_string())
    } else {
        Err(UnityCatalogError::Internal {
            message: format!(
                "unknown authorized storage governance object: {}",
                path_decision.object_id
            ),
        })
    }
}

fn supports_vended_operation(operation: CredentialOperation) -> bool {
    matches!(
        operation,
        CredentialOperation::Read | CredentialOperation::Write | CredentialOperation::List
    )
}

fn credential_denied(
    reason_code: &str,
    audit_event_id: Option<&str>,
) -> UnityCatalogResult<(StatusCode, Json<serde_json::Value>)> {
    let suffix = audit_event_id.map_or_else(String::new, |id| format!(" audit_event_id={id}"));
    Err(UnityCatalogError::Forbidden {
        message: format!("credential_scope_denied:{reason_code}{suffix}"),
    })
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

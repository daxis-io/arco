//! Storage-governance enforcement for client-supplied table locations.
//!
//! Issue #358: `create_table` and `register_table` accept client-supplied
//! locations that previously bypassed the governed-path model entirely. When
//! storage governance is enabled for a tenant/workspace scope (a published
//! storage-governance projection exists), client-supplied locations must
//! resolve to exactly one active path authority bound to the request
//! workspace. When storage governance is not enabled, current behavior is
//! preserved unchanged.

use arco_catalog::CatalogError;
use arco_catalog::metastore::publish::load_published_storage_governance_if_configured;
use arco_core::ScopedStorage;

use crate::error::{IcebergError, IcebergResult};

/// Validates a client-supplied table location against published storage
/// governance.
///
/// Behavior:
///
/// - **Governance disabled** (no storage-governance projection has ever been
///   published for the tenant/workspace scope): the location passes through
///   and current behavior is preserved.
/// - **Governance enabled**: the location must resolve to exactly one active
///   path authority (external location or managed root) bound to the request
///   workspace. Ungoverned locations, ambiguously governed (overlapping)
///   locations, and locations that do not parse as governed URIs are denied
///   with a typed 400 error.
/// - **Governance enabled but the projection is stale or corrupt**: denies
///   closed with 503, matching the credential-vending posture.
///
/// Server-derived default locations are not routed through this check; the
/// scope of #358 is the client-controlled location channel.
pub async fn validate_client_supplied_location(
    storage: &ScopedStorage,
    workspace: &str,
    location: &str,
) -> IcebergResult<()> {
    let published = load_published_storage_governance_if_configured(storage)
        .await
        .map_err(|error| governance_state_unavailable(&error))?;
    let Some(published) = published else {
        return Ok(());
    };

    match published.state.authority_for_path(workspace, location) {
        Ok(_) => Ok(()),
        Err(CatalogError::NotFound { .. }) => Err(IcebergError::BadRequest {
            message: format!(
                "Table location '{location}' is not governed by any storage-governance path \
                 authority bound to this workspace"
            ),
            error_type: "BadRequestException",
        }),
        Err(CatalogError::PreconditionFailed { .. }) => Err(IcebergError::BadRequest {
            message: format!(
                "Table location '{location}' is ambiguously governed by overlapping \
                 storage-governance path authorities"
            ),
            error_type: "BadRequestException",
        }),
        Err(CatalogError::Validation { message }) => Err(IcebergError::BadRequest {
            message: format!(
                "Invalid table location '{location}' under storage governance: {message}"
            ),
            error_type: "BadRequestException",
        }),
        Err(error) => Err(governance_state_unavailable(&error)),
    }
}

fn governance_state_unavailable(error: &CatalogError) -> IcebergError {
    IcebergError::ServiceUnavailable {
        message: format!(
            "Storage governance state unavailable for table location validation: {error}"
        ),
        retry_after_seconds: Some(1),
    }
}

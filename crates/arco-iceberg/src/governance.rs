//! Storage-governance enforcement for client-supplied table locations.
//!
//! Issue #358: `create_table` and `register_table` accept client-supplied
//! locations that previously bypassed the governed-path model entirely. When
//! storage governance is enabled for a tenant/workspace scope (a published
//! storage-governance projection exists), client-supplied locations must
//! resolve to exactly one active path authority bound to the request
//! workspace. When storage governance is not enabled, current behavior is
//! preserved unchanged.
//!
//! The same rule covers the location-bearing table properties
//! ([`LOCATION_BEARING_TABLE_PROPERTIES`]): a property such as
//! `write.data.path` redirects data files to an arbitrary location, so under
//! governance it is validated exactly like the advertised table location.

use std::collections::HashMap;
use std::hash::BuildHasher;

use arco_catalog::CatalogError;
use arco_catalog::metastore::publish::{
    PublishedStorageGovernance, load_published_storage_governance_if_configured,
};
use arco_core::ScopedStorage;

use crate::error::{IcebergError, IcebergResult};

/// Iceberg table properties that carry storage locations and are therefore
/// validated against storage governance exactly like the advertised table
/// location when governance is enabled for the scope.
///
/// Re-exported from the catalog so every enforcement surface (Iceberg REST,
/// UC, native API) shares one key list; a key added there is enforced here
/// without a second edit.
pub use arco_catalog::metastore::publish::LOCATION_BEARING_TABLE_PROPERTIES;

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
///
/// # Errors
///
/// Returns a typed 400 for ungoverned, ambiguously governed, or unparseable
/// locations, and a 503 when the published projection is stale or corrupt.
pub async fn validate_client_supplied_location(
    storage: &ScopedStorage,
    workspace: &str,
    location: &str,
) -> IcebergResult<()> {
    let Some(published) = load_governance_if_configured(storage).await? else {
        return Ok(());
    };
    validate_location_against(&published, workspace, location, None)
}

/// Validates the known location-bearing table properties against published
/// storage governance.
///
/// When governance is enabled for the scope, each of
/// [`LOCATION_BEARING_TABLE_PROPERTIES`] present in `properties` must resolve
/// to a governed path authority bound to the request workspace, exactly like
/// the advertised table location; violations are denied with a typed 400
/// naming the offending property. When governance is not configured, the
/// properties pass through untouched.
///
/// # Errors
///
/// Returns a typed 400 naming the offending property for ungoverned,
/// ambiguously governed, or unparseable property locations, and a 503 when the
/// published projection is stale or corrupt.
pub async fn validate_location_bearing_table_properties<H: BuildHasher + Sync>(
    storage: &ScopedStorage,
    workspace: &str,
    properties: &HashMap<String, String, H>,
) -> IcebergResult<()> {
    let targeted: Vec<(&str, &str)> = LOCATION_BEARING_TABLE_PROPERTIES
        .iter()
        .filter_map(|key| properties.get(*key).map(|value| (*key, value.as_str())))
        .collect();
    if targeted.is_empty() {
        return Ok(());
    }
    let Some(published) = load_governance_if_configured(storage).await? else {
        return Ok(());
    };
    for (property, location) in targeted {
        validate_location_against(&published, workspace, location, Some(property))?;
    }
    Ok(())
}

/// Request-scoped storage-governance enforcement handle.
///
/// The route layer is not a sufficient enforcement point for #358: the commit
/// machinery ([`crate::commit::CommitService`],
/// [`crate::coordinator::MultiTableTransactionCoordinator`]) is reached from
/// several routes (single-table commit, one-table `transactions/commit`,
/// multi-table `transactions/commit`), and each of those can carry
/// `set-properties` updates that redirect data or metadata writes to a foreign
/// bucket. Carrying this handle *into* the commit machinery makes the check a
/// property of the authoritative write path rather than of any one route, so a
/// new caller cannot reach a metadata write without it.
#[derive(Clone)]
pub struct TableLocationGovernance {
    storage: ScopedStorage,
    workspace: String,
}

impl TableLocationGovernance {
    /// Binds governance enforcement to one request's storage scope.
    #[must_use]
    pub fn new(storage: ScopedStorage, workspace: impl Into<String>) -> Self {
        Self {
            storage,
            workspace: workspace.into(),
        }
    }

    /// Validates a client-supplied table location for this scope.
    ///
    /// # Errors
    ///
    /// Returns a typed 400 for ungoverned, ambiguous, or unparseable locations
    /// and a 503 when the published projection is stale or corrupt.
    pub async fn validate_location(&self, location: &str) -> IcebergResult<()> {
        validate_client_supplied_location(&self.storage, &self.workspace, location).await
    }

    /// Validates the *effective* location-bearing property map for this scope.
    ///
    /// "Effective" means the map that will actually be persisted — after
    /// `set-properties`/`remove-properties` updates have been applied to the
    /// base metadata, or the property map carried by an imported metadata file
    /// — not the properties named in the request. A commit that only removes a
    /// governed property therefore passes, while one that leaves a foreign
    /// location in place is rejected even if the request itself set no
    /// property.
    ///
    /// # Errors
    ///
    /// Returns a typed 400 naming the offending property for ungoverned,
    /// ambiguous, or unparseable property locations, and a 503 when the
    /// published projection is stale or corrupt.
    pub async fn validate_effective_properties<H: BuildHasher + Sync>(
        &self,
        properties: &HashMap<String, String, H>,
    ) -> IcebergResult<()> {
        validate_location_bearing_table_properties(&self.storage, &self.workspace, properties).await
    }
}

async fn load_governance_if_configured(
    storage: &ScopedStorage,
) -> IcebergResult<Option<PublishedStorageGovernance>> {
    load_published_storage_governance_if_configured(storage)
        .await
        .map_err(|error| governance_state_unavailable(&error))
}

fn validate_location_against(
    published: &PublishedStorageGovernance,
    workspace: &str,
    location: &str,
    property: Option<&str>,
) -> IcebergResult<()> {
    let described = property.map_or_else(
        || format!("Table location '{location}'"),
        |property| format!("Table property '{property}' location '{location}'"),
    );
    match published.state.authority_for_path(workspace, location) {
        Ok(_) => Ok(()),
        Err(CatalogError::NotFound { .. }) => Err(IcebergError::BadRequest {
            message: format!(
                "{described} is not governed by any storage-governance path authority bound to \
                 this workspace"
            ),
            error_type: "BadRequestException",
        }),
        Err(CatalogError::PreconditionFailed { .. }) => Err(IcebergError::BadRequest {
            message: format!(
                "{described} is ambiguously governed by overlapping storage-governance path \
                 authorities"
            ),
            error_type: "BadRequestException",
        }),
        Err(CatalogError::Validation { message }) => {
            let invalid = property.map_or_else(
                || format!("Invalid table location '{location}'"),
                |property| format!("Invalid table property '{property}' location '{location}'"),
            );
            Err(IcebergError::BadRequest {
                message: format!("{invalid} under storage governance: {message}"),
                error_type: "BadRequestException",
            })
        }
        Err(error) => Err(governance_state_unavailable(&error)),
    }
}

/// Maps an unavailable/corrupt governance projection onto a stable public
/// reason code.
///
/// The underlying [`CatalogError`] carries internal state (ledger event IDs,
/// projection object paths and versions, raw storage errors). It is correlated
/// in logs; the client sees only the reason code and the retry hint.
fn governance_state_unavailable(error: &CatalogError) -> IcebergError {
    tracing::warn!(
        internal_error = %error,
        "redacted storage-governance state error for table location validation"
    );
    IcebergError::ServiceUnavailable {
        message: "storage_governance_state_unavailable".to_string(),
        retry_after_seconds: Some(1),
    }
}

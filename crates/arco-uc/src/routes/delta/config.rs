//! Delta catalog protocol configuration route for the Unity Catalog facade.

use axum::Json;
use axum::Router;
use axum::extract::{Extension, Query, State};
use axum::routing::get;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};
use crate::routes::{common, preview};
use crate::state::UnityCatalogState;

/// Delta catalog protocol config route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new().route("/delta/v1/config", get(get_config))
}

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[serde(default)]
pub(crate) struct DeltaCatalogConfigQuery {
    catalog: Option<String>,
    #[serde(rename = "protocol-versions")]
    protocol_versions: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub(crate) struct DeltaCatalogConfigResponse {
    endpoints: Vec<String>,
    #[serde(rename = "protocol-version")]
    protocol_version: String,
}

/// Represents a protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProtocolVersion {
    /// Major version.
    pub major: u32,
    /// Minor version.
    pub minor: u32,
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// Protocol versions arco serves for the UC Delta API.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[ProtocolVersion] =
    &[ProtocolVersion { major: 1, minor: 0 }];

struct ClientRange {
    major: u32,
    max_minor: u32,
}

fn parse_protocol_range(token: &str) -> UnityCatalogResult<ClientRange> {
    let (major, max_minor) =
        token
            .split_once('.')
            .ok_or_else(|| UnityCatalogError::BadRequest {
                message: format!("invalid protocol-versions entry {token:?}: expected M.N"),
            })?;

    let parse_part = |part: &str| {
        part.parse::<u32>()
            .map_err(|_| UnityCatalogError::BadRequest {
                message: format!("invalid protocol-versions entry {token:?}: expected numeric M.N"),
            })
    };
    Ok(ClientRange {
        major: parse_part(major)?,
        max_minor: parse_part(max_minor)?,
    })
}

fn parse_protocol_versions(protocol_versions: &str) -> UnityCatalogResult<Vec<ClientRange>> {
    let tokens = protocol_versions.split(',').collect::<Vec<_>>();
    if tokens.iter().any(|token| token.trim().is_empty()) {
        return Err(UnityCatalogError::BadRequest {
            message: "invalid protocol-versions: expected a comma-separated list like \"1.1,2.3\""
                .to_string(),
        });
    }

    tokens
        .iter()
        .map(|token| parse_protocol_range(token.trim()))
        .collect()
}

/// Selects the highest mutually supported protocol version.
///
/// Each client range `(M, N)` means the client supports `M.0` through `M.N`.
/// The server returns its highest supported version covered by any client
/// range, or `None` when there is no overlap.
#[must_use]
fn negotiate_protocol_version(client_ranges: &[ClientRange]) -> Option<ProtocolVersion> {
    negotiate_from(SUPPORTED_PROTOCOL_VERSIONS, client_ranges)
}

#[allow(clippy::suspicious_operation_groupings)]
fn negotiate_from(
    server_versions: &[ProtocolVersion],
    client_ranges: &[ClientRange],
) -> Option<ProtocolVersion> {
    server_versions
        .iter()
        .copied()
        .filter(|server| {
            client_ranges
                .iter()
                .any(|client| client.major == server.major && server.minor <= client.max_minor)
        })
        .max()
}

fn supported_versions_display() -> String {
    SUPPORTED_PROTOCOL_VERSIONS
        .iter()
        .map(ProtocolVersion::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Delta endpoint signatures served for the negotiated protocol version.
///
/// List of supported endpoint signatures are in the form "METHOD /v1/path".
/// Paths are relative to the catalog API root (/api/2.1/unity-catalog/delta).
fn delta_endpoints_for_protocol_version(version: &str) -> Vec<String> {
    match version {
        "1.0" => vec![],
        _ => Vec::new(),
    }
}

/// Get delta catalog configuration.
///
/// # Errors
///
/// Returns [`UnityCatalogError`] when request validation fails, or catalog is missing.
#[utoipa::path(
    get,
    path = "/delta/v1/config",
    tag = "DeltaConfiguration",
    params(
        ("catalog" = String, Query, description = "Catalog name"),
        ("protocol-versions" = String, Query, description = "Comma-separated list of highest protocol versions the client supports per major version
            (e.g., 1.1,2.3 means the client supports 1.0-1.1 and 2.0-2.3).
            The server selects the highest mutually supported protocol version and returns endpoints for that version.")
    ),
    responses(
        (status = 200, description = "Configuration retrieved successfully", body = DeltaCatalogConfigResponse),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse)
    )
)]
pub(crate) async fn get_config(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    query: Query<DeltaCatalogConfigQuery>,
) -> UnityCatalogResult<Json<DeltaCatalogConfigResponse>> {
    let Query(query) = query;
    let _ = &query.extra;

    let catalog = preview::require_identifier(query.catalog, "catalog")?;
    let protocol_versions =
        preview::require_non_empty_string(query.protocol_versions, "protocol-versions")?;
    let client_ranges = parse_protocol_versions(&protocol_versions)?;
    let protocol_version = negotiate_protocol_version(&client_ranges).ok_or_else(|| {
        UnityCatalogError::BadRequest {
            message: format!(
                "no mutually supported protocol version: client requested {protocol_versions}; server supports {}",
                supported_versions_display()
            ),
        }
    })?
    .to_string();

    // Verify catalog exists
    let reader = common::authoritative_catalog_reader(&state, &ctx)
        .await?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("catalog not found: {catalog}"),
        })?;
    reader
        .get_catalog(&catalog)
        .await
        .map_err(common::map_catalog_error)?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("catalog not found: {catalog}"),
        })?;

    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog = %catalog,
        protocol_versions = %protocol_versions,
        protocol_version = %protocol_version,
        "unity catalog get delta catalog config from authoritative catalog state"
    );

    Ok(Json(DeltaCatalogConfigResponse {
        endpoints: delta_endpoints_for_protocol_version(&protocol_version),
        protocol_version,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pv(major: u32, minor: u32) -> ProtocolVersion {
        ProtocolVersion { major, minor }
    }

    fn cr(major: u32, max_minor: u32) -> ClientRange {
        ClientRange { major, max_minor }
    }

    #[test]
    fn no_overlapping_major_returns_none() {
        let servers = [pv(1, 0)];
        let clients = [cr(2, 5)];
        assert_eq!(negotiate_from(&servers, &clients), None);
    }

    #[test]
    fn server_minor_equal_to_client_max_minor_matches() {
        let servers = [pv(1, 3)];
        let clients = [cr(1, 3)];
        assert_eq!(negotiate_from(&servers, &clients), Some(pv(1, 3)));
    }

    #[test]
    fn server_minor_greater_than_client_max_minor_excluded() {
        let servers = [pv(1, 4)];
        let clients = [cr(1, 3)];
        assert_eq!(negotiate_from(&servers, &clients), None);
    }

    #[test]
    fn picks_highest_minor_within_client_bound() {
        let servers = [pv(1, 0), pv(1, 1), pv(1, 2), pv(1, 3), pv(1, 4)];
        let clients = [cr(1, 3)];
        assert_eq!(negotiate_from(&servers, &clients), Some(pv(1, 3)));
    }

    #[test]
    fn higher_major_wins_even_with_lower_minor() {
        let servers = [pv(1, 9), pv(2, 0)];
        let clients = [cr(1, 5), cr(2, 0)];
        assert_eq!(negotiate_from(&servers, &clients), Some(pv(2, 0)));
    }

    #[test]
    fn higher_major_ineligible_falls_back_to_lower_major() {
        let servers = [pv(1, 2), pv(2, 0)];
        let clients = [cr(1, 5)];
        assert_eq!(negotiate_from(&servers, &clients), Some(pv(1, 2)));
    }
}

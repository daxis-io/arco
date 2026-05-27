//! Route-level Unity Catalog compatibility support registry.

use axum::http::Method;

/// Arco's support classification for a Unity Catalog operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupportLevel {
    /// The route is implemented over Arco-native authoritative state.
    Implemented,
    /// The route is callable but intentionally exposes a documented subset.
    CompatiblePartial,
    /// The route is a known UC operation but intentionally returns `501`.
    KnownUnsupported,
    /// The route family is planned but not callable yet.
    Planned,
}

impl SupportLevel {
    /// Returns the stable support label used in docs and `OpenAPI` extensions.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Implemented => "implemented",
            Self::CompatiblePartial => "compatible-partial",
            Self::KnownUnsupported => "known-unsupported",
            Self::Planned => "planned",
        }
    }

    pub(crate) const fn returns_not_implemented(self) -> bool {
        matches!(self, Self::KnownUnsupported | Self::Planned)
    }
}

/// Route-level support metadata for one UC operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UcOperationSupport {
    /// Uppercase HTTP method.
    pub method: &'static str,
    /// OpenAPI-style path template.
    pub path_template: &'static str,
    /// Route group label.
    pub group: &'static str,
    /// Arco support level.
    pub support_level: SupportLevel,
    /// Native backing or implementation boundary.
    pub native_backing: &'static str,
    /// Authorization boundary for the route.
    pub authz_boundary: &'static str,
    /// Known gap that keeps the route partial, unsupported, or planned.
    pub known_gap: Option<&'static str>,
    /// Whether the route is Arco-native rather than a pinned UC fixture route.
    pub arco_native: bool,
    /// Whether the generated Arco `OpenAPI` spec documents this operation.
    pub documented: bool,
}

/// Matched registry operation for a concrete request path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UcSupportMatch<'a> {
    /// Support metadata that matched.
    pub operation: &'static UcOperationSupport,
    /// Concrete request path after UC mount-prefix normalization.
    pub path: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Segment {
    Literal(&'static str),
    Parameter,
}

const REGISTRY: &[UcOperationSupport] = &[
    implemented(
        "GET",
        "/catalogs",
        "Catalogs",
        "authoritative catalog ledger and manifest-published snapshot",
        "catalog route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    implemented(
        "POST",
        "/catalogs",
        "Catalogs",
        "authoritative catalog writer transaction",
        "catalog route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    implemented(
        "GET",
        "/catalogs/{name}",
        "Catalogs",
        "authoritative catalog ledger and manifest-published snapshot",
        "catalog route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    implemented(
        "PATCH",
        "/catalogs/{name}",
        "Catalogs",
        "authoritative catalog writer transaction",
        "catalog route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    implemented(
        "DELETE",
        "/catalogs/{name}",
        "Catalogs",
        "authoritative catalog writer transaction",
        "catalog route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    implemented(
        "GET",
        "/schemas",
        "Schemas",
        "authoritative catalog ledger and manifest-published snapshot",
        "schema route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    implemented(
        "POST",
        "/schemas",
        "Schemas",
        "authoritative catalog writer transaction",
        "schema route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    implemented(
        "GET",
        "/schemas/{full_name}",
        "Schemas",
        "authoritative catalog ledger and manifest-published snapshot",
        "schema route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    implemented(
        "PATCH",
        "/schemas/{full_name}",
        "Schemas",
        "authoritative catalog writer transaction",
        "schema route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    implemented(
        "DELETE",
        "/schemas/{full_name}",
        "Schemas",
        "authoritative catalog writer transaction",
        "schema route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    known_unsupported(
        "POST",
        "/staging-tables",
        "Tables",
        "managed-table staging protocol is not implemented",
    ),
    implemented(
        "GET",
        "/tables",
        "Tables",
        "authoritative catalog ledger and manifest-published snapshot",
        "table route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    implemented(
        "POST",
        "/tables",
        "Tables",
        "authoritative catalog writer transaction",
        "table route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    implemented(
        "GET",
        "/tables/{full_name}",
        "Tables",
        "authoritative catalog ledger and manifest-published snapshot",
        "table route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    implemented(
        "DELETE",
        "/tables/{full_name}",
        "Tables",
        "authoritative catalog writer transaction",
        "table route enforcement is not yet backed by route-wide compiled grants",
        true,
    ),
    partial(
        "GET",
        "/permissions/{securable_type}/{full_name}",
        "Grants",
        "injected compiled permission view",
        "compiled permissions are injected application state, not yet a manifest-published grant projection with writer-backed mutations",
        true,
    ),
    known_unsupported(
        "PATCH",
        "/permissions/{securable_type}/{full_name}",
        "Grants",
        "writer-backed grant persistence, grant-option enforcement, and grant mutation audit remain pending",
    ),
    partial(
        "POST",
        "/temporary-table-credentials",
        "TemporaryCredentials",
        "compiled authorization plus published storage-governance projection",
        "provider token material, revocation metadata, and expiry exposure remain planned",
        true,
    ),
    partial(
        "POST",
        "/temporary-path-credentials",
        "TemporaryCredentials",
        "compiled authorization plus published storage-governance projection",
        "provider token material, revocation metadata, and expiry exposure remain planned",
        true,
    ),
    known_unsupported(
        "POST",
        "/temporary-volume-credentials",
        "TemporaryCredentials",
        "volume ownership and credential vending scopes are planned but not authoritative",
    ),
    known_unsupported(
        "POST",
        "/temporary-model-version-credentials",
        "TemporaryCredentials",
        "model artifact ownership and credential vending scopes are planned but not authoritative",
    ),
    partial(
        "GET",
        "/delta/preview/commits",
        "DeltaCommits",
        "Delta commit coordinator state",
        "preview coordinator surface; stronger catalog-bound managed Delta validation remains planned",
        true,
    ),
    partial(
        "POST",
        "/delta/preview/commits",
        "DeltaCommits",
        "Delta commit coordinator state with idempotency key support",
        "preview coordinator surface; stronger catalog-bound managed Delta validation remains planned",
        true,
    ),
    known_unsupported(
        "GET",
        "/metastore_summary",
        "Metastores",
        "metastore summary discovery route is not mounted in this facade",
    ),
    known_unsupported(
        "GET",
        "/credentials",
        "Credentials",
        "pinned UC credential route is not mounted; Arco currently exposes storage-governance routes under /storage-credentials",
    ),
    known_unsupported(
        "POST",
        "/credentials",
        "Credentials",
        "pinned UC credential route is not mounted; Arco currently exposes storage-governance routes under /storage-credentials",
    ),
    known_unsupported(
        "GET",
        "/credentials/{name}",
        "Credentials",
        "pinned UC credential route is not mounted; Arco currently exposes storage-governance routes under /storage-credentials",
    ),
    known_unsupported(
        "PATCH",
        "/credentials/{name}",
        "Credentials",
        "credential update lifecycle and provider secret integration remain planned",
    ),
    known_unsupported(
        "DELETE",
        "/credentials/{name}",
        "Credentials",
        "credential delete lifecycle and provider secret integration remain planned",
    ),
    partial_arco_native(
        "GET",
        "/storage-credentials",
        "Credentials",
        "scoped metastore ledger replay through storage-governance state",
        "Arco-native compatibility path; pinned UC /credentials route is not mounted",
    ),
    partial_arco_native(
        "POST",
        "/storage-credentials",
        "Credentials",
        "scoped metastore ledger mutation plus storage-governance validation",
        "Arco-native compatibility path; pinned UC /credentials route is not mounted",
    ),
    partial_arco_native(
        "GET",
        "/storage-credentials/{credential_id}",
        "Credentials",
        "scoped metastore ledger replay through storage-governance state",
        "Arco-native compatibility path; pinned UC /credentials route is not mounted",
    ),
    partial(
        "GET",
        "/external-locations",
        "External Locations",
        "scoped metastore ledger replay through storage-governance state",
        "update/delete lifecycle and broader binding support remain planned",
        false,
    ),
    partial(
        "POST",
        "/external-locations",
        "External Locations",
        "scoped metastore ledger mutation plus path-overlap validation",
        "update/delete lifecycle and broader binding support remain planned",
        false,
    ),
    partial(
        "GET",
        "/external-locations/{name}",
        "External Locations",
        "scoped metastore ledger replay through storage-governance state",
        "update/delete lifecycle and broader binding support remain planned",
        false,
    ),
    known_unsupported(
        "PATCH",
        "/external-locations/{name}",
        "External Locations",
        "external-location update lifecycle remains planned",
    ),
    known_unsupported(
        "DELETE",
        "/external-locations/{name}",
        "External Locations",
        "external-location delete lifecycle remains planned",
    ),
    planned(
        "GET",
        "/volumes",
        "Volumes",
        "volume object family is not authoritative yet",
    ),
    planned(
        "POST",
        "/volumes",
        "Volumes",
        "volume object family is not authoritative yet",
    ),
    planned(
        "GET",
        "/volumes/{name}",
        "Volumes",
        "volume object family is not authoritative yet",
    ),
    planned(
        "PATCH",
        "/volumes/{name}",
        "Volumes",
        "volume object family is not authoritative yet",
    ),
    planned(
        "DELETE",
        "/volumes/{name}",
        "Volumes",
        "volume object family is not authoritative yet",
    ),
    planned(
        "GET",
        "/functions",
        "Functions",
        "function metadata family is planned; execution is out of scope",
    ),
    planned(
        "POST",
        "/functions",
        "Functions",
        "function metadata family is planned; execution is out of scope",
    ),
    planned(
        "GET",
        "/functions/{name}",
        "Functions",
        "function metadata family is planned; execution is out of scope",
    ),
    planned(
        "DELETE",
        "/functions/{name}",
        "Functions",
        "function metadata family is planned; execution is out of scope",
    ),
    planned(
        "GET",
        "/models",
        "RegisteredModels",
        "registered model metadata is not authoritative yet",
    ),
    planned(
        "POST",
        "/models",
        "RegisteredModels",
        "registered model metadata is not authoritative yet",
    ),
    planned(
        "GET",
        "/models/{full_name}",
        "RegisteredModels",
        "registered model metadata is not authoritative yet",
    ),
    planned(
        "PATCH",
        "/models/{full_name}",
        "RegisteredModels",
        "registered model metadata is not authoritative yet",
    ),
    planned(
        "DELETE",
        "/models/{full_name}",
        "RegisteredModels",
        "registered model metadata is not authoritative yet",
    ),
    planned(
        "POST",
        "/models/versions",
        "ModelVersions",
        "model version metadata is not authoritative yet",
    ),
    planned(
        "GET",
        "/models/{full_name}/versions",
        "ModelVersions",
        "model version metadata is not authoritative yet",
    ),
    planned(
        "GET",
        "/models/{full_name}/versions/{version}",
        "ModelVersions",
        "model version metadata is not authoritative yet",
    ),
    planned(
        "PATCH",
        "/models/{full_name}/versions/{version}",
        "ModelVersions",
        "model version metadata is not authoritative yet",
    ),
    planned(
        "DELETE",
        "/models/{full_name}/versions/{version}",
        "ModelVersions",
        "model version metadata is not authoritative yet",
    ),
    planned(
        "PATCH",
        "/models/{full_name}/versions/{version}/finalize",
        "ModelVersions",
        "model version lifecycle is not authoritative yet",
    ),
];

/// Returns the full UC support registry.
#[must_use]
pub const fn registry_entries() -> &'static [UcOperationSupport] {
    REGISTRY
}

/// Returns operations documented in Arco's generated `OpenAPI` surface.
pub fn documented_operations() -> impl Iterator<Item = &'static UcOperationSupport> {
    REGISTRY.iter().filter(|operation| operation.documented)
}

/// Finds route support metadata for a concrete request.
#[must_use]
pub fn operation_support<'a>(method: &str, path: &'a str) -> Option<UcSupportMatch<'a>> {
    let normalized = normalize_path(canonicalize_request_path(path));
    REGISTRY
        .iter()
        .find(|operation| {
            operation.method.eq_ignore_ascii_case(method)
                && path_template_matches(operation.path_template, normalized)
        })
        .map(|operation| UcSupportMatch {
            operation,
            path: normalized,
        })
}

pub(crate) fn unsupported_message(method: &Method, path: &str) -> Option<String> {
    let method = method.as_str();
    if let Some(matched) = operation_support(method, path) {
        let operation = matched.operation;
        if operation.support_level.returns_not_implemented() {
            return Some(unsupported_registry_message(
                method,
                matched.path,
                operation,
            ));
        }
        return None;
    }
    None
}

fn unsupported_registry_message(
    method: &str,
    path: &str,
    operation: &UcOperationSupport,
) -> String {
    let known_gap = operation
        .known_gap
        .unwrap_or("operation is not part of Arco's current embedded UC support contract");
    format!(
        "operation not supported: {method} {path}; support-level={}; route-group={}; path-template={}; known-gap={known_gap}",
        operation.support_level.as_str(),
        operation.group,
        operation.path_template,
    )
}

const fn implemented(
    method: &'static str,
    path_template: &'static str,
    group: &'static str,
    native_backing: &'static str,
    known_gap: &'static str,
    documented: bool,
) -> UcOperationSupport {
    UcOperationSupport {
        method,
        path_template,
        group,
        support_level: SupportLevel::Implemented,
        native_backing,
        authz_boundary: "trusted request context plus route-local validation; full compiled-grant enforcement is not route-wide yet",
        known_gap: Some(known_gap),
        arco_native: false,
        documented,
    }
}

const fn partial(
    method: &'static str,
    path_template: &'static str,
    group: &'static str,
    native_backing: &'static str,
    known_gap: &'static str,
    documented: bool,
) -> UcOperationSupport {
    UcOperationSupport {
        method,
        path_template,
        group,
        support_level: SupportLevel::CompatiblePartial,
        native_backing,
        authz_boundary: "trusted request context, compiled permissions where required, and deny-closed behavior when required projections are unavailable",
        known_gap: Some(known_gap),
        arco_native: false,
        documented,
    }
}

const fn partial_arco_native(
    method: &'static str,
    path_template: &'static str,
    group: &'static str,
    native_backing: &'static str,
    known_gap: &'static str,
) -> UcOperationSupport {
    UcOperationSupport {
        method,
        path_template,
        group,
        support_level: SupportLevel::CompatiblePartial,
        native_backing,
        authz_boundary: "trusted request context plus compiled metastore MANAGE permission",
        known_gap: Some(known_gap),
        arco_native: true,
        documented: false,
    }
}

const fn known_unsupported(
    method: &'static str,
    path_template: &'static str,
    group: &'static str,
    known_gap: &'static str,
) -> UcOperationSupport {
    UcOperationSupport {
        method,
        path_template,
        group,
        support_level: SupportLevel::KnownUnsupported,
        native_backing: "no production-backed Arco route is mounted for this operation",
        authz_boundary: "not enforced because the operation is rejected before mutation or data access",
        known_gap: Some(known_gap),
        arco_native: false,
        documented: false,
    }
}

const fn planned(
    method: &'static str,
    path_template: &'static str,
    group: &'static str,
    known_gap: &'static str,
) -> UcOperationSupport {
    UcOperationSupport {
        method,
        path_template,
        group,
        support_level: SupportLevel::Planned,
        native_backing: "planned object family; no authoritative Arco writer/projection/API behavior yet",
        authz_boundary: "not enforced because the operation is rejected before mutation or data access",
        known_gap: Some(known_gap),
        arco_native: false,
        documented: false,
    }
}

fn path_template_matches(path_template: &'static str, path: &str) -> bool {
    let expected = parse_template(path_template);
    let actual_segments: Vec<&str> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    expected.len() == actual_segments.len()
        && expected
            .iter()
            .zip(actual_segments)
            .all(
                |(expected_segment, actual_segment)| match expected_segment {
                    Segment::Literal(expected_literal) => *expected_literal == actual_segment,
                    Segment::Parameter => !actual_segment.is_empty(),
                },
            )
}

fn parse_template(path_template: &'static str) -> Vec<Segment> {
    normalize_path(path_template)
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            if segment.starts_with('{') && segment.ends_with('}') && segment.len() > 2 {
                Segment::Parameter
            } else {
                Segment::Literal(segment)
            }
        })
        .collect()
}

fn normalize_path(path: &str) -> &str {
    if path != "/" && path.ends_with('/') {
        path.trim_end_matches('/')
    } else {
        path
    }
}

fn canonicalize_request_path(path: &str) -> &str {
    const UC_MOUNT_PREFIX: &str = "/api/2.1/unity-catalog";
    if path == UC_MOUNT_PREFIX {
        "/"
    } else if let Some(stripped) = path.strip_prefix(UC_MOUNT_PREFIX) {
        if stripped.starts_with('/') {
            stripped
        } else {
            path
        }
    } else {
        path
    }
}

//! Path resolution helpers for Iceberg metadata locations.

use crate::error::{IcebergError, IcebergResult};

/// Resolves a metadata location into a storage-relative path.
///
/// Accepts absolute URIs (e.g. `gs://bucket/tenant=.../workspace=.../path`)
/// and scoped locations, returning a path relative to the tenant/workspace scope.
pub fn resolve_metadata_path(
    location: &str,
    tenant: &str,
    workspace: &str,
) -> IcebergResult<String> {
    let path = if let Some((_, rest)) = location.split_once("://") {
        let mut parts = rest.splitn(2, '/');
        let _bucket = parts.next();
        parts
            .next()
            .ok_or_else(|| IcebergError::Internal {
                message: format!("Invalid metadata location: {location}"),
            })?
            .to_string()
    } else {
        location.to_string()
    };

    let scoped_prefix = format!("tenant={tenant}/workspace={workspace}/");
    let relative = path
        .strip_prefix(&scoped_prefix)
        .unwrap_or(path.as_str())
        .to_string();

    if relative.is_empty() {
        return Err(IcebergError::Internal {
            message: format!("Invalid metadata location: {location}"),
        });
    }

    Ok(relative)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_metadata_path_with_scheme_and_scope() {
        let location =
            "gs://bucket/tenant=acme/workspace=prod/warehouse/table/metadata/v1.metadata.json";
        let path = resolve_metadata_path(location, "acme", "prod").expect("resolve");
        assert_eq!(path, "warehouse/table/metadata/v1.metadata.json");
    }

    #[test]
    fn test_resolve_metadata_path_with_scope_only() {
        let location = "tenant=acme/workspace=prod/warehouse/table/metadata/v1.metadata.json";
        let path = resolve_metadata_path(location, "acme", "prod").expect("resolve");
        assert_eq!(path, "warehouse/table/metadata/v1.metadata.json");
    }

    #[test]
    fn test_resolve_metadata_path_without_scope() {
        let location = "warehouse/table/metadata/v1.metadata.json";
        let path = resolve_metadata_path(location, "acme", "prod").expect("resolve");
        assert_eq!(path, "warehouse/table/metadata/v1.metadata.json");
    }

    #[test]
    fn test_resolve_metadata_path_rejects_empty_path() {
        let location = "gs://bucket";
        let err = resolve_metadata_path(location, "acme", "prod").expect_err("error");
        assert!(err.to_string().contains("Invalid metadata location"));
    }
}

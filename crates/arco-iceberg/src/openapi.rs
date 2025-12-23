//! `OpenAPI` (3.1) specification generation for the Iceberg REST Catalog.
//!
//! The generated spec can be used to validate compliance with the
//! Apache Iceberg REST Catalog specification.

use utoipa::OpenApi;

/// `OpenAPI` documentation for the Iceberg REST Catalog API.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Iceberg REST Catalog API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Apache Iceberg REST Catalog API with Arco extensions for credential vending and CAS-based commits.",
        license(
            name = "Apache-2.0",
            url = "https://www.apache.org/licenses/LICENSE-2.0"
        )
    ),
    paths(
        crate::routes::config::get_config,
        crate::routes::namespaces::list_namespaces,
        crate::routes::namespaces::get_namespace,
        crate::routes::namespaces::head_namespace,
        crate::routes::tables::list_tables,
        crate::routes::tables::load_table,
        crate::routes::tables::head_table,
        crate::routes::tables::get_credentials,
    ),
    components(
        schemas(
            crate::types::ConfigResponse,
            crate::types::ListNamespacesResponse,
            crate::types::GetNamespaceResponse,
            crate::types::NamespaceIdent,
            crate::types::ListTablesResponse,
            crate::types::LoadTableResponse,
            crate::types::TableIdent,
            crate::types::TableMetadata,
            crate::types::Schema,
            crate::types::SchemaField,
            crate::types::PartitionSpec,
            crate::types::PartitionField,
            crate::types::SortOrder,
            crate::types::SortField,
            crate::types::Snapshot,
            crate::types::SnapshotLogEntry,
            crate::types::MetadataLogEntry,
            crate::types::SnapshotRefMetadata,
            crate::types::TableUuid,
            crate::types::TableCredentialsResponse,
            crate::types::StorageCredential,
            crate::error::IcebergErrorResponse,
        )
    ),
    tags(
        (name = "Configuration", description = "Catalog configuration endpoint"),
        (name = "Namespaces", description = "Namespace management operations"),
        (name = "Tables", description = "Table management and loading operations"),
    ),
)]
pub struct IcebergApiDoc;

/// Returns the generated `OpenAPI` spec.
#[must_use]
pub fn openapi() -> utoipa::openapi::OpenApi {
    IcebergApiDoc::openapi()
}

/// Returns the generated `OpenAPI` spec serialized as pretty JSON.
///
/// # Errors
///
/// Returns an error if JSON serialization fails (should not happen).
pub fn openapi_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&openapi())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_openapi_generation() {
        let spec = openapi();
        assert_eq!(spec.info.title, "Iceberg REST Catalog API");
        assert!(spec.paths.paths.contains_key("/v1/config"));
    }

    #[test]
    fn test_openapi_json_serialization() {
        let json = openapi_json().expect("serialization should succeed");
        assert!(json.contains("Iceberg REST Catalog API"));
        assert!(json.contains("/v1/config"));
        assert!(json.contains("/v1/{prefix}/namespaces"));
    }
}

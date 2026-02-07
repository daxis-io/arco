//! `OpenAPI` (3.1) specification generation for the Unity Catalog facade.

use std::sync::OnceLock;

use utoipa::OpenApi;

/// `OpenAPI` documentation for the Unity Catalog OSS parity facade.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Unity Catalog API (Arco facade)",
        version = env!("CARGO_PKG_VERSION"),
        description = "Unity Catalog OSS parity facade for Arco (contract pinned by vendored OpenAPI spec)."
    ),
    paths(
        crate::routes::catalogs::create_catalog,
        crate::routes::catalogs::list_catalogs,
        crate::routes::catalogs::get_catalog,
        crate::routes::catalogs::update_catalog,
        crate::routes::catalogs::delete_catalog,
        crate::routes::schemas::create_schema,
        crate::routes::schemas::list_schemas,
        crate::routes::schemas::get_schema,
        crate::routes::schemas::update_schema,
        crate::routes::schemas::delete_schema,
        crate::routes::tables::create_table,
        crate::routes::tables::list_tables,
        crate::routes::tables::get_table,
        crate::routes::tables::delete_table,
        crate::routes::permissions::get_permissions,
        crate::routes::permissions::update_permissions,
        crate::routes::credentials::temporary_model_version_credentials,
        crate::routes::credentials::temporary_table_credentials,
        crate::routes::credentials::temporary_volume_credentials,
        crate::routes::credentials::temporary_path_credentials,
        crate::routes::delta_commits::list_unbackfilled_commits,
        crate::routes::delta_commits::register_commit,
        crate::routes::openapi::get_openapi_json,
    ),
    components(
        schemas(
            crate::error::UnityCatalogErrorResponse,
        )
    ),
    tags(
        (name = "Catalogs", description = "Catalog operations"),
        (name = "Schemas", description = "Schema operations"),
        (name = "Tables", description = "Table operations"),
        (name = "Permissions", description = "Permission operations"),
        (name = "TemporaryCredentials", description = "Temporary credential operations"),
        (name = "DeltaCommits", description = "Delta commit coordinator operations"),
        (name = "OpenAPI", description = "OpenAPI specification endpoint"),
    ),
)]
pub struct UnityCatalogApiDoc;

/// Returns the generated `OpenAPI` spec.
#[must_use]
pub fn openapi() -> utoipa::openapi::OpenApi {
    UnityCatalogApiDoc::openapi()
}

static OPENAPI_JSON_CACHE: OnceLock<String> = OnceLock::new();

/// Returns the generated `OpenAPI` spec serialized as pretty JSON.
///
/// # Errors
///
/// Returns an error if JSON serialization fails (should not happen).
pub fn openapi_json() -> Result<String, serde_json::Error> {
    if let Some(spec) = OPENAPI_JSON_CACHE.get() {
        return Ok(spec.clone());
    }

    let spec = serde_json::to_string_pretty(&openapi())?;
    let _ = OPENAPI_JSON_CACHE.set(spec.clone());
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_openapi_generation() {
        let spec = openapi();
        assert_eq!(spec.info.title, "Unity Catalog API (Arco facade)");
        assert!(spec.paths.paths.contains_key("/openapi.json"));
        assert!(spec.paths.paths.contains_key("/catalogs"));
        assert!(spec.paths.paths.contains_key("/delta/preview/commits"));
    }
}

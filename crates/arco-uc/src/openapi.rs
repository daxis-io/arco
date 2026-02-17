//! `OpenAPI` (3.1) specification generation for the Unity Catalog facade.

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
        crate::routes::openapi::get_openapi_json,
    ),
    components(
        schemas(
            crate::error::UnityCatalogErrorResponse,
        )
    ),
    tags(
        (name = "OpenAPI", description = "OpenAPI specification endpoint"),
    ),
)]
pub struct UnityCatalogApiDoc;

/// Returns the generated `OpenAPI` spec.
#[must_use]
pub fn openapi() -> utoipa::openapi::OpenApi {
    UnityCatalogApiDoc::openapi()
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
        assert_eq!(spec.info.title, "Unity Catalog API (Arco facade)");
        assert!(spec.paths.paths.contains_key("/openapi.json"));
    }
}

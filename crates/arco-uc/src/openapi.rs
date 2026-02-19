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
        crate::routes::openapi::get_openapi_json,
        crate::routes::catalogs::get_catalogs,
        crate::routes::catalogs::post_catalogs,
        crate::routes::schemas::get_schemas,
        crate::routes::schemas::post_schemas,
        crate::routes::tables::get_tables,
        crate::routes::tables::post_tables,
        crate::routes::delta_commits::get_delta_preview_commits,
        crate::routes::delta_commits::post_delta_preview_commits,
        crate::routes::credentials::post_temporary_table_credentials,
        crate::routes::credentials::post_temporary_path_credentials,
    ),
    components(
        schemas(
            crate::error::UnityCatalogErrorResponse,
        )
    ),
    tags(
        (name = "OpenAPI", description = "OpenAPI specification endpoint"),
        (name = "Catalogs", description = "Catalog operations"),
        (name = "Schemas", description = "Schema operations"),
        (name = "Tables", description = "Table operations"),
        (name = "DeltaCommits", description = "Delta commit coordinator operations"),
        (name = "TemporaryCredentials", description = "Temporary credential vending operations"),
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
    use serde_json::Value;

    fn operation_request_schema_ref(spec: &Value, path: &str, method: &str) -> Option<String> {
        spec.get("paths")
            .and_then(Value::as_object)?
            .get(path)?
            .get(method)?
            .get("requestBody")?
            .get("content")?
            .get("application/json")?
            .get("schema")?
            .get("$ref")?
            .as_str()
            .map(str::to_string)
    }

    fn operation_response_schema_ref(
        spec: &Value,
        path: &str,
        method: &str,
        status_code: &str,
    ) -> Option<String> {
        spec.get("paths")
            .and_then(Value::as_object)?
            .get(path)?
            .get(method)?
            .get("responses")?
            .get(status_code)?
            .get("content")?
            .get("application/json")?
            .get("schema")?
            .get("$ref")?
            .as_str()
            .map(str::to_string)
    }

    #[test]
    fn test_openapi_generation() {
        let spec = openapi();
        assert_eq!(spec.info.title, "Unity Catalog API (Arco facade)");
        assert!(spec.paths.paths.contains_key("/openapi.json"));
    }

    #[test]
    fn test_openapi_includes_v1_unity_catalog_surface() {
        let spec = serde_json::to_value(openapi()).expect("serialize openapi");
        let paths = spec
            .get("paths")
            .and_then(Value::as_object)
            .expect("paths object");

        for path in [
            "/catalogs",
            "/schemas",
            "/tables",
            "/delta/preview/commits",
            "/temporary-table-credentials",
            "/temporary-path-credentials",
        ] {
            assert!(
                paths.contains_key(path),
                "missing v1 Unity Catalog path {path}"
            );
        }
    }

    #[test]
    fn test_openapi_uses_typed_request_schemas_for_preview_operations() {
        let spec = serde_json::to_value(openapi()).expect("serialize openapi");

        let expectations = [
            ("/catalogs", "post", "CreateCatalogRequestBody"),
            ("/schemas", "post", "CreateSchemaRequestBody"),
            ("/tables", "post", "CreateTableRequestBody"),
            (
                "/delta/preview/commits",
                "get",
                "DeltaGetCommitsRequestBody",
            ),
            ("/delta/preview/commits", "post", "DeltaCommitRequestBody"),
            (
                "/temporary-table-credentials",
                "post",
                "GenerateTemporaryTableCredentialRequestBody",
            ),
            (
                "/temporary-path-credentials",
                "post",
                "GenerateTemporaryPathCredentialRequestBody",
            ),
        ];

        for (path, method, expected_component) in expectations {
            let schema_ref = operation_request_schema_ref(&spec, path, method);
            assert!(
                schema_ref.is_some(),
                "missing request schema for {method} {path}"
            );
            let schema_ref = schema_ref.unwrap_or_default();
            assert!(
                schema_ref.contains(expected_component),
                "unexpected request schema for {method} {path}: {schema_ref}"
            );
        }
    }

    #[test]
    fn test_openapi_uses_typed_response_schemas_for_interop_crud_subset() {
        let spec = serde_json::to_value(openapi()).expect("serialize openapi");

        let expectations = [
            ("/catalogs", "get", "200", "ListCatalogsResponse"),
            ("/catalogs", "post", "200", "CatalogInfo"),
            ("/schemas", "get", "200", "ListSchemasResponse"),
            ("/schemas", "post", "200", "SchemaInfo"),
            ("/tables", "get", "200", "ListTablesResponse"),
            ("/tables", "post", "200", "TableInfo"),
        ];

        for (path, method, status, expected_component) in expectations {
            let schema_ref = operation_response_schema_ref(&spec, path, method, status);
            assert!(
                schema_ref.is_some(),
                "missing response schema for {method} {path} ({status})"
            );
            let schema_ref = schema_ref.unwrap_or_default();
            assert!(
                schema_ref.contains(expected_component),
                "unexpected response schema for {method} {path} ({status}): {schema_ref}"
            );
        }
    }
}

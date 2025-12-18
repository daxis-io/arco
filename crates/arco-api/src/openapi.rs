//! `OpenAPI` (3.1) specification generation for `arco-api`.
//!
//! The checked-in spec is used to generate external clients (TS / Rust) and to
//! detect breaking API changes in CI.

use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

/// `OpenAPI` documentation for the Arco REST API (`/api/v1/*`).
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Arco API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Arco catalog REST API (M4/M5)"
    ),
    paths(
        crate::routes::namespaces::create_namespace,
        crate::routes::namespaces::list_namespaces,
        crate::routes::namespaces::get_namespace,
        crate::routes::namespaces::delete_namespace,
        crate::routes::tables::register_table,
        crate::routes::tables::list_tables,
        crate::routes::tables::get_table,
        crate::routes::tables::update_table,
        crate::routes::tables::drop_table,
        crate::routes::lineage::add_edges,
        crate::routes::lineage::get_lineage,
        crate::routes::browser::mint_urls,
    ),
    components(
        schemas(
            crate::error::ApiErrorBody,
            crate::routes::namespaces::CreateNamespaceRequest,
            crate::routes::namespaces::NamespaceResponse,
            crate::routes::namespaces::ListNamespacesResponse,
            crate::routes::tables::RegisterTableRequest,
            crate::routes::tables::ColumnDefinition,
            crate::routes::tables::UpdateTableRequest,
            crate::routes::tables::TableResponse,
            crate::routes::tables::ColumnResponse,
            crate::routes::tables::ListTablesResponse,
            crate::routes::lineage::AddEdgesRequest,
            crate::routes::lineage::EdgeDefinition,
            crate::routes::lineage::EdgeResponse,
            crate::routes::lineage::AddEdgesResponse,
            crate::routes::lineage::LineageResponse,
            crate::routes::browser::MintUrlsRequest,
            crate::routes::browser::MintUrlsResponse,
            crate::routes::browser::SignedUrl,
        )
    ),
    tags(
        (name = "namespaces", description = "Namespace operations"),
        (name = "tables", description = "Table operations"),
        (name = "lineage", description = "Lineage operations"),
        (name = "browser", description = "Browser signed URL minting"),
    ),
    modifiers(&SecurityAddon),
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
    }
}

/// Returns the generated `OpenAPI` spec.
#[must_use]
pub fn openapi() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

/// Returns the generated `OpenAPI` spec serialized as pretty JSON.
///
/// # Errors
///
/// Returns an error if JSON serialization fails (should not happen).
pub fn openapi_json() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&openapi())
}

//! Metadata for owner-approved tables visible to the current principal.

use std::sync::Arc;

use arco_catalog::CatalogError;
use arco_catalog::authz::privileges::Privilege;
use arco_core::TableFormat;
use axum::Router;
use axum::extract::{Extension, Query, State};
use axum::response::Response;
use axum::routing::get;
use serde::{Deserialize, Serialize};

use super::json_response;
use crate::audit;
use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogResult};
use crate::routes::common::{
    authoritative_catalog_reader, authz_context_denial_reason_for_watermark,
    authz_denial_reason_for_watermark, resolve_compiled_permissions, scoped_storage,
};
use crate::routes::credentials::revalidate_observed_watermark;
use crate::state::{GovernedCatalogReadConfig, UnityCatalogState};

/// Mounts the table inventory handler for the governed read profile.
pub fn discovery_routes() -> Router<UnityCatalogState> {
    Router::new().route("/extensions/tables", get(list_visible_tables))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableDiscoveryQuery {
    catalog_name: String,
    schema_name: String,
}

#[derive(Serialize)]
struct VisibleTable {
    table_id: String,
    name: String,
    format: &'static str,
}

#[derive(Serialize)]
struct VisibleTables {
    tables: Vec<VisibleTable>,
}

async fn list_visible_tables(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Extension(profile): Extension<Arc<GovernedCatalogReadConfig>>,
    Query(query): Query<TableDiscoveryQuery>,
) -> UnityCatalogResult<Response> {
    profile.require_scope(&ctx)?;
    let catalog =
        crate::routes::preview::require_identifier(Some(query.catalog_name), "catalog_name")?;
    let schema =
        crate::routes::preview::require_identifier(Some(query.schema_name), "schema_name")?;
    let storage = scoped_storage(&state, &ctx)?;
    let observed = state
        .storage_governance_cache
        .load_observed(&storage)
        .await
        .map_err(|_| discovery_unavailable())?;
    let permissions = resolve_compiled_permissions(&state, &ctx).await;
    let watermark = Some(observed.published.ledger_watermark.as_str());
    if authz_context_denial_reason_for_watermark(permissions.as_deref(), &ctx, watermark).is_some()
    {
        audit::emit_permissions_deny(&state, &ctx, "TABLE", "discovery");
        return Err(discovery_unavailable());
    }

    let mut visible = Vec::new();
    if let Some(reader) = authoritative_catalog_reader(&state, &ctx)
        .await
        .map_err(|_| discovery_unavailable())?
    {
        let tables = match reader.list_tables(&catalog, &schema).await {
            Ok(tables) => tables,
            Err(CatalogError::NotFound { .. }) => Vec::new(),
            Err(_) => return Err(discovery_unavailable()),
        };
        visible.extend(tables.into_iter().filter_map(|table| {
            if !profile.admits(&table.id)
                || ![Privilege::Browse, Privilege::Select]
                    .into_iter()
                    .any(|privilege| {
                        authz_denial_reason_for_watermark(
                            permissions.as_deref(),
                            &ctx,
                            &table.id,
                            "TABLE",
                            privilege,
                            watermark,
                        )
                        .is_none()
                    })
            {
                return None;
            }
            let format = TableFormat::effective(table.format.as_deref()).ok()?;
            if format == TableFormat::Iceberg {
                return None;
            }
            Some(VisibleTable {
                table_id: table.id,
                name: table.name,
                format: format.as_str(),
            })
        }));
    }
    visible.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    revalidate_observed_watermark(&state, &ctx, &observed)
        .await
        .map_err(|_| discovery_unavailable())?;
    audit::emit_permissions_allow(&state, &ctx, "TABLE", "discovery");
    json_response(&VisibleTables { tables: visible })
}

fn discovery_unavailable() -> UnityCatalogError {
    UnityCatalogError::ServiceUnavailable {
        message: "table_discovery_unavailable".to_string(),
    }
}

//! Exact-root catalog authority resolution for native API routes.

use std::sync::Arc;

use arco_catalog::{CatalogAuthority, CatalogAuthorityKind, StateScope, Tier1Compactor};

use crate::context::RequestContext;
use crate::error::ApiError;
use crate::server::AppState;

pub(super) async fn resolve(
    state: &AppState,
    ctx: &RequestContext,
) -> Result<CatalogAuthority, ApiError> {
    let storage = ctx.scoped_storage(state.storage_backend()?)?;
    match state
        .catalog_authority_bindings()
        .resolve(&ctx.tenant, &ctx.workspace)
    {
        CatalogAuthorityKind::Legacy => {
            let compactor = state
                .sync_compactor()
                .unwrap_or_else(|| Arc::new(Tier1Compactor::new(storage.clone())));
            CatalogAuthority::legacy(storage, compactor)
                .await
                .map_err(ApiError::from)
        }
        CatalogAuthorityKind::ControlV1 => CatalogAuthority::control_v1_bound(
            storage,
            StateScope::new(&ctx.tenant, &ctx.workspace, "catalog"),
            state.catalog_authority_bindings().as_ref(),
        )
        .map_err(ApiError::from),
    }
}

pub(super) fn resolve_read(
    state: &AppState,
    ctx: &RequestContext,
) -> Result<CatalogAuthority, ApiError> {
    let storage = ctx.scoped_storage(state.storage_backend()?)?;
    match state
        .catalog_authority_bindings()
        .resolve(&ctx.tenant, &ctx.workspace)
    {
        CatalogAuthorityKind::Legacy => {
            let compactor = state
                .sync_compactor()
                .unwrap_or_else(|| Arc::new(Tier1Compactor::new(storage.clone())));
            Ok(CatalogAuthority::legacy_existing(storage, compactor))
        }
        CatalogAuthorityKind::ControlV1 => CatalogAuthority::control_v1_bound(
            storage,
            StateScope::new(&ctx.tenant, &ctx.workspace, "catalog"),
            state.catalog_authority_bindings().as_ref(),
        )
        .map_err(ApiError::from),
    }
}

pub(super) fn reject_table_commit_for_control_v1(
    state: &AppState,
    ctx: &RequestContext,
) -> Result<(), ApiError> {
    if state
        .catalog_authority_bindings()
        .resolve(&ctx.tenant, &ctx.workspace)
        == CatalogAuthorityKind::ControlV1
    {
        return Err(ApiError::catalog_table_commit_disabled());
    }
    Ok(())
}

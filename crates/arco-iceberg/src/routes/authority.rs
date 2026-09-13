//! Exact-root catalog authority resolution for Iceberg routes.

use std::sync::Arc;

use arco_catalog::{CatalogAuthority, CatalogAuthorityKind, StateScope, Tier1Compactor};

use crate::context::IcebergRequestContext;
use crate::error::{IcebergError, IcebergResult};
use crate::state::IcebergState;

pub(super) fn is_control_v1(state: &IcebergState, ctx: &IcebergRequestContext) -> bool {
    state
        .catalog_authority_bindings
        .resolve(&ctx.tenant, &ctx.workspace)
        == CatalogAuthorityKind::ControlV1
}

pub(super) fn read(
    state: &IcebergState,
    ctx: &IcebergRequestContext,
) -> IcebergResult<CatalogAuthority> {
    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    if is_control_v1(state, ctx) {
        return CatalogAuthority::control_v1_bound(
            storage,
            StateScope::new(&ctx.tenant, &ctx.workspace, "catalog"),
            state.catalog_authority_bindings.as_ref(),
        )
        .map_err(IcebergError::from);
    }
    Ok(CatalogAuthority::legacy_existing(
        storage.clone(),
        Arc::new(Tier1Compactor::new(storage)),
    ))
}

pub(super) async fn write(
    state: &IcebergState,
    ctx: &IcebergRequestContext,
) -> IcebergResult<CatalogAuthority> {
    let storage = ctx.scoped_storage(Arc::clone(&state.storage))?;
    if is_control_v1(state, ctx) {
        return CatalogAuthority::control_v1_bound(
            storage,
            StateScope::new(&ctx.tenant, &ctx.workspace, "catalog"),
            state.catalog_authority_bindings.as_ref(),
        )
        .map_err(IcebergError::from);
    }
    let compactor = state.create_compactor(&storage)?;
    CatalogAuthority::legacy(storage, compactor)
        .await
        .map_err(IcebergError::from)
}

pub(super) fn reject_table_commit_for_control_v1(
    state: &IcebergState,
    ctx: &IcebergRequestContext,
) -> IcebergResult<()> {
    if is_control_v1(state, ctx) {
        return Err(IcebergError::ServiceUnavailable {
            message: "catalog_authority_table_commit_disabled: Iceberg metadata commits are not enabled for the control/v1 catalog-DDL pilot"
                .to_string(),
            retry_after_seconds: Some(5),
        });
    }
    Ok(())
}

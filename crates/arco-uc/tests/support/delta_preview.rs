//! Table grants and trusted request context for Delta preview integration tests.

use std::sync::Arc;

use arco_catalog::authz::compiler::{CompiledPermissionRow, CompiledPermissionSet};
use arco_catalog::authz::privileges::Privilege;
use arco_core::ScopedStorage;
use arco_uc::context::UnityCatalogRequestContext;
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::Router;
use uuid::Uuid;

const PRINCIPAL: &str = "user_delta_preview";

pub fn router_with_table_privileges(
    storage: &ScopedStorage,
    table_ids: &[&str],
    privileges: &[Privilege],
) -> Router {
    let mut rows = Vec::new();
    for table_id in table_ids {
        for privilege in privileges {
            rows.push(CompiledPermissionRow {
                principal_id: PRINCIPAL.to_string(),
                object_id: (*table_id).to_string(),
                object_type: "TABLE".to_string(),
                privilege: *privilege,
                source: "grant".to_string(),
                source_grant_id: Some(format!("grant-{table_id}-{privilege}")),
                source_principal_id: PRINCIPAL.to_string(),
                source_object_id: (*table_id).to_string(),
                inheritance_path: (*table_id).to_string(),
                grant_option: false,
                group_snapshot_version: "test-groups".to_string(),
            });
        }
    }

    let permissions = CompiledPermissionSet::new("test-ledger", "test-groups", true, rows);
    unity_catalog_router(
        UnityCatalogState::new(Arc::clone(storage.backend()))
            .with_compiled_permissions(permissions),
    )
}

pub fn trusted_context(
    tenant: &str,
    workspace: &str,
    idempotency_key: Option<&str>,
    request_id: Option<&str>,
) -> UnityCatalogRequestContext {
    UnityCatalogRequestContext {
        tenant: tenant.to_string(),
        workspace: workspace.to_string(),
        request_id: request_id.map_or_else(|| Uuid::now_v7().to_string(), str::to_string),
        user_id: Some(PRINCIPAL.to_string()),
        idempotency_key: idempotency_key.map(str::to_string),
    }
}

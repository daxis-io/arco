//! Storage credential UC adapter routes.

use std::collections::BTreeMap;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;

use arco_catalog::authz::privileges::Privilege;
use arco_catalog::metastore::events::{
    LifecycleState, MetastoreEvent, MetastoreMutation, StorageCredentialRecord,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::storage_governance::StorageGovernanceState;

use crate::context::UnityCatalogRequestContext;
use crate::error::UnityCatalogError;
use crate::routes::common::{
    control_plane_scope, map_catalog_error, require_authz, scoped_storage,
};
use crate::state::UnityCatalogState;

#[derive(Debug, Deserialize)]
struct CreateStorageCredentialRequest {
    credential_id: String,
    name: String,
    cloud: String,
    owner: String,
}

/// Storage credential route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route(
            "/storage-credentials",
            post(create_storage_credential).get(list_storage_credentials),
        )
        .route(
            "/storage-credentials/:credential_id",
            get(get_storage_credential),
        )
}

async fn create_storage_credential(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Json(request): Json<CreateStorageCredentialRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), UnityCatalogError> {
    require_storage_governance_admin(&state, &ctx)?;
    let ledger = ledger(&state, &ctx)?;
    let metastore = ledger.replay().await.map_err(map_catalog_error)?;
    let storage_state =
        StorageGovernanceState::from_metastore_state(&metastore).map_err(map_catalog_error)?;
    if storage_state
        .get_storage_credential(&request.credential_id)
        .map_err(map_catalog_error)?
        .is_some()
    {
        return Err(UnityCatalogError::Conflict {
            message: format!(
                "already exists: storage_credential {}",
                request.credential_id
            ),
        });
    }

    let scope = control_plane_scope(&ctx)?;
    let event = MetastoreEvent::new_scoped(
        &scope,
        ulid::Ulid::new().to_string(),
        ledger.next_sequence().await.map_err(map_catalog_error)?,
        MetastoreMutation::StorageCredentialUpserted(StorageCredentialRecord {
            credential_id: request.credential_id,
            name: request.name,
            cloud: request.cloud,
            owner: request.owner,
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: Utc::now().timestamp_millis(),
            properties: BTreeMap::new(),
        }),
    );
    ledger
        .append_event(&event)
        .await
        .map_err(map_catalog_error)?;

    let MetastoreMutation::StorageCredentialUpserted(record) = event.mutation else {
        unreachable!("constructed storage credential event")
    };
    Ok((
        StatusCode::CREATED,
        Json(storage_credential_payload(
            &record,
            Some(event.event_id.as_str()),
        )),
    ))
}

async fn list_storage_credentials(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
) -> Result<(StatusCode, Json<serde_json::Value>), UnityCatalogError> {
    require_storage_governance_admin(&state, &ctx)?;
    let ledger = ledger(&state, &ctx)?;
    let metastore = ledger.replay().await.map_err(map_catalog_error)?;
    let credentials = metastore
        .storage_credentials
        .values()
        .map(|record| storage_credential_payload(record, metastore.ledger_watermark.as_deref()))
        .collect::<Vec<_>>();

    Ok((
        StatusCode::OK,
        Json(json!({
            "storage_credentials": credentials,
            "ledger_watermark": metastore.ledger_watermark,
        })),
    ))
}

async fn get_storage_credential(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(credential_id): Path<String>,
) -> Result<(StatusCode, Json<serde_json::Value>), UnityCatalogError> {
    require_storage_governance_admin(&state, &ctx)?;
    let ledger = ledger(&state, &ctx)?;
    let metastore = ledger.replay().await.map_err(map_catalog_error)?;
    let Some(record) = metastore.storage_credentials.get(&credential_id) else {
        return Err(UnityCatalogError::NotFound {
            message: format!("not found: storage_credential {credential_id}"),
        });
    };

    Ok((
        StatusCode::OK,
        Json(storage_credential_payload(
            record,
            metastore.ledger_watermark.as_deref(),
        )),
    ))
}

pub(crate) fn storage_credential_payload(
    record: &StorageCredentialRecord,
    ledger_watermark: Option<&str>,
) -> serde_json::Value {
    json!({
        "credential_id": record.credential_id,
        "name": record.name,
        "cloud": record.cloud,
        "owner": record.owner,
        "lifecycle_state": record.lifecycle_state.as_str(),
        "updated_at": record.updated_at_ms,
        "ledger_watermark": ledger_watermark,
    })
}

fn ledger(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
) -> Result<MetastoreLedger, UnityCatalogError> {
    Ok(MetastoreLedger::new(scoped_storage(state, ctx)?))
}

fn require_storage_governance_admin(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
) -> Result<(), UnityCatalogError> {
    let scope = control_plane_scope(ctx)?;
    require_authz(
        state,
        ctx,
        scope.metastore_id(),
        "METASTORE",
        Privilege::Manage,
        "storage_governance_denied",
    )
}

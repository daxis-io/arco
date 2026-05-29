//! External location UC adapter routes.

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
    ExternalLocationRecord, LifecycleState, MetastoreEvent, MetastoreMutation,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::storage_governance::StorageGovernanceState;
use arco_catalog::storage_governance::external_locations::ExternalLocation;

use crate::context::UnityCatalogRequestContext;
use crate::error::UnityCatalogError;
use crate::routes::common::{
    control_plane_scope, map_catalog_error, require_authz, scoped_storage,
};
use crate::state::UnityCatalogState;

#[derive(Debug, Deserialize)]
struct CreateExternalLocationRequest {
    location_id: String,
    name: String,
    url: String,
    credential_id: String,
    owner: String,
}

/// External location route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route(
            "/external-locations",
            post(create_external_location).get(list_external_locations),
        )
        .route("/external-locations/:name", get(get_external_location))
}

async fn create_external_location(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Json(request): Json<CreateExternalLocationRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), UnityCatalogError> {
    require_storage_governance_admin(&state, &ctx)?;
    let ledger = ledger(&state, &ctx)?;
    let metastore = ledger.replay().await.map_err(map_catalog_error)?;
    let mut storage_state =
        StorageGovernanceState::from_metastore_state(&metastore).map_err(map_catalog_error)?;

    let location = ExternalLocation::new(
        request.location_id.clone(),
        request.name.clone(),
        &request.url,
        request.credential_id.clone(),
        request.owner.clone(),
    )
    .map_err(map_catalog_error)?;
    storage_state
        .create_external_location(location.clone())
        .map_err(map_catalog_error)?;

    let scope = control_plane_scope(&ctx)?;
    let event = MetastoreEvent::new_scoped(
        &scope,
        ulid::Ulid::new().to_string(),
        ledger.next_sequence().await.map_err(map_catalog_error)?,
        MetastoreMutation::ExternalLocationUpserted(ExternalLocationRecord {
            location_id: location.location_id,
            name: location.name,
            url: location.path.canonical_uri(),
            credential_id: location.credential_id,
            owner: location.owner,
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: Utc::now().timestamp_millis(),
            properties: BTreeMap::new(),
        }),
    );
    ledger
        .append_event(&event)
        .await
        .map_err(map_catalog_error)?;

    let MetastoreMutation::ExternalLocationUpserted(record) = event.mutation else {
        unreachable!("constructed external location event")
    };
    Ok((
        StatusCode::CREATED,
        Json(external_location_payload(
            &record,
            Some(event.event_id.as_str()),
        )),
    ))
}

async fn list_external_locations(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
) -> Result<(StatusCode, Json<serde_json::Value>), UnityCatalogError> {
    require_storage_governance_admin(&state, &ctx)?;
    let ledger = ledger(&state, &ctx)?;
    let metastore = ledger.replay().await.map_err(map_catalog_error)?;
    let locations = metastore
        .external_locations
        .values()
        .map(|record| external_location_payload(record, metastore.ledger_watermark.as_deref()))
        .collect::<Vec<_>>();

    Ok((
        StatusCode::OK,
        Json(json!({
            "external_locations": locations,
            "ledger_watermark": metastore.ledger_watermark,
        })),
    ))
}

async fn get_external_location(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<serde_json::Value>), UnityCatalogError> {
    require_storage_governance_admin(&state, &ctx)?;
    let ledger = ledger(&state, &ctx)?;
    let metastore = ledger.replay().await.map_err(map_catalog_error)?;
    let Some(record) = metastore
        .external_locations
        .values()
        .find(|record| record.name == name)
    else {
        return Err(UnityCatalogError::NotFound {
            message: format!("not found: external_location {name}"),
        });
    };

    Ok((
        StatusCode::OK,
        Json(external_location_payload(
            record,
            metastore.ledger_watermark.as_deref(),
        )),
    ))
}

fn external_location_payload(
    record: &ExternalLocationRecord,
    ledger_watermark: Option<&str>,
) -> serde_json::Value {
    json!({
        "location_id": record.location_id,
        "name": record.name,
        "url": record.url,
        "credential_id": record.credential_id,
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

//! External location UC adapter routes.

use std::collections::BTreeMap;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::Utc;
use serde::Deserialize;
use serde_json::json;

use arco_catalog::authz::privileges::Privilege;
use arco_catalog::idempotency::validate_uuidv7;
use arco_catalog::metastore::events::{
    ExternalLocationRecord, LifecycleState, ManagedRootRecord, MetastoreEvent, MetastoreMutation,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::metastore::projections::ProjectionRegistry;
use arco_catalog::metastore::publish::{
    MetastoreProjectionPublication, publish_current_metastore_projection,
};
use arco_catalog::storage_governance::StorageGovernanceState;
use arco_catalog::storage_governance::external_locations::ExternalLocation;
use arco_catalog::storage_governance::managed_roots::ManagedRoot;

use crate::context::UnityCatalogRequestContext;
use crate::error::UnityCatalogError;
use crate::routes::common::{
    control_plane_scope, governance_mutation_view, map_catalog_error,
    publish_storage_governance_projection, require_authz, scoped_storage,
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
        .route(
            "/storage-governance/managed-roots/:root_id",
            put(create_managed_root).delete(revoke_managed_root),
        )
        .route(
            "/storage-governance/projection/republish",
            post(republish_storage_governance_projection_route),
        )
}

async fn create_external_location(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Json(request): Json<CreateExternalLocationRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), UnityCatalogError> {
    require_storage_governance_admin(&state, &ctx).await?;
    // Self-heal before validating: republish any projection left stale by an
    // earlier commit whose synchronous publication failed (#362).
    publish_storage_governance_projection(&state, &ctx).await?;
    let ledger = ledger(&state, &ctx)?;
    let (metastore, sequence) = governance_mutation_view(&ledger).await?;
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

    // Belt-and-suspenders for path canonicalization: never persist a
    // canonical URL that does not round-trip through `GovernedPath::parse`.
    // A non-round-trip URL in the append-only ledger would fail every future
    // replay and leave the scope permanently deny-closed.
    let canonical_url = location
        .path
        .persistable_canonical_uri()
        .map_err(map_catalog_error)?;

    let scope = control_plane_scope(&ctx)?;
    let event = MetastoreEvent::new_scoped(
        &scope,
        ulid::Ulid::new().to_string(),
        sequence,
        MetastoreMutation::ExternalLocationUpserted(ExternalLocationRecord {
            location_id: location.location_id,
            name: location.name,
            url: canonical_url,
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
    // Commit-synchronous publication: the projection freshness rule requires
    // the published watermark to match the ledger exactly, so every committed
    // event must republish before vending can serve it (#362).
    publish_storage_governance_projection(&state, &ctx).await?;

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

async fn create_managed_root(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(root_id): Path<String>,
) -> Result<(StatusCode, Json<ManagedRootRecord>), UnityCatalogError> {
    require_storage_governance_admin(&state, &ctx).await?;
    validate_managed_root_id(&root_id)?;
    let storage = scoped_storage(&state, &ctx)?;
    let url = state
        .managed_delta_storage
        .as_ref()
        .ok_or_else(|| UnityCatalogError::ServiceUnavailable {
            message: "managed_delta_storage_unavailable".to_string(),
        })?
        .workspace_uri(&storage);
    publish_storage_governance_projection(&state, &ctx).await?;
    let ledger = MetastoreLedger::new(storage).map_err(map_catalog_error)?;
    let (metastore, sequence) = governance_mutation_view(&ledger).await?;
    if let Some(record) = metastore.managed_roots.get(&root_id) {
        if record.lifecycle_state == LifecycleState::Active
            && record.workspace_id == ctx.workspace
            && record.url == url
        {
            return Ok((StatusCode::OK, Json(record.clone())));
        }
        return Err(UnityCatalogError::Conflict {
            message: "managed_root_id_already_used".to_string(),
        });
    }
    if metastore.managed_roots.values().any(|record| {
        record.lifecycle_state == LifecycleState::Active && record.workspace_id == ctx.workspace
    }) {
        return Err(UnityCatalogError::Conflict {
            message: "workspace_managed_root_already_active".to_string(),
        });
    }
    let owner = ctx
        .user_id
        .clone()
        .ok_or_else(|| UnityCatalogError::Forbidden {
            message: "storage_governance_denied".to_string(),
        })?;
    let root = ManagedRoot::new(&root_id, "managed-delta", &ctx.workspace, &url, owner)
        .map_err(map_catalog_error)?;
    let mut governance =
        StorageGovernanceState::from_metastore_state(&metastore).map_err(map_catalog_error)?;
    let record = ManagedRootRecord {
        url: root
            .path
            .persistable_canonical_uri()
            .map_err(map_catalog_error)?,
        root_id: root.root_id.clone(),
        name: root.name.clone(),
        workspace_id: root.workspace_id.clone(),
        owner: root.owner.clone(),
        lifecycle_state: root.lifecycle_state,
        updated_at_ms: Utc::now().timestamp_millis(),
        properties: BTreeMap::new(),
    };
    governance
        .create_managed_root(root)
        .map_err(map_catalog_error)?;
    write_managed_root(&state, &ctx, &ledger, sequence, &record).await?;
    Ok((StatusCode::CREATED, Json(record)))
}

async fn revoke_managed_root(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(root_id): Path<String>,
) -> Result<(StatusCode, Json<ManagedRootRecord>), UnityCatalogError> {
    require_storage_governance_admin(&state, &ctx).await?;
    validate_managed_root_id(&root_id)?;
    publish_storage_governance_projection(&state, &ctx).await?;
    let ledger = ledger(&state, &ctx)?;
    let (mut metastore, sequence) = governance_mutation_view(&ledger).await?;
    let mut record = metastore
        .managed_roots
        .remove(&root_id)
        .filter(|record| record.workspace_id == ctx.workspace)
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: "managed_root_not_found".to_string(),
        })?;
    if record.lifecycle_state != LifecycleState::Deleted {
        record.lifecycle_state = LifecycleState::Deleted;
        record.updated_at_ms = Utc::now().timestamp_millis();
        write_managed_root(&state, &ctx, &ledger, sequence, &record).await?;
    }
    Ok((StatusCode::OK, Json(record)))
}

async fn write_managed_root(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    ledger: &MetastoreLedger,
    sequence: u64,
    record: &ManagedRootRecord,
) -> Result<(), UnityCatalogError> {
    let event = MetastoreEvent::new_scoped(
        &control_plane_scope(ctx)?,
        ulid::Ulid::new().to_string(),
        sequence,
        MetastoreMutation::ManagedRootUpserted(record.clone()),
    );
    ledger
        .append_event(&event)
        .await
        .map_err(map_catalog_error)?;
    publish_storage_governance_projection(state, ctx).await
}

fn validate_managed_root_id(root_id: &str) -> Result<(), UnityCatalogError> {
    validate_uuidv7(root_id)
        .map(|_| ())
        .map_err(|_| UnityCatalogError::BadRequest {
            message: "managed root ID must be a canonical UUIDv7".to_string(),
        })
}

async fn list_external_locations(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
) -> Result<(StatusCode, Json<serde_json::Value>), UnityCatalogError> {
    require_storage_governance_admin(&state, &ctx).await?;
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
    require_storage_governance_admin(&state, &ctx).await?;
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

/// Idempotent, non-mutating storage-governance projection republish (#362
/// recovery path).
///
/// Appends **no** ledger event: it only republishes the metastore projection
/// at the current ledger watermark, so a projection left stale by a failed
/// commit-synchronous publication can be healed immediately by an
/// administrator instead of waiting for a future governance POST. Requires
/// the same METASTORE `Manage` authorization as the other governance routes.
async fn republish_storage_governance_projection_route(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
) -> Result<(StatusCode, Json<serde_json::Value>), UnityCatalogError> {
    require_storage_governance_admin(&state, &ctx).await?;
    let storage = scoped_storage(&state, &ctx)?;
    let publication =
        publish_current_metastore_projection(&storage, &ProjectionRegistry::default())
            .await
            .map_err(map_catalog_error)?;

    let payload = match publication {
        MetastoreProjectionPublication::EmptyLedger => json!({
            "status": "empty_ledger",
        }),
        MetastoreProjectionPublication::AlreadyCurrent {
            ledger_watermark_sequence,
        } => json!({
            "status": "already_current",
            "ledger_watermark_sequence": ledger_watermark_sequence,
        }),
        MetastoreProjectionPublication::Published(manifest) => json!({
            "status": "published",
            "ledger_watermark": manifest.ledger_watermark,
            "ledger_watermark_sequence": manifest.ledger_watermark_sequence,
        }),
    };
    Ok((StatusCode::OK, Json(payload)))
}

fn ledger(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
) -> Result<MetastoreLedger, UnityCatalogError> {
    MetastoreLedger::new(scoped_storage(state, ctx)?).map_err(map_catalog_error)
}

async fn require_storage_governance_admin(
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
    .await
}

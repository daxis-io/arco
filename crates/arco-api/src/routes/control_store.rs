//! Operator-only control-store routes (`/internal/control-store/*`).
//!
//! # Why these live in `arco-api`
//!
//! Platform IAM makes `arco-api` the **sole writer** of the `state-store/`
//! object prefix and prohibits other service accounts from mutating it. Both
//! operations here write that prefix — the shadow import writes the isolated
//! shadow scope, and the projection-outbox operations write acknowledgement
//! and source-domain state — so no other service can host them. They were
//! previously mounted on `arco-compactor`, whose service account has no such
//! grant; that composition could never have worked in a real deployment.
//!
//! # Access
//!
//! These are roadmap Phase 4/5 operator surfaces ("write APIs behind internal
//! or operator-only access"), not tenant-facing routes:
//!
//! - They are mounted only when `control_store_operator_endpoints` is enabled
//!   (`ARCO_CONTROL_STORE_OPERATOR_ENDPOINTS`, default off). When disabled the
//!   routes do not exist and requests 404.
//! - They sit behind the same authentication middleware as every other
//!   authenticated route, so the tenant/workspace scope they operate on is the
//!   *verified* request scope, never a caller-supplied one.
//!
//! The shadow import writes only the isolated shadow scope and never
//! authority. The projection-outbox endpoint preserves the single-consumer
//! binding semantics of the underlying worker, including the deliberate
//! force-rebind escape hatch.

use std::sync::Arc;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;

use arco_catalog::CatalogError;
use arco_catalog::state_store::projection_outbox_acks::{
    AckOnlyProjectionHandler, ProjectionOutboxWorker,
};
use arco_catalog::state_store::shadow_replay::{
    ShadowComparisonStatus, ShadowDifferenceClass, import_current_catalog_shadow,
};

use crate::context::RequestContext;
use crate::error::ApiError;
use crate::server::AppState;

/// Returns the operator-only control-store routes.
///
/// The caller mounts these under `/internal` and applies the authentication
/// layer; see [`crate::server`].
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/control-store/projection-outbox",
            post(projection_outbox_handler),
        )
        .route("/control-store/shadow-import", post(shadow_import_handler))
}

/// Operator request against one source domain's projection outbox.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlStoreOutboxRequest {
    source_domain: String,
    consumer_id: String,
    /// Operator drain: acknowledge pending records WITHOUT projecting them.
    #[serde(default)]
    drain: bool,
    /// Trim events this consumer already acknowledged from the source outbox.
    #[serde(default)]
    trim: bool,
    /// Explicit writer epoch for source/ack commits. Omit to operate
    /// cooperatively at each domain's currently published epoch.
    #[serde(default)]
    writer_epoch: Option<u64>,
    /// Deliberately transfer the source domain's single-consumer drain/trim
    /// authority to `consumer_id`; the response reports the previous binding
    /// and the newly minted binding incarnation.
    #[serde(default)]
    force_rebind_consumer: bool,
}

async fn projection_outbox_handler(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Json(request): Json<ControlStoreOutboxRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let storage = ctx.scoped_storage(state.storage_backend()?)?;
    let worker =
        ProjectionOutboxWorker::new(storage, &request.source_domain, request.consumer_id.clone())
            .map_err(control_store_error)?;

    // An externally supplied epoch is a request to publish *at* that epoch,
    // never a grant of authority over it: the store additionally requires it
    // to equal the published pointer epoch, and refuses u64::MAX outright.
    let worker = match request.writer_epoch {
        Some(epoch) => worker
            .with_writer_epoch(epoch)
            .map_err(control_store_error)?,
        None => worker,
    };

    let rebind_report = if request.force_rebind_consumer {
        Some(
            worker
                .rebind_consumer()
                .await
                .map_err(control_store_error)?,
        )
    } else {
        None
    };
    let drain_report = if request.drain {
        Some(
            worker
                .drain(&AckOnlyProjectionHandler)
                .await
                .map_err(control_store_error)?,
        )
    } else {
        None
    };
    let trim_report = if request.trim {
        Some(worker.trim_acked().await.map_err(control_store_error)?)
    } else {
        None
    };
    let backlog = worker.backlog().await.map_err(control_store_error)?;
    let freshness = worker.freshness().await.map_err(control_store_error)?;

    Ok(Json(serde_json::json!({
        "backlog": backlog,
        "freshness": format!("{freshness:?}"),
        "drain": drain_report,
        "trim": trim_report,
        "rebind": rebind_report,
    })))
}

async fn shadow_import_handler(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
) -> Result<impl IntoResponse, ApiError> {
    let storage = ctx.scoped_storage(state.storage_backend()?)?;
    let report = import_current_catalog_shadow(&storage)
        .await
        .map_err(control_store_error)?;
    let comparisons = report
        .comparisons()
        .iter()
        .map(|comparison| {
            serde_json::json!({
                "domain": format!("{:?}", comparison.domain()),
                "status": shadow_status_label(comparison.status()),
                "detail": comparison.detail(),
            })
        })
        .collect::<Vec<_>>();
    let deferred = report
        .deferred_domains()
        .iter()
        .map(|entry| {
            serde_json::json!({
                "domain": format!("{:?}", entry.domain()),
                "reason": entry.reason(),
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(serde_json::json!({
        "sourceManifestId": report.source().manifest_id(),
        "snapshotVersion": report.source().snapshot_version(),
        "comparisons": comparisons,
        "deferred": deferred,
    })))
}

fn shadow_status_label(status: ShadowComparisonStatus) -> &'static str {
    match status {
        ShadowComparisonStatus::Equivalent => "equivalent",
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::CurrentStateGap) => {
            "current_state_gap"
        }
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::UnsupportedScope) => {
            "unsupported_scope"
        }
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::StaleProjection) => {
            "stale_projection"
        }
        ShadowComparisonStatus::Difference(ShadowDifferenceClass::BugDivergentResult) => {
            "bug_divergent_result"
        }
    }
}

/// Maps a control-store failure onto the API error contract, attaching the
/// operator hint that says how to make the refused operation legitimate.
///
/// The typed status comes from the existing `CatalogError` mapping, so a
/// fenced writer, a binding conflict, and a corrupt artifact stay
/// distinguishable instead of collapsing into one opaque failure.
fn control_store_error(error: CatalogError) -> ApiError {
    let hint = match &error {
        CatalogError::StaleWriterEpoch { .. } => Some(
            "pass writerEpoch equal to the published epoch, or omit it to \
             operate cooperatively at the current epoch",
        ),
        CatalogError::PreconditionFailed { message } if message.contains("bound to consumer") => {
            Some(
                "pass forceRebindConsumer=true to deliberately transfer the \
                 single-consumer drain/trim authority to this consumerId",
            )
        }
        CatalogError::PreconditionFailed { message } if message.contains("never claimed") => Some(
            "writerEpoch must equal the published pointer epoch; only a \
             writer-authority claim advances it, so a future epoch cannot \
             be published directly",
        ),
        CatalogError::PreconditionFailed { message }
            if message.contains("transferred mid-trim") =>
        {
            Some(
                "the source domain's consumer binding was rebound while this \
                 trim was in flight; re-run drain and trim under the current \
                 binding incarnation",
            )
        }
        _ => None,
    };
    let api_error = ApiError::from(error);
    match hint {
        Some(hint) => api_error.with_details(serde_json::json!({ "hint": hint })),
        None => api_error,
    }
}

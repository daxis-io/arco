//! Operator-only control-store routes (`/internal/control-store/*`).
//!
//! # Why these live in `arco-api`
//!
//! Platform IAM makes `arco-api` the **sole writer** of the `control/`
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
//! or operator-only access"), not tenant-facing routes. Reaching them requires
//! three independent conditions, and every one of them fails closed:
//!
//! - They are mounted only when `control_store_operator_endpoints` is enabled
//!   (`ARCO_CONTROL_STORE_OPERATOR_ENDPOINTS`, default off). When disabled the
//!   routes do not exist and requests 404. A public posture never mounts them.
//! - They sit behind the same authentication middleware as every other
//!   authenticated route, so the tenant/workspace scope they operate on is the
//!   *verified* request scope, never a caller-supplied one.
//! - **Authentication is not authorization here.** Every handler additionally
//!   requires the verified principal to carry the configured operator group
//!   (`control_store_operator_group` / `ARCO_CONTROL_STORE_OPERATOR_GROUP`).
//!   An ordinary valid tenant principal is refused with `403`, and when no
//!   operator group is configured *every* caller is refused — enabling the
//!   flag alone opens nothing.
//!
//! # Why an operator claim rather than a second credential
//!
//! `arco-compactor` and `arco-flow` gate their internal surfaces with a
//! dedicated `InternalOidcVerifier` middleware. That pattern cannot simply be
//! layered here: it consumes `Authorization`, and so does the tenant JWT
//! middleware these routes depend on to derive the verified tenant/workspace
//! scope they act upon. Two middlewares cannot both own that header, and a
//! service-account-only token carries no tenant scope, so it could not name
//! the workspace to operate on without reintroducing a caller-supplied scope —
//! exactly the property these routes must not have.
//!
//! The operator authority therefore rides *inside* the same verified token, as
//! a group claim. It inherits the issuer, audience, signature, and expiry
//! verification the tenant token already gets, keeps the scope binding
//! verified, and stays a repository-visible authorization boundary rather than
//! delegating the decision to network ingress rules. Ingress restrictions
//! remain useful defence in depth on top of it.
//!
//! Every mutation (drain, rebind, trim, shadow import) emits an audit record
//! naming the operator identity, and every refusal emits a deny record.
//!
//! The shadow import writes only the isolated shadow scope and never
//! authority. The projection-outbox endpoint preserves the single-consumer
//! binding semantics of the underlying worker, including the deliberate
//! force-rebind escape hatch.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use arco_catalog::manifest::SnapshotInfo;
use arco_catalog::state_store::projection_outbox_acks::{
    AckOnlyProjectionHandler, ProjectionOutboxWorker,
};
use arco_catalog::state_store::shadow_replay::{
    ShadowComparisonStatus, ShadowDifferenceClass, import_current_catalog_shadow,
};
use arco_catalog::{
    CATALOG_PARQUET_PROJECTION_CONSUMER_ID, CatalogError, CatalogProjectionMaterializer,
};
use arco_catalog::{
    CatalogAuthorityKind, CatalogDomainManifest, DomainManifestPointer, is_projection_only_artifact,
};
use arco_core::storage::ObjectMeta;
use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};

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
        .route(
            "/control-store/catalog-projection/urls",
            post(catalog_projection_urls_handler),
        )
}

const CATALOG_READ_DESCRIPTOR_VERSION: u32 = 1;
const CATALOG_READ_URL_TTL_SECONDS: u64 = 900;
const CATALOG_READ_FILES: [&str; 4] = [
    "catalogs.parquet",
    "namespaces.parquet",
    "tables.parquet",
    "columns.parquet",
];

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogProjectionReadDescriptor {
    descriptor_version: u32,
    scope: CatalogProjectionReadScope,
    authority: CatalogProjectionReadAuthority,
    projection: CatalogProjectionReadCut,
    ttl_seconds: u64,
    files: Vec<CatalogProjectionReadFile>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogProjectionReadScope {
    tenant_id: String,
    workspace_id: String,
    domain: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogProjectionReadAuthority {
    kind: &'static str,
    manifest_id: String,
    sequence: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogProjectionReadCut {
    sequence: u64,
    manifest_path: String,
    manifest_checksum_sha256: String,
    manifest_storage_version: String,
    manifest_etag: Option<String>,
    published_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogProjectionReadFile {
    path: String,
    format: &'static str,
    checksum_sha256: String,
    byte_size: u64,
    row_count: u64,
    storage_version: String,
    etag: Option<String>,
    url: String,
}

enum CatalogProjectionReadWitness {
    Legacy {
        pointer_path: String,
        pointer_bytes: Bytes,
    },
    ControlV1,
}

struct CatalogProjectionReadSource {
    authority_kind: &'static str,
    authority_manifest_id: String,
    authority_sequence: u64,
    projection_sequence: u64,
    manifest_path: String,
    manifest_bytes: Bytes,
    snapshot: SnapshotInfo,
    witness: CatalogProjectionReadWitness,
}

/// Mint one complete, immutable catalog read set for a verified operator scope.
async fn catalog_projection_urls_handler(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    authorize_operator(&state, &ctx, "control-store/catalog-projection/urls")?;
    if !body.is_empty() && body.as_ref() != b"{}" {
        return Err(ApiError::bad_request(
            "catalog projection URL requests take no selectors",
        ));
    }
    let storage = ctx.scoped_storage(state.storage_backend()?)?;
    let source = load_catalog_read_source(&state, &ctx, &storage).await?;
    validate_catalog_read_source(&source)?;

    // Every allowlisted object must exist at the declared size before any URL
    // is minted. The second metadata pass below fences replacements during the
    // request without downloading whole Parquet objects.
    let manifest_meta = checked_head(&storage, &source.manifest_path, None).await?;
    let mut checked_files = Vec::with_capacity(CATALOG_READ_FILES.len());
    for name in CATALOG_READ_FILES {
        let file = source
            .snapshot
            .files
            .iter()
            .find(|file| file.path == name)
            .ok_or_else(ApiError::catalog_projection_unavailable)?;
        let path = format!(
            "{}/{}",
            source.snapshot.path.trim_end_matches('/'),
            file.path
        );
        if is_projection_only_artifact(&path) {
            return Err(ApiError::catalog_projection_unavailable());
        }
        let meta = checked_head(&storage, &path, Some(file.byte_size)).await?;
        checked_files.push((file, path, meta));
    }

    let mut files = Vec::with_capacity(CATALOG_READ_FILES.len());
    for (file, path, meta) in &checked_files {
        files.push(CatalogProjectionReadFile {
            path: path.clone(),
            format: "parquet",
            checksum_sha256: file.checksum_sha256.clone(),
            byte_size: file.byte_size,
            row_count: file.row_count,
            storage_version: meta.version.clone(),
            etag: meta.etag.clone(),
            url: storage
                .signed_url_raw(path, Duration::from_secs(CATALOG_READ_URL_TTL_SECONDS))
                .await?,
        });
    }
    verify_catalog_read_cut(&storage, &source, &manifest_meta, &checked_files).await?;

    crate::metrics::record_signed_urls_minted(files.len());
    Ok(Json(CatalogProjectionReadDescriptor {
        descriptor_version: CATALOG_READ_DESCRIPTOR_VERSION,
        scope: CatalogProjectionReadScope {
            tenant_id: ctx.tenant.clone(),
            workspace_id: ctx.workspace.clone(),
            domain: "catalog",
        },
        authority: CatalogProjectionReadAuthority {
            kind: source.authority_kind,
            manifest_id: source.authority_manifest_id,
            sequence: source.authority_sequence,
        },
        projection: CatalogProjectionReadCut {
            sequence: source.projection_sequence,
            manifest_path: source.manifest_path,
            manifest_checksum_sha256: hex::encode(Sha256::digest(&source.manifest_bytes)),
            manifest_storage_version: manifest_meta.version,
            manifest_etag: manifest_meta.etag,
            published_at: source.snapshot.published_at,
        },
        ttl_seconds: CATALOG_READ_URL_TTL_SECONDS,
        files,
    }))
}

async fn load_catalog_read_source(
    state: &AppState,
    ctx: &RequestContext,
    storage: &ScopedStorage,
) -> Result<CatalogProjectionReadSource, ApiError> {
    match state
        .catalog_authority_bindings()
        .resolve(&ctx.tenant, &ctx.workspace)
    {
        CatalogAuthorityKind::Legacy => {
            let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
            let pointer_bytes = storage
                .get_raw(&pointer_path)
                .await
                .map_err(|_| ApiError::catalog_projection_unavailable())?;
            let pointer: DomainManifestPointer = serde_json::from_slice(&pointer_bytes)
                .map_err(|_| ApiError::catalog_projection_unavailable())?;
            let manifest_path = CatalogPaths::domain_manifest_snapshot(
                CatalogDomain::Catalog,
                &pointer.manifest_id,
            );
            if pointer.manifest_path != manifest_path {
                return Err(ApiError::catalog_projection_unavailable());
            }
            let manifest_bytes = storage
                .get_raw(&manifest_path)
                .await
                .map_err(|_| ApiError::catalog_projection_unavailable())?;
            let manifest: CatalogDomainManifest = serde_json::from_slice(&manifest_bytes)
                .map_err(|_| ApiError::catalog_projection_unavailable())?;
            if manifest.manifest_id != pointer.manifest_id {
                return Err(ApiError::catalog_projection_unavailable());
            }
            let snapshot = manifest
                .snapshot
                .ok_or_else(ApiError::catalog_projection_unavailable)?;
            Ok(CatalogProjectionReadSource {
                authority_kind: "legacy",
                authority_manifest_id: pointer.manifest_id,
                authority_sequence: manifest.snapshot_version,
                projection_sequence: manifest.snapshot_version,
                manifest_path,
                manifest_bytes,
                snapshot,
                witness: CatalogProjectionReadWitness::Legacy {
                    pointer_path,
                    pointer_bytes,
                },
            })
        }
        CatalogAuthorityKind::ControlV1 => {
            let materializer =
                CatalogProjectionMaterializer::new(storage.clone()).map_err(control_store_error)?;
            let status = materializer
                .status()
                .await
                .map_err(control_store_error)?
                .ok_or_else(ApiError::catalog_projection_unavailable)?;
            let backlog = ProjectionOutboxWorker::new(
                storage.clone(),
                "catalog",
                CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
            )
            .map_err(control_store_error)?
            .backlog()
            .await
            .map_err(control_store_error)?;
            let sequence = status
                .applied_authority_sequence()
                .ok_or_else(ApiError::catalog_projection_unavailable)?;
            if status.failure_state().is_some()
                || status.observed_authority_sequence() != Some(sequence)
                || backlog.committed_sequence != Some(sequence)
                || !backlog.pending_record_ids.is_empty()
            {
                return Err(ApiError::catalog_projection_unavailable());
            }
            let manifest_path = status
                .artifact_manifest_path()
                .filter(|path| !path.contains(".."))
                .ok_or_else(ApiError::catalog_projection_unavailable)?
                .to_string();
            let authority_manifest_id = control_projection_manifest_id(&manifest_path, sequence)
                .ok_or_else(ApiError::catalog_projection_unavailable)?;
            let manifest_bytes = storage
                .get_raw(&manifest_path)
                .await
                .map_err(|_| ApiError::catalog_projection_unavailable())?;
            let snapshot: SnapshotInfo = serde_json::from_slice(&manifest_bytes)
                .map_err(|_| ApiError::catalog_projection_unavailable())?;
            Ok(CatalogProjectionReadSource {
                authority_kind: "controlV1",
                authority_manifest_id,
                authority_sequence: sequence,
                projection_sequence: sequence,
                manifest_path,
                manifest_bytes,
                snapshot,
                witness: CatalogProjectionReadWitness::ControlV1,
            })
        }
    }
}

fn validate_catalog_read_source(source: &CatalogProjectionReadSource) -> Result<(), ApiError> {
    let directory = source
        .manifest_path
        .strip_suffix("manifest.json")
        .unwrap_or_default();
    let declared_bytes = source
        .snapshot
        .files
        .iter()
        .try_fold(0_u64, |total, file| total.checked_add(file.byte_size));
    let declared_rows = source
        .snapshot
        .files
        .iter()
        .try_fold(0_u64, |total, file| total.checked_add(file.row_count));
    if CATALOG_READ_FILES.iter().any(|name| {
        source
            .snapshot
            .files
            .iter()
            .filter(|file| file.path == *name)
            .count()
            != 1
    }) || source.snapshot.version != source.projection_sequence
        || source.snapshot.files.iter().any(|file| {
            file.path.contains('/')
                || file.path.contains("..")
                || !is_sha256_hex(&file.checksum_sha256)
        })
        || declared_bytes != Some(source.snapshot.total_bytes)
        || declared_rows != Some(source.snapshot.total_rows)
        || (source.manifest_path.starts_with("control/v1/") && source.snapshot.path != directory)
        || (!source.manifest_path.starts_with("control/v1/")
            && (!source
                .snapshot
                .path
                .starts_with(&CatalogPaths::snapshot_dir(
                    CatalogDomain::Catalog,
                    source.snapshot.version,
                ))
                || !source.snapshot.path.ends_with('/')
                || source.snapshot.path.contains("..")))
    {
        return Err(ApiError::catalog_projection_unavailable());
    }
    Ok(())
}

fn control_projection_manifest_id(path: &str, sequence: u64) -> Option<String> {
    let directory = path
        .strip_prefix("control/v1/projections/catalog-parquet/")?
        .strip_suffix("/manifest.json")?;
    let (encoded_sequence, manifest_id) = directory.split_once('-')?;
    (encoded_sequence.len() == 20
        && encoded_sequence.parse::<u64>().ok() == Some(sequence)
        && !manifest_id.is_empty()
        && !manifest_id.contains('/'))
    .then(|| manifest_id.to_string())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn checked_head(
    storage: &ScopedStorage,
    path: &str,
    expected_size: Option<u64>,
) -> Result<ObjectMeta, ApiError> {
    let meta = storage
        .head_raw(path)
        .await
        .map_err(|_| ApiError::catalog_projection_unavailable())?
        .ok_or_else(ApiError::catalog_projection_unavailable)?;
    if meta.version.is_empty() || expected_size.is_some_and(|size| meta.size != size) {
        return Err(ApiError::catalog_projection_unavailable());
    }
    Ok(meta)
}

async fn verify_catalog_read_cut(
    storage: &ScopedStorage,
    source: &CatalogProjectionReadSource,
    manifest_meta: &ObjectMeta,
    files: &[(&arco_catalog::manifest::SnapshotFile, String, ObjectMeta)],
) -> Result<(), ApiError> {
    let final_manifest_bytes = storage
        .get_raw(&source.manifest_path)
        .await
        .map_err(|_| ApiError::catalog_projection_unavailable())?;
    let final_manifest_meta = checked_head(storage, &source.manifest_path, None).await?;
    if final_manifest_bytes != source.manifest_bytes
        || final_manifest_meta.version != manifest_meta.version
        || final_manifest_meta.size != manifest_meta.size
        || final_manifest_meta.etag != manifest_meta.etag
    {
        return Err(ApiError::catalog_projection_unavailable());
    }
    for (file, path, initial) in files {
        let final_meta = checked_head(storage, path, Some(file.byte_size)).await?;
        if final_meta.version != initial.version
            || final_meta.size != initial.size
            || final_meta.etag != initial.etag
        {
            return Err(ApiError::catalog_projection_unavailable());
        }
    }
    match &source.witness {
        CatalogProjectionReadWitness::Legacy {
            pointer_path,
            pointer_bytes,
        } => {
            let final_pointer = storage
                .get_raw(pointer_path)
                .await
                .map_err(|_| ApiError::catalog_projection_unavailable())?;
            if final_pointer != *pointer_bytes {
                return Err(ApiError::catalog_projection_unavailable());
            }
        }
        CatalogProjectionReadWitness::ControlV1 => {
            let materializer =
                CatalogProjectionMaterializer::new(storage.clone()).map_err(control_store_error)?;
            let status = materializer
                .status()
                .await
                .map_err(control_store_error)?
                .ok_or_else(ApiError::catalog_projection_unavailable)?;
            let backlog = ProjectionOutboxWorker::new(
                storage.clone(),
                "catalog",
                CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
            )
            .map_err(control_store_error)?
            .backlog()
            .await
            .map_err(control_store_error)?;
            if status.failure_state().is_some()
                || status.applied_authority_sequence() != Some(source.projection_sequence)
                || status.observed_authority_sequence() != Some(source.projection_sequence)
                || status.artifact_manifest_path() != Some(source.manifest_path.as_str())
                || backlog.committed_sequence != Some(source.authority_sequence)
                || !backlog.pending_record_ids.is_empty()
            {
                return Err(ApiError::catalog_projection_unavailable());
            }
        }
    }
    Ok(())
}

/// Operator request against one source domain's projection outbox.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlStoreOutboxRequest {
    source_domain: String,
    consumer_id: String,
    /// Drain pending records. Catalog uses the materializer; other internal
    /// domains retain the explicitly operator-only acknowledgement handler.
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

/// Refuses any principal that does not hold the configured operator authority.
///
/// Fails closed in both directions: an authenticated principal without the
/// group is refused, and so is *every* principal when no group is configured.
/// The refusal never falls back to "authenticated is good enough", because the
/// operations behind it (materialization/drain, binding transfer, source trim)
/// can silently destroy projection data for the legitimate consumer.
fn authorize_operator(
    state: &AppState,
    ctx: &RequestContext,
    resource: &str,
) -> Result<(), ApiError> {
    let configured = state
        .config
        .control_store_operator_group
        .as_deref()
        .map(str::trim)
        .filter(|group| !group.is_empty());

    let Some(group) = configured else {
        crate::audit::emit_control_store_deny(
            state,
            ctx,
            resource,
            crate::audit::REASON_OPERATOR_AUTHORITY_UNCONFIGURED,
        );
        return Err(ApiError::forbidden(
            "control-store operator endpoints are enabled but no operator authority is \
             configured, so every caller is refused",
        )
        .with_request_id(ctx.request_id.clone())
        .with_details(serde_json::json!({
            "hint": "set control_store_operator_group \
                     (ARCO_CONTROL_STORE_OPERATOR_GROUP) to the group that may drain, \
                     rebind, and trim; enabling the endpoints alone grants nothing",
        })));
    };

    if !ctx.groups.iter().any(|held| held == group) {
        crate::audit::emit_control_store_deny(
            state,
            ctx,
            resource,
            crate::audit::REASON_NOT_OPERATOR,
        );
        return Err(ApiError::forbidden(
            "control-store operator endpoints require operator authority; an authenticated \
             tenant principal is not sufficient",
        )
        .with_request_id(ctx.request_id.clone())
        .with_details(serde_json::json!({
            "hint": "the principal's verified groups claim must carry the group named by \
                     control_store_operator_group",
        })));
    }

    Ok(())
}

async fn projection_outbox_handler(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Json(request): Json<ControlStoreOutboxRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let resource = format!("control-store/projection-outbox:{}", request.source_domain);
    authorize_operator(&state, &ctx, &resource)?;

    if request.source_domain == "catalog" {
        if request.consumer_id != CATALOG_PARQUET_PROJECTION_CONSUMER_ID {
            return Err(ApiError::bad_request(format!(
                "the catalog projection outbox is reserved for consumer {CATALOG_PARQUET_PROJECTION_CONSUMER_ID}"
            )));
        }
        if request.force_rebind_consumer || request.trim {
            return Err(ApiError::bad_request(
                "catalog projection acknowledgements are isolated from catalog authority; source-domain rebind and trim are unsupported",
            ));
        }
    }

    let storage = ctx.scoped_storage(state.storage_backend()?)?;
    let worker = ProjectionOutboxWorker::new(
        storage.clone(),
        &request.source_domain,
        request.consumer_id.clone(),
    )
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

    // Each mutation is audited as it lands, so a request that rebinds and
    // drains but then fails to trim leaves a record of exactly what changed.
    let rebind_report = if request.force_rebind_consumer {
        let report = worker
            .rebind_consumer()
            .await
            .map_err(control_store_error)?;
        crate::audit::emit_control_store_mutation(&state, &ctx, &format!("{resource}:rebind"));
        Some(report)
    } else {
        None
    };
    let drain_report = if request.drain {
        let report = if request.source_domain == "catalog" {
            let materializer =
                CatalogProjectionMaterializer::new(storage).map_err(control_store_error)?;
            let materializer = match request.writer_epoch {
                Some(epoch) => materializer
                    .with_writer_epoch(epoch)
                    .map_err(control_store_error)?,
                None => materializer,
            };
            materializer
                .drain_once()
                .await
                .map_err(control_store_error)?
        } else {
            worker
                .drain(&AckOnlyProjectionHandler)
                .await
                .map_err(control_store_error)?
        };
        crate::audit::emit_control_store_mutation(&state, &ctx, &format!("{resource}:drain"));
        Some(report)
    } else {
        None
    };
    let trim_report = if request.trim {
        let report = worker.trim_acked().await.map_err(control_store_error)?;
        crate::audit::emit_control_store_mutation(&state, &ctx, &format!("{resource}:trim"));
        Some(report)
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
    let resource = "control-store/shadow-import".to_string();
    authorize_operator(&state, &ctx, &resource)?;

    let storage = ctx.scoped_storage(state.storage_backend()?)?;
    let report = import_current_catalog_shadow(&storage)
        .await
        .map_err(control_store_error)?;
    crate::audit::emit_control_store_mutation(&state, &ctx, &resource);
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

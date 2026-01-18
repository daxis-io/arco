//! Manifest management API routes for M1.
//!
//! Provides manifest upload and retrieval endpoints for asset deployment.
//!
//! ## Routes
//!
//! - `POST   /workspaces/{workspace_id}/manifests` - Deploy a new manifest
//! - `GET    /workspaces/{workspace_id}/manifests/{manifest_id}` - Get manifest by ID
//! - `GET    /workspaces/{workspace_id}/manifests` - List manifests

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ulid::Ulid;
use utoipa::ToSchema;

use crate::context::RequestContext;
use crate::error::{ApiError, ApiErrorBody};
use crate::orchestration_compaction::compact_orchestration_events;
use crate::paths::{
    MANIFEST_IDEMPOTENCY_PREFIX, MANIFEST_PREFIX, manifest_idempotency_path, manifest_path,
};
use crate::server::AppState;
use arco_core::{Error as CoreError, ScopedStorage, WritePrecondition, WriteResult};
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use arco_flow::orchestration::ledger::LedgerWriter;

// ============================================================================
// Request/Response Types
// ============================================================================

/// Request to deploy a new manifest.
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DeployManifestRequest {
    /// Manifest version (e.g., "1.0").
    pub manifest_version: String,
    /// Code version identifier (e.g., git commit SHA).
    pub code_version_id: String,
    /// Git context for provenance.
    #[serde(default)]
    pub git: GitContext,
    /// Asset definitions.
    pub assets: Vec<AssetEntry>,
    /// Cron schedules (optional).
    #[serde(default)]
    pub schedules: Vec<ScheduleEntry>,
    /// Who deployed this manifest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployed_by: Option<String>,
    /// Additional metadata.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// Git context for deployment provenance.
#[derive(Debug, Default, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitContext {
    /// Repository URL.
    #[serde(default)]
    pub repository: String,
    /// Branch name.
    #[serde(default)]
    pub branch: String,
    /// Commit SHA.
    #[serde(default)]
    pub commit_sha: String,
    /// Commit message.
    #[serde(default)]
    pub commit_message: String,
    /// Author.
    #[serde(default)]
    pub author: String,
    /// Whether working directory was dirty.
    #[serde(default)]
    pub dirty: bool,
}

/// Asset entry in manifest.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AssetEntry {
    /// Asset key (namespace + name).
    pub key: AssetKey,
    /// Asset ID (UUID).
    pub id: String,
    /// Asset description.
    #[serde(default)]
    pub description: String,
    /// Asset owners.
    #[serde(default)]
    pub owners: Vec<String>,
    /// Asset tags.
    #[serde(default)]
    pub tags: serde_json::Value,
    /// Partitioning configuration.
    #[serde(default)]
    pub partitioning: serde_json::Value,
    /// Dependencies on other assets.
    #[serde(default)]
    pub dependencies: Vec<AssetDependency>,
    /// Code location.
    #[serde(default)]
    pub code: serde_json::Value,
    /// Data quality checks.
    #[serde(default)]
    pub checks: Vec<serde_json::Value>,
    /// Execution configuration.
    #[serde(default)]
    pub execution: serde_json::Value,
    /// Resource requirements.
    #[serde(default)]
    pub resources: serde_json::Value,
    /// I/O configuration.
    #[serde(default)]
    pub io: serde_json::Value,
    /// Transform fingerprint for change detection.
    #[serde(default)]
    pub transform_fingerprint: String,
}

/// Asset key (namespace + name).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AssetKey {
    /// Namespace.
    pub namespace: String,
    /// Asset name.
    pub name: String,
}

/// Asset dependency.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AssetDependency {
    /// Upstream asset key.
    pub upstream_key: AssetKey,
    /// Parameter name in function signature.
    #[serde(default)]
    pub parameter_name: String,
    /// Partition mapping strategy.
    #[serde(default)]
    pub mapping: String,
}

/// Cron schedule entry.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleEntry {
    /// Schedule identifier.
    pub id: String,
    /// Cron expression.
    pub cron: String,
    /// Assets to trigger.
    pub assets: Vec<String>,
    /// Timezone.
    #[serde(default = "default_timezone")]
    pub timezone: String,
}

fn default_timezone() -> String {
    "UTC".to_string()
}

/// Response after deploying a manifest.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeployManifestResponse {
    /// Manifest ID (ULID).
    pub manifest_id: String,
    /// Workspace ID.
    pub workspace_id: String,
    /// Code version ID.
    pub code_version_id: String,
    /// Content fingerprint (SHA-256).
    pub fingerprint: String,
    /// Number of assets in manifest.
    pub asset_count: u32,
    /// Deployment timestamp.
    pub deployed_at: DateTime<Utc>,
}

/// Stored manifest metadata (what gets persisted).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct StoredManifest {
    /// Manifest ID.
    pub manifest_id: String,
    /// Tenant ID.
    pub tenant_id: String,
    /// Workspace ID.
    pub workspace_id: String,
    /// Manifest version.
    pub manifest_version: String,
    /// Code version ID.
    pub code_version_id: String,
    /// Content fingerprint.
    pub fingerprint: String,
    /// Git context.
    pub git: GitContext,
    /// Asset entries.
    pub assets: Vec<AssetEntry>,
    /// Schedules.
    pub schedules: Vec<ScheduleEntry>,
    /// Deployment timestamp.
    pub deployed_at: DateTime<Utc>,
    /// Who deployed this.
    pub deployed_by: String,
    /// Additional metadata.
    pub metadata: serde_json::Value,
}

/// Manifest list item.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ManifestListItem {
    /// Manifest ID.
    pub manifest_id: String,
    /// Code version ID.
    pub code_version_id: String,
    /// Content fingerprint.
    pub fingerprint: String,
    /// Number of assets.
    pub asset_count: u32,
    /// Git branch (if available).
    pub git_branch: Option<String>,
    /// Git commit (if available).
    pub git_commit: Option<String>,
    /// Deployment timestamp.
    pub deployed_at: DateTime<Utc>,
    /// Who deployed this.
    pub deployed_by: String,
}

/// Response for listing manifests.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListManifestsResponse {
    /// List of manifests.
    pub manifests: Vec<ManifestListItem>,
}

const MAX_MANIFEST_BYTES: usize = 10 * 1024 * 1024;
const MANIFEST_LATEST_INDEX_PATH: &str = "manifests/_index.json";

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LatestManifestIndex {
    latest_manifest_id: String,
    deployed_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestIdempotencyRecord {
    idempotency_key: String,
    manifest_id: String,
    workspace_id: String,
    code_version_id: String,
    fingerprint: String,
    asset_count: u32,
    deployed_at: DateTime<Utc>,
}

// ============================================================================
// Helpers
// ============================================================================

fn ensure_workspace(ctx: &RequestContext, workspace_id: &str) -> Result<(), ApiError> {
    if workspace_id != ctx.workspace {
        return Err(ApiError::not_found("workspace not found"));
    }
    Ok(())
}

fn validate_manifest(request: &DeployManifestRequest) -> Result<(), ApiError> {
    if request.manifest_version.trim().is_empty() {
        return Err(ApiError::bad_request("manifestVersion is required"));
    }
    if request.code_version_id.trim().is_empty() {
        return Err(ApiError::bad_request("codeVersionId is required"));
    }
    if request.assets.is_empty() {
        return Err(ApiError::bad_request(
            "manifest must contain at least one asset",
        ));
    }

    let mut asset_keys = HashSet::new();
    for asset in &request.assets {
        if asset.key.namespace.trim().is_empty() || asset.key.name.trim().is_empty() {
            return Err(ApiError::bad_request(
                "asset key must include namespace and name",
            ));
        }
        if asset.id.trim().is_empty() {
            return Err(ApiError::bad_request("asset id is required"));
        }

        let canonical = arco_flow::orchestration::canonicalize_asset_key(&format!(
            "{}/{}",
            asset.key.namespace, asset.key.name
        ))
        .map_err(ApiError::bad_request)?;

        if !asset_keys.insert(canonical) {
            return Err(ApiError::bad_request("duplicate asset key in manifest"));
        }
    }

    for asset in &request.assets {
        let asset_key = arco_flow::orchestration::canonicalize_asset_key(&format!(
            "{}/{}",
            asset.key.namespace, asset.key.name
        ))
        .map_err(ApiError::bad_request)?;

        for dependency in &asset.dependencies {
            let upstream = arco_flow::orchestration::canonicalize_asset_key(&format!(
                "{}/{}",
                dependency.upstream_key.namespace, dependency.upstream_key.name
            ))
            .map_err(ApiError::bad_request)?;

            if !asset_keys.contains(&upstream) {
                return Err(ApiError::bad_request(format!(
                    "asset '{asset_key}' depends on unknown asset '{upstream}'",
                )));
            }
        }
    }

    let mut schedule_ids = HashSet::new();
    for schedule in &request.schedules {
        if schedule.id.trim().is_empty() {
            return Err(ApiError::bad_request("schedule id is required"));
        }
        if schedule.cron.trim().is_empty() {
            return Err(ApiError::bad_request("schedule cron is required"));
        }

        let field_count = schedule.cron.split_whitespace().count();
        if field_count != 5 && field_count != 6 {
            return Err(ApiError::bad_request(
                "schedule cron must have 5 fields (min..dow) or 6 fields (sec..dow)",
            ));
        }
        let normalized = if field_count == 5 {
            format!("0 {}", schedule.cron)
        } else {
            schedule.cron.clone()
        };
        if cron::Schedule::from_str(&normalized).is_err() {
            return Err(ApiError::bad_request("invalid schedule cron"));
        }

        if schedule.timezone.parse::<chrono_tz::Tz>().is_err() {
            return Err(ApiError::bad_request("invalid schedule timezone"));
        }
        if schedule.assets.is_empty() {
            return Err(ApiError::bad_request(
                "schedule must include at least one asset",
            ));
        }

        for asset in &schedule.assets {
            let canonical = arco_flow::orchestration::canonicalize_asset_key(asset)
                .map_err(ApiError::bad_request)?;

            if !asset_keys.contains(&canonical) {
                return Err(ApiError::bad_request(format!(
                    "schedule '{}' references unknown asset '{canonical}'",
                    schedule.id
                )));
            }
        }

        if !schedule_ids.insert(schedule.id.clone()) {
            return Err(ApiError::bad_request("duplicate schedule id in manifest"));
        }
    }

    Ok(())
}

fn compute_fingerprint(request: &DeployManifestRequest) -> Result<String, ApiError> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct FingerprintPayload {
        manifest_version: String,
        code_version_id: String,
        git: GitContext,
        assets: Vec<AssetEntry>,
        schedules: Vec<ScheduleEntry>,
        metadata: serde_json::Value,
    }

    let mut assets = request.assets.clone();
    assets.sort_by(|a, b| {
        (&a.key.namespace, &a.key.name, &a.id).cmp(&(&b.key.namespace, &b.key.name, &b.id))
    });

    let mut schedules = request.schedules.clone();
    schedules
        .sort_by(|a, b| (a.id.as_str(), a.cron.as_str()).cmp(&(b.id.as_str(), b.cron.as_str())));

    let payload = FingerprintPayload {
        manifest_version: request.manifest_version.clone(),
        code_version_id: request.code_version_id.clone(),
        git: request.git.clone(),
        assets,
        schedules,
        metadata: request.metadata.clone(),
    };

    let json = serde_json::to_vec(&payload).map_err(|e| {
        ApiError::internal(format!("failed to serialize manifest for fingerprint: {e}"))
    })?;
    let hash = Sha256::digest(&json);
    Ok(hex::encode(hash))
}

async fn load_idempotency_record(
    storage: &ScopedStorage,
    idempotency_key: &str,
) -> Result<Option<ManifestIdempotencyRecord>, ApiError> {
    let path = manifest_idempotency_path(idempotency_key);
    match storage.get_raw(&path).await {
        Ok(bytes) => {
            let record: ManifestIdempotencyRecord =
                serde_json::from_slice(&bytes).map_err(|e| {
                    ApiError::internal(format!("failed to parse idempotency record: {e}"))
                })?;
            Ok(Some(record))
        }
        Err(CoreError::NotFound(_) | CoreError::ResourceNotFound { .. }) => Ok(None),
        Err(err) => Err(ApiError::internal(format!(
            "failed to read idempotency record: {err}"
        ))),
    }
}

async fn upsert_schedule_definitions(
    state: &AppState,
    storage: ScopedStorage,
    tenant_id: &str,
    workspace_id: &str,
    manifest_id: &str,
    schedules: &[ScheduleEntry],
) -> Result<(), ApiError> {
    if schedules.is_empty() {
        return Ok(());
    }

    fn normalize_asset_selection(values: &[String]) -> Vec<String> {
        let mut values = values.to_vec();
        values.sort();
        values
    }

    let compactor = MicroCompactor::new(storage.clone());
    let (_, fold_state) = compactor
        .load_state()
        .await
        .map_err(|e| ApiError::internal(format!("failed to load orchestration state: {e}")))?;

    let mut events: Vec<OrchestrationEvent> = Vec::new();
    for schedule in schedules {
        let existing = fold_state.schedule_definitions.get(schedule.id.as_str());

        let enabled = existing.map_or(true, |row| row.enabled);
        let catchup_window_minutes = existing.map_or(0, |row| row.catchup_window_minutes);
        let max_catchup_ticks = existing.map_or(1, |row| row.max_catchup_ticks);

        let needs_upsert = existing.is_none_or(|row| {
            row.cron_expression != schedule.cron
                || row.timezone != schedule.timezone
                || row.enabled != enabled
                || row.catchup_window_minutes != catchup_window_minutes
                || row.max_catchup_ticks != max_catchup_ticks
                || normalize_asset_selection(&row.asset_selection)
                    != normalize_asset_selection(&schedule.assets)
        });

        if !needs_upsert {
            continue;
        }

        let mut event = OrchestrationEvent::new(
            tenant_id,
            workspace_id,
            OrchestrationEventData::ScheduleDefinitionUpserted {
                schedule_id: schedule.id.clone(),
                cron_expression: schedule.cron.clone(),
                timezone: schedule.timezone.clone(),
                catchup_window_minutes,
                asset_selection: schedule.assets.clone(),
                max_catchup_ticks,
                enabled,
            },
        );

        if let Some(existing) = existing {
            while event.event_id <= existing.row_version {
                event.event_id = Ulid::new().to_string();
            }
        }

        event.idempotency_key = format!("sched_def_manifest:{manifest_id}:{}", schedule.id);
        events.push(event);
    }

    if events.is_empty() {
        return Ok(());
    }

    let ledger = LedgerWriter::new(storage.clone());
    let event_paths: Vec<String> = events.iter().map(LedgerWriter::event_path).collect();
    ledger
        .append_all(events)
        .await
        .map_err(|e| ApiError::internal(format!("failed to append schedule events: {e}")))?;

    compact_orchestration_events(&state.config, storage, event_paths).await?;

    Ok(())
}

// ============================================================================
// Route Handlers
// ============================================================================

/// Deploy a new manifest.
///
/// Uploads and registers an asset manifest for the workspace.
#[utoipa::path(
    post,
    path = "/api/v1/workspaces/{workspace_id}/manifests",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID")
    ),
    request_body = DeployManifestRequest,
    responses(
        (status = 201, description = "Manifest deployed", body = DeployManifestResponse),
        (status = 400, description = "Invalid manifest", body = ApiErrorBody),
        (status = 404, description = "Workspace not found", body = ApiErrorBody),
    ),
    tag = "Manifests",
    security(
        ("bearerAuth" = [])
    )
)]
#[allow(clippy::too_many_lines)]
pub(crate) async fn deploy_manifest(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path(workspace_id): Path<String>,
    Json(request): Json<DeployManifestRequest>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        workspace_id = %workspace_id,
        code_version = %request.code_version_id,
        asset_count = request.assets.len(),
        "Deploying manifest"
    );

    ensure_workspace(&ctx, &workspace_id)?;
    validate_manifest(&request)?;

    let fingerprint = compute_fingerprint(&request)?;
    let idempotency_key = ctx.idempotency_key.clone();

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    if let Some(key) = idempotency_key.as_deref() {
        if let Some(existing) = load_idempotency_record(&storage, key).await? {
            if existing.fingerprint == fingerprint {
                upsert_schedule_definitions(
                    state.as_ref(),
                    storage.clone(),
                    &ctx.tenant,
                    &workspace_id,
                    &existing.manifest_id,
                    &request.schedules,
                )
                .await?;

                return Ok((
                    axum::http::StatusCode::OK,
                    Json(DeployManifestResponse {
                        manifest_id: existing.manifest_id,
                        workspace_id: existing.workspace_id,
                        code_version_id: existing.code_version_id,
                        fingerprint: existing.fingerprint,
                        asset_count: existing.asset_count,
                        deployed_at: existing.deployed_at,
                    }),
                ));
            }

            return Err(ApiError::conflict(
                "idempotency key already used for a different manifest",
            ));
        }
    }

    // Generate IDs
    let manifest_id = Ulid::new().to_string();
    let now = Utc::now();
    let deployed_by = request
        .deployed_by
        .clone()
        .or_else(|| ctx.user_id.clone())
        .unwrap_or_else(|| "api".to_string());

    // Create stored manifest
    let stored = StoredManifest {
        manifest_id: manifest_id.clone(),
        tenant_id: ctx.tenant.clone(),
        workspace_id: workspace_id.clone(),
        manifest_version: request.manifest_version.clone(),
        code_version_id: request.code_version_id.clone(),
        fingerprint: fingerprint.clone(),
        git: request.git.clone(),
        assets: request.assets.clone(),
        schedules: request.schedules.clone(),
        deployed_at: now,
        deployed_by: deployed_by.clone(),
        metadata: request.metadata.clone(),
    };

    let manifest_json = serde_json::to_string_pretty(&stored)
        .map_err(|e| ApiError::internal(format!("failed to serialize manifest: {e}")))?;

    let path = manifest_path(&manifest_id);
    let result = storage
        .put_raw(
            &path,
            Bytes::from(manifest_json),
            WritePrecondition::DoesNotExist,
        )
        .await
        .map_err(|e| ApiError::internal(format!("failed to store manifest: {e}")))?;

    if matches!(result, WriteResult::PreconditionFailed { .. }) {
        return Err(ApiError::conflict("manifest already exists; please retry"));
    }

    if let Some(key) = idempotency_key {
        let record = ManifestIdempotencyRecord {
            idempotency_key: key.clone(),
            manifest_id: manifest_id.clone(),
            workspace_id: workspace_id.clone(),
            code_version_id: request.code_version_id.clone(),
            fingerprint: fingerprint.clone(),
            asset_count: u32::try_from(request.assets.len()).unwrap_or(u32::MAX),
            deployed_at: now,
        };

        let record_json = serde_json::to_string(&record).map_err(|e| {
            ApiError::internal(format!("failed to serialize idempotency record: {e}"))
        })?;
        let record_path = manifest_idempotency_path(&key);
        let record_result = storage
            .put_raw(
                &record_path,
                Bytes::from(record_json),
                WritePrecondition::DoesNotExist,
            )
            .await
            .map_err(|e| ApiError::internal(format!("failed to store idempotency record: {e}")))?;

        if matches!(record_result, WriteResult::PreconditionFailed { .. }) {
            tracing::warn!(
                idempotency_key = %key,
                manifest_id = %manifest_id,
                "idempotency record already exists after manifest write"
            );
        }
    }

    upsert_schedule_definitions(
        state.as_ref(),
        storage.clone(),
        &ctx.tenant,
        &workspace_id,
        &manifest_id,
        &request.schedules,
    )
    .await?;

    tracing::info!(
        manifest_id = %manifest_id,
        fingerprint = %fingerprint,
        "Manifest deployed successfully"
    );

    Ok((
        axum::http::StatusCode::CREATED,
        Json(DeployManifestResponse {
            manifest_id,
            workspace_id,
            code_version_id: request.code_version_id,
            fingerprint,
            asset_count: u32::try_from(request.assets.len()).unwrap_or(u32::MAX),
            deployed_at: now,
        }),
    ))
}

/// Get manifest by ID.
///
/// Returns the full manifest including all asset definitions.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/manifests/{manifest_id}",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID"),
        ("manifest_id" = String, Path, description = "Manifest ID")
    ),
    responses(
        (status = 200, description = "Manifest details", body = StoredManifest),
        (status = 404, description = "Manifest not found", body = ApiErrorBody),
    ),
    tag = "Manifests",
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn get_manifest(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path((workspace_id, manifest_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        workspace_id = %workspace_id,
        manifest_id = %manifest_id,
        "Getting manifest"
    );

    ensure_workspace(&ctx, &workspace_id)?;

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    let path = manifest_path(&manifest_id);
    let bytes = match storage.get_raw(&path).await {
        Ok(bytes) => bytes,
        Err(CoreError::NotFound(_) | CoreError::ResourceNotFound { .. }) => {
            return Err(ApiError::not_found(format!(
                "manifest not found: {manifest_id}"
            )));
        }
        Err(err) => {
            return Err(ApiError::internal(format!(
                "failed to read manifest: {err}"
            )));
        }
    };

    let manifest: StoredManifest = serde_json::from_slice(&bytes)
        .map_err(|e| ApiError::internal(format!("failed to parse manifest: {e}")))?;

    Ok(Json(manifest))
}

/// List manifests.
///
/// Returns a list of deployed manifests for the workspace.
#[utoipa::path(
    get,
    path = "/api/v1/workspaces/{workspace_id}/manifests",
    params(
        ("workspace_id" = String, Path, description = "Workspace ID")
    ),
    responses(
        (status = 200, description = "List of manifests", body = ListManifestsResponse),
        (status = 404, description = "Workspace not found", body = ApiErrorBody),
    ),
    tag = "Manifests",
    security(
        ("bearerAuth" = [])
    )
)]
pub(crate) async fn list_manifests(
    State(state): State<Arc<AppState>>,
    ctx: RequestContext,
    Path(workspace_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::info!(
        workspace_id = %workspace_id,
        "Listing manifests"
    );

    ensure_workspace(&ctx, &workspace_id)?;

    let backend = state.storage_backend()?;
    let storage = ctx.scoped_storage(backend)?;

    // List manifest files
    let prefix = MANIFEST_PREFIX;
    let entries = storage
        .list_meta(prefix)
        .await
        .map_err(|e| ApiError::internal(format!("failed to list manifests: {e}")))?;

    let mut manifests = Vec::new();
    for entry in entries {
        let path = entry.path.as_str();
        // Skip non-JSON files and potential index files
        let is_json = std::path::Path::new(path)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"));
        if !is_json
            || path.ends_with("_index.json")
            || path.starts_with(MANIFEST_IDEMPOTENCY_PREFIX)
        {
            continue;
        }

        let bytes = match storage.get_raw(path).await {
            Ok(b) => b,
            Err(err) => {
                tracing::warn!(path = %path, error = ?err, "failed to read manifest; skipping");
                continue;
            }
        };

        let stored: StoredManifest = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(err) => {
                tracing::warn!(path = %path, error = ?err, "failed to parse manifest; skipping");
                continue;
            }
        };

        manifests.push(ManifestListItem {
            manifest_id: stored.manifest_id,
            code_version_id: stored.code_version_id,
            fingerprint: stored.fingerprint,
            asset_count: u32::try_from(stored.assets.len()).unwrap_or(u32::MAX),
            git_branch: if stored.git.branch.is_empty() {
                None
            } else {
                Some(stored.git.branch)
            },
            git_commit: if stored.git.commit_sha.is_empty() {
                None
            } else {
                Some(stored.git.commit_sha)
            },
            deployed_at: stored.deployed_at,
            deployed_by: stored.deployed_by,
        });
    }

    // Sort by deployed_at descending
    manifests.sort_by(|a, b| b.deployed_at.cmp(&a.deployed_at));

    Ok(Json(ListManifestsResponse { manifests }))
}

// ============================================================================
// Router
// ============================================================================

/// Creates the manifest routes.
pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/workspaces/:workspace_id/manifests", post(deploy_manifest))
        .route("/workspaces/:workspace_id/manifests", get(list_manifests))
        .route(
            "/workspaces/:workspace_id/manifests/:manifest_id",
            get(get_manifest),
        )
        .layer(DefaultBodyLimit::max(MAX_MANIFEST_BYTES))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deploy_request_deserialization() {
        let json = r#"{
            "manifestVersion": "1.0",
            "codeVersionId": "abc123",
            "git": {
                "branch": "main",
                "commitSha": "abc123def456"
            },
            "assets": [{
                "key": {"namespace": "analytics", "name": "users"},
                "id": "01HQXYZ123",
                "description": "User analytics asset"
            }]
        }"#;

        let request: DeployManifestRequest = serde_json::from_str(json).expect("deserialize");
        assert_eq!(request.manifest_version, "1.0");
        assert_eq!(request.assets.len(), 1);
        assert_eq!(request.assets[0].key.namespace, "analytics");
        assert_eq!(request.assets[0].key.name, "users");
    }

    #[test]
    fn test_stored_manifest_serialization() {
        let stored = StoredManifest {
            manifest_id: "01HQXYZ123".to_string(),
            tenant_id: "tenant-1".to_string(),
            workspace_id: "workspace-1".to_string(),
            manifest_version: "1.0".to_string(),
            code_version_id: "abc123".to_string(),
            fingerprint: "deadbeef".to_string(),
            git: GitContext {
                branch: "main".to_string(),
                commit_sha: "abc123".to_string(),
                ..Default::default()
            },
            assets: vec![],
            schedules: vec![],
            deployed_at: Utc::now(),
            deployed_by: "test-user".to_string(),
            metadata: serde_json::Value::Null,
        };

        let json = serde_json::to_string(&stored).expect("serialize");
        assert!(json.contains("manifestId"));
        assert!(json.contains("workspaceId"));
    }
}

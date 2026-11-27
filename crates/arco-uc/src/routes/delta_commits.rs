//! Delta commit coordination routes for the Unity Catalog facade.
//!
//! This is an implemented subsystem backed by Delta coordinator state. It is
//! authoritative for Delta commit coordination, but it is not catalog DDL or a
//! generic governance metadata surface.

use axum::Json;
use axum::Router;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::routing::get;

use arco_catalog::authz::privileges::Privilege;
use arco_catalog::managed_delta::resolve_analytical_storage;
use arco_core::error::Error as CoreError;
use arco_core::storage::{StorageBackend as _, WritePrecondition, WriteResult};
use arco_core::{DeltaPaths, ScopedStorage};
use arco_delta::{CommitDeltaRequest, DeltaCommitCoordinator, DeltaError};
use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tracing::warn;
use uuid::Uuid;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};
use crate::routes::common;
use crate::state::UnityCatalogState;

/// Delta commit route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new().route(
        "/delta/preview/commits",
        get(get_delta_preview_commits).post(post_delta_preview_commits),
    )
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[schema(title = "DeltaGetCommitsRequestBody")]
/// Request payload for `GET /delta/preview/commits`.
pub(crate) struct DeltaGetCommitsRequestBody {
    /// Target table identifier (UUID string).
    pub table_id: String,
    /// Target table URI (opaque storage location).
    pub table_uri: String,
    /// Starting version for listing commits.
    pub start_version: Option<i64>,
    /// Ending version for listing commits.
    pub end_version: Option<i64>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[schema(title = "DeltaCommitRequestBody")]
/// Request payload for `POST /delta/preview/commits`.
pub(crate) struct DeltaCommitRequestBody {
    /// Target table identifier (UUID string).
    pub table_id: String,
    /// Target table URI (opaque storage location).
    pub table_uri: String,
    /// Commit metadata to register.
    pub commit_info: Option<DeltaCommitInfo>,
    /// Latest version that has been backfilled into the Delta log.
    pub latest_backfilled_version: Option<i64>,
    /// Optional metadata envelope.
    pub metadata: Option<Value>,
    /// Optional uniform settings envelope.
    pub uniform: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
/// Metadata about a coordinated Delta commit.
pub(crate) struct DeltaCommitInfo {
    /// Delta log version.
    pub version: i64,
    /// Commit timestamp (milliseconds since epoch).
    pub timestamp: i64,
    /// Commit file name (e.g. `00000000000000000000.json`).
    pub file_name: String,
    /// Commit file size in bytes.
    pub file_size: i64,
    /// Commit file modification timestamp (milliseconds since epoch).
    pub file_modification_timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DeltaPreviewBackfillState {
    latest_backfilled_version: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    table_uri: Option<String>,
}

const BACKFILL_STATE_CAS_RETRIES: usize = 16;
const MAX_UNBACKFILLED_COMMITS_PER_TABLE: i64 = 1000;
// shortcut: cap ranges at 100 entries and 8 MiB, add cursor pagination if clients need automatic traversal.
const MAX_PREVIEW_HISTORY_VERSIONS: i64 = 100;
const MAX_PREVIEW_HISTORY_BYTES: u64 = 8 * 1024 * 1024;

fn delta_preview_backfill_path(table_id: Uuid) -> String {
    format!("uc/delta_preview/{table_id}.json")
}

fn deterministic_staged_path(table_id: Uuid, idempotency_key: &str) -> String {
    format!("delta/staging/{table_id}/{idempotency_key}.json")
}

fn validate_table_uri(table_uri: &str) -> UnityCatalogResult<()> {
    if table_uri.trim().is_empty() {
        return Err(UnityCatalogError::BadRequest {
            message: "table_uri must not be empty".to_string(),
        });
    }
    Ok(())
}

fn table_uri_mismatch_error(table_id: Uuid) -> UnityCatalogError {
    UnityCatalogError::BadRequest {
        message: format!("table_uri mismatch for table {table_id}"),
    }
}

async fn reject_event_stream_target(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    storage: &ScopedStorage,
    table_id: Uuid,
) -> UnityCatalogResult<()> {
    if state
        .config
        .usage_bindings
        .table_id(&ctx.tenant, &ctx.workspace)
        == Some(table_id)
        || crate::is_usage_table(storage, table_id)
            .await
            .map_err(|error| map_internal_error("check usage table ownership", error))?
    {
        return Err(UnityCatalogError::Forbidden {
            message: "usage_table_requires_owner_api".to_string(),
        });
    }
    if state
        .config
        .event_streams
        .table_ids(&ctx.tenant, &ctx.workspace)
        .any(|bound| bound == table_id)
        || crate::is_event_stream_table(storage, table_id)
            .await
            .map_err(|error| map_internal_error("check event stream table ownership", error))?
    {
        return Err(UnityCatalogError::Forbidden {
            message: "event_stream_table_requires_owner_api".to_string(),
        });
    }
    Ok(())
}

struct DeltaTarget {
    paths: DeltaPaths,
    log_storage: ScopedStorage,
}

impl DeltaTarget {
    fn coordinator(&self, catalog: &ScopedStorage) -> DeltaCommitCoordinator {
        DeltaCommitCoordinator::with_paths(catalog.clone(), self.paths.clone())
            .with_log_storage(self.log_storage.clone())
    }
}

async fn require_catalog_managed_delta_table(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    storage: &ScopedStorage,
    table_id: Uuid,
    table_uri: &str,
    privilege: Privilege,
) -> UnityCatalogResult<DeltaTarget> {
    reject_event_stream_target(state, ctx, storage, table_id).await?;
    let table_id_text = table_id.to_string();
    let reader = common::authoritative_catalog_reader(state, ctx)
        .await?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("table not found: {table_id_text}"),
        })?;
    let table = reader
        .get_table_by_id(&table_id_text)
        .await
        .map_err(common::map_catalog_error)?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("table not found: {table_id_text}"),
        })?;

    common::require_authz(
        state,
        ctx,
        &table_id_text,
        "TABLE",
        privilege,
        "delta_preview_denied",
    )
    .await?;

    let is_delta = table
        .format
        .as_deref()
        .is_some_and(|format| format.eq_ignore_ascii_case("delta"));
    let is_managed = table
        .table_type
        .as_deref()
        .is_none_or(|table_type| table_type.eq_ignore_ascii_case("managed"));
    if !is_delta || !is_managed {
        return Err(UnityCatalogError::Conflict {
            message: format!(
                "delta preview commits require a managed Delta table: {table_id_text}"
            ),
        });
    }

    let location = table
        .location
        .as_deref()
        .ok_or_else(|| UnityCatalogError::BadRequest {
            message: format!("table has no storage location: {table_id_text}"),
        })?;
    if location != table_uri {
        return Err(table_uri_mismatch_error(table_id));
    }

    let analytical =
        resolve_analytical_storage(state.managed_delta_storage.as_ref(), storage, &table)
            .await
            .map_err(common::map_catalog_error)?;
    let paths = if analytical.is_some() {
        DeltaPaths::legacy(table_id)
    } else {
        DeltaPaths::from_table_location(table_id, Some(location), &ctx.tenant, &ctx.workspace)
            .map_err(|_| UnityCatalogError::BadRequest {
                message: "registered Delta table location is invalid".to_string(),
            })?
    };
    if let Some(root_table_id) = paths.canonical_root_table_id()
        && root_table_id != table_id
    {
        reject_event_stream_target(state, ctx, storage, root_table_id).await?;
    }
    Ok(DeltaTarget {
        paths,
        log_storage: analytical.unwrap_or_else(|| storage.clone()),
    })
}

fn validate_idempotency_key(idempotency_key: &str) -> UnityCatalogResult<()> {
    let parsed = Uuid::parse_str(idempotency_key).map_err(|_| UnityCatalogError::BadRequest {
        message: "Idempotency-Key must be a valid UUID".to_string(),
    })?;

    if parsed.get_version_num() != 7 {
        return Err(UnityCatalogError::BadRequest {
            message: "Idempotency-Key must be UUIDv7 (RFC 9562)".to_string(),
        });
    }

    if parsed.to_string() != idempotency_key {
        return Err(UnityCatalogError::BadRequest {
            message: "Idempotency-Key must be a canonical lowercase UUID string".to_string(),
        });
    }

    Ok(())
}

fn validate_version_range(
    start_version: Option<i64>,
    end_version: Option<i64>,
) -> UnityCatalogResult<()> {
    if let Some(start_version) = start_version {
        if start_version < 0 {
            return Err(UnityCatalogError::BadRequest {
                message: "start_version must be >= 0".to_string(),
            });
        }
    }

    if let Some(end_version) = end_version {
        if end_version < 0 {
            return Err(UnityCatalogError::BadRequest {
                message: "end_version must be >= 0".to_string(),
            });
        }
    }

    if let (Some(start_version), Some(end_version)) = (start_version, end_version) {
        if start_version > end_version {
            return Err(UnityCatalogError::BadRequest {
                message: "start_version must be <= end_version".to_string(),
            });
        }
    }

    Ok(())
}

fn expected_file_name_for_version(version: i64) -> Option<String> {
    u64::try_from(version)
        .ok()
        .map(|version| format!("{version:020}.json"))
}

fn validate_commit_info(commit_info: &DeltaCommitInfo) -> UnityCatalogResult<()> {
    if commit_info.version < 0 {
        return Err(UnityCatalogError::BadRequest {
            message: "commit_info.version must be >= 0".to_string(),
        });
    }
    if commit_info.file_size < 0 {
        return Err(UnityCatalogError::BadRequest {
            message: "commit_info.file_size must be >= 0".to_string(),
        });
    }

    let expected_file_name =
        expected_file_name_for_version(commit_info.version).ok_or_else(|| {
            UnityCatalogError::BadRequest {
                message: "commit_info.version must be >= 0".to_string(),
            }
        })?;
    if commit_info.file_name != expected_file_name {
        return Err(UnityCatalogError::BadRequest {
            message: format!(
                "commit_info.file_name must equal {expected_file_name} for version {}",
                commit_info.version
            ),
        });
    }

    Ok(())
}

fn map_delta_error(err: DeltaError) -> UnityCatalogError {
    match err {
        DeltaError::BadRequest { message } => UnityCatalogError::BadRequest { message },
        DeltaError::Conflict { message } => UnityCatalogError::Conflict { message },
        DeltaError::NotFound { message } => UnityCatalogError::NotFound { message },
        DeltaError::Serialization { message } => UnityCatalogError::Internal { message },
        DeltaError::Storage(err) => match err {
            CoreError::NotFound(_) | CoreError::ResourceNotFound { .. } => {
                UnityCatalogError::NotFound {
                    message: "table not found".to_string(),
                }
            }
            other => UnityCatalogError::Internal {
                message: other.to_string(),
            },
        },
        error => UnityCatalogError::Internal {
            message: error.to_string(),
        },
    }
}

fn map_internal_error(context: &str, err: impl std::fmt::Display) -> UnityCatalogError {
    UnityCatalogError::Internal {
        message: format!("failed to {context}: {err}"),
    }
}

async fn load_backfill_state(
    storage: &ScopedStorage,
    table_id: Uuid,
) -> UnityCatalogResult<Option<DeltaPreviewBackfillState>> {
    let path = delta_preview_backfill_path(table_id);
    let meta = storage
        .head_raw(&path)
        .await
        .map_err(|err| map_internal_error("read delta preview backfill state metadata", err))?;
    if meta.is_none() {
        return Ok(None);
    }

    let bytes = storage
        .get_raw(&path)
        .await
        .map_err(|err| map_internal_error("read delta preview backfill state", err))?;
    let state: DeltaPreviewBackfillState = serde_json::from_slice(&bytes)
        .map_err(|err| map_internal_error("deserialize delta preview backfill state", err))?;
    Ok(Some(state))
}

async fn store_backfill_state(
    storage: &ScopedStorage,
    table_id: Uuid,
    state: &DeltaPreviewBackfillState,
) -> UnityCatalogResult<()> {
    let path = delta_preview_backfill_path(table_id);
    for _ in 0..BACKFILL_STATE_CAS_RETRIES {
        let head = storage
            .head_raw(&path)
            .await
            .map_err(|err| map_internal_error("read delta preview backfill state metadata", err))?;

        let (current, expected_version) = if let Some(meta) = head {
            let bytes = storage
                .get_raw(&path)
                .await
                .map_err(|err| map_internal_error("read delta preview backfill state", err))?;
            let current =
                serde_json::from_slice::<DeltaPreviewBackfillState>(&bytes).map_err(|err| {
                    map_internal_error("deserialize delta preview backfill state", err)
                })?;
            (current, Some(meta.version))
        } else {
            (DeltaPreviewBackfillState::default(), None)
        };

        let merged = DeltaPreviewBackfillState {
            latest_backfilled_version: match (
                current.latest_backfilled_version,
                state.latest_backfilled_version,
            ) {
                (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                (Some(existing), None) => Some(existing),
                (None, Some(incoming)) => Some(incoming),
                (None, None) => None,
            },
            table_uri: match (current.table_uri.as_ref(), state.table_uri.as_ref()) {
                (Some(existing), Some(incoming)) if existing != incoming => {
                    return Err(table_uri_mismatch_error(table_id));
                }
                (Some(existing), _) => Some(existing.clone()),
                (None, Some(incoming)) => Some(incoming.clone()),
                (None, None) => None,
            },
        };

        if merged.latest_backfilled_version == current.latest_backfilled_version
            && merged.table_uri == current.table_uri
        {
            return Ok(());
        }

        let bytes = serde_json::to_vec(&merged)
            .map_err(|err| map_internal_error("serialize delta preview backfill state", err))?;
        let precondition = expected_version.map_or(WritePrecondition::DoesNotExist, |version| {
            WritePrecondition::MatchesVersion(version)
        });
        let write = storage
            .put_raw(&path, Bytes::from(bytes), precondition)
            .await
            .map_err(|err| map_internal_error("write delta preview backfill state", err))?;
        match write {
            WriteResult::Success { .. } => return Ok(()),
            WriteResult::PreconditionFailed { .. } => {}
        }
    }

    Err(UnityCatalogError::Conflict {
        message: "failed to update delta preview backfill state after concurrent writes"
            .to_string(),
    })
}

async fn stage_payload_for_idempotency(
    storage: &ScopedStorage,
    table_id: Uuid,
    idempotency_key: &str,
    payload: Bytes,
) -> UnityCatalogResult<arco_delta::StagedCommit> {
    let staged_path = deterministic_staged_path(table_id, idempotency_key);

    if let Some(meta) = storage
        .head_raw(&staged_path)
        .await
        .map_err(|err| map_internal_error("read staged payload metadata", err))?
    {
        let existing = storage
            .get_raw(&staged_path)
            .await
            .map_err(|err| map_internal_error("read staged payload", err))?;
        if existing != payload {
            return Err(UnityCatalogError::Conflict {
                message: "Idempotency-Key already used with different request body".to_string(),
            });
        }

        return Ok(arco_delta::StagedCommit {
            staged_path,
            staged_version: meta.version,
        });
    }

    let write = storage
        .put_raw(
            &staged_path,
            payload.clone(),
            WritePrecondition::DoesNotExist,
        )
        .await
        .map_err(|err| map_internal_error("write staged payload", err))?;

    match write {
        WriteResult::Success { version } => Ok(arco_delta::StagedCommit {
            staged_path,
            staged_version: version,
        }),
        WriteResult::PreconditionFailed { .. } => {
            let meta = storage
                .head_raw(&staged_path)
                .await
                .map_err(|err| map_internal_error("read staged payload metadata after race", err))?
                .ok_or_else(|| UnityCatalogError::Conflict {
                    message: "staged payload conflict after concurrent write".to_string(),
                })?;
            let existing = storage
                .get_raw(&staged_path)
                .await
                .map_err(|err| map_internal_error("read staged payload after race", err))?;
            if existing != payload {
                return Err(UnityCatalogError::Conflict {
                    message: "Idempotency-Key already used with different request body".to_string(),
                });
            }

            Ok(arco_delta::StagedCommit {
                staged_path,
                staged_version: meta.version,
            })
        }
    }
}

fn build_commit_payload(
    table_uri: &str,
    commit_info: &DeltaCommitInfo,
    metadata: Option<&Value>,
    uniform: Option<&Value>,
) -> UnityCatalogResult<Bytes> {
    let mut payload = Map::new();
    payload.insert(
        "commitInfo".to_string(),
        serde_json::to_value(commit_info)
            .map_err(|err| map_internal_error("serialize commit_info", err))?,
    );
    payload.insert("tableUri".to_string(), Value::String(table_uri.to_string()));
    if let Some(metadata) = metadata {
        payload.insert("metadata".to_string(), metadata.clone());
    }
    if let Some(uniform) = uniform {
        payload.insert("uniform".to_string(), uniform.clone());
    }

    let mut body = serde_json::to_vec(&Value::Object(payload))
        .map_err(|err| map_internal_error("serialize staged commit payload", err))?;
    body.push(b'\n');
    Ok(Bytes::from(body))
}

fn default_commit_info_for_payload(version: i64, payload_len: usize) -> DeltaCommitInfo {
    DeltaCommitInfo {
        version,
        timestamp: 0,
        file_name: expected_file_name_for_version(version)
            .unwrap_or_else(|| "00000000000000000000.json".to_string()),
        file_size: i64::try_from(payload_len).unwrap_or(i64::MAX),
        file_modification_timestamp: 0,
    }
}

fn parse_commit_info_from_payload(version: i64, payload: &[u8]) -> DeltaCommitInfo {
    let parsed = serde_json::from_slice::<Value>(payload).ok();
    let mut info = parsed
        .as_ref()
        .and_then(|value| value.get("commitInfo"))
        .and_then(|value| serde_json::from_value::<DeltaCommitInfo>(value.clone()).ok())
        .unwrap_or_else(|| default_commit_info_for_payload(version, payload.len()));

    info.version = version;
    if info.file_name.trim().is_empty() {
        info.file_name = expected_file_name_for_version(version)
            .unwrap_or_else(|| "00000000000000000000.json".to_string());
    }
    if info.file_size < 0 {
        info.file_size = i64::try_from(payload.len()).unwrap_or(i64::MAX);
    }
    info
}

async fn preview_finalized_version(
    coordinator: &DeltaCommitCoordinator,
    target: &DeltaTarget,
) -> UnityCatalogResult<Option<i64>> {
    if let Some(state) = coordinator
        .state_for_root()
        .await
        .map_err(map_delta_error)?
    {
        return Ok(Some(state.latest_version));
    }
    let existing = target
        .log_storage
        .list_page_meta(&target.paths.delta_log_prefix(), None, 1)
        .await
        .map_err(|err| map_internal_error("check for uncoordinated Delta logs", err))?;
    if !existing.objects.is_empty() || existing.next_start_after.is_some() {
        return Err(UnityCatalogError::Conflict {
            message: "delta_preview_log_migration_required".to_string(),
        });
    }
    Ok(None)
}

fn ensure_table_uri_matches(
    table_id: Uuid,
    requested_table_uri: &str,
    expected_table_uri: Option<&str>,
) -> UnityCatalogResult<()> {
    if let Some(expected_table_uri) = expected_table_uri {
        if requested_table_uri != expected_table_uri {
            return Err(table_uri_mismatch_error(table_id));
        }
    }

    Ok(())
}

async fn list_commits(
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    start_version: i64,
    end_version: Option<i64>,
    latest_version: i64,
) -> UnityCatalogResult<Vec<DeltaCommitInfo>> {
    if latest_version < 0 || start_version > latest_version {
        return Ok(Vec::new());
    }

    let upper_bound = end_version.map_or(latest_version, |end| end.min(latest_version));
    if upper_bound < start_version {
        return Ok(Vec::new());
    }
    if upper_bound.saturating_sub(start_version) >= MAX_PREVIEW_HISTORY_VERSIONS {
        return Err(UnityCatalogError::BadRequest {
            message: format!(
                "Delta preview history accepts at most {MAX_PREVIEW_HISTORY_VERSIONS} versions per request"
            ),
        });
    }

    let mut commits = Vec::new();
    let mut bytes_read = 0u64;
    for version in start_version..=upper_bound {
        let path = paths
            .delta_log_json(version)
            .map_err(|err| map_internal_error("derive Delta log path", err))?;
        let metadata = storage
            .head_raw(&path)
            .await
            .map_err(|err| map_internal_error("inspect finalized Delta log", err))?
            .filter(|metadata| metadata.size > 0)
            .ok_or_else(|| UnityCatalogError::Conflict {
                message: format!("finalized Delta log unavailable for version {version}"),
            })?;
        bytes_read = bytes_read.saturating_add(metadata.size);
        if bytes_read > MAX_PREVIEW_HISTORY_BYTES {
            return Err(UnityCatalogError::BadRequest {
                message: "Delta preview history exceeds 8 MiB. Request a smaller range."
                    .to_string(),
            });
        }
        let payload = storage
            .get_range(&path, 0..metadata.size)
            .await
            .map_err(|err| map_internal_error("read delta log payload", err))?;
        if payload.len() as u64 != metadata.size {
            return Err(UnityCatalogError::Conflict {
                message: format!("finalized Delta log changed for version {version}"),
            });
        }
        commits.push(parse_commit_info_from_payload(version, &payload));
    }

    Ok(commits)
}

/// Lists finalized, unbackfilled Delta commits for callers with table `SELECT`.
///
/// Select a version range of up to 100 entries and 8 MiB of Delta logs.
///
/// # Errors
///
/// Returns an error if the request is invalid, the table has no preview state,
/// or scoped storage operations fail.
#[utoipa::path(
    get,
    path = "/delta/preview/commits",
    tag = "DeltaCommits",
    request_body = DeltaGetCommitsRequestBody,
    responses(
        (status = 200, description = "Successful response."),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn get_delta_preview_commits(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Json(request): Json<DeltaGetCommitsRequestBody>,
) -> UnityCatalogResult<(StatusCode, Json<Value>)> {
    let DeltaGetCommitsRequestBody {
        table_id,
        table_uri,
        start_version,
        end_version,
    } = request;

    validate_table_uri(&table_uri)?;
    validate_version_range(start_version, end_version)?;

    let table_id = Uuid::parse_str(&table_id).map_err(|_| UnityCatalogError::BadRequest {
        message: "table_id must be a UUID".to_string(),
    })?;

    let storage = ctx.scoped_storage(state.storage.clone())?;
    let target = require_catalog_managed_delta_table(
        &state,
        &ctx,
        &storage,
        table_id,
        &table_uri,
        Privilege::Select,
    )
    .await?;
    let coordinator = target.coordinator(&storage);
    let finalized = preview_finalized_version(&coordinator, &target).await?;
    let backfill_state = load_backfill_state(&storage, table_id).await?;
    if finalized.is_none() && backfill_state.is_none() {
        return Err(UnityCatalogError::NotFound {
            message: format!("table not found: {table_id}"),
        });
    }

    let expected_uri = backfill_state
        .as_ref()
        .and_then(|state| state.table_uri.as_deref());
    ensure_table_uri_matches(table_id, &table_uri, expected_uri)?;
    let latest_table_version = finalized.unwrap_or(-1);

    let backfilled = backfill_state.and_then(|state| state.latest_backfilled_version);

    let start = start_version
        .unwrap_or(0)
        .max(backfilled.map_or(0, |version| version.saturating_add(1)));

    let commits = list_commits(
        &target.log_storage,
        &target.paths,
        start,
        end_version,
        latest_table_version,
    )
    .await?;

    let payload = json!({
        "latest_table_version": latest_table_version,
        "commits": commits
    });

    Ok((StatusCode::OK, Json(payload)))
}

async fn handle_commit_registration(
    storage: &ScopedStorage,
    ctx: &UnityCatalogRequestContext,
    target: &DeltaTarget,
    table_uri: &str,
    commit_info: DeltaCommitInfo,
    metadata: Option<&Value>,
    uniform: Option<&Value>,
) -> UnityCatalogResult<()> {
    let table_id = target.paths.table_id();
    validate_commit_info(&commit_info)?;

    let idempotency_key =
        ctx.idempotency_key
            .clone()
            .ok_or_else(|| UnityCatalogError::BadRequest {
                message: "Idempotency-Key is required".to_string(),
            })?;
    validate_idempotency_key(&idempotency_key)?;

    let coordinator = target.coordinator(storage);
    let finalized = preview_finalized_version(&coordinator, target).await?;
    let backfill_state = load_backfill_state(storage, table_id).await?;
    let expected_uri = backfill_state
        .as_ref()
        .and_then(|state| state.table_uri.as_deref());
    ensure_table_uri_matches(table_id, table_uri, expected_uri)?;
    let latest_table_version = finalized.unwrap_or(-1);
    let latest_backfilled = backfill_state
        .as_ref()
        .and_then(|state| state.latest_backfilled_version)
        .unwrap_or(-1);
    let unbackfilled_count = latest_table_version
        .saturating_sub(latest_backfilled)
        .max(0);
    if unbackfilled_count >= MAX_UNBACKFILLED_COMMITS_PER_TABLE {
        return Err(UnityCatalogError::TooManyRequests {
            message: "maximum number of unbackfilled commits per table has been reached"
                .to_string(),
        });
    }

    let payload = build_commit_payload(table_uri, &commit_info, metadata, uniform)?;
    coordinator
        .ensure_root_authority()
        .await
        .map_err(map_delta_error)?;
    let staged =
        stage_payload_for_idempotency(storage, table_id, &idempotency_key, payload).await?;
    let staged_path = staged.staged_path.clone();
    let staged_version = staged.staged_version.clone();

    let committed = coordinator
        .commit(
            CommitDeltaRequest {
                read_version: commit_info.version - 1,
                staged_path,
                staged_version,
                idempotency_key,
            },
            Utc::now(),
        )
        .await
        .map_err(map_delta_error)?;

    if committed.version != commit_info.version {
        return Err(UnityCatalogError::Conflict {
            message: format!(
                "stale read_version: expected {}, got {}",
                committed.version,
                commit_info.version - 1
            ),
        });
    }

    store_backfill_state(
        storage,
        table_id,
        &DeltaPreviewBackfillState {
            latest_backfilled_version: None,
            table_uri: Some(table_uri.to_string()),
        },
    )
    .await?;

    if let Err(err) = storage.delete(&staged.staged_path).await {
        warn!(
            %table_id,
            staged_path = %staged.staged_path,
            error = %err,
            "failed to delete staged delta payload after successful commit"
        );
    }

    Ok(())
}

async fn handle_backfill_registration(
    storage: &ScopedStorage,
    target: &DeltaTarget,
    table_uri: &str,
    backfilled: i64,
) -> UnityCatalogResult<()> {
    if backfilled < -1 {
        return Err(UnityCatalogError::BadRequest {
            message: "latest_backfilled_version must be >= -1".to_string(),
        });
    }

    let table_id = target.paths.table_id();
    let coordinator = target.coordinator(storage);
    let finalized = preview_finalized_version(&coordinator, target).await?;
    let backfill_state = load_backfill_state(storage, table_id).await?;
    if finalized.is_none() && backfill_state.is_none() {
        return Err(UnityCatalogError::NotFound {
            message: format!("table not found: {table_id}"),
        });
    }

    let expected_uri = backfill_state
        .as_ref()
        .and_then(|state| state.table_uri.as_deref());
    ensure_table_uri_matches(table_id, table_uri, expected_uri)?;
    let latest_table_version = finalized.unwrap_or(-1);
    if backfilled > latest_table_version {
        return Err(UnityCatalogError::BadRequest {
            message: format!(
                "latest_backfilled_version {backfilled} exceeds latest_table_version {latest_table_version}"
            ),
        });
    }

    coordinator
        .ensure_root_authority()
        .await
        .map_err(map_delta_error)?;
    store_backfill_state(
        storage,
        table_id,
        &DeltaPreviewBackfillState {
            latest_backfilled_version: Some(backfilled),
            table_uri: Some(table_uri.to_string()),
        },
    )
    .await?;

    Ok(())
}

/// Commits or backfills changes for callers with table `MODIFY`.
///
/// # Errors
///
/// Returns an error if the request is invalid, preview state cannot be read or
/// written, or the referenced table does not exist.
#[utoipa::path(
    post,
    path = "/delta/preview/commits",
    tag = "DeltaCommits",
    params(
        ("Idempotency-Key" = Option<String>, Header, description = "Idempotency key required when commit_info is provided; must be canonical UUIDv7."),
    ),
    request_body = DeltaCommitRequestBody,
    responses(
        (status = 200, description = "Successful response."),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 409, description = "Conflict.", body = UnityCatalogErrorResponse),
        (status = 429, description = "Too many requests.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse),
        (status = 501, description = "Not implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_delta_preview_commits(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Json(request): Json<DeltaCommitRequestBody>,
) -> UnityCatalogResult<(StatusCode, Json<Value>)> {
    common::reject_table_commit_for_control_v1(&state, &ctx)?;
    let DeltaCommitRequestBody {
        table_id,
        table_uri,
        commit_info,
        latest_backfilled_version,
        metadata,
        uniform,
    } = request;

    validate_table_uri(&table_uri)?;
    let table_id = Uuid::parse_str(&table_id).map_err(|_| UnityCatalogError::BadRequest {
        message: "table_id must be a UUID".to_string(),
    })?;

    let has_commit = commit_info.is_some();
    let has_backfill = latest_backfilled_version.is_some();
    if has_commit == has_backfill {
        return Err(UnityCatalogError::BadRequest {
            message: "request must include either commit_info or latest_backfilled_version"
                .to_string(),
        });
    }

    let storage = ctx.scoped_storage(state.storage.clone())?;
    let target = require_catalog_managed_delta_table(
        &state,
        &ctx,
        &storage,
        table_id,
        &table_uri,
        Privilege::Modify,
    )
    .await?;

    if let Some(commit_info) = commit_info {
        handle_commit_registration(
            &storage,
            &ctx,
            &target,
            &table_uri,
            commit_info,
            metadata.as_ref(),
            uniform.as_ref(),
        )
        .await?;

        return Ok((StatusCode::OK, Json(json!({}))));
    }

    if let Some(backfilled) = latest_backfilled_version {
        handle_backfill_registration(&storage, &target, &table_uri, backfilled).await?;

        return Ok((StatusCode::OK, Json(json!({}))));
    }

    Err(UnityCatalogError::BadRequest {
        message: "request must include either commit_info or latest_backfilled_version".to_string(),
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::sync::Arc;

    use arco_core::ScopedStorage;
    use arco_core::storage::MemoryBackend;
    use uuid::Uuid;

    use super::{DeltaPreviewBackfillState, load_backfill_state, store_backfill_state};

    #[tokio::test]
    async fn stale_backfill_write_does_not_regress_watermark() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant1", "workspace1").expect("scoped storage");
        let table_id = Uuid::now_v7();

        store_backfill_state(
            &storage,
            table_id,
            &DeltaPreviewBackfillState {
                latest_backfilled_version: Some(10),
                table_uri: Some("gs://bucket/path".to_string()),
            },
        )
        .await
        .expect("write high watermark");

        store_backfill_state(
            &storage,
            table_id,
            &DeltaPreviewBackfillState {
                latest_backfilled_version: Some(2),
                table_uri: Some("gs://bucket/path".to_string()),
            },
        )
        .await
        .expect("write stale watermark");

        let state = load_backfill_state(&storage, table_id)
            .await
            .expect("load backfill state")
            .expect("backfill state present");
        assert_eq!(state.latest_backfilled_version, Some(10));
    }
}

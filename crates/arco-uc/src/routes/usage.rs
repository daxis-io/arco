//! Publish and read hourly Logfood materializations through the catalog owner.

use std::collections::{BTreeMap, BTreeSet};

use arco_catalog::authz::privileges::Privilege;
use arco_catalog::credential_vending::{CredentialOperation, DEFAULT_CREDENTIAL_TTL};
use arco_catalog::metastore::publish::ObservedStorageGovernance;
use arco_core::storage::{StorageBackend as _, WritePrecondition, WriteResult};
use arco_core::{ControlPlaneScope, DeltaPaths, ScopedStorage};
use arco_delta::{DeltaCommitCoordinator, DeltaError};
use arco_flow::orchestration::accepted_plan::{ACCEPTED_PLAN_SHA256_LABEL, load_accepted_run_plan};
use arco_flow::orchestration::callbacks::PublicationDescriptor;
use arco_flow::orchestration::compactor::{MicroCompactor, RunState, TaskState};
use arco_flow::orchestration::events::OutputVisibilityState;
use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use axum::extract::{DefaultBodyLimit, Extension, Query, State};
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use bytes::Bytes;
use chrono::Utc;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

use crate::context::UnityCatalogRequestContext as Context;
use crate::error::{UnityCatalogError as Error, UnityCatalogResult as Result};
use crate::event_streams::LOGFOOD_STREAM;
use crate::routes::common::{reject_table_commit_for_control_v1, scoped_storage};
use crate::routes::credentials::{
    authorize_path_access, authorize_table_access, revalidate_observed_watermark,
};
use crate::routes::event_streams::{load_pin, load_stream_root};
use crate::routes::table_reads::{
    delta::DeltaRoot, json_response, load_table, preflight_parquet_footer,
};
use crate::state::UnityCatalogState;

const ASSET: &str = "analytics.surface_usage_hourly";
const BOOTSTRAP_KEY: &str = "00000000-0000-7000-8000-000000000000";
const HOUR_MS: i64 = 3_600_000;
const JS_MAX: i64 = (1_i64 << 53) - 2;
const MAX_LOG: u64 = 64 * 1024;
// shortcut: copy the bounded hour index on publish until Usage needs larger histories.
const MAX_INDEX: u64 = 16 * 1024 * 1024;
const MAX_PARQUET: u64 = 8 * 1024 * 1024;
const MAX_ROWS: usize = 4_096;
const FIELDS: [(&str, &str, DataType, bool); 11] = [
    ("workspace_id", "string", DataType::Utf8, false),
    ("hour_start_ms", "long", DataType::Int64, false),
    ("surface_id", "string", DataType::Utf8, false),
    ("release_id", "string", DataType::Utf8, true),
    ("source_delta_version", "long", DataType::Int64, false),
    ("admitted", "long", DataType::Int64, false),
    ("admission_denied", "long", DataType::Int64, false),
    ("executed", "long", DataType::Int64, false),
    ("execution_failed", "long", DataType::Int64, false),
    ("delivery_authorized", "long", DataType::Int64, false),
    ("delivery_denied", "long", DataType::Int64, false),
];

/// Configured Workspace destinations and approved transforms for hourly usage.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(try_from = "Vec<Binding>", into = "Vec<Binding>")]
pub struct UsageBindings(Vec<Binding>);

/// Catalog table and transform approved for one Workspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    tenant: String,
    workspace: String,
    table_id: Uuid,
    transform_sha256: String,
}

impl From<UsageBindings> for Vec<Binding> {
    fn from(bindings: UsageBindings) -> Self {
        bindings.0
    }
}

impl TryFrom<Vec<Binding>> for UsageBindings {
    type Error = &'static str;

    fn try_from(bindings: Vec<Binding>) -> std::result::Result<Self, Self::Error> {
        let mut scopes = BTreeSet::new();
        for binding in &bindings {
            if ControlPlaneScope::workspace_alias(&binding.tenant, &binding.workspace).is_err()
                || binding.table_id.is_nil()
                || !is_sha256(&binding.transform_sha256)
                || !scopes.insert((&binding.tenant, &binding.workspace))
            {
                return Err("invalid or duplicate Workspace usage binding");
            }
        }
        Ok(Self(bindings))
    }
}

impl UsageBindings {
    fn resolve(&self, tenant: &str, workspace: &str) -> Option<&Binding> {
        self.0
            .iter()
            .find(|b| b.tenant == tenant && b.workspace == workspace)
    }

    /// Returns the configured destination in exactly this scope.
    #[must_use]
    pub fn table_id(&self, tenant: &str, workspace: &str) -> Option<Uuid> {
        self.resolve(tenant, workspace).map(|b| b.table_id)
    }

    /// Returns whether the owner needs to mount the native route.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Confirms each destination has a separate governed query-activity source.
    ///
    /// # Errors
    /// Rejects missing or overlapping table bindings.
    pub fn validate_sources(
        &self,
        events: &crate::EventStreamBindings,
    ) -> std::result::Result<(), &'static str> {
        for binding in &self.0 {
            if events
                .resolve(&binding.tenant, &binding.workspace, LOGFOOD_STREAM)
                .is_none()
                || events
                    .table_ids(&binding.tenant, &binding.workspace)
                    .any(|id| id == binding.table_id)
            {
                return Err("a usage destination requires a distinct query-activity source");
            }
        }
        Ok(())
    }
}

fn ownership_path(table: Uuid) -> String {
    format!("usage/v1/tables/{table}.sha256")
}

/// Checks for a durable owner marker even if a destination's configuration was removed.
///
/// # Errors
/// Fails if the scoped storage cannot be checked.
pub async fn is_usage_table(storage: &ScopedStorage, table: Uuid) -> arco_core::Result<bool> {
    Ok(storage.head_raw(&ownership_path(table)).await?.is_some())
}

/// Native Arco hourly usage publication and read API.
#[derive(OpenApi)]
#[openapi(paths(publish, read, read_receipt), tags((name = "usage", description = "Cataloged hourly Logfood usage")))]
pub struct UsageApiDoc;

/// Mounts the Usage owner endpoints.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route("/hourly", get(read).post(publish))
        .route("/hourly/receipt", get(read_receipt))
        .layer(DefaultBodyLimit::max(2048))
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct UsagePublishRequest {
    run_id: String,
    attempt_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InputPin {
    stream_pin_id: Option<String>,
    input_identity: Option<Source>,
    input_sha256: Option<String>,
    hour_start_ms: i64,
    transform_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    tenant: String,
    workspace: String,
    table_id: Uuid,
    delta_version: i64,
    stream_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct UsageReceipt {
    table_id: Uuid,
    table_version: i64,
    source_table_id: Uuid,
    source_delta_version: i64,
    hour_start_ms: i64,
    run_id: String,
    attempt_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    effect: Uuid,
    receipt: UsageReceipt,
    artifact_sha256: String,
    byte_size: u64,
    row_count: usize,
}

impl Entry {
    fn file(&self) -> String {
        format!("part-{}-{}.parquet", self.effect, self.artifact_sha256)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Selected {
    entry: Entry,
    index_sha256: String,
}

type Index = BTreeMap<i64, Entry>;

#[derive(Serialize, ToSchema)]
struct UsageRow {
    surface_id: String,
    release_id: Option<String>,
    admitted: i64,
    admission_denied: i64,
    executed: i64,
    execution_failed: i64,
    delivery_authorized: i64,
    delivery_denied: i64,
}

#[derive(Serialize, ToSchema)]
struct UsagePage {
    workspace_id: String,
    published_version: i64,
    receipt: Option<UsageReceipt>,
    rows: Vec<UsageRow>,
    next_offset: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UsagePageQuery {
    hour_start_ms: Option<i64>,
    published_version: Option<i64>,
    offset: Option<usize>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UsageReceiptQuery {
    hour_start_ms: i64,
    source_delta_version: i64,
}

async fn authorize<'a>(
    state: &'a UnityCatalogState,
    ctx: &Context,
    privilege: Privilege,
    operation: CredentialOperation,
) -> Result<(&'a Binding, DeltaRoot, ObservedStorageGovernance)> {
    let binding = state
        .config
        .usage_bindings
        .resolve(&ctx.tenant, &ctx.workspace)
        .ok_or_else(|| Error::Forbidden {
            message: "usage_workspace_not_bound".into(),
        })?;
    reject_table_commit_for_control_v1(state, ctx)?;
    let access =
        authorize_table_access(state, ctx, &binding.table_id.to_string(), privilege).await?;
    let root = usage_root(state, ctx, binding).await?;
    let path = authorize_path_access(
        state,
        ctx,
        &root.uri,
        operation,
        Some(access.target),
        Some(access.observed),
        DEFAULT_CREDENTIAL_TTL,
    )
    .await?;
    Ok((binding, root, path.observed))
}

async fn usage_root(
    state: &UnityCatalogState,
    ctx: &Context,
    binding: &Binding,
) -> Result<DeltaRoot> {
    let (table, cols) = load_table(state, ctx, &binding.table_id.to_string()).await?;
    if table.name != "surface_usage_hourly"
        || cols.len() != FIELDS.len()
        || !cols
            .iter()
            .zip(&FIELDS)
            .all(|(col, (name, kind, _, nullable))| {
                col.name == *name
                    && col.data_type.eq_ignore_ascii_case(kind)
                    && col.is_nullable == *nullable
            })
    {
        return Err(conflict("usage_table_schema_mismatch"));
    }
    DeltaRoot::resolve(state, ctx, &table)
}

#[utoipa::path(
    post, path = "/api/v1/usage/hourly", tag = "usage", request_body = UsagePublishRequest,
    responses(
        (status = 200, description = "Finalized Delta replacement receipt", body = UsageReceipt),
        (status = 400, description = "Invalid publication request"),
        (status = 401, description = "JWT authentication required"),
        (status = 403, description = "Missing Workspace binding or table MODIFY grant"),
        (status = 404, description = "Flow run, task, or stream pin unavailable in the Workspace"),
        (status = 409, description = "Attempt, artifact, binding, source version, or destination head conflicts"),
        (status = 413, description = "Materialization exceeds the supported size"),
        (status = 503, description = "Usage owner unavailable"),
    ), security(("bearerAuth" = []))
)]
async fn publish(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<Context>,
    request: std::result::Result<
        Json<UsagePublishRequest>,
        axum::extract::rejection::JsonRejection,
    >,
) -> Result<Response> {
    let Json(request) = request.map_err(|_| bad_request("usage_publish_invalid"))?;
    if !valid_id(&request.run_id) || !valid_id(&request.attempt_id) {
        return Err(bad_request("usage_publish_invalid"));
    }
    let (binding, root, observed) =
        authorize(&state, &ctx, Privilege::Modify, CredentialOperation::Write).await?;
    let storage = scoped_storage(&state, &ctx)?;
    let (pin, publication) = verified_run(&storage, &request).await?;
    let (source_table_id, source_delta_version) =
        verify_input(&state, &ctx, &storage, binding, &pin).await?;
    let checksum = &publication.checksum_sha256;
    let effect = effect_id(
        binding.table_id,
        source_table_id,
        pin.hour_start_ms,
        source_delta_version,
    );
    let effect_key = effect.to_string();
    let coordinator = DeltaCommitCoordinator::with_paths(storage.clone(), root.paths.clone());
    revalidate_observed_watermark(&state, &ctx, &observed).await?;
    ensure_destination(
        &storage,
        &root.paths,
        &coordinator,
        bootstrap_log(binding, source_table_id)?,
    )
    .await?;
    if let Some((version, log)) = coordinator
        .recover_owner_replacement(&effect_key)
        .await
        .map_err(|error| delta_error(&error))?
    {
        let selected = selected_log(&log)?;
        if selected.entry.effect != effect
            || selected.entry.receipt.table_version != version
            || selected.entry.artifact_sha256 != *checksum
        {
            return Err(conflict("usage_effect_conflict"));
        }
        revalidate_observed_watermark(&state, &ctx, &observed).await?;
        return json_response(&selected.entry.receipt);
    }
    let head = coordinator
        .finalized_version()
        .await
        .map_err(|error| delta_error(&error))?;
    let index = read_index(&storage, &root.paths, &coordinator, head).await?;
    let previous = index.get(&pin.hour_start_ms);
    if let Some(entry) = previous.filter(|entry| entry.effect == effect) {
        if entry.artifact_sha256 != *checksum {
            return Err(conflict("usage_effect_conflict"));
        }
        revalidate_observed_watermark(&state, &ctx, &observed).await?;
        return json_response(&entry.receipt);
    }
    if previous.is_some_and(|entry| entry.receipt.source_delta_version >= source_delta_version) {
        return Err(conflict("usage_source_version_stale"));
    }
    let artifact = read_verified_artifact(&storage, &publication).await?;
    let rows = decode_rows(
        &artifact,
        &ctx.workspace,
        pin.hour_start_ms,
        source_delta_version,
    )?;
    let receipt = UsageReceipt {
        table_id: binding.table_id,
        table_version: head
            .checked_add(1)
            .filter(|v| valid_version(*v))
            .ok_or_else(unavailable)?,
        source_table_id,
        source_delta_version,
        hour_start_ms: pin.hour_start_ms,
        run_id: request.run_id,
        attempt_id: request.attempt_id,
    };
    let entry = Entry {
        effect,
        receipt: receipt.clone(),
        artifact_sha256: publication.checksum_sha256,
        byte_size: artifact.len() as u64,
        row_count: rows.len(),
    };
    let log = stage_replacement(&storage, &root.paths, index, entry, artifact).await?;
    revalidate_observed_watermark(&state, &ctx, &observed).await?;
    let committed = coordinator
        .commit_owner_replacement(&effect_key, head, log, Utc::now())
        .await
        .map_err(|error| delta_error(&error))?;
    if committed.version != receipt.table_version {
        return Err(unavailable());
    }
    json_response(&receipt)
}

async fn stage_replacement(
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    mut index: Index,
    entry: Entry,
    artifact: Bytes,
) -> Result<Bytes> {
    let previous = index.insert(entry.receipt.hour_start_ms, entry.clone());
    let index_bytes = json_bytes(&index)?;
    if index_bytes.len() as u64 > MAX_INDEX {
        return Err(Error::PayloadTooLarge {
            message: "usage_index_too_large".into(),
        });
    }
    let index_sha256 = sha256(&index_bytes);
    write_once(
        storage,
        &format!("{}/{}", paths.table_root(), entry.file()),
        artifact,
    )
    .await?;
    write_once(
        storage,
        &index_path(paths.table_id(), &index_sha256),
        index_bytes,
    )
    .await?;
    replacement_log(
        &Selected {
            entry,
            index_sha256,
        },
        previous.as_ref().map(Entry::file).as_deref(),
    )
}

async fn verify_input(
    state: &UnityCatalogState,
    ctx: &Context,
    storage: &ScopedStorage,
    binding: &Binding,
    pin: &InputPin,
) -> Result<(Uuid, i64)> {
    let source = state
        .config
        .event_streams
        .resolve(&ctx.tenant, &ctx.workspace, LOGFOOD_STREAM)
        .ok_or_else(|| conflict("usage_source_not_bound"))?;
    if !valid_hour(pin.hour_start_ms) || pin.transform_sha256 != binding.transform_sha256 {
        return Err(conflict("usage_accepted_input_mismatch"));
    }
    let source_root = load_stream_root(state, ctx, source).await?;
    let version = match (
        pin.stream_pin_id.as_deref(),
        pin.input_identity.as_ref(),
        pin.input_sha256.as_deref(),
    ) {
        (Some(id), None, None) => load_pin(storage, source, &source_root, id).await?,
        (None, Some(identity), Some(digest))
            if identity.table_id == source.table_id
                && identity.tenant == ctx.tenant
                && identity.workspace == ctx.workspace
                && identity.stream_id == LOGFOOD_STREAM
                && is_sha256(digest) =>
        {
            identity.delta_version
        }
        _ => return Err(conflict("usage_accepted_input_mismatch")),
    };
    if !valid_version(version) {
        return Err(conflict("usage_accepted_input_mismatch"));
    }
    let source_head = DeltaCommitCoordinator::with_paths(storage.clone(), source_root.paths)
        .finalized_version()
        .await
        .map_err(|error| delta_error(&error))?;
    if version > source_head.max(0) {
        return Err(conflict("usage_source_version_unpublished"));
    }
    Ok((source.table_id, version))
}

async fn ensure_destination(
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    coordinator: &DeltaCommitCoordinator,
    bootstrap: Bytes,
) -> Result<()> {
    let initial_head = coordinator
        .finalized_version()
        .await
        .map_err(|error| delta_error(&error))?;
    if initial_head >= 0 {
        require_bootstrap(storage, paths, &bootstrap).await?;
    }
    let fingerprint = sha256(&bootstrap);
    // Preserve the binding across removal or accidental reassignment of configuration.
    write_once(
        storage,
        "usage/v1/destination.sha256",
        Bytes::from(fingerprint.clone()),
    )
    .await?;
    write_once(
        storage,
        &ownership_path(paths.table_id()),
        Bytes::from(fingerprint),
    )
    .await?;
    if initial_head < 0 {
        let response = coordinator
            .commit_owner_append(BOOTSTRAP_KEY, bootstrap, Utc::now())
            .await
            .map_err(|error| delta_error(&error))?;
        if response.version != 0 {
            return Err(unavailable());
        }
    }
    Ok(())
}

async fn verified_run(
    storage: &ScopedStorage,
    request: &UsagePublishRequest,
) -> Result<(InputPin, PublicationDescriptor)> {
    let (manifest, fold) = MicroCompactor::new(storage.clone())
        .load_state()
        .await
        .map_err(|_| unavailable())?;
    if manifest.watermarks.last_visible_event_id.is_none()
        || manifest.watermarks.last_visible_event_id != manifest.watermarks.last_committed_event_id
    {
        return Err(conflict("usage_flow_projection_unpublished"));
    }
    let run = fold
        .runs
        .get(&request.run_id)
        .ok_or_else(|| not_found("usage_flow_run_not_found"))?;
    let task = fold
        .tasks
        .get(&(request.run_id.clone(), ASSET.to_string()))
        .ok_or_else(|| not_found("usage_flow_task_not_found"))?;
    if run.state != RunState::Succeeded
        || task.state != TaskState::Succeeded
        || task.attempt_id.as_deref() != Some(&request.attempt_id)
        || task.output_visibility_state != Some(OutputVisibilityState::Visible)
    {
        return Err(conflict("usage_flow_attempt_not_verified"));
    }
    let artifact = task
        .publication
        .as_ref()
        .ok_or_else(|| conflict("usage_flow_attempt_not_verified"))?;
    let owner = artifact
        .owner_evidence
        .as_ref()
        .ok_or_else(|| conflict("usage_flow_attempt_not_verified"))?;
    let digest = run
        .labels
        .get(ACCEPTED_PLAN_SHA256_LABEL)
        .ok_or_else(|| conflict("usage_accepted_plan_missing"))?;
    let plan = load_accepted_run_plan(storage, &request.run_id, &run.plan_id, digest)
        .await
        .map_err(|_| conflict("usage_accepted_plan_missing"))?;
    let input = plan
        .task_payloads
        .get(ASSET)
        .and_then(|asset| asset.pointer("/asset/execution/payload"))
        .ok_or_else(|| conflict("usage_accepted_input_mismatch"))?;
    let pin = serde_json::from_value(input.clone())
        .map_err(|_| conflict("usage_accepted_input_mismatch"))?;
    if owner.object_version != artifact.object_version
        || artifact.format != "parquet"
        || artifact.byte_size == 0
        || artifact.byte_size > MAX_PARQUET
        || !is_sha256(&artifact.checksum_sha256)
    {
        return Err(conflict("usage_flow_artifact_mismatch"));
    }
    Ok((pin, artifact.clone()))
}

async fn read_verified_artifact(
    storage: &ScopedStorage,
    artifact: &PublicationDescriptor,
) -> Result<Bytes> {
    let before = storage
        .head_raw(&artifact.object_path)
        .await
        .map_err(|_| unavailable())?
        .filter(|meta| meta.version == artifact.object_version && meta.size == artifact.byte_size)
        .ok_or_else(|| conflict("usage_flow_artifact_mismatch"))?;
    let bytes = bounded(storage, &artifact.object_path, MAX_PARQUET).await?;
    let after = storage
        .head_raw(&artifact.object_path)
        .await
        .map_err(|_| unavailable())?;
    if after.is_none_or(|meta| meta.version != before.version || meta.size != before.size)
        || sha256(&bytes) != artifact.checksum_sha256
    {
        return Err(conflict("usage_flow_artifact_mismatch"));
    }
    Ok(bytes)
}

#[utoipa::path(
    get, path = "/api/v1/usage/hourly", tag = "usage",
    params(
        ("hour_start_ms" = Option<i64>, Query, description = "UTC hour start in milliseconds"),
        ("published_version" = Option<i64>, Query, description = "Exact Usage table version"),
        ("offset" = Option<usize>, Query, description = "Continuation offset in the pinned hour and version"),
        ("limit" = Option<usize>, Query, description = "Page size from 1 to 50, default 25"),
    ),
    responses(
        (status = 200, description = "One hour from the selected Delta version", body = UsagePage),
        (status = 400, description = "Invalid hour, page, or version"),
        (status = 401, description = "JWT authentication required"),
        (status = 403, description = "Missing Workspace binding or table SELECT grant"),
        (status = 409, description = "Committed output identity mismatch"),
        (status = 503, description = "Usage owner unavailable"),
    ), security(("bearerAuth" = []))
)]
async fn read(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<Context>,
    query: std::result::Result<Query<UsagePageQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Response> {
    let Query(query) = query.map_err(|_| bad_request("usage_page_invalid"))?;
    let limit = query.limit.unwrap_or(25);
    let offset = query.offset.unwrap_or(0);
    if !(1..=50).contains(&limit)
        || offset > MAX_ROWS
        || (offset > 0 && (query.hour_start_ms.is_none() || query.published_version.is_none()))
        || query.hour_start_ms.is_some_and(|h| !valid_hour(h))
        || query.published_version.is_some_and(|v| !valid_version(v))
    {
        return Err(bad_request("usage_page_invalid"));
    }
    let (binding, root, observed) =
        authorize(&state, &ctx, Privilege::Select, CredentialOperation::Read).await?;
    let storage = scoped_storage(&state, &ctx)?;
    let coordinator = DeltaCommitCoordinator::with_paths(storage.clone(), root.paths.clone());
    let latest = coordinator
        .finalized_version()
        .await
        .map_err(|error| delta_error(&error))?;
    let available_version = latest.max(0);
    let version = query.published_version.unwrap_or(available_version);
    if version > available_version {
        return Err(bad_request("usage_page_invalid"));
    }
    let source = state
        .config
        .event_streams
        .resolve(&ctx.tenant, &ctx.workspace, LOGFOOD_STREAM)
        .ok_or_else(|| conflict("usage_source_not_bound"))?;
    if latest >= 0 {
        require_bootstrap(
            &storage,
            &root.paths,
            &bootstrap_log(binding, source.table_id)?,
        )
        .await?;
    }
    let index = read_index(&storage, &root.paths, &coordinator, version).await?;
    let selected = query.hour_start_ms.map_or_else(
        || index.last_key_value().map(|(_, entry)| entry),
        |hour| index.get(&hour),
    );
    let mut page = UsagePage {
        workspace_id: ctx.workspace.clone(),
        published_version: version,
        receipt: None,
        rows: Vec::new(),
        next_offset: None,
    };
    if let Some(entry) = selected {
        let path = format!("{}/{}", root.paths.table_root(), entry.file());
        let bytes = bounded(&storage, &path, MAX_PARQUET).await?;
        if sha256(&bytes) != entry.artifact_sha256 || bytes.len() as u64 != entry.byte_size {
            return Err(conflict("usage_committed_artifact_mismatch"));
        }
        let rows = decode_rows(
            &bytes,
            &ctx.workspace,
            entry.receipt.hour_start_ms,
            entry.receipt.source_delta_version,
        )?;
        if rows.len() != entry.row_count {
            return Err(conflict("usage_committed_artifact_mismatch"));
        }
        if offset > rows.len() {
            return Err(bad_request("usage_page_invalid"));
        }
        page.next_offset = (offset + limit < rows.len()).then_some(offset + limit);
        page.rows = rows.into_iter().skip(offset).take(limit).collect();
        page.receipt = Some(entry.receipt.clone());
    }
    usage_root(&state, &ctx, binding).await?;
    revalidate_observed_watermark(&state, &ctx, &observed).await?;
    json_response(&page)
}

#[utoipa::path(
    get, path = "/api/v1/usage/hourly/receipt", tag = "usage",
    params(
        ("hour_start_ms" = i64, Query, description = "UTC hour start in milliseconds"),
        ("source_delta_version" = i64, Query, description = "Activity table version of the original publication"),
    ),
    responses(
        (status = 200, description = "Original finalized receipt for the hour and source version", body = UsageReceipt),
        (status = 400, description = "Invalid hour or source version"),
        (status = 401, description = "JWT authentication required"),
        (status = 403, description = "Missing Workspace binding or table SELECT grant"),
        (status = 404, description = "The requested replacement has not finalized"),
        (status = 503, description = "Usage owner unavailable"),
    ), security(("bearerAuth" = []))
)]
async fn read_receipt(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<Context>,
    query: std::result::Result<Query<UsageReceiptQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Response> {
    let Query(query) = query.map_err(|_| bad_request("usage_receipt_invalid"))?;
    if !valid_hour(query.hour_start_ms) || !valid_version(query.source_delta_version) {
        return Err(bad_request("usage_receipt_invalid"));
    }
    let (binding, root, observed) =
        authorize(&state, &ctx, Privilege::Select, CredentialOperation::Read).await?;
    let storage = scoped_storage(&state, &ctx)?;
    let source = state
        .config
        .event_streams
        .resolve(&ctx.tenant, &ctx.workspace, LOGFOOD_STREAM)
        .ok_or_else(|| conflict("usage_source_not_bound"))?;
    let coordinator = DeltaCommitCoordinator::with_paths(storage.clone(), root.paths.clone());
    let head = coordinator
        .finalized_version()
        .await
        .map_err(|error| delta_error(&error))?;
    if head <= 0 {
        return Err(not_found("usage_receipt_not_found"));
    }
    require_bootstrap(
        &storage,
        &root.paths,
        &bootstrap_log(binding, source.table_id)?,
    )
    .await?;
    let effect = effect_id(
        binding.table_id,
        source.table_id,
        query.hour_start_ms,
        query.source_delta_version,
    );
    let Some((version, log)) = coordinator
        .read_owner_replacement(&effect.to_string())
        .await
        .map_err(|error| delta_error(&error))?
    else {
        return Err(not_found("usage_receipt_not_found"));
    };
    let selected = selected_log(&log)?;
    if selected.entry.effect != effect || selected.entry.receipt.table_version != version {
        return Err(conflict("usage_receipt_identity_mismatch"));
    }
    revalidate_observed_watermark(&state, &ctx, &observed).await?;
    json_response(&selected.entry.receipt)
}

fn usage_parquet(bytes: &Bytes) -> Result<ParquetRecordBatchReader> {
    preflight_parquet_footer(bytes).map_err(|_| conflict("usage_artifact_schema_invalid"))?;
    let parquet = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .map_err(|_| conflict("usage_artifact_schema_invalid"))?;
    let fields = parquet.schema().fields();
    if fields.len() != FIELDS.len()
        || !fields
            .iter()
            .zip(&FIELDS)
            .all(|(field, (name, _, kind, nullable))| {
                field.name() == name
                    && field.data_type() == kind
                    && field.is_nullable() == *nullable
            })
    {
        return Err(conflict("usage_artifact_schema_invalid"));
    }
    if !usize::try_from(parquet.metadata().file_metadata().num_rows())
        .is_ok_and(|count| count <= MAX_ROWS)
    {
        return Err(Error::PayloadTooLarge {
            message: "usage_artifact_too_many_rows".into(),
        });
    }
    let uncompressed = parquet
        .metadata()
        .row_groups()
        .iter()
        .try_fold(0u64, |total, group| {
            total.checked_add(u64::try_from(group.total_byte_size()).ok()?)
        });
    if uncompressed.is_none_or(|bytes| bytes > 4 * MAX_PARQUET) {
        return Err(Error::PayloadTooLarge {
            message: "usage_artifact_uncompressed_size".into(),
        });
    }
    parquet
        .build()
        .map_err(|_| conflict("usage_artifact_schema_invalid"))
}

fn decode_rows(bytes: &Bytes, workspace: &str, hour: i64, version: i64) -> Result<Vec<UsageRow>> {
    let parquet = usage_parquet(bytes)?;
    let mut seen = BTreeSet::new();
    let mut rows = Vec::new();
    for batch in parquet {
        let batch = batch.map_err(|_| conflict("usage_artifact_schema_invalid"))?;
        let text = |col| {
            batch
                .column(col)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| conflict("usage_artifact_schema_invalid"))
        };
        let number = |col| {
            batch
                .column(col)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| conflict("usage_artifact_schema_invalid"))
        };
        let (workspaces, surfaces, releases) = (text(0)?, text(2)?, text(3)?);
        let (hours, versions) = (number(1)?, number(4)?);
        let counts = (5..11).map(number).collect::<Result<Vec<_>>>()?;
        let [
            admitted,
            admission_denied,
            executed,
            execution_failed,
            delivery_authorized,
            delivery_denied,
        ] = counts.as_slice()
        else {
            return Err(conflict("usage_artifact_schema_invalid"));
        };
        for row in 0..batch.num_rows() {
            if workspaces.is_null(row)
                || hours.is_null(row)
                || surfaces.is_null(row)
                || versions.is_null(row)
                || workspaces.value(row) != workspace
                || hours.value(row) != hour
                || versions.value(row) != version
                || !valid_id(surfaces.value(row))
                || counts
                    .iter()
                    .any(|c| c.is_null(row) || !valid_version(c.value(row)))
            {
                return Err(conflict("usage_artifact_rows_invalid"));
            }
            let release = (!releases.is_null(row)).then(|| releases.value(row));
            if release.is_some_and(|id| !valid_id(id))
                || !seen.insert((surfaces.value(row).to_string(), release.map(str::to_string)))
            {
                return Err(conflict("usage_artifact_rows_invalid"));
            }
            rows.push(UsageRow {
                surface_id: surfaces.value(row).to_string(),
                release_id: release.map(str::to_string),
                admitted: admitted.value(row),
                admission_denied: admission_denied.value(row),
                executed: executed.value(row),
                execution_failed: execution_failed.value(row),
                delivery_authorized: delivery_authorized.value(row),
                delivery_denied: delivery_denied.value(row),
            });
        }
    }
    if rows.len() > MAX_ROWS {
        return Err(Error::PayloadTooLarge {
            message: "usage_artifact_too_many_rows".into(),
        });
    }
    rows.sort_by(|a, b| (&a.surface_id, &a.release_id).cmp(&(&b.surface_id, &b.release_id)));
    Ok(rows)
}

fn bootstrap_log(binding: &Binding, source: Uuid) -> Result<Bytes> {
    let schema = json!({"type":"struct","fields":FIELDS.map(|(name,kind,_,nullable)|
        json!({"name":name,"type":kind,"nullable":nullable,"metadata":{}}))});
    let protocol = json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}});
    let metadata = json!({"metaData":{"id":binding.table_id,"name":"surface_usage_hourly",
        "format":{"provider":"parquet","options":{}},"schemaString":schema.to_string(),
        "partitionColumns":[],"configuration":{}}});
    let binding_key = sha256(&json_bytes(&(source, binding))?);
    let owner = json!({"commitInfo":{"userMetadata":binding_key}});
    Ok(Bytes::from(format!("{protocol}\n{metadata}\n{owner}\n")))
}

async fn require_bootstrap(
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    expected: &Bytes,
) -> Result<()> {
    if bounded(
        storage,
        &paths.delta_log_json(0).map_err(|_| unavailable())?,
        MAX_LOG,
    )
    .await?
        != expected
    {
        return Err(conflict("usage_binding_mismatch"));
    }
    Ok(())
}

fn replacement_log(selected: &Selected, previous_file: Option<&str>) -> Result<Bytes> {
    let entry = &selected.entry;
    let info = json!({"commitInfo":{"operation":"WRITE","operationParameters":{"mode":"Overwrite"},
        "userMetadata":serde_json::to_string(selected).map_err(|_| unavailable())?}});
    let add = json!({"add":{"path":entry.file(),"partitionValues":{},"size":entry.byte_size,
        "modificationTime":entry.receipt.hour_start_ms,"dataChange":true,
        "tags":{"arco.content.sha256":entry.artifact_sha256}}});
    let remove = previous_file.map(|path| json!({"remove":{"path":path,"dataChange":true}}));
    let mut log = Vec::new();
    for action in [Some(info), remove, Some(add)].into_iter().flatten() {
        serde_json::to_writer(&mut log, &action).map_err(|_| unavailable())?;
        log.push(b'\n');
    }
    Ok(Bytes::from(log))
}

fn selected_log(log: &[u8]) -> Result<Selected> {
    let first = log
        .split(|byte| *byte == b'\n')
        .next()
        .ok_or_else(unavailable)?;
    let action: Value = serde_json::from_slice(first).map_err(|_| unavailable())?;
    let metadata = action
        .pointer("/commitInfo/userMetadata")
        .and_then(Value::as_str)
        .ok_or_else(unavailable)?;
    serde_json::from_str(metadata).map_err(|_| unavailable())
}

async fn read_index(
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    coordinator: &DeltaCommitCoordinator,
    version: i64,
) -> Result<Index> {
    if version <= 0 {
        return Ok(Index::new());
    }
    let log = bounded(
        storage,
        &paths.delta_log_json(version).map_err(|_| unavailable())?,
        MAX_LOG,
    )
    .await?;
    let head = selected_log(&log)?;
    if head.entry.receipt.table_version != version
        || head.entry.receipt.table_id != paths.table_id()
        || !is_sha256(&head.index_sha256)
    {
        return Err(conflict("usage_index_identity_mismatch"));
    }
    coordinator
        .verify_owner_replacement(&head.entry.effect.to_string(), version, &log)
        .await
        .map_err(|error| delta_error(&error))?;
    let index = bounded(
        storage,
        &index_path(paths.table_id(), &head.index_sha256),
        MAX_INDEX,
    )
    .await?;
    if sha256(&index) != head.index_sha256 {
        return Err(conflict("usage_index_identity_mismatch"));
    }
    serde_json::from_slice(&index).map_err(|_| unavailable())
}

fn index_path(table: Uuid, digest: &str) -> String {
    format!("usage/v1/indexes/{table}/{digest}.json")
}

fn effect_id(table: Uuid, source: Uuid, hour: i64, version: i64) -> Uuid {
    let digest: [u8; 32] =
        Sha256::digest(format!("arco.usage.v1:{table}:{source}:{hour}:{version}")).into();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    let hour_bytes = hour.to_be_bytes();
    bytes[..6].copy_from_slice(&hour_bytes[2..]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

async fn bounded(storage: &ScopedStorage, path: &str, max: u64) -> Result<Bytes> {
    let meta = storage
        .head_raw(path)
        .await
        .map_err(|_| unavailable())?
        .ok_or_else(unavailable)?;
    if meta.size == 0 || meta.size > max {
        return Err(unavailable());
    }
    let bytes = storage
        .get_range(path, 0..meta.size)
        .await
        .map_err(|_| unavailable())?;
    if bytes.len() as u64 != meta.size {
        return Err(unavailable());
    }
    Ok(bytes)
}

async fn write_once(storage: &ScopedStorage, path: &str, bytes: Bytes) -> Result<()> {
    let length = bytes.len() as u64;
    let result = storage
        .put_raw(path, bytes.clone(), WritePrecondition::DoesNotExist)
        .await
        .map_err(|_| unavailable())?;
    if matches!(result, WriteResult::PreconditionFailed { .. })
        && bounded(storage, path, length).await? != bytes
    {
        return Err(conflict("usage_immutable_object_conflict"));
    }
    Ok(())
}

fn json_bytes(value: &impl Serialize) -> Result<Bytes> {
    Ok(Bytes::from(
        serde_json::to_vec(value).map_err(|_| unavailable())?,
    ))
}
fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn valid_id(value: &str) -> bool {
    value
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}
fn valid_version(value: i64) -> bool {
    (0..=JS_MAX).contains(&value)
}
fn valid_hour(value: i64) -> bool {
    (0..=281_474_976_710_655).contains(&value) && value % HOUR_MS == 0
}
fn bad_request(message: &str) -> Error {
    Error::BadRequest {
        message: message.into(),
    }
}
fn conflict(message: &str) -> Error {
    Error::Conflict {
        message: message.into(),
    }
}
fn not_found(message: &str) -> Error {
    Error::NotFound {
        message: message.into(),
    }
}
fn unavailable() -> Error {
    Error::ServiceUnavailable {
        message: "usage_owner_unavailable".into(),
    }
}
fn delta_error(error: &DeltaError) -> Error {
    match error {
        DeltaError::Conflict { .. } => conflict("usage_delta_conflict"),
        _ => unavailable(),
    }
}

//! Workspace-bound, typed events on Arco-managed Delta tables.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arco_catalog::authz::privileges::Privilege;
use arco_catalog::credential_vending::{CredentialOperation, DEFAULT_CREDENTIAL_TTL};
use arco_catalog::metastore::publish::ObservedStorageGovernance;
use arco_core::canonical_json::to_canonical_bytes;
use arco_core::storage::{StorageBackend as _, WritePrecondition, WriteResult};
use arco_core::{DeltaPaths, ScopedStorage};
use arco_delta::{DeltaCommitCoordinator, DeltaError};
use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use utoipa::{OpenApi, ToSchema};
use uuid::Uuid;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogResult};
use crate::event_streams::{
    EventFieldType, EventSchema, LOGFOOD_STREAM, StreamBinding, event_stream_table_path,
};
use crate::routes::common::{reject_table_commit_for_control_v1, scoped_storage};
use crate::routes::credentials::{
    authorize_path_access, authorize_table_access, revalidate_observed_watermark,
};
use crate::routes::table_reads::{delta::DeltaRoot, json_response, load_table};
use crate::state::UnityCatalogState;

mod pins;
pub(super) use pins::load_pin;

const MAX_BODY_BYTES: usize = 8 * 1024;
const MAX_OBJECT_BYTES: u64 = 64 * 1024;
const MAX_PAGE_SIZE: i64 = 50;
const BOOTSTRAP_KEY: &str = "00000000-0000-7000-8000-000000000000";
const EVENT_TAG: &str = "arco.activity.event.v1";
const EVENT_DIGEST: &str = "arco.activity.event.sha256";

const EVENT_COLUMNS: [(&str, &str, DataType, bool); 4] = [
    ("workspace_id", "string", DataType::Utf8, false),
    ("schema_version", "integer", DataType::Int32, false),
    ("source_event_id", "string", DataType::Utf8, false),
    ("occurred_at_ms", "long", DataType::Int64, false),
];

/// Native Arco event-stream API.
#[derive(OpenApi)]
#[openapi(
    paths(stream_append, stream_read, pins::create),
    tags((name = "event-streams", description = "Workspace-bound typed event streams"))
)]
pub struct EventStreamApiDoc;

/// Creates the native event stream routes.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route("/:stream_id/events", get(stream_read).post(stream_append))
        .route("/:stream_id/pins", post(pins::create))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
}

/// Preserves the Logfood adapter's original endpoints.
pub fn activity_routes() -> Router<UnityCatalogState> {
    Router::new()
        .route(
            "/extensions/activity-events",
            get(activity_read).post(activity_append),
        )
        .layer(DefaultBodyLimit::max(2048))
}

#[derive(Serialize, Deserialize)]
struct Event {
    schema_version: i32,
    source_event_id: Uuid,
    occurred_at_ms: i64,
    #[serde(flatten)]
    fields: EventFields,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StreamEvent {
    schema_version: i32,
    #[schema(value_type = String, format = "uuid")]
    source_event_id: Uuid,
    occurred_at_ms: i64,
    /// Fields defined and validated by the bound stream schema.
    #[schema(value_type = BTreeMap<String, Value>)]
    fields: EventFields,
}

impl From<StreamEvent> for Event {
    fn from(event: StreamEvent) -> Self {
        Self {
            schema_version: event.schema_version,
            source_event_id: event.source_event_id,
            occurred_at_ms: event.occurred_at_ms,
            fields: event.fields,
        }
    }
}

impl From<Event> for StreamEvent {
    fn from(event: Event) -> Self {
        Self {
            schema_version: event.schema_version,
            source_event_id: event.source_event_id,
            occurred_at_ms: event.occurred_at_ms,
            fields: event.fields,
        }
    }
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct StreamReceipt {
    #[schema(value_type = String, format = "uuid")]
    source_event_id: Uuid,
    commit_version: i64,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct StreamRow {
    workspace_id: String,
    commit_version: i64,
    event: StreamEvent,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct StreamPage {
    events: Vec<StreamRow>,
    published_version: i64,
    next_before_version: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StreamPageQuery {
    limit: Option<i64>,
    before_version: Option<i64>,
    published_version: Option<i64>,
    pin_id: Option<String>,
}

#[derive(Serialize, ToSchema)]
struct StreamErrorBody {
    code: String,
    message: String,
}

struct StreamError(UnityCatalogError);

impl From<UnityCatalogError> for StreamError {
    fn from(error: UnityCatalogError) -> Self {
        Self(error)
    }
}

impl IntoResponse for StreamError {
    fn into_response(self) -> Response {
        let (status, payload) = self.0.to_status_and_payload();
        let error = StreamErrorBody {
            code: payload.error.error_code,
            message: payload.error.message,
        };
        self.0
            .with_retry_after((status, Json(error)).into_response())
    }
}

#[derive(Serialize)]
#[serde(transparent)]
struct EventFields(BTreeMap<String, Value>);

impl<'de> Deserialize<'de> for EventFields {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FieldsVisitor;

        impl<'de> Visitor<'de> for FieldsVisitor {
            type Value = EventFields;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an event with unique fields")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut fields = BTreeMap::new();
                while let Some((name, value)) = map.next_entry()? {
                    if fields.insert(name, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate event field"));
                    }
                }
                Ok(EventFields(fields))
            }
        }

        deserializer.deserialize_map(FieldsVisitor)
    }
}

impl Event {
    fn admit(&mut self, schema: &EventSchema) -> bool {
        if self.schema_version != schema.version()
            || self.source_event_id.get_version_num() != 7
            || self.source_event_id.to_string() == BOOTSTRAP_KEY
            || self.occurred_at_ms <= 0
            || DateTime::from_timestamp_millis(self.occurred_at_ms).is_none()
        {
            return false;
        }
        let fields = &mut self.fields.0;
        for field in schema.fields() {
            if !field.admits(fields.entry(field.name.clone()).or_insert(Value::Null)) {
                return false;
            }
        }
        if fields.len() != schema.fields().len() {
            return false;
        }
        !schema.is_logfood()
            || matches!(
                (
                    fields.get("stage").and_then(Value::as_str),
                    fields.get("outcome").and_then(Value::as_str)
                ),
                (Some("admission"), Some("admitted" | "denied"))
                    | (Some("execution"), Some("succeeded" | "failed"))
                    | (Some("delivery"), Some("authorized" | "denied"))
            )
    }

    fn file_name(&self) -> String {
        format!("part-{}.parquet", self.source_event_id)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundEvent {
    workspace_id: String,
    event: Event,
}

#[derive(Serialize)]
struct PageEvent {
    commit_version: i64,
    workspace_id: String,
    #[serde(flatten)]
    event: Event,
}

#[derive(Serialize)]
struct ActivityPage {
    events: Vec<PageEvent>,
    published_version: i64,
    next_before_version: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageQuery {
    limit: Option<i64>,
    before_version: Option<i64>,
    published_version: Option<i64>,
}

async fn authorize(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    binding: &StreamBinding,
    privilege: Privilege,
    operation: CredentialOperation,
) -> UnityCatalogResult<(DeltaRoot, ObservedStorageGovernance)> {
    reject_table_commit_for_control_v1(state, ctx)?;
    let table_auth =
        authorize_table_access(state, ctx, &binding.table_id.to_string(), privilege).await?;
    let root = load_stream_root(state, ctx, binding).await?;
    let auth = authorize_path_access(
        state,
        ctx,
        &root.uri,
        operation,
        Some(table_auth.target),
        Some(table_auth.observed),
        DEFAULT_CREDENTIAL_TTL,
    )
    .await?;
    Ok((root, auth.observed))
}

fn binding_for<'a>(
    state: &'a UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    stream_id: &str,
) -> UnityCatalogResult<&'a StreamBinding> {
    state
        .config
        .event_streams
        .resolve(&ctx.tenant, &ctx.workspace, stream_id)
        .ok_or_else(|| UnityCatalogError::Forbidden {
            message: "event_stream_not_bound".to_string(),
        })
}

fn columns(schema: &EventSchema) -> impl Iterator<Item = (&str, &str, DataType, bool)> {
    EVENT_COLUMNS
        .into_iter()
        .chain(schema.fields().iter().map(|field| {
            let (kind, arrow_type) = field.kind.column_types();
            (field.name.as_str(), kind, arrow_type, field.nullable)
        }))
}

pub(super) async fn load_stream_root(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    binding: &StreamBinding,
) -> UnityCatalogResult<DeltaRoot> {
    let (table, registered_columns) = load_table(state, ctx, &binding.table_id.to_string()).await?;
    if (binding.schema.is_logfood() && table.name != "activity_events")
        || registered_columns.len() != EVENT_COLUMNS.len() + binding.schema.fields().len()
        || !registered_columns.iter().zip(columns(&binding.schema)).all(
            |(column, (name, kind, _, nullable))| {
                column.name == name
                    && column.data_type.eq_ignore_ascii_case(kind)
                    && column.is_nullable == nullable
            },
        )
    {
        return Err(UnityCatalogError::Conflict {
            message: "event_stream_table_schema_mismatch".to_string(),
        });
    }
    if arco_catalog::managed_delta::has_placement(&scoped_storage(state, ctx)?, binding.table_id)
        .await
        .map_err(|_| unavailable())?
    {
        return Err(UnityCatalogError::Conflict {
            message: "event_stream_analytical_placement_unsupported".to_string(),
        });
    }
    DeltaRoot::resolve(state, ctx, &table, false)
}

#[utoipa::path(
    post, path = "/api/v1/event-streams/{streamId}/events", tag = "event-streams",
    params(("streamId" = String, Path, description = "Approved Workspace stream ID")),
    request_body = StreamEvent,
    responses(
        (status = 200, description = "Finalized event receipt", body = StreamReceipt),
        (status = 400, description = "Event failed schema validation", body = StreamErrorBody),
        (status = 401, description = "JWT authentication required", body = StreamErrorBody),
        (status = 403, description = "Missing stream binding or table MODIFY grant", body = StreamErrorBody),
        (status = 409, description = "Event or stream binding conflicts", body = StreamErrorBody),
        (status = 413, description = "Event body exceeds the size limit", body = StreamErrorBody),
        (status = 429, description = "Tenant rate limit exceeded", body = StreamErrorBody),
        (status = 503, description = "Event commit is unavailable", body = StreamErrorBody),
    ),
    security(("bearerAuth" = []))
)]
async fn stream_append(
    Path(stream_id): Path<String>,
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    payload: Result<Json<StreamEvent>, JsonRejection>,
) -> Result<Response, StreamError> {
    let event = parse_event(payload)?;
    let receipt = append(&state, &ctx, &stream_id, event.into()).await?;
    json_response(&receipt).map_err(Into::into)
}

async fn activity_append(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    payload: Result<Json<Event>, JsonRejection>,
) -> UnityCatalogResult<Response> {
    let event = parse_event(payload)?;
    let receipt = append(&state, &ctx, LOGFOOD_STREAM, event).await?;
    json_response(
        &json!({"source_event_id": receipt.source_event_id, "commit_version": receipt.commit_version}),
    )
}

fn parse_event<T>(payload: Result<Json<T>, JsonRejection>) -> UnityCatalogResult<T> {
    let Json(event) = payload.map_err(|error| {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            return UnityCatalogError::PayloadTooLarge {
                message: "event_too_large".to_string(),
            };
        }
        bad_request("event_invalid")
    })?;
    Ok(event)
}

async fn append(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    stream_id: &str,
    mut event: Event,
) -> UnityCatalogResult<StreamReceipt> {
    let binding = binding_for(state, ctx, stream_id)?;
    let (root, observed) = authorize(
        state,
        ctx,
        binding,
        Privilege::Modify,
        CredentialOperation::Write,
    )
    .await?;
    if !event.admit(&binding.schema) {
        return Err(bad_request("event_invalid"));
    }
    let storage = scoped_storage(state, ctx)?;
    let coordinator = DeltaCommitCoordinator::with_paths(storage.clone(), root.paths.clone());
    let head = coordinator
        .finalized_version()
        .await
        .map_err(|_| unavailable())?;
    let fingerprint = binding_fingerprint(binding)?;
    let initial = bootstrap_log(binding, &fingerprint);
    if head >= 0 {
        verify_bootstrap(&storage, &root.paths, &initial).await?;
    }
    revalidate_observed_watermark(state, ctx, &observed).await?;
    validate_binding(&storage, &stream_binding_path(binding), &fingerprint, true).await?;
    validate_binding(
        &storage,
        &event_stream_table_path(binding.table_id),
        &fingerprint,
        true,
    )
    .await?;
    if head < 0 {
        let commit = coordinator
            .commit_owner_append(BOOTSTRAP_KEY, initial, Utc::now())
            .await
            .map_err(|_| unavailable())?;
        if commit.version != 0 {
            return Err(unavailable());
        }
    }
    revalidate_observed_watermark(state, ctx, &observed).await?;

    let row = BoundEvent {
        workspace_id: ctx.workspace.clone(),
        event,
    };
    let key = row.event.source_event_id.to_string();
    let payload = prepare_event(&storage, &root.paths, &binding.schema, &row).await?;
    // shortcut: one event per commit, batch when measured write load requires it
    let commit = coordinator
        .commit_owner_append(&key, payload, Utc::now())
        .await
        .map_err(|_| unavailable())?;
    Ok(StreamReceipt {
        source_event_id: row.event.source_event_id,
        commit_version: commit.version,
    })
}

#[utoipa::path(
    get, path = "/api/v1/event-streams/{streamId}/events", tag = "event-streams",
    params(
        ("streamId" = String, Path, description = "Approved Workspace stream ID"),
        ("limit" = Option<i64>, Query, description = "Page size from 1 to 50, default 25"),
        ("beforeVersion" = Option<i64>, Query, description = "Exclusive continuation version"),
        ("publishedVersion" = Option<i64>, Query, description = "Published version from the first page"),
        ("pinId" = Option<String>, Query, description = "Arco-issued pin. Use in place of publishedVersion"),
    ),
    responses(
        (status = 200, description = "Authorized event page", body = StreamPage),
        (status = 400, description = "Invalid page bounds", body = StreamErrorBody),
        (status = 401, description = "JWT authentication required", body = StreamErrorBody),
        (status = 403, description = "Missing stream binding or table SELECT grant", body = StreamErrorBody),
        (status = 404, description = "Pin was not found in this Workspace", body = StreamErrorBody),
        (status = 409, description = "Stream identity conflict", body = StreamErrorBody),
        (status = 429, description = "Tenant rate limit exceeded", body = StreamErrorBody),
        (status = 503, description = "Event stream is unavailable", body = StreamErrorBody),
    ),
    security(("bearerAuth" = []))
)]
async fn stream_read(
    Path(stream_id): Path<String>,
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    query: Result<Query<StreamPageQuery>, QueryRejection>,
) -> Result<Response, StreamError> {
    let Query(query) = query.map_err(|_| bad_request("event_page_invalid"))?;
    let page = read_page(
        &state,
        &ctx,
        &stream_id,
        PageQuery {
            limit: query.limit,
            before_version: query.before_version,
            published_version: query.published_version,
        },
        query.pin_id.as_deref(),
    )
    .await?;
    json_response(&StreamPage {
        events: page
            .events
            .into_iter()
            .map(|row| StreamRow {
                workspace_id: row.workspace_id,
                commit_version: row.commit_version,
                event: row.event.into(),
            })
            .collect(),
        published_version: page.published_version,
        next_before_version: page.next_before_version,
    })
    .map_err(Into::into)
}

async fn activity_read(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> UnityCatalogResult<Response> {
    let Query(query) = query.map_err(|_| bad_request("event_page_invalid"))?;
    let page = read_page(&state, &ctx, LOGFOOD_STREAM, query, None).await?;
    json_response(&page)
}

async fn read_page(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    stream_id: &str,
    query: PageQuery,
    pin_id: Option<&str>,
) -> UnityCatalogResult<ActivityPage> {
    let limit = query.limit.unwrap_or(25);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(bad_request("event_page_invalid"));
    }
    let binding = binding_for(state, ctx, stream_id)?;
    let (root, observed) = authorize(
        state,
        ctx,
        binding,
        Privilege::Select,
        CredentialOperation::Read,
    )
    .await?;
    let storage = scoped_storage(state, ctx)?;
    let (coordinator, head) = published_head(&storage, &root.paths, binding).await?;
    let through = match pin_id {
        Some(id) if query.published_version.is_none() => {
            load_pin(&storage, binding, &root, id).await?
        }
        Some(_) => return Err(bad_request("event_page_invalid")),
        None => query.published_version.unwrap_or(head),
    };
    let newest_bound = through.saturating_add(1);
    let before = query.before_version.unwrap_or(newest_bound);
    if through < 0 || through > head || before < 1 || before > newest_bound {
        return Err(bad_request("event_page_invalid"));
    }
    let last = through.min(before - 1);
    let first = (last - limit + 1).max(1);
    let mut events = Vec::new();
    for version in (first..=last).rev() {
        events.push(
            read_event(
                &storage,
                &root.paths,
                &coordinator,
                &binding.schema,
                &ctx.workspace,
                version,
            )
            .await?,
        );
    }
    load_stream_root(state, ctx, binding).await?;
    revalidate_observed_watermark(state, ctx, &observed).await?;
    Ok(ActivityPage {
        events,
        published_version: through,
        next_before_version: (first > 1).then_some(first),
    })
}

async fn published_head(
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    binding: &StreamBinding,
) -> UnityCatalogResult<(DeltaCommitCoordinator, i64)> {
    let fingerprint = binding_fingerprint(binding)?;
    validate_binding(storage, &stream_binding_path(binding), &fingerprint, false).await?;
    let coordinator = DeltaCommitCoordinator::with_paths(storage.clone(), paths.clone());
    let finalized = coordinator
        .finalized_version()
        .await
        .map_err(|_| unavailable())?;
    if finalized >= 0 {
        verify_bootstrap(storage, paths, &bootstrap_log(binding, &fingerprint)).await?;
    }
    Ok((coordinator, finalized.max(0)))
}

fn binding_fingerprint(binding: &StreamBinding) -> UnityCatalogResult<String> {
    let canonical = to_canonical_bytes(binding).map_err(|_| unavailable())?;
    Ok(hex::encode(Sha256::digest(canonical)))
}

fn stream_binding_path(binding: &StreamBinding) -> String {
    format!("event-streams/v1/{}.sha256", binding.stream_id)
}

async fn validate_binding(
    storage: &ScopedStorage,
    path: &str,
    fingerprint: &str,
    pin: bool,
) -> UnityCatalogResult<()> {
    if storage
        .head_raw(path)
        .await
        .map_err(|_| unavailable())?
        .is_none()
    {
        if !pin {
            return Ok(());
        }
        let put = storage
            .put_raw(
                path,
                Bytes::copy_from_slice(fingerprint.as_bytes()),
                WritePrecondition::DoesNotExist,
            )
            .await
            .map_err(|_| unavailable())?;
        if matches!(put, WriteResult::Success { .. }) {
            return Ok(());
        }
    }
    if read_bounded(storage, path).await?.as_ref() != fingerprint.as_bytes() {
        return Err(UnityCatalogError::Conflict {
            message: "event_stream_binding_mismatch".to_string(),
        });
    }
    Ok(())
}

fn bootstrap_log(binding: &StreamBinding, fingerprint: &str) -> Bytes {
    let fields = columns(&binding.schema).map(|(name, kind, _, nullable)| {
        json!({"name": name, "type": kind, "nullable": nullable, "metadata": {}})
    }).collect::<Vec<_>>();
    let schema = json!({"type": "struct", "fields": fields});
    let protocol = json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}});
    let name = if binding.schema.is_logfood() {
        "activity_events"
    } else {
        &binding.stream_id
    };
    let metadata = json!({"metaData": {
        "id": binding.table_id, "name": name, "format": {"provider": "parquet", "options": {}},
        "schemaString": schema.to_string(), "partitionColumns": [], "configuration": {}
    }});
    if binding.schema.is_logfood() {
        // The original activity table's version zero is part of its pinned identity.
        Bytes::from(format!("{protocol}\n{metadata}\n"))
    } else {
        let identity = json!({"commitInfo": {"userMetadata": fingerprint}});
        Bytes::from(format!("{protocol}\n{metadata}\n{identity}\n"))
    }
}

async fn verify_bootstrap(
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    expected: &Bytes,
) -> UnityCatalogResult<()> {
    let path = paths.delta_log_json(0).map_err(|_| unavailable())?;
    if read_bounded(storage, &path).await?.as_ref() != expected.as_ref() {
        return Err(UnityCatalogError::Conflict {
            message: "event_stream_table_bootstrap_mismatch".to_string(),
        });
    }
    Ok(())
}

async fn prepare_event(
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    schema: &EventSchema,
    row: &BoundEvent,
) -> UnityCatalogResult<Bytes> {
    let canonical = to_canonical_bytes(row).map_err(|_| unavailable())?;
    let digest = hex::encode(Sha256::digest(&canonical));
    let filename = row.event.file_name();
    let path = format!("{}/{filename}", paths.table_root());
    let data = parquet_bytes(schema, row, &digest)?;
    let data = match storage
        .put_raw(&path, data.clone(), WritePrecondition::DoesNotExist)
        .await
        .map_err(|_| unavailable())?
    {
        WriteResult::Success { .. } => data,
        WriteResult::PreconditionFailed { .. } => {
            let existing = read_bounded(storage, &path).await?;
            let reader = SerializedFileReader::new(existing.clone()).map_err(|_| unavailable())?;
            let metadata = reader.metadata().file_metadata();
            let matching = metadata
                .key_value_metadata()
                .and_then(|items| items.iter().find(|item| item.key == EVENT_DIGEST))
                .and_then(|item| item.value.as_deref());
            match matching {
                Some(value) if value == digest => existing,
                Some(_) => {
                    return Err(UnityCatalogError::Conflict {
                        message: "event_id_conflict".to_string(),
                    });
                }
                None => return Err(unavailable()),
            }
        }
    };
    let event_json = String::from_utf8(canonical).map_err(|_| unavailable())?;
    let log = json!({"add": {
        "path": filename, "size": data.len(), "partitionValues": {},
        "modificationTime": row.event.occurred_at_ms, "dataChange": true,
        "tags": {"arco.content.sha256": hex::encode(Sha256::digest(&data)), (EVENT_TAG): event_json}
    }});
    Ok(Bytes::from(format!("{log}\n")))
}

fn parquet_bytes(
    schema: &EventSchema,
    row: &BoundEvent,
    digest: &str,
) -> UnityCatalogResult<Bytes> {
    let event = &row.event;
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![row.workspace_id.as_str()])),
        Arc::new(Int32Array::from(vec![event.schema_version])),
        Arc::new(StringArray::from(vec![event.source_event_id.to_string()])),
        Arc::new(Int64Array::from(vec![event.occurred_at_ms])),
    ];
    for field in schema.fields() {
        let value = event.fields.0.get(&field.name).ok_or_else(unavailable)?;
        let array: ArrayRef = match field.kind {
            EventFieldType::Identifier | EventFieldType::Enum => {
                Arc::new(StringArray::from(vec![value.as_str()]))
            }
            EventFieldType::Integer => Arc::new(Int64Array::from(vec![value.as_i64()])),
            EventFieldType::Boolean => Arc::new(BooleanArray::from(vec![value.as_bool()])),
        };
        arrays.push(array);
    }
    let fields = columns(schema)
        .map(|(name, _, kind, nullable)| Field::new(name, kind, nullable))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(|_| unavailable())?;
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue::new(
            EVENT_DIGEST.to_string(),
            digest.to_string(),
        )]))
        .build();
    let mut writer =
        ArrowWriter::try_new(Vec::new(), schema, Some(properties)).map_err(|_| unavailable())?;
    writer.write(&batch).map_err(|_| unavailable())?;
    writer
        .into_inner()
        .map(Bytes::from)
        .map_err(|_| unavailable())
}

async fn read_event(
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    coordinator: &DeltaCommitCoordinator,
    schema: &EventSchema,
    workspace: &str,
    version: i64,
) -> UnityCatalogResult<PageEvent> {
    let path = paths.delta_log_json(version).map_err(|_| unavailable())?;
    let log = read_bounded(storage, &path).await?;
    let action: Value = serde_json::from_slice(&log).map_err(|_| unavailable())?;
    let add = action.get("add").ok_or_else(identity_mismatch)?;
    let tags = add.get("tags").ok_or_else(identity_mismatch)?;
    let tag = tags
        .get(EVENT_TAG)
        .and_then(Value::as_str)
        .ok_or_else(identity_mismatch)?;
    let mut row: BoundEvent = serde_json::from_str(tag).map_err(|_| identity_mismatch())?;
    let filename = row.event.file_name();
    if row.workspace_id != workspace
        || !row.event.admit(schema)
        || add.get("path").and_then(Value::as_str) != Some(filename.as_str())
    {
        return Err(identity_mismatch());
    }
    coordinator
        .verify_owner_commit(&row.event.source_event_id.to_string(), version, &log)
        .await
        .map_err(|error| match error {
            DeltaError::Conflict { .. } => identity_mismatch(),
            _ => unavailable(),
        })?;
    let data = read_bounded(storage, &format!("{}/{filename}", paths.table_root())).await?;
    let digest = hex::encode(Sha256::digest(&data));
    if add.get("size").and_then(Value::as_u64) != u64::try_from(data.len()).ok()
        || tags.get("arco.content.sha256").and_then(Value::as_str) != Some(digest.as_str())
    {
        return Err(identity_mismatch());
    }
    Ok(PageEvent {
        commit_version: version,
        workspace_id: row.workspace_id,
        event: row.event,
    })
}

async fn read_bounded(storage: &ScopedStorage, path: &str) -> UnityCatalogResult<Bytes> {
    let meta = storage
        .head_raw(path)
        .await
        .map_err(|_| unavailable())?
        .ok_or_else(unavailable)?;
    if !(1..=MAX_OBJECT_BYTES).contains(&meta.size) {
        return Err(unavailable());
    }
    let bytes = storage
        .get_range(path, 0..meta.size)
        .await
        .map_err(|_| unavailable())?;
    if u64::try_from(bytes.len()).ok() != Some(meta.size) {
        return Err(unavailable());
    }
    Ok(bytes)
}

fn bad_request(message: &str) -> UnityCatalogError {
    UnityCatalogError::BadRequest {
        message: message.to_string(),
    }
}

fn identity_mismatch() -> UnityCatalogError {
    UnityCatalogError::Conflict {
        message: "event_stream_commit_identity_mismatch".to_string(),
    }
}

fn unavailable() -> UnityCatalogError {
    UnityCatalogError::ServiceUnavailable {
        message: "event_stream_unavailable".to_string(),
    }
}

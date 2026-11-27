//! Admits an append-only Delta log prefix and passes its original bytes to the reader.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use arco_catalog::authz::privileges::Privilege;
use arco_catalog::credential_vending::{CredentialOperation, DEFAULT_CREDENTIAL_TTL};
use arco_catalog::managed_delta::{PLACEMENT_PROPERTY, TABLE_COLUMNS, resolve_analytical_storage};
use arco_catalog::storage_governance::{PathDecision, path_normalization::GovernedPath};
use arco_catalog::{Column, Table};
use arco_core::canonical_json::to_canonical_bytes;
use arco_core::storage::StorageBackend as _;
use arco_core::{DeltaPaths, ScopedStorage, TableFormat};
use arco_delta::coordinator::MAX_PINNED_DELTA_VERSION;
use arco_delta::{DeltaCommitCoordinator, DeltaError};
use axum::response::Response;
use base64::Engine as _;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::{
    MAX_TABLE_READ_BYTES, UnityCatalogError, UnityCatalogResult, dataset_descriptor_response,
    is_lowercase_sha256, json_response, load_table, preflight_parquet_footer, sha256_hex,
    table_fingerprint, table_read_conflict, table_read_storage_error,
};
use crate::audit;
use crate::context::UnityCatalogRequestContext;
use crate::routes::common::{map_catalog_error, scoped_storage};
use crate::routes::credentials::{
    AuthorizedCredentialPath, authorize_path_access, authorize_table_access,
    revalidate_observed_watermark,
};
use crate::state::UnityCatalogState;

const MAX_LOG_BYTES: u64 = 256 * 1024;
const MAX_DATA_FILES: usize = 32;
const MAX_COLUMNS: usize = 128;
const MAX_DATA_NAME_STEM_BYTES: usize = 120;
const READ_TIMEOUT: Duration = Duration::from_secs(15);
const CONTENT_SHA256_TAG: &str = "arco.content.sha256";

pub(super) enum Disclosure {
    Descriptor,
    Bundle,
}

pub(super) async fn respond(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    table_id: &str,
    version: i64,
    expected_snapshot_id: Option<&str>,
    disclosure: Disclosure,
) -> UnityCatalogResult<Response> {
    if Uuid::parse_str(table_id).is_err() || version < 0 {
        return Err(bad_request("delta_read_invalid_table_id_or_version"));
    }
    if version > MAX_PINNED_DELTA_VERSION {
        return Err(limit_exceeded());
    }
    if matches!(disclosure, Disclosure::Bundle) && expected_snapshot_id.is_none() {
        return Err(bad_request("delta_read_pin_required"));
    }
    tokio::time::timeout(
        READ_TIMEOUT,
        disclose_snapshot(
            state,
            ctx,
            table_id,
            version,
            expected_snapshot_id,
            disclosure,
        ),
    )
    .await
    .map_err(|_| UnityCatalogError::ServiceUnavailable {
        message: "delta_read_deadline_exceeded".to_string(),
    })?
}

async fn disclose_snapshot(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    table_id: &str,
    version: i64,
    expected_snapshot_id: Option<&str>,
    disclosure: Disclosure,
) -> UnityCatalogResult<Response> {
    let table_auth = authorize_table_access(state, ctx, table_id, Privilege::Select).await?;
    let (table, columns) = load_table(state, ctx, table_id).await?;
    let storage = scoped_storage(state, ctx)?;
    let analytical =
        resolve_analytical_storage(state.managed_delta_storage.as_ref(), &storage, &table)
            .await
            .map_err(map_catalog_error)?;
    let root = DeltaRoot::resolve(state, ctx, &table, analytical.is_some())?;
    let fingerprint = table_fingerprint(&table, &columns)?;
    let authorization = authorize_path_access(
        state,
        ctx,
        &root.uri,
        CredentialOperation::Read,
        Some(table_auth.target),
        Some(table_auth.observed),
        DEFAULT_CREDENTIAL_TTL,
    )
    .await?;
    let governance = DeltaGovernance::new(&authorization, ctx, &root.uri)?;
    let snapshot = load_snapshot(
        &storage,
        analytical.as_ref().unwrap_or(&storage),
        &root.paths,
        &governance,
        version,
    )
    .await?;
    if analytical.is_some() && !snapshot.matches_allocated_schema() {
        return Err(bad_request("delta_read_allocated_schema_mismatch"));
    }
    let (snapshot_id, identity) = snapshot.identity(ctx, &table.id, &fingerprint, version)?;

    let final_table_auth = authorize_table_access(state, ctx, table_id, Privilege::Select).await?;
    let (final_table, final_columns) = load_table(state, ctx, table_id).await?;
    if table_fingerprint(&final_table, &final_columns)? != fingerprint {
        return Err(table_read_conflict());
    }
    if resolve_analytical_storage(state.managed_delta_storage.as_ref(), &storage, &final_table)
        .await
        .map_err(map_catalog_error)?
        .is_some()
        != analytical.is_some()
    {
        return Err(table_read_conflict());
    }
    let final_authorization = authorize_path_access(
        state,
        ctx,
        &root.uri,
        CredentialOperation::Read,
        Some(final_table_auth.target),
        Some(final_table_auth.observed),
        DEFAULT_CREDENTIAL_TTL,
    )
    .await?;
    let final_governance = DeltaGovernance::new(&final_authorization, ctx, &root.uri)?;
    for object in &snapshot.objects {
        final_governance.check(&object.identity.path)?;
    }
    revalidate_observed_watermark(state, ctx, &final_authorization.observed).await?;
    if expected_snapshot_id.is_some_and(|expected| expected != snapshot_id) {
        return Err(UnityCatalogError::Conflict {
            message: "table_read_snapshot_mismatch".to_string(),
        });
    }
    audit::emit_table_read_allow(
        state,
        ctx,
        table_id,
        &snapshot_id,
        &final_authorization.observed.published.ledger_watermark,
        &final_authorization.decision,
    );

    match disclosure {
        Disclosure::Descriptor => dataset_descriptor_response(
            &final_table,
            &descriptor_schema(snapshot.fields, &final_columns),
            &identity,
            &json!({
                "object": "bounded_delta_snapshot_v1", "snapshot": "explicit_committed_delta_version",
                "row_filter": "unsupported", "column_mask": "unsupported",
            }),
        ),
        Disclosure::Bundle => json_response(&json!({
            "input_identity": identity,
            "objects": snapshot.objects.into_iter().map(SnapshotObject::into_wire).collect::<Vec<_>>(),
            "authorization": {
                "permission_watermark": final_authorization.observed.published.ledger_watermark,
                "audit_event_id": final_authorization.decision.audit_event_id,
                "principal_sha256": sha256_hex(ctx.user_id.as_deref().unwrap_or_default().as_bytes()),
            },
        })),
    }
}

pub(in crate::routes) struct DeltaRoot {
    pub(in crate::routes) paths: DeltaPaths,
    pub(in crate::routes) uri: String,
}

impl DeltaRoot {
    pub(in crate::routes) fn resolve(
        state: &UnityCatalogState,
        ctx: &UnityCatalogRequestContext,
        table: &Table,
        allocated: bool,
    ) -> UnityCatalogResult<Self> {
        if TableFormat::effective(table.format.as_deref()).ok() != Some(TableFormat::Delta)
            || !table
                .table_type
                .as_deref()
                .is_some_and(|kind| kind.eq_ignore_ascii_case("managed"))
        {
            return Err(bad_request("delta_read_managed_delta_required"));
        }
        if table.properties.as_ref().is_some_and(|properties| {
            properties.keys().any(|name| {
                !(matches!(
                    name.as_str(),
                    "arco.dataset.grain"
                        | "arco.dataset.units"
                        | "arco.dataset.loaded_at"
                        | "arco.dataset.source_data_cutoff"
                ) || allocated && name == PLACEMENT_PROPERTY)
            })
        }) {
            return Err(bad_request("delta_read_unsupported_table_properties"));
        }
        let id = Uuid::parse_str(&table.id)
            .map_err(|_| bad_request("delta_read_invalid_table_id_or_version"))?;
        let paths = DeltaPaths::legacy(id);
        if allocated {
            let uri = table.location.clone().ok_or_else(path_denied)?;
            return Ok(Self { paths, uri });
        }
        let bucket = state
            .config
            .table_read_storage_root
            .as_deref()
            .filter(|root| root.starts_with("s3://"))
            .ok_or_else(|| UnityCatalogError::ServiceUnavailable {
                message: "delta_read_storage_mapping_unavailable".to_string(),
            })?;
        let uri = format!(
            "{bucket}/tenant={}/workspace={}/{}/",
            ctx.tenant,
            ctx.workspace,
            paths.table_root()
        );
        if let Some(location) = table.location.as_deref() {
            let mapped = GovernedPath::parse(location).map(|path| path.canonical_uri());
            let writer_paths =
                DeltaPaths::from_table_location(id, Some(location), &ctx.tenant, &ctx.workspace);
            if mapped.as_deref().ok() != Some(uri.as_str())
                || writer_paths.as_ref().ok() != Some(&paths)
            {
                return Err(bad_request("delta_read_location_not_mapped"));
            }
        }
        Ok(Self { paths, uri })
    }
}

struct DeltaGovernance<'a> {
    authorization: &'a AuthorizedCredentialPath,
    workspace: &'a str,
    root: &'a str,
    authority: PathDecision,
}

impl<'a> DeltaGovernance<'a> {
    fn new(
        authorization: &'a AuthorizedCredentialPath,
        ctx: &'a UnityCatalogRequestContext,
        root: &'a str,
    ) -> UnityCatalogResult<Self> {
        let authority = authorization
            .observed
            .published
            .state
            .authority_for_path(&ctx.workspace, root)
            .map_err(|_| path_denied())?;
        if authorization.decision.authorized_object_id.as_deref() != Some(&authority.object_id)
            || authorization
                .decision
                .provider
                .as_deref()
                .is_some_and(|provider| !provider.eq_ignore_ascii_case("s3"))
        {
            return Err(path_denied());
        }
        Ok(Self {
            authorization,
            workspace: &ctx.workspace,
            root,
            authority,
        })
    }

    fn check(&self, path: &str) -> UnityCatalogResult<()> {
        let uri = format!("{}{path}", self.root);
        let actual = self
            .authorization
            .observed
            .published
            .state
            .authority_for_path(self.workspace, &uri)
            .map_err(|_| path_denied())?;
        if actual == self.authority {
            Ok(())
        } else {
            Err(path_denied())
        }
    }
}

struct Snapshot {
    delta_table_id: Uuid,
    fields: Vec<SchemaField>,
    objects: Vec<SnapshotObject>,
    byte_size: u64,
}

impl Snapshot {
    fn matches_allocated_schema(&self) -> bool {
        self.fields.len() == TABLE_COLUMNS.len()
            && self
                .fields
                .iter()
                .zip(TABLE_COLUMNS)
                .all(|(field, name)| field.name == name && field.kind == "long" && !field.nullable)
    }

    fn identity(
        &self,
        ctx: &UnityCatalogRequestContext,
        table_id: &str,
        fingerprint: &str,
        version: i64,
    ) -> UnityCatalogResult<(String, Value)> {
        let manifest = self
            .objects
            .iter()
            .map(|object| &object.identity)
            .collect::<Vec<_>>();
        let Value::Object(mut identity) = json!({
            "format": "delta", "bundle_version": 1, "tenant": ctx.tenant, "workspace": ctx.workspace,
            "table_id": table_id, "table_fingerprint": fingerprint, "delta_table_id": self.delta_table_id,
            "delta_version": version, "byte_size": self.byte_size,
            "manifest_sha256": canonical_digest(&manifest)?,
        }) else {
            return Err(identity_unavailable());
        };
        let snapshot_id = canonical_digest(&identity)?;
        identity.insert("snapshot_id".to_string(), json!(snapshot_id));
        Ok((snapshot_id, Value::Object(identity)))
    }
}

async fn load_snapshot(
    catalog: &ScopedStorage,
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    governance: &DeltaGovernance<'_>,
    version: i64,
) -> UnityCatalogResult<Snapshot> {
    let digests = DeltaCommitCoordinator::with_paths(catalog.clone(), paths.clone())
        .pinned_log_digests(version)
        .await
        .map_err(coordinator_error)?;
    let mut log = AppendLog::default();
    let mut objects = Vec::new();
    let mut remaining = MAX_TABLE_READ_BYTES;
    for (index, digest) in (0..=version).zip(digests) {
        let path = paths
            .delta_log_json(index)
            .map_err(|error| table_read_storage_error(&error))?;
        let relative = path
            .strip_prefix(paths.table_root())
            .and_then(|suffix| suffix.strip_prefix('/'))
            .ok_or_else(path_denied)?;
        let object = read_object(
            storage,
            paths,
            governance,
            relative,
            &digest,
            remaining.min(MAX_LOG_BYTES),
        )
        .await?;
        log.admit(&object.bytes, index == 0)?;
        remaining -= object.identity.byte_size;
        objects.push(object);
    }
    let metadata = log.metadata.ok_or_else(unsupported_log)?;
    for (path, file) in log.files {
        let object =
            read_object(storage, paths, governance, &path, &file.sha256, remaining).await?;
        if object.identity.byte_size != file.size {
            return Err(changed_object());
        }
        preflight_parquet_footer(&object.bytes)?;
        remaining -= object.identity.byte_size;
        objects.push(object);
    }
    Ok(Snapshot {
        delta_table_id: metadata.id,
        fields: metadata.fields,
        objects,
        byte_size: MAX_TABLE_READ_BYTES - remaining,
    })
}

#[derive(Serialize)]
struct ObjectIdentity {
    path: String,
    byte_size: u64,
    sha256: String,
}

struct SnapshotObject {
    identity: ObjectIdentity,
    bytes: Bytes,
}

impl SnapshotObject {
    fn into_wire(self) -> Value {
        json!({
            "path": self.identity.path, "byte_size": self.identity.byte_size,
            "sha256": self.identity.sha256,
            "data_base64": base64::engine::general_purpose::STANDARD.encode(self.bytes),
        })
    }
}

async fn read_object(
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    governance: &DeltaGovernance<'_>,
    relative: &str,
    expected_sha256: &str,
    limit: u64,
) -> UnityCatalogResult<SnapshotObject> {
    governance.check(relative)?;
    let path = format!("{}/{relative}", paths.table_root());
    let meta = storage
        .head_raw(&path)
        .await
        .map_err(|error| table_read_storage_error(&error))?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: "delta_read_object_not_found".to_string(),
        })?;
    if meta.size > limit {
        return Err(limit_exceeded());
    }
    if meta.size == 0 {
        return Err(changed_object());
    }
    let bytes = storage
        .get_range(&path, 0..meta.size)
        .await
        .map_err(|error| table_read_storage_error(&error))?;
    if u64::try_from(bytes.len()).ok() != Some(meta.size) || sha256_hex(&bytes) != expected_sha256 {
        return Err(changed_object());
    }
    Ok(SnapshotObject {
        identity: ObjectIdentity {
            path: relative.to_string(),
            byte_size: meta.size,
            sha256: expected_sha256.to_string(),
        },
        bytes,
    })
}

// Admit a subset with no removals or schema changes, so every admitted add is
// in the snapshot. The native reader still resolves and reads the Delta log.
#[derive(Default)]
struct AppendLog {
    initial_protocol: bool,
    metadata: Option<SnapshotMetadata>,
    files: BTreeMap<String, DataFile>,
}

struct SnapshotMetadata {
    id: Uuid,
    fields: Vec<SchemaField>,
}

struct DataFile {
    size: u64,
    sha256: String,
}

impl AppendLog {
    fn admit(&mut self, bytes: &[u8], initial: bool) -> UnityCatalogResult<()> {
        let log = std::str::from_utf8(bytes).map_err(|_| unsupported_log())?;
        for line in log.lines() {
            let action: Action = serde_json::from_str(line).map_err(|_| unsupported_log())?;
            match action {
                Action::Protocol(protocol)
                    if initial
                        && !self.initial_protocol
                        && (protocol.min_reader_version, protocol.min_writer_version) == (1, 2) =>
                {
                    self.initial_protocol = true;
                }
                Action::Metadata(metadata) if initial && self.metadata.is_none() => {
                    self.metadata = Some(metadata.admit()?);
                }
                Action::Add(add) => self.admit_add(add)?,
                Action::CommitInfo => {}
                Action::Protocol(_) | Action::Metadata(_) => return Err(unsupported_log()),
            }
        }
        if !self.initial_protocol || self.metadata.is_none() {
            return Err(unsupported_log());
        }
        Ok(())
    }

    fn admit_add(&mut self, add: Add) -> UnityCatalogResult<()> {
        if !valid_file_name(&add.path) {
            return Err(bad_request("delta_read_out_of_scope_object"));
        }
        if !add.partition_values.is_empty() || !add.data_change || add.size == 0 {
            return Err(unsupported_log());
        }
        if self.files.len() >= MAX_DATA_FILES {
            return Err(limit_exceeded());
        }
        let sha256 = add
            .tags
            .get(CONTENT_SHA256_TAG)
            .filter(|hash| is_lowercase_sha256(hash))
            .ok_or_else(|| UnityCatalogError::Conflict {
                message: "delta_read_data_identity_unavailable".to_string(),
            })?;
        if self
            .files
            .keys()
            .any(|path| path.eq_ignore_ascii_case(&add.path))
        {
            return Err(unsupported_log());
        }
        self.files.insert(
            add.path,
            DataFile {
                size: add.size,
                sha256: sha256.clone(),
            },
        );
        Ok(())
    }
}

#[derive(Deserialize)]
enum Action {
    #[serde(rename = "protocol")]
    Protocol(Protocol),
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    #[serde(rename = "add")]
    Add(Add),
    #[serde(rename = "commitInfo", deserialize_with = "commit_info")]
    CommitInfo,
}

fn commit_info<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<(), D::Error> {
    Map::<String, Value>::deserialize(deserializer).map(|_| ())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Protocol {
    min_reader_version: u8,
    min_writer_version: u8,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Metadata {
    id: Uuid,
    format: Format,
    schema_string: String,
    partition_columns: Vec<String>,
    configuration: Map<String, Value>,
    #[serde(rename = "name")]
    _name: Option<String>,
    #[serde(rename = "description")]
    _description: Option<String>,
    #[serde(rename = "createdTime")]
    _created_time: Option<i64>,
}

impl Metadata {
    fn admit(self) -> UnityCatalogResult<SnapshotMetadata> {
        if !self.partition_columns.is_empty()
            || !self.configuration.is_empty()
            || self.format.provider != "parquet"
            || !self.format.options.is_empty()
        {
            return Err(unsupported_log());
        }
        let schema: LogSchema =
            serde_json::from_str(&self.schema_string).map_err(|_| unsupported_log())?;
        let mut names = BTreeSet::new();
        if schema.kind != "struct"
            || schema.fields.is_empty()
            || schema.fields.len() > MAX_COLUMNS
            || schema.fields.iter().any(|field| {
                field.name.is_empty()
                    || field.name.len() > 128
                    || !field.metadata.is_empty()
                    || !names.insert(field.name.to_ascii_lowercase())
                    || !matches!(
                        field.kind.as_str(),
                        "date" | "long" | "integer" | "string" | "boolean"
                    )
            })
        {
            return Err(unsupported_log());
        }
        Ok(SnapshotMetadata {
            id: self.id,
            fields: schema.fields,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Format {
    provider: String,
    options: Map<String, Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Add {
    path: String,
    size: u64,
    data_change: bool,
    partition_values: Map<String, Value>,
    #[serde(rename = "modificationTime")]
    _modification_time: i64,
    #[serde(default)]
    tags: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogSchema {
    #[serde(rename = "type")]
    kind: String,
    fields: Vec<SchemaField>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaField {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    nullable: bool,
    metadata: Map<String, Value>,
}

fn valid_file_name(path: &str) -> bool {
    path.strip_suffix(".parquet").is_some_and(|stem| {
        stem.len() <= MAX_DATA_NAME_STEM_BYTES
            && stem
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && stem
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    })
}

fn descriptor_schema(fields: Vec<SchemaField>, columns: &[Column]) -> Vec<Value> {
    fields
        .into_iter()
        .map(|field| {
            let meaning = columns
                .iter()
                .find(|column| column.name == field.name)
                .and_then(|column| column.description.as_deref());
            json!({"name":field.name,"type":field.kind,"nullable":field.nullable,"meaning":meaning})
        })
        .collect()
}

fn canonical_digest(value: &impl Serialize) -> UnityCatalogResult<String> {
    to_canonical_bytes(value)
        .map(|bytes| sha256_hex(&bytes))
        .map_err(|_| identity_unavailable())
}

fn identity_unavailable() -> UnityCatalogError {
    UnityCatalogError::Internal {
        message: "delta_read_identity_unavailable".to_string(),
    }
}

fn coordinator_error(error: DeltaError) -> UnityCatalogError {
    match error {
        DeltaError::NotFound { message } => UnityCatalogError::NotFound { message },
        DeltaError::Conflict { message } => UnityCatalogError::Conflict { message },
        _ => {
            tracing::warn!(internal_error = %error, "redacted Delta read coordinator error");
            UnityCatalogError::ServiceUnavailable {
                message: "delta_read_coordinator_unavailable".to_string(),
            }
        }
    }
}

fn bad_request(message: &str) -> UnityCatalogError {
    UnityCatalogError::BadRequest {
        message: message.to_string(),
    }
}

fn unsupported_log() -> UnityCatalogError {
    bad_request("delta_read_unsupported_log")
}

fn path_denied() -> UnityCatalogError {
    UnityCatalogError::Forbidden {
        message: "delta_read_storage_authority_denied".to_string(),
    }
}

fn limit_exceeded() -> UnityCatalogError {
    UnityCatalogError::PayloadTooLarge {
        message: "delta_read_limit_exceeded".to_string(),
    }
}

fn changed_object() -> UnityCatalogError {
    UnityCatalogError::Conflict {
        message: "delta_read_object_identity_mismatch".to_string(),
    }
}

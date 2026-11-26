//! Bounded Arco-native table-read extension.
//!
//! Returns one verified Parquet object or a bounded, pinned Delta snapshot.
//! Both profiles reuse UC credential authorization and fence disclosure.

pub(super) mod delta;

use std::collections::BTreeMap;

use arco_catalog::authz::privileges::Privilege;
use arco_catalog::credential_vending::{CredentialOperation, DEFAULT_CREDENTIAL_TTL};
use arco_catalog::storage_governance::path_normalization::GovernedPath;
use arco_catalog::{Column, Table};
use arco_core::storage::ObjectMeta;
use arco_core::{ScopedStorage, TableFormat};
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Extension, Path, Request, State};
use axum::http::{HeaderName, HeaderValue, StatusCode, header};
use axum::response::Response;
use axum::routing::post;
use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::to_thrift;
use parquet::thrift::{TCompactOutputProtocol, TSerializable};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::audit;
use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogResult};
use crate::routes::common::{catalog_authority, map_catalog_error, scoped_storage};
use crate::routes::credentials::{
    authorize_path_access, authorize_table_access, revalidate_observed_watermark,
};
use crate::state::UnityCatalogState;

const MAX_TABLE_READ_BYTES: u64 = 8 * 1024 * 1024;
const MAX_TABLE_READ_REQUEST_BYTES: usize = 1024;
const MAX_TABLE_READ_PARQUET_FOOTER_BYTES: usize = 1024 * 1024;
const MAX_TABLE_READ_PARQUET_THRIFT_CONTAINER_ITEMS: usize = 65_536;
const MAX_TABLE_READ_PARQUET_THRIFT_TOTAL_CONTAINER_ELEMENTS: usize = 65_536;
const MAX_TABLE_READ_PARQUET_THRIFT_DEPTH: usize = 64;
const TABLE_READ_VERSION: &str = "1";
const PHYSICAL_SCHEMA_DOMAIN: &[u8] = b"arco.table-read.physical-schema.v1\0";
const TABLE_READ_ENFORCEMENT: &str =
    "table_select,storage_governance;row_filter=unsupported;column_mask=unsupported";

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";
const COMPACT_THRIFT_STOP: u8 = 0;
const COMPACT_THRIFT_BOOL_TRUE: u8 = 1;
const COMPACT_THRIFT_BOOL_FALSE: u8 = 2;
const COMPACT_THRIFT_BYTE: u8 = 3;
const COMPACT_THRIFT_I16: u8 = 4;
const COMPACT_THRIFT_I32: u8 = 5;
const COMPACT_THRIFT_I64: u8 = 6;
const COMPACT_THRIFT_DOUBLE: u8 = 7;
const COMPACT_THRIFT_BINARY: u8 = 8;
const COMPACT_THRIFT_LIST: u8 = 9;
const COMPACT_THRIFT_SET: u8 = 10;
const COMPACT_THRIFT_MAP: u8 = 11;
const COMPACT_THRIFT_STRUCT: u8 = 12;

/// Table-read route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route("/extensions/table-reads/:table_id", post(post_table_read))
        .route(
            "/extensions/table-descriptors/:table_id",
            post(post_table_descriptor),
        )
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TableReadRequest {
    expected_snapshot_id: Option<String>,
    delta_version: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedTableObject {
    relative_path: String,
    governed_path: String,
}

/// `POST /extensions/table-reads/{table_id}`.
///
/// The caller receives a single, bounded Parquet object only after the current
/// TABLE SELECT, storage-governance, catalog, object, and ledger fences agree.
async fn post_table_read(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(table_id): Path<String>,
    request: Request,
) -> UnityCatalogResult<Response> {
    let request = parse_table_read_request(request).await?;
    if let Some(version) = request.delta_version {
        return delta::respond(
            &state,
            &ctx,
            &table_id,
            version,
            request.expected_snapshot_id.as_deref(),
            delta::Disclosure::Bundle,
        )
        .await;
    }

    let read = authorized_table_read(&state, &ctx, &table_id, request).await?;
    table_read_response(
        read.bytes,
        &ctx,
        &read.table,
        &read.table_fingerprint,
        &read.object_version_sha256,
        &read.content_sha256,
        read.byte_size,
        &read.physical_schema_sha256,
        &read.snapshot_id,
        &read.permission_watermark,
        &read.audit_event_id,
    )
}

/// Owner metadata for a pinned object or Delta version.
async fn post_table_descriptor(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(table_id): Path<String>,
    request: Request,
) -> UnityCatalogResult<Response> {
    let request = parse_table_read_request(request).await?;
    if let Some(version) = request.delta_version {
        return delta::respond(
            &state,
            &ctx,
            &table_id,
            version,
            request.expected_snapshot_id.as_deref(),
            delta::Disclosure::Descriptor,
        )
        .await;
    }
    let read = authorized_table_read(&state, &ctx, &table_id, request).await?;
    table_descriptor_response(&ctx, &read)
}

struct AuthorizedTableRead {
    bytes: Bytes,
    table: Table,
    columns: Vec<Column>,
    table_fingerprint: String,
    object_version_sha256: String,
    content_sha256: String,
    byte_size: u64,
    physical_schema_sha256: String,
    snapshot_id: String,
    permission_watermark: String,
    audit_event_id: String,
}

async fn authorized_table_read(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    table_id: &str,
    request: TableReadRequest,
) -> UnityCatalogResult<AuthorizedTableRead> {
    // Authorization precedes catalog lookup to avoid turning this endpoint
    // into a table-existence oracle for principals without SELECT.
    let initial_table_authorization =
        authorize_table_access(state, ctx, table_id, Privilege::Select).await?;
    let (table, columns) = load_table(state, ctx, table_id).await?;
    let initial_table_fingerprint = table_fingerprint(&table, &columns)?;
    let object = scoped_table_object(state, ctx, &table)?;
    authorize_path_access(
        state,
        ctx,
        &object.governed_path,
        CredentialOperation::Read,
        Some(initial_table_authorization.target),
        Some(initial_table_authorization.observed),
        DEFAULT_CREDENTIAL_TTL,
    )
    .await?;

    let storage = scoped_storage(state, ctx)?;
    let (initial_meta, bytes) = read_bounded_object(&storage, &object.relative_path).await?;
    let object_version_sha256 = opaque_version_sha256(&initial_meta)?;
    let content_sha256 = sha256_hex(&bytes);
    let physical_schema_sha256 = physical_schema_sha256(&bytes)?;
    let snapshot_id = snapshot_id(
        ctx,
        &table.id,
        &initial_table_fingerprint,
        &object_version_sha256,
        &content_sha256,
        initial_meta.size,
        &physical_schema_sha256,
    )?;

    // Re-resolve all authority after data has been read but before it is
    // disclosed. The final watermark fence happens after the object fence.
    let final_table_authorization =
        authorize_table_access(state, ctx, table_id, Privilege::Select).await?;
    let (final_table, final_columns) = load_table(state, ctx, table_id).await?;
    let final_fingerprint = table_fingerprint(&final_table, &final_columns)?;
    let final_object = scoped_table_object(state, ctx, &final_table)?;
    if final_fingerprint != initial_table_fingerprint || final_object != object {
        return Err(table_read_conflict());
    }
    let final_path_authorization = authorize_path_access(
        state,
        ctx,
        &final_object.governed_path,
        CredentialOperation::Read,
        Some(final_table_authorization.target),
        Some(final_table_authorization.observed),
        DEFAULT_CREDENTIAL_TTL,
    )
    .await?;
    verify_unchanged_object(&storage, &object.relative_path, &initial_meta).await?;
    revalidate_observed_watermark(state, ctx, &final_path_authorization.observed).await?;

    if let Some(expected_snapshot_id) = request.expected_snapshot_id.as_deref() {
        if expected_snapshot_id != snapshot_id {
            return Err(UnityCatalogError::Conflict {
                message: "table_read_snapshot_mismatch".to_string(),
            });
        }
    }

    audit::emit_credentials_allow(
        state,
        ctx,
        &object.governed_path,
        &final_path_authorization.decision.reason_code,
    );

    Ok(AuthorizedTableRead {
        bytes,
        table: final_table,
        columns: final_columns,
        table_fingerprint: initial_table_fingerprint,
        object_version_sha256,
        content_sha256,
        byte_size: initial_meta.size,
        physical_schema_sha256,
        snapshot_id,
        permission_watermark: final_path_authorization
            .observed
            .published
            .ledger_watermark
            .clone(),
        audit_event_id: final_path_authorization.decision.audit_event_id,
    })
}

async fn parse_table_read_request(request: Request) -> UnityCatalogResult<TableReadRequest> {
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = to_bytes(request.into_body(), MAX_TABLE_READ_REQUEST_BYTES)
        .await
        .map_err(|_| UnityCatalogError::PayloadTooLarge {
            message: "table_read_request_too_large".to_string(),
        })?;
    if body.is_empty() {
        return Ok(TableReadRequest::default());
    }
    if !content_type
        .as_deref()
        .is_some_and(|value| value.starts_with("application/json"))
    {
        return Err(UnityCatalogError::BadRequest {
            message: "table_read_request_requires_json".to_string(),
        });
    }
    let payload = serde_json::from_slice::<TableReadRequest>(&body).map_err(|_| {
        UnityCatalogError::BadRequest {
            message: "table_read_request_invalid".to_string(),
        }
    })?;
    if payload
        .expected_snapshot_id
        .as_deref()
        .is_some_and(|value| !is_lowercase_sha256(value))
    {
        return Err(UnityCatalogError::BadRequest {
            message: "table_read_expected_snapshot_id_invalid".to_string(),
        });
    }
    Ok(payload)
}

pub(super) async fn load_table(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    table_id: &str,
) -> UnityCatalogResult<(Table, Vec<Column>)> {
    let authority = catalog_authority(state, ctx).await?;
    let table = authority
        .get_table_by_id(table_id)
        .await
        .map_err(map_catalog_error)?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("table not found: {table_id}"),
        })?;
    let columns = authority
        .get_columns(table_id)
        .await
        .map_err(map_catalog_error)?;
    Ok((table, columns))
}

fn scoped_table_object(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    table: &Table,
) -> UnityCatalogResult<ScopedTableObject> {
    if TableFormat::effective(table.format.as_deref()).map_err(|_| {
        UnityCatalogError::BadRequest {
            message: "table_read_unsupported_format".to_string(),
        }
    })? != TableFormat::Parquet
    {
        return Err(UnityCatalogError::BadRequest {
            message: "table_read_unsupported_format".to_string(),
        });
    }
    let Some(configured_root) = state.config.table_read_storage_root.as_deref() else {
        return Err(UnityCatalogError::ServiceUnavailable {
            message: "table_read_storage_mapping_unavailable".to_string(),
        });
    };
    let root = GovernedPath::parse(configured_root).map_err(|_| {
        UnityCatalogError::ServiceUnavailable {
            message: "table_read_storage_mapping_unavailable".to_string(),
        }
    })?;
    let root = root.canonical_uri();
    let root = root.trim_end_matches('/');
    let scoped_root = GovernedPath::parse(&format!(
        "{root}/tenant={}/workspace={}/",
        ctx.tenant, ctx.workspace
    ))
    .map_err(|_| UnityCatalogError::ServiceUnavailable {
        message: "table_read_storage_mapping_unavailable".to_string(),
    })?
    .canonical_uri();

    let raw_location = table
        .location
        .as_deref()
        .map(str::trim)
        .filter(|location| !location.is_empty())
        .ok_or_else(|| UnityCatalogError::BadRequest {
            message: "table_read_location_not_mapped".to_string(),
        })?;
    if raw_location.ends_with('/') {
        return Err(UnityCatalogError::BadRequest {
            message: "table_read_location_not_mapped".to_string(),
        });
    }
    let governed_path = GovernedPath::parse(raw_location)
        .map_err(|_| UnityCatalogError::BadRequest {
            message: "table_read_location_not_mapped".to_string(),
        })?
        .canonical_uri();
    let relative_path = governed_path
        .strip_prefix(&scoped_root)
        .and_then(|suffix| suffix.strip_suffix('/'))
        .filter(|suffix| !suffix.is_empty() && suffix.ends_with(".parquet"))
        .ok_or_else(|| UnityCatalogError::BadRequest {
            message: "table_read_location_not_mapped".to_string(),
        })?;
    ScopedStorage::validate_path(relative_path).map_err(|_| UnityCatalogError::BadRequest {
        message: "table_read_location_not_mapped".to_string(),
    })?;

    Ok(ScopedTableObject {
        relative_path: relative_path.to_string(),
        governed_path,
    })
}

async fn read_bounded_object(
    storage: &ScopedStorage,
    path: &str,
) -> UnityCatalogResult<(ObjectMeta, Bytes)> {
    let metadata = storage
        .head_raw(path)
        .await
        .map_err(|error| table_read_storage_error(&error))?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: "table_read_object_not_found".to_string(),
        })?;
    if metadata.size > MAX_TABLE_READ_BYTES {
        return Err(UnityCatalogError::PayloadTooLarge {
            message: "table_read_object_too_large".to_string(),
        });
    }
    let bytes = storage
        .get_raw(path)
        .await
        .map_err(|error| table_read_storage_error(&error))?;
    let actual_size =
        u64::try_from(bytes.len()).map_err(|_| UnityCatalogError::PayloadTooLarge {
            message: "table_read_object_too_large".to_string(),
        })?;
    if actual_size > MAX_TABLE_READ_BYTES {
        return Err(UnityCatalogError::PayloadTooLarge {
            message: "table_read_object_too_large".to_string(),
        });
    }
    if actual_size != metadata.size {
        return Err(table_read_conflict());
    }
    Ok((metadata, bytes))
}

async fn verify_unchanged_object(
    storage: &ScopedStorage,
    path: &str,
    expected: &ObjectMeta,
) -> UnityCatalogResult<()> {
    let current = storage
        .head_raw(path)
        .await
        .map_err(|error| table_read_storage_error(&error))?;
    if current
        .is_some_and(|current| current.version == expected.version && current.size == expected.size)
    {
        Ok(())
    } else {
        Err(table_read_conflict())
    }
}

fn opaque_version_sha256(metadata: &ObjectMeta) -> UnityCatalogResult<String> {
    if metadata.version.is_empty() {
        return Err(UnityCatalogError::ServiceUnavailable {
            message: "table_read_object_version_unavailable".to_string(),
        });
    }
    Ok(sha256_hex(metadata.version.as_bytes()))
}

fn physical_schema_sha256(bytes: &Bytes) -> UnityCatalogResult<String> {
    preflight_parquet_footer(bytes)?;
    let reader = SerializedFileReader::new(bytes.clone()).map_err(|_| invalid_parquet())?;
    let elements = to_thrift(
        reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .root_schema(),
    )
    .map_err(|_| invalid_parquet())?;
    let element_count = u32::try_from(elements.len()).map_err(|_| invalid_parquet())?;
    if element_count == 0 {
        return Err(invalid_parquet());
    }

    let mut hasher = Sha256::new();
    hasher.update(PHYSICAL_SCHEMA_DOMAIN);
    hasher.update(element_count.to_be_bytes());
    for element in elements {
        let mut encoded = Vec::new();
        {
            let mut protocol = TCompactOutputProtocol::new(&mut encoded);
            element
                .write_to_out_protocol(&mut protocol)
                .map_err(|_| invalid_parquet())?;
        }
        let encoded_len = u32::try_from(encoded.len()).map_err(|_| invalid_parquet())?;
        hasher.update(encoded_len.to_be_bytes());
        hasher.update(encoded);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Rejects metadata that could make Parquet's generated Compact-Thrift reader
/// allocate based on attacker-controlled list lengths before it reaches EOF.
pub(super) fn preflight_parquet_footer(bytes: &Bytes) -> UnityCatalogResult<()> {
    let bytes = bytes.as_ref();
    if bytes.len() < 12 || !bytes.starts_with(PARQUET_MAGIC) || !bytes.ends_with(PARQUET_MAGIC) {
        return Err(invalid_parquet());
    }
    let footer_size = bytes
        .get(bytes.len() - 8..bytes.len() - 4)
        .ok_or_else(invalid_parquet)?;
    let footer_len = u32::from_le_bytes(footer_size.try_into().map_err(|_| invalid_parquet())?);
    let footer_len = usize::try_from(footer_len).map_err(|_| invalid_parquet())?;
    if footer_len > MAX_TABLE_READ_PARQUET_FOOTER_BYTES {
        return Err(invalid_parquet());
    }
    let footer_start = bytes
        .len()
        .checked_sub(8)
        .and_then(|end| end.checked_sub(footer_len))
        .filter(|start| *start >= PARQUET_MAGIC.len())
        .ok_or_else(invalid_parquet)?;
    let metadata = bytes
        .get(footer_start..bytes.len() - 8)
        .ok_or_else(invalid_parquet)?;

    CompactThriftFooterPreflight::new(metadata)
        .validate()
        .map_err(|()| invalid_parquet())
}

/// Structural Compact-Thrift validation for the Parquet footer only.
///
/// This deliberately does not interpret Parquet metadata. It bounds recursive
/// shape and collection sizes before the Parquet decoder materializes vectors.
struct CompactThriftFooterPreflight<'a> {
    input: &'a [u8],
    remaining_container_elements: usize,
}

impl<'a> CompactThriftFooterPreflight<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            remaining_container_elements: MAX_TABLE_READ_PARQUET_THRIFT_TOTAL_CONTAINER_ELEMENTS,
        }
    }

    fn validate(mut self) -> Result<(), ()> {
        self.skip_struct(0)?;
        if self.input.is_empty() {
            Ok(())
        } else {
            Err(())
        }
    }

    fn skip_struct(&mut self, depth: usize) -> Result<(), ()> {
        if depth > MAX_TABLE_READ_PARQUET_THRIFT_DEPTH {
            return Err(());
        }
        loop {
            let header = self.read_byte()?;
            let field_type = header & 0x0f;
            if field_type == COMPACT_THRIFT_STOP {
                return Ok(());
            }
            if header >> 4 == 0 {
                self.read_vlq()?;
            }
            match field_type {
                COMPACT_THRIFT_BOOL_TRUE | COMPACT_THRIFT_BOOL_FALSE => {}
                _ => self.skip_value(field_type, depth)?,
            }
        }
    }

    fn skip_value(&mut self, value_type: u8, depth: usize) -> Result<(), ()> {
        match value_type {
            COMPACT_THRIFT_BOOL_TRUE | COMPACT_THRIFT_BOOL_FALSE => Ok(()),
            COMPACT_THRIFT_BYTE => {
                self.read_byte()?;
                Ok(())
            }
            COMPACT_THRIFT_I16 | COMPACT_THRIFT_I32 | COMPACT_THRIFT_I64 => {
                self.read_vlq()?;
                Ok(())
            }
            COMPACT_THRIFT_DOUBLE => self.advance(8),
            COMPACT_THRIFT_BINARY => {
                let len = self.read_vlq()?;
                let len = usize::try_from(len).map_err(|_| ())?;
                self.advance(len)
            }
            COMPACT_THRIFT_LIST | COMPACT_THRIFT_SET => {
                self.skip_collection(Self::nested_depth(depth)?)
            }
            COMPACT_THRIFT_MAP => self.skip_map(Self::nested_depth(depth)?),
            COMPACT_THRIFT_STRUCT => self.skip_struct(Self::nested_depth(depth)?),
            _ => Err(()),
        }
    }

    fn nested_depth(depth: usize) -> Result<usize, ()> {
        let depth = depth.checked_add(1).ok_or(())?;
        if depth > MAX_TABLE_READ_PARQUET_THRIFT_DEPTH {
            return Err(());
        }
        Ok(depth)
    }

    fn skip_collection(&mut self, depth: usize) -> Result<(), ()> {
        let header = self.read_byte()?;
        let value_type = header & 0x0f;
        if !is_compact_thrift_value_type(value_type) {
            return Err(());
        }
        let count = if header >> 4 == 15 {
            self.read_vlq()?
        } else {
            u64::from(header >> 4)
        };
        let count = bounded_compact_thrift_count(count)?;
        self.reserve_container_elements(count)?;
        for _ in 0..count {
            self.skip_collection_value(value_type, depth)?;
        }
        Ok(())
    }

    fn skip_map(&mut self, depth: usize) -> Result<(), ()> {
        let count = bounded_compact_thrift_count(self.read_vlq()?)?;
        self.reserve_container_elements(count)?;
        if count == 0 {
            return Ok(());
        }
        let types = self.read_byte()?;
        let key_type = types >> 4;
        let value_type = types & 0x0f;
        if !is_compact_thrift_value_type(key_type) || !is_compact_thrift_value_type(value_type) {
            return Err(());
        }
        for _ in 0..count {
            self.skip_collection_value(key_type, depth)?;
            self.skip_collection_value(value_type, depth)?;
        }
        Ok(())
    }

    fn reserve_container_elements(&mut self, count: usize) -> Result<(), ()> {
        self.remaining_container_elements = self
            .remaining_container_elements
            .checked_sub(count)
            .ok_or(())?;
        Ok(())
    }

    fn skip_collection_value(&mut self, value_type: u8, depth: usize) -> Result<(), ()> {
        match value_type {
            COMPACT_THRIFT_BOOL_TRUE | COMPACT_THRIFT_BOOL_FALSE => match self.read_byte()? {
                0 | COMPACT_THRIFT_BOOL_TRUE | COMPACT_THRIFT_BOOL_FALSE => Ok(()),
                _ => Err(()),
            },
            _ => self.skip_value(value_type, depth),
        }
    }

    fn read_vlq(&mut self) -> Result<u64, ()> {
        let mut value = 0_u64;
        for index in 0..10 {
            let byte = self.read_byte()?;
            let chunk = u64::from(byte & 0x7f);
            if index == 9 && (byte & 0x80 != 0 || chunk > 1) {
                return Err(());
            }
            value |= chunk << (index * 7);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(())
    }

    fn read_byte(&mut self) -> Result<u8, ()> {
        let (byte, remaining) = self.input.split_first().ok_or(())?;
        self.input = remaining;
        Ok(*byte)
    }

    fn advance(&mut self, len: usize) -> Result<(), ()> {
        self.input = self.input.get(len..).ok_or(())?;
        Ok(())
    }
}

fn bounded_compact_thrift_count(count: u64) -> Result<usize, ()> {
    if count > u64::try_from(MAX_TABLE_READ_PARQUET_THRIFT_CONTAINER_ITEMS).map_err(|_| ())? {
        return Err(());
    }
    usize::try_from(count).map_err(|_| ())
}

const fn is_compact_thrift_value_type(value_type: u8) -> bool {
    matches!(
        value_type,
        COMPACT_THRIFT_BOOL_TRUE
            | COMPACT_THRIFT_BOOL_FALSE
            | COMPACT_THRIFT_BYTE
            | COMPACT_THRIFT_I16
            | COMPACT_THRIFT_I32
            | COMPACT_THRIFT_I64
            | COMPACT_THRIFT_DOUBLE
            | COMPACT_THRIFT_BINARY
            | COMPACT_THRIFT_LIST
            | COMPACT_THRIFT_SET
            | COMPACT_THRIFT_MAP
            | COMPACT_THRIFT_STRUCT
    )
}

fn table_fingerprint(table: &Table, columns: &[Column]) -> UnityCatalogResult<String> {
    let mut columns = columns.iter().collect::<Vec<_>>();
    columns.sort_by(|left, right| {
        left.ordinal
            .cmp(&right.ordinal)
            .then_with(|| left.id.cmp(&right.id))
    });
    let document = TableFingerprintDocument {
        domain_version: 1,
        table: TableFingerprintTable {
            id: &table.id,
            namespace_id: &table.namespace_id,
            name: &table.name,
            description: table.description.as_deref(),
            location: table.location.as_deref(),
            format: table.format.as_deref(),
            table_type: table.table_type.as_deref(),
            properties: table.properties.as_ref(),
            created_at: table.created_at,
            updated_at: table.updated_at,
        },
        columns: columns
            .into_iter()
            .map(|column| TableFingerprintColumn {
                id: &column.id,
                table_id: &column.table_id,
                name: &column.name,
                data_type: &column.data_type,
                is_nullable: column.is_nullable,
                ordinal: column.ordinal,
                description: column.description.as_deref(),
            })
            .collect(),
    };
    let bytes = serde_json::to_vec(&document).map_err(|_| UnityCatalogError::Internal {
        message: "table_read_fingerprint_unavailable".to_string(),
    })?;
    Ok(sha256_hex(&bytes))
}

fn snapshot_id(
    ctx: &UnityCatalogRequestContext,
    table_id: &str,
    table_fingerprint: &str,
    object_version_sha256: &str,
    content_sha256: &str,
    byte_size: u64,
    physical_schema_sha256: &str,
) -> UnityCatalogResult<String> {
    let document = SnapshotDocument {
        domain_version: 1,
        tenant: &ctx.tenant,
        workspace: &ctx.workspace,
        table_id,
        table_fingerprint,
        object_version_sha256,
        content_sha256,
        byte_size,
        physical_schema_sha256,
    };
    let bytes = serde_json::to_vec(&document).map_err(|_| UnityCatalogError::Internal {
        message: "table_read_snapshot_unavailable".to_string(),
    })?;
    Ok(sha256_hex(&bytes))
}

#[allow(clippy::too_many_arguments)]
fn table_read_response(
    bytes: Bytes,
    ctx: &UnityCatalogRequestContext,
    table: &Table,
    table_fingerprint: &str,
    object_version_sha256: &str,
    content_sha256: &str,
    byte_size: u64,
    physical_schema_sha256: &str,
    snapshot_id: &str,
    permission_watermark: &str,
    audit_event_id: &str,
) -> UnityCatalogResult<Response> {
    let principal = ctx
        .user_id
        .as_deref()
        .ok_or_else(|| UnityCatalogError::Forbidden {
            message: "credential_scope_denied:unauthenticated_principal".to_string(),
        })?;
    let principal_sha256 = sha256_hex(principal.as_bytes());
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apache.parquet"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));

    let byte_size = byte_size.to_string();
    for (name, value) in [
        ("x-arco-table-read-version", TABLE_READ_VERSION),
        ("x-arco-table-read-tenant", ctx.tenant.as_str()),
        ("x-arco-table-read-workspace", ctx.workspace.as_str()),
        (
            "x-arco-table-read-principal-sha256",
            principal_sha256.as_str(),
        ),
        ("x-arco-table-read-table-id", table.id.as_str()),
        ("x-arco-table-read-table-fingerprint", table_fingerprint),
        (
            "x-arco-table-read-object-version-sha256",
            object_version_sha256,
        ),
        ("x-arco-table-read-content-sha256", content_sha256),
        ("x-arco-table-read-byte-size", byte_size.as_str()),
        (
            "x-arco-table-read-physical-schema-sha256",
            physical_schema_sha256,
        ),
        ("x-arco-table-read-snapshot-id", snapshot_id),
        (
            "x-arco-table-read-permission-watermark",
            permission_watermark,
        ),
        ("x-arco-table-read-authorization-audit-id", audit_event_id),
        ("x-arco-table-read-enforcement", TABLE_READ_ENFORCEMENT),
    ] {
        insert_header(&mut response, name, value)?;
    }
    Ok(response)
}

fn table_descriptor_response(
    ctx: &UnityCatalogRequestContext,
    read: &AuthorizedTableRead,
) -> UnityCatalogResult<Response> {
    let mut columns = read.columns.iter().collect::<Vec<_>>();
    columns.sort_by(|left, right| {
        left.ordinal
            .cmp(&right.ordinal)
            .then_with(|| left.id.cmp(&right.id))
    });
    let schema = columns
        .into_iter()
        .map(|column| {
            serde_json::json!({"name":column.name,"type":column.data_type,
                "nullable":column.is_nullable,"meaning":column.description})
        })
        .collect::<Vec<_>>();
    dataset_descriptor_response(
        &read.table,
        &schema,
        &serde_json::json!({"snapshot_id":read.snapshot_id,"sha256":read.content_sha256,
            "byte_size":read.byte_size,"physical_schema_sha256":read.physical_schema_sha256,
            "table_id":read.table.id,"table_fingerprint":read.table_fingerprint,
            "object_version_sha256":read.object_version_sha256,"tenant":ctx.tenant,
            "workspace":ctx.workspace}),
        &serde_json::json!({"object":"one_current_bounded_parquet_object",
            "snapshot":"pin_required_for_repeatable_read","row_filter":"unsupported",
            "column_mask":"unsupported"}),
    )
}

fn dataset_descriptor_response(
    table: &Table,
    schema: &[Value],
    input_identity: &Value,
    limitations: &Value,
) -> UnityCatalogResult<Response> {
    let properties = table.properties.as_ref();
    let property = |name: &str| properties.and_then(|values| values.get(name));
    let loaded_at = owner_timestamp(property("arco.dataset.loaded_at").map(String::as_str));
    let source_data_cutoff =
        owner_timestamp(property("arco.dataset.source_data_cutoff").map(String::as_str));
    let units_available = property("arco.dataset.units")
        .and_then(|value| serde_json::from_str::<Value>(value).ok())
        .filter(Value::is_object);
    let units = units_available
        .clone()
        .unwrap_or_else(|| serde_json::json!({}));
    let mut unavailable_facts = vec!["completeness".to_string()];
    if property("arco.dataset.grain").is_none() {
        unavailable_facts.push("grain".to_string());
    }
    if table.description.is_none() {
        unavailable_facts.push("meaning".to_string());
    }
    if units_available.is_none() {
        unavailable_facts.push("units".to_string());
    }
    if loaded_at.is_none() {
        unavailable_facts.push("loaded_at".to_string());
    }
    if source_data_cutoff.is_none() {
        unavailable_facts.push("source_data_cutoff".to_string());
    }
    let semantic = serde_json::json!({"domain_version":1,
        "snapshot_id":input_identity.get("snapshot_id"),"table_fingerprint":input_identity.get("table_fingerprint"),
        "schema":schema,"grain":property("arco.dataset.grain"),"units":units,
        "meaning":table.description,"loaded_at":loaded_at,
        "source_data_cutoff":source_data_cutoff});
    let semantic_metadata_identity =
        sha256_hex(
            &serde_json::to_vec(&semantic).map_err(|_| UnityCatalogError::Internal {
                message: "table_descriptor_identity_unavailable".to_string(),
            })?,
        );
    let descriptor = serde_json::json!({
        "dataset_id":table.id,
        "schema":schema,
        "grain":property("arco.dataset.grain"),
        "units":units,
        "meaning":table.description,
        "input_identity":input_identity,
        "semantic_metadata_identity":semantic_metadata_identity,
        "loaded_at":loaded_at,
        "source_data_cutoff":source_data_cutoff,
        "completeness":"unknown","unavailable_facts":unavailable_facts,
        "enforcement":{"table_select":"enforced","storage_governance":"enforced",
            "row_filter":"unsupported","column_mask":"unsupported"},
        "limitations":limitations
    });
    json_response(&descriptor)
}

pub(super) fn json_response(value: &impl Serialize) -> UnityCatalogResult<Response> {
    let body = serde_json::to_vec(value).map_err(|_| UnityCatalogError::Internal {
        message: "table_descriptor_response_unavailable".to_string(),
    })?;
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}

fn owner_timestamp(value: Option<&str>) -> Option<&str> {
    value.filter(|value| chrono::DateTime::parse_from_rfc3339(value).is_ok())
}

fn insert_header(response: &mut Response, name: &str, value: &str) -> UnityCatalogResult<()> {
    let name =
        HeaderName::from_bytes(name.as_bytes()).map_err(|_| UnityCatalogError::Internal {
            message: "table_read_response_header_invalid".to_string(),
        })?;
    let value = HeaderValue::from_str(value).map_err(|_| UnityCatalogError::Internal {
        message: "table_read_response_header_invalid".to_string(),
    })?;
    response.headers_mut().insert(name, value);
    Ok(())
}

fn table_read_storage_error(error: &arco_core::Error) -> UnityCatalogError {
    tracing::warn!(internal_error = %error, "redacted table-read storage error");
    UnityCatalogError::ServiceUnavailable {
        message: "table_read_storage_unavailable".to_string(),
    }
}

fn table_read_conflict() -> UnityCatalogError {
    UnityCatalogError::Conflict {
        message: "table_read_object_or_catalog_changed".to_string(),
    }
}

fn invalid_parquet() -> UnityCatalogError {
    UnityCatalogError::BadRequest {
        message: "table_read_invalid_parquet".to_string(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Serialize)]
struct SnapshotDocument<'a> {
    domain_version: u8,
    tenant: &'a str,
    workspace: &'a str,
    table_id: &'a str,
    table_fingerprint: &'a str,
    object_version_sha256: &'a str,
    content_sha256: &'a str,
    byte_size: u64,
    physical_schema_sha256: &'a str,
}

#[derive(Serialize)]
struct TableFingerprintDocument<'a> {
    domain_version: u8,
    table: TableFingerprintTable<'a>,
    columns: Vec<TableFingerprintColumn<'a>>,
}

#[derive(Serialize)]
struct TableFingerprintTable<'a> {
    id: &'a str,
    namespace_id: &'a str,
    name: &'a str,
    description: Option<&'a str>,
    location: Option<&'a str>,
    format: Option<&'a str>,
    table_type: Option<&'a str>,
    properties: Option<&'a BTreeMap<String, String>>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Serialize)]
struct TableFingerprintColumn<'a> {
    id: &'a str,
    table_id: &'a str,
    name: &'a str,
    data_type: &'a str,
    is_nullable: bool,
    ordinal: i32,
    description: Option<&'a str>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    use super::{
        MAX_TABLE_READ_PARQUET_FOOTER_BYTES, MAX_TABLE_READ_PARQUET_THRIFT_CONTAINER_ITEMS,
        MAX_TABLE_READ_PARQUET_THRIFT_DEPTH,
        MAX_TABLE_READ_PARQUET_THRIFT_TOTAL_CONTAINER_ELEMENTS, physical_schema_sha256,
        preflight_parquet_footer,
    };

    #[test]
    fn parquet_footer_preflight_accepts_a_valid_footer() {
        let parquet = valid_parquet();

        assert!(preflight_parquet_footer(&parquet).is_ok());
        assert!(physical_schema_sha256(&parquet).is_ok());
    }

    #[test]
    fn parquet_footer_preflight_rejects_an_oversized_footer_before_decode() {
        let parquet =
            parquet_with_footer_metadata(&vec![0; MAX_TABLE_READ_PARQUET_FOOTER_BYTES + 1]);

        assert!(preflight_parquet_footer(&parquet).is_err());
    }

    #[test]
    fn parquet_footer_preflight_rejects_an_oversized_thrift_container_before_decode() {
        let mut metadata = vec![0x19, 0xfc];
        metadata.extend(compact_vlq(
            u64::try_from(MAX_TABLE_READ_PARQUET_THRIFT_CONTAINER_ITEMS + 1)
                .expect("container cap fits u64"),
        ));
        let parquet = parquet_with_footer_metadata(&metadata);

        assert!(preflight_parquet_footer(&parquet).is_err());
    }

    #[test]
    fn parquet_footer_preflight_rejects_excessive_aggregate_thrift_container_items() {
        let mut metadata = Vec::new();
        for _ in 0..=MAX_TABLE_READ_PARQUET_THRIFT_TOTAL_CONTAINER_ELEMENTS {
            // A one-element list of an empty binary value. Each list is below
            // the per-container cap; their total must still remain bounded.
            metadata.extend([0x19, 0x18, 0x00]);
        }
        metadata.push(0x00);
        let parquet = parquet_with_footer_metadata(&metadata);

        assert!(preflight_parquet_footer(&parquet).is_err());
    }

    #[test]
    fn parquet_footer_preflight_rejects_excessive_thrift_depth_before_decode() {
        let metadata = vec![0x1c; MAX_TABLE_READ_PARQUET_THRIFT_DEPTH + 1];
        let parquet = parquet_with_footer_metadata(&metadata);

        assert!(preflight_parquet_footer(&parquet).is_err());
    }

    #[test]
    fn parquet_footer_preflight_rejects_excessive_depth_through_nested_lists() {
        let mut metadata = vec![0x19; MAX_TABLE_READ_PARQUET_THRIFT_DEPTH + 2];
        metadata.extend([0x18, 0x00, 0x00]);
        let parquet = parquet_with_footer_metadata(&metadata);

        assert!(preflight_parquet_footer(&parquet).is_err());
    }

    fn valid_parquet() -> Bytes {
        let schema = Arc::new(
            parse_message_type("message schema { REQUIRED INT64 revenue; }")
                .expect("valid test schema"),
        );
        let properties = Arc::new(WriterProperties::builder().build());
        let writer = SerializedFileWriter::new(Vec::new(), schema, properties)
            .expect("construct test parquet writer");
        Bytes::from(writer.into_inner().expect("write test parquet footer"))
    }

    fn parquet_with_footer_metadata(metadata: &[u8]) -> Bytes {
        let metadata_len = u32::try_from(metadata.len()).expect("test footer length fits u32");
        let mut bytes = Vec::with_capacity(4 + metadata.len() + 8);
        bytes.extend_from_slice(b"PAR1");
        bytes.extend_from_slice(metadata);
        bytes.extend_from_slice(&metadata_len.to_le_bytes());
        bytes.extend_from_slice(b"PAR1");
        Bytes::from(bytes)
    }

    fn compact_vlq(mut value: u64) -> Vec<u8> {
        let mut encoded = Vec::new();
        loop {
            let mut byte = u8::try_from(value & 0x7f).expect("seven-bit chunk fits u8");
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            encoded.push(byte);
            if value == 0 {
                return encoded;
            }
        }
    }
}

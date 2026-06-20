//! ADR-041 orchestration event-log storage contracts.
//!
//! This module intentionally covers only the L0 inbox bundle slice. It does not
//! promote bundles to L1, publish L2 projections, or change the default callback
//! ingestion path.

use std::{collections::HashSet, io::Cursor, sync::Arc};

use arco_core::{FlowPaths, ScopedStorage, WritePrecondition, WriteResult};
use arrow::{
    array::{ArrayRef, StringArray, UInt32Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties, format::KeyValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// Current ADR-041 L0 inbox bundle schema version.
pub const L0_INBOX_SCHEMA_VERSION: u32 = 1;

/// One accepted orchestration fact inside an ADR-041 L0 inbox bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L0BundleEvent {
    /// Globally stable event identity used for dedupe.
    pub event_id: String,
    /// Tenant scope validated before ingest.
    pub tenant_id: String,
    /// Workspace scope validated before ingest.
    pub workspace_id: String,
    /// Orchestration run scope.
    pub run_id: String,
    /// Task key scoped by tenant, workspace, and run.
    pub task_key: String,
    /// Attempt identity scoped by tenant, workspace, run, and task key.
    pub attempt_id: String,
    /// Event type, for example `TaskStarted`.
    pub event_type: String,
    /// Producer identity used with `producer_seq` for dedupe.
    pub producer_id: String,
    /// Producer-local monotonically assigned sequence.
    pub producer_seq: u64,
    /// Event occurrence timestamp.
    pub occurred_at: DateTime<Utc>,
    /// Event-specific payload encoded as JSON.
    pub payload: Value,
    /// Event schema version.
    pub schema_version: u32,
}

/// Derived metadata for an ADR-041 L0 inbox bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L0BundleMetadata {
    /// Bundle identity used in the object key.
    pub bundle_id: String,
    /// Tenant scope shared by all events.
    pub tenant_id: String,
    /// Workspace scope shared by all events.
    pub workspace_id: String,
    /// Run scope shared by all events.
    pub run_id: String,
    /// Task scope shared by all events.
    pub task_key: String,
    /// Producer scope shared by all events.
    pub producer_id: String,
    /// Attempt scope shared by all events.
    pub attempt_id: String,
    /// Number of events in the bundle.
    pub event_count: u32,
    /// Minimum producer sequence in the bundle.
    pub min_producer_seq: u64,
    /// Maximum producer sequence in the bundle.
    pub max_producer_seq: u64,
    /// Bundle schema version.
    pub schema_version: u32,
    /// Physical bundle format for this slice.
    pub format: String,
    /// Compression marker for this slice.
    pub compression: String,
    /// SHA-256 checksum over the serialized events array.
    pub checksum: String,
    /// Bundle creation timestamp.
    pub created_at: DateTime<Utc>,
}

/// ADR-041 L0 inbox bundle payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct L0InboxBundle {
    /// Bundle metadata.
    pub metadata: L0BundleMetadata,
    /// Accepted events.
    pub events: Vec<L0BundleEvent>,
}

impl L0InboxBundle {
    /// Creates and validates an L0 inbox bundle.
    ///
    /// # Errors
    ///
    /// Returns [`L0InboxError::InvalidBundle`] for malformed bundle data and
    /// [`L0InboxError::Serialization`] if checksum input serialization fails.
    pub fn new(
        bundle_id: impl Into<String>,
        events: Vec<L0BundleEvent>,
        created_at: DateTime<Utc>,
    ) -> Result<Self, L0InboxError> {
        let bundle_id = bundle_id.into();
        require_safe_path_segment("bundle_id", &bundle_id)?;

        let Some(first) = events.first() else {
            return Err(invalid("bundle must contain at least one event"));
        };

        validate_event(first)?;
        let tenant_id = first.tenant_id.clone();
        let workspace_id = first.workspace_id.clone();
        let run_id = first.run_id.clone();
        let task_key = first.task_key.clone();
        let producer_id = first.producer_id.clone();
        let attempt_id = first.attempt_id.clone();

        let mut event_ids = HashSet::new();
        let mut producer_sequences = HashSet::new();
        let mut min_producer_seq = u64::MAX;
        let mut max_producer_seq = 0_u64;

        for event in &events {
            validate_event(event)?;
            if event.tenant_id != tenant_id {
                return Err(invalid("event tenant_id does not match bundle tenant_id"));
            }
            if event.workspace_id != workspace_id {
                return Err(invalid(
                    "event workspace_id does not match bundle workspace_id",
                ));
            }
            if event.run_id != run_id {
                return Err(invalid("event run_id does not match bundle run_id"));
            }
            if event.task_key != task_key {
                return Err(invalid("event task_key does not match bundle task_key"));
            }
            if event.producer_id != producer_id {
                return Err(invalid(
                    "event producer_id does not match bundle producer_id",
                ));
            }
            if event.attempt_id != attempt_id {
                return Err(invalid("event attempt_id does not match bundle attempt_id"));
            }
            if !event_ids.insert(event.event_id.as_str()) {
                return Err(invalid(format!("duplicate event_id {}", event.event_id)));
            }
            if !producer_sequences.insert((event.producer_id.as_str(), event.producer_seq)) {
                return Err(invalid(format!(
                    "duplicate producer_id + producer_seq {}:{}",
                    event.producer_id, event.producer_seq
                )));
            }
            min_producer_seq = min_producer_seq.min(event.producer_seq);
            max_producer_seq = max_producer_seq.max(event.producer_seq);
        }

        let event_count = u32::try_from(events.len())
            .map_err(|_| invalid("bundle event_count does not fit in u32"))?;
        let checksum = checksum_events(&events)?;

        Ok(Self {
            metadata: L0BundleMetadata {
                bundle_id,
                tenant_id,
                workspace_id,
                run_id,
                task_key,
                producer_id,
                attempt_id,
                event_count,
                min_producer_seq,
                max_producer_seq,
                schema_version: L0_INBOX_SCHEMA_VERSION,
                format: "parquet".to_string(),
                compression: "uncompressed".to_string(),
                checksum,
                created_at,
            },
            events,
        })
    }

    /// Returns the scope-relative object path for this bundle.
    #[must_use]
    pub fn path(&self) -> String {
        FlowPaths::orchestration_l0_inbox_bundle_path(
            &self.metadata.run_id,
            &self.metadata.producer_id,
            &self.metadata.attempt_id,
            &self.metadata.bundle_id,
        )
    }
}

/// Result of writing an L0 inbox bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum L0InboxWriteOutcome {
    /// The bundle object was created.
    Created {
        /// Scope-relative bundle path.
        path: String,
        /// Storage version returned by the backend.
        version: String,
    },
    /// The bundle path already existed and was left unchanged.
    AlreadyExists {
        /// Scope-relative bundle path.
        path: String,
        /// Current storage version returned by the backend.
        current_version: Option<String>,
    },
}

/// Writer for ADR-041 L0 inbox bundles.
#[derive(Clone)]
pub struct L0InboxWriter {
    storage: ScopedStorage,
}

impl L0InboxWriter {
    /// Creates an L0 inbox writer.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self { storage }
    }

    /// Writes a bundle with create-if-absent semantics.
    ///
    /// # Errors
    ///
    /// Returns serialization or storage errors. Duplicate bundle objects are
    /// reported as [`L0InboxWriteOutcome::AlreadyExists`] and are not errors.
    pub async fn write_bundle(
        &self,
        bundle: &L0InboxBundle,
    ) -> Result<L0InboxWriteOutcome, L0InboxError> {
        self.validate_storage_scope(bundle)?;
        let path = bundle.path();
        let payload = bundle_to_parquet(bundle)?;
        match self
            .storage
            .put_raw(&path, payload, WritePrecondition::DoesNotExist)
            .await?
        {
            WriteResult::Success { version } => Ok(L0InboxWriteOutcome::Created { path, version }),
            WriteResult::PreconditionFailed { current_version } => {
                Ok(L0InboxWriteOutcome::AlreadyExists {
                    path,
                    current_version: Some(current_version),
                })
            }
        }
    }

    fn validate_storage_scope(&self, bundle: &L0InboxBundle) -> Result<(), L0InboxError> {
        if bundle.metadata.tenant_id != self.storage.tenant_id()
            || bundle.metadata.workspace_id != self.storage.workspace_id()
        {
            return Err(invalid(
                "bundle storage scope mismatch: metadata tenant_id/workspace_id must match ScopedStorage",
            ));
        }
        Ok(())
    }
}

/// L0 inbox bundle error.
#[derive(Debug, thiserror::Error)]
pub enum L0InboxError {
    /// Bundle validation failed.
    #[error("invalid L0 inbox bundle: {message}")]
    InvalidBundle {
        /// Validation failure detail.
        message: String,
    },
    /// Serialization failed.
    #[error("serialization error: {message}")]
    Serialization {
        /// Serialization failure detail.
        message: String,
    },
    /// Storage failed.
    #[error("storage error: {0}")]
    Storage(#[from] arco_core::Error),
}

fn validate_event(event: &L0BundleEvent) -> Result<(), L0InboxError> {
    require_non_empty("event_id", &event.event_id)?;
    require_non_empty("tenant_id", &event.tenant_id)?;
    require_non_empty("workspace_id", &event.workspace_id)?;
    require_safe_path_segment("run_id", &event.run_id)?;
    require_non_empty("task_key", &event.task_key)?;
    require_safe_path_segment("attempt_id", &event.attempt_id)?;
    require_non_empty("event_type", &event.event_type)?;
    require_safe_path_segment("producer_id", &event.producer_id)?;
    if event.producer_seq == 0 {
        return Err(invalid("producer_seq must be greater than zero"));
    }
    if event.schema_version != L0_INBOX_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported schema_version {}; expected {L0_INBOX_SCHEMA_VERSION}",
            event.schema_version
        )));
    }
    Ok(())
}

fn bundle_to_parquet(bundle: &L0InboxBundle) -> Result<Bytes, L0InboxError> {
    let schema = l0_inbox_schema();
    let columns = l0_inbox_columns(bundle)?;
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|e| {
        L0InboxError::Serialization {
            message: format!("build L0 inbox parquet batch: {e}"),
        }
    })?;
    let mut cursor = Cursor::new(Vec::<u8>::new());
    let props = l0_inbox_writer_properties(bundle);

    {
        let mut writer = ArrowWriter::try_new(&mut cursor, schema, Some(props)).map_err(|e| {
            L0InboxError::Serialization {
                message: format!("initialize L0 inbox parquet writer: {e}"),
            }
        })?;
        writer
            .write(&batch)
            .map_err(|e| L0InboxError::Serialization {
                message: format!("write L0 inbox parquet batch: {e}"),
            })?;
        writer.close().map_err(|e| L0InboxError::Serialization {
            message: format!("close L0 inbox parquet writer: {e}"),
        })?;
    }

    Ok(Bytes::from(cursor.into_inner()))
}

fn l0_inbox_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("bundle_id", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("workspace_id", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("task_key", DataType::Utf8, false),
        Field::new("attempt_id", DataType::Utf8, false),
        Field::new("producer_id", DataType::Utf8, false),
        Field::new("event_id", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("producer_seq", DataType::UInt64, false),
        Field::new("occurred_at", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("event_schema_version", DataType::UInt32, false),
        Field::new("bundle_schema_version", DataType::UInt32, false),
        Field::new("bundle_event_count", DataType::UInt32, false),
        Field::new("bundle_min_producer_seq", DataType::UInt64, false),
        Field::new("bundle_max_producer_seq", DataType::UInt64, false),
        Field::new("bundle_format", DataType::Utf8, false),
        Field::new("bundle_compression", DataType::Utf8, false),
        Field::new("bundle_checksum", DataType::Utf8, false),
        Field::new("bundle_created_at", DataType::Utf8, false),
    ]))
}

fn l0_inbox_columns(bundle: &L0InboxBundle) -> Result<Vec<ArrayRef>, L0InboxError> {
    let row_count = bundle.events.len();
    let mut columns = bundle_scope_columns(&bundle.metadata, row_count);
    columns.extend(bundle_event_columns(bundle)?);
    columns.extend(bundle_metadata_columns(&bundle.metadata, row_count));
    Ok(columns)
}

fn bundle_scope_columns(metadata: &L0BundleMetadata, row_count: usize) -> Vec<ArrayRef> {
    vec![
        repeated_string_array(&metadata.bundle_id, row_count),
        repeated_string_array(&metadata.tenant_id, row_count),
        repeated_string_array(&metadata.workspace_id, row_count),
        repeated_string_array(&metadata.run_id, row_count),
        repeated_string_array(&metadata.task_key, row_count),
        repeated_string_array(&metadata.attempt_id, row_count),
        repeated_string_array(&metadata.producer_id, row_count),
    ]
}

fn bundle_event_columns(bundle: &L0InboxBundle) -> Result<Vec<ArrayRef>, L0InboxError> {
    let payloads = bundle
        .events
        .iter()
        .map(|event| {
            serde_json::to_string(&event.payload).map_err(|e| L0InboxError::Serialization {
                message: format!("serialize L0 inbox event payload: {e}"),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let occurred_at = bundle
        .events
        .iter()
        .map(|event| event.occurred_at.to_rfc3339())
        .collect::<Vec<_>>();

    Ok(vec![
        Arc::new(StringArray::from(
            bundle
                .events
                .iter()
                .map(|event| event.event_id.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            bundle
                .events
                .iter()
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            bundle
                .events
                .iter()
                .map(|event| event.producer_seq)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(occurred_at)),
        Arc::new(StringArray::from(payloads)),
        Arc::new(UInt32Array::from(
            bundle
                .events
                .iter()
                .map(|event| event.schema_version)
                .collect::<Vec<_>>(),
        )),
    ])
}

fn bundle_metadata_columns(metadata: &L0BundleMetadata, row_count: usize) -> Vec<ArrayRef> {
    vec![
        Arc::new(UInt32Array::from(vec![metadata.schema_version; row_count])),
        Arc::new(UInt32Array::from(vec![metadata.event_count; row_count])),
        Arc::new(UInt64Array::from(vec![
            metadata.min_producer_seq;
            row_count
        ])),
        Arc::new(UInt64Array::from(vec![
            metadata.max_producer_seq;
            row_count
        ])),
        repeated_string_array(&metadata.format, row_count),
        repeated_string_array(&metadata.compression, row_count),
        repeated_string_array(&metadata.checksum, row_count),
        Arc::new(StringArray::from(vec![
            metadata.created_at.to_rfc3339();
            row_count
        ])),
    ]
}

fn repeated_string_array(value: &str, row_count: usize) -> ArrayRef {
    Arc::new(StringArray::from(vec![value; row_count]))
}

fn l0_inbox_writer_properties(bundle: &L0InboxBundle) -> WriterProperties {
    WriterProperties::builder()
        .set_key_value_metadata(Some(vec![
            KeyValue {
                key: "created_by".to_string(),
                value: Some("arco-flow".to_string()),
            },
            KeyValue {
                key: "arco.l0.bundle_schema_version".to_string(),
                value: Some(bundle.metadata.schema_version.to_string()),
            },
            KeyValue {
                key: "arco.l0.bundle_format".to_string(),
                value: Some(bundle.metadata.format.clone()),
            },
            KeyValue {
                key: "arco.l0.bundle_compression".to_string(),
                value: Some(bundle.metadata.compression.clone()),
            },
        ]))
        .build()
}

fn require_safe_path_segment(field: &str, value: &str) -> Result<(), L0InboxError> {
    require_non_empty(field, value)?;
    if value == "." || value == ".." || value.contains(['/', '\\', '\0']) {
        return Err(invalid(format!(
            "{field} must be a safe object-key segment"
        )));
    }
    Ok(())
}

fn require_non_empty(field: &str, value: &str) -> Result<(), L0InboxError> {
    if value.is_empty() {
        return Err(invalid(format!("{field} must not be empty")));
    }
    Ok(())
}

fn checksum_events(events: &[L0BundleEvent]) -> Result<String, L0InboxError> {
    let bytes = serde_json::to_vec(events).map_err(|e| L0InboxError::Serialization {
        message: format!("serialize L0 inbox events for checksum: {e}"),
    })?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn invalid(message: impl Into<String>) -> L0InboxError {
    L0InboxError::InvalidBundle {
        message: message.into(),
    }
}

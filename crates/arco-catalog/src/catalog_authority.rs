//! Catalog authority selection and the `control/v1` catalog domain adapter.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use tracing::warn;
use ulid::Ulid;
use uuid::Uuid;

use arco_core::{ScopedStorage, TableFormat, WritePrecondition, WriteResult};

use crate::error::{CatalogError, Result};
use crate::idempotency::validate_uuidv7;
use crate::parquet_util::{CatalogRecord, ColumnRecord, NamespaceRecord, TableRecord};
use crate::state::CatalogState;
use crate::state_store::projection_outbox_acks::{
    PROJECTION_OUTBOX_ACK_DOMAIN, ProjectionMaterializationStatus, ProjectionOutboxAckWriter,
    ProjectionOutboxDrainReport, ProjectionOutboxHandler, ProjectionOutboxProcessDisposition,
    ProjectionOutboxWorker,
};
use crate::state_store::{
    ArcoStateReader, ArcoStateTxn, ControlMvpStateStore, ControlMvpTxn, ProjectionIntentV1,
    ScanContinuation, ScanContinuationKey, ScanRequest, StateScope, StateToken, TxnOptions,
};
use crate::tier1_snapshot;
use crate::write_options::WriteOptions;
use crate::writer::{
    Catalog, CatalogPatch, CatalogWriter, Column, ColumnDefinition, RegisterTableInSchemaRequest,
    Schema, SchemaPatch, Table, TablePatch,
};
use crate::{CatalogReader, SyncCompactor};

const OBJECT_KEY_TAG: u8 = 1;
const NAME_INDEX_KEY_TAG: u8 = 2;
const IDEMPOTENCY_KEY_TAG: u8 = 3;
const AUDIT_KEY_TAG: u8 = 4;
const CATALOG_KIND: u8 = 1;
const SCHEMA_KIND: u8 = 2;
const TABLE_KIND: u8 = 3;
const COLUMN_KIND: u8 = 4;
const RECORD_VERSION: u32 = 1;
const RETRY_BUDGET: Duration = Duration::from_millis(1_500);
/// Durable outbox kind and single-consumer identity for the catalog Parquet
/// projection worker.
pub const CATALOG_PARQUET_PROJECTION_CONSUMER_ID: &str = "catalog-parquet-v1";

/// Non-blocking wake-up seam invoked after a catalog authority commit.
///
/// Implementations may enqueue an external notification or schedule a local
/// anti-entropy drain. Returning an error never changes the already-committed
/// catalog mutation; the durable intent remains the recovery source.
pub trait CatalogProjectionNotifier: Send + Sync {
    /// Best-effort notification for one durably committed intent.
    ///
    /// # Errors
    ///
    /// Returns an enqueue or local scheduling error. The caller records and
    /// ignores it because the authority mutation is already committed.
    fn notify(&self, intent: &ProjectionIntentV1) -> Result<()>;
}

#[derive(Clone)]
struct CatalogProjectionDrainNotifier {
    storage: ScopedStorage,
}

impl CatalogProjectionNotifier for CatalogProjectionDrainNotifier {
    fn notify(&self, intent: &ProjectionIntentV1) -> Result<()> {
        let materializer = CatalogProjectionMaterializer::new(self.storage.clone())?;
        let intent_id = intent.intent_id().to_string();
        let projection_kind = intent.projection_kind().to_string();
        tokio::spawn(async move {
            if let Err(error) = materializer.drain_once().await {
                warn!(
                    intent_id,
                    projection_kind,
                    error = %error,
                    "best-effort catalog projection wake failed; durable anti-entropy will retry"
                );
            }
        });
        Ok(())
    }
}

/// Catalog authority implementation selected for one exact metastore root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogAuthorityKind {
    /// Existing ledger plus synchronous compactor authority.
    Legacy,
    /// `control/v1` object-store state authority.
    ControlV1,
}

/// Exact tenant/workspace catalog-authority binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogAuthorityBinding {
    tenant_id: String,
    workspace_id: String,
    kind: CatalogAuthorityKind,
}

impl CatalogAuthorityBinding {
    /// Creates an exact binding to the legacy catalog authority.
    #[must_use]
    pub fn legacy(tenant_id: impl Into<String>, workspace_id: impl Into<String>) -> Self {
        Self::new(tenant_id, workspace_id, CatalogAuthorityKind::Legacy)
    }

    /// Creates an exact binding to the `control/v1` catalog authority.
    #[must_use]
    pub fn control_v1(tenant_id: impl Into<String>, workspace_id: impl Into<String>) -> Self {
        Self::new(tenant_id, workspace_id, CatalogAuthorityKind::ControlV1)
    }

    fn new(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        kind: CatalogAuthorityKind,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            workspace_id: workspace_id.into(),
            kind,
        }
    }
}

/// Validated exact-root binding registry.
///
/// Unlisted roots always resolve to legacy authority. The registry deliberately
/// supports no wildcard or prefix forms, so a pilot binding cannot overlap a
/// customer root by construction.
#[derive(Debug, Clone, Default)]
pub struct CatalogAuthorityBindings {
    exact: BTreeMap<(String, String), CatalogAuthorityKind>,
    continuation_key: Option<ScanContinuationKey>,
}

/// Bounded catalog list request shared by native, UC, and Iceberg adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogListRequest {
    max_results: usize,
    page_token: Option<String>,
}

impl CatalogListRequest {
    /// Creates a bounded request.
    ///
    /// # Errors
    ///
    /// Returns a validation error for zero or more than 1,000 results.
    pub fn new(max_results: usize) -> Result<Self> {
        if !(1..=1_000).contains(&max_results) {
            return Err(CatalogError::Validation {
                message: "catalog list max_results must be between 1 and 1000".to_string(),
            });
        }
        Ok(Self {
            max_results,
            page_token: None,
        })
    }

    /// Continues a preceding authority page.
    #[must_use]
    pub fn with_page_token(mut self, page_token: impl Into<String>) -> Self {
        self.page_token = Some(page_token.into());
        self
    }

    /// Returns the validated maximum number of results in this page.
    #[must_use]
    pub const fn max_results(&self) -> usize {
        self.max_results
    }
}

/// One bounded catalog-authority page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogListPage<T> {
    items: Vec<T>,
    next_page_token: Option<String>,
}

impl<T> CatalogListPage<T> {
    /// Returns the selected items.
    #[must_use]
    pub fn items(&self) -> &[T] {
        &self.items
    }

    /// Consumes the page into its selected items.
    #[must_use]
    pub fn into_items(self) -> Vec<T> {
        self.items
    }

    /// Returns the opaque token for the next authority-pinned page.
    #[must_use]
    pub fn next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
}

impl CatalogAuthorityBindings {
    /// Validates and constructs an exact binding registry.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid scope components or duplicate roots.
    pub fn new(bindings: impl IntoIterator<Item = CatalogAuthorityBinding>) -> Result<Self> {
        Self::new_inner(bindings, None)
    }

    /// Constructs bindings with a deployment-stable 32-byte continuation key.
    ///
    /// Production control bindings must use this constructor so sealed cursors
    /// remain valid across replicas and process restarts.
    ///
    /// # Errors
    ///
    /// Returns validation errors for invalid/duplicate bindings, a key other
    /// than 32 bytes, or a supplied key without a control binding.
    pub fn new_with_continuation_key(
        bindings: impl IntoIterator<Item = CatalogAuthorityBinding>,
        continuation_key: &[u8],
    ) -> Result<Self> {
        Self::new_inner(
            bindings,
            Some(ScanContinuationKey::from_bytes(continuation_key)?),
        )
    }

    fn new_inner(
        bindings: impl IntoIterator<Item = CatalogAuthorityBinding>,
        supplied_continuation_key: Option<ScanContinuationKey>,
    ) -> Result<Self> {
        let mut exact = BTreeMap::new();
        for binding in bindings {
            StateScope::new(&binding.tenant_id, &binding.workspace_id, "catalog").validate()?;
            let key = (binding.tenant_id, binding.workspace_id);
            if exact.insert(key.clone(), binding.kind).is_some() {
                return Err(CatalogError::Validation {
                    message: format!(
                        "duplicate catalog authority binding for tenant={} workspace={}",
                        key.0, key.1
                    ),
                });
            }
        }
        let has_control_binding = exact
            .values()
            .any(|kind| *kind == CatalogAuthorityKind::ControlV1);
        let continuation_key = match (has_control_binding, supplied_continuation_key) {
            (true, Some(key)) => Some(key),
            (true, None) => Some(ScanContinuationKey::generate()?),
            (false, Some(_)) => {
                return Err(CatalogError::Validation {
                    message: "scan-continuation key requires an exact control/v1 root binding"
                        .to_string(),
                });
            }
            (false, None) => None,
        };
        Ok(Self {
            exact,
            continuation_key,
        })
    }

    /// Resolves one exact root, defaulting every unlisted root to legacy.
    #[must_use]
    pub fn resolve(&self, tenant_id: &str, workspace_id: &str) -> CatalogAuthorityKind {
        self.exact
            .get(&(tenant_id.to_string(), workspace_id.to_string()))
            .copied()
            .unwrap_or(CatalogAuthorityKind::Legacy)
    }

    fn control_continuation_key(
        &self,
        tenant_id: &str,
        workspace_id: &str,
    ) -> Result<ScanContinuationKey> {
        if self.resolve(tenant_id, workspace_id) != CatalogAuthorityKind::ControlV1 {
            return Err(CatalogError::Validation {
                message: "control catalog authority requires an exact control/v1 root binding"
                    .to_string(),
            });
        }
        self.continuation_key
            .clone()
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "control/v1 root binding has no continuation key".to_string(),
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CatalogRecordV1 {
    version: u32,
    id: String,
    name: String,
    description: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SchemaRecordV1 {
    version: u32,
    id: String,
    catalog_id: String,
    name: String,
    description: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TableRecordV1 {
    version: u32,
    id: String,
    schema_id: String,
    name: String,
    description: Option<String>,
    location: Option<String>,
    format: Option<String>,
    table_type: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ColumnRecordV1 {
    version: u32,
    id: String,
    table_id: String,
    name: String,
    data_type: String,
    is_nullable: bool,
    ordinal: i32,
    description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", content = "value")]
enum MutationResponseV1 {
    Catalog(CatalogRecordV1),
    Schema(SchemaRecordV1),
    Table(TableRecordV1),
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IdempotencyReceiptV1 {
    version: u32,
    operation_family: String,
    request_digest: String,
    response: MutationResponseV1,
    authority_manifest_id: String,
    logical_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CatalogAuditRecordV1 {
    version: u32,
    operation_id: String,
    operation_family: String,
    request_digest: String,
    actor: String,
    occurred_at_ms: i64,
    authority_manifest_id: String,
    logical_sequence: u64,
}

#[derive(Debug, Clone)]
struct FrozenMutation {
    operation_id: String,
    family: &'static str,
    digest: String,
    actor: String,
    occurred_at_ms: i64,
    receipt_key: Vec<u8>,
    request_id: Option<String>,
    command: FrozenCommand,
}

#[derive(Debug, Clone)]
enum FrozenCommand {
    CreateCatalog {
        id: String,
        name: String,
        description: Option<String>,
        properties: Option<BTreeMap<String, String>>,
        storage_root: Option<String>,
    },
    PatchCatalog {
        name: String,
        patch: CatalogPatch,
    },
    DeleteCatalog {
        name: String,
        force: bool,
    },
    CreateSchema {
        id: String,
        catalog: String,
        name: String,
        description: Option<String>,
        properties: Option<BTreeMap<String, String>>,
        storage_root: Option<String>,
    },
    PatchSchema {
        catalog: String,
        name: String,
        patch: SchemaPatch,
    },
    DeleteSchema {
        catalog: String,
        name: String,
        force: bool,
    },
    RegisterTable {
        id: String,
        column_ids: Vec<String>,
        catalog: String,
        schema: String,
        request: RegisterTableInSchemaRequest,
    },
    UpdateTable {
        catalog: String,
        schema: String,
        name: String,
        patch: TablePatch,
    },
    RenameTable {
        catalog: String,
        schema: String,
        name: String,
        new_name: String,
    },
    DropTable {
        catalog: String,
        schema: String,
        name: String,
    },
}

/// `control/v1` implementation of catalog authority for one exact root.
#[derive(Clone)]
pub struct ControlCatalogAuthority {
    store: ControlMvpStateStore,
    continuation_key: ScanContinuationKey,
    projection_notifier: Arc<dyn CatalogProjectionNotifier>,
}

/// Restart-safe anti-entropy worker for catalog Parquet projections.
pub struct CatalogProjectionMaterializer {
    storage: ScopedStorage,
    source: ControlMvpStateStore,
    status: ProjectionOutboxAckWriter,
    explicit_epoch: Option<u64>,
}

impl CatalogProjectionMaterializer {
    /// Creates a materializer for the exact scoped catalog authority root.
    ///
    /// # Errors
    ///
    /// Returns scope or state-store construction errors.
    pub fn new(storage: ScopedStorage) -> Result<Self> {
        let source_scope = StateScope::new(storage.tenant_id(), storage.workspace_id(), "catalog");
        let ack_scope = StateScope::new(
            storage.tenant_id(),
            storage.workspace_id(),
            PROJECTION_OUTBOX_ACK_DOMAIN,
        );
        Ok(Self {
            source: ControlMvpStateStore::new(storage.clone(), source_scope)?,
            status: ProjectionOutboxAckWriter::new(storage.clone(), ack_scope)?,
            storage,
            explicit_epoch: None,
        })
    }

    /// Pins source/ack maintenance commits to an explicit published epoch.
    ///
    /// # Errors
    ///
    /// Returns validation errors for unsupported epochs.
    pub fn with_writer_epoch(mut self, writer_epoch: u64) -> Result<Self> {
        self.status = self.status.with_writer_epoch(writer_epoch)?;
        self.explicit_epoch = Some(writer_epoch);
        Ok(self)
    }

    /// Runs one anti-entropy drain. Each pending intent is materialized and
    /// durably marked successful before the generic worker acknowledges it.
    ///
    /// # Errors
    ///
    /// Returns materialization, status, acknowledgement, or authority errors.
    pub async fn drain_once(&self) -> Result<ProjectionOutboxDrainReport> {
        let worker = ProjectionOutboxWorker::new(
            self.storage.clone(),
            "catalog",
            CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
        )?;
        let worker = match self.explicit_epoch {
            Some(epoch) => worker.with_writer_epoch(epoch)?,
            None => worker,
        };
        worker.drain_fixed_consumer(self).await
    }

    /// Reads durable materialization status.
    ///
    /// # Errors
    ///
    /// Returns storage or corrupt-record errors.
    pub async fn status(&self) -> Result<Option<ProjectionMaterializationStatus>> {
        self.status
            .projection_status(CATALOG_PARQUET_PROJECTION_CONSUMER_ID)
            .await
    }

    async fn materialize(
        &self,
        intent: &ProjectionIntentV1,
        record: &crate::state_store::ControlMvpProjectionOutboxRecord,
    ) -> Result<String> {
        if intent.projection_kind() != CATALOG_PARQUET_PROJECTION_CONSUMER_ID
            || intent.source_scope().domain() != "catalog"
        {
            return Err(CatalogError::Validation {
                message: "catalog materializer received an incompatible projection intent"
                    .to_string(),
            });
        }
        let token = self
            .source
            .resolve_projection_source(record, intent)
            .await?;
        let reader = self.source.read_at(token).await?;
        let state = catalog_state_from_reader(reader.as_ref()).await?;
        let directory = format!(
            "control/v1/projections/catalog-parquet/{:020}-{}/",
            intent.source_logical_sequence(),
            intent.source_authority_manifest_id()
        );
        let snapshot = tier1_snapshot::write_catalog_snapshot_in_dir(
            &self.storage,
            intent.source_logical_sequence(),
            &directory,
            &state,
        )
        .await?;
        let manifest_path = format!("{directory}manifest.json");
        let manifest_bytes = Bytes::from(serde_json::to_vec(&snapshot).map_err(|error| {
            CatalogError::Serialization {
                message: format!("catalog projection manifest encode failed: {error}"),
            }
        })?);
        match self
            .storage
            .put_raw(
                &manifest_path,
                manifest_bytes.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await?
        {
            WriteResult::Success { .. } => {}
            WriteResult::PreconditionFailed { .. } => {
                if self.storage.get_raw(&manifest_path).await? != manifest_bytes {
                    return Err(CatalogError::PreconditionFailed {
                        message: "catalog projection manifest already exists with different bytes"
                            .to_string(),
                    });
                }
            }
        }
        Ok(manifest_path)
    }
}

#[async_trait::async_trait]
impl ProjectionOutboxHandler for CatalogProjectionMaterializer {
    async fn process(
        &self,
        record: &crate::state_store::ControlMvpProjectionOutboxRecord,
    ) -> Result<ProjectionOutboxProcessDisposition> {
        let at_ms = Utc::now().timestamp_millis();
        let source_sequence =
            record
                .origin_sequence()
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "committed catalog projection intent is missing its origin sequence"
                        .to_string(),
                })?;
        let intent: ProjectionIntentV1 =
            if let Ok(intent) = serde_json::from_slice(record.payload()) {
                intent
            } else {
                self.status
                    .record_projection_quarantine(
                        CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
                        source_sequence,
                        record.record_id(),
                        "INVALID_PROJECTION_INTENT",
                        at_ms,
                    )
                    .await?;
                return Ok(ProjectionOutboxProcessDisposition::Quarantined);
            };
        match self.materialize(&intent, record).await {
            Ok(manifest_path) => {
                self.status
                    .record_projection_success(
                        CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
                        intent.source_logical_sequence(),
                        &manifest_path,
                        at_ms,
                    )
                    .await?;
                Ok(ProjectionOutboxProcessDisposition::Materialized)
            }
            Err(error) => {
                let retryable = matches!(
                    error,
                    CatalogError::Storage { .. }
                        | CatalogError::CasFailed { .. }
                        | CatalogError::MaintenanceBackpressure { .. }
                        | CatalogError::AmbiguousAuthorityOutcome { .. }
                );
                if retryable {
                    self.status
                        .record_projection_failure(
                            CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
                            intent.source_logical_sequence(),
                            "CATALOG_PROJECTION_FAILED",
                            true,
                            at_ms,
                        )
                        .await?;
                    Err(error)
                } else {
                    self.status
                        .record_projection_quarantine(
                            CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
                            source_sequence,
                            record.record_id(),
                            "INCOMPATIBLE_PROJECTION_INTENT",
                            at_ms,
                        )
                        .await?;
                    Ok(ProjectionOutboxProcessDisposition::Quarantined)
                }
            }
        }
    }
}

/// One deep catalog interface selected before protocol route handling.
///
/// Every native, UC, and Iceberg catalog-facing handler can use this type
/// without learning which durable authority owns the exact root.
#[derive(Clone)]
pub enum CatalogAuthority {
    /// Existing catalog snapshots plus synchronous compaction.
    Legacy {
        /// Mutation facade for the legacy authority.
        writer: Arc<CatalogWriter>,
        /// Snapshot reader for the legacy authority.
        reader: Arc<CatalogReader>,
    },
    /// `control/v1` authority and asynchronous projections.
    ControlV1(Box<ControlCatalogAuthority>),
}

impl ControlCatalogAuthority {
    /// Creates a catalog authority bound to one exact `catalog` state scope.
    ///
    /// # Errors
    ///
    /// Returns an error when scope/storage do not match or the domain is not `catalog`.
    pub fn new(storage: ScopedStorage, scope: StateScope) -> Result<Self> {
        Self::new_with_continuation_key(storage, scope, ScanContinuationKey::generate()?)
    }

    fn new_with_continuation_key(
        storage: ScopedStorage,
        scope: StateScope,
        continuation_key: ScanContinuationKey,
    ) -> Result<Self> {
        let projection_notifier = Arc::new(CatalogProjectionDrainNotifier {
            storage: storage.clone(),
        });
        if scope.domain() != "catalog" {
            return Err(CatalogError::Validation {
                message: "control catalog authority requires the catalog state domain".to_string(),
            });
        }
        Ok(Self {
            store: ControlMvpStateStore::new(storage, scope)?,
            continuation_key,
            projection_notifier,
        })
    }

    /// Replaces the default process-local wake-up with an injected
    /// cloud-neutral notifier. The notifier remains best effort after commit.
    #[must_use]
    pub fn with_projection_notifier(
        mut self,
        projection_notifier: Arc<dyn CatalogProjectionNotifier>,
    ) -> Self {
        self.projection_notifier = projection_notifier;
        self
    }

    /// Lists one bounded, authority-pinned catalog page.
    ///
    /// # Errors
    ///
    /// Returns validation, corruption, or authority-read errors.
    pub async fn list_catalogs_page(
        &self,
        request: CatalogListRequest,
    ) -> Result<CatalogListPage<Catalog>> {
        let continuation = self.decode_scan_continuation(&request, None)?;
        let page = self
            .scan_name_index_page(name_index_prefix(CATALOG_KIND, None), request, continuation)
            .await?;
        let reader = self.reader_for_page(&page).await?;
        let mut items = Vec::with_capacity(page.entries().len());
        for entry in page.entries() {
            let id = decode_index_id(entry.value().bytes())?;
            let bytes = reader
                .get(&object_key(CATALOG_KIND, &id))
                .await?
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: format!("catalog name index points to missing object {id}"),
                })?;
            items.push(Catalog::from(decode_catalog(&bytes)?));
        }
        Ok(CatalogListPage {
            items,
            next_page_token: encode_scan_page_token(&page, &self.continuation_key, None)?,
        })
    }

    /// Lists one bounded, authority-pinned schema page.
    ///
    /// # Errors
    ///
    /// Returns missing-parent, validation, corruption, or authority-read errors.
    pub async fn list_schemas_page(
        &self,
        catalog: &str,
        request: CatalogListRequest,
    ) -> Result<CatalogListPage<Schema>> {
        self.list_schemas_page_bound(catalog, None, None, request)
            .await
    }

    async fn list_schemas_page_bound(
        &self,
        catalog: &str,
        required_schema: Option<&str>,
        query_binding: Option<&[u8]>,
        request: CatalogListRequest,
    ) -> Result<CatalogListPage<Schema>> {
        let continuation = self.decode_scan_continuation(&request, query_binding)?;
        let catalog_record = if let Some(continuation) = continuation.as_ref() {
            let reader = self
                .store
                .read_at(continuation.observed_token().clone())
                .await?;
            let catalog_record = get_catalog_from_reader(reader.as_ref(), catalog).await?;
            if let Some(parent) = required_schema {
                get_schema_from_reader(reader.as_ref(), catalog, parent)
                    .await?
                    .ok_or_else(|| CatalogError::NotFound {
                        entity: "schema".to_string(),
                        name: format!("{catalog}.{parent}"),
                    })?;
            }
            catalog_record
        } else {
            if let Some(parent) = required_schema {
                self.get_schema(catalog, parent)
                    .await?
                    .ok_or_else(|| CatalogError::NotFound {
                        entity: "schema".to_string(),
                        name: format!("{catalog}.{parent}"),
                    })?;
            }
            self.get_catalog(catalog).await?
        }
        .ok_or_else(|| CatalogError::NotFound {
            entity: "catalog".to_string(),
            name: catalog.to_string(),
        })?;
        let page = self
            .scan_name_index_page(
                name_index_prefix(SCHEMA_KIND, Some(&catalog_record.id)),
                request,
                continuation,
            )
            .await?;
        let reader = self.reader_for_page(&page).await?;
        let mut items = Vec::with_capacity(page.entries().len());
        for entry in page.entries() {
            let id = decode_index_id(entry.value().bytes())?;
            let bytes = reader
                .get(&object_key(SCHEMA_KIND, &id))
                .await?
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: format!("schema name index points to missing object {id}"),
                })?;
            items.push(Schema::from(decode_schema(&bytes)?));
        }
        Ok(CatalogListPage {
            items,
            next_page_token: encode_scan_page_token(&page, &self.continuation_key, query_binding)?,
        })
    }

    /// Lists an Iceberg namespace page whose retained continuation is bound
    /// to the exact parent filter and separator used to interpret flat schema
    /// names. Parent existence is evaluated at the retained authority cut.
    ///
    /// # Errors
    ///
    /// Returns missing-parent, continuation, corruption, or authority-read
    /// errors.
    pub async fn list_iceberg_namespaces_page(
        &self,
        parent_name: Option<&str>,
        separator: &str,
        request: CatalogListRequest,
    ) -> Result<CatalogListPage<Schema>> {
        let query_binding = iceberg_namespace_query_binding(parent_name, separator);
        self.list_schemas_page_bound("default", parent_name, Some(&query_binding), request)
            .await
    }

    /// Lists one bounded, authority-pinned table page.
    ///
    /// # Errors
    ///
    /// Returns missing-parent, validation, corruption, or authority-read errors.
    pub async fn list_tables_page(
        &self,
        catalog: &str,
        schema: &str,
        request: CatalogListRequest,
    ) -> Result<CatalogListPage<Table>> {
        let continuation = self.decode_scan_continuation(&request, None)?;
        let schema_record = match continuation.as_ref() {
            Some(continuation) => {
                let reader = self
                    .store
                    .read_at(continuation.observed_token().clone())
                    .await?;
                get_schema_from_reader(reader.as_ref(), catalog, schema).await?
            }
            None => self.get_schema(catalog, schema).await?,
        }
        .ok_or_else(|| CatalogError::NotFound {
            entity: "schema".to_string(),
            name: format!("{catalog}.{schema}"),
        })?;
        let page = self
            .scan_name_index_page(
                name_index_prefix(TABLE_KIND, Some(&schema_record.id)),
                request,
                continuation,
            )
            .await?;
        let reader = self.reader_for_page(&page).await?;
        let mut items = Vec::with_capacity(page.entries().len());
        for entry in page.entries() {
            let id = decode_index_id(entry.value().bytes())?;
            let bytes = reader
                .get(&object_key(TABLE_KIND, &id))
                .await?
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: format!("table name index points to missing object {id}"),
                })?;
            items.push(Table::from(decode_table(&bytes)?));
        }
        Ok(CatalogListPage {
            items,
            next_page_token: encode_scan_page_token(&page, &self.continuation_key, None)?,
        })
    }

    async fn scan_name_index_page(
        &self,
        prefix: Vec<u8>,
        request: CatalogListRequest,
        continuation: Option<ScanContinuation>,
    ) -> Result<crate::state_store::ScanPage> {
        let mut scan = ScanRequest::new(prefix).with_limits(
            request.max_results,
            crate::state_store::MAX_SCAN_PAGE_BYTES,
            crate::state_store::MAX_SCAN_PAGE_SEGMENTS,
        );
        if let Some(continuation) = continuation {
            scan = scan.with_token(continuation);
        }
        self.store.scan(scan).await
    }

    fn decode_scan_continuation(
        &self,
        request: &CatalogListRequest,
        query_binding: Option<&[u8]>,
    ) -> Result<Option<ScanContinuation>> {
        request
            .page_token
            .as_deref()
            .map(|token| {
                let continuation = ScanContinuation::decode_opaque(token, &self.continuation_key)?;
                continuation.validate_query_binding(query_binding)?;
                Ok(continuation)
            })
            .transpose()
    }

    async fn reader_for_page(
        &self,
        page: &crate::state_store::ScanPage,
    ) -> Result<Box<dyn ArcoStateReader>> {
        let token = page
            .observed_token()
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "catalog authority page has no observed state token".to_string(),
            })?
            .clone();
        self.store.read_at(token).await
    }

    /// Lists catalogs from the selected authority cut.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt, unavailable, or over-budget authority state.
    pub async fn list_catalogs(&self) -> Result<Vec<Catalog>> {
        let mut catalogs = self
            .scan_objects(CATALOG_KIND)
            .await?
            .into_iter()
            .map(|bytes| decode_catalog(&bytes).map(Catalog::from))
            .collect::<Result<Vec<_>>>()?;
        catalogs.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(catalogs)
    }

    /// Gets a catalog by its exact name.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt or unavailable authority state.
    pub async fn get_catalog(&self, name: &str) -> Result<Option<Catalog>> {
        let Some(id) = self.lookup_name(CATALOG_KIND, None, name).await? else {
            return Ok(None);
        };
        self.load_catalog(&id)
            .await
            .map(|value| value.map(Catalog::from))
    }

    /// Gets a schema by exact catalog and schema names.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt or unavailable authority state.
    pub async fn get_schema(&self, catalog: &str, schema: &str) -> Result<Option<Schema>> {
        let Some(catalog) = self.get_catalog(catalog).await? else {
            return Ok(None);
        };
        let Some(id) = self
            .lookup_name(SCHEMA_KIND, Some(&catalog.id), schema)
            .await?
        else {
            return Ok(None);
        };
        self.load_schema(&id)
            .await
            .map(|value| value.map(Schema::from))
    }

    /// Lists schemas under one catalog stable ID.
    ///
    /// # Errors
    ///
    /// Returns an error for missing catalogs or corrupt, unavailable, or over-budget state.
    pub async fn list_schemas(&self, catalog: &str) -> Result<Vec<Schema>> {
        let catalog = self
            .get_catalog(catalog)
            .await?
            .ok_or_else(|| CatalogError::NotFound {
                entity: "catalog".to_string(),
                name: catalog.to_string(),
            })?;
        let mut schemas = self
            .scan_objects(SCHEMA_KIND)
            .await?
            .into_iter()
            .map(|bytes| decode_schema(&bytes))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|schema| schema.catalog_id == catalog.id)
            .map(Schema::from)
            .collect::<Vec<_>>();
        schemas.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(schemas)
    }

    /// Gets a table by exact catalog, schema, and table names.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt or unavailable authority state.
    pub async fn get_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<Table>> {
        let Some(schema) = self.get_schema(catalog, schema).await? else {
            return Ok(None);
        };
        let Some(id) = self
            .lookup_name(TABLE_KIND, Some(&schema.id), table)
            .await?
        else {
            return Ok(None);
        };
        self.load_table(&id)
            .await
            .map(|value| value.map(Table::from))
    }

    /// Lists tables under one exact catalog/schema pair.
    ///
    /// # Errors
    ///
    /// Returns an error for missing parents or corrupt, unavailable, or over-budget state.
    pub async fn list_tables(&self, catalog: &str, schema: &str) -> Result<Vec<Table>> {
        let schema =
            self.get_schema(catalog, schema)
                .await?
                .ok_or_else(|| CatalogError::NotFound {
                    entity: "schema".to_string(),
                    name: format!("{catalog}.{schema}"),
                })?;
        let mut tables = self
            .scan_objects(TABLE_KIND)
            .await?
            .into_iter()
            .map(|bytes| decode_table(&bytes))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|table| table.schema_id == schema.id)
            .map(Table::from)
            .collect::<Vec<_>>();
        tables.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(tables)
    }

    /// Gets a table by stable ID.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt or unavailable authority state.
    pub async fn get_table_by_id(&self, table_id: &str) -> Result<Option<Table>> {
        self.load_table(table_id)
            .await
            .map(|value| value.map(Table::from))
    }

    /// Lists columns in stable ordinal order for one table ID.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt, unavailable, or over-budget authority state.
    pub async fn get_columns(&self, table_id: &str) -> Result<Vec<Column>> {
        let prefix = column_prefix(table_id);
        let mut columns = self
            .scan_prefix(&prefix)
            .await?
            .into_iter()
            .map(|bytes| decode_column(&bytes).map(Column::from))
            .collect::<Result<Vec<_>>>()?;
        columns.sort_by_key(|column| column.ordinal);
        Ok(columns)
    }

    async fn lookup_name(
        &self,
        kind: u8,
        parent_id: Option<&str>,
        name: &str,
    ) -> Result<Option<String>> {
        self.store
            .get(&name_index_key(kind, parent_id, name))
            .await?
            .map(|bytes| decode_index_id(&bytes))
            .transpose()
    }

    async fn load_catalog(&self, id: &str) -> Result<Option<CatalogRecordV1>> {
        self.store
            .get(&object_key(CATALOG_KIND, id))
            .await?
            .map(|bytes| decode_catalog(&bytes))
            .transpose()
    }

    async fn load_schema(&self, id: &str) -> Result<Option<SchemaRecordV1>> {
        self.store
            .get(&object_key(SCHEMA_KIND, id))
            .await?
            .map(|bytes| decode_schema(&bytes))
            .transpose()
    }

    async fn load_table(&self, id: &str) -> Result<Option<TableRecordV1>> {
        self.store
            .get(&object_key(TABLE_KIND, id))
            .await?
            .map(|bytes| decode_table(&bytes))
            .transpose()
    }

    async fn scan_objects(&self, kind: u8) -> Result<Vec<Bytes>> {
        self.scan_prefix(&object_prefix(kind)).await
    }

    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<Bytes>> {
        let mut values = Vec::new();
        let mut continuation: Option<ScanContinuation> = None;
        loop {
            let mut request = ScanRequest::new(prefix).with_limits(10_000, 4 * 1024 * 1024, 64);
            if let Some(token) = continuation.take() {
                request = request.with_token(token);
            }
            let page = self.store.scan(request).await?;
            values.extend(
                page.entries()
                    .iter()
                    .map(|entry| entry.value().bytes().clone()),
            );
            continuation = page.continuation().cloned();
            if continuation.is_none() {
                return Ok(values);
            }
        }
    }
}

impl CatalogAuthority {
    /// Constructs a legacy authority after the caller has proven it is initialized.
    ///
    /// This read-path constructor performs no durable initialization writes.
    #[must_use]
    pub fn legacy_existing(storage: ScopedStorage, compactor: Arc<dyn SyncCompactor>) -> Self {
        Self::Legacy {
            writer: Arc::new(CatalogWriter::new(storage.clone()).with_sync_compactor(compactor)),
            reader: Arc::new(CatalogReader::new(storage)),
        }
    }

    /// Constructs and initializes the legacy catalog authority.
    ///
    /// # Errors
    ///
    /// Returns an error when legacy authority initialization fails.
    pub async fn legacy(storage: ScopedStorage, compactor: Arc<dyn SyncCompactor>) -> Result<Self> {
        let writer = Arc::new(CatalogWriter::new(storage.clone()).with_sync_compactor(compactor));
        writer.initialize().await?;
        Ok(Self::Legacy {
            writer,
            reader: Arc::new(CatalogReader::new(storage)),
        })
    }

    /// Constructs the `control/v1` catalog authority for one exact root.
    ///
    /// # Errors
    ///
    /// Returns an error when the root and storage scope differ.
    pub fn control_v1(storage: ScopedStorage, scope: StateScope) -> Result<Self> {
        Ok(Self::ControlV1(Box::new(ControlCatalogAuthority::new(
            storage, scope,
        )?)))
    }

    /// Constructs the `control/v1` authority with the server-shared sealed
    /// continuation key from its exact-root registry.
    ///
    /// # Errors
    ///
    /// Returns an error unless the scope is the exact configured control root.
    pub fn control_v1_bound(
        storage: ScopedStorage,
        scope: StateScope,
        bindings: &CatalogAuthorityBindings,
    ) -> Result<Self> {
        let key = bindings.control_continuation_key(scope.tenant_id(), scope.workspace_id())?;
        Ok(Self::ControlV1(Box::new(
            ControlCatalogAuthority::new_with_continuation_key(storage, scope, key)?,
        )))
    }

    /// Returns the selected durable authority kind.
    #[must_use]
    pub const fn kind(&self) -> CatalogAuthorityKind {
        match self {
            Self::Legacy { .. } => CatalogAuthorityKind::Legacy,
            Self::ControlV1(_) => CatalogAuthorityKind::ControlV1,
        }
    }

    /// Lists catalogs from the selected authority.
    ///
    /// # Errors
    ///
    /// Returns authority read errors.
    pub async fn list_catalogs(&self) -> Result<Vec<Catalog>> {
        match self {
            Self::Legacy { reader, .. } => reader.list_catalogs().await,
            Self::ControlV1(authority) => authority.list_catalogs().await,
        }
    }

    /// Lists one bounded catalog page without aggregating control authority.
    ///
    /// # Errors
    ///
    /// Returns validation or authority read errors.
    pub async fn list_catalogs_page(
        &self,
        request: CatalogListRequest,
    ) -> Result<CatalogListPage<Catalog>> {
        match self {
            Self::Legacy { reader, .. } => {
                let items = reader.list_catalogs().await?;
                legacy_catalog_page(items, &request, |item| &item.name)
            }
            Self::ControlV1(authority) => authority.list_catalogs_page(request).await,
        }
    }

    /// Gets one catalog by exact name.
    ///
    /// # Errors
    ///
    /// Returns authority read errors.
    pub async fn get_catalog(&self, name: &str) -> Result<Option<Catalog>> {
        match self {
            Self::Legacy { reader, .. } => reader.get_catalog(name).await,
            Self::ControlV1(authority) => authority.get_catalog(name).await,
        }
    }

    /// Lists schemas in one catalog.
    ///
    /// # Errors
    ///
    /// Returns authority read errors.
    pub async fn list_schemas(&self, catalog: &str) -> Result<Vec<Schema>> {
        match self {
            Self::Legacy { reader, .. } => reader.list_schemas(catalog).await,
            Self::ControlV1(authority) => authority.list_schemas(catalog).await,
        }
    }

    /// Lists one bounded schema page without aggregating control authority.
    ///
    /// # Errors
    ///
    /// Returns validation, missing-parent, or authority read errors.
    pub async fn list_schemas_page(
        &self,
        catalog: &str,
        request: CatalogListRequest,
    ) -> Result<CatalogListPage<Schema>> {
        match self {
            Self::Legacy { reader, .. } => {
                let items = reader.list_schemas(catalog).await?;
                legacy_catalog_page(items, &request, |item| &item.name)
            }
            Self::ControlV1(authority) => authority.list_schemas_page(catalog, request).await,
        }
    }

    /// Gets one schema by exact catalog and schema names.
    ///
    /// # Errors
    ///
    /// Returns authority read errors.
    pub async fn get_schema(&self, catalog: &str, schema: &str) -> Result<Option<Schema>> {
        match self {
            Self::Legacy { reader, .. } => Ok(reader
                .list_schemas(catalog)
                .await?
                .into_iter()
                .find(|candidate| candidate.name == schema)),
            Self::ControlV1(authority) => authority.get_schema(catalog, schema).await,
        }
    }

    /// Lists tables in one exact catalog/schema pair.
    ///
    /// # Errors
    ///
    /// Returns authority read errors.
    pub async fn list_tables(&self, catalog: &str, schema: &str) -> Result<Vec<Table>> {
        match self {
            Self::Legacy { reader, .. } => reader.list_tables_in_schema(catalog, schema).await,
            Self::ControlV1(authority) => authority.list_tables(catalog, schema).await,
        }
    }

    /// Lists one bounded table page without aggregating control authority.
    ///
    /// # Errors
    ///
    /// Returns validation, missing-parent, or authority read errors.
    pub async fn list_tables_page(
        &self,
        catalog: &str,
        schema: &str,
        request: CatalogListRequest,
    ) -> Result<CatalogListPage<Table>> {
        match self {
            Self::Legacy { reader, .. } => {
                let items = reader.list_tables_in_schema(catalog, schema).await?;
                legacy_catalog_page(items, &request, |item| &item.name)
            }
            Self::ControlV1(authority) => {
                authority.list_tables_page(catalog, schema, request).await
            }
        }
    }

    /// Gets one table by exact catalog/schema/table names.
    ///
    /// # Errors
    ///
    /// Returns authority read errors.
    pub async fn get_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<Table>> {
        match self {
            Self::Legacy { reader, .. } => reader.get_table_in_schema(catalog, schema, table).await,
            Self::ControlV1(authority) => authority.get_table(catalog, schema, table).await,
        }
    }

    /// Gets one table by stable ID.
    ///
    /// # Errors
    ///
    /// Returns authority read errors.
    pub async fn get_table_by_id(&self, table_id: &str) -> Result<Option<Table>> {
        match self {
            Self::Legacy { reader, .. } => reader.get_table_by_id(table_id).await,
            Self::ControlV1(authority) => authority.get_table_by_id(table_id).await,
        }
    }

    /// Lists columns for one stable table ID.
    ///
    /// # Errors
    ///
    /// Returns authority read errors.
    pub async fn get_columns(&self, table_id: &str) -> Result<Vec<Column>> {
        match self {
            Self::Legacy { reader, .. } => reader.get_columns(table_id).await,
            Self::ControlV1(authority) => authority.get_columns(table_id).await,
        }
    }

    /// Lists namespaces through the native API's implicit `default` catalog
    /// compatibility contract.
    ///
    /// # Errors
    ///
    /// Returns selected-authority read errors.
    pub async fn list_native_namespaces(&self) -> Result<Vec<Schema>> {
        match self {
            Self::Legacy { reader, .. } => reader.list_namespaces().await,
            Self::ControlV1(authority) => authority.list_schemas("default").await,
        }
    }

    /// Lists one bounded native namespace page.
    ///
    /// # Errors
    ///
    /// Returns validation or selected-authority read errors.
    pub async fn list_native_namespaces_page(
        &self,
        request: CatalogListRequest,
    ) -> Result<CatalogListPage<Schema>> {
        match self {
            Self::Legacy { reader, .. } => {
                legacy_catalog_page(reader.list_namespaces().await?, &request, |item| &item.name)
            }
            Self::ControlV1(authority) => authority.list_schemas_page("default", request).await,
        }
    }

    /// Lists one bounded Iceberg namespace page, binding any continuation to
    /// the complete parent-filter query and resolving that parent at the
    /// retained authority cut.
    ///
    /// Legacy callers retain the protocol's established offset pagination;
    /// this method is intended for an exact control/v1 binding.
    ///
    /// # Errors
    ///
    /// Returns selected-authority, missing-parent, or continuation errors.
    pub async fn list_iceberg_namespaces_page(
        &self,
        parent_name: Option<&str>,
        separator: &str,
        request: CatalogListRequest,
    ) -> Result<CatalogListPage<Schema>> {
        match self {
            Self::Legacy { reader, .. } => {
                legacy_catalog_page(reader.list_namespaces().await?, &request, |item| &item.name)
            }
            Self::ControlV1(authority) => {
                authority
                    .list_iceberg_namespaces_page(parent_name, separator, request)
                    .await
            }
        }
    }

    /// Gets a namespace through the native API's implicit `default` catalog
    /// compatibility contract.
    ///
    /// # Errors
    ///
    /// Returns selected-authority read errors.
    pub async fn get_native_namespace(&self, name: &str) -> Result<Option<Schema>> {
        match self {
            Self::Legacy { reader, .. } => reader.get_namespace(name).await,
            Self::ControlV1(authority) => authority.get_schema("default", name).await,
        }
    }

    /// Lists tables through the native API's implicit `default` catalog
    /// compatibility contract.
    ///
    /// # Errors
    ///
    /// Returns selected-authority read errors.
    pub async fn list_native_tables(&self, namespace: &str) -> Result<Vec<Table>> {
        match self {
            Self::Legacy { reader, .. } => reader.list_tables(namespace).await,
            Self::ControlV1(authority) => authority.list_tables("default", namespace).await,
        }
    }

    /// Lists one bounded native table page.
    ///
    /// # Errors
    ///
    /// Returns validation or selected-authority read errors.
    pub async fn list_native_tables_page(
        &self,
        namespace: &str,
        request: CatalogListRequest,
    ) -> Result<CatalogListPage<Table>> {
        match self {
            Self::Legacy { reader, .. } => {
                legacy_catalog_page(reader.list_tables(namespace).await?, &request, |item| {
                    &item.name
                })
            }
            Self::ControlV1(authority) => {
                authority
                    .list_tables_page("default", namespace, request)
                    .await
            }
        }
    }

    /// Gets a table through the native API's implicit `default` catalog
    /// compatibility contract.
    ///
    /// # Errors
    ///
    /// Returns selected-authority read errors.
    pub async fn get_native_table(&self, namespace: &str, table: &str) -> Result<Option<Table>> {
        match self {
            Self::Legacy { reader, .. } => reader.get_table(namespace, table).await,
            Self::ControlV1(authority) => authority.get_table("default", namespace, table).await,
        }
    }

    /// Creates a catalog including UC metadata.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn create_catalog_with_metadata(
        &self,
        name: &str,
        description: Option<&str>,
        properties: Option<BTreeMap<String, String>>,
        storage_root: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Catalog> {
        match self {
            Self::Legacy { writer, .. } => {
                writer
                    .create_catalog_with_metadata(name, description, properties, storage_root, opts)
                    .await
            }
            Self::ControlV1(authority) => {
                authority
                    .create_catalog_with_metadata(name, description, properties, storage_root, opts)
                    .await
            }
        }
    }

    /// Patches or renames a catalog.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn patch_catalog(
        &self,
        name: &str,
        patch: CatalogPatch,
        opts: WriteOptions,
    ) -> Result<Catalog> {
        match self {
            Self::Legacy { writer, .. } => writer.patch_catalog(name, patch, opts).await,
            Self::ControlV1(authority) => authority.patch_catalog(name, patch, opts).await,
        }
    }

    /// Deletes a catalog with an optional cascade.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn delete_catalog(&self, name: &str, force: bool, opts: WriteOptions) -> Result<()> {
        match self {
            Self::Legacy { writer, .. } => writer.delete_catalog(name, force, opts).await,
            Self::ControlV1(authority) => authority.delete_catalog(name, force, opts).await,
        }
    }

    /// Creates a schema including UC metadata.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_schema_with_metadata(
        &self,
        catalog: &str,
        name: &str,
        description: Option<&str>,
        properties: Option<BTreeMap<String, String>>,
        storage_root: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Schema> {
        match self {
            Self::Legacy { writer, .. } => {
                writer
                    .create_schema_with_metadata(
                        catalog,
                        name,
                        description,
                        properties,
                        storage_root,
                        opts,
                    )
                    .await
            }
            Self::ControlV1(authority) => {
                authority
                    .create_schema_with_metadata(
                        catalog,
                        name,
                        description,
                        properties,
                        storage_root,
                        opts,
                    )
                    .await
            }
        }
    }

    /// Creates a namespace through the native API's implicit `default`
    /// catalog compatibility contract.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn create_native_namespace(
        &self,
        name: &str,
        description: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Schema> {
        validate_write_options_idempotency(&opts)?;
        match self {
            Self::Legacy { writer, reader } => {
                if reader.get_catalog("default").await?.is_none() {
                    let bootstrap_options = WriteOptions {
                        actor: opts.actor.clone(),
                        request_id: opts.request_id.clone(),
                        ..WriteOptions::default()
                    };
                    match writer
                        .create_catalog_with_metadata(
                            "default",
                            None,
                            None,
                            None,
                            bootstrap_options,
                        )
                        .await
                    {
                        Ok(_) | Err(CatalogError::AlreadyExists { .. }) => {}
                        Err(error) => return Err(error),
                    }
                }
                writer
                    .create_schema_with_metadata("default", name, description, None, None, opts)
                    .await
            }
            Self::ControlV1(authority) => {
                authority
                    .create_schema_with_metadata("default", name, description, None, None, opts)
                    .await
            }
        }
    }

    /// Updates a namespace through the native API's implicit `default`
    /// catalog compatibility contract.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn patch_native_namespace(
        &self,
        name: &str,
        patch: SchemaPatch,
        opts: WriteOptions,
    ) -> Result<Schema> {
        match self {
            Self::Legacy { writer, .. } => {
                if patch.new_name.is_some()
                    || patch.properties.is_some()
                    || patch.storage_root.is_some()
                {
                    return Err(CatalogError::UnsupportedOperation {
                        message: "native namespace updates only support description changes"
                            .to_string(),
                    });
                }
                let description = patch.description.flatten();
                writer
                    .update_namespace(name, description.as_deref(), opts)
                    .await
            }
            Self::ControlV1(authority) => {
                authority
                    .patch_schema_in_catalog("default", name, patch, opts)
                    .await
            }
        }
    }

    /// Patches or renames a schema.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn patch_schema(
        &self,
        catalog: &str,
        schema: &str,
        patch: SchemaPatch,
        opts: WriteOptions,
    ) -> Result<Schema> {
        match self {
            Self::Legacy { writer, .. } => {
                writer
                    .patch_schema_in_catalog(catalog, schema, patch, opts)
                    .await
            }
            Self::ControlV1(authority) => {
                authority
                    .patch_schema_in_catalog(catalog, schema, patch, opts)
                    .await
            }
        }
    }

    /// Deletes a schema with an optional cascade.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn delete_schema(
        &self,
        catalog: &str,
        schema: &str,
        force: bool,
        opts: WriteOptions,
    ) -> Result<()> {
        match self {
            Self::Legacy { writer, .. } => {
                writer
                    .delete_schema_in_catalog(catalog, schema, force, opts)
                    .await
            }
            Self::ControlV1(authority) => {
                authority
                    .delete_schema_in_catalog(catalog, schema, force, opts)
                    .await
            }
        }
    }

    /// Deletes a namespace through the native API's implicit `default`
    /// catalog compatibility contract.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn delete_native_namespace(&self, name: &str, opts: WriteOptions) -> Result<()> {
        match self {
            Self::Legacy { writer, .. } => writer.delete_namespace(name, opts).await,
            Self::ControlV1(authority) => {
                authority
                    .delete_schema_in_catalog("default", name, false, opts)
                    .await
            }
        }
    }

    /// Registers a table and its columns.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn register_table(
        &self,
        catalog: &str,
        schema: &str,
        request: RegisterTableInSchemaRequest,
        opts: WriteOptions,
    ) -> Result<Table> {
        match self {
            Self::Legacy { writer, .. } => {
                writer
                    .register_table_in_schema(catalog, schema, request, opts)
                    .await
            }
            Self::ControlV1(authority) => {
                authority
                    .register_table_in_schema(catalog, schema, request, opts)
                    .await
            }
        }
    }

    /// Registers a table through the native API's implicit `default` catalog
    /// compatibility contract.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn register_native_table(
        &self,
        namespace: &str,
        request: RegisterTableInSchemaRequest,
        opts: WriteOptions,
    ) -> Result<Table> {
        validate_write_options_idempotency(&opts)?;
        match self {
            Self::Legacy { writer, reader } => {
                if request.table_type.is_some() {
                    return Err(CatalogError::Validation {
                        message: "native table registration does not accept a UC table type"
                            .to_string(),
                    });
                }
                if reader.get_catalog("default").await?.is_some() {
                    return writer
                        .register_table_in_schema("default", namespace, request, opts)
                        .await;
                }
                writer
                    .register_table(
                        crate::writer::RegisterTableRequest {
                            namespace: namespace.to_string(),
                            name: request.name,
                            description: request.description,
                            location: request.location,
                            format: request.format,
                            columns: request.columns,
                        },
                        opts,
                    )
                    .await
            }
            Self::ControlV1(authority) => {
                authority
                    .register_table_in_schema("default", namespace, request, opts)
                    .await
            }
        }
    }

    /// Updates table metadata.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn update_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        patch: TablePatch,
        opts: WriteOptions,
    ) -> Result<Table> {
        match self {
            Self::Legacy { writer, reader } => {
                writer
                    .update_table_in_schema_transaction(catalog, schema, table, patch, opts)
                    .await?;
                reader
                    .get_table_in_schema(catalog, schema, table)
                    .await?
                    .ok_or_else(|| CatalogError::InvariantViolation {
                        message: "legacy table update published without a readable table"
                            .to_string(),
                    })
            }
            Self::ControlV1(authority) => {
                authority
                    .update_table_in_schema(catalog, schema, table, patch, opts)
                    .await
            }
        }
    }

    /// Updates a table through the native API's implicit `default` catalog
    /// compatibility contract.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn update_native_table(
        &self,
        namespace: &str,
        table: &str,
        patch: TablePatch,
        opts: WriteOptions,
    ) -> Result<Table> {
        match self {
            Self::Legacy { writer, .. } => writer.update_table(namespace, table, patch, opts).await,
            Self::ControlV1(authority) => {
                authority
                    .update_table_in_schema("default", namespace, table, patch, opts)
                    .await
            }
        }
    }

    /// Renames a table while retaining its stable ID.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn rename_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        new_name: &str,
        opts: WriteOptions,
    ) -> Result<Table> {
        match self {
            Self::Legacy { writer, reader } => {
                writer
                    .rename_table_in_schema_transaction(catalog, schema, table, new_name, opts)
                    .await?;
                reader
                    .get_table_in_schema(catalog, schema, new_name)
                    .await?
                    .ok_or_else(|| CatalogError::InvariantViolation {
                        message: "legacy table rename published without a readable table"
                            .to_string(),
                    })
            }
            Self::ControlV1(authority) => {
                authority
                    .rename_table(catalog, schema, table, new_name, opts)
                    .await
            }
        }
    }

    /// Renames a table through the native API's implicit `default` catalog
    /// compatibility contract.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn rename_native_table(
        &self,
        namespace: &str,
        table: &str,
        new_name: &str,
        opts: WriteOptions,
    ) -> Result<Table> {
        match self {
            Self::Legacy { writer, .. } => {
                writer
                    .rename_table(namespace, table, namespace, new_name, opts)
                    .await
            }
            Self::ControlV1(authority) => {
                authority
                    .rename_table("default", namespace, table, new_name, opts)
                    .await
            }
        }
    }

    /// Drops a table and all column records.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn drop_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
        opts: WriteOptions,
    ) -> Result<()> {
        match self {
            Self::Legacy { writer, .. } => writer
                .drop_table_in_schema_transaction(catalog, schema, table, opts)
                .await
                .map(|_| ()),
            Self::ControlV1(authority) => authority.drop_table(catalog, schema, table, opts).await,
        }
    }

    /// Drops a table through the native API's implicit `default` catalog
    /// compatibility contract.
    ///
    /// # Errors
    ///
    /// Returns selected-authority mutation errors.
    pub async fn drop_native_table(
        &self,
        namespace: &str,
        table: &str,
        opts: WriteOptions,
    ) -> Result<()> {
        match self {
            Self::Legacy { writer, .. } => writer.drop_table(namespace, table, opts).await,
            Self::ControlV1(authority) => {
                authority
                    .drop_table("default", namespace, table, opts)
                    .await
            }
        }
    }
}

fn validate_write_options_idempotency(opts: &WriteOptions) -> Result<()> {
    if let Some(key) = opts.idempotency_key.as_ref() {
        validate_uuidv7(key.as_str()).map_err(|error| CatalogError::Validation {
            message: error.to_string(),
        })?;
    }
    Ok(())
}

impl From<CatalogRecordV1> for Catalog {
    fn from(record: CatalogRecordV1) -> Self {
        Self {
            id: record.id,
            name: record.name,
            description: record.description,
            properties: record.properties,
            storage_root: record.storage_root,
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

impl From<SchemaRecordV1> for Schema {
    fn from(record: SchemaRecordV1) -> Self {
        Self {
            id: record.id,
            catalog_id: Some(record.catalog_id),
            name: record.name,
            description: record.description,
            properties: record.properties,
            storage_root: record.storage_root,
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

impl From<TableRecordV1> for Table {
    fn from(record: TableRecordV1) -> Self {
        Self {
            id: record.id,
            namespace_id: record.schema_id,
            name: record.name,
            description: record.description,
            location: record.location,
            format: record.format,
            table_type: record.table_type,
            properties: record.properties,
            created_at: record.created_at,
            updated_at: record.updated_at,
        }
    }
}

impl From<ColumnRecordV1> for Column {
    fn from(record: ColumnRecordV1) -> Self {
        Self {
            id: record.id,
            table_id: record.table_id,
            name: record.name,
            data_type: record.data_type,
            is_nullable: record.is_nullable,
            ordinal: record.ordinal,
            description: record.description,
        }
    }
}

fn response_catalog(response: MutationResponseV1) -> Result<Catalog> {
    match response {
        MutationResponseV1::Catalog(record) => Ok(record.into()),
        _ => Err(response_kind_mismatch("catalog")),
    }
}

fn response_schema(response: MutationResponseV1) -> Result<Schema> {
    match response {
        MutationResponseV1::Schema(record) => Ok(record.into()),
        _ => Err(response_kind_mismatch("schema")),
    }
}

fn response_table(response: MutationResponseV1) -> Result<Table> {
    match response {
        MutationResponseV1::Table(record) => Ok(record.into()),
        _ => Err(response_kind_mismatch("table")),
    }
}

fn response_deleted(response: &MutationResponseV1) -> Result<()> {
    match response {
        MutationResponseV1::Deleted => Ok(()),
        _ => Err(response_kind_mismatch("deleted")),
    }
}

fn response_kind_mismatch(expected: &str) -> CatalogError {
    CatalogError::InvariantViolation {
        message: format!("idempotency receipt response is not a {expected} result"),
    }
}

fn freeze_mutation(
    family: &'static str,
    command: &FrozenCommand,
    opts: WriteOptions,
) -> Result<FrozenMutation> {
    let canonical =
        serde_json::to_vec(&command_value(command)).map_err(|error| serialization_error(&error))?;
    let digest = sha256_hex(&canonical);
    let idempotency_hash = opts
        .idempotency_key
        .as_ref()
        .map(|key| sha256_hex(key.as_str().as_bytes()));
    let operation_id = idempotency_hash.as_ref().map_or_else(
        || format!("op-{}", Ulid::new().to_string().to_ascii_lowercase()),
        |hash| format!("op-{}", &hash[..32]),
    );
    let receipt_identity = idempotency_hash.as_deref().unwrap_or(&operation_id);
    let receipt_key = receipt_key(family, receipt_identity);
    Ok(FrozenMutation {
        operation_id,
        family,
        digest,
        actor: opts.actor.unwrap_or_else(|| "api".to_string()),
        occurred_at_ms: Utc::now().timestamp_millis(),
        receipt_key,
        request_id: opts.request_id,
        command: command.clone(),
    })
}

#[allow(clippy::too_many_lines)]
fn command_value(command: &FrozenCommand) -> serde_json::Value {
    match command {
        FrozenCommand::CreateCatalog {
            name,
            description,
            properties,
            storage_root,
            ..
        } => serde_json::json!({
            "type": "create_catalog",
            "name": name,
            "description": description,
            "properties": properties,
            "storage_root": storage_root,
        }),
        FrozenCommand::PatchCatalog { name, patch } => serde_json::json!({
            "type": "patch_catalog",
            "name": name,
            "description": patch.description,
            "new_name": patch.new_name,
            "properties": patch.properties,
            "storage_root": patch.storage_root,
        }),
        FrozenCommand::DeleteCatalog { name, force } => serde_json::json!({
            "type": "delete_catalog",
            "name": name,
            "force": force,
        }),
        FrozenCommand::CreateSchema {
            catalog,
            name,
            description,
            properties,
            storage_root,
            ..
        } => serde_json::json!({
            "type": "create_schema",
            "catalog": catalog,
            "name": name,
            "description": description,
            "properties": properties,
            "storage_root": storage_root,
        }),
        FrozenCommand::PatchSchema {
            catalog,
            name,
            patch,
        } => serde_json::json!({
            "type": "patch_schema",
            "catalog": catalog,
            "name": name,
            "description": patch.description,
            "new_name": patch.new_name,
            "properties": patch.properties,
            "storage_root": patch.storage_root,
        }),
        FrozenCommand::DeleteSchema {
            catalog,
            name,
            force,
        } => serde_json::json!({
            "type": "delete_schema",
            "catalog": catalog,
            "name": name,
            "force": force,
        }),
        FrozenCommand::RegisterTable {
            catalog,
            schema,
            request,
            ..
        } => serde_json::json!({
            "type": "register_table",
            "catalog": catalog,
            "schema": schema,
            "name": request.name,
            "description": request.description,
            "location": request.location,
            "format": request.format,
            "table_type": request.table_type,
            "properties": request.properties,
            "columns": request.columns.iter().map(|column| serde_json::json!({
                "name": column.name,
                "data_type": column.data_type,
                "is_nullable": column.is_nullable,
                "ordinal": column.ordinal,
                "description": column.description,
            })).collect::<Vec<_>>(),
        }),
        FrozenCommand::UpdateTable {
            catalog,
            schema,
            name,
            patch,
        } => serde_json::json!({
            "type": "update_table",
            "catalog": catalog,
            "schema": schema,
            "name": name,
            "description": patch.description,
            "location": patch.location,
            "format": patch.format,
        }),
        FrozenCommand::RenameTable {
            catalog,
            schema,
            name,
            new_name,
        } => serde_json::json!({
            "type": "rename_table",
            "catalog": catalog,
            "schema": schema,
            "name": name,
            "new_name": new_name,
        }),
        FrozenCommand::DropTable {
            catalog,
            schema,
            name,
        } => serde_json::json!({
            "type": "drop_table",
            "catalog": catalog,
            "schema": schema,
            "name": name,
        }),
    }
}

async fn load_receipt(txn: &mut ControlMvpTxn, key: &[u8]) -> Result<Option<IdempotencyReceiptV1>> {
    txn.get(key)
        .await?
        .map(|value| decode_json(value.bytes(), "catalog idempotency receipt"))
        .transpose()
}

async fn stage_commit_records(
    txn: &mut ControlMvpTxn,
    frozen: &FrozenMutation,
    response: &MutationResponseV1,
    predicted: &StateToken,
) -> Result<()> {
    let receipt = IdempotencyReceiptV1 {
        version: RECORD_VERSION,
        operation_family: frozen.family.to_string(),
        request_digest: frozen.digest.clone(),
        response: response.clone(),
        authority_manifest_id: predicted.authority_manifest_id().to_string(),
        logical_sequence: predicted.logical_sequence(),
    };
    txn.put(
        &frozen.receipt_key,
        encode_json(&receipt, "catalog idempotency receipt")?,
    )
    .await?;
    let audit = CatalogAuditRecordV1 {
        version: RECORD_VERSION,
        operation_id: frozen.operation_id.clone(),
        operation_family: frozen.family.to_string(),
        request_digest: frozen.digest.clone(),
        actor: frozen.actor.clone(),
        occurred_at_ms: frozen.occurred_at_ms,
        authority_manifest_id: predicted.authority_manifest_id().to_string(),
        logical_sequence: predicted.logical_sequence(),
    };
    let audit_bytes = encode_json(&audit, "catalog audit record")?;
    txn.put(&audit_key(&frozen.operation_id), audit_bytes.clone())
        .await?;
    txn.stage_projection_intent(
        frozen.operation_id.clone(),
        CATALOG_PARQUET_PROJECTION_CONSUMER_ID,
        audit_bytes,
    )?;
    Ok(())
}

async fn apply_command(
    txn: &mut ControlMvpTxn,
    command: &FrozenCommand,
) -> Result<MutationResponseV1> {
    match command {
        FrozenCommand::CreateCatalog {
            id,
            name,
            description,
            properties,
            storage_root,
        } => {
            create_catalog(
                txn,
                id,
                name,
                description.clone(),
                properties.clone(),
                storage_root.clone(),
            )
            .await
        }
        FrozenCommand::PatchCatalog { name, patch } => {
            patch_catalog(txn, name, patch.clone()).await
        }
        FrozenCommand::DeleteCatalog { name, force } => delete_catalog(txn, name, *force).await,
        FrozenCommand::CreateSchema {
            id,
            catalog,
            name,
            description,
            properties,
            storage_root,
        } => {
            create_schema(
                txn,
                id,
                catalog,
                name,
                description.clone(),
                properties.clone(),
                storage_root.clone(),
            )
            .await
        }
        FrozenCommand::PatchSchema {
            catalog,
            name,
            patch,
        } => patch_schema(txn, catalog, name, patch.clone()).await,
        FrozenCommand::DeleteSchema {
            catalog,
            name,
            force,
        } => delete_schema(txn, catalog, name, *force).await,
        FrozenCommand::RegisterTable {
            id,
            column_ids,
            catalog,
            schema,
            request,
        } => register_table(txn, id, column_ids, catalog, schema, request.clone()).await,
        FrozenCommand::UpdateTable {
            catalog,
            schema,
            name,
            patch,
        } => update_table(txn, catalog, schema, name, patch.clone()).await,
        FrozenCommand::RenameTable {
            catalog,
            schema,
            name,
            new_name,
        } => rename_table(txn, catalog, schema, name, new_name).await,
        FrozenCommand::DropTable {
            catalog,
            schema,
            name,
        } => drop_table(txn, catalog, schema, name).await,
    }
}

async fn create_catalog(
    txn: &mut ControlMvpTxn,
    id: &str,
    name: &str,
    description: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
) -> Result<MutationResponseV1> {
    validate_name(name, "catalog")?;
    let index_key = name_index_key(CATALOG_KIND, None, name);
    assert_name_available(txn, &index_key, "catalog", name).await?;
    let now = Utc::now().timestamp_millis();
    let record = CatalogRecordV1 {
        version: RECORD_VERSION,
        id: id.to_string(),
        name: name.to_string(),
        description,
        properties,
        storage_root,
        created_at: now,
        updated_at: now,
    };
    txn.put(
        &object_key(CATALOG_KIND, id),
        encode_json(&record, "catalog object")?,
    )
    .await?;
    txn.put(&index_key, Bytes::copy_from_slice(id.as_bytes()))
        .await?;
    Ok(MutationResponseV1::Catalog(record))
}

async fn patch_catalog(
    txn: &mut ControlMvpTxn,
    name: &str,
    patch: CatalogPatch,
) -> Result<MutationResponseV1> {
    let (old_index, mut record) = resolve_catalog(txn, name).await?;
    let next_name = patch
        .new_name
        .clone()
        .unwrap_or_else(|| record.name.clone());
    validate_name(&next_name, "catalog")?;
    if record.name == "default" && next_name != record.name {
        return Err(CatalogError::Validation {
            message: "default catalog cannot be renamed".to_string(),
        });
    }
    if next_name == "default" && record.name != "default" {
        return Err(CatalogError::Validation {
            message: "catalogs cannot be renamed to reserved name 'default'".to_string(),
        });
    }
    if next_name != record.name {
        let next_index = name_index_key(CATALOG_KIND, None, &next_name);
        assert_name_available(txn, &next_index, "catalog", &next_name).await?;
        txn.delete(&old_index).await?;
        txn.put(&next_index, Bytes::copy_from_slice(record.id.as_bytes()))
            .await?;
        record.name = next_name;
    }
    if let Some(description) = patch.description {
        record.description = description;
    }
    if let Some(properties) = patch.properties {
        record.properties = properties;
    }
    if let Some(storage_root) = patch.storage_root {
        record.storage_root = storage_root;
    }
    record.updated_at = Utc::now().timestamp_millis();
    txn.put(
        &object_key(CATALOG_KIND, &record.id),
        encode_json(&record, "catalog object")?,
    )
    .await?;
    Ok(MutationResponseV1::Catalog(record))
}

async fn delete_catalog(
    txn: &mut ControlMvpTxn,
    name: &str,
    force: bool,
) -> Result<MutationResponseV1> {
    if name == "default" {
        return Err(CatalogError::Validation {
            message: "default catalog cannot be deleted".to_string(),
        });
    }
    let (index_key, record) = resolve_catalog(txn, name).await?;
    let schemas = txn_scan_decoded::<SchemaRecordV1>(txn, &object_prefix(SCHEMA_KIND)).await?;
    let children = schemas
        .into_iter()
        .filter(|(_, schema)| schema.catalog_id == record.id)
        .collect::<Vec<_>>();
    if !force && !children.is_empty() {
        return Err(CatalogError::PreconditionFailed {
            message: format!("catalog {name} is not empty"),
        });
    }
    for (schema_key, schema) in children {
        delete_schema_record(txn, schema_key, schema, true).await?;
    }
    txn.delete(&index_key).await?;
    txn.delete(&object_key(CATALOG_KIND, &record.id)).await?;
    Ok(MutationResponseV1::Deleted)
}

async fn create_schema(
    txn: &mut ControlMvpTxn,
    id: &str,
    catalog: &str,
    name: &str,
    description: Option<String>,
    properties: Option<BTreeMap<String, String>>,
    storage_root: Option<String>,
) -> Result<MutationResponseV1> {
    validate_name(name, "schema")?;
    let (_, catalog_record) = resolve_catalog(txn, catalog).await?;
    let index_key = name_index_key(SCHEMA_KIND, Some(&catalog_record.id), name);
    assert_name_available(txn, &index_key, "schema", name).await?;
    let now = Utc::now().timestamp_millis();
    let record = SchemaRecordV1 {
        version: RECORD_VERSION,
        id: id.to_string(),
        catalog_id: catalog_record.id,
        name: name.to_string(),
        description,
        properties,
        storage_root,
        created_at: now,
        updated_at: now,
    };
    txn.put(
        &object_key(SCHEMA_KIND, id),
        encode_json(&record, "schema object")?,
    )
    .await?;
    txn.put(&index_key, Bytes::copy_from_slice(id.as_bytes()))
        .await?;
    Ok(MutationResponseV1::Schema(record))
}

async fn patch_schema(
    txn: &mut ControlMvpTxn,
    catalog: &str,
    name: &str,
    patch: SchemaPatch,
) -> Result<MutationResponseV1> {
    let (_, catalog_record) = resolve_catalog(txn, catalog).await?;
    let (old_index, mut record) = resolve_schema(txn, &catalog_record.id, name).await?;
    let next_name = patch
        .new_name
        .clone()
        .unwrap_or_else(|| record.name.clone());
    validate_name(&next_name, "schema")?;
    if next_name != record.name {
        let next_index = name_index_key(SCHEMA_KIND, Some(&catalog_record.id), &next_name);
        assert_name_available(txn, &next_index, "schema", &next_name).await?;
        txn.delete(&old_index).await?;
        txn.put(&next_index, Bytes::copy_from_slice(record.id.as_bytes()))
            .await?;
        record.name = next_name;
    }
    if let Some(description) = patch.description {
        record.description = description;
    }
    if let Some(properties) = patch.properties {
        record.properties = properties;
    }
    if let Some(storage_root) = patch.storage_root {
        record.storage_root = storage_root;
    }
    record.updated_at = Utc::now().timestamp_millis();
    txn.put(
        &object_key(SCHEMA_KIND, &record.id),
        encode_json(&record, "schema object")?,
    )
    .await?;
    Ok(MutationResponseV1::Schema(record))
}

async fn delete_schema(
    txn: &mut ControlMvpTxn,
    catalog: &str,
    name: &str,
    force: bool,
) -> Result<MutationResponseV1> {
    let (_, catalog_record) = resolve_catalog(txn, catalog).await?;
    let (_index, record) = resolve_schema(txn, &catalog_record.id, name).await?;
    let key = object_key(SCHEMA_KIND, &record.id);
    delete_schema_record(txn, key, record, force).await?;
    Ok(MutationResponseV1::Deleted)
}

#[allow(clippy::too_many_arguments)]
async fn register_table(
    txn: &mut ControlMvpTxn,
    id: &str,
    column_ids: &[String],
    catalog: &str,
    schema: &str,
    mut request: RegisterTableInSchemaRequest,
) -> Result<MutationResponseV1> {
    validate_name(&request.name, "table")?;
    validate_columns(&request.columns, column_ids)?;
    request.format = Some(normalize_new_table_format(request.format.as_deref())?);
    let (_, catalog_record) = resolve_catalog(txn, catalog).await?;
    let (_, schema_record) = resolve_schema(txn, &catalog_record.id, schema).await?;
    let index_key = name_index_key(TABLE_KIND, Some(&schema_record.id), &request.name);
    assert_name_available(txn, &index_key, "table", &request.name).await?;
    let now = Utc::now().timestamp_millis();
    let record = TableRecordV1 {
        version: RECORD_VERSION,
        id: id.to_string(),
        schema_id: schema_record.id,
        name: request.name,
        description: request.description,
        location: request.location,
        format: request.format,
        table_type: request.table_type,
        properties: request.properties,
        created_at: now,
        updated_at: now,
    };
    txn.put(
        &object_key(TABLE_KIND, id),
        encode_json(&record, "table object")?,
    )
    .await?;
    txn.put(&index_key, Bytes::copy_from_slice(id.as_bytes()))
        .await?;
    for (definition, column_id) in request.columns.into_iter().zip(column_ids) {
        let column = ColumnRecordV1 {
            version: RECORD_VERSION,
            id: column_id.clone(),
            table_id: id.to_string(),
            name: definition.name,
            data_type: definition.data_type,
            is_nullable: definition.is_nullable,
            ordinal: definition.ordinal,
            description: definition.description,
        };
        txn.put(
            &column_key(id, column.ordinal, &column.id)?,
            encode_json(&column, "column object")?,
        )
        .await?;
    }
    Ok(MutationResponseV1::Table(record))
}

async fn update_table(
    txn: &mut ControlMvpTxn,
    catalog: &str,
    schema: &str,
    name: &str,
    patch: TablePatch,
) -> Result<MutationResponseV1> {
    let (_, catalog_record) = resolve_catalog(txn, catalog).await?;
    let (_, schema_record) = resolve_schema(txn, &catalog_record.id, schema).await?;
    let (_, mut record) = resolve_table(txn, &schema_record.id, name).await?;
    if let Some(description) = patch.description {
        record.description = description;
    }
    if let Some(location) = patch.location {
        record.location = location;
    }
    if let Some(format) = patch.format {
        record.format = format
            .as_deref()
            .map(TableFormat::normalize)
            .transpose()
            .map_err(CatalogError::from)?;
    }
    record.updated_at = Utc::now().timestamp_millis();
    txn.put(
        &object_key(TABLE_KIND, &record.id),
        encode_json(&record, "table object")?,
    )
    .await?;
    Ok(MutationResponseV1::Table(record))
}

async fn rename_table(
    txn: &mut ControlMvpTxn,
    catalog: &str,
    schema: &str,
    name: &str,
    new_name: &str,
) -> Result<MutationResponseV1> {
    validate_name(new_name, "table")?;
    let (_, catalog_record) = resolve_catalog(txn, catalog).await?;
    let (_, schema_record) = resolve_schema(txn, &catalog_record.id, schema).await?;
    let (old_index, mut record) = resolve_table(txn, &schema_record.id, name).await?;
    if new_name != record.name {
        let next_index = name_index_key(TABLE_KIND, Some(&schema_record.id), new_name);
        assert_name_available(txn, &next_index, "table", new_name).await?;
        txn.delete(&old_index).await?;
        txn.put(&next_index, Bytes::copy_from_slice(record.id.as_bytes()))
            .await?;
        record.name = new_name.to_string();
        record.updated_at = Utc::now().timestamp_millis();
        txn.put(
            &object_key(TABLE_KIND, &record.id),
            encode_json(&record, "table object")?,
        )
        .await?;
    }
    Ok(MutationResponseV1::Table(record))
}

async fn drop_table(
    txn: &mut ControlMvpTxn,
    catalog: &str,
    schema: &str,
    name: &str,
) -> Result<MutationResponseV1> {
    let (_, catalog_record) = resolve_catalog(txn, catalog).await?;
    let (_, schema_record) = resolve_schema(txn, &catalog_record.id, schema).await?;
    let (index_key, record) = resolve_table(txn, &schema_record.id, name).await?;
    delete_table_record(txn, index_key, record).await?;
    Ok(MutationResponseV1::Deleted)
}

async fn delete_schema_record(
    txn: &mut ControlMvpTxn,
    object_key: Vec<u8>,
    schema: SchemaRecordV1,
    force: bool,
) -> Result<()> {
    let tables = txn_scan_decoded::<TableRecordV1>(txn, &object_prefix(TABLE_KIND)).await?;
    let children = tables
        .into_iter()
        .filter(|(_, table)| table.schema_id == schema.id)
        .collect::<Vec<_>>();
    if !force && !children.is_empty() {
        return Err(CatalogError::PreconditionFailed {
            message: format!("schema {} is not empty", schema.name),
        });
    }
    for (_, table) in children {
        let index_key = name_index_key(TABLE_KIND, Some(&schema.id), &table.name);
        delete_table_record(txn, index_key, table).await?;
    }
    txn.delete(&name_index_key(
        SCHEMA_KIND,
        Some(&schema.catalog_id),
        &schema.name,
    ))
    .await?;
    txn.delete(&object_key).await
}

async fn delete_table_record(
    txn: &mut ControlMvpTxn,
    index_key: Vec<u8>,
    table: TableRecordV1,
) -> Result<()> {
    let columns = txn_scan_pairs(txn, &column_prefix(&table.id)).await?;
    for (key, _) in columns {
        txn.delete(&key).await?;
    }
    txn.delete(&index_key).await?;
    txn.delete(&object_key(TABLE_KIND, &table.id)).await
}

async fn resolve_catalog(
    txn: &mut ControlMvpTxn,
    name: &str,
) -> Result<(Vec<u8>, CatalogRecordV1)> {
    let index_key = name_index_key(CATALOG_KIND, None, name);
    let id = require_index(txn, &index_key, "catalog", name).await?;
    let record = require_record(txn, &object_key(CATALOG_KIND, &id), "catalog", name).await?;
    Ok((index_key, record))
}

async fn resolve_schema(
    txn: &mut ControlMvpTxn,
    catalog_id: &str,
    name: &str,
) -> Result<(Vec<u8>, SchemaRecordV1)> {
    let index_key = name_index_key(SCHEMA_KIND, Some(catalog_id), name);
    let id = require_index(txn, &index_key, "schema", name).await?;
    let record = require_record(txn, &object_key(SCHEMA_KIND, &id), "schema", name).await?;
    Ok((index_key, record))
}

async fn resolve_table(
    txn: &mut ControlMvpTxn,
    schema_id: &str,
    name: &str,
) -> Result<(Vec<u8>, TableRecordV1)> {
    let index_key = name_index_key(TABLE_KIND, Some(schema_id), name);
    let id = require_index(txn, &index_key, "table", name).await?;
    let record = require_record(txn, &object_key(TABLE_KIND, &id), "table", name).await?;
    Ok((index_key, record))
}

async fn require_index(
    txn: &mut ControlMvpTxn,
    key: &[u8],
    entity: &str,
    name: &str,
) -> Result<String> {
    txn.get(key)
        .await?
        .map(|value| decode_index_id(value.bytes()))
        .transpose()?
        .ok_or_else(|| CatalogError::NotFound {
            entity: entity.to_string(),
            name: name.to_string(),
        })
}

async fn require_record<T: for<'de> Deserialize<'de>>(
    txn: &mut ControlMvpTxn,
    key: &[u8],
    entity: &str,
    name: &str,
) -> Result<T> {
    txn.get(key)
        .await?
        .map(|value| decode_json(value.bytes(), entity))
        .transpose()?
        .ok_or_else(|| CatalogError::InvariantViolation {
            message: format!("{entity} name index for {name} points to a missing object"),
        })
}

async fn assert_name_available(
    txn: &mut ControlMvpTxn,
    key: &[u8],
    entity: &str,
    name: &str,
) -> Result<()> {
    if txn.get(key).await?.is_some() {
        return Err(CatalogError::AlreadyExists {
            entity: entity.to_string(),
            name: name.to_string(),
        });
    }
    txn.assert_absent(key).await
}

async fn txn_scan_decoded<T: for<'de> Deserialize<'de>>(
    txn: &mut ControlMvpTxn,
    prefix: &[u8],
) -> Result<Vec<(Vec<u8>, T)>> {
    txn_scan_pairs(txn, prefix)
        .await?
        .into_iter()
        .map(|(key, bytes)| {
            decode_json(&bytes, "catalog authority object").map(|value| (key, value))
        })
        .collect()
}

async fn txn_scan_pairs(txn: &mut ControlMvpTxn, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
    let mut pairs = Vec::new();
    let mut continuation: Option<ScanContinuation> = None;
    loop {
        let mut request = ScanRequest::new(prefix).with_limits(10_000, 4 * 1024 * 1024, 64);
        if let Some(token) = continuation.take() {
            request = request.with_token(token);
        }
        let page = txn.scan(request).await?;
        pairs.extend(
            page.entries()
                .iter()
                .map(|entry| (entry.key().to_vec(), entry.value().bytes().clone())),
        );
        continuation = page.continuation().cloned();
        if continuation.is_none() {
            return Ok(pairs);
        }
    }
}

fn validate_name(name: &str, entity: &str) -> Result<()> {
    if name.trim().is_empty() || name.chars().any(char::is_control) || name.len() > 255 {
        return Err(CatalogError::Validation {
            message: format!(
                "{entity} name must be nonblank, at most 255 bytes, and contain no control characters"
            ),
        });
    }
    Ok(())
}

fn validate_columns(columns: &[ColumnDefinition], ids: &[String]) -> Result<()> {
    if columns.len() != ids.len() {
        return Err(CatalogError::InvariantViolation {
            message: "frozen table column IDs do not match the request".to_string(),
        });
    }
    let mut names = BTreeSet::new();
    for (expected, column) in columns.iter().enumerate() {
        validate_name(&column.name, "column")?;
        if column.data_type.trim().is_empty() {
            return Err(CatalogError::Validation {
                message: format!("column {} has an empty data type", column.name),
            });
        }
        let expected = i32::try_from(expected).map_err(|_| CatalogError::Validation {
            message: "table has too many columns".to_string(),
        })?;
        if column.ordinal != expected {
            return Err(CatalogError::Validation {
                message: "column ordinals must be contiguous and start at zero".to_string(),
            });
        }
        if !names.insert(column.name.as_str()) {
            return Err(CatalogError::AlreadyExists {
                entity: "column".to_string(),
                name: column.name.clone(),
            });
        }
    }
    Ok(())
}

fn normalize_new_table_format(format: Option<&str>) -> Result<String> {
    format.map_or_else(
        || Ok(TableFormat::default_for_new_tables().as_str().to_string()),
        |value| TableFormat::normalize(value).map_err(CatalogError::from),
    )
}

async fn catalog_state_from_reader(reader: &dyn ArcoStateReader) -> Result<CatalogState> {
    let catalogs = scan_reader_values(reader, &object_prefix(CATALOG_KIND))
        .await?
        .into_iter()
        .map(|bytes| {
            let catalog = Catalog::from(decode_catalog(&bytes)?);
            CatalogRecord::try_from(&catalog)
        })
        .collect::<Result<Vec<_>>>()?;
    let namespaces = scan_reader_values(reader, &object_prefix(SCHEMA_KIND))
        .await?
        .into_iter()
        .map(|bytes| {
            let schema = Schema::from(decode_schema(&bytes)?);
            NamespaceRecord::try_from(&schema)
        })
        .collect::<Result<Vec<_>>>()?;
    let tables = scan_reader_values(reader, &object_prefix(TABLE_KIND))
        .await?
        .into_iter()
        .map(|bytes| {
            let table = Table::from(decode_table(&bytes)?);
            TableRecord::try_from(&table)
        })
        .collect::<Result<Vec<_>>>()?;
    let columns = scan_reader_values(reader, &object_prefix(COLUMN_KIND))
        .await?
        .into_iter()
        .map(|bytes| decode_column(&bytes).map(|column| ColumnRecord::from(&Column::from(column))))
        .collect::<Result<Vec<_>>>()?;
    Ok(CatalogState {
        catalogs,
        namespaces,
        tables,
        columns,
        commits: Vec::new(),
    })
}

async fn scan_reader_values(reader: &dyn ArcoStateReader, prefix: &[u8]) -> Result<Vec<Bytes>> {
    let mut values = Vec::new();
    let mut continuation = None;
    loop {
        let mut request = ScanRequest::new(prefix).with_limits(
            10_000,
            crate::state_store::MAX_SCAN_PAGE_BYTES,
            crate::state_store::MAX_SCAN_PAGE_SEGMENTS,
        );
        if let Some(token) = continuation.take() {
            request = request.with_token(token);
        }
        let page = reader.scan(request).await?;
        values.extend(
            page.entries()
                .iter()
                .map(|entry| entry.value().bytes().clone()),
        );
        continuation = page.continuation().cloned();
        if continuation.is_none() {
            return Ok(values);
        }
    }
}

fn object_prefix(kind: u8) -> Vec<u8> {
    vec![OBJECT_KEY_TAG, kind]
}

fn object_key(kind: u8, id: &str) -> Vec<u8> {
    let mut key = object_prefix(kind);
    push_component(&mut key, id.as_bytes());
    key
}

fn name_index_key(kind: u8, parent_id: Option<&str>, name: &str) -> Vec<u8> {
    let mut key = name_index_prefix(kind, parent_id);
    push_component(&mut key, name.as_bytes());
    key
}

fn name_index_prefix(kind: u8, parent_id: Option<&str>) -> Vec<u8> {
    let mut key = vec![NAME_INDEX_KEY_TAG, kind];
    push_component(&mut key, parent_id.unwrap_or_default().as_bytes());
    key
}

fn column_prefix(table_id: &str) -> Vec<u8> {
    let mut key = vec![OBJECT_KEY_TAG, COLUMN_KIND];
    push_component(&mut key, table_id.as_bytes());
    key
}

fn column_key(table_id: &str, ordinal: i32, column_id: &str) -> Result<Vec<u8>> {
    let ordinal = u32::try_from(ordinal).map_err(|_| CatalogError::Validation {
        message: "column ordinal cannot be negative".to_string(),
    })?;
    let mut key = column_prefix(table_id);
    key.extend_from_slice(&ordinal.to_be_bytes());
    push_component(&mut key, column_id.as_bytes());
    Ok(key)
}

fn receipt_key(family: &str, idempotency_hash: &str) -> Vec<u8> {
    let mut key = vec![IDEMPOTENCY_KEY_TAG];
    push_component(&mut key, family.as_bytes());
    push_component(&mut key, idempotency_hash.as_bytes());
    key
}

fn audit_key(operation_id: &str) -> Vec<u8> {
    let mut key = vec![AUDIT_KEY_TAG];
    push_component(&mut key, operation_id.as_bytes());
    key
}

fn push_component(key: &mut Vec<u8>, component: &[u8]) {
    for byte in component {
        if *byte == 0 {
            key.extend_from_slice(&[0, 255]);
        } else {
            key.push(*byte);
        }
    }
    key.extend_from_slice(&[0, 0]);
}

fn encode_scan_page_token(
    page: &crate::state_store::ScanPage,
    key: &ScanContinuationKey,
    query_binding: Option<&[u8]>,
) -> Result<Option<String>> {
    page.continuation()
        .cloned()
        .map(|continuation| continuation.bind_query(query_binding).encode_opaque(key))
        .transpose()
}

fn iceberg_namespace_query_binding(parent_name: Option<&str>, separator: &str) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"arco/iceberg/namespace-list/v1\0");
    digest.update(separator.len().to_be_bytes());
    digest.update(separator.as_bytes());
    if let Some(parent_name) = parent_name {
        digest.update([1]);
        digest.update(parent_name.len().to_be_bytes());
        digest.update(parent_name.as_bytes());
    } else {
        digest.update([0]);
    }
    digest.finalize().to_vec()
}

fn legacy_catalog_page<T>(
    mut items: Vec<T>,
    request: &CatalogListRequest,
    key: impl Fn(&T) -> &str,
) -> Result<CatalogListPage<T>> {
    let start = request
        .page_token
        .as_deref()
        .map(|token| {
            token
                .parse::<usize>()
                .map_err(|_| CatalogError::Validation {
                    message: "invalid page_token: expected non-negative integer offset".to_string(),
                })
        })
        .transpose()?
        .unwrap_or(0);
    items.sort_by(|left, right| key(left).cmp(key(right)));
    if start >= items.len() {
        return Ok(CatalogListPage {
            items: Vec::new(),
            next_page_token: None,
        });
    }
    let end = start.saturating_add(request.max_results).min(items.len());
    let next_page_token = (end < items.len()).then(|| end.to_string());
    let selected = items.into_iter().skip(start).take(end - start).collect();
    Ok(CatalogListPage {
        items: selected,
        next_page_token,
    })
}

fn decode_index_id(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(|error| CatalogError::InvariantViolation {
        message: format!("catalog name index contains a non-UTF-8 stable ID: {error}"),
    })
}

async fn lookup_name_from_reader(
    reader: &dyn ArcoStateReader,
    kind: u8,
    parent_id: Option<&str>,
    name: &str,
) -> Result<Option<String>> {
    reader
        .get(&name_index_key(kind, parent_id, name))
        .await?
        .map(|bytes| decode_index_id(&bytes))
        .transpose()
}

async fn get_catalog_from_reader(
    reader: &dyn ArcoStateReader,
    name: &str,
) -> Result<Option<Catalog>> {
    let Some(id) = lookup_name_from_reader(reader, CATALOG_KIND, None, name).await? else {
        return Ok(None);
    };
    reader
        .get(&object_key(CATALOG_KIND, &id))
        .await?
        .map(|bytes| decode_catalog(&bytes).map(Catalog::from))
        .transpose()
}

async fn get_schema_from_reader(
    reader: &dyn ArcoStateReader,
    catalog: &str,
    schema: &str,
) -> Result<Option<Schema>> {
    let Some(catalog) = get_catalog_from_reader(reader, catalog).await? else {
        return Ok(None);
    };
    let Some(id) = lookup_name_from_reader(reader, SCHEMA_KIND, Some(&catalog.id), schema).await?
    else {
        return Ok(None);
    };
    reader
        .get(&object_key(SCHEMA_KIND, &id))
        .await?
        .map(|bytes| decode_schema(&bytes).map(Schema::from))
        .transpose()
}

fn encode_json<T: Serialize>(value: &T, label: &str) -> Result<Bytes> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| CatalogError::Serialization {
            message: format!("failed to encode {label}: {error}"),
        })
}

fn decode_json<T: for<'de> Deserialize<'de>>(bytes: &[u8], label: &str) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|error| CatalogError::InvariantViolation {
        message: format!("failed to decode {label}: {error}"),
    })
}

fn decode_catalog(bytes: &[u8]) -> Result<CatalogRecordV1> {
    let record: CatalogRecordV1 = decode_json(bytes, "catalog object")?;
    validate_record_version(record.version, "catalog")?;
    Ok(record)
}

fn decode_schema(bytes: &[u8]) -> Result<SchemaRecordV1> {
    let record: SchemaRecordV1 = decode_json(bytes, "schema object")?;
    validate_record_version(record.version, "schema")?;
    Ok(record)
}

fn decode_table(bytes: &[u8]) -> Result<TableRecordV1> {
    let record: TableRecordV1 = decode_json(bytes, "table object")?;
    validate_record_version(record.version, "table")?;
    Ok(record)
}

fn decode_column(bytes: &[u8]) -> Result<ColumnRecordV1> {
    let record: ColumnRecordV1 = decode_json(bytes, "column object")?;
    validate_record_version(record.version, "column")?;
    Ok(record)
}

fn validate_record_version(version: u32, entity: &str) -> Result<()> {
    if version == RECORD_VERSION {
        Ok(())
    } else {
        Err(CatalogError::UnsupportedAuthorityFormat {
            message: format!("unsupported control catalog {entity} record version {version}"),
        })
    }
}

fn serialization_error(error: &serde_json::Error) -> CatalogError {
    CatalogError::Serialization {
        message: format!("failed to encode canonical catalog command: {error}"),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn idempotency_conflict() -> CatalogError {
    CatalogError::PreconditionFailed {
        message: "idempotency key was already used for a different catalog request".to_string(),
    }
}

impl ControlCatalogAuthority {
    /// Creates a catalog and its exact name index atomically.
    ///
    /// # Errors
    ///
    /// Returns semantic conflict, validation, capacity, or authority publication errors.
    pub async fn create_catalog(
        &self,
        name: &str,
        description: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Catalog> {
        self.create_catalog_with_metadata(name, description, None, None, opts)
            .await
    }

    /// Creates a catalog including authoritative UC metadata.
    ///
    /// # Errors
    ///
    /// Returns semantic conflict, validation, capacity, or authority publication errors.
    pub async fn create_catalog_with_metadata(
        &self,
        name: &str,
        description: Option<&str>,
        properties: Option<BTreeMap<String, String>>,
        storage_root: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Catalog> {
        let command = FrozenCommand::CreateCatalog {
            id: Uuid::now_v7().to_string(),
            name: name.to_string(),
            description: description.map(str::to_string),
            properties,
            storage_root: storage_root.map(str::to_string),
        };
        let response = self
            .execute(freeze_mutation("create_catalog", &command, opts)?)
            .await?;
        response_catalog(response)
    }

    /// Patches or renames a catalog while retaining its stable ID.
    ///
    /// # Errors
    ///
    /// Returns semantic conflict, validation, capacity, or authority publication errors.
    pub async fn patch_catalog(
        &self,
        name: &str,
        patch: CatalogPatch,
        opts: WriteOptions,
    ) -> Result<Catalog> {
        let command = FrozenCommand::PatchCatalog {
            name: name.to_string(),
            patch,
        };
        let response = self
            .execute(freeze_mutation("patch_catalog", &command, opts)?)
            .await?;
        response_catalog(response)
    }

    /// Deletes a catalog, optionally cascading through schemas, tables, and columns.
    ///
    /// # Errors
    ///
    /// Returns semantic conflict, validation, capacity, or authority publication errors.
    pub async fn delete_catalog(&self, name: &str, force: bool, opts: WriteOptions) -> Result<()> {
        let command = FrozenCommand::DeleteCatalog {
            name: name.to_string(),
            force,
        };
        let response = self
            .execute(freeze_mutation("delete_catalog", &command, opts)?)
            .await?;
        response_deleted(&response)
    }

    /// Creates a schema and its parent-scoped name index atomically.
    ///
    /// # Errors
    ///
    /// Returns semantic conflict, validation, capacity, or authority publication errors.
    pub async fn create_schema(
        &self,
        catalog: &str,
        name: &str,
        description: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Schema> {
        self.create_schema_with_metadata(catalog, name, description, None, None, opts)
            .await
    }

    /// Creates a schema including authoritative UC metadata.
    ///
    /// # Errors
    ///
    /// Returns semantic conflict, validation, capacity, or authority publication errors.
    pub async fn create_schema_with_metadata(
        &self,
        catalog: &str,
        name: &str,
        description: Option<&str>,
        properties: Option<BTreeMap<String, String>>,
        storage_root: Option<&str>,
        opts: WriteOptions,
    ) -> Result<Schema> {
        let command = FrozenCommand::CreateSchema {
            id: Uuid::now_v7().to_string(),
            catalog: catalog.to_string(),
            name: name.to_string(),
            description: description.map(str::to_string),
            properties,
            storage_root: storage_root.map(str::to_string),
        };
        let response = self
            .execute(freeze_mutation("create_schema", &command, opts)?)
            .await?;
        response_schema(response)
    }

    /// Patches or renames a schema while retaining its stable ID.
    ///
    /// # Errors
    ///
    /// Returns semantic conflict, validation, capacity, or authority publication errors.
    pub async fn patch_schema_in_catalog(
        &self,
        catalog: &str,
        name: &str,
        patch: SchemaPatch,
        opts: WriteOptions,
    ) -> Result<Schema> {
        let command = FrozenCommand::PatchSchema {
            catalog: catalog.to_string(),
            name: name.to_string(),
            patch,
        };
        let response = self
            .execute(freeze_mutation("patch_schema", &command, opts)?)
            .await?;
        response_schema(response)
    }

    /// Deletes a schema, optionally cascading through its tables and columns.
    ///
    /// # Errors
    ///
    /// Returns semantic conflict, validation, capacity, or authority publication errors.
    pub async fn delete_schema_in_catalog(
        &self,
        catalog: &str,
        name: &str,
        force: bool,
        opts: WriteOptions,
    ) -> Result<()> {
        let command = FrozenCommand::DeleteSchema {
            catalog: catalog.to_string(),
            name: name.to_string(),
            force,
        };
        let response = self
            .execute(freeze_mutation("delete_schema", &command, opts)?)
            .await?;
        response_deleted(&response)
    }

    /// Registers a table and all ordered column records atomically.
    ///
    /// # Errors
    ///
    /// Returns semantic conflict, validation, capacity, or authority publication errors.
    pub async fn register_table_in_schema(
        &self,
        catalog: &str,
        schema: &str,
        request: RegisterTableInSchemaRequest,
        opts: WriteOptions,
    ) -> Result<Table> {
        let column_ids = request
            .columns
            .iter()
            .map(|_| Uuid::now_v7().to_string())
            .collect();
        let command = FrozenCommand::RegisterTable {
            id: Uuid::now_v7().to_string(),
            column_ids,
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            request,
        };
        let response = self
            .execute(freeze_mutation("register_table", &command, opts)?)
            .await?;
        response_table(response)
    }

    /// Applies a table metadata patch while retaining its stable ID and columns.
    ///
    /// # Errors
    ///
    /// Returns semantic conflict, validation, capacity, or authority publication errors.
    pub async fn update_table_in_schema(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
        patch: TablePatch,
        opts: WriteOptions,
    ) -> Result<Table> {
        let command = FrozenCommand::UpdateTable {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            name: name.to_string(),
            patch,
        };
        let response = self
            .execute(freeze_mutation("update_table", &command, opts)?)
            .await?;
        response_table(response)
    }

    /// Renames a table atomically with its name index.
    ///
    /// # Errors
    ///
    /// Returns semantic conflict, validation, capacity, or authority publication errors.
    pub async fn rename_table(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
        new_name: &str,
        opts: WriteOptions,
    ) -> Result<Table> {
        let command = FrozenCommand::RenameTable {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            name: name.to_string(),
            new_name: new_name.to_string(),
        };
        let response = self
            .execute(freeze_mutation("rename_table", &command, opts)?)
            .await?;
        response_table(response)
    }

    /// Drops a table, its name index, and every column record atomically.
    ///
    /// # Errors
    ///
    /// Returns semantic conflict, validation, capacity, or authority publication errors.
    pub async fn drop_table(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
        opts: WriteOptions,
    ) -> Result<()> {
        let command = FrozenCommand::DropTable {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
            name: name.to_string(),
        };
        let response = self
            .execute(freeze_mutation("drop_table", &command, opts)?)
            .await?;
        response_deleted(&response)
    }

    async fn execute(&self, frozen: FrozenMutation) -> Result<MutationResponseV1> {
        let started = Instant::now();
        let mut attempt = 0_u32;
        loop {
            attempt = attempt.saturating_add(1);
            let mut options = TxnOptions::default()
                .with_operation_id(format!("{}-{:04}", frozen.operation_id, attempt));
            if let Some(request_id) = &frozen.request_id {
                options = options.with_request_id(request_id);
            }
            let mut txn = self.store.begin_control_txn(options).await?;
            if let Some(receipt) = load_receipt(&mut txn, &frozen.receipt_key).await? {
                if receipt.operation_family != frozen.family
                    || receipt.request_digest != frozen.digest
                {
                    return Err(idempotency_conflict());
                }
                return Ok(receipt.response);
            }

            let response = apply_command(&mut txn, &frozen.command).await?;
            let predicted = txn.predicted_state_token()?;
            stage_commit_records(&mut txn, &frozen, &response, &predicted).await?;
            match txn.commit().await {
                Ok(outcome) => {
                    if outcome.state_token() != &predicted {
                        return Err(CatalogError::InvariantViolation {
                            message: "control catalog commit returned a different StateToken than its atomically bound records".to_string(),
                        });
                    }
                    for intent in outcome.projection_intents() {
                        if let Err(error) = self.projection_notifier.notify(intent) {
                            warn!(
                                intent_id = intent.intent_id(),
                                projection_kind = intent.projection_kind(),
                                error = %error,
                                "catalog projection notification failed after authority commit; durable anti-entropy will retry"
                            );
                        }
                    }
                    return Ok(response);
                }
                Err(CatalogError::CasFailed { .. }) if started.elapsed() < RETRY_BUDGET => {
                    let jitter = 5_u64 + u64::from(attempt % 11);
                    sleep(Duration::from_millis(jitter)).await;
                }
                Err(CatalogError::CasFailed { .. }) => {
                    return Err(CatalogError::CasFailed {
                        message:
                            "control catalog conflict retry budget exhausted after 1.5 seconds"
                                .to_string(),
                    });
                }
                Err(error) => return Err(error),
            }
        }
    }
}

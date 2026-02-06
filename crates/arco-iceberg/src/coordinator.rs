//! Multi-table transaction coordinator for ICE-7 atomic commits.
//!
//! Implements the prepare→commit→finalize protocol for atomically committing
//! changes across multiple Iceberg tables.

use bytes::Bytes;
use chrono::{Duration, Utc};
use std::fmt;
use std::sync::Arc;
use uuid::Uuid;

use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};

use crate::commit::validate_requirements;
use crate::error::{IcebergError, IcebergResult};
use crate::idempotency::IdempotencyMarker;
use crate::pointer::{
    CasResult, IcebergTablePointer, PendingPointerUpdate, PointerStore, UpdateSource,
};
use crate::transactions::{
    CreateTransactionResult, DEFAULT_TRANSACTION_TIMEOUT, MAX_TABLES_PER_TRANSACTION,
    TransactionCasResult, TransactionRecord, TransactionStatus, TransactionStore,
    TransactionStoreImpl, TransactionTableEntry,
};
use crate::types::commit::{
    CommitTableRequest, SnapshotRefType as UpdateSnapshotRefType, TableUpdate,
};
use crate::types::{
    MetadataLogEntry, ObjectVersion, SnapshotLogEntry, SnapshotRefMetadata, TableMetadata,
};

/// Error from multi-table transaction commit.
#[derive(Debug)]
pub enum MultiTableCommitError {
    /// Standard Iceberg error.
    Iceberg(IcebergError),
    /// Temporary failure - retry after the specified seconds.
    RetryAfter {
        /// Suggested retry delay in seconds.
        seconds: u32,
    },
    /// Transaction was aborted.
    Aborted {
        /// The abort reason.
        reason: String,
    },
}

impl fmt::Display for MultiTableCommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Iceberg(e) => write!(f, "{e}"),
            Self::RetryAfter { seconds } => write!(f, "Retry after {seconds} seconds"),
            Self::Aborted { reason } => write!(f, "Transaction aborted: {reason}"),
        }
    }
}

impl From<IcebergError> for MultiTableCommitError {
    fn from(err: IcebergError) -> Self {
        Self::Iceberg(err)
    }
}

/// Input for a single table in a multi-table transaction.
#[derive(Debug, Clone)]
pub struct TableCommitInput {
    /// The table UUID.
    pub table_uuid: Uuid,
    /// Namespace name.
    pub namespace: String,
    /// Table name.
    pub table_name: String,
    /// Commit request with requirements and updates.
    pub request: CommitTableRequest,
}

/// Result of a successful multi-table commit.
#[derive(Debug, Clone)]
pub struct MultiTableCommitResult {
    /// Transaction ID.
    pub tx_id: String,
    /// Per-table outputs.
    pub tables: Vec<TableCommitOutput>,
}

/// Output for a single table after successful commit.
#[derive(Debug, Clone)]
pub struct TableCommitOutput {
    /// The table UUID.
    pub table_uuid: Uuid,
    /// Location of the new metadata file.
    pub metadata_location: String,
}

/// Configuration for the transaction coordinator.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Maximum number of tables in a transaction.
    pub max_tables: usize,
    /// Timeout for considering a preparing transaction as stale.
    pub transaction_timeout: Duration,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            max_tables: MAX_TABLES_PER_TRANSACTION,
            transaction_timeout: DEFAULT_TRANSACTION_TIMEOUT,
        }
    }
}

struct PreparedTable {
    table_uuid: Uuid,
    namespace: String,
    table_name: String,
    pointer_version: ObjectVersion,
    new_metadata_location: String,
    new_snapshot_id: Option<i64>,
    new_last_sequence_number: i64,
}

/// Coordinator for multi-table atomic transactions.
pub struct MultiTableTransactionCoordinator<S, P> {
    storage: Arc<S>,
    pointer_store: Arc<P>,
    tx_store: TransactionStoreImpl<S>,
    config: CoordinatorConfig,
}

impl<S: StorageBackend, P: PointerStore> MultiTableTransactionCoordinator<S, P> {
    /// Creates a new coordinator with default configuration.
    #[must_use]
    pub fn new(storage: Arc<S>, pointer_store: Arc<P>) -> Self {
        let tx_store = TransactionStoreImpl::new(Arc::clone(&storage));
        Self {
            storage,
            pointer_store,
            tx_store,
            config: CoordinatorConfig::default(),
        }
    }

    /// Creates a new coordinator with custom configuration.
    #[must_use]
    pub fn with_config(storage: Arc<S>, pointer_store: Arc<P>, config: CoordinatorConfig) -> Self {
        let tx_store = TransactionStoreImpl::new(Arc::clone(&storage));
        Self {
            storage,
            pointer_store,
            tx_store,
            config,
        }
    }

    /// Commits changes to multiple tables atomically.
    ///
    /// # Protocol
    /// 1. Validate inputs (max tables, `tx_id` format)
    /// 2. Check idempotency (existing tx with same `tx_id`)
    /// 3. Create `TransactionRecord` in Preparing status
    /// 4. Prepare phase: for each table (sorted by UUID), write metadata and CAS pointer with pending
    /// 5. Commit barrier: re-read pointers to confirm pending state
    /// 6. Commit: CAS transaction record to Committed
    /// 7. Finalize: best-effort clear pending from pointers
    ///
    /// # Errors
    ///
    /// Returns `MultiTableCommitError` on validation failure, conflicts, or storage errors.
    #[allow(clippy::too_many_lines)]
    pub async fn commit(
        &self,
        tx_id: String,
        request_hash: String,
        tables: Vec<TableCommitInput>,
        source: UpdateSource,
        tenant: &str,
        workspace: &str,
    ) -> Result<MultiTableCommitResult, MultiTableCommitError> {
        // Step 1: Validate inputs
        IdempotencyMarker::validate_uuidv7(&tx_id).map_err(|e| IcebergError::BadRequest {
            message: format!("Invalid transaction ID: {e}"),
            error_type: "BadRequestException",
        })?;

        if tables.len() > self.config.max_tables {
            return Err(IcebergError::BadRequest {
                message: format!(
                    "Transaction exceeds maximum table limit ({} > {})",
                    tables.len(),
                    self.config.max_tables
                ),
                error_type: "BadRequestException",
            }
            .into());
        }

        if tables.is_empty() {
            return Err(IcebergError::BadRequest {
                message: "Transaction must include at least one table".to_string(),
                error_type: "BadRequestException",
            }
            .into());
        }

        // Step 2: Check idempotency
        if let Some((existing, existing_version)) = self.tx_store.load(&tx_id).await? {
            if existing.request_hash != request_hash {
                return Err(IcebergError::Conflict {
                    message: "Idempotency key reused with different request".to_string(),
                    error_type: "CommitFailedException",
                }
                .into());
            }

            match existing.status {
                TransactionStatus::Committed => {
                    let outputs = existing
                        .tables
                        .iter()
                        .map(|t| TableCommitOutput {
                            table_uuid: t.table_uuid,
                            metadata_location: t.new_metadata_location.clone(),
                        })
                        .collect();
                    return Ok(MultiTableCommitResult {
                        tx_id: existing.tx_id,
                        tables: outputs,
                    });
                }
                TransactionStatus::Aborted => {
                    let reason = existing.abort_reason.unwrap_or_default();
                    return Err(MultiTableCommitError::Aborted { reason });
                }
                TransactionStatus::Preparing => {
                    if existing.is_stale(self.config.transaction_timeout) {
                        let aborted = existing.abort("Stale transaction aborted by retry");
                        if let TransactionCasResult::Success { .. } = self
                            .tx_store
                            .compare_and_swap(&aborted, &existing_version)
                            .await?
                        {
                            tracing::info!(
                                tx_id = %tx_id,
                                "Aborted stale preparing transaction, caller should retry"
                            );
                        }
                        return Err(MultiTableCommitError::RetryAfter { seconds: 1 });
                    }
                    return Err(MultiTableCommitError::RetryAfter { seconds: 30 });
                }
            }
        }

        // Step 3: Create TransactionRecord in Preparing status
        let mut tx_record = TransactionRecord::new_preparing(tx_id.clone(), request_hash);
        let create_result = self.tx_store.create(&tx_record).await?;
        let mut tx_version = match create_result {
            CreateTransactionResult::Success { version } => version,
            CreateTransactionResult::Exists { record, .. } => {
                // Race: another request created it first
                return Self::handle_existing_transaction(*record);
            }
        };

        // Sort tables by UUID for deterministic lock ordering
        let mut sorted_tables = tables;
        sorted_tables.sort_by_key(|t| t.table_uuid);

        // Step 4: Prepare phase
        let mut prepared: Vec<PreparedTable> = Vec::with_capacity(sorted_tables.len());
        for table_input in &sorted_tables {
            match self
                .prepare_table(table_input, &tx_id, tenant, workspace)
                .await
            {
                Ok(prep) => prepared.push(prep),
                Err(e) => {
                    // Prepare failed - abort transaction
                    self.abort_transaction(&tx_record, &tx_version, &prepared, &e.to_string())
                        .await;
                    return Err(e);
                }
            }
        }

        // Build transaction record with table entries
        tx_record.tables = prepared
            .iter()
            .map(|p| TransactionTableEntry {
                table_uuid: p.table_uuid,
                namespace: p.namespace.clone(),
                table_name: p.table_name.clone(),
                base_pointer_version: p.pointer_version.as_str().to_string(),
                new_metadata_location: p.new_metadata_location.clone(),
                new_snapshot_id: p.new_snapshot_id,
                new_last_sequence_number: p.new_last_sequence_number,
            })
            .collect();

        // Update transaction record with table entries (still Preparing)
        match self
            .tx_store
            .compare_and_swap(&tx_record, &tx_version)
            .await?
        {
            TransactionCasResult::Success { version } => tx_version = version,
            TransactionCasResult::Conflict { .. } => {
                self.abort_transaction(
                    &tx_record,
                    &tx_version,
                    &prepared,
                    "CAS conflict on tx record",
                )
                .await;
                return Err(MultiTableCommitError::RetryAfter { seconds: 5 });
            }
        }

        // Step 5: Commit barrier - re-read pointers to confirm pending state
        if let Err(e) = self.verify_commit_barrier(&prepared, &tx_id).await {
            self.abort_transaction(&tx_record, &tx_version, &prepared, &e.to_string())
                .await;
            return Err(e);
        }

        // Step 6: Commit - CAS transaction record to Committed
        let committed_record = tx_record.commit();
        match self
            .tx_store
            .compare_and_swap(&committed_record, &tx_version)
            .await?
        {
            TransactionCasResult::Success { .. } => {}
            TransactionCasResult::Conflict { .. } => {
                // CAS failed - check if someone else committed it
                if let Some((current, _)) = self.tx_store.load(&tx_id).await? {
                    if current.status == TransactionStatus::Committed {
                        // Already committed - return success
                        return Ok(MultiTableCommitResult {
                            tx_id,
                            tables: prepared
                                .iter()
                                .map(|p| TableCommitOutput {
                                    table_uuid: p.table_uuid,
                                    metadata_location: p.new_metadata_location.clone(),
                                })
                                .collect(),
                        });
                    }
                }
                return Err(MultiTableCommitError::RetryAfter { seconds: 5 });
            }
        }

        // Step 7: Finalize - best-effort clear pending from pointers
        self.finalize_pointers(&prepared, &tx_id, &source).await;

        Ok(MultiTableCommitResult {
            tx_id,
            tables: prepared
                .iter()
                .map(|p| TableCommitOutput {
                    table_uuid: p.table_uuid,
                    metadata_location: p.new_metadata_location.clone(),
                })
                .collect(),
        })
    }

    fn handle_existing_transaction(
        existing: TransactionRecord,
    ) -> Result<MultiTableCommitResult, MultiTableCommitError> {
        match existing.status {
            TransactionStatus::Committed => Ok(MultiTableCommitResult {
                tx_id: existing.tx_id,
                tables: existing
                    .tables
                    .iter()
                    .map(|t| TableCommitOutput {
                        table_uuid: t.table_uuid,
                        metadata_location: t.new_metadata_location.clone(),
                    })
                    .collect(),
            }),
            TransactionStatus::Aborted => Err(MultiTableCommitError::Aborted {
                reason: existing.abort_reason.unwrap_or_default(),
            }),
            TransactionStatus::Preparing => Err(MultiTableCommitError::RetryAfter { seconds: 30 }),
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn prepare_table(
        &self,
        input: &TableCommitInput,
        tx_id: &str,
        tenant: &str,
        workspace: &str,
    ) -> Result<PreparedTable, MultiTableCommitError> {
        // Load pointer
        let (pointer, pointer_version) = self
            .pointer_store
            .load(&input.table_uuid)
            .await?
            .ok_or_else(|| IcebergError::NotFound {
                message: format!("Table not found: {}.{}", input.namespace, input.table_name),
                error_type: "NoSuchTableException",
            })?;

        if let Some(ref pending) = pointer.pending {
            if let Some((other_tx, other_tx_version)) = self.tx_store.load(&pending.tx_id).await? {
                match other_tx.status {
                    TransactionStatus::Committed => {
                        tracing::info!(
                            tx_id = %pending.tx_id,
                            table_uuid = %input.table_uuid,
                            "Finalizing pending from committed transaction during prepare"
                        );
                        let finalized = pointer.finalize_pending();
                        let _ = self
                            .pointer_store
                            .compare_and_swap(&input.table_uuid, &pointer_version, &finalized)
                            .await;
                        return Err(MultiTableCommitError::RetryAfter { seconds: 1 });
                    }
                    TransactionStatus::Preparing => {
                        if other_tx.is_stale(self.config.transaction_timeout) {
                            tracing::warn!(
                                tx_id = %pending.tx_id,
                                table_uuid = %input.table_uuid,
                                "Aborting stale transaction during prepare"
                            );
                            let aborted =
                                other_tx.abort("Stale transaction aborted during prepare");
                            if let TransactionCasResult::Success { .. } = self
                                .tx_store
                                .compare_and_swap(&aborted, &other_tx_version)
                                .await?
                            {
                                let cleared = pointer.clone().without_pending();
                                let _ = self
                                    .pointer_store
                                    .compare_and_swap(&input.table_uuid, &pointer_version, &cleared)
                                    .await;
                            }
                            return Err(MultiTableCommitError::RetryAfter { seconds: 5 });
                        }
                        return Err(MultiTableCommitError::RetryAfter { seconds: 30 });
                    }
                    TransactionStatus::Aborted => {
                        tracing::info!(
                            tx_id = %pending.tx_id,
                            table_uuid = %input.table_uuid,
                            "Clearing pending from aborted transaction during prepare"
                        );
                        let cleared = pointer.clone().without_pending();
                        let _ = self
                            .pointer_store
                            .compare_and_swap(&input.table_uuid, &pointer_version, &cleared)
                            .await;
                        return Err(MultiTableCommitError::RetryAfter { seconds: 5 });
                    }
                }
            }
            tracing::warn!(
                tx_id = %pending.tx_id,
                table_uuid = %input.table_uuid,
                "Clearing orphaned pending state during prepare"
            );
            let cleared = pointer.clone().without_pending();
            let _ = self
                .pointer_store
                .compare_and_swap(&input.table_uuid, &pointer_version, &cleared)
                .await;
            return Err(MultiTableCommitError::RetryAfter { seconds: 5 });
        }

        // Load base metadata
        let base_metadata = self
            .load_metadata(&pointer.current_metadata_location, tenant, workspace)
            .await?;

        // Validate requirements
        validate_requirements(&base_metadata, &input.request.requirements)?;

        // Compute new metadata
        let mut new_metadata = base_metadata.clone();
        apply_updates(&mut new_metadata, &input.request.updates)?;
        let new_sequence = new_metadata.last_sequence_number + 1;
        finalize_metadata(&mut new_metadata, &base_metadata, &pointer, new_sequence);

        // Build metadata location
        let new_metadata_location = build_metadata_location(
            &new_metadata.location,
            new_sequence,
            tx_id,
            input.table_uuid,
        );

        // Write new metadata file
        let metadata_path = resolve_metadata_path(&new_metadata_location, tenant, workspace)?;
        let metadata_bytes =
            serde_json::to_vec(&new_metadata).map_err(|e| IcebergError::Internal {
                message: format!("Failed to serialize metadata: {e}"),
            })?;

        match self
            .storage
            .put(
                &metadata_path,
                Bytes::from(metadata_bytes),
                WritePrecondition::DoesNotExist,
            )
            .await
        {
            Ok(WriteResult::Success { .. } | WriteResult::PreconditionFailed { .. }) => {}
            Err(e) => {
                return Err(IcebergError::Internal {
                    message: format!("Failed to write metadata: {e}"),
                }
                .into());
            }
        }

        // CAS pointer to add pending
        let pending = PendingPointerUpdate {
            tx_id: tx_id.to_string(),
            metadata_location: new_metadata_location.clone(),
            snapshot_id: new_metadata.current_snapshot_id,
            last_sequence_number: new_metadata.last_sequence_number,
            prepared_at: Utc::now(),
        };

        let new_pointer = pointer.clone().with_pending(pending);

        match self
            .pointer_store
            .compare_and_swap(&input.table_uuid, &pointer_version, &new_pointer)
            .await?
        {
            CasResult::Success { .. } => {}
            CasResult::Conflict { .. } => {
                return Err(IcebergError::Conflict {
                    message: format!(
                        "Pointer conflict while preparing table {}",
                        input.table_uuid
                    ),
                    error_type: "CommitFailedException",
                }
                .into());
            }
        }

        Ok(PreparedTable {
            table_uuid: input.table_uuid,
            namespace: input.namespace.clone(),
            table_name: input.table_name.clone(),
            pointer_version,
            new_metadata_location,
            new_snapshot_id: new_metadata.current_snapshot_id,
            new_last_sequence_number: new_metadata.last_sequence_number,
        })
    }

    async fn verify_commit_barrier(
        &self,
        prepared: &[PreparedTable],
        tx_id: &str,
    ) -> Result<(), MultiTableCommitError> {
        for prep in prepared {
            let (pointer, _) = self
                .pointer_store
                .load(&prep.table_uuid)
                .await?
                .ok_or_else(|| IcebergError::Internal {
                    message: format!("Table pointer disappeared: {}", prep.table_uuid),
                })?;

            let Some(pending) = &pointer.pending else {
                return Err(IcebergError::Conflict {
                    message: format!(
                        "Table {} pending state was cleared before commit",
                        prep.table_uuid
                    ),
                    error_type: "CommitFailedException",
                }
                .into());
            };

            if pending.tx_id != tx_id {
                return Err(IcebergError::Conflict {
                    message: format!(
                        "Table {} pending state was taken over by another transaction",
                        prep.table_uuid
                    ),
                    error_type: "CommitFailedException",
                }
                .into());
            }

            if pending.metadata_location != prep.new_metadata_location {
                return Err(IcebergError::Internal {
                    message: format!(
                        "Table {} pending metadata location mismatch",
                        prep.table_uuid
                    ),
                }
                .into());
            }
        }

        Ok(())
    }

    async fn abort_transaction(
        &self,
        record: &TransactionRecord,
        version: &ObjectVersion,
        prepared: &[PreparedTable],
        reason: &str,
    ) {
        // Best-effort abort the transaction record
        let aborted = record.clone().abort(reason);
        let _ = self.tx_store.compare_and_swap(&aborted, version).await;

        // Best-effort clear pending from pointers
        for prep in prepared {
            if let Ok(Some((pointer, ver))) = self.pointer_store.load(&prep.table_uuid).await {
                if pointer
                    .pending
                    .as_ref()
                    .is_some_and(|p| p.tx_id == record.tx_id)
                {
                    let cleared = pointer.clone().without_pending();
                    let _ = self
                        .pointer_store
                        .compare_and_swap(&prep.table_uuid, &ver, &cleared)
                        .await;
                }
            }
        }
    }

    async fn finalize_pointers(
        &self,
        prepared: &[PreparedTable],
        tx_id: &str,
        source: &UpdateSource,
    ) {
        for prep in prepared {
            if let Ok(Some((pointer, version))) = self.pointer_store.load(&prep.table_uuid).await {
                if pointer.pending.as_ref().is_some_and(|p| p.tx_id == tx_id) {
                    let finalized = pointer.finalize_pending_with_source(source.clone());
                    let _ = self
                        .pointer_store
                        .compare_and_swap(&prep.table_uuid, &version, &finalized)
                        .await;
                }
            }
        }
    }

    async fn load_metadata(
        &self,
        location: &str,
        tenant: &str,
        workspace: &str,
    ) -> IcebergResult<TableMetadata> {
        let path = resolve_metadata_path(location, tenant, workspace)?;
        let bytes = self
            .storage
            .get(&path)
            .await
            .map_err(|e| IcebergError::Internal {
                message: format!("Failed to read metadata: {e}"),
            })?;
        serde_json::from_slice(&bytes).map_err(|e| IcebergError::Internal {
            message: format!("Failed to parse metadata: {e}"),
        })
    }
}

fn resolve_metadata_path(location: &str, tenant: &str, workspace: &str) -> IcebergResult<String> {
    crate::paths::resolve_metadata_path(location, tenant, workspace)
}

fn build_metadata_location(
    table_location: &str,
    sequence_number: i64,
    tx_id: &str,
    table_uuid: Uuid,
) -> String {
    let base = table_location.trim_end_matches('/');
    format!("{base}/metadata/{sequence_number:05}-{tx_id}-{table_uuid}.metadata.json")
}

fn apply_updates(metadata: &mut TableMetadata, updates: &[TableUpdate]) -> IcebergResult<()> {
    for update in updates {
        apply_update(metadata, update)?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn apply_update(metadata: &mut TableMetadata, update: &TableUpdate) -> IcebergResult<()> {
    use crate::types::TableUuid;
    use std::collections::HashSet;

    match update {
        TableUpdate::AssignUuid { uuid } => {
            metadata.table_uuid = TableUuid::new(*uuid);
        }
        TableUpdate::UpgradeFormatVersion { format_version } => {
            if *format_version < metadata.format_version {
                return Err(IcebergError::BadRequest {
                    message: "format-version cannot be downgraded".to_string(),
                    error_type: "BadRequestException",
                });
            }
            metadata.format_version = *format_version;
        }
        TableUpdate::AddSchema {
            schema,
            last_column_id,
        } => {
            metadata.schemas.push(schema.clone());
            let max_field_id = schema.fields.iter().map(|f| f.id).max();
            if let Some(max_field_id) = max_field_id {
                if max_field_id > metadata.last_column_id {
                    metadata.last_column_id = max_field_id;
                }
            }
            if let Some(last_column_id) = last_column_id {
                if *last_column_id < metadata.last_column_id {
                    return Err(IcebergError::BadRequest {
                        message: "last-column-id cannot move backwards".to_string(),
                        error_type: "BadRequestException",
                    });
                }
                metadata.last_column_id = *last_column_id;
            }
        }
        TableUpdate::SetCurrentSchema { schema_id } => {
            let exists = metadata.schemas.iter().any(|s| s.schema_id == *schema_id);
            if !exists {
                return Err(IcebergError::BadRequest {
                    message: format!("Schema {schema_id} does not exist"),
                    error_type: "BadRequestException",
                });
            }
            metadata.current_schema_id = *schema_id;
        }
        TableUpdate::AddPartitionSpec { spec } => {
            let max_field_id = spec.fields.iter().map(|f| f.field_id).max();
            if let Some(max_field_id) = max_field_id {
                if max_field_id > metadata.last_partition_id {
                    metadata.last_partition_id = max_field_id;
                }
            }
            metadata.partition_specs.push(spec.clone());
        }
        TableUpdate::SetDefaultSpec { spec_id } => {
            let exists = metadata
                .partition_specs
                .iter()
                .any(|s| s.spec_id == *spec_id);
            if !exists {
                return Err(IcebergError::BadRequest {
                    message: format!("Partition spec {spec_id} does not exist"),
                    error_type: "BadRequestException",
                });
            }
            metadata.default_spec_id = *spec_id;
        }
        TableUpdate::AddSortOrder { sort_order } => {
            metadata.sort_orders.push(sort_order.clone());
        }
        TableUpdate::SetDefaultSortOrder { sort_order_id } => {
            let exists = metadata
                .sort_orders
                .iter()
                .any(|o| o.order_id == *sort_order_id);
            if !exists {
                return Err(IcebergError::BadRequest {
                    message: format!("Sort order {sort_order_id} does not exist"),
                    error_type: "BadRequestException",
                });
            }
            metadata.default_sort_order_id = *sort_order_id;
        }
        TableUpdate::AddSnapshot { snapshot } => {
            metadata.last_sequence_number =
                metadata.last_sequence_number.max(snapshot.sequence_number);
            metadata.snapshots.push(snapshot.clone());
        }
        TableUpdate::SetSnapshotRef {
            ref_name,
            ref_type,
            snapshot_id,
            ..
        } => {
            let ref_type_str = match ref_type {
                UpdateSnapshotRefType::Branch => "branch",
                UpdateSnapshotRefType::Tag => "tag",
            };
            metadata.refs.insert(
                ref_name.clone(),
                SnapshotRefMetadata {
                    snapshot_id: *snapshot_id,
                    ref_type: ref_type_str.to_string(),
                },
            );
            if ref_name == "main" {
                metadata.current_snapshot_id = Some(*snapshot_id);
                metadata.snapshot_log.push(SnapshotLogEntry {
                    snapshot_id: *snapshot_id,
                    timestamp_ms: Utc::now().timestamp_millis(),
                });
            }
        }
        TableUpdate::RemoveSnapshotRef { ref_name } => {
            metadata.refs.remove(ref_name);
            if ref_name == "main" {
                metadata.current_snapshot_id = None;
            }
        }
        TableUpdate::RemoveSnapshots { snapshot_ids } => {
            let ids: HashSet<i64> = snapshot_ids.iter().copied().collect();
            metadata.snapshots.retain(|s| !ids.contains(&s.snapshot_id));
            metadata.refs.retain(|_, r| !ids.contains(&r.snapshot_id));
            if metadata
                .current_snapshot_id
                .is_some_and(|id| ids.contains(&id))
            {
                metadata.current_snapshot_id = None;
            }
        }
        TableUpdate::SetLocation { .. } => {
            return Err(IcebergError::BadRequest {
                message: "SetLocation is not permitted".to_string(),
                error_type: "BadRequestException",
            });
        }
        TableUpdate::SetProperties { updates } => {
            for (key, value) in updates {
                metadata.properties.insert(key.clone(), value.clone());
            }
        }
        TableUpdate::RemoveProperties { removals } => {
            for key in removals {
                metadata.properties.remove(key);
            }
        }
    }
    Ok(())
}

fn finalize_metadata(
    metadata: &mut TableMetadata,
    base_metadata: &TableMetadata,
    pointer: &IcebergTablePointer,
    next_sequence: i64,
) {
    let previous = pointer.current_metadata_location.clone();
    if metadata
        .metadata_log
        .last()
        .is_none_or(|entry| entry.metadata_file != previous)
    {
        metadata.metadata_log.push(MetadataLogEntry {
            metadata_file: previous,
            timestamp_ms: base_metadata.last_updated_ms,
        });
    }
    metadata.last_sequence_number = metadata.last_sequence_number.max(next_sequence);
    metadata.last_updated_ms = Utc::now().timestamp_millis();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_metadata_location() {
        let location = "gs://bucket/warehouse/table";
        let result = build_metadata_location(
            location,
            5,
            "tx-123",
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        );
        assert!(result.contains("/metadata/00005-tx-123-"));
        assert!(result.ends_with(".metadata.json"));
    }
}

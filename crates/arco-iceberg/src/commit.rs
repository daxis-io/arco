//! Iceberg table commit flow implementation.
//!
//! See design doc Section 5 for the complete 12-step flow.

use bytes::Bytes;
use chrono::{Duration, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};

use crate::error::{IcebergError, IcebergErrorResponse, IcebergResult};
use crate::events::{CommittedReceipt, PendingReceipt};
use crate::idempotency::{
    ClaimResult, FinalizeResult, IdempotencyMarker, IdempotencyStatus, IdempotencyStore,
    IdempotencyStoreImpl,
};
use crate::metrics;
use crate::paths::resolve_metadata_path;
use crate::pointer::{
    CasResult, IcebergTablePointer, PointerStore, PointerStoreImpl, SnapshotRef, SnapshotRefType,
    UpdateSource, resolve_effective_metadata_location,
};
use crate::transactions::{
    DEFAULT_TRANSACTION_TIMEOUT, TransactionCasResult, TransactionStatus, TransactionStore,
    TransactionStoreImpl,
};
use crate::types::commit::{
    CommitTableRequest, CommitTableResponse, SnapshotRefType as UpdateSnapshotRefType, TableUpdate,
    UpdateRequirement,
};
use crate::types::{
    CommitKey, MetadataLogEntry, SnapshotLogEntry, SnapshotRefMetadata, TableMetadata, TableUuid,
};

/// Validates update requirements against current table metadata.
///
/// # Errors
///
/// Returns `IcebergError::Conflict` if any requirement is not met.
pub fn validate_requirements(
    metadata: &TableMetadata,
    requirements: &[UpdateRequirement],
) -> IcebergResult<()> {
    for requirement in requirements {
        validate_requirement(metadata, requirement)?;
    }
    Ok(())
}

fn validate_requirement(
    metadata: &TableMetadata,
    requirement: &UpdateRequirement,
) -> IcebergResult<()> {
    match requirement {
        UpdateRequirement::AssertTableUuid { uuid } => {
            if metadata.table_uuid.as_uuid() != uuid {
                return Err(IcebergError::commit_conflict(format!(
                    "Table UUID mismatch: expected {}, found {}",
                    uuid,
                    metadata.table_uuid.as_uuid()
                )));
            }
        }
        UpdateRequirement::AssertRefSnapshotId {
            ref_name,
            snapshot_id,
        } => {
            let current_snapshot_id =
                metadata
                    .refs
                    .get(ref_name)
                    .map(|r| r.snapshot_id)
                    .or_else(|| {
                        if ref_name == "main" {
                            metadata.current_snapshot_id
                        } else {
                            None
                        }
                    });

            if current_snapshot_id != *snapshot_id {
                return Err(IcebergError::commit_conflict(format!(
                    "Ref '{ref_name}' snapshot mismatch: expected {snapshot_id:?}, found {current_snapshot_id:?}",
                )));
            }
        }
        UpdateRequirement::AssertCurrentSchemaId { current_schema_id } => {
            if metadata.current_schema_id != *current_schema_id {
                return Err(IcebergError::commit_conflict(format!(
                    "Current schema ID mismatch: expected {}, found {}",
                    current_schema_id, metadata.current_schema_id
                )));
            }
        }
        UpdateRequirement::AssertLastAssignedFieldId {
            last_assigned_field_id,
        } => {
            if metadata.last_column_id != *last_assigned_field_id {
                return Err(IcebergError::commit_conflict(format!(
                    "Last assigned field ID mismatch: expected {}, found {}",
                    last_assigned_field_id, metadata.last_column_id
                )));
            }
        }
        UpdateRequirement::AssertDefaultSpecId { default_spec_id } => {
            if metadata.default_spec_id != *default_spec_id {
                return Err(IcebergError::commit_conflict(format!(
                    "Default spec ID mismatch: expected {}, found {}",
                    default_spec_id, metadata.default_spec_id
                )));
            }
        }
        UpdateRequirement::AssertDefaultSortOrderId {
            default_sort_order_id,
        } => {
            if metadata.default_sort_order_id != *default_sort_order_id {
                return Err(IcebergError::commit_conflict(format!(
                    "Default sort order ID mismatch: expected {}, found {}",
                    default_sort_order_id, metadata.default_sort_order_id
                )));
            }
        }
        UpdateRequirement::AssertLastAssignedPartitionId {
            last_assigned_partition_id,
        } => {
            if metadata.last_partition_id != *last_assigned_partition_id {
                return Err(IcebergError::commit_conflict(format!(
                    "Last assigned partition ID mismatch: expected {}, found {}",
                    last_assigned_partition_id, metadata.last_partition_id
                )));
            }
        }
    }
    Ok(())
}

const IN_PROGRESS_TIMEOUT: Duration = Duration::minutes(10);
const MAX_TAKEOVER_ATTEMPTS: usize = 3;
const COMMIT_ENDPOINT: &str = "/v1/{prefix}/namespaces/{namespace}/tables/{table}";
const COMMIT_METHOD: &str = "POST";

enum PendingHandleAction {
    Retry,
    RetryAfter { seconds: u32 },
}

/// Error returned from commit operations that may carry cached failure state.
#[derive(Debug)]
pub enum CommitError {
    /// Standard Iceberg error response.
    Iceberg(IcebergError),
    /// Cached error response from a failed idempotency marker.
    CachedFailure {
        /// Cached HTTP status for the failed commit.
        status: u16,
        /// Cached error payload for the failed commit.
        payload: IcebergErrorResponse,
    },
    /// Temporary failure - retry after the specified seconds.
    RetryAfter {
        /// Suggested retry delay in seconds.
        seconds: u32,
    },
}

impl From<IcebergError> for CommitError {
    fn from(err: IcebergError) -> Self {
        Self::Iceberg(err)
    }
}

/// Commit flow implementation for the Iceberg REST write path.
pub struct CommitService<S> {
    storage: Arc<S>,
    in_progress_timeout: Duration,
}

impl<S: StorageBackend> CommitService<S> {
    /// Creates a new commit service using the provided storage.
    #[must_use]
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            in_progress_timeout: IN_PROGRESS_TIMEOUT,
        }
    }

    /// Executes the 12-step commit flow for `commit_table`.
    ///
    /// # Errors
    ///
    /// Returns a `CommitError` when commit preconditions fail or when storage
    /// operations cannot be completed.
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_table(
        &self,
        table_uuid: Uuid,
        namespace: &str,
        table: &str,
        request: CommitTableRequest,
        request_hash: String,
        idempotency_key: String,
        source: UpdateSource,
        tenant: &str,
        workspace: &str,
    ) -> Result<CommitTableResponse, CommitError> {
        let pointer_store = PointerStoreImpl::new(Arc::clone(&self.storage));
        let idempotency_store = IdempotencyStoreImpl::new(Arc::clone(&self.storage));
        let idempotency_key_hash = IdempotencyMarker::hash_key(&idempotency_key);

        let tx_store = TransactionStoreImpl::new(Arc::clone(&self.storage));

        for _ in 0..MAX_TAKEOVER_ATTEMPTS {
            let (pointer, pointer_version) = pointer_store
                .load(&table_uuid)
                .await?
                .ok_or_else(|| IcebergError::table_not_found(namespace, table))?;
            pointer
                .validate_version()
                .map_err(|err| IcebergError::Internal {
                    message: err.to_string(),
                })?;

            if let Some(action) = self
                .handle_pending_transaction(
                    &pointer_store,
                    &table_uuid,
                    &pointer,
                    &pointer_version,
                    &tx_store,
                )
                .await?
            {
                match action {
                    PendingHandleAction::Retry => continue,
                    PendingHandleAction::RetryAfter { seconds } => {
                        return Err(CommitError::RetryAfter { seconds });
                    }
                }
            }
            let base_metadata = self
                .load_metadata(&pointer.current_metadata_location, tenant, workspace)
                .await?;

            // Step 4: Claim idempotency marker
            let new_metadata_location = build_metadata_location(
                &base_metadata.location,
                base_metadata.last_sequence_number + 1,
                &idempotency_key_hash,
            )?;
            let mut marker = IdempotencyMarker::new_in_progress(
                idempotency_key.clone(),
                table_uuid,
                request_hash.clone(),
                pointer.current_metadata_location.clone(),
                new_metadata_location.clone(),
            );

            let (mut marker_version, existing_marker) =
                match idempotency_store.claim(&marker).await? {
                    ClaimResult::Success { version } => (version, None),
                    ClaimResult::Exists { marker, version } => (version, Some(*marker)),
                };

            if let Some(existing) = existing_marker {
                // Same idempotency key but different payload - deterministic conflict.
                if existing.request_hash != request_hash {
                    return Err(IcebergError::idempotency_key_conflict().into());
                }

                match existing.status {
                    IdempotencyStatus::Committed => {
                        return self
                            .response_from_marker(&existing, tenant, workspace)
                            .await;
                    }
                    IdempotencyStatus::Failed => {
                        let status = existing.error_http_status.unwrap_or(409);
                        let payload = existing.error_payload.unwrap_or_else(|| {
                            IcebergErrorResponse::from(&IcebergError::idempotency_key_conflict())
                        });
                        return Err(CommitError::CachedFailure { status, payload });
                    }
                    IdempotencyStatus::InProgress => {
                        let current_pointer = pointer_store
                            .load(&table_uuid)
                            .await?
                            .ok_or_else(|| IcebergError::table_not_found(namespace, table))?
                            .0;

                        let effective =
                            resolve_effective_metadata_location(&current_pointer, &tx_store)
                                .await?;

                        if effective.metadata_location == existing.metadata_location {
                            let response = self
                                .response_from_location(
                                    &existing.metadata_location,
                                    tenant,
                                    workspace,
                                )
                                .await?;
                            let finalized =
                                existing.finalize_committed(response.metadata_location.clone());
                            let _ = idempotency_store
                                .finalize(&finalized, &marker_version)
                                .await;
                            let _ = self
                                .write_committed_receipt(
                                    &finalized,
                                    &response.metadata_location,
                                    current_pointer.previous_metadata_location.clone(),
                                    current_pointer.current_snapshot_id,
                                    source.clone(),
                                )
                                .await;
                            return Ok(response);
                        }

                        if existing.is_stale(self.in_progress_timeout) {
                            // Attempt takeover by CAS-updating the marker.
                            marker = IdempotencyMarker {
                                started_at: Utc::now(),
                                event_id: ulid::Ulid::new(),
                                base_metadata_location: pointer.current_metadata_location.clone(),
                                metadata_location: new_metadata_location.clone(),
                                ..existing
                            };

                            match idempotency_store.finalize(&marker, &marker_version).await? {
                                FinalizeResult::Success { version } => {
                                    marker_version = version;
                                    // proceed with commit
                                }
                                FinalizeResult::Conflict { .. } => {
                                    // Lost the race; retry from the start.
                                    continue;
                                }
                            }
                        } else {
                            return Err(CommitError::RetryAfter {
                                seconds: self.retry_after_seconds(),
                            });
                        }
                    }
                }
            }

            // Step 5: Validate update requirements
            validate_requirements(&base_metadata, &request.requirements)?;

            // Step 6: Compute new metadata state
            let mut new_metadata = base_metadata.clone();
            apply_updates(&mut new_metadata, &request.updates)?;
            finalize_metadata(
                &mut new_metadata,
                &base_metadata,
                &pointer,
                base_metadata.last_sequence_number + 1,
            );

            // Step 7: Write new metadata file
            let metadata_path =
                resolve_metadata_path(&marker.metadata_location, tenant, workspace)?;
            let metadata_bytes =
                serde_json::to_vec(&new_metadata).map_err(|e| IcebergError::Internal {
                    message: format!("Failed to serialize table metadata: {e}"),
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
                Ok(WriteResult::Success { .. }) => {}
                Ok(WriteResult::PreconditionFailed { .. }) => {
                    metrics::record_cas_conflict(COMMIT_ENDPOINT, COMMIT_METHOD, "metadata_write");
                    let err = IcebergError::commit_conflict(
                        "Metadata file already exists for this commit",
                    );
                    self.finalize_failed_marker(&idempotency_store, &marker, &marker_version, &err)
                        .await?;
                    return Err(err.into());
                }
                Err(e) => {
                    return Err(IcebergError::Internal {
                        message: format!("Failed to write metadata file: {e}"),
                    }
                    .into());
                }
            }

            let commit_key = CommitKey::from_metadata_location(&marker.metadata_location);

            // Step 8: Write pending receipt (best effort)
            let _ = self
                .write_pending_receipt(
                    &marker,
                    &commit_key,
                    new_metadata.current_snapshot_id,
                    source.clone(),
                )
                .await;

            // Step 9: Pointer CAS
            let new_pointer = pointer_from_metadata(&pointer, &new_metadata, &marker, &source)?;
            match pointer_store
                .compare_and_swap(&table_uuid, &pointer_version, &new_pointer)
                .await?
            {
                CasResult::Success { .. } => {}
                CasResult::Conflict { .. } => {
                    metrics::record_cas_conflict(COMMIT_ENDPOINT, COMMIT_METHOD, "pointer_cas");
                    let err = IcebergError::commit_conflict(
                        "Table pointer was updated by another writer",
                    );
                    self.finalize_failed_marker(&idempotency_store, &marker, &marker_version, &err)
                        .await?;
                    return Err(err.into());
                }
            }

            // Step 10: Write committed receipt (best effort)
            let metadata_location = marker.metadata_location.clone();
            let _ = self
                .write_committed_receipt(
                    &marker,
                    &metadata_location,
                    pointer.previous_metadata_location.clone(),
                    new_metadata.current_snapshot_id,
                    source.clone(),
                )
                .await;

            // Step 11: Finalize idempotency marker
            let finalized = marker.finalize_committed(metadata_location.clone());
            self.finalize_committed_marker(&idempotency_store, &finalized, &marker_version)
                .await?;

            // Step 12: Return response
            let metadata_value =
                serde_json::to_value(&new_metadata).map_err(|e| IcebergError::Internal {
                    message: format!("Failed to serialize metadata response: {e}"),
                })?;
            return Ok(CommitTableResponse {
                metadata_location,
                metadata: metadata_value,
            });
        }

        Err(CommitError::RetryAfter {
            seconds: self.retry_after_seconds(),
        })
    }

    fn retry_after_seconds(&self) -> u32 {
        let seconds = self.in_progress_timeout.num_seconds().max(0);
        u32::try_from(seconds).unwrap_or(u32::MAX)
    }

    async fn handle_pending_transaction(
        &self,
        pointer_store: &PointerStoreImpl<S>,
        table_uuid: &Uuid,
        pointer: &IcebergTablePointer,
        pointer_version: &crate::types::ObjectVersion,
        tx_store: &TransactionStoreImpl<S>,
    ) -> Result<Option<PendingHandleAction>, CommitError> {
        let Some(pending) = &pointer.pending else {
            return Ok(None);
        };

        let tx_result = tx_store.load(&pending.tx_id).await?;

        if let Some((tx_record, tx_version)) = tx_result {
            match tx_record.status {
                TransactionStatus::Committed => {
                    tracing::info!(
                        tx_id = %pending.tx_id,
                        table_uuid = %table_uuid,
                        "Finalizing committed transaction pending state"
                    );
                    let finalized_pointer = pointer.clone().finalize_pending();
                    let _ = pointer_store
                        .compare_and_swap(table_uuid, pointer_version, &finalized_pointer)
                        .await?;
                    Ok(Some(PendingHandleAction::Retry))
                }
                TransactionStatus::Preparing => {
                    if tx_record.is_stale(DEFAULT_TRANSACTION_TIMEOUT) {
                        tracing::warn!(
                            tx_id = %pending.tx_id,
                            table_uuid = %table_uuid,
                            started_at = %tx_record.started_at,
                            "Aborting stale multi-table transaction"
                        );
                        let aborted_record =
                            tx_record.abort("Stale transaction aborted by single-table commit");

                        if let TransactionCasResult::Success { .. } = tx_store
                            .compare_and_swap(&aborted_record, &tx_version)
                            .await?
                        {
                            let cleared_pointer = IcebergTablePointer {
                                pending: None,
                                ..pointer.clone()
                            };
                            let _ = pointer_store
                                .compare_and_swap(table_uuid, pointer_version, &cleared_pointer)
                                .await;
                        }
                        Ok(Some(PendingHandleAction::Retry))
                    } else {
                        tracing::info!(
                            tx_id = %pending.tx_id,
                            table_uuid = %table_uuid,
                            "Table has active multi-table transaction in progress"
                        );
                        Ok(Some(PendingHandleAction::RetryAfter { seconds: 30 }))
                    }
                }
                TransactionStatus::Aborted => {
                    tracing::info!(
                        tx_id = %pending.tx_id,
                        table_uuid = %table_uuid,
                        "Clearing pending state from aborted transaction"
                    );
                    let cleared_pointer = IcebergTablePointer {
                        pending: None,
                        ..pointer.clone()
                    };
                    let _ = pointer_store
                        .compare_and_swap(table_uuid, pointer_version, &cleared_pointer)
                        .await;
                    Ok(Some(PendingHandleAction::Retry))
                }
            }
        } else {
            tracing::warn!(
                tx_id = %pending.tx_id,
                table_uuid = %table_uuid,
                "Clearing orphaned pending state (transaction record not found)"
            );
            let cleared_pointer = IcebergTablePointer {
                pending: None,
                ..pointer.clone()
            };
            let _ = pointer_store
                .compare_and_swap(table_uuid, pointer_version, &cleared_pointer)
                .await;
            Ok(Some(PendingHandleAction::Retry))
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
            .map_err(|err| IcebergError::Internal {
                message: err.to_string(),
            })?;
        serde_json::from_slice(&bytes).map_err(|err| IcebergError::Internal {
            message: format!("Failed to parse table metadata: {err}"),
        })
    }

    async fn response_from_marker(
        &self,
        marker: &IdempotencyMarker,
        tenant: &str,
        workspace: &str,
    ) -> Result<CommitTableResponse, CommitError> {
        let location = marker
            .response_metadata_location
            .as_deref()
            .unwrap_or(&marker.metadata_location);
        self.response_from_location(location, tenant, workspace)
            .await
    }

    async fn response_from_location(
        &self,
        location: &str,
        tenant: &str,
        workspace: &str,
    ) -> Result<CommitTableResponse, CommitError> {
        let path = resolve_metadata_path(location, tenant, workspace)?;
        let bytes = self
            .storage
            .get(&path)
            .await
            .map_err(|err| IcebergError::Internal {
                message: err.to_string(),
            })?;
        let metadata: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|err| IcebergError::Internal {
                message: format!("Failed to parse metadata response: {err}"),
            })?;
        Ok(CommitTableResponse {
            metadata_location: location.to_string(),
            metadata,
        })
    }

    async fn write_pending_receipt(
        &self,
        marker: &IdempotencyMarker,
        commit_key: &CommitKey,
        snapshot_id: Option<i64>,
        source: UpdateSource,
    ) -> IcebergResult<()> {
        let receipt = PendingReceipt {
            table_uuid: marker.table_uuid,
            commit_key: commit_key.clone(),
            event_id: marker.event_id,
            metadata_location: marker.metadata_location.clone(),
            base_metadata_location: marker.base_metadata_location.clone(),
            snapshot_id,
            source,
            started_at: marker.started_at,
        };
        let path = PendingReceipt::storage_path(Utc::now().date_naive(), commit_key);
        let bytes = serde_json::to_vec(&receipt).map_err(|e| IcebergError::Internal {
            message: format!("Failed to serialize pending receipt: {e}"),
        })?;
        let _ = self
            .storage
            .put(&path, Bytes::from(bytes), WritePrecondition::DoesNotExist)
            .await;
        Ok(())
    }

    async fn write_committed_receipt(
        &self,
        marker: &IdempotencyMarker,
        metadata_location: &str,
        previous_metadata_location: Option<String>,
        snapshot_id: Option<i64>,
        source: UpdateSource,
    ) -> IcebergResult<()> {
        let receipt = CommittedReceipt {
            table_uuid: marker.table_uuid,
            commit_key: CommitKey::from_metadata_location(metadata_location),
            event_id: marker.event_id,
            metadata_location: metadata_location.to_string(),
            snapshot_id,
            previous_metadata_location,
            source,
            committed_at: Utc::now(),
        };
        let path = CommittedReceipt::storage_path(Utc::now().date_naive(), &receipt.commit_key);
        let bytes = serde_json::to_vec(&receipt).map_err(|e| IcebergError::Internal {
            message: format!("Failed to serialize committed receipt: {e}"),
        })?;
        let _ = self
            .storage
            .put(&path, Bytes::from(bytes), WritePrecondition::DoesNotExist)
            .await;
        Ok(())
    }

    async fn finalize_failed_marker(
        &self,
        store: &IdempotencyStoreImpl<S>,
        marker: &IdempotencyMarker,
        marker_version: &crate::types::ObjectVersion,
        err: &IcebergError,
    ) -> Result<(), CommitError> {
        if !err.status_code().is_client_error() {
            return Ok(());
        }
        let error_payload = IcebergErrorResponse::from(err);
        let failed = marker
            .clone()
            .finalize_failed(err.status_code().as_u16(), error_payload);
        match store.finalize(&failed, marker_version).await? {
            FinalizeResult::Success { .. } => Ok(()),
            FinalizeResult::Conflict { .. } => Err(CommitError::RetryAfter {
                seconds: self.retry_after_seconds(),
            }),
        }
    }

    async fn finalize_committed_marker(
        &self,
        store: &IdempotencyStoreImpl<S>,
        marker: &IdempotencyMarker,
        marker_version: &crate::types::ObjectVersion,
    ) -> Result<(), CommitError> {
        match store.finalize(marker, marker_version).await? {
            FinalizeResult::Success { .. } => Ok(()),
            FinalizeResult::Conflict { .. } => {
                // Re-load marker to see if it was finalized by another attempt.
                let existing = store
                    .load(&marker.table_uuid, &marker.idempotency_key_hash)
                    .await?;
                if let Some((existing, _)) = existing {
                    if existing.status == IdempotencyStatus::Committed {
                        return Ok(());
                    }
                }
                Err(CommitError::RetryAfter {
                    seconds: self.retry_after_seconds(),
                })
            }
        }
    }
}

fn build_metadata_location(
    table_location: &str,
    sequence_number: i64,
    idempotency_key_hash: &str,
) -> IcebergResult<String> {
    let base = table_location.trim_end_matches('/');
    if base.is_empty() {
        return Err(IcebergError::Internal {
            message: "Table location is empty".to_string(),
        });
    }
    Ok(format!(
        "{base}/metadata/{sequence_number}-{idempotency_key_hash}.metadata.json"
    ))
}

fn apply_updates(metadata: &mut TableMetadata, updates: &[TableUpdate]) -> IcebergResult<()> {
    for update in updates {
        apply_update(metadata, update)?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn apply_update(metadata: &mut TableMetadata, update: &TableUpdate) -> IcebergResult<()> {
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
            let exists = metadata
                .schemas
                .iter()
                .any(|schema| schema.schema_id == *schema_id);
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
                .any(|spec| spec.spec_id == *spec_id);
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
                .any(|order| order.order_id == *sort_order_id);
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
            let ref_type = match ref_type {
                UpdateSnapshotRefType::Branch => "branch",
                UpdateSnapshotRefType::Tag => "tag",
            };
            metadata.refs.insert(
                ref_name.clone(),
                SnapshotRefMetadata {
                    snapshot_id: *snapshot_id,
                    ref_type: ref_type.to_string(),
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
            metadata
                .snapshots
                .retain(|snap| !ids.contains(&snap.snapshot_id));
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

fn pointer_from_metadata(
    pointer: &IcebergTablePointer,
    metadata: &TableMetadata,
    marker: &IdempotencyMarker,
    source: &UpdateSource,
) -> IcebergResult<IcebergTablePointer> {
    let refs = metadata
        .refs
        .iter()
        .map(|(name, r)| {
            let ref_type = match r.ref_type.as_str() {
                "branch" => SnapshotRefType::Branch,
                "tag" => SnapshotRefType::Tag,
                other => {
                    return Err(IcebergError::Internal {
                        message: format!("Unsupported snapshot ref type: {other}"),
                    });
                }
            };
            Ok((
                name.clone(),
                SnapshotRef {
                    snapshot_id: r.snapshot_id,
                    ref_type,
                    max_ref_age_ms: None,
                    max_snapshot_age_ms: None,
                    min_snapshots_to_keep: None,
                },
            ))
        })
        .collect::<IcebergResult<HashMap<_, _>>>()?;

    Ok(IcebergTablePointer {
        version: pointer.version,
        table_uuid: pointer.table_uuid,
        current_metadata_location: marker.metadata_location.clone(),
        current_snapshot_id: metadata.current_snapshot_id,
        refs,
        last_sequence_number: metadata.last_sequence_number,
        previous_metadata_location: Some(pointer.current_metadata_location.clone()),
        updated_at: Utc::now(),
        updated_by: source.clone(),
        pending: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SnapshotRefMetadata, TableUuid};
    use std::collections::HashMap;
    use uuid::Uuid;

    fn test_metadata() -> TableMetadata {
        TableMetadata {
            format_version: 2,
            table_uuid: TableUuid::new(
                Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("valid uuid"),
            ),
            location: "gs://bucket/table".to_string(),
            last_sequence_number: 5,
            last_updated_ms: 1_234_567_890_000,
            last_column_id: 10,
            current_schema_id: 0,
            schemas: vec![],
            current_snapshot_id: Some(100),
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            properties: HashMap::new(),
            default_spec_id: 0,
            partition_specs: vec![],
            last_partition_id: 1000,
            refs: [(
                "main".to_string(),
                SnapshotRefMetadata {
                    snapshot_id: 100,
                    ref_type: "branch".to_string(),
                },
            )]
            .into_iter()
            .collect(),
            default_sort_order_id: 0,
            sort_orders: vec![],
        }
    }

    #[test]
    fn test_validate_table_uuid_success() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertTableUuid {
            uuid: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("valid uuid"),
        }];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }

    #[test]
    fn test_validate_table_uuid_failure() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertTableUuid {
            uuid: Uuid::new_v4(),
        }];
        assert!(validate_requirements(&metadata, &requirements).is_err());
    }

    #[test]
    fn test_validate_ref_snapshot_id_success() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertRefSnapshotId {
            ref_name: "main".to_string(),
            snapshot_id: Some(100),
        }];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }

    #[test]
    fn test_validate_ref_snapshot_id_failure() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertRefSnapshotId {
            ref_name: "main".to_string(),
            snapshot_id: Some(999),
        }];
        assert!(validate_requirements(&metadata, &requirements).is_err());
    }

    #[test]
    fn test_validate_ref_snapshot_id_fallback_current_snapshot() {
        let mut metadata = test_metadata();
        metadata.refs.clear();
        metadata.current_snapshot_id = Some(100);
        let requirements = vec![UpdateRequirement::AssertRefSnapshotId {
            ref_name: "main".to_string(),
            snapshot_id: Some(100),
        }];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }

    #[test]
    fn test_validate_missing_ref() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertRefSnapshotId {
            ref_name: "missing".to_string(),
            snapshot_id: None, // Expecting ref to not exist
        }];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }

    #[test]
    fn test_validate_current_schema_id_success() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertCurrentSchemaId {
            current_schema_id: 0,
        }];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }

    #[test]
    fn test_validate_current_schema_id_failure() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertCurrentSchemaId {
            current_schema_id: 5,
        }];
        assert!(validate_requirements(&metadata, &requirements).is_err());
    }

    #[test]
    fn test_validate_last_assigned_field_id_success() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertLastAssignedFieldId {
            last_assigned_field_id: 10,
        }];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }

    #[test]
    fn test_validate_default_spec_id_success() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertDefaultSpecId { default_spec_id: 0 }];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }

    #[test]
    fn test_validate_default_sort_order_id_success() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertDefaultSortOrderId {
            default_sort_order_id: 0,
        }];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }

    #[test]
    fn test_validate_last_assigned_partition_id_success() {
        let metadata = test_metadata();
        let requirements = vec![UpdateRequirement::AssertLastAssignedPartitionId {
            last_assigned_partition_id: 1000,
        }];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }

    #[test]
    fn test_validate_multiple_requirements() {
        let metadata = test_metadata();
        let requirements = vec![
            UpdateRequirement::AssertTableUuid {
                uuid: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("valid uuid"),
            },
            UpdateRequirement::AssertRefSnapshotId {
                ref_name: "main".to_string(),
                snapshot_id: Some(100),
            },
            UpdateRequirement::AssertCurrentSchemaId {
                current_schema_id: 0,
            },
        ];
        assert!(validate_requirements(&metadata, &requirements).is_ok());
    }

    #[test]
    fn test_validate_multiple_requirements_one_fails() {
        let metadata = test_metadata();
        let requirements = vec![
            UpdateRequirement::AssertTableUuid {
                uuid: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("valid uuid"),
            },
            UpdateRequirement::AssertRefSnapshotId {
                ref_name: "main".to_string(),
                snapshot_id: Some(999), // This one fails
            },
        ];
        assert!(validate_requirements(&metadata, &requirements).is_err());
    }
}

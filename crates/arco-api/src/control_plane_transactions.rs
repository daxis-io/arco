//! Shared runtime service for single-domain control-plane transactions.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use base64::Engine as _;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use prost::Message;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use ulid::Ulid;

use arco_catalog::idempotency::canonical_request_hash;
use arco_catalog::write_options::WriteOptions;
use arco_catalog::writer::CatalogTransactionCommit;
use arco_catalog::{
    CatalogWriter, ColumnDefinition, RegisterTableInSchemaRequest, TablePatch, Tier1Compactor,
};
use arco_core::ScopedStorage;
use arco_core::catalog_paths::{CatalogDomain, CatalogPaths};
use arco_core::control_plane_transactions::{
    CatalogTxReceipt, CatalogTxRecord, ControlPlaneIdempotencyRecord, ControlPlaneTxDomain,
    ControlPlaneTxKind, ControlPlaneTxPaths, ControlPlaneTxRecord, ControlPlaneTxStatus,
    DomainCommit, OrchestrationTxReceipt, OrchestrationTxRecord, RootTxManifest,
    RootTxManifestDomain, RootTxReceipt, RootTxRecord,
};
use arco_core::lock::{DEFAULT_LOCK_TTL, DistributedLock};
use arco_core::partition::{PartitionKey as CorePartitionKey, ScalarValue as CoreScalarValue};
use arco_core::storage::WritePrecondition;
use arco_flow::orchestration::events::{
    BackfillState, OrchestrationEvent, OrchestrationEventData, PartitionSelector, RunRequest,
    SensorEvalStatus, SourceRef, TaskDef as RuntimeTaskDef, TaskOutcome as RuntimeTaskOutcome,
    TickStatus, TimerType as RuntimeTimerType, TriggerInfo as RuntimeTriggerInfo,
    TriggerSource as RuntimeTriggerSource,
};
use arco_flow::orchestration_compaction_lock_path;
use arco_proto::arco::catalog::v1::{
    CatalogDdlOperation, CreateCatalogOp, CreateSchemaOp, DropTableOp, RegisterTableOp,
    RenameTableOp, UpdateTableOp, catalog_ddl_operation,
};
use arco_proto::arco::common::v1::{
    PartitionKey as ProtoPartitionKey, ScalarValue as ProtoScalarValue,
    TableFormat as ProtoTableFormat, scalar_value,
};
use arco_proto::arco::controlplane::v1::{
    ApplyCatalogDdlRequest, ApplyCatalogDdlResponse, CatalogTxReceipt as ProtoCatalogTxReceipt,
    CatalogTxStatus, CommitOrchestrationBatchRequest, CommitOrchestrationBatchResponse,
    CommitRootTransactionRequest, CommitRootTransactionResponse, DomainCommit as ProtoDomainCommit,
    DomainMutation, GetCatalogTransactionRequest, GetCatalogTransactionResponse,
    GetOrchestrationTransactionRequest, GetOrchestrationTransactionResponse,
    GetRootTransactionRequest, GetRootTransactionResponse, OrchestrationBatchSpec,
    OrchestrationTxReceipt as ProtoOrchestrationTxReceipt, OrchestrationTxStatus,
    RootTxParticipant, RootTxReceipt as ProtoRootTxReceipt, RootTxStatus, TransactionDomain,
    TransactionStatus, domain_mutation,
};
use arco_proto::arco::orchestration::v1::{
    BackfillState as ProtoBackfillState, OrchestrationEventEnvelope,
    PartitionSelector as ProtoPartitionSelector, RunRequest as ProtoRunRequest,
    RunTriggered as ProtoRunTriggered, SensorEvalStatus as ProtoSensorEvalStatus,
    TaskCallbackOutput as ProtoTaskCallbackOutput, TaskDef as ProtoTaskDef,
    TaskError as ProtoTaskError, TaskErrorCategory as ProtoTaskErrorCategory,
    TaskMetrics as ProtoTaskMetrics, TaskOutcome as ProtoTaskOutcome,
    TickStatus as ProtoTickStatus, TimerType as ProtoTimerType, TriggerInfo as ProtoTriggerInfo,
    TriggerSource as ProtoTriggerSource, orchestration_event_envelope, partition_selector,
    trigger_info, trigger_source,
};

use crate::context::RequestContext;
use crate::error::ApiError;
use crate::orchestration_compaction::append_events_and_compact_with_result;
use crate::server::AppState;

/// Service entry point for transaction commit and lookup operations.
pub struct ControlPlaneTransactionService<'a> {
    state: &'a AppState,
    ctx: RequestContext,
    storage: ScopedStorage,
}

impl<'a> ControlPlaneTransactionService<'a> {
    /// Creates a transaction service bound to the current request scope.
    pub fn new(state: &'a AppState, ctx: RequestContext) -> Result<Self, ApiError> {
        let storage = ctx.scoped_storage(state.storage_backend()?)?;
        Ok(Self {
            state,
            ctx,
            storage,
        })
    }

    /// Applies a catalog DDL transaction and returns the visible receipt.
    pub async fn apply_catalog_ddl(
        &self,
        request: ApplyCatalogDdlRequest,
    ) -> Result<ApplyCatalogDdlResponse, ApiError> {
        request
            .validate_contract()
            .map_err(|error| ApiError::bad_request(error.to_string()))?;

        let meta = self.resolve_commit_metadata()?;
        let command = CatalogMutation::from_proto(
            request
                .ddl
                .as_ref()
                .ok_or_else(|| ApiError::bad_request("catalog DDL payload is required"))?,
        )?;
        let outcome = self.execute_catalog_mutation(&meta, command).await?;
        Ok(ApplyCatalogDdlResponse {
            receipt: Some(catalog_receipt_to_proto(&outcome.receipt)),
            repair_pending: outcome.repair_pending,
        })
    }

    /// Looks up a catalog transaction by `tx_id`.
    pub async fn get_catalog_transaction(
        &self,
        request: GetCatalogTransactionRequest,
    ) -> Result<GetCatalogTransactionResponse, ApiError> {
        if request.tx_id.is_empty() {
            return Err(ApiError::bad_request("tx_id is required"));
        }

        let record = self
            .load_record::<CatalogTxReceipt>(ControlPlaneTxDomain::Catalog, request.tx_id.as_str())
            .await?
            .ok_or_else(|| {
                ApiError::not_found(format!("catalog transaction not found: {}", request.tx_id))
            })?;

        Ok(GetCatalogTransactionResponse {
            status: Some(catalog_status_to_proto(&record)),
        })
    }

    /// Commits an orchestration batch and returns the visible receipt.
    pub async fn commit_orchestration_batch(
        &self,
        request: CommitOrchestrationBatchRequest,
    ) -> Result<CommitOrchestrationBatchResponse, ApiError> {
        request
            .validate_contract()
            .map_err(|error| ApiError::bad_request(error.to_string()))?;

        let meta = self.resolve_commit_metadata()?;
        let batch = OrchestrationBatchMutation::from_request(&request)?;
        let outcome = self.execute_orchestration_batch(&meta, batch).await?;
        Ok(CommitOrchestrationBatchResponse {
            receipt: Some(orchestration_receipt_to_proto(&outcome.receipt)),
            repair_pending: outcome.repair_pending,
        })
    }

    /// Looks up an orchestration transaction by `tx_id`.
    pub async fn get_orchestration_transaction(
        &self,
        request: GetOrchestrationTransactionRequest,
    ) -> Result<GetOrchestrationTransactionResponse, ApiError> {
        if request.tx_id.is_empty() {
            return Err(ApiError::bad_request("tx_id is required"));
        }

        let record = self
            .load_record::<OrchestrationTxReceipt>(
                ControlPlaneTxDomain::Orchestration,
                request.tx_id.as_str(),
            )
            .await?
            .ok_or_else(|| {
                ApiError::not_found(format!(
                    "orchestration transaction not found: {}",
                    request.tx_id
                ))
            })?;

        Ok(GetOrchestrationTransactionResponse {
            status: Some(orchestration_status_to_proto(&record)),
        })
    }

    /// Commits a multi-domain root transaction and returns the visible receipt.
    pub async fn commit_root_transaction(
        &self,
        request: CommitRootTransactionRequest,
    ) -> Result<CommitRootTransactionResponse, ApiError> {
        request
            .validate_contract()
            .map_err(|error| ApiError::bad_request(error.to_string()))?;

        let meta = self.resolve_commit_metadata()?;
        let mutations = request
            .mutations
            .iter()
            .map(RootMutation::from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let mut seen_domains = BTreeSet::new();
        for mutation in &mutations {
            if !seen_domains.insert(mutation.domain()) {
                return Err(ApiError::bad_request(format!(
                    "duplicate root mutation for domain '{}'",
                    mutation.domain()
                )));
            }
        }

        let request_hash = root_request_hash(&mutations)?;
        let claim = self
            .claim_idempotency(
                ControlPlaneTxDomain::Root,
                ControlPlaneTxKind::RootCommit,
                &meta,
                &request_hash,
            )
            .await?;

        if let IdempotencyClaim::ExistingVisible(existing) = &claim {
            let record = self
                .resolve_existing_visible_record::<RootTxReceipt>(
                    ControlPlaneTxDomain::Root,
                    existing,
                )
                .await?;
            let receipt = record
                .result
                .clone()
                .ok_or_else(|| ApiError::internal("visible root transaction is missing result"))?;
            return Ok(CommitRootTransactionResponse {
                receipt: Some(root_receipt_to_proto(&receipt)),
                repair_pending: record.repair_pending,
            });
        }
        if let IdempotencyClaim::ExistingInProgress { tx_id } = &claim {
            return Err(ApiError::conflict(format!(
                "transaction is already prepared for idempotency key '{}'; poll GetRootTransaction for tx_id '{}'",
                meta.idempotency_key, tx_id
            )));
        }

        let tx_id = claim.tx_id().to_string();
        let prepared_at = Utc::now();
        let root_lock_path = ControlPlaneTxPaths::root_lock();
        let prepared = RootTxRecord {
            tx_id: tx_id.clone(),
            kind: ControlPlaneTxKind::RootCommit,
            status: ControlPlaneTxStatus::Prepared,
            repair_pending: false,
            request_id: meta.request_id.clone(),
            idempotency_key: meta.idempotency_key.clone(),
            request_hash,
            lock_path: root_lock_path.clone(),
            fencing_token: 0,
            prepared_at,
            visible_at: None,
            result: None,
        };
        self.store_prepared(ControlPlaneTxDomain::Root, &prepared)
            .await?;

        let root_lock = DistributedLock::new(self.storage.backend().clone(), &root_lock_path);
        let guard = match root_lock.acquire(DEFAULT_LOCK_TTL, 10).await {
            Ok(guard) => guard,
            Err(error) => {
                self.abort_transaction(ControlPlaneTxDomain::Root, &tx_id)
                    .await;
                return Err(ApiError::conflict(format!(
                    "failed to acquire root transaction lock: {error}"
                )));
            }
        };
        let fencing_token = guard.fencing_token().sequence();

        let result = async {
            let mut repair_pending = false;
            let mut domain_commits = Vec::with_capacity(mutations.len());
            let mut manifest_domains = BTreeMap::new();

            for mutation in mutations {
                let participant_meta = self.root_participant_metadata(&meta, mutation.domain());
                match mutation {
                    RootMutation::Catalog(command) => {
                        let outcome = self
                            .execute_catalog_mutation(&participant_meta, command)
                            .await?;
                        repair_pending |= outcome.repair_pending;
                        let commit = root_domain_commit_from_catalog(&outcome.receipt);
                        manifest_domains.insert(
                            ControlPlaneTxDomain::Catalog,
                            RootTxManifestDomain {
                                manifest_id: commit.manifest_id.clone(),
                                manifest_path: commit.manifest_path.clone(),
                                commit_id: commit.commit_id.clone(),
                            },
                        );
                        domain_commits.push(commit);
                    }
                    RootMutation::Orchestration(batch) => {
                        let outcome = self
                            .execute_orchestration_batch(&participant_meta, batch)
                            .await?;
                        repair_pending |= outcome.repair_pending;
                        let commit = root_domain_commit_from_orchestration(&outcome.receipt);
                        manifest_domains.insert(
                            ControlPlaneTxDomain::Orchestration,
                            RootTxManifestDomain {
                                manifest_id: commit.manifest_id.clone(),
                                manifest_path: commit.manifest_path.clone(),
                                commit_id: commit.commit_id.clone(),
                            },
                        );
                        domain_commits.push(commit);
                    }
                }
            }

            let visible_at = Utc::now();
            let super_manifest_path = ControlPlaneTxPaths::root_super_manifest(&tx_id);
            let super_manifest = RootTxManifest {
                tx_id: tx_id.clone(),
                fencing_token,
                published_at: visible_at,
                domains: manifest_domains,
            };
            match self
                .write_json(
                    &super_manifest_path,
                    &super_manifest,
                    WritePrecondition::DoesNotExist,
                )
                .await?
            {
                WriteOutcome::Written => {}
                WriteOutcome::PreconditionFailed => {
                    return Err(ApiError::conflict(format!(
                        "root super-manifest already exists: {super_manifest_path}"
                    )));
                }
            }

            let receipt = RootTxReceipt {
                tx_id: tx_id.clone(),
                root_commit_id: Ulid::new().to_string(),
                super_manifest_path: super_manifest_path.clone(),
                domain_commits,
                read_token: format!("root:{tx_id}"),
                visible_at,
            };

            self.finalize_visible(
                ControlPlaneTxDomain::Root,
                &tx_id,
                root_lock_path.clone(),
                fencing_token,
                repair_pending,
                visible_at,
                receipt.clone(),
            )
            .await?;

            let mut root_repair_pending = repair_pending;
            match self
                .write_json(
                    &ControlPlaneTxPaths::root_commit_receipt(&receipt.root_commit_id),
                    &receipt,
                    WritePrecondition::DoesNotExist,
                )
                .await
            {
                Ok(WriteOutcome::Written) => {}
                Ok(WriteOutcome::PreconditionFailed) => {
                    root_repair_pending = true;
                    tracing::warn!(
                        tx_id,
                        root_commit_id = %receipt.root_commit_id,
                        "root commit receipt already exists after visibility; leaving repair pending"
                    );
                }
                Err(error) => {
                    root_repair_pending = true;
                    tracing::warn!(
                        error = ?error,
                        tx_id,
                        root_commit_id = %receipt.root_commit_id,
                        "failed to persist root commit receipt after root visibility"
                    );
                }
            }
            if root_repair_pending && !repair_pending {
                if let Err(error) = self
                    .mark_visible_repair_pending::<RootTxReceipt>(
                        ControlPlaneTxDomain::Root,
                        &tx_id,
                    )
                    .await
                {
                    tracing::warn!(
                        error = ?error,
                        tx_id,
                        "failed to mark visible root transaction repair_pending after audit receipt failure"
                    );
                }
            }

            Ok(TxExecutionOutcome {
                receipt,
                repair_pending: root_repair_pending,
            })
        }
        .await;

        if let Err(error) = guard.release().await {
            tracing::warn!(
                error = %error,
                tx_id,
                "failed to release root transaction lock after execution; relying on TTL cleanup"
            );
        }

        match result {
            Ok(outcome) => Ok(CommitRootTransactionResponse {
                receipt: Some(root_receipt_to_proto(&outcome.receipt)),
                repair_pending: outcome.repair_pending,
            }),
            Err(error) => {
                self.abort_transaction(ControlPlaneTxDomain::Root, &tx_id)
                    .await;
                Err(error)
            }
        }
    }

    /// Looks up a root transaction by `tx_id`.
    pub async fn get_root_transaction(
        &self,
        request: GetRootTransactionRequest,
    ) -> Result<GetRootTransactionResponse, ApiError> {
        if request.tx_id.is_empty() {
            return Err(ApiError::bad_request("tx_id is required"));
        }

        let record = self
            .load_record::<RootTxReceipt>(ControlPlaneTxDomain::Root, request.tx_id.as_str())
            .await?
            .ok_or_else(|| {
                ApiError::not_found(format!("root transaction not found: {}", request.tx_id))
            })?;

        Ok(GetRootTransactionResponse {
            status: Some(root_status_to_proto(&record)),
        })
    }

    async fn execute_catalog_mutation(
        &self,
        meta: &ResolvedRequestMetadata,
        command: CatalogMutation,
    ) -> Result<TxExecutionOutcome<CatalogTxReceipt>, ApiError> {
        let request_hash = command.request_hash()?;
        let claim = self
            .claim_idempotency(
                ControlPlaneTxDomain::Catalog,
                ControlPlaneTxKind::CatalogDdl,
                meta,
                &request_hash,
            )
            .await?;

        if let IdempotencyClaim::ExistingVisible(existing) = &claim {
            let record = self
                .resolve_existing_visible_record::<CatalogTxReceipt>(
                    ControlPlaneTxDomain::Catalog,
                    existing,
                )
                .await?;
            let receipt = record.result.clone().ok_or_else(|| {
                ApiError::internal("visible catalog transaction is missing result")
            })?;
            return Ok(TxExecutionOutcome {
                receipt,
                repair_pending: record.repair_pending,
            });
        }
        if let IdempotencyClaim::ExistingInProgress { tx_id } = &claim {
            return Err(ApiError::conflict(format!(
                "transaction is already prepared for idempotency key '{}'; poll GetCatalogTransaction for tx_id '{}'",
                meta.idempotency_key, tx_id
            )));
        }

        let tx_id = claim.tx_id().to_string();
        let prepared_at = Utc::now();
        let prepared = CatalogTxRecord {
            tx_id: tx_id.clone(),
            kind: ControlPlaneTxKind::CatalogDdl,
            status: ControlPlaneTxStatus::Prepared,
            repair_pending: false,
            request_id: meta.request_id.clone(),
            idempotency_key: meta.idempotency_key.clone(),
            request_hash,
            lock_path: CatalogPaths::domain_lock(CatalogDomain::Catalog),
            fencing_token: 0,
            prepared_at,
            visible_at: None,
            result: None,
        };
        self.store_prepared(ControlPlaneTxDomain::Catalog, &prepared)
            .await?;

        let writer = self.catalog_writer()?;
        if let Err(error) = writer.initialize().await.map_err(ApiError::from) {
            self.abort_transaction(ControlPlaneTxDomain::Catalog, &tx_id)
                .await;
            return Err(error);
        }
        let options = self.catalog_write_options(meta);
        let commit = match command
            .apply(&writer, options)
            .await
            .map_err(ApiError::from)
        {
            Ok(commit) => commit,
            Err(error) => {
                self.abort_transaction(ControlPlaneTxDomain::Catalog, &tx_id)
                    .await;
                return Err(error);
            }
        };

        let visible_at = Utc::now();
        let read_token = format!("catalog:{}", commit.manifest_id);
        let receipt = CatalogTxReceipt {
            tx_id: tx_id.clone(),
            event_id: commit.event_id,
            commit_id: commit.commit_id,
            manifest_id: commit.manifest_id,
            snapshot_version: commit.snapshot_version,
            pointer_version: commit.pointer_version,
            read_token,
            visible_at,
        };

        self.finalize_visible(
            ControlPlaneTxDomain::Catalog,
            &tx_id,
            commit.lock_path,
            commit.fencing_token,
            commit.repair_pending,
            visible_at,
            receipt.clone(),
        )
        .await?;

        Ok(TxExecutionOutcome {
            receipt,
            repair_pending: commit.repair_pending,
        })
    }

    async fn execute_orchestration_batch(
        &self,
        meta: &ResolvedRequestMetadata,
        batch: OrchestrationBatchMutation,
    ) -> Result<TxExecutionOutcome<OrchestrationTxReceipt>, ApiError> {
        let request_hash = batch.request_hash()?;
        let events = batch.events(meta)?;
        let claim = self
            .claim_idempotency(
                ControlPlaneTxDomain::Orchestration,
                ControlPlaneTxKind::OrchestrationBatch,
                meta,
                &request_hash,
            )
            .await?;

        if let IdempotencyClaim::ExistingVisible(existing) = &claim {
            let record = self
                .resolve_existing_visible_record::<OrchestrationTxReceipt>(
                    ControlPlaneTxDomain::Orchestration,
                    existing,
                )
                .await?;
            let receipt = record.result.clone().ok_or_else(|| {
                ApiError::internal("visible orchestration transaction is missing result")
            })?;
            return Ok(TxExecutionOutcome {
                receipt,
                repair_pending: record.repair_pending,
            });
        }
        if let IdempotencyClaim::ExistingInProgress { tx_id } = &claim {
            return Err(ApiError::conflict(format!(
                "transaction is already prepared for idempotency key '{}'; poll GetOrchestrationTransaction for tx_id '{}'",
                meta.idempotency_key, tx_id
            )));
        }

        let tx_id = claim.tx_id().to_string();
        let prepared_at = Utc::now();
        let prepared = OrchestrationTxRecord {
            tx_id: tx_id.clone(),
            kind: ControlPlaneTxKind::OrchestrationBatch,
            status: ControlPlaneTxStatus::Prepared,
            repair_pending: false,
            request_id: meta.request_id.clone(),
            idempotency_key: meta.idempotency_key.clone(),
            request_hash,
            lock_path: orchestration_compaction_lock_path().to_string(),
            fencing_token: 0,
            prepared_at,
            visible_at: None,
            result: None,
        };
        self.store_prepared(ControlPlaneTxDomain::Orchestration, &prepared)
            .await?;

        let commit = match append_events_and_compact_with_result(
            &self.state.config,
            self.storage.clone(),
            events,
            Some(meta.request_id.as_str()),
        )
        .await
        {
            Ok(commit) => commit,
            Err(error) => {
                self.abort_transaction(ControlPlaneTxDomain::Orchestration, &tx_id)
                    .await;
                return Err(error);
            }
        };

        let visible_at = Utc::now();
        let commit_id = Ulid::new().to_string();
        let receipt = OrchestrationTxReceipt {
            tx_id: tx_id.clone(),
            commit_id: commit_id.clone(),
            manifest_id: commit.manifest_id.clone(),
            revision_ulid: commit.manifest_revision,
            delta_id: commit.delta_id.unwrap_or_default(),
            pointer_version: commit.pointer_version,
            events_processed: commit.events_processed,
            read_token: format!("orchestration:{}", commit.manifest_id),
            visible_at,
        };
        let mut repair_pending = commit.repair_pending;
        match self
            .write_json(
                &ControlPlaneTxPaths::orchestration_commit_receipt(&commit_id),
                &receipt,
                WritePrecondition::DoesNotExist,
            )
            .await
        {
            Ok(WriteOutcome::Written) => {}
            Ok(WriteOutcome::PreconditionFailed) => {
                repair_pending = true;
                tracing::warn!(
                    tx_id,
                    commit_id = %receipt.commit_id,
                    "orchestration commit receipt already exists after visibility; leaving repair pending"
                );
            }
            Err(error) => {
                repair_pending = true;
                tracing::warn!(
                    error = ?error,
                    tx_id,
                    commit_id = %receipt.commit_id,
                    "failed to persist orchestration commit receipt after visibility"
                );
            }
        }

        self.finalize_visible(
            ControlPlaneTxDomain::Orchestration,
            &tx_id,
            commit.lock_path,
            commit.fencing_token,
            repair_pending,
            visible_at,
            receipt.clone(),
        )
        .await?;

        Ok(TxExecutionOutcome {
            receipt,
            repair_pending,
        })
    }

    fn root_participant_metadata(
        &self,
        meta: &ResolvedRequestMetadata,
        domain: ControlPlaneTxDomain,
    ) -> ResolvedRequestMetadata {
        ResolvedRequestMetadata {
            tenant: meta.tenant.clone(),
            workspace: meta.workspace.clone(),
            request_id: meta.request_id.clone(),
            idempotency_key: format!("root:{}:{}", meta.idempotency_key, domain.as_str()),
        }
    }

    fn catalog_writer(&self) -> Result<CatalogWriter, ApiError> {
        let compactor = self
            .state
            .sync_compactor()
            .unwrap_or_else(|| Arc::new(Tier1Compactor::new(self.storage.clone())));
        Ok(CatalogWriter::new(self.storage.clone()).with_sync_compactor(compactor))
    }

    fn catalog_write_options(&self, meta: &ResolvedRequestMetadata) -> WriteOptions {
        WriteOptions::default()
            .with_actor(format!("api:{}", meta.tenant))
            .with_request_id(meta.request_id.as_str())
            .with_idempotency_key(meta.idempotency_key.as_str())
    }

    fn resolve_commit_metadata(&self) -> Result<ResolvedRequestMetadata, ApiError> {
        let idempotency_key = self
            .ctx
            .idempotency_key
            .clone()
            .ok_or_else(|| ApiError::bad_request("idempotency_key is required"))?;
        if idempotency_key.is_empty() {
            return Err(ApiError::bad_request("idempotency_key is required"));
        }

        Ok(ResolvedRequestMetadata {
            tenant: self.ctx.tenant.clone(),
            workspace: self.ctx.workspace.clone(),
            request_id: self.ctx.request_id.clone(),
            idempotency_key,
        })
    }

    async fn claim_idempotency(
        &self,
        domain: ControlPlaneTxDomain,
        kind: ControlPlaneTxKind,
        meta: &ResolvedRequestMetadata,
        request_hash: &str,
    ) -> Result<IdempotencyClaim, ApiError> {
        let path = ControlPlaneTxPaths::idempotency(domain, meta.idempotency_key.as_str());
        for _ in 0..4 {
            let claim = ControlPlaneIdempotencyRecord {
                tx_id: Ulid::new().to_string(),
                kind,
                request_id: meta.request_id.clone(),
                idempotency_key: meta.idempotency_key.clone(),
                request_hash: request_hash.to_string(),
                created_at: Utc::now(),
                visible_at: None,
                tx_record: None,
            };

            match self
                .write_json(&path, &claim, WritePrecondition::DoesNotExist)
                .await?
            {
                WriteOutcome::Written => return Ok(IdempotencyClaim::Fresh(claim)),
                WriteOutcome::PreconditionFailed => {
                    let existing = self
                        .load_json_with_version_required::<ControlPlaneIdempotencyRecord>(&path)
                        .await?
                        .ok_or_else(|| {
                            ApiError::internal(
                                "idempotency marker disappeared after claim conflict",
                            )
                        })?;
                    if existing.value.request_hash != request_hash {
                        return Err(ApiError::conflict(
                            "Idempotency-Key already used with different request body",
                        ));
                    }
                    match self
                        .classify_existing_idempotency(domain, &existing.value)
                        .await?
                    {
                        ExistingClaimDisposition::Visible => {
                            return Ok(IdempotencyClaim::ExistingVisible(existing.value));
                        }
                        ExistingClaimDisposition::InProgress => {
                            return Ok(IdempotencyClaim::ExistingInProgress {
                                tx_id: existing.value.tx_id,
                            });
                        }
                        ExistingClaimDisposition::Retryable => {
                            let replacement = ControlPlaneIdempotencyRecord {
                                tx_id: Ulid::new().to_string(),
                                kind,
                                request_id: meta.request_id.clone(),
                                idempotency_key: meta.idempotency_key.clone(),
                                request_hash: request_hash.to_string(),
                                created_at: Utc::now(),
                                visible_at: None,
                                tx_record: None,
                            };
                            match self
                                .write_json(
                                    &path,
                                    &replacement,
                                    WritePrecondition::MatchesVersion(existing.version),
                                )
                                .await?
                            {
                                WriteOutcome::Written => {
                                    return Ok(IdempotencyClaim::Fresh(replacement));
                                }
                                WriteOutcome::PreconditionFailed => continue,
                            }
                        }
                    }
                }
            }
        }

        Err(ApiError::conflict(
            "failed to claim idempotency key after concurrent retries",
        ))
    }

    async fn store_prepared<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        record: &ControlPlaneTxRecord<TResult>,
    ) -> Result<(), ApiError>
    where
        TResult: Serialize,
    {
        let path = ControlPlaneTxPaths::record(domain, record.tx_id.as_str());
        match self
            .write_json(&path, record, WritePrecondition::DoesNotExist)
            .await?
        {
            WriteOutcome::Written => Ok(()),
            WriteOutcome::PreconditionFailed => Err(ApiError::conflict(format!(
                "transaction record already exists: {}",
                record.tx_id
            ))),
        }
    }

    async fn finalize_visible<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        tx_id: &str,
        lock_path: String,
        fencing_token: u64,
        repair_pending: bool,
        visible_at: DateTime<Utc>,
        result: TResult,
    ) -> Result<(), ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone,
    {
        let path = ControlPlaneTxPaths::record(domain, tx_id);
        let mut record: ControlPlaneTxRecord<TResult> = self
            .load_json_required(&path)
            .await?
            .ok_or_else(|| ApiError::internal(format!("transaction record not found: {tx_id}")))?;
        record.status = ControlPlaneTxStatus::Visible;
        record.lock_path = lock_path;
        record.fencing_token = fencing_token;
        record.repair_pending = repair_pending;
        record.visible_at = Some(visible_at);
        record.result = Some(result.clone());

        self.persist_visible_record_and_idempotency(domain, &record)
            .await
    }

    async fn mark_visible_repair_pending<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        tx_id: &str,
    ) -> Result<(), ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone,
    {
        let path = ControlPlaneTxPaths::record(domain, tx_id);
        let mut record: ControlPlaneTxRecord<TResult> = self
            .load_json_required(&path)
            .await?
            .ok_or_else(|| ApiError::internal(format!("transaction record not found: {tx_id}")))?;
        if record.status != ControlPlaneTxStatus::Visible {
            return Err(ApiError::internal(format!(
                "cannot mark non-visible transaction repair_pending: {tx_id}"
            )));
        }
        if record.repair_pending {
            return Ok(());
        }

        record.repair_pending = true;
        self.persist_visible_record_and_idempotency(domain, &record)
            .await
    }

    async fn persist_visible_record_and_idempotency<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        record: &ControlPlaneTxRecord<TResult>,
    ) -> Result<(), ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone,
    {
        let path = ControlPlaneTxPaths::record(domain, record.tx_id.as_str());
        let idem_path = ControlPlaneTxPaths::idempotency(domain, record.idempotency_key.as_str());
        let mut idem: ControlPlaneIdempotencyRecord = self
            .load_json_required(&idem_path)
            .await?
            .ok_or_else(|| ApiError::internal("idempotency record missing during finalize"))?;
        idem.visible_at = record.visible_at;
        idem.tx_record = Some(serde_json::to_value(record).map_err(|error| {
            ApiError::internal(format!(
                "failed to encode visible transaction record for idempotency replay: {error}"
            ))
        })?);

        let tx_write = self
            .write_json(&path, record, WritePrecondition::None)
            .await;
        let idem_write = self
            .write_json(&idem_path, &idem, WritePrecondition::None)
            .await;
        match (tx_write, idem_write) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(error), Ok(_)) | (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(cache_error)) => {
                tracing::warn!(
                    error = ?cache_error,
                    tx_id = record.tx_id.as_str(),
                    domain = %domain,
                    "failed to persist visible transaction record to both durable locations"
                );
                Err(error)
            }
        }
    }

    async fn load_record<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        tx_id: &str,
    ) -> Result<Option<ControlPlaneTxRecord<TResult>>, ApiError>
    where
        TResult: DeserializeOwned,
    {
        self.load_json_required(&ControlPlaneTxPaths::record(domain, tx_id))
            .await
    }

    async fn resolve_existing_visible_record<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        existing: &ControlPlaneIdempotencyRecord,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone,
    {
        if let Some(record) = self.load_record(domain, existing.tx_id.as_str()).await? {
            if record.status == ControlPlaneTxStatus::Visible {
                if existing.visible_at != record.visible_at || existing.tx_record.is_none() {
                    if let Err(error) = self
                        .persist_visible_record_and_idempotency(domain, &record)
                        .await
                    {
                        tracing::warn!(
                            error = ?error,
                            tx_id = existing.tx_id.as_str(),
                            domain = %domain,
                            "failed to repair visible idempotency record from transaction record"
                        );
                    }
                }
                return Ok(record);
            }
        }

        let record = self.record_from_idempotency(existing)?.ok_or_else(|| {
            ApiError::internal(format!(
                "{domain} transaction record missing for tx_id '{}'",
                existing.tx_id
            ))
        })?;
        if record.status != ControlPlaneTxStatus::Visible {
            return Err(ApiError::conflict(format!(
                "transaction is already prepared for tx_id '{}'",
                existing.tx_id
            )));
        }

        let path = ControlPlaneTxPaths::record(domain, existing.tx_id.as_str());
        if let Err(error) = self
            .write_json(&path, &record, WritePrecondition::None)
            .await
        {
            tracing::warn!(
                error = ?error,
                tx_id = existing.tx_id.as_str(),
                domain = %domain,
                "failed to repair visible transaction record from idempotency cache"
            );
        }

        Ok(record)
    }

    async fn classify_existing_idempotency(
        &self,
        domain: ControlPlaneTxDomain,
        existing: &ControlPlaneIdempotencyRecord,
    ) -> Result<ExistingClaimDisposition, ApiError> {
        let stale_timeout = self.state.config.idempotency_stale_timeout();
        let now = Utc::now();
        if existing.tx_record.is_some() {
            return Ok(ExistingClaimDisposition::Visible);
        }
        let record = self
            .load_json_required::<TxRecordLifecycle>(&ControlPlaneTxPaths::record(
                domain,
                existing.tx_id.as_str(),
            ))
            .await?;
        let is_retryable = match record {
            Some(record) => match record.status {
                ControlPlaneTxStatus::Visible => return Ok(ExistingClaimDisposition::Visible),
                ControlPlaneTxStatus::Aborted => true,
                ControlPlaneTxStatus::Prepared => record.prepared_at + stale_timeout <= now,
            },
            None => existing.created_at + stale_timeout <= now,
        };

        if is_retryable {
            Ok(ExistingClaimDisposition::Retryable)
        } else {
            Ok(ExistingClaimDisposition::InProgress)
        }
    }

    async fn abort_transaction(&self, domain: ControlPlaneTxDomain, tx_id: &str) {
        if let Err(error) = self.try_abort_transaction(domain, tx_id).await {
            tracing::warn!(
                error = ?error,
                tx_id,
                domain = %domain,
                "failed to mark transaction aborted"
            );
        }
    }

    async fn try_abort_transaction(
        &self,
        domain: ControlPlaneTxDomain,
        tx_id: &str,
    ) -> Result<(), ApiError> {
        let path = ControlPlaneTxPaths::record(domain, tx_id);
        let Some(stored) = self
            .load_json_with_version_required::<ControlPlaneTxRecord<serde_json::Value>>(&path)
            .await?
        else {
            return Ok(());
        };

        if stored.value.status == ControlPlaneTxStatus::Visible {
            return Ok(());
        }

        let mut record = stored.value;
        record.status = ControlPlaneTxStatus::Aborted;
        record.visible_at = None;
        record.result = None;

        match self
            .write_json(
                &path,
                &record,
                WritePrecondition::MatchesVersion(stored.version),
            )
            .await?
        {
            WriteOutcome::Written | WriteOutcome::PreconditionFailed => Ok(()),
        }
    }

    async fn load_json_required<T>(&self, path: &str) -> Result<Option<T>, ApiError>
    where
        T: DeserializeOwned,
    {
        match self.storage.get_raw(path).await {
            Ok(bytes) => serde_json::from_slice::<T>(bytes.as_ref())
                .map(Some)
                .map_err(|error| {
                    ApiError::internal(format!("failed to decode JSON at '{path}': {error}"))
                }),
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                Ok(None)
            }
            Err(error) => Err(ApiError::from(error)),
        }
    }

    async fn load_json_with_version_required<T>(
        &self,
        path: &str,
    ) -> Result<Option<VersionedValue<T>>, ApiError>
    where
        T: DeserializeOwned,
    {
        match self.storage.head_raw(path).await {
            Ok(Some(meta)) => {
                Ok(self
                    .load_json_required(path)
                    .await?
                    .map(|value| VersionedValue {
                        value,
                        version: meta.version,
                    }))
            }
            Ok(None) => Ok(None),
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                Ok(None)
            }
            Err(error) => Err(ApiError::from(error)),
        }
    }

    fn record_from_idempotency<TResult>(
        &self,
        record: &ControlPlaneIdempotencyRecord,
    ) -> Result<Option<ControlPlaneTxRecord<TResult>>, ApiError>
    where
        TResult: DeserializeOwned,
    {
        record
            .tx_record
            .as_ref()
            .map(|value| {
                serde_json::from_value::<ControlPlaneTxRecord<TResult>>(value.clone()).map_err(
                    |error| {
                        ApiError::internal(format!(
                            "failed to decode cached idempotency transaction record: {error}"
                        ))
                    },
                )
            })
            .transpose()
    }

    async fn write_json<T>(
        &self,
        path: &str,
        value: &T,
        precondition: WritePrecondition,
    ) -> Result<WriteOutcome, ApiError>
    where
        T: Serialize,
    {
        let payload = serde_json::to_vec(value)
            .map(Bytes::from)
            .map_err(|error| ApiError::internal(format!("failed to encode JSON: {error}")))?;
        match self.storage.put_raw(path, payload, precondition).await? {
            arco_core::WriteResult::Success { .. } => Ok(WriteOutcome::Written),
            arco_core::WriteResult::PreconditionFailed { .. } => {
                Ok(WriteOutcome::PreconditionFailed)
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ResolvedRequestMetadata {
    tenant: String,
    workspace: String,
    request_id: String,
    idempotency_key: String,
}

#[derive(Debug, Clone)]
struct TxExecutionOutcome<T> {
    receipt: T,
    repair_pending: bool,
}

#[derive(Debug, Clone)]
enum CatalogMutation {
    CreateCatalog {
        name: String,
        description: Option<String>,
    },
    CreateSchema {
        catalog_name: String,
        schema_name: String,
        description: Option<String>,
    },
    RegisterTable {
        catalog_name: String,
        schema_name: String,
        table_name: String,
        description: Option<String>,
        location: Option<String>,
        format: Option<String>,
        columns: Vec<ColumnDefinition>,
    },
    UpdateTable {
        catalog_name: String,
        schema_name: String,
        table_name: String,
        description: Option<Option<String>>,
        location: Option<Option<String>>,
        format: Option<Option<String>>,
    },
    DropTable {
        catalog_name: String,
        schema_name: String,
        table_name: String,
    },
    RenameTable {
        catalog_name: String,
        schema_name: String,
        old_table_name: String,
        new_table_name: String,
    },
}

#[derive(Debug, Clone)]
struct OrchestrationBatchMutation {
    events: Vec<OrchestrationEventEnvelope>,
}

impl OrchestrationBatchMutation {
    fn from_request(request: &CommitOrchestrationBatchRequest) -> Result<Self, ApiError> {
        Self::from_parts(&request.events)
    }

    fn from_spec(spec: &OrchestrationBatchSpec) -> Result<Self, ApiError> {
        Self::from_parts(&spec.events)
    }

    fn from_parts(events: &[OrchestrationEventEnvelope]) -> Result<Self, ApiError> {
        if events.is_empty() {
            return Err(ApiError::bad_request(
                "orchestration batch must include at least one event",
            ));
        }

        Ok(Self {
            events: events.to_vec(),
        })
    }

    fn request_hash_value(&self) -> serde_json::Value {
        serde_json::json!({
            "events": self.events.iter().map(orchestration_event_hash_value).collect::<Vec<_>>(),
        })
    }

    fn request_hash(&self) -> Result<String, ApiError> {
        prefixed_request_hash(&self.request_hash_value()).map_err(|error| {
            ApiError::bad_request(format!("failed to hash orchestration request: {error}"))
        })
    }

    fn events(&self, meta: &ResolvedRequestMetadata) -> Result<Vec<OrchestrationEvent>, ApiError> {
        self.events
            .iter()
            .map(|event| envelope_to_event(meta, event))
            .collect()
    }
}

#[derive(Debug, Clone)]
enum RootMutation {
    Catalog(CatalogMutation),
    Orchestration(OrchestrationBatchMutation),
}

impl RootMutation {
    fn from_proto(mutation: &DomainMutation) -> Result<Self, ApiError> {
        match mutation.kind.as_ref() {
            Some(domain_mutation::Kind::Catalog(operation)) => {
                Ok(Self::Catalog(CatalogMutation::from_proto(operation)?))
            }
            Some(domain_mutation::Kind::Orchestration(spec)) => Ok(Self::Orchestration(
                OrchestrationBatchMutation::from_spec(spec)?,
            )),
            None => Err(ApiError::bad_request("root mutation kind is required")),
        }
    }

    const fn domain(&self) -> ControlPlaneTxDomain {
        match self {
            Self::Catalog(_) => ControlPlaneTxDomain::Catalog,
            Self::Orchestration(_) => ControlPlaneTxDomain::Orchestration,
        }
    }

    fn request_hash_value(&self) -> Result<serde_json::Value, ApiError> {
        match self {
            Self::Catalog(mutation) => Ok(serde_json::json!({
                "domain": "catalog",
                "request": mutation.request_hash_value()?,
            })),
            Self::Orchestration(batch) => Ok(serde_json::json!({
                "domain": "orchestration",
                "request": batch.request_hash_value(),
            })),
        }
    }
}

impl CatalogMutation {
    fn from_proto(operation: &CatalogDdlOperation) -> Result<Self, ApiError> {
        match operation.op.as_ref() {
            Some(catalog_ddl_operation::Op::CreateCatalog(CreateCatalogOp {
                name,
                description,
            })) => Ok(Self::CreateCatalog {
                name: name.clone(),
                description: description.clone(),
            }),
            Some(catalog_ddl_operation::Op::CreateSchema(CreateSchemaOp {
                catalog_name,
                schema_name,
                description,
            })) => Ok(Self::CreateSchema {
                catalog_name: catalog_name.clone(),
                schema_name: schema_name.clone(),
                description: description.clone(),
            }),
            Some(catalog_ddl_operation::Op::RegisterTable(RegisterTableOp {
                catalog_name,
                schema_name,
                table_name,
                description,
                location,
                format,
                columns,
            })) => Ok(Self::RegisterTable {
                catalog_name: catalog_name.clone(),
                schema_name: schema_name.clone(),
                table_name: table_name.clone(),
                description: description.clone(),
                location: location.clone(),
                format: parse_table_format(*format)?,
                columns: columns
                    .iter()
                    .map(|column| ColumnDefinition {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        is_nullable: column.is_nullable,
                        description: column.description.clone(),
                    })
                    .collect(),
            }),
            Some(catalog_ddl_operation::Op::UpdateTable(UpdateTableOp {
                catalog_name,
                schema_name,
                table_name,
                description,
                location,
                format,
            })) => Ok(Self::UpdateTable {
                catalog_name: catalog_name.clone(),
                schema_name: schema_name.clone(),
                table_name: table_name.clone(),
                description: description.clone().map(Some),
                location: location.clone().map(Some),
                format: format
                    .as_ref()
                    .map(|value| parse_table_format(*value))
                    .transpose()?,
            }),
            Some(catalog_ddl_operation::Op::DropTable(DropTableOp {
                catalog_name,
                schema_name,
                table_name,
            })) => Ok(Self::DropTable {
                catalog_name: catalog_name.clone(),
                schema_name: schema_name.clone(),
                table_name: table_name.clone(),
            }),
            Some(catalog_ddl_operation::Op::RenameTable(RenameTableOp {
                catalog_name,
                schema_name,
                old_table_name,
                new_table_name,
            })) => Ok(Self::RenameTable {
                catalog_name: catalog_name.clone(),
                schema_name: schema_name.clone(),
                old_table_name: old_table_name.clone(),
                new_table_name: new_table_name.clone(),
            }),
            None => Err(ApiError::bad_request("catalog DDL operation is required")),
        }
    }

    fn request_hash(&self) -> Result<String, ApiError> {
        prefixed_request_hash(&self.request_hash_value()?).map_err(|error| {
            ApiError::bad_request(format!("failed to hash catalog request: {error}"))
        })
    }

    fn request_hash_value(&self) -> Result<serde_json::Value, ApiError> {
        Ok(match self {
            Self::CreateCatalog { name, description } => serde_json::json!({
                "type": "create_catalog",
                "name": name,
                "description": description,
            }),
            Self::CreateSchema {
                catalog_name,
                schema_name,
                description,
            } => serde_json::json!({
                "type": "create_schema",
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "description": description,
            }),
            Self::RegisterTable {
                catalog_name,
                schema_name,
                table_name,
                description,
                location,
                format,
                columns,
            } => serde_json::json!({
                "type": "register_table",
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "table_name": table_name,
                "description": description,
                "location": location,
                "format": format,
                "columns": columns.iter().map(|column| serde_json::json!({
                    "name": column.name,
                    "data_type": column.data_type,
                    "is_nullable": column.is_nullable,
                    "description": column.description,
                })).collect::<Vec<_>>(),
            }),
            Self::UpdateTable {
                catalog_name,
                schema_name,
                table_name,
                description,
                location,
                format,
            } => {
                let mut value = serde_json::Map::new();
                value.insert(
                    "type".to_string(),
                    serde_json::Value::String("update_table".to_string()),
                );
                value.insert(
                    "catalog_name".to_string(),
                    serde_json::Value::String(catalog_name.clone()),
                );
                value.insert(
                    "schema_name".to_string(),
                    serde_json::Value::String(schema_name.clone()),
                );
                value.insert(
                    "table_name".to_string(),
                    serde_json::Value::String(table_name.clone()),
                );
                if let Some(description) = description {
                    value.insert(
                        "description".to_string(),
                        serde_json::to_value(description).map_err(|error| {
                            ApiError::internal(format!(
                                "failed to serialize update_table description for hashing: {error}"
                            ))
                        })?,
                    );
                }
                if let Some(location) = location {
                    value.insert(
                        "location".to_string(),
                        serde_json::to_value(location).map_err(|error| {
                            ApiError::internal(format!(
                                "failed to serialize update_table location for hashing: {error}"
                            ))
                        })?,
                    );
                }
                if let Some(format) = format {
                    value.insert(
                        "format".to_string(),
                        serde_json::to_value(format).map_err(|error| {
                            ApiError::internal(format!(
                                "failed to serialize update_table format for hashing: {error}"
                            ))
                        })?,
                    );
                }
                serde_json::Value::Object(value)
            }
            Self::DropTable {
                catalog_name,
                schema_name,
                table_name,
            } => serde_json::json!({
                "type": "drop_table",
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "table_name": table_name,
            }),
            Self::RenameTable {
                catalog_name,
                schema_name,
                old_table_name,
                new_table_name,
            } => serde_json::json!({
                "type": "rename_table",
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "old_table_name": old_table_name,
                "new_table_name": new_table_name,
            }),
        })
    }

    async fn apply(
        &self,
        writer: &CatalogWriter,
        options: WriteOptions,
    ) -> arco_catalog::Result<CatalogTransactionCommit> {
        match self {
            Self::CreateCatalog { name, description } => {
                writer
                    .create_catalog_transaction(name, description.as_deref(), options)
                    .await
            }
            Self::CreateSchema {
                catalog_name,
                schema_name,
                description,
            } => {
                if catalog_name == "default" {
                    writer
                        .create_namespace_transaction(schema_name, description.as_deref(), options)
                        .await
                } else {
                    writer
                        .create_schema_transaction(
                            catalog_name,
                            schema_name,
                            description.as_deref(),
                            options,
                        )
                        .await
                }
            }
            Self::RegisterTable {
                catalog_name,
                schema_name,
                table_name,
                description,
                location,
                format,
                columns,
            } => {
                writer
                    .register_table_in_schema_transaction(
                        catalog_name,
                        schema_name,
                        RegisterTableInSchemaRequest {
                            name: table_name.clone(),
                            description: description.clone(),
                            location: location.clone(),
                            format: format.clone(),
                            columns: columns.clone(),
                        },
                        options,
                    )
                    .await
            }
            Self::UpdateTable {
                catalog_name,
                schema_name,
                table_name,
                description,
                location,
                format,
            } => {
                writer
                    .update_table_in_schema_transaction(
                        catalog_name,
                        schema_name,
                        table_name,
                        TablePatch {
                            description: description.clone(),
                            location: location.clone(),
                            format: format.clone(),
                        },
                        options,
                    )
                    .await
            }
            Self::DropTable {
                catalog_name,
                schema_name,
                table_name,
            } => {
                writer
                    .drop_table_in_schema_transaction(
                        catalog_name,
                        schema_name,
                        table_name,
                        options,
                    )
                    .await
            }
            Self::RenameTable {
                catalog_name,
                schema_name,
                old_table_name,
                new_table_name,
            } => {
                writer
                    .rename_table_in_schema_transaction(
                        catalog_name,
                        schema_name,
                        old_table_name,
                        new_table_name,
                        options,
                    )
                    .await
            }
        }
    }
}

#[derive(Debug, Clone)]
enum IdempotencyClaim {
    Fresh(ControlPlaneIdempotencyRecord),
    ExistingVisible(ControlPlaneIdempotencyRecord),
    ExistingInProgress { tx_id: String },
}

impl IdempotencyClaim {
    fn tx_id(&self) -> &str {
        match self {
            Self::Fresh(record) | Self::ExistingVisible(record) => record.tx_id.as_str(),
            Self::ExistingInProgress { tx_id } => tx_id.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum WriteOutcome {
    Written,
    PreconditionFailed,
}

#[derive(Debug)]
struct VersionedValue<T> {
    value: T,
    version: String,
}

#[derive(Debug, Clone, Copy)]
enum ExistingClaimDisposition {
    Visible,
    InProgress,
    Retryable,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TxRecordLifecycle {
    status: ControlPlaneTxStatus,
    prepared_at: DateTime<Utc>,
}

fn table_format_string(format: ProtoTableFormat) -> Option<String> {
    match format {
        ProtoTableFormat::Unspecified => None,
        ProtoTableFormat::Delta => Some("delta".to_string()),
        ProtoTableFormat::Iceberg => Some("iceberg".to_string()),
        ProtoTableFormat::Parquet => Some("parquet".to_string()),
    }
}

fn parse_table_format(value: i32) -> Result<Option<String>, ApiError> {
    let format = ProtoTableFormat::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("unknown table format value: {value}")))?;
    Ok(table_format_string(format))
}

fn orchestration_event_hash_value(event: &OrchestrationEventEnvelope) -> serde_json::Value {
    serde_json::json!({
        "event_kind": event.event.as_ref().map(orchestration_event_kind_name),
        "wire_payload": base64::engine::general_purpose::STANDARD.encode(event.encode_to_vec()),
    })
}

fn orchestration_event_kind_name(event: &orchestration_event_envelope::Event) -> &'static str {
    match event {
        orchestration_event_envelope::Event::RunTriggered(_) => "RunTriggered",
        orchestration_event_envelope::Event::PlanCreated(_) => "PlanCreated",
        orchestration_event_envelope::Event::RunCancelRequested(_) => "RunCancelRequested",
        orchestration_event_envelope::Event::TaskStarted(_) => "TaskStarted",
        orchestration_event_envelope::Event::TaskHeartbeat(_) => "TaskHeartbeat",
        orchestration_event_envelope::Event::TaskFinished(_) => "TaskFinished",
        orchestration_event_envelope::Event::DispatchRequested(_) => "DispatchRequested",
        orchestration_event_envelope::Event::TimerRequested(_) => "TimerRequested",
        orchestration_event_envelope::Event::DispatchEnqueued(_) => "DispatchEnqueued",
        orchestration_event_envelope::Event::TimerEnqueued(_) => "TimerEnqueued",
        orchestration_event_envelope::Event::TimerFired(_) => "TimerFired",
        orchestration_event_envelope::Event::ScheduleDefinitionUpserted(_) => {
            "ScheduleDefinitionUpserted"
        }
        orchestration_event_envelope::Event::ScheduleTicked(_) => "ScheduleTicked",
        orchestration_event_envelope::Event::SensorEvaluated(_) => "SensorEvaluated",
        orchestration_event_envelope::Event::RunRequested(_) => "RunRequested",
        orchestration_event_envelope::Event::BackfillCreated(_) => "BackfillCreated",
        orchestration_event_envelope::Event::BackfillChunkPlanned(_) => "BackfillChunkPlanned",
        orchestration_event_envelope::Event::BackfillStateChanged(_) => "BackfillStateChanged",
    }
}

fn root_request_hash(mutations: &[RootMutation]) -> Result<String, ApiError> {
    let value = serde_json::json!({
        "mutations": mutations
            .iter()
            .map(RootMutation::request_hash_value)
            .collect::<Result<Vec<_>, _>>()?,
    });
    prefixed_request_hash(&value)
        .map_err(|error| ApiError::bad_request(format!("failed to hash root request: {error}")))
}

fn prefixed_request_hash(
    value: &serde_json::Value,
) -> Result<String, arco_catalog::idempotency::CanonicalizationError> {
    canonical_request_hash(value).map(|hash| format!("sha256:{hash}"))
}

fn envelope_to_event(
    meta: &ResolvedRequestMetadata,
    envelope: &OrchestrationEventEnvelope,
) -> Result<OrchestrationEvent, ApiError> {
    let data = envelope_event_data(envelope)?;
    let event_type = data.event_type().to_string();
    let correlation_id = envelope
        .correlation_id
        .clone()
        .or_else(|| data.run_id().map(ToString::to_string));

    Ok(OrchestrationEvent {
        event_id: envelope.event_id.clone(),
        event_type,
        event_version: envelope.event_version,
        timestamp: protobuf_timestamp_to_chrono(
            envelope.timestamp.as_ref().ok_or_else(|| {
                ApiError::bad_request("orchestration event timestamp is required")
            })?,
        )?,
        source: envelope.source.clone(),
        tenant_id: meta.tenant.clone(),
        workspace_id: meta.workspace.clone(),
        idempotency_key: envelope.idempotency_key.clone(),
        correlation_id,
        causation_id: envelope.causation_id.clone(),
        data,
    })
}

fn envelope_event_data(
    envelope: &OrchestrationEventEnvelope,
) -> Result<OrchestrationEventData, ApiError> {
    let event = envelope
        .event
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("orchestration event payload is required"))?;

    match event {
        orchestration_event_envelope::Event::RunTriggered(event) => run_triggered_to_runtime(event)
            .map(|runtime| OrchestrationEventData::RunTriggered {
                run_id: runtime.run_id,
                plan_id: runtime.plan_id,
                trigger: runtime.trigger,
                root_assets: runtime.root_assets,
                run_key: runtime.run_key,
                labels: runtime.labels,
                code_version: runtime.code_version,
            }),
        orchestration_event_envelope::Event::PlanCreated(event) => {
            Ok(OrchestrationEventData::PlanCreated {
                run_id: event.run_id.clone(),
                plan_id: event.plan_id.clone(),
                tasks: event
                    .tasks
                    .iter()
                    .map(proto_task_def_to_runtime)
                    .collect::<Result<Vec<_>, _>>()?,
            })
        }
        orchestration_event_envelope::Event::RunCancelRequested(event) => {
            Ok(OrchestrationEventData::RunCancelRequested {
                run_id: event.run_id.clone(),
                reason: event.reason.clone(),
                requested_by: event.requested_by.clone(),
            })
        }
        orchestration_event_envelope::Event::TaskStarted(event) => {
            Ok(OrchestrationEventData::TaskStarted {
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                attempt_id: event.attempt_id.clone(),
                worker_id: event.worker_id.clone(),
            })
        }
        orchestration_event_envelope::Event::TaskHeartbeat(event) => {
            Ok(OrchestrationEventData::TaskHeartbeat {
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                attempt_id: event.attempt_id.clone(),
                worker_id: event.worker_id.clone(),
                heartbeat_at: event
                    .heartbeat_at
                    .as_ref()
                    .map(protobuf_timestamp_to_chrono)
                    .transpose()?,
                progress_pct: event
                    .progress_pct
                    .map(|value| {
                        u8::try_from(value).map_err(|_| {
                            ApiError::bad_request(format!(
                                "task heartbeat progress_pct exceeds u8: {value}"
                            ))
                        })
                    })
                    .transpose()?,
                message: event.message.clone(),
            })
        }
        orchestration_event_envelope::Event::TaskFinished(event) => {
            Ok(OrchestrationEventData::TaskFinished {
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                attempt_id: event.attempt_id.clone(),
                worker_id: event.worker_id.clone(),
                outcome: task_outcome_to_runtime(event.outcome)?,
                materialization_id: event
                    .callback_output
                    .as_ref()
                    .and_then(|output| output.materialization_id.clone()),
                error_message: event.error.as_ref().map(|error| error.message.clone()),
                output: event
                    .callback_output
                    .as_ref()
                    .map(task_callback_output_to_json),
                error: event.error.as_ref().map(task_error_to_json),
                metrics: event.metrics.as_ref().map(task_metrics_to_json),
                cancelled_during_phase: event.cancelled_during_phase.clone(),
                partial_progress: event
                    .partial_progress_json
                    .as_ref()
                    .map(|value| {
                        serde_json::from_str::<serde_json::Value>(value).map_err(|error| {
                            ApiError::bad_request(format!(
                                "task_finished partial_progress_json is invalid JSON: {error}"
                            ))
                        })
                    })
                    .transpose()?,
                asset_key: event.asset_key.clone(),
                partition_key: proto_partition_key_to_string(event.partition_key.as_ref())?,
                code_version: event.code_version.clone(),
            })
        }
        orchestration_event_envelope::Event::DispatchRequested(event) => {
            Ok(OrchestrationEventData::DispatchRequested {
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                attempt_id: event.attempt_id.clone(),
                worker_queue: event.worker_queue.clone(),
                dispatch_id: event.dispatch_id.clone(),
            })
        }
        orchestration_event_envelope::Event::TimerRequested(event) => {
            Ok(OrchestrationEventData::TimerRequested {
                timer_id: event.timer_id.clone(),
                timer_type: timer_type_to_runtime(event.timer_type)?,
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                fire_at: protobuf_timestamp_to_chrono(event.fire_at.as_ref().ok_or_else(
                    || ApiError::bad_request("timer_requested fire_at is required"),
                )?)?,
            })
        }
        orchestration_event_envelope::Event::DispatchEnqueued(event) => {
            Ok(OrchestrationEventData::DispatchEnqueued {
                dispatch_id: event.dispatch_id.clone(),
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                cloud_task_id: event.cloud_task_id.clone(),
            })
        }
        orchestration_event_envelope::Event::TimerEnqueued(event) => {
            Ok(OrchestrationEventData::TimerEnqueued {
                timer_id: event.timer_id.clone(),
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                cloud_task_id: event.cloud_task_id.clone(),
            })
        }
        orchestration_event_envelope::Event::TimerFired(event) => {
            Ok(OrchestrationEventData::TimerFired {
                timer_id: event.timer_id.clone(),
                timer_type: timer_type_to_runtime(event.timer_type)?,
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
            })
        }
        orchestration_event_envelope::Event::ScheduleDefinitionUpserted(event) => {
            Ok(OrchestrationEventData::ScheduleDefinitionUpserted {
                schedule_id: event.schedule_id.clone(),
                cron_expression: event.cron_expression.clone(),
                timezone: event.timezone.clone(),
                catchup_window_minutes: event.catchup_window_minutes,
                asset_selection: event.asset_selection.clone(),
                max_catchup_ticks: event.max_catchup_ticks,
                enabled: event.enabled,
            })
        }
        orchestration_event_envelope::Event::ScheduleTicked(event) => {
            Ok(OrchestrationEventData::ScheduleTicked {
                schedule_id: event.schedule_id.clone(),
                scheduled_for: protobuf_timestamp_to_chrono(
                    event.scheduled_for.as_ref().ok_or_else(|| {
                        ApiError::bad_request("schedule_ticked scheduled_for is required")
                    })?,
                )?,
                tick_id: event.tick_id.clone(),
                definition_version: event.definition_version.clone(),
                asset_selection: event.asset_selection.clone(),
                partition_selection: (!event.partition_selection.is_empty())
                    .then(|| event.partition_selection.clone()),
                status: tick_status_to_runtime(
                    event.status,
                    event.skipped_reason.as_deref(),
                    event.failure_message.as_deref(),
                )?,
                run_key: event.run_key.clone(),
                request_fingerprint: event.request_fingerprint.clone(),
            })
        }
        orchestration_event_envelope::Event::SensorEvaluated(event) => {
            Ok(OrchestrationEventData::SensorEvaluated {
                sensor_id: event.sensor_id.clone(),
                eval_id: event.eval_id.clone(),
                cursor_before: event.cursor_before.clone(),
                cursor_after: event.cursor_after.clone(),
                expected_state_version: event.expected_state_version,
                trigger_source: trigger_source_to_runtime(
                    event.trigger_source.as_ref().ok_or_else(|| {
                        ApiError::bad_request("sensor_evaluated trigger_source is required")
                    })?,
                )?,
                run_requests: event
                    .run_requests
                    .iter()
                    .map(proto_run_request_to_runtime)
                    .collect(),
                status: sensor_eval_status_to_runtime(
                    event.status,
                    event.error_message.as_deref(),
                )?,
            })
        }
        orchestration_event_envelope::Event::RunRequested(event) => {
            Ok(OrchestrationEventData::RunRequested {
                run_key: event.run_key.clone(),
                request_fingerprint: event.request_fingerprint.clone(),
                asset_selection: event.asset_selection.clone(),
                partition_selection: (!event.partition_selection.is_empty())
                    .then(|| event.partition_selection.clone()),
                trigger_source_ref: trigger_info_to_source_ref(
                    event.trigger.as_ref().ok_or_else(|| {
                        ApiError::bad_request("run_requested trigger is required")
                    })?,
                )?,
                labels: labels_to_hash_map(&event.labels),
            })
        }
        orchestration_event_envelope::Event::BackfillCreated(event) => {
            Ok(OrchestrationEventData::BackfillCreated {
                backfill_id: event.backfill_id.clone(),
                client_request_id: event.client_request_id.clone(),
                asset_selection: event.asset_selection.clone(),
                partition_selector: partition_selector_to_runtime(
                    event.partition_selector.as_ref().ok_or_else(|| {
                        ApiError::bad_request("backfill_created partition_selector is required")
                    })?,
                )?,
                total_partitions: event.total_partitions,
                chunk_size: event.chunk_size,
                max_concurrent_runs: event.max_concurrent_runs,
                parent_backfill_id: event.parent_backfill_id.clone(),
            })
        }
        orchestration_event_envelope::Event::BackfillChunkPlanned(event) => {
            Ok(OrchestrationEventData::BackfillChunkPlanned {
                backfill_id: event.backfill_id.clone(),
                chunk_id: event.chunk_id.clone(),
                chunk_index: event.chunk_index,
                partition_keys: event.partition_keys.clone(),
                run_key: event.run_key.clone(),
                request_fingerprint: event.request_fingerprint.clone(),
            })
        }
        orchestration_event_envelope::Event::BackfillStateChanged(event) => {
            Ok(OrchestrationEventData::BackfillStateChanged {
                backfill_id: event.backfill_id.clone(),
                from_state: backfill_state_to_runtime(event.from_state)?,
                to_state: backfill_state_to_runtime(event.to_state)?,
                state_version: event.state_version,
                changed_by: event.changed_by.clone(),
            })
        }
    }
}

fn run_triggered_to_runtime(event: &ProtoRunTriggered) -> Result<RuntimeRunTriggered, ApiError> {
    Ok(RuntimeRunTriggered {
        run_id: event.run_id.clone(),
        plan_id: event.plan_id.clone(),
        trigger: trigger_info_to_runtime(
            event
                .trigger
                .as_ref()
                .ok_or_else(|| ApiError::bad_request("run_triggered trigger is required"))?,
        )?,
        root_assets: event.root_assets.clone(),
        run_key: event.run_key.clone(),
        labels: labels_to_hash_map(&event.labels),
        code_version: event.code_version.clone(),
    })
}

fn proto_task_def_to_runtime(task: &ProtoTaskDef) -> Result<RuntimeTaskDef, ApiError> {
    Ok(RuntimeTaskDef {
        key: task.key.clone(),
        depends_on: task.depends_on.clone(),
        asset_key: task.asset_key.clone(),
        partition_key: proto_partition_key_to_string(task.partition_key.as_ref())?,
        max_attempts: task.max_attempts,
        heartbeat_timeout_sec: task.heartbeat_timeout_sec,
    })
}

fn task_outcome_to_runtime(value: i32) -> Result<RuntimeTaskOutcome, ApiError> {
    let outcome = ProtoTaskOutcome::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("unknown task outcome value: {value}")))?;
    match outcome {
        ProtoTaskOutcome::Unspecified => Err(ApiError::bad_request(
            "task outcome must not be UNSPECIFIED",
        )),
        ProtoTaskOutcome::Succeeded => Ok(RuntimeTaskOutcome::Succeeded),
        ProtoTaskOutcome::Failed => Ok(RuntimeTaskOutcome::Failed),
        ProtoTaskOutcome::Skipped => Ok(RuntimeTaskOutcome::Skipped),
        ProtoTaskOutcome::Cancelled => Ok(RuntimeTaskOutcome::Cancelled),
    }
}

fn timer_type_to_runtime(value: i32) -> Result<RuntimeTimerType, ApiError> {
    let timer_type = ProtoTimerType::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("unknown timer type value: {value}")))?;
    match timer_type {
        ProtoTimerType::Unspecified => {
            Err(ApiError::bad_request("timer_type must not be UNSPECIFIED"))
        }
        ProtoTimerType::Retry => Ok(RuntimeTimerType::Retry),
        ProtoTimerType::HeartbeatCheck => Ok(RuntimeTimerType::HeartbeatCheck),
        ProtoTimerType::Cron => Ok(RuntimeTimerType::Cron),
        ProtoTimerType::SlaCheck => Ok(RuntimeTimerType::SlaCheck),
    }
}

fn tick_status_to_runtime(
    value: i32,
    skipped_reason: Option<&str>,
    failure_message: Option<&str>,
) -> Result<TickStatus, ApiError> {
    let status = ProtoTickStatus::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("unknown tick status value: {value}")))?;
    match status {
        ProtoTickStatus::Unspecified => {
            Err(ApiError::bad_request("tick status must not be UNSPECIFIED"))
        }
        ProtoTickStatus::Triggered => Ok(TickStatus::Triggered),
        ProtoTickStatus::Skipped => Ok(TickStatus::Skipped {
            reason: skipped_reason
                .unwrap_or("schedule tick skipped")
                .to_string(),
        }),
        ProtoTickStatus::Failed => Ok(TickStatus::Failed {
            error: failure_message
                .unwrap_or("schedule tick failed")
                .to_string(),
        }),
    }
}

fn sensor_eval_status_to_runtime(
    value: i32,
    error_message: Option<&str>,
) -> Result<SensorEvalStatus, ApiError> {
    let status = ProtoSensorEvalStatus::try_from(value).map_err(|_| {
        ApiError::bad_request(format!("unknown sensor evaluation status value: {value}"))
    })?;
    match status {
        ProtoSensorEvalStatus::Unspecified => Err(ApiError::bad_request(
            "sensor evaluation status must not be UNSPECIFIED",
        )),
        ProtoSensorEvalStatus::Triggered => Ok(SensorEvalStatus::Triggered),
        ProtoSensorEvalStatus::NoNewData => Ok(SensorEvalStatus::NoNewData),
        ProtoSensorEvalStatus::Error => Ok(SensorEvalStatus::Error {
            message: error_message
                .unwrap_or("sensor evaluation failed")
                .to_string(),
        }),
        ProtoSensorEvalStatus::SkippedStaleCursor => Ok(SensorEvalStatus::SkippedStaleCursor),
    }
}

fn trigger_info_to_runtime(trigger: &ProtoTriggerInfo) -> Result<RuntimeTriggerInfo, ApiError> {
    match trigger
        .trigger
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("trigger_info.trigger is required"))?
    {
        trigger_info::Trigger::Manual(manual) => Ok(RuntimeTriggerInfo::Manual {
            user_id: manual.user_id.clone(),
        }),
        trigger_info::Trigger::Schedule(schedule) => Ok(RuntimeTriggerInfo::Cron {
            schedule_id: schedule.schedule_id.clone(),
        }),
        trigger_info::Trigger::Materialization(materialization) => {
            Ok(RuntimeTriggerInfo::Materialization {
                upstream_materialization_id: materialization.upstream_materialization_id.clone(),
            })
        }
        trigger_info::Trigger::Webhook(webhook) => Ok(RuntimeTriggerInfo::Webhook {
            webhook_id: webhook.webhook_id.clone(),
        }),
        trigger_info::Trigger::Sensor(sensor) => Ok(RuntimeTriggerInfo::Sensor {
            sensor_id: sensor.sensor_id.clone(),
            cursor: sensor.cursor.clone().unwrap_or_default(),
        }),
        trigger_info::Trigger::Backfill(_) => Err(ApiError::bad_request(
            "run_triggered backfill triggers are not supported by the runtime event model",
        )),
    }
}

fn trigger_info_to_source_ref(trigger: &ProtoTriggerInfo) -> Result<SourceRef, ApiError> {
    match trigger
        .trigger
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("trigger_info.trigger is required"))?
    {
        trigger_info::Trigger::Manual(manual) => Ok(SourceRef::Manual {
            user_id: manual.user_id.clone(),
            request_id: manual.request_id.clone().ok_or_else(|| {
                ApiError::bad_request("run_requested manual trigger.request_id is required")
            })?,
        }),
        trigger_info::Trigger::Schedule(schedule) => Ok(SourceRef::Schedule {
            schedule_id: schedule.schedule_id.clone(),
            tick_id: schedule.tick_id.clone().ok_or_else(|| {
                ApiError::bad_request("run_requested schedule trigger.tick_id is required")
            })?,
        }),
        trigger_info::Trigger::Sensor(sensor) => Ok(SourceRef::Sensor {
            sensor_id: sensor.sensor_id.clone(),
            eval_id: sensor.eval_id.clone().ok_or_else(|| {
                ApiError::bad_request("run_requested sensor trigger.eval_id is required")
            })?,
        }),
        trigger_info::Trigger::Backfill(backfill) => Ok(SourceRef::Backfill {
            backfill_id: backfill.backfill_id.clone(),
            chunk_id: backfill.chunk_id.clone().ok_or_else(|| {
                ApiError::bad_request("run_requested backfill trigger.chunk_id is required")
            })?,
        }),
        trigger_info::Trigger::Materialization(_) => Err(ApiError::bad_request(
            "run_requested materialization triggers are not supported by the runtime source model",
        )),
        trigger_info::Trigger::Webhook(_) => Err(ApiError::bad_request(
            "run_requested webhook triggers are not supported by the runtime source model",
        )),
    }
}

fn trigger_source_to_runtime(
    trigger_source: &ProtoTriggerSource,
) -> Result<RuntimeTriggerSource, ApiError> {
    match trigger_source
        .source
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("trigger_source.source is required"))?
    {
        trigger_source::Source::Push(push) => Ok(RuntimeTriggerSource::Push {
            message_id: push.message_id.clone(),
        }),
        trigger_source::Source::Poll(poll) => Ok(RuntimeTriggerSource::Poll {
            poll_epoch: poll.poll_epoch,
        }),
    }
}

fn proto_run_request_to_runtime(request: &ProtoRunRequest) -> RunRequest {
    RunRequest {
        run_key: request.run_key.clone(),
        request_fingerprint: request.request_fingerprint.clone(),
        asset_selection: request.asset_selection.clone(),
        partition_selection: (!request.partition_selection.is_empty())
            .then(|| request.partition_selection.clone()),
    }
}

fn backfill_state_to_runtime(value: i32) -> Result<BackfillState, ApiError> {
    let state = ProtoBackfillState::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("unknown backfill state value: {value}")))?;
    match state {
        ProtoBackfillState::Unspecified => Err(ApiError::bad_request(
            "backfill state must not be UNSPECIFIED",
        )),
        ProtoBackfillState::Pending => Ok(BackfillState::Pending),
        ProtoBackfillState::Running => Ok(BackfillState::Running),
        ProtoBackfillState::Paused => Ok(BackfillState::Paused),
        ProtoBackfillState::Succeeded => Ok(BackfillState::Succeeded),
        ProtoBackfillState::Failed => Ok(BackfillState::Failed),
        ProtoBackfillState::Cancelled => Ok(BackfillState::Cancelled),
    }
}

fn partition_selector_to_runtime(
    selector: &ProtoPartitionSelector,
) -> Result<PartitionSelector, ApiError> {
    match selector
        .selector
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("partition_selector.selector is required"))?
    {
        partition_selector::Selector::Range(range) => Ok(PartitionSelector::Range {
            start: range.start.clone(),
            end: range.end.clone(),
        }),
        partition_selector::Selector::Explicit(explicit) => Ok(PartitionSelector::Explicit {
            partition_keys: explicit.partition_keys.clone(),
        }),
        partition_selector::Selector::Filter(filter) => Ok(PartitionSelector::Filter {
            filters: filter
                .filters
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        }),
    }
}

fn task_callback_output_to_json(output: &ProtoTaskCallbackOutput) -> serde_json::Value {
    let mut value = serde_json::Map::new();
    if let Some(materialization_id) = &output.materialization_id {
        value.insert(
            "materializationId".to_string(),
            serde_json::Value::String(materialization_id.clone()),
        );
    }
    if let Some(row_count) = output.row_count {
        value.insert("rowCount".to_string(), serde_json::Value::from(row_count));
    }
    if let Some(byte_size) = output.byte_size {
        value.insert("byteSize".to_string(), serde_json::Value::from(byte_size));
    }
    if let Some(output_path) = &output.output_path {
        value.insert(
            "outputPath".to_string(),
            serde_json::Value::String(output_path.clone()),
        );
    }
    if let Some(delta_table) = &output.delta_table {
        value.insert(
            "deltaTable".to_string(),
            serde_json::Value::String(delta_table.clone()),
        );
    }
    if let Some(delta_version) = output.delta_version {
        value.insert(
            "deltaVersion".to_string(),
            serde_json::Value::from(delta_version),
        );
    }
    if let Some(delta_partition) = &output.delta_partition {
        value.insert(
            "deltaPartition".to_string(),
            serde_json::Value::String(delta_partition.clone()),
        );
    }
    serde_json::Value::Object(value)
}

fn task_error_to_json(error: &ProtoTaskError) -> serde_json::Value {
    let mut value = serde_json::Map::new();
    value.insert(
        "category".to_string(),
        serde_json::Value::String(task_error_category_name(error.category).to_string()),
    );
    value.insert(
        "message".to_string(),
        serde_json::Value::String(error.message.clone()),
    );
    if let Some(detail) = &error.detail {
        value.insert(
            "detail".to_string(),
            serde_json::Value::String(detail.clone()),
        );
    }
    if let Some(retryable) = error.retryable {
        value.insert("retryable".to_string(), serde_json::Value::Bool(retryable));
    }
    serde_json::Value::Object(value)
}

fn task_metrics_to_json(metrics: &ProtoTaskMetrics) -> serde_json::Value {
    let mut value = serde_json::Map::new();
    if let Some(cpu_time_ms) = metrics.cpu_time_ms {
        value.insert(
            "cpuTimeMs".to_string(),
            serde_json::Value::from(cpu_time_ms),
        );
    }
    if let Some(peak_memory_bytes) = metrics.peak_memory_bytes {
        value.insert(
            "peakMemoryBytes".to_string(),
            serde_json::Value::from(peak_memory_bytes),
        );
    }
    if let Some(io_read_bytes) = metrics.io_read_bytes {
        value.insert(
            "ioReadBytes".to_string(),
            serde_json::Value::from(io_read_bytes),
        );
    }
    if let Some(io_write_bytes) = metrics.io_write_bytes {
        value.insert(
            "ioWriteBytes".to_string(),
            serde_json::Value::from(io_write_bytes),
        );
    }
    serde_json::Value::Object(value)
}

fn task_error_category_name(value: i32) -> &'static str {
    match ProtoTaskErrorCategory::try_from(value) {
        Ok(ProtoTaskErrorCategory::Unspecified) => "unspecified",
        Ok(ProtoTaskErrorCategory::UserCode) => "user_code",
        Ok(ProtoTaskErrorCategory::DataQuality) => "data_quality",
        Ok(ProtoTaskErrorCategory::Infrastructure) => "infrastructure",
        Ok(ProtoTaskErrorCategory::Configuration) => "configuration",
        Ok(ProtoTaskErrorCategory::Timeout) => "timeout",
        Ok(ProtoTaskErrorCategory::Cancelled) => "cancelled",
        Ok(ProtoTaskErrorCategory::Unknown) | Err(_) => "unknown",
    }
}

fn labels_to_hash_map(labels: &BTreeMap<String, String>) -> HashMap<String, String> {
    labels
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn proto_partition_key_to_string(
    partition_key: Option<&ProtoPartitionKey>,
) -> Result<Option<String>, ApiError> {
    let Some(partition_key) = partition_key else {
        return Ok(None);
    };
    if partition_key.dimensions.is_empty() {
        return Ok(None);
    }

    let mut canonical = CorePartitionKey::new();
    for dimension in &partition_key.dimensions {
        validate_partition_dimension_name(&dimension.name)?;
        if canonical.get(&dimension.name).is_some() {
            return Err(ApiError::bad_request(format!(
                "duplicate partition dimension '{}'",
                dimension.name
            )));
        }
        let value = dimension.value.as_ref().ok_or_else(|| {
            ApiError::bad_request(format!(
                "partition dimension '{}' is missing value",
                dimension.name
            ))
        })?;
        canonical.insert(dimension.name.clone(), proto_scalar_value_to_core(value)?);
    }

    Ok(Some(canonical.canonical_string()))
}

fn validate_partition_dimension_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty()
        || !name
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_lowercase())
        || !name
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        return Err(ApiError::bad_request(format!(
            "invalid partition dimension name '{name}'"
        )));
    }
    Ok(())
}

fn proto_scalar_value_to_core(value: &ProtoScalarValue) -> Result<CoreScalarValue, ApiError> {
    match value
        .value
        .as_ref()
        .ok_or_else(|| ApiError::bad_request("partition scalar value is required"))?
    {
        scalar_value::Value::StringValue(value) => Ok(CoreScalarValue::String(value.clone())),
        scalar_value::Value::Int64Value(value) => Ok(CoreScalarValue::Int64(*value)),
        scalar_value::Value::BoolValue(value) => Ok(CoreScalarValue::Boolean(*value)),
        scalar_value::Value::DateValue(value) => Ok(CoreScalarValue::Date(value.clone())),
        scalar_value::Value::TimestampValue(value) => Ok(CoreScalarValue::Timestamp(value.clone())),
        scalar_value::Value::NullValue(_) => Ok(CoreScalarValue::Null),
    }
}

#[derive(Debug)]
struct RuntimeRunTriggered {
    run_id: String,
    plan_id: String,
    trigger: RuntimeTriggerInfo,
    root_assets: Vec<String>,
    run_key: Option<String>,
    labels: HashMap<String, String>,
    code_version: Option<String>,
}

fn protobuf_timestamp_to_chrono(
    timestamp: &prost_types::Timestamp,
) -> Result<DateTime<Utc>, ApiError> {
    DateTime::<Utc>::from_timestamp(
        timestamp.seconds,
        u32::try_from(timestamp.nanos)
            .map_err(|_| ApiError::bad_request("protobuf timestamp nanos must be non-negative"))?,
    )
    .ok_or_else(|| ApiError::bad_request("invalid protobuf timestamp"))
}

fn chrono_to_timestamp(timestamp: DateTime<Utc>) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp {
        seconds: timestamp.timestamp(),
        nanos: i32::try_from(timestamp.timestamp_subsec_nanos()).unwrap_or(i32::MAX),
    })
}

fn catalog_receipt_to_proto(receipt: &CatalogTxReceipt) -> ProtoCatalogTxReceipt {
    ProtoCatalogTxReceipt {
        tx_id: receipt.tx_id.clone(),
        event_id: receipt.event_id.clone(),
        commit_id: receipt.commit_id.clone(),
        manifest_id: receipt.manifest_id.clone(),
        snapshot_version: receipt.snapshot_version,
        pointer_version: receipt.pointer_version.clone(),
        read_token: receipt.read_token.clone(),
        visible_at: chrono_to_timestamp(receipt.visible_at),
    }
}

fn orchestration_receipt_to_proto(receipt: &OrchestrationTxReceipt) -> ProtoOrchestrationTxReceipt {
    ProtoOrchestrationTxReceipt {
        tx_id: receipt.tx_id.clone(),
        commit_id: receipt.commit_id.clone(),
        manifest_id: receipt.manifest_id.clone(),
        revision_ulid: receipt.revision_ulid.clone(),
        delta_id: receipt.delta_id.clone(),
        pointer_version: receipt.pointer_version.clone(),
        events_processed: receipt.events_processed,
        read_token: receipt.read_token.clone(),
        visible_at: chrono_to_timestamp(receipt.visible_at),
    }
}

fn root_receipt_to_proto(receipt: &RootTxReceipt) -> ProtoRootTxReceipt {
    ProtoRootTxReceipt {
        tx_id: receipt.tx_id.clone(),
        root_commit_id: receipt.root_commit_id.clone(),
        super_manifest_path: receipt.super_manifest_path.clone(),
        domain_commits: receipt
            .domain_commits
            .iter()
            .map(domain_commit_to_proto)
            .collect(),
        read_token: receipt.read_token.clone(),
        visible_at: chrono_to_timestamp(receipt.visible_at),
    }
}

fn catalog_status_to_proto(record: &CatalogTxRecord) -> CatalogTxStatus {
    CatalogTxStatus {
        tx_id: record.tx_id.clone(),
        status: proto_status(record.status) as i32,
        request_hash: record.request_hash.clone(),
        lock_path: record.lock_path.clone(),
        fencing_token: record.fencing_token,
        prepared_at: chrono_to_timestamp(record.prepared_at),
        visible_at: record.visible_at.and_then(chrono_to_timestamp),
        result: record.result.as_ref().map(catalog_receipt_to_proto),
        repair_pending: record.repair_pending,
    }
}

fn orchestration_status_to_proto(record: &OrchestrationTxRecord) -> OrchestrationTxStatus {
    OrchestrationTxStatus {
        tx_id: record.tx_id.clone(),
        status: proto_status(record.status) as i32,
        request_hash: record.request_hash.clone(),
        lock_path: record.lock_path.clone(),
        fencing_token: record.fencing_token,
        prepared_at: chrono_to_timestamp(record.prepared_at),
        visible_at: record.visible_at.and_then(chrono_to_timestamp),
        result: record.result.as_ref().map(orchestration_receipt_to_proto),
        repair_pending: record.repair_pending,
    }
}

fn root_status_to_proto(record: &RootTxRecord) -> RootTxStatus {
    RootTxStatus {
        tx_id: record.tx_id.clone(),
        status: proto_status(record.status) as i32,
        request_hash: record.request_hash.clone(),
        lock_path: record.lock_path.clone(),
        fencing_token: record.fencing_token,
        prepared_at: chrono_to_timestamp(record.prepared_at),
        visible_at: record.visible_at.and_then(chrono_to_timestamp),
        super_manifest_path: record
            .result
            .as_ref()
            .map(|result| result.super_manifest_path.clone())
            .unwrap_or_default(),
        repair_pending: record.repair_pending,
        domains: record
            .result
            .as_ref()
            .map(root_participants_from_receipt)
            .unwrap_or_default(),
        result: record.result.as_ref().map(root_receipt_to_proto),
    }
}

fn domain_commit_to_proto(commit: &DomainCommit) -> ProtoDomainCommit {
    ProtoDomainCommit {
        domain: proto_domain(commit.domain) as i32,
        tx_id: commit.tx_id.clone(),
        commit_id: commit.commit_id.clone(),
        manifest_id: commit.manifest_id.clone(),
        manifest_path: commit.manifest_path.clone(),
        read_token: commit.read_token.clone(),
    }
}

fn root_participants_from_receipt(receipt: &RootTxReceipt) -> Vec<RootTxParticipant> {
    receipt
        .domain_commits
        .iter()
        .map(|commit| RootTxParticipant {
            domain: proto_domain(commit.domain) as i32,
            lock_path: participant_lock_path(commit.domain),
            tx_id: commit.tx_id.clone(),
            manifest_id: commit.manifest_id.clone(),
            manifest_path: commit.manifest_path.clone(),
        })
        .collect()
}

fn participant_lock_path(domain: ControlPlaneTxDomain) -> String {
    match domain {
        ControlPlaneTxDomain::Catalog => CatalogPaths::domain_lock(CatalogDomain::Catalog),
        ControlPlaneTxDomain::Orchestration => orchestration_compaction_lock_path().to_string(),
        ControlPlaneTxDomain::Root => ControlPlaneTxPaths::root_lock(),
    }
}

fn root_domain_commit_from_catalog(receipt: &CatalogTxReceipt) -> DomainCommit {
    DomainCommit {
        domain: ControlPlaneTxDomain::Catalog,
        tx_id: receipt.tx_id.clone(),
        commit_id: receipt.commit_id.clone(),
        manifest_id: receipt.manifest_id.clone(),
        manifest_path: CatalogPaths::domain_manifest_snapshot(
            CatalogDomain::Catalog,
            &receipt.manifest_id,
        ),
        read_token: receipt.read_token.clone(),
    }
}

fn root_domain_commit_from_orchestration(receipt: &OrchestrationTxReceipt) -> DomainCommit {
    DomainCommit {
        domain: ControlPlaneTxDomain::Orchestration,
        tx_id: receipt.tx_id.clone(),
        commit_id: receipt.commit_id.clone(),
        manifest_id: receipt.manifest_id.clone(),
        manifest_path: format!("state/orchestration/manifests/{}.json", receipt.manifest_id),
        read_token: receipt.read_token.clone(),
    }
}

fn proto_domain(domain: ControlPlaneTxDomain) -> TransactionDomain {
    match domain {
        ControlPlaneTxDomain::Catalog => TransactionDomain::Catalog,
        ControlPlaneTxDomain::Orchestration => TransactionDomain::Orchestration,
        ControlPlaneTxDomain::Root => TransactionDomain::Root,
    }
}

fn proto_status(status: ControlPlaneTxStatus) -> TransactionStatus {
    match status {
        ControlPlaneTxStatus::Prepared => TransactionStatus::Prepared,
        ControlPlaneTxStatus::Visible => TransactionStatus::Visible,
        ControlPlaneTxStatus::Aborted => TransactionStatus::Aborted,
    }
}

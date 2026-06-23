//! Shared runtime service for single-domain control-plane transactions.

#![allow(
    clippy::future_not_send,
    clippy::option_option,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::unnecessary_wraps,
    clippy::unused_self
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

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
    CatalogTxReceipt, CatalogTxRecord, ControlPlaneDurableAppend, ControlPlaneIdempotencyRecord,
    ControlPlaneTxDomain, ControlPlaneTxKind, ControlPlaneTxPaths, ControlPlaneTxRecord,
    ControlPlaneTxStatus, DomainCommit, OrchestrationTxReceipt, OrchestrationTxRecord,
    RootTxManifest, RootTxManifestDomain, RootTxReceipt, RootTxRecord,
};
use arco_core::lock::{DEFAULT_LOCK_TTL, DistributedLock};
use arco_core::storage::WritePrecondition;
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData, SourceRef};
use arco_flow::orchestration::proto::event_from_proto_envelope;
use arco_flow::orchestration_compaction_lock_path;
use arco_proto::arco::catalog::v1::{
    CatalogDdlOperation, CreateCatalogOp, CreateSchemaOp, DropTableOp, MetastoreMutation,
    RegisterTableOp, RenameTableOp, TableFormat as ProtoTableFormat, UpdateTableOp,
    catalog_ddl_operation,
};
use arco_proto::arco::controlplane::v1::{
    ApplyCatalogDdlRequest, ApplyCatalogDdlResponse, CatalogTxReceipt as ProtoCatalogTxReceipt,
    CatalogTxStatus, CommitOrchestrationBatchRequest, CommitOrchestrationBatchResponse,
    CommitRootTransactionRequest, CommitRootTransactionResponse, DomainCommit as ProtoDomainCommit,
    DomainMutation, GetCatalogTransactionRequest, GetCatalogTransactionResponse,
    GetOrchestrationTransactionRequest, GetOrchestrationTransactionResponse,
    GetRootTransactionRequest, GetRootTransactionResponse, OrchestrationBatchSpec,
    OrchestrationTxReceipt as ProtoOrchestrationTxReceipt, OrchestrationTxStatus,
    RootTxParticipant, RootTxReceipt as ProtoRootTxReceipt, RootTxStatus, ScopedMetastoreMutation,
    TransactionDomain, TransactionStatus, domain_mutation,
};
use arco_proto::arco::orchestration::v1::OrchestrationEventEnvelope;

use crate::context::RequestContext;
use crate::error::ApiError;
use crate::orchestration_compaction::{
    OrchestrationCommitError, OrchestrationCommitOutcome, append_events_and_compact_with_result,
    compact_event_paths_with_result,
};
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
            mutation.validate_request_scope(&meta)?;
            if mutation.is_metastore() {
                continue;
            }
            let domain = mutation.domain();
            if !seen_domains.insert(domain) {
                return Err(ApiError::bad_request(format!(
                    "duplicate root mutation for domain '{domain}'"
                )));
            }
        }

        let request_hash = root_request_hash(&mutations, &meta)?;
        let idempotency_path = ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Root,
            meta.idempotency_key.as_str(),
        );
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
                    idempotency_path.as_str(),
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
        if let Some(tx_id) = match &claim {
            IdempotencyClaim::ExistingInProgress { tx_id } => Some(tx_id),
            IdempotencyClaim::ExistingRepairPending(record) => Some(&record.tx_id),
            _ => None,
        } {
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
            durable_append: None,
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
            if mutations.iter().any(RootMutation::is_metastore) {
                return Err(ApiError::not_implemented(
                    "metastore root mutations are not implemented yet",
                ));
            }

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
                    RootMutation::Metastore(_) | RootMutation::ScopedMetastore(_) => {
                        return Err(ApiError::not_implemented(
                            "metastore root mutations are not implemented yet",
                        ));
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
                idempotency_path.as_str(),
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
                        idempotency_path.as_str(),
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
        let idempotency_path = ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Catalog,
            meta.idempotency_key.as_str(),
        );
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
                    idempotency_path.as_str(),
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
        if let Some(tx_id) = match &claim {
            IdempotencyClaim::ExistingInProgress { tx_id } => Some(tx_id),
            IdempotencyClaim::ExistingRepairPending(record) => Some(&record.tx_id),
            _ => None,
        } {
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
            durable_append: None,
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
            idempotency_path.as_str(),
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

    #[allow(clippy::cognitive_complexity)]
    async fn execute_orchestration_batch(
        &self,
        meta: &ResolvedRequestMetadata,
        batch: OrchestrationBatchMutation,
    ) -> Result<TxExecutionOutcome<OrchestrationTxReceipt>, ApiError> {
        let events = batch.events(meta)?;
        let request_hash = batch.request_hash_for_events(&events)?;
        let idempotency_path = ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Orchestration,
            meta.idempotency_key.as_str(),
        );
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
                    idempotency_path.as_str(),
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
        if let IdempotencyClaim::ExistingRepairPending(existing) = &claim {
            return self
                .repair_prepared_orchestration_batch(meta, idempotency_path.as_str(), existing)
                .await;
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
            durable_append: None,
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
            Err(OrchestrationCommitError::Definite(error)) => {
                self.abort_transaction(ControlPlaneTxDomain::Orchestration, &tx_id)
                    .await;
                return Err(error);
            }
            Err(OrchestrationCommitError::AmbiguousAfterAppend {
                error,
                durable_append,
            }) => {
                if let Err(mark_error) = self
                    .mark_prepared_repair_pending::<OrchestrationTxReceipt>(
                        ControlPlaneTxDomain::Orchestration,
                        &tx_id,
                        Some(durable_append),
                    )
                    .await
                {
                    tracing::warn!(
                        error = ?mark_error,
                        tx_id,
                        "failed to mark ambiguous orchestration transaction repair_pending"
                    );
                }
                return Err(error);
            }
        };

        self.finalize_orchestration_commit(&tx_id, idempotency_path.as_str(), commit)
            .await
    }

    async fn repair_prepared_orchestration_batch(
        &self,
        meta: &ResolvedRequestMetadata,
        idempotency_path: &str,
        existing: &ControlPlaneIdempotencyRecord,
    ) -> Result<TxExecutionOutcome<OrchestrationTxReceipt>, ApiError> {
        let stored = self
            .load_json_required::<OrchestrationTxRecord>(&ControlPlaneTxPaths::record(
                ControlPlaneTxDomain::Orchestration,
                existing.tx_id.as_str(),
            ))
            .await?
            .ok_or_else(|| {
                ApiError::internal(format!(
                    "repair-pending orchestration transaction record missing: {}",
                    existing.tx_id
                ))
            })?;
        if stored.status != ControlPlaneTxStatus::Prepared || !stored.repair_pending {
            return Err(ApiError::conflict(format!(
                "orchestration transaction '{}' is no longer repair pending",
                existing.tx_id
            )));
        }
        if stored.request_hash != existing.request_hash
            || stored.idempotency_key != existing.idempotency_key
            || existing.idempotency_key != meta.idempotency_key
        {
            return Err(ApiError::conflict(
                "repair-pending orchestration transaction ownership mismatch",
            ));
        }
        let durable_append = stored.durable_append.as_ref().ok_or_else(|| {
            ApiError::conflict(format!(
                "repair-pending orchestration transaction '{}' is missing durable append metadata",
                existing.tx_id
            ))
        })?;
        if durable_append.event_paths.is_empty() {
            return Err(ApiError::conflict(format!(
                "repair-pending orchestration transaction '{}' has no event paths to repair",
                existing.tx_id
            )));
        }

        let commit = match compact_event_paths_with_result(
            &self.state.config,
            self.storage.clone(),
            durable_append.event_paths.clone(),
            Some(meta.request_id.as_str()),
        )
        .await
        {
            Ok(commit) => commit,
            Err(error) => return Err(error),
        };

        self.finalize_orchestration_commit(existing.tx_id.as_str(), idempotency_path, commit)
            .await
    }

    async fn finalize_orchestration_commit(
        &self,
        tx_id: &str,
        idempotency_path: &str,
        commit: OrchestrationCommitOutcome,
    ) -> Result<TxExecutionOutcome<OrchestrationTxReceipt>, ApiError> {
        let visible_at = Utc::now();
        let commit_id = Ulid::new().to_string();
        let receipt = OrchestrationTxReceipt {
            tx_id: tx_id.to_string(),
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

        if let Err(error) = self
            .finalize_visible(
                ControlPlaneTxDomain::Orchestration,
                tx_id,
                idempotency_path,
                commit.lock_path,
                commit.fencing_token,
                repair_pending,
                visible_at,
                receipt.clone(),
            )
            .await
        {
            tracing::warn!(
                error = ?error,
                tx_id,
                commit_id = %receipt.commit_id,
                "failed to finalize orchestration visibility after writing commit receipt"
            );
            return Err(error);
        }

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
                        if matches!(
                            self.classify_existing_idempotency(domain, &existing.value)
                                .await?,
                            ExistingClaimDisposition::RepairPending
                        ) {
                            return Ok(IdempotencyClaim::ExistingRepairPending(existing.value));
                        }
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
                        ExistingClaimDisposition::RepairPending => {
                            return Ok(IdempotencyClaim::ExistingRepairPending(existing.value));
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
                                WriteOutcome::PreconditionFailed => {}
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
        idempotency_path: &str,
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
        let stored = self
            .load_json_with_version_required::<ControlPlaneTxRecord<TResult>>(&path)
            .await?
            .ok_or_else(|| ApiError::internal(format!("transaction record not found: {tx_id}")))?;
        let mut record = stored.value;
        record.status = ControlPlaneTxStatus::Visible;
        record.lock_path = lock_path;
        record.fencing_token = fencing_token;
        record.repair_pending = repair_pending;
        record.visible_at = Some(visible_at);
        record.durable_append = None;
        record.result = Some(result.clone());

        self.persist_visible_record_and_idempotency(
            domain,
            idempotency_path,
            &record,
            WritePrecondition::MatchesVersion(stored.version),
        )
        .await
    }

    async fn mark_visible_repair_pending<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        tx_id: &str,
        idempotency_path: &str,
    ) -> Result<(), ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone,
    {
        let path = ControlPlaneTxPaths::record(domain, tx_id);
        let stored = self
            .load_json_with_version_required::<ControlPlaneTxRecord<TResult>>(&path)
            .await?
            .ok_or_else(|| ApiError::internal(format!("transaction record not found: {tx_id}")))?;
        let mut record = stored.value;
        if record.status != ControlPlaneTxStatus::Visible {
            return Err(ApiError::internal(format!(
                "cannot mark non-visible transaction repair_pending: {tx_id}"
            )));
        }
        if record.repair_pending {
            return Ok(());
        }

        record.repair_pending = true;
        self.persist_visible_record_and_idempotency(
            domain,
            idempotency_path,
            &record,
            WritePrecondition::MatchesVersion(stored.version),
        )
        .await
    }

    async fn mark_prepared_repair_pending<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        tx_id: &str,
        durable_append: Option<ControlPlaneDurableAppend>,
    ) -> Result<(), ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone,
    {
        let path = ControlPlaneTxPaths::record(domain, tx_id);
        let stored = self
            .load_json_with_version_required::<ControlPlaneTxRecord<TResult>>(&path)
            .await?
            .ok_or_else(|| ApiError::internal(format!("transaction record not found: {tx_id}")))?;
        let mut record = stored.value;
        if record.status != ControlPlaneTxStatus::Prepared {
            return Ok(());
        }
        if record.repair_pending && (durable_append.is_none() || record.durable_append.is_some()) {
            return Ok(());
        }

        record.repair_pending = true;
        if durable_append.is_some() {
            record.durable_append = durable_append;
        }
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

    async fn persist_visible_record_and_idempotency<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        idempotency_path: &str,
        record: &ControlPlaneTxRecord<TResult>,
        record_precondition: WritePrecondition,
    ) -> Result<(), ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone,
    {
        let path = ControlPlaneTxPaths::record(domain, record.tx_id.as_str());
        let stored_idem = self
            .load_json_with_version_required::<ControlPlaneIdempotencyRecord>(idempotency_path)
            .await?
            .ok_or_else(|| ApiError::internal("idempotency record missing during finalize"))?;
        let mut idem = stored_idem.value;
        if idem.tx_id != record.tx_id {
            return Err(ApiError::conflict(format!(
                "idempotency marker ownership changed during {domain} transaction finalize"
            )));
        }
        if idem.request_hash != record.request_hash {
            return Err(ApiError::conflict(format!(
                "idempotency marker request hash changed during {domain} transaction finalize"
            )));
        }
        idem.visible_at = record.visible_at;
        idem.tx_record = Some(serde_json::to_value(record).map_err(|error| {
            ApiError::internal(format!(
                "failed to encode visible transaction record for idempotency replay: {error}"
            ))
        })?);

        let idem_write = self
            .write_json(
                idempotency_path,
                &idem,
                WritePrecondition::MatchesVersion(stored_idem.version),
            )
            .await;

        match idem_write {
            Ok(WriteOutcome::Written) => {
                match self.write_json(&path, record, record_precondition).await? {
                    WriteOutcome::Written => Ok(()),
                    WriteOutcome::PreconditionFailed => Err(ApiError::conflict(format!(
                        "{domain} transaction record changed during finalize"
                    ))),
                }
            }
            Ok(WriteOutcome::PreconditionFailed) => Err(ApiError::conflict(format!(
                "idempotency marker changed during {domain} transaction finalize"
            ))),
            Err(idempotency_error) => {
                match self.write_json(&path, record, record_precondition).await {
                    Ok(WriteOutcome::Written) => {}
                    Ok(WriteOutcome::PreconditionFailed) => {
                        tracing::warn!(
                            tx_id = record.tx_id.as_str(),
                            domain = %domain,
                            "transaction record changed after idempotency finalize write failed"
                        );
                    }
                    Err(record_error) => {
                        tracing::warn!(
                            error = ?record_error,
                            tx_id = record.tx_id.as_str(),
                            domain = %domain,
                            "failed to persist visible transaction record after idempotency finalize write failed"
                        );
                    }
                }
                Err(idempotency_error)
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
        idempotency_path: &str,
        existing: &ControlPlaneIdempotencyRecord,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone,
    {
        let stored_record = self
            .load_json_with_version_required::<ControlPlaneTxRecord<TResult>>(
                &ControlPlaneTxPaths::record(domain, existing.tx_id.as_str()),
            )
            .await?;
        if let Some(stored_record) = &stored_record {
            if stored_record.value.status == ControlPlaneTxStatus::Visible {
                if existing.visible_at != stored_record.value.visible_at
                    || existing.tx_record.is_none()
                {
                    // Safe as a repair write: replayers converge on the same visible receipt,
                    // and any later repair_pending flip is recovered from the canonical tx record.
                    if let Err(error) = self
                        .persist_visible_record_and_idempotency(
                            domain,
                            idempotency_path,
                            &stored_record.value,
                            WritePrecondition::MatchesVersion(stored_record.version.clone()),
                        )
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
                return Ok(stored_record.value.clone());
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
        if record.tx_id != existing.tx_id
            || record.kind != existing.kind
            || record.request_hash != existing.request_hash
            || (!existing.idempotency_key.is_empty()
                && record.idempotency_key != existing.idempotency_key)
        {
            return Err(ApiError::internal(format!(
                "{domain} idempotency cache does not match marker ownership for tx_id '{}'",
                existing.tx_id
            )));
        }

        let path = ControlPlaneTxPaths::record(domain, existing.tx_id.as_str());
        let record_precondition = if let Some(stored_record) = stored_record {
            if stored_record.value.tx_id != record.tx_id
                || stored_record.value.kind != record.kind
                || stored_record.value.request_hash != record.request_hash
                || (!stored_record.value.idempotency_key.is_empty()
                    && stored_record.value.idempotency_key != record.idempotency_key)
            {
                return Err(ApiError::internal(format!(
                    "{domain} transaction record path is occupied by a different record for tx_id '{}'",
                    existing.tx_id
                )));
            }
            WritePrecondition::MatchesVersion(stored_record.version)
        } else {
            WritePrecondition::DoesNotExist
        };
        match self.write_json(&path, &record, record_precondition).await {
            Ok(WriteOutcome::Written | WriteOutcome::PreconditionFailed) => {}
            Err(error) => {
                tracing::warn!(
                    error = ?error,
                    tx_id = existing.tx_id.as_str(),
                    domain = %domain,
                    "failed to repair visible transaction record from idempotency cache"
                );
            }
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
                ControlPlaneTxStatus::Prepared if record.repair_pending => {
                    return Ok(ExistingClaimDisposition::RepairPending);
                }
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
        catalog: String,
        description: Option<String>,
    },
    CreateSchema {
        catalog: String,
        schema: String,
        description: Option<String>,
    },
    RegisterTable {
        catalog: String,
        schema: String,
        table: String,
        description: Option<String>,
        location: Option<String>,
        format: Option<String>,
        columns: Vec<ColumnDefinition>,
    },
    UpdateTable {
        catalog: String,
        schema: String,
        table: String,
        description: Option<Option<String>>,
        location: Option<Option<String>>,
        format: Option<Option<String>>,
    },
    DropTable {
        catalog: String,
        schema: String,
        table: String,
    },
    RenameTable {
        catalog: String,
        schema: String,
        table: String,
        new_table: String,
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

    fn request_hash_value(
        &self,
        meta: &ResolvedRequestMetadata,
    ) -> Result<serde_json::Value, ApiError> {
        let events = self.events(meta)?;
        Self::request_hash_value_for_events(&sanitize_runtime_events_for_request_hash(&events))
    }

    fn request_hash_for_events(&self, events: &[OrchestrationEvent]) -> Result<String, ApiError> {
        prefixed_request_hash(&Self::request_hash_value_for_events(
            &sanitize_runtime_events_for_request_hash(events),
        )?)
        .map_err(|error| {
            ApiError::bad_request(format!("failed to hash orchestration request: {error}"))
        })
    }

    fn request_hash_value_for_events(
        events: &[OrchestrationEvent],
    ) -> Result<serde_json::Value, ApiError> {
        let events = serde_json::to_value(events).map_err(|error| {
            ApiError::internal(format!(
                "failed to serialize orchestration events for hashing: {error}"
            ))
        })?;
        Ok(serde_json::json!({ "events": events }))
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
    Metastore(MetastoreMutation),
    ScopedMetastore(ScopedMetastoreMutation),
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
            Some(domain_mutation::Kind::Metastore(mutation)) => {
                if !mutation.has_contract_operation() {
                    return Err(ApiError::bad_request(
                        "metastore mutation operation is required",
                    ));
                }
                Ok(Self::Metastore(mutation.clone()))
            }
            Some(domain_mutation::Kind::ScopedMetastore(mutation)) => {
                if !mutation.has_contract_operation() {
                    return Err(ApiError::bad_request(
                        "metastore mutation operation is required",
                    ));
                }
                Ok(Self::ScopedMetastore(mutation.clone()))
            }
            None => Err(ApiError::bad_request("root mutation kind is required")),
        }
    }

    const fn domain(&self) -> ControlPlaneTxDomain {
        match self {
            Self::Catalog(_) | Self::Metastore(_) | Self::ScopedMetastore(_) => {
                ControlPlaneTxDomain::Catalog
            }
            Self::Orchestration(_) => ControlPlaneTxDomain::Orchestration,
        }
    }

    const fn is_metastore(&self) -> bool {
        matches!(self, Self::Metastore(_) | Self::ScopedMetastore(_))
    }

    fn validate_request_scope(&self, meta: &ResolvedRequestMetadata) -> Result<(), ApiError> {
        let Self::ScopedMetastore(mutation) = self else {
            return Ok(());
        };
        let scope = mutation
            .scope
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("scoped metastore scope is required"))?;
        if scope.tenant_id != meta.tenant {
            return Err(ApiError::bad_request(format!(
                "scoped metastore tenant_id '{}' must match request tenant '{}'",
                scope.tenant_id, meta.tenant
            )));
        }
        if scope.workspace_id != meta.workspace {
            return Err(ApiError::bad_request(format!(
                "scoped metastore workspace_id '{}' must match request workspace '{}'",
                scope.workspace_id, meta.workspace
            )));
        }
        if scope.request_id != meta.request_id {
            return Err(ApiError::bad_request(format!(
                "scoped metastore request_id '{}' must match request_id '{}'",
                scope.request_id, meta.request_id
            )));
        }
        Ok(())
    }

    fn request_hash_value(
        &self,
        meta: &ResolvedRequestMetadata,
    ) -> Result<serde_json::Value, ApiError> {
        match self {
            Self::Catalog(mutation) => Ok(serde_json::json!({
                "domain": "catalog",
                "request": mutation.request_hash_value()?,
            })),
            Self::Orchestration(batch) => Ok(serde_json::json!({
                "domain": "orchestration",
                "request": batch.request_hash_value(meta)?,
            })),
            Self::Metastore(mutation) => Ok(serde_json::json!({
                "domain": "metastore",
                "request": {
                    "protoHex": hex::encode(mutation.encode_to_vec()),
                },
            })),
            Self::ScopedMetastore(mutation) => Ok(serde_json::json!({
                "domain": "metastore",
                "request": {
                    "protoHex": hex::encode(mutation.encode_to_vec()),
                },
            })),
        }
    }
}

impl CatalogMutation {
    fn from_proto(operation: &CatalogDdlOperation) -> Result<Self, ApiError> {
        match operation.op.as_ref() {
            Some(catalog_ddl_operation::Op::CreateCatalog(CreateCatalogOp {
                catalog,
                description,
            })) => Ok(Self::CreateCatalog {
                catalog: catalog.clone(),
                description: description.clone(),
            }),
            Some(catalog_ddl_operation::Op::CreateSchema(CreateSchemaOp {
                catalog,
                schema,
                description,
            })) => Ok(Self::CreateSchema {
                catalog: catalog.clone(),
                schema: schema.clone(),
                description: description.clone(),
            }),
            Some(catalog_ddl_operation::Op::RegisterTable(RegisterTableOp {
                catalog,
                schema,
                table,
                description,
                location,
                format,
                columns,
            })) => Ok(Self::RegisterTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                description: description.clone(),
                location: location.clone(),
                format: (*format).map(parse_table_format).transpose()?,
                columns: columns
                    .iter()
                    .map(|column| ColumnDefinition {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        is_nullable: column.is_nullable,
                        ordinal: column.ordinal,
                        description: column.description.clone(),
                    })
                    .collect(),
            }),
            Some(catalog_ddl_operation::Op::UpdateTable(UpdateTableOp {
                catalog,
                schema,
                table,
                description,
                location,
                format,
            })) => Ok(Self::UpdateTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                description: description.clone().map(Some),
                location: location.clone().map(Some),
                format: (*format).map(parse_table_format_patch).transpose()?,
            }),
            Some(catalog_ddl_operation::Op::DropTable(DropTableOp {
                catalog,
                schema,
                table,
            })) => Ok(Self::DropTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
            }),
            Some(catalog_ddl_operation::Op::RenameTable(RenameTableOp {
                catalog,
                schema,
                table,
                new_table,
            })) => Ok(Self::RenameTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                new_table: new_table.clone(),
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
            Self::CreateCatalog {
                catalog,
                description,
            } => serde_json::json!({
                "type": "create_catalog",
                "catalog": catalog,
                "description": description,
            }),
            Self::CreateSchema {
                catalog,
                schema,
                description,
            } => serde_json::json!({
                "type": "create_schema",
                "catalog": catalog,
                "schema": schema,
                "description": description,
            }),
            Self::RegisterTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
                columns,
            } => serde_json::json!({
                "type": "register_table",
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "description": description,
                "location": location,
                "format": format,
                "columns": columns.iter().map(|column| serde_json::json!({
                    "name": column.name,
                    "data_type": column.data_type,
                    "is_nullable": column.is_nullable,
                    "ordinal": column.ordinal,
                    "description": column.description,
                })).collect::<Vec<_>>(),
            }),
            Self::UpdateTable {
                catalog,
                schema,
                table,
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
                    "catalog".to_string(),
                    serde_json::Value::String(catalog.clone()),
                );
                value.insert(
                    "schema".to_string(),
                    serde_json::Value::String(schema.clone()),
                );
                value.insert(
                    "table".to_string(),
                    serde_json::Value::String(table.clone()),
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
                catalog,
                schema,
                table,
            } => serde_json::json!({
                "type": "drop_table",
                "catalog": catalog,
                "schema": schema,
                "table": table,
            }),
            Self::RenameTable {
                catalog,
                schema,
                table,
                new_table,
            } => serde_json::json!({
                "type": "rename_table",
                "catalog": catalog,
                "schema": schema,
                "table": table,
                "new_table": new_table,
            }),
        })
    }

    async fn apply(
        &self,
        writer: &CatalogWriter,
        options: WriteOptions,
    ) -> arco_catalog::Result<CatalogTransactionCommit> {
        match self {
            Self::CreateCatalog {
                catalog,
                description,
            } => {
                writer
                    .create_catalog_transaction(catalog, description.as_deref(), options)
                    .await
            }
            Self::CreateSchema {
                catalog,
                schema,
                description,
            } => {
                writer
                    .create_schema_transaction(catalog, schema, description.as_deref(), options)
                    .await
            }
            Self::RegisterTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
                columns,
            } => {
                writer
                    .register_table_in_schema_transaction(
                        catalog,
                        schema,
                        RegisterTableInSchemaRequest {
                            name: table.clone(),
                            description: description.clone(),
                            location: location.clone(),
                            format: format.clone(),
                            table_type: None,
                            properties: None,
                            columns: columns.clone(),
                        },
                        options,
                    )
                    .await
            }
            Self::UpdateTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
            } => {
                writer
                    .update_table_in_schema_transaction(
                        catalog,
                        schema,
                        table,
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
                catalog,
                schema,
                table,
            } => {
                writer
                    .drop_table_in_schema_transaction(catalog, schema, table, options)
                    .await
            }
            Self::RenameTable {
                catalog,
                schema,
                table,
                new_table,
            } => {
                writer
                    .rename_table_in_schema_transaction(catalog, schema, table, new_table, options)
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
    ExistingRepairPending(ControlPlaneIdempotencyRecord),
}

impl IdempotencyClaim {
    fn tx_id(&self) -> &str {
        match self {
            Self::Fresh(record)
            | Self::ExistingVisible(record)
            | Self::ExistingRepairPending(record) => record.tx_id.as_str(),
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
    RepairPending,
    Retryable,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TxRecordLifecycle {
    status: ControlPlaneTxStatus,
    #[serde(default)]
    repair_pending: bool,
    prepared_at: DateTime<Utc>,
}

fn parse_table_format(value: i32) -> Result<String, ApiError> {
    let format = ProtoTableFormat::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("unknown table format value: {value}")))?;
    match format {
        ProtoTableFormat::Unspecified => Err(ApiError::bad_request(
            "table format must not be TABLE_FORMAT_UNSPECIFIED",
        )),
        ProtoTableFormat::Delta => Ok("delta".to_string()),
        ProtoTableFormat::Iceberg => Ok("iceberg".to_string()),
        ProtoTableFormat::Parquet => Ok("parquet".to_string()),
    }
}

fn parse_table_format_patch(value: i32) -> Result<Option<String>, ApiError> {
    let format = ProtoTableFormat::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("unknown table format value: {value}")))?;
    match format {
        ProtoTableFormat::Unspecified => Ok(None),
        ProtoTableFormat::Delta => Ok(Some("delta".to_string())),
        ProtoTableFormat::Iceberg => Ok(Some("iceberg".to_string())),
        ProtoTableFormat::Parquet => Ok(Some("parquet".to_string())),
    }
}

fn root_request_hash(
    mutations: &[RootMutation],
    meta: &ResolvedRequestMetadata,
) -> Result<String, ApiError> {
    let value = serde_json::json!({
        "mutations": mutations
            .iter()
            .map(|mutation| mutation.request_hash_value(meta))
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

fn sanitize_runtime_events_for_request_hash(
    events: &[OrchestrationEvent],
) -> Vec<OrchestrationEvent> {
    events
        .iter()
        .cloned()
        .map(|mut event| {
            if let OrchestrationEventData::RunRequested {
                trigger_source_ref: SourceRef::Manual { request_id, .. },
                ..
            } = &mut event.data
            {
                request_id.clear();
            }

            event
        })
        .collect()
}

fn envelope_to_event(
    meta: &ResolvedRequestMetadata,
    envelope: &OrchestrationEventEnvelope,
) -> Result<OrchestrationEvent, ApiError> {
    event_from_proto_envelope(&meta.tenant, &meta.workspace, envelope)
        .map_err(|error| ApiError::bad_request(format!("invalid orchestration event: {error}")))
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

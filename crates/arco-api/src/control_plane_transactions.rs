//! Shared runtime service for single-domain control-plane transactions.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use base64::Engine as _;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use ulid::Ulid;

use arco_catalog::idempotency::canonical_request_hash;
use arco_catalog::write_options::WriteOptions;
use arco_catalog::writer::CatalogTransactionCommit;
use arco_catalog::{
    CatalogWriter, ColumnDefinition, RegisterTableRequest, TablePatch, Tier1Compactor,
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
use arco_core::storage::WritePrecondition;
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use arco_flow::orchestration_compaction_lock_path;
use arco_proto::catalog_ddl_operation::Op as CatalogDdlOp;
use arco_proto::domain_mutation::Kind as RootMutationKind;
use arco_proto::{
    AlterTableOp, ApplyCatalogDdlRequest, ApplyCatalogDdlResponse, AssetFormat, CatalogTxStatus,
    CommitOrchestrationBatchRequest, CommitOrchestrationBatchResponse,
    CommitRootTransactionRequest, CommitRootTransactionResponse, CreateNamespaceOp, DomainMutation,
    DropTableOp, GetCatalogTransactionRequest, GetCatalogTransactionResponse,
    GetOrchestrationTransactionRequest, GetOrchestrationTransactionResponse,
    GetRootTransactionRequest, GetRootTransactionResponse, OrchestrationBatchSpec,
    OrchestrationEventEnvelope, OrchestrationTxStatus, RegisterTableOp, RequestHeader,
    RootTxParticipant, RootTxStatus, TransactionDomain, TransactionStatus,
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

        if request.require_visible == Some(false) {
            return Err(ApiError::bad_request(
                "ApplyCatalogDdl requires visible publication",
            ));
        }

        let header = request
            .header
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("request header is required"))?;
        let meta = self.resolve_commit_metadata(header)?;
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
        let header = request
            .header
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("request header is required"))?;
        self.validate_request_header(header)?;
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

        if request.require_visible == Some(false) {
            return Err(ApiError::bad_request(
                "CommitOrchestrationBatch requires visible publication",
            ));
        }

        let header = request
            .header
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("request header is required"))?;
        let meta = self.resolve_commit_metadata(header)?;
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
        let header = request
            .header
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("request header is required"))?;
        self.validate_request_header(header)?;
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

        let header = request
            .header
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("request header is required"))?;
        let meta = self.resolve_commit_metadata(header)?;
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
        let header = request
            .header
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("request header is required"))?;
        self.validate_request_header(header)?;
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
        let receipt = OrchestrationTxReceipt {
            tx_id: tx_id.clone(),
            // The current orchestration compactor surfaces one stable manifest revision ULID.
            // Until a distinct audit-chain commit artifact exists, use that revision as commit_id.
            commit_id: commit.manifest_revision.clone(),
            manifest_id: commit.manifest_id.clone(),
            revision_ulid: commit.manifest_revision,
            delta_id: commit.delta_id.unwrap_or_default(),
            pointer_version: commit.pointer_version,
            events_processed: commit.events_processed,
            read_token: format!("orchestration:{}", commit.manifest_id),
            visible_at,
        };

        self.finalize_visible(
            ControlPlaneTxDomain::Orchestration,
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

    fn resolve_commit_metadata(
        &self,
        header: &RequestHeader,
    ) -> Result<ResolvedRequestMetadata, ApiError> {
        self.validate_request_header(header)?;

        let idempotency_key = if header.idempotency_key.is_empty() {
            self.ctx
                .idempotency_key
                .clone()
                .ok_or_else(|| ApiError::bad_request("idempotency_key is required"))?
        } else if let Some(transport_key) = self.ctx.idempotency_key.as_deref() {
            if header.idempotency_key != transport_key {
                return Err(ApiError::bad_request(
                    "request header idempotency_key does not match transport idempotency key",
                ));
            }
            header.idempotency_key.clone()
        } else {
            header.idempotency_key.clone()
        };

        if idempotency_key.is_empty() {
            return Err(ApiError::bad_request("idempotency_key is required"));
        }

        Ok(ResolvedRequestMetadata {
            tenant: self.ctx.tenant.clone(),
            workspace: self.ctx.workspace.clone(),
            request_id: header.request_id.clone(),
            idempotency_key,
        })
    }

    fn validate_request_header(&self, header: &RequestHeader) -> Result<(), ApiError> {
        let body_tenant = header
            .tenant_id
            .as_ref()
            .map(|tenant| tenant.value.as_str())
            .filter(|value| !value.is_empty())
            .unwrap_or(self.ctx.tenant.as_str());
        if body_tenant != self.ctx.tenant {
            return Err(ApiError::bad_request(
                "request header tenant_id does not match request scope",
            ));
        }

        let body_workspace = header
            .workspace_id
            .as_ref()
            .map(|workspace| workspace.value.as_str())
            .filter(|value| !value.is_empty())
            .unwrap_or(self.ctx.workspace.as_str());
        if body_workspace != self.ctx.workspace {
            return Err(ApiError::bad_request(
                "request header workspace_id does not match request scope",
            ));
        }

        if header.request_id.is_empty() {
            return Err(ApiError::bad_request("request_id is required"));
        }
        if header.request_id != self.ctx.request_id {
            return Err(ApiError::bad_request(
                "request header request_id does not match transport request id",
            ));
        }
        Ok(())
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
    CreateNamespace {
        name: String,
        description: Option<String>,
        properties: BTreeMap<String, String>,
    },
    RegisterTable {
        namespace: String,
        name: String,
        description: Option<String>,
        location: Option<String>,
        format: Option<String>,
        columns: Vec<ColumnDefinition>,
        properties: BTreeMap<String, String>,
    },
    AlterTable {
        namespace: String,
        name: String,
        description: Option<Option<String>>,
        location: Option<Option<String>>,
        format: Option<Option<String>>,
    },
    DropTable {
        namespace: String,
        name: String,
    },
}

#[derive(Debug, Clone)]
struct OrchestrationBatchMutation {
    events: Vec<OrchestrationEventEnvelope>,
    allow_inline_merge: bool,
}

impl OrchestrationBatchMutation {
    fn from_request(request: &CommitOrchestrationBatchRequest) -> Result<Self, ApiError> {
        Self::from_parts(&request.events, request.allow_inline_merge.unwrap_or(false))
    }

    fn from_spec(spec: &OrchestrationBatchSpec) -> Result<Self, ApiError> {
        Self::from_parts(&spec.events, spec.allow_inline_merge.unwrap_or(false))
    }

    fn from_parts(
        events: &[OrchestrationEventEnvelope],
        allow_inline_merge: bool,
    ) -> Result<Self, ApiError> {
        if allow_inline_merge {
            return Err(ApiError::bad_request(
                "CommitOrchestrationBatch allow_inline_merge is not supported",
            ));
        }

        Ok(Self {
            events: events.to_vec(),
            allow_inline_merge,
        })
    }

    fn request_hash_value(&self) -> serde_json::Value {
        serde_json::json!({
            "events": self.events.iter().map(|event| serde_json::json!({
                "event_id": event.event_id,
                "event_type": event.event_type,
                "event_version": event.event_version,
                "timestamp": event.timestamp.as_ref().map(|ts| serde_json::json!({
                    "seconds": ts.seconds,
                    "nanos": ts.nanos,
                })),
                "source": event.source,
                "idempotency_key": event.idempotency_key,
                "correlation_id": event.correlation_id,
                "causation_id": event.causation_id,
                "payload_json": base64::engine::general_purpose::STANDARD.encode(&event.payload_json),
            })).collect::<Vec<_>>(),
            "require_visible": true,
            "allow_inline_merge": self.allow_inline_merge,
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
            Some(RootMutationKind::Catalog(operation)) => {
                Ok(Self::Catalog(CatalogMutation::from_proto(operation)?))
            }
            Some(RootMutationKind::Orchestration(spec)) => Ok(Self::Orchestration(
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
    fn from_proto(operation: &arco_proto::CatalogDdlOperation) -> Result<Self, ApiError> {
        match operation.op.as_ref() {
            Some(CatalogDdlOp::CreateNamespace(CreateNamespaceOp {
                name,
                description,
                properties,
            })) => {
                if !properties.is_empty() {
                    return Err(ApiError::bad_request(
                        "catalog namespace properties are not supported",
                    ));
                }
                Ok(Self::CreateNamespace {
                    name: name.clone(),
                    description: description.clone(),
                    properties: properties.clone(),
                })
            }
            Some(CatalogDdlOp::RegisterTable(RegisterTableOp {
                namespace,
                name,
                description,
                location,
                format,
                columns,
                properties,
            })) => {
                if !properties.is_empty() {
                    return Err(ApiError::bad_request(
                        "catalog table properties are not supported",
                    ));
                }
                Ok(Self::RegisterTable {
                    namespace: namespace.clone(),
                    name: name.clone(),
                    description: description.clone(),
                    location: location.clone(),
                    format: format
                        .map(parse_register_table_format)
                        .transpose()?
                        .flatten(),
                    columns: columns
                        .iter()
                        .map(|column| ColumnDefinition {
                            name: column.name.clone(),
                            data_type: column.data_type.clone(),
                            is_nullable: column.is_nullable,
                            description: column.description.clone(),
                        })
                        .collect(),
                    properties: properties.clone(),
                })
            }
            Some(CatalogDdlOp::AlterTable(AlterTableOp {
                namespace,
                name,
                description,
                location,
                format,
            })) => Ok(Self::AlterTable {
                namespace: namespace.clone(),
                name: name.clone(),
                description: description.clone().map(Some),
                location: location.clone().map(Some),
                format: format.map(parse_alter_table_format).transpose()?,
            }),
            Some(CatalogDdlOp::DropTable(DropTableOp { namespace, name })) => Ok(Self::DropTable {
                namespace: namespace.clone(),
                name: name.clone(),
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
            Self::CreateNamespace {
                name,
                description,
                properties,
            } => serde_json::json!({
                "type": "create_namespace",
                "name": name,
                "description": description,
                "properties": properties,
                "require_visible": true,
            }),
            Self::RegisterTable {
                namespace,
                name,
                description,
                location,
                format,
                columns,
                properties,
            } => serde_json::json!({
                "type": "register_table",
                "namespace": namespace,
                "name": name,
                "description": description,
                "location": location,
                "format": format,
                "columns": columns.iter().map(|column| serde_json::json!({
                    "name": column.name,
                    "data_type": column.data_type,
                    "is_nullable": column.is_nullable,
                    "description": column.description,
                })).collect::<Vec<_>>(),
                "properties": properties,
                "require_visible": true,
            }),
            Self::AlterTable {
                namespace,
                name,
                description,
                location,
                format,
            } => {
                let mut value = serde_json::Map::new();
                value.insert(
                    "type".to_string(),
                    serde_json::Value::String("alter_table".to_string()),
                );
                value.insert(
                    "namespace".to_string(),
                    serde_json::Value::String(namespace.clone()),
                );
                value.insert("name".to_string(), serde_json::Value::String(name.clone()));
                if let Some(description) = description {
                    value.insert(
                        "description".to_string(),
                        serde_json::to_value(description).map_err(|error| {
                            ApiError::internal(format!(
                                "failed to serialize alter_table description for hashing: {error}"
                            ))
                        })?,
                    );
                }
                if let Some(location) = location {
                    value.insert(
                        "location".to_string(),
                        serde_json::to_value(location).map_err(|error| {
                            ApiError::internal(format!(
                                "failed to serialize alter_table location for hashing: {error}"
                            ))
                        })?,
                    );
                }
                if let Some(format) = format {
                    value.insert(
                        "format".to_string(),
                        serde_json::to_value(format).map_err(|error| {
                            ApiError::internal(format!(
                                "failed to serialize alter_table format for hashing: {error}"
                            ))
                        })?,
                    );
                }
                value.insert("require_visible".to_string(), serde_json::Value::Bool(true));
                serde_json::Value::Object(value)
            }
            Self::DropTable { namespace, name } => serde_json::json!({
                "type": "drop_table",
                "namespace": namespace,
                "name": name,
                "require_visible": true,
            }),
        })
    }

    async fn apply(
        &self,
        writer: &CatalogWriter,
        options: WriteOptions,
    ) -> arco_catalog::Result<CatalogTransactionCommit> {
        match self {
            Self::CreateNamespace {
                name, description, ..
            } => {
                writer
                    .create_namespace_transaction(name, description.as_deref(), options)
                    .await
            }
            Self::RegisterTable {
                namespace,
                name,
                description,
                location,
                format,
                columns,
                ..
            } => {
                writer
                    .register_table_transaction(
                        RegisterTableRequest {
                            namespace: namespace.clone(),
                            name: name.clone(),
                            description: description.clone(),
                            location: location.clone(),
                            format: format.clone(),
                            columns: columns.clone(),
                        },
                        options,
                    )
                    .await
            }
            Self::AlterTable {
                namespace,
                name,
                description,
                location,
                format,
            } => {
                writer
                    .update_table_transaction(
                        namespace,
                        name,
                        TablePatch {
                            description: description.clone(),
                            location: location.clone(),
                            format: format.clone(),
                        },
                        options,
                    )
                    .await
            }
            Self::DropTable { namespace, name } => {
                writer
                    .drop_table_transaction(namespace, name, options)
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

fn asset_format_string(format: AssetFormat) -> Option<String> {
    match format {
        AssetFormat::Unspecified => None,
        AssetFormat::Parquet => Some("parquet".to_string()),
        AssetFormat::Delta => Some("delta".to_string()),
        AssetFormat::Iceberg => Some("iceberg".to_string()),
    }
}

fn parse_register_table_format(value: i32) -> Result<Option<String>, ApiError> {
    let format = AssetFormat::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("unknown asset format value: {value}")))?;
    Ok(asset_format_string(format))
}

fn parse_alter_table_format(value: i32) -> Result<Option<String>, ApiError> {
    let format = AssetFormat::try_from(value)
        .map_err(|_| ApiError::bad_request(format!("unknown asset format value: {value}")))?;
    Ok(asset_format_string(format))
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
    let payload =
        serde_json::from_slice::<serde_json::Value>(&envelope.payload_json).map_err(|error| {
            ApiError::bad_request(format!(
                "failed to parse orchestration payload for event '{}': {error}",
                envelope.event_id
            ))
        })?;
    let mut payload_object = payload.as_object().cloned().ok_or_else(|| {
        ApiError::bad_request(format!(
            "orchestration payload for event '{}' must be a JSON object",
            envelope.event_id
        ))
    })?;
    payload_object.insert(
        "type".to_string(),
        serde_json::Value::String(camel_to_snake(envelope.event_type.as_str())),
    );
    let data =
        serde_json::from_value::<OrchestrationEventData>(serde_json::Value::Object(payload_object))
            .map_err(|error| {
                ApiError::bad_request(format!(
                    "failed to decode orchestration event payload for '{}': {error}",
                    envelope.event_id
                ))
            })?;

    if data.event_type() != envelope.event_type {
        return Err(ApiError::bad_request(format!(
            "orchestration event_type '{}' does not match payload type '{}'",
            envelope.event_type,
            data.event_type()
        )));
    }

    Ok(OrchestrationEvent {
        event_id: envelope.event_id.clone(),
        event_type: envelope.event_type.clone(),
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
        correlation_id: envelope.correlation_id.clone(),
        causation_id: envelope.causation_id.clone(),
        data,
    })
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

fn catalog_receipt_to_proto(receipt: &CatalogTxReceipt) -> arco_proto::CatalogTxReceipt {
    arco_proto::CatalogTxReceipt {
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

fn orchestration_receipt_to_proto(
    receipt: &OrchestrationTxReceipt,
) -> arco_proto::OrchestrationTxReceipt {
    arco_proto::OrchestrationTxReceipt {
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

fn root_receipt_to_proto(receipt: &RootTxReceipt) -> arco_proto::RootTxReceipt {
    arco_proto::RootTxReceipt {
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
        request_id: record.request_id.clone(),
        idempotency_key: record.idempotency_key.clone(),
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
        request_id: record.request_id.clone(),
        idempotency_key: record.idempotency_key.clone(),
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
        request_id: record.request_id.clone(),
        idempotency_key: record.idempotency_key.clone(),
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

fn domain_commit_to_proto(commit: &DomainCommit) -> arco_proto::DomainCommit {
    arco_proto::DomainCommit {
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

fn camel_to_snake(value: &str) -> String {
    let mut output = String::new();
    for (index, ch) in value.chars().enumerate() {
        if ch.is_uppercase() {
            if index > 0 {
                output.push('_');
            }
            output.extend(ch.to_lowercase());
        } else {
            output.push(ch);
        }
    }
    output
}

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
use arco_catalog::manifest::CommitRecord;
use arco_catalog::write_options::{CatalogTransactionIdentity, WriteOptions};
use arco_catalog::writer::{CatalogTransactionCommit, CatalogTransactionRequest};
use arco_catalog::{
    CatalogWriter, ColumnDefinition, RegisterTableInSchemaRequest, TablePatch, Tier1Compactor,
};
use arco_core::ScopedStorage;
use arco_core::canonical_json::to_canonical_bytes;
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
use arco_flow::orchestration::ledger::LedgerWriter;
use arco_flow::orchestration::proto::event_from_proto_envelope;
use arco_flow::orchestration::state::{
    OrchestrationPublicationWitness, validate_selected_orchestration_publication,
};
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

mod handles;
#[cfg(test)]
mod handles_tests;

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
        Box::pin(self.commit_root_transaction_with_policy(
            request,
            IdempotencyClaimPolicy::LegacyReplaceRetryable,
        ))
        .await
    }

    async fn commit_root_transaction_for_handle(
        &self,
        request: CommitRootTransactionRequest,
    ) -> Result<CommitRootTransactionResponse, ApiError> {
        Box::pin(self.commit_root_transaction_with_policy(
            request,
            IdempotencyClaimPolicy::FrozenHandle {
                expected_tx_id: None,
            },
        ))
        .await
    }

    async fn commit_root_transaction_with_policy(
        &self,
        request: CommitRootTransactionRequest,
        claim_policy: IdempotencyClaimPolicy<'_>,
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
                claim_policy,
            )
            .await?;

        if let IdempotencyClaim::ExistingVisible(existing) = &claim {
            let record = self
                .resolve_existing_visible_record_with_policy::<RootTxReceipt>(
                    ControlPlaneTxDomain::Root,
                    idempotency_path.as_str(),
                    existing,
                    claim_policy.visible_marker_policy(),
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

        let root_lock_path = ControlPlaneTxPaths::root_lock();
        let prepared = if let IdempotencyClaim::ExistingPrepared(existing) = &claim {
            self.load_frozen_prepared::<RootTxReceipt>(
                ControlPlaneTxDomain::Root,
                existing,
                &root_lock_path,
            )
            .await?
        } else {
            let prepared = RootTxRecord {
                tx_id: claim.tx_id().to_string(),
                kind: ControlPlaneTxKind::RootCommit,
                status: ControlPlaneTxStatus::Prepared,
                repair_pending: false,
                request_id: meta.request_id.clone(),
                idempotency_key: meta.idempotency_key.clone(),
                request_hash,
                lock_path: root_lock_path.clone(),
                fencing_token: 0,
                prepared_at: Utc::now(),
                visible_at: None,
                durable_append: None,
                result: None,
            };
            self.store_prepared(ControlPlaneTxDomain::Root, &prepared)
                .await?;
            prepared
        };
        let tx_id = prepared.tx_id.clone();

        let result = self
            .execute_claimed_root(
                &meta,
                mutations,
                &tx_id,
                &idempotency_path,
                &prepared.request_hash,
                false,
                claim_policy.is_frozen(),
            )
            .await;

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

    #[cfg_attr(not(test), allow(dead_code))]
    async fn recover_root_transaction_in_place(
        &self,
        request: CommitRootTransactionRequest,
        expected_tx_id: &str,
        expected_request_hash: &str,
    ) -> Result<TxExecutionOutcome<RootTxReceipt>, ApiError> {
        request
            .validate_contract()
            .map_err(|error| ApiError::bad_request(error.to_string()))?;
        let meta = self.resolve_commit_metadata()?;
        let mutations = request
            .mutations
            .iter()
            .map(RootMutation::from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let computed_hash = root_request_hash(&mutations, &meta)?;
        if computed_hash != expected_request_hash {
            return Err(ApiError::conflict(
                "root recovery request does not match the reviewed staged mutation",
            ));
        }
        let idempotency_path = ControlPlaneTxPaths::idempotency(
            ControlPlaneTxDomain::Root,
            meta.idempotency_key.as_str(),
        );
        let marker = self
            .load_json_with_version_required::<ControlPlaneIdempotencyRecord>(&idempotency_path)
            .await?
            .ok_or_else(|| ApiError::conflict("root recovery idempotency marker is missing"))?;
        if marker.value.tx_id != expected_tx_id
            || marker.value.kind != ControlPlaneTxKind::RootCommit
            || marker.value.request_id != meta.request_id
            || marker.value.idempotency_key != meta.idempotency_key
            || marker.value.request_hash != expected_request_hash
        {
            return Err(ApiError::conflict(
                "root recovery idempotency ownership changed",
            ));
        }
        if marker.value.tx_record.is_none() && marker.value.visible_at.is_some() {
            return Err(ApiError::conflict(
                "root recovery claim-only marker contains visibility evidence",
            ));
        }
        if marker.value.tx_record.is_some() {
            let record = self
                .resolve_existing_visible_record_with_policy::<RootTxReceipt>(
                    ControlPlaneTxDomain::Root,
                    &idempotency_path,
                    &marker.value,
                    VisibleMarkerPolicy::DeferredForHandleValidation,
                )
                .await?;
            let receipt = record
                .result
                .ok_or_else(|| ApiError::internal("visible root transaction is missing result"))?;
            return Ok(TxExecutionOutcome {
                receipt,
                repair_pending: record.repair_pending,
            });
        }

        let record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, expected_tx_id);
        let stored = self
            .load_json_with_version_required::<RootTxRecord>(&record_path)
            .await?
            .ok_or_else(|| ApiError::conflict("root recovery transaction record is missing"))?;
        if stored.value.tx_id != expected_tx_id
            || stored.value.kind != ControlPlaneTxKind::RootCommit
            || stored.value.request_id != meta.request_id
            || stored.value.idempotency_key != meta.idempotency_key
            || stored.value.request_hash != expected_request_hash
        {
            return Err(ApiError::conflict(
                "root recovery transaction ownership changed",
            ));
        }
        if stored.value.status == ControlPlaneTxStatus::Visible {
            let record = self
                .resolve_existing_visible_record_with_policy::<RootTxReceipt>(
                    ControlPlaneTxDomain::Root,
                    &idempotency_path,
                    &marker.value,
                    VisibleMarkerPolicy::DeferredForHandleValidation,
                )
                .await?;
            let receipt = record
                .result
                .ok_or_else(|| ApiError::internal("visible root transaction is missing result"))?;
            return Ok(TxExecutionOutcome {
                receipt,
                repair_pending: record.repair_pending,
            });
        }
        if !matches!(
            stored.value.status,
            ControlPlaneTxStatus::Prepared | ControlPlaneTxStatus::Aborted
        ) {
            return Err(ApiError::conflict(
                "root recovery transaction status is unsupported",
            ));
        }
        if stored.value.lock_path != ControlPlaneTxPaths::root_lock()
            || stored.value.fencing_token != 0
            || stored.value.visible_at.is_some()
            || stored.value.result.is_some()
            || stored.value.durable_append.is_some()
        {
            return Err(ApiError::conflict(
                "root recovery predecessor has non-canonical pre-visibility authority",
            ));
        }

        let observed_child_domains = self
            .preflight_root_recovery_children(&meta, &mutations)
            .await?;
        let super_manifest_path = ControlPlaneTxPaths::root_super_manifest(expected_tx_id);
        if let Some(existing) = self
            .load_json_required::<RootTxManifest>(&super_manifest_path)
            .await?
        {
            let expected_domains = mutations
                .iter()
                .map(RootMutation::domain)
                .collect::<BTreeSet<_>>();
            let manifest_domains = existing.domains.keys().copied().collect::<BTreeSet<_>>();
            if existing.tx_id != expected_tx_id
                || existing.fencing_token == 0
                || existing.published_at < stored.value.prepared_at
                || manifest_domains != expected_domains
                || observed_child_domains.as_ref() != Some(&existing.domains)
            {
                return Err(ApiError::conflict(
                    "existing root super-manifest has invalid recovery authority",
                ));
            }
        }

        let mut rearmed = stored.value;
        rearmed.status = ControlPlaneTxStatus::Prepared;
        rearmed.repair_pending = true;
        rearmed.visible_at = None;
        rearmed.result = None;
        match self
            .write_json(
                &record_path,
                &rearmed,
                WritePrecondition::MatchesVersion(stored.version),
            )
            .await?
        {
            WriteOutcome::Written => {}
            WriteOutcome::PreconditionFailed => {
                return Err(ApiError::conflict(
                    "root recovery record changed while it was rearmed",
                ));
            }
        }
        let marker_after = self
            .load_json_with_version_required::<ControlPlaneIdempotencyRecord>(&idempotency_path)
            .await?
            .ok_or_else(|| ApiError::conflict("root recovery marker disappeared"))?;
        let record_after = self
            .load_json_required::<RootTxRecord>(&record_path)
            .await?
            .ok_or_else(|| ApiError::conflict("root recovery record disappeared"))?;
        if marker_after.version != marker.version
            || marker_after.value != marker.value
            || marker_after.value.tx_id != expected_tx_id
            || marker_after.value.kind != ControlPlaneTxKind::RootCommit
            || marker_after.value.request_id != meta.request_id
            || marker_after.value.idempotency_key != meta.idempotency_key
            || marker_after.value.request_hash != expected_request_hash
            || record_after.tx_id != expected_tx_id
            || record_after.kind != ControlPlaneTxKind::RootCommit
            || record_after.request_id != meta.request_id
            || record_after.idempotency_key != meta.idempotency_key
            || record_after.request_hash != expected_request_hash
            || record_after.status != ControlPlaneTxStatus::Prepared
            || !record_after.repair_pending
        {
            return Err(ApiError::conflict(
                "root recovery ownership changed after fencing",
            ));
        }

        self.execute_claimed_root(
            &meta,
            mutations,
            expected_tx_id,
            &idempotency_path,
            expected_request_hash,
            true,
            true,
        )
        .await
    }

    async fn execute_claimed_root(
        &self,
        meta: &ResolvedRequestMetadata,
        mutations: Vec<RootMutation>,
        tx_id: &str,
        idempotency_path: &str,
        expected_request_hash: &str,
        adopt_existing_manifest: bool,
        handle_owned: bool,
    ) -> Result<TxExecutionOutcome<RootTxReceipt>, ApiError> {
        let root_lock_path = ControlPlaneTxPaths::root_lock();
        let root_lock = DistributedLock::new(self.storage.backend().clone(), &root_lock_path);
        let guard = root_lock
            .acquire(DEFAULT_LOCK_TTL, 10)
            .await
            .map_err(|error| {
                ApiError::conflict(format!("failed to acquire root transaction lock: {error}"))
            })?;
        let fencing_token = guard.fencing_token().sequence();
        let result = async {
            if mutations.iter().any(RootMutation::is_metastore) {
                return Err(ApiError::not_implemented(
                    "metastore root mutations are not implemented yet",
                ));
            }

            if adopt_existing_manifest
                && let Some(outcome) = self
                    .resolve_visible_root_recovery(
                        meta,
                        tx_id,
                        idempotency_path,
                        expected_request_hash,
                        if handle_owned {
                            VisibleMarkerPolicy::DeferredForHandleValidation
                        } else {
                            VisibleMarkerPolicy::Immediate
                        },
                    )
                    .await?
            {
                return Ok(outcome);
            }

            let mut repair_pending = false;
            let mut domain_commits = Vec::with_capacity(mutations.len());
            let mut manifest_domains = BTreeMap::new();
            for mutation in &mutations {
                let participant_meta = self.root_participant_metadata(meta, mutation.domain());
                match mutation {
                    RootMutation::Catalog(command) => {
                        let outcome = if handle_owned {
                            self.execute_catalog_mutation_for_handle(
                                &participant_meta,
                                command.clone(),
                                None,
                            )
                            .await?
                        } else {
                            self.execute_catalog_mutation(&participant_meta, command.clone())
                                .await?
                        };
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
                        let outcome = if handle_owned {
                            self.execute_orchestration_batch_for_handle(
                                &participant_meta,
                                batch.clone(),
                                None,
                            )
                            .await?
                        } else {
                            self.execute_orchestration_batch(&participant_meta, batch.clone())
                                .await?
                        };
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

            if handle_owned {
                let validated_domains = self
                    .preflight_root_recovery_children(meta, &mutations)
                    .await?
                    .ok_or_else(|| {
                        ApiError::conflict(
                            "root child authority is not exact-visible before manifest publication",
                        )
                    })?;
                if validated_domains != manifest_domains {
                    return Err(ApiError::conflict(
                        "root child authority changed before manifest publication",
                    ));
                }
            }

            let proposed_visible_at = Utc::now();
            let super_manifest_path = ControlPlaneTxPaths::root_super_manifest(tx_id);
            let super_manifest = RootTxManifest {
                tx_id: tx_id.to_string(),
                fencing_token,
                published_at: proposed_visible_at,
                domains: manifest_domains.clone(),
            };
            let (visible_at, finalized_fencing_token) = match self
                .write_json(
                    &super_manifest_path,
                    &super_manifest,
                    WritePrecondition::DoesNotExist,
                )
                .await?
            {
                WriteOutcome::Written => (proposed_visible_at, fencing_token),
                WriteOutcome::PreconditionFailed if adopt_existing_manifest => {
                    let existing = self
                        .load_json_required::<RootTxManifest>(&super_manifest_path)
                        .await?
                        .ok_or_else(|| {
                            ApiError::conflict("root super-manifest disappeared during recovery")
                        })?;
                    let claim = self
                        .load_record::<RootTxReceipt>(ControlPlaneTxDomain::Root, tx_id)
                        .await?
                        .ok_or_else(|| {
                            ApiError::conflict(
                                "root transaction claim disappeared during manifest adoption",
                            )
                        })?;
                    if existing.tx_id != tx_id
                        || existing.domains != manifest_domains
                        || existing.fencing_token == 0
                        || existing.published_at < claim.prepared_at
                    {
                        return Err(ApiError::conflict(
                            "existing root super-manifest does not match recovered participants",
                        ));
                    }
                    (existing.published_at, existing.fencing_token)
                }
                WriteOutcome::PreconditionFailed => {
                    return Err(ApiError::conflict(format!(
                        "root super-manifest already exists: {super_manifest_path}"
                    )));
                }
            };

            let proposed_receipt = RootTxReceipt {
                tx_id: tx_id.to_string(),
                root_commit_id: Ulid::new().to_string(),
                super_manifest_path: super_manifest_path.clone(),
                domain_commits,
                read_token: format!("root:{tx_id}"),
                visible_at,
            };
            let winner = self
                .finalize_visible(
                    ControlPlaneTxDomain::Root,
                    tx_id,
                    idempotency_path,
                    root_lock_path.clone(),
                    finalized_fencing_token,
                    repair_pending || handle_owned,
                    visible_at,
                    proposed_receipt,
                    if handle_owned {
                        VisibleMarkerPolicy::DeferredForHandleValidation
                    } else {
                        VisibleMarkerPolicy::Immediate
                    },
                )
                .await?;
            let receipt = winner.result.clone().ok_or_else(|| {
                ApiError::internal("visible root transaction winner is missing result")
            })?;

            if handle_owned {
                return Ok(TxExecutionOutcome {
                    receipt,
                    repair_pending: winner.repair_pending,
                });
            }

            let mut root_repair_pending = winner.repair_pending;
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
            if root_repair_pending && !winner.repair_pending
                && let Err(error) = self
                    .mark_visible_repair_pending::<RootTxReceipt>(
                        ControlPlaneTxDomain::Root,
                        tx_id,
                        idempotency_path,
                        if handle_owned {
                            VisibleMarkerPolicy::DeferredForHandleValidation
                        } else {
                            VisibleMarkerPolicy::Immediate
                        },
                    )
                    .await
            {
                tracing::warn!(
                    error = ?error,
                    tx_id,
                    "failed to mark visible root transaction repair_pending after audit receipt failure"
                );
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
        result
    }

    async fn resolve_visible_root_recovery(
        &self,
        meta: &ResolvedRequestMetadata,
        tx_id: &str,
        idempotency_path: &str,
        expected_request_hash: &str,
        visible_marker_policy: VisibleMarkerPolicy,
    ) -> Result<Option<TxExecutionOutcome<RootTxReceipt>>, ApiError> {
        let marker = self
            .load_json_required::<ControlPlaneIdempotencyRecord>(idempotency_path)
            .await?
            .ok_or_else(|| ApiError::conflict("root recovery idempotency marker disappeared"))?;
        if marker.tx_id != tx_id
            || marker.kind != ControlPlaneTxKind::RootCommit
            || marker.request_id != meta.request_id
            || marker.idempotency_key != meta.idempotency_key
            || marker.request_hash != expected_request_hash
        {
            return Err(ApiError::conflict(
                "root recovery ownership changed while waiting for the root lock",
            ));
        }
        let stored = self
            .load_record::<RootTxReceipt>(ControlPlaneTxDomain::Root, tx_id)
            .await?;
        let visibility_exists = marker.tx_record.is_some()
            || stored
                .as_ref()
                .is_some_and(|record| record.status == ControlPlaneTxStatus::Visible);
        if !visibility_exists {
            return Ok(None);
        }
        let record = self
            .resolve_existing_visible_record_with_policy::<RootTxReceipt>(
                ControlPlaneTxDomain::Root,
                idempotency_path,
                &marker,
                visible_marker_policy,
            )
            .await?;
        if record.tx_id != tx_id
            || record.kind != ControlPlaneTxKind::RootCommit
            || record.request_id != meta.request_id
            || record.idempotency_key != meta.idempotency_key
            || record.request_hash != expected_request_hash
            || record.request_hash != marker.request_hash
            || record.status != ControlPlaneTxStatus::Visible
        {
            return Err(ApiError::conflict(
                "visible root recovery record does not match its frozen participant",
            ));
        }
        let receipt = record
            .result
            .ok_or_else(|| ApiError::internal("visible root transaction is missing result"))?;
        Ok(Some(TxExecutionOutcome {
            receipt,
            repair_pending: record.repair_pending,
        }))
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
        self.execute_catalog_mutation_with_policy(
            meta,
            command,
            IdempotencyClaimPolicy::LegacyReplaceRetryable,
        )
        .await
    }

    async fn execute_catalog_mutation_for_handle(
        &self,
        meta: &ResolvedRequestMetadata,
        command: CatalogMutation,
        expected_tx_id: Option<&str>,
    ) -> Result<TxExecutionOutcome<CatalogTxReceipt>, ApiError> {
        self.execute_catalog_mutation_with_policy(
            meta,
            command,
            IdempotencyClaimPolicy::FrozenHandle { expected_tx_id },
        )
        .await
    }

    async fn execute_catalog_mutation_with_policy(
        &self,
        meta: &ResolvedRequestMetadata,
        command: CatalogMutation,
        claim_policy: IdempotencyClaimPolicy<'_>,
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
                claim_policy,
            )
            .await?;

        if let IdempotencyClaim::ExistingVisible(existing) = &claim {
            let record = self
                .resolve_existing_visible_record_with_policy::<CatalogTxReceipt>(
                    ControlPlaneTxDomain::Catalog,
                    idempotency_path.as_str(),
                    existing,
                    claim_policy.visible_marker_policy(),
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

        let prepared = if let IdempotencyClaim::ExistingPrepared(existing) = &claim {
            self.load_frozen_prepared::<CatalogTxReceipt>(
                ControlPlaneTxDomain::Catalog,
                existing,
                &CatalogPaths::domain_lock(CatalogDomain::Catalog),
            )
            .await?
        } else {
            let prepared = CatalogTxRecord {
                tx_id: claim.tx_id().to_string(),
                kind: ControlPlaneTxKind::CatalogDdl,
                status: ControlPlaneTxStatus::Prepared,
                repair_pending: false,
                request_id: meta.request_id.clone(),
                idempotency_key: meta.idempotency_key.clone(),
                request_hash,
                lock_path: CatalogPaths::domain_lock(CatalogDomain::Catalog),
                fencing_token: 0,
                prepared_at: Utc::now(),
                visible_at: None,
                durable_append: None,
                result: None,
            };
            self.store_prepared(ControlPlaneTxDomain::Catalog, &prepared)
                .await?;
            prepared
        };
        let tx_id = prepared.tx_id.clone();

        let writer = self.catalog_writer()?;
        if let Err(error) = writer.initialize().await.map_err(ApiError::from) {
            self.abort_transaction(ControlPlaneTxDomain::Catalog, &tx_id)
                .await;
            return Err(error);
        }
        let transaction_identity = if claim_policy.is_frozen() {
            Some(
                writer
                    .authorize_frozen_catalog_transaction(
                        &tx_id,
                        &prepared.request_hash,
                        &prepared.request_id,
                        &prepared.idempotency_key,
                    )
                    .await
                    .map_err(ApiError::from)?,
            )
        } else {
            None
        };
        let options = self.catalog_write_options(meta, transaction_identity.as_ref());
        let recovered = if let Some(identity) = &transaction_identity {
            writer
                .recover_catalog_transaction(identity, Some(meta.request_id.clone()))
                .await
                .map_err(ApiError::from)?
        } else {
            None
        };
        let commit = match if let Some(commit) = recovered {
            Ok(commit)
        } else {
            command
                .apply(&writer, options)
                .await
                .map_err(ApiError::from)
        } {
            Ok(commit) => commit,
            Err(error) => {
                if !claim_policy.is_frozen() {
                    self.abort_transaction(ControlPlaneTxDomain::Catalog, &tx_id)
                        .await;
                }
                return Err(error);
            }
        };

        let visible_at = Utc::now();
        let read_token = format!("catalog:{}", commit.manifest_id);
        let proposed_receipt = CatalogTxReceipt {
            tx_id: tx_id.clone(),
            event_id: commit.event_id,
            commit_id: commit.commit_id,
            manifest_id: commit.manifest_id,
            snapshot_version: commit.snapshot_version,
            pointer_version: commit.pointer_version,
            read_token,
            visible_at,
        };

        let winner = self
            .finalize_visible(
                ControlPlaneTxDomain::Catalog,
                &tx_id,
                idempotency_path.as_str(),
                commit.lock_path,
                commit.fencing_token,
                commit.repair_pending,
                visible_at,
                proposed_receipt,
                claim_policy.visible_marker_policy(),
            )
            .await?;
        let receipt = winner.result.ok_or_else(|| {
            ApiError::internal("visible catalog transaction winner is missing result")
        })?;

        Ok(TxExecutionOutcome {
            receipt,
            repair_pending: winner.repair_pending,
        })
    }

    #[allow(clippy::cognitive_complexity)]
    async fn execute_orchestration_batch(
        &self,
        meta: &ResolvedRequestMetadata,
        batch: OrchestrationBatchMutation,
    ) -> Result<TxExecutionOutcome<OrchestrationTxReceipt>, ApiError> {
        self.execute_orchestration_batch_with_policy(
            meta,
            batch,
            IdempotencyClaimPolicy::LegacyReplaceRetryable,
        )
        .await
    }

    async fn execute_orchestration_batch_for_handle(
        &self,
        meta: &ResolvedRequestMetadata,
        batch: OrchestrationBatchMutation,
        expected_tx_id: Option<&str>,
    ) -> Result<TxExecutionOutcome<OrchestrationTxReceipt>, ApiError> {
        self.execute_orchestration_batch_with_policy(
            meta,
            batch,
            IdempotencyClaimPolicy::FrozenHandle { expected_tx_id },
        )
        .await
    }

    #[allow(clippy::cognitive_complexity)]
    async fn execute_orchestration_batch_with_policy(
        &self,
        meta: &ResolvedRequestMetadata,
        batch: OrchestrationBatchMutation,
        claim_policy: IdempotencyClaimPolicy<'_>,
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
                claim_policy,
            )
            .await?;

        if let IdempotencyClaim::ExistingVisible(existing) = &claim {
            let record = self
                .resolve_existing_visible_record_with_policy::<OrchestrationTxReceipt>(
                    ControlPlaneTxDomain::Orchestration,
                    idempotency_path.as_str(),
                    existing,
                    claim_policy.visible_marker_policy(),
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
                .repair_prepared_orchestration_batch(
                    meta,
                    idempotency_path.as_str(),
                    existing,
                    &events,
                    claim_policy.visible_marker_policy(),
                )
                .await;
        }
        if let IdempotencyClaim::ExistingInProgress { tx_id } = &claim {
            return Err(ApiError::conflict(format!(
                "transaction is already prepared for idempotency key '{}'; poll GetOrchestrationTransaction for tx_id '{}'",
                meta.idempotency_key, tx_id
            )));
        }

        if let IdempotencyClaim::ExistingPrepared(existing) = &claim {
            return self
                .resume_clean_prepared_orchestration_batch(
                    meta,
                    idempotency_path.as_str(),
                    existing,
                    &events,
                    claim_policy.visible_marker_policy(),
                )
                .await;
        }

        let prepared = OrchestrationTxRecord {
            tx_id: claim.tx_id().to_string(),
            kind: ControlPlaneTxKind::OrchestrationBatch,
            status: ControlPlaneTxStatus::Prepared,
            repair_pending: false,
            request_id: meta.request_id.clone(),
            idempotency_key: meta.idempotency_key.clone(),
            request_hash,
            lock_path: orchestration_compaction_lock_path().to_string(),
            fencing_token: 0,
            prepared_at: Utc::now(),
            visible_at: None,
            durable_append: None,
            result: None,
        };
        self.store_prepared(ControlPlaneTxDomain::Orchestration, &prepared)
            .await?;
        let tx_id = prepared.tx_id.clone();

        let commit = match append_events_and_compact_with_result(
            &self.state.config,
            self.storage.clone(),
            events.clone(),
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

        self.finalize_orchestration_commit(
            &tx_id,
            idempotency_path.as_str(),
            commit,
            &events,
            claim_policy.visible_marker_policy(),
        )
        .await
    }

    async fn resume_clean_prepared_orchestration_batch(
        &self,
        meta: &ResolvedRequestMetadata,
        idempotency_path: &str,
        existing: &ControlPlaneIdempotencyRecord,
        events: &[OrchestrationEvent],
        visible_marker_policy: VisibleMarkerPolicy,
    ) -> Result<TxExecutionOutcome<OrchestrationTxReceipt>, ApiError> {
        let repair_lock_path = format!(
            "locks/transactions/orchestration/{}.repair.lock.json",
            existing.tx_id
        );
        let repair_lock = DistributedLock::new(self.storage.backend().clone(), &repair_lock_path);
        let guard = repair_lock
            .acquire(DEFAULT_LOCK_TTL, 10)
            .await
            .map_err(|error| {
                ApiError::conflict(format!(
                    "failed to acquire orchestration transaction resume lock: {error}"
                ))
            })?;
        let result = async {
            let marker = self
                .load_json_required::<ControlPlaneIdempotencyRecord>(idempotency_path)
                .await?
                .ok_or_else(|| {
                    ApiError::conflict("orchestration resume idempotency marker disappeared")
                })?;
            if marker != *existing
                && (marker.tx_id != existing.tx_id
                    || marker.kind != existing.kind
                    || marker.request_id != existing.request_id
                    || marker.idempotency_key != existing.idempotency_key
                    || marker.request_hash != existing.request_hash)
            {
                return Err(ApiError::conflict(
                    "orchestration resume idempotency ownership changed",
                ));
            }
            let record = self
                .load_record::<OrchestrationTxReceipt>(
                    ControlPlaneTxDomain::Orchestration,
                    &existing.tx_id,
                )
                .await?
                .ok_or_else(|| {
                    ApiError::conflict("frozen prepared orchestration record is missing")
                })?;
            if record.status == ControlPlaneTxStatus::Visible || marker.tx_record.is_some() {
                let winner = self
                    .resolve_existing_visible_record_with_policy::<OrchestrationTxReceipt>(
                        ControlPlaneTxDomain::Orchestration,
                        idempotency_path,
                        &marker,
                        visible_marker_policy,
                    )
                    .await?;
                let receipt = winner.result.ok_or_else(|| {
                    ApiError::internal("visible orchestration transaction is missing result")
                })?;
                return Ok(TxExecutionOutcome {
                    receipt,
                    repair_pending: winner.repair_pending,
                });
            }
            self.load_frozen_prepared::<OrchestrationTxReceipt>(
                ControlPlaneTxDomain::Orchestration,
                &marker,
                orchestration_compaction_lock_path(),
            )
            .await?;

            let commit = append_events_and_compact_with_result(
                &self.state.config,
                self.storage.clone(),
                events.to_vec(),
                Some(meta.request_id.as_str()),
            )
            .await
            .map_err(OrchestrationCommitError::into_api_error)?;
            self.finalize_orchestration_commit(
                &existing.tx_id,
                idempotency_path,
                commit,
                events,
                visible_marker_policy,
            )
            .await
        }
        .await;
        if let Err(error) = guard.release().await {
            tracing::warn!(
                error = %error,
                tx_id = %existing.tx_id,
                "failed to release orchestration transaction resume lock; relying on TTL cleanup"
            );
        }
        result
    }

    async fn repair_prepared_orchestration_batch(
        &self,
        meta: &ResolvedRequestMetadata,
        idempotency_path: &str,
        existing: &ControlPlaneIdempotencyRecord,
        expected_events: &[OrchestrationEvent],
        visible_marker_policy: VisibleMarkerPolicy,
    ) -> Result<TxExecutionOutcome<OrchestrationTxReceipt>, ApiError> {
        let repair_lock_path = format!(
            "locks/transactions/orchestration/{}.repair.lock.json",
            existing.tx_id
        );
        let repair_lock = DistributedLock::new(self.storage.backend().clone(), &repair_lock_path);
        let guard = repair_lock
            .acquire(DEFAULT_LOCK_TTL, 10)
            .await
            .map_err(|error| {
                ApiError::conflict(format!(
                    "failed to acquire orchestration transaction repair lock: {error}"
                ))
            })?;
        let result = self
            .repair_prepared_orchestration_batch_locked(
                meta,
                idempotency_path,
                existing,
                expected_events,
                visible_marker_policy,
            )
            .await;
        if let Err(error) = guard.release().await {
            tracing::warn!(
                error = %error,
                tx_id = %existing.tx_id,
                "failed to release orchestration transaction repair lock; relying on TTL cleanup"
            );
        }
        result
    }

    async fn repair_prepared_orchestration_batch_locked(
        &self,
        meta: &ResolvedRequestMetadata,
        idempotency_path: &str,
        existing: &ControlPlaneIdempotencyRecord,
        expected_events: &[OrchestrationEvent],
        visible_marker_policy: VisibleMarkerPolicy,
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
        if stored.status == ControlPlaneTxStatus::Visible {
            let marker = self
                .load_json_required::<ControlPlaneIdempotencyRecord>(idempotency_path)
                .await?
                .ok_or_else(|| {
                    ApiError::conflict(
                        "orchestration repair idempotency marker disappeared after locking",
                    )
                })?;
            let winner = self
                .resolve_existing_visible_record_with_policy::<OrchestrationTxReceipt>(
                    ControlPlaneTxDomain::Orchestration,
                    idempotency_path,
                    &marker,
                    visible_marker_policy,
                )
                .await?;
            let receipt = winner.result.ok_or_else(|| {
                ApiError::internal("visible orchestration transaction is missing result")
            })?;
            return Ok(TxExecutionOutcome {
                receipt,
                repair_pending: winner.repair_pending,
            });
        }
        if stored.status != ControlPlaneTxStatus::Prepared
            || !stored.repair_pending
            || stored.visible_at.is_some()
            || stored.result.is_some()
        {
            return Err(ApiError::conflict(format!(
                "orchestration transaction '{}' is not an exact repair-pending predecessor",
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
        self.validate_durable_append_for_events(
            &stored,
            durable_append,
            expected_events,
            visible_marker_policy == VisibleMarkerPolicy::DeferredForHandleValidation,
        )
        .await?;

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

        self.finalize_orchestration_commit(
            existing.tx_id.as_str(),
            idempotency_path,
            commit,
            expected_events,
            visible_marker_policy,
        )
        .await
    }

    async fn validate_durable_append_for_events(
        &self,
        stored: &OrchestrationTxRecord,
        durable_append: &ControlPlaneDurableAppend,
        expected_events: &[OrchestrationEvent],
        require_reviewed_event_identity: bool,
    ) -> Result<(), ApiError> {
        let canonical_lock_path = orchestration_compaction_lock_path();
        let expected_paths = expected_events
            .iter()
            .map(LedgerWriter::event_path)
            .collect::<Vec<_>>();
        if durable_append.event_paths.is_empty()
            || durable_append.lock_path != canonical_lock_path
            || durable_append.fencing_token == 0
            || stored.lock_path != canonical_lock_path
            || stored.fencing_token != 0
        {
            return Err(ApiError::conflict(format!(
                "repair-pending orchestration transaction '{}' durable append does not match the reviewed batch",
                stored.tx_id
            )));
        }
        if !require_reviewed_event_identity {
            return Ok(());
        }
        if durable_append.event_paths != expected_paths {
            return Err(ApiError::conflict(format!(
                "repair-pending orchestration transaction '{}' durable append does not match the reviewed batch",
                stored.tx_id
            )));
        }
        for (path, event) in durable_append.event_paths.iter().zip(expected_events) {
            let actual = match self.storage.get_raw(path).await {
                Ok(actual) => actual,
                Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                    return Err(ApiError::conflict(format!(
                        "repair-pending orchestration transaction '{}' event object is missing",
                        stored.tx_id
                    )));
                }
                Err(error) => return Err(ApiError::from(error)),
            };
            let actual: serde_json::Value =
                serde_json::from_slice(actual.as_ref()).map_err(|_| {
                    ApiError::conflict(format!(
                        "repair-pending orchestration transaction '{}' event object is corrupt",
                        stored.tx_id
                    ))
                })?;
            let actual = to_canonical_bytes(&actual).map_err(|error| {
                ApiError::internal(format!(
                    "failed to canonicalize durable orchestration event during recovery: {error}"
                ))
            })?;
            let expected = to_canonical_bytes(event).map_err(|error| {
                ApiError::internal(format!(
                    "failed to encode reviewed orchestration event during recovery: {error}"
                ))
            })?;
            if actual != expected {
                return Err(ApiError::conflict(format!(
                    "repair-pending orchestration transaction '{}' event object does not match the reviewed batch",
                    stored.tx_id
                )));
            }
        }
        Ok(())
    }

    async fn validate_orchestration_commit_for_events(
        &self,
        stored: &OrchestrationTxRecord,
        commit: &OrchestrationCommitOutcome,
        expected_events: &[OrchestrationEvent],
    ) -> Result<DateTime<Utc>, ApiError> {
        let expected_paths = expected_events
            .iter()
            .map(LedgerWriter::event_path)
            .collect::<Vec<_>>();
        let expected_count = u32::try_from(expected_events.len()).map_err(|_| {
            ApiError::bad_request("orchestration transaction event count exceeds u32")
        })?;
        let expected_hash = OrchestrationBatchMutation { events: Vec::new() }
            .request_hash_for_events(expected_events)?;
        if expected_events.is_empty()
            || stored.request_hash != expected_hash
            || commit.event_paths != expected_paths
            || commit.events_processed != expected_count
            || commit.lock_path != orchestration_compaction_lock_path()
            || commit.fencing_token == 0
            || !is_canonical_manifest_sequence(&commit.manifest_id)
            || !is_canonical_transaction_ulid(&commit.manifest_revision)
            || commit.pointer_version.is_empty()
        {
            return Err(ApiError::conflict(
                "orchestration commit does not match the reviewed event batch",
            ));
        }
        for (path, event) in expected_paths.iter().zip(expected_events) {
            let actual = self.storage.get_raw(path).await.map_err(ApiError::from)?;
            let actual: serde_json::Value =
                serde_json::from_slice(actual.as_ref()).map_err(|_| {
                    ApiError::conflict(
                        "orchestration commit event object is corrupt and cannot be reviewed",
                    )
                })?;
            let actual = to_canonical_bytes(&actual).map_err(|error| {
                ApiError::internal(format!(
                    "failed to canonicalize durable orchestration event during commit validation: {error}"
                ))
            })?;
            let expected = to_canonical_bytes(event).map_err(|error| {
                ApiError::internal(format!(
                    "failed to encode reviewed orchestration event during commit validation: {error}"
                ))
            })?;
            if actual != expected {
                return Err(ApiError::conflict(
                    "orchestration commit event object differs from the reviewed batch",
                ));
            }
        }

        let witness = OrchestrationPublicationWitness::for_events(expected_events)
            .map_err(ApiError::conflict)?;
        validate_selected_orchestration_publication(
            &self.storage,
            &commit.manifest_id,
            &commit.manifest_revision,
            commit.fencing_token,
            commit.delta_id.as_deref(),
            &witness,
        )
        .await
        .map_err(|error| {
            ApiError::conflict(format!(
                "orchestration commit selected publication authority diverges: {error}"
            ))
        })
    }

    async fn finalize_orchestration_commit(
        &self,
        tx_id: &str,
        idempotency_path: &str,
        commit: OrchestrationCommitOutcome,
        expected_events: &[OrchestrationEvent],
        visible_marker_policy: VisibleMarkerPolicy,
    ) -> Result<TxExecutionOutcome<OrchestrationTxReceipt>, ApiError> {
        let record_path = ControlPlaneTxPaths::record(ControlPlaneTxDomain::Orchestration, tx_id);
        let stored = self
            .load_json_with_version_required::<OrchestrationTxRecord>(&record_path)
            .await?
            .ok_or_else(|| {
                ApiError::internal(format!(
                    "orchestration transaction record not found during finalize: {tx_id}"
                ))
            })?;
        let marker = self
            .load_json_required::<ControlPlaneIdempotencyRecord>(idempotency_path)
            .await?
            .ok_or_else(|| {
                ApiError::internal("orchestration idempotency marker missing during finalize")
            })?;
        if marker.tx_id != tx_id
            || marker.kind != ControlPlaneTxKind::OrchestrationBatch
            || marker.request_id != stored.value.request_id
            || marker.idempotency_key != stored.value.idempotency_key
            || marker.request_hash != stored.value.request_hash
        {
            return Err(ApiError::conflict(
                "orchestration finalize ownership changed",
            ));
        }
        if marker.tx_record.is_some() || stored.value.status == ControlPlaneTxStatus::Visible {
            let winner = self
                .resolve_existing_visible_record_with_policy::<OrchestrationTxReceipt>(
                    ControlPlaneTxDomain::Orchestration,
                    idempotency_path,
                    &marker,
                    visible_marker_policy,
                )
                .await?;
            let receipt = winner.result.ok_or_else(|| {
                ApiError::internal("visible orchestration transaction is missing result")
            })?;
            return Ok(TxExecutionOutcome {
                receipt,
                repair_pending: winner.repair_pending,
            });
        }
        if stored.value.status != ControlPlaneTxStatus::Prepared
            || stored.value.visible_at.is_some()
            || stored.value.result.is_some()
        {
            return Err(ApiError::conflict(format!(
                "orchestration transaction is not an exact prepared predecessor during finalize: {tx_id}"
            )));
        }

        let manifest_published_at = match visible_marker_policy {
            VisibleMarkerPolicy::Immediate => None,
            VisibleMarkerPolicy::DeferredForHandleValidation => Some(
                self.validate_orchestration_commit_for_events(
                    &stored.value,
                    &commit,
                    expected_events,
                )
                .await?,
            ),
        };
        let visible_at = manifest_published_at
            .map_or_else(Utc::now, |published_at| Utc::now().max(published_at));
        let commit_id = match visible_marker_policy {
            VisibleMarkerPolicy::Immediate => Ulid::new().to_string(),
            VisibleMarkerPolicy::DeferredForHandleValidation => tx_id.to_string(),
        };
        let mut receipt = OrchestrationTxReceipt {
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
                let existing = self
                    .load_json_required::<OrchestrationTxReceipt>(
                        &ControlPlaneTxPaths::orchestration_commit_receipt(&commit_id),
                    )
                    .await?
                    .ok_or_else(|| {
                        ApiError::conflict(
                            "orchestration commit receipt disappeared after create conflict",
                        )
                    })?;
                if existing.tx_id != tx_id
                    || existing.commit_id != commit_id
                    || existing.manifest_id != receipt.manifest_id
                    || existing.revision_ulid != receipt.revision_ulid
                    || existing.delta_id != receipt.delta_id
                    || existing.pointer_version != receipt.pointer_version
                    || existing.events_processed != receipt.events_processed
                    || existing.visible_at < stored.value.prepared_at
                    || manifest_published_at
                        .is_some_and(|published_at| existing.visible_at < published_at)
                    || !is_canonical_manifest_sequence(&existing.manifest_id)
                    || !is_canonical_transaction_ulid(&existing.revision_ulid)
                    || (!existing.delta_id.is_empty()
                        && !is_canonical_transaction_ulid(&existing.delta_id))
                    || existing.read_token != format!("orchestration:{}", existing.manifest_id)
                {
                    return Err(ApiError::conflict(
                        "existing orchestration commit receipt conflicts with frozen authority",
                    ));
                }
                receipt = existing;
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
        let mut candidate = stored.value;
        candidate.status = ControlPlaneTxStatus::Visible;
        candidate.lock_path = commit.lock_path;
        candidate.fencing_token = commit.fencing_token;
        candidate.repair_pending = repair_pending;
        candidate.visible_at = Some(receipt.visible_at);
        candidate.durable_append = None;
        candidate.result = Some(receipt);
        let winner = self
            .persist_visible_record(
                ControlPlaneTxDomain::Orchestration,
                idempotency_path,
                &candidate,
                WritePrecondition::MatchesVersion(stored.version),
                visible_marker_policy,
            )
            .await?;
        let receipt = winner.result.ok_or_else(|| {
            ApiError::internal("visible orchestration transaction is missing result")
        })?;
        Ok(TxExecutionOutcome {
            receipt,
            repair_pending: winner.repair_pending,
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

    async fn preflight_root_recovery_children(
        &self,
        meta: &ResolvedRequestMetadata,
        mutations: &[RootMutation],
    ) -> Result<Option<BTreeMap<ControlPlaneTxDomain, RootTxManifestDomain>>, ApiError> {
        let mut domains = BTreeMap::new();
        let mut all_visible = true;
        for mutation in mutations {
            let domain = mutation.domain();
            let child_meta = self.root_participant_metadata(meta, domain);
            let (kind, request_hash, expected_events) = match mutation {
                RootMutation::Catalog(command) => (
                    ControlPlaneTxKind::CatalogDdl,
                    command.request_hash()?,
                    None,
                ),
                RootMutation::Orchestration(batch) => {
                    let events = batch.events(meta)?;
                    let request_hash = batch.request_hash_for_events(&events)?;
                    (
                        ControlPlaneTxKind::OrchestrationBatch,
                        request_hash,
                        Some(events),
                    )
                }
                RootMutation::Metastore(_) | RootMutation::ScopedMetastore(_) => {
                    return Err(ApiError::not_implemented(
                        "metastore root mutations are not implemented yet",
                    ));
                }
            };
            let marker_path = ControlPlaneTxPaths::idempotency(domain, &child_meta.idempotency_key);
            let Some(marker) = self
                .load_json_required::<ControlPlaneIdempotencyRecord>(&marker_path)
                .await?
            else {
                all_visible = false;
                continue;
            };
            if marker.kind != kind
                || marker.request_id != child_meta.request_id
                || marker.idempotency_key != child_meta.idempotency_key
                || marker.request_hash != request_hash
            {
                return Err(ApiError::conflict(
                    "root child recovery ownership does not match the reviewed mutation",
                ));
            }
            match domain {
                ControlPlaneTxDomain::Catalog => {
                    let stored = self
                        .load_record::<CatalogTxReceipt>(domain, &marker.tx_id)
                        .await?;
                    let visibility_exists = marker.tx_record.is_some()
                        || stored
                            .as_ref()
                            .is_some_and(|record| record.status == ControlPlaneTxStatus::Visible);
                    if !visibility_exists {
                        all_visible = false;
                        continue;
                    }
                    let record = self
                        .observe_existing_visible_record::<CatalogTxReceipt>(
                            domain,
                            &marker_path,
                            &marker,
                        )
                        .await?;
                    let receipt = self.validate_catalog_visible_authority(&record).await?;
                    let commit = root_domain_commit_from_catalog(&receipt);
                    domains.insert(
                        domain,
                        RootTxManifestDomain {
                            manifest_id: commit.manifest_id,
                            manifest_path: commit.manifest_path,
                            commit_id: commit.commit_id,
                        },
                    );
                }
                ControlPlaneTxDomain::Orchestration => {
                    let stored = self
                        .load_record::<OrchestrationTxReceipt>(domain, &marker.tx_id)
                        .await?;
                    if let Some(stored) = &stored
                        && stored.status == ControlPlaneTxStatus::Prepared
                        && stored.repair_pending
                    {
                        if !idempotency_marker_matches_transaction(&marker, stored)
                            || stored.visible_at.is_some()
                            || stored.result.is_some()
                        {
                            return Err(ApiError::conflict(
                                "root orchestration child repair authority changed before recovery",
                            ));
                        }
                        let durable_append = stored.durable_append.as_ref().ok_or_else(|| {
                            ApiError::conflict(
                                "root orchestration child repair is missing durable append authority",
                            )
                        })?;
                        self.validate_durable_append_for_events(
                            stored,
                            durable_append,
                            expected_events.as_deref().unwrap_or_default(),
                            true,
                        )
                        .await?;
                    }
                    let visibility_exists = marker.tx_record.is_some()
                        || stored
                            .as_ref()
                            .is_some_and(|record| record.status == ControlPlaneTxStatus::Visible);
                    if !visibility_exists {
                        all_visible = false;
                        continue;
                    }
                    let record = self
                        .observe_existing_visible_record::<OrchestrationTxReceipt>(
                            domain,
                            &marker_path,
                            &marker,
                        )
                        .await?;
                    let expected_events = expected_events.as_deref().ok_or_else(|| {
                        ApiError::internal(
                            "root orchestration child is missing its reviewed event batch",
                        )
                    })?;
                    let receipt = self
                        .validate_orchestration_visible_authority(&record, expected_events)
                        .await?;
                    let commit = root_domain_commit_from_orchestration(&receipt);
                    domains.insert(
                        domain,
                        RootTxManifestDomain {
                            manifest_id: commit.manifest_id,
                            manifest_path: commit.manifest_path,
                            commit_id: commit.commit_id,
                        },
                    );
                }
                ControlPlaneTxDomain::Root => unreachable!("root cannot contain itself"),
            }
        }
        Ok(all_visible.then_some(domains))
    }

    async fn validate_catalog_visible_authority(
        &self,
        record: &CatalogTxRecord,
    ) -> Result<CatalogTxReceipt, ApiError> {
        let receipt = record
            .result
            .clone()
            .ok_or_else(|| ApiError::internal("visible catalog result is missing"))?;
        if record.status != ControlPlaneTxStatus::Visible
            || record.lock_path != CatalogPaths::domain_lock(CatalogDomain::Catalog)
            || record.fencing_token == 0
            || record
                .visible_at
                .is_none_or(|visible_at| visible_at < record.prepared_at)
            || receipt.tx_id != record.tx_id
            || Some(receipt.visible_at) != record.visible_at
            || !is_canonical_transaction_ulid(&receipt.event_id)
            || !is_canonical_transaction_ulid(&receipt.commit_id)
            || !is_canonical_manifest_sequence(&receipt.manifest_id)
            || receipt.pointer_version.is_empty()
            || receipt.read_token != format!("catalog:{}", receipt.manifest_id)
        {
            return Err(ApiError::internal(
                "visible catalog receipt has non-canonical authority",
            ));
        }
        let audit_path = CatalogPaths::commit(CatalogDomain::Catalog, &receipt.commit_id);
        if let Some(audit) = self.load_json_required::<CommitRecord>(&audit_path).await?
            && audit.commit_id != receipt.commit_id
        {
            return Err(ApiError::internal(
                "catalog audit record diverges from visible authority",
            ));
        }
        let writer = self.catalog_writer()?;
        let identity = writer
            .authorize_frozen_catalog_transaction(
                &record.tx_id,
                &record.request_hash,
                &record.request_id,
                &record.idempotency_key,
            )
            .await
            .map_err(ApiError::from)?;
        let published_at = writer
            .validate_catalog_transaction_commit(
                &identity,
                &CatalogTransactionCommit {
                    event_id: receipt.event_id.clone(),
                    commit_id: receipt.commit_id.clone(),
                    manifest_id: receipt.manifest_id.clone(),
                    snapshot_version: receipt.snapshot_version,
                    pointer_version: receipt.pointer_version.clone(),
                    lock_path: record.lock_path.clone(),
                    fencing_token: record.fencing_token,
                    repair_pending: record.repair_pending,
                    dropped_table: None,
                },
            )
            .await
            .map_err(ApiError::from)?;
        if receipt.visible_at < published_at {
            return Err(ApiError::internal(
                "catalog receipt predates its immutable manifest",
            ));
        }
        Ok(receipt)
    }

    async fn validate_orchestration_visible_authority(
        &self,
        record: &OrchestrationTxRecord,
        expected_events: &[OrchestrationEvent],
    ) -> Result<OrchestrationTxReceipt, ApiError> {
        let receipt = record
            .result
            .clone()
            .ok_or_else(|| ApiError::internal("visible orchestration result is missing"))?;
        if record.status != ControlPlaneTxStatus::Visible
            || record.lock_path != orchestration_compaction_lock_path()
            || record.fencing_token == 0
            || record
                .visible_at
                .is_none_or(|visible_at| visible_at < record.prepared_at)
            || receipt.tx_id != record.tx_id
            || Some(receipt.visible_at) != record.visible_at
            || !is_canonical_transaction_ulid(&receipt.commit_id)
            || !is_canonical_manifest_sequence(&receipt.manifest_id)
            || !is_canonical_transaction_ulid(&receipt.revision_ulid)
            || (!receipt.delta_id.is_empty() && !is_canonical_transaction_ulid(&receipt.delta_id))
            || receipt.read_token != format!("orchestration:{}", receipt.manifest_id)
        {
            return Err(ApiError::internal(
                "visible orchestration receipt has non-canonical authority",
            ));
        }
        let audit_path = ControlPlaneTxPaths::orchestration_commit_receipt(&receipt.commit_id);
        match self
            .load_json_required::<OrchestrationTxReceipt>(&audit_path)
            .await?
        {
            Some(audit) if audit != receipt => {
                return Err(ApiError::internal(
                    "orchestration audit receipt diverges from visible authority",
                ));
            }
            None if !record.repair_pending => {
                return Err(ApiError::internal(
                    "visible orchestration audit receipt is missing without repair state",
                ));
            }
            Some(_) | None => {}
        }
        let commit = OrchestrationCommitOutcome {
            event_paths: expected_events
                .iter()
                .map(LedgerWriter::event_path)
                .collect(),
            lock_path: record.lock_path.clone(),
            fencing_token: record.fencing_token,
            manifest_id: receipt.manifest_id.clone(),
            manifest_revision: receipt.revision_ulid.clone(),
            pointer_version: receipt.pointer_version.clone(),
            delta_id: (!receipt.delta_id.is_empty()).then(|| receipt.delta_id.clone()),
            events_processed: receipt.events_processed,
            repair_pending: record.repair_pending,
        };
        let published_at = self
            .validate_orchestration_commit_for_events(record, &commit, expected_events)
            .await?;
        if receipt.visible_at < published_at {
            return Err(ApiError::internal(
                "orchestration receipt predates its immutable manifest",
            ));
        }
        Ok(receipt)
    }

    fn catalog_writer(&self) -> Result<CatalogWriter, ApiError> {
        let compactor = self
            .state
            .sync_compactor()
            .unwrap_or_else(|| Arc::new(Tier1Compactor::new(self.storage.clone())));
        Ok(CatalogWriter::new(self.storage.clone()).with_sync_compactor(compactor))
    }

    fn catalog_write_options(
        &self,
        meta: &ResolvedRequestMetadata,
        transaction_identity: Option<&CatalogTransactionIdentity>,
    ) -> WriteOptions {
        let actor = transaction_identity.map_or_else(
            || format!("api:{}", meta.tenant),
            |identity| format!("api:{}:transaction:{}", meta.tenant, identity.tx_id()),
        );
        let mut options = WriteOptions::default()
            .with_actor(actor)
            .with_request_id(meta.request_id.as_str())
            .with_idempotency_key(meta.idempotency_key.as_str());
        if let Some(identity) = transaction_identity {
            options = options.with_transaction_identity(identity.clone());
        }
        options
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
        policy: IdempotencyClaimPolicy<'_>,
    ) -> Result<IdempotencyClaim, ApiError> {
        let _handle_identity_guard =
            if matches!(policy, IdempotencyClaimPolicy::LegacyReplaceRetryable) {
                handles::guard_legacy_handle_identity(
                    self,
                    domain,
                    kind,
                    meta.idempotency_key.as_str(),
                )
                .await?
            } else {
                None
            };
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

            let claim_write = if policy.expected_tx_id().is_some() {
                WriteOutcome::PreconditionFailed
            } else {
                self.write_json(&path, &claim, WritePrecondition::DoesNotExist)
                    .await?
            };
            match claim_write {
                WriteOutcome::Written => return Ok(IdempotencyClaim::Fresh(claim)),
                WriteOutcome::PreconditionFailed => {
                    let existing = self
                        .load_json_with_version_required::<ControlPlaneIdempotencyRecord>(&path)
                        .await?
                        .ok_or_else(|| {
                            if policy.is_frozen() {
                                ApiError::conflict("frozen handle idempotency marker is missing")
                            } else {
                                ApiError::internal(
                                    "idempotency marker disappeared after claim conflict",
                                )
                            }
                        })?;
                    if let IdempotencyClaimPolicy::FrozenHandle { expected_tx_id } = policy
                        && (existing.value.kind != kind
                            || existing.value.request_id != meta.request_id
                            || existing.value.idempotency_key != meta.idempotency_key
                            || existing.value.request_hash != request_hash
                            || expected_tx_id.is_some_and(|tx_id| tx_id != existing.value.tx_id))
                    {
                        return Err(ApiError::conflict(
                            "frozen handle idempotency ownership changed",
                        ));
                    }
                    if existing.value.request_hash != request_hash {
                        if matches!(
                            self.classify_existing_idempotency(domain, &existing.value, policy)
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
                        .classify_existing_idempotency(domain, &existing.value, policy)
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
                        ExistingClaimDisposition::Resumable => {
                            return Ok(IdempotencyClaim::ExistingPrepared(existing.value));
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

    async fn load_frozen_prepared<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        marker: &ControlPlaneIdempotencyRecord,
        expected_lock_path: &str,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: DeserializeOwned,
    {
        let record = self
            .load_record::<TResult>(domain, &marker.tx_id)
            .await?
            .ok_or_else(|| ApiError::conflict("frozen prepared transaction record is missing"))?;
        if !idempotency_marker_matches_transaction(marker, &record)
            || record.status != ControlPlaneTxStatus::Prepared
            || record.repair_pending
            || record.visible_at.is_some()
            || record.result.is_some()
            || record.durable_append.is_some()
            || record.lock_path != expected_lock_path
            || record.fencing_token != 0
        {
            return Err(ApiError::conflict(format!(
                "frozen {domain} transaction is not an exact resumable prepared predecessor"
            )));
        }
        Ok(record)
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
        visible_marker_policy: VisibleMarkerPolicy,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone + PartialEq,
    {
        let path = ControlPlaneTxPaths::record(domain, tx_id);
        let stored = self
            .load_json_with_version_required::<ControlPlaneTxRecord<TResult>>(&path)
            .await?
            .ok_or_else(|| ApiError::internal(format!("transaction record not found: {tx_id}")))?;
        let mut record = stored.value;
        if record.status != ControlPlaneTxStatus::Prepared
            || record.visible_at.is_some()
            || record.result.is_some()
        {
            return Err(ApiError::conflict(format!(
                "transaction is not an exact prepared predecessor during {domain} finalize: {tx_id}"
            )));
        }
        record.status = ControlPlaneTxStatus::Visible;
        record.lock_path = lock_path;
        record.fencing_token = fencing_token;
        record.repair_pending = repair_pending;
        record.visible_at = Some(visible_at);
        record.durable_append = None;
        record.result = Some(result);

        self.persist_visible_record(
            domain,
            idempotency_path,
            &record,
            WritePrecondition::MatchesVersion(stored.version),
            visible_marker_policy,
        )
        .await
    }

    async fn mark_visible_repair_pending<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        tx_id: &str,
        idempotency_path: &str,
        visible_marker_policy: VisibleMarkerPolicy,
    ) -> Result<(), ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone + PartialEq,
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
        self.persist_visible_record(
            domain,
            idempotency_path,
            &record,
            WritePrecondition::MatchesVersion(stored.version),
            visible_marker_policy,
        )
        .await
        .map(|_| ())
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

    async fn persist_visible_record<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        idempotency_path: &str,
        record: &ControlPlaneTxRecord<TResult>,
        record_precondition: WritePrecondition,
        visible_marker_policy: VisibleMarkerPolicy,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone + PartialEq,
    {
        let marker = self
            .load_json_with_version_required::<ControlPlaneIdempotencyRecord>(idempotency_path)
            .await?
            .ok_or_else(|| ApiError::internal("idempotency record missing during finalize"))?;
        if !idempotency_marker_matches_transaction(&marker.value, record) {
            return Err(ApiError::conflict(format!(
                "idempotency marker ownership changed during {domain} transaction finalize"
            )));
        }
        match visible_marker_policy {
            VisibleMarkerPolicy::Immediate => {
                // Legacy endpoints historically publish their cached visible marker first. That
                // marker is the durable recovery evidence when the following exact-record write
                // fails, so changing this order would strand an otherwise visible legacy
                // transaction as a fresh Prepared claim until its stale timeout elapsed.
                let mut visible_marker = marker.value;
                visible_marker.visible_at = record.visible_at;
                visible_marker.tx_record =
                    Some(serde_json::to_value(record).map_err(|error| {
                        ApiError::internal(format!(
                            "failed to encode visible transaction record for idempotency replay: {error}"
                        ))
                    })?);
                let marker_write = self
                    .write_json(
                        idempotency_path,
                        &visible_marker,
                        WritePrecondition::MatchesVersion(marker.version),
                    )
                    .await;
                match marker_write {
                    Ok(WriteOutcome::Written) => {
                        let winner = self
                            .persist_exact_visible_record(domain, record, record_precondition)
                            .await?;
                        self.reconcile_legacy_marker_after_exact_winner(
                            domain,
                            idempotency_path,
                            &visible_marker,
                            &winner,
                        )
                        .await
                    }
                    Ok(WriteOutcome::PreconditionFailed) => Err(ApiError::conflict(format!(
                        "idempotency marker changed during {domain} transaction finalize"
                    ))),
                    Err(marker_error) => {
                        if let Err(record_error) = self
                            .persist_exact_visible_record(domain, record, record_precondition)
                            .await
                        {
                            tracing::warn!(
                                error = ?record_error,
                                tx_id = record.tx_id.as_str(),
                                domain = %domain,
                                "failed to persist exact visible transaction record after legacy idempotency finalize write failed"
                            );
                        }
                        Err(marker_error)
                    }
                }
            }
            VisibleMarkerPolicy::DeferredForHandleValidation => {
                self.persist_exact_visible_record(domain, record, record_precondition)
                    .await
            }
        }
    }

    async fn reconcile_legacy_marker_after_exact_winner<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        idempotency_path: &str,
        proposed_marker: &ControlPlaneIdempotencyRecord,
        winner: &ControlPlaneTxRecord<TResult>,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone + PartialEq,
    {
        let encoded_winner = serde_json::to_value(winner).map_err(|error| {
            ApiError::internal(format!(
                "failed to encode exact visible transaction winner: {error}"
            ))
        })?;
        let mut last_write_error = None;
        for _ in 0..6 {
            let stored = self
                .load_json_with_version_required::<ControlPlaneIdempotencyRecord>(idempotency_path)
                .await?
                .ok_or_else(|| {
                    ApiError::internal(
                        "legacy idempotency marker disappeared after visible publication",
                    )
                })?;
            if !idempotency_marker_matches_transaction(&stored.value, winner) {
                return Err(ApiError::conflict(format!(
                    "{domain} idempotency marker ownership changed after exact finalize"
                )));
            }
            if stored.value.visible_at == winner.visible_at
                && stored.value.tx_record.as_ref() == Some(&encoded_winner)
            {
                return Ok(winner.clone());
            }
            if stored.value != *proposed_marker {
                return Err(ApiError::internal(format!(
                    "{domain} idempotency marker diverged from both the proposed and exact visible transaction"
                )));
            }

            let mut repaired = stored.value;
            repaired.visible_at = winner.visible_at;
            repaired.tx_record = Some(encoded_winner.clone());
            match self
                .write_json(
                    idempotency_path,
                    &repaired,
                    WritePrecondition::MatchesVersion(stored.version),
                )
                .await
            {
                Ok(WriteOutcome::Written | WriteOutcome::PreconditionFailed) => {
                    last_write_error = None;
                }
                Err(error) => last_write_error = Some(error),
            }
        }
        if let Some(error) = last_write_error {
            return Err(error);
        }
        Err(ApiError::conflict(format!(
            "{domain} legacy idempotency marker did not converge on the exact visible winner"
        )))
    }

    async fn persist_exact_visible_record<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        record: &ControlPlaneTxRecord<TResult>,
        record_precondition: WritePrecondition,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone + PartialEq,
    {
        if record
            .visible_at
            .is_none_or(|visible_at| visible_at < record.prepared_at)
        {
            return Err(ApiError::conflict(format!(
                "{domain} visible transaction chronology is invalid"
            )));
        }
        let path = ControlPlaneTxPaths::record(domain, record.tx_id.as_str());
        let mut candidate = record.clone();
        let mut precondition = record_precondition;
        for _ in 0..4 {
            let write_error = self.write_json(&path, &candidate, precondition).await.err();
            let stored = self
                .load_json_with_version_required::<ControlPlaneTxRecord<TResult>>(&path)
                .await?;
            let Some(stored) = stored else {
                if let Some(error) = write_error {
                    return Err(error);
                }
                return Err(ApiError::internal(format!(
                    "{domain} transaction record disappeared during visible CAS"
                )));
            };
            if stored.value.status != ControlPlaneTxStatus::Visible
                || stored.value.visible_at.is_none()
                || stored.value.result.is_none()
                || stored.value.durable_append.is_some()
                || stored
                    .value
                    .visible_at
                    .is_none_or(|visible_at| visible_at < stored.value.prepared_at)
                || !same_transaction_ownership(&stored.value, &candidate)
            {
                if let Some(error) = write_error {
                    return Err(error);
                }
                return Err(ApiError::conflict(format!(
                    "{domain} transaction record changed during visible CAS"
                )));
            }

            if candidate.repair_pending
                && !stored.value.repair_pending
                && let Ok(joined) = join_visible_repair_pending(domain, &stored.value, &candidate)
                && joined != stored.value
            {
                candidate = joined;
                precondition = WritePrecondition::MatchesVersion(stored.version);
                continue;
            }
            return Ok(stored.value);
        }
        Err(ApiError::conflict(format!(
            "{domain} transaction record did not converge after visible CAS"
        )))
    }

    async fn persist_idempotency_from_exact_record<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        idempotency_path: &str,
        expected: &ControlPlaneTxRecord<TResult>,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone + PartialEq,
    {
        let record_path = ControlPlaneTxPaths::record(domain, expected.tx_id.as_str());
        let mut authority = expected.clone();
        let mut last_write_error = None;
        for _ in 0..6 {
            let stored = self
                .load_json_with_version_required::<ControlPlaneTxRecord<TResult>>(&record_path)
                .await?
                .ok_or_else(|| {
                    ApiError::internal(format!(
                        "exact visible {domain} transaction record is missing"
                    ))
                })?;
            if stored.value.status != ControlPlaneTxStatus::Visible
                || stored.value.visible_at.is_none()
                || stored.value.result.is_none()
                || stored.value.durable_append.is_some()
                || stored
                    .value
                    .visible_at
                    .is_none_or(|visible_at| visible_at < stored.value.prepared_at)
                || !same_transaction_ownership(&stored.value, &authority)
            {
                return Err(ApiError::conflict(format!(
                    "exact visible {domain} transaction authority changed"
                )));
            }
            let joined = join_visible_repair_pending(domain, &stored.value, &authority)?;
            if joined != stored.value {
                authority = self
                    .persist_exact_visible_record(
                        domain,
                        &joined,
                        WritePrecondition::MatchesVersion(stored.version),
                    )
                    .await?;
                continue;
            }
            authority = joined;

            let marker = self
                .load_json_with_version_required::<ControlPlaneIdempotencyRecord>(idempotency_path)
                .await?
                .ok_or_else(|| ApiError::internal("visible idempotency marker is missing"))?;
            if !idempotency_marker_matches_transaction(&marker.value, &authority) {
                return Err(ApiError::conflict(format!(
                    "{domain} idempotency marker ownership changed during visible repair"
                )));
            }

            if let Some(cached) = self.record_from_idempotency::<TResult>(&marker.value)? {
                validate_cached_visible_record(domain, &marker.value, &cached)?;
                let joined = join_visible_repair_pending(domain, &authority, &cached)?;
                if joined != authority {
                    authority = self
                        .persist_exact_visible_record(
                            domain,
                            &joined,
                            WritePrecondition::MatchesVersion(stored.version),
                        )
                        .await?;
                    continue;
                }
            }

            let encoded = serde_json::to_value(&authority).map_err(|error| {
                ApiError::internal(format!(
                    "failed to encode exact visible transaction authority: {error}"
                ))
            })?;
            if marker.value.visible_at == authority.visible_at
                && marker.value.tx_record.as_ref() == Some(&encoded)
            {
                return Ok(authority);
            }
            let mut repaired = marker.value;
            repaired.visible_at = authority.visible_at;
            repaired.tx_record = Some(encoded);
            match self
                .write_json(
                    idempotency_path,
                    &repaired,
                    WritePrecondition::MatchesVersion(marker.version),
                )
                .await
            {
                Ok(WriteOutcome::Written | WriteOutcome::PreconditionFailed) => {
                    last_write_error = None;
                }
                Err(error) => last_write_error = Some(error),
            }
        }
        if let Some(error) = last_write_error {
            return Err(error);
        }
        Err(ApiError::conflict(format!(
            "{domain} idempotency marker did not converge on exact visible authority"
        )))
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
        TResult: Serialize + DeserializeOwned + Clone + PartialEq,
    {
        let winner = self
            .resolve_existing_visible_exact_record::<TResult>(domain, idempotency_path, existing)
            .await?;
        self.persist_idempotency_from_exact_record(domain, idempotency_path, &winner)
            .await
    }

    async fn resolve_existing_visible_record_with_policy<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        idempotency_path: &str,
        existing: &ControlPlaneIdempotencyRecord,
        visible_marker_policy: VisibleMarkerPolicy,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone + PartialEq,
    {
        match visible_marker_policy {
            VisibleMarkerPolicy::Immediate => {
                self.resolve_existing_visible_record(domain, idempotency_path, existing)
                    .await
            }
            VisibleMarkerPolicy::DeferredForHandleValidation => {
                self.observe_existing_visible_record(domain, idempotency_path, existing)
                    .await
            }
        }
    }

    async fn observe_existing_visible_record<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        idempotency_path: &str,
        existing: &ControlPlaneIdempotencyRecord,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone + PartialEq,
    {
        let marker = self
            .load_json_with_version_required::<ControlPlaneIdempotencyRecord>(idempotency_path)
            .await?
            .ok_or_else(|| ApiError::internal("visible idempotency marker is missing"))?;
        if marker.value != *existing {
            return Err(ApiError::conflict(format!(
                "{domain} idempotency marker changed during visible observation"
            )));
        }
        let cached_record = self.record_from_idempotency::<TResult>(&marker.value)?;
        if let Some(cached_record) = &cached_record {
            validate_cached_visible_record(domain, &marker.value, cached_record)?;
        }
        let record_path = ControlPlaneTxPaths::record(domain, existing.tx_id.as_str());
        let stored_record = self
            .load_json_with_version_required::<ControlPlaneTxRecord<TResult>>(&record_path)
            .await?;
        match (stored_record, cached_record) {
            (Some(stored), cached) if stored.value.status == ControlPlaneTxStatus::Visible => {
                validate_visible_record_ownership(domain, &marker.value, &stored.value)?;
                if let Some(cached) = &cached {
                    join_visible_repair_pending(domain, &stored.value, cached)
                } else {
                    Ok(stored.value)
                }
            }
            (Some(stored), Some(cached)) => {
                if !matches!(
                    stored.value.status,
                    ControlPlaneTxStatus::Prepared | ControlPlaneTxStatus::Aborted
                ) || stored.value.visible_at.is_some()
                    || stored.value.result.is_some()
                    || !same_transaction_ownership(&stored.value, &cached)
                {
                    return Err(ApiError::internal(format!(
                        "{domain} transaction record path conflicts with cached visible evidence for tx_id '{}'",
                        existing.tx_id
                    )));
                }
                Ok(cached)
            }
            (None, Some(cached)) => Ok(cached),
            (Some(stored), None) => Err(ApiError::conflict(format!(
                "transaction is already {:?} for tx_id '{}'",
                stored.value.status, existing.tx_id
            ))),
            (None, None) => Err(ApiError::internal(format!(
                "{domain} transaction record missing for tx_id '{}'",
                existing.tx_id
            ))),
        }
    }

    async fn resolve_existing_visible_exact_record<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        idempotency_path: &str,
        existing: &ControlPlaneIdempotencyRecord,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone + PartialEq,
    {
        let marker = self
            .load_json_with_version_required::<ControlPlaneIdempotencyRecord>(idempotency_path)
            .await?
            .ok_or_else(|| ApiError::internal("visible idempotency marker is missing"))?;
        if marker.value != *existing {
            return Err(ApiError::conflict(format!(
                "{domain} idempotency marker changed during visible reconciliation"
            )));
        }
        let cached_record = self.record_from_idempotency::<TResult>(&marker.value)?;
        if let Some(cached_record) = &cached_record {
            validate_cached_visible_record(domain, &marker.value, cached_record)?;
        }
        let record_path = ControlPlaneTxPaths::record(domain, existing.tx_id.as_str());
        let stored_record = self
            .load_json_with_version_required::<ControlPlaneTxRecord<TResult>>(&record_path)
            .await?;
        match (stored_record, cached_record) {
            (Some(stored), cached) if stored.value.status == ControlPlaneTxStatus::Visible => {
                validate_visible_record_ownership(domain, &marker.value, &stored.value)?;
                let joined = if let Some(cached) = &cached {
                    join_visible_repair_pending(domain, &stored.value, cached)?
                } else {
                    stored.value.clone()
                };
                if stored.value != joined {
                    return self
                        .persist_validated_exact_visible_reconciliation(
                            domain,
                            &marker.value,
                            &joined,
                            WritePrecondition::MatchesVersion(stored.version),
                        )
                        .await;
                }
                Ok(joined)
            }
            (Some(stored), Some(cached)) => {
                if !matches!(
                    stored.value.status,
                    ControlPlaneTxStatus::Prepared | ControlPlaneTxStatus::Aborted
                ) || stored.value.visible_at.is_some()
                    || stored.value.result.is_some()
                    || !same_transaction_ownership(&stored.value, &cached)
                {
                    return Err(ApiError::internal(format!(
                        "{domain} transaction record path conflicts with cached visible evidence for tx_id '{}'",
                        existing.tx_id
                    )));
                }
                self.persist_validated_exact_visible_reconciliation(
                    domain,
                    &marker.value,
                    &cached,
                    WritePrecondition::MatchesVersion(stored.version),
                )
                .await
            }
            (None, Some(cached)) => {
                self.persist_validated_exact_visible_reconciliation(
                    domain,
                    &marker.value,
                    &cached,
                    WritePrecondition::DoesNotExist,
                )
                .await
            }
            (Some(stored), None) => Err(ApiError::conflict(format!(
                "transaction is already {:?} for tx_id '{}'",
                stored.value.status, existing.tx_id
            ))),
            (None, None) => Err(ApiError::internal(format!(
                "{domain} transaction record missing for tx_id '{}'",
                existing.tx_id
            ))),
        }
    }

    async fn persist_validated_exact_visible_reconciliation<TResult>(
        &self,
        domain: ControlPlaneTxDomain,
        marker: &ControlPlaneIdempotencyRecord,
        record: &ControlPlaneTxRecord<TResult>,
        record_precondition: WritePrecondition,
    ) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
    where
        TResult: Serialize + DeserializeOwned + Clone + PartialEq,
    {
        if !idempotency_marker_matches_transaction(marker, record) {
            return Err(ApiError::conflict(format!(
                "{domain} idempotency marker changed during visible reconciliation"
            )));
        }
        self.persist_exact_visible_record(domain, record, record_precondition)
            .await
    }

    async fn classify_existing_idempotency(
        &self,
        domain: ControlPlaneTxDomain,
        existing: &ControlPlaneIdempotencyRecord,
        policy: IdempotencyClaimPolicy<'_>,
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
        if policy.is_frozen() {
            return Ok(record.map_or(
                ExistingClaimDisposition::RepairPending,
                |record| match record.status {
                    ControlPlaneTxStatus::Visible => ExistingClaimDisposition::Visible,
                    ControlPlaneTxStatus::Prepared if record.repair_pending => {
                        ExistingClaimDisposition::RepairPending
                    }
                    ControlPlaneTxStatus::Prepared => ExistingClaimDisposition::Resumable,
                    ControlPlaneTxStatus::Aborted => ExistingClaimDisposition::RepairPending,
                },
            ));
        }
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

#[derive(Debug, Clone, Copy)]
enum IdempotencyClaimPolicy<'a> {
    LegacyReplaceRetryable,
    FrozenHandle { expected_tx_id: Option<&'a str> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VisibleMarkerPolicy {
    Immediate,
    DeferredForHandleValidation,
}

impl<'a> IdempotencyClaimPolicy<'a> {
    const fn is_frozen(self) -> bool {
        matches!(self, Self::FrozenHandle { .. })
    }

    const fn expected_tx_id(self) -> Option<&'a str> {
        match self {
            Self::LegacyReplaceRetryable => None,
            Self::FrozenHandle { expected_tx_id } => expected_tx_id,
        }
    }

    const fn visible_marker_policy(self) -> VisibleMarkerPolicy {
        if self.is_frozen() {
            VisibleMarkerPolicy::DeferredForHandleValidation
        } else {
            VisibleMarkerPolicy::Immediate
        }
    }
}

fn validate_cached_visible_record<TResult>(
    domain: ControlPlaneTxDomain,
    marker: &ControlPlaneIdempotencyRecord,
    record: &ControlPlaneTxRecord<TResult>,
) -> Result<(), ApiError> {
    validate_visible_record_ownership(domain, marker, record)?;
    if marker.visible_at != record.visible_at {
        return Err(ApiError::internal(format!(
            "{domain} cached visible timestamp does not match its idempotency marker"
        )));
    }
    Ok(())
}

fn validate_visible_record_ownership<TResult>(
    domain: ControlPlaneTxDomain,
    marker: &ControlPlaneIdempotencyRecord,
    record: &ControlPlaneTxRecord<TResult>,
) -> Result<(), ApiError> {
    if record.status != ControlPlaneTxStatus::Visible
        || record.visible_at.is_none()
        || record
            .visible_at
            .is_some_and(|visible_at| visible_at < record.prepared_at)
        || record.result.is_none()
        || record.durable_append.is_some()
        || !idempotency_marker_matches_transaction(marker, record)
    {
        return Err(ApiError::internal(format!(
            "{domain} visible transaction record does not match its idempotency marker"
        )));
    }
    Ok(())
}

fn idempotency_marker_matches_transaction<TResult>(
    marker: &ControlPlaneIdempotencyRecord,
    record: &ControlPlaneTxRecord<TResult>,
) -> bool {
    record.tx_id == marker.tx_id
        && record.kind == marker.kind
        && record.request_hash == marker.request_hash
        && (marker.request_id.is_empty() || record.request_id == marker.request_id)
        && (marker.idempotency_key.is_empty() || record.idempotency_key == marker.idempotency_key)
}

fn same_transaction_ownership<TLeft, TRight>(
    left: &ControlPlaneTxRecord<TLeft>,
    right: &ControlPlaneTxRecord<TRight>,
) -> bool {
    left.tx_id == right.tx_id
        && left.kind == right.kind
        && left.request_id == right.request_id
        && left.idempotency_key == right.idempotency_key
        && left.request_hash == right.request_hash
        && left.lock_path == right.lock_path
        && left.prepared_at == right.prepared_at
}

fn join_visible_repair_pending<TResult>(
    domain: ControlPlaneTxDomain,
    stored: &ControlPlaneTxRecord<TResult>,
    cached: &ControlPlaneTxRecord<TResult>,
) -> Result<ControlPlaneTxRecord<TResult>, ApiError>
where
    TResult: Clone + PartialEq,
{
    let mut normalized_stored = stored.clone();
    normalized_stored.repair_pending = false;
    let mut normalized_cached = cached.clone();
    normalized_cached.repair_pending = false;
    if normalized_stored != normalized_cached {
        return Err(ApiError::internal(format!(
            "{domain} cached visible transaction conflicts with its exact transaction record"
        )));
    }
    normalized_stored.repair_pending = stored.repair_pending || cached.repair_pending;
    Ok(normalized_stored)
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
        self.transaction_request()
            .request_hash()
            .map_err(ApiError::from)
    }

    fn request_hash_value(&self) -> Result<serde_json::Value, ApiError> {
        Ok(self.transaction_request().request_value())
    }

    fn transaction_request(&self) -> CatalogTransactionRequest {
        match self {
            Self::CreateCatalog {
                catalog,
                description,
            } => CatalogTransactionRequest::CreateCatalog {
                catalog: catalog.clone(),
                description: description.clone(),
            },
            Self::CreateSchema {
                catalog,
                schema,
                description,
            } => CatalogTransactionRequest::CreateSchema {
                catalog: catalog.clone(),
                schema: schema.clone(),
                description: description.clone(),
            },
            Self::RegisterTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
                columns,
            } => CatalogTransactionRequest::RegisterTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                description: description.clone(),
                location: location.clone(),
                format: format.clone(),
                columns: columns.clone(),
            },
            Self::UpdateTable {
                catalog,
                schema,
                table,
                description,
                location,
                format,
            } => CatalogTransactionRequest::UpdateTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                description: description.clone(),
                location: location.clone(),
                format: format.clone(),
            },
            Self::DropTable {
                catalog,
                schema,
                table,
            } => CatalogTransactionRequest::DropTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
            },
            Self::RenameTable {
                catalog,
                schema,
                table,
                new_table,
            } => CatalogTransactionRequest::RenameTable {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
                new_table: new_table.clone(),
            },
        }
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
    ExistingPrepared(ControlPlaneIdempotencyRecord),
    ExistingVisible(ControlPlaneIdempotencyRecord),
    ExistingInProgress { tx_id: String },
    ExistingRepairPending(ControlPlaneIdempotencyRecord),
}

impl IdempotencyClaim {
    fn tx_id(&self) -> &str {
        match self {
            Self::Fresh(record)
            | Self::ExistingPrepared(record)
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
    Resumable,
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

fn is_canonical_transaction_ulid(value: &str) -> bool {
    Ulid::from_string(value)
        .ok()
        .is_some_and(|parsed| parsed.to_string() == value)
}

fn is_canonical_manifest_sequence(value: &str) -> bool {
    value
        .parse::<u64>()
        .ok()
        .is_some_and(|parsed| format!("{parsed:020}") == value)
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

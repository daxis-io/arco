//! Bounded workspace epoch operations. Legacy entry points retain their codec and I/O path.
use super::{
    ArmedRetainedPointerIntent, Bytes, CatalogError, Digest, Future, LockGuard,
    RETENTION_MUTATION_EPOCH_PATH, Result, RetainedPointerIntentPrecondition,
    RetentionMutationEpoch, RetentionMutationEpochRecord, RetentionMutationKind,
    RetentionMutationState, ScopedStorage, Sha256, WritePrecondition, WriteResult, decode_record,
    encode_record, validate_identity, validation,
};
use crate::workspace_io_budget::{EPOCH_BYTES, WorkspaceIoBudget};

async fn read_epoch(
    storage: &ScopedStorage,
    budget: &mut WorkspaceIoBudget,
) -> Result<Option<(RetentionMutationEpochRecord, String)>> {
    let Some((bytes, version)) = budget
        .read_stable(storage, RETENTION_MUTATION_EPOCH_PATH, EPOCH_BYTES)
        .await?
    else {
        return Ok(None);
    };
    let record = decode_record(&bytes)?;
    if encode_record(&record)? != bytes {
        return Err(validation("bounded retention epoch is not canonical"));
    }
    Ok(Some((record, version)))
}

// Once a write may have been sent, only exact complete readback can resolve it.
// Admission failures before sending remain ordinary backpressure.
async fn publish_epoch(
    storage: &ScopedStorage,
    record: &RetentionMutationEpochRecord,
    precondition: WritePrecondition,
    budget: &mut WorkspaceIoBudget,
) -> Result<String> {
    let bytes = encode_record(record)?;
    if bytes.len() > EPOCH_BYTES {
        return Err(validation("bounded retention epoch exceeds 256 KiB"));
    }
    budget.reserve_bytes(bytes.len())?;
    budget.charge_operations(1)?;
    let outcome = storage
        .put_raw(
            RETENTION_MUTATION_EPOCH_PATH,
            Bytes::from(bytes),
            precondition,
        )
        .await;
    if let Ok(result) = &outcome {
        budget
            .charge_write(result)
            .map_err(|_| CatalogError::AmbiguousAuthorityOutcome {
                message:
                    "retention epoch PUT response exceeded metadata admission; outcome unresolved"
                        .into(),
            })?;
    }
    if let Ok(WriteResult::Success { version }) = outcome {
        if !version.is_empty() {
            return Ok(version);
        }
    }
    match read_epoch(storage, budget).await {
        Ok(Some((selected, version))) if selected == *record => Ok(version),
        _ => Err(CatalogError::AmbiguousAuthorityOutcome {
            message: "retention epoch publication lacks exact bounded readback".into(),
        }),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RetainedPointerSend {
    PossiblyCommitted,
    PreconditionFailed,
}

/// One invocation-scoped uncertain-mutation guard.
///
/// Dropping the guard leaves the epoch uncertain. A successful finish restores
/// only uncertainty that predates the guard or was explicitly retained through
/// `Self::mark_uncertain`.
pub struct BoundedMutation<'a> {
    epoch: &'a mut RetentionMutationEpoch,
    must_remain_uncertain: bool,
}

impl BoundedMutation<'_> {
    /// Arming is sticky even when the caller handles an error or finishes successfully.
    /// This only records the reconciliation boundary; it cannot send authority HEAD.
    #[allow(
        dead_code,
        reason = "restore publication remains disabled pending finalization qualification"
    )]
    pub(crate) async fn arm_restore_publication(
        &mut self,
        intent: super::ArmedRestorePublicationIntent,
        guard: &mut LockGuard<ScopedStorage>,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<()> {
        if self.must_remain_uncertain || self.epoch.record.has_armed_intent() {
            return Err(validation(
                "restore publication epoch is already uncertain or armed",
            ));
        }
        let mut armed = self.epoch.record.clone();
        armed.armed_restore_publication = Some(Box::new(intent));
        armed.version = super::ARMED_RESTORE_VERSION;
        armed.validate()?;
        // Do not let an interrupted refence followed by a handled error clear
        // the enclosing mutation's uncertainty, or admit another arming call.
        self.mark_uncertain();
        self.epoch.refence_bounded(guard, budget).await?;
        self.epoch.record = armed;
        self.epoch.claimed_version = publish_epoch(
            self.epoch.bounded_storage()?,
            &self.epoch.record,
            WritePrecondition::MatchesVersion(self.epoch.claimed_version.clone()),
            budget,
        )
        .await?;
        Ok(())
    }

    /// Borrows the held epoch only for fenced read/refence operations.
    pub(crate) fn epoch(&self) -> &RetentionMutationEpoch {
        &*self.epoch
    }

    /// Keeps the epoch in flight even if the caller handles its operation error.
    pub(crate) fn mark_uncertain(&mut self) {
        self.must_remain_uncertain = true;
        self.epoch.mark_uncertain();
    }

    /// Finishes this scoped operation, preserving sticky uncertainty on success.
    pub(crate) fn finish<T>(self, result: Result<T>) -> Result<T> {
        if result.is_ok() {
            self.epoch.uncertain_mutation = self.must_remain_uncertain;
        }
        result
    }
}

impl RetentionMutationEpoch {
    fn bounded_storage(&self) -> Result<&ScopedStorage> {
        self.storage
            .as_legacy_scoped()
            .ok_or_else(|| validation("bounded workspace epoch requires scoped storage"))
    }

    pub(crate) async fn terminal_match_is_in_flight_bounded(
        storage: &ScopedStorage,
        operation_kind: RetentionMutationKind,
        terminal_operation_ids: &std::collections::BTreeSet<String>,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<bool> {
        if terminal_operation_ids.is_empty() {
            return Ok(false);
        }
        Ok(read_epoch(storage, budget)
            .await?
            .is_some_and(|(record, _)| {
                record.state == RetentionMutationState::InFlight
                    && record.operation_kind == operation_kind
                    && terminal_operation_ids.contains(&record.operation_id)
            }))
    }

    pub(crate) async fn settle_terminal_matching_bounded(
        storage: &ScopedStorage,
        guard: &mut LockGuard<ScopedStorage>,
        operation_kind: RetentionMutationKind,
        terminal_operation_ids: &std::collections::BTreeSet<String>,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<bool> {
        if terminal_operation_ids.is_empty() {
            return Ok(false);
        }
        let Some((record, version)) = read_epoch(storage, budget).await? else {
            return Ok(false);
        };
        if record.state == RetentionMutationState::Idle
            || record.operation_kind != operation_kind
            || !terminal_operation_ids.contains(&record.operation_id)
        {
            return Ok(false);
        }
        if record.has_armed_intent() {
            return Err(validation(
                "generic settlement cannot clear an armed publication intent",
            ));
        }
        budget.extend_retention_lock(guard).await?;
        publish_epoch(
            storage,
            &record.completed()?,
            WritePrecondition::MatchesVersion(version),
            budget,
        )
        .await?;
        Ok(true)
    }

    pub(crate) fn operation_id(&self) -> &str {
        &self.record.operation_id
    }

    pub(crate) fn can_settle_bounded(&self) -> bool {
        !self.uncertain_mutation && !self.record.has_armed_intent()
    }

    pub(crate) fn armed_pointer(&self) -> Option<&ArmedRetainedPointerIntent> {
        self.record.armed_retained_pointer.as_ref()
    }

    /// Loads an exact prior snapshot epoch for read-only terminal reconciliation.
    pub(crate) async fn load_snapshot_bounded(
        storage: ScopedStorage,
        operation_id: &str,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<Option<Self>> {
        let Some((record, claimed_version)) = read_epoch(&storage, budget).await? else {
            return Ok(None);
        };
        if record.state == RetentionMutationState::Idle {
            return Ok(None);
        }
        if record.operation_id != operation_id
            || !matches!(
                record.operation_kind,
                RetentionMutationKind::WorkspaceSnapshotFinalize
                    | RetentionMutationKind::WorkspaceSnapshotRetry
            )
        {
            return Err(CatalogError::PreconditionFailed {
                message: "foreign retention epoch blocks snapshot recovery".into(),
            });
        }
        Ok(Some(Self {
            storage: storage.into(),
            record,
            claimed_version,
            uncertain_mutation: true,
        }))
    }

    pub(crate) async fn settle_recovered_snapshot_bounded(
        self,
        proof: Option<crate::state_store::VerifiedRetainedSource>,
        guard: &mut LockGuard<ScopedStorage>,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<()> {
        if let Some(intent) = &self.record.armed_retained_pointer {
            if !proof
                .as_ref()
                .is_some_and(|proof| proof.pointer.proves_visible(intent))
            {
                return Err(validation(
                    "armed snapshot recovery requires exact retained membership",
                ));
            }
        }
        budget.extend_retention_lock(guard).await?;
        match read_epoch(self.bounded_storage()?, budget).await? {
            Some((record, version)) if record == self.record && version == self.claimed_version => {
            }
            _ => {
                return Err(CatalogError::PreconditionFailed {
                    message: "snapshot recovery epoch changed".into(),
                });
            }
        }
        let mut cleared = self.record.clone();
        cleared.armed_retained_pointer = None;
        let completed = cleared.completed()?;
        publish_epoch(
            self.bounded_storage()?,
            &completed,
            WritePrecondition::MatchesVersion(self.claimed_version.clone()),
            budget,
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn run_bounded_mutation<T>(
        &mut self,
        operation: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let mutation = self.begin_bounded_mutation();
        let result = operation.await;
        mutation.finish(result)
    }

    /// Marks one operation uncertain before it can await implementation-owned I/O.
    pub(crate) fn begin_bounded_mutation(&mut self) -> BoundedMutation<'_> {
        let must_remain_uncertain = self.uncertain_mutation;
        self.uncertain_mutation = true;
        BoundedMutation {
            epoch: self,
            must_remain_uncertain,
        }
    }

    pub(crate) async fn refence_bounded(
        &self,
        guard: &mut LockGuard<ScopedStorage>,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<()> {
        if guard.holder_id() != self.record.holder_id {
            return Err(validation("retained publication lease holder differs"));
        }
        budget.extend_retention_lock(guard).await?;
        match read_epoch(self.bounded_storage()?, budget).await? {
            Some((record, version)) if record == self.record && version == self.claimed_version => {
                Ok(())
            }
            _ => Err(CatalogError::PreconditionFailed {
                message: "retained publication epoch changed".into(),
            }),
        }
    }

    pub(crate) async fn arm_retained_pointer(
        &mut self,
        intent: ArmedRetainedPointerIntent,
        guard: &mut LockGuard<ScopedStorage>,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<()> {
        if self.uncertain_mutation || self.record.has_armed_intent() {
            return Err(validation(
                "retention epoch already has an uncertain or armed mutation",
            ));
        }
        intent.validate()?;
        self.refence_bounded(guard, budget).await?;
        let mut armed = self.record.clone();
        armed.armed_retained_pointer = Some(intent);
        armed.validate()?;
        self.uncertain_mutation = true;
        self.record = armed;
        self.claimed_version = publish_epoch(
            self.bounded_storage()?,
            &self.record,
            WritePrecondition::MatchesVersion(self.claimed_version.clone()),
            budget,
        )
        .await?;
        self.uncertain_mutation = false;
        Ok(())
    }

    /// One send only. Every response still requires exact pointer and membership proof.
    pub(crate) async fn send_retained_pointer(
        &mut self,
        guard: &mut LockGuard<ScopedStorage>,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<RetainedPointerSend> {
        if self.uncertain_mutation {
            return Err(validation("an uncertain retained pointer cannot be resent"));
        }
        self.refence_bounded(guard, budget).await?;
        let intent = self
            .record
            .armed_retained_pointer
            .as_ref()
            .ok_or_else(|| validation("retained pointer send requires durable intent"))?;
        let precondition = match &intent.precondition {
            RetainedPointerIntentPrecondition::DoesNotExist {} => WritePrecondition::DoesNotExist,
            RetainedPointerIntentPrecondition::MatchesVersion { version } => {
                WritePrecondition::MatchesVersion(version.clone())
            }
        };
        budget.reserve_bytes(intent.expected_pointer_json.len())?;
        budget.charge_operations(1)?;
        self.uncertain_mutation = true;
        let result = self
            .storage
            .put_raw(
                &intent.pointer_path,
                Bytes::copy_from_slice(intent.expected_pointer_json.as_bytes()),
                precondition,
            )
            .await;
        let outcome = if matches!(&result, Ok(WriteResult::PreconditionFailed { .. })) {
            RetainedPointerSend::PreconditionFailed
        } else {
            RetainedPointerSend::PossiblyCommitted
        };
        if let Ok(result) = result {
            budget
                .charge_write(&result)
                .map_err(|_| CatalogError::AmbiguousAuthorityOutcome {
                    message: "retained pointer response exceeded admission; armed intent remains"
                        .into(),
                })?;
        }
        Ok(outcome)
    }

    pub(crate) async fn clear_retained_pointer(
        &mut self,
        proof: crate::state_store::control_mvp::VerifiedRetainedPointer,
        guard: &mut LockGuard<ScopedStorage>,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<()> {
        let intent = self
            .record
            .armed_retained_pointer
            .as_ref()
            .ok_or_else(|| validation("retained pointer clear requires armed intent"))?;
        if !proof.matches(intent) {
            return Err(validation(
                "retained pointer proof differs from armed intent",
            ));
        }
        self.refence_bounded(guard, budget).await?;
        let mut cleared = self.record.clone();
        cleared.armed_retained_pointer = None;
        self.uncertain_mutation = true;
        let version = publish_epoch(
            self.bounded_storage()?,
            &cleared,
            WritePrecondition::MatchesVersion(self.claimed_version.clone()),
            budget,
        )
        .await?;
        self.record = cleared;
        self.claimed_version = version;
        self.uncertain_mutation = false;
        Ok(())
    }

    pub(crate) async fn put_immutable_bounded(
        &mut self,
        path: &str,
        bytes: Bytes,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<()> {
        self.put_immutable_bounded_with_limit(
            path,
            bytes,
            crate::workspace_io_budget::RECORD_BYTES,
            budget,
        )
        .await
    }

    pub(crate) async fn put_immutable_bounded_with_limit(
        &mut self,
        path: &str,
        bytes: Bytes,
        limit: usize,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<()> {
        if limit == 0 {
            return Err(validation("bounded immutable metadata limit is zero"));
        }
        if bytes.is_empty() || bytes.len() > limit {
            return Err(validation(
                "bounded immutable metadata exceeds its encoded limit",
            ));
        }
        budget.reserve_bytes(bytes.len())?;
        budget.charge_operations(1)?;
        #[cfg(feature = "test-utils")]
        crate::state_store::control_mvp::record_sha256_work(bytes.len());
        let digest = hex::encode(Sha256::digest(&bytes));
        let prior_uncertainty = self.uncertain_mutation;
        self.uncertain_mutation = true;
        let outcome = self
            .storage
            .put_raw(path, bytes.clone(), WritePrecondition::DoesNotExist)
            .await;
        if let Ok(result) = &outcome {
            budget.charge_write(result).map_err(|_| CatalogError::AmbiguousAuthorityOutcome {
                message: "immutable PUT response exceeded metadata admission; epoch remains in flight".into(),
            })?;
        }
        if matches!(outcome, Ok(WriteResult::Success { .. })) {
            self.uncertain_mutation = prior_uncertainty;
            return Ok(());
        }
        match budget
            .read_immutable(
                self.bounded_storage()?,
                path,
                Some(bytes.len()),
                &digest,
                limit,
            )
            .await
        {
            Ok(selected) if selected == bytes => {
                self.uncertain_mutation = prior_uncertainty;
                Ok(())
            }
            _ => Err(CatalogError::AmbiguousAuthorityOutcome {
                message:
                    "immutable publication lacks exact bounded readback; epoch remains in flight"
                        .into(),
            }),
        }
    }

    pub(crate) async fn claim_bounded(
        storage: ScopedStorage,
        guard: &mut LockGuard<ScopedStorage>,
        operation_kind: RetentionMutationKind,
        operation_id: &str,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<Self> {
        validate_identity(operation_id, "operation_id")?;
        let (epoch, precondition) = match read_epoch(&storage, budget).await? {
            None => (1, WritePrecondition::DoesNotExist),
            Some((previous, version)) => {
                if previous.state != RetentionMutationState::Idle {
                    return Err(CatalogError::PreconditionFailed {
                        message: "a retention mutation epoch is already in flight".into(),
                    });
                }
                let epoch =
                    previous
                        .epoch
                        .checked_add(1)
                        .ok_or_else(|| CatalogError::CasFailed {
                            message: "durable retention mutation epoch is exhausted".into(),
                        })?;
                (epoch, WritePrecondition::MatchesVersion(version))
            }
        };
        let record = RetentionMutationEpochRecord::in_flight(
            epoch,
            guard.holder_id(),
            operation_kind,
            operation_id,
        )?;
        budget.extend_retention_lock(guard).await?;
        let claimed_version = publish_epoch(&storage, &record, precondition, budget).await?;
        // A failure here leaves durable exclusion in flight for reconciliation.
        budget.extend_retention_lock(guard).await?;
        Ok(Self {
            storage: storage.into(),
            record,
            claimed_version,
            uncertain_mutation: false,
        })
    }

    pub(crate) async fn settle_bounded(
        self,
        guard: &mut LockGuard<ScopedStorage>,
        budget: &mut WorkspaceIoBudget,
    ) -> Result<()> {
        if self.uncertain_mutation {
            return Err(CatalogError::AmbiguousAuthorityOutcome {
                message: "retention mutation outcome is uncertain; epoch remains in flight".into(),
            });
        }
        let completed = self.record.completed()?;
        if guard.holder_id() != self.record.holder_id {
            return Err(validation(
                "retention epoch belongs to another lease holder",
            ));
        }
        let storage = self
            .storage
            .as_legacy_scoped()
            .ok_or_else(|| validation("bounded workspace epoch requires scoped storage"))?;
        budget.extend_retention_lock(guard).await?;
        match read_epoch(storage, budget).await? {
            Some((record, version)) if record == self.record && version == self.claimed_version => {
            }
            _ => {
                return Err(CatalogError::PreconditionFailed {
                    message: "retention epoch ownership changed before settlement".into(),
                });
            }
        }
        publish_epoch(
            storage,
            &completed,
            WritePrecondition::MatchesVersion(self.claimed_version),
            budget,
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::workspace_io_budget::OPERATIONS;
    use crate::workspace_snapshot::RETENTION_GC_LOCK_PATH;
    use arco_core::{DistributedLock, MemoryBackend};
    use std::sync::Arc;

    fn storage() -> ScopedStorage {
        ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").expect("scope")
    }

    async fn claimed_epoch() -> (
        ScopedStorage,
        WorkspaceIoBudget,
        LockGuard<ScopedStorage>,
        RetentionMutationEpoch,
    ) {
        let storage = storage();
        let mut budget = WorkspaceIoBudget::new();
        let mut guard = budget
            .acquire_retention_lock(storage.clone(), "scoped-mutation")
            .await
            .expect("lock");
        let epoch = RetentionMutationEpoch::claim_bounded(
            storage.clone(),
            &mut guard,
            RetentionMutationKind::WorkspaceRestoreApply,
            "scoped-mutation",
            &mut budget,
        )
        .await
        .expect("claim");
        (storage, budget, guard, epoch)
    }

    fn restore_intent() -> super::super::ArmedRestorePublicationIntent {
        super::super::ArmedRestorePublicationIntent {
            intent_type: "arco.restore.publication-intent".into(),
            version: 1,
            domain: "catalog".into(),
            plan_sha256: "11".repeat(32),
            owner_generation: 1,
            candidate_id: "22".repeat(32),
            prepared_path: format!(
                "control/v1/domains/catalog/restore/v7/{}/prepared.json",
                "22".repeat(32)
            ),
            prepared_sha256: "33".repeat(32),
            precondition: RetainedPointerIntentPrecondition::DoesNotExist {},
        }
    }

    #[tokio::test]
    async fn armed_restore_epoch_is_rejected_by_legacy_envelope_validation() {
        #[derive(serde::Deserialize)]
        struct LegacyEnvelope {
            record_type: String,
            version: u32,
        }
        let (storage, mut budget, mut guard, mut epoch) = claimed_epoch().await;
        let mut mutation = epoch.begin_bounded_mutation();
        mutation
            .arm_restore_publication(restore_intent(), &mut guard, &mut budget)
            .await
            .unwrap();
        mutation.finish(Ok(())).unwrap();
        let bytes = storage
            .get_raw(RETENTION_MUTATION_EPOCH_PATH)
            .await
            .unwrap();
        // The pinned b0af V1 decoder ignores new fields, then checks exactly
        // this envelope before any completed()/settlement operation.
        let legacy: LegacyEnvelope = serde_json::from_slice(&bytes).unwrap();
        assert!(
            legacy.record_type != super::super::RECORD_TYPE || legacy.version != 1,
            "old binaries must reject before silently dropping the restore intent"
        );
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["version"], 2);
        value["version"] = 1.into();
        assert!(decode_record(&serde_jcs::to_vec(&value).unwrap()).is_err());
        value["version"] = 2.into();
        value
            .as_object_mut()
            .unwrap()
            .remove("armed_restore_publication");
        assert!(decode_record(&serde_jcs::to_vec(&value).unwrap()).is_err());
    }

    #[tokio::test]
    async fn restore_intent_operator_clear_returns_to_legacy_epoch_bytes() {
        let (storage, mut budget, mut guard, mut epoch) = claimed_epoch().await;
        let mut mutation = epoch.begin_bounded_mutation();
        mutation
            .arm_restore_publication(restore_intent(), &mut guard, &mut budget)
            .await
            .unwrap();
        mutation.finish(Ok(())).unwrap();
        budget.release_retention_lock(guard).await.unwrap();
        let recovered = super::super::recover_stale_retention_epoch(
            &storage.clone().into(),
            "test: all remote requests independently terminal",
        )
        .await
        .unwrap()
        .unwrap();
        assert!(recovered.operator_override);
        let selected = read_epoch(&storage, &mut budget).await.unwrap().unwrap().0;
        assert_eq!(selected.version, 1);
        assert_eq!(selected.state, RetentionMutationState::Idle);
        assert!(!selected.has_armed_intent());
        let json: serde_json::Value =
            serde_json::from_slice(&encode_record(&selected).unwrap()).unwrap();
        assert!(json.get("armed_restore_publication").is_none());
    }

    #[tokio::test]
    async fn restore_arming_rejects_prior_uncertainty_and_foreign_epoch_before_io() {
        for prior_uncertainty in [false, true] {
            let (backend, storage, mut budget, mut guard) = fault_fixture().await;
            let kind = if prior_uncertainty {
                RetentionMutationKind::WorkspaceRestoreApply
            } else {
                RetentionMutationKind::WorkspaceSnapshotFinalize
            };
            let mut epoch = RetentionMutationEpoch::claim_bounded(
                storage,
                &mut guard,
                kind,
                "operation",
                &mut budget,
            )
            .await
            .unwrap();
            if prior_uncertainty {
                epoch.mark_uncertain();
            }
            backend.set(Fault::None);
            let mut mutation = epoch.begin_bounded_mutation();
            assert!(
                mutation
                    .arm_restore_publication(restore_intent(), &mut guard, &mut budget)
                    .await
                    .is_err()
            );
            backend.counts(0, 0, 0);
        }
    }

    #[tokio::test]
    async fn restore_arming_preserves_intent_and_uncertainty_without_prepared_metadata() {
        let (storage, mut budget, mut guard, mut epoch) = claimed_epoch().await;
        let mut mutation = epoch.begin_bounded_mutation();
        mutation
            .arm_restore_publication(restore_intent(), &mut guard, &mut budget)
            .await
            .expect("arm");
        mutation.finish(Ok(())).expect("handled success");
        assert!(!epoch.can_settle_bounded());
        let selected = read_epoch(&storage, &mut budget)
            .await
            .expect("read")
            .expect("epoch")
            .0;
        assert_eq!(
            selected.armed_restore_publication.as_deref(),
            Some(&restore_intent())
        );
        assert!(
            storage
                .head_raw(&restore_intent().prepared_path)
                .await
                .expect("HEAD")
                .is_none()
        );
        let ids = std::collections::BTreeSet::from(["scoped-mutation".to_string()]);
        assert!(
            RetentionMutationEpoch::settle_terminal_matching_bounded(
                &storage,
                &mut guard,
                RetentionMutationKind::WorkspaceRestoreApply,
                &ids,
                &mut budget
            )
            .await
            .is_err()
        );
        assert_in_flight(&storage, epoch, &mut guard, &mut budget).await;
    }

    #[tokio::test]
    async fn restore_arming_faults_cannot_rearm_or_clear_uncertainty() {
        for fault in [
            Fault::None,
            Fault::CommitLost,
            Fault::NoWrite,
            Fault::PauseBefore,
            Fault::PauseAfter,
            Fault::PauseReadback,
            Fault::PauseArmedReadback,
        ] {
            let (backend, storage, mut budget, mut guard) = fault_fixture().await;
            let mut epoch = RetentionMutationEpoch::claim_bounded(
                storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceRestoreApply,
                "restore",
                &mut budget,
            )
            .await
            .expect("claim");
            backend.set(fault);
            let mut mutation = epoch.begin_bounded_mutation();
            let mut pending = Box::pin(mutation.arm_restore_publication(
                restore_intent(),
                &mut guard,
                &mut budget,
            ));
            if matches!(
                fault,
                Fault::PauseBefore
                    | Fault::PauseAfter
                    | Fault::PauseReadback
                    | Fault::PauseArmedReadback
            ) {
                assert!(futures::poll!(pending.as_mut()).is_pending(), "{fault:?}");
                drop(pending);
            } else {
                let result = pending.await;
                assert_eq!(result.is_ok(), fault != Fault::NoWrite, "{fault:?}");
            }
            mutation.finish(Ok(())).expect("handled result");
            assert!(!epoch.can_settle_bounded());
            backend.set(Fault::None);
            let selected = read_epoch(&storage, &mut budget).await.unwrap().unwrap().0;
            assert_eq!(
                selected.armed_restore_publication.is_some(),
                matches!(
                    fault,
                    Fault::None | Fault::CommitLost | Fault::PauseAfter | Fault::PauseArmedReadback
                ),
                "{fault:?}"
            );
            let mut second = epoch.begin_bounded_mutation();
            assert!(
                second
                    .arm_restore_publication(restore_intent(), &mut guard, &mut budget)
                    .await
                    .is_err()
            );
            assert_eq!(backend.puts.load(SeqCst), 0, "no second arming write");
            second.finish(Ok(())).expect("handled rejection");
            assert_in_flight(&storage, epoch, &mut guard, &mut budget).await;
        }
    }

    #[tokio::test]
    async fn restore_arming_exhausted_control_preserves_the_epoch() {
        let (storage, mut budget, mut guard, mut epoch) = claimed_epoch().await;
        let original = storage
            .get_raw(RETENTION_MUTATION_EPOCH_PATH)
            .await
            .unwrap();
        let mut exhausted = WorkspaceIoBudget::new();
        exhausted.charge_operations(OPERATIONS).unwrap();
        let mut mutation = epoch.begin_bounded_mutation();
        assert!(
            mutation
                .arm_restore_publication(restore_intent(), &mut guard, &mut exhausted)
                .await
                .is_err()
        );
        mutation.finish(Ok(())).expect("handled exhaustion");
        assert_eq!(
            storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .unwrap(),
            original
        );
        assert_in_flight(&storage, epoch, &mut guard, &mut budget).await;
    }

    async fn assert_in_flight(
        storage: &ScopedStorage,
        epoch: RetentionMutationEpoch,
        guard: &mut LockGuard<ScopedStorage>,
        budget: &mut WorkspaceIoBudget,
    ) {
        assert!(epoch.settle_bounded(guard, budget).await.is_err());
        assert_eq!(
            decode_record(
                &storage
                    .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                    .await
                    .expect("epoch")
            )
            .expect("record")
            .state,
            RetentionMutationState::InFlight
        );
    }

    #[tokio::test]
    async fn bounded_mutation_success_settles() {
        let (storage, mut budget, mut guard, mut epoch) = claimed_epoch().await;
        epoch
            .begin_bounded_mutation()
            .finish(Ok(()))
            .expect("success");
        epoch
            .settle_bounded(&mut guard, &mut budget)
            .await
            .expect("settle");
        assert_eq!(
            decode_record(
                &storage
                    .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                    .await
                    .expect("epoch")
            )
            .expect("record")
            .state,
            RetentionMutationState::Idle
        );
    }

    #[tokio::test]
    async fn bounded_mutation_error_remains_in_flight() {
        let (storage, mut budget, mut guard, mut epoch) = claimed_epoch().await;
        assert!(
            epoch
                .begin_bounded_mutation()
                .finish::<()>(Err(validation("expected error")))
                .is_err()
        );
        assert_in_flight(&storage, epoch, &mut guard, &mut budget).await;
    }

    #[tokio::test]
    async fn bounded_mutation_drop_remains_in_flight() {
        let (storage, mut budget, mut guard, mut epoch) = claimed_epoch().await;
        {
            let _mutation = epoch.begin_bounded_mutation();
        }
        assert_in_flight(&storage, epoch, &mut guard, &mut budget).await;
    }

    #[tokio::test]
    async fn bounded_mutation_cancelled_run_remains_in_flight() {
        let (storage, mut budget, mut guard, mut epoch) = claimed_epoch().await;
        let mut pending =
            Box::pin(epoch.run_bounded_mutation(std::future::pending::<Result<()>>()));
        assert!(futures::poll!(pending.as_mut()).is_pending());
        drop(pending);
        assert_in_flight(&storage, epoch, &mut guard, &mut budget).await;
    }

    #[tokio::test]
    async fn bounded_mutation_preserves_prior_uncertainty_after_success() {
        let (storage, mut budget, mut guard, mut epoch) = claimed_epoch().await;
        epoch.mark_uncertain();
        epoch
            .begin_bounded_mutation()
            .finish(Ok(()))
            .expect("handled success");
        assert_in_flight(&storage, epoch, &mut guard, &mut budget).await;
    }

    #[tokio::test]
    async fn bounded_mutation_handled_ambiguity_remains_in_flight() {
        let (storage, mut budget, mut guard, mut epoch) = claimed_epoch().await;
        let mut mutation = epoch.begin_bounded_mutation();
        mutation.mark_uncertain();
        mutation.finish(Ok(())).expect("handled result");
        assert_in_flight(&storage, epoch, &mut guard, &mut budget).await;
    }

    #[tokio::test]
    async fn bounded_mutation_immutable_epoch_reborrow_can_refence() {
        let (storage, mut budget, mut guard, mut epoch) = claimed_epoch().await;
        let mutation = epoch.begin_bounded_mutation();
        mutation
            .epoch()
            .refence_bounded(&mut guard, &mut budget)
            .await
            .expect("refence");
        mutation.finish(Ok(())).expect("success");
        epoch
            .settle_bounded(&mut guard, &mut budget)
            .await
            .expect("settle");
        assert_eq!(
            decode_record(
                &storage
                    .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                    .await
                    .expect("epoch")
            )
            .expect("record")
            .state,
            RetentionMutationState::Idle
        );
    }

    #[tokio::test]
    async fn bounded_epoch_claim_cannot_escape_exhausted_workspace_admission() {
        let storage = storage();
        let mut budget = WorkspaceIoBudget::new();
        let mut guard = budget
            .acquire_retention_lock(storage.clone(), "claim")
            .await
            .expect("lock");
        budget
            .charge_operations(OPERATIONS - 20)
            .expect("exhaust operations");
        assert!(matches!(
            RetentionMutationEpoch::claim_bounded(
                storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceSnapshotFinalize,
                "snapshot",
                &mut budget
            )
            .await,
            Err(CatalogError::MaintenanceBackpressure { .. })
        ));
        assert!(
            storage
                .head_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("HEAD")
                .is_none()
        );
    }

    #[tokio::test]
    async fn bounded_epoch_rejects_oversized_legacy_compatible_bytes() {
        let storage = storage();
        let mut budget = WorkspaceIoBudget::new();
        let mut guard = budget
            .acquire_retention_lock(storage.clone(), "claim")
            .await
            .expect("lock");
        let old = RetentionMutationEpochRecord::in_flight(
            1,
            "previous",
            RetentionMutationKind::WorkspaceSnapshotFinalize,
            "old",
        )
        .expect("old epoch")
        .completed()
        .expect("idle epoch");
        let mut value = serde_json::to_value(old).expect("JSON");
        value["unknown_legacy_field"] = serde_json::Value::from("x".repeat(EPOCH_BYTES));
        let bytes = Bytes::from(serde_jcs::to_vec(&value).expect("oversized record"));
        storage
            .put_raw(
                RETENTION_MUTATION_EPOCH_PATH,
                bytes.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("fixture");
        assert!(
            RetentionMutationEpoch::claim_bounded(
                storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceSnapshotFinalize,
                "snapshot",
                &mut budget
            )
            .await
            .is_err()
        );
        assert_eq!(
            storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("selected"),
            bytes
        );
    }

    #[tokio::test]
    async fn bounded_epoch_settlement_requires_remaining_admission() {
        let storage = storage();
        let mut budget = WorkspaceIoBudget::new();
        let mut guard = budget
            .acquire_retention_lock(storage.clone(), "claim")
            .await
            .expect("lock");
        let epoch = RetentionMutationEpoch::claim_bounded(
            storage.clone(),
            &mut guard,
            RetentionMutationKind::WorkspaceSnapshotFinalize,
            "snapshot",
            &mut budget,
        )
        .await
        .expect("claim");
        let selected = storage
            .get_raw(RETENTION_MUTATION_EPOCH_PATH)
            .await
            .expect("selected");
        let mut exhausted = WorkspaceIoBudget::new();
        exhausted
            .charge_operations(OPERATIONS)
            .expect("exhaust operations");
        assert!(matches!(
            epoch.settle_bounded(&mut guard, &mut exhausted).await,
            Err(CatalogError::MaintenanceBackpressure { .. })
        ));
        assert_eq!(
            storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("selected"),
            selected
        );
    }
    #[tokio::test]
    async fn bounded_epoch_claim_rejects_a_stale_guard_before_publication() {
        let storage = storage();
        let mut budget = WorkspaceIoBudget::new();
        let mut old = budget
            .acquire_retention_lock(storage.clone(), "old")
            .await
            .expect("old lock");
        DistributedLock::new(Arc::new(storage.clone()), RETENTION_GC_LOCK_PATH)
            .force_break()
            .await
            .expect("test break");
        let replacement = budget
            .acquire_retention_lock(storage.clone(), "replacement")
            .await
            .expect("replacement lock");
        assert!(
            RetentionMutationEpoch::claim_bounded(
                storage.clone(),
                &mut old,
                RetentionMutationKind::WorkspaceSnapshotFinalize,
                "snapshot",
                &mut budget
            )
            .await
            .is_err()
        );
        assert!(
            storage
                .head_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("HEAD")
                .is_none(),
            "stale holder must not publish an epoch"
        );
        budget
            .release_retention_lock(replacement)
            .await
            .expect("release replacement");
    }

    #[tokio::test]
    async fn bounded_epoch_claim_and_settlement_preserve_monotonic_v1_records() {
        let storage = storage();
        let mut budget = WorkspaceIoBudget::new();
        let mut guard = budget
            .acquire_retention_lock(storage.clone(), "epoch")
            .await
            .expect("lock");
        for expected in 1..=2 {
            let epoch = RetentionMutationEpoch::claim_bounded(
                storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceSnapshotFinalize,
                "snapshot",
                &mut budget,
            )
            .await
            .expect("claim");
            assert_eq!(epoch.record.epoch, expected);
            assert!(epoch.record.armed_retained_pointer.is_none());
            epoch
                .settle_bounded(&mut guard, &mut budget)
                .await
                .expect("settle");
            let selected = storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("selected");
            let record = decode_record(&selected).expect("V1 decode");
            assert_eq!(record.state, RetentionMutationState::Idle);
            assert_eq!(encode_record(&record).expect("V1 encode"), selected);
        }
        budget.release_retention_lock(guard).await.expect("release");
    }
    #[tokio::test]
    async fn bounded_epoch_put_metadata_exhaustion_is_unresolved_after_send() {
        for failed in [false, true] {
            let backend =
                Arc::new(crate::workspace_io_budget::tests::WriteMetadataBackend::default());
            *backend.target.lock().expect("target") = RETENTION_MUTATION_EPOCH_PATH.into();
            backend.failed.store(failed, SeqCst);
            let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scope");
            let mut budget = WorkspaceIoBudget::new();
            let mut guard = budget
                .acquire_retention_lock(storage.clone(), "claim")
                .await
                .expect("lock");
            let result = RetentionMutationEpoch::claim_bounded(
                storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceSnapshotFinalize,
                "snapshot",
                &mut budget,
            )
            .await;
            assert!(
                matches!(result, Err(CatalogError::AmbiguousAuthorityOutcome { .. })),
                "successful PUT response ownership cannot escape admission"
            );
            assert!(
                budget.charge_operations(1).is_ok(),
                "byte exhaustion must not spend extra operation slots"
            );
            assert!(
                budget.reserve_bytes(1).is_err(),
                "actual returned bytes remain charged"
            );
            assert_eq!(
                storage
                    .head_raw(RETENTION_MUTATION_EPOCH_PATH)
                    .await
                    .expect("HEAD")
                    .is_some(),
                !failed
            );
        }
    }
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    enum Fault {
        #[default]
        None,
        NoWrite,
        CommitLost,
        PauseBefore,
        PauseAfter,
        PauseReadback,
        PauseArmedReadback,
        PauseRenewal,
        Unstable,
    }

    #[derive(Debug, Default)]
    struct EpochFaultBackend {
        inner: MemoryBackend,
        target: std::sync::Mutex<Option<String>>,
        mode: std::sync::Mutex<Fault>,
        heads: std::sync::atomic::AtomicUsize,
        head_owned_bytes: std::sync::atomic::AtomicUsize,
        puts: std::sync::atomic::AtomicUsize,
        lock_ranges: std::sync::atomic::AtomicUsize,
        pointer_puts: std::sync::atomic::AtomicUsize,
        ranges: std::sync::Mutex<Vec<std::ops::Range<u64>>>,
    }

    use arco_core::{ObjectMeta, StorageBackend};
    use std::sync::atomic::Ordering::SeqCst;

    impl EpochFaultBackend {
        fn is_target(&self, path: &str) -> bool {
            path.ends_with(
                self.target
                    .lock()
                    .expect("target")
                    .as_deref()
                    .unwrap_or(RETENTION_MUTATION_EPOCH_PATH),
            )
        }
        fn mode(&self) -> Fault {
            *self.mode.lock().expect("fault")
        }
        fn set(&self, fault: Fault) {
            *self.mode.lock().expect("fault") = fault;
            self.heads.store(0, SeqCst);
            self.head_owned_bytes.store(0, SeqCst);
            self.puts.store(0, SeqCst);
            self.ranges.lock().expect("ranges").clear();
        }
        fn counts(&self, heads: usize, ranges: usize, puts: usize) {
            assert_eq!(self.heads.load(SeqCst), heads);
            assert_eq!(self.ranges.lock().expect("ranges").len(), ranges);
            assert_eq!(self.puts.load(SeqCst), puts);
            assert_eq!(self.pointer_puts.load(SeqCst), 0);
        }
    }

    #[async_trait::async_trait]
    impl StorageBackend for EpochFaultBackend {
        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            self.inner.get(path).await
        }
        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }
        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }
        async fn signed_url(
            &self,
            path: &str,
            expiry: std::time::Duration,
        ) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            let mut meta = self.inner.head(path).await?;
            if self.is_target(path) {
                let call = self.heads.fetch_add(1, SeqCst);
                if self.mode() == Fault::Unstable {
                    if let Some(meta) = &mut meta {
                        meta.version = format!("unstable-{call}");
                    }
                }
            }
            if self.is_target(path) {
                if let Some(meta) = &meta {
                    self.head_owned_bytes.fetch_add(
                        size_of::<ObjectMeta>()
                            + meta.path.capacity()
                            + meta.version.capacity()
                            + meta.etag.as_ref().map_or(0, String::capacity),
                        SeqCst,
                    );
                }
            }
            Ok(meta)
        }
        async fn get_range(
            &self,
            path: &str,
            range: std::ops::Range<u64>,
        ) -> arco_core::Result<Bytes> {
            let bytes = self.inner.get_range(path, range.clone()).await?;
            if self.is_target(path) {
                self.ranges.lock().expect("ranges").push(range);
                if self.mode() == Fault::PauseReadback
                    || (self.mode() == Fault::PauseArmedReadback && self.puts.load(SeqCst) > 0)
                {
                    std::future::pending::<()>().await;
                }
            } else if path.ends_with(RETENTION_GC_LOCK_PATH) {
                let call = self.lock_ranges.fetch_add(1, SeqCst) + 1;
                if self.mode() == Fault::PauseRenewal && call == 2 {
                    std::future::pending::<()>().await;
                }
            }
            Ok(bytes)
        }
        async fn put(
            &self,
            path: &str,
            bytes: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            if path.ends_with("retained/v1/current.json") {
                self.pointer_puts.fetch_add(1, SeqCst);
            }
            if !self.is_target(path) {
                return self.inner.put(path, bytes, precondition).await;
            }
            self.puts.fetch_add(1, SeqCst);
            let fault = self.mode();
            if fault == Fault::NoWrite {
                return Err(arco_core::Error::storage("injected no write"));
            }
            if fault == Fault::PauseBefore {
                std::future::pending::<()>().await;
            }
            let result = self.inner.put(path, bytes, precondition).await?;
            if fault == Fault::PauseAfter {
                std::future::pending::<()>().await;
            }
            if matches!(
                fault,
                Fault::CommitLost | Fault::PauseReadback | Fault::PauseArmedReadback
            ) {
                return Err(arco_core::Error::storage(
                    "injected committed lost response",
                ));
            }
            Ok(result)
        }
    }

    async fn fault_fixture() -> (
        Arc<EpochFaultBackend>,
        ScopedStorage,
        WorkspaceIoBudget,
        LockGuard<ScopedStorage>,
    ) {
        let backend = Arc::new(EpochFaultBackend::default());
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scope");
        let mut budget = WorkspaceIoBudget::new();
        let guard = budget
            .acquire_retention_lock(storage.clone(), "claim")
            .await
            .expect("lock");
        (backend, storage, budget, guard)
    }

    #[tokio::test]
    async fn bounded_claim_reconciles_only_exact_committed_lost_responses() {
        for fault in [Fault::CommitLost, Fault::NoWrite] {
            let (backend, storage, mut budget, mut guard) = fault_fixture().await;
            backend.set(fault);
            let outcome = RetentionMutationEpoch::claim_bounded(
                storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceSnapshotFinalize,
                "snapshot",
                &mut budget,
            )
            .await;
            if fault == Fault::CommitLost {
                backend.counts(3, 1, 1);
                let epoch = outcome.expect("exact readback");
                let bytes = storage
                    .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                    .await
                    .expect("selected");
                assert_eq!(encode_record(&epoch.record).expect("canonical"), bytes);
                assert_eq!(
                    storage
                        .head_raw(RETENTION_MUTATION_EPOCH_PATH)
                        .await
                        .expect("HEAD")
                        .expect("exists")
                        .version,
                    epoch.claimed_version
                );
            } else {
                backend.counts(2, 0, 1);
                assert!(matches!(
                    outcome,
                    Err(CatalogError::AmbiguousAuthorityOutcome { .. })
                ));
                assert!(
                    storage
                        .head_raw(RETENTION_MUTATION_EPOCH_PATH)
                        .await
                        .expect("HEAD")
                        .is_none()
                );
            }
        }
    }

    #[tokio::test]
    async fn bounded_settlement_reconciles_only_exact_committed_lost_responses() {
        for fault in [Fault::CommitLost, Fault::NoWrite] {
            let (backend, storage, mut budget, mut guard) = fault_fixture().await;
            let epoch = RetentionMutationEpoch::claim_bounded(
                storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceSnapshotFinalize,
                "snapshot",
                &mut budget,
            )
            .await
            .expect("claim");
            let before = storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("before");
            let version = epoch.claimed_version.clone();
            backend.set(fault);
            let outcome = epoch.settle_bounded(&mut guard, &mut budget).await;
            backend.counts(4, 2, 1);
            let selected = storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("selected");
            if fault == Fault::CommitLost {
                outcome.expect("exact readback");
                let record = decode_record(&selected).expect("record");
                assert_eq!(record.state, RetentionMutationState::Idle);
                assert_eq!(encode_record(&record).expect("canonical"), selected);
            } else {
                assert!(matches!(
                    outcome,
                    Err(CatalogError::AmbiguousAuthorityOutcome { .. })
                ));
                assert_eq!(selected, before);
                assert_eq!(
                    storage
                        .head_raw(RETENTION_MUTATION_EPOCH_PATH)
                        .await
                        .expect("HEAD")
                        .expect("exists")
                        .version,
                    version
                );
            }
        }
    }

    #[tokio::test]
    async fn bounded_epoch_cancellation_preserves_the_actual_durable_transition() {
        for settle in [false, true] {
            for fault in [Fault::PauseBefore, Fault::PauseAfter] {
                let (backend, storage, mut budget, mut guard) = fault_fixture().await;
                let mut before = None;
                if settle {
                    let epoch = RetentionMutationEpoch::claim_bounded(
                        storage.clone(),
                        &mut guard,
                        RetentionMutationKind::WorkspaceSnapshotFinalize,
                        "snapshot",
                        &mut budget,
                    )
                    .await
                    .expect("claim");
                    before = Some(
                        storage
                            .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                            .await
                            .expect("before"),
                    );
                    backend.set(fault);
                    let mut pending = Box::pin(epoch.settle_bounded(&mut guard, &mut budget));
                    assert!(futures::poll!(pending.as_mut()).is_pending());
                    drop(pending);
                    backend.counts(2, 1, 1);
                } else {
                    backend.set(fault);
                    let mut pending = Box::pin(RetentionMutationEpoch::claim_bounded(
                        storage.clone(),
                        &mut guard,
                        RetentionMutationKind::WorkspaceSnapshotFinalize,
                        "snapshot",
                        &mut budget,
                    ));
                    assert!(futures::poll!(pending.as_mut()).is_pending());
                    drop(pending);
                    backend.counts(1, 0, 1);
                }
                match storage.get_raw(RETENTION_MUTATION_EPOCH_PATH).await {
                    Ok(bytes) => {
                        let record = decode_record(&bytes).expect("record");
                        assert_eq!(encode_record(&record).expect("canonical"), bytes);
                        if settle && fault == Fault::PauseAfter {
                            assert_eq!(record.state, RetentionMutationState::Idle);
                        } else {
                            assert_eq!(record.state, RetentionMutationState::InFlight);
                        }
                        if settle && fault == Fault::PauseBefore {
                            assert_eq!(Some(bytes), before);
                        }
                    }
                    Err(arco_core::Error::NotFound(_)) => {
                        assert!(!settle && fault == Fault::PauseBefore);
                    }
                    Err(error) => panic!("unexpected read error: {error}"),
                }
            }
        }
    }

    #[tokio::test]
    async fn bounded_claim_cancelled_readback_or_renewal_leaves_durable_exclusion() {
        for fault in [Fault::PauseReadback, Fault::PauseRenewal] {
            let (backend, storage, mut budget, mut guard) = fault_fixture().await;
            backend.set(fault);
            let mut pending = Box::pin(RetentionMutationEpoch::claim_bounded(
                storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceSnapshotFinalize,
                "snapshot",
                &mut budget,
            ));
            assert!(futures::poll!(pending.as_mut()).is_pending());
            drop(pending);
            if fault == Fault::PauseReadback {
                backend.counts(2, 1, 1);
            } else {
                backend.counts(1, 0, 1);
                assert_eq!(backend.lock_ranges.load(SeqCst), 2);
            }
            let selected = storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("selected");
            let record = decode_record(&selected).expect("record");
            assert_eq!(record.state, RetentionMutationState::InFlight);
            assert_eq!(encode_record(&record).expect("canonical"), selected);
        }
    }

    #[tokio::test]
    async fn bounded_epoch_unstable_read_uses_exactly_four_probes() {
        let (backend, storage, mut budget, mut guard) = fault_fixture().await;
        let epoch = RetentionMutationEpoch::claim_bounded(
            storage.clone(),
            &mut guard,
            RetentionMutationKind::WorkspaceSnapshotFinalize,
            "snapshot",
            &mut budget,
        )
        .await
        .expect("claim");
        epoch
            .settle_bounded(&mut guard, &mut budget)
            .await
            .expect("settle");
        budget.release_retention_lock(guard).await.expect("release");
        backend.set(Fault::Unstable);
        let mut budget = WorkspaceIoBudget::new();
        assert!(matches!(
            read_epoch(&storage, &mut budget).await,
            Err(CatalogError::CasFailed { .. })
        ));
        backend.counts(8, 4, 0);
        let owned_heads = backend.head_owned_bytes.load(SeqCst);
        assert_eq!(
            budget.test_accounting(),
            (4 * (EPOCH_BYTES + 1) + owned_heads, 12, owned_heads, 0)
        );
        let probe_end = u64::try_from(EPOCH_BYTES)
            .expect("epoch cap")
            .checked_add(1)
            .expect("probe");
        let probe = 0..probe_end;
        assert_eq!(*backend.ranges.lock().expect("ranges"), vec![probe; 4]);
        budget
            .charge_operations(OPERATIONS - 12)
            .expect("twelve operations charged");
        assert!(budget.charge_operations(1).is_err());
    }
    #[tokio::test]
    async fn bounded_epoch_exhausted_readback_never_assumes_retry_safety() {
        for settle in [false, true] {
            for fault in [Fault::CommitLost, Fault::NoWrite] {
                let (backend, storage, mut setup_budget, mut guard) = fault_fixture().await;
                let mut budget = WorkspaceIoBudget::new();
                let outcome = if settle {
                    let epoch = RetentionMutationEpoch::claim_bounded(
                        storage.clone(),
                        &mut guard,
                        RetentionMutationKind::WorkspaceSnapshotFinalize,
                        "snapshot",
                        &mut setup_budget,
                    )
                    .await
                    .expect("claim");
                    budget
                        .charge_operations(OPERATIONS - 7)
                        .expect("remaining settlement operations");
                    backend.set(fault);
                    epoch.settle_bounded(&mut guard, &mut budget).await
                } else {
                    budget
                        .charge_operations(OPERATIONS - 5)
                        .expect("remaining claim operations");
                    backend.set(fault);
                    RetentionMutationEpoch::claim_bounded(
                        storage.clone(),
                        &mut guard,
                        RetentionMutationKind::WorkspaceSnapshotFinalize,
                        "snapshot",
                        &mut budget,
                    )
                    .await
                    .map(|_| ())
                };
                assert!(matches!(
                    outcome,
                    Err(CatalogError::AmbiguousAuthorityOutcome { .. })
                ));
                if settle {
                    backend.counts(2, 1, 1);
                } else {
                    backend.counts(1, 0, 1);
                }
                assert!(budget.charge_operations(1).is_err());
                match storage.get_raw(RETENTION_MUTATION_EPOCH_PATH).await {
                    Ok(bytes) => {
                        let record = decode_record(&bytes).expect("record");
                        let expected = if settle && fault == Fault::CommitLost {
                            RetentionMutationState::Idle
                        } else {
                            RetentionMutationState::InFlight
                        };
                        assert_eq!(record.state, expected);
                    }
                    Err(arco_core::Error::NotFound(_)) => {
                        assert!(!settle && fault == Fault::NoWrite);
                    }
                    Err(error) => panic!("unexpected evidence read: {error}"),
                }
            }
        }
    }
    #[tokio::test]
    async fn bounded_immutable_writes_cannot_escape_admission() {
        for oversized in [false, true] {
            let storage = storage();
            let mut budget = WorkspaceIoBudget::new();
            let mut guard = budget
                .acquire_retention_lock(storage.clone(), "immutable")
                .await
                .expect("lock");
            let mut epoch = RetentionMutationEpoch::claim_bounded(
                storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceSnapshotFinalize,
                "snapshot",
                &mut budget,
            )
            .await
            .expect("claim");
            let mut write_budget = WorkspaceIoBudget::new();
            let bytes = if oversized {
                Bytes::from(vec![0; crate::workspace_io_budget::RECORD_BYTES + 1])
            } else {
                write_budget
                    .charge_operations(OPERATIONS)
                    .expect("exhaust operations");
                Bytes::from_static(b"descriptor")
            };
            assert!(
                epoch
                    .put_immutable_bounded("immutable.json", bytes, &mut write_budget)
                    .await
                    .is_err()
            );
            assert!(
                storage
                    .head_raw("immutable.json")
                    .await
                    .expect("HEAD")
                    .is_none()
            );
            assert!(
                !epoch.uncertain_mutation,
                "pre-send admission cannot invent a pending write"
            );
        }
    }

    #[tokio::test]
    async fn bounded_immutable_write_metadata_failure_preserves_uncertainty() {
        let backend = Arc::new(crate::workspace_io_budget::tests::WriteMetadataBackend::default());
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scope");
        let mut budget = WorkspaceIoBudget::new();
        let mut guard = budget
            .acquire_retention_lock(storage.clone(), "immutable")
            .await
            .expect("lock");
        let mut epoch = RetentionMutationEpoch::claim_bounded(
            storage.clone(),
            &mut guard,
            RetentionMutationKind::WorkspaceSnapshotFinalize,
            "snapshot",
            &mut budget,
        )
        .await
        .expect("claim");
        *backend.target.lock().expect("target") = "immutable.json".into();
        assert!(matches!(
            epoch
                .put_immutable_bounded(
                    "immutable.json",
                    Bytes::from_static(b"descriptor"),
                    &mut budget
                )
                .await,
            Err(CatalogError::AmbiguousAuthorityOutcome { .. })
        ));
        assert!(epoch.uncertain_mutation);
        assert!(
            epoch
                .settle_bounded(&mut guard, &mut WorkspaceIoBudget::new())
                .await
                .is_err()
        );
        assert_eq!(
            decode_record(
                &storage
                    .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                    .await
                    .expect("selected")
            )
            .expect("epoch")
            .state,
            RetentionMutationState::InFlight
        );
    }
    #[tokio::test]
    async fn bounded_immutable_publication_reconciles_exact_bytes_and_preserves_cancelled_writes() {
        for fault in [
            Fault::None,
            Fault::NoWrite,
            Fault::CommitLost,
            Fault::PauseBefore,
            Fault::PauseAfter,
            Fault::PauseReadback,
        ] {
            let (backend, storage, mut budget, mut guard) = fault_fixture().await;
            let mut epoch = RetentionMutationEpoch::claim_bounded(
                storage.clone(),
                &mut guard,
                RetentionMutationKind::WorkspaceSnapshotFinalize,
                "snapshot",
                &mut budget,
            )
            .await
            .expect("claim");
            *backend.target.lock().expect("target") = Some("descriptor.json".into());
            backend.set(fault);
            let bytes = Bytes::from_static(b"immutable descriptor");
            if matches!(
                fault,
                Fault::PauseBefore | Fault::PauseAfter | Fault::PauseReadback
            ) {
                let mut pending = Box::pin(epoch.put_immutable_bounded(
                    "descriptor.json",
                    bytes.clone(),
                    &mut budget,
                ));
                assert!(futures::poll!(pending.as_mut()).is_pending());
                drop(pending);
                assert!(epoch.uncertain_mutation);
                if fault == Fault::PauseReadback {
                    backend.counts(1, 1, 1);
                } else {
                    backend.counts(0, 0, 1);
                }
            } else {
                let outcome = epoch
                    .put_immutable_bounded("descriptor.json", bytes.clone(), &mut budget)
                    .await;
                if fault == Fault::NoWrite {
                    assert!(matches!(
                        outcome,
                        Err(CatalogError::AmbiguousAuthorityOutcome { .. })
                    ));
                    assert!(epoch.uncertain_mutation);
                    backend.counts(1, 0, 1);
                } else {
                    outcome.expect("known or reconciled write");
                    assert!(!epoch.uncertain_mutation);
                    if fault == Fault::CommitLost {
                        backend.counts(1, 1, 1);
                    } else {
                        backend.counts(0, 0, 1);
                    }
                    backend.set(Fault::None);
                    epoch
                        .put_immutable_bounded("descriptor.json", bytes.clone(), &mut budget)
                        .await
                        .expect("exact collision replay");
                    backend.counts(1, 1, 1);
                    assert!(!epoch.uncertain_mutation);
                    epoch.uncertain_mutation = true;
                    epoch
                        .put_immutable_bounded("descriptor.json", bytes.clone(), &mut budget)
                        .await
                        .expect("same bytes");
                    assert!(
                        epoch.uncertain_mutation,
                        "a later exact write cannot clear prior uncertainty"
                    );
                }
            }
            let selected = storage.get_raw("descriptor.json").await;
            if matches!(fault, Fault::NoWrite | Fault::PauseBefore) {
                assert!(matches!(selected, Err(arco_core::Error::NotFound(_))));
            } else {
                assert_eq!(selected.expect("immutable exists"), bytes);
            }
        }
    }
}

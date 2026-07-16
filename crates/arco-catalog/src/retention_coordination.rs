//! Durable exclusion between retained-root publication and mutating GC runs.

use std::collections::BTreeSet;
use std::future::Future;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use arco_core::lock::LockGuard;
use arco_core::{ScopedStorage, WritePrecondition, WriteResult};

use crate::error::{CatalogError, Result};
use crate::workspace_snapshot::RETENTION_GC_LOCK_TTL;

const RECORD_TYPE: &str = "arco.retention_mutation_epoch";
const VERSION: u32 = 1;

/// The one workspace-scoped durable exclusion record.
pub const RETENTION_MUTATION_EPOCH_PATH: &str = "retention/coordination/mutation-epoch.json";

/// The bounded set of operations allowed to mutate retention-visible state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionMutationKind {
    WorkspaceSnapshotFinalize,
    WorkspaceSnapshotRetry,
    WorkspaceExportFinalize,
    WorkspaceExportRetry,
    WorkspaceRestoreApply,
    CatalogGc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum RetentionMutationState {
    InFlight,
    Idle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RetentionMutationEpochRecord {
    record_type: String,
    version: u32,
    epoch: u64,
    state: RetentionMutationState,
    holder_id: String,
    operation_kind: RetentionMutationKind,
    operation_id: String,
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
}

impl RetentionMutationEpochRecord {
    fn in_flight(
        epoch: u64,
        holder_id: impl Into<String>,
        operation_kind: RetentionMutationKind,
        operation_id: impl Into<String>,
    ) -> Result<Self> {
        let record = Self {
            record_type: RECORD_TYPE.to_string(),
            version: VERSION,
            epoch,
            state: RetentionMutationState::InFlight,
            holder_id: holder_id.into(),
            operation_kind,
            operation_id: operation_id.into(),
            started_at: Utc::now(),
            completed_at: None,
        };
        record.validate()?;
        Ok(record)
    }

    fn completed(&self) -> Result<Self> {
        let completed_at = Utc::now().max(self.started_at);
        let record = Self {
            state: RetentionMutationState::Idle,
            completed_at: Some(completed_at),
            ..self.clone()
        };
        record.validate()?;
        Ok(record)
    }

    fn validate(&self) -> Result<()> {
        if self.record_type != RECORD_TYPE || self.version != VERSION {
            return Err(validation("unsupported retention mutation epoch envelope"));
        }
        if self.epoch == 0 {
            return Err(validation("retention mutation epoch must be positive"));
        }
        validate_identity(&self.holder_id, "holder_id")?;
        validate_identity(&self.operation_id, "operation_id")?;
        match (self.state, self.completed_at) {
            (RetentionMutationState::InFlight, None) => {}
            (RetentionMutationState::Idle, Some(completed_at))
                if completed_at >= self.started_at => {}
            (RetentionMutationState::InFlight, Some(_)) => {
                return Err(validation(
                    "in-flight retention mutation epoch cannot be completed",
                ));
            }
            (RetentionMutationState::Idle, None) => {
                return Err(validation(
                    "idle retention mutation epoch requires completion evidence",
                ));
            }
            (RetentionMutationState::Idle, Some(_)) => {
                return Err(validation(
                    "retention mutation epoch completion precedes its claim",
                ));
            }
        }
        Ok(())
    }
}

/// An invocation-local capability for mutations covered by one durable epoch.
pub struct RetentionMutationEpoch {
    storage: ScopedStorage,
    record: RetentionMutationEpochRecord,
    claimed_version: String,
    uncertain_mutation: bool,
}

impl RetentionMutationEpoch {
    /// Claims the exact durable epoch while the caller owns the distributed lock.
    ///
    /// The lock is re-proved once after the claim. From that point until
    /// settlement, the durable in-flight record is the exclusion boundary; a
    /// later lease holder must observe it and abort without product mutation.
    pub(crate) async fn claim(
        storage: ScopedStorage,
        guard: &mut LockGuard<ScopedStorage>,
        operation_kind: RetentionMutationKind,
        operation_id: impl Into<String>,
    ) -> Result<Self> {
        let (epoch, precondition) = match storage.head_raw(RETENTION_MUTATION_EPOCH_PATH).await? {
            None => (1, WritePrecondition::DoesNotExist),
            Some(meta) => {
                let previous_bytes = storage.get_raw(RETENTION_MUTATION_EPOCH_PATH).await?;
                let previous = decode_record(&previous_bytes)?;
                if previous.state == RetentionMutationState::InFlight {
                    return Err(CatalogError::PreconditionFailed {
                        message: "a retention mutation epoch is already in flight".to_string(),
                    });
                }
                let epoch =
                    previous
                        .epoch
                        .checked_add(1)
                        .ok_or_else(|| CatalogError::CasFailed {
                            message: "durable retention mutation epoch is exhausted".to_string(),
                        })?;
                (epoch, WritePrecondition::MatchesVersion(meta.version))
            }
        };
        let record = RetentionMutationEpochRecord::in_flight(
            epoch,
            guard.holder_id(),
            operation_kind,
            operation_id,
        )?;
        let bytes = encode_record(&record)?;

        let claimed_version = match storage
            .put_raw(
                RETENTION_MUTATION_EPOCH_PATH,
                Bytes::from(bytes),
                precondition,
            )
            .await?
        {
            WriteResult::Success { version } => version,
            WriteResult::PreconditionFailed { .. } => {
                return Err(CatalogError::CasFailed {
                    message: "retention mutation epoch claim lost CAS".to_string(),
                });
            }
        };

        guard.extend(RETENTION_GC_LOCK_TTL).await.map_err(|error| {
            CatalogError::PreconditionFailed {
                message: format!(
                    "retention coordination lease was lost after durable epoch claim: {error}"
                ),
            }
        })?;

        Ok(Self {
            storage,
            record,
            claimed_version,
            uncertain_mutation: false,
        })
    }

    /// Executes one exact put while retaining uncertainty on transport failure.
    pub(crate) async fn put_raw(
        &mut self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        match self.storage.put_raw(path, data, precondition).await {
            Ok(result) => Ok(result),
            Err(error) => {
                self.uncertain_mutation = true;
                Err(error.into())
            }
        }
    }

    /// Awaits an implementation-owned mutation whose storage calls cannot be
    /// individually wrapped at this module boundary.
    ///
    /// An error is conservatively treated as an uncertain mutation outcome.
    pub(crate) async fn run_external_mutation<T, F>(&mut self, mutation: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        match mutation.await {
            Ok(value) => Ok(value),
            Err(error) => {
                self.uncertain_mutation = true;
                Err(error)
            }
        }
    }

    /// Conservatively keeps this epoch in flight when exact reconciliation cannot
    /// prove whether an implementation-owned mutation became visible.
    pub(crate) fn mark_uncertain(&mut self) {
        self.uncertain_mutation = true;
    }

    /// Performs a stable, read-only check for the exact uncertain epoch that a
    /// workflow has independently proven terminal.
    pub(crate) async fn terminal_match_is_in_flight(
        storage: &ScopedStorage,
        operation_kind: RetentionMutationKind,
        terminal_operation_ids: &BTreeSet<String>,
    ) -> Result<bool> {
        if terminal_operation_ids.is_empty() {
            return Ok(false);
        }
        for _ in 0..4 {
            let Some(before) = storage.head_raw(RETENTION_MUTATION_EPOCH_PATH).await? else {
                return Ok(false);
            };
            let bytes = storage.get_raw(RETENTION_MUTATION_EPOCH_PATH).await?;
            let Some(after) = storage.head_raw(RETENTION_MUTATION_EPOCH_PATH).await? else {
                return Err(CatalogError::CasFailed {
                    message: "retention mutation epoch disappeared during terminal precheck"
                        .to_string(),
                });
            };
            if before.version != after.version {
                continue;
            }
            let record = decode_record(&bytes)?;
            return Ok(record.state == RetentionMutationState::InFlight
                && record.operation_kind == operation_kind
                && terminal_operation_ids.contains(&record.operation_id));
        }
        Err(CatalogError::CasFailed {
            message: "retention mutation epoch was unstable during terminal precheck".to_string(),
        })
    }

    /// Settles a previously uncertain epoch only after its owning workflow has
    /// supplied exact terminal operation identities while holding the shared
    /// retention lock.
    ///
    /// This is intentionally not an adoption path: an in-flight epoch for any
    /// other operation remains in flight and fails closed. The caller must prove
    /// the supplied identities terminal before invoking this method.
    pub(crate) async fn settle_terminal_matching(
        storage: ScopedStorage,
        guard: &mut LockGuard<ScopedStorage>,
        operation_kind: RetentionMutationKind,
        terminal_operation_ids: &BTreeSet<String>,
    ) -> Result<bool> {
        if terminal_operation_ids.is_empty() {
            return Ok(false);
        }
        let Some(before) = storage.head_raw(RETENTION_MUTATION_EPOCH_PATH).await? else {
            return Ok(false);
        };
        let bytes = storage.get_raw(RETENTION_MUTATION_EPOCH_PATH).await?;
        let after = storage
            .head_raw(RETENTION_MUTATION_EPOCH_PATH)
            .await?
            .ok_or_else(|| CatalogError::CasFailed {
                message: "retention mutation epoch disappeared during reconciliation".to_string(),
            })?;
        if before.version != after.version {
            return Err(CatalogError::CasFailed {
                message: "retention mutation epoch changed during reconciliation".to_string(),
            });
        }
        let record = decode_record(&bytes)?;
        if record.state == RetentionMutationState::Idle {
            return Ok(false);
        }
        if record.operation_kind != operation_kind
            || !terminal_operation_ids.contains(&record.operation_id)
        {
            // A foreign in-flight operation remains the global fail-closed
            // boundary. This recovery helper does not adopt or rewrite it.
            return Ok(false);
        }

        guard.extend(RETENTION_GC_LOCK_TTL).await.map_err(|error| {
            CatalogError::PreconditionFailed {
                message: format!(
                    "retention coordination lease was lost before terminal settlement: {error}"
                ),
            }
        })?;
        let completed = record.completed()?;
        let completed_bytes = Bytes::from(encode_record(&completed)?);
        let write = storage
            .put_raw(
                RETENTION_MUTATION_EPOCH_PATH,
                completed_bytes,
                WritePrecondition::MatchesVersion(after.version),
            )
            .await;
        match write {
            Ok(WriteResult::Success { .. }) => Ok(true),
            Ok(WriteResult::PreconditionFailed { .. }) | Err(_) => {
                let selected =
                    decode_record(&storage.get_raw(RETENTION_MUTATION_EPOCH_PATH).await?)?;
                if selected.epoch == record.epoch
                    && selected.state == RetentionMutationState::Idle
                    && selected.operation_kind == record.operation_kind
                    && selected.operation_id == record.operation_id
                    && selected.holder_id == record.holder_id
                    && selected.started_at == record.started_at
                {
                    Ok(true)
                } else {
                    Err(CatalogError::CasFailed {
                        message: "terminal retention mutation epoch settlement is uncertain"
                            .to_string(),
                    })
                }
            }
        }
    }

    /// Executes one exact delete while retaining uncertainty on transport failure.
    pub(crate) async fn delete(&mut self, path: &str) -> Result<()> {
        match self.storage.delete(path).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.uncertain_mutation = true;
                Err(CatalogError::CasFailed {
                    message: format!(
                        "retention mutation delete outcome is uncertain for {path}: {error}"
                    ),
                })
            }
        }
    }

    /// Marks a quiescent epoch idle after every coordinated mutation returned.
    ///
    /// An uncertain mutation result is never cleared automatically. Cancellation
    /// also skips this method and therefore leaves the record in flight.
    pub(crate) async fn settle(mut self) -> Result<()> {
        if self.uncertain_mutation {
            return Err(CatalogError::Storage {
                message: "retention mutation outcome is uncertain; durable epoch remains in flight"
                    .to_string(),
            });
        }
        let completed = self.record.completed()?;
        let bytes = encode_record(&completed)?;
        match self
            .storage
            .put_raw(
                RETENTION_MUTATION_EPOCH_PATH,
                Bytes::from(bytes),
                WritePrecondition::MatchesVersion(self.claimed_version.clone()),
            )
            .await?
        {
            WriteResult::Success { version } => {
                self.record = completed;
                self.claimed_version = version;
                Ok(())
            }
            WriteResult::PreconditionFailed { .. } => Err(CatalogError::CasFailed {
                message: "retention mutation epoch settlement lost CAS".to_string(),
            }),
        }
    }
}

fn encode_record(record: &RetentionMutationEpochRecord) -> Result<Vec<u8>> {
    record.validate()?;
    serde_jcs::to_vec(record).map_err(|error| CatalogError::Serialization {
        message: format!("failed to serialize retention mutation epoch: {error}"),
    })
}

fn decode_record(bytes: &[u8]) -> Result<RetentionMutationEpochRecord> {
    let record: RetentionMutationEpochRecord =
        serde_json::from_slice(bytes).map_err(|error| CatalogError::Serialization {
            message: format!("failed to deserialize retention mutation epoch: {error}"),
        })?;
    record.validate()?;
    Ok(record)
}

fn validate_identity(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(validation(format!(
            "retention mutation {field} is not a safe identifier"
        )));
    }
    Ok(())
}

fn validation(message: impl Into<String>) -> CatalogError {
    CatalogError::Validation {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::Value;

    use arco_core::lock::DistributedLock;
    use arco_core::{MemoryBackend, ScopedStorage, WritePrecondition};

    use super::*;
    use crate::workspace_snapshot::RETENTION_GC_LOCK_PATH;

    const SNAPSHOT_ID: &str = "snap_01ARZ3NDEKTSV4RRFFQ69G5FAV";

    fn storage() -> ScopedStorage {
        ScopedStorage::new(Arc::new(MemoryBackend::default()), "tenant", "workspace")
            .expect("scoped storage")
    }

    async fn acquire(storage: &ScopedStorage) -> LockGuard<ScopedStorage> {
        DistributedLock::new(Arc::new(storage.clone()), RETENTION_GC_LOCK_PATH)
            .acquire(RETENTION_GC_LOCK_TTL, 1)
            .await
            .expect("retention lock")
    }

    async fn read_epoch(storage: &ScopedStorage) -> Value {
        serde_json::from_slice(
            &storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("epoch bytes"),
        )
        .expect("epoch JSON")
    }

    #[tokio::test]
    async fn settlement_is_exact_cas_and_the_next_claim_is_monotonic() {
        let storage = storage();
        let mut first_guard = acquire(&storage).await;
        let first_epoch = RetentionMutationEpoch::claim(
            storage.clone(),
            &mut first_guard,
            RetentionMutationKind::WorkspaceSnapshotFinalize,
            SNAPSHOT_ID,
        )
        .await
        .expect("first claim");
        let first = read_epoch(&storage).await;
        assert_eq!(first["state"], Value::from("IN_FLIGHT"));
        first_epoch.settle().await.expect("first settlement");
        let settled = read_epoch(&storage).await;
        assert_eq!(settled["state"], Value::from("IDLE"));
        first_guard.release().await.expect("first release");

        let mut second_guard = acquire(&storage).await;
        let second_epoch = RetentionMutationEpoch::claim(
            storage.clone(),
            &mut second_guard,
            RetentionMutationKind::WorkspaceSnapshotRetry,
            SNAPSHOT_ID,
        )
        .await
        .expect("second claim");
        let second = read_epoch(&storage).await;
        assert!(
            second["epoch"].as_u64().expect("second epoch")
                > first["epoch"].as_u64().expect("first epoch")
        );
        second_epoch.settle().await.expect("second settlement");
        second_guard.release().await.expect("second release");
    }

    #[tokio::test]
    async fn durable_epoch_advances_after_the_released_lock_record_is_deleted() {
        let storage = storage();
        let mut first_guard = acquire(&storage).await;
        assert_eq!(first_guard.fencing_token().sequence(), 1);
        let first_epoch = RetentionMutationEpoch::claim(
            storage.clone(),
            &mut first_guard,
            RetentionMutationKind::WorkspaceSnapshotFinalize,
            SNAPSHOT_ID,
        )
        .await
        .expect("first claim");
        assert_eq!(read_epoch(&storage).await["epoch"], Value::from(1_u64));
        first_epoch.settle().await.expect("first settlement");
        first_guard.release().await.expect("first release");

        DistributedLock::new(Arc::new(storage.clone()), RETENTION_GC_LOCK_PATH)
            .force_break()
            .await
            .expect("force-break stale released lock record");
        let mut recreated_guard = acquire(&storage).await;
        assert_eq!(
            recreated_guard.fencing_token().sequence(),
            1,
            "a recreated lease demonstrates why its sequence cannot number durable epochs"
        );
        let second_epoch = RetentionMutationEpoch::claim(
            storage.clone(),
            &mut recreated_guard,
            RetentionMutationKind::WorkspaceSnapshotRetry,
            SNAPSHOT_ID,
        )
        .await
        .expect("durable second claim");
        let second = read_epoch(&storage).await;
        assert_eq!(second["epoch"], Value::from(2_u64));
        assert_eq!(second["state"], Value::from("IN_FLIGHT"));
        second_epoch.settle().await.expect("second settlement");
        let settled = read_epoch(&storage).await;
        assert_eq!(settled["epoch"], Value::from(2_u64));
        assert_eq!(settled["state"], Value::from("IDLE"));
        recreated_guard.release().await.expect("second release");
    }

    #[tokio::test]
    async fn exhausted_durable_epoch_fails_closed_without_rewriting_the_record() {
        let storage = storage();
        let exhausted = RetentionMutationEpochRecord::in_flight(
            u64::MAX,
            "previous-holder",
            RetentionMutationKind::CatalogGc,
            "previous-operation",
        )
        .expect("max in-flight record")
        .completed()
        .expect("max idle record");
        let exhausted_bytes = Bytes::from(encode_record(&exhausted).expect("encode max epoch"));
        storage
            .put_raw(
                RETENTION_MUTATION_EPOCH_PATH,
                exhausted_bytes.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("seed max epoch");
        let mut guard = acquire(&storage).await;

        let error = match RetentionMutationEpoch::claim(
            storage.clone(),
            &mut guard,
            RetentionMutationKind::WorkspaceSnapshotFinalize,
            SNAPSHOT_ID,
        )
        .await
        {
            Ok(_) => panic!("durable epoch overflow must fail closed"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            CatalogError::CasFailed { message }
                if message.contains("durable retention mutation epoch is exhausted")
        ));
        assert_eq!(
            storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("max epoch remains"),
            exhausted_bytes
        );
        guard.release().await.expect("release lease");
    }

    #[tokio::test]
    async fn a_new_lease_holder_never_rewrites_a_foreign_inflight_epoch() {
        let storage = storage();
        let mut first_guard = acquire(&storage).await;
        let _first_epoch = RetentionMutationEpoch::claim(
            storage.clone(),
            &mut first_guard,
            RetentionMutationKind::WorkspaceSnapshotFinalize,
            SNAPSHOT_ID,
        )
        .await
        .expect("first claim");
        let before = storage
            .get_raw(RETENTION_MUTATION_EPOCH_PATH)
            .await
            .expect("in-flight bytes");
        first_guard.release().await.expect("release first lease");

        let mut second_guard = acquire(&storage).await;
        let second_operation_id = second_guard.holder_id().to_string();
        assert!(
            RetentionMutationEpoch::claim(
                storage.clone(),
                &mut second_guard,
                RetentionMutationKind::CatalogGc,
                second_operation_id,
            )
            .await
            .is_err()
        );
        let after = storage
            .get_raw(RETENTION_MUTATION_EPOCH_PATH)
            .await
            .expect("unchanged in-flight bytes");
        assert_eq!(after, before);
        second_guard.release().await.expect("release second lease");
    }

    #[tokio::test]
    async fn terminal_reconciliation_requires_the_exact_operation_identity() {
        let storage = storage();
        let mut first_guard = acquire(&storage).await;
        let _uncertain = RetentionMutationEpoch::claim(
            storage.clone(),
            &mut first_guard,
            RetentionMutationKind::WorkspaceRestoreApply,
            "restore-apply-exact-plan-digest",
        )
        .await
        .expect("restore claim");
        let before = storage
            .get_raw(RETENTION_MUTATION_EPOCH_PATH)
            .await
            .expect("in-flight bytes");
        first_guard.release().await.expect("release first lease");

        let mut recovery_guard = acquire(&storage).await;
        assert!(
            !RetentionMutationEpoch::settle_terminal_matching(
                storage.clone(),
                &mut recovery_guard,
                RetentionMutationKind::WorkspaceRestoreApply,
                &BTreeSet::from(["restore-apply-wrong-plan-digest".to_string()]),
            )
            .await
            .expect("mismatched terminal proof is a no-op")
        );
        assert_eq!(
            before,
            storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("in-flight bytes remain")
        );
        assert!(
            RetentionMutationEpoch::claim(
                storage.clone(),
                &mut recovery_guard,
                RetentionMutationKind::CatalogGc,
                "blocked-gc",
            )
            .await
            .is_err(),
            "a mismatched terminal proof must leave GC fail closed"
        );

        assert!(
            RetentionMutationEpoch::settle_terminal_matching(
                storage.clone(),
                &mut recovery_guard,
                RetentionMutationKind::WorkspaceRestoreApply,
                &BTreeSet::from(["restore-apply-exact-plan-digest".to_string()]),
            )
            .await
            .expect("exact terminal proof settles")
        );
        assert_eq!("IDLE", read_epoch(&storage).await["state"]);
        recovery_guard
            .release()
            .await
            .expect("release recovery lease");
    }

    #[tokio::test]
    async fn settlement_cas_loss_preserves_the_competing_epoch_bytes() {
        let storage = storage();
        let mut guard = acquire(&storage).await;
        let epoch = RetentionMutationEpoch::claim(
            storage.clone(),
            &mut guard,
            RetentionMutationKind::WorkspaceSnapshotFinalize,
            SNAPSHOT_ID,
        )
        .await
        .expect("claim");
        let mut competing = read_epoch(&storage).await;
        competing["additive_v1_field"] = Value::from("preserved");
        let competing_bytes = Bytes::from(serde_json::to_vec(&competing).expect("competing JSON"));
        storage
            .put_raw(
                RETENTION_MUTATION_EPOCH_PATH,
                competing_bytes.clone(),
                WritePrecondition::None,
            )
            .await
            .expect("replace epoch version");

        assert!(epoch.settle().await.is_err());
        assert_eq!(
            storage
                .get_raw(RETENTION_MUTATION_EPOCH_PATH)
                .await
                .expect("competing bytes remain"),
            competing_bytes
        );
        guard.release().await.expect("release lease");
    }
}

//! Invocation-local admission for bounded workspace metadata I/O.
use crate::error::{CatalogError, Result};
use crate::workspace_snapshot::{
    RETENTION_GC_LOCK_MAX_RETRIES, RETENTION_GC_LOCK_PATH, RETENTION_GC_LOCK_TTL,
};
use arco_core::lock::{DistributedLock, LockGuard, LockResponseMetadata};
use arco_core::storage::ObjectMeta;
use arco_core::{ScopedStorage, StorageBackend, WriteResult};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub const METADATA_BYTES: usize = 32 * 1024 * 1024;
pub const OPERATIONS: usize = 4096;
pub const SELECTOR_BYTES: usize = 64 * 1024;
pub const EPOCH_BYTES: usize = 256 * 1024;
pub const RECORD_BYTES: usize = 4 * 1024 * 1024;

/// Shared read admission supplied to an explicitly bounded capture provider.
///
/// This context borrows the workspace invocation budget and exposes no backend
/// handle or write operation. It is not serializable or independently constructible.
pub struct WorkspaceCaptureIo<'a> {
    storage: &'a ScopedStorage,
    budget: &'a mut WorkspaceIoBudget,
}

impl<'a> WorkspaceCaptureIo<'a> {
    pub(crate) fn new(storage: &'a ScopedStorage, budget: &'a mut WorkspaceIoBudget) -> Self {
        Self { storage, budget }
    }

    pub(crate) fn workspace_budget(&mut self) -> &mut WorkspaceIoBudget {
        self.budget
    }

    /// Reads a selector within the shared metadata admission.
    ///
    /// # Errors
    /// Returns an error for oversized, unstable, or unavailable metadata.
    pub async fn read_selector(&mut self, path: &str) -> Result<Option<Bytes>> {
        ScopedStorage::validate_path(path)?;
        Ok(self
            .budget
            .read_stable(self.storage, path, SELECTOR_BYTES)
            .await?
            .map(|(bytes, _)| bytes))
    }

    /// Authenticates an explicitly declared immutable provider object.
    ///
    /// # Errors
    /// Returns an error for invalid size, digest, or exhausted admission.
    pub async fn read_required_object(
        &mut self,
        object: &crate::workspace_snapshot::RequiredObject,
    ) -> Result<Bytes> {
        object.validate()?;
        let length = usize::try_from(object.byte_size()).map_err(|_| backpressure())?;
        if length > RECORD_BYTES {
            return Err(CatalogError::MaintenanceBackpressure {
                message: "bounded provider object exceeds its 4 MiB admission".into(),
            });
        }
        self.budget
            .read_immutable(
                self.storage,
                object.relative_path(),
                Some(length),
                object.sha256(),
                RECORD_BYTES,
            )
            .await
    }
}

pub fn object_meta_owned_bytes(meta: &ObjectMeta) -> Result<usize> {
    size_of::<ObjectMeta>()
        .checked_add(meta.path.capacity())
        .and_then(|n| n.checked_add(meta.version.capacity()))
        .and_then(|n| n.checked_add(meta.etag.as_ref().map_or(0, String::capacity)))
        .ok_or_else(backpressure)
}

// Exhaustive: adding an owned CatalogError field requires a new invoice.
pub fn catalog_error_string_capacity(error: &CatalogError) -> Option<usize> {
    match error {
        CatalogError::Storage { message }
        | CatalogError::Serialization { message }
        | CatalogError::Parquet { message }
        | CatalogError::Validation { message }
        | CatalogError::PreconditionFailed { message }
        | CatalogError::CasFailed { message }
        | CatalogError::StaleWriterEpoch { message }
        | CatalogError::AmbiguousAuthorityOutcome { message }
        | CatalogError::MaintenanceBackpressure { message }
        | CatalogError::UnsupportedAuthorityFormat { message }
        | CatalogError::RequestFailed { message, .. }
        | CatalogError::InvariantViolation { message }
        | CatalogError::UnsupportedOperation { message } => Some(message.capacity()),
        CatalogError::AlreadyExists { entity, name } | CatalogError::NotFound { entity, name } => {
            entity.capacity().checked_add(name.capacity())
        }
    }
}

fn catalog_error_owned_bytes(error: &CatalogError) -> Option<usize> {
    let payload = catalog_error_string_capacity(error)?;
    size_of::<CatalogError>().checked_add(payload)
}

pub struct WorkspaceIoBudget {
    bytes: usize,
    operations: usize,
    head_owned_bytes: usize,
    write_owned_bytes: usize,
    error_owned_bytes: usize,
    stopped: bool,
}

impl WorkspaceIoBudget {
    #[cfg(test)]
    pub const fn test_accounting(&self) -> (usize, usize, usize, usize) {
        (
            self.bytes,
            self.operations,
            self.head_owned_bytes,
            self.write_owned_bytes,
        )
    }

    pub const fn new() -> Self {
        Self {
            bytes: 0,
            operations: 0,
            head_owned_bytes: 0,
            write_owned_bytes: 0,
            error_owned_bytes: 0,
            stopped: false,
        }
    }

    pub(crate) fn reserve_bytes(&mut self, bytes: usize) -> Result<()> {
        if self.stopped {
            return Err(self.stopped_error());
        }
        let next = self.bytes.checked_add(bytes).ok_or_else(backpressure)?;
        if next > METADATA_BYTES {
            return Err(backpressure());
        }
        self.bytes = next;
        Ok(())
    }

    // An error has already arrived. Retain its actual capacity even if it
    // exhausts admission; returning () on failure allocates no replacement.
    pub(crate) fn charge_error(&mut self, error: &CatalogError) -> std::result::Result<(), ()> {
        let owned = catalog_error_owned_bytes(error).unwrap_or(usize::MAX);
        self.error_owned_bytes = self.error_owned_bytes.saturating_add(owned);
        self.bytes = self.bytes.saturating_add(owned);
        self.stopped |= self.bytes > METADATA_BYTES;
        if self.stopped { Err(()) } else { Ok(()) }
    }

    fn stopped_error(&mut self) -> CatalogError {
        let error = backpressure();
        let _ = self.charge_error(&error);
        error
    }

    pub(crate) fn charge_operations(&mut self, operations: usize) -> Result<()> {
        if self.stopped {
            return Err(self.stopped_error());
        }
        let next = self
            .operations
            .checked_add(operations)
            .ok_or_else(backpressure)?;
        if next > OPERATIONS {
            return Err(backpressure());
        }
        self.operations = next;
        Ok(())
    }

    pub(crate) fn charge_head(&mut self, meta: &ObjectMeta) -> Result<()> {
        let owned = object_meta_owned_bytes(meta)?;
        // HEAD ownership has already arrived. Preserve the actual charge even
        // when it exceeds admission, then stop before another storage call.
        self.head_owned_bytes = self
            .head_owned_bytes
            .checked_add(owned)
            .ok_or_else(backpressure)?;
        self.bytes = self.bytes.checked_add(owned).ok_or_else(backpressure)?;
        if self.bytes > METADATA_BYTES {
            return Err(backpressure());
        }
        Ok(())
    }

    pub(crate) fn charge_write(&mut self, result: &WriteResult) -> Result<()> {
        let version = match result {
            WriteResult::Success { version } => version,
            WriteResult::PreconditionFailed { current_version } => current_version,
        };
        let owned = size_of::<WriteResult>()
            .checked_add(version.capacity())
            .ok_or_else(backpressure)?;
        self.write_owned_bytes = self
            .write_owned_bytes
            .checked_add(owned)
            .ok_or_else(backpressure)?;
        self.bytes = self.bytes.checked_add(owned).ok_or_else(backpressure)?;
        if self.bytes > METADATA_BYTES {
            return Err(backpressure());
        }
        Ok(())
    }

    pub(crate) async fn read_pin(
        &mut self,
        storage: &ScopedStorage,
        pin_id: &str,
    ) -> Result<(
        crate::workspace_snapshot::RetentionPinRevision,
        crate::workspace_snapshot::RetentionPinRevision,
    )> {
        use crate::workspace_snapshot::{
            decode_retention_pin_latest, decode_retention_pin_revision, retention_pin_latest_path,
            retention_pin_revision_path,
        };
        let (bytes, _) = self
            .read_stable(storage, &retention_pin_latest_path(pin_id)?, SELECTOR_BYTES)
            .await?
            .ok_or_else(|| missing_record("selected retention pin"))?;
        let selector = decode_retention_pin_latest(&bytes)?;
        if selector.pin_id() != pin_id || selector.revision() > 1024 {
            return Err(CatalogError::Validation {
                message: "retention pin identity or traversal limit differs".into(),
            });
        }
        let mut digest = selector.revision_sha256().to_string();
        let mut ordinal = selector.revision();
        let mut successor = None;
        let mut latest = None;
        loop {
            let path = retention_pin_revision_path(pin_id, ordinal)?;
            let raw = digest
                .strip_prefix("sha256:")
                .ok_or_else(|| CatalogError::Validation {
                    message: "pin digest prefix is absent".into(),
                })?;
            let bytes = self
                .read_immutable(storage, &path, None, raw, RECORD_BYTES)
                .await?;
            let current = decode_retention_pin_revision(&bytes)?;
            if current.pin_id() != pin_id || current.revision() != ordinal {
                return Err(CatalogError::Validation {
                    message: "pin revision identity differs".into(),
                });
            }
            if let Some(successor) = &successor {
                crate::gc::reachability::validate_pin_transition(&current, &digest, successor)?;
            } else {
                latest = Some(current.clone());
            }
            if ordinal == 1 {
                let mut latest = latest.ok_or_else(|| CatalogError::Validation {
                    message: "pin chain is empty".into(),
                })?;
                latest.mark_chain_verified(selector.revision_sha256().to_string())?;
                return Ok((current, latest));
            }
            let predecessor = current
                .predecessor()
                .ok_or_else(|| CatalogError::Validation {
                    message: "pin predecessor is absent".into(),
                })?;
            digest = predecessor.revision_sha256().to_string();
            ordinal -= 1;
            successor = Some(current);
        }
    }

    pub(crate) async fn head<S: StorageBackend + ?Sized>(
        &mut self,
        storage: &S,
        path: &str,
    ) -> Result<Option<ObjectMeta>> {
        self.charge_operations(1)?;
        let meta = storage.head(path).await?;
        if let Some(meta) = &meta {
            self.charge_head(meta)?;
        }
        Ok(meta)
    }

    pub(crate) async fn read_stable<S: StorageBackend + ?Sized>(
        &mut self,
        storage: &S,
        path: &str,
        cap: usize,
    ) -> Result<Option<(Bytes, String)>> {
        let probe = cap.checked_add(1).ok_or_else(backpressure)?;
        for attempt in 0..4 {
            let Some(before) = self.head(storage, path).await? else {
                return if attempt == 0 {
                    Ok(None)
                } else {
                    Err(changed_record())
                };
            };
            let length = admitted_length(&before, cap)?;
            if before.version.is_empty() {
                return Err(changed_record());
            }
            self.reserve_bytes(probe)?;
            self.charge_operations(1)?;
            let bytes = storage.get_range(path, 0..probe as u64).await?;
            let after = self.head(storage, path).await?.ok_or_else(changed_record)?;
            if after.version.is_empty() {
                return Err(changed_record());
            }
            if before.version != after.version {
                continue;
            }
            if before.size != after.size || bytes.len() != length {
                return Err(changed_record());
            }
            return Ok(Some((bytes, after.version)));
        }
        Err(changed_record())
    }

    pub(crate) async fn read_exact_stable_record<S: StorageBackend + ?Sized>(
        &mut self,
        storage: &S,
        path: &str,
        cap: usize,
    ) -> Result<(Bytes, String)> {
        for attempt in 0..4 {
            let Some(before) = self.head(storage, path).await? else {
                return if attempt == 0 {
                    Err(missing_record(path))
                } else {
                    Err(changed_record())
                };
            };
            let length = admitted_length(&before, cap)?;
            if before.version.is_empty() {
                return Err(changed_record());
            }
            let probe = length.checked_add(1).ok_or_else(backpressure)?;
            self.reserve_bytes(probe)?;
            self.charge_operations(1)?;
            let bytes = storage.get_range(path, 0..probe as u64).await?;
            let after = self.head(storage, path).await?.ok_or_else(changed_record)?;
            if after.version.is_empty() {
                return Err(changed_record());
            }
            if before.version != after.version {
                continue;
            }
            if before.size != after.size || bytes.len() != length {
                return Err(changed_record());
            }
            return Ok((bytes, after.version));
        }
        Err(changed_record())
    }

    pub(crate) async fn read_immutable<S: StorageBackend + ?Sized>(
        &mut self,
        storage: &S,
        path: &str,
        length: Option<usize>,
        sha256: &str,
        cap: usize,
    ) -> Result<Bytes> {
        let digest = sha256.strip_prefix("sha256:").unwrap_or(sha256);
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(validation("invalid immutable metadata checksum"));
        }
        if length.is_some_and(|length| length == 0 || length > cap) {
            return Err(validation("immutable metadata length exceeds its cap"));
        }
        let meta = self
            .head(storage, path)
            .await?
            .ok_or_else(|| missing_record(path))?;
        let admitted = admitted_length(&meta, cap)?;
        if length.is_some_and(|length| admitted != length) {
            return Err(validation(
                "immutable metadata length differs from its witness",
            ));
        }
        let probe = admitted.checked_add(1).ok_or_else(backpressure)?;
        self.reserve_bytes(probe)?;
        self.charge_operations(1)?;
        let bytes = storage.get_range(path, 0..probe as u64).await?;
        if bytes.len() != admitted {
            return Err(validation("immutable metadata length changed"));
        }
        #[cfg(feature = "test-utils")]
        crate::state_store::control_mvp::record_sha256_work(bytes.len());
        if hex::encode(Sha256::digest(&bytes)) != digest {
            return Err(validation("immutable metadata checksum mismatch"));
        }
        Ok(bytes)
    }

    pub(crate) async fn acquire_retention_lock(
        &mut self,
        storage: ScopedStorage,
        operation: &str,
    ) -> Result<LockGuard<ScopedStorage>> {
        if operation.len() > SELECTOR_BYTES {
            return Err(validation("lock operation exceeds 64 KiB"));
        }
        let attempts = RETENTION_GC_LOCK_MAX_RETRIES as usize;
        self.charge_operations(4 * attempts)?;
        self.reserve_bytes(attempts * (SELECTOR_BYTES + 1))?;
        let lock =
            DistributedLock::new(Arc::new(storage), RETENTION_GC_LOCK_PATH).with_bounded_records();
        let mut rejected = None;
        let result = lock
            .acquire_with_operation_observing_metadata(
                RETENTION_GC_LOCK_TTL,
                RETENTION_GC_LOCK_MAX_RETRIES,
                Some(operation.into()),
                &mut |meta| self.observe_lock_metadata(meta, &mut rejected),
            )
            .await;
        if let Some(error) = rejected {
            return Err(error);
        }
        Ok(result?)
    }

    pub(crate) async fn extend_retention_lock(
        &mut self,
        guard: &mut LockGuard<ScopedStorage>,
    ) -> Result<()> {
        self.charge_operations(3)?;
        self.reserve_bytes(SELECTOR_BYTES + 1)?;
        let mut rejected = None;
        let result = guard
            .extend_observing_metadata(RETENTION_GC_LOCK_TTL, &mut |meta| {
                self.observe_lock_metadata(meta, &mut rejected)
            })
            .await;
        if let Some(error) = rejected {
            return Err(error);
        }
        Ok(result?)
    }

    fn observe_lock_metadata(
        &mut self,
        metadata: LockResponseMetadata<'_>,
        rejected: &mut Option<CatalogError>,
    ) -> arco_core::Result<()> {
        let result = match metadata {
            LockResponseMetadata::Head(meta) => self.charge_head(meta),
            LockResponseMetadata::Write(result) => self.charge_write(result),
        };
        result.map_err(|error| {
            // Preserve the typed caller error rather than classifying a core
            // error string as backpressure after the lock returns.
            *rejected = Some(error);
            arco_core::Error::InvalidInput("workspace metadata admission rejected response".into())
        })
    }

    pub(crate) async fn release_retention_lock(
        &mut self,
        guard: LockGuard<ScopedStorage>,
    ) -> Result<()> {
        self.charge_operations(2)?;
        self.reserve_bytes(SELECTOR_BYTES + 1)?;
        let mut rejected = None;
        let result = guard
            .release_observing_metadata(&mut |metadata| {
                self.observe_lock_metadata(metadata, &mut rejected)
            })
            .await;
        if let Some(error) = rejected {
            return Err(error);
        }
        Ok(result?)
    }
}

fn admitted_length(meta: &ObjectMeta, cap: usize) -> Result<usize> {
    let length =
        usize::try_from(meta.size).map_err(|_| validation("metadata length does not fit usize"))?;
    if length == 0 || length > cap {
        return Err(validation("metadata length exceeds its encoded cap"));
    }
    Ok(length)
}

fn validation(message: &str) -> CatalogError {
    CatalogError::Validation {
        message: message.into(),
    }
}
fn changed_record() -> CatalogError {
    CatalogError::CasFailed {
        message: "workspace metadata changed or disappeared during bounded observation".into(),
    }
}
fn missing_record(path: &str) -> CatalogError {
    CatalogError::NotFound {
        entity: "object".into(),
        name: path.into(),
    }
}
fn backpressure() -> CatalogError {
    CatalogError::MaintenanceBackpressure {
        message: "workspace metadata exceeds its 32 MiB or 4096-operation admission".into(),
    }
}

#[cfg(test)]
pub mod tests {
    #![allow(clippy::expect_used, clippy::panic)]
    #[test]
    fn workspace_error_census_uses_all_owned_fields_and_accumulates() {
        let message = || String::with_capacity(193);
        let variants = [
            CatalogError::Storage { message: message() },
            CatalogError::Serialization { message: message() },
            CatalogError::Parquet { message: message() },
            CatalogError::Validation { message: message() },
            CatalogError::PreconditionFailed { message: message() },
            CatalogError::CasFailed { message: message() },
            CatalogError::StaleWriterEpoch { message: message() },
            CatalogError::AmbiguousAuthorityOutcome { message: message() },
            CatalogError::MaintenanceBackpressure { message: message() },
            CatalogError::UnsupportedAuthorityFormat { message: message() },
            CatalogError::RequestFailed {
                http_status: 409,
                message: message(),
            },
            CatalogError::InvariantViolation { message: message() },
            CatalogError::UnsupportedOperation { message: message() },
            CatalogError::AlreadyExists {
                entity: String::with_capacity(97),
                name: String::with_capacity(96),
            },
            CatalogError::NotFound {
                entity: String::with_capacity(97),
                name: String::with_capacity(96),
            },
        ];
        let mut budget = WorkspaceIoBudget::new();
        for error in &variants {
            assert_eq!(
                catalog_error_owned_bytes(error),
                Some(size_of::<CatalogError>() + 193)
            );
            budget.charge_error(error).expect("charge");
        }
        assert_eq!(
            budget.error_owned_bytes,
            variants.len() * (size_of::<CatalogError>() + 193)
        );
        assert_eq!(budget.bytes, budget.error_owned_bytes);
    }

    #[tokio::test]
    async fn workspace_returned_error_ownership_survives_cleanup_and_stops_exhausted_budget() {
        let mut message = String::with_capacity(4096);
        message.push_str("retained adapter error");
        let error = CatalogError::InvariantViolation { message };
        let mut budget = WorkspaceIoBudget::new();
        budget.charge_error(&error).expect("error admission");
        tokio::task::yield_now().await;
        assert!(budget.bytes >= 4096);
        drop(error);
        assert!(budget.bytes >= 4096, "invocation accounting is cumulative");

        let mut budget = WorkspaceIoBudget::new();
        budget
            .reserve_bytes(METADATA_BYTES - 1)
            .expect("prior work");
        let error = CatalogError::NotFound {
            entity: String::with_capacity(128),
            name: String::with_capacity(4096),
        };
        assert!(budget.charge_error(&error).is_err());
        assert!(budget.bytes >= METADATA_BYTES - 1 + 128 + 4096);
        assert!(
            budget.charge_operations(1).is_err(),
            "no cleanup I/O after rejected arrived ownership"
        );
        assert_eq!(budget.operations, 0);
        assert!(budget.reserve_bytes(0).is_err());
    }

    use super::*;
    use arco_core::{MemoryBackend, WritePrecondition};

    #[test]
    fn workspace_metadata_and_operation_admission_is_cumulative() {
        let mut budget = WorkspaceIoBudget::new();
        budget.reserve_bytes(METADATA_BYTES).expect("last byte");
        assert!(matches!(
            budget.reserve_bytes(1),
            Err(CatalogError::MaintenanceBackpressure { .. })
        ));
        assert_eq!(budget.bytes, METADATA_BYTES);
        budget
            .charge_operations(OPERATIONS)
            .expect("last operation");
        assert!(matches!(
            budget.charge_operations(1),
            Err(CatalogError::MaintenanceBackpressure { .. })
        ));
        assert_eq!(budget.operations, OPERATIONS);
    }

    #[tokio::test]
    async fn workspace_reads_charge_head_ownership_and_fixed_or_exact_probes() {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scope");
        let bytes = Bytes::from_static(b"record");
        storage
            .put_raw(
                "record.json",
                bytes.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("fixture");
        let digest = hex::encode(Sha256::digest(&bytes));
        let meta = storage
            .head_raw("record.json")
            .await
            .expect("HEAD")
            .expect("exists");
        let owned = size_of::<ObjectMeta>()
            + meta.path.capacity()
            + meta.version.capacity()
            + meta.etag.as_ref().map_or(0, String::capacity);
        let mut budget = WorkspaceIoBudget::new();
        let (selected, _) = budget
            .read_stable(&storage, "record.json", SELECTOR_BYTES)
            .await
            .expect("stable")
            .expect("exists");
        assert_eq!(selected, bytes);
        assert_eq!(budget.operations, 3);
        assert_eq!(budget.head_owned_bytes, 2 * owned);
        assert_eq!(budget.bytes, SELECTOR_BYTES + 1 + 2 * owned);
        let before = budget.bytes;
        assert_eq!(
            budget
                .read_immutable(
                    &storage,
                    "record.json",
                    Some(bytes.len()),
                    &digest,
                    RECORD_BYTES
                )
                .await
                .expect("immutable"),
            bytes
        );
        assert_eq!(budget.operations, 5);
        assert_eq!(budget.bytes - before, bytes.len() + 1 + owned);
        assert!(
            budget
                .read_immutable(
                    &storage,
                    "record.json",
                    Some(bytes.len()),
                    &"00".repeat(32),
                    RECORD_BYTES
                )
                .await
                .is_err()
        );
        assert!(
            budget
                .read_immutable(
                    &storage,
                    "record.json",
                    Some(bytes.len() - 1),
                    &digest,
                    RECORD_BYTES
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn exact_stable_record_retries_a_versioned_resize_and_charges_each_probe() {
        let backend = Arc::new(WriteMetadataBackend::default());
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scope");
        storage
            .put_raw(
                "record.json",
                Bytes::from_static(b"old"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("fixture");
        *backend.rewrite_on_get.lock().expect("rewrite") = Some(Bytes::from_static(b"resized"));

        let mut budget = WorkspaceIoBudget::new();
        assert_eq!(
            budget
                .read_exact_stable_record(&storage, "record.json", RECORD_BYTES)
                .await
                .expect("second stable version")
                .0,
            Bytes::from_static(b"resized")
        );
        assert_eq!(budget.operations, 6, "two HEAD/range/HEAD attempts");
        assert!(
            budget.bytes < SELECTOR_BYTES,
            "exact-sized records must not consume a fixed 64 KiB probe: {}",
            budget.bytes
        );

        let mut missing = WorkspaceIoBudget::new();
        assert!(matches!(
            missing
                .read_exact_stable_record(&storage, "missing.json", RECORD_BYTES)
                .await,
            Err(CatalogError::NotFound { .. })
        ));
        assert_eq!(missing.operations, 1, "missing records perform one HEAD");
    }
    #[tokio::test]
    async fn workspace_lock_invoices_precede_io_and_include_internal_heads() {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scope");
        let mut denied = WorkspaceIoBudget::new();
        denied.bytes = METADATA_BYTES;
        assert!(matches!(
            denied
                .acquire_retention_lock(storage.clone(), "denied")
                .await,
            Err(CatalogError::MaintenanceBackpressure { .. })
        ));
        assert!(
            storage
                .head_raw(RETENTION_GC_LOCK_PATH)
                .await
                .expect("HEAD")
                .is_none()
        );
        let mut budget = WorkspaceIoBudget::new();
        let mut guard = budget
            .acquire_retention_lock(storage, "admitted")
            .await
            .expect("acquire");
        assert_eq!(budget.operations, 20);
        assert_eq!(
            budget.bytes,
            5 * (SELECTOR_BYTES + 1) + budget.write_owned_bytes
        );
        budget
            .extend_retention_lock(&mut guard)
            .await
            .expect("renew");
        assert_eq!(budget.operations, 23);
        assert!(budget.head_owned_bytes > 0);
        assert_eq!(
            budget.bytes,
            6 * (SELECTOR_BYTES + 1) + budget.head_owned_bytes + budget.write_owned_bytes
        );
        let head_bytes = budget.head_owned_bytes;
        budget.release_retention_lock(guard).await.expect("release");
        assert_eq!(budget.operations, 25);
        assert_eq!(budget.head_owned_bytes, head_bytes);
        assert_eq!(
            budget.bytes,
            7 * (SELECTOR_BYTES + 1) + head_bytes + budget.write_owned_bytes
        );
    }

    #[test]
    fn workspace_head_admission_charges_owned_capacity() {
        let meta = ObjectMeta {
            path: "object".into(),
            size: 1,
            version: String::with_capacity(METADATA_BYTES),
            last_modified: None,
            etag: None,
        };
        let mut budget = WorkspaceIoBudget::new();
        assert!(matches!(
            budget.charge_head(&meta),
            Err(CatalogError::MaintenanceBackpressure { .. })
        ));
        assert!(
            budget.head_owned_bytes > METADATA_BYTES,
            "already-returned metadata ownership must not be understated"
        );
    }
    #[test]
    fn workspace_lock_rejects_oversized_operation_before_ownership_copy() {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scope");
        let operation = "x".repeat(2 * 1024 * 1024);
        let mut budget = WorkspaceIoBudget::new();
        let mut rejected = false;
        let allocated = allocation_counter::measure(|| {
            rejected =
                futures::executor::block_on(budget.acquire_retention_lock(storage, &operation))
                    .is_err();
        });
        assert!(rejected);
        assert!(
            allocated.bytes_total < SELECTOR_BYTES as u64,
            "workspace wrapper copied oversized input: {} bytes",
            allocated.bytes_total
        );
    }
    #[derive(Debug, Default)]
    pub struct WriteMetadataBackend {
        pub(crate) inner: MemoryBackend,
        pub(crate) target: std::sync::Mutex<String>,
        rewrite_on_get: std::sync::Mutex<Option<Bytes>>,
        pub(crate) failed: std::sync::atomic::AtomicBool,
        matches_only: std::sync::atomic::AtomicBool,
        targeted_puts: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl StorageBackend for WriteMetadataBackend {
        async fn signed_url(
            &self,
            path: &str,
            expiry: std::time::Duration,
        ) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }

        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            self.inner.get(path).await
        }
        async fn get_range(
            &self,
            path: &str,
            range: std::ops::Range<u64>,
        ) -> arco_core::Result<Bytes> {
            let bytes = self.inner.get_range(path, range).await?;
            let rewrite = self.rewrite_on_get.lock().expect("rewrite").take();
            if let Some(rewrite) = rewrite {
                self.inner
                    .put(path, rewrite, WritePrecondition::None)
                    .await?;
            }
            Ok(bytes)
        }
        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.inner.head(path).await
        }
        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }
        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }
        async fn put(
            &self,
            path: &str,
            bytes: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            let target = self.target.lock().expect("target").clone();
            let targeted = !target.is_empty() && path.ends_with(&target);
            if targeted {
                self.targeted_puts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            let poison = targeted
                && (!self.matches_only.load(std::sync::atomic::Ordering::SeqCst)
                    || matches!(&precondition, WritePrecondition::MatchesVersion(_)));
            let result = if poison && self.failed.load(std::sync::atomic::Ordering::SeqCst) {
                WriteResult::PreconditionFailed {
                    current_version: "foreign".into(),
                }
            } else {
                self.inner.put(path, bytes, precondition).await?
            };
            if !poison {
                return Ok(result);
            }
            let inflate = |mut version: String| {
                version.reserve_exact(40 * 1024 * 1024);
                version
            };
            Ok(match result {
                WriteResult::Success { version } => WriteResult::Success {
                    version: inflate(version),
                },
                WriteResult::PreconditionFailed { current_version } => {
                    WriteResult::PreconditionFailed {
                        current_version: inflate(current_version),
                    }
                }
            })
        }
    }

    #[tokio::test]
    async fn workspace_lock_accounts_for_successful_and_failed_put_metadata() {
        for phase in ["acquire", "failed acquire", "extend", "release"] {
            let backend = Arc::new(WriteMetadataBackend::default());
            let storage =
                ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scope");
            let mut budget = WorkspaceIoBudget::new();
            if phase.contains("acquire") {
                *backend.target.lock().expect("target") = RETENTION_GC_LOCK_PATH.into();
                backend.failed.store(
                    phase == "failed acquire",
                    std::sync::atomic::Ordering::SeqCst,
                );
                assert!(
                    budget
                        .acquire_retention_lock(storage, "claim")
                        .await
                        .is_err(),
                    "{phase} accepted oversized PUT metadata"
                );
            } else {
                let mut guard = budget
                    .acquire_retention_lock(storage, "claim")
                    .await
                    .expect("claim");
                *backend.target.lock().expect("target") = RETENTION_GC_LOCK_PATH.into();
                let result = if phase == "extend" {
                    budget.extend_retention_lock(&mut guard).await
                } else {
                    budget.release_retention_lock(guard).await
                };
                assert!(result.is_err(), "{phase} accepted oversized PUT metadata");
            }
            assert!(
                budget.bytes > METADATA_BYTES,
                "actual returned ownership must be charged for {phase}"
            );
        }
    }
    #[tokio::test]
    async fn workspace_takeover_put_response_is_charged_before_returning_a_guard() {
        let backend = Arc::new(WriteMetadataBackend::default());
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scope");
        let mut budget = WorkspaceIoBudget::new();
        let guard = budget
            .acquire_retention_lock(storage.clone(), "previous")
            .await
            .expect("previous lease");
        budget
            .release_retention_lock(guard)
            .await
            .expect("expire previous lease");
        *backend.target.lock().expect("target") = RETENTION_GC_LOCK_PATH.into();
        backend
            .matches_only
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(matches!(
            budget.acquire_retention_lock(storage, "takeover").await,
            Err(CatalogError::MaintenanceBackpressure { .. })
        ));
        assert_eq!(
            backend
                .targeted_puts
                .load(std::sync::atomic::Ordering::SeqCst),
            2,
            "initial create failure then exactly one takeover CAS"
        );
        assert!(budget.write_owned_bytes > METADATA_BYTES);
    }
    #[tokio::test]
    async fn bounded_provider_context_shares_admission_and_authenticates_objects() {
        use crate::workspace_snapshot::{RequiredObject, RequiredObjectKind};
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scope");
        let bytes = Bytes::from_static(b"provider metadata");
        storage
            .put_raw(
                "provider.json",
                bytes.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("fixture");
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(&bytes)));
        let object = RequiredObject::new(
            "provider.json",
            bytes.len() as u64,
            RequiredObjectKind::Other,
            &digest,
        )
        .expect("object");
        let mut budget = WorkspaceIoBudget::new();
        budget
            .charge_operations(OPERATIONS - 2)
            .expect("two operations remain");
        assert_eq!(
            WorkspaceCaptureIo::new(&storage, &mut budget)
                .read_required_object(&object)
                .await
                .expect("first provider"),
            bytes
        );
        assert!(
            matches!(
                WorkspaceCaptureIo::new(&storage, &mut budget)
                    .read_required_object(&object)
                    .await,
                Err(CatalogError::MaintenanceBackpressure { .. })
            ),
            "second provider must share the exhausted budget"
        );
        for (length, checksum) in [
            (bytes.len() as u64 + 1, digest.clone()),
            (bytes.len() as u64, format!("sha256:{}", "00".repeat(32))),
            (RECORD_BYTES as u64 + 1, digest),
        ] {
            let object =
                RequiredObject::new("provider.json", length, RequiredObjectKind::Other, checksum)
                    .expect("declared object");
            assert!(
                WorkspaceCaptureIo::new(&storage, &mut WorkspaceIoBudget::new())
                    .read_required_object(&object)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn bounded_provider_selectors_cannot_escape_the_shared_budget_or_size_cap() {
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scope");
        storage
            .put_raw(
                "selector.json",
                Bytes::from(vec![0; SELECTOR_BYTES + 1]),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("fixture");
        let mut budget = WorkspaceIoBudget::new();
        assert!(
            WorkspaceCaptureIo::new(&storage, &mut budget)
                .read_selector("selector.json")
                .await
                .is_err()
        );
        let mut exhausted = WorkspaceIoBudget::new();
        exhausted
            .charge_operations(OPERATIONS)
            .expect("exhaust operations");
        assert!(matches!(
            WorkspaceCaptureIo::new(&storage, &mut exhausted)
                .read_selector("missing.json")
                .await,
            Err(CatalogError::MaintenanceBackpressure { .. })
        ));
    }

    struct LegacyCaptureOnly(std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl crate::workspace_snapshot_service::ProjectionWatermarkProvider for LegacyCaptureOnly {
        async fn capture(
            &self,
            _authority: &crate::workspace_snapshot::DomainAuthorityReference,
        ) -> Result<crate::workspace_snapshot_service::ProjectionWatermarkCut> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(CatalogError::UnsupportedOperation {
                message: "legacy provider invoked".into(),
            })
        }
    }

    #[async_trait::async_trait]
    impl crate::workspace_snapshot_service::EventArchiveProvider for LegacyCaptureOnly {
        async fn capture(
            &self,
            _authority: &crate::workspace_snapshot::DomainAuthorityReference,
        ) -> Result<crate::workspace_snapshot_service::EventArchiveCapture> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(CatalogError::UnsupportedOperation {
                message: "legacy provider invoked".into(),
            })
        }
    }

    #[tokio::test]
    async fn bounded_provider_defaults_never_call_legacy_capture() {
        use crate::state_store::{PersistedAuthorityKind, PersistedAuthorityReference, StateScope};
        use crate::workspace_snapshot::{DomainAuthorityReference, WorkspaceScope};
        let scope = StateScope::new("tenant", "workspace", "catalog");
        let reference = PersistedAuthorityReference::new(
            "arco-state-control-mvp",
            scope,
            PersistedAuthorityKind::StateToken,
            "manifest",
            1,
            "control/v1/domains/catalog/manifests/manifest.json",
            format!("sha256:{}", "11".repeat(32)),
            None,
            None,
            chrono::Utc::now() + chrono::Duration::days(1),
        )
        .expect("reference");
        let authority = DomainAuthorityReference::new(
            "catalog",
            WorkspaceScope::new("tenant", "workspace").expect("scope"),
            reference,
        )
        .expect("authority");
        let provider = LegacyCaptureOnly(std::sync::atomic::AtomicUsize::new(0));
        let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
            .expect("scope");
        let mut budget = WorkspaceIoBudget::new();
        let mut context = WorkspaceCaptureIo::new(&storage, &mut budget);
        assert!(
            crate::workspace_snapshot_service::ProjectionWatermarkProvider::capture_bounded(
                &provider,
                &authority,
                &mut context
            )
            .await
            .is_err()
        );
        assert!(
            crate::workspace_snapshot_service::EventArchiveProvider::capture_bounded(
                &provider,
                &authority,
                &mut context
            )
            .await
            .is_err()
        );
        assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}

//! Tier 1 manifest writer with distributed lock + CAS semantics.
//!
//! Tier 1 writes are the strongly-consistent catalog operations (DDL-like):
//! create/update/drop assets, schemas, and other low-frequency mutations.
//!
//! The critical invariants are:
//! - Only one writer enters the critical section at a time (distributed lock)
//! - Manifest updates are committed via CAS (`MatchesVersion`)
//! - Writers retry on CAS conflicts (e.g., if a writer bypasses the lock)
//! - On-disk manifests are physically multi-file (root + domain manifests)

use std::{collections::HashSet, time::Duration};

use bytes::Bytes;
use chrono::Utc;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use ulid::Ulid;

use arco_core::publish::{
    SnapshotPointerDurability, SnapshotPointerPublishOutcome, publish_snapshot_pointer_transaction,
};
use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};
use arco_core::storage_keys::{LedgerKey, StateKey};
use arco_core::storage_traits::LedgerPutStore;
use arco_core::{
    CatalogDomain, CatalogEvent, CatalogEventPayload, CatalogPaths, EventId, ScopedStorage,
};

use crate::error::{CatalogError, Result};
use crate::lock::LockGuard;
use crate::lock::{DEFAULT_LOCK_TTL, DEFAULT_MAX_RETRIES, DistributedLock};
use crate::manifest::{
    CatalogDomainManifest, CatalogManifest, CommitRecord, DomainManifestPointer,
    ExecutionsManifest, INITIAL_MANIFEST_ID, LineageManifest, RootManifest, SearchManifest,
    compute_manifest_hash, next_manifest_id, parse_manifest_id,
};
use crate::parquet_util::{CatalogCommitEventWitness, decode_catalog_commit_event_witnesses};
use crate::tier1_state;
use crate::write_options::CatalogTransactionIdentity;

/// Maximum CAS retries for manifest writes.
const DEFAULT_MAX_CAS_RETRIES: u32 = 10;

const CATALOG_TRANSACTION_INTENT_RECORD_TYPE: &str = "catalog_transaction_event_intent";
const CATALOG_TRANSACTION_INTENT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CatalogTransactionEventIntent {
    record_type: String,
    version: u32,
    tx_id: String,
    request_hash: String,
    base_manifest_id: String,
    base_manifest_path: String,
    base_manifest_sha256: String,
    event_binding_sha256: String,
    source: String,
    revision: u64,
    event_ids: Vec<String>,
    active_event_id: String,
    active_event_path: String,
    event_json: String,
}

#[derive(Debug)]
pub(crate) struct PublishedCatalogTransactionEvent {
    pub event_id: String,
    pub manifest: CatalogDomainManifest,
    pub authority_version: String,
    pub is_current_pointer: bool,
}

pub(crate) struct CatalogTransactionPublication<'a> {
    pub event_id: &'a str,
    pub commit_id: &'a str,
    pub manifest_id: &'a str,
    pub snapshot_version: u64,
    pub authority_version: &'a str,
    pub fencing_token: u64,
}

#[derive(Debug)]
pub(crate) enum CatalogTransactionEventRecovery {
    Published(Box<PublishedCatalogTransactionEvent>),
    Ready(EventId),
    RetryUnlocked,
}

pub(crate) enum CatalogTransactionEventInspection {
    Published(Box<PublishedCatalogTransactionEvent>),
    Unpublished(CatalogTransactionRecoveryInspection),
}

pub(crate) struct CatalogTransactionRecoveryInspection {
    intent_version: String,
    head_manifest_id: String,
    pointer_version: String,
}

enum SelectedCatalogTransactionPublication {
    Published(Box<PublishedCatalogTransactionEvent>),
    Unpublished,
    RequiresHistoryScan,
}

#[derive(Debug)]
struct VersionedCatalogTransactionIntent {
    value: CatalogTransactionEventIntent,
    version: String,
}

struct StableCatalogManifest {
    value: CatalogDomainManifest,
    bytes: Bytes,
    version: String,
}

struct SelectedCatalogCommitHistory {
    current: StableCatalogManifest,
    pointer_version: String,
    commits: Vec<crate::parquet_util::CatalogCommitRecord>,
}

enum SelectedCatalogCommitHistoryAvailability {
    Available(Box<SelectedCatalogCommitHistory>),
    Unpublished,
    RequiresHistoryScan,
}

/// Tier 1 writer for catalog manifests.
///
/// Owns:
/// - Tenant/workspace scoped storage
/// - A distributed lock instance
/// - CAS retry policy
pub struct Tier1Writer {
    storage: ScopedStorage,
    lock: DistributedLock<dyn StorageBackend>,
    lock_ttl: Duration,
    lock_max_retries: u32,
    cas_max_retries: u32,
}

impl Tier1Writer {
    /// Creates a new Tier 1 writer for the given scope.
    ///
    /// The lock path is derived from [`ScopedStorage::lock`].
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        let backend = storage.backend().clone();
        let lock_path = storage.lock(CatalogDomain::Catalog);
        let lock = DistributedLock::new(backend, lock_path);

        Self {
            storage,
            lock,
            lock_ttl: DEFAULT_LOCK_TTL,
            lock_max_retries: DEFAULT_MAX_RETRIES,
            cas_max_retries: DEFAULT_MAX_CAS_RETRIES,
        }
    }

    /// Sets the lock acquisition policy for this writer.
    #[must_use]
    pub const fn with_lock_policy(mut self, ttl: Duration, max_retries: u32) -> Self {
        self.lock_ttl = ttl;
        self.lock_max_retries = max_retries;
        self
    }

    /// Sets the maximum CAS retries for manifest updates.
    #[must_use]
    pub const fn with_cas_retries(mut self, max_retries: u32) -> Self {
        self.cas_max_retries = max_retries;
        self
    }

    /// Initializes the catalog manifests (idempotent).
    ///
    /// Creates:
    /// - `manifests/root.manifest.json` (entry point)
    /// - `manifests/catalog.pointer.json` + `manifests/catalog/{manifest_id}.json`
    /// - `manifests/lineage.pointer.json` + `manifests/lineage/{manifest_id}.json`
    /// - `manifests/executions.manifest.json`
    /// - `manifests/search.pointer.json` + `manifests/search/{manifest_id}.json`
    ///
    /// # Errors
    ///
    /// Returns an error if storage operations fail.
    pub async fn initialize(&self) -> Result<()> {
        let guard = self
            .lock
            .acquire_with_operation(
                self.lock_ttl,
                self.lock_max_retries,
                Some("InitializeCatalog".into()),
            )
            .await
            .map_err(CatalogError::from)?;

        let mut root = match self.storage.get_raw(CatalogPaths::ROOT_MANIFEST).await {
            Ok(bytes) => serde_json::from_slice::<RootManifest>(&bytes).map_err(|e| {
                CatalogError::Serialization {
                    message: format!("parse JSON at {}: {e}", CatalogPaths::ROOT_MANIFEST),
                }
            })?,
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                RootManifest::new()
            }
            Err(error) => return Err(CatalogError::from(error)),
        };
        let legacy_root = root.clone();
        root.normalize_paths();
        self.bootstrap_tier1_manifest(
            CatalogDomain::Catalog,
            &legacy_root.catalog_manifest_path,
            &CatalogDomainManifest::new(),
            |manifest: &CatalogDomainManifest| manifest.manifest_id.as_str(),
            |manifest: &CatalogDomainManifest| manifest.fencing_token.unwrap_or(manifest.epoch),
            |manifest: &mut CatalogDomainManifest, snapshot_manifest_path| {
                sanitize_legacy_bootstrap_history(
                    CatalogDomain::Catalog,
                    &mut manifest.previous_manifest_path,
                    &mut manifest.parent_hash,
                    snapshot_manifest_path,
                );
            },
        )
        .await?;
        self.bootstrap_tier1_manifest(
            CatalogDomain::Lineage,
            &legacy_root.lineage_manifest_path,
            &LineageManifest::new(),
            |manifest: &LineageManifest| manifest.manifest_id.as_str(),
            |manifest: &LineageManifest| manifest.fencing_token.unwrap_or(manifest.epoch),
            |manifest: &mut LineageManifest, snapshot_manifest_path| {
                sanitize_legacy_bootstrap_history(
                    CatalogDomain::Lineage,
                    &mut manifest.previous_manifest_path,
                    &mut manifest.parent_hash,
                    snapshot_manifest_path,
                );
            },
        )
        .await?;
        self.bootstrap_tier1_manifest(
            CatalogDomain::Search,
            &legacy_root.search_manifest_path,
            &SearchManifest::new(),
            |manifest: &SearchManifest| manifest.manifest_id.as_str(),
            |manifest: &SearchManifest| manifest.fencing_token.unwrap_or(manifest.epoch),
            |manifest: &mut SearchManifest, snapshot_manifest_path| {
                sanitize_legacy_bootstrap_history(
                    CatalogDomain::Search,
                    &mut manifest.previous_manifest_path,
                    &mut manifest.parent_hash,
                    snapshot_manifest_path,
                );
            },
        )
        .await?;
        self.storage
            .put_raw(
                CatalogPaths::ROOT_MANIFEST,
                json_bytes(&root)?,
                WritePrecondition::None,
            )
            .await?;
        self.ensure_json_exists(&root.executions_manifest_path, &ExecutionsManifest::new())
            .await?;
        self.repair_legacy_catalog_manifest_history(guard.fencing_token().sequence())
            .await?;

        guard.release().await.map_err(CatalogError::from)
    }

    /// Reads the current catalog manifest by loading domain manifests.
    ///
    /// # Errors
    ///
    /// Returns an error if any required manifest is missing or cannot be parsed.
    pub async fn read_manifest(&self) -> Result<CatalogManifest> {
        let mut root: RootManifest = self.read_json(CatalogPaths::ROOT_MANIFEST).await?;
        root.normalize_paths();

        let catalog: CatalogDomainManifest = self
            .read_current_domain_manifest(CatalogDomain::Catalog)
            .await?;
        let lineage: LineageManifest = self
            .read_current_domain_manifest(CatalogDomain::Lineage)
            .await?;
        let executions: ExecutionsManifest = self.read_json(&root.executions_manifest_path).await?;
        let search: SearchManifest = self
            .read_current_domain_manifest(CatalogDomain::Search)
            .await?;

        Ok(CatalogManifest {
            version: root.version,
            catalog,
            lineage,
            executions,
            search,
            created_at: root.updated_at,
            updated_at: Utc::now(),
        })
    }

    /// Applies an update to the manifest and commits via CAS.
    ///
    /// The provided closure may be invoked multiple times if CAS conflicts occur,
    /// and must therefore be free of side effects.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock cannot be acquired, manifests are missing, or
    /// if the CAS update fails after all retries.
    #[deprecated(
        since = "0.1.0",
        note = "use CatalogWriter + SyncCompactor for API writes, or update_locked for low-level lock-held flows"
    )]
    pub async fn update<F>(&self, mut update_fn: F) -> Result<CommitRecord>
    where
        F: FnMut(&mut CatalogDomainManifest) -> Result<()>,
    {
        let guard = self
            .lock
            .acquire_with_operation(self.lock_ttl, self.lock_max_retries, Some("Update".into()))
            .await
            .map_err(CatalogError::from)?;

        let result = self.update_inner(&guard, &mut update_fn).await;

        match result {
            Ok(commit) => {
                guard.release().await.map_err(CatalogError::from)?;
                Ok(commit)
            }
            Err(e) => Err(e),
        }
    }

    /// Applies an update to the catalog domain manifest while an external lock is held.
    ///
    /// This is used by higher-level writers that acquire the lock once, perform
    /// snapshot writes, then publish by updating the manifest in the same critical
    /// section.
    ///
    /// The passed `guard` is a proof of lock acquisition; it is not otherwise used.
    ///
    /// # Errors
    ///
    /// Returns an error if manifest reads/writes fail or if the update closure returns an error.
    pub async fn update_locked<F>(
        &self,
        guard: &LockGuard<dyn StorageBackend>,
        mut update_fn: F,
    ) -> Result<CommitRecord>
    where
        F: FnMut(&mut CatalogDomainManifest) -> Result<()>,
    {
        self.update_inner(guard, &mut update_fn).await
    }

    /// Acquires the catalog domain lock and returns a guard.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock cannot be acquired within the retry budget.
    pub async fn acquire_lock(
        &self,
        ttl: Duration,
        max_retries: u32,
    ) -> Result<LockGuard<dyn StorageBackend>> {
        self.lock
            .acquire(ttl, max_retries)
            .await
            .map_err(Self::map_lock)
    }

    fn map_lock(err: arco_core::Error) -> CatalogError {
        CatalogError::from(err)
    }

    fn catalog_transaction_intent_path(identity: &CatalogTransactionIdentity) -> Result<String> {
        let tx_id =
            Ulid::from_string(&identity.tx_id).map_err(|_| CatalogError::InvariantViolation {
                message: "catalog transaction identity must use a canonical ULID".to_string(),
            })?;
        if tx_id.to_string() != identity.tx_id
            || !identity.request_hash.starts_with("sha256:")
            || identity.request_hash.len() != "sha256:".len() + 64
            || !identity.request_hash["sha256:".len()..]
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(CatalogError::InvariantViolation {
                message: "catalog transaction identity is non-canonical".to_string(),
            });
        }
        Ok(format!(
            "transactions/catalog/{}.intent.json",
            identity.tx_id
        ))
    }

    fn validate_catalog_transaction_intent_envelope(
        identity: &CatalogTransactionIdentity,
        source: Option<&str>,
        intent: &CatalogTransactionEventIntent,
    ) -> Result<CatalogEvent<serde_json::Value>> {
        let canonical_sha256 = |value: &str| {
            value.starts_with("sha256:")
                && value.len() == "sha256:".len() + 64
                && value["sha256:".len()..]
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        };
        let active = intent.event_ids.last().map(String::as_str);
        let event_ids = intent
            .event_ids
            .iter()
            .map(|event_id| {
                event_id
                    .parse::<EventId>()
                    .ok()
                    .filter(|parsed| parsed.to_string() == *event_id)
            })
            .collect::<Option<Vec<_>>>();
        let event_history_is_canonical = event_ids.as_ref().is_some_and(|event_ids| {
            usize::try_from(intent.revision).ok() == Some(event_ids.len())
                && event_ids
                    .windows(2)
                    .all(|pair| matches!(pair, [left, right] if left < right))
        });
        if intent.record_type != CATALOG_TRANSACTION_INTENT_RECORD_TYPE
            || intent.version != CATALOG_TRANSACTION_INTENT_VERSION
            || intent.tx_id != identity.tx_id
            || intent.request_hash != identity.request_hash
            || intent.base_manifest_id.len() != 20
            || !intent
                .base_manifest_id
                .bytes()
                .all(|byte| byte.is_ascii_digit())
            || intent.base_manifest_path
                != CatalogPaths::domain_manifest_snapshot(
                    CatalogDomain::Catalog,
                    &intent.base_manifest_id,
                )
            || !canonical_sha256(&intent.base_manifest_sha256)
            || !canonical_sha256(&intent.event_binding_sha256)
            || source.is_some_and(|source| source != intent.source)
            || intent.revision == 0
            || intent.event_ids.is_empty()
            || active != Some(intent.active_event_id.as_str())
            || !event_history_is_canonical
            || intent.active_event_path
                != LedgerKey::event(CatalogDomain::Catalog, &intent.active_event_id).as_ref()
        {
            return Err(CatalogError::InvariantViolation {
                message: "catalog transaction event intent is corrupt or out of scope".to_string(),
            });
        }
        let event: CatalogEvent<serde_json::Value> = serde_json::from_str(&intent.event_json)
            .map_err(|_| CatalogError::InvariantViolation {
                message: "catalog transaction event intent contains invalid event JSON".to_string(),
            })?;
        event
            .validate()
            .map_err(|_| CatalogError::InvariantViolation {
                message: "catalog transaction event intent contains an invalid event envelope"
                    .to_string(),
            })?;
        let expected_idempotency_key = CatalogEvent::<()>::generate_idempotency_key(
            &event.event_type,
            event.event_version,
            &event.payload,
        )
        .map_err(|_| CatalogError::InvariantViolation {
            message: "catalog transaction event intent idempotency binding is invalid".to_string(),
        })?;
        if event.source != intent.source || event.idempotency_key != expected_idempotency_key {
            return Err(CatalogError::InvariantViolation {
                message: "catalog transaction event intent envelope binding is invalid".to_string(),
            });
        }
        Ok(event)
    }

    fn catalog_transaction_event_binding(
        identity: &CatalogTransactionIdentity,
        intent: &CatalogTransactionEventIntent,
        event: &CatalogEvent<serde_json::Value>,
        event_semantics: &serde_json::Value,
    ) -> Result<String> {
        let value = serde_json::json!({
            "recordType": "catalog_transaction_event_binding",
            "version": 1,
            "txId": identity.tx_id,
            "requestHash": identity.request_hash,
            "stagedSha256": identity.staged_sha256,
            "baseManifestId": intent.base_manifest_id,
            "baseManifestPath": intent.base_manifest_path,
            "baseManifestSha256": intent.base_manifest_sha256,
            "source": intent.source,
            "eventType": event.event_type,
            "eventVersion": event.event_version,
            "eventSemantics": event_semantics,
        });
        let bytes = arco_core::canonical_json::to_canonical_bytes(&value).map_err(|error| {
            CatalogError::Serialization {
                message: format!(
                    "failed to canonicalize catalog transaction event binding: {error}"
                ),
            }
        })?;
        Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
    }

    async fn validate_catalog_transaction_intent(
        &self,
        identity: &CatalogTransactionIdentity,
        source: Option<&str>,
        intent: &CatalogTransactionEventIntent,
    ) -> Result<()> {
        if identity.reviewed_request.request_hash()? != identity.request_hash {
            return Err(CatalogError::InvariantViolation {
                message: "catalog transaction capability lost its reviewed request binding"
                    .to_string(),
            });
        }
        let event = Self::validate_catalog_transaction_intent_envelope(identity, source, intent)?;
        let base = self
            .load_stable_catalog_manifest(&intent.base_manifest_path)
            .await?;
        if base.value.manifest_id != intent.base_manifest_id
            || compute_manifest_hash(&base.bytes) != intent.base_manifest_sha256
        {
            return Err(CatalogError::InvariantViolation {
                message:
                    "catalog transaction event intent diverges from its immutable base manifest"
                        .to_string(),
            });
        }
        let base_state =
            tier1_state::load_catalog_state(&self.storage, &base.value.snapshot_path).await?;
        let event_semantics = identity.reviewed_request.validate_event_realization(
            &event.event_type,
            event.event_version,
            &event.payload,
            &base_state,
            &identity.tenant_id,
            &identity.workspace_id,
        )?;
        let binding =
            Self::catalog_transaction_event_binding(identity, intent, &event, &event_semantics)?;
        if binding != intent.event_binding_sha256 {
            return Err(CatalogError::InvariantViolation {
                message: "catalog transaction event intent has a divergent operation binding"
                    .to_string(),
            });
        }
        Ok(())
    }

    fn validate_catalog_transaction_intent_collision(
        candidate: &CatalogTransactionEventIntent,
        winner: &CatalogTransactionEventIntent,
    ) -> Result<()> {
        if winner.base_manifest_id != candidate.base_manifest_id
            || winner.base_manifest_path != candidate.base_manifest_path
            || winner.base_manifest_sha256 != candidate.base_manifest_sha256
            || winner.event_binding_sha256 != candidate.event_binding_sha256
        {
            return Err(CatalogError::InvariantViolation {
                message:
                    "catalog transaction event intent collision diverges from the current execution base"
                        .to_string(),
            });
        }
        Ok(())
    }

    async fn load_catalog_transaction_intent(
        &self,
        identity: &CatalogTransactionIdentity,
    ) -> Result<Option<VersionedCatalogTransactionIntent>> {
        let path = Self::catalog_transaction_intent_path(identity)?;
        let Some(metadata) = self.storage.head_raw(&path).await? else {
            return Ok(None);
        };
        let bytes = self.storage.get_raw(&path).await?;
        let value: CatalogTransactionEventIntent =
            serde_json::from_slice(bytes.as_ref()).map_err(|_| {
                CatalogError::InvariantViolation {
                    message: "catalog transaction event intent is corrupt".to_string(),
                }
            })?;
        let canonical =
            serde_json::to_vec(&value).map_err(|error| CatalogError::Serialization {
                message: format!(
                    "failed to canonicalize catalog transaction event intent: {error}"
                ),
            })?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(CatalogError::InvariantViolation {
                message: "catalog transaction event intent is not canonical JSON".to_string(),
            });
        }
        self.validate_catalog_transaction_intent(identity, None, &value)
            .await?;
        Ok(Some(VersionedCatalogTransactionIntent {
            value,
            version: metadata.version,
        }))
    }

    async fn ensure_catalog_transaction_event(
        &self,
        intent: &CatalogTransactionEventIntent,
    ) -> Result<()> {
        let payload = Bytes::from(intent.event_json.clone());
        match self
            .storage
            .put_raw(
                &intent.active_event_path,
                payload.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await?
        {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => {
                let existing = self.storage.get_raw(&intent.active_event_path).await?;
                if existing == payload {
                    Ok(())
                } else {
                    Err(CatalogError::InvariantViolation {
                        message: "catalog transaction event path contains different bytes"
                            .to_string(),
                    })
                }
            }
        }
    }

    async fn build_catalog_transaction_event_intent<
        T: CatalogEventPayload + serde::Serialize + Sync,
    >(
        &self,
        payload: &T,
        source: &str,
        identity: &CatalogTransactionIdentity,
    ) -> Result<CatalogTransactionEventIntent> {
        let idempotency_key =
            CatalogEvent::<()>::generate_idempotency_key(T::EVENT_TYPE, T::EVENT_VERSION, payload)
                .map_err(|error| CatalogError::Serialization {
                    message: format!("failed to generate idempotency key: {error}"),
                })?;
        let envelope = CatalogEvent {
            event_type: T::EVENT_TYPE.to_string(),
            event_version: T::EVENT_VERSION,
            idempotency_key,
            occurred_at: Utc::now(),
            source: source.to_string(),
            trace_id: None,
            sequence_position: None,
            payload,
        };
        envelope
            .validate()
            .map_err(|error| CatalogError::InvariantViolation {
                message: format!("invalid event envelope: {error}"),
            })?;
        let event_json =
            String::from_utf8(serde_json::to_vec_pretty(&envelope).map_err(|error| {
                CatalogError::Serialization {
                    message: format!("failed to serialize event: {error}"),
                }
            })?)
            .map_err(|error| CatalogError::Serialization {
                message: format!("catalog event JSON was not UTF-8: {error}"),
            })?;
        let base = self.load_current_catalog_manifest().await?;
        let base_state =
            tier1_state::load_catalog_state(&self.storage, &base.value.snapshot_path).await?;
        let event_value: CatalogEvent<serde_json::Value> = serde_json::from_str(&event_json)
            .map_err(|error| CatalogError::Serialization {
                message: format!("failed to decode catalog transaction event: {error}"),
            })?;
        let event_semantics = identity.reviewed_request.validate_event_realization(
            &event_value.event_type,
            event_value.event_version,
            &event_value.payload,
            &base_state,
            &identity.tenant_id,
            &identity.workspace_id,
        )?;
        let base_manifest_id = base.value.manifest_id.clone();
        let base_watermark = base
            .value
            .watermark_event_id
            .as_deref()
            .map(str::parse::<EventId>)
            .transpose()
            .map_err(|_| CatalogError::InvariantViolation {
                message: "catalog manifest has an invalid watermark event ID".to_string(),
            })?;
        let event_id = EventId::generate_after(base_watermark).map_err(CatalogError::from)?;
        let mut intent = CatalogTransactionEventIntent {
            record_type: CATALOG_TRANSACTION_INTENT_RECORD_TYPE.to_string(),
            version: CATALOG_TRANSACTION_INTENT_VERSION,
            tx_id: identity.tx_id.clone(),
            request_hash: identity.request_hash.clone(),
            base_manifest_path: CatalogPaths::domain_manifest_snapshot(
                CatalogDomain::Catalog,
                &base_manifest_id,
            ),
            base_manifest_id,
            base_manifest_sha256: compute_manifest_hash(&base.bytes),
            event_binding_sha256: String::new(),
            source: source.to_string(),
            revision: 1,
            event_ids: vec![event_id.to_string()],
            active_event_id: event_id.to_string(),
            active_event_path: LedgerKey::event(CatalogDomain::Catalog, &event_id.to_string())
                .as_ref()
                .to_string(),
            event_json,
        };
        intent.event_binding_sha256 = Self::catalog_transaction_event_binding(
            identity,
            &intent,
            &event_value,
            &event_semantics,
        )?;
        Ok(intent)
    }

    /// Appends one ledger event through a durable transaction-owned event intent.
    ///
    /// The intent is published before the event, so recovery can address and
    /// recreate an interrupted append without listing the ledger.
    ///
    /// # Errors
    ///
    /// Returns an error when the identity or event is invalid, or when the
    /// durable intent and exact event object cannot be published or reconciled.
    pub(crate) async fn append_ledger_event_for_transaction<
        T: CatalogEventPayload + serde::Serialize + Sync,
    >(
        &self,
        _guard: &LockGuard<dyn StorageBackend>,
        domain: CatalogDomain,
        payload: &T,
        source: &str,
        identity: &CatalogTransactionIdentity,
    ) -> Result<EventId> {
        if domain != CatalogDomain::Catalog {
            return Err(CatalogError::InvariantViolation {
                message: "catalog transaction event intent used for a non-catalog domain"
                    .to_string(),
            });
        }
        let intent_path = Self::catalog_transaction_intent_path(identity)?;
        let candidate = self
            .build_catalog_transaction_event_intent(payload, source, identity)
            .await?;
        let bytes =
            serde_json::to_vec(&candidate).map_err(|error| CatalogError::Serialization {
                message: format!("failed to encode catalog transaction event intent: {error}"),
            })?;
        let intent = match self
            .storage
            .put_raw(
                &intent_path,
                Bytes::from(bytes),
                WritePrecondition::DoesNotExist,
            )
            .await?
        {
            WriteResult::Success { .. } => candidate,
            WriteResult::PreconditionFailed { .. } => {
                let winner = self
                    .load_catalog_transaction_intent(identity)
                    .await?
                    .ok_or_else(|| CatalogError::InvariantViolation {
                        message: "catalog transaction event intent disappeared".to_string(),
                    })?
                    .value;
                Self::validate_catalog_transaction_intent_collision(&candidate, &winner)?;
                winner
            }
        };
        self.ensure_catalog_transaction_event(&intent).await?;
        intent
            .active_event_id
            .parse::<EventId>()
            .map_err(|_| CatalogError::InvariantViolation {
                message: "catalog transaction event intent has an invalid active event ID"
                    .to_string(),
            })
    }

    pub(crate) async fn has_catalog_transaction_intent(
        &self,
        identity: &CatalogTransactionIdentity,
    ) -> Result<bool> {
        let path = Self::catalog_transaction_intent_path(identity)?;
        Ok(self.storage.head_raw(&path).await?.is_some())
    }

    async fn load_stable_catalog_manifest_pointer(
        &self,
    ) -> Result<(DomainManifestPointer, String)> {
        let path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let metadata_before = self.storage.head_raw(&path).await?.ok_or_else(|| {
            CatalogError::InvariantViolation {
                message: "catalog manifest pointer is missing during transaction recovery"
                    .to_string(),
            }
        })?;
        let bytes = self.storage.get_raw(&path).await?;
        let metadata_after = self.storage.head_raw(&path).await?.ok_or_else(|| {
            CatalogError::InvariantViolation {
                message: "catalog manifest pointer disappeared during transaction recovery"
                    .to_string(),
            }
        })?;
        if metadata_before.version != metadata_after.version {
            return Err(CatalogError::PreconditionFailed {
                message: "catalog manifest pointer changed during transaction recovery".to_string(),
            });
        }
        let pointer: DomainManifestPointer =
            serde_json::from_slice(bytes.as_ref()).map_err(|_| {
                CatalogError::InvariantViolation {
                    message: "catalog manifest pointer is corrupt during transaction recovery"
                        .to_string(),
                }
            })?;
        if pointer.manifest_path
            != CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, &pointer.manifest_id)
        {
            return Err(CatalogError::InvariantViolation {
                message: "catalog manifest pointer names a non-canonical manifest".to_string(),
            });
        }
        Ok((pointer, metadata_after.version))
    }

    async fn load_stable_catalog_manifest(&self, path: &str) -> Result<StableCatalogManifest> {
        let metadata_before =
            self.storage
                .head_raw(path)
                .await?
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "catalog manifest is missing during transaction recovery".to_string(),
                })?;
        let bytes = self.storage.get_raw(path).await?;
        let metadata_after =
            self.storage
                .head_raw(path)
                .await?
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "catalog manifest disappeared during transaction recovery".to_string(),
                })?;
        if metadata_before.version != metadata_after.version {
            return Err(CatalogError::PreconditionFailed {
                message: "catalog manifest changed during transaction recovery".to_string(),
            });
        }
        let value: CatalogDomainManifest =
            serde_json::from_slice(bytes.as_ref()).map_err(|_| {
                CatalogError::InvariantViolation {
                    message: "catalog manifest is corrupt during transaction recovery".to_string(),
                }
            })?;
        if path
            != CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, &value.manifest_id)
        {
            return Err(CatalogError::InvariantViolation {
                message: "catalog manifest chain contains a non-canonical path".to_string(),
            });
        }
        Ok(StableCatalogManifest {
            value,
            bytes,
            version: metadata_after.version,
        })
    }

    async fn load_current_catalog_manifest(&self) -> Result<StableCatalogManifest> {
        Ok(self.load_selected_catalog_manifest().await?.0)
    }

    async fn load_selected_catalog_manifest(&self) -> Result<(StableCatalogManifest, String)> {
        let (pointer, pointer_version) = self.load_stable_catalog_manifest_pointer().await?;
        let stored = self
            .load_stable_catalog_manifest(&pointer.manifest_path)
            .await?;
        if stored.value.manifest_id != pointer.manifest_id || stored.value.epoch != pointer.epoch {
            return Err(CatalogError::InvariantViolation {
                message: "catalog pointer and immutable base manifest diverge".to_string(),
            });
        }
        Ok((stored, pointer_version))
    }

    async fn load_selected_catalog_commit_history(
        &self,
    ) -> Result<SelectedCatalogCommitHistoryAvailability> {
        let (current, pointer_version) = self.load_selected_catalog_manifest().await?;
        let Some(snapshot) = current.value.snapshot.as_ref() else {
            return if current.value.snapshot_version == 0
                && current
                    .value
                    .snapshot_path
                    .split('/')
                    .any(|segment| segment == "v0")
            {
                Ok(SelectedCatalogCommitHistoryAvailability::Unpublished)
            } else {
                Ok(SelectedCatalogCommitHistoryAvailability::RequiresHistoryScan)
            };
        };
        if snapshot.version != current.value.snapshot_version
            || snapshot.path != current.value.snapshot_path
        {
            return Err(CatalogError::InvariantViolation {
                message: "selected catalog snapshot metadata diverges from its manifest"
                    .to_string(),
            });
        }
        let mut commit_files = snapshot
            .files
            .iter()
            .filter(|file| file.path == "commits.parquet");
        let Some(commit_file) = commit_files.next() else {
            return Ok(SelectedCatalogCommitHistoryAvailability::RequiresHistoryScan);
        };
        if commit_files.next().is_some() {
            return Err(CatalogError::InvariantViolation {
                message: "selected catalog snapshot contains duplicate commit history".to_string(),
            });
        }
        let commit_path =
            StateKey::snapshot_file_in_dir(&current.value.snapshot_path, "commits.parquet");
        let commit_bytes = self.storage.get_raw(commit_path.as_ref()).await?;
        if u64::try_from(commit_bytes.len()).ok() != Some(commit_file.byte_size)
            || hex::encode(Sha256::digest(&commit_bytes)) != commit_file.checksum_sha256
        {
            return Err(CatalogError::InvariantViolation {
                message: "selected catalog commit history fails its manifest checksum".to_string(),
            });
        }
        let commits = crate::parquet_util::read_commits(&commit_bytes)?;
        if u64::try_from(commits.len()).ok() != Some(commit_file.row_count) {
            return Err(CatalogError::InvariantViolation {
                message: "selected catalog commit history fails its manifest row count".to_string(),
            });
        }
        Ok(SelectedCatalogCommitHistoryAvailability::Available(
            Box::new(SelectedCatalogCommitHistory {
                current,
                pointer_version,
                commits,
            }),
        ))
    }

    async fn validate_selected_catalog_transaction_commit(
        &self,
        intent: &CatalogTransactionEventIntent,
        history: &SelectedCatalogCommitHistory,
        commit: &crate::parquet_util::CatalogCommitRecord,
        witness: &CatalogCommitEventWitness,
    ) -> Result<SelectedCatalogTransactionPublication> {
        let Some(manifest_id) = commit.manifest_id.as_deref() else {
            return Ok(SelectedCatalogTransactionPublication::RequiresHistoryScan);
        };
        parse_manifest_id(manifest_id).map_err(|_| CatalogError::InvariantViolation {
            message: "selected catalog commit names a non-canonical manifest".to_string(),
        })?;
        let manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, manifest_id);
        let stored = if manifest_path
            == CatalogPaths::domain_manifest_snapshot(
                CatalogDomain::Catalog,
                &history.current.value.manifest_id,
            ) {
            StableCatalogManifest {
                value: history.current.value.clone(),
                bytes: history.current.bytes.clone(),
                version: history.current.version.clone(),
            }
        } else {
            self.load_stable_catalog_manifest(&manifest_path).await?
        };
        let manifest = &stored.value;
        let snapshot_version = i64::try_from(manifest.snapshot_version).map_err(|_| {
            CatalogError::InvariantViolation {
                message: "catalog publication snapshot version exceeds commit history".to_string(),
            }
        })?;
        let fencing_token = manifest
            .fencing_token
            .and_then(|token| i64::try_from(token).ok());
        let watermark_event_id = commit.watermark_event_id.as_deref().ok_or_else(|| {
            CatalogError::InvariantViolation {
                message: "selected catalog transaction commit has no watermark event ID"
                    .to_string(),
            }
        })?;
        if manifest.manifest_id != manifest_id
            || snapshot_version != commit.snapshot_version
            || manifest.updated_at.timestamp_millis() != commit.published_at
            || fencing_token != Some(commit.fencing_token)
            || i64::try_from(manifest.epoch).ok() != Some(commit.fencing_token)
            || manifest.watermark_event_id.as_deref() != Some(watermark_event_id)
            || manifest.last_commit_id.as_deref() != Some(commit.commit_ulid.as_str())
            || manifest.commit_ulid.as_deref() != Some(commit.commit_ulid.as_str())
        {
            return Err(CatalogError::InvariantViolation {
                message: "selected catalog commit diverges from immutable publication authority"
                    .to_string(),
            });
        }
        if !intent.event_ids.contains(&witness.event_id)
            || witness.event_sha256 != sha256_prefixed(intent.event_json.as_bytes())
        {
            return Err(CatalogError::InvariantViolation {
                message: "published catalog transaction event differs from its intent".to_string(),
            });
        }
        let is_current_pointer = manifest_id == history.current.value.manifest_id;
        let authority_version = if is_current_pointer {
            history.pointer_version.clone()
        } else {
            stored.version
        };
        Ok(SelectedCatalogTransactionPublication::Published(Box::new(
            PublishedCatalogTransactionEvent {
                event_id: witness.event_id.clone(),
                manifest: stored.value,
                authority_version,
                is_current_pointer,
            },
        )))
    }

    async fn find_selected_catalog_transaction_event(
        &self,
        intent: &CatalogTransactionEventIntent,
    ) -> Result<SelectedCatalogTransactionPublication> {
        let history = match self.load_selected_catalog_commit_history().await? {
            SelectedCatalogCommitHistoryAvailability::Available(history) => *history,
            SelectedCatalogCommitHistoryAvailability::Unpublished => {
                return Ok(SelectedCatalogTransactionPublication::Unpublished);
            }
            SelectedCatalogCommitHistoryAvailability::RequiresHistoryScan => {
                return Ok(SelectedCatalogTransactionPublication::RequiresHistoryScan);
            }
        };
        let mut witnessed_match = None;
        let mut legacy_match = false;
        for (commit_index, commit) in history.commits.iter().enumerate() {
            if let Some(encoded) = commit.event_witnesses_json.as_deref() {
                let witnesses = decode_catalog_commit_event_witnesses(encoded)?;
                if commit.watermark_event_id.as_deref()
                    != witnesses.last().map(|witness| witness.event_id.as_str())
                {
                    return Err(CatalogError::InvariantViolation {
                        message: "catalog commit event witnesses diverge from the commit watermark"
                            .to_string(),
                    });
                }
                for witness in witnesses {
                    if intent.event_ids.contains(&witness.event_id) {
                        if witnessed_match.is_some() || legacy_match {
                            return Err(CatalogError::InvariantViolation {
                                message:
                                    "catalog transaction intent has multiple selected publications"
                                        .to_string(),
                            });
                        }
                        witnessed_match = Some((commit_index, witness));
                    }
                }
            } else if commit
                .watermark_event_id
                .as_ref()
                .is_some_and(|event_id| intent.event_ids.contains(event_id))
            {
                if witnessed_match.is_some() || legacy_match {
                    return Err(CatalogError::InvariantViolation {
                        message: "catalog transaction intent has multiple selected publications"
                            .to_string(),
                    });
                }
                legacy_match = true;
            }
        }
        if let Some((commit_index, witness)) = witnessed_match {
            let commit = history.commits.get(commit_index).ok_or_else(|| {
                CatalogError::InvariantViolation {
                    message: "selected catalog commit witness has no commit row".to_string(),
                }
            })?;
            return self
                .validate_selected_catalog_transaction_commit(intent, &history, commit, &witness)
                .await;
        }
        if legacy_match {
            Ok(SelectedCatalogTransactionPublication::RequiresHistoryScan)
        } else {
            Ok(SelectedCatalogTransactionPublication::Unpublished)
        }
    }

    async fn find_published_catalog_transaction_event_in_history(
        &self,
        intent: &CatalogTransactionEventIntent,
    ) -> Result<Option<PublishedCatalogTransactionEvent>> {
        let (pointer, pointer_version) = self.load_stable_catalog_manifest_pointer().await?;
        let mut path = pointer.manifest_path.clone();
        let mut visited = HashSet::new();
        let mut successor: Option<CatalogDomainManifest> = None;
        loop {
            if !visited.insert(path.clone()) {
                return Err(CatalogError::InvariantViolation {
                    message: "catalog manifest chain cycles during transaction recovery"
                        .to_string(),
                });
            }
            let stored = self.load_stable_catalog_manifest(&path).await?;
            let manifest = stored.value;
            if let Some(successor) = &successor {
                successor
                    .validate_succession(&manifest, &compute_manifest_hash(&stored.bytes))
                    .map_err(|message| CatalogError::InvariantViolation {
                        message: format!(
                            "catalog manifest chain is invalid during transaction recovery: {message}"
                        ),
                    })?;
            } else if manifest.manifest_id != pointer.manifest_id || manifest.epoch != pointer.epoch
            {
                return Err(CatalogError::InvariantViolation {
                    message:
                        "catalog pointer and immutable manifest diverge during transaction recovery"
                            .to_string(),
                });
            }
            if let Some(event_id) = manifest
                .watermark_event_id
                .as_ref()
                .filter(|event_id| intent.event_ids.contains(event_id))
            {
                let event_path = LedgerKey::event(CatalogDomain::Catalog, event_id)
                    .as_ref()
                    .to_string();
                let event_bytes = self.storage.get_raw(&event_path).await?;
                if event_bytes.as_ref() != intent.event_json.as_bytes() {
                    return Err(CatalogError::InvariantViolation {
                        message: "published catalog transaction event differs from its intent"
                            .to_string(),
                    });
                }
                let is_current_pointer = path == pointer.manifest_path;
                let authority_version = if is_current_pointer {
                    pointer_version.clone()
                } else {
                    stored.version
                };
                return Ok(Some(PublishedCatalogTransactionEvent {
                    event_id: event_id.clone(),
                    manifest,
                    authority_version,
                    is_current_pointer,
                }));
            }
            if path == intent.base_manifest_path {
                return Ok(None);
            }
            let Some(previous) = manifest.previous_manifest_path.clone() else {
                return Err(CatalogError::InvariantViolation {
                    message: "catalog manifest chain does not reach the transaction intent base"
                        .to_string(),
                });
            };
            successor = Some(manifest);
            path = previous;
        }
    }

    async fn find_published_catalog_transaction_event(
        &self,
        intent: &CatalogTransactionEventIntent,
    ) -> Result<Option<PublishedCatalogTransactionEvent>> {
        match self.find_selected_catalog_transaction_event(intent).await? {
            SelectedCatalogTransactionPublication::Published(published) => Ok(Some(*published)),
            SelectedCatalogTransactionPublication::Unpublished => Ok(None),
            SelectedCatalogTransactionPublication::RequiresHistoryScan => {
                self.find_published_catalog_transaction_event_in_history(intent)
                    .await
            }
        }
    }

    pub(crate) async fn inspect_catalog_transaction_event(
        &self,
        identity: &CatalogTransactionIdentity,
    ) -> Result<CatalogTransactionEventInspection> {
        let intent_path = Self::catalog_transaction_intent_path(identity)?;
        for _ in 0..8 {
            let (pointer_before, pointer_version_before) =
                self.load_stable_catalog_manifest_pointer().await?;
            let intent = self
                .load_catalog_transaction_intent(identity)
                .await?
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "catalog transaction event intent is missing".to_string(),
                })?;
            if let Some(published) = self
                .find_published_catalog_transaction_event(&intent.value)
                .await?
            {
                return Ok(CatalogTransactionEventInspection::Published(Box::new(
                    published,
                )));
            }
            let (pointer_after, pointer_version_after) =
                self.load_stable_catalog_manifest_pointer().await?;
            let intent_version_after = self
                .storage
                .head_raw(&intent_path)
                .await?
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "catalog transaction event intent disappeared".to_string(),
                })?
                .version;
            if pointer_before.manifest_id == pointer_after.manifest_id
                && pointer_version_before == pointer_version_after
                && intent.version == intent_version_after
            {
                return Ok(CatalogTransactionEventInspection::Unpublished(
                    CatalogTransactionRecoveryInspection {
                        intent_version: intent.version,
                        head_manifest_id: pointer_after.manifest_id,
                        pointer_version: pointer_version_after,
                    },
                ));
            }
        }
        Err(CatalogError::PreconditionFailed {
            message: "catalog transaction publication inspection did not stabilize".to_string(),
        })
    }

    pub(crate) async fn validate_catalog_transaction_publication(
        &self,
        identity: &CatalogTransactionIdentity,
        publication: &CatalogTransactionPublication<'_>,
    ) -> Result<chrono::DateTime<Utc>> {
        let intent = self
            .load_catalog_transaction_intent(identity)
            .await?
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "catalog transaction event intent is missing".to_string(),
            })?;
        if !intent
            .value
            .event_ids
            .iter()
            .any(|candidate| candidate == publication.event_id)
        {
            return Err(CatalogError::InvariantViolation {
                message: "catalog transaction receipt names an event outside its intent"
                    .to_string(),
            });
        }
        let published = self
            .find_published_catalog_transaction_event(&intent.value)
            .await?
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "catalog transaction event has no immutable manifest authority"
                    .to_string(),
            })?;
        let manifest = &published.manifest;
        if published.event_id != publication.event_id
            || manifest.manifest_id != publication.manifest_id
            || manifest.snapshot_version != publication.snapshot_version
            || manifest.last_commit_id.as_deref() != Some(publication.commit_id)
            || manifest.commit_ulid.as_deref() != Some(publication.commit_id)
            || manifest.fencing_token != Some(publication.fencing_token)
            || manifest.epoch != publication.fencing_token
            || published.is_current_pointer
                && published.authority_version != publication.authority_version
        {
            return Err(CatalogError::InvariantViolation {
                message:
                    "catalog transaction receipt diverges from immutable publication authority"
                        .to_string(),
            });
        }
        Ok(manifest.updated_at)
    }

    async fn rebind_catalog_transaction_intent_base(
        &self,
        identity: &CatalogTransactionIdentity,
        current: &StableCatalogManifest,
        current_manifest_sha256: &str,
        intent: &mut CatalogTransactionEventIntent,
    ) -> Result<()> {
        let event = Self::validate_catalog_transaction_intent_envelope(identity, None, intent)?;
        let current_state =
            tier1_state::load_catalog_state(&self.storage, &current.value.snapshot_path).await?;
        let event_semantics = identity.reviewed_request.validate_event_realization(
            &event.event_type,
            event.event_version,
            &event.payload,
            &current_state,
            &identity.tenant_id,
            &identity.workspace_id,
        )?;
        intent
            .base_manifest_id
            .clone_from(&current.value.manifest_id);
        intent.base_manifest_path = CatalogPaths::domain_manifest_snapshot(
            CatalogDomain::Catalog,
            &current.value.manifest_id,
        );
        intent.base_manifest_sha256 = current_manifest_sha256.to_string();
        intent.event_binding_sha256 =
            Self::catalog_transaction_event_binding(identity, intent, &event, &event_semantics)?;
        Ok(())
    }

    fn advance_catalog_transaction_intent_event(
        intent: &mut CatalogTransactionEventIntent,
        watermark: EventId,
    ) -> Result<()> {
        let next = EventId::generate_after(Some(watermark)).map_err(CatalogError::from)?;
        intent.revision =
            intent
                .revision
                .checked_add(1)
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "catalog transaction event intent revision overflowed".to_string(),
                })?;
        intent.event_ids.push(next.to_string());
        intent.active_event_id = next.to_string();
        intent.active_event_path = LedgerKey::event(CatalogDomain::Catalog, &next.to_string())
            .as_ref()
            .to_string();
        Ok(())
    }

    async fn reconcile_catalog_transaction_intent_for_current_base(
        &self,
        identity: &CatalogTransactionIdentity,
        stored: VersionedCatalogTransactionIntent,
        current: &StableCatalogManifest,
    ) -> Result<Option<CatalogTransactionEventIntent>> {
        let active = stored
            .value
            .active_event_id
            .parse::<EventId>()
            .map_err(|_| CatalogError::InvariantViolation {
                message: "catalog transaction event intent has invalid active event ID".to_string(),
            })?;
        let watermark = current
            .value
            .watermark_event_id
            .as_deref()
            .map(str::parse::<EventId>)
            .transpose()
            .map_err(|_| CatalogError::InvariantViolation {
                message: "catalog manifest has an invalid watermark event ID".to_string(),
            })?;
        let current_manifest_sha256 = compute_manifest_hash(&current.bytes);
        let base_changed = stored.value.base_manifest_id != current.value.manifest_id
            || stored.value.base_manifest_path
                != CatalogPaths::domain_manifest_snapshot(
                    CatalogDomain::Catalog,
                    &current.value.manifest_id,
                )
            || stored.value.base_manifest_sha256 != current_manifest_sha256;
        let event_id_is_stale = watermark.is_some_and(|watermark| active <= watermark);
        if !base_changed && !event_id_is_stale {
            return Ok(Some(stored.value));
        }

        let mut next_intent = stored.value;
        if base_changed {
            self.rebind_catalog_transaction_intent_base(
                identity,
                current,
                &current_manifest_sha256,
                &mut next_intent,
            )
            .await?;
        }
        if event_id_is_stale {
            Self::advance_catalog_transaction_intent_event(
                &mut next_intent,
                watermark.ok_or_else(|| CatalogError::InvariantViolation {
                    message: "stale catalog transaction event has no manifest watermark"
                        .to_string(),
                })?,
            )?;
        }
        let bytes =
            serde_json::to_vec(&next_intent).map_err(|error| CatalogError::Serialization {
                message: format!(
                    "failed to encode revised catalog transaction event intent: {error}"
                ),
            })?;
        let path = Self::catalog_transaction_intent_path(identity)?;
        match self
            .storage
            .put_raw(
                &path,
                Bytes::from(bytes),
                WritePrecondition::MatchesVersion(stored.version),
            )
            .await?
        {
            WriteResult::Success { .. } => Ok(Some(next_intent)),
            WriteResult::PreconditionFailed { .. } => Ok(None),
        }
    }

    #[cfg(test)]
    pub(crate) async fn recover_catalog_transaction_event(
        &self,
        identity: &CatalogTransactionIdentity,
    ) -> Result<CatalogTransactionEventRecovery> {
        self.recover_catalog_transaction_event_inner(identity, None)
            .await
    }

    pub(crate) async fn recover_catalog_transaction_event_after_inspection(
        &self,
        identity: &CatalogTransactionIdentity,
        inspection: &CatalogTransactionRecoveryInspection,
    ) -> Result<CatalogTransactionEventRecovery> {
        self.recover_catalog_transaction_event_inner(identity, Some(inspection))
            .await
    }

    async fn recover_catalog_transaction_event_inner(
        &self,
        identity: &CatalogTransactionIdentity,
        inspection: Option<&CatalogTransactionRecoveryInspection>,
    ) -> Result<CatalogTransactionEventRecovery> {
        for _ in 0..8 {
            let stored = self
                .load_catalog_transaction_intent(identity)
                .await?
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "catalog transaction event intent is missing".to_string(),
                })?;
            match self
                .find_selected_catalog_transaction_event(&stored.value)
                .await?
            {
                SelectedCatalogTransactionPublication::Published(published) => {
                    return Ok(CatalogTransactionEventRecovery::Published(published));
                }
                SelectedCatalogTransactionPublication::Unpublished => {}
                SelectedCatalogTransactionPublication::RequiresHistoryScan => {
                    let Some(inspection) = inspection else {
                        return Ok(CatalogTransactionEventRecovery::RetryUnlocked);
                    };
                    let (pointer, pointer_version) =
                        self.load_stable_catalog_manifest_pointer().await?;
                    if stored.version != inspection.intent_version
                        || pointer.manifest_id != inspection.head_manifest_id
                        || pointer_version != inspection.pointer_version
                    {
                        return Ok(CatalogTransactionEventRecovery::RetryUnlocked);
                    }
                }
            }
            let current = self.load_current_catalog_manifest().await?;
            let Some(intent) = self
                .reconcile_catalog_transaction_intent_for_current_base(identity, stored, &current)
                .await?
            else {
                continue;
            };
            self.ensure_catalog_transaction_event(&intent).await?;
            return Ok(CatalogTransactionEventRecovery::Ready(
                intent.active_event_id.parse::<EventId>().map_err(|_| {
                    CatalogError::InvariantViolation {
                        message: "catalog transaction recovery event ID is invalid".to_string(),
                    }
                })?,
            ));
        }
        Err(CatalogError::PreconditionFailed {
            message: "catalog transaction event intent did not converge".to_string(),
        })
    }

    /// Appends a ledger event for a Tier-1 DDL operation (ADR-018).
    ///
    /// This is the new flow for Tier-1 operations where API only appends events
    /// to the ledger and the compactor is responsible for writing Parquet and
    /// updating manifests.
    ///
    /// # Flow
    ///
    /// 1. API holds distributed lock
    /// 2. API calls this method to append the DDL event
    /// 3. API calls compactor sync RPC with explicit event paths
    /// 4. Compactor writes Parquet + publishes manifest
    /// 5. API releases lock
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or storage fails.
    pub async fn append_ledger_event<T: CatalogEventPayload + serde::Serialize + Sync>(
        &self,
        guard: &LockGuard<dyn StorageBackend>,
        domain: CatalogDomain,
        payload: &T,
        source: &str,
    ) -> Result<EventId> {
        self.append_ledger_event_after(guard, domain, payload, source, None)
            .await
    }

    /// Append a ledger event that must sort after a previous same-lock event.
    ///
    /// Multi-event mutations compact by lexicographic event ID. Threading the
    /// previously allocated ID through the batch keeps event application order
    /// deterministic even when the visible manifest watermark has not advanced.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or storage fails.
    pub async fn append_ledger_event_after<T: CatalogEventPayload + serde::Serialize + Sync>(
        &self,
        _guard: &LockGuard<dyn StorageBackend>,
        domain: CatalogDomain,
        payload: &T,
        source: &str,
        previous_event_id: Option<EventId>,
    ) -> Result<EventId> {
        let idempotency_key =
            CatalogEvent::<()>::generate_idempotency_key(T::EVENT_TYPE, T::EVENT_VERSION, payload)
                .map_err(|e| CatalogError::Serialization {
                    message: format!("failed to generate idempotency key: {e}"),
                })?;

        let envelope = CatalogEvent {
            event_type: T::EVENT_TYPE.to_string(),
            event_version: T::EVENT_VERSION,
            idempotency_key: idempotency_key.clone(),
            occurred_at: Utc::now(),
            source: source.to_string(),
            trace_id: None,
            sequence_position: None, // Tier-1 doesn't need sequence positions
            payload,
        };

        envelope
            .validate()
            .map_err(|e| CatalogError::InvariantViolation {
                message: format!("invalid event envelope: {e}"),
            })?;

        let json =
            serde_json::to_vec_pretty(&envelope).map_err(|e| CatalogError::Serialization {
                message: format!("failed to serialize event: {e}"),
            })?;

        let watermark = self.current_watermark_event_id(domain).await?;
        let floor = match (watermark, previous_event_id) {
            (Some(watermark), Some(previous_event_id)) => Some(watermark.max(previous_event_id)),
            (Some(watermark), None) => Some(watermark),
            (None, Some(previous_event_id)) => Some(previous_event_id),
            (None, None) => None,
        };
        let mut event_id = EventId::generate_after(floor).map_err(CatalogError::from)?;
        for _ in 0..1024 {
            let key = LedgerKey::event(domain, &event_id.to_string());
            match self
                .storage
                .put_ledger(&key, Bytes::from(json.clone()))
                .await?
            {
                WriteResult::Success { .. } => return Ok(event_id),
                WriteResult::PreconditionFailed { .. } => {
                    if self
                        .existing_ledger_event_matches(
                            key.as_ref(),
                            T::EVENT_TYPE,
                            T::EVENT_VERSION,
                            &idempotency_key,
                        )
                        .await?
                    {
                        tracing::debug!(
                            event_id = %event_id,
                            "duplicate ledger event matches idempotency key"
                        );
                        return Ok(event_id);
                    }
                    event_id =
                        EventId::generate_after(Some(event_id)).map_err(CatalogError::from)?;
                }
            }
        }

        Err(CatalogError::InvariantViolation {
            message: "failed to allocate unique ledger event ID after collision retries"
                .to_string(),
        })
    }

    async fn existing_ledger_event_matches(
        &self,
        path: &str,
        event_type: &str,
        event_version: u32,
        idempotency_key: &str,
    ) -> Result<bool> {
        let bytes = self.storage.get_raw(path).await?;
        let existing: CatalogEvent<serde_json::Value> =
            serde_json::from_slice(&bytes).map_err(|e| CatalogError::Serialization {
                message: format!("failed to parse existing ledger event at {path}: {e}"),
            })?;
        Ok(existing.event_type == event_type
            && existing.event_version == event_version
            && existing.idempotency_key == idempotency_key)
    }

    async fn current_watermark_event_id(&self, domain: CatalogDomain) -> Result<Option<EventId>> {
        let manifest = self.read_manifest().await?;
        let watermark = match domain {
            CatalogDomain::Catalog => manifest.catalog.watermark_event_id,
            CatalogDomain::Lineage => manifest.lineage.watermark_event_id,
            CatalogDomain::Search => manifest.search.watermark_event_id,
            CatalogDomain::Executions => None,
        };
        watermark
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(CatalogError::from)
    }

    #[allow(clippy::too_many_lines)]
    async fn update_inner<F>(
        &self,
        guard: &LockGuard<dyn StorageBackend>,
        update_fn: &mut F,
    ) -> Result<CommitRecord>
    where
        F: FnMut(&mut CatalogDomainManifest) -> Result<()>,
    {
        let writer_epoch = guard.fencing_token().sequence();

        for attempt in 1..=self.cas_max_retries {
            let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
            let pointer_meta = self.storage.head_raw(&pointer_path).await?.ok_or_else(|| {
                CatalogError::NotFound {
                    entity: "catalog manifest pointer".to_string(),
                    name: pointer_path.clone(),
                }
            })?;
            let pointer_bytes = self.storage.get_raw(&pointer_path).await?;
            let pointer: DomainManifestPointer =
                serde_json::from_slice(&pointer_bytes).map_err(|e| {
                    CatalogError::Serialization {
                        message: format!("parse JSON at {pointer_path}: {e}"),
                    }
                })?;

            if writer_epoch < pointer.epoch {
                return Err(CatalogError::PreconditionFailed {
                    message: format!(
                        "stale epoch: writer epoch {writer_epoch} is behind pointer epoch {}",
                        pointer.epoch
                    ),
                });
            }

            let pointer_expected_version = Some(pointer_meta.version);
            let pointer_parent_hash = Some(compute_manifest_hash(&pointer_bytes));
            let previous_manifest_path = pointer.manifest_path.clone();
            let prev_bytes = self.storage.get_raw(&pointer.manifest_path).await?;

            let mut catalog: CatalogDomainManifest =
                serde_json::from_slice(&prev_bytes).map_err(|e| CatalogError::Serialization {
                    message: format!("parse JSON at {previous_manifest_path}: {e}"),
                })?;
            let prev_raw_hash = compute_manifest_hash(&prev_bytes);
            let prev_catalog = catalog.clone();

            update_fn(&mut catalog)?;

            catalog.updated_at = Utc::now();
            catalog.parent_hash = Some(prev_raw_hash.clone());
            catalog.fencing_token = Some(writer_epoch);
            catalog.epoch = writer_epoch;
            catalog.previous_manifest_path = Some(previous_manifest_path.clone());
            catalog.writer_session_id = Some(Ulid::new().to_string());
            let commit_ulid = next_commit_ulid(prev_catalog.commit_ulid.as_deref())?;
            catalog.commit_ulid = Some(commit_ulid.clone());
            catalog.manifest_id = next_available_manifest_id(
                &self.storage,
                CatalogDomain::Catalog,
                &prev_catalog.manifest_id,
            )
            .await?;

            catalog
                .validate_succession(&prev_catalog, &prev_raw_hash)
                .map_err(|message| CatalogError::InvariantViolation { message })?;

            let commit = Self::build_commit_record(&prev_catalog, &catalog, &commit_ulid)?;
            catalog.last_commit_id = Some(commit.commit_id.clone());

            let catalog_bytes = json_bytes(&catalog)?;
            let snapshot_manifest_path = CatalogPaths::domain_manifest_snapshot(
                CatalogDomain::Catalog,
                &catalog.manifest_id,
            );

            let pointer = DomainManifestPointer {
                manifest_id: catalog.manifest_id.clone(),
                manifest_path: snapshot_manifest_path.clone(),
                epoch: writer_epoch,
                parent_pointer_hash: pointer_parent_hash.clone(),
                updated_at: Utc::now(),
            };
            let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
            match publish_snapshot_pointer_transaction(
                &self.storage,
                &snapshot_manifest_path,
                catalog_bytes.clone(),
                &pointer_path,
                json_bytes(&pointer)?,
                pointer_expected_version.as_deref(),
                None,
                SnapshotPointerDurability::Visible,
                async { Ok(()) },
            )
            .await
            {
                Ok(SnapshotPointerPublishOutcome::Visible { .. }) => {
                    return Ok(commit);
                }
                Ok(SnapshotPointerPublishOutcome::PersistedNotVisible) => {
                    return Err(CatalogError::InvariantViolation {
                        message:
                            "unexpected persisted-not-visible outcome in visible durability mode"
                                .to_string(),
                    });
                }
                Err(arco_core::Error::PreconditionFailed { .. }) => {
                    if attempt == self.cas_max_retries {
                        return Err(CatalogError::PreconditionFailed {
                            message: "manifest update lost CAS race after max retries".into(),
                        });
                    }
                    crate::metrics::record_cas_retry("catalog_manifest_pointer");
                }
                Err(e) => return Err(CatalogError::from(e)),
            }
        }

        Err(CatalogError::InvariantViolation {
            message: "unreachable: CAS retry loop exhausted".into(),
        })
    }

    async fn ensure_json_exists<T>(&self, path: &str, value: &T) -> Result<()>
    where
        T: serde::Serialize + Sync,
    {
        let bytes = json_bytes(value)?;
        match self
            .storage
            .put_raw(path, bytes, WritePrecondition::DoesNotExist)
            .await?
        {
            WriteResult::PreconditionFailed { .. } | WriteResult::Success { .. } => Ok(()),
        }
    }

    async fn bootstrap_tier1_manifest<T, FManifestId, FEpoch, FSanitize>(
        &self,
        domain: CatalogDomain,
        legacy_root_path: &str,
        default_manifest: &T,
        manifest_id: FManifestId,
        epoch: FEpoch,
        sanitize_legacy_manifest: FSanitize,
    ) -> Result<()>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Sync,
        FManifestId: Fn(&T) -> &str,
        FEpoch: Fn(&T) -> u64,
        FSanitize: Fn(&mut T, &str),
    {
        let pointer_path = CatalogPaths::domain_manifest_pointer(domain);
        if self.storage.head_raw(&pointer_path).await?.is_some() {
            return Ok(());
        }

        let legacy_path = legacy_manifest_candidate_path(domain, legacy_root_path);
        if let Some(legacy_bytes) = self.get_raw_if_exists(&legacy_path).await? {
            let manifest: T =
                serde_json::from_slice(&legacy_bytes).map_err(|e| CatalogError::Serialization {
                    message: format!("parse JSON at {legacy_path}: {e}"),
                })?;
            let mut manifest = manifest;
            let snapshot_manifest_path =
                CatalogPaths::domain_manifest_snapshot(domain, manifest_id(&manifest));
            sanitize_legacy_manifest(&mut manifest, &snapshot_manifest_path);
            let manifest_bytes = json_bytes(&manifest)?;
            match self
                .storage
                .put_raw(
                    &snapshot_manifest_path,
                    manifest_bytes,
                    WritePrecondition::DoesNotExist,
                )
                .await?
            {
                WriteResult::Success { .. } | WriteResult::PreconditionFailed { .. } => {}
            }
            self.ensure_json_exists(
                &pointer_path,
                &DomainManifestPointer {
                    manifest_id: manifest_id(&manifest).to_string(),
                    manifest_path: snapshot_manifest_path,
                    epoch: epoch(&manifest),
                    parent_pointer_hash: None,
                    updated_at: Utc::now(),
                },
            )
            .await?;
            return Ok(());
        }

        self.ensure_json_exists(
            &CatalogPaths::domain_manifest_snapshot(domain, INITIAL_MANIFEST_ID),
            default_manifest,
        )
        .await?;
        self.ensure_json_exists(&pointer_path, &DomainManifestPointer::new(domain))
            .await?;
        Ok(())
    }

    async fn repair_legacy_catalog_manifest_history(&self, writer_epoch: u64) -> Result<()> {
        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let Some(pointer_meta) = self.storage.head_raw(&pointer_path).await? else {
            return Ok(());
        };
        let pointer_bytes = self.storage.get_raw(&pointer_path).await?;
        let pointer: DomainManifestPointer =
            serde_json::from_slice(&pointer_bytes).map_err(|e| CatalogError::Serialization {
                message: format!("parse JSON at {pointer_path}: {e}"),
            })?;
        let current_bytes = self.storage.get_raw(&pointer.manifest_path).await?;
        let current: CatalogDomainManifest =
            serde_json::from_slice(&current_bytes).map_err(|e| CatalogError::Serialization {
                message: format!("parse JSON at {}: {e}", pointer.manifest_path),
            })?;

        if !self
            .catalog_history_reaches_legacy_mutable_head(&pointer.manifest_path, &current)
            .await?
        {
            return Ok(());
        }

        let mut repaired = current;
        repaired.manifest_id =
            next_available_manifest_id(&self.storage, CatalogDomain::Catalog, &pointer.manifest_id)
                .await?;
        repaired.previous_manifest_path = None;
        repaired.parent_hash = None;
        repaired.epoch = writer_epoch;
        repaired.fencing_token = Some(writer_epoch);
        repaired.updated_at = Utc::now();

        let repaired_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, &repaired.manifest_id);
        let repaired_pointer = DomainManifestPointer {
            manifest_id: repaired.manifest_id.clone(),
            manifest_path: repaired_path.clone(),
            epoch: writer_epoch,
            parent_pointer_hash: Some(compute_manifest_hash(&pointer_bytes)),
            updated_at: Utc::now(),
        };
        match publish_snapshot_pointer_transaction(
            &self.storage,
            &repaired_path,
            json_bytes(&repaired)?,
            &pointer_path,
            json_bytes(&repaired_pointer)?,
            Some(pointer_meta.version).as_deref(),
            None,
            SnapshotPointerDurability::Visible,
            async { Ok(()) },
        )
        .await
        {
            Ok(SnapshotPointerPublishOutcome::Visible { .. }) => Ok(()),
            Ok(SnapshotPointerPublishOutcome::PersistedNotVisible) => {
                Err(CatalogError::InvariantViolation {
                    message: "unexpected persisted-not-visible outcome in visible durability mode"
                        .to_string(),
                })
            }
            Err(e) => Err(CatalogError::from(e)),
        }
    }

    async fn catalog_history_reaches_legacy_mutable_head(
        &self,
        current_manifest_path: &str,
        current_manifest: &CatalogDomainManifest,
    ) -> Result<bool> {
        let mut visited = HashSet::new();
        let mut manifest_path = current_manifest_path.to_string();
        let mut manifest = current_manifest.clone();

        visited.insert(manifest_path.clone());

        while let Some(previous_path) = manifest.previous_manifest_path.clone() {
            if is_legacy_domain_manifest_path(CatalogDomain::Catalog, &previous_path)
                || previous_path == manifest_path
            {
                return Ok(true);
            }

            if !visited.insert(previous_path.clone()) {
                return Ok(false);
            }

            let Some(bytes) = self.get_raw_if_exists(&previous_path).await? else {
                return Ok(false);
            };
            manifest = serde_json::from_slice(&bytes).map_err(|e| CatalogError::Serialization {
                message: format!("parse JSON at {previous_path}: {e}"),
            })?;
            manifest_path = previous_path;
        }

        Ok(false)
    }

    async fn read_json<T>(&self, path: &str) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let bytes = self.storage.get_raw(path).await?;
        serde_json::from_slice(&bytes).map_err(|e| CatalogError::Serialization {
            message: format!("parse JSON at {path}: {e}"),
        })
    }

    async fn read_current_domain_manifest<T>(&self, domain: CatalogDomain) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let pointer_path = CatalogPaths::domain_manifest_pointer(domain);
        let pointer_bytes = self.storage.get_raw(&pointer_path).await?;
        let pointer: DomainManifestPointer =
            serde_json::from_slice(&pointer_bytes).map_err(|e| CatalogError::Serialization {
                message: format!("parse JSON at {pointer_path}: {e}"),
            })?;
        self.read_json(&pointer.manifest_path).await
    }

    async fn get_raw_if_exists(&self, path: &str) -> Result<Option<Bytes>> {
        match self.storage.get_raw(path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(arco_core::Error::NotFound(_) | arco_core::Error::ResourceNotFound { .. }) => {
                Ok(None)
            }
            Err(error) => Err(CatalogError::from(error)),
        }
    }

    /// Builds a commit record for an update operation.
    ///
    /// Current Tier-1 writes thread through `last_commit_id` for correlation,
    /// but they do not load or persist durable commit-record objects. The
    /// returned receipt therefore keeps `prev_commit_id` when available and
    /// leaves `prev_commit_hash` unset.
    fn build_commit_record(
        prev: &CatalogDomainManifest,
        next: &CatalogDomainManifest,
        commit_id: &str,
    ) -> Result<CommitRecord> {
        let payload_hash = sha256_prefixed(&json_vec(next)?);
        let prev_commit_id = prev.last_commit_id.clone();

        Ok(CommitRecord {
            commit_id: commit_id.to_string(),
            prev_commit_id,
            prev_commit_hash: None,
            operation: "Update".into(),
            payload_hash,
            created_at: Utc::now(),
        })
    }
}

fn next_commit_ulid(previous: Option<&str>) -> Result<String> {
    let candidate = Ulid::new();

    let Some(previous) = previous else {
        return Ok(candidate.to_string());
    };

    let previous = Ulid::from_string(previous).map_err(|e| CatalogError::InvariantViolation {
        message: format!("invalid previous commit_ulid '{previous}': {e}"),
    })?;

    if candidate > previous {
        return Ok(candidate.to_string());
    }

    let next = previous
        .increment()
        .ok_or_else(|| CatalogError::InvariantViolation {
            message: "commit_ulid overflow while generating monotonic successor".to_string(),
        })?;
    Ok(next.to_string())
}

fn legacy_manifest_candidate_path(domain: CatalogDomain, root_path: &str) -> String {
    if let Some(domain) = root_path
        .strip_prefix("manifests/")
        .and_then(|path| path.strip_suffix(".manifest.json"))
    {
        return CatalogPaths::domain_manifest_str(domain);
    }

    CatalogPaths::domain_manifest(domain)
}

fn sanitize_legacy_bootstrap_history(
    domain: CatalogDomain,
    previous_manifest_path: &mut Option<String>,
    parent_hash: &mut Option<String>,
    snapshot_manifest_path: &str,
) {
    let Some(previous_path) = previous_manifest_path.as_deref() else {
        return;
    };

    if is_legacy_domain_manifest_path(domain, previous_path)
        || previous_path == snapshot_manifest_path
    {
        *previous_manifest_path = None;
        *parent_hash = None;
    }
}

fn is_legacy_domain_manifest_path(domain: CatalogDomain, path: &str) -> bool {
    let Some(domain_name) = path
        .strip_prefix("manifests/")
        .and_then(|path| path.strip_suffix(".manifest.json"))
    else {
        return false;
    };

    CatalogPaths::domain_manifest_str(domain_name) == CatalogPaths::domain_manifest(domain)
}

async fn next_available_manifest_id(
    storage: &ScopedStorage,
    domain: CatalogDomain,
    previous_manifest_id: &str,
) -> Result<String> {
    let mut candidate = next_manifest_id(previous_manifest_id)
        .map_err(|message| CatalogError::InvariantViolation { message })?;
    loop {
        let candidate_path = CatalogPaths::domain_manifest_snapshot(domain, &candidate);
        if storage.head_raw(&candidate_path).await?.is_none() {
            return Ok(candidate);
        }
        candidate = next_manifest_id(&candidate)
            .map_err(|message| CatalogError::InvariantViolation { message })?;
    }
}

fn json_vec<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| CatalogError::Serialization {
        message: format!("serialize JSON: {e}"),
    })
}

fn json_bytes<T: serde::Serialize>(value: &T) -> Result<Bytes> {
    Ok(Bytes::from(json_vec(value)?))
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let hash = Sha256::digest(bytes);
    format!("sha256:{}", hex::encode(hash))
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::tier1_compactor::Tier1Compactor;
    use crate::write_options::WriteOptions;
    use crate::writer::{
        CatalogTransactionRequest, CatalogWriter, RegisterTableInSchemaRequest, TablePatch,
    };
    use arco_core::Result as CoreResult;
    use arco_core::storage::{MemoryBackend, ObjectMeta};
    use serde::de::DeserializeOwned;
    use std::ops::Range;

    fn parse_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        serde_json::from_slice(bytes).map_err(|e| CatalogError::Serialization {
            message: format!("failed to parse json: {e}"),
        })
    }

    #[derive(Debug)]
    struct HookedBackend {
        inner: MemoryBackend,
        inject_once: AtomicBool,
    }

    impl HookedBackend {
        fn new() -> Self {
            Self {
                inner: MemoryBackend::new(),
                inject_once: AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for HookedBackend {
        async fn get(&self, path: &str) -> CoreResult<Bytes> {
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> CoreResult<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> CoreResult<WriteResult> {
            // Inject a no-op write once to force a CAS conflict.
            if matches!(&precondition, WritePrecondition::MatchesVersion(_))
                && path.ends_with(&CatalogPaths::domain_manifest_pointer(
                    CatalogDomain::Catalog,
                ))
                && self.inject_once.swap(false, Ordering::SeqCst)
            {
                let current = self.inner.get(path).await?;
                let mut pointer: DomainManifestPointer =
                    serde_json::from_slice(&current).expect("parse injected pointer");
                pointer.updated_at = Utc::now();
                let _ = self
                    .inner
                    .put(
                        path,
                        Bytes::from(
                            serde_json::to_vec(&pointer).expect("serialize injected pointer"),
                        ),
                        WritePrecondition::None,
                    )
                    .await?;
            }

            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> CoreResult<()> {
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> CoreResult<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> CoreResult<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> CoreResult<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    #[derive(Debug)]
    struct ObservingBackend {
        inner: MemoryBackend,
        unrelated_manifest_gets: AtomicUsize,
        catalog_snapshot_gets: AtomicUsize,
        catalog_lock_active: AtomicBool,
        watched_manifest_suffix: Mutex<Option<String>>,
        watched_manifest_read_while_locked: AtomicBool,
    }

    impl ObservingBackend {
        fn new() -> Self {
            Self {
                inner: MemoryBackend::new(),
                unrelated_manifest_gets: AtomicUsize::new(0),
                catalog_snapshot_gets: AtomicUsize::new(0),
                catalog_lock_active: AtomicBool::new(false),
                watched_manifest_suffix: Mutex::new(None),
                watched_manifest_read_while_locked: AtomicBool::new(false),
            }
        }

        fn reset_io_counts(&self) {
            self.unrelated_manifest_gets.store(0, Ordering::SeqCst);
            self.catalog_snapshot_gets.store(0, Ordering::SeqCst);
        }

        fn unrelated_manifest_gets(&self) -> usize {
            self.unrelated_manifest_gets.load(Ordering::SeqCst)
        }

        fn catalog_snapshot_gets(&self) -> usize {
            self.catalog_snapshot_gets.load(Ordering::SeqCst)
        }

        fn watch_manifest_while_locked(&self, path: String) {
            *self
                .watched_manifest_suffix
                .lock()
                .expect("watch mutex poisoned") = Some(path);
            self.watched_manifest_read_while_locked
                .store(false, Ordering::SeqCst);
        }

        fn watched_manifest_was_read_while_locked(&self) -> bool {
            self.watched_manifest_read_while_locked
                .load(Ordering::SeqCst)
        }

        fn is_unrelated_manifest_path(path: &str) -> bool {
            path.contains("/manifests/lineage")
                || path.contains("/manifests/search")
                || path.ends_with("/manifests/executions.manifest.json")
        }
    }

    #[async_trait]
    impl StorageBackend for ObservingBackend {
        async fn get(&self, path: &str) -> CoreResult<Bytes> {
            if Self::is_unrelated_manifest_path(path) {
                self.unrelated_manifest_gets.fetch_add(1, Ordering::SeqCst);
            }
            if path.contains("snapshots/catalog/") && path.ends_with(".parquet") {
                self.catalog_snapshot_gets.fetch_add(1, Ordering::SeqCst);
            }
            if self.catalog_lock_active.load(Ordering::SeqCst)
                && self
                    .watched_manifest_suffix
                    .lock()
                    .expect("watch mutex poisoned")
                    .as_deref()
                    .is_some_and(|watched| path.ends_with(watched))
            {
                self.watched_manifest_read_while_locked
                    .store(true, Ordering::SeqCst);
            }
            self.inner.get(path).await
        }

        async fn get_range(&self, path: &str, range: Range<u64>) -> CoreResult<Bytes> {
            self.inner.get_range(path, range).await
        }

        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> CoreResult<WriteResult> {
            if path.ends_with(&CatalogPaths::domain_lock(CatalogDomain::Catalog))
                && let Ok(lock) = serde_json::from_slice::<crate::lock::LockInfo>(&data)
            {
                self.catalog_lock_active
                    .store(!lock.is_expired(), Ordering::SeqCst);
            }
            self.inner.put(path, data, precondition).await
        }

        async fn delete(&self, path: &str) -> CoreResult<()> {
            if path.ends_with(&CatalogPaths::domain_lock(CatalogDomain::Catalog)) {
                self.catalog_lock_active.store(false, Ordering::SeqCst);
            }
            self.inner.delete(path).await
        }

        async fn list(&self, prefix: &str) -> CoreResult<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }

        async fn head(&self, path: &str) -> CoreResult<Option<ObjectMeta>> {
            self.inner.head(path).await
        }

        async fn signed_url(&self, path: &str, expiry: Duration) -> CoreResult<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    async fn append_catalog_manifest_only_successors(
        storage: &ScopedStorage,
        count: usize,
    ) -> Result<Vec<String>> {
        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let pointer_bytes = storage.get_raw(&pointer_path).await?;
        let pointer: DomainManifestPointer = parse_json(&pointer_bytes)?;
        let mut previous_path = pointer.manifest_path;
        let mut previous_bytes = storage.get_raw(&previous_path).await?;
        let mut previous: CatalogDomainManifest = parse_json(&previous_bytes)?;
        let mut watermark = previous
            .watermark_event_id
            .as_deref()
            .map(str::parse::<EventId>)
            .transpose()
            .map_err(|_| CatalogError::InvariantViolation {
                message: "test catalog manifest has an invalid watermark".to_string(),
            })?;
        let mut paths = Vec::with_capacity(count);

        for _ in 0..count {
            let manifest_id = next_manifest_id(&previous.manifest_id)
                .map_err(|message| CatalogError::InvariantViolation { message })?;
            let event_id = EventId::generate_after(watermark).map_err(CatalogError::from)?;
            let path = CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, &manifest_id);
            let mut successor = previous.clone();
            successor.manifest_id = manifest_id;
            successor.previous_manifest_path = Some(previous_path.clone());
            successor.parent_hash = Some(compute_manifest_hash(&previous_bytes));
            successor.watermark_event_id = Some(event_id.to_string());
            successor.last_commit_id = None;
            successor.commit_ulid = None;
            successor.updated_at = Utc::now();
            let bytes = Bytes::from(serde_json::to_vec(&successor).map_err(|error| {
                CatalogError::Serialization {
                    message: format!("encode test catalog manifest: {error}"),
                }
            })?);
            let write = storage
                .put_raw(&path, bytes.clone(), WritePrecondition::DoesNotExist)
                .await?;
            if !matches!(write, WriteResult::Success { .. }) {
                return Err(CatalogError::InvariantViolation {
                    message: "test catalog manifest path unexpectedly existed".to_string(),
                });
            }
            paths.push(path.clone());
            watermark = Some(event_id);
            previous_path = path;
            previous_bytes = bytes;
            previous = successor;
        }

        let pointer = DomainManifestPointer {
            manifest_id: previous.manifest_id,
            manifest_path: previous_path,
            epoch: previous.epoch,
            parent_pointer_hash: Some(compute_manifest_hash(&pointer_bytes)),
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                &pointer_path,
                Bytes::from(serde_json::to_vec(&pointer).map_err(|error| {
                    CatalogError::Serialization {
                        message: format!("encode test catalog pointer: {error}"),
                    }
                })?),
                WritePrecondition::None,
            )
            .await?;
        Ok(paths)
    }

    async fn rewrite_catalog_commit_for_test(
        storage: &ScopedStorage,
        event_id: &str,
        rewrite: impl FnOnce(&mut crate::parquet_util::CatalogCommitRecord) -> Result<()>,
    ) -> Result<()> {
        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let pointer: DomainManifestPointer = parse_json(&storage.get_raw(&pointer_path).await?)?;
        let manifest_path = pointer.manifest_path;
        let mut manifest: CatalogDomainManifest =
            parse_json(&storage.get_raw(&manifest_path).await?)?;
        let commit_path =
            StateKey::snapshot_file_in_dir(&manifest.snapshot_path, "commits.parquet");
        let mut commits =
            crate::parquet_util::read_commits(&storage.get_raw(commit_path.as_ref()).await?)?;
        let commit = commits
            .iter_mut()
            .find(|commit| {
                commit.watermark_event_id.as_deref() == Some(event_id)
                    || commit
                        .event_witnesses_json
                        .as_deref()
                        .and_then(|encoded| decode_catalog_commit_event_witnesses(encoded).ok())
                        .is_some_and(|witnesses| {
                            witnesses.iter().any(|witness| witness.event_id == event_id)
                        })
            })
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "test catalog transaction commit is missing".to_string(),
            })?;
        rewrite(commit)?;
        let commit_bytes = crate::parquet_util::write_commits(&commits)?;
        storage
            .put_raw(
                commit_path.as_ref(),
                commit_bytes.clone(),
                WritePrecondition::None,
            )
            .await?;
        let snapshot =
            manifest
                .snapshot
                .as_mut()
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "test catalog manifest has no snapshot metadata".to_string(),
                })?;
        let commit_file = snapshot
            .files
            .iter_mut()
            .find(|file| file.path == "commits.parquet")
            .ok_or_else(|| CatalogError::InvariantViolation {
                message: "test catalog snapshot has no commit file".to_string(),
            })?;
        commit_file.checksum_sha256 = hex::encode(Sha256::digest(&commit_bytes));
        commit_file.byte_size =
            u64::try_from(commit_bytes.len()).map_err(|_| CatalogError::InvariantViolation {
                message: "test catalog commit file is too large".to_string(),
            })?;
        storage
            .put_raw(
                &manifest_path,
                Bytes::from(serde_json::to_vec(&manifest).map_err(|error| {
                    CatalogError::Serialization {
                        message: format!("encode test catalog manifest: {error}"),
                    }
                })?),
                WritePrecondition::None,
            )
            .await?;
        Ok(())
    }

    async fn strip_catalog_commit_manifest_witness(
        storage: &ScopedStorage,
        event_id: &str,
    ) -> Result<()> {
        rewrite_catalog_commit_for_test(storage, event_id, |commit| {
            commit.manifest_id = None;
            Ok(())
        })
        .await
    }

    fn catalog_transaction_options(identity: &CatalogTransactionIdentity) -> WriteOptions {
        WriteOptions::default()
            .with_request_id(identity.request_id.clone())
            .with_idempotency_key(identity.idempotency_key.clone())
            .with_transaction_identity(identity.clone())
    }

    async fn catalog_transaction_intent_fixture(
        writer: &Tier1Writer,
    ) -> Result<(CatalogTransactionIdentity, CatalogTransactionEventIntent)> {
        let reviewed_request = CatalogTransactionRequest::CreateCatalog {
            catalog: "reviewed_operation_a".to_string(),
            description: Some("reviewed".to_string()),
        };
        let identity = catalog_transaction_identity(reviewed_request)?;
        let event_id = EventId::generate_after(None).expect("event ID");
        let now = Utc::now().timestamp_millis();
        let payload =
            serde_json::to_value(crate::tier1_events::CatalogDdlEventV2::CatalogCreated {
                catalog: crate::parquet_util::CatalogRecord {
                    id: uuid::Uuid::now_v7().to_string(),
                    name: "reviewed_operation_a".to_string(),
                    description: Some("reviewed".to_string()),
                    created_at: now,
                    updated_at: now,
                    properties_json: None,
                    storage_root: None,
                },
            })
            .expect("catalog event payload");
        let idempotency_key =
            CatalogEvent::<()>::generate_idempotency_key("catalog.ddl", 2, &payload)
                .expect("idempotency key");
        let envelope = CatalogEvent {
            event_type: "catalog.ddl".to_string(),
            event_version: 2,
            idempotency_key,
            occurred_at: Utc::now(),
            source: "api:test".to_string(),
            trace_id: None,
            sequence_position: None,
            payload,
        };
        let base = writer.load_current_catalog_manifest().await?;
        let base_state =
            tier1_state::load_catalog_state(&writer.storage, &base.value.snapshot_path).await?;
        let event_semantics = identity.reviewed_request.validate_event_realization(
            &envelope.event_type,
            envelope.event_version,
            &envelope.payload,
            &base_state,
            &identity.tenant_id,
            &identity.workspace_id,
        )?;
        let mut intent = CatalogTransactionEventIntent {
            record_type: CATALOG_TRANSACTION_INTENT_RECORD_TYPE.to_string(),
            version: CATALOG_TRANSACTION_INTENT_VERSION,
            tx_id: identity.tx_id.clone(),
            request_hash: identity.request_hash.clone(),
            base_manifest_id: base.value.manifest_id.clone(),
            base_manifest_path: CatalogPaths::domain_manifest_snapshot(
                CatalogDomain::Catalog,
                &base.value.manifest_id,
            ),
            base_manifest_sha256: compute_manifest_hash(&base.bytes),
            event_binding_sha256: String::new(),
            source: envelope.source.clone(),
            revision: 1,
            event_ids: vec![event_id.to_string()],
            active_event_id: event_id.to_string(),
            active_event_path: LedgerKey::event(CatalogDomain::Catalog, &event_id.to_string())
                .as_ref()
                .to_string(),
            event_json: serde_json::to_string_pretty(&envelope).expect("event JSON"),
        };
        intent.event_binding_sha256 = Tier1Writer::catalog_transaction_event_binding(
            &identity,
            &intent,
            &envelope,
            &event_semantics,
        )?;
        Ok((identity, intent))
    }

    fn catalog_transaction_identity(
        reviewed_request: CatalogTransactionRequest,
    ) -> Result<CatalogTransactionIdentity> {
        Ok(CatalogTransactionIdentity {
            tx_id: Ulid::new().to_string(),
            request_hash: reviewed_request.request_hash()?,
            tenant_id: "acme".to_string(),
            workspace_id: "production".to_string(),
            request_id: "handle:hdl_00000000000000000000000000:mutation:00000000000000000001"
                .to_string(),
            idempotency_key: "handle:hdl_00000000000000000000000000:mutation:00000000000000000001"
                .to_string(),
            handle_id: "hdl_00000000000000000000000000".to_string(),
            ordinal: 1,
            staged_sha256: format!("sha256:{}", "b".repeat(64)),
            reviewed_request,
            mutation_authorized: true,
        })
    }

    #[tokio::test]
    async fn catalog_transaction_visible_validation_survives_more_than_10000_later_manifests()
    -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let catalog_writer = CatalogWriter::new(storage.clone())
            .with_sync_compactor(Arc::new(Tier1Compactor::new(storage.clone())));
        catalog_writer.initialize().await?;
        let request = CatalogTransactionRequest::CreateCatalog {
            catalog: "reviewed_operation_a".to_string(),
            description: Some("reviewed".to_string()),
        };
        let identity = catalog_transaction_identity(request)?;
        let commit = catalog_writer
            .create_catalog_transaction(
                "reviewed_operation_a",
                Some("reviewed"),
                catalog_transaction_options(&identity),
            )
            .await?;
        let later_manifests = append_catalog_manifest_only_successors(&storage, 10_001).await?;
        assert_eq!(later_manifests.len(), 10_001);

        Tier1Writer::new(storage)
            .validate_catalog_transaction_publication(
                &identity,
                &CatalogTransactionPublication {
                    event_id: &commit.event_id,
                    commit_id: &commit.commit_id,
                    manifest_id: &commit.manifest_id,
                    snapshot_version: commit.snapshot_version,
                    authority_version: &commit.pointer_version,
                    fencing_token: commit.fencing_token,
                },
            )
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_visible_validation_survives_ledger_event_removal() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let catalog_writer = CatalogWriter::new(storage.clone())
            .with_sync_compactor(Arc::new(Tier1Compactor::new(storage.clone())));
        catalog_writer.initialize().await?;
        let request = CatalogTransactionRequest::CreateCatalog {
            catalog: "reviewed_operation_a".to_string(),
            description: Some("reviewed".to_string()),
        };
        let identity = catalog_transaction_identity(request)?;
        let commit = catalog_writer
            .create_catalog_transaction(
                "reviewed_operation_a",
                Some("reviewed"),
                catalog_transaction_options(&identity),
            )
            .await?;
        let event_path = LedgerKey::event(CatalogDomain::Catalog, &commit.event_id);
        storage.delete(event_path.as_ref()).await?;

        Tier1Writer::new(storage)
            .validate_catalog_transaction_publication(
                &identity,
                &CatalogTransactionPublication {
                    event_id: &commit.event_id,
                    commit_id: &commit.commit_id,
                    manifest_id: &commit.manifest_id,
                    snapshot_version: commit.snapshot_version,
                    authority_version: &commit.pointer_version,
                    fencing_token: commit.fencing_token,
                },
            )
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_visible_validation_rejects_a_divergent_event_witness() -> Result<()>
    {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let catalog_writer = CatalogWriter::new(storage.clone())
            .with_sync_compactor(Arc::new(Tier1Compactor::new(storage.clone())));
        catalog_writer.initialize().await?;
        let request = CatalogTransactionRequest::CreateCatalog {
            catalog: "reviewed_operation_a".to_string(),
            description: Some("reviewed".to_string()),
        };
        let identity = catalog_transaction_identity(request)?;
        let commit = catalog_writer
            .create_catalog_transaction(
                "reviewed_operation_a",
                Some("reviewed"),
                catalog_transaction_options(&identity),
            )
            .await?;
        rewrite_catalog_commit_for_test(&storage, &commit.event_id, |row| {
            let mut witnesses = decode_catalog_commit_event_witnesses(
                row.event_witnesses_json.as_deref().ok_or_else(|| {
                    CatalogError::InvariantViolation {
                        message: "test catalog commit has no event witnesses".to_string(),
                    }
                })?,
            )?;
            let witness = witnesses
                .iter_mut()
                .find(|witness| witness.event_id == commit.event_id)
                .ok_or_else(|| CatalogError::InvariantViolation {
                    message: "test catalog commit has no matching event witness".to_string(),
                })?;
            witness.event_sha256 = format!("sha256:{}", "c".repeat(64));
            row.event_witnesses_json = Some(
                crate::parquet_util::encode_catalog_commit_event_witnesses(witnesses)?,
            );
            Ok(())
        })
        .await?;

        Tier1Writer::new(storage)
            .validate_catalog_transaction_publication(
                &identity,
                &CatalogTransactionPublication {
                    event_id: &commit.event_id,
                    commit_id: &commit.commit_id,
                    manifest_id: &commit.manifest_id,
                    snapshot_version: commit.snapshot_version,
                    authority_version: &commit.pointer_version,
                    fencing_token: commit.fencing_token,
                },
            )
            .await
            .expect_err("a divergent event digest must not prove transaction visibility");
        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_recovery_finds_a_nonmax_event_in_a_selected_batch() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let tier1 = Tier1Writer::new(storage.clone());
        tier1.initialize().await?;
        let (identity, intent) = catalog_transaction_intent_fixture(&tier1).await?;
        let intent_path = Tier1Writer::catalog_transaction_intent_path(&identity)?;
        storage
            .put_raw(
                &intent_path,
                Bytes::from(serde_json::to_vec(&intent).expect("intent JSON")),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        tier1.ensure_catalog_transaction_event(&intent).await?;

        let intent_event_id = intent
            .active_event_id
            .parse::<EventId>()
            .expect("intent event ID");
        let later_event_id =
            EventId::generate_after(Some(intent_event_id)).expect("later event ID");
        let now = Utc::now().timestamp_millis();
        let later_payload = crate::tier1_events::CatalogDdlEventV2::CatalogCreated {
            catalog: crate::parquet_util::CatalogRecord {
                id: uuid::Uuid::now_v7().to_string(),
                name: "background_batch_successor".to_string(),
                description: Some("later".to_string()),
                created_at: now,
                updated_at: now,
                properties_json: None,
                storage_root: None,
            },
        };
        let later_event = CatalogEvent {
            event_type: crate::tier1_events::CatalogDdlEventV2::EVENT_TYPE.to_string(),
            event_version: crate::tier1_events::CatalogDdlEventV2::EVENT_VERSION,
            idempotency_key: CatalogEvent::<()>::generate_idempotency_key(
                crate::tier1_events::CatalogDdlEventV2::EVENT_TYPE,
                crate::tier1_events::CatalogDdlEventV2::EVENT_VERSION,
                &later_payload,
            )
            .expect("later idempotency key"),
            occurred_at: Utc::now(),
            source: "compactor:test".to_string(),
            trace_id: None,
            sequence_position: None,
            payload: later_payload,
        };
        let later_event_path =
            LedgerKey::event(CatalogDomain::Catalog, &later_event_id.to_string());
        storage
            .put_raw(
                later_event_path.as_ref(),
                Bytes::from(serde_json::to_vec_pretty(&later_event).expect("later event JSON")),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        let guard = tier1.acquire_lock(Duration::from_secs(30), 1).await?;
        let compacted = Tier1Compactor::new(storage.clone())
            .sync_compact(
                CatalogDomain::Catalog.as_str(),
                vec![
                    intent.active_event_path.clone(),
                    later_event_path.as_ref().to_string(),
                ],
                guard.fencing_token().sequence(),
            )
            .await
            .expect("background-style multi-event compaction");
        guard.release().await?;

        let recovered = CatalogWriter::new(storage.clone())
            .with_sync_compactor(Arc::new(Tier1Compactor::new(storage)))
            .recover_catalog_transaction(&identity, Some(identity.request_id.clone()))
            .await?
            .expect("selected batch should recover the frozen transaction");

        assert_eq!(recovered.event_id, intent.active_event_id);
        assert_eq!(recovered.manifest_id, compacted.manifest_id);
        assert_eq!(recovered.snapshot_version, compacted.snapshot_version);
        Ok(())
    }

    #[tokio::test]
    async fn legacy_catalog_transaction_publication_walk_has_no_history_ceiling() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let catalog_writer = CatalogWriter::new(storage.clone())
            .with_sync_compactor(Arc::new(Tier1Compactor::new(storage.clone())));
        catalog_writer.initialize().await?;
        let request = CatalogTransactionRequest::CreateCatalog {
            catalog: "reviewed_operation_a".to_string(),
            description: Some("reviewed".to_string()),
        };
        let identity = catalog_transaction_identity(request)?;
        let commit = catalog_writer
            .create_catalog_transaction(
                "reviewed_operation_a",
                Some("reviewed"),
                catalog_transaction_options(&identity),
            )
            .await?;
        strip_catalog_commit_manifest_witness(&storage, &commit.event_id).await?;
        let later_manifests = append_catalog_manifest_only_successors(&storage, 10_001).await?;
        assert_eq!(later_manifests.len(), 10_001);

        Tier1Writer::new(storage)
            .validate_catalog_transaction_publication(
                &identity,
                &CatalogTransactionPublication {
                    event_id: &commit.event_id,
                    commit_id: &commit.commit_id,
                    manifest_id: &commit.manifest_id,
                    snapshot_version: commit.snapshot_version,
                    authority_version: &commit.pointer_version,
                    fencing_token: commit.fencing_token,
                },
            )
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_recovery_never_scans_manifest_history_while_locked() -> Result<()>
    {
        let backend = Arc::new(ObservingBackend::new());
        let storage = ScopedStorage::new(backend.clone(), "acme", "production")?;
        let tier1 = Tier1Writer::new(storage.clone());
        tier1.initialize().await?;
        let (identity, intent) = catalog_transaction_intent_fixture(&tier1).await?;
        let intent_path = Tier1Writer::catalog_transaction_intent_path(&identity)?;
        storage
            .put_raw(
                &intent_path,
                Bytes::from(serde_json::to_vec(&intent).expect("intent JSON")),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        let later_manifests = append_catalog_manifest_only_successors(&storage, 3).await?;
        backend.watch_manifest_while_locked(later_manifests[0].clone());

        let catalog_writer = CatalogWriter::new(storage.clone())
            .with_sync_compactor(Arc::new(Tier1Compactor::new(storage)));
        let recovered = catalog_writer
            .recover_catalog_transaction(&identity, Some(identity.request_id.clone()))
            .await?;
        assert!(recovered.is_some(), "orphan intent should recover");
        assert!(
            !backend.watched_manifest_was_read_while_locked(),
            "deep manifest history must be inspected before acquiring the catalog lock"
        );

        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_intent_uses_only_the_stable_catalog_base_watermark() -> Result<()>
    {
        let backend = Arc::new(ObservingBackend::new());
        let storage = ScopedStorage::new(backend.clone(), "acme", "production")?;
        let tier1 = Tier1Writer::new(storage);
        tier1.initialize().await?;
        let (identity, intent) = catalog_transaction_intent_fixture(&tier1).await?;
        let envelope: CatalogEvent<serde_json::Value> =
            serde_json::from_str(&intent.event_json).expect("catalog event envelope");
        let payload: crate::tier1_events::CatalogDdlEventV2 =
            serde_json::from_value(envelope.payload).expect("catalog event payload");
        backend.reset_io_counts();

        tier1
            .build_catalog_transaction_event_intent(&payload, "api:test", &identity)
            .await?;

        assert_eq!(
            backend.unrelated_manifest_gets(),
            0,
            "a frozen catalog intent must not load unrelated domain manifests"
        );
        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_collision_loads_each_catalog_state_once() -> Result<()> {
        let backend = Arc::new(ObservingBackend::new());
        let storage = ScopedStorage::new(backend.clone(), "acme", "production")?;
        let catalog_writer = CatalogWriter::new(storage.clone())
            .with_sync_compactor(Arc::new(Tier1Compactor::new(storage.clone())));
        catalog_writer.initialize().await?;
        catalog_writer
            .create_catalog_transaction("seed", Some("seed"), WriteOptions::default())
            .await?;
        let tier1 = Tier1Writer::new(storage.clone());
        let (identity, intent) = catalog_transaction_intent_fixture(&tier1).await?;
        let intent_path = Tier1Writer::catalog_transaction_intent_path(&identity)?;
        storage
            .put_raw(
                &intent_path,
                Bytes::from(serde_json::to_vec(&intent).expect("intent JSON")),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        let envelope: CatalogEvent<serde_json::Value> =
            serde_json::from_str(&intent.event_json).expect("catalog event envelope");
        let payload: crate::tier1_events::CatalogDdlEventV2 =
            serde_json::from_value(envelope.payload).expect("catalog event payload");
        backend.reset_io_counts();
        let guard = tier1.acquire_lock(Duration::from_secs(30), 1).await?;

        let event_id = tier1
            .append_ledger_event_for_transaction(
                &guard,
                CatalogDomain::Catalog,
                &payload,
                "api:test",
                &identity,
            )
            .await?;
        guard.release().await?;

        assert_eq!(event_id.to_string(), intent.active_event_id);
        assert_eq!(
            backend.catalog_snapshot_gets(),
            10,
            "candidate and collision winner should each load one five-file catalog state"
        );
        Ok(())
    }

    async fn stale_update_transaction_fixture() -> Result<(
        ScopedStorage,
        CatalogWriter,
        Tier1Writer,
        CatalogTransactionIdentity,
        CatalogTransactionEventIntent,
    )> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = CatalogWriter::new(storage.clone())
            .with_sync_compactor(Arc::new(Tier1Compactor::new(storage.clone())));
        writer.initialize().await?;
        writer
            .create_schema_transaction("default", "reviewed", None, WriteOptions::default())
            .await?;
        writer
            .register_table_in_schema_transaction(
                "default",
                "reviewed",
                RegisterTableInSchemaRequest {
                    name: "events".to_string(),
                    description: Some("before".to_string()),
                    location: Some("s3://catalog/before".to_string()),
                    format: Some("parquet".to_string()),
                    table_type: None,
                    properties: None,
                    columns: vec![],
                },
                WriteOptions::default(),
            )
            .await?;

        let tier1 = Tier1Writer::new(storage.clone());
        let base = tier1.load_current_catalog_manifest().await?;
        let state = tier1_state::load_catalog_state(&storage, &base.value.snapshot_path).await?;
        let mut table = state
            .tables
            .iter()
            .find(|table| table.name == "events")
            .expect("seeded table")
            .clone();
        let reviewed_request = CatalogTransactionRequest::UpdateTable {
            catalog: "default".to_string(),
            schema: "reviewed".to_string(),
            table: "events".to_string(),
            description: Some(Some("reviewed update".to_string())),
            location: None,
            format: None,
        };
        let identity = catalog_transaction_identity(reviewed_request)?;
        table.description = Some("reviewed update".to_string());
        table.updated_at = table.updated_at.saturating_add(1);
        let event = crate::tier1_events::CatalogDdlEvent::TableUpdated { table };
        let intent = tier1
            .build_catalog_transaction_event_intent(&event, "api:test", &identity)
            .await?;

        Ok((storage, writer, tier1, identity, intent))
    }

    fn replace_with_self_bound_operation_b(
        identity: &CatalogTransactionIdentity,
        intent: &mut CatalogTransactionEventIntent,
    ) -> Result<()> {
        let payload = serde_json::json!({
            "kind": "namespace_deleted",
            "namespace_id": "namespace-operation-b",
            "namespace_name": "operation_b"
        });
        let envelope = CatalogEvent {
            event_type: "catalog.ddl".to_string(),
            event_version: 1,
            idempotency_key: CatalogEvent::<()>::generate_idempotency_key(
                "catalog.ddl",
                1,
                &payload,
            )
            .expect("operation B idempotency key"),
            occurred_at: Utc::now(),
            source: "api:test".to_string(),
            trace_id: None,
            sequence_position: None,
            payload,
        };
        intent.event_json =
            serde_json::to_string_pretty(&envelope).expect("operation B event JSON");
        intent.event_binding_sha256 = Tier1Writer::catalog_transaction_event_binding(
            identity,
            intent,
            &envelope,
            &envelope.payload,
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_intent_rejects_tampered_event_envelope() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage);
        writer.initialize().await?;
        let (identity, intent) = catalog_transaction_intent_fixture(&writer).await?;
        writer
            .validate_catalog_transaction_intent(&identity, Some("api:test"), &intent)
            .await
            .expect("valid event intent");

        let mut tampered = intent.clone();
        tampered.revision = 2;
        writer
            .validate_catalog_transaction_intent(&identity, Some("api:test"), &tampered)
            .await
            .expect_err("intent revision must equal its immutable event history length");

        let mut tampered = intent.clone();
        let mut envelope: serde_json::Value =
            serde_json::from_str(&tampered.event_json).expect("decode event JSON");
        *envelope
            .pointer_mut("/payload/catalog/name")
            .expect("catalog event name") = serde_json::json!("unreviewed");
        tampered.event_json =
            serde_json::to_string_pretty(&envelope).expect("encode tampered event JSON");
        writer
            .validate_catalog_transaction_intent(&identity, Some("api:test"), &tampered)
            .await
            .expect_err("payload changed without its deterministic idempotency key must fail");

        tampered.event_json = serde_json::json!({"source": "api:test"}).to_string();
        writer
            .validate_catalog_transaction_intent(&identity, Some("api:test"), &tampered)
            .await
            .expect_err("incomplete event envelope must fail");

        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_recovery_rejects_another_operation_with_the_reviewed_hash()
    -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;
        let (identity, mut operation_b_intent) =
            catalog_transaction_intent_fixture(&writer).await?;
        replace_with_self_bound_operation_b(&identity, &mut operation_b_intent)?;
        let intent_path = Tier1Writer::catalog_transaction_intent_path(&identity)?;
        storage
            .put_raw(
                &intent_path,
                Bytes::from(
                    serde_json::to_vec(&operation_b_intent).expect("operation B intent JSON"),
                ),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let pointer_before = storage.get_raw(&pointer_path).await?;

        writer
            .recover_catalog_transaction_event(&identity)
            .await
            .expect_err("operation B must not recover under operation A's reviewed hash");

        assert!(
            storage
                .head_raw(&operation_b_intent.active_event_path)
                .await?
                .is_none(),
            "a divergent event must fail before publication"
        );
        assert_eq!(
            storage.get_raw(&pointer_path).await?,
            pointer_before,
            "a divergent event must fail before manifest publication"
        );

        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_collision_rejects_another_operation_with_the_reviewed_hash()
    -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;
        let (identity, mut operation_b_intent) =
            catalog_transaction_intent_fixture(&writer).await?;
        let reviewed_event: CatalogEvent<serde_json::Value> =
            serde_json::from_str(&operation_b_intent.event_json).expect("reviewed event JSON");
        let reviewed_payload: crate::tier1_events::CatalogDdlEventV2 =
            serde_json::from_value(reviewed_event.payload).expect("reviewed event payload");
        replace_with_self_bound_operation_b(&identity, &mut operation_b_intent)?;
        let intent_path = Tier1Writer::catalog_transaction_intent_path(&identity)?;
        storage
            .put_raw(
                &intent_path,
                Bytes::from(
                    serde_json::to_vec(&operation_b_intent).expect("operation B intent JSON"),
                ),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let pointer_before = storage.get_raw(&pointer_path).await?;
        let guard = writer
            .acquire_lock(Duration::from_secs(30), 1)
            .await
            .expect("catalog lock");

        writer
            .append_ledger_event_for_transaction(
                &guard,
                CatalogDomain::Catalog,
                &reviewed_payload,
                "api:test",
                &identity,
            )
            .await
            .expect_err("operation B must not win operation A's intent collision");
        guard.release().await?;

        assert!(
            storage
                .head_raw(&operation_b_intent.active_event_path)
                .await?
                .is_none(),
            "a divergent collision winner must fail before event publication"
        );
        assert_eq!(
            storage.get_raw(&pointer_path).await?,
            pointer_before,
            "a divergent collision winner must fail before manifest publication"
        );

        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_recovery_rejects_stale_inherited_table_state() -> Result<()> {
        let (storage, writer, tier1, identity, intent) = stale_update_transaction_fixture().await?;
        let intent_path = Tier1Writer::catalog_transaction_intent_path(&identity)?;
        let intent_bytes = Bytes::from(serde_json::to_vec(&intent).expect("intent JSON"));
        storage
            .put_raw(
                &intent_path,
                intent_bytes.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        writer
            .update_table_in_schema_transaction(
                "default",
                "reviewed",
                "events",
                TablePatch {
                    location: Some(Some("s3://catalog/intervening".to_string())),
                    ..TablePatch::default()
                },
                WriteOptions::default(),
            )
            .await?;
        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let pointer_before = storage.get_raw(&pointer_path).await?;

        tier1
            .recover_catalog_transaction_event(&identity)
            .await
            .expect_err("recovery must not reissue stale inherited table state");

        assert_eq!(
            storage.get_raw(&intent_path).await?,
            intent_bytes,
            "stale-base recovery must fail before revising its intent"
        );
        assert!(
            storage.head_raw(&intent.active_event_path).await?.is_none(),
            "stale-base recovery must fail before event publication"
        );
        assert_eq!(
            storage.get_raw(&pointer_path).await?,
            pointer_before,
            "stale-base recovery must fail before manifest publication"
        );

        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_collision_rejects_stale_inherited_table_state() -> Result<()> {
        let (storage, writer, tier1, identity, intent) = stale_update_transaction_fixture().await?;
        let intent_path = Tier1Writer::catalog_transaction_intent_path(&identity)?;
        let intent_bytes = Bytes::from(serde_json::to_vec(&intent).expect("intent JSON"));
        storage
            .put_raw(
                &intent_path,
                intent_bytes.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        writer
            .update_table_in_schema_transaction(
                "default",
                "reviewed",
                "events",
                TablePatch {
                    location: Some(Some("s3://catalog/intervening".to_string())),
                    ..TablePatch::default()
                },
                WriteOptions::default(),
            )
            .await?;

        let current = tier1.load_current_catalog_manifest().await?;
        let current_state =
            tier1_state::load_catalog_state(&storage, &current.value.snapshot_path).await?;
        let mut current_table = current_state
            .tables
            .iter()
            .find(|table| table.name == "events")
            .expect("current table")
            .clone();
        current_table.description = Some("reviewed update".to_string());
        current_table.updated_at = current_table.updated_at.saturating_add(1);
        let current_event = crate::tier1_events::CatalogDdlEvent::TableUpdated {
            table: current_table,
        };
        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let pointer_before = storage.get_raw(&pointer_path).await?;
        let guard = tier1
            .acquire_lock(Duration::from_secs(30), 1)
            .await
            .expect("catalog lock");

        let result = tier1
            .append_ledger_event_for_transaction(
                &guard,
                CatalogDomain::Catalog,
                &current_event,
                "api:test",
                &identity,
            )
            .await;
        guard.release().await?;
        result.expect_err("collision must not adopt stale inherited table state");

        assert_eq!(
            storage.get_raw(&intent_path).await?,
            intent_bytes,
            "stale-base collision must not revise its intent"
        );
        assert!(
            storage.head_raw(&intent.active_event_path).await?.is_none(),
            "stale-base collision must fail before event publication"
        );
        assert_eq!(
            storage.get_raw(&pointer_path).await?,
            pointer_before,
            "stale-base collision must fail before manifest publication"
        );

        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_recovery_recreates_the_exact_intended_event() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;
        let (identity, intent) = catalog_transaction_intent_fixture(&writer).await?;
        let intent_path = Tier1Writer::catalog_transaction_intent_path(&identity)?;
        let result = storage
            .put_raw(
                &intent_path,
                Bytes::from(serde_json::to_vec(&intent).expect("intent JSON")),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        assert!(matches!(result, WriteResult::Success { .. }));
        assert!(storage.head_raw(&intent.active_event_path).await?.is_none());

        let recovered = writer.recover_catalog_transaction_event(&identity).await?;
        assert!(matches!(
            recovered,
            CatalogTransactionEventRecovery::Ready(event_id)
                if event_id.to_string() == intent.active_event_id
        ));
        assert_eq!(
            storage.get_raw(&intent.active_event_path).await?.as_ref(),
            intent.event_json.as_bytes()
        );

        Ok(())
    }

    #[tokio::test]
    async fn catalog_transaction_recovery_rejects_noncanonical_intent_json() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;
        let (identity, intent) = catalog_transaction_intent_fixture(&writer).await?;
        let intent_path = Tier1Writer::catalog_transaction_intent_path(&identity)?;
        storage
            .put_raw(
                &intent_path,
                Bytes::from(serde_json::to_vec_pretty(&intent).expect("pretty intent JSON")),
                WritePrecondition::DoesNotExist,
            )
            .await?;

        writer
            .recover_catalog_transaction_event(&identity)
            .await
            .expect_err("noncanonical transaction intent JSON must fail closed");
        assert!(storage.head_raw(&intent.active_event_path).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_initialize_catalog_creates_required_files() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());

        writer.initialize().await?;

        let root_bytes = storage.get_raw(CatalogPaths::ROOT_MANIFEST).await?;
        let mut root: RootManifest = parse_json(&root_bytes)?;
        root.normalize_paths();
        assert_eq!(root.version, 1);

        let catalog_pointer_bytes = storage.get_raw(&root.catalog_manifest_path).await?;
        let catalog_pointer: DomainManifestPointer = parse_json(&catalog_pointer_bytes)?;
        let catalog_bytes = storage.get_raw(&catalog_pointer.manifest_path).await?;
        let catalog: CatalogDomainManifest = parse_json(&catalog_bytes)?;
        assert_eq!(catalog.snapshot_version, 0);

        let lineage_pointer_bytes = storage.get_raw(&root.lineage_manifest_path).await?;
        let lineage_pointer: DomainManifestPointer = parse_json(&lineage_pointer_bytes)?;
        let lineage_bytes = storage.get_raw(&lineage_pointer.manifest_path).await?;
        let lineage: LineageManifest = parse_json(&lineage_bytes)?;
        assert_eq!(lineage.snapshot_version, 0);

        let exec_bytes = storage.get_raw(&root.executions_manifest_path).await?;
        let exec: ExecutionsManifest = parse_json(&exec_bytes)?;
        assert_eq!(exec.watermark_version, 0);

        let search_pointer_bytes = storage.get_raw(&root.search_manifest_path).await?;
        let search_pointer: DomainManifestPointer = parse_json(&search_pointer_bytes)?;
        let search_bytes = storage.get_raw(&search_pointer.manifest_path).await?;
        let search: SearchManifest = parse_json(&search_bytes)?;
        assert_eq!(search.snapshot_version, 0);
        assert!(
            storage
                .head_raw(&CatalogPaths::domain_manifest(CatalogDomain::Catalog))
                .await?
                .is_none()
        );
        assert!(
            storage
                .head_raw(&CatalogPaths::domain_manifest(CatalogDomain::Lineage))
                .await?
                .is_none()
        );
        assert!(
            storage
                .head_raw(&CatalogPaths::domain_manifest(CatalogDomain::Search))
                .await?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_initialize_idempotent() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage);

        writer.initialize().await?;
        writer.initialize().await?;

        Ok(())
    }

    #[tokio::test]
    async fn append_ledger_event_advances_past_catalog_watermark() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;

        let watermark: EventId =
            "7ZZZZZZZZZ0000000000000000"
                .parse()
                .map_err(|e| CatalogError::Validation {
                    message: format!("parse watermark: {e}"),
                })?;
        let pointer_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest_pointer(
                CatalogDomain::Catalog,
            ))
            .await?;
        let pointer: DomainManifestPointer = parse_json(&pointer_bytes)?;
        let mut manifest: CatalogDomainManifest =
            parse_json(&storage.get_raw(&pointer.manifest_path).await?)?;
        manifest.watermark_event_id = Some(watermark.to_string());
        storage
            .put_raw(
                &pointer.manifest_path,
                json_bytes(&manifest)?,
                WritePrecondition::None,
            )
            .await?;

        let guard = writer.acquire_lock(Duration::from_secs(30), 1).await?;
        let event = crate::tier1_events::CatalogDdlEvent::NamespaceCreated {
            namespace: crate::parquet_util::NamespaceRecord {
                id: "ns-1".to_string(),
                catalog_id: None,
                name: "sales".to_string(),
                description: None,
                created_at: 1,
                updated_at: 1,
                properties_json: None,
                storage_root: None,
            },
        };

        let event_id = writer
            .append_ledger_event(&guard, CatalogDomain::Catalog, &event, "test")
            .await?;
        guard.release().await.map_err(CatalogError::from)?;

        assert!(
            event_id > watermark,
            "appended event ID must sort after the visible catalog watermark"
        );
        Ok(())
    }

    #[tokio::test]
    async fn append_ledger_event_allocates_distinct_ids_when_watermark_is_in_future() -> Result<()>
    {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;

        let watermark: EventId =
            "7ZZZZZZZZZ0000000000000000"
                .parse()
                .map_err(|e| CatalogError::Validation {
                    message: format!("parse watermark: {e}"),
                })?;
        let pointer_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest_pointer(
                CatalogDomain::Catalog,
            ))
            .await?;
        let pointer: DomainManifestPointer = parse_json(&pointer_bytes)?;
        let mut manifest: CatalogDomainManifest =
            parse_json(&storage.get_raw(&pointer.manifest_path).await?)?;
        manifest.watermark_event_id = Some(watermark.to_string());
        storage
            .put_raw(
                &pointer.manifest_path,
                json_bytes(&manifest)?,
                WritePrecondition::None,
            )
            .await?;

        let guard = writer.acquire_lock(Duration::from_secs(30), 1).await?;
        let first = crate::tier1_events::CatalogDdlEvent::NamespaceCreated {
            namespace: crate::parquet_util::NamespaceRecord {
                id: "ns-1".to_string(),
                catalog_id: None,
                name: "sales".to_string(),
                description: None,
                created_at: 1,
                updated_at: 1,
                properties_json: None,
                storage_root: None,
            },
        };
        let second = crate::tier1_events::CatalogDdlEvent::NamespaceCreated {
            namespace: crate::parquet_util::NamespaceRecord {
                id: "ns-2".to_string(),
                catalog_id: None,
                name: "support".to_string(),
                description: None,
                created_at: 2,
                updated_at: 2,
                properties_json: None,
                storage_root: None,
            },
        };

        let first_id = writer
            .append_ledger_event(&guard, CatalogDomain::Catalog, &first, "test")
            .await?;
        let second_id = writer
            .append_ledger_event(&guard, CatalogDomain::Catalog, &second, "test")
            .await?;
        guard.release().await.map_err(CatalogError::from)?;

        assert!(
            second_id > first_id,
            "same-lock events must receive strictly increasing ledger IDs"
        );
        let ledger_files = storage.list("ledger/catalog/").await?;
        assert_eq!(ledger_files.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn append_ledger_event_after_advances_past_previous_same_lock_event() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;

        let previous_event_id: EventId =
            "7ZZZZZZZZZ0000000000000000"
                .parse()
                .map_err(|e| CatalogError::Validation {
                    message: format!("parse previous event id: {e}"),
                })?;
        let event = crate::tier1_events::CatalogDdlEvent::NamespaceCreated {
            namespace: crate::parquet_util::NamespaceRecord {
                id: "ns-ordered".to_string(),
                catalog_id: None,
                name: "ordered".to_string(),
                description: None,
                created_at: 1,
                updated_at: 1,
                properties_json: None,
                storage_root: None,
            },
        };

        let guard = writer.acquire_lock(Duration::from_secs(30), 1).await?;
        let event_id = writer
            .append_ledger_event_after(
                &guard,
                CatalogDomain::Catalog,
                &event,
                "test",
                Some(previous_event_id),
            )
            .await?;
        guard.release().await.map_err(CatalogError::from)?;

        assert!(
            event_id > previous_event_id,
            "same-lock event ID must sort after the previous generated event ID"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_initialize_migrates_legacy_tier1_heads_to_pointers() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;

        let legacy_root = RootManifest {
            version: 1,
            catalog_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Catalog),
            lineage_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Lineage),
            executions_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Executions),
            search_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Search),
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                CatalogPaths::ROOT_MANIFEST,
                json_bytes(&legacy_root)?,
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let mut legacy_catalog = CatalogDomainManifest::new();
        legacy_catalog.manifest_id = "00000000000000000007".to_string();
        legacy_catalog.snapshot_version = 7;
        legacy_catalog.snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 7);
        storage
            .put_raw(
                &CatalogPaths::domain_manifest(CatalogDomain::Catalog),
                json_bytes(&legacy_catalog)?,
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let mut legacy_lineage = LineageManifest::new();
        legacy_lineage.manifest_id = "00000000000000000003".to_string();
        legacy_lineage.snapshot_version = 3;
        legacy_lineage.edges_path = CatalogPaths::snapshot_dir(CatalogDomain::Lineage, 3);
        storage
            .put_raw(
                &CatalogPaths::domain_manifest(CatalogDomain::Lineage),
                json_bytes(&legacy_lineage)?,
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let mut legacy_search = SearchManifest::new();
        legacy_search.manifest_id = "00000000000000000005".to_string();
        legacy_search.snapshot_version = 5;
        legacy_search.base_path = CatalogPaths::snapshot_dir(CatalogDomain::Search, 5);
        storage
            .put_raw(
                &CatalogPaths::domain_manifest(CatalogDomain::Search),
                json_bytes(&legacy_search)?,
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;

        let root_bytes = storage.get_raw(CatalogPaths::ROOT_MANIFEST).await?;
        let root: RootManifest = parse_json(&root_bytes)?;
        assert_eq!(
            root.catalog_manifest_path,
            CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog)
        );
        assert_eq!(
            root.lineage_manifest_path,
            CatalogPaths::domain_manifest_pointer(CatalogDomain::Lineage)
        );
        assert_eq!(
            root.search_manifest_path,
            CatalogPaths::domain_manifest_pointer(CatalogDomain::Search)
        );

        let catalog_pointer: DomainManifestPointer = parse_json(
            &storage
                .get_raw(&CatalogPaths::domain_manifest_pointer(
                    CatalogDomain::Catalog,
                ))
                .await?,
        )?;
        let migrated_catalog: CatalogDomainManifest =
            parse_json(&storage.get_raw(&catalog_pointer.manifest_path).await?)?;
        assert_eq!(migrated_catalog.manifest_id, legacy_catalog.manifest_id);
        assert_eq!(
            migrated_catalog.snapshot_version,
            legacy_catalog.snapshot_version
        );

        let lineage_pointer: DomainManifestPointer = parse_json(
            &storage
                .get_raw(&CatalogPaths::domain_manifest_pointer(
                    CatalogDomain::Lineage,
                ))
                .await?,
        )?;
        let migrated_lineage: LineageManifest =
            parse_json(&storage.get_raw(&lineage_pointer.manifest_path).await?)?;
        assert_eq!(migrated_lineage.manifest_id, legacy_lineage.manifest_id);
        assert_eq!(
            migrated_lineage.snapshot_version,
            legacy_lineage.snapshot_version
        );

        let search_pointer: DomainManifestPointer = parse_json(
            &storage
                .get_raw(&CatalogPaths::domain_manifest_pointer(
                    CatalogDomain::Search,
                ))
                .await?,
        )?;
        let migrated_search: SearchManifest =
            parse_json(&storage.get_raw(&search_pointer.manifest_path).await?)?;
        assert_eq!(migrated_search.manifest_id, legacy_search.manifest_id);
        assert_eq!(
            migrated_search.snapshot_version,
            legacy_search.snapshot_version
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_initialize_roots_self_referential_legacy_catalog_history() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;

        let legacy_root = RootManifest {
            version: 1,
            catalog_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Catalog),
            lineage_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Lineage),
            executions_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Executions),
            search_manifest_path: CatalogPaths::domain_manifest(CatalogDomain::Search),
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                CatalogPaths::ROOT_MANIFEST,
                json_bytes(&legacy_root)?,
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let legacy_catalog_path = CatalogPaths::domain_manifest(CatalogDomain::Catalog);
        let mut legacy_catalog = CatalogDomainManifest::new();
        legacy_catalog.manifest_id = "00000000000000000001".to_string();
        legacy_catalog.snapshot_version = 1;
        legacy_catalog.snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1);
        legacy_catalog.previous_manifest_path = Some(legacy_catalog_path.clone());
        legacy_catalog.parent_hash = Some("sha256:legacy-mutable-head".to_string());
        storage
            .put_raw(
                &legacy_catalog_path,
                json_bytes(&legacy_catalog)?,
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;

        let pointer: DomainManifestPointer = parse_json(
            &storage
                .get_raw(&CatalogPaths::domain_manifest_pointer(
                    CatalogDomain::Catalog,
                ))
                .await?,
        )?;
        let migrated: CatalogDomainManifest =
            parse_json(&storage.get_raw(&pointer.manifest_path).await?)?;

        assert_eq!(
            pointer.manifest_path,
            CatalogPaths::domain_manifest_snapshot(
                CatalogDomain::Catalog,
                &legacy_catalog.manifest_id
            )
        );
        assert!(migrated.previous_manifest_path.is_none());
        assert!(migrated.parent_hash.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_initialize_repairs_pointerized_legacy_catalog_history() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;

        let root = RootManifest::new();
        storage
            .put_raw(
                CatalogPaths::ROOT_MANIFEST,
                json_bytes(&root)?,
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let legacy_catalog_path = CatalogPaths::domain_manifest(CatalogDomain::Catalog);
        let first_manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, "00000000000000000001");
        let mut first_manifest = CatalogDomainManifest::new();
        first_manifest.manifest_id = "00000000000000000001".to_string();
        first_manifest.snapshot_version = 1;
        first_manifest.snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1);
        first_manifest.previous_manifest_path = Some(legacy_catalog_path.clone());
        first_manifest.parent_hash = Some("sha256:legacy-mutable-head".to_string());
        let first_manifest_bytes = json_bytes(&first_manifest)?;
        storage
            .put_raw(
                &first_manifest_path,
                first_manifest_bytes.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        storage
            .put_raw(
                &legacy_catalog_path,
                first_manifest_bytes.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let second_manifest_path =
            CatalogPaths::domain_manifest_snapshot(CatalogDomain::Catalog, "00000000000000000002");
        let mut second_manifest = first_manifest.clone();
        second_manifest.manifest_id = "00000000000000000002".to_string();
        second_manifest.snapshot_version = 2;
        second_manifest.snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 2);
        second_manifest.previous_manifest_path = Some(first_manifest_path);
        second_manifest.parent_hash = Some(compute_manifest_hash(&first_manifest_bytes));
        storage
            .put_raw(
                &second_manifest_path,
                json_bytes(&second_manifest)?,
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let old_pointer = DomainManifestPointer {
            manifest_id: second_manifest.manifest_id.clone(),
            manifest_path: second_manifest_path,
            epoch: second_manifest.epoch,
            parent_pointer_hash: None,
            updated_at: Utc::now(),
        };
        storage
            .put_raw(
                &CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog),
                json_bytes(&old_pointer)?,
                WritePrecondition::DoesNotExist,
            )
            .await?;

        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;

        let pointer: DomainManifestPointer = parse_json(
            &storage
                .get_raw(&CatalogPaths::domain_manifest_pointer(
                    CatalogDomain::Catalog,
                ))
                .await?,
        )?;
        let repaired: CatalogDomainManifest =
            parse_json(&storage.get_raw(&pointer.manifest_path).await?)?;

        assert_eq!(pointer.manifest_id, "00000000000000000003");
        assert_eq!(repaired.manifest_id, pointer.manifest_id);
        assert_eq!(repaired.snapshot_version, second_manifest.snapshot_version);
        assert_eq!(repaired.snapshot_path, second_manifest.snapshot_path);
        assert!(repaired.previous_manifest_path.is_none());
        assert!(repaired.parent_hash.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_update_with_cas() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());

        writer.initialize().await?;

        let commit = writer
            .update(|manifest| {
                manifest.snapshot_version = 1;
                manifest.snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1);
                Ok(())
            })
            .await?;

        assert_eq!(commit.operation, "Update");

        let pointer_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest_pointer(
                CatalogDomain::Catalog,
            ))
            .await?;
        let pointer: DomainManifestPointer = parse_json(&pointer_bytes)?;
        let core_bytes = storage.get_raw(&pointer.manifest_path).await?;
        let core: CatalogDomainManifest = parse_json(&core_bytes)?;
        assert_eq!(core.snapshot_version, 1);
        assert_eq!(
            core.snapshot_path,
            CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1)
        );
        assert!(
            storage
                .head_raw(&CatalogPaths::domain_manifest(CatalogDomain::Catalog))
                .await?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cas_conflict_retries_and_succeeds() -> Result<()> {
        let backend = Arc::new(HookedBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone()).with_cas_retries(5);

        writer.initialize().await?;

        writer
            .update(|manifest| {
                manifest.snapshot_version += 1;
                Ok(())
            })
            .await?;

        let pointer_bytes = storage
            .get_raw(&CatalogPaths::domain_manifest_pointer(
                CatalogDomain::Catalog,
            ))
            .await?;
        let pointer: DomainManifestPointer = parse_json(&pointer_bytes)?;
        let core_bytes = storage.get_raw(&pointer.manifest_path).await?;
        let core: CatalogDomainManifest = parse_json(&core_bytes)?;
        assert_eq!(core.snapshot_version, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_writes_pointer_and_snapshot_manifest() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());
        writer.initialize().await?;

        let commit = writer
            .update(|manifest| {
                manifest.snapshot_version = 1;
                manifest.snapshot_path = CatalogPaths::snapshot_dir(CatalogDomain::Catalog, 1);
                Ok(())
            })
            .await?;

        let pointer_path = CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog);
        let pointer_bytes = storage.get_raw(&pointer_path).await?;
        let pointer: DomainManifestPointer = parse_json(&pointer_bytes)?;
        assert_eq!(pointer.manifest_id, "00000000000000000001");

        let snapshot_bytes = storage.get_raw(&pointer.manifest_path).await?;
        let snapshot: CatalogDomainManifest = parse_json(&snapshot_bytes)?;
        assert_eq!(snapshot.manifest_id, "00000000000000000001");
        assert_eq!(snapshot.snapshot_version, 1);
        assert!(
            storage
                .head_raw(&CatalogPaths::domain_manifest(CatalogDomain::Catalog))
                .await?
                .is_none()
        );
        assert!(
            storage
                .head_raw(&CatalogPaths::commit(
                    CatalogDomain::Catalog,
                    &commit.commit_id
                ))
                .await?
                .is_none()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_commit_receipts_keep_prev_commit_ids_without_legacy_hash_chain() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production")?;
        let writer = Tier1Writer::new(storage.clone());

        writer.initialize().await?;

        // First update - no previous commit to link
        let commit1 = writer
            .update(|manifest| {
                manifest.snapshot_version = 1;
                Ok(())
            })
            .await?;
        assert!(commit1.prev_commit_id.is_none());
        assert!(commit1.prev_commit_hash.is_none());

        // Second update - should link to first commit
        let commit2 = writer
            .update(|manifest| {
                manifest.snapshot_version = 2;
                Ok(())
            })
            .await?;
        assert_eq!(commit2.prev_commit_id, Some(commit1.commit_id.clone()));
        assert!(commit2.prev_commit_hash.is_none());

        // Third update - should link to second commit
        let commit3 = writer
            .update(|manifest| {
                manifest.snapshot_version = 3;
                Ok(())
            })
            .await?;
        assert_eq!(commit3.prev_commit_id, Some(commit2.commit_id.clone()));
        assert!(commit3.prev_commit_hash.is_none());
        assert!(
            storage
                .head_raw(&CatalogPaths::commit(
                    CatalogDomain::Catalog,
                    &commit1.commit_id
                ))
                .await?
                .is_none()
        );
        assert!(
            storage
                .head_raw(&CatalogPaths::commit(
                    CatalogDomain::Catalog,
                    &commit2.commit_id
                ))
                .await?
                .is_none()
        );
        assert!(
            storage
                .head_raw(&CatalogPaths::commit(
                    CatalogDomain::Catalog,
                    &commit3.commit_id
                ))
                .await?
                .is_none()
        );

        Ok(())
    }
}

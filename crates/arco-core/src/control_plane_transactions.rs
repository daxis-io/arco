//! Shared control-plane transaction paths and storage record types.
//!
//! This module sketches the shared `transactions/...` layout for catalog,
//! orchestration, and root visibility-scoped commits without changing the
//! existing writers yet. Catalog and orchestration remain pointer-published;
//! root transactions use a tx-scoped record plus immutable super-manifest.

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::storage_keys::LockKey;

/// Canonical control-plane transaction domains used in storage paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlPlaneTxDomain {
    /// Single-domain catalog transaction objects.
    Catalog,
    /// Single-domain orchestration transaction objects.
    Orchestration,
    /// Cross-domain root transaction objects.
    Root,
}

impl ControlPlaneTxDomain {
    /// Returns the canonical path segment for this domain.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Orchestration => "orchestration",
            Self::Root => "root",
        }
    }
}

impl Display for ControlPlaneTxDomain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Logical transaction kind stored inside transaction records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlPlaneTxKind {
    /// Catalog DDL transaction.
    CatalogDdl,
    /// Orchestration batch commit transaction.
    OrchestrationBatch,
    /// Cross-domain root commit transaction.
    RootCommit,
}

/// Transaction lifecycle state for control-plane records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ControlPlaneTxStatus {
    /// Immutable artifacts were staged but are not yet visible.
    Prepared,
    /// The domain head or root read token is visible to readers.
    Visible,
    /// The transaction was terminated without becoming visible.
    Aborted,
}

/// Canonical path builders for shared control-plane transaction artifacts.
pub struct ControlPlaneTxPaths;

impl ControlPlaneTxPaths {
    /// Top-level transaction prefix.
    pub const PREFIX: &str = "transactions";

    fn hash_idempotency_key(idempotency_key: &str) -> String {
        format!("{:x}", Sha256::digest(idempotency_key.as_bytes()))
    }

    /// Returns the idempotency marker path for a transaction domain and key.
    #[must_use]
    pub fn idempotency(domain: ControlPlaneTxDomain, idempotency_key: &str) -> String {
        let key_hash = Self::hash_idempotency_key(idempotency_key);
        let prefix = key_hash.get(0..2).unwrap_or("00");
        format!(
            "{}/idempotency/{}/{prefix}/{key_hash}.json",
            Self::PREFIX,
            domain.as_str()
        )
    }

    /// Returns the transaction record path for a domain and transaction id.
    #[must_use]
    pub fn record(domain: ControlPlaneTxDomain, tx_id: &str) -> String {
        format!("{}/{}/{}.json", Self::PREFIX, domain.as_str(), tx_id)
    }

    /// Returns the canonical root lock path.
    #[must_use]
    pub fn root_lock() -> String {
        LockKey::custom("root").to_string()
    }

    /// Returns the immutable tx-scoped root super-manifest path.
    #[must_use]
    pub fn root_super_manifest(tx_id: &str) -> String {
        format!("{}/root/{tx_id}.manifest.json", Self::PREFIX)
    }

    /// Returns the optional root commit receipt path.
    #[must_use]
    pub fn root_commit_receipt(commit_id: &str) -> String {
        format!("commits/root/{commit_id}.json")
    }
}

/// Shared transaction record stored under `transactions/{domain}/{tx_id}.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlPlaneTxRecord<TResult> {
    /// Transaction identifier.
    pub tx_id: String,
    /// Logical operation kind for audit/debugging.
    pub kind: ControlPlaneTxKind,
    /// Current lifecycle status.
    pub status: ControlPlaneTxStatus,
    /// Whether post-commit repair work is still outstanding.
    #[serde(default)]
    pub repair_pending: bool,
    /// Unique request identifier for this attempt.
    pub request_id: String,
    /// Caller idempotency key for replay detection.
    pub idempotency_key: String,
    /// Canonical request payload hash.
    pub request_hash: String,
    /// Canonical lock path used to fence the writer.
    pub lock_path: String,
    /// Lock-derived fencing token or publish permit epoch.
    pub fencing_token: u64,
    /// When the transaction entered prepared state.
    pub prepared_at: DateTime<Utc>,
    /// When the transaction became visible, if it did.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visible_at: Option<DateTime<Utc>>,
    /// Domain-specific result payload once visible.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<TResult>,
}

/// Catalog transaction success receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogTxReceipt {
    /// Transaction identifier.
    pub tx_id: String,
    /// Ledger event identifier.
    pub event_id: String,
    /// Immutable commit receipt identifier.
    pub commit_id: String,
    /// Immutable catalog manifest identifier.
    pub manifest_id: String,
    /// Visible snapshot version.
    pub snapshot_version: u64,
    /// Object-store version/etag observed for the pointer CAS.
    pub pointer_version: String,
    /// Pinned read token for the visible manifest head.
    pub read_token: String,
    /// Visibility timestamp.
    pub visible_at: DateTime<Utc>,
}

/// Orchestration transaction success receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrchestrationTxReceipt {
    /// Transaction identifier.
    pub tx_id: String,
    /// Immutable commit receipt identifier.
    pub commit_id: String,
    /// Immutable orchestration manifest identifier.
    pub manifest_id: String,
    /// Visible orchestration revision ULID.
    pub revision_ulid: String,
    /// Immutable L0 delta identifier.
    pub delta_id: String,
    /// Object-store version/etag observed for the pointer CAS.
    pub pointer_version: String,
    /// Count of events folded into this commit.
    pub events_processed: u32,
    /// Pinned read token for the visible manifest head.
    pub read_token: String,
    /// Visibility timestamp.
    pub visible_at: DateTime<Utc>,
}

/// Per-domain commit reference returned by a root transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DomainCommit {
    /// Domain name (`catalog` or `orchestration`).
    pub domain: ControlPlaneTxDomain,
    /// Domain-local transaction identifier.
    pub tx_id: String,
    /// Immutable commit receipt identifier.
    pub commit_id: String,
    /// Immutable manifest identifier published for this domain.
    pub manifest_id: String,
    /// Immutable manifest path published for this domain.
    pub manifest_path: String,
    /// Pinned per-domain read token.
    pub read_token: String,
}

/// Root transaction success receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootTxReceipt {
    /// Root transaction identifier.
    pub tx_id: String,
    /// Immutable root commit identifier.
    pub root_commit_id: String,
    /// Immutable tx-scoped root super-manifest path.
    pub super_manifest_path: String,
    /// Domain commits published as part of the root transaction.
    pub domain_commits: Vec<DomainCommit>,
    /// Pinned root read token, resolved via `transactions/root/{tx_id}.json`.
    pub read_token: String,
    /// Visibility timestamp.
    pub visible_at: DateTime<Utc>,
}

/// Pinned manifest reference for one domain inside a root super-manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootTxManifestDomain {
    /// Immutable domain manifest identifier.
    pub manifest_id: String,
    /// Immutable domain manifest path.
    pub manifest_path: String,
    /// Immutable domain commit identifier.
    pub commit_id: String,
}

/// Immutable tx-scoped root super-manifest used as a pinned multi-domain read token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RootTxManifest {
    /// Root transaction identifier.
    pub tx_id: String,
    /// Fencing token used to finalize this root read token.
    pub fencing_token: u64,
    /// When the root read token became visible.
    pub published_at: DateTime<Utc>,
    /// Pinned manifest references for participating domains.
    pub domains: BTreeMap<ControlPlaneTxDomain, RootTxManifestDomain>,
}

/// Shared idempotency claim record for control-plane transactions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControlPlaneIdempotencyRecord {
    /// Transaction identifier that owns the claim.
    pub tx_id: String,
    /// Logical operation kind.
    pub kind: ControlPlaneTxKind,
    /// Unique request identifier for this attempt.
    pub request_id: String,
    /// Caller idempotency key.
    pub idempotency_key: String,
    /// Canonical request payload hash.
    pub request_hash: String,
    /// When the claim was created.
    pub created_at: DateTime<Utc>,
    /// When the claimed transaction became visible, if it did.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visible_at: Option<DateTime<Utc>>,
    /// Cached visible transaction record for replay/repair after partial finalize failures.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_record: Option<serde_json::Value>,
}

/// Type alias for catalog transaction records.
pub type CatalogTxRecord = ControlPlaneTxRecord<CatalogTxReceipt>;

/// Type alias for orchestration transaction records.
pub type OrchestrationTxRecord = ControlPlaneTxRecord<OrchestrationTxReceipt>;

/// Type alias for root transaction records.
pub type RootTxRecord = ControlPlaneTxRecord<RootTxReceipt>;

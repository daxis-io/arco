//! Canonical typed path builders for flow/orchestration and Delta artifacts.

use std::fmt::Display;

use crate::error::{Error, Result};
use base64::Engine;
use chrono::NaiveDate;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

/// Typed flow/orchestration path API.
pub struct FlowPaths;

impl FlowPaths {
    /// Prefix for orchestration ledger events.
    pub const ORCHESTRATION_LEDGER_PREFIX: &str = "ledger/orchestration";
    /// Prefix for flow execution ledgers.
    pub const FLOW_LEDGER_PREFIX: &str = "ledger/flow";
    /// Prefix for orchestration state objects.
    pub const ORCHESTRATION_STATE_PREFIX: &str = "state/orchestration";
    /// Canonical orchestration manifest path.
    pub const ORCHESTRATION_MANIFEST_PATH: &str = "state/orchestration/manifest.json";

    /// Canonical path for orchestration event objects.
    #[must_use]
    pub fn orchestration_event_path(date: &str, event_id: &str) -> String {
        format!(
            "{}/{date}/{event_id}.json",
            Self::ORCHESTRATION_LEDGER_PREFIX
        )
    }

    /// Canonical path for flow event objects.
    #[must_use]
    pub fn flow_event_path(domain: &str, date: &str, event_id: &str) -> String {
        format!(
            "{}/{domain}/{date}/{event_id}.json",
            Self::FLOW_LEDGER_PREFIX
        )
    }

    /// Canonical orchestration manifest path.
    #[must_use]
    pub const fn orchestration_manifest_path() -> &'static str {
        Self::ORCHESTRATION_MANIFEST_PATH
    }

    /// Canonical path for orchestration L0 directory.
    #[must_use]
    pub fn orchestration_l0_dir(delta_id: &str) -> String {
        format!("{}/l0/{delta_id}", Self::ORCHESTRATION_STATE_PREFIX)
    }
}

/// Canonical path builders for API non-catalog artifacts.
pub struct ApiPaths;

impl ApiPaths {
    /// Prefix for manifest payloads.
    pub const MANIFEST_PREFIX: &str = "manifests/";
    /// Prefix for manifest deploy idempotency records.
    pub const MANIFEST_IDEMPOTENCY_PREFIX: &str = "manifests/idempotency/";
    /// Prefix for orchestration backfill idempotency records.
    pub const BACKFILL_IDEMPOTENCY_PREFIX: &str = "orchestration/backfills/idempotency/";
    /// Canonical path for latest-manifest index.
    pub const MANIFEST_LATEST_INDEX_PATH: &str = "manifests/_index.json";

    /// Canonical manifest payload path for a manifest id.
    #[must_use]
    pub fn manifest_path(manifest_id: &str) -> String {
        format!("{}{manifest_id}.json", Self::MANIFEST_PREFIX)
    }

    /// Canonical latest-manifest index path.
    #[must_use]
    pub const fn manifest_latest_index_path() -> &'static str {
        Self::MANIFEST_LATEST_INDEX_PATH
    }

    fn encode_idempotency_key(idempotency_key: &str) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(idempotency_key.as_bytes())
    }

    /// Canonical manifest deploy idempotency record path.
    #[must_use]
    pub fn manifest_idempotency_path(idempotency_key: &str) -> String {
        let encoded = Self::encode_idempotency_key(idempotency_key);
        format!("{}{encoded}.json", Self::MANIFEST_IDEMPOTENCY_PREFIX)
    }

    /// Canonical backfill idempotency record path.
    #[must_use]
    pub fn backfill_idempotency_path(idempotency_key: &str) -> String {
        let encoded = Self::encode_idempotency_key(idempotency_key);
        format!("{}{encoded}.json", Self::BACKFILL_IDEMPOTENCY_PREFIX)
    }
}

/// Canonical path builders for Iceberg control-plane artifacts.
pub struct IcebergPaths;

impl IcebergPaths {
    /// Prefix for table pointer objects.
    pub const POINTER_PREFIX: &str = "_catalog/iceberg_pointers";
    /// Prefix for commit idempotency marker objects.
    pub const IDEMPOTENCY_PREFIX: &str = "_catalog/iceberg_idempotency";
    /// Prefix for multi-table transaction records.
    pub const TRANSACTIONS_PREFIX: &str = "_catalog/iceberg_transactions";

    const JSON_SUFFIX: &str = ".json";

    /// Canonical prefix for pointer objects.
    #[must_use]
    pub const fn pointer_prefix() -> &'static str {
        Self::POINTER_PREFIX
    }

    /// Canonical path for a table pointer.
    #[must_use]
    pub fn pointer_path(table_uuid: &Uuid) -> String {
        format!("{}/{table_uuid}{}", Self::POINTER_PREFIX, Self::JSON_SUFFIX)
    }

    /// Canonical prefix for all idempotency markers for one table.
    #[must_use]
    pub fn idempotency_table_prefix(table_uuid: &Uuid) -> String {
        format!("{}/{table_uuid}/", Self::IDEMPOTENCY_PREFIX)
    }

    /// Canonical path for an idempotency marker.
    #[must_use]
    pub fn idempotency_marker_path(table_uuid: &Uuid, key_hash: &str) -> String {
        let prefix = &key_hash[..2.min(key_hash.len())];
        format!(
            "{}/{table_uuid}/{prefix}/{key_hash}{}",
            Self::IDEMPOTENCY_PREFIX,
            Self::JSON_SUFFIX
        )
    }

    /// Canonical path for a transaction record.
    #[must_use]
    pub fn transaction_record_path(tx_id: &str) -> String {
        format!("{}/{tx_id}{}", Self::TRANSACTIONS_PREFIX, Self::JSON_SUFFIX)
    }

    /// Canonical prefix for pending commit receipts on a date.
    #[must_use]
    pub fn pending_receipt_prefix(date: NaiveDate) -> String {
        format!("events/{}/iceberg/pending/", date.format("%Y-%m-%d"))
    }

    /// Canonical prefix for committed commit receipts on a date.
    #[must_use]
    pub fn committed_receipt_prefix(date: NaiveDate) -> String {
        format!("events/{}/iceberg/committed/", date.format("%Y-%m-%d"))
    }

    /// Canonical path for a pending commit receipt.
    #[must_use]
    pub fn pending_receipt_path(date: NaiveDate, commit_key: impl Display) -> String {
        format!(
            "{}{}{}",
            Self::pending_receipt_prefix(date),
            commit_key,
            Self::JSON_SUFFIX
        )
    }

    /// Canonical path for a committed commit receipt.
    #[must_use]
    pub fn committed_receipt_path(date: NaiveDate, commit_key: impl Display) -> String {
        format!(
            "{}{}{}",
            Self::committed_receipt_prefix(date),
            commit_key,
            Self::JSON_SUFFIX
        )
    }

    /// Extracts a pointer UUID from a storage path if it matches canonical form.
    #[must_use]
    pub fn pointer_uuid_from_path(path: &str) -> Option<Uuid> {
        let prefix = format!("{}/", Self::POINTER_PREFIX);
        let filename = path.strip_prefix(&prefix)?;
        let uuid = filename.strip_suffix(Self::JSON_SUFFIX)?;
        Uuid::parse_str(uuid).ok()
    }
}

/// Typed Delta control-plane paths for a single table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaPaths {
    table_id: Uuid,
    table_root: String,
}

impl DeltaPaths {
    /// Creates typed paths rooted at the deterministic legacy location `tables/{table_id}`.
    #[must_use]
    pub fn legacy(table_id: Uuid) -> Self {
        Self {
            table_id,
            table_root: format!("tables/{table_id}"),
        }
    }

    /// Creates typed paths for `table_id` rooted at `table_root`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] if `table_root` is empty.
    pub fn new(table_id: Uuid, table_root: impl AsRef<str>) -> Result<Self> {
        let table_root = normalize_relative_path(table_root.as_ref())?;
        Ok(Self {
            table_id,
            table_root,
        })
    }

    /// Resolves typed paths from optional table location metadata.
    ///
    /// If `table_location` is missing, falls back to legacy root `tables/{table_id}`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when a non-empty location cannot be resolved.
    pub fn from_table_location(
        table_id: Uuid,
        table_location: Option<&str>,
        tenant: &str,
        workspace: &str,
    ) -> Result<Self> {
        let table_root = match table_location {
            Some(location) => resolve_table_location_root(location, tenant, workspace)?,
            None => format!("tables/{table_id}"),
        };
        Self::new(table_id, table_root)
    }

    /// Returns the table identifier associated with these paths.
    #[must_use]
    pub const fn table_id(&self) -> Uuid {
        self.table_id
    }

    /// Returns the scope-relative root path for table data/logs.
    #[must_use]
    pub fn table_root(&self) -> &str {
        &self.table_root
    }

    /// Returns the staging payload path for a ULID.
    #[must_use]
    pub fn staging_payload(&self, ulid: &str) -> String {
        format!("delta/staging/{}/{}.json", self.table_id, ulid)
    }

    /// Returns the coordinator state path.
    #[must_use]
    pub fn coordinator_state(&self) -> String {
        format!("delta/coordinator/{}.json", self.table_id)
    }

    /// Returns the idempotency marker path for an idempotency key hash.
    #[must_use]
    pub fn idempotency_marker_from_hash(&self, key_hash: &str) -> String {
        let prefix = key_hash.get(0..2).unwrap_or("00");
        format!(
            "delta/idempotency/{}/{prefix}/{key_hash}.json",
            self.table_id
        )
    }

    /// Returns the idempotency marker path for a raw idempotency key.
    #[must_use]
    pub fn idempotency_marker(&self, idempotency_key: &str) -> String {
        let key_hash = sha256_hex(idempotency_key.as_bytes());
        self.idempotency_marker_from_hash(&key_hash)
    }

    /// Returns `_delta_log` JSON path for a specific Delta log version.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when `version` is negative.
    pub fn delta_log_json(&self, version: i64) -> Result<String> {
        if version < 0 {
            return Err(Error::Validation {
                message: "delta log version must be non-negative".to_string(),
            });
        }

        Ok(format!("{}/_delta_log/{version:020}.json", self.table_root))
    }
}

/// Resolves a table location to a scope-relative table root directory.
///
/// Accepted forms:
/// - `gs://bucket/tenant=.../workspace=.../warehouse/table`
/// - `tenant=.../workspace=.../warehouse/table`
/// - `warehouse/table`
///
/// # Errors
///
/// Returns [`Error::Validation`] when the resolved path is empty.
pub fn resolve_table_location_root(
    location: &str,
    tenant: &str,
    workspace: &str,
) -> Result<String> {
    let location = location.trim();
    if location.is_empty() {
        return Err(Error::Validation {
            message: "table location must not be empty".to_string(),
        });
    }

    let path = if let Some((_, rest)) = location.split_once("://") {
        let mut parts = rest.splitn(2, '/');
        let _bucket = parts.next();
        parts.next().unwrap_or_default()
    } else {
        location
    };

    let scoped_prefix = format!("tenant={tenant}/workspace={workspace}/");
    let relative = path.strip_prefix(&scoped_prefix).unwrap_or(path);
    normalize_relative_path(relative)
}

fn normalize_relative_path(path: &str) -> Result<String> {
    let normalized = path.trim().trim_matches('/').to_string();
    if normalized.is_empty() {
        return Err(Error::Validation {
            message: "path must not be empty".to_string(),
        });
    }

    if normalized.contains('\\') {
        return Err(Error::Validation {
            message: "backslashes are not allowed in paths".to_string(),
        });
    }

    if normalized.contains('%') {
        return Err(Error::Validation {
            message: "percent-encoding is not allowed in paths".to_string(),
        });
    }

    if normalized.contains('\n') || normalized.contains('\r') || normalized.contains('\0') {
        return Err(Error::Validation {
            message: "control characters are not allowed in paths".to_string(),
        });
    }

    for segment in normalized.split('/') {
        if segment == "." || segment == ".." {
            return Err(Error::Validation {
                message: "path traversal is not allowed".to_string(),
            });
        }
    }

    Ok(normalized)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

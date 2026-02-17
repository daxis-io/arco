//! Canonical path builders for flow/orchestration storage objects.

use std::fmt::Display;

use base64::Engine;
use chrono::NaiveDate;
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

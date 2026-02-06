//! Path resolution helpers for Iceberg metadata locations.

use chrono::NaiveDate;
use uuid::Uuid;

use crate::error::{IcebergError, IcebergResult};
use crate::types::CommitKey;

pub const ICEBERG_POINTER_PREFIX: &str = "_catalog/iceberg_pointers";
pub const ICEBERG_IDEMPOTENCY_PREFIX: &str = "_catalog/iceberg_idempotency";
pub const ICEBERG_TRANSACTIONS_PREFIX: &str = "_catalog/iceberg_transactions";

pub fn iceberg_pointer_path(table_uuid: &Uuid) -> String {
    format!("{ICEBERG_POINTER_PREFIX}/{table_uuid}.json")
}

pub fn iceberg_idempotency_table_prefix(table_uuid: &Uuid) -> String {
    format!("{ICEBERG_IDEMPOTENCY_PREFIX}/{table_uuid}/")
}

pub fn iceberg_idempotency_marker_path(table_uuid: &Uuid, key_hash: &str) -> String {
    let prefix = &key_hash[..2.min(key_hash.len())];
    format!("{ICEBERG_IDEMPOTENCY_PREFIX}/{table_uuid}/{prefix}/{key_hash}.json")
}

pub fn iceberg_transaction_record_path(tx_id: &str) -> String {
    format!("{ICEBERG_TRANSACTIONS_PREFIX}/{tx_id}.json")
}

pub fn iceberg_pending_receipt_prefix(date: NaiveDate) -> String {
    format!("events/{}/iceberg/pending/", date.format("%Y-%m-%d"))
}

pub fn iceberg_committed_receipt_prefix(date: NaiveDate) -> String {
    format!("events/{}/iceberg/committed/", date.format("%Y-%m-%d"))
}

pub fn iceberg_pending_receipt_path(date: NaiveDate, commit_key: &CommitKey) -> String {
    format!(
        "{}{}.json",
        iceberg_pending_receipt_prefix(date),
        commit_key
    )
}

pub fn iceberg_committed_receipt_path(date: NaiveDate, commit_key: &CommitKey) -> String {
    format!(
        "{}{}.json",
        iceberg_committed_receipt_prefix(date),
        commit_key
    )
}

/// Resolves a metadata location into a storage-relative path.
///
/// Accepts absolute URIs (e.g. `gs://bucket/tenant=.../workspace=.../path`)
/// and scoped locations, returning a path relative to the tenant/workspace scope.
pub fn resolve_metadata_path(
    location: &str,
    tenant: &str,
    workspace: &str,
) -> IcebergResult<String> {
    let path = if let Some((_, rest)) = location.split_once("://") {
        let mut parts = rest.splitn(2, '/');
        let _bucket = parts.next();
        parts
            .next()
            .ok_or_else(|| IcebergError::Internal {
                message: format!("Invalid metadata location: {location}"),
            })?
            .to_string()
    } else {
        location.to_string()
    };

    let scoped_prefix = format!("tenant={tenant}/workspace={workspace}/");
    let relative = path
        .strip_prefix(&scoped_prefix)
        .unwrap_or(path.as_str())
        .to_string();

    if relative.is_empty() {
        return Err(IcebergError::Internal {
            message: format!("Invalid metadata location: {location}"),
        });
    }

    Ok(relative)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_metadata_path_with_scheme_and_scope() {
        let location =
            "gs://bucket/tenant=acme/workspace=prod/warehouse/table/metadata/v1.metadata.json";
        let path = resolve_metadata_path(location, "acme", "prod").expect("resolve");
        assert_eq!(path, "warehouse/table/metadata/v1.metadata.json");
    }

    #[test]
    fn test_resolve_metadata_path_with_scope_only() {
        let location = "tenant=acme/workspace=prod/warehouse/table/metadata/v1.metadata.json";
        let path = resolve_metadata_path(location, "acme", "prod").expect("resolve");
        assert_eq!(path, "warehouse/table/metadata/v1.metadata.json");
    }

    #[test]
    fn test_resolve_metadata_path_without_scope() {
        let location = "warehouse/table/metadata/v1.metadata.json";
        let path = resolve_metadata_path(location, "acme", "prod").expect("resolve");
        assert_eq!(path, "warehouse/table/metadata/v1.metadata.json");
    }

    #[test]
    fn test_resolve_metadata_path_rejects_empty_path() {
        let location = "gs://bucket";
        let err = resolve_metadata_path(location, "acme", "prod").expect_err("error");
        assert!(err.to_string().contains("Invalid metadata location"));
    }
}

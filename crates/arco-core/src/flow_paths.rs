//! Typed path helpers for flow/control-plane artifacts.

use crate::error::{Error, Result};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

/// Typed Delta control-plane paths for a single table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaPaths {
    table_id: Uuid,
    table_root: String,
}

impl DeltaPaths {
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
        Ok(format!(
            "{}/_delta_log/{version:020}.json",
            self.table_root.as_str()
        ))
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

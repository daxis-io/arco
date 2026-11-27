//! Allocation and physical placement of Arco-owned managed Delta tables.

use std::sync::Arc;

use arco_core::storage::{StorageBackend, WritePrecondition, WriteResult};
use arco_core::{DeltaPaths, ScopedStorage};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::{CatalogError, Result};
use crate::metastore::publish::load_published_storage_governance;
use crate::storage_governance::PathAuthorityKind;
use crate::storage_governance::path_normalization::GovernedPath;
use crate::writer::Table;

/// Catalog of the owner verification fixture.
pub const CATALOG: &str = "turntable";
/// Schema of the owner verification fixture.
pub const SCHEMA: &str = "verification";
/// Name of the owner verification fixture.
pub const TABLE: &str = "managed_delta_fixture";
/// Non-null BIGINT columns in the owner verification fixture.
pub const TABLE_COLUMNS: [&str; 2] = ["id", "amount"];
/// Denial marker for catalog rows that require a placement.
pub const PLACEMENT_PROPERTY: &str = "arco.managed_delta.placement";
pub(crate) const PLACEMENT_VERSION: &str = "v1";
const MAX_PLACEMENT_BYTES: u64 = 4096;

/// Owner intent. Location, table ID, format, and schema are selected by Arco.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ManagedDeltaAllocation {
    /// Caller operation `UUIDv7`, reused for an identical retry.
    pub operation_id: Uuid,
    /// Authenticated provisioning principal.
    pub owner: String,
    /// Stable caller fingerprint for the provisioning intent.
    pub provisioning_fingerprint: String,
}

impl ManagedDeltaAllocation {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.operation_id.get_version_num() != 7
            || [&self.owner, &self.provisioning_fingerprint]
                .into_iter()
                .any(|value| {
                    value.trim().is_empty()
                        || value.len() > 256
                        || value.chars().any(char::is_control)
                })
        {
            return Err(CatalogError::Validation {
                message:
                    "managed Delta requires a UUIDv7 operation and bounded owner and fingerprint"
                        .to_string(),
            });
        }
        Ok(())
    }
}

/// Deployment-selected S3 bucket and prefix, with the backend opened for that bucket.
#[derive(Clone)]
pub struct ManagedDeltaStorage {
    root: GovernedPath,
    prefix: String,
    backend: Arc<dyn StorageBackend>,
}

impl ManagedDeltaStorage {
    /// Opens the approved analytical root. The resolver receives only the bucket URI.
    ///
    /// # Errors
    ///
    /// Returns invalid configuration or backend initialization errors.
    pub fn open(
        root: &str,
        open_bucket: impl FnOnce(&str) -> arco_core::Result<Arc<dyn StorageBackend>>,
    ) -> arco_core::Result<Self> {
        let invalid_root = || {
            arco_core::Error::InvalidInput(
                "ARCO_MANAGED_DELTA_ROOT requires s3://<bucket>/<nonempty prefix>".to_string(),
            )
        };
        let (bucket, prefix) = root
            .strip_prefix("s3://")
            .and_then(|uri| uri.split_once('/'))
            .ok_or_else(invalid_root)?;
        let prefix = prefix.strip_suffix('/').unwrap_or(prefix);
        if !(3..=63).contains(&bucket.len())
            || !bucket.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b".-".contains(&byte)
            })
            || prefix.is_empty()
            || prefix.starts_with("tenant=")
            || prefix.len() > 512
            || !prefix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"/-_.=".contains(&byte))
        {
            return Err(invalid_root());
        }
        let root = GovernedPath::parse(root).map_err(|_| invalid_root())?;
        Ok(Self {
            root,
            prefix: prefix.to_string(),
            backend: open_bucket(&format!("s3://{bucket}"))?,
        })
    }

    pub(crate) fn root_uri(&self) -> String {
        self.root.canonical_uri()
    }

    fn scoped_uri(&self, catalog: &ScopedStorage, path: &str) -> String {
        format!("{}{}/{path}", self.root_uri(), catalog.scope().prefix())
    }

    fn table_uri(&self, catalog: &ScopedStorage, table_id: Uuid) -> String {
        self.scoped_uri(
            catalog,
            &format!("{}/", DeltaPaths::legacy(table_id).table_root()),
        )
    }

    fn scoped_storage(&self, catalog: &ScopedStorage) -> Result<ScopedStorage> {
        Ok(ScopedStorage::new(
            Arc::clone(&self.backend),
            catalog.tenant_id(),
            catalog.workspace_id(),
        )?
        .with_storage_prefix(&self.prefix)?)
    }

    /// Checks write, read, and cleanup under the current governed analytical root.
    ///
    /// Returns false if overlapping probes remove the temporary object in all three attempts.
    ///
    /// # Errors
    /// Returns governance, storage, or probe integrity errors.
    pub async fn probe(&self, catalog: &ScopedStorage, operation_id: Uuid) -> Result<bool> {
        let path = format!("managed-delta/probes/{operation_id}");
        governed_root(catalog, &self.scoped_uri(catalog, &path)).await?;
        let storage = self.scoped_storage(catalog)?;
        let payload = Bytes::from_static(b"arco-managed-delta-storage-probe-v1");
        for _ in 0..3 {
            storage
                .put_raw(&path, payload.clone(), WritePrecondition::DoesNotExist)
                .await?;
            let observed = async {
                let Some(meta) = storage.head_raw(&path).await? else {
                    return Ok(false);
                };
                if meta.size != payload.len() as u64 {
                    return Err(CatalogError::storage("managed storage probe size mismatch"));
                }
                match storage.get_raw(&path).await {
                    Ok(bytes) if bytes == payload => Ok(true),
                    Ok(_) => Err(CatalogError::storage(
                        "managed storage probe payload mismatch",
                    )),
                    Err(arco_core::Error::NotFound(_)) => Ok(false),
                    Err(error) => Err(error.into()),
                }
            }
            .await;
            match observed {
                // Another request can delete the key while this request is reading it.
                Ok(false) => {}
                result => {
                    storage.delete(&path).await?;
                    result?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

/// Immutable binding published before its table becomes visible.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ManagedDeltaPlacement {
    /// UUID of the allocated table incarnation.
    pub table_id: Uuid,
    /// Exact S3 URI of the Delta table.
    pub location: String,
    /// Schema digest of the fixed owner verification fixture.
    pub schema_digest: String,
    /// Original owner allocation intent.
    pub allocation: ManagedDeltaAllocation,
    approved_root: String,
    tenant: String,
    workspace: String,
    managed_root_id: String,
}

impl ManagedDeltaPlacement {
    pub(crate) fn schema_digest() -> String {
        let digest = Sha256::digest(b"id:BIGINT:NOT NULL;amount:BIGINT:NOT NULL");
        format!("sha256:{digest:x}")
    }

    pub(crate) async fn create(
        catalog: &ScopedStorage,
        root: &ManagedDeltaStorage,
        table_id: Uuid,
        allocation: &ManagedDeltaAllocation,
    ) -> Result<Self> {
        let location = root.table_uri(catalog, table_id);
        let managed_root_id = governed_root(catalog, &location).await?;
        let placement = Self {
            table_id,
            location,
            schema_digest: Self::schema_digest(),
            allocation: allocation.clone(),
            approved_root: root.root_uri(),
            tenant: catalog.tenant_id().to_string(),
            workspace: catalog.workspace_id().to_string(),
            managed_root_id,
        };
        let bytes = serde_json::to_vec(&placement).map_err(CatalogError::storage)?;
        if bytes.len() as u64 > MAX_PLACEMENT_BYTES {
            return Err(placement_mismatch());
        }
        let written = catalog
            .put_raw(
                &placement_path(table_id),
                bytes.into(),
                WritePrecondition::DoesNotExist,
            )
            .await?;
        if matches!(written, WriteResult::PreconditionFailed { .. })
            && load_placement(catalog, table_id).await?.as_ref() != Some(&placement)
        {
            return Err(placement_mismatch());
        }
        Ok(placement)
    }

    pub(crate) async fn for_replay(
        catalog: &ScopedStorage,
        root: &ManagedDeltaStorage,
        table: &Table,
        intent: &ManagedDeltaAllocation,
    ) -> Result<Self> {
        let placement = load_placement(catalog, parse_table_id(table)?)
            .await?
            .ok_or_else(placement_mismatch)?;
        if &placement.allocation != intent {
            return Err(placement_mismatch());
        }
        placement.verify(catalog, root, table).await?;
        Ok(placement)
    }

    async fn verify(
        &self,
        catalog: &ScopedStorage,
        root: &ManagedDeltaStorage,
        table: &Table,
    ) -> Result<()> {
        if self.table_id != parse_table_id(table)?
            || self.tenant != catalog.tenant_id()
            || self.workspace != catalog.workspace_id()
            || self.approved_root != root.root_uri()
            || self.location != root.table_uri(catalog, self.table_id)
            || table.location.as_deref() != Some(&self.location)
            || table.table_type.as_deref() != Some("MANAGED")
            || table.format.as_deref() != Some("delta")
            || placement_marker(table) != Some(PLACEMENT_VERSION)
            || self.schema_digest != Self::schema_digest()
        {
            return Err(placement_mismatch());
        }
        if governed_root(catalog, &self.location).await? != self.managed_root_id {
            return Err(placement_mismatch());
        }
        Ok(())
    }
}

pub(crate) fn placement_marker(table: &Table) -> Option<&str> {
    table
        .properties
        .as_ref()?
        .get(PLACEMENT_PROPERTY)
        .map(String::as_str)
}

fn parse_table_id(table: &Table) -> Result<Uuid> {
    Uuid::parse_str(&table.id).map_err(|_| CatalogError::Validation {
        message: "Delta table id must be a UUID".to_string(),
    })
}

fn placement_path(table_id: Uuid) -> String {
    format!("delta/placements/{table_id}.json")
}

fn placement_mismatch() -> CatalogError {
    CatalogError::RequestFailed {
        http_status: 409,
        message: "managed_delta_placement_mismatch".to_string(),
    }
}

async fn load_placement(
    catalog: &ScopedStorage,
    table_id: Uuid,
) -> Result<Option<ManagedDeltaPlacement>> {
    let path = placement_path(table_id);
    let Some(meta) = catalog.head_raw(&path).await? else {
        return Ok(None);
    };
    if !(1..=MAX_PLACEMENT_BYTES).contains(&meta.size) {
        return Err(placement_mismatch());
    }
    let bytes = catalog.get_range(&path, 0..meta.size).await?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| placement_mismatch())
}

async fn governed_root(catalog: &ScopedStorage, uri: &str) -> Result<String> {
    let published = load_published_storage_governance(catalog).await?;
    let decision = published
        .state
        .authority_for_path(catalog.workspace_id(), uri);
    match decision {
        Ok(authority) if authority.authority_kind == PathAuthorityKind::ManagedRoot => {
            Ok(authority.object_id)
        }
        _ => Err(CatalogError::RequestFailed {
            http_status: 403,
            message: "managed_delta_root_not_governed".to_string(),
        }),
    }
}

/// Tests whether the table has an allocation record, including after a catalog drop.
///
/// # Errors
///
/// Returns catalog storage errors.
pub async fn has_placement(catalog: &ScopedStorage, table_id: Uuid) -> Result<bool> {
    Ok(catalog.head_raw(&placement_path(table_id)).await?.is_some())
}

/// Resolves the physical Delta store from a verified table placement.
/// Legacy tables that do not claim the configured root return `None`.
///
/// # Errors
///
/// Returns an error for unavailable configuration, denied storage governance,
/// or a missing, corrupt, or mismatched placement.
pub async fn resolve_analytical_storage(
    configured: Option<&ManagedDeltaStorage>,
    catalog: &ScopedStorage,
    table: &Table,
) -> Result<Option<ScopedStorage>> {
    let Some(placement) = load_placement(catalog, parse_table_id(table)?).await? else {
        let claims_root =
            configured
                .zip(table.location.as_deref())
                .is_some_and(|(storage, location)| {
                    GovernedPath::parse(location).is_ok_and(|path| storage.root.contains(&path))
                });
        if placement_marker(table).is_some() || claims_root {
            return Err(placement_mismatch());
        }
        return Ok(None);
    };
    let configured = configured.ok_or_else(|| CatalogError::RequestFailed {
        http_status: 503,
        message: "managed_delta_storage_unavailable".to_string(),
    })?;
    placement.verify(catalog, configured, table).await?;
    configured.scoped_storage(catalog).map(Some)
}

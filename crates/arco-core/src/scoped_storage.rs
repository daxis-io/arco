//! Tenant + workspace scoped storage with architecture-aligned path layout.
//!
//! This module enforces the documented storage layout for multi-tenant, multi-workspace
//! catalog operations. All paths are prefixed with `tenant={tenant}/workspace={workspace}/`.
//! Per unified platform design: tenant + workspace = primary scoping boundary.
//!
//! The key=value path format provides:
//! - Operational ergonomics (grep-friendly: `tenant=acme` is self-documenting)
//! - Consistent with Hive partition conventions
//! - Easy extraction of scope from any path
//!
//! # Security
//!
//! This module enforces strict path isolation:
//! - All paths are prefixed with tenant/workspace scope
//! - Path traversal attempts (`..`) are rejected
//! - Tenant/workspace IDs are validated at construction

use bytes::Bytes;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use crate::catalog_paths::{CatalogDomain, CatalogPaths};
use crate::error::{Error, Result};
use crate::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use async_trait::async_trait;

/// Tenant + workspace scoped storage wrapper.
///
/// Enforces isolation by prefixing all paths with `tenant={tenant}/workspace={workspace}/`.
/// Path helpers align with the documented catalog storage layout.
#[derive(Clone)]
pub struct ScopedStorage {
    backend: Arc<dyn StorageBackend>,
    tenant_id: String,
    workspace_id: String,
}

/// Metadata about an object relative to a tenant/workspace scope.
#[derive(Debug, Clone)]
pub struct ScopedObjectMeta {
    /// Object path relative to the scope (no `tenant=.../workspace=.../` prefix).
    pub path: ScopedPath,
    /// Object size in bytes.
    pub size: u64,
    /// Object version token for CAS operations.
    pub version: String,
    /// Last modification timestamp, if provided by the backend.
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
    /// Entity tag for cache validation.
    pub etag: Option<String>,
}

impl ScopedStorage {
    /// Creates a new scoped storage wrapper.
    ///
    /// # Errors
    ///
    /// Returns an error if `tenant_id` or `workspace_id` is invalid.
    /// IDs must be non-empty, ASCII lowercase alphanumeric (plus `-` and `_`),
    /// and must not contain path separators or other control characters.
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
    ) -> Result<Self> {
        let tenant_id = tenant_id.into();
        let workspace_id = workspace_id.into();

        Self::validate_id(&tenant_id, "tenant_id")?;
        Self::validate_id(&workspace_id, "workspace_id")?;

        Ok(Self {
            backend,
            tenant_id,
            workspace_id,
        })
    }

    /// Validates an ID for use in paths.
    fn validate_id(id: &str, field: &str) -> Result<()> {
        if id.is_empty() {
            return Err(Error::InvalidId {
                message: format!("{field} cannot be empty"),
            });
        }

        if id.contains('/') || id.contains('\\') {
            return Err(Error::InvalidId {
                message: format!("{field} cannot contain path separators"),
            });
        }

        if id.contains('\n') || id.contains('\r') || id.contains('\0') {
            return Err(Error::InvalidId {
                message: format!("{field} cannot contain control characters"),
            });
        }

        if !id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
        {
            return Err(Error::InvalidId {
                message: format!(
                    "{field} contains invalid characters (allowed: a-z, 0-9, '-', '_')"
                ),
            });
        }

        Ok(())
    }

    /// Validates a relative path for traversal and encoding attacks.
    ///
    /// This is the canonical path validator used by scoped APIs to reject
    /// absolute paths, `.`/`..` segments, percent-encoding tricks, and control
    /// characters before path-prefixing is applied.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidInput` when the path is unsafe.
    pub fn validate_path(path: &str) -> Result<()> {
        if path.starts_with('/') || path.starts_with('\\') {
            return Err(Error::InvalidInput(format!(
                "absolute paths not allowed: {path}"
            )));
        }

        if path.contains('\\') {
            return Err(Error::InvalidInput(format!(
                "backslashes not allowed in paths: {path}"
            )));
        }

        if path.contains('%') {
            return Err(Error::InvalidInput(format!(
                "percent-encoding not allowed in paths: {path}"
            )));
        }

        if path.contains('\n') || path.contains('\r') || path.contains('\0') {
            return Err(Error::InvalidInput(format!(
                "control characters not allowed in paths: {path}"
            )));
        }

        for segment in path.split('/') {
            if segment == "." || segment == ".." {
                return Err(Error::InvalidInput(format!(
                    "path traversal not allowed: {path}"
                )));
            }
        }

        Ok(())
    }

    /// Returns the tenant ID.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the workspace ID.
    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    /// Returns the backend for advanced operations.
    #[must_use]
    pub fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    // === Path Construction ===

    fn scope_prefix(&self) -> String {
        format!("tenant={}/workspace={}", self.tenant_id, self.workspace_id)
    }

    fn scoped_path(&self, path: &str) -> String {
        format!("{}/{}", self.scope_prefix(), path)
    }

    // =========================================================================
    // LEGACY PATH HELPERS (deprecated per ADR-003)
    //
    // These methods use old domain naming conventions (core, execution, governance).
    // Use the canonical methods below (manifest, lock, snapshot_dir, etc.) instead.
    //
    // Deprecation timeline:
    // - v0.1.x: Deprecated with warnings
    // - v0.2.x: Removed
    // =========================================================================

    /// Path to the root catalog manifest.
    ///
    /// This is the entry point - readers load this first to find domain manifests.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `manifest_root()` instead. See ADR-003 for canonical domain naming."
    )]
    pub fn root_manifest_path(&self) -> String {
        self.scoped_path("manifests/root.manifest.json")
    }

    /// Path to the core domain manifest.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `manifest(CatalogDomain::Catalog)` instead. See ADR-003 for canonical domain naming."
    )]
    pub fn core_manifest_path(&self) -> String {
        self.scoped_path("manifests/core.manifest.json")
    }

    /// Path to the execution domain manifest.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `manifest(CatalogDomain::Executions)` instead. See ADR-003 for canonical domain naming."
    )]
    pub fn execution_manifest_path(&self) -> String {
        self.scoped_path("manifests/execution.manifest.json")
    }

    /// Path to the distributed lock file for core catalog operations.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `lock(CatalogDomain::Catalog)` instead. See ADR-003 for canonical domain naming."
    )]
    pub fn core_lock_path(&self) -> String {
        self.scoped_path("locks/core.lock")
    }

    /// Path to a catalog snapshot directory.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `snapshot_dir(CatalogDomain::Catalog, version)` instead. See ADR-003/ADR-005 for canonical paths."
    )]
    pub fn core_snapshot_path(&self, version: u64) -> String {
        self.scoped_path(&format!("core/snapshots/v{version}/"))
    }

    /// Path to a specific commit record.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `commit(CatalogDomain::Catalog, commit_id)` instead. See ADR-003/ADR-005 for canonical paths."
    )]
    pub fn core_commit_path(&self, commit_id: &str) -> String {
        self.scoped_path(&format!("core/commits/{commit_id}.json"))
    }

    // === Ledger Paths (Tier 2 Input - Append-Only Events) ===

    /// Path to event log partition (domain/date).
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `ledger_dir(CatalogDomain)` or `ledger_event(CatalogDomain, event_id)` instead. See ADR-005 for canonical paths."
    )]
    pub fn ledger_path(&self, domain: &str, date: &str) -> String {
        self.scoped_path(&format!("ledger/{domain}/{date}/"))
    }

    // === State Paths (Tier 2 Output - Compacted Parquet) ===

    /// Path to compacted state table.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `state_dir(CatalogDomain)` or `state_snapshot(CatalogDomain, version, ulid)` instead. See ADR-005 for canonical paths."
    )]
    pub fn state_path(&self, domain: &str, table: &str) -> String {
        self.scoped_path(&format!("state/{domain}/{table}/"))
    }

    // === Lineage Domain ===

    /// Path to lineage domain manifest.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `manifest(CatalogDomain::Lineage)` instead. See ADR-003 for canonical domain naming."
    )]
    pub fn lineage_manifest_path(&self) -> String {
        self.scoped_path("manifests/lineage.manifest.json")
    }

    /// Path to governance domain manifest.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `manifest(CatalogDomain::Search)` instead. 'governance' is now 'search'. See ADR-003."
    )]
    pub fn governance_manifest_path(&self) -> String {
        self.scoped_path("manifests/governance.manifest.json")
    }

    /// Path to lineage edges state.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `state_dir(CatalogDomain::Lineage)` instead. See ADR-005 for canonical paths."
    )]
    pub fn lineage_edges_path(&self) -> String {
        self.scoped_path("state/lineage/edges/")
    }

    // === Governance Domain ===

    /// Path to governance tags table.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `state_dir(CatalogDomain::Search)` instead. 'governance' is now 'search'. See ADR-003."
    )]
    pub fn governance_tags_path(&self) -> String {
        self.scoped_path("governance/tags.parquet")
    }

    /// Path to governance owners table.
    #[must_use]
    #[deprecated(
        since = "0.1.0",
        note = "Use `state_dir(CatalogDomain::Search)` instead. 'governance' is now 'search'. See ADR-003."
    )]
    pub fn governance_owners_path(&self) -> String {
        self.scoped_path("governance/owners.parquet")
    }

    // =========================================================================
    // Canonical Path Methods (per ADR-003, ADR-005)
    //
    // These methods use CatalogPaths for consistency. Use these over the
    // legacy methods above when writing new code.
    // =========================================================================

    /// Path to the root manifest (canonical).
    ///
    /// This is the entry point for all readers per ADR-005.
    #[must_use]
    pub fn manifest_root(&self) -> String {
        self.scoped_path(CatalogPaths::ROOT_MANIFEST)
    }

    /// Path to a domain manifest (canonical).
    #[must_use]
    pub fn manifest(&self, domain: CatalogDomain) -> String {
        self.scoped_path(&CatalogPaths::domain_manifest(domain))
    }

    /// Path to a domain manifest pointer (canonical).
    #[must_use]
    pub fn manifest_pointer(&self, domain: CatalogDomain) -> String {
        self.scoped_path(&CatalogPaths::domain_manifest_pointer(domain))
    }

    /// Path to an immutable domain manifest snapshot (canonical).
    #[must_use]
    pub fn manifest_snapshot(&self, domain: CatalogDomain, manifest_id: &str) -> String {
        self.scoped_path(&CatalogPaths::domain_manifest_snapshot(domain, manifest_id))
    }

    /// Path to a domain lock file (canonical).
    #[must_use]
    pub fn lock(&self, domain: CatalogDomain) -> String {
        self.scoped_path(&CatalogPaths::domain_lock(domain))
    }

    /// Path to a commit record (canonical).
    #[must_use]
    pub fn commit(&self, domain: CatalogDomain, commit_id: &str) -> String {
        self.scoped_path(&CatalogPaths::commit(domain, commit_id))
    }

    /// Path to a Tier-1 snapshot directory (canonical).
    #[must_use]
    pub fn snapshot_dir(&self, domain: CatalogDomain, version: u64) -> String {
        self.scoped_path(&CatalogPaths::snapshot_dir(domain, version))
    }

    /// Path to a Tier-1 snapshot file (canonical).
    #[must_use]
    pub fn snapshot_file(&self, domain: CatalogDomain, version: u64, filename: &str) -> String {
        self.scoped_path(&CatalogPaths::snapshot_file(domain, version, filename))
    }

    /// Path to a Tier-2 ledger event (canonical).
    #[must_use]
    pub fn ledger_event(&self, domain: CatalogDomain, event_id: &str) -> String {
        self.scoped_path(&CatalogPaths::ledger_event(domain, event_id))
    }

    /// Path to a Tier-2 ledger directory (canonical).
    #[must_use]
    pub fn ledger_dir(&self, domain: CatalogDomain) -> String {
        self.scoped_path(&CatalogPaths::ledger_dir(domain))
    }

    /// Path to a Tier-2 state snapshot (canonical).
    #[must_use]
    pub fn state_snapshot(&self, domain: CatalogDomain, version: u64, ulid: &str) -> String {
        self.scoped_path(&CatalogPaths::state_snapshot(domain, version, ulid))
    }

    /// Path to a Tier-2 state directory (canonical).
    #[must_use]
    pub fn state_dir(&self, domain: CatalogDomain) -> String {
        self.scoped_path(&CatalogPaths::state_dir(domain))
    }

    // === High-Level Operations ===

    /// Gets the core manifest.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest is not found or a storage error occurs.
    #[deprecated(
        since = "0.1.0",
        note = "Use canonical methods with CatalogDomain::Catalog. See ADR-003."
    )]
    #[allow(deprecated)]
    pub async fn get_core_manifest(&self) -> Result<Bytes> {
        self.backend.get(&self.core_manifest_path()).await
    }

    /// Puts the core manifest with precondition.
    ///
    /// # Errors
    ///
    /// Returns an error if a storage error occurs.
    #[deprecated(
        since = "0.1.0",
        note = "Use canonical methods with CatalogDomain::Catalog. See ADR-003."
    )]
    #[allow(deprecated)]
    pub async fn put_core_manifest(
        &self,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        self.backend
            .put(&self.core_manifest_path(), data, precondition)
            .await
    }

    /// Gets the core manifest metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if a storage error occurs.
    #[deprecated(
        since = "0.1.0",
        note = "Use canonical methods with CatalogDomain::Catalog. See ADR-003."
    )]
    #[allow(deprecated)]
    pub async fn head_core_manifest(&self) -> Result<Option<ObjectMeta>> {
        self.backend.head(&self.core_manifest_path()).await
    }

    // === Raw Operations (for paths not covered by helpers) ===

    /// Reads data at a scope-relative path.
    ///
    /// # Errors
    ///
    /// Returns an error if the path contains traversal sequences or the object is not found.
    pub async fn get_raw(&self, path: &str) -> Result<Bytes> {
        Self::validate_path(path)?;
        self.backend.get(&self.scoped_path(path)).await
    }

    /// Writes data at a scope-relative path.
    ///
    /// # Errors
    ///
    /// Returns an error if the path contains traversal sequences.
    pub async fn put_raw(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        Self::validate_path(path)?;
        self.backend
            .put(&self.scoped_path(path), data, precondition)
            .await
    }

    /// Deletes data at a scope-relative path.
    ///
    /// # Errors
    ///
    /// Returns an error if the path contains traversal sequences.
    pub async fn delete(&self, path: &str) -> Result<()> {
        Self::validate_path(path)?;
        self.backend.delete(&self.scoped_path(path)).await
    }

    /// Lists objects at a scope-relative path prefix.
    ///
    /// Returns relative paths (without the scope prefix).
    ///
    /// # Errors
    ///
    /// Returns an error if the prefix contains traversal sequences.
    pub async fn list(&self, prefix: &str) -> Result<Vec<ScopedPath>> {
        Self::validate_path(prefix)?;
        let full_prefix = self.scoped_path(prefix);
        let scope_prefix = format!("{}/", self.scope_prefix());

        let metas = self.backend.list(&full_prefix).await?;

        Ok(metas
            .into_iter()
            .filter_map(|m| {
                m.path
                    .strip_prefix(&scope_prefix)
                    .map(|p| ScopedPath(p.to_string()))
            })
            .collect())
    }

    /// Lists objects and returns metadata at a scope-relative prefix.
    ///
    /// Returned paths are relative to the scope and may be safely passed back to
    /// other `ScopedStorage` methods (e.g., `get_raw`).
    ///
    /// # Errors
    ///
    /// Returns an error if the prefix contains traversal sequences or listing fails.
    pub async fn list_meta(&self, prefix: &str) -> Result<Vec<ScopedObjectMeta>> {
        Self::validate_path(prefix)?;
        let full_prefix = self.scoped_path(prefix);
        let scope_prefix = format!("{}/", self.scope_prefix());

        let metas = self.backend.list(&full_prefix).await?;
        Ok(metas
            .into_iter()
            .filter_map(|m| {
                m.path
                    .strip_prefix(&scope_prefix)
                    .map(|relative| ScopedObjectMeta {
                        path: ScopedPath(relative.to_string()),
                        size: m.size,
                        version: m.version,
                        last_modified: m.last_modified,
                        etag: m.etag,
                    })
            })
            .collect())
    }

    /// Gets metadata at a scope-relative path.
    ///
    /// # Errors
    ///
    /// Returns an error if the path contains traversal sequences.
    pub async fn head_raw(&self, path: &str) -> Result<Option<ObjectMeta>> {
        Self::validate_path(path)?;
        self.backend.head(&self.scoped_path(path)).await
    }

    /// Generates signed URL for a scope-relative path.
    ///
    /// # Errors
    ///
    /// Returns an error if the path contains traversal sequences.
    pub async fn signed_url_raw(&self, path: &str, expiry: Duration) -> Result<String> {
        Self::validate_path(path)?;
        self.backend
            .signed_url(&self.scoped_path(path), expiry)
            .await
    }
}

/// A path relative to a scope, for display in list results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedPath(String);

impl ScopedPath {
    /// Returns the path as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ScopedPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Allow scoped storage to satisfy StorageBackend for components that expect it.
#[async_trait]
impl StorageBackend for ScopedStorage {
    async fn get(&self, path: &str) -> Result<Bytes> {
        self.get_raw(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        Self::validate_path(path)?;
        self.backend.get_range(&self.scoped_path(path), range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> Result<WriteResult> {
        self.put_raw(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        Self::validate_path(path)?;
        self.backend.delete(&self.scoped_path(path)).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let metas = self.list_meta(prefix).await?;
        Ok(metas
            .into_iter()
            .map(|meta| ObjectMeta {
                path: meta.path.to_string(),
                size: meta.size,
                version: meta.version,
                last_modified: meta.last_modified,
                etag: meta.etag,
            })
            .collect())
    }

    async fn head(&self, path: &str) -> Result<Option<ObjectMeta>> {
        self.head_raw(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> Result<String> {
        self.signed_url_raw(path, expiry).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryBackend;

    // =========================================================================
    // Legacy Path Tests (backwards compatibility during migration)
    // These tests verify deprecated methods still work correctly.
    // =========================================================================

    #[test]
    #[allow(deprecated)]
    fn test_legacy_core_paths() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        // Legacy paths still work (for backwards compatibility during migration)
        assert_eq!(
            storage.root_manifest_path(),
            "tenant=acme/workspace=production/manifests/root.manifest.json"
        );
        assert_eq!(
            storage.core_manifest_path(),
            "tenant=acme/workspace=production/manifests/core.manifest.json"
        );
        assert_eq!(
            storage.execution_manifest_path(),
            "tenant=acme/workspace=production/manifests/execution.manifest.json"
        );
        assert_eq!(
            storage.core_lock_path(),
            "tenant=acme/workspace=production/locks/core.lock"
        );
        assert_eq!(
            storage.core_snapshot_path(42),
            "tenant=acme/workspace=production/core/snapshots/v42/"
        );
        assert_eq!(
            storage.core_commit_path("commit_abc123"),
            "tenant=acme/workspace=production/core/commits/commit_abc123.json"
        );
    }

    #[test]
    #[allow(deprecated)]
    fn test_legacy_ledger_paths() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        assert_eq!(
            storage.ledger_path("execution", "2025-01-15"),
            "tenant=acme/workspace=production/ledger/execution/2025-01-15/"
        );
        assert_eq!(
            storage.ledger_path("quality", "2025-01-15"),
            "tenant=acme/workspace=production/ledger/quality/2025-01-15/"
        );
        assert_eq!(
            storage.ledger_path("lineage", "2025-01-15"),
            "tenant=acme/workspace=production/ledger/lineage/2025-01-15/"
        );
    }

    #[test]
    #[allow(deprecated)]
    fn test_legacy_state_paths() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        assert_eq!(
            storage.state_path("execution", "materializations"),
            "tenant=acme/workspace=production/state/execution/materializations/"
        );
        assert_eq!(
            storage.state_path("lineage", "edges"),
            "tenant=acme/workspace=production/state/lineage/edges/"
        );
    }

    #[test]
    #[allow(deprecated)]
    fn test_legacy_governance_paths() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        assert_eq!(
            storage.governance_tags_path(),
            "tenant=acme/workspace=production/governance/tags.parquet"
        );
        assert_eq!(
            storage.governance_owners_path(),
            "tenant=acme/workspace=production/governance/owners.parquet"
        );
    }

    #[tokio::test]
    async fn test_tenant_workspace_isolation() {
        let backend = Arc::new(MemoryBackend::new());

        // Same tenant, different workspaces
        let prod = ScopedStorage::new(backend.clone(), "acme", "production").unwrap();
        let staging = ScopedStorage::new(backend.clone(), "acme", "staging").unwrap();

        prod.put_raw(
            "test.txt",
            Bytes::from("prod-data"),
            WritePrecondition::None,
        )
        .await
        .expect("put should succeed");
        staging
            .put_raw(
                "test.txt",
                Bytes::from("staging-data"),
                WritePrecondition::None,
            )
            .await
            .expect("put should succeed");

        // Each workspace sees only their own data
        let prod_data = prod.get_raw("test.txt").await.expect("get should succeed");
        let staging_data = staging
            .get_raw("test.txt")
            .await
            .expect("get should succeed");

        assert_eq!(prod_data, Bytes::from("prod-data"));
        assert_eq!(staging_data, Bytes::from("staging-data"));
    }

    #[tokio::test]
    async fn test_different_tenants_isolation() {
        let backend = Arc::new(MemoryBackend::new());

        let acme = ScopedStorage::new(backend.clone(), "acme", "production").unwrap();
        let globex = ScopedStorage::new(backend.clone(), "globex", "production").unwrap();

        acme.put_raw(
            "test.txt",
            Bytes::from("acme-data"),
            WritePrecondition::None,
        )
        .await
        .expect("put should succeed");
        globex
            .put_raw(
                "test.txt",
                Bytes::from("globex-data"),
                WritePrecondition::None,
            )
            .await
            .expect("put should succeed");

        let acme_data = acme.get_raw("test.txt").await.expect("get should succeed");
        let globex_data = globex
            .get_raw("test.txt")
            .await
            .expect("get should succeed");

        assert_eq!(acme_data, Bytes::from("acme-data"));
        assert_eq!(globex_data, Bytes::from("globex-data"));
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_legacy_catalog_path_operations() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        // Write to core manifest path (legacy method, testing backwards compatibility)
        let manifest_data = Bytes::from(r#"{"version": 1}"#);
        storage
            .put_core_manifest(manifest_data.clone(), WritePrecondition::None)
            .await
            .expect("put should succeed");

        // Read back
        let retrieved = storage
            .get_core_manifest()
            .await
            .expect("get should succeed");
        assert_eq!(retrieved, manifest_data);
    }

    // =========================================================================
    // ISOLATION BOUNDARY TESTS - Critical for multi-tenant security
    // =========================================================================

    #[tokio::test]
    async fn test_path_traversal_prevention() {
        // CRITICAL: ScopedStorage must reject path traversal attacks.
        // Attempts to escape the scope boundary via ".." must fail.

        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        // Attempt to write outside scope via path traversal
        let traversal_paths = [
            "../other_workspace/secret.txt",
            "../../other_tenant/data.txt",
            "manifests/../../../escape.txt",
            "ledger/execution/..%2F..%2Fsecret.txt", // URL-encoded
            "ledger/execution/%2e%2e/%2e%2e/secret.txt",
            "ledger/../../../etc/passwd",
        ];

        for path in &traversal_paths {
            let result = storage
                .put_raw(path, Bytes::from("attack"), WritePrecondition::None)
                .await;
            assert!(result.is_err(), "path traversal must be rejected: {path}");
        }

        // Similarly for reads
        for path in &traversal_paths {
            let result = storage.get_raw(path).await;
            assert!(
                result.is_err(),
                "path traversal read must be rejected: {path}"
            );
        }
    }

    #[tokio::test]
    async fn test_list_isolation_boundary() {
        // INVARIANT: list operations must be scoped to tenant+workspace.
        // One scope cannot enumerate another scope's files.

        let backend = Arc::new(MemoryBackend::new());

        let acme_prod = ScopedStorage::new(backend.clone(), "acme", "production").unwrap();
        let acme_staging = ScopedStorage::new(backend.clone(), "acme", "staging").unwrap();
        let globex_prod = ScopedStorage::new(backend.clone(), "globex", "production").unwrap();

        // Write files to each scope
        acme_prod
            .put_raw(
                "secret1.txt",
                Bytes::from("acme-prod"),
                WritePrecondition::None,
            )
            .await
            .expect("put");
        acme_staging
            .put_raw(
                "secret2.txt",
                Bytes::from("acme-staging"),
                WritePrecondition::None,
            )
            .await
            .expect("put");
        globex_prod
            .put_raw(
                "secret3.txt",
                Bytes::from("globex-prod"),
                WritePrecondition::None,
            )
            .await
            .expect("put");

        // Each scope should only see its own files
        let acme_prod_files = acme_prod.list("").await.expect("list");
        let acme_staging_files = acme_staging.list("").await.expect("list");
        let globex_prod_files = globex_prod.list("").await.expect("list");

        assert_eq!(acme_prod_files.len(), 1, "acme/prod sees only its file");
        assert_eq!(
            acme_staging_files.len(),
            1,
            "acme/staging sees only its file"
        );
        assert_eq!(globex_prod_files.len(), 1, "globex/prod sees only its file");

        // Verify correct files visible
        assert!(acme_prod_files[0].as_str().contains("secret1.txt"));
        assert!(acme_staging_files[0].as_str().contains("secret2.txt"));
        assert!(globex_prod_files[0].as_str().contains("secret3.txt"));
    }

    #[tokio::test]
    async fn test_cross_scope_read_fails() {
        // INVARIANT: ScopedStorage instances cannot read each other's data
        // even if the underlying paths are known.

        let backend = Arc::new(MemoryBackend::new());

        let acme = ScopedStorage::new(backend.clone(), "acme", "production").unwrap();
        let globex = ScopedStorage::new(backend.clone(), "globex", "production").unwrap();

        // Write data under acme scope
        acme.put_raw(
            "secret.txt",
            Bytes::from("acme-secret"),
            WritePrecondition::None,
        )
        .await
        .expect("put");

        // Verify acme can read its own data
        let acme_data = acme.get_raw("secret.txt").await.expect("should succeed");
        assert_eq!(acme_data, Bytes::from("acme-secret"));

        // Globex cannot read acme's data using the same relative path
        let globex_result = globex.get_raw("secret.txt").await;
        assert!(globex_result.is_err(), "cross-scope read must fail");
    }

    #[tokio::test]
    async fn test_cross_scope_delete_fails() {
        // INVARIANT: ScopedStorage cannot delete files in other scopes.

        let backend = Arc::new(MemoryBackend::new());

        let acme = ScopedStorage::new(backend.clone(), "acme", "production").unwrap();
        let globex = ScopedStorage::new(backend.clone(), "globex", "production").unwrap();

        // Write data under acme scope
        acme.put_raw(
            "important.txt",
            Bytes::from("critical-data"),
            WritePrecondition::None,
        )
        .await
        .expect("put");

        // Globex tries to delete acme's file using same relative path
        // This is a no-op since globex's path doesn't exist
        let _ = globex.delete("important.txt").await;

        // Verify acme's data is still intact
        let acme_data = acme
            .get_raw("important.txt")
            .await
            .expect("should still exist");
        assert_eq!(acme_data, Bytes::from("critical-data"));
    }

    #[test]
    fn test_empty_tenant_id_returns_error() {
        let backend = Arc::new(MemoryBackend::new());
        assert!(ScopedStorage::new(backend, "", "workspace").is_err());
    }

    #[test]
    fn test_tenant_with_slash_returns_error() {
        let backend = Arc::new(MemoryBackend::new());
        assert!(ScopedStorage::new(backend, "tenant/evil", "workspace").is_err());
    }

    #[test]
    fn test_tenant_with_dots_returns_error() {
        let backend = Arc::new(MemoryBackend::new());
        assert!(ScopedStorage::new(backend, "tenant..name", "workspace").is_err());
    }

    #[test]
    fn test_tenant_with_space_returns_error() {
        let backend = Arc::new(MemoryBackend::new());
        assert!(ScopedStorage::new(backend, "tenant name", "workspace").is_err());
    }

    #[test]
    fn test_tenant_with_newline_returns_error() {
        let backend = Arc::new(MemoryBackend::new());
        assert!(ScopedStorage::new(backend, "tenant\nname", "workspace").is_err());
    }

    #[tokio::test]
    async fn test_valid_relative_paths() {
        // Normal paths should work fine
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        storage
            .put_raw("file.txt", Bytes::from("data"), WritePrecondition::None)
            .await
            .expect("put");

        let data = storage.get_raw("file.txt").await.expect("get");
        assert_eq!(data, Bytes::from("data"));
    }

    // =========================================================================
    // Canonical Path Tests (ADR-003, ADR-005)
    // =========================================================================

    #[test]
    fn test_canonical_manifest_paths() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        assert_eq!(
            storage.manifest_root(),
            "tenant=acme/workspace=production/manifests/root.manifest.json"
        );
        assert_eq!(
            storage.manifest(CatalogDomain::Catalog),
            "tenant=acme/workspace=production/manifests/catalog.manifest.json"
        );
        assert_eq!(
            storage.manifest(CatalogDomain::Lineage),
            "tenant=acme/workspace=production/manifests/lineage.manifest.json"
        );
        assert_eq!(
            storage.manifest(CatalogDomain::Executions),
            "tenant=acme/workspace=production/manifests/executions.manifest.json"
        );
        assert_eq!(
            storage.manifest(CatalogDomain::Search),
            "tenant=acme/workspace=production/manifests/search.manifest.json"
        );
    }

    #[test]
    fn test_canonical_lock_paths() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        assert_eq!(
            storage.lock(CatalogDomain::Catalog),
            "tenant=acme/workspace=production/locks/catalog.lock.json"
        );
        assert_eq!(
            storage.lock(CatalogDomain::Lineage),
            "tenant=acme/workspace=production/locks/lineage.lock.json"
        );
    }

    #[test]
    fn test_canonical_snapshot_paths() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        assert_eq!(
            storage.snapshot_dir(CatalogDomain::Catalog, 42),
            "tenant=acme/workspace=production/snapshots/catalog/v42/"
        );
        assert_eq!(
            storage.snapshot_file(CatalogDomain::Catalog, 42, "namespaces.parquet"),
            "tenant=acme/workspace=production/snapshots/catalog/v42/namespaces.parquet"
        );
    }

    #[test]
    fn test_canonical_ledger_paths() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        assert_eq!(
            storage.ledger_dir(CatalogDomain::Executions),
            "tenant=acme/workspace=production/ledger/executions/"
        );
        assert_eq!(
            storage.ledger_event(CatalogDomain::Executions, "01ARZ3NDEK"),
            "tenant=acme/workspace=production/ledger/executions/01ARZ3NDEK.json"
        );
    }

    #[test]
    fn test_canonical_state_paths() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        assert_eq!(
            storage.state_dir(CatalogDomain::Executions),
            "tenant=acme/workspace=production/state/executions/"
        );
        assert_eq!(
            storage.state_snapshot(CatalogDomain::Executions, 1, "01ARZ3NDEK"),
            "tenant=acme/workspace=production/state/executions/snapshot_v1_01ARZ3NDEK.parquet"
        );
    }
}

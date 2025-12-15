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
use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};

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

    /// Validates a relative path for path traversal attacks.
    fn validate_path(path: &str) -> Result<()> {
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

    // === Core Catalog Paths (Tier 1) ===
    //
    // Path conventions align with arco-catalog manifest.rs and lock.rs:
    // - Manifests under `manifests/` (physically multi-file for reduced contention)
    // - Locks under `locks/` (per lock.rs::paths conventions)
    // - Snapshots and commits under `core/`

    /// Path to the root catalog manifest.
    ///
    /// This is the entry point - readers load this first to find domain manifests.
    #[must_use]
    pub fn root_manifest_path(&self) -> String {
        self.scoped_path("manifests/root.manifest.json")
    }

    /// Path to the core domain manifest.
    #[must_use]
    pub fn core_manifest_path(&self) -> String {
        self.scoped_path("manifests/core.manifest.json")
    }

    /// Path to the execution domain manifest.
    #[must_use]
    pub fn execution_manifest_path(&self) -> String {
        self.scoped_path("manifests/execution.manifest.json")
    }

    /// Path to the distributed lock file for core catalog operations.
    #[must_use]
    pub fn core_lock_path(&self) -> String {
        self.scoped_path("locks/core.lock")
    }

    /// Path to a catalog snapshot directory.
    #[must_use]
    pub fn core_snapshot_path(&self, version: u64) -> String {
        self.scoped_path(&format!("core/snapshots/v{version}/"))
    }

    /// Path to a specific commit record.
    #[must_use]
    pub fn core_commit_path(&self, commit_id: &str) -> String {
        self.scoped_path(&format!("core/commits/{commit_id}.json"))
    }

    // === Ledger Paths (Tier 2 Input - Append-Only Events) ===

    /// Path to event log partition (domain/date).
    #[must_use]
    pub fn ledger_path(&self, domain: &str, date: &str) -> String {
        self.scoped_path(&format!("ledger/{domain}/{date}/"))
    }

    // === State Paths (Tier 2 Output - Compacted Parquet) ===

    /// Path to compacted state table.
    #[must_use]
    pub fn state_path(&self, domain: &str, table: &str) -> String {
        self.scoped_path(&format!("state/{domain}/{table}/"))
    }

    // === Lineage Domain ===

    /// Path to lineage domain manifest.
    #[must_use]
    pub fn lineage_manifest_path(&self) -> String {
        self.scoped_path("manifests/lineage.manifest.json")
    }

    /// Path to governance domain manifest.
    #[must_use]
    pub fn governance_manifest_path(&self) -> String {
        self.scoped_path("manifests/governance.manifest.json")
    }

    /// Path to lineage edges state.
    #[must_use]
    pub fn lineage_edges_path(&self) -> String {
        self.scoped_path("state/lineage/edges/")
    }

    // === Governance Domain ===

    /// Path to governance tags table.
    #[must_use]
    pub fn governance_tags_path(&self) -> String {
        self.scoped_path("governance/tags.parquet")
    }

    /// Path to governance owners table.
    #[must_use]
    pub fn governance_owners_path(&self) -> String {
        self.scoped_path("governance/owners.parquet")
    }

    // === High-Level Operations ===

    /// Gets the core manifest.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest is not found or a storage error occurs.
    pub async fn get_core_manifest(&self) -> Result<Bytes> {
        self.backend.get(&self.core_manifest_path()).await
    }

    /// Puts the core manifest with precondition.
    ///
    /// # Errors
    ///
    /// Returns an error if a storage error occurs.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryBackend;

    #[test]
    fn test_core_paths_match_architecture() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        // Per unified platform design: tenant={tenant}/workspace={workspace}/...
        // Key=value format for operational ergonomics (grep-friendly, self-documenting)
        // Manifests under manifests/ (aligned with arco-catalog manifest.rs)
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
        // Locks under locks/ (aligned with arco-catalog lock.rs)
        assert_eq!(
            storage.core_lock_path(),
            "tenant=acme/workspace=production/locks/core.lock"
        );
        // Snapshots and commits under core/
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
    fn test_ledger_paths_match_architecture() {
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
    fn test_state_paths_match_architecture() {
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
    fn test_governance_paths_match_architecture() {
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
    async fn test_catalog_path_operations() {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "acme", "production").unwrap();

        // Write to core manifest path
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
}

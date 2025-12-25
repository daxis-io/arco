//! Tier-1 Atomic Publish Failure Injection Tests per Task 3.7.
//!
//! These tests verify that the two-phase commit (snapshot write + manifest CAS)
//! maintains consistency even when failures occur between phases.
//!
//! # Invariants Tested
//!
//! 1. **Snapshot invisibility after CAS failure**: If CAS fails, snapshot MUST NOT
//!    be visible to readers (orphaned files may exist for GC)
//! 2. **Atomic visibility**: A snapshot is either fully visible or not at all
//! 3. **No partial state**: Readers never see partially-committed state

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashSet;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_core::{Error as CoreError, Result as CoreResult, ScopedStorage};

use arco_catalog::manifest::{CatalogDomainManifest, RootManifest};
use arco_catalog::write_options::WriteOptions;
use arco_catalog::{CatalogWriter, Tier1Compactor};

// ============================================================================
// FailingBackend - Configurable failure injection
// ============================================================================

/// Backend wrapper that injects failures at configurable paths.
///
/// Used for testing crash recovery and atomic publish guarantees.
#[derive(Debug)]
pub struct FailingBackend {
    inner: MemoryBackend,
    /// Paths that should fail on next write (exact match).
    fail_on_write: Arc<RwLock<HashSet<String>>>,
    /// Paths that should fail on next read (exact match).
    fail_on_read: Arc<RwLock<HashSet<String>>>,
    /// If true, fail all operations (simulates total backend failure).
    fail_all: AtomicBool,
}

impl FailingBackend {
    /// Creates a new `FailingBackend` wrapping an empty `MemoryBackend`.
    pub fn new() -> Self {
        Self {
            inner: MemoryBackend::new(),
            fail_on_write: Arc::new(RwLock::new(HashSet::new())),
            fail_on_read: Arc::new(RwLock::new(HashSet::new())),
            fail_all: AtomicBool::new(false),
        }
    }

    /// Configure the backend to fail writes to the specified path.
    ///
    /// The failure is consumed after one use (single-shot).
    pub fn fail_on_write(&self, path: &str) {
        self.fail_on_write.write().unwrap().insert(path.to_string());
    }

    /// Configure the backend to fail reads from the specified path.
    pub fn fail_on_read(&self, path: &str) {
        self.fail_on_read.write().unwrap().insert(path.to_string());
    }

    /// Configure the backend to fail all operations.
    pub fn fail_all(&self) {
        self.fail_all.store(true, Ordering::SeqCst);
    }

    /// Check if a write should fail (and consume the failure if so).
    fn should_fail_write(&self, path: &str) -> bool {
        if self.fail_all.load(Ordering::SeqCst) {
            return true;
        }
        self.fail_on_write.write().unwrap().remove(path)
    }

    /// Check if a read should fail (and consume the failure if so).
    fn should_fail_read(&self, path: &str) -> bool {
        if self.fail_all.load(Ordering::SeqCst) {
            return true;
        }
        self.fail_on_read.write().unwrap().remove(path)
    }
}

impl Default for FailingBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for FailingBackend {
    async fn get(&self, path: &str) -> CoreResult<Bytes> {
        if self.should_fail_read(path) {
            return Err(CoreError::Storage {
                message: format!("Injected read failure: {path}"),
                source: None,
            });
        }
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> CoreResult<Bytes> {
        if self.should_fail_read(path) {
            return Err(CoreError::Storage {
                message: format!("Injected read failure: {path}"),
                source: None,
            });
        }
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> CoreResult<WriteResult> {
        if self.should_fail_write(path) {
            return Err(CoreError::Storage {
                message: format!("Injected write failure: {path}"),
                source: None,
            });
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> CoreResult<()> {
        if self.should_fail_write(path) {
            return Err(CoreError::Storage {
                message: format!("Injected delete failure: {path}"),
                source: None,
            });
        }
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> CoreResult<Vec<ObjectMeta>> {
        if self.fail_all.load(Ordering::SeqCst) {
            return Err(CoreError::Storage {
                message: format!("Injected list failure: {prefix}"),
                source: None,
            });
        }
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> CoreResult<Option<ObjectMeta>> {
        if self.should_fail_read(path) {
            return Err(CoreError::Storage {
                message: format!("Injected head failure: {path}"),
                source: None,
            });
        }
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> CoreResult<String> {
        self.inner.signed_url(path, expiry).await
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Build the scoped path for a tenant/workspace.
fn scoped_path(tenant: &str, workspace: &str, path: &str) -> String {
    format!("tenant={tenant}/workspace={workspace}/{path}")
}

/// Read the catalog manifest version from storage.
async fn read_catalog_manifest_version(storage: &ScopedStorage) -> u64 {
    let root_bytes = storage
        .get_raw("manifests/root.manifest.json")
        .await
        .expect("read root manifest");
    let mut root: RootManifest = serde_json::from_slice(&root_bytes).expect("parse root manifest");
    root.normalize_paths();

    let catalog_bytes = storage
        .get_raw(&root.catalog_manifest_path)
        .await
        .expect("read catalog manifest");
    let catalog: CatalogDomainManifest =
        serde_json::from_slice(&catalog_bytes).expect("parse catalog manifest");
    catalog.snapshot_version
}

/// List all files under a prefix in the storage.
async fn list_files_under(storage: &ScopedStorage, prefix: &str) -> Vec<String> {
    storage
        .list(prefix)
        .await
        .expect("list objects")
        .into_iter()
        .map(|p| p.to_string())
        .collect()
}

// Test constants
const TEST_TENANT: &str = "test-tenant";
const TEST_WORKSPACE: &str = "test-workspace";

// ============================================================================
// Failure Injection Tests
// ============================================================================

/// Test that a CAS failure during manifest update leaves snapshot invisible.
///
/// Scenario:
/// 1. Initialize catalog (creates v0 manifest)
/// 2. Configure backend to fail on manifest write
/// 3. Attempt create_namespace (writes snapshot, then fails on CAS)
/// 4. Verify: manifest still shows old version (snapshot invisible)
/// 5. Verify: orphaned snapshot files may exist (GC will clean)
#[tokio::test]
async fn tier1_crash_between_snapshot_and_cas() {
    let backend = Arc::new(FailingBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), TEST_TENANT, TEST_WORKSPACE).expect("scoped storage");
    let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

    // Initialize catalog (this should succeed)
    writer
        .initialize()
        .await
        .expect("initialize should succeed");

    // Get initial manifest version
    let initial_version = read_catalog_manifest_version(&storage).await;

    // Configure failure on the catalog manifest CAS (using scoped path)
    let manifest_path = scoped_path(
        TEST_TENANT,
        TEST_WORKSPACE,
        "manifests/catalog.manifest.json",
    );
    backend.fail_on_write(&manifest_path);

    // Attempt to create namespace (should fail during CAS)
    let result = writer
        .create_namespace("test-namespace", Some("Test"), WriteOptions::default())
        .await;

    // The operation should fail
    assert!(
        result.is_err(),
        "create_namespace should fail due to injected CAS failure"
    );

    // CRITICAL INVARIANT: Manifest should NOT have been updated
    // (snapshot should be invisible)
    let final_version = read_catalog_manifest_version(&storage).await;
    assert_eq!(
        initial_version, final_version,
        "Manifest version should NOT change after failed CAS. \
         Initial: {initial_version}, Final: {final_version}. Snapshot must remain invisible."
    );

    // Verify: orphaned snapshot files MAY exist (they will be GC'd later)
    // This is acceptable - the key invariant is that they're not visible via manifest
    let snapshots = list_files_under(&storage, "snapshots/catalog/").await;
    assert!(
        !snapshots.is_empty(),
        "Expected orphaned snapshot files after failed CAS"
    );
}

/// Test that read operations don't see uncommitted state.
#[tokio::test]
async fn tier1_reader_sees_only_committed_state() {
    let backend = Arc::new(FailingBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), TEST_TENANT, TEST_WORKSPACE).expect("scoped storage");
    let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

    // Initialize
    writer.initialize().await.expect("initialize");

    // Create a namespace successfully
    writer
        .create_namespace("committed-ns", None, WriteOptions::default())
        .await
        .expect("create namespace");

    // Configure failure for next namespace creation (using scoped path)
    let manifest_path = scoped_path(
        TEST_TENANT,
        TEST_WORKSPACE,
        "manifests/catalog.manifest.json",
    );
    backend.fail_on_write(&manifest_path);

    // Attempt another namespace creation (will fail)
    let result = writer
        .create_namespace("uncommitted-ns", None, WriteOptions::default())
        .await;
    assert!(
        result.is_err(),
        "Second create_namespace should fail due to injected CAS failure"
    );

    // Create a reader to verify state
    let reader = arco_catalog::CatalogReader::new(storage);

    // Reader should see only the committed namespace
    let namespaces = reader.list_namespaces().await.expect("list namespaces");
    assert_eq!(
        namespaces.len(),
        1,
        "Reader should see exactly 1 committed namespace"
    );
    let first = namespaces.first().expect("committed namespace");
    assert_eq!(first.name, "committed-ns");

    // Uncommitted namespace should NOT be visible
    let uncommitted = reader.get_namespace("uncommitted-ns").await;
    assert!(
        uncommitted.is_err() || uncommitted.unwrap().is_none(),
        "Uncommitted namespace should not be visible"
    );
}

/// Test idempotent initialization survives backend hiccups.
#[tokio::test]
async fn tier1_initialize_idempotent_after_failure() {
    let backend = Arc::new(FailingBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), TEST_TENANT, TEST_WORKSPACE).expect("scoped storage");
    let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

    // First initialization succeeds
    writer.initialize().await.expect("first init");

    // Second initialization (idempotent) should also succeed
    writer.initialize().await.expect("second init (idempotent)");

    // Configure temporary failure (using scoped path)
    let manifest_path = scoped_path(
        TEST_TENANT,
        TEST_WORKSPACE,
        "manifests/catalog.manifest.json",
    );
    backend.fail_on_write(&manifest_path);

    // Third init with failure should not corrupt state
    // (because it checks if already initialized first)
    let result = writer.initialize().await;
    // This might succeed (already initialized) or fail (backend error)
    // Either way, state should be consistent
    let _ = result;

    // Remove failure and verify state is still good
    let reader = arco_catalog::CatalogReader::new(storage);
    let freshness = reader
        .get_freshness(arco_core::CatalogDomain::Catalog)
        .await;
    assert!(
        freshness.is_ok(),
        "Catalog should be readable after init failures"
    );
}

/// Test that lock acquisition failure is handled gracefully.
#[tokio::test]
async fn tier1_lock_failure_does_not_corrupt_state() {
    let backend = Arc::new(FailingBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), TEST_TENANT, TEST_WORKSPACE).expect("scoped storage");
    let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);

    // Initialize
    writer.initialize().await.expect("initialize");

    // Configure failure on lock file write (using scoped path)
    let lock_path = scoped_path(TEST_TENANT, TEST_WORKSPACE, "locks/catalog.lock.json");
    backend.fail_on_write(&lock_path);

    // Attempt operation - should fail during lock acquisition
    let result = writer
        .create_namespace("test", None, WriteOptions::default())
        .await;

    assert!(result.is_err(), "Should fail when lock cannot be acquired");

    // State should remain consistent - no partial changes
    let reader = arco_catalog::CatalogReader::new(storage);
    let namespaces = reader.list_namespaces().await.expect("list");
    assert!(
        namespaces.is_empty(),
        "No namespaces should exist after failed lock"
    );
}

// ============================================================================
// FailingBackend Unit Tests
// ============================================================================

#[tokio::test]
async fn failing_backend_single_shot_failure() {
    let backend = FailingBackend::new();

    // Configure single-shot failure
    backend.fail_on_write("test.txt");

    // First write fails
    let result = backend
        .put("test.txt", Bytes::from("data"), WritePrecondition::None)
        .await;
    assert!(result.is_err(), "First write should fail");

    // Second write succeeds (failure consumed)
    let result = backend
        .put("test.txt", Bytes::from("data"), WritePrecondition::None)
        .await;
    assert!(result.is_ok(), "Second write should succeed");
}

#[tokio::test]
async fn failing_backend_fail_all_mode() {
    let backend = FailingBackend::new();

    // Write some data first
    backend
        .put("test.txt", Bytes::from("data"), WritePrecondition::None)
        .await
        .expect("initial write");

    // Enable fail-all mode
    backend.fail_all();

    // All operations fail
    assert!(backend.get("test.txt").await.is_err());
    assert!(backend.list("").await.is_err());
    assert!(
        backend
            .put("new.txt", Bytes::from("x"), WritePrecondition::None)
            .await
            .is_err()
    );
}

//! IAM smoke tests verifying Gate 5 prefix-scoped permissions.
//!
//! These tests require a deployed GCP environment with prefix-scoped IAM.
//!
//! # Running Tests
//!
//! Run with API service account credentials:
//! ```bash
//! ARCO_TEST_BUCKET=<bucket> \
//! ARCO_TEST_TENANT=<tenant> \
//! ARCO_TEST_WORKSPACE=<workspace> \
//! cargo test --package arco-integration-tests --features iam-smoke -- --ignored
//! ```
//!
//! # What These Tests Verify
//!
//! - API cannot write to `state/` prefix (Compactor-only)
//! - API CAN write to `ledger/` prefix (API-allowed)
//! - Prefix-scoped IAM enforces Gate 5 sole-writer invariant

#![cfg(feature = "iam-smoke")]

use arco_core::{
    CatalogDomain, ObjectStoreBackend, StorageBackend, WritePrecondition, WriteResult,
};
use arco_core::storage_keys::{LedgerKey, StateKey};
use bytes::Bytes;
use std::env;
use std::sync::Arc;
use ulid::Ulid;

/// Helper to get test configuration from environment.
fn test_config() -> (String, String, String) {
    let bucket = env::var("ARCO_TEST_BUCKET").expect("ARCO_TEST_BUCKET required");
    let tenant = env::var("ARCO_TEST_TENANT").unwrap_or_else(|_| "test-tenant".to_string());
    let workspace =
        env::var("ARCO_TEST_WORKSPACE").unwrap_or_else(|_| "test-workspace".to_string());
    (bucket, tenant, workspace)
}

/// Helper to create a scoped path for testing.
fn scoped_path(tenant: &str, workspace: &str, path: &str) -> String {
    format!("tenant={tenant}/workspace={workspace}/{path}")
}

/// Test that API service account CANNOT write to state/ prefix.
///
/// This is the critical Gate 5 invariant: only Compactor writes Parquet state.
/// If this test passes (write fails), IAM is correctly configured.
///
/// # Expected Result
///
/// The write should fail with a permission denied error (403).
#[tokio::test]
#[ignore = "requires deployed IAM with API credentials"]
async fn test_api_cannot_write_state() {
    let (bucket, tenant, workspace) = test_config();

    // Create GCS backend (uses default credentials - should be API SA)
    let backend = Arc::new(
        ObjectStoreBackend::gcs(&bucket).expect("Failed to create GCS backend"),
    );

    // Attempt to write to state/ prefix - this SHOULD FAIL
    let state_key = StateKey::state_snapshot(
        CatalogDomain::Catalog,
        0,
        &format!("iam-smoke-{}", Ulid::new()),
    );
    let full_path = scoped_path(&tenant, &workspace, state_key.as_ref());

    let result = backend
        .put(&full_path, Bytes::from("test"), WritePrecondition::None)
        .await;

    // The write should fail - if it succeeds, IAM is not configured correctly
    assert!(
        result.is_err(),
        "SECURITY VIOLATION: API should NOT be able to write to state/ prefix!\n\
         Expected permission denied, but write succeeded.\n\
         Path: {full_path}\n\
         Check IAM configuration in infra/terraform/iam_conditions.tf"
    );

    let err = result.unwrap_err();
    let err_str = err.to_string().to_lowercase();

    // Verify it's a permission error, not some other failure
    assert!(
        err_str.contains("permission denied")
            || err_str.contains("403")
            || err_str.contains("forbidden")
            || err_str.contains("access denied"),
        "Expected permission denied error, got: {err}\n\
         Path: {full_path}"
    );

    println!("PASS: API correctly denied write to state/ prefix");
}

/// Regression test: prefix scoping must not be bypassable with `contains()`.
///
/// If IAM conditions use `resource.name.contains("/ledger/")` instead of an
/// anchored match on the expected path prefix, an attacker could write to:
/// `state/ledger/...` while still satisfying the ledger condition.
#[tokio::test]
#[ignore = "requires deployed IAM with API credentials"]
async fn test_api_cannot_write_state_even_if_path_contains_ledger_segment() {
    let (bucket, tenant, workspace) = test_config();

    let backend = Arc::new(
        ObjectStoreBackend::gcs(&bucket).expect("Failed to create GCS backend"),
    );

    // This is NOT under `ledger/` at the correct path boundary.
    // A `contains("/ledger/")` condition would incorrectly allow it.
    let bypass_path = format!("state/ledger/iam-smoke-bypass-{}.txt", Ulid::new());
    let full_path = scoped_path(&tenant, &workspace, &bypass_path);

    let result = backend
        .put(
            &full_path,
            Bytes::from("bypass-test"),
            WritePrecondition::DoesNotExist,
        )
        .await;

    assert!(
        result.is_err(),
        "SECURITY VIOLATION: API should NOT be able to write outside ledger/ via a contains() bypass!\n\
         Path: {full_path}\n\
         Ensure IAM conditions are anchored (e.g., resource.name.matches(\"^.../ledger/\"))"
    );
}

/// Test that API service account CAN write to ledger/ prefix.
///
/// This verifies that the prefix-scoped IAM allows legitimate operations.
/// If this test fails, IAM may be too restrictive.
///
/// # Expected Result
///
/// The write should succeed.
#[tokio::test]
#[ignore = "requires deployed IAM with API credentials"]
async fn test_api_can_write_ledger() {
    let (bucket, tenant, workspace) = test_config();

    // Create GCS backend (uses default credentials - should be API SA)
    let backend = Arc::new(
        ObjectStoreBackend::gcs(&bucket).expect("Failed to create GCS backend"),
    );

    // Write to ledger/ prefix - this SHOULD SUCCEED
    let event_id = format!("iam-smoke-{}", Ulid::new());
    let ledger_key = LedgerKey::event(CatalogDomain::Catalog, &event_id);
    let full_path = scoped_path(&tenant, &workspace, ledger_key.as_ref());

    let result = backend
        .put(
            &full_path,
            Bytes::from(r#"{"event":"test"}"#),
            WritePrecondition::DoesNotExist,
        )
        .await;

    assert!(
        result.is_ok(),
        "API should be able to write to ledger/ prefix!\n\
         Error: {:?}\n\
         Path: {full_path}\n\
         Check IAM configuration in infra/terraform/iam_conditions.tf",
        result.err()
    );

    match result.unwrap() {
        WriteResult::Success { .. } => {
            println!("PASS: API correctly allowed to write to ledger/ prefix");

            // Clean up test artifact
            let _ = backend.delete(&full_path).await;
        }
        WriteResult::PreconditionFailed { .. } => {
            // This is acceptable if another test run created the same file
            println!("PASS: File already exists (precondition failed, but write was attempted)");
        }
    }
}

/// Test that API can write to locks/ prefix.
///
/// Distributed locks are managed by API.
#[tokio::test]
#[ignore = "requires deployed IAM with API credentials"]
async fn test_api_can_write_locks() {
    let (bucket, tenant, workspace) = test_config();

    let backend = Arc::new(
        ObjectStoreBackend::gcs(&bucket).expect("Failed to create GCS backend"),
    );

    // Write to locks/ prefix
    let lock_id = format!("iam-smoke-{}", Ulid::new());
    let lock_path = format!("locks/{lock_id}.lock.json");
    let full_path = scoped_path(&tenant, &workspace, &lock_path);

    let result = backend
        .put(
            &full_path,
            Bytes::from(r#"{"holder":"test"}"#),
            WritePrecondition::None,
        )
        .await;

    assert!(
        result.is_ok(),
        "API should be able to write to locks/ prefix!\n\
         Error: {:?}\n\
         Path: {full_path}",
        result.err()
    );

    println!("PASS: API correctly allowed to write to locks/ prefix");

    // Clean up
    let _ = backend.delete(&full_path).await;
}

/// Test that API can write to commits/ prefix.
///
/// Commit records (audit trail) are created by API.
#[tokio::test]
#[ignore = "requires deployed IAM with API credentials"]
async fn test_api_can_write_commits() {
    let (bucket, tenant, workspace) = test_config();

    let backend = Arc::new(
        ObjectStoreBackend::gcs(&bucket).expect("Failed to create GCS backend"),
    );

    // Write to commits/ prefix
    let commit_id = format!("iam-smoke-{}", Ulid::new());
    let commit_path = format!("commits/catalog/{commit_id}.json");
    let full_path = scoped_path(&tenant, &workspace, &commit_path);

    let result = backend
        .put(
            &full_path,
            Bytes::from(r#"{"commit_id":"test"}"#),
            WritePrecondition::DoesNotExist,
        )
        .await;

    assert!(
        result.is_ok(),
        "API should be able to write to commits/ prefix!\n\
         Error: {:?}\n\
         Path: {full_path}",
        result.err()
    );

    println!("PASS: API correctly allowed to write to commits/ prefix");

    // Clean up
    let _ = backend.delete(&full_path).await;
}

/// Summary test that prints IAM verification results.
#[tokio::test]
#[ignore = "requires deployed IAM with API credentials"]
async fn test_iam_summary() {
    println!("\n========================================");
    println!("Gate 5 IAM Smoke Test Summary");
    println!("========================================");
    println!("These tests verify prefix-scoped IAM:");
    println!();
    println!("  API Service Account:");
    println!("    - ledger/    : WRITE ALLOWED");
    println!("    - locks/     : WRITE ALLOWED");
    println!("    - commits/   : WRITE ALLOWED");
    println!("    - manifests/ : WRITE ALLOWED (CAS)");
    println!("    - state/     : WRITE DENIED");
    println!("    - l0/        : WRITE DENIED");
    println!();
    println!("  Compactor Service Account:");
    println!("    - state/     : WRITE ALLOWED");
    println!("    - l0/        : WRITE ALLOWED");
    println!("    - manifests/ : WRITE ALLOWED (CAS)");
    println!("    - ledger/    : WRITE DENIED");
    println!("    - locks/     : WRITE DENIED");
    println!("    - commits/   : WRITE DENIED");
    println!();
    println!("Run individual tests to verify each permission.");
    println!("========================================\n");
}

//! Integration tests between arco-catalog and arco-flow.

use arco_core::prelude::*;

#[test]
fn test_tenant_isolation() {
    let tenant_a = TenantId::new("tenant-a").unwrap();
    let tenant_b = TenantId::new("tenant-b").unwrap();

    // Verify different storage prefixes.
    assert_ne!(tenant_a.storage_prefix(), tenant_b.storage_prefix());
    assert!(tenant_a.storage_prefix().contains("tenant-a"));
    assert!(tenant_b.storage_prefix().contains("tenant-b"));
}

#[test]
fn test_asset_id_uniqueness() {
    let id1 = AssetId::generate();
    let id2 = AssetId::generate();

    assert_ne!(id1, id2);
}

//! Basic usage example demonstrating core Arco concepts.
//!
//! Run with: `cargo run --example basic_usage`

use arco_core::error::Result;
use arco_core::prelude::*;

fn main() -> Result<()> {
    // Create a tenant context
    let tenant = TenantId::new("acme-corp")?;
    println!("Tenant: {}", tenant);
    println!("Storage prefix: {}", tenant.storage_prefix());

    // Generate asset IDs
    let asset_id = AssetId::generate();
    println!("Asset ID: {}", asset_id);

    Ok(())
}

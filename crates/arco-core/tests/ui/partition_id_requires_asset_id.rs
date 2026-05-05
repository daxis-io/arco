//! Test: PartitionId::derive should require a typed AssetId, not arbitrary strings.

use arco_core::id::AssetId;
use arco_core::partition::{PartitionId, PartitionKey, ScalarValue};

fn main() {
    let mut pk = PartitionKey::new();
    pk.insert("date", ScalarValue::Date("2025-01-15".into()));

    let asset_id = AssetId::generate();
    let _ = PartitionId::derive(&asset_id, &pk);
    let _ = PartitionId::derive("table-01", &pk);
}

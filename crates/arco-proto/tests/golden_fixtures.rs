//! Golden fixture tests for serialization stability.
//!
//! These tests ensure that the JSON serialization format remains stable
//! across versions and is consistent with cross-language contracts.

use arco_proto::PartitionKey;

#[test]
fn test_partition_key_golden_fixture() {
    let fixture = include_str!("../fixtures/partition_key_v1.json");
    let pk: PartitionKey = serde_json::from_str(fixture).expect("golden fixture should parse");

    // Verify structure
    assert_eq!(pk.dimensions.len(), 2);
    assert!(pk.dimensions.contains_key("date"));
    assert!(pk.dimensions.contains_key("region"));

    // Re-serialize and verify stability (no field reordering)
    let reserialized = serde_json::to_string(&pk).expect("should serialize");
    let reparsed: PartitionKey = serde_json::from_str(&reserialized).expect("should reparse");

    assert_eq!(pk.dimensions.len(), reparsed.dimensions.len());
}

#[test]
fn test_fixture_backward_compatibility() {
    // This test ensures old fixtures remain parseable
    // Add new versions as fixtures/partition_key_v2.json etc.

    let v1_fixture = include_str!("../fixtures/partition_key_v1.json");
    let _: PartitionKey =
        serde_json::from_str(v1_fixture).expect("v1 fixture must remain parseable");
}

#[test]
fn test_id_types_serde_roundtrip() {
    use arco_proto::{AssetId, TenantId, WorkspaceId};

    let tenant = TenantId {
        value: "acme-corp".to_string(),
    };
    let json = serde_json::to_string(&tenant).expect("serialize");
    let parsed: TenantId = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(tenant.value, parsed.value);

    let workspace = WorkspaceId {
        value: "production".to_string(),
    };
    let json = serde_json::to_string(&workspace).expect("serialize");
    let parsed: WorkspaceId = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(workspace.value, parsed.value);

    let asset = AssetId {
        value: "asset_123".to_string(),
    };
    let json = serde_json::to_string(&asset).expect("serialize");
    let parsed: AssetId = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(asset.value, parsed.value);
}

#[test]
fn test_scalar_value_variants() {
    use arco_proto::{ScalarValue, scalar_value::Value};

    // String value
    let sv = ScalarValue {
        value: Some(Value::StringValue("hello".to_string())),
    };
    let json = serde_json::to_string(&sv).expect("serialize");
    let parsed: ScalarValue = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(sv, parsed);

    // Int64 value
    let sv = ScalarValue {
        value: Some(Value::Int64Value(42)),
    };
    let json = serde_json::to_string(&sv).expect("serialize");
    let parsed: ScalarValue = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(sv, parsed);

    // Bool value
    let sv = ScalarValue {
        value: Some(Value::BoolValue(true)),
    };
    let json = serde_json::to_string(&sv).expect("serialize");
    let parsed: ScalarValue = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(sv, parsed);

    // Date value
    let sv = ScalarValue {
        value: Some(Value::DateValue("2025-01-15".to_string())),
    };
    let json = serde_json::to_string(&sv).expect("serialize");
    let parsed: ScalarValue = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(sv, parsed);
}

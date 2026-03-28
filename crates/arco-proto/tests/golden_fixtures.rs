//! Golden fixture tests for serialization stability.
//!
//! These tests ensure that the JSON serialization format remains stable
//! across versions and is consistent with cross-language contracts.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use arco_proto::{FileEntry, PartitionKey, ScalarValue};
use serde_json::{Value as JsonValue, json};

#[test]
fn test_partition_key_golden_fixture() {
    let fixture = include_str!("../fixtures/partition_key_v2.json");
    let pk: PartitionKey = serde_json::from_str(fixture).expect("golden fixture should parse");
    let actual = serde_json::to_value(&pk).expect("should serialize");
    let expected: JsonValue = serde_json::from_str(fixture).expect("fixture should be valid JSON");

    assert_eq!(actual, expected);
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
        value: "01959a32-4517-7d8b-a13d-5d3e8f840f13".to_string(),
    };
    let json = serde_json::to_string(&asset).expect("serialize");
    let parsed: AssetId = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(asset.value, parsed.value);
}

#[test]
fn test_scalar_value_variants() {
    let cases = [
        json!({ "stringValue": "hello" }),
        json!({ "int64Value": "42" }),
        json!({ "boolValue": true }),
        json!({ "date": { "year": 2025, "month": 1, "day": 15 } }),
        json!({ "timestamp": "2025-01-15T10:30:00+00:00" }),
        json!({ "doubleValue": 3.5 }),
        json!({ "bytesValue": "AQIDBA==" }),
        json!({ "nullValue": "NULL_VALUE_NULL" }),
    ];

    for case in cases {
        let parsed: ScalarValue = serde_json::from_value(case.clone()).expect("deserialize");
        let actual = serde_json::to_value(&parsed).expect("serialize");
        assert_eq!(actual, case);
    }
}

#[test]
fn test_file_entry_golden_fixture() {
    let fixture = json!({
        "path": "s3://bucket/materializations/part-000.parquet",
        "sizeBytes": "9223372036854775808",
        "rowCount": "9223372036854775808",
        "contentDigest": {
            "algorithm": "HASH_ALGORITHM_SHA256",
            "digest": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
        },
        "fileFormat": "FILE_FORMAT_PARQUET"
    });
    let entry: FileEntry =
        serde_json::from_value(fixture.clone()).expect("file entry fixture should parse");
    let actual = serde_json::to_value(&entry).expect("should serialize");

    assert_eq!(actual, fixture);
}

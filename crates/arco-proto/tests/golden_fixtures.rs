//! Golden fixture tests for the authoritative shared protobuf surface.

#![allow(clippy::expect_used)]

use arco_proto::arco::catalog::v1::MetastoreMutation;
use arco_proto::arco::common::v1::{PartitionDimension, PartitionKey, ScalarValue, scalar_value};

#[test]
fn partition_key_fixture_preserves_explicit_dimension_order() {
    let fixture = include_str!("../fixtures/partition_key_v2.json");
    let pk: PartitionKey = serde_json::from_str(fixture).expect("golden fixture should parse");

    assert_eq!(pk.dimensions.len(), 2);
    assert_eq!(pk.dimensions[0].name, "date");
    assert_eq!(pk.dimensions[1].name, "region");

    let reserialized = serde_json::to_string(&pk).expect("should serialize");
    let reparsed: PartitionKey = serde_json::from_str(&reserialized).expect("should reparse");

    assert_eq!(reparsed.dimensions[0].name, "date");
    assert_eq!(reparsed.dimensions[1].name, "region");
}

#[test]
fn scalar_value_variants_roundtrip() {
    let values = [
        ScalarValue {
            value: Some(scalar_value::Value::StringValue("hello".to_string())),
        },
        ScalarValue {
            value: Some(scalar_value::Value::Int64Value(42)),
        },
        ScalarValue {
            value: Some(scalar_value::Value::BoolValue(true)),
        },
        ScalarValue {
            value: Some(scalar_value::Value::DateValue("2026-04-09".to_string())),
        },
    ];

    for value in values {
        let json = serde_json::to_string(&value).expect("serialize");
        let parsed: ScalarValue = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(value, parsed);
    }
}

#[test]
fn partition_dimensions_use_explicit_messages_instead_of_maps() {
    let pk = PartitionKey {
        dimensions: vec![
            PartitionDimension {
                name: "date".to_string(),
                value: Some(ScalarValue {
                    value: Some(scalar_value::Value::DateValue("2026-04-09".to_string())),
                }),
            },
            PartitionDimension {
                name: "region".to_string(),
                value: Some(ScalarValue {
                    value: Some(scalar_value::Value::StringValue("us-east".to_string())),
                }),
            },
        ],
    };

    assert_eq!(pk.dimensions[0].name, "date");
    assert_eq!(pk.dimensions[1].name, "region");
}

#[test]
fn docs_name_the_pre_freeze_proto_hard_cut_policy() {
    let readme = include_str!("../../../README.md");
    let style = include_str!("../../../proto/STYLE.md");

    assert!(readme.contains("pre-freeze hard cut"));
    assert!(style.contains("pre-freeze hard cut"));
    assert!(style.contains("After the post-hard-cut baseline is regenerated"));
}

#[test]
fn metastore_mutation_protojson_field_names_are_contract() {
    let fixture = include_str!("../fixtures/metastore_mutation_v1.json");
    let parsed: MetastoreMutation =
        serde_json::from_str(fixture).expect("metastore mutation fixture should parse");

    let json = serde_json::to_value(&parsed).expect("metastore mutation should serialize");
    assert!(json.get("storageCredential").is_some());
    assert!(json.to_string().contains("credentialId"));
    assert!(json.to_string().contains("lakehouse-prod"));
}

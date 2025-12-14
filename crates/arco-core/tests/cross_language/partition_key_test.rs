//! Cross-language PartitionKey canonical encoding tests.
//!
//! These tests use shared fixtures to ensure Rust and Python
//! produce identical canonical strings.

use arco_core::partition::{PartitionKey, ScalarValue};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct TestCase {
    name: String,
    dimensions: HashMap<String, DimensionValue>,
    expected_canonical: String,
    #[allow(dead_code)]
    #[serde(default)]
    comment: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DimensionValue {
    #[serde(rename = "type")]
    typ: String,
    value: String,
}

fn load_test_cases() -> Vec<TestCase> {
    let fixture = include_str!("../fixtures/partition_key_cases.json");
    serde_json::from_str(fixture).expect("fixture should parse")
}

#[test]
fn test_all_fixtures_match_expected_canonical() {
    let cases = load_test_cases();

    for case in cases {
        let mut pk = PartitionKey::new();

        for (key, dim) in &case.dimensions {
            let value = match dim.typ.as_str() {
                "string" => ScalarValue::String(dim.value.clone()),
                "date" => ScalarValue::Date(dim.value.clone()),
                "timestamp" => ScalarValue::Timestamp(dim.value.clone()),
                "int64" => ScalarValue::Int64(dim.value.parse().expect("int64 should parse")),
                "bool" => ScalarValue::Boolean(dim.value.parse().expect("bool should parse")),
                "null" => ScalarValue::Null,
                other => panic!("unknown type in fixture: {other}"),
            };
            pk.insert(key, value);
        }

        let actual = pk.canonical_string();
        assert_eq!(
            actual, case.expected_canonical,
            "FAIL: case '{}'\n  expected: {}\n  actual:   {}",
            case.name, case.expected_canonical, actual
        );
    }
}

#[test]
fn test_fixture_count() {
    let cases = load_test_cases();
    // Ensure we have a reasonable number of test cases
    assert!(cases.len() >= 10, "expected at least 10 fixture cases");
}

#[test]
fn test_round_trip_all_fixtures() {
    // Every canonical string must be parseable back to original dimensions
    let cases = load_test_cases();

    for case in cases {
        // Build partition key from fixture
        let mut pk = PartitionKey::new();
        for (key, dim) in &case.dimensions {
            let value = match dim.typ.as_str() {
                "string" => ScalarValue::String(dim.value.clone()),
                "date" => ScalarValue::Date(dim.value.clone()),
                "timestamp" => ScalarValue::Timestamp(dim.value.clone()),
                "int64" => ScalarValue::Int64(dim.value.parse().unwrap()),
                "bool" => ScalarValue::Boolean(dim.value.parse().unwrap()),
                "null" => ScalarValue::Null,
                other => panic!("unknown type: {other}"),
            };
            pk.insert(key, value);
        }

        // Get canonical string
        let canonical = pk.canonical_string();

        // Parse back (this tests the parse implementation)
        let parsed = PartitionKey::parse(&canonical)
            .unwrap_or_else(|e| panic!("case '{}' should parse: {} - error: {e:?}", case.name, canonical));

        // Must produce identical canonical string
        assert_eq!(
            parsed.canonical_string(),
            canonical,
            "FAIL round-trip: case '{}'\n  original:   {}\n  after parse: {}",
            case.name,
            canonical,
            parsed.canonical_string()
        );
    }
}

#[test]
fn test_canonical_is_path_safe() {
    // All canonical strings must be usable as path segments
    let cases = load_test_cases();

    for case in cases {
        let canonical = &case.expected_canonical;

        // Must not contain path-unsafe characters
        assert!(!canonical.contains('/'), "case '{}' has /", case.name);
        assert!(!canonical.contains('\\'), "case '{}' has \\", case.name);
        assert!(!canonical.contains('?'), "case '{}' has ?", case.name);
        assert!(!canonical.contains('#'), "case '{}' has #", case.name);
        assert!(!canonical.contains('%'), "case '{}' has %", case.name);

        // Only allowed special char is '=' for key=value and ',' for separators
        // and ':' for type tags
    }
}

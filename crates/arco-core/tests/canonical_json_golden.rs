//! Golden vector tests for cross-language canonical JSON verification.
//!
//! These tests ensure Rust produces identical output to the Python reference:
//! `json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)`

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use arco_core::canonical_json::to_canonical_string;
use serde_json::Value;

#[derive(Debug, serde::Deserialize)]
struct VectorFile {
    vectors: Vec<Vector>,
}

#[derive(Debug, serde::Deserialize)]
struct Vector {
    name: String,
    input: Value,
    expected_canonical: String,
}

/// Golden vectors that must match across Rust and Python implementations.
///
/// Source of truth is `tests/golden/canonical_json_vectors.json`, which can also
/// be consumed by a Python verifier script.
#[test]
fn canonical_json_golden_vectors() {
    let file: VectorFile = serde_json::from_str(include_str!("golden/canonical_json_vectors.json"))
        .expect("Failed to parse golden vectors file");

    for vector in file.vectors {
        let canonical = to_canonical_string(&vector.input)
            .unwrap_or_else(|e| panic!("Failed to canonicalize '{}': {}", vector.name, e));
        assert_eq!(
            canonical, vector.expected_canonical,
            "vector '{}' mismatch:\n  got:      {}\n  expected: {}",
            vector.name, canonical, vector.expected_canonical
        );
    }
}

/// Verify that the golden vectors can be validated with Python.
///
/// This test documents how to verify cross-language compatibility:
/// ```python
/// import json
///
/// with open('tests/golden/canonical_json_vectors.json') as f:
///     data = json.load(f)
///
/// for vector in data['vectors']:
///     canonical = json.dumps(vector['input'], sort_keys=True, separators=(',', ':'), ensure_ascii=False)
///     assert canonical == vector['expected_canonical'], f"{vector['name']}: {canonical} != {vector['expected_canonical']}"
/// ```
#[test]
fn golden_vectors_document_python_equivalent() {
    // This test exists to document the Python verification approach
    // The actual verification happens in canonical_json_golden_vectors
}

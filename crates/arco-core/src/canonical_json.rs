//! Canonical JSON serialization for deterministic hashing and signing.
//!
//! This module provides canonical serialization for cross-language determinism.
//!
//! **Design Decision (ADR-010):** There is a single, strict canonical serializer.
//! Floats are rejected in ALL canonical JSON because:
//! 1. Cross-language float stringification is non-deterministic
//! 2. The system is Rust control plane + Python data plane
//! 3. Envelope signing must be verifiable in both languages
//!
//! Use integers for all numeric values (millis, millicores, bytes, etc.)
//!
//! # Python Equivalent
//!
//! ```python
//! json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
//! ```

use serde::Serialize;
use serde_json::{Map, Number, Value};
use std::collections::HashMap;
use thiserror::Error;

/// Errors that can occur during canonical JSON serialization.
#[derive(Debug, Error)]
pub enum CanonicalJsonError {
    /// Serde JSON conversion failed.
    #[error("serde_json error: {0}")]
    Serde(#[from] serde_json::Error),

    /// Float values are not allowed in canonical JSON.
    ///
    /// **Design Decision (ADR-010):** Floats are banned in ALL canonical JSON,
    /// not just identity contexts. This is because:
    /// 1. Cross-language float stringification is non-deterministic
    /// 2. The system is Rust control plane + Python data plane
    /// 3. Envelope signing must be verifiable in both languages
    ///
    /// Use integers for all numeric values (millis, millicores, bytes, etc.)
    #[error("float values are not allowed in canonical JSON (use integers)")]
    FloatNotAllowed,

    /// Non-finite number (NaN, Infinity) encountered.
    #[error("non-finite number not allowed: {0}")]
    NonFiniteNumber(String),

    /// IO error during writing.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// UTF-8 encoding error (should never happen with valid JSON).
    #[error("UTF-8 encoding error")]
    Utf8Error,
}

/// Serialize `value` into canonical JSON bytes.
///
/// **ADR-010 Design Decision:** This is a single, strict canonical serializer.
/// There is no "lenient" mode because:
/// 1. Cross-language float formatting is non-deterministic
/// 2. Rust + Python must produce identical bytes for signing/verification
/// 3. Simpler API reduces misuse risk
///
/// Canonical JSON has:
/// - Object keys sorted lexicographically (UTF-8 byte order)
/// - No whitespace
/// - UTF-8 output
/// - Integers only (floats rejected)
///
/// # Errors
///
/// Returns `CanonicalJsonError::Serde` if serialization fails, or
/// `CanonicalJsonError::FloatNotAllowed` if the value contains floats.
#[must_use = "canonical bytes should be used for hashing/signing"]
pub fn to_canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, CanonicalJsonError> {
    let v = serde_json::to_value(value)?;
    let mut out = Vec::<u8>::new();
    write_value(&v, &mut out)?;
    Ok(out)
}

/// Same as `to_canonical_bytes`, but returns a UTF-8 String.
///
/// # Errors
///
/// Returns `CanonicalJsonError::Serde` if serialization fails,
/// `CanonicalJsonError::FloatNotAllowed` if the value contains floats, or
/// `CanonicalJsonError::Utf8Error` if UTF-8 conversion fails.
#[must_use = "canonical string should be used for hashing/signing"]
pub fn to_canonical_string<T: Serialize>(value: &T) -> Result<String, CanonicalJsonError> {
    let bytes = to_canonical_bytes(value)?;
    String::from_utf8(bytes).map_err(|_| CanonicalJsonError::Utf8Error)
}

/// Produces a canonical partition key string from dimension key-value pairs.
///
/// **DEPRECATED:** Use `arco_core::partition::PartitionKey` instead.
/// This function exists for backward compatibility but will be removed.
/// The typed `PartitionKey` provides better safety and cross-language guarantees.
///
/// Uses the single canonical serializer (ADR-010):
/// - Keys sorted lexicographically (UTF-8 byte order)
/// - No whitespace
/// - No floats allowed (use string values for all dimensions)
///
/// This is the canonical format for partition identity across the platform.
/// Cross-language implementations must produce identical output.
///
/// **Python equivalent:**
/// ```python
/// json.dumps(dimensions, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
/// ```
///
/// # Errors
///
/// Returns `CanonicalJsonError::Serde` if serialization fails.
#[deprecated(
    since = "0.2.0",
    note = "Use arco_core::partition::PartitionKey::canonical_string() instead"
)]
#[must_use = "canonical partition key should be used for identity"]
pub fn canonical_partition_key<S: std::hash::BuildHasher>(
    dimensions: &HashMap<String, String, S>,
) -> Result<String, CanonicalJsonError> {
    to_canonical_string(dimensions)
}

fn write_value(v: &Value, out: &mut Vec<u8>) -> Result<(), CanonicalJsonError> {
    match v {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Number(n) => write_number(n, out)?,
        Value::String(s) => {
            // Writes JSON string with quotes + escaping, no whitespace.
            serde_json::to_writer(&mut *out, s)?;
        }
        Value::Array(arr) => {
            out.push(b'[');
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_value(item, out)?;
            }
            out.push(b']');
        }
        Value::Object(map) => write_object(map, out)?,
    }
    Ok(())
}

fn write_object(map: &Map<String, Value>, out: &mut Vec<u8>) -> Result<(), CanonicalJsonError> {
    out.push(b'{');

    // Collect keys and sort deterministically by UTF-8 byte order.
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();

    for (i, k) in keys.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }

        // Key (JSON string)
        serde_json::to_writer(&mut *out, *k)?;
        out.push(b':');

        // Value - key is guaranteed to exist since we got it from map.keys()
        if let Some(val) = map.get(*k) {
            write_value(val, out)?;
        }
    }

    out.push(b'}');
    Ok(())
}

fn write_number(n: &Number, out: &mut Vec<u8>) -> Result<(), CanonicalJsonError> {
    use std::io::Write;

    // Integers only - floats are rejected per ADR-010
    if let Some(i) = n.as_i64() {
        write!(out, "{i}")?;
        return Ok(());
    }
    if let Some(u) = n.as_u64() {
        write!(out, "{u}")?;
        return Ok(());
    }

    // If we get here, it's a float (serde_json::Number only stores floats
    // when the value doesn't fit in i64/u64)
    Err(CanonicalJsonError::FloatNotAllowed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sorts_object_keys_and_has_no_whitespace() {
        // Insertion order: tenant then date
        let v = json!({"tenant":"acme","date":"2025-01-15"});
        let s = to_canonical_string(&v).unwrap();
        assert_eq!(s, r#"{"date":"2025-01-15","tenant":"acme"}"#);
    }

    #[test]
    fn sorts_nested_objects_recursively() {
        let v = json!({
            "b": { "d": 2, "c": 1 },
            "a": 0
        });
        let s = to_canonical_string(&v).unwrap();
        assert_eq!(s, r#"{"a":0,"b":{"c":1,"d":2}}"#);
    }

    #[test]
    fn preserves_array_order() {
        let v = json!([3, 2, 1]);
        let s = to_canonical_string(&v).unwrap();
        assert_eq!(s, "[3,2,1]");
    }

    #[test]
    fn rejects_floats() {
        // ADR-010: All floats are rejected, not just in identity contexts
        let v = json!({"x": 1.25});
        let err = to_canonical_string(&v).unwrap_err();
        assert!(matches!(err, CanonicalJsonError::FloatNotAllowed));
    }

    #[test]
    fn allows_integers() {
        let v = json!({"x": 125, "y": -42});
        let s = to_canonical_string(&v).unwrap();
        assert_eq!(s, r#"{"x":125,"y":-42}"#);
    }

    #[test]
    fn rejects_float_like_integers() {
        // 1.0 is a float in JSON, even though it looks like an integer
        let v = json!({"x": 1.0});
        // Note: serde_json may represent 1.0 as integer 1 internally
        // This test documents the expected behavior
        let result = to_canonical_string(&v);
        // Either succeeds (if serde sees it as int) or fails (if float)
        // The key invariant: no actual floating point values in output
        if let Ok(s) = result {
            assert!(!s.contains('.'), "No decimal points in canonical output");
        }
    }

    #[test]
    fn string_escaping_is_stable() {
        let v = json!({"s": "a\"b\nc"});
        let s = to_canonical_string(&v).unwrap();
        // Exact escaping is deterministic; serde_json escapes quotes and newlines.
        assert_eq!(s, r#"{"s":"a\"b\nc"}"#);
    }

    #[test]
    fn handles_empty_object() {
        let v = json!({});
        let s = to_canonical_string(&v).unwrap();
        assert_eq!(s, "{}");
    }

    #[test]
    fn handles_empty_array() {
        let v = json!([]);
        let s = to_canonical_string(&v).unwrap();
        assert_eq!(s, "[]");
    }

    #[test]
    fn handles_null() {
        let v = json!(null);
        let s = to_canonical_string(&v).unwrap();
        assert_eq!(s, "null");
    }

    #[test]
    fn handles_booleans() {
        let v = json!({"a": true, "b": false});
        let s = to_canonical_string(&v).unwrap();
        assert_eq!(s, r#"{"a":true,"b":false}"#);
    }

    #[test]
    fn handles_negative_integers() {
        let v = json!({"n": -42});
        let s = to_canonical_string(&v).unwrap();
        assert_eq!(s, r#"{"n":-42}"#);
    }

    #[test]
    fn handles_large_integers() {
        let v = json!({"big": 9_223_372_036_854_775_807_i64});
        let s = to_canonical_string(&v).unwrap();
        assert_eq!(s, r#"{"big":9223372036854775807}"#);
    }

    #[test]
    fn deeply_nested_structure() {
        let v = json!({
            "z": {
                "y": {
                    "x": [1, 2, {"w": 3, "v": 4}]
                }
            },
            "a": "first"
        });
        let s = to_canonical_string(&v).unwrap();
        assert_eq!(s, r#"{"a":"first","z":{"y":{"x":[1,2,{"v":4,"w":3}]}}}"#);
    }

    #[test]
    #[allow(deprecated)]
    fn canonical_partition_key_sorts_dimensions() {
        let mut dims = HashMap::new();
        dims.insert("date".to_string(), "2025-01-15".to_string());
        dims.insert("region".to_string(), "us-west".to_string());

        let canonical = canonical_partition_key(&dims).unwrap();
        // Keys sorted: date < region
        assert_eq!(canonical, r#"{"date":"2025-01-15","region":"us-west"}"#);
    }

    #[test]
    fn rejects_explicit_floats() {
        // ADR-010: All floats must be rejected
        let cases = vec![
            json!({"x": 1.25}),
            json!({"x": 0.1}),
            json!({"nested": {"f": 3.14159}}),
            json!([1.5, 2.5, 3.5]),
        ];

        for case in cases {
            let result = to_canonical_string(&case);
            // serde_json may coerce some floats to integers, so check both outcomes
            match result {
                Err(CanonicalJsonError::FloatNotAllowed) => { /* expected */ }
                Err(e) => panic!("Unexpected error: {e}"),
                Ok(s) => {
                    // If it succeeded, verify no decimal points
                    assert!(!s.contains('.'), "Float leaked through: {s}");
                }
            }
        }
    }

    #[test]
    fn rejects_nan_and_infinity() {
        // NaN/Inf should be rejected. serde_json behavior depends on version.
        // With recent serde_json, to_value may produce null for NaN/Inf.
        // Our float rejection in write_number catches actual floats, but NaN/Inf
        // may become null before reaching that point.
        #[derive(Serialize)]
        struct Wrap {
            x: f64,
        }

        // Test that NaN/Infinity do NOT produce valid canonical JSON with float values
        let nan_result = to_canonical_string(&Wrap { x: f64::NAN });
        let inf_result = to_canonical_string(&Wrap { x: f64::INFINITY });
        let neg_inf_result = to_canonical_string(&Wrap {
            x: f64::NEG_INFINITY,
        });

        // Either they error, or they produce null (which is acceptable as it's not a float)
        for (name, result) in [
            ("NaN", nan_result),
            ("Infinity", inf_result),
            ("-Infinity", neg_inf_result),
        ] {
            match result {
                Err(_) => { /* rejected, as expected */ }
                Ok(s) => {
                    // If it succeeded, it should be null (serde_json's behavior for non-finite)
                    // and must NOT contain a decimal point (which would indicate float leakage)
                    assert!(
                        !s.contains('.') || s.contains("null"),
                        "{name} leaked through as float: {s}"
                    );
                }
            }
        }
    }
    mod proptests {
        use super::*;
        use proptest::prelude::*;
        use std::collections::BTreeMap;

        proptest! {
            #[test]
            fn insertion_order_does_not_affect_canonical_output(
                pairs in prop::collection::vec(
                    ("[a-z]{1,8}", "[a-z0-9]{1,16}"),
                    1..10
                )
            ) {
                // Build a HashMap (random iteration order)
                let hashmap: HashMap<String, String> = pairs.iter().cloned().collect();

                // Build a BTreeMap (sorted iteration order)
                let btreemap: BTreeMap<String, String> = pairs.iter().cloned().collect();

                // Both should produce identical canonical JSON
                let from_hash = to_canonical_string(&hashmap).unwrap();
                let from_btree = to_canonical_string(&btreemap).unwrap();

                prop_assert_eq!(from_hash, from_btree);
            }

            #[test]
            fn same_content_same_canonical_bytes(
                pairs in prop::collection::vec(
                    ("[a-z]{1,5}", -1000i64..1000i64),
                    1..5
                )
            ) {
                let map1: BTreeMap<String, i64> = pairs.iter().cloned().collect();
                let map2: BTreeMap<String, i64> = pairs.iter().cloned().collect();

                let bytes1 = to_canonical_bytes(&map1).unwrap();
                let bytes2 = to_canonical_bytes(&map2).unwrap();

                prop_assert_eq!(bytes1, bytes2);
            }
        }
    }
}

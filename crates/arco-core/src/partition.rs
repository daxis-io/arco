//! Partition key types with cross-language canonical encoding.
//!
//! This module provides deterministic, URL-safe partition key encoding
//! that is identical across Rust and Python implementations.
//!
//! # Canonical Encoding Specification
//!
//! ```text
//! GRAMMAR:
//!   PARTITION_KEY_CANONICAL ::= dimension ("," dimension)*
//!   dimension              ::= key "=" typed_value
//!   key                    ::= [a-z][a-z0-9_]*   (alphabetically sorted)
//!   typed_value            ::= type_tag ":" encoded_value
//!
//!   type_tag ::=
//!     "s" (string)  | "i" (int64)    | "b" (bool)
//!     "d" (date)    | "t" (timestamp) | "n" (null)
//!
//!   encoded_value ::=
//!     For "s": base64url_no_pad(utf8_bytes)
//!     For "i": decimal integer (no leading zeros except "0")
//!     For "b": "true" | "false"
//!     For "d": "YYYY-MM-DD"
//!     For "t": "YYYY-MM-DDTHH:MM:SS.ffffffZ"
//!     For "n": "null"
//! ```
//!
//! # Examples
//!
//! ```rust
//! use arco_core::partition::{PartitionKey, ScalarValue};
//!
//! let mut pk = PartitionKey::new();
//! pk.insert("date", ScalarValue::Date("2025-01-15".into()));
//! pk.insert("region", ScalarValue::String("us-east".into()));
//!
//! // Keys are sorted, strings are base64url encoded
//! let canonical = pk.canonical_string();
//! assert!(canonical.starts_with("date=d:"));
//! ```

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

use crate::id::AssetId;

/// Scalar value types allowed in partition keys.
///
/// Floats are intentionally excluded to prevent precision drift
/// across languages and serialization formats.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ScalarValue {
    /// Arbitrary string (base64url encoded in canonical form).
    String(String),
    /// 64-bit signed integer.
    Int64(i64),
    /// Boolean value.
    Boolean(bool),
    /// Date in YYYY-MM-DD format.
    Date(String),
    /// Timestamp in ISO 8601 format with microseconds, UTC.
    Timestamp(String),
    /// Explicit null value.
    Null,
}

impl ScalarValue {
    /// Returns the canonical representation with type tag.
    ///
    /// String values are base64url encoded (no padding) for URL safety.
    #[must_use]
    pub fn canonical_repr(&self) -> String {
        match self {
            Self::String(s) => {
                let encoded = URL_SAFE_NO_PAD.encode(s.as_bytes());
                format!("s:{encoded}")
            }
            Self::Int64(n) => format!("i:{n}"),
            Self::Boolean(b) => format!("b:{}", if *b { "true" } else { "false" }),
            Self::Date(d) => format!("d:{d}"),
            Self::Timestamp(ts) => format!("t:{ts}"),
            Self::Null => "n:null".to_string(),
        }
    }

    /// Returns the type tag character.
    #[must_use]
    pub const fn type_tag(&self) -> char {
        match self {
            Self::String(_) => 's',
            Self::Int64(_) => 'i',
            Self::Boolean(_) => 'b',
            Self::Date(_) => 'd',
            Self::Timestamp(_) => 't',
            Self::Null => 'n',
        }
    }
}

/// Multi-dimensional partition key with deterministic canonical form.
///
/// Uses `BTreeMap` internally to ensure keys are always sorted alphabetically.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PartitionKey(BTreeMap<String, ScalarValue>);

impl PartitionKey {
    /// Creates a new empty partition key.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a dimension into the partition key.
    ///
    /// If a dimension with the same key exists, it is replaced.
    pub fn insert(&mut self, key: impl Into<String>, value: ScalarValue) {
        self.0.insert(key.into(), value);
    }

    /// Returns the canonical string representation.
    ///
    /// This is deterministic: same logical key produces same string,
    /// regardless of insertion order.
    #[must_use]
    pub fn canonical_string(&self) -> String {
        self.0
            .iter()
            .map(|(k, v)| format!("{k}={}", v.canonical_repr()))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Returns canonical bytes for hashing.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        self.canonical_string().into_bytes()
    }

    /// Returns true if the partition key has no dimensions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of dimensions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Gets a dimension value by key.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&ScalarValue> {
        self.0.get(key)
    }

    /// Returns an iterator over dimensions.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &ScalarValue)> {
        self.0.iter()
    }

    /// Parses a canonical string back into a `PartitionKey`.
    ///
    /// This is the inverse of `canonical_string()`. Round-trip property:
    /// `PartitionKey::parse(pk.canonical_string()) == Ok(pk)`
    ///
    /// # Errors
    /// Returns error if the string is malformed.
    pub fn parse(s: &str) -> Result<Self, PartitionKeyParseError> {
        if s.is_empty() {
            return Ok(Self::new());
        }

        let mut pk = Self::new();
        for part in s.split(',') {
            let (key, encoded) = part
                .split_once('=')
                .ok_or_else(|| PartitionKeyParseError::MissingEquals(part.to_string()))?;

            // Validate key (must start with letter, then alphanumeric/underscore)
            if key.is_empty()
                || !key.chars().next().is_some_and(|c| c.is_ascii_lowercase())
                || !key.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            {
                return Err(PartitionKeyParseError::InvalidKey(key.to_string()));
            }

            // Parse type prefix and value
            let (type_prefix, value_part) = encoded
                .split_once(':')
                .ok_or_else(|| PartitionKeyParseError::MissingTypePrefix(encoded.to_string()))?;

            let scalar = match type_prefix {
                "n" => {
                    if value_part != "null" {
                        return Err(PartitionKeyParseError::InvalidNull(value_part.to_string()));
                    }
                    ScalarValue::Null
                }
                "b" => match value_part {
                    "true" => ScalarValue::Boolean(true),
                    "false" => ScalarValue::Boolean(false),
                    _ => return Err(PartitionKeyParseError::InvalidBool(value_part.to_string())),
                },
                "i" => {
                    let n: i64 = value_part
                        .parse()
                        .map_err(|_| PartitionKeyParseError::InvalidInt(value_part.to_string()))?;
                    ScalarValue::Int64(n)
                }
                "s" => {
                    // Decode base64url
                    let bytes = URL_SAFE_NO_PAD
                        .decode(value_part)
                        .map_err(|_| PartitionKeyParseError::InvalidBase64(value_part.to_string()))?;
                    let string = String::from_utf8(bytes)
                        .map_err(|_| PartitionKeyParseError::InvalidUtf8(value_part.to_string()))?;
                    ScalarValue::String(string)
                }
                "d" => {
                    // Validate date format (YYYY-MM-DD)
                    if !is_valid_date_format(value_part) {
                        return Err(PartitionKeyParseError::InvalidDate(value_part.to_string()));
                    }
                    ScalarValue::Date(value_part.to_string())
                }
                "t" => {
                    // Validate timestamp format
                    if !is_valid_timestamp_format(value_part) {
                        return Err(PartitionKeyParseError::InvalidTimestamp(
                            value_part.to_string(),
                        ));
                    }
                    ScalarValue::Timestamp(value_part.to_string())
                }
                _ => return Err(PartitionKeyParseError::UnknownType(type_prefix.to_string())),
            };

            pk.insert(key.to_string(), scalar);
        }

        Ok(pk)
    }
}

/// Validates date format (YYYY-MM-DD).
fn is_valid_date_format(s: &str) -> bool {
    if s.len() != 10 {
        return false;
    }
    let bytes = s.as_bytes();
    // SAFETY: We've verified s.len() == 10, so these indices are valid
    bytes.get(4) == Some(&b'-')
        && bytes.get(7) == Some(&b'-')
        && s.get(..4).is_some_and(|y| y.parse::<u16>().is_ok())
        && s.get(5..7).is_some_and(|m| m.parse::<u8>().is_ok())
        && s.get(8..).is_some_and(|d| d.parse::<u8>().is_ok())
}

/// Validates timestamp format (YYYY-MM-DDTHH:MM:SS.ffffffZ).
fn is_valid_timestamp_format(s: &str) -> bool {
    // Basic length check for ISO 8601 with microseconds
    s.len() >= 20 && s.ends_with('Z') && s.contains('T')
}

/// Errors that can occur when parsing a canonical partition key string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionKeyParseError {
    /// Missing '=' separator between key and value.
    MissingEquals(String),
    /// Invalid key (empty or invalid characters).
    InvalidKey(String),
    /// Missing type prefix (e.g., "s:", "i:").
    MissingTypePrefix(String),
    /// Unknown type prefix.
    UnknownType(String),
    /// Invalid boolean value (must be "true" or "false").
    InvalidBool(String),
    /// Invalid integer value.
    InvalidInt(String),
    /// Invalid base64 encoding.
    InvalidBase64(String),
    /// Invalid UTF-8 in decoded string.
    InvalidUtf8(String),
    /// Invalid date value.
    InvalidDate(String),
    /// Invalid timestamp value.
    InvalidTimestamp(String),
    /// Invalid null value (must be "null").
    InvalidNull(String),
}

impl fmt::Display for PartitionKeyParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEquals(s) => write!(f, "missing '=' in partition segment: {s}"),
            Self::InvalidKey(s) => write!(f, "invalid partition key: {s}"),
            Self::MissingTypePrefix(s) => write!(f, "missing type prefix in value: {s}"),
            Self::UnknownType(s) => write!(f, "unknown type prefix: {s}"),
            Self::InvalidBool(s) => write!(f, "invalid boolean value: {s}"),
            Self::InvalidInt(s) => write!(f, "invalid integer value: {s}"),
            Self::InvalidBase64(s) => write!(f, "invalid base64 encoding: {s}"),
            Self::InvalidUtf8(s) => write!(f, "invalid UTF-8 in decoded string: {s}"),
            Self::InvalidDate(s) => write!(f, "invalid date value: {s}"),
            Self::InvalidTimestamp(s) => write!(f, "invalid timestamp value: {s}"),
            Self::InvalidNull(s) => write!(f, "invalid null value: {s}"),
        }
    }
}

impl std::error::Error for PartitionKeyParseError {}

/// Derived partition identifier (stable across re-materializations).
///
/// The partition ID is derived from `hash(asset_id + canonical_partition_key)`,
/// ensuring the same asset partition always has the same ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionId(String);

impl PartitionId {
    /// Derives partition ID from asset ID + partition key.
    ///
    /// The derivation is deterministic: same inputs always produce same ID.
    #[must_use]
    pub fn derive(asset_id: &AssetId, partition_key: &PartitionKey) -> Self {
        let input = format!("{}:{}", asset_id, partition_key.canonical_string());
        let hash = Sha256::digest(input.as_bytes());
        // Use first 16 hex chars (64 bits) for reasonable uniqueness
        let short_hash = &hex::encode(hash)[..16];
        Self(format!("part_{short_hash}"))
    }

    /// Returns the ID string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_canonical_string_single_date() {
        let mut pk = PartitionKey::new();
        pk.insert("date", ScalarValue::Date("2025-01-15".into()));

        assert_eq!(pk.canonical_string(), "date=d:2025-01-15");
    }

    #[test]
    fn test_canonical_string_sorted_keys() {
        let mut pk = PartitionKey::new();
        // Insert in reverse order
        pk.insert("region", ScalarValue::String("us-east".into()));
        pk.insert("date", ScalarValue::Date("2025-01-15".into()));

        // Keys should be alphabetically sorted in output
        // "us-east" base64url = "dXMtZWFzdA"
        assert_eq!(
            pk.canonical_string(),
            "date=d:2025-01-15,region=s:dXMtZWFzdA"
        );
    }

    #[test]
    fn test_canonical_string_url_safe() {
        let mut pk = PartitionKey::new();
        pk.insert("path", ScalarValue::String("foo/bar?baz=1&x=2".into()));

        let canonical = pk.canonical_string();

        // Must not contain URL-unsafe characters in the value part
        let value_part = canonical.split('=').nth(1).unwrap();
        assert!(!value_part.contains('/'));
        assert!(!value_part.contains('?'));
        assert!(!value_part.contains('&'));

        // "foo/bar?baz=1&x=2" base64url = "Zm9vL2Jhcj9iYXo9MSZ4PTI"
        assert_eq!(canonical, "path=s:Zm9vL2Jhcj9iYXo9MSZ4PTI");
    }

    #[test]
    fn test_canonical_string_all_types() {
        let mut pk = PartitionKey::new();
        pk.insert("active", ScalarValue::Boolean(true));
        pk.insert("count", ScalarValue::Int64(42));
        pk.insert("date", ScalarValue::Date("2025-01-15".into()));
        pk.insert("name", ScalarValue::String("test".into()));
        pk.insert(
            "ts",
            ScalarValue::Timestamp("2025-01-15T10:30:00.000000Z".into()),
        );
        pk.insert("empty", ScalarValue::Null);

        let canonical = pk.canonical_string();

        assert!(canonical.contains("active=b:true"));
        assert!(canonical.contains("count=i:42"));
        assert!(canonical.contains("date=d:2025-01-15"));
        assert!(canonical.contains("empty=n:null"));
        // "test" base64url = "dGVzdA"
        assert!(canonical.contains("name=s:dGVzdA"));
        assert!(canonical.contains("ts=t:2025-01-15T10:30:00.000000Z"));
    }

    #[test]
    fn test_canonical_deterministic_regardless_of_insertion_order() {
        let mut pk1 = PartitionKey::new();
        pk1.insert("z", ScalarValue::Int64(1));
        pk1.insert("a", ScalarValue::Int64(2));
        pk1.insert("m", ScalarValue::Int64(3));

        let mut pk2 = PartitionKey::new();
        pk2.insert("a", ScalarValue::Int64(2));
        pk2.insert("m", ScalarValue::Int64(3));
        pk2.insert("z", ScalarValue::Int64(1));

        let mut pk3 = PartitionKey::new();
        pk3.insert("m", ScalarValue::Int64(3));
        pk3.insert("z", ScalarValue::Int64(1));
        pk3.insert("a", ScalarValue::Int64(2));

        assert_eq!(pk1.canonical_string(), pk2.canonical_string());
        assert_eq!(pk2.canonical_string(), pk3.canonical_string());
    }

    #[test]
    fn test_partition_id_derivation() {
        let asset_id = AssetId::generate();
        let mut pk = PartitionKey::new();
        pk.insert("date", ScalarValue::Date("2025-01-15".into()));

        let id1 = PartitionId::derive(&asset_id, &pk);
        let id2 = PartitionId::derive(&asset_id, &pk);

        // Same inputs -> same ID
        assert_eq!(id1, id2);

        // Different inputs -> different ID
        let mut pk_different = PartitionKey::new();
        pk_different.insert("date", ScalarValue::Date("2025-01-16".into()));
        let id3 = PartitionId::derive(&asset_id, &pk_different);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_empty_partition_key() {
        let pk = PartitionKey::new();
        assert!(pk.is_empty());
        assert_eq!(pk.canonical_string(), "");
    }

    #[test]
    fn test_partition_key_roundtrip() {
        let mut pk = PartitionKey::new();
        pk.insert("date", ScalarValue::Date("2025-01-15".into()));
        pk.insert("region", ScalarValue::String("us-east".into()));
        pk.insert("count", ScalarValue::Int64(42));
        pk.insert("active", ScalarValue::Boolean(true));
        pk.insert("empty", ScalarValue::Null);

        let canonical = pk.canonical_string();
        let parsed = PartitionKey::parse(&canonical).expect("should parse");

        assert_eq!(pk, parsed);
    }

    #[test]
    fn test_partition_key_parse_invalid_key() {
        let result = PartitionKey::parse("123invalid=s:dGVzdA");
        assert!(matches!(result, Err(PartitionKeyParseError::InvalidKey(_))));
    }

    #[test]
    fn test_partition_key_parse_missing_equals() {
        let result = PartitionKey::parse("noequals");
        assert!(matches!(
            result,
            Err(PartitionKeyParseError::MissingEquals(_))
        ));
    }

    #[test]
    fn test_partition_key_parse_unknown_type() {
        let result = PartitionKey::parse("key=x:value");
        assert!(matches!(
            result,
            Err(PartitionKeyParseError::UnknownType(_))
        ));
    }

    #[test]
    fn test_scalar_value_type_tags() {
        assert_eq!(ScalarValue::String("test".into()).type_tag(), 's');
        assert_eq!(ScalarValue::Int64(42).type_tag(), 'i');
        assert_eq!(ScalarValue::Boolean(true).type_tag(), 'b');
        assert_eq!(ScalarValue::Date("2025-01-15".into()).type_tag(), 'd');
        assert_eq!(
            ScalarValue::Timestamp("2025-01-15T10:30:00.000000Z".into()).type_tag(),
            't'
        );
        assert_eq!(ScalarValue::Null.type_tag(), 'n');
    }

    #[test]
    fn test_partition_id_format() {
        let asset_id = AssetId::generate();
        let mut pk = PartitionKey::new();
        pk.insert("date", ScalarValue::Date("2025-01-15".into()));

        let id = PartitionId::derive(&asset_id, &pk);

        // Should start with "part_" and have 16 hex chars
        assert!(id.as_str().starts_with("part_"));
        assert_eq!(id.as_str().len(), 5 + 16); // "part_" + 16 hex chars
    }
}

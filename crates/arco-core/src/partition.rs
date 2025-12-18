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

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
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

    /// Returns a rank for ordering by type.
    const fn type_rank(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::Int64(_) => 2,
            Self::String(_) => 3,
            Self::Date(_) => 4,
            Self::Timestamp(_) => 5,
        }
    }
}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScalarValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        // Order by type first, then by value
        match self.type_rank().cmp(&other.type_rank()) {
            Ordering::Equal => {}
            ord => return ord,
        }

        // Same type - compare values
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Boolean(a), Self::Boolean(b)) => a.cmp(b),
            (Self::Int64(a), Self::Int64(b)) => a.cmp(b),
            // String-like types all compare as strings
            (Self::String(a), Self::String(b))
            | (Self::Date(a), Self::Date(b))
            | (Self::Timestamp(a), Self::Timestamp(b)) => a.cmp(b),
            // Different types are handled by type_rank comparison above
            _ => unreachable!("same type_rank should match same variant"),
        }
    }
}

/// Multi-dimensional partition key with deterministic canonical form.
///
/// Uses `BTreeMap` internally to ensure keys are always sorted alphabetically.
/// Implements `Ord` for use in sorted collections and deterministic comparisons.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct PartitionKey(BTreeMap<String, ScalarValue>);

impl PartialOrd for PartitionKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PartitionKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by canonical string for deterministic cross-language ordering
        self.canonical_string().cmp(&other.canonical_string())
    }
}

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
                || !key
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
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
                    let bytes = URL_SAFE_NO_PAD.decode(value_part).map_err(|_| {
                        PartitionKeyParseError::InvalidBase64(value_part.to_string())
                    })?;
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

            // Reject duplicate keys
            if pk.0.contains_key(key) {
                return Err(PartitionKeyParseError::DuplicateKey(key.to_string()));
            }

            pk.insert(key.to_string(), scalar);
        }

        Ok(pk)
    }
}

/// Validates date format (YYYY-MM-DD) with valid ranges.
fn is_valid_date_format(s: &str) -> bool {
    if s.len() != 10 {
        return false;
    }
    let bytes = s.as_bytes();
    if bytes.get(4) != Some(&b'-') || bytes.get(7) != Some(&b'-') {
        return false;
    }

    // Parse and validate ranges
    let _year: u16 = match s.get(..4).and_then(|y| y.parse().ok()) {
        Some(y) if y >= 1970 => y, // Reasonable minimum year
        _ => return false,
    };
    let month: u8 = match s.get(5..7).and_then(|m| m.parse().ok()) {
        Some(m) if (1..=12).contains(&m) => m,
        _ => return false,
    };
    let day: u8 = match s.get(8..).and_then(|d| d.parse().ok()) {
        Some(d) if (1..=31).contains(&d) => d,
        _ => return false,
    };

    // Basic month-day validation (not checking leap years for simplicity)
    match month {
        2 => day <= 29,
        4 | 6 | 9 | 11 => day <= 30,
        _ => true,
    }
}

/// Validates timestamp format (YYYY-MM-DDTHH:MM:SS.ffffffZ).
/// Requires exact microsecond precision ISO 8601 format.
fn is_valid_timestamp_format(s: &str) -> bool {
    // Expected format: YYYY-MM-DDTHH:MM:SS.ffffffZ (27 chars)
    if s.len() != 27 || !s.ends_with('Z') {
        return false;
    }

    let bytes = s.as_bytes();

    // Check structural characters
    if bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
        || bytes.get(19) != Some(&b'.')
    {
        return false;
    }

    // Validate date portion
    let Some(date_part) = s.get(..10) else {
        return false;
    };
    if !is_valid_date_format(date_part) {
        return false;
    }

    // Validate time components
    let _hour: u8 = match s.get(11..13).and_then(|h| h.parse().ok()) {
        Some(h) if h <= 23 => h,
        _ => return false,
    };
    let _minute: u8 = match s.get(14..16).and_then(|m| m.parse().ok()) {
        Some(m) if m <= 59 => m,
        _ => return false,
    };
    let _second: u8 = match s.get(17..19).and_then(|s| s.parse().ok()) {
        Some(s) if s <= 59 => s,
        _ => return false,
    };

    // Validate microseconds (6 digits)
    s.get(20..26)
        .is_some_and(|us| us.chars().all(|c| c.is_ascii_digit()))
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
    /// Duplicate key in partition key.
    DuplicateKey(String),
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
            Self::DuplicateKey(s) => write!(f, "duplicate key in partition key: {s}"),
        }
    }
}

impl std::error::Error for PartitionKeyParseError {}

/// Derived partition identifier (stable across re-materializations).
///
/// The partition ID is derived from `hash(asset_id + partition_key.canonical_string())`,
/// ensuring the same asset partition always has the same ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionId(String);

impl PartitionId {
    /// Derives partition ID from asset ID + partition key.
    ///
    /// The derivation is deterministic: same inputs always produce same ID.
    /// Uses 128 bits of SHA-256 for collision resistance at high cardinality.
    #[must_use]
    pub fn derive(asset_id: &AssetId, partition_key: &PartitionKey) -> Self {
        let input = format!("{}:{}", asset_id, partition_key.canonical_string());
        let hash = Sha256::digest(input.as_bytes());
        // Use first 32 hex chars (128 bits) for collision resistance
        let short_hash = &hex::encode(hash)[..32];
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
        let value_part = match canonical.split_once('=') {
            Some((_key, value)) => value,
            None => {
                assert!(false, "canonical string should contain '=': {canonical}");
                ""
            }
        };
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
        let parsed = match PartitionKey::parse(&canonical) {
            Ok(value) => value,
            Err(err) => {
                assert!(false, "partition key should roundtrip: {err}");
                return;
            }
        };

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

        // Should start with "part_" and have 32 hex chars (128 bits)
        assert!(id.as_str().starts_with("part_"));
        assert_eq!(id.as_str().len(), 5 + 32); // "part_" + 32 hex chars
    }

    #[test]
    fn test_parse_rejects_duplicate_keys() {
        let result = PartitionKey::parse("date=d:2025-01-15,date=d:2025-01-16");
        assert!(matches!(
            result,
            Err(PartitionKeyParseError::DuplicateKey(_))
        ));
    }

    #[test]
    fn test_parse_rejects_invalid_date_month() {
        let result = PartitionKey::parse("date=d:2025-13-15");
        assert!(matches!(
            result,
            Err(PartitionKeyParseError::InvalidDate(_))
        ));
    }

    #[test]
    fn test_parse_rejects_invalid_date_day() {
        let result = PartitionKey::parse("date=d:2025-02-30");
        assert!(matches!(
            result,
            Err(PartitionKeyParseError::InvalidDate(_))
        ));
    }

    #[test]
    fn test_parse_rejects_invalid_timestamp_format() {
        // Missing microseconds
        let result = PartitionKey::parse("ts=t:2025-01-15T10:30:00Z");
        assert!(matches!(
            result,
            Err(PartitionKeyParseError::InvalidTimestamp(_))
        ));

        // Wrong length
        let result = PartitionKey::parse("ts=t:2025-01-15T10:30:00.000Z");
        assert!(matches!(
            result,
            Err(PartitionKeyParseError::InvalidTimestamp(_))
        ));
    }

    #[test]
    fn test_parse_accepts_valid_timestamp() -> Result<(), PartitionKeyParseError> {
        let pk = PartitionKey::parse("ts=t:2025-01-15T10:30:00.000000Z")?;
        assert_eq!(
            pk.get("ts"),
            Some(&ScalarValue::Timestamp(
                "2025-01-15T10:30:00.000000Z".into()
            ))
        );
        Ok(())
    }

    #[test]
    fn test_parse_rejects_year_before_1970() {
        let result = PartitionKey::parse("date=d:1969-12-31");
        assert!(matches!(
            result,
            Err(PartitionKeyParseError::InvalidDate(_))
        ));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Proptest configuration for CI predictability.
    ///
    /// Bounds the number of cases to ensure predictable runtime.
    /// Override via `PROPTEST_CASES` environment variable if needed.
    const PROPTEST_CASES: u32 = 256;

    fn test_config() -> ProptestConfig {
        ProptestConfig {
            cases: PROPTEST_CASES,
            ..ProptestConfig::default()
        }
    }

    fn key_name_strategy() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_]{0,19}".prop_filter("non-empty key", |s| !s.is_empty())
    }

    fn date_strategy() -> impl Strategy<Value = String> {
        (1970u16..2100, 1u8..=12, 1u8..=28)
            .prop_map(|(year, month, day)| format!("{year:04}-{month:02}-{day:02}"))
    }

    fn timestamp_strategy() -> impl Strategy<Value = String> {
        (
            1970u16..2100,
            1u8..=12,
            1u8..=28,
            0u8..24,
            0u8..60,
            0u8..60,
            0u32..1_000_000,
        )
            .prop_map(|(year, month, day, hour, minute, second, micros)| {
                format!(
                    "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}Z"
                )
            })
    }

    fn scalar_value_strategy() -> impl Strategy<Value = ScalarValue> {
        prop_oneof![
            any::<String>().prop_map(ScalarValue::String),
            any::<i64>().prop_map(ScalarValue::Int64),
            any::<bool>().prop_map(ScalarValue::Boolean),
            date_strategy().prop_map(ScalarValue::Date),
            timestamp_strategy().prop_map(ScalarValue::Timestamp),
            Just(ScalarValue::Null),
        ]
    }

    fn partition_key_strategy() -> impl Strategy<Value = PartitionKey> {
        prop::collection::btree_map(key_name_strategy(), scalar_value_strategy(), 0..=5)
            .prop_map(PartitionKey)
    }

    proptest! {
        #![proptest_config(test_config())]

        #[test]
        fn partition_key_roundtrip(pk in partition_key_strategy()) {
            let canonical = pk.canonical_string();
            let parsed = match PartitionKey::parse(&canonical) {
                Ok(value) => value,
                Err(err) => {
                    prop_assert!(
                        false,
                        "canonical string should always be parseable: {err} (canonical: {canonical})"
                    );
                    PartitionKey::new()
                }
            };
            prop_assert_eq!(pk, parsed, "roundtrip failed for canonical: {}", canonical);
        }

        #[test]
        fn canonical_string_deterministic(pk in partition_key_strategy()) {
            let s1 = pk.canonical_string();
            let s2 = pk.canonical_string();
            prop_assert_eq!(s1, s2);
        }

        #[test]
        fn canonical_string_url_safe(pk in partition_key_strategy()) {
            let canonical = pk.canonical_string();
            for segment in canonical.split(',') {
                if let Some((_key, value)) = segment.split_once('=') {
                    let after_colon = value.split_once(':').map_or(value, |(_, v)| v);
                    prop_assert!(!after_colon.contains('/'), "URL-unsafe '/' in value: {}", value);
                    prop_assert!(!after_colon.contains('?'), "URL-unsafe '?' in value: {}", value);
                    prop_assert!(!after_colon.contains('&'), "URL-unsafe '&' in value: {}", value);
                    prop_assert!(!after_colon.contains(' '), "URL-unsafe space in value: {}", value);
                }
            }
        }

        #[test]
        fn partition_id_deterministic(pk in partition_key_strategy()) {
            let asset_id = AssetId::generate();
            let id1 = PartitionId::derive(&asset_id, &pk);
            let id2 = PartitionId::derive(&asset_id, &pk);
            prop_assert!(id1.as_str().starts_with("part_"));
            prop_assert_eq!(id1.as_str().len(), 5 + 32);
            prop_assert_eq!(id1, id2);
        }
    }
}

//! Governed storage path canonicalization.

use crate::error::{CatalogError, Result};

/// Canonical governed path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct GovernedPath {
    scheme: String,
    authority: Option<String>,
    path: String,
}

impl GovernedPath {
    /// Parses and canonicalizes a governed URI.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported URI shapes, traversal segments, or bad
    /// percent encoding.
    pub fn parse(raw: &str) -> Result<Self> {
        let (scheme, rest) = raw
            .split_once("://")
            .ok_or_else(|| validation("path must include a URI scheme"))?;
        let scheme = scheme.to_ascii_lowercase();
        match scheme.as_str() {
            "gs" | "s3" | "abfss" => parse_cloud_uri(&scheme, rest),
            "file" => parse_file_uri(rest),
            _ => Err(validation(format!("unsupported URI scheme '{scheme}'"))),
        }
    }

    /// Returns the canonical URI.
    ///
    /// Path segments are percent-encoded on emission (RFC 3986 `pchar` bytes
    /// stay literal, everything else — including `%` itself — is escaped), so
    /// the canonical URI is a fixed point of [`GovernedPath::parse`]: for
    /// every parse-accepted input, re-parsing the canonical URI succeeds and
    /// yields an equal `GovernedPath`.
    #[must_use]
    pub fn canonical_uri(&self) -> String {
        let encoded_path = encode_canonical_path(&self.path);
        self.authority.as_ref().map_or_else(
            || format!("{}://{}", self.scheme, encoded_path),
            |authority| format!("{}://{}{}", self.scheme, authority, encoded_path),
        )
    }

    /// Returns the canonical URI after verifying it round-trips through
    /// [`GovernedPath::parse`] unchanged.
    ///
    /// Persistence boundaries (external-location creation, path-governance
    /// declarations) must use this instead of [`GovernedPath::canonical_uri`]:
    /// a canonical string that failed to re-parse would poison every future
    /// replay of the append-only ledger, so it is rejected before anything is
    /// persisted.
    ///
    /// # Errors
    ///
    /// Returns a validation error when re-parsing the canonical URI fails or
    /// yields a different path. [`GovernedPath::canonical_uri`] is constructed
    /// so this cannot happen; the guard exists to keep any future
    /// canonicalization regression from persisting unreadable state.
    pub fn persistable_canonical_uri(&self) -> Result<String> {
        let canonical = self.canonical_uri();
        match Self::parse(&canonical) {
            Ok(reparsed) if reparsed == *self => Ok(canonical),
            Ok(_) => Err(validation(format!(
                "canonical path '{canonical}' does not round-trip to the same governed path and \
                 cannot be persisted"
            ))),
            Err(error) => Err(validation(format!(
                "canonical path '{canonical}' fails re-parsing and cannot be persisted: {error}"
            ))),
        }
    }

    /// Returns true when `self` is a prefix authority for `candidate`.
    #[must_use]
    pub fn contains(&self, candidate: &Self) -> bool {
        self.scheme == candidate.scheme
            && self.authority == candidate.authority
            && candidate.path.starts_with(&self.path)
    }

    /// Returns true when two path authorities overlap.
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        let self_contains_other = self.path.starts_with(&other.path);
        let other_contains_self = other.path.starts_with(&self.path);

        self.scheme == other.scheme
            && self.authority == other.authority
            && (self_contains_other || other_contains_self)
    }
}

fn parse_cloud_uri(scheme: &str, rest: &str) -> Result<GovernedPath> {
    let (authority, path) = rest
        .split_once('/')
        .map_or((rest, ""), |(authority, path)| (authority, path));
    if authority.is_empty() {
        return Err(validation("cloud URI authority must not be empty"));
    }
    Ok(GovernedPath {
        scheme: scheme.to_string(),
        authority: Some(authority.to_ascii_lowercase()),
        // The authority split already consumed the single separator that roots
        // the object key, so a further leading empty segment is a consecutive
        // slash run, not structure.
        path: canonical_path(path, LeadingSeparator::Consumed)?,
    })
}

fn parse_file_uri(rest: &str) -> Result<GovernedPath> {
    Ok(GovernedPath {
        scheme: "file".to_string(),
        authority: None,
        // `file:///tmp/...` roots the path with a separator that belongs to the
        // path itself, so one leading empty segment is structural.
        path: canonical_path(rest, LeadingSeparator::Structural)?,
    })
}

/// Whether a leading empty segment in the raw path is structure or a
/// consecutive-slash run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeadingSeparator {
    /// The scheme/authority split already consumed the rooting separator
    /// (cloud URIs), so a leading empty segment is a duplicate slash.
    Consumed,
    /// The rooting separator is part of the path (`file://` URIs), so one
    /// leading empty segment is structural.
    Structural,
}

/// Canonicalizes the path component, rejecting non-structural empty segments.
///
/// Cloud object stores address objects by an opaque key in which consecutive
/// `/` bytes are preserved: `a/b/object` and `a//b/object` are *different*
/// objects under different physical prefixes. Collapsing empty segments would
/// therefore alias distinct physical prefixes onto one canonical governed
/// identity, letting a path authority declared over `gs://bucket/a/b/`
/// authorize (and vend credentials scoped to) `gs://bucket/a//b/`, which is
/// not physically under it. Rather than model per-provider key semantics, the
/// boundary rejects the ambiguous input: only a rooting leading separator and
/// a single trailing separator are structural.
fn canonical_path(path: &str, leading: LeadingSeparator) -> Result<String> {
    let raw_segments: Vec<&str> = path.split('/').collect();
    let last_index = raw_segments.len().saturating_sub(1);
    let mut segments = Vec::with_capacity(raw_segments.len());
    for (index, raw_segment) in raw_segments.iter().enumerate() {
        if raw_segment.is_empty() {
            if index == last_index || (index == 0 && leading == LeadingSeparator::Structural) {
                continue;
            }
            return Err(validation(
                "empty path segments are not allowed: consecutive '/' separators address a \
                 distinct object prefix",
            ));
        }
        let segment = percent_decode(raw_segment)?;
        if segment == "." || segment == ".." {
            return Err(validation("path traversal segments are not allowed"));
        }
        if segment.contains('/') || segment.contains('\\') {
            return Err(validation("encoded path separators are not allowed"));
        }
        segments.push(segment);
    }
    let mut canonical = String::new();
    canonical.push('/');
    canonical.push_str(&segments.join("/"));
    if !canonical.ends_with('/') {
        canonical.push('/');
    }
    Ok(canonical)
}

fn encode_canonical_path(path: &str) -> String {
    // The canonical path shape is "/" or "/segment/.../"; splitting on '/'
    // preserves the leading and trailing empty pieces, so joining re-creates
    // exactly the same slash structure around the encoded segments.
    path.split('/')
        .map(percent_encode_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn percent_encode_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if is_literal_segment_byte(byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(hex_digit(byte >> 4));
            encoded.push(hex_digit(byte & 0x0f));
        }
    }
    encoded
}

/// RFC 3986 `pchar` bytes that may appear literally in an emitted canonical
/// segment: unreserved characters, sub-delimiters, `:` and `@`.
///
/// Everything else — including `%` itself, whitespace, `?`, `#`, and
/// non-ASCII bytes — is percent-encoded so that decoding the emitted segment
/// yields exactly the stored segment.
const fn is_literal_segment_byte(byte: u8) -> bool {
    matches!(byte,
        b'A'..=b'Z'
        | b'a'..=b'z'
        | b'0'..=b'9'
        | b'-' | b'.' | b'_' | b'~'
        | b'!' | b'$' | b'&' | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b';' | b'='
        | b':' | b'@')
}

fn hex_digit(value: u8) -> char {
    if value < 10 {
        char::from(b'0' + value)
    } else {
        char::from(b'A' + (value - 10))
    }
}

fn percent_decode(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        if byte == b'%' {
            let Some(&high_byte) = bytes.get(index + 1) else {
                return Err(validation("incomplete percent encoding"));
            };
            let Some(&low_byte) = bytes.get(index + 2) else {
                return Err(validation("incomplete percent encoding"));
            };
            let high = hex_value(high_byte)?;
            let low = hex_value(low_byte)?;
            decoded.push(high * 16 + low);
            index += 3;
        } else {
            decoded.push(byte);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| validation("percent-decoded path is not UTF-8"))
}

fn hex_value(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(validation("invalid percent encoding")),
    }
}

fn validation(message: impl Into<String>) -> CatalogError {
    CatalogError::Validation {
        message: message.into(),
    }
}

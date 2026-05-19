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
    #[must_use]
    pub fn canonical_uri(&self) -> String {
        self.authority.as_ref().map_or_else(
            || format!("{}://{}", self.scheme, self.path),
            |authority| format!("{}://{}{}", self.scheme, authority, self.path),
        )
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
        path: canonical_path(path, false)?,
    })
}

fn parse_file_uri(rest: &str) -> Result<GovernedPath> {
    Ok(GovernedPath {
        scheme: "file".to_string(),
        authority: None,
        path: canonical_path(rest, true)?,
    })
}

fn canonical_path(path: &str, absolute: bool) -> Result<String> {
    let mut segments = Vec::new();
    for raw_segment in path.split('/') {
        if raw_segment.is_empty() {
            continue;
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
    if absolute && !canonical.starts_with('/') {
        canonical.insert(0, '/');
    }
    Ok(canonical)
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

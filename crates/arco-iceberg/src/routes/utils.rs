//! Shared helpers for Iceberg REST routes.

use crate::error::{IcebergError, IcebergResult};
use crate::state::IcebergConfig;
use crate::types::NamespaceIdent;

/// Validates the catalog prefix in the request path.
pub fn ensure_prefix(prefix: &str, config: &IcebergConfig) -> IcebergResult<()> {
    if prefix != config.prefix {
        return Err(IcebergError::BadRequest {
            message: format!("Unsupported catalog prefix: {prefix}"),
            error_type: "BadRequestException",
        });
    }
    Ok(())
}

/// Parses a namespace string into an identifier list.
pub fn parse_namespace(raw: &str, separator: &str) -> IcebergResult<NamespaceIdent> {
    if separator.is_empty() {
        return Err(IcebergError::Internal {
            message: "Namespace separator is empty".to_string(),
        });
    }
    let mut normalized = raw.replace("%1F", "\u{1F}").replace("%1f", "\u{1F}");
    if separator != "\u{1F}" {
        normalized = normalized.replace(separator, "\u{1F}");
    }

    let parts: Vec<String> = normalized.split('\u{1F}').map(str::to_string).collect();
    if parts.is_empty() || parts.iter().any(String::is_empty) {
        return Err(IcebergError::BadRequest {
            message: format!("Invalid namespace: {raw}"),
            error_type: "BadRequestException",
        });
    }
    Ok(parts)
}

/// Joins a namespace identifier list into a single string.
pub fn join_namespace(ident: &[String], separator: &str) -> IcebergResult<String> {
    if ident.is_empty() {
        return Err(IcebergError::BadRequest {
            message: "Namespace cannot be empty".to_string(),
            error_type: "BadRequestException",
        });
    }
    Ok(ident.join(separator))
}

/// Checks if a table format string indicates an Iceberg table.
pub fn is_iceberg_table(format: Option<&str>) -> bool {
    format.is_some_and(|f| f.eq_ignore_ascii_case("iceberg"))
}

/// Applies page token and size to a vector of items.
pub fn paginate<T>(
    mut items: Vec<T>,
    page_token: Option<String>,
    page_size: Option<u32>,
) -> IcebergResult<(Vec<T>, Option<String>)> {
    let start = match page_token {
        Some(token) => token
            .parse::<usize>()
            .map_err(|_| IcebergError::BadRequest {
                message: "Invalid pageToken".to_string(),
                error_type: "BadRequestException",
            })?,
        None => 0,
    };
    let size = match page_size {
        Some(0) => {
            return Err(IcebergError::BadRequest {
                message: "pageSize must be greater than zero".to_string(),
                error_type: "BadRequestException",
            });
        }
        Some(size) => size as usize,
        None => items.len(),
    };

    if start > items.len() {
        return Err(IcebergError::BadRequest {
            message: "pageToken out of range".to_string(),
            error_type: "BadRequestException",
        });
    }

    let end = (start + size).min(items.len());
    let next = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };

    let page = items.drain(start..end).collect();
    Ok((page, next))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_namespace_unit_separator() {
        let ident = parse_namespace("a\u{1F}b", "\u{1F}").expect("parse");
        assert_eq!(ident, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_parse_namespace_percent_encoded() {
        let ident = parse_namespace("a%1Fb", "\u{1F}").expect("parse");
        assert_eq!(ident, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_parse_namespace_percent_encoded_lowercase() {
        let ident = parse_namespace("a%1fb", "\u{1F}").expect("parse");
        assert_eq!(ident, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_paginate() {
        let (page, next) = paginate(vec![1, 2, 3], None, Some(2)).expect("paginate");
        assert_eq!(page, vec![1, 2]);
        assert_eq!(next, Some("2".to_string()));
    }
}

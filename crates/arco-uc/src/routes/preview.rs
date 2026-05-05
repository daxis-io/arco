//! Shared validation and pagination helpers for UC routes.
//!
//! Catalog/schema/table CRUD no longer uses preview JSON storage. The remaining
//! explicit preview surfaces (for example Delta preview endpoints) keep their
//! own state handling; this module now exists only for generic request parsing.

use crate::error::{UnityCatalogError, UnityCatalogResult};

pub const DEFAULT_PAGE_SIZE: usize = 100;

pub struct Pagination {
    start: usize,
    limit: usize,
}

impl Pagination {
    pub const fn start(&self) -> usize {
        self.start
    }

    pub const fn limit(&self) -> usize {
        self.limit
    }
}

pub fn require_identifier(value: Option<String>, field: &str) -> UnityCatalogResult<String> {
    let Some(value) = value else {
        return Err(UnityCatalogError::BadRequest {
            message: format!("missing required field: {field}"),
        });
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(UnityCatalogError::BadRequest {
            message: format!("missing required field: {field}"),
        });
    }
    validate_identifier(value, field)?;
    Ok(value.to_string())
}

pub fn require_non_empty_string(value: Option<String>, field: &str) -> UnityCatalogResult<String> {
    let Some(value) = value else {
        return Err(UnityCatalogError::BadRequest {
            message: format!("missing required field: {field}"),
        });
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(UnityCatalogError::BadRequest {
            message: format!("missing required field: {field}"),
        });
    }
    Ok(value.to_string())
}

pub fn require_present<T>(value: Option<T>, field: &str) -> UnityCatalogResult<T> {
    value.ok_or_else(|| UnityCatalogError::BadRequest {
        message: format!("missing required field: {field}"),
    })
}

fn validate_identifier(value: &str, field: &str) -> UnityCatalogResult<()> {
    let is_valid = value
        .chars()
        .all(|char| char.is_ascii_alphanumeric() || matches!(char, '_' | '-'));
    if is_valid {
        return Ok(());
    }

    Err(UnityCatalogError::BadRequest {
        message: format!("invalid {field}: must contain only ASCII letters, numbers, '_' or '-'"),
    })
}

pub fn parse_pagination(
    page_token: Option<&str>,
    max_results: Option<i32>,
    default_page_size: usize,
    max_page_size: usize,
) -> UnityCatalogResult<Pagination> {
    let start = parse_page_token(page_token)?;
    let limit = parse_max_results(max_results, default_page_size, max_page_size)?;
    Ok(Pagination { start, limit })
}

fn parse_page_token(page_token: Option<&str>) -> UnityCatalogResult<usize> {
    let Some(page_token) = page_token else {
        return Ok(0);
    };

    page_token
        .parse::<usize>()
        .map_err(|_err| UnityCatalogError::BadRequest {
            message: "invalid page_token: expected non-negative integer offset".to_string(),
        })
}

fn parse_max_results(
    max_results: Option<i32>,
    default_page_size: usize,
    max_page_size: usize,
) -> UnityCatalogResult<usize> {
    let effective_default = default_page_size.min(max_page_size).max(1);
    match max_results {
        Some(value) if value < 0 => Err(UnityCatalogError::BadRequest {
            message: "invalid max_results: must be greater than or equal to 0".to_string(),
        }),
        Some(0) | None => Ok(effective_default),
        Some(value) => {
            let value = usize::try_from(value).map_err(|_| UnityCatalogError::BadRequest {
                message: "invalid max_results: must be greater than or equal to 0".to_string(),
            })?;
            Ok(value.min(max_page_size))
        }
    }
}

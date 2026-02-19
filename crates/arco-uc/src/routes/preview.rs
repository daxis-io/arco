//! Shared helpers for Unity Catalog preview interoperability handlers.

use arco_core::ScopedStorage;
use arco_core::error::Error as CoreError;
use arco_core::storage::{WritePrecondition, WriteResult};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{UnityCatalogError, UnityCatalogResult};

pub(crate) const CATALOGS_PREFIX: &str = "unity-catalog-preview/catalogs";
pub(crate) const SCHEMAS_PREFIX: &str = "unity-catalog-preview/schemas";
pub(crate) const TABLES_PREFIX: &str = "unity-catalog-preview/tables";
pub(crate) const DEFAULT_PAGE_SIZE: usize = 100;

pub(crate) struct Pagination {
    pub(crate) start: usize,
    pub(crate) limit: usize,
}

pub(crate) fn catalog_path(name: &str) -> String {
    format!("{CATALOGS_PREFIX}/{name}.json")
}

pub(crate) fn schema_prefix(catalog_name: &str) -> String {
    format!("{SCHEMAS_PREFIX}/{catalog_name}/")
}

pub(crate) fn schema_path(catalog_name: &str, schema_name: &str) -> String {
    format!("{}{schema_name}.json", schema_prefix(catalog_name))
}

pub(crate) fn table_prefix(catalog_name: &str, schema_name: &str) -> String {
    format!("{TABLES_PREFIX}/{catalog_name}/{schema_name}/")
}

pub(crate) fn table_path(catalog_name: &str, schema_name: &str, table_name: &str) -> String {
    format!(
        "{}{table_name}.json",
        table_prefix(catalog_name, schema_name)
    )
}

pub(crate) fn require_identifier(value: Option<String>, field: &str) -> UnityCatalogResult<String> {
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

pub(crate) fn require_non_empty_string(
    value: Option<String>,
    field: &str,
) -> UnityCatalogResult<String> {
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

pub(crate) fn require_present<T>(value: Option<T>, field: &str) -> UnityCatalogResult<T> {
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

pub(crate) async fn write_json_if_absent<T: Serialize>(
    storage: &ScopedStorage,
    path: &str,
    value: &T,
    operation_name: &str,
) -> UnityCatalogResult<WriteResult> {
    let bytes = serde_json::to_vec(value).map_err(|err| UnityCatalogError::Internal {
        message: format!("failed to serialize {operation_name}: {err}"),
    })?;
    storage
        .put_raw(path, bytes.into(), WritePrecondition::DoesNotExist)
        .await
        .map_err(|err| storage_error(operation_name, err))
}

pub(crate) async fn read_json_page<T: DeserializeOwned>(
    storage: &ScopedStorage,
    prefix: &str,
    operation_name: &str,
    pagination: &Pagination,
) -> UnityCatalogResult<(Vec<T>, Option<String>)> {
    let mut object_paths = storage
        .list(prefix)
        .await
        .map_err(|err| storage_error(operation_name, err))?;
    object_paths.sort_by(|left, right| left.as_str().cmp(right.as_str()));

    if pagination.start >= object_paths.len() {
        return Ok((Vec::new(), None));
    }

    let end = pagination
        .start
        .saturating_add(pagination.limit)
        .min(object_paths.len());
    let mut parsed = Vec::with_capacity(end.saturating_sub(pagination.start));
    for object_path in &object_paths[pagination.start..end] {
        let body = storage
            .get_raw(object_path.as_str())
            .await
            .map_err(|err| storage_error(operation_name, err))?;
        let value =
            serde_json::from_slice::<T>(&body).map_err(|err| UnityCatalogError::Internal {
                message: format!(
                    "failed to parse {operation_name} payload at {}: {err}",
                    object_path.as_str()
                ),
            })?;
        parsed.push(value);
    }

    let next_page_token = (end < object_paths.len()).then(|| end.to_string());
    Ok((parsed, next_page_token))
}

pub(crate) async fn object_exists(
    storage: &ScopedStorage,
    path: &str,
    operation_name: &str,
) -> UnityCatalogResult<bool> {
    storage
        .head_raw(path)
        .await
        .map(|meta| meta.is_some())
        .map_err(|err| storage_error(operation_name, err))
}

pub(crate) fn parse_pagination(
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
        Some(value) => Ok((value as usize).min(max_page_size)),
    }
}

fn storage_error(operation_name: &str, err: CoreError) -> UnityCatalogError {
    match err {
        CoreError::NotFound(_) | CoreError::ResourceNotFound { .. } => {
            UnityCatalogError::NotFound {
                message: format!("{operation_name} resource not found"),
            }
        }
        other => UnityCatalogError::Internal {
            message: format!("failed to {operation_name}: {other}"),
        },
    }
}

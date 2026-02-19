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

pub(crate) fn catalog_path(name: &str) -> String {
    format!("{CATALOGS_PREFIX}/{name}.json")
}

pub(crate) fn schema_prefix(catalog_name: &str) -> String {
    format!("{SCHEMAS_PREFIX}/{catalog_name}")
}

pub(crate) fn schema_path(catalog_name: &str, schema_name: &str) -> String {
    format!("{}/{schema_name}.json", schema_prefix(catalog_name))
}

pub(crate) fn table_prefix(catalog_name: &str, schema_name: &str) -> String {
    format!("{TABLES_PREFIX}/{catalog_name}/{schema_name}")
}

pub(crate) fn table_path(catalog_name: &str, schema_name: &str, table_name: &str) -> String {
    format!(
        "{}/{table_name}.json",
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

pub(crate) async fn read_json_list<T: DeserializeOwned>(
    storage: &ScopedStorage,
    prefix: &str,
    operation_name: &str,
) -> UnityCatalogResult<Vec<T>> {
    let mut object_paths = storage
        .list(prefix)
        .await
        .map_err(|err| storage_error(operation_name, err))?;
    object_paths.sort_by(|left, right| left.as_str().cmp(right.as_str()));

    let mut parsed = Vec::with_capacity(object_paths.len());
    for object_path in object_paths {
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
    Ok(parsed)
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

pub(crate) fn paginate<T>(
    values: Vec<T>,
    page_token: Option<&str>,
    max_results: Option<i32>,
) -> UnityCatalogResult<(Vec<T>, Option<String>)> {
    let total = values.len();
    let start = parse_page_token(page_token)?;
    let Some(remaining) = total.checked_sub(start) else {
        return Ok((Vec::new(), None));
    };

    let max_results = parse_max_results(max_results)?;
    let take_count = match max_results {
        Some(0) | None => remaining,
        Some(limit) => limit.min(remaining),
    };
    let end = start.saturating_add(take_count);

    let paged = values
        .into_iter()
        .skip(start)
        .take(take_count)
        .collect::<Vec<_>>();
    let next_page_token = (end < total).then(|| end.to_string());
    Ok((paged, next_page_token))
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

fn parse_max_results(max_results: Option<i32>) -> UnityCatalogResult<Option<usize>> {
    match max_results {
        Some(value) if value < 0 => Err(UnityCatalogError::BadRequest {
            message: "invalid max_results: must be greater than or equal to 0".to_string(),
        }),
        Some(value) => Ok(Some(value as usize)),
        None => Ok(None),
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

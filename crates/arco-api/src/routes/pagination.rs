//! Shared cursor pagination for catalog-style list endpoints.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::error::ApiError;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 500;

/// Query parameters for existing catalog-style list endpoints.
#[derive(Debug, Default, Deserialize)]
pub struct ListPageQuery {
    /// Maximum number of items to return.
    pub limit: Option<usize>,
    /// Opaque cursor returned by the previous page.
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListCursor {
    key: String,
}

pub(super) fn page_by_key<T>(
    mut items: Vec<T>,
    query: &ListPageQuery,
    key: impl Fn(&T) -> &str,
) -> Result<(Vec<T>, Option<String>), ApiError> {
    let limit = parse_limit(query.limit)?;
    let after_key = query.cursor.as_deref().map(decode_cursor).transpose()?;

    items.sort_by(|left, right| key(left).cmp(key(right)));

    let start = after_key.as_deref().map_or(0, |cursor| {
        items.partition_point(|item| key(item) <= cursor)
    });
    let end = start.saturating_add(limit).min(items.len());
    let next_cursor = if end < items.len() && end > start {
        items
            .get(end - 1)
            .map(|item| encode_cursor(key(item)))
            .transpose()?
    } else {
        None
    };
    let page = items.into_iter().skip(start).take(limit).collect();

    Ok((page, next_cursor))
}

fn parse_limit(limit: Option<usize>) -> Result<usize, ApiError> {
    match limit.unwrap_or(DEFAULT_LIMIT) {
        0 => Err(ApiError::bad_request("limit must be greater than 0")),
        value if value > MAX_LIMIT => Err(ApiError::bad_request(format!(
            "limit must be less than or equal to {MAX_LIMIT}"
        ))),
        value => Ok(value),
    }
}

fn encode_cursor(key: &str) -> Result<String, ApiError> {
    let cursor = ListCursor {
        key: key.to_string(),
    };
    let json = serde_json::to_vec(&cursor)
        .map_err(|e| ApiError::internal(format!("failed to encode cursor: {e}")))?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

fn decode_cursor(cursor: &str) -> Result<String, ApiError> {
    let json = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| ApiError::bad_request("invalid cursor"))?;
    let cursor: ListCursor =
        serde_json::from_slice(&json).map_err(|_| ApiError::bad_request("invalid cursor"))?;
    Ok(cursor.key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Item {
        name: &'static str,
    }

    #[test]
    fn page_by_key_returns_stable_sorted_pages() {
        let items = vec![
            Item { name: "charlie" },
            Item { name: "alpha" },
            Item { name: "bravo" },
        ];
        let query = ListPageQuery {
            limit: Some(2),
            cursor: None,
        };

        let (page, cursor) = page_by_key(items, &query, |item| item.name).unwrap();

        assert_eq!(
            page.iter().map(|item| item.name).collect::<Vec<_>>(),
            vec!["alpha", "bravo"]
        );

        let query = ListPageQuery {
            limit: Some(2),
            cursor,
        };
        let (page, cursor) = page_by_key(
            vec![
                Item { name: "charlie" },
                Item { name: "alpha" },
                Item { name: "bravo" },
            ],
            &query,
            |item| item.name,
        )
        .unwrap();

        assert_eq!(
            page.iter().map(|item| item.name).collect::<Vec<_>>(),
            vec!["charlie"]
        );
        assert!(cursor.is_none());
    }
}

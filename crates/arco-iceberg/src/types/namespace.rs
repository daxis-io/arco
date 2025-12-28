//! Namespace-related request and response types.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A namespace identifier, represented as a list of strings.
///
/// Example: `["accounting", "tax"]` represents namespace `accounting.tax`
pub type NamespaceIdent = Vec<String>;

/// Response from `GET /v1/{prefix}/namespaces`.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ListNamespacesResponse {
    /// List of namespace identifiers.
    pub namespaces: Vec<NamespaceIdent>,

    /// Token for fetching the next page of results.
    #[serde(rename = "next-page-token", skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// Query parameters for listing namespaces.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams)]
pub struct ListNamespacesQuery {
    /// Return namespaces that are children of this parent namespace.
    #[serde(default)]
    pub parent: Option<String>,

    /// Token for pagination.
    #[serde(rename = "pageToken")]
    pub page_token: Option<String>,

    /// Maximum number of results to return.
    #[serde(rename = "pageSize")]
    pub page_size: Option<u32>,
}

/// Response from `GET /v1/{prefix}/namespaces/{namespace}`.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct GetNamespaceResponse {
    /// The namespace identifier.
    pub namespace: NamespaceIdent,

    /// Namespace properties.
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl GetNamespaceResponse {
    /// Creates a response for a namespace with the given name and properties.
    #[must_use]
    pub fn new(namespace: NamespaceIdent, properties: HashMap<String, String>) -> Self {
        Self {
            namespace,
            properties,
        }
    }

    /// Creates a response for a namespace with just a name (no properties).
    #[must_use]
    pub fn from_name(name: impl Into<String>) -> Self {
        Self {
            namespace: vec![name.into()],
            properties: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_namespaces_response() {
        let response = ListNamespacesResponse {
            namespaces: vec![
                vec!["db1".to_string()],
                vec!["db2".to_string(), "schema1".to_string()],
            ],
            next_page_token: Some("token123".to_string()),
        };

        let json = serde_json::to_string(&response).expect("serialization failed");
        assert!(json.contains("\"namespaces\""));
        assert!(json.contains("\"next-page-token\""));
        assert!(json.contains("\"token123\""));
    }

    #[test]
    fn test_list_namespaces_no_token() {
        let response = ListNamespacesResponse {
            namespaces: vec![vec!["db1".to_string()]],
            next_page_token: None,
        };

        let json = serde_json::to_string(&response).expect("serialization failed");
        // Token should be omitted when None
        assert!(!json.contains("next-page-token"));
    }

    #[test]
    fn test_get_namespace_response() {
        let response = GetNamespaceResponse {
            namespace: vec!["prod".to_string(), "analytics".to_string()],
            properties: HashMap::from([
                ("owner".to_string(), "data-team".to_string()),
                (
                    "location".to_string(),
                    "gs://bucket/prod/analytics".to_string(),
                ),
            ]),
        };

        let json = serde_json::to_string(&response).expect("serialization failed");
        assert!(json.contains("\"namespace\""));
        assert!(json.contains("\"prod\""));
        assert!(json.contains("\"analytics\""));
        assert!(json.contains("\"properties\""));
    }

    #[test]
    fn test_namespace_ident_nested() {
        // Verify nested namespaces serialize as arrays
        let ns: NamespaceIdent = vec![
            "level1".to_string(),
            "level2".to_string(),
            "level3".to_string(),
        ];
        let json = serde_json::to_string(&ns).expect("serialization failed");
        assert_eq!(json, r#"["level1","level2","level3"]"#);
    }

    #[test]
    fn test_list_namespaces_roundtrip() {
        let json = r#"{"namespaces":[["db1"],["db2","schema1"]],"next-page-token":"token123"}"#;

        let value: serde_json::Value = serde_json::from_str(json).expect("parse failed");
        let parsed: ListNamespacesResponse =
            serde_json::from_value(value.clone()).expect("deserialization failed");
        let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

        assert_eq!(roundtrip, value);
    }

    #[test]
    fn test_get_namespace_roundtrip() {
        let json = r#"{"namespace":["prod","analytics"],"properties":{"location":"gs://bucket/prod/analytics","owner":"data-team"}}"#;

        let value: serde_json::Value = serde_json::from_str(json).expect("parse failed");
        let parsed: GetNamespaceResponse =
            serde_json::from_value(value.clone()).expect("deserialization failed");
        let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

        assert_eq!(roundtrip, value);
    }

    #[test]
    fn test_list_namespaces_empty_roundtrip() {
        let json = r#"{"namespaces":[]}"#;

        let value: serde_json::Value = serde_json::from_str(json).expect("parse failed");
        let parsed: ListNamespacesResponse =
            serde_json::from_value(value.clone()).expect("deserialization failed");
        let roundtrip = serde_json::to_value(&parsed).expect("serialization failed");

        assert_eq!(roundtrip, value);
    }
}

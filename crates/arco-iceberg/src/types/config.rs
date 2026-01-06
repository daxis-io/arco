//! Configuration types for `/v1/config` endpoint.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::state::IcebergConfig;

const COMMIT_ENDPOINT_SUPPORTED: bool = true;

/// Response from the `/v1/config` endpoint.
///
/// Provides client configuration including catalog defaults, overrides,
/// and supported endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ConfigResponse {
    /// Default configuration values (may be overridden by client).
    #[serde(default)]
    pub defaults: HashMap<String, String>,

    /// Configuration overrides (client must use these values).
    #[serde(default)]
    pub overrides: HashMap<String, String>,

    /// Lifetime of idempotency keys in ISO 8601 duration format.
    ///
    /// Example: "PT1H" for 1 hour.
    #[serde(
        rename = "idempotency-key-lifetime",
        skip_serializing_if = "Option::is_none"
    )]
    pub idempotency_key_lifetime: Option<String>,

    /// List of supported endpoints.
    ///
    /// If present, this is the authoritative list of endpoints.
    /// Clients should not attempt to use endpoints not in this list.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoints: Option<Vec<String>>,
}

impl ConfigResponse {
    /// Creates the configuration response from server config.
    #[must_use]
    pub fn from_config(config: &IcebergConfig, credentials_enabled: bool) -> Self {
        let mut endpoints = vec![
            "GET /v1/config".to_string(),
            "GET /v1/{prefix}/namespaces".to_string(),
            "HEAD /v1/{prefix}/namespaces/{namespace}".to_string(),
            "GET /v1/{prefix}/namespaces/{namespace}".to_string(),
            "GET /v1/{prefix}/namespaces/{namespace}/tables".to_string(),
            "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string(),
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string(),
        ];

        if credentials_enabled {
            endpoints.push(
                "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials".to_string(),
            );
        }

        if config.allow_write && COMMIT_ENDPOINT_SUPPORTED {
            endpoints.push("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string());
        }

        if config.allow_namespace_crud {
            endpoints.extend([
                "POST /v1/{prefix}/namespaces".to_string(),
                "DELETE /v1/{prefix}/namespaces/{namespace}".to_string(),
                "POST /v1/{prefix}/namespaces/{namespace}/properties".to_string(),
            ]);
        }

        if config.allow_table_crud {
            endpoints.extend([
                "POST /v1/{prefix}/namespaces/{namespace}/tables".to_string(),
                "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string(),
                "POST /v1/{prefix}/namespaces/{namespace}/register".to_string(),
            ]);
        }

        Self {
            defaults: HashMap::new(),
            overrides: HashMap::from([
                ("prefix".to_string(), config.prefix.clone()),
                (
                    "namespace-separator".to_string(),
                    config.namespace_separator.clone(),
                ),
            ]),
            idempotency_key_lifetime: config.idempotency_key_lifetime.clone(),
            endpoints: Some(endpoints),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_response_serialization() {
        let config = ConfigResponse::from_config(&IcebergConfig::default(), true);
        let json = serde_json::to_string_pretty(&config).expect("serialization failed");

        // Verify required fields
        assert!(json.contains("\"prefix\""));
        assert!(json.contains("\"arco\""));
        assert!(json.contains("\"namespace-separator\""));
        assert!(json.contains("\"%1F\""));
        assert!(json.contains("\"idempotency-key-lifetime\""));
        assert!(json.contains("\"PT1H\""));
        assert!(json.contains("\"endpoints\""));
    }

    #[test]
    fn test_config_response_deserialization() {
        let json = r#"{
            "defaults": {},
            "overrides": {
                "prefix": "arco",
                "namespace-separator": "%1F"
            },
            "idempotency-key-lifetime": "PT1H",
            "endpoints": ["GET /v1/config"]
        }"#;

        let config: ConfigResponse = serde_json::from_str(json).expect("deserialization failed");
        assert_eq!(config.overrides.get("prefix"), Some(&"arco".to_string()));
        assert_eq!(config.idempotency_key_lifetime, Some("PT1H".to_string()));
        assert_eq!(config.endpoints.as_ref().map(Vec::len), Some(1));
    }

    #[test]
    fn test_arco_default_has_read_endpoints() {
        let config = ConfigResponse::from_config(&IcebergConfig::default(), false);
        let endpoints = config.endpoints.expect("endpoints should be present");

        assert!(endpoints.contains(&"GET /v1/config".to_string()));
        assert!(endpoints.contains(&"GET /v1/{prefix}/namespaces".to_string()));
        assert!(
            endpoints
                .contains(&"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string())
        );

        // Should NOT have write endpoints in default (Phase A)
        assert!(!endpoints.contains(&"POST /v1/{prefix}/namespaces".to_string()));
    }

    #[test]
    fn test_allow_write_does_not_advertise_namespace_crud() {
        let mut config = IcebergConfig::default();
        config.allow_write = true;
        let config = ConfigResponse::from_config(&config, false);
        let endpoints = config.endpoints.expect("endpoints should be present");

        assert!(
            endpoints
                .contains(&"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string())
        );
        assert!(!endpoints.contains(&"POST /v1/{prefix}/namespaces".to_string()));
    }

    #[test]
    fn test_namespace_crud_advertises_endpoints() {
        let mut config = IcebergConfig::default();
        config.allow_namespace_crud = true;
        let config = ConfigResponse::from_config(&config, false);
        let endpoints = config.endpoints.expect("endpoints should be present");

        assert!(endpoints.contains(&"POST /v1/{prefix}/namespaces".to_string()));
        assert!(endpoints.contains(&"DELETE /v1/{prefix}/namespaces/{namespace}".to_string()));
        assert!(
            endpoints.contains(&"POST /v1/{prefix}/namespaces/{namespace}/properties".to_string())
        );
    }

    #[test]
    fn test_table_crud_advertises_endpoints() {
        let mut config = IcebergConfig::default();
        config.allow_table_crud = true;
        let config = ConfigResponse::from_config(&config, false);
        let endpoints = config.endpoints.expect("endpoints should be present");

        assert!(endpoints.contains(&"POST /v1/{prefix}/namespaces/{namespace}/tables".to_string()));
        assert!(
            endpoints
                .contains(&"DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string())
        );
        assert!(
            endpoints.contains(&"POST /v1/{prefix}/namespaces/{namespace}/register".to_string())
        );
    }

    #[test]
    fn test_table_crud_not_advertised_by_default() {
        let config = ConfigResponse::from_config(&IcebergConfig::default(), false);
        let endpoints = config.endpoints.expect("endpoints should be present");

        assert!(
            !endpoints.contains(&"POST /v1/{prefix}/namespaces/{namespace}/tables".to_string())
        );
        assert!(
            !endpoints
                .contains(&"DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string())
        );
    }
}

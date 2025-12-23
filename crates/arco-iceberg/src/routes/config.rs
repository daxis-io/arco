//! Handler for `/v1/config` endpoint.

use axum::extract::State;
use axum::Json;

use crate::types::ConfigResponse;
use crate::state::IcebergState;

/// Handler for `GET /v1/config`.
///
/// Returns the catalog configuration including supported endpoints,
/// namespace separator, and idempotency key lifetime.
#[utoipa::path(
    get,
    path = "/v1/config",
    responses(
        (status = 200, description = "Catalog configuration", body = ConfigResponse)
    ),
    tag = "Configuration"
)]
pub async fn get_config(State(state): State<IcebergState>) -> Json<ConfigResponse> {
    Json(ConfigResponse::from_config(
        &state.config,
        state.credentials_enabled(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use arco_core::storage::MemoryBackend;
    use std::sync::Arc;
    use tower::ServiceExt;
    use crate::error::IcebergResult;
    use crate::state::{CredentialProvider, CredentialRequest};
    use async_trait::async_trait;
    use crate::types::StorageCredential;

    fn app() -> Router {
        let storage = Arc::new(MemoryBackend::new());
        let state = IcebergState::new(storage);
        Router::new()
            .route("/v1/config", axum::routing::get(get_config))
            .with_state(state)
    }

    #[tokio::test]
    async fn test_get_config_returns_200() {
        let app = app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/config")
                    .body(Body::empty())
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_config_content() {
        let app = app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/config")
                    .body(Body::empty())
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read failed");
        let config: ConfigResponse = serde_json::from_slice(&body).expect("json parse failed");

        assert_eq!(config.overrides.get("prefix"), Some(&"arco".to_string()));
        assert_eq!(
            config.overrides.get("namespace-separator"),
            Some(&"%1F".to_string())
        );
        assert_eq!(config.idempotency_key_lifetime, Some("PT1H".to_string()));
        assert!(config.endpoints.is_some());
    }

    #[tokio::test]
    async fn test_get_config_has_required_endpoints() {
        let app = app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/config")
                    .body(Body::empty())
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read failed");
        let config: ConfigResponse = serde_json::from_slice(&body).expect("json parse failed");

        let endpoints = config.endpoints.expect("endpoints should be present");

        // Verify Phase A endpoints are listed
        assert!(endpoints.contains(&"GET /v1/config".to_string()));
        assert!(endpoints.contains(&"GET /v1/{prefix}/namespaces".to_string()));
        assert!(endpoints.contains(&"GET /v1/{prefix}/namespaces/{namespace}".to_string()));
        assert!(endpoints.contains(&"GET /v1/{prefix}/namespaces/{namespace}/tables".to_string()));
        assert!(endpoints
            .contains(&"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string()));
        assert!(
            !endpoints.contains(
                &"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials".to_string()
            )
        );
    }

    struct DummyProvider;

    #[async_trait]
    impl CredentialProvider for DummyProvider {
        async fn vended_credentials(
            &self,
            _request: CredentialRequest,
        ) -> IcebergResult<Vec<StorageCredential>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn test_get_config_includes_credentials_when_enabled() {
        let storage = Arc::new(MemoryBackend::new());
        let state = IcebergState::new(storage).with_credentials(Arc::new(DummyProvider));
        let app = Router::new()
            .route("/v1/config", axum::routing::get(get_config))
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/config")
                    .body(Body::empty())
                    .expect("request build failed"),
            )
            .await
            .expect("request failed");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body read failed");
        let config: ConfigResponse = serde_json::from_slice(&body).expect("json parse failed");
        let endpoints = config.endpoints.expect("endpoints should be present");
        assert!(endpoints.contains(
            &"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials".to_string()
        ));
    }
}

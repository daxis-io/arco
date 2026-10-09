//! Operator-owned durable HTTP ingress for packaged Flow binaries.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;

use super::{EnqueueOptions, EnqueueResult, HttpTaskEnqueuer};
use crate::error::{Error, Result};

/// HTTP sender that acknowledges only durable operator ingress responses.
#[derive(Clone)]
pub struct OperatorHttpEnqueuer {
    client: reqwest::Client,
    ingress_url: String,
    token: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IngressTask<'a> {
    task_id: &'a str,
    target_url: &'a str,
    body: &'a str,
    audience: Option<&'a str>,
    headers: Option<HashMap<String, String>>,
    routing_key: Option<&'a str>,
}

impl OperatorHttpEnqueuer {
    /// Creates a sender to an ingress that durably accepts named tasks.
    ///
    /// # Errors
    ///
    /// Refuses missing or malformed ingress credentials and URLs.
    pub fn new(ingress_url: String, token: String) -> Result<Self> {
        let parsed = reqwest::Url::parse(&ingress_url)
            .map_err(|error| Error::configuration(format!("invalid HTTP ingress URL: {error}")))?;
        if !matches!(parsed.scheme(), "http" | "https") || token.trim().is_empty() {
            return Err(Error::configuration(
                "HTTP ingress requires an HTTP(S) URL and non-empty token",
            ));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| Error::configuration(format!("HTTP client: {error}")))?;
        Ok(Self {
            client,
            ingress_url,
            token,
        })
    }
}

#[async_trait]
impl HttpTaskEnqueuer for OperatorHttpEnqueuer {
    async fn enqueue_http(
        &self,
        task_id: &str,
        target_url: &str,
        body: &[u8],
        options: EnqueueOptions,
        audience: Option<&str>,
        extra_headers: Option<HashMap<String, String>>,
    ) -> Result<EnqueueResult> {
        // A delayed timer cannot be represented by this ingress contract.
        if options.delay.is_some() || options.priority.is_some() {
            return Err(Error::dispatch(
                "HTTP ingress does not accept timer or priority work",
            ));
        }
        let body = std::str::from_utf8(body)
            .map_err(|error| Error::serialization(format!("worker body is not UTF-8: {error}")))?;
        let task = IngressTask {
            task_id,
            target_url,
            body,
            audience,
            headers: extra_headers,
            routing_key: options.routing_key.as_deref(),
        };
        let response = self
            .client
            .post(&self.ingress_url)
            .bearer_auth(&self.token)
            .json(&task)
            .send()
            .await
            .map_err(|error| {
                Error::dispatch(format!("HTTP ingress acceptance uncertain: {error}"))
            })?;
        match response.status().as_u16() {
            202 => Ok(EnqueueResult::Enqueued {
                message_id: task_id.to_string(),
            }),
            409 => Ok(EnqueueResult::Deduplicated {
                existing_message_id: task_id.to_string(),
            }),
            429 => Ok(EnqueueResult::QueueFull),
            status => Err(Error::dispatch(format!(
                "HTTP ingress acceptance uncertain: status {status}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Json, Router};

    #[tokio::test]
    async fn operator_ingress_acknowledges_only_known_acceptance_statuses() {
        let app = Router::new().route(
            "/accept",
            post(
                |headers: HeaderMap, Json(task): Json<serde_json::Value>| async move {
                    assert_eq!(
                        headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok()),
                        Some("Bearer test-token")
                    );
                    assert_eq!(task["body"], r#"{"taskId":"worker"}"#);
                    match task["taskId"].as_str() {
                        Some("accepted") => StatusCode::ACCEPTED,
                        Some("duplicate") => StatusCode::CONFLICT,
                        Some("full") => StatusCode::TOO_MANY_REQUESTS,
                        _ => StatusCode::INTERNAL_SERVER_ERROR,
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let url = format!("http://{}/accept", listener.local_addr().expect("addr"));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        let queue = OperatorHttpEnqueuer::new(url, "test-token".to_string()).expect("queue");
        for (id, expected) in [
            (
                "accepted",
                EnqueueResult::Enqueued {
                    message_id: "accepted".to_string(),
                },
            ),
            (
                "duplicate",
                EnqueueResult::Deduplicated {
                    existing_message_id: "duplicate".to_string(),
                },
            ),
            ("full", EnqueueResult::QueueFull),
        ] {
            assert_eq!(
                expected,
                queue
                    .enqueue_http(
                        id,
                        "http://worker/dispatch",
                        br#"{"taskId":"worker"}"#,
                        EnqueueOptions::new(),
                        None,
                        None
                    )
                    .await
                    .expect("accepted response")
            );
        }
        assert!(
            queue
                .enqueue_http(
                    "uncertain",
                    "http://worker/dispatch",
                    br#"{"taskId":"worker"}"#,
                    EnqueueOptions::new(),
                    None,
                    None
                )
                .await
                .is_err()
        );
        assert!(
            queue
                .enqueue_http(
                    "timer",
                    "http://worker/dispatch",
                    br"{}",
                    EnqueueOptions::new().with_delay(Duration::from_secs(1)),
                    None,
                    None
                )
                .await
                .is_err()
        );
        server.abort();
    }
}

//! Google Cloud Tasks dispatcher implementation.
//!
//! This module provides [`CloudTasksDispatcher`], a production-ready implementation
//! of the [`TaskQueue`] trait using Google Cloud Tasks.
//!
//! ## Features
//!
//! - **Idempotent dispatch**: Uses `run_id/task_id/attempt` as Cloud Tasks deduplication key
//! - **Retry policy**: Configurable exponential backoff with jitter
//! - **Queue routing**: Routes tasks to specific queues via routing keys
//! - **OIDC authentication**: Supports service account authentication for secure task invocation
//!
//! Applying retry configuration requires `cloudtasks.queues.update` permissions
//! unless `apply_queue_retry_config` is disabled in the configuration.
//!
//! ## Usage
//!
//! This module is only compiled when the `gcp` feature is enabled:
//!
//! ```toml
//! [dependencies]
//! arco-flow = { version = "0.1", features = ["gcp"] }
//! ```
//!
//! ## Example
//!
//! ```rust,ignore
//! use arco_flow::dispatch::cloud_tasks::{CloudTasksDispatcher, CloudTasksConfig};
//!
//! let config = CloudTasksConfig::new(
//!     "my-project",
//!     "us-central1",
//!     "arco-tasks",
//!     "https://my-service.run.app",
//! )
//! .with_queue_retry_updates(false); // Skip queue updates for IaC-managed queues.
//!
//! let dispatcher = CloudTasksDispatcher::new(config).await?;
//! dispatcher.enqueue(envelope, options).await?;
//! ```

use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{EnqueueOptions, EnqueueResult, TaskEnvelope, TaskQueue};
use crate::error::{Error, Result};

/// Configuration for Cloud Tasks dispatcher.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudTasksConfig {
    /// GCP project ID.
    pub project_id: String,
    /// Cloud Tasks location (e.g., "us-central1").
    pub location: String,
    /// Default queue name.
    pub queue_name: String,
    /// Target service URL for HTTP tasks.
    pub service_url: String,
    /// Optional service account email for OIDC auth.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_account_email: Option<String>,
    /// Task timeout (default: 30 minutes).
    #[serde(default = "default_task_timeout")]
    pub task_timeout: Duration,
    /// Retry configuration applied to Cloud Tasks queues on first use.
    #[serde(default)]
    pub retry_config: RetryConfig,
    /// Whether to apply retry configuration to queues via the Cloud Tasks API.
    ///
    /// Defaults to true; set to false for IaC-managed queues.
    #[serde(default = "default_apply_queue_retry_config")]
    pub apply_queue_retry_config: bool,
}

fn default_task_timeout() -> Duration {
    Duration::from_secs(30 * 60)
}

fn default_apply_queue_retry_config() -> bool {
    true
}

/// Retry configuration for Cloud Tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    pub max_attempts: u32,
    /// Minimum backoff duration.
    pub min_backoff: Duration,
    /// Maximum backoff duration.
    pub max_backoff: Duration,
    /// Maximum time for retries (deadline from first attempt).
    pub max_retry_duration: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            min_backoff: Duration::from_secs(10),
            max_backoff: Duration::from_secs(300),
            max_retry_duration: Duration::from_secs(3600),
        }
    }
}

impl CloudTasksConfig {
    /// Creates a new config with required fields.
    #[must_use]
    pub fn new(
        project_id: impl Into<String>,
        location: impl Into<String>,
        queue_name: impl Into<String>,
        service_url: impl Into<String>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            location: location.into(),
            queue_name: queue_name.into(),
            service_url: service_url.into(),
            service_account_email: None,
            task_timeout: default_task_timeout(),
            retry_config: RetryConfig::default(),
            apply_queue_retry_config: default_apply_queue_retry_config(),
        }
    }

    /// Sets the service account for OIDC authentication.
    #[must_use]
    pub fn with_service_account(mut self, email: impl Into<String>) -> Self {
        self.service_account_email = Some(email.into());
        self
    }

    /// Sets the task timeout.
    #[must_use]
    pub const fn with_task_timeout(mut self, timeout: Duration) -> Self {
        self.task_timeout = timeout;
        self
    }

    /// Sets the retry configuration.
    #[must_use]
    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    /// Sets whether queue retry configuration should be applied via API.
    #[must_use]
    pub const fn with_queue_retry_updates(mut self, enabled: bool) -> Self {
        self.apply_queue_retry_config = enabled;
        self
    }

    /// Returns the full queue path for Cloud Tasks API.
    #[must_use]
    pub fn queue_path(&self) -> String {
        format!(
            "projects/{}/locations/{}/queues/{}",
            self.project_id, self.location, self.queue_name
        )
    }
}

// ============================================================================
// GCP Feature-Gated Implementation
// ============================================================================

#[cfg(feature = "gcp")]
mod gcp_impl {
    use super::*;
    use base64::Engine;
    use gcp_auth::TokenProvider;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// Google Cloud Tasks dispatcher.
    ///
    /// Implements the [`TaskQueue`] trait for dispatching tasks to Google Cloud Tasks.
    ///
    /// ## Idempotency
    ///
    /// Tasks are identified by `{run_id}/{task_id}/{attempt}` which is used as the Cloud Tasks
    /// task name. This ensures:
    /// - Retry of the same attempt won't create duplicate Cloud Tasks
    /// - Different attempts (after failure) create distinct Cloud Tasks
    ///
    /// ## Queue Routing
    ///
    /// The dispatcher supports routing tasks to different queues based on:
    /// - Custom routing key (via `EnqueueOptions::routing_key`)
    ///
    /// Cloud Tasks does not support per-task priorities. Higher-priority tasks
    /// should be enqueued first by the scheduler or routed to dedicated queues.
    pub struct CloudTasksDispatcher {
        config: CloudTasksConfig,
        token_provider: Arc<dyn TokenProvider>,
        client: reqwest::Client,
        configured_queues: Mutex<HashSet<String>>,
    }

    // Manual Debug implementation since TokenProvider doesn't implement Debug
    impl std::fmt::Debug for CloudTasksDispatcher {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("CloudTasksDispatcher")
                .field("config", &self.config)
                .field("token_provider", &"<TokenProvider>")
                .field("client", &self.client)
                .field("configured_queues", &"<configured_queues>")
                .finish()
        }
    }

    /// Cloud Tasks API request body for creating a task.
    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct CreateTaskRequest {
        task: CloudTask,
    }

    /// Cloud Task resource.
    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct CloudTask {
        /// Task name (optional, but we set it for idempotency).
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// HTTP request to execute.
        http_request: HttpRequest,
        /// Schedule time (optional).
        #[serde(skip_serializing_if = "Option::is_none")]
        schedule_time: Option<String>,
        /// Maximum time the worker has to respond.
        #[serde(skip_serializing_if = "Option::is_none")]
        dispatch_deadline: Option<String>,
    }

    /// HTTP request configuration for Cloud Tasks.
    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct HttpRequest {
        /// Target URL.
        url: String,
        /// HTTP method.
        http_method: String,
        /// Request headers.
        #[serde(skip_serializing_if = "Option::is_none")]
        headers: Option<std::collections::HashMap<String, String>>,
        /// Request body (base64 encoded).
        #[serde(skip_serializing_if = "Option::is_none")]
        body: Option<String>,
        /// OIDC token configuration.
        #[serde(skip_serializing_if = "Option::is_none")]
        oidc_token: Option<OidcToken>,
    }

    /// OIDC token configuration for authenticated tasks.
    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct OidcToken {
        /// Service account email.
        service_account_email: String,
        /// Audience (usually the service URL).
        #[serde(skip_serializing_if = "Option::is_none")]
        audience: Option<String>,
    }

    /// Queue patch request for applying retry configuration.
    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct QueuePatchRequest {
        name: String,
        retry_config: QueueRetryConfig,
    }

    /// Queue retry configuration.
    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    struct QueueRetryConfig {
        max_attempts: u32,
        min_backoff: String,
        max_backoff: String,
        max_retry_duration: String,
    }

    /// Cloud Tasks API error response.
    #[derive(Debug, Deserialize)]
    struct CloudTasksErrorResponse {
        error: CloudTasksError,
    }

    #[derive(Debug, Deserialize)]
    #[allow(dead_code)] // Fields used for deserialization
    struct CloudTasksError {
        code: i32,
        message: String,
        status: String,
    }

    /// Cloud Tasks API success response.
    #[derive(Debug, Deserialize)]
    struct CloudTasksSuccessResponse {
        name: String,
    }

    impl CloudTasksDispatcher {
        /// Creates a new Cloud Tasks dispatcher.
        ///
        /// # Errors
        ///
        /// Returns an error if:
        /// - Configuration is invalid
        /// - GCP authentication cannot be initialized
        pub async fn new(config: CloudTasksConfig) -> Result<Self> {
            // Validate configuration
            if config.project_id.is_empty() {
                return Err(Error::configuration("project_id cannot be empty"));
            }
            if config.location.is_empty() {
                return Err(Error::configuration("location cannot be empty"));
            }
            if config.queue_name.is_empty() {
                return Err(Error::configuration("queue_name cannot be empty"));
            }
            if config.service_url.is_empty() {
                return Err(Error::configuration("service_url cannot be empty"));
            }
            if config.task_timeout.is_zero() {
                return Err(Error::configuration("task_timeout must be greater than zero"));
            }

            // Initialize GCP authentication using the provider() function
            // which automatically discovers credentials from environment
            let token_provider = gcp_auth::provider()
                .await
                .map_err(|e| Error::configuration(format!("Failed to initialize GCP auth: {e}")))?;

            // Create HTTP client with reasonable defaults
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|e| Error::configuration(format!("Failed to create HTTP client: {e}")))?;

            Ok(Self {
                config,
                token_provider,
                client,
                configured_queues: Mutex::new(HashSet::new()),
            })
        }

        /// Returns the queue path for a given routing key.
        fn queue_path_for_routing(&self, routing_key: Option<&str>) -> String {
            routing_key.map_or_else(
                || self.config.queue_path(),
                |key| {
                    format!(
                        "projects/{}/locations/{}/queues/{}",
                        self.config.project_id, self.config.location, key
                    )
                },
            )
        }

        /// Builds the HTTP target URL for a task.
        fn task_url(&self, envelope: &TaskEnvelope) -> String {
            format!(
                "{}/v1/tasks/{}/execute",
                self.config.service_url, envelope.task_id
            )
        }

        /// Sanitizes a string for use as a Cloud Tasks task ID.
        ///
        /// Cloud Tasks task IDs must:
        /// - Start with a letter or underscore
        /// - Contain only letters, numbers, underscores, and hyphens
        /// - Be at most 500 characters
        pub(crate) fn sanitize_task_id(key: &str) -> String {
            let sanitized: String = key
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();

            // Ensure it starts with a letter or underscore
            if sanitized.starts_with(|c: char| c.is_ascii_digit() || c == '-') {
                format!("t_{sanitized}")
            } else {
                sanitized
            }
            .chars()
            .take(500)
            .collect()
        }

        /// Gets an access token for the Cloud Tasks API.
        async fn get_access_token(&self) -> Result<String> {
            let scopes = &["https://www.googleapis.com/auth/cloud-tasks"];
            let token = self
                .token_provider
                .token(scopes)
                .await
                .map_err(|e| Error::dispatch(format!("Failed to get GCP access token: {e}")))?;

            Ok(token.as_str().to_string())
        }

        /// Formats a duration as a Cloud Tasks API duration string.
        pub(crate) fn format_duration(duration: Duration) -> String {
            let secs = duration.as_secs();
            let nanos = duration.subsec_nanos();
            if nanos == 0 {
                return format!("{secs}s");
            }

            let mut fractional = format!("{nanos:09}");
            while fractional.ends_with('0') {
                fractional.pop();
            }

            format!("{secs}.{fractional}s")
        }

        /// Formats a duration as an RFC 3339 timestamp offset from now.
        fn format_schedule_time(delay: Duration) -> String {
            let now = chrono::Utc::now();
            let scheduled = now + chrono::Duration::from_std(delay).unwrap_or_default();
            scheduled.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
        }

        fn retry_config_payload(&self) -> QueueRetryConfig {
            QueueRetryConfig {
                max_attempts: self.config.retry_config.max_attempts,
                min_backoff: Self::format_duration(self.config.retry_config.min_backoff),
                max_backoff: Self::format_duration(self.config.retry_config.max_backoff),
                max_retry_duration: Self::format_duration(self.config.retry_config.max_retry_duration),
            }
        }

        async fn apply_retry_config(&self, queue_path: &str) -> Result<()> {
            let access_token = self.get_access_token().await?;
            let request = QueuePatchRequest {
                name: queue_path.to_string(),
                retry_config: self.retry_config_payload(),
            };

            let api_url = format!(
                "https://cloudtasks.googleapis.com/v2/{}?updateMask=retryConfig",
                queue_path
            );

            let response = self
                .client
                .patch(&api_url)
                .bearer_auth(&access_token)
                .json(&request)
                .send()
                .await
                .map_err(|e| Error::dispatch(format!("Cloud Tasks queue update failed: {e}")))?;

            let status = response.status();

            if status.is_success() {
                Ok(())
            } else {
                let error_body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "unknown error".to_string());

                if let Ok(error_response) = serde_json::from_str::<CloudTasksErrorResponse>(&error_body)
                {
                    Err(Error::configuration(format!(
                        "Cloud Tasks queue update error: {} ({})",
                        error_response.error.message, error_response.error.status
                    )))
                } else {
                    Err(Error::configuration(format!(
                        "Cloud Tasks queue update error: {} - {}",
                        status, error_body
                    )))
                }
            }
        }

        async fn ensure_queue_retry_config(&self, queue_path: &str) -> Result<()> {
            if !self.config.apply_queue_retry_config {
                return Ok(());
            }

            {
                let configured = self.configured_queues.lock().await;
                if configured.contains(queue_path) {
                    return Ok(());
                }
            }

            self.apply_retry_config(queue_path).await?;

            let mut configured = self.configured_queues.lock().await;
            configured.insert(queue_path.to_string());
            Ok(())
        }
    }

    #[async_trait]
    impl TaskQueue for CloudTasksDispatcher {
        async fn enqueue(
            &self,
            envelope: TaskEnvelope,
            options: EnqueueOptions,
        ) -> Result<EnqueueResult> {
            // Build the Cloud Tasks task name for idempotency
            let idempotency_key = envelope.idempotency_key();
            let sanitized_id = Self::sanitize_task_id(&idempotency_key);
            let queue_path = self.queue_path_for_routing(options.routing_key.as_deref());
            let task_name = format!("{}/tasks/{}", queue_path, sanitized_id);

            self.ensure_queue_retry_config(&queue_path).await?;

            // Build HTTP target
            let target_url = self.task_url(&envelope);

            // Serialize envelope to JSON body
            let body_bytes =
                serde_json::to_vec(&envelope).map_err(|e| Error::serialization(e.to_string()))?;
            let body_base64 = base64::engine::general_purpose::STANDARD.encode(&body_bytes);

            // Build OIDC token if service account is configured
            let oidc_token = self.config.service_account_email.as_ref().map(|email| {
                OidcToken {
                    service_account_email: email.clone(),
                    audience: Some(self.config.service_url.clone()),
                }
            });

            // Build the request
            let request = CreateTaskRequest {
                task: CloudTask {
                    name: Some(task_name.clone()),
                    http_request: HttpRequest {
                        url: target_url,
                        http_method: "POST".to_string(),
                        headers: Some({
                            let mut headers = std::collections::HashMap::new();
                            headers.insert("Content-Type".to_string(), "application/json".to_string());
                            headers
                        }),
                        body: Some(body_base64),
                        oidc_token,
                    },
                    schedule_time: options.delay.map(Self::format_schedule_time),
                    dispatch_deadline: Some(Self::format_duration(self.config.task_timeout)),
                },
            };

            // Get access token
            let access_token = self.get_access_token().await?;

            // Make the API call
            let api_url = format!(
                "https://cloudtasks.googleapis.com/v2/{}/tasks",
                queue_path
            );

            let response = self
                .client
                .post(&api_url)
                .bearer_auth(&access_token)
                .json(&request)
                .send()
                .await
                .map_err(|e| Error::dispatch(format!("Cloud Tasks API request failed: {e}")))?;

            let status = response.status();

            if status.is_success() {
                // Task created successfully
                let success: CloudTasksSuccessResponse = response
                    .json()
                    .await
                    .map_err(|e| Error::dispatch(format!("Failed to parse success response: {e}")))?;

                Ok(EnqueueResult::Enqueued {
                    message_id: success.name,
                })
            } else if status.as_u16() == 409 {
                // ALREADY_EXISTS - task with this name already exists (idempotent)
                Ok(EnqueueResult::Deduplicated {
                    existing_message_id: task_name,
                })
            } else if status.as_u16() == 429 {
                // RESOURCE_EXHAUSTED - queue is full or rate limited
                Ok(EnqueueResult::QueueFull)
            } else {
                // Other error
                let error_body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "unknown error".to_string());

                // Try to parse as structured error
                if let Ok(error_response) = serde_json::from_str::<CloudTasksErrorResponse>(&error_body) {
                    Err(Error::dispatch(format!(
                        "Cloud Tasks API error: {} ({})",
                        error_response.error.message, error_response.error.status
                    )))
                } else {
                    Err(Error::dispatch(format!(
                        "Cloud Tasks API error: {} - {}",
                        status, error_body
                    )))
                }
            }
        }

        async fn queue_depth(&self) -> Result<usize> {
            // Cloud Tasks doesn't provide an efficient way to get exact queue depth.
            // The API lists tasks with pagination, which is expensive for large queues.
            // Production systems should use Cloud Monitoring metrics instead.
            //
            // For now, return 0 as a sentinel value indicating "unknown".
            // Callers should use metrics for accurate queue depth monitoring.
            Ok(0)
        }

        fn queue_name(&self) -> &str {
            &self.config.queue_name
        }
    }
}

// ============================================================================
// Non-GCP Placeholder Implementation
// ============================================================================

#[cfg(not(feature = "gcp"))]
mod placeholder_impl {
    use super::*;

    /// Placeholder Cloud Tasks dispatcher (GCP feature not enabled).
    ///
    /// This is a stub implementation that returns errors indicating the
    /// GCP feature must be enabled for actual Cloud Tasks integration.
    #[derive(Debug)]
    pub struct CloudTasksDispatcher {
        config: CloudTasksConfig,
    }

    impl CloudTasksDispatcher {
        /// Creates a new placeholder dispatcher.
        ///
        /// # Errors
        ///
        /// Returns an error if configuration is invalid.
        pub fn new(config: CloudTasksConfig) -> Result<Self> {
            // Validate configuration even in placeholder mode
            if config.project_id.is_empty() {
                return Err(Error::configuration("project_id cannot be empty"));
            }
            if config.location.is_empty() {
                return Err(Error::configuration("location cannot be empty"));
            }
            if config.queue_name.is_empty() {
                return Err(Error::configuration("queue_name cannot be empty"));
            }
            if config.service_url.is_empty() {
                return Err(Error::configuration("service_url cannot be empty"));
            }

            Ok(Self { config })
        }
    }

    #[async_trait]
    impl TaskQueue for CloudTasksDispatcher {
        async fn enqueue(
            &self,
            _envelope: TaskEnvelope,
            _options: EnqueueOptions,
        ) -> Result<EnqueueResult> {
            Err(Error::configuration(
                "CloudTasksDispatcher requires the 'gcp' feature to be enabled. \
                 Add `arco-flow = { features = [\"gcp\"] }` to your Cargo.toml.",
            ))
        }

        async fn queue_depth(&self) -> Result<usize> {
            Ok(0)
        }

        fn queue_name(&self) -> &str {
            &self.config.queue_name
        }
    }
}

// Re-export the appropriate implementation
#[cfg(feature = "gcp")]
pub use gcp_impl::CloudTasksDispatcher;

#[cfg(not(feature = "gcp"))]
pub use placeholder_impl::CloudTasksDispatcher;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_queue_path() {
        let config = CloudTasksConfig::new(
            "my-project",
            "us-central1",
            "arco-tasks",
            "https://example.run.app",
        );

        assert_eq!(
            config.queue_path(),
            "projects/my-project/locations/us-central1/queues/arco-tasks"
        );
    }

    #[test]
    fn config_builder_pattern() {
        let config = CloudTasksConfig::new(
            "my-project",
            "us-central1",
            "arco-tasks",
            "https://example.run.app",
        )
        .with_service_account("sa@my-project.iam.gserviceaccount.com")
        .with_task_timeout(Duration::from_secs(60 * 60))
        .with_retry_config(RetryConfig {
            max_attempts: 10,
            ..Default::default()
        });

        assert_eq!(
            config.service_account_email,
            Some("sa@my-project.iam.gserviceaccount.com".to_string())
        );
        assert_eq!(config.task_timeout, Duration::from_secs(3600));
        assert_eq!(config.retry_config.max_attempts, 10);
        assert!(config.apply_queue_retry_config);
    }

    #[test]
    fn retry_config_defaults() {
        let config = RetryConfig::default();

        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.min_backoff, Duration::from_secs(10));
        assert_eq!(config.max_backoff, Duration::from_secs(300));
        assert_eq!(config.max_retry_duration, Duration::from_secs(3600));
    }

    #[test]
    fn config_disables_queue_retry_updates() {
        let config = CloudTasksConfig::new(
            "my-project",
            "us-central1",
            "arco-tasks",
            "https://example.run.app",
        )
        .with_queue_retry_updates(false);

        assert!(!config.apply_queue_retry_config);
    }

    #[cfg(not(feature = "gcp"))]
    mod placeholder_tests {
        use super::*;

        #[test]
        fn dispatcher_validates_config() {
            // Empty project_id should fail
            let result = CloudTasksDispatcher::new(CloudTasksConfig::new(
                "",
                "us-central1",
                "queue",
                "https://example.com",
            ));
            assert!(result.is_err());

            // Valid config should succeed
            let result = CloudTasksDispatcher::new(CloudTasksConfig::new(
                "project",
                "us-central1",
                "queue",
                "https://example.com",
            ));
            assert!(result.is_ok());
        }
    }

    #[cfg(feature = "gcp")]
    mod gcp_tests {
        use super::*;

        #[test]
        fn sanitize_task_id_basic() {
            assert_eq!(
                CloudTasksDispatcher::sanitize_task_id("run_123/task_456/1"),
                "run_123_task_456_1"
            );
        }

        #[test]
        fn sanitize_task_id_starting_with_digit() {
            assert_eq!(
                CloudTasksDispatcher::sanitize_task_id("123-task"),
                "t_123-task"
            );
        }

        #[test]
        fn sanitize_task_id_special_chars() {
            assert_eq!(
                CloudTasksDispatcher::sanitize_task_id("a@b#c$d"),
                "a_b_c_d"
            );
        }

        #[test]
        fn format_duration_seconds_only() {
            assert_eq!(
                CloudTasksDispatcher::format_duration(Duration::from_secs(10)),
                "10s"
            );
        }

        #[test]
        fn format_duration_subsecond() {
            assert_eq!(
                CloudTasksDispatcher::format_duration(Duration::from_millis(1500)),
                "1.5s"
            );
            assert_eq!(
                CloudTasksDispatcher::format_duration(Duration::new(1, 5_000_000)),
                "1.005s"
            );
        }
    }
}

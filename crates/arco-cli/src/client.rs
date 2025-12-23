//! HTTP client for Arco API.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::Config;

/// API client for Arco orchestration endpoints.
pub struct ApiClient {
    client: Client,
    base_url: String,
    token: Option<String>,
}

impl ApiClient {
    /// Creates a new API client from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be constructed.
    pub fn new(config: &Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            base_url: config.api_url.clone(),
            token: config.api_token.clone(),
        })
    }

    /// Triggers a new run.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn trigger_run(
        &self,
        workspace_id: &str,
        request: TriggerRunRequest,
    ) -> Result<TriggerRunResponse> {
        let url = format!("{}/api/v1/workspaces/{workspace_id}/runs", self.base_url);

        let mut req = self.client.post(&url).json(&request);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }

        let response = req.send().await.context("Failed to send request")?;

        if response.status().is_success() {
            response.json().await.context("Failed to parse response")
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("API error ({status}): {body}")
        }
    }

    /// Gets run status.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn get_run(&self, workspace_id: &str, run_id: &str) -> Result<RunResponse> {
        let url = format!("{}/api/v1/workspaces/{workspace_id}/runs/{run_id}", self.base_url);

        let mut req = self.client.get(&url);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }

        let response = req.send().await.context("Failed to send request")?;

        if response.status().is_success() {
            response.json().await.context("Failed to parse response")
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("API error ({status}): {body}")
        }
    }

    /// Lists runs.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn list_runs(
        &self,
        workspace_id: &str,
        limit: Option<u32>,
    ) -> Result<ListRunsResponse> {
        let mut url = format!("{}/api/v1/workspaces/{workspace_id}/runs", self.base_url);
        if let Some(limit) = limit {
            url = format!("{url}?limit={limit}");
        }

        let mut req = self.client.get(&url);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }

        let response = req.send().await.context("Failed to send request")?;

        if response.status().is_success() {
            response.json().await.context("Failed to parse response")
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("API error ({status}): {body}")
        }
    }

    /// Backfills a `run_key` reservation fingerprint.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn backfill_run_key(
        &self,
        workspace_id: &str,
        request: RunKeyBackfillRequest,
    ) -> Result<RunKeyBackfillResponse> {
        let url = format!(
            "{}/api/v1/workspaces/{workspace_id}/runs/run-key/backfill",
            self.base_url
        );

        let mut req = self.client.post(&url).json(&request);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }

        let response = req.send().await.context("Failed to send request")?;

        if response.status().is_success() {
            response.json().await.context("Failed to parse response")
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("API error ({status}): {body}")
        }
    }

    /// Deploys a manifest.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response cannot be parsed.
    pub async fn deploy_manifest(
        &self,
        workspace_id: &str,
        request: DeployManifestRequest,
    ) -> Result<DeployManifestResponse> {
        let url = format!(
            "{}/api/v1/workspaces/{workspace_id}/manifests",
            self.base_url
        );

        let mut req = self.client.post(&url).json(&request);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }

        let response = req.send().await.context("Failed to send request")?;

        if response.status().is_success() {
            response.json().await.context("Failed to parse response")
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("API error ({status}): {body}")
        }
    }
}

// ============================================================================
// API Types
// ============================================================================

/// Request to trigger a new run.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerRunRequest {
    /// Asset selection (asset keys to materialize).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub selection: Vec<String>,
    /// Partition overrides (key=value pairs).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<PartitionValue>,
    /// Idempotency key for deduplication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_key: Option<String>,
    /// Additional labels for the run.
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub labels: std::collections::HashMap<String, String>,
}

/// Response after triggering a run.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TriggerRunResponse {
    /// Run ID (ULID).
    pub run_id: String,
    /// Plan ID for this run.
    pub plan_id: String,
    /// Current run state.
    pub state: RunState,
    /// Whether this is a new run or an existing one (`run_key` deduplication).
    pub created: bool,
    /// Run creation timestamp.
    pub created_at: DateTime<Utc>,
}

/// Run state values.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunState {
    /// Run is pending (waiting to start).
    Pending,
    /// Run is actively executing.
    Running,
    /// Run completed successfully.
    Succeeded,
    /// Run failed.
    Failed,
    /// Run was cancelled.
    Cancelled,
    /// Run timed out.
    TimedOut,
}

impl std::fmt::Display for RunState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::TimedOut => "TIMED_OUT",
        };
        write!(f, "{s}")
    }
}

/// Full run details response.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunResponse {
    /// Run ID.
    pub run_id: String,
    /// Workspace ID.
    pub workspace_id: String,
    /// Plan ID.
    pub plan_id: String,
    /// Current state.
    pub state: RunState,
    /// Run creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Run start timestamp.
    pub started_at: Option<DateTime<Utc>>,
    /// Run completion timestamp.
    pub completed_at: Option<DateTime<Utc>>,
    /// Task summaries.
    pub tasks: Vec<TaskSummary>,
    /// Task counts by state.
    pub task_counts: TaskCounts,
}

/// Task summary.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSummary {
    /// Task key.
    pub task_key: String,
    /// Asset key.
    pub asset_key: Option<String>,
    /// Task state.
    pub state: TaskState,
    /// Current attempt number.
    pub attempt: u32,
    /// Task start time.
    pub started_at: Option<DateTime<Utc>>,
    /// Task completion time.
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message.
    pub error_message: Option<String>,
}

/// Task state values.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskState {
    /// Task is pending.
    Pending,
    /// Task is queued.
    Queued,
    /// Task is running.
    Running,
    /// Task succeeded.
    Succeeded,
    /// Task failed.
    Failed,
    /// Task was skipped.
    Skipped,
    /// Task was cancelled.
    Cancelled,
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "PENDING",
            Self::Queued => "QUEUED",
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Skipped => "SKIPPED",
            Self::Cancelled => "CANCELLED",
        };
        write!(f, "{s}")
    }
}

/// Task counts by state.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskCounts {
    /// Total tasks.
    pub total: u32,
    /// Pending tasks.
    pub pending: u32,
    /// Queued tasks.
    pub queued: u32,
    /// Running tasks.
    pub running: u32,
    /// Succeeded tasks.
    pub succeeded: u32,
    /// Failed tasks.
    pub failed: u32,
    /// Skipped tasks.
    pub skipped: u32,
    /// Cancelled tasks.
    pub cancelled: u32,
}

/// Partition key-value pair.
#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionValue {
    /// Partition dimension name.
    pub key: String,
    /// Partition value.
    pub value: String,
}

/// Request to backfill missing `run_key` fingerprints.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunKeyBackfillRequest {
    /// Idempotency key to backfill.
    pub run_key: String,
    /// Asset selection (asset keys to materialize).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub selection: Vec<String>,
    /// Partition overrides (key=value pairs).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<PartitionValue>,
    /// Additional labels for the run.
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub labels: std::collections::HashMap<String, String>,
}

/// Response after backfilling a `run_key` reservation.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunKeyBackfillResponse {
    /// Run key that was updated.
    pub run_key: String,
    /// Run ID associated with the reservation.
    pub run_id: String,
    /// Fingerprint stored on the reservation.
    pub request_fingerprint: String,
    /// Whether the reservation was updated.
    pub updated: bool,
    /// Reservation creation timestamp.
    pub created_at: DateTime<Utc>,
}

/// Response for listing runs.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListRunsResponse {
    /// List of runs.
    pub runs: Vec<RunListItem>,
    /// Next page cursor.
    pub next_cursor: Option<String>,
}

/// Run list item.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunListItem {
    /// Run ID.
    pub run_id: String,
    /// Current state.
    pub state: RunState,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Completion timestamp.
    pub completed_at: Option<DateTime<Utc>>,
    /// Total task count.
    pub task_count: u32,
    /// Tasks succeeded.
    pub tasks_succeeded: u32,
    /// Tasks failed.
    pub tasks_failed: u32,
}

// ============================================================================
// Manifest Types
// ============================================================================

/// Request to deploy a manifest.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeployManifestRequest {
    /// Manifest version (e.g., "1.0").
    pub manifest_version: String,
    /// Code version identifier.
    pub code_version_id: String,
    /// Git context.
    #[serde(default)]
    pub git: GitContext,
    /// Asset definitions.
    pub assets: Vec<serde_json::Value>,
    /// Cron schedules.
    #[serde(default)]
    pub schedules: Vec<serde_json::Value>,
    /// Deployed by (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployed_by: Option<String>,
    /// Additional metadata.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

/// Git context for deployment provenance.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitContext {
    /// Repository URL.
    #[serde(default)]
    pub repository: String,
    /// Branch name.
    #[serde(default)]
    pub branch: String,
    /// Commit SHA.
    #[serde(default)]
    pub commit_sha: String,
    /// Commit message.
    #[serde(default)]
    pub commit_message: String,
    /// Author.
    #[serde(default)]
    pub author: String,
    /// Whether working directory was dirty.
    #[serde(default)]
    pub dirty: bool,
}

/// Response after deploying a manifest.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeployManifestResponse {
    /// Manifest ID (ULID).
    pub manifest_id: String,
    /// Workspace ID.
    pub workspace_id: String,
    /// Code version ID.
    pub code_version_id: String,
    /// Content fingerprint (SHA-256).
    pub fingerprint: String,
    /// Number of assets in manifest.
    pub asset_count: u32,
    /// Deployment timestamp.
    pub deployed_at: DateTime<Utc>,
}

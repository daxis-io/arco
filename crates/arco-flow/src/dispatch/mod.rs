//! Task dispatch abstraction for orchestration.
//!
//! This module provides:
//!
//! - [`TaskQueue`]: Trait for enqueueing tasks to execution backends
//! - [`TaskEnvelope`]: Serializable task dispatch payload
//! - [`InMemoryTaskQueue`]: In-memory queue for testing
//!
//! ## Design Principles
//!
//! - **Backend agnostic**: Same interface for Cloud Tasks, SQS, local workers
//! - **Idempotent dispatch**: Task IDs enable deduplication
//! - **Structured payloads**: JSON-serializable task envelopes

pub mod memory;

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use arco_core::{AssetId, RunId, TaskId};

use crate::error::Result;
use crate::plan::ResourceRequirements;
use crate::task_key::TaskKey;

/// Envelope for a task to be dispatched.
///
/// Contains all information needed by a worker to execute the task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskEnvelope {
    /// Unique task identifier.
    pub task_id: TaskId,
    /// Run this task belongs to.
    pub run_id: RunId,
    /// Asset being materialized.
    pub asset_id: AssetId,
    /// Semantic task key for deduplication.
    pub task_key: TaskKey,
    /// Tenant identifier for routing.
    pub tenant_id: String,
    /// Workspace identifier.
    pub workspace_id: String,
    /// Attempt number (1-indexed).
    pub attempt: u32,
    /// Resource requirements for scheduling.
    pub resources: ResourceRequirements,
    /// When the task was enqueued.
    pub enqueued_at: DateTime<Utc>,
    /// Deadline for task completion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline: Option<DateTime<Utc>>,
}

impl TaskEnvelope {
    /// Creates a new task envelope.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: TaskId,
        run_id: RunId,
        asset_id: AssetId,
        task_key: TaskKey,
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        attempt: u32,
        resources: ResourceRequirements,
    ) -> Self {
        Self {
            task_id,
            run_id,
            asset_id,
            task_key,
            tenant_id: tenant_id.into(),
            workspace_id: workspace_id.into(),
            attempt,
            resources,
            enqueued_at: Utc::now(),
            deadline: None,
        }
    }

    /// Sets the task deadline.
    #[must_use]
    pub const fn with_deadline(mut self, deadline: DateTime<Utc>) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Returns the idempotency key for this task.
    ///
    /// Uses `task_id` + attempt to ensure retries are distinguishable.
    #[must_use]
    pub fn idempotency_key(&self) -> String {
        format!("{}-{}", self.task_id, self.attempt)
    }
}

/// Result of enqueuing a task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueResult {
    /// Task was enqueued successfully.
    Enqueued {
        /// Queue-specific message ID.
        message_id: String,
    },
    /// Task was deduplicated (already enqueued).
    Deduplicated {
        /// The existing message ID.
        existing_message_id: String,
    },
    /// Queue is at capacity.
    QueueFull,
}

impl EnqueueResult {
    /// Returns true if the task was successfully enqueued.
    #[must_use]
    pub const fn is_enqueued(&self) -> bool {
        matches!(self, Self::Enqueued { .. })
    }

    /// Returns the message ID if enqueued.
    #[must_use]
    pub fn message_id(&self) -> Option<&str> {
        match self {
            Self::Enqueued { message_id } | Self::Deduplicated { existing_message_id: message_id } => {
                Some(message_id)
            }
            Self::QueueFull => None,
        }
    }
}

/// Options for task enqueueing.
#[derive(Debug, Clone, Default)]
pub struct EnqueueOptions {
    /// Delay before the task becomes visible to workers.
    pub delay: Option<Duration>,
    /// Priority (lower = higher priority, backend-specific).
    pub priority: Option<i32>,
    /// Custom routing key for queue selection.
    pub routing_key: Option<String>,
}

impl EnqueueOptions {
    /// Creates default options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the delay before task becomes visible.
    #[must_use]
    pub const fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Sets the task priority.
    #[must_use]
    pub const fn with_priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Sets a custom routing key.
    #[must_use]
    pub fn with_routing_key(mut self, key: impl Into<String>) -> Self {
        self.routing_key = Some(key.into());
        self
    }
}

/// Task queue abstraction for dispatching tasks to execution backends.
///
/// Implementations may target:
/// - Google Cloud Tasks
/// - AWS SQS
/// - Local in-memory queues (for testing)
///
/// ## Thread Safety
///
/// All methods are `Send + Sync` to support concurrent access from
/// multiple scheduler tasks.
///
/// ## Example
///
/// ```rust,ignore
/// use arco_flow::dispatch::{TaskQueue, TaskEnvelope, EnqueueOptions};
///
/// async fn dispatch_task<Q: TaskQueue>(queue: &Q, envelope: TaskEnvelope) {
///     let result = queue.enqueue(envelope, EnqueueOptions::default()).await?;
///     if result.is_enqueued() {
///         println!("Task dispatched: {:?}", result.message_id());
///     }
/// }
/// ```
#[async_trait]
pub trait TaskQueue: Send + Sync {
    /// Enqueues a task for execution.
    ///
    /// # Arguments
    ///
    /// * `envelope` - The task payload
    /// * `options` - Enqueueing options (delay, priority, routing)
    ///
    /// # Returns
    ///
    /// - `EnqueueResult::Enqueued` with message ID on success
    /// - `EnqueueResult::Deduplicated` if task was already enqueued
    /// - `EnqueueResult::QueueFull` if queue is at capacity
    async fn enqueue(&self, envelope: TaskEnvelope, options: EnqueueOptions) -> Result<EnqueueResult>;

    /// Enqueues multiple tasks in a batch.
    ///
    /// Default implementation calls `enqueue` for each task.
    /// Implementations may override for batch optimization.
    async fn enqueue_batch(
        &self,
        tasks: Vec<(TaskEnvelope, EnqueueOptions)>,
    ) -> Result<Vec<EnqueueResult>> {
        let mut results = Vec::with_capacity(tasks.len());
        for (envelope, options) in tasks {
            results.push(self.enqueue(envelope, options).await?);
        }
        Ok(results)
    }

    /// Returns the approximate number of tasks in the queue.
    ///
    /// This is an estimate and may not be exact.
    async fn queue_depth(&self) -> Result<usize>;

    /// Returns the queue's name or identifier.
    fn queue_name(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::AssetKey;
    use crate::task_key::TaskOperation;

    fn create_test_envelope() -> TaskEnvelope {
        TaskEnvelope::new(
            TaskId::generate(),
            RunId::generate(),
            AssetId::generate(),
            TaskKey::new(
                AssetKey::new("raw", "events"),
                TaskOperation::Materialize,
            ),
            "test-tenant",
            "test-workspace",
            1,
            ResourceRequirements::default(),
        )
    }

    #[test]
    fn task_envelope_idempotency_key() {
        let envelope = create_test_envelope();
        let key = envelope.idempotency_key();
        assert!(key.contains(&envelope.task_id.to_string()));
        assert!(key.contains("-1"));
    }

    #[test]
    fn task_envelope_serializes() {
        let envelope = create_test_envelope();
        let json = serde_json::to_string(&envelope);
        assert!(json.is_ok());

        let parsed: serde_json::Result<TaskEnvelope> = serde_json::from_str(&json.unwrap());
        assert!(parsed.is_ok());
    }

    #[test]
    fn enqueue_result_is_enqueued() {
        assert!(EnqueueResult::Enqueued {
            message_id: "msg-1".to_string()
        }
        .is_enqueued());

        assert!(!EnqueueResult::Deduplicated {
            existing_message_id: "msg-1".to_string()
        }
        .is_enqueued());

        assert!(!EnqueueResult::QueueFull.is_enqueued());
    }

    #[test]
    fn enqueue_result_message_id() {
        assert_eq!(
            EnqueueResult::Enqueued {
                message_id: "msg-1".to_string()
            }
            .message_id(),
            Some("msg-1")
        );

        assert_eq!(
            EnqueueResult::Deduplicated {
                existing_message_id: "msg-2".to_string()
            }
            .message_id(),
            Some("msg-2")
        );

        assert_eq!(EnqueueResult::QueueFull.message_id(), None);
    }

    #[test]
    fn enqueue_options_builder() {
        let options = EnqueueOptions::new()
            .with_delay(Duration::from_secs(30))
            .with_priority(5)
            .with_routing_key("high-priority");

        assert_eq!(options.delay, Some(Duration::from_secs(30)));
        assert_eq!(options.priority, Some(5));
        assert_eq!(options.routing_key, Some("high-priority".to_string()));
    }
}

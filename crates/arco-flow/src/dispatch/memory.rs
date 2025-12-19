//! In-memory task queue implementation for testing.
//!
//! This module provides [`InMemoryTaskQueue`], a simple in-memory implementation
//! of the [`TaskQueue`] trait suitable for testing and development.
//!
//! ## Limitations
//!
//! - **NOT suitable for production**: No persistence, no distribution
//! - **Single-process only**: Tasks are not visible across process boundaries
//! - **No delay support**: Delay option is accepted but ignored
//! - **Deduplication is queue-scoped**: Keys are released when tasks are dequeued

use std::collections::{HashMap, VecDeque};
use std::sync::{PoisonError, RwLock};

use async_trait::async_trait;
use ulid::Ulid;

use super::{EnqueueOptions, EnqueueResult, TaskEnvelope, TaskQueue};
use crate::error::{Error, Result};

/// Entry in the in-memory queue.
#[derive(Debug, Clone)]
pub struct QueueEntry {
    /// Message ID.
    pub message_id: String,
    /// Idempotency key for deduplication.
    pub idempotency_key: String,
    /// Task envelope.
    pub envelope: TaskEnvelope,
    /// Options used when enqueuing.
    pub options: EnqueueOptions,
}

/// Internal queue state protected by a single lock.
#[derive(Debug, Default)]
struct QueueState {
    queue: VecDeque<QueueEntry>,
    seen_keys: HashMap<String, String>,
}

/// In-memory task queue for testing.
///
/// Provides a simple, thread-safe implementation of the [`TaskQueue`] trait
/// using `RwLock` for synchronization.
///
/// ## Example
///
/// ```rust
/// use arco_flow::dispatch::memory::InMemoryTaskQueue;
///
/// let queue = InMemoryTaskQueue::new("test-queue");
/// // Enqueue tasks in tests...
/// ```
#[derive(Debug)]
pub struct InMemoryTaskQueue {
    name: String,
    state: RwLock<QueueState>,
    /// Maximum queue capacity.
    max_capacity: Option<usize>,
}

impl Default for InMemoryTaskQueue {
    fn default() -> Self {
        Self::new("default")
    }
}

/// Converts a lock poison error to a storage error.
fn poison_err<T>(_: PoisonError<T>) -> Error {
    Error::storage("task queue lock poisoned")
}

impl InMemoryTaskQueue {
    /// Creates a new in-memory task queue.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            state: RwLock::new(QueueState::default()),
            max_capacity: None,
        }
    }

    /// Creates a queue with a maximum capacity.
    #[must_use]
    pub fn with_capacity(name: impl Into<String>, max_capacity: usize) -> Self {
        Self {
            name: name.into(),
            state: RwLock::new(QueueState::default()),
            max_capacity: Some(max_capacity),
        }
    }

    /// Generates a new message ID.
    fn generate_message_id() -> String {
        Ulid::new().to_string()
    }

    /// Takes the next task from the queue.
    ///
    /// Returns `None` if the queue is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub fn take(&self) -> Result<Option<QueueEntry>> {
        let mut state = self.state.write().map_err(poison_err)?;
        let entry = state.queue.pop_front();
        if let Some(ref entry) = entry {
            state.seen_keys.remove(&entry.idempotency_key);
        }
        drop(state);
        Ok(entry)
    }

    /// Peeks at the next task without removing it.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub fn peek(&self) -> Result<Option<QueueEntry>> {
        let state = self.state.read().map_err(poison_err)?;
        Ok(state.queue.front().cloned())
    }

    /// Returns all enqueued tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub fn drain(&self) -> Result<Vec<QueueEntry>> {
        let mut state = self.state.write().map_err(poison_err)?;
        let drained: Vec<_> = state.queue.drain(..).collect();
        for entry in &drained {
            state.seen_keys.remove(&entry.idempotency_key);
        }
        drop(state);
        Ok(drained)
    }

    /// Clears the queue and deduplication state.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub fn clear(&self) -> Result<()> {
        let mut state = self.state.write().map_err(poison_err)?;
        state.queue.clear();
        state.seen_keys.clear();
        drop(state);
        Ok(())
    }
}

#[async_trait]
impl TaskQueue for InMemoryTaskQueue {
    async fn enqueue(&self, envelope: TaskEnvelope, options: EnqueueOptions) -> Result<EnqueueResult> {
        let idempotency_key = envelope.idempotency_key();

        let mut state = self.state.write().map_err(poison_err)?;

        if let Some(existing) = state.seen_keys.get(&idempotency_key) {
            return Ok(EnqueueResult::Deduplicated {
                existing_message_id: existing.clone(),
            });
        }

        if let Some(max) = self.max_capacity {
            if state.queue.len() >= max {
                return Ok(EnqueueResult::QueueFull);
            }
        }

        let message_id = Self::generate_message_id();
        state
            .seen_keys
            .insert(idempotency_key.clone(), message_id.clone());
        state.queue.push_back(QueueEntry {
            message_id: message_id.clone(),
            idempotency_key,
            envelope,
            options,
        });
        drop(state);

        Ok(EnqueueResult::Enqueued { message_id })
    }

    async fn queue_depth(&self) -> Result<usize> {
        let state = self.state.read().map_err(poison_err)?;
        Ok(state.queue.len())
    }

    fn queue_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::TaskEnvelope;
    use crate::plan::{AssetKey, ResourceRequirements};
    use crate::task_key::{TaskKey, TaskOperation};
    use arco_core::{AssetId, RunId, TaskId};

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

    #[tokio::test]
    async fn enqueue_and_take() -> Result<()> {
        let queue = InMemoryTaskQueue::new("test");

        let envelope = create_test_envelope();
        let envelope_clone = envelope.clone();
        let task_id = envelope.task_id;

        let result = queue.enqueue(envelope, EnqueueOptions::default()).await?;
        assert!(result.is_enqueued());

        let entry = queue.take()?.expect("should have entry");
        assert_eq!(entry.envelope.task_id, task_id);

        // Queue should be empty now
        assert!(queue.take()?.is_none());

        // Dedup key should be released after take
        let result = queue
            .enqueue(envelope_clone, EnqueueOptions::default())
            .await?;
        assert!(result.is_enqueued());

        Ok(())
    }

    #[tokio::test]
    async fn deduplication() -> Result<()> {
        let queue = InMemoryTaskQueue::new("test");

        let envelope = create_test_envelope();
        let envelope2 = envelope.clone();

        // First enqueue succeeds
        let result1 = queue.enqueue(envelope, EnqueueOptions::default()).await?;
        assert!(result1.is_enqueued());
        let first_message_id = match result1 {
            EnqueueResult::Enqueued { message_id } => message_id,
            _ => return Err(Error::dispatch("expected Enqueued result")),
        };

        // Second enqueue with same idempotency key is deduplicated
        let result2 = queue.enqueue(envelope2, EnqueueOptions::default()).await?;
        assert!(!result2.is_enqueued());
        if let EnqueueResult::Deduplicated {
            existing_message_id,
        } = result2
        {
            assert_eq!(existing_message_id, first_message_id);
        } else {
            panic!("Expected Deduplicated");
        }

        // Only one task in queue
        assert_eq!(queue.queue_depth().await?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn different_attempts_are_distinct() -> Result<()> {
        let queue = InMemoryTaskQueue::new("test");

        let mut envelope1 = create_test_envelope();
        envelope1.attempt = 1;

        let mut envelope2 = envelope1.clone();
        envelope2.attempt = 2;

        // Both should enqueue (different attempts = different idempotency keys)
        let result1 = queue.enqueue(envelope1, EnqueueOptions::default()).await?;
        assert!(result1.is_enqueued());

        let result2 = queue.enqueue(envelope2, EnqueueOptions::default()).await?;
        assert!(result2.is_enqueued());

        assert_eq!(queue.queue_depth().await?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn capacity_limit() -> Result<()> {
        let queue = InMemoryTaskQueue::with_capacity("test", 2);

        // Enqueue up to capacity
        let result1 = queue
            .enqueue(create_test_envelope(), EnqueueOptions::default())
            .await?;
        assert!(result1.is_enqueued());

        let result2 = queue
            .enqueue(create_test_envelope(), EnqueueOptions::default())
            .await?;
        assert!(result2.is_enqueued());

        // Third should fail
        let result3 = queue
            .enqueue(create_test_envelope(), EnqueueOptions::default())
            .await?;
        assert!(matches!(result3, EnqueueResult::QueueFull));

        Ok(())
    }

    #[tokio::test]
    async fn peek_does_not_remove() -> Result<()> {
        let queue = InMemoryTaskQueue::new("test");

        let envelope = create_test_envelope();
        queue.enqueue(envelope, EnqueueOptions::default()).await?;

        // Peek should return the task
        let peeked = queue.peek()?;
        assert!(peeked.is_some());

        // Queue depth should still be 1
        assert_eq!(queue.queue_depth().await?, 1);

        // Take should still work
        let taken = queue.take()?;
        assert!(taken.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn drain_clears_queue() -> Result<()> {
        let queue = InMemoryTaskQueue::new("test");

        queue
            .enqueue(create_test_envelope(), EnqueueOptions::default())
            .await?;
        queue
            .enqueue(create_test_envelope(), EnqueueOptions::default())
            .await?;

        let drained = queue.drain()?;
        assert_eq!(drained.len(), 2);
        assert_eq!(queue.queue_depth().await?, 0);

        Ok(())
    }

    #[tokio::test]
    async fn enqueue_batch() -> Result<()> {
        let queue = InMemoryTaskQueue::new("test");

        let tasks = vec![
            (create_test_envelope(), EnqueueOptions::default()),
            (create_test_envelope(), EnqueueOptions::default()),
            (create_test_envelope(), EnqueueOptions::default()),
        ];

        let results = queue.enqueue_batch(tasks).await?;
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.is_enqueued()));

        assert_eq!(queue.queue_depth().await?, 3);

        Ok(())
    }

    #[test]
    fn queue_name() {
        let queue = InMemoryTaskQueue::new("my-queue");
        assert_eq!(queue.queue_name(), "my-queue");
    }

    #[tokio::test]
    async fn clear_resets_state() -> Result<()> {
        let queue = InMemoryTaskQueue::new("test");

        let envelope = create_test_envelope();
        let envelope_clone = envelope.clone();

        queue.enqueue(envelope, EnqueueOptions::default()).await?;
        assert_eq!(queue.queue_depth().await?, 1);

        queue.clear()?;
        assert_eq!(queue.queue_depth().await?, 0);

        // Should be able to enqueue same task again after clear
        let result = queue
            .enqueue(envelope_clone, EnqueueOptions::default())
            .await?;
        assert!(result.is_enqueued());

        Ok(())
    }
}

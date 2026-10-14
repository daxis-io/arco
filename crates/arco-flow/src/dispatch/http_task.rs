//! HTTP task handoff used by the Flow dispatcher and sweeper.

use std::collections::HashMap;

use async_trait::async_trait;

use super::{CloudTasksDispatcher, EnqueueOptions, EnqueueResult};
use crate::error::{Error, Result};
use crate::orchestration::worker_contract::WorkerDispatchEnvelope;

/// Sends a task to an operator-selected queue for eventual HTTP delivery.
///
/// Task ID is the idempotency key. Implementations must durably accept a task
/// before reporting `Enqueued`, and report `Deduplicated` only for a known
/// duplicate ID. A lost or uncertain response is an error, never an
/// acknowledged enqueue; the durable dispatch outbox will retry the ID.
#[async_trait]
pub trait HttpTaskEnqueuer: Send + Sync {
    /// Enqueues one HTTP task. `options.delay` must be honored for timers.
    ///
    /// # Errors
    ///
    /// Returns an error if queue acceptance cannot be established.
    async fn enqueue_http(
        &self,
        task_id: &str,
        target_url: &str,
        body: &[u8],
        options: EnqueueOptions,
        audience: Option<&str>,
        extra_headers: Option<HashMap<String, String>>,
    ) -> Result<EnqueueResult>;
}

#[async_trait]
impl HttpTaskEnqueuer for CloudTasksDispatcher {
    async fn enqueue_http(
        &self,
        task_id: &str,
        target_url: &str,
        body: &[u8],
        options: EnqueueOptions,
        audience: Option<&str>,
        extra_headers: Option<HashMap<String, String>>,
    ) -> Result<EnqueueResult> {
        Self::enqueue_http(
            self,
            task_id,
            target_url,
            body,
            options,
            audience,
            extra_headers,
        )
        .await
    }
}

/// Enqueues the canonical worker envelope through an operator-selected queue.
///
/// The packaged dispatcher and sweeper use this path. An operator-owned
/// dispatcher can use it with another [`HttpTaskEnqueuer`] implementation.
///
/// # Errors
///
/// Returns a serialization error or the queue's acceptance error.
#[allow(
    clippy::implicit_hasher,
    reason = "the queue interface accepts owned default-hasher headers"
)]
pub async fn enqueue_worker_dispatch(
    queue: &dyn HttpTaskEnqueuer,
    task_id: &str,
    target_url: &str,
    audience: &str,
    headers: &HashMap<String, String>,
    envelope: &WorkerDispatchEnvelope,
) -> Result<EnqueueResult> {
    let body = envelope
        .to_json()
        .map_err(|error| Error::serialization(format!("dispatch envelope error: {error}")))?;
    let mut options = EnqueueOptions::new();
    if envelope.worker_queue != "default-queue" {
        options = options.with_routing_key(envelope.worker_queue.clone());
    }
    queue
        .enqueue_http(
            task_id,
            target_url,
            body.as_bytes(),
            options,
            Some(audience),
            Some(headers.clone()),
        )
        .await
}

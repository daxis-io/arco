# ADR-017: Cloud Tasks Dispatcher

## Status
Accepted

## Context
Arco needs reliable task dispatch to workers with:
- **At-least-once delivery**: Tasks must not be lost
- **Idempotency**: Duplicate dispatches must be handled safely
- **Retry with backoff**: Transient failures should be retried
- **Scalability**: Support thousands of concurrent tasks
- **Observability**: Track dispatch latency and queue depth

Options considered:
1. **Direct HTTP calls**: Simple but unreliable (no retry, no persistence)
2. **Message queues** (Pub/Sub, SQS): Good for async, but complex ACK handling
3. **Cloud Tasks** (GCP): Purpose-built for HTTP task dispatch with built-in retry

## Decision
Use Google Cloud Tasks as the primary production dispatcher:

### TaskQueue Trait
```rust
#[async_trait]
pub trait TaskQueue: Send + Sync {
    /// Enqueue a task for execution.
    async fn enqueue(&self, envelope: TaskEnvelope, options: EnqueueOptions) -> Result<EnqueueResult>;

    /// Get approximate queue depth.
    async fn queue_depth(&self) -> Result<usize>;

    /// Get queue name for logging.
    fn queue_name(&self) -> &str;
}
```

### Idempotency
Tasks use `{run_id}/{task_id}/{attempt}` as the idempotency key to ensure:
- Same attempt dispatched twice → Cloud Tasks deduplicates
- New attempt after failure → Distinct Cloud Tasks entry

The dispatcher sanitizes the key to meet Cloud Tasks naming rules.

### OIDC Authentication
Workers receive OIDC tokens for secure verification:
```rust
OidcToken {
    service_account_email: "scheduler@project.iam.gserviceaccount.com",
    audience: "https://worker.run.app",
}
```

### Retry Configuration
```rust
RetryConfig {
    max_attempts: 5,
    min_backoff: Duration::from_secs(10),
    max_backoff: Duration::from_secs(300),
    max_retry_duration: Duration::from_secs(3600),
}
```
These settings are applied to the queue via the Cloud Tasks API on first use
and require `cloudtasks.queues.update` permissions. For IaC-managed queues,
set `apply_queue_retry_config = false` to skip queue updates.

### Queue Routing
Tasks can be routed to different queues based on:
- Priority (scheduler ordering or priority-mapped queues)
- Tenant (tenant-specific queues for isolation)
- Task type (different resource pools)

### Feature Flag
Available under the `gcp` feature flag. Without it, `CloudTasksDispatcher` returns a configuration error.

## Consequences

### Positive
- Built-in persistence and retry (no message loss)
- Native idempotency via task naming
- OIDC authentication for secure worker invocation
- Managed service (no queue infrastructure to operate)
- Good observability via Cloud Monitoring

### Negative
- GCP-specific (requires porting for AWS/Azure)
- Cost per task dispatch (pricing consideration at scale)
- Queue depth not efficiently queryable (use metrics instead)
- Cold start latency for first request

### Alternatives
For non-GCP environments:
- `InMemoryTaskQueue`: Testing only
- Future: AWS SQS dispatcher, Azure Queue dispatcher

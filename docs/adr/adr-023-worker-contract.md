# ADR-023: Worker Contract Specification

## Status

Accepted

## Context

Servo orchestration dispatches tasks to workers via Cloud Tasks (ADR-017) and expects
workers to report results back. The dispatch payload (`TaskEnvelope`) is well-defined
in `crates/arco-flow/src/dispatch/mod.rs`, but the **worker callback contract** is not
specified:

- Where do workers POST results?
- How do workers authenticate?
- What payloads do workers send?
- How do heartbeats and cancellation work?

Without a formal contract, workers cannot be implemented correctly, and the control
plane cannot validate worker behavior.

This ADR is referenced as **ORCH-ADR-004** in the exception planning document.

## Decision

Define a complete worker contract covering:

1. Callback endpoints (REST/HTTP)
2. Authentication (short-lived tokens)
3. Request/response payloads
4. Heartbeat protocol
5. Cancellation protocol

### 1. Callback Endpoints

Workers report results to the control plane via HTTP POST. All endpoints are
scoped to the task being executed.

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/v1/tasks/{task_id}/started` | POST | Worker began execution |
| `/v1/tasks/{task_id}/heartbeat` | POST | Worker is still alive |
| `/v1/tasks/{task_id}/completed` | POST | Task finished (success or failure) |

**Base URL**: Provided in TaskEnvelope as `callback_base_url`.

### 2. Authentication

Workers authenticate using short-lived bearer tokens scoped to the specific task.

```
Authorization: Bearer <task_token>
```

**Token properties:**
- Scoped to `(tenant_id, workspace_id, run_id, task_id, attempt)`
- Expires after `token_ttl_seconds` (default: 3600, max: 7200)
- Cryptographically signed by control plane (HMAC-SHA256 or JWT)
- Cannot be used for other tasks or attempts
- Token lifetime MUST cover the task deadline (or be extended via re-dispatch)

**Token is provided in TaskEnvelope:**

```json
{
  "taskId": "task_01HGW5N7XHJM4GX29KZRXQP6N9",
  "runId": "run_01HGW5N7XHJM4GX29KZRXQP6N8",
  ...
  "callbackBaseUrl": "https://api.arco.dev",
  "taskToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "tokenExpiresAt": "2025-01-15T12:00:00Z"
}
```

### 3. Request/Response Payloads

#### 3.1 TaskStarted

Worker sends when execution begins.

**Request:**
```json
POST /v1/tasks/{task_id}/started
Authorization: Bearer <task_token>
Content-Type: application/json

{
  "attempt": 1,
  "workerId": "worker-abc123",
  "startedAt": "2025-01-15T10:00:00Z"
}
```

**Response (200 OK):**
```json
{
  "acknowledged": true,
  "serverTime": "2025-01-15T10:00:01Z"
}
```

**Response (409 Conflict):** Task already completed or cancelled.
```json
{
  "error": "task_already_terminal",
  "state": "CANCELLED",
  "message": "Task was cancelled before worker started"
}
```

Worker MUST abort execution if 409 is received.

#### 3.2 Heartbeat

Worker sends periodically to indicate liveness.

**Request:**
```json
POST /v1/tasks/{task_id}/heartbeat
Authorization: Bearer <task_token>
Content-Type: application/json

{
  "attempt": 1,
  "workerId": "worker-abc123",
  "heartbeatAt": "2025-01-15T10:05:00Z",
  "progressPct": 45,
  "message": "Processing partition 5 of 10"
}
```

If `heartbeatAt` is omitted, the control plane uses receipt time.

**Response (200 OK):**
```json
{
  "acknowledged": true,
  "shouldCancel": false,
  "serverTime": "2025-01-15T10:05:00Z"
}
```

**Response (200 OK with cancel signal):**
```json
{
  "acknowledged": true,
  "shouldCancel": true,
  "cancelReason": "user_requested",
  "serverTime": "2025-01-15T10:05:00Z"
}
```

Worker MUST check `shouldCancel` and initiate graceful shutdown if true.

**Response (410 Gone):** Task timed out or marked zombie.
```json
{
  "error": "task_expired",
  "message": "Task exceeded heartbeat timeout"
}
```

Worker MUST abort execution immediately if 410 is received.

#### 3.3 TaskCompleted

Worker sends when execution finishes (success or failure).

**Request (Success):**
```json
POST /v1/tasks/{task_id}/completed
Authorization: Bearer <task_token>
Content-Type: application/json

{
  "attempt": 1,
  "workerId": "worker-abc123",
  "outcome": "SUCCEEDED",
  "completedAt": "2025-01-15T10:30:00Z",
  "output": {
    "materializationId": "mat_01HGW5N7XHJM4GX29KZRXQP6NA",
    "rowCount": 1000000,
    "byteSize": 52428800,
    "outputPath": "gs://bucket/tenant/workspace/assets/raw/events/2025-01-15/"
  },
  "metrics": {
    "cpuTimeMs": 45000,
    "peakMemoryBytes": 2147483648,
    "ioReadBytes": 10485760,
    "ioWriteBytes": 52428800
  }
}
```

**Request (Failure):**
```json
POST /v1/tasks/{task_id}/completed
Authorization: Bearer <task_token>
Content-Type: application/json

{
  "attempt": 1,
  "workerId": "worker-abc123",
  "outcome": "FAILED",
  "completedAt": "2025-01-15T10:30:00Z",
  "error": {
    "category": "USER_CODE",
    "message": "KeyError: 'missing_column'",
    "stackTrace": "Traceback (most recent call last):\n  File ...",
    "retryable": true
  },
  "metrics": {
    "cpuTimeMs": 5000,
    "peakMemoryBytes": 1073741824
  }
}
```

**Response (200 OK):**
```json
{
  "acknowledged": true,
  "finalState": "SUCCEEDED",
  "serverTime": "2025-01-15T10:30:01Z"
}
```

**Response (409 Conflict):** Different attempt already completed.
```json
{
  "error": "attempt_mismatch",
  "expectedAttempt": 2,
  "receivedAttempt": 1,
  "message": "A newer attempt has already completed"
}
```

Late results (wrong attempt) are logged but ignored.

### 4. Heartbeat Protocol

**Timing:**
- `heartbeat_interval_ms`: How often worker sends heartbeats (default: 30000)
- `heartbeat_timeout_ms`: How long before control plane marks task as zombie (default: 90000)

**Invariant:** `heartbeat_timeout_ms >= 2 * heartbeat_interval_ms`

**Worker behavior:**
1. Send heartbeat every `heartbeat_interval_ms`
2. If heartbeat fails with network error, retry with exponential backoff
3. If heartbeat returns 410 (Gone), abort immediately
4. If heartbeat returns `shouldCancel: true`, initiate graceful shutdown

**Control plane behavior:**
1. Track `last_heartbeat_at` per task
2. If `now() - last_heartbeat_at > heartbeat_timeout_ms`, mark task as ZOMBIE
3. ZOMBIE tasks transition to FAILED with reason `HEARTBEAT_TIMEOUT`
4. Zombie detection runs every `heartbeat_interval_ms / 2`

### 5. Cancellation Protocol

Cancellation is cooperative. The control plane signals intent; the worker decides
how to respond.

**Signal delivery:**
- Via heartbeat response: `shouldCancel: true`
- Via `/started` response: 409 with `state: CANCELLED`

**Worker behavior on cancel signal:**
1. Stop accepting new work
2. Complete in-flight operations if safe (or abort)
3. Clean up resources
4. Send `/completed` with outcome `CANCELLED`

**Grace period:**
- `cancel_grace_period_ms`: Time allowed for graceful shutdown (default: 30000)
- After grace period, control plane marks task as FAILED with reason `CANCEL_TIMEOUT`

**Worker response (cancelled):**
```json
{
  "attempt": 1,
  "workerId": "worker-abc123",
  "outcome": "CANCELLED",
  "completedAt": "2025-01-15T10:15:00Z",
  "cancelledDuringPhase": "processing",
  "partialProgress": {
    "recordsProcessed": 500000,
    "lastCheckpoint": "partition-5"
  }
}
```

### 6. Error Categories

Workers MUST categorize errors to enable correct retry behavior:

| Category | Description | Retryable by Default |
|----------|-------------|---------------------|
| `USER_CODE` | Error in asset code (exceptions, assertion failures) | Yes |
| `DATA_QUALITY` | Input data fails validation | No |
| `INFRASTRUCTURE` | Cloud service errors, OOM, disk full | Yes |
| `CONFIGURATION` | Missing env vars, invalid config | No |
| `TIMEOUT` | Execution exceeded deadline | Yes (with backoff) |
| `CANCELLED` | Task was cancelled | No |

### 7. Attempt Guard

Every callback includes `attempt` number. Control plane MUST verify:

```rust
fn handle_callback(request: CallbackRequest, stored_attempt: u32) -> Result<Response> {
    if request.attempt != stored_attempt {
        // Late result from previous attempt - ignore
        log::warn!(
            "Ignoring late result: received attempt {} but current is {}",
            request.attempt, stored_attempt
        );
        return Ok(Response::conflict("attempt_mismatch"));
    }
    // Process callback...
}
```

This prevents stale workers from corrupting state after retries.

### 8. TaskEnvelope Extension

Extend `TaskEnvelope` to include callback configuration:

```rust
pub struct TaskEnvelope {
    // Existing fields...
    pub task_id: TaskId,
    pub run_id: RunId,
    pub asset_id: AssetId,
    pub task_key: TaskKey,
    pub tenant_id: String,
    pub workspace_id: String,
    pub attempt: u32,
    pub resources: ResourceRequirements,
    pub enqueued_at: DateTime<Utc>,
    pub deadline: Option<DateTime<Utc>>,

    // NEW: Callback configuration
    pub callback_base_url: String,
    pub task_token: String,
    pub token_expires_at: DateTime<Utc>,
    pub heartbeat_interval_ms: u32,
    pub heartbeat_timeout_ms: u32,
    pub cancel_grace_period_ms: u32,
}
```

## Consequences

### Positive

- Complete specification for worker implementation
- Clear authentication model with scoped tokens
- Cooperative cancellation prevents resource leaks
- Attempt guards prevent stale-worker corruption
- Error categorization enables intelligent retry policies
- Heartbeat protocol detects stuck workers

### Negative

- Token management adds complexity to control plane
- Workers must implement heartbeat loop correctly
- Network partitions may cause false-positive zombie detection
- Cancellation is cooperative (worker can ignore)

### Mitigations

- Provide SDK/library for worker callback handling (Python, Rust)
- Heartbeat timeout should be generous (3x interval)
- Force-cancel after extended grace period

## Related ADRs

- ADR-017: Cloud Tasks Dispatcher (dispatch mechanism)
- ADR-020: Orchestration as Unified Domain (event model)
- ADR-021: Cloud Tasks Naming Convention (task ID generation)

# ADR-023: Worker Contract Specification

## Status

Accepted

## Context

Arco Flow orchestration dispatches tasks to workers via Cloud Tasks (ADR-017) and expects
workers to report results back. The worker wire contract is published in
`crates/arco-worker-contract` for Rust/serde/OpenAPI consumers and additively mirrored in
`proto/arco/orchestration/v1/orchestration.proto` for language-neutral protobuf consumers.
Historically, downstream workers had to infer pieces of this contract from dispatcher payloads,
API route DTOs, and Python worker code; this ADR names the canonical surface.

- Where do workers POST results?
- How do workers authenticate?
- What payloads do workers send?
- How do heartbeats and cancellation work?

Without a formal contract, workers cannot be implemented correctly, and the control
plane cannot validate worker behavior.

This ADR is referenced as **ORCH-ADR-004** in the exception planning document.

## Decision

Define a complete worker contract covering:

1. Dispatch envelope
2. Callback endpoints (REST/HTTP)
3. Authentication (short-lived tokens)
4. Request/response payloads
5. Heartbeat protocol
6. Cancellation protocol

The canonical JSON field spelling for new writes is `camelCase`. Runtime readers must continue
to accept the legacy snake_case dispatch envelope during migration. Compatibility fixtures live
under `crates/arco-worker-contract/tests/fixtures/worker_protocol/` and
`crates/arco-proto/fixtures/worker_protocol/`.

### 1. Dispatch Envelope

Dispatcher and sweeper services emit `WorkerDispatchEnvelope`. Workers use `taskId` for callback
URLs and `taskKey` to select the work to execute. `taskId` is an opaque callback identifier derived
from `(runId, taskKey)`; it is not the semantic task key and should not be parsed outside the
published helpers.

```json
{
  "tenantId": "tenant-a",
  "workspaceId": "workspace-b",
  "taskId": "ct1_cnVuLTEyMwBhbmFseXRpY3MuZGFpbHlfc2FsZXM",
  "taskKey": "analytics.daily_sales",
  "runId": "run-123",
  "attempt": 1,
  "attemptId": "att-123",
  "dispatchId": "dispatch:run-123:analytics.daily_sales:1",
  "executionLocationId": "local-dev",
  "workerQueue": "default-queue",
  "callbackBaseUrl": "https://api.arco.dev",
  "taskToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "tokenExpiresAt": "2026-01-01T00:00:00Z",
  "payload": {}
}
```

Compatibility requirements:
- `executionLocationId` is optional and remains backward compatible.
- Legacy snake_case dispatch payloads are read-compatible.
- Callback endpoints accept opaque `taskId` first; legacy task-key paths are accepted only when
  the task key is unambiguous in current orchestration state.
- Minimal embedded Rust client/server sketches live in `crates/arco-worker-contract/README.md`
  and `crates/arco-worker-contract/examples/embedded_protocol.rs`. The Python worker client and
  server consume the same envelope and callback shapes.

### 2. Callback Endpoints

Workers report results to the control plane via HTTP POST. All endpoints are
scoped to the task being executed.

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/tasks/{taskId}/started` | POST | Worker began execution |
| `/api/v1/tasks/{taskId}/heartbeat` | POST | Worker is still alive |
| `/api/v1/tasks/{taskId}/completed` | POST | Task finished (success or failure) |

**Base URL**: Provided in `WorkerDispatchEnvelope` as `callbackBaseUrl`. This is the API
root; bundled clients append `/api/v1/tasks/{taskId}/...`.

### 3. Authentication

Workers authenticate using short-lived bearer tokens scoped to the specific task.

```
Authorization: Bearer <task_token>
```

**Token properties:**
- Scoped to `(tenantId, workspaceId, taskId)`, with callback handlers validating run and attempt
  against current orchestration state.
- Expires after `token_ttl_seconds` (default: 3600, max: 7200)
- Cryptographically signed by control plane (HMAC-SHA256 or JWT)
- Cannot be used for other tasks or attempts
- Token lifetime MUST cover the task deadline (or be extended via re-dispatch)

**Token is provided in `WorkerDispatchEnvelope`:**

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

### 4. Request/Response Payloads

#### 3.1 TaskStarted

Worker sends when execution begins.

**Request:**
```json
POST /api/v1/tasks/{taskId}/started
Authorization: Bearer <task_token>
Content-Type: application/json

{
  "attempt": 1,
  "attemptId": "01HQ123ATT",
  "workerId": "worker-abc123",
  "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
  "startedAt": "2025-01-15T10:00:00Z"
}
```

`traceparent` is optional and used for end-to-end trace correlation.

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
POST /api/v1/tasks/{taskId}/heartbeat
Authorization: Bearer <task_token>
Content-Type: application/json

{
  "attempt": 1,
  "attemptId": "01HQ123ATT",
  "workerId": "worker-abc123",
  "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
  "heartbeatAt": "2025-01-15T10:05:00Z",
  "progressPct": 45,
  "message": "Processing partition 5 of 10"
}
```

If `heartbeatAt` is omitted, the control plane uses receipt time.
Canonical API responses include a JSON body. Client helpers may treat an empty successful
heartbeat response body as implicit continue (`acknowledged=true`, `shouldCancel=false`) for
legacy compatibility.

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

**Response (410 Gone):** Task is no longer active (expired or already terminal).
```json
{
  "error": "task_expired",
  "message": "Task is no longer active",
  "state": "SUCCEEDED"
}
```

Worker MUST abort execution immediately if 410 is received.

#### 3.3 TaskCompleted

Worker sends when execution finishes (success or failure).

**Request (Success):**
```json
POST /api/v1/tasks/{taskId}/completed
Authorization: Bearer <task_token>
Content-Type: application/json

{
  "attempt": 1,
  "attemptId": "01HQ123ATT",
  "workerId": "worker-abc123",
  "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
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
POST /api/v1/tasks/{taskId}/completed
Authorization: Bearer <task_token>
Content-Type: application/json

{
  "attempt": 1,
  "attemptId": "01HQ123ATT",
  "workerId": "worker-abc123",
  "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
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

**Payload flexibility:** The control plane records whatever fields the worker sends for observability.
Conventions (not enforced):
- `SUCCEEDED`: output expected, error should be `null`
- `FAILED`: error expected, output optional (partial results allowed)
- `CANCELLED`: output/error optional
- Legacy `result` in completed payloads is accepted as an alias for `output`.

### 5. Heartbeat Protocol

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

### 6. Cancellation Protocol

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
  "attemptId": "01HQ123ATT",
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

Every callback includes `attempt` number and `attemptId`. Control plane MUST verify:

```rust
fn handle_callback(
    request: CallbackRequest,
    stored_attempt: u32,
    stored_attempt_id: &str,
) -> Result<Response> {
    if request.attempt != stored_attempt {
        // Late result from previous attempt - ignore
        log::warn!(
            "Ignoring late result: received attempt {} but current is {}",
            request.attempt, stored_attempt
        );
        return Ok(Response::conflict("attempt_mismatch"));
    }
    if request.attempt_id != stored_attempt_id {
        log::warn!(
            "Ignoring late result: received attempt_id {} but current is {}",
            request.attempt_id, stored_attempt_id
        );
        return Ok(Response::conflict("attempt_id_mismatch"));
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
    pub attempt_id: String,
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

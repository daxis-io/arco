# ADR-021: Cloud Tasks Naming Convention

## Status

Accepted

## Context

Cloud Tasks task IDs have specific constraints:
- Only `[A-Za-z0-9_-]` characters allowed (no colons, slashes)
- Maximum 500 characters
- Cannot reuse names for ~24 hours after completion (deduplication window)
- Sequential prefixes increase latency (Google recommendation: use hashed prefixes)

Our orchestration system uses human-readable internal IDs for debugging and Parquet queries:
- `dispatch:{run_id}:{task_key}:{attempt}`
- `timer:retry:{run_id}:{task_key}:{attempt}:{due_epoch}`

These contain colons, which are invalid for Cloud Tasks.

### Current Implementation

The existing `sanitize_task_id()` function in `cloud_tasks.rs:383-404` replaces invalid
characters with underscores. This approach has limitations:

1. **Non-deterministic collisions**: `a:b` and `a_b` both become `a_b`
2. **Sequential prefixes**: `dispatch_run1_task1_1`, `dispatch_run1_task1_2` share prefix
3. **Loses structure**: Colons are semantic separators, underscores lose that meaning

## Decision

**Maintain TWO identifiers for every dispatch and timer operation:**

### Internal ID (Parquet PK, idempotency_key, logs)

Human-readable with colons, stored in Parquet and used for debugging:

```
dispatch:{run_id}:{task_key}:{attempt}
timer:retry:{run_id}:{task_key}:{attempt}:{due_epoch}
timer:heartbeat:{run_id}:{task_key}:{check_epoch}
```

### Cloud Tasks ID (API-compliant)

Hash-based, deterministic, non-sequential:

```
d_{base32(sha256(internal_id))[0..26]}   # dispatch
t_{base32(sha256(internal_id))[0..26]}   # timer
```

### Properties

| Property | Internal ID | Cloud Tasks ID |
|----------|-------------|----------------|
| Readable | Yes | No (opaque hash) |
| Valid for API | No (colons) | Yes |
| Deterministic | Yes | Yes |
| Collision risk | None | SHA-256: ~1 in 2^128 |
| Sequential prefix | Yes | No (hash distributes) |

### Implementation

```rust
use sha2::{Sha256, Digest};

/// Generate Cloud Tasks-compliant ID from internal ID.
///
/// The result is deterministic: same internal_id always produces the same
/// Cloud Tasks ID. This enables idempotent dispatch operations.
pub fn cloud_task_id(kind: &str, internal_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(internal_id.as_bytes());
    let hash = hasher.finalize();

    // Base32 encode for URL-safe characters
    // Take 26 chars = 130 bits of entropy (plenty for uniqueness)
    let encoded = base32::encode(
        base32::Alphabet::RFC4648 { padding: false },
        &hash,
    );

    format!("{}_{}", kind, &encoded[..26].to_lowercase())
}

// Examples:
// cloud_task_id("d", "dispatch:run1:extract:1")
//   -> "d_jbswy3dpehpk3pxp4xlx7lq2..."
//
// cloud_task_id("t", "timer:retry:run1:extract:1:1705340400")
//   -> "t_krsxg5baij2w4ylnmuqho4te..."
```

### Schema Impact

Both `dispatch_outbox.parquet` and `timers.parquet` store BOTH identifiers:

```
dispatch_outbox.parquet:
  dispatch_id: STRING      # "dispatch:{run_id}:{task_key}:{attempt}"
  cloud_task_id: STRING    # "d_{hash}"

timers.parquet:
  timer_id: STRING         # "timer:retry:{run_id}:{task_key}:{attempt}:{epoch}"
  cloud_task_id: STRING    # "t_{hash}"
```

This allows:
1. **Debugging**: Query Parquet by human-readable `dispatch_id`
2. **Correlation**: Find Cloud Tasks logs by `cloud_task_id`
3. **Idempotency**: Same `dispatch_id` always produces same `cloud_task_id`

## Consequences

### Positive

- Internal IDs remain human-readable for debugging and Parquet analysis
- Cloud Tasks IDs are opaque but deterministic
- No collision risk from character replacement
- Non-sequential prefixes avoid Cloud Tasks latency issues
- Existing `sanitize_task_id` can be deprecated (backward compatible)

### Negative

- Slightly more complex schema (two ID columns)
- Cannot directly look up Cloud Tasks by internal ID (need hash)
- 26 extra characters per row in Parquet

### Migration

The existing `sanitize_task_id()` function can continue to work alongside
the new hash-based approach. New orchestration code should use hash-based IDs
exclusively.

### Implementation Note

Cloud Tasks IDs are generated using base32-encoded SHA-256 per this ADR. The
legacy `sanitize_task_id()` path is deprecated and should be removed once
all callers migrate to hash-based IDs.

## Deduplication Window Clarification

Cloud Tasks deduplication window is **up to 24 hours** for Cloud Tasks HTTP targets
(not 9 days, which applies to legacy App Engine queue.yaml queues). After 24 hours,
the same task name can be reused.

For orchestration, this is acceptable because:
1. Task attempts include unique `attempt_id` (ULID)
2. Retry timers include epoch timestamp in ID
3. Tasks completing successfully are removed from outbox

## Related ADRs

- ADR-017: Cloud Tasks Dispatcher (existing implementation)
- ADR-020: Orchestration as Unified Domain (schema context)

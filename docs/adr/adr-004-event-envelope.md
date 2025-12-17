# ADR-004: Event Envelope Format and Evolution

## Status
Accepted

## Context
Tier-2 (eventual consistency) operations write events to an append-only ledger before compaction. These events must:
- Be safely deduplicated during compaction
- Support schema evolution without breaking existing readers
- Enable tracing and debugging in production
- Carry enough metadata for quarantine/retry decisions

Writing ad-hoc JSON without a versioned envelope makes evolution dangerous and debugging difficult.

## Decision

### Canonical Event Envelope

All Tier-2 events use this envelope structure:

```rust
/// Canonical envelope for all Tier-2 ledger events.
pub struct CatalogEvent<T> {
    /// Event type discriminator (e.g., "materialization.completed")
    pub event_type: String,

    /// Schema version for this event type (enables evolution)
    pub event_version: u32,

    /// Idempotency key for deduplication (client-provided or generated)
    pub idempotency_key: String,

    /// When the event occurred (source timestamp)
    pub occurred_at: DateTime<Utc>,

    /// Service/component that produced the event
    pub source: String,

    /// Correlation ID for request tracing
    pub trace_id: Option<String>,

    /// Optional monotonic position for ordering (assigned at ingest)
    pub sequence_position: Option<u64>,

    /// The actual event payload
    pub payload: T,
}
```

### Proto Definition

```protobuf
message CatalogEvent {
  string event_type = 1;
  uint32 event_version = 2;
  string idempotency_key = 3;
  google.protobuf.Timestamp occurred_at = 4;
  string source = 5;
  optional string trace_id = 6;
  optional uint64 sequence_position = 7;
  google.protobuf.Any payload = 8;
}
```

### Required Fields

| Field | Required | Purpose |
|-------|----------|---------|
| `event_type` | Yes | Routing and schema selection |
| `event_version` | Yes | Schema evolution gates |
| `idempotency_key` | Yes | Deduplication during compaction |
| `occurred_at` | Yes | Ordering fallback, debugging |
| `source` | Yes | Origin tracking, debugging |
| `trace_id` | No | Request correlation |
| `sequence_position` | No | Monotonic ordering (clock-skew resistant) |
| `payload` | Yes | Actual event data |

### Evolution Strategy

**Additive-only with version gates:**

1. New optional fields can be added to payloads at any time
2. Breaking changes require new `event_type` or `event_version` bump
3. Compactor handles unknown `event_version` per policy:
   - **Reject**: Fail compaction if unknown version encountered
   - **Quarantine**: Move to dead-letter path for manual review
   - **Fallback**: Use default/empty values for unknown fields

### Idempotency Key Generation

If client doesn't provide an idempotency key, generate one deterministically:

```rust
fn generate_idempotency_key(event: &impl Serialize) -> String {
    let content_hash = sha256(canonical_json(event));
    format!("auto:{}", hex::encode(&content_hash[..16]))
}
```

This ensures same event content produces same key across retries.

### Ordering Guarantees

1. **Primary**: Use `sequence_position` if present (monotonic, clock-skew resistant)
2. **Fallback**: Use event file name (ULID-based, lexicographically sortable)
3. **Last resort**: Use `occurred_at` (vulnerable to clock skew)

The compactor should use `sequence_position` once ingestion assigns monotonic positions to all events.

## Consequences

### Positive
- Safe schema evolution with version gates
- Reliable deduplication via idempotency keys
- Production debugging via trace_id and source
- Ordering resilient to clock skew (when sequence_position used)

### Negative
- Envelope overhead on every event (~100 bytes)
- Client must populate required fields
- Compactor must handle version mismatch cases

### Conformance

CI validates:
1. `EventWriter` wraps all payloads in `CatalogEvent` envelope
2. Required fields are non-empty
3. Compactor tests include unknown version handling

# ADR-013: ID Type Wire Formats

## Status
Accepted

## Context
Arco uses multiple ID types with different semantics.

Some IDs must be **stable across renames** (entity identity), while others must be **lexicographically sortable** (ordered logs, watermarks, file naming).

The canonical implementation is Rust (`crates/arco-core/src/id.rs`). This ADR documents the wire formats that must be consistent across Rust, Python, and proto.

## Decision
Use different wire formats based on ID semantics:

- **`AssetId`**: UUID string (generated as UUIDv7 in the canonical implementation)
- **`RunId`, `TaskId`, `MaterializationId`, `EventId`**: ULID string

### Wire Format
All IDs are represented as strings on the wire (proto wraps them as `message FooId { string value = 1; }`).

#### UUID (AssetId)
- Canonical representation: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` (36 chars, lowercase hex)
- Example:
  - `01930a0b-1234-7abc-9def-0123456789ab`

#### ULID (RunId/TaskId/MaterializationId/EventId)
- Canonical representation: Crockford Base32, 26 characters, uppercase alphanumeric
- Example:
  - `01ARZ3NDEKTSV4RRFFQ69G5FAV`

### Generation
- `AssetId` generation uses UUIDv7 (RFC 9562) in Rust via `Uuid::now_v7()`.
- ULID-based IDs use monotonic generation where available.

### Validation
- `AssetId` values must parse as UUID strings.
- ULID-based values must parse as ULIDs.

## Consequences
### Positive
- Aligns docs/proto with the canonical implementation.
- Preserves both stable identity (UUID) and sortable log identity (ULID).

### Negative
- Two different ID formats must be maintained across languages.

# ADR-002: ID Strategy by Entity Type

## Status
Accepted

## Context
Arco requires unique identifiers for various entity types. Different use cases have conflicting requirements:
- **Stability**: Entity IDs used in lineage graphs must remain stable across renames
- **Sortability**: Event and version IDs must support efficient time-ordered queries and watermarking
- **Uniqueness**: All IDs must be globally unique without coordination

The Technical Vision mandates "stable UUIDs for lineage integrity," but operational systems benefit from sortable, time-ordered identifiers.

## Decision

Use different ID formats based on entity type requirements:

| Entity Type | ID Format | Rationale |
|-------------|-----------|-----------|
| Namespace, Table, Asset, Column | UUID v7 | Stability across renames; time-sortable but identity-stable |
| Event, Materialization | ULID | Lexicographic sortability for compaction and watermarking |
| Commit, Version | ULID | Time-ordered for audit chain and incremental reads |

### Implementation

1. **Stable Entity IDs (UUID v7)**
   - Used for: `namespace_id`, `table_id`, `asset_id`, `column_id`
   - Generated once at creation, never changes
   - Allows entity to be renamed without breaking lineage references
   - UUID v7 provides time-ordering for creation date queries

2. **Ordered Log IDs (ULID)**
   - Used for: `event_id`, `materialization_id`, `commit_id`, `version_id`
   - Lexicographic ordering enables efficient range queries
   - Supports watermark-based compaction (compare `event_id > watermark`)
   - 128-bit like UUID but with millisecond precision ordering

### Proto Representation

All IDs are represented as `string` in proto for maximum compatibility:

```protobuf
message AssetId {
  string value = 1;  // UUID v7 format: "01930a0b-1234-7abc-..."
}

message EventId {
  string value = 1;  // ULID format: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
}
```

### Validation Rules

- UUID v7 IDs: Must parse as valid UUID
- ULID IDs: Must be exactly 26 uppercase alphanumeric characters
- All IDs: Must be URL-safe (no encoding required)

## Consequences

### Positive
- Lineage integrity preserved across entity renames
- Efficient time-range queries on events and versions
- Watermark comparison uses simple string comparison
- Clear separation of identity vs ordering concerns

### Negative
- Two ID formats to maintain in codebase
- Developers must use correct ID type for each entity
- Migration required if existing code uses inconsistent formats

### Conformance

CI validates:
1. Entity ID fields use `arco_core::id::AssetId` (UUID-backed)
2. Event ID fields use `arco_core::id::EventId` or ULID
3. No UUID/ULID type conflicts in proto definitions

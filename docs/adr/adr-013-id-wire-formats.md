# ADR-013: ID Type Wire Formats

## Status
Accepted

## Context
Arco uses multiple ID types with different semantics:
- `RunId`: Execution run identifier (time-ordered for querying)
- `TaskId`: Individual task execution identifier
- `AssetId`: Stable asset identifier (random for even distribution)
- `TenantId`, `WorkspaceId`: Organizational identifiers

These IDs need to be:
- Globally unique without coordination
- Sortable (for some types)
- Compact in storage and logs
- Consistent across Rust and Python

## Decision
Use ULID (Universally Unique Lexicographically Sortable Identifier) as the primary ID format:

### ULID Properties
- **128-bit**: Same size as UUID
- **Lexicographically sortable**: First 48 bits are timestamp
- **Monotonic**: IDs generated in same millisecond are still ordered
- **Base32 Crockford**: 26 characters, case-insensitive, no ambiguous characters

### Wire Format
All IDs serialize as lowercase Crockford Base32 strings:
```
run_01HGW5N7XHJM4GX29KZRXQP6N8
task_01HGW5N7XHJM4GX29KZRXQP6N9
asset_01HGW5N7XHJM4GX29KZRXQP6NA
```

### ID Type Prefixes
Each ID type uses a distinct prefix for debuggability:
- `run_` for `RunId`
- `task_` for `TaskId`
- `asset_` for `AssetId`
- `tenant_` for `TenantId`
- `ws_` for `WorkspaceId`
- `part_` for `PartitionId` (derived, not ULID)

### Generation
IDs are generated using the `ulid` crate with default monotonic generation.

## Consequences

### Positive
- Time-ordered IDs enable efficient range queries
- Prefixes make logs and debugging easier
- No coordination required for uniqueness
- Compact representation (26 chars vs 36 for UUID)

### Negative
- 48-bit timestamp overflows in year 10889 (acceptable)
- Monotonic generation needs thread-local state
- Slightly less random than UUIDv4 (timestamp bits are predictable)
- Python needs `python-ulid` package for compatibility

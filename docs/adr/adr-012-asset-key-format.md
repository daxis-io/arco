# ADR-012: AssetKey Canonical String Format

## Status
Accepted

## Context
Arco needs a canonical string representation for asset identifiers (`AssetKey`) that:
- Is deterministic for fingerprinting and hashing
- Can be used in storage paths
- Is human-readable for debugging
- Clearly separates namespace and name components

Historical code used `.` as a separator (`namespace.name`), which conflicts with common naming patterns (e.g., `staging.users.daily`).

## Decision
Use `/` as the canonical separator for `AssetKey`:

```text
canonical_string = namespace "/" name
```

### Format Details
- **Separator**: Forward slash (`/`) for canonical strings
- **Display format**: Dot (`.`) preserved for backward compatibility in logs
- **Namespace**: lowercase alphanumeric with underscores (`raw`, `staging`, `mart`)
- **Name**: lowercase alphanumeric with underscores and dots (`users`, `events.daily`)

### Examples
```rust
let key = AssetKey::new("raw", "events");
assert_eq!(key.canonical_string(), "raw/events");
assert_eq!(key.to_string(), "raw.events");  // Display format
```

### Usage in TaskKey
`TaskKey` uses the canonical string format for deterministic task identification:
```
{asset_key.canonical_string()}/{partition_key.canonical_string()}/{operation}
```

## Consequences

### Positive
- Clear visual separation between namespace and name
- Compatible with storage path conventions (GCS, S3)
- Dots in asset names don't cause parsing ambiguity
- Deterministic for fingerprinting

### Negative
- Two string representations (canonical vs display) may cause confusion
- Existing logs/dashboards using `.` format need mental translation
- Cannot use `/` in namespace or name (must validate)

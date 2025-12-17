# ADR-005: Canonical Storage Layout

## Status
Accepted

## Context
Arco stores all metadata on object storage (S3, GCS, Azure Blob). Path conventions must be:
- Self-documenting (grep-friendly key=value segments)
- Consistent across all writers
- Supportive of multi-tenant isolation
- Compatible with bucket lifecycle policies

Hardcoded paths scattered across codebase lead to divergence and bugs.

## Decision

### Canonical Layout

```text
tenant={tenant}/workspace={workspace}/
├── manifests/
│   ├── root.manifest.json           # Entry point - points to domain manifests
│   ├── catalog.manifest.json        # Tier-1: namespaces/tables/columns
│   ├── lineage.manifest.json        # Tier-1: lineage edges
│   ├── executions.manifest.json     # Tier-2: run/task execution state
│   └── search.manifest.json         # Tier-1: token postings
├── locks/
│   ├── catalog.lock.json            # Distributed lock per domain
│   ├── lineage.lock.json
│   ├── executions.lock.json
│   └── search.lock.json
├── commits/
│   └── {domain}/
│       └── {commit_id}.json         # Per-domain commit chain (audit trail)
├── snapshots/
│   ├── catalog/
│   │   └── v{version}/
│   │       ├── namespaces.parquet   # Tier-1 Parquet snapshots
│   │       ├── tables.parquet
│   │       └── columns.parquet
│   ├── lineage/
│   │   └── v{version}/
│   │       └── lineage_edges.parquet
│   └── search/
│       └── v{version}/
│           └── token_postings.parquet
├── ledger/
│   └── {domain}/
│       └── {event_id}.json          # Tier-2 append-only events
└── state/
    └── {domain}/
        └── snapshot_v{version}_{ulid}.parquet  # Tier-2 compacted state
```

### Path Conventions

1. **Tenant/Workspace Prefix**: `tenant={tenant}/workspace={workspace}/`
   - Self-documenting key=value format
   - Grep-friendly for debugging
   - Enables bucket policies per tenant

2. **Domain Separation**: Each domain (`catalog`, `lineage`, `executions`, `search`) has:
   - Its own manifest file
   - Its own lock file
   - Its own commit chain
   - Its own snapshots/ledger/state directories

3. **Version Directories**: `v{version}/` for atomic visibility
   - Write all files to version directory
   - Update manifest only after all files written
   - Old versions retained for rollback

4. **Event IDs**: ULID-based for lexicographic ordering
   - Enables efficient range queries
   - Supports watermark-based compaction

### Single Path Module

All paths MUST be constructed through `arco_catalog::paths`:

```rust
pub mod paths {
    pub const ROOT_MANIFEST: &str = "manifests/root.manifest.json";

    pub fn domain_manifest(domain: &str) -> String {
        format!("manifests/{domain}.manifest.json")
    }

    pub fn domain_lock(domain: &str) -> String {
        format!("locks/{domain}.lock.json")
    }

    pub fn snapshot_dir(domain: &str, version: u64) -> String {
        format!("snapshots/{domain}/v{version}/")
    }

    pub fn ledger_event(domain: &str, event_id: &str) -> String {
        format!("ledger/{domain}/{event_id}.json")
    }

    pub fn state_snapshot(domain: &str, version: u64, ulid: &str) -> String {
        format!("state/{domain}/snapshot_v{version}_{ulid}.parquet")
    }
}
```

### Path Validation Rules

1. No hardcoded path strings outside `paths` module
2. All paths are relative to tenant/workspace root
3. No path traversal (`..`) allowed
4. Domain names are lowercase alphanumeric only

## Consequences

### Positive
- Single source of truth for all paths
- Self-documenting layout (grep for `tenant=acme`)
- Easy bucket lifecycle rules (prefix-based)
- Clear domain isolation

### Negative
- Migration required from existing layouts
- Path module must be updated for new features
- Slightly longer paths than minimal

### Conformance

CI validates:
1. All writers use `paths` module functions
2. No hardcoded path strings in production code
3. Unit tests assert path format consistency
4. Integration tests verify actual writes match expected paths

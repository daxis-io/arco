# ADR-003: Manifest Domain Names and Contention Strategy

## Status
Accepted

## Context
The catalog manages multiple types of metadata with different update frequencies:
- **DDL operations** (namespaces, tables): Low frequency, strong consistency required
- **Lineage edges**: Medium frequency (per-execution), written as tasks complete
- **Execution state**: High frequency (Tier-2), compacted from event ledger
- **Search index**: Low frequency, rebuilt periodically

Using a single manifest creates lock contention: lineage writes happening "as tasks execute" would block catalog DDL operations. This violates the architectural principle of domain isolation.

## Decision

### Four Domain Manifests

Split catalog state into four independent domains, each with its own manifest and lock:

| Domain | Manifest | Lock | Update Rate | Contents |
|--------|----------|------|-------------|----------|
| catalog | `catalog.manifest.json` | `catalog.lock.json` | Low (DDL) | Namespaces, tables, columns |
| lineage | `lineage.manifest.json` | `lineage.lock.json` | Medium | Lineage edges, dependency graph |
| executions | `executions.manifest.json` | `executions.lock.json` | High (Tier-2) | Run/task execution state |
| search | `search.manifest.json` | `search.lock.json` | Low (rebuild) | Token postings index |

### Root Manifest as Entry Point

The `root.manifest.json` is the **only stable contract** for external readers:

```json
{
  "version": 1,
  "catalog_manifest_path": "manifests/catalog.manifest.json",
  "lineage_manifest_path": "manifests/lineage.manifest.json",
  "executions_manifest_path": "manifests/executions.manifest.json",
  "search_manifest_path": "manifests/search.manifest.json",
  "updated_at": "2025-01-15T10:00:00Z"
}
```

Readers MUST start from root manifest and follow paths to domain manifests. This decouples filenames from client code.

### Migration Strategy (from current naming)

Current code uses `core`, `execution` (singular), `governance`. Migration approach:

1. **Reader compatibility**: Root manifest loader accepts both old and new domain names
2. **Write-forward**: On next successful write/initialize, emit new canonical names
3. **Migration tool**: `arco-catalog migrate-manifests` (idempotent) converts old layouts
4. **Hardening tests**: Verify both layouts can be read; only new layout is written

### CatalogWriter Internal Structure

`CatalogWriter` uses **separate `Tier1Writer` instances per domain**:

```rust
pub struct CatalogWriter {
    catalog_tier1: Tier1Writer,   // catalog domain
    lineage_tier1: Tier1Writer,   // lineage domain
    // executions uses EventWriter (Tier-2)
    // search uses Tier1Writer
}
```

This ensures:
- Lineage writes don't contend with catalog DDL
- Each domain can be updated independently
- Lock acquisition is scoped to affected domain only

## Consequences

### Positive
- No lock contention between domains
- Lineage writes proceed independently of catalog DDL
- Readers can load only domains they need
- Clear ownership boundaries for each domain

### Negative
- More files to manage per workspace
- Migration complexity for existing deployments
- Must track consistency across domains if cross-domain operations are needed

### Naming Note

Previous implementation used different names (`core`, `execution`, `governance`). The canonical names in this ADR align with the Technical Vision documentation. Migration preserves backward compatibility while standardizing forward.

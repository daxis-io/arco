# Olympia-Inspired Arco Strategy

**Date:** 2026-06-20

## Thesis

Olympia and Arco target the same lakehouse control-plane pain, but they operate
at different layers.

Olympia defines a storage-only catalog format. Its strongest ideas are
catalog-wide versioning, a concrete storage layout, transaction handles,
snapshot export, and engine-facing transaction ergonomics.

Arco defines a broader file-native control plane. It owns catalog DDL,
metastore state, lineage, orchestration state, system-table projections, Delta
coordination, tenant/workspace isolation, compatibility APIs, and governance
surfaces.

Arco should not copy Olympia's file layout or collapse into a single catalog
root. Arco should use Olympia as a reference design for the parts of the product
that need a clearer external contract: storage format documentation,
workspace-consistent snapshots, rollback/export, durable transaction handles,
and conflict-aware publication.

## External Reference Boundary

Use Olympia as a design reference, not as a production baseline. The project is
young, narrow, and early in its implementation life. Its docs give Arco a useful
example of a crisp storage-format story, but Arco should validate every borrowed
idea against its own invariants before adding it to the roadmap.

That means:

1. Borrow the external contract shape, not the exact B-tree layout.
2. Borrow the transaction UX, not unqualified distributed database claims.
3. Borrow rollback/export semantics, not a hot-path global root.
4. Borrow conflict-analysis concepts, not Olympia's current implementation.

## Current Arco Boundary

Arco's current authority boundary is domain-scoped fenced pointer publication:

1. Writers append immutable events or artifacts.
2. The compactor writes immutable Parquet/JSON state for the affected domain.
3. A fenced pointer CAS makes the new manifest visible.
4. Ordinary readers start from the domain pointer and manifest.
5. Root-token readers start from a transaction record and immutable
   super-manifest.

This boundary matters. The pointer CAS, not the immutable artifact write, is the
visibility point. Listing stays out of correctness paths. Parquet projections
remain the queryable state format. Compactor-owned publication preserves the IAM
separation that Arco needs as a control-plane service.

The strategy below keeps those properties intact.

## Strategy

Arco should borrow Olympia's crisp external semantics, then express them through
Arco's existing primitives:

| Olympia strength | Arco-native expression |
|---|---|
| Storage-only format spec | `Arco Storage Format v0` over tenant/workspace/domain layouts, manifests, pointers, root transactions, retention, and failure states |
| Whole-catalog versioning | Workspace snapshots backed by root transaction super-manifests |
| Transaction handles | Resumable control-plane transactions with TTL, prepare, commit, abort, and read tokens |
| Engine-friendly transaction UX | `arco tx` CLI and optional SQL/command endpoint ergonomics |
| Object-key to definition lookup | Derived point-lookup indexes referenced by manifests |
| Snapshot export | Export manifests with relative paths, retention metadata, and relocation rules |
| Action-based conflict checks | Read/write-set summaries and action receipts for optimistic retries |

The design principle is narrow: keep domain-local publication as the hot path;
create cross-domain cuts only when callers ask for them.

## Priorities

### P0: Publish An External Storage Format Spec

Arco has ADRs, plans, protobufs, release notes, tests, and implementation
evidence. A new integrator still has to read too many files to understand the
stable storage contract.

Create `docs/spec/arco-storage-format-v0.md` with this scope:

1. Concepts: tenant, workspace, domain, ledger, snapshot, manifest, pointer,
   transaction, root token, read token.
2. Object-store requirements: conditional create, CAS precondition, strong
   read-after-write for new objects, no listing for correctness.
3. Canonical layout: `tenant={tenant}/workspace={workspace}/...`.
4. Domain publication protocol: append, compact, write immutable manifest,
   pointer CAS, visibility.
5. Root transaction protocol: transaction record, super-manifest, root read
   token, pinned reads.
6. Retention and GC: visible, pinned, orphaned, expired, repairable.
7. Compatibility: additive schema changes, layout versioning, migration rules.
8. Failure states: CAS loss, stale fencing, orphan artifact, partial compactor
   failure, expired transaction handle, stale pointer read.

This spec should not try to document every future governance object. It should
document the durable publication contract first.

### P1: Productize Workspace Snapshots

Olympia's cleanest product story is catalog time travel, rollback, and export.
Arco can offer the same user promise without adding a moving global workspace
head.

Add a first-class workspace snapshot primitive:

```text
WorkspaceSnapshot {
  snapshot_id
  created_at
  retention_policy
  domains: {
    catalog: manifest_path + manifest_id + commit_id
    lineage: manifest_path + manifest_id + commit_id
    orchestration: manifest_path + manifest_id + commit_id
    search: manifest_path + manifest_id + commit_id
  }
  export_policy
  parent_snapshot_id?
}
```

Expose it through a service shaped like:

```text
WorkspaceSnapshotService
  CreateWorkspaceSnapshot
  GetWorkspaceSnapshot
  ExportWorkspaceSnapshot
  RollForwardRollbackWorkspaceSnapshot
```

Rollback should commit a new visible state that points to the selected historical
cut. It should not mutate old snapshots.

### P2: Add Resumable Control-Plane Transaction Handles

Arco already has transaction records, receipts, fencing tokens, root read tokens,
and domain publication. The missing product surface is a transaction handle that
can span processes and human review steps.

Start with a scoped control-plane transaction model:

```text
transactions/control_plane/{tx_id}.json
transactions/control_plane/{tx_id}/mutations/{mutation_id}.json
transactions/control_plane/{tx_id}.manifest.json
```

States:

```text
OPEN
PREPARED
VISIBLE
ABORTED
EXPIRED
```

Initial workflows:

1. A writer begins a transaction and stages catalog or governance changes.
2. An auditor reads the transaction token and validates the staged result.
3. A publisher commits the transaction or aborts it.
4. Readers use the issued read token for pinned validation.

This should be framed as resumable control-plane transactions, not general
distributed database transactions.

### P3: Make Transactions Human-Usable

Olympia's Spark transaction syntax is memorable. Arco should expose equivalent
ergonomics through an Arco-native surface.

CLI:

```text
arco tx begin --workspace prod --isolation serializable
arco tx apply catalog-ddl create_table.json
arco tx apply orchestration batch.json
arco tx prepare
arco tx commit
```

Optional SQL or command endpoint:

```sql
BEGIN ARCO TRANSACTION ISOLATION SERIALIZABLE;
CALL arco.apply_catalog_ddl(...);
CALL arco.prepare_transaction();
CALL arco.commit_transaction();
```

Keep `system.*` read-only. Transaction commands can live in a command endpoint
or CLI even if DataFusion continues to serve only read queries.

### P4: Add Conflict Summaries For Optimistic Retry

Arco's current publication path is conservative and safe: lock, append explicit
events, compact, pointer CAS. Over time, Arco can reduce false conflicts by
storing explicit read/write sets and action summaries in transaction receipts.

Example:

```json
{
  "tx_id": "01J...",
  "domain": "catalog",
  "read_set": ["catalog.schema.table_a"],
  "write_set": ["catalog.schema.table_b"],
  "actions": [
    {"op": "CREATE_TABLE", "object": "catalog.schema.table_b"}
  ],
  "parent_manifest_hash": "...",
  "result_manifest_hash": "..."
}
```

On CAS loss, Arco can distinguish a true conflicting write from an independent
write that can be replayed on the new head. This borrows Olympia's
conflict-analysis idea while preserving Arco's locks, fencing, compactor, and
pointer-CAS visibility boundary.

### P5: Add Derived Catalog Point-Lookup Indexes

Arco's Parquet snapshots serve SQL and system-table workflows well. Point
lookups need a narrower path:

1. Resolve object by fully qualified name.
2. Resolve object by stable ID.
3. Resolve active storage location.
4. Resolve active table-format contract.

Add manifest-referenced derived indexes:

```text
snapshots/catalog/v123/tables.parquet
indexes/catalog/v123/by_fqn.arrow
indexes/catalog/v123/by_stable_id.arrow
indexes/catalog/v123/by_storage_location.arrow
manifests/catalog/00000000000000000123.json
```

The ledger and manifest-published snapshot remain authoritative. Indexes are
derived acceleration structures.

#### Why Not A B-Tree As Authority?

A B-tree is a good fit for sorted point lookup. It can resolve object keys in a
few reads, support namespace range scans, and give each root a clear catalog
version. Those properties explain why Olympia's format is easy to describe.

Arco's authoritative state has different pressure points:

1. A B-tree would add write amplification. One logical catalog update can
   rewrite a leaf, parent nodes, and a new root.
2. A tree root would create extra contention if every catalog, lineage,
   orchestration, or governance write had to publish through the same structure.
3. Object stores make random node reads expensive. S3 and GCS reward larger
   immutable artifacts, caching, and manifest-driven reads more than many small
   page fetches.
4. Copy-on-write trees leave old nodes behind. Retention, workspace snapshots,
   rollback, and export would need stricter GC rules for tree nodes and orphaned
   branches.
5. A B-tree does not serve analytical metadata queries as well as Parquet
   projections with column pruning, statistics, and DataFusion access.
6. Tree split, merge, rebase, and corruption-check logic would become part of
   Arco's public storage contract.

Arco should use B-tree-like indexes only as derived lookup aids. The
authoritative publication path should stay ledger, compacted Parquet/JSON
artifacts, immutable manifest, and fenced pointer CAS.

### P6: Add Exportable Workspace Snapshot Manifests

Export should become a first-class product contract:

```json
{
  "snapshot_id": "01J...",
  "format": "arco.workspace_snapshot.v1",
  "root_prefix": "tenant=acme/workspace=prod/",
  "required_objects": [
    "manifests/catalog/...",
    "snapshots/catalog/v123/tables.parquet",
    "state/orchestration/manifests/..."
  ],
  "retention_until": "2026-07-20T00:00:00Z",
  "relocation": {
    "paths_are_relative": true,
    "rewrite_required": false
  }
}
```

This supports backup, migration, environment promotion, reproducibility, and
disaster recovery.

## Non-Goals

### Do Not Add A Hot-Path Global Workspace Root

Arco split domains to avoid unnecessary contention. Catalog DDL, lineage,
orchestration, search, and governance have different write rates and consistency
requirements. Workspace snapshots should pin cross-domain cuts on demand.

### Do Not Replace Parquet Projections With A B-Tree

Queryable metadata is part of Arco's product identity. Keep Parquet projections
for `system.*` and analytical access. Add point-lookup indexes only where they
reduce latency or object-store reads.

### Do Not Weaken Compactor/IAM Separation

The API should not gain broad write authority over `snapshots/`, `state/`, or
`manifests/` to mimic an embedded SDK. Arco's service boundary needs IAM-backed
sole-writer rules.

### Do Not Treat System Tables As A Write Surface

System tables should expose published state, snapshot records, transaction
receipts, and audit projections. They should not become the correctness path for
authorization, transactions, or mutation commands.

## Roadmap Issues

1. Add `docs/spec/arco-storage-format-v0.md`.
2. Add `docs/spec/object-store-contract.md` with backend conformance matrix.
3. Add failure-mode conformance tests for stale fencing, lost CAS, duplicate
   retries, orphan snapshots, compactor timeout, expired transaction handles,
   and snapshot retention.
4. Add `WorkspaceSnapshotService`.
5. Add workspace snapshot export manifests.
6. Add roll-forward rollback for workspace snapshots.
7. Add resumable control-plane transaction handles.
8. Add `arco tx` CLI commands.
9. Expose `system.catalog.snapshots` and `system.catalog.transactions` as
   read-only system tables.
10. Add read/write-set and action-summary records to transaction receipts.
11. Add derived point-lookup indexes for FQN, stable ID, and storage location.

## Acceptance Criteria

This strategy succeeds when Arco can make these claims without relying on
private ADR context:

1. An external reader can implement an Arco storage reader from the format spec.
2. Operators can create, retain, export, and roll forward to workspace snapshots.
3. A multi-step governance or migration workflow can stage changes, expose a
   review token, and commit or abort later.
4. Ordinary domain reads stay pointer-first and avoid listing.
5. Root-token reads stay pinned to immutable super-manifests.
6. System tables remain derived, read-only projections.
7. The compactor remains the sole writer for published state artifacts.

## References

- [Olympia format](https://olympiaformat.org/)
- [Olympia repository](https://github.com/olympiaformat/olympia)
- [ADR-003: Manifest Domain Names and Contention Strategy](../adr/adr-003-manifest-domains.md)
- [ADR-018: Tier-1 Write Path Architecture](../adr/adr-018-tier1-write-path.md)
- [ADR-034: Fenced Head-Published Control-Plane Transactions](../adr/adr-034-fenced-head-published-control-plane-transactions.md)
- [Control-Plane Transactions Implementation Plan](2026-03-30-control-plane-transactions.md)
- [Control-Plane Scope](../guide/src/reference/control-plane-scope.md)

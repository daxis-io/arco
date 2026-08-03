# Olympia-Inspired Arco Strategy

**Date:** 2026-06-20

**Related planning:**

- [Arco Tier-1 Single Authority Vision](2026-06-26-arco-tier1-single-authority-combined-vision.md)
- [Arco Tier-1 Control Store Strategy](2026-06-25-arco-tier1-control-store-strategy.md)

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

## Decision

Arco will keep object-store-backed fenced pointer publication as the visibility
primitive. The current ledger plus synchronous compactor path remains the
baseline. If the Tier-1 control-store prototype satisfies its correctness,
latency, provider-CAS, replay, projection, authorization-freshness, and recovery
gates, final Tier-1 catalog and governance domains should publish a
control-store manifest as the authority root.

Olympia-inspired work should make Arco's control-plane state easier to read,
snapshot, export, restore, audit, and operate. It should not introduce a second
permanent Tier-1 authority path.

The hierarchy is:

1. Authority root: the CAS-protected object-store pointer that selects visible
   authority for a scope. Today this can be a domain manifest. In the target
   control-store model, this is a control-store manifest.
2. State token or checkpoint: retained pin over an authority root and logical
   sequence.
3. Workspace snapshot or root token: immutable cross-domain/read-time pin over
   already-visible authority heads and projection watermarks.
4. Export manifest: portable package of a retained workspace snapshot and all
   reachable authority, projection, event, and retention artifacts.
5. System tables: read-only, watermarked Parquet projections over committed
   records.

No other object should decide Tier-1 mutation visibility.

## Relationship To Tier-1 Control Store Strategy

This document defines the product and external-contract layer inspired by
Olympia. It does not define the final Tier-1 mutation substrate by itself.

The companion [Arco Tier-1 Control Store Strategy](2026-06-25-arco-tier1-control-store-strategy.md)
defines the candidate internal authority path: `ArcoStateTxn`, state tokens,
control-store manifests, object-store provider CAS, checkpoints, replay
equivalence, projection watermarks, and migration from synchronous compaction.
That strategy is approved as prototype strategy, not as an accepted ADR.

The umbrella [Arco Tier-1 Single Authority Vision](2026-06-26-arco-tier1-single-authority-combined-vision.md)
records the combined direction and the wording expected for a later ADR.

The combined vision is:

```text
control-store authority, if validated
  + Olympia-inspired snapshots/export/transaction UX/storage contract
  + Parquet system-table projections
```

The old ledger plus synchronous compactor path may be adapted behind
`ArcoStateStore` during migration. It must not become a second permanent Tier-1
write path next to the control store.

## External Reference Boundary

Use Olympia as a design reference, not as a production baseline. The project is
young, narrow, and early in its implementation life. Its docs give Arco a useful
example of a crisp storage-format story, but Arco should validate every borrowed
idea against its own invariants before adding it to the roadmap.

That means:

1. Borrow the external contract shape, not the exact B-tree layout.
2. Borrow the transaction UX, not unqualified distributed database claims.
3. Borrow rollback/export semantics through pinned workspace snapshots, not
   through a hot-path moving global root.
4. Borrow conflict-analysis concepts, not Olympia's current implementation.

## Current Arco Boundary

Arco's current Tier-1 authority boundary is ledger append plus synchronous
compactor publication:

1. Writers append immutable events or artifacts.
2. The compactor writes immutable Parquet/JSON state for the affected domain.
3. A fenced pointer CAS makes the new manifest visible.
4. Ordinary readers start from the domain pointer and manifest.
5. Root-token readers start from a transaction record and immutable
   super-manifest.

This baseline established important invariants: object storage is the durable
authority boundary, pointer CAS is the visibility point, listing stays out of
request-time correctness, and public Parquet projections remain queryable.

If the control-store prototype validates the required gates, the synchronous
compactor path becomes a migration baseline for final Tier-1 catalog/governance
mutations, not the target. The target keeps the same object-store and
fenced-publication invariants while moving online Tier-1 authority to the Arco
control store:

```text
API/domain service
  -> ArcoStateTxn
  -> control-store transaction/event record + folded state
  -> fenced control manifest pointer CAS
  -> StateToken
  -> async Parquet projection publication
```

The strategy below keeps those properties intact.

## Strategy

Arco should borrow Olympia's crisp external semantics, then express them through
Arco's existing primitives:

| Olympia strength | Arco-native expression in the combined vision |
|---|---|
| Storage-only format spec | `Arco Storage Format v0` describing object-store provider requirements, control-store authority roots, projection roots, snapshots, exports, retention, and failure states |
| Whole-catalog versioning | Workspace snapshots that pin control-store checkpoints/state tokens plus projection watermarks, not a moving global root |
| Transaction handles | Resumable control-plane transaction handles that stage, review, commit, and abort mutations through `ArcoStateTxn` or higher-level workflow records |
| Engine-friendly transaction UX | `arco tx` CLI and command APIs layered over transaction handles, never writes to `system.*` |
| Object-key lookup | Authoritative point/range reads through control-store keys, with optional derived indexes only for projection/query acceleration |
| Snapshot export | Export manifests that include control manifests, checkpoints, reachable txlog/segments, event archive boundaries, Parquet projections, and relocation metadata |
| Action-based conflict checks | Serializable `ArcoStateTxn` preconditions, read/write/predicate input sets, and transaction receipts that support retry after revalidation and re-authorization |

The design principle is narrow: keep domain-local authority as the hot path.
Create cross-domain cuts only when callers ask for a snapshot, export, root
token, or transaction handle.

## Decision And Sequencing

The work has dependencies. Do not start with CLI syntax, optimistic retries, or
indexes. Start by making the storage contract legible and testable.

### Phase 1: External Contract And Correctness

This phase publishes the contract that every later feature depends on:

1. `docs/spec/arco-storage-format-v0.md`
2. `docs/spec/object-store-contract.md`
3. reader-contract pseudocode and conformance fixtures
4. failure-mode tests for stale fencing, CAS loss, duplicate retry, orphan
   artifacts, stale pointer reads, compactor timeout, expired transaction
   handles, and snapshot retention
5. GC and retention reachability rules
6. read-only projections for snapshot and transaction records when the records
   exist

### Phase 2: Workspace Snapshots

This phase productizes pinned workspace cuts:

1. `WorkspaceSnapshotService`
2. workspace snapshot records
3. `system.catalog.snapshots`
4. export manifests
5. roll-forward restore for domain and workspace snapshots

### Phase 3: Durable Transaction Handles

This phase adds resumable transaction state after the reader, retention, and
snapshot contracts are clear:

1. resumable control-plane transaction records
2. mutation staging objects
3. prepare, commit, abort, and recover state machine
4. `system.catalog.transactions`
5. review-token workflow

### Phase 4: Ergonomics And Optimization

This phase makes the model easier to use and faster:

1. `arco tx` CLI
2. optional SQL-like command endpoint outside the DataFusion read-query surface
3. read/write-set and action-summary receipts
4. optimistic retry after revalidation and re-authorization
5. optional derived point-lookup indexes

## Workstreams

### P0: Publish Arco Storage Format v0 And Conformance Suite

Arco has ADRs, plans, protobufs, release notes, tests, and implementation
evidence. A new integrator still has to read too many files to understand the
stable storage contract.

Create `docs/spec/arco-storage-format-v0.md` with this scope:

1. Concepts: tenant, workspace, domain, authority root, control-store manifest,
   projection manifest, state token, checkpoint token, workspace snapshot,
   transaction, root token, and read token.
2. Object-store requirements: conditional create, CAS precondition, strong
   read-after-write for new objects, no listing for correctness.
3. Canonical layout: `tenant={tenant}/workspace={workspace}/...`.
4. Current-domain publication protocol: append, compact, write immutable
   manifest, pointer CAS, visibility.
5. Target control-store publication protocol: transaction/event record, folded
   state, control-store manifest, pointer CAS, `StateToken`.
6. Root transaction and workspace snapshot protocol: transaction record,
   super-manifest or snapshot record, root read token, pinned reads.
7. Projection protocol: Parquet projection manifests, watermarks, derived
   indexes, and lag/error reporting.
8. Retention and GC: visible, pinned, orphaned, expired, repairable.
9. Compatibility: additive schema changes, layout versioning, migration rules.
10. Failure states: CAS loss, stale fencing, orphan artifact, partial compactor
    failure, expired transaction handle, stale pointer read, stale projection.

This spec should not try to document every future governance object. It should
document the durable publication contract first.

#### Reader Contracts v0

External readers should not need to implement the hot control-store writer
protocol. The public/readable contract is the snapshot, export, projection, and
watermark contract. Internal Arco services use authority readers.

Authority reader:

1. Read at the current authority root or a supplied `StateToken`.
2. Resolve a retained control-store manifest, or a later manifest that covers
   the required logical sequence.
3. Validate scope, logical sequence, manifest hash, and token retention.
4. Read control-store state through `ArcoStateReader.read_at(token)`.
5. Fail closed if enforcement, credential vending, or policy reads are stale or
   missing.
6. Never list object storage for request-time correctness.

Projection reader:

1. Resolve a workspace snapshot, export manifest, or projection root.
2. Read the projection manifest and declared watermark.
3. Validate projection schema version, source authority root, logical sequence,
   artifact hashes, and lag policy.
4. Read only manifest-referenced Parquet/JSON artifacts.
5. Return explicit stale-projection errors when the projection watermark is too
   old for the requested use.

Root-token reader:

1. Resolve the transaction record from the read token.
2. Require `status = VISIBLE`.
3. Read the immutable super-manifest or workspace snapshot record.
4. Resolve referenced authority heads and projection watermarks directly.
5. Read only artifacts reachable from those records.

#### Conformance Acceptance Bar

A third-party reader should be able to:

1. Resolve a tenant/workspace root.
2. Resolve an authority root or projection root according to its role.
3. Load an immutable control-store or projection manifest.
4. Read manifest-selected Parquet/JSON artifacts when using the projection
   contract.
5. Resolve a root read token or `StateToken`.
6. Load a pinned super-manifest or workspace snapshot record.
7. Avoid listing for correctness.
8. Reject expired or unsupported layout versions.
9. Detect stale projection watermarks.

The conformance suite should include object-store provider conformance for
authority writes, internal authority-reader conformance for token-pinned reads,
and external projection-reader conformance for snapshots, exports, and Parquet
system-table watermarks.

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
  scope
  authority_heads: {
    catalog: ControlStoreCheckpointRef {
      control_root
      manifest_id
      logical_sequence
      checkpoint_id?
      min_retained_until
      manifest_hash
    }
    access: ControlStoreCheckpointRef { ... }
    storage: ControlStoreCheckpointRef { ... }
  }
  projection_watermarks: {
    system.catalog: logical_sequence
    system.access: logical_sequence
    system.storage: logical_sequence
    system.audit: logical_sequence
  }
  event_archive_boundaries: {
    catalog: sequence_range
    access: sequence_range
    storage: sequence_range
  }
  export_policy
  parent_snapshot_id?
}
```

Historical snapshots may still point at Parquet/JSON manifests from the current
path. Treat that as migration and backward compatibility, not the final
authoring model for new Tier-1 snapshots after a domain moves to the control
store.

Expose it through a service shaped like:

```text
WorkspaceSnapshotService
  CreateWorkspaceSnapshot
  GetWorkspaceSnapshot
  ExportWorkspaceSnapshot
  RestoreDomainToSnapshot
  RestoreWorkspaceToSnapshot
```

Restore is a roll-forward transaction. It publishes new visible domain heads or
a new root token that references artifacts from the selected historical cut,
subject to retention and compatibility checks. It does not mutate old snapshots.

Domain restore and workspace restore need separate APIs:

```text
RestoreDomainToSnapshot(domain, snapshot_id)
RestoreWorkspaceToSnapshot(snapshot_id)
```

Workspace restore must define omitted-domain behavior:

```text
omitted_domain_policy:
  ERROR
  CARRY_FORWARD_CURRENT
  OMIT_FROM_ROOT_TOKEN
```

Default to `ERROR`. ADR-034 already treats omitted root transaction
participants as omitted from that root token. There is no implicit carry-forward
from a global workspace head.

#### Retention Pins And GC

A manifest or artifact is GC-eligible only if:

1. It is not currently pointer-targeted.
2. It is not reachable from any unexpired workspace snapshot.
3. It is not reachable from any retained transaction read token.
4. It is not inside an active export manifest.
5. It is older than the orphan grace period.

Workspace snapshots, export manifests, transaction handles, root read tokens,
and review tokens all create retention pressure. The storage format spec must
make those pins explicit before implementation.

### P2: Add Resumable Control-Plane Transaction Handles

Arco already has transaction records, receipts, fencing tokens, root read tokens,
and domain publication. The missing product surface is a transaction handle that
can span processes and human review steps.

Start with a scoped control-plane transaction model. After a domain migrates to
the control store, transaction handles should stage and commit through
`ArcoStateTxn` or a higher-level workflow record over `ArcoStateTxn`.

```text
transactions/control_plane/{tx_id}.json
transactions/control_plane/{tx_id}/mutations/{mutation_id}.json
transactions/control_plane/{tx_id}.manifest.json
```

States:

```text
OPEN
PREPARING
PREPARED
COMMITTING
VISIBLE
ABORTED
EXPIRED
REPAIR_REQUIRED
```

Allowed transitions:

```text
OPEN -> PREPARING -> PREPARED -> COMMITTING -> VISIBLE
OPEN -> ABORTED
PREPARED -> ABORTED
OPEN -> EXPIRED
PREPARED -> EXPIRED
COMMITTING -> VISIBLE
COMMITTING -> REPAIR_REQUIRED
REPAIR_REQUIRED -> VISIBLE
REPAIR_REQUIRED -> ABORTED
```

TTL rules:

1. An `OPEN` transaction can expire safely if no manifest was prepared.
2. A `PREPARED` transaction requires explicit abort or recovery after expiry.
3. A `COMMITTING` transaction must be recovered, not expired.
4. A `VISIBLE` transaction record must be retained while snapshots or read
   tokens depend on it.

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

Mutation commands must not be expressed as writes to `system.*`. They may be
exposed through CLI, gRPC/HTTP command APIs, or a SQL-like command endpoint that
is explicitly outside the DataFusion read-query surface.

### P4: Add Conflict Summaries For Optimistic Retry

Arco's current publication path is conservative and safe: lock, append explicit
events, compact, pointer CAS. Over time, Arco can reduce false conflicts by
storing explicit read/write sets and action summaries in transaction receipts.

This depends on durable transaction handles. Conflict summaries need stable
object identity, canonical operation names, read-set semantics, write-set
semantics, predicate input sets, parent manifest hashes, result manifest hashes,
replay rules, and authorization recheck rules.

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
  "predicate_sets": [
    {"kind": "NO_OVERLAPPING_STORAGE_PREFIX", "prefix": "s3://bucket/path/"}
  ],
  "parent_manifest_hash": "...",
  "result_manifest_hash": "..."
}
```

On CAS loss, Arco can distinguish a true conflicting write from an independent
write that can be replayed on the new head. This borrows Olympia's
conflict-analysis idea while preserving Arco's locks, fencing, compactor, and
pointer-CAS visibility boundary.

A replayed transaction must re-run validation and authorization against the new
parent manifest before commit. Object existence, ownership, policies, storage
governance, and credential constraints may have changed after the original
parent manifest.

### P5: Add Derived Catalog Point-Lookup Indexes

The control store should be authoritative for Tier-1 point/range/predicate
reads after a domain migrates. Its keyspace handles:

1. Resolve object by fully qualified name.
2. Resolve object by stable ID.
3. Resolve active storage location.
4. Resolve active table-format contract.
5. Resolve grant and policy state.
6. Check storage prefix ownership.
7. Resolve idempotency records and object generations.

Parquet projections and manifest-referenced indexes remain optional derived
read accelerators for SQL, audit, discovery, and debugging:

```text
snapshots/catalog/v123/tables.parquet
indexes/catalog/v123/by_fqn.arrow
indexes/catalog/v123/by_stable_id.arrow
indexes/catalog/v123/by_storage_location.arrow
manifests/catalog/00000000000000000123.json
```

A stale or corrupt projection/index must not affect mutation correctness,
enforcement, or credential vending decisions.

Index rules:

1. Indexes are optional acceleration artifacts.
2. Readers must be able to fall back to manifest-selected Parquet projections.
3. A missing, stale, or corrupt index must not affect correctness.
4. Each index declares the `manifest_id` and artifact hashes it was built from.
5. Index readers must reject an index whose declared source manifest does not
   match the projection manifest selected by the current projection root,
   workspace snapshot, or root token.

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

Arco should use B-tree-like indexes only as derived lookup aids. The current
baseline remains ledger, compacted Parquet/JSON artifacts, immutable manifest,
and fenced pointer CAS. After a domain migrates, authoritative point lookup
should use the control-store keyspace, while public indexes remain derived.

### P6: Add Exportable Workspace Snapshot Manifests

Export should become a first-class product contract:

```json
{
  "snapshot_id": "01J...",
  "format": "arco.workspace_snapshot.v1",
  "root_prefix": "tenant=acme/workspace=prod/",
  "required_objects": [
    "control/manifests/...",
    "control/checkpoints/...",
    "control/txlog/...",
    "manifests/catalog/...",
    "snapshots/catalog/v123/tables.parquet",
    "state/orchestration/manifests/..."
  ],
  "projection_watermarks": {
    "system.catalog": 123,
    "system.access": 120
  },
  "event_archive_boundaries": {
    "catalog": "1..123",
    "access": "1..120"
  },
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

### Do Not Weaken Root Ownership Or Parquet Projection Ownership

No two roles should have independent CAS authority over the same root.

The active control-store writer owns the mutation-visible control root. The
projection compactor owns public Parquet projection artifacts and
projection-watermark publication. The projection compactor must not
independently publish the mutation-visible control root. The API/control writer
may write authoritative control-store artifacts, but must not write public
Parquet projection files.

The compactor remains the sole writer of public Parquet state. It no longer has
to be the synchronous success gate for Tier-1 mutations after a domain has moved
to a validated control-store path.

### Do Not Treat System Tables As A Write Surface

System tables should expose published state, snapshot records, transaction
receipts, and audit projections. They should not become the correctness path for
authorization, transactions, or mutation commands.

## Roadmap

### Phase 1: External Contract And Correctness

1. Add `docs/spec/arco-storage-format-v0.md`.
2. Add `docs/spec/object-store-contract.md`.
3. Add reader-contract pseudocode and conformance fixtures.
4. Add failure-mode tests for stale fencing, CAS loss, duplicate retry, orphan
   artifacts, stale pointer reads, compactor timeout, and expired transaction
   handles.
5. Add GC and retention reachability rules.
6. Define the read-only system-table projection contract for snapshot and
   transaction records. Implement concrete tables in the phase that creates the
   corresponding records.
7. Define the relationship between the current manifest authority baseline and
   the candidate control-store authority path.

### Phase 2: Workspace Snapshots

1. Add `WorkspaceSnapshotService`.
2. Add workspace snapshot records that pin authority heads, checkpoints,
   projection watermarks, and event archive boundaries.
3. Add `system.catalog.snapshots`.
4. Add export manifests.
5. Add roll-forward restore for domain and workspace snapshots.

### Phase 3: Durable Transaction Handles

1. Add resumable control-plane transaction records.
2. Add mutation staging objects.
3. Add prepare, commit, abort, and recover state machine.
4. Add `system.catalog.transactions`.
5. Add review-token workflow.

### Phase 4: Ergonomics And Optimization

1. Add `arco tx` CLI.
2. Add optional SQL-like command endpoint outside the DataFusion read-query
   surface.
3. Add read/write-set and action-summary receipts.
4. Add optimistic retry only after revalidation and re-authorization against
   the current authority head.
5. Add optional derived point-lookup indexes.

## Acceptance Criteria

This strategy succeeds when Arco can make these claims without relying on
private ADR context:

1. Arco can describe one final Tier-1 write authority for migrated domains:
   committed control-store transaction plus `StateToken`.
2. The current ledger plus synchronous compactor path is documented as current
   baseline and migration/rollback compatibility, not the final authority for a
   migrated Tier-1 domain.
3. System tables remain read-only, watermarked Parquet projections.
4. The projection compactor remains the sole writer of public Parquet projection
   artifacts.
5. Enforcement and credential vending read authoritative control state or
   fresh-enough compiled state, never lagging system tables.
6. Workspace snapshots pin control-store checkpoints/state tokens and projection
   watermarks.
7. Exports include all reachable authority, event archive, checkpoint,
   projection, and retention metadata required for restore/audit.
8. Object-store listing remains outside request-time correctness.
9. There is no hot-path global workspace root.
10. A migrated Tier-1 domain cannot accept authoritative writes through both the
    old manifest path and the new control-store path.
11. A reader can reject unsupported major layout versions and tolerate additive
    minor-version fields.
12. A migration can publish new-format manifests without breaking old retained
    snapshots.

## References

- [Olympia format](https://olympiaformat.org/)
- [Olympia repository](https://github.com/olympiaformat/olympia)
- [ADR-003: Manifest Domain Names and Contention Strategy](../adr/adr-003-manifest-domains.md)
- [ADR-018: Tier-1 Write Path Architecture](../adr/adr-018-tier1-write-path.md)
- [ADR-034: Fenced Head-Published Control-Plane Transactions](../adr/adr-034-fenced-head-published-control-plane-transactions.md)
- [Control-Plane Transactions Implementation Plan](2026-03-30-control-plane-transactions.md)
- [Arco Tier-1 Single Authority Vision](2026-06-26-arco-tier1-single-authority-combined-vision.md)
- [Arco Tier-1 Control Store Strategy](2026-06-25-arco-tier1-control-store-strategy.md)
- [Control-Plane Scope](../guide/src/reference/control-plane-scope.md)

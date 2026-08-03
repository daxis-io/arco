# Arco Tier-1 Single Authority Vision

**Subtitle:** Control-store authority with Olympia-inspired snapshots, export, transaction handles, and storage-contract semantics.

**Status:** Proposed umbrella vision.

**Audience:** Arco control-plane, catalog, governance, storage, and platform reviewers.

**Related documents:**

- [Olympia-Inspired Arco Strategy](2026-06-20-olympia-inspired-arco-strategy.md)
- [Arco Tier-1 Control Store Strategy](2026-06-25-arco-tier1-control-store-strategy.md)

---

## Executive summary

Arco should converge on one final Tier-1 write authority:

```text
API/domain service
  -> ArcoStateTxn
  -> object-store-backed control-store transaction
  -> fenced control manifest pointer CAS
  -> StateToken returned to caller
  -> async Parquet projection compactor
  -> watermarked system tables, audit views, snapshots, exports
```

The current ledger + synchronous compactor + Parquet/JSON manifest path should be treated as a migration adapter, shadowing tool, and rollback aid. It should not remain a permanent peer authority after migration.

The Olympia-inspired strategy remains important, but its role changes. It should not preserve Parquet/JSON manifest publication as the final Tier-1 mutation authority. Instead, it should define the product and storage-contract layer around the control store:

```text
workspace snapshots
rollback/export
transaction handles
root/read tokens
retention and GC pins
reader/projection conformance
operator-readable storage state
```

The combined vision is therefore:

```text
control-store authority
  + Olympia-inspired contract/snapshot/export/transaction semantics
  + Parquet system-table projections
  + object storage as durable authority
```

---

## Decision

Arco will standardize the final Tier-1 catalog and governance write path on an object-store-backed control store, assuming the prototype satisfies the required correctness and operational gates.

A successful Tier-1 mutation means:

```text
committed ArcoStateTxn
+ visible fenced control-store manifest pointer CAS
+ returned StateToken
```

It does not require synchronous Parquet system-table publication.

Parquet remains a first-class Arco surface, but as derived, watermarked projection state:

```text
system tables
SQL metadata views
audit projections
lineage/search/discovery projections
export-readable projection files
```

The projection compactor remains the sole writer of public Parquet projection artifacts. It does not own the mutation-visible control root.

The current synchronous-compactor authority path may remain behind `ArcoStateStore` during migration, but only as a temporary adapter. After a Tier-1 domain migrates, old-path authoritative writes for that domain must be disabled. After all targeted Tier-1 domains migrate, `arco-state-current` should be removed from production write paths.

---

## Why this is stronger than either document alone

The Olympia-inspired strategy gives Arco a crisp file-native product contract: snapshots, exports, transaction handles, root tokens, retention, GC, conformance, and operator-readable storage semantics.

The Tier-1 control-store strategy gives Arco the right online mutation substrate: state tokens, fast point/range/predicate preconditions, authoritative governance state, idempotency, fail-closed authorization, and decoupled Parquet projection.

Either document alone is incomplete:

| Option | Strength | Weakness |
|---|---|---|
| Olympia-inspired only | Clean storage/snapshot/export/product contract. | Keeps synchronous Parquet publication as the Tier-1 success gate. Does not solve hot point preconditions, idempotency, governance freshness, or compactor availability pressure. |
| Control-store only | Better final mutation substrate. | Risks becoming an internal KV/storage-engine project without a crisp product contract for snapshots, export, rollback, conformance, and external reader/operator semantics. |
| Combined single design | One final write authority plus a clean file-native product contract. | Requires Arco to own a real transactional state layer and its recovery/GC/provider tests. |

The combined design is worth doing if Tier-1 catalog/governance growth is real and synchronous compaction is expected to become a latency/availability/correctness bottleneck.

---

## Architecture at a glance

```text
Clients / compatibility APIs
  -> arco-api
  -> Arco catalog/governance domain services
  -> ArcoStateStore / ArcoStateTxn
  -> active control-store writer
  -> object storage:
       control/manifest/current.pointer.json
       control/manifest/{manifest_id}.json
       control/txlog/{physical_txn_id}.txn
       control/segments/...
       control/checkpoints/...
       events/archive/...
  -> StateToken returned

Async projection path:
  control-store event/outbox records
    -> projection compactor
    -> projection/{surface}/current.pointer.json
    -> Parquet system tables + watermarks

Snapshot/export path:
  snapshot service
    -> pins control-store checkpoints/state tokens
    -> pins projection watermarks
    -> pins event archive boundaries
    -> writes WorkspaceSnapshot
    -> optional ExportManifest
```

---

## Core invariants

1. **One final Tier-1 write authority.** Migrated Tier-1 domains commit through the control store only.
2. **Object storage remains durable authority.** Arco does not require an external database for Tier-1 state.
3. **Fenced pointer CAS is the visibility primitive.** Immutable artifacts are candidates until a fenced pointer selects them.
4. **No listing for request-time correctness.** Listing may support repair, audit, migration, and anti-entropy only.
5. **No split-brain root ownership.** No two roles get independent CAS authority over the same mutation-visible root.
6. **State tokens provide read-after-write.** Successful writes return a token naming the logical sequence and retained authority state.
7. **Parquet is projection, not Tier-1 authority.** System tables are open, queryable, watermarked, and derived.
8. **Enforcement fails closed.** Authorization and credential vending read authoritative control state or fresh-enough compiled state; stale/missing enforcement state denies.
9. **Snapshots pin authority and projection progress.** Workspace snapshots retain checkpoints/state tokens, event archive boundaries, and projection watermarks.
10. **No hot global workspace root.** Cross-domain cuts are created on demand through snapshots, exports, root/read tokens, or transaction handles.

---

## Final write path

A representative create-table flow:

```text
1. API receives create_table.
2. API routes to the active control-store writer for the metastore/domain scope.
3. Domain service begins ArcoStateTxn.
4. Transaction reads current authoritative state.
5. Transaction validates:
   - catalog/schema existence
   - normalized table name absence
   - object generation preconditions
   - authorization inputs
   - storage-governance predicates
   - idempotency key
6. Transaction writes:
   - immutable domain event
   - folded object state
   - name index keys
   - table current pointer
   - owner/grant records
   - projection outbox record
   - idempotency result
7. Writer writes immutable transaction object.
8. Writer writes next control-store manifest.
9. Writer CAS-publishes control manifest pointer.
10. API returns table ID and StateToken.
11. Projection compactor later publishes Parquet system-table rows and watermarks.
```

The synchronous success path ends at `StateToken`, not Parquet publication.

---

## Read model

Arco has three read modes:

| Read mode | Source | Freshness contract |
|---|---|---|
| Control-plane reads | Control store | Current or token-pinned authoritative state. |
| Enforcement reads | Control store or compiled state with sufficient token/watermark | Fail closed if stale or unavailable. |
| System-table reads | Parquet projections | Watermarked; may lag authority. |

API responses should expose the distinction:

```json
{
  "table_id": "table_456",
  "state_token": {
    "scope": "tenant/acme/metastore/prod",
    "logical_sequence": 18422,
    "snapshot_id": "manifest-01J...",
    "issued_at": "2026-06-26T00:00:00Z",
    "min_retained_until": "2026-06-26T01:00:00Z"
  },
  "projection_status": {
    "system_catalog_watermark": 18410,
    "system_tables_may_lag": true
  }
}
```

Compatibility APIs may expose tokens through headers, metadata, optional extension fields, or internal-only handling depending on client tolerance.

---

## Authority, projection, and export vocabulary

Use consistent language across both source docs and future ADRs:

```text
Control-store authority
  The final source of truth for Tier-1 catalog/governance mutations.

Control manifest
  The immutable manifest selected by the control root. It names visible logical
  sequences, transaction records, folded-state files/segments, checkpoints, and
  retention metadata.

StateToken
  A read-after-write token naming a logical sequence and retained control-store
  state.

CheckpointToken
  A stronger retention pin for projection jobs, long scans, export, migration,
  backup, and replay-equivalence tests.

Projection
  Rebuildable query-optimized state derived from authority. Public Parquet
  system tables are projections.

ProjectionWatermark
  The highest authority logical sequence included in a derived projection.

WorkspaceSnapshot
  A retained cut over authority checkpoints/state tokens, projection
  watermarks, event archive boundaries, and retention/export metadata.

ExportManifest
  A portable manifest listing every required authority, event, checkpoint,
  projection, checksum, compatibility, and relocation object needed to restore
  or audit a snapshot.
```

---

## Workspace snapshots

A workspace snapshot should pin control-store authority, not old Parquet manifest authority.

Suggested shape:

```text
WorkspaceSnapshot {
  snapshot_id
  scope
  created_at
  created_by
  retained_until
  authority_heads: {
    catalog: ControlStoreCheckpointRef {
      control_root
      manifest_id
      logical_sequence
      checkpoint_id?
      manifest_hash
      min_retained_until
    }
    access: ControlStoreCheckpointRef { ... }
    storage: ControlStoreCheckpointRef { ... }
  }
  projection_watermarks: {
    system.catalog: 18410
    system.access: 18405
    system.storage: 18401
    system.audit: 18390
  }
  event_archive_boundaries: {
    catalog: [1, 18422]
    access: [1, 18405]
    storage: [1, 18401]
  }
  export_policy
  parent_snapshot_id?
}
```

Retained historical snapshots from the old path may still reference old Parquet/JSON domain manifests. That is a compatibility/migration concern, not the final Tier-1 authoring model.

Restore is roll-forward:

```text
RestoreDomainToSnapshot(domain, snapshot_id)
RestoreWorkspaceToSnapshot(snapshot_id)
```

Restore publishes new visible authority or returns a new root/read token referencing the retained historical cut. It does not mutate old snapshots.

---

## Export manifests

Export is a first-class product contract.

An export manifest should include:

1. Control-store manifests and pointer metadata.
2. Retained transaction records required by the snapshot/checkpoint.
3. Control-store segments or bounded replay windows.
4. Checkpoint records and retention metadata.
5. Domain event archive boundaries and required event objects.
6. Projection manifests, Parquet files, and projection watermarks.
7. Checksums/digests for authoritative artifacts.
8. Layout versions and compatibility metadata.
9. Relative-path and relocation rules.
10. GC protection metadata for the export lifetime.

Example:

```json
{
  "export_id": "exp_01J...",
  "format": "arco.workspace_export.v1",
  "snapshot_id": "snap_01J...",
  "root_prefix": "tenant=acme/workspace=prod/",
  "required_objects": [
    "control/manifest/manifest-01J.json",
    "control/checkpoints/chk_01J.json",
    "control/txlog/01J.txn",
    "events/catalog/archive/000000000000018422.json",
    "projection/catalog/manifest-01J.json",
    "projection/catalog/tables/part-000.parquet"
  ],
  "projection_watermarks": {
    "system.catalog": 18410,
    "system.access": 18405
  },
  "retention_until": "2026-07-26T00:00:00Z",
  "relocation": {
    "paths_are_relative": true,
    "rewrite_required": false
  }
}
```

---

## Durable transaction handles

`ArcoStateTxn` is the low-level authority transaction. A durable control-plane transaction handle is the product/workflow layer above it.

Use handles for:

1. Human review.
2. Multi-step migrations.
3. Governance changes requiring staged approval.
4. Cross-domain workflows that need prepare/commit/abort/recover semantics.
5. Pinned read tokens for validation.

State machine:

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

Initial semantics:

```text
Single-domain ArcoStateTxn commits are atomic.

Cross-domain control-plane workflows are durable workflows over multiple
single-domain commits/checkpoints. They should not be described as general
unqualified distributed database transactions until a separate ADR defines that
contract.
```

---

## IAM and root ownership

Use this hard rule:

```text
No two roles get independent CAS authority over the same mutation-visible root.
```

Recommended ownership:

| Root | CAS owner | Purpose |
|---|---|---|
| `control/{scope}/manifest/current.pointer.json` | Active control-store writer | Tier-1 mutation visibility. |
| `projection/{surface}/current.pointer.json` | Projection compactor | Parquet projection visibility and watermarks. |
| `snapshots/{snapshot_id}.json` | Snapshot service | Immutable retained cuts. |
| `exports/{export_id}.json` | Export service | Portable export package. |
| Old manifest pointer during migration | Old compactor only while scope is `OldAuthority` | Temporary migration authority. |

The API/control writer may write authoritative control-store artifacts. It must not write public Parquet projections. The projection compactor may write public Parquet projections. It must not publish the mutation-visible control root independently.

---

## Migration model

Every Tier-1 scope moves through explicit states:

```text
OldAuthority
  ledger + synchronous compactor + Parquet/JSON manifest pointer accepts writes

ShadowControlStore
  old path still accepts writes;
  control store replays/imports/compares;
  no control-store authority writes

ControlStoreAuthority
  control store accepts writes;
  old path writes disabled;
  Parquet is async projection only

RetiredOldAuthority
  old authority code removed from production writes;
  retained historical artifacts readable through snapshot/export compatibility
```

No scope can be `OldAuthority` and `ControlStoreAuthority` at the same time.

---

## Roadmap

### Phase 0: Umbrella ADR and language cleanup

Deliver:

```text
docs/adr/adr-0XX-tier1-control-store-single-authority.md
docs/spec/arco-storage-format-v0.md
docs/spec/object-store-contract.md
docs/spec/state-token-and-checkpoint-contract.md
docs/spec/projection-watermark-contract.md
```

Update the Olympia doc to describe contract/snapshot/export semantics over the control-store final authority. Update the Tier-1 control-store doc to state that `arco-state-current` is transitional.

### Phase 1: State-store trait with current adapter

Implement `ArcoStateReader`, `ArcoStateStore`, `ArcoStateTxn`, and `ArcoStateAdmin`. Put the current ledger + synchronous compactor path behind the trait without behavior change.

Goal: make domain services call the new seam without changing external API semantics.

### Phase 2: Deterministic model and object-store provider tests

Build `arco-state-model` and provider conformance tests for:

1. Conditional create.
2. Conditional pointer replacement.
3. Stable version tokens.
4. Addressed read-after-write.
5. Checksums and corruption detection.
6. Timeout/retry/idempotency behavior.
7. Stale writer epoch handling.
8. Orphan artifact recovery.
9. No listing for request-time correctness.

### Phase 3: Control-store MVP

Implement `arco-state-control-mvp` with:

1. Immutable transaction objects.
2. Control manifests.
3. Fenced manifest pointer CAS.
4. State tokens.
5. Checkpoints.
6. Bounded manifest-reachable replay.
7. Projection outbox records.
8. Failure-state tests.

Do not start with a custom segment format unless bounded replay fails to meet prototype budgets.

### Phase 4: Shadow replay and equivalence

Replay/import current catalog and governance state into the control store. Compare:

1. Object records.
2. Normalized name indexes.
3. Table current pointers.
4. Grants and ownership.
5. Storage-governance state.
6. Idempotency records.
7. Projection watermarks.
8. Event replay hashes.
9. Parquet projection equality through each watermark.

No production authority writes yet.

### Phase 5: Low-risk writable domains

First writable domains should be low-risk and non-enforcement-critical:

1. Projection job checkpoints.
2. Projection outbox acknowledgements.
3. Non-enforcement watermarks.
4. Synthetic internal failure-test domains.

Do not start with grants, credential vending, or broad catalog DDL.

### Phase 6: Storage-governance metadata without vending authority

Move storage credentials, external locations, and path-governance metadata only after range/predicate checks are proven. Keep credential vending on the old/fresh path until revocation freshness and deny-closed behavior pass.

### Phase 7: Idempotency, grants, and catalog DDL pilots

Move idempotency only when the protected mutation also commits in the control store. Move grants only after freshness, revocation, and compiled-cache tests pass. Move a narrow catalog DDL subset only after name, ID, ownership, table pointer, outbox, rollback, compatibility, and projection tests pass.

### Phase 8: Per-domain cutover

For each migrated domain:

1. Set state to `ControlStoreAuthority`.
2. Disable old-path authoritative writes.
3. Return StateTokens for successful mutations.
4. Publish Parquet projections asynchronously.
5. Expose projection watermarks.
6. Keep rollback to old artifacts only as retained snapshot/export compatibility, not as a live write path.

### Phase 9: Snapshot/export over control checkpoints

Update workspace snapshots and export manifests to pin control-store checkpoints/state tokens, projection watermarks, event archive boundaries, and relocation metadata.

### Phase 10: Retire old Tier-1 authority path

After all targeted Tier-1 domains are migrated and retained historical compatibility is handled, remove `arco-state-current` from production write routing.

---

## Acceptance criteria

This combined vision succeeds when Arco can make these claims:

1. A migrated Tier-1 domain has exactly one production write authority.
2. Successful Tier-1 writes return StateTokens and do not wait on Parquet projection publication.
3. Compactor outages do not block committed Tier-1 mutations, though projections may lag.
4. System tables are read-only, watermarked, derived Parquet projections.
5. Authorization and credential vending never depend on lagging system-table projections.
6. Control-store event replay equals folded KV state at each committed token.
7. Parquet projections equal authoritative state through each projection watermark.
8. Object-store listing is not used for request-time correctness.
9. Provider CAS and retry semantics are proven for every production backend.
10. Workspace snapshots and exports can retain, restore, and audit control-store authority and Parquet projections.
11. There is no hot-path global workspace root.
12. No two roles have independent CAS authority over the same mutation-visible root.
13. The old ledger + synchronous compactor path has a documented retirement plan.

---

## How the two source docs should relate

### Olympia-inspired strategy

This doc should become the contract/product layer:

```text
external storage contract
projection reader contract
workspace snapshots
rollback/export
transaction handles
root/read tokens
retention and GC
conformance fixtures
operator-readable failure states
```

It should not assert that Parquet/JSON domain manifests remain the final Tier-1 authoritative write path.

### Tier-1 control-store strategy

This doc should remain the technical authority-path layer:

```text
ArcoStateStore traits
object-store provider contract
control-store manifest model
StateToken and CheckpointToken
transaction isolation
keyspace
writer fencing
failure states
migration plan
projection watermarks
prototype decision criteria
```

It should state clearly that `arco-state-current` is transitional and that the intended final Tier-1 architecture has one write authority.

---

## Recommended ADR wording

```text
Decision:
Arco will adopt an object-store-backed control store as the single final
Tier-1 mutation authority for catalog and governance domains, pending prototype
validation.

The visible success boundary for a migrated Tier-1 mutation is a committed
`ArcoStateTxn`, a fenced control-store manifest pointer CAS, and a returned
`StateToken`.

Parquet system tables are asynchronous, watermarked projections. They remain
Arco's open query surface, but not the mutation authority or enforcement source.

The Olympia-inspired storage-contract work will define snapshots, exports,
transaction handles, root/read tokens, retention, GC, conformance, and
operator-readable failure states over the control-store authority model.

The current ledger + synchronous compactor path may exist behind
`ArcoStateStore` only during migration, shadow replay, rollback validation, or
retained historical compatibility. It must not remain a permanent production
write path for migrated Tier-1 domains.
```

---

## Open questions

1. Should final control roots be metastore-scoped, workspace-scoped, or domain-sharded under a metastore?
2. Which surfaces must expose `StateToken`s publicly versus through internal metadata/headers only?
3. What revocation freshness budget should credential vending enforce?
4. How long should mutation `StateToken`s remain usable by default?
5. Which checkpoints are user-visible versus internal-only?
6. What is the minimum public documentation level for the control-store file format?
7. What is the exact rollback story after old authority writes are disabled?
8. What are the production provider requirements for S3, GCS, Azure Blob, and S3-compatible stores?
9. When can custom control-store segments be introduced, and what compatibility guarantees do they need?
10. Which Tier-1 domain is the first real production writable control-store domain after synthetic/low-risk domains?

---

## Bottom line

The clean final vision is:

```text
one Tier-1 authority path:
  control-store transaction -> StateToken

one public query/projection path:
  async Parquet system tables -> projection watermarks

one product contract layer:
  snapshots -> exports -> transaction handles -> retention/GC -> conformance
```

This gives Arco the production-grade mutation model from the Tier-1 control-store strategy and the crisp file-native product semantics from the Olympia-inspired strategy, without locking the team into maintaining two permanent Tier-1 write paths.

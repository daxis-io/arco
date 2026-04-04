# ADR-034: Fenced Head-Published Control-Plane Transactions

## Status

Proposed

## Context

ADR-032 established immutable manifest snapshots plus pointer CAS as the
publication primitive for control-plane state. ADR-033 added internal
durability modes for orchestration compaction. Catalog and orchestration now
need one shared transaction model with a single commit point, aligned failure
semantics, and explicit treatment of repairable post-commit side effects.

The core requirement is that Parquet remains the immutable projection format,
but visibility is defined by head publication. The commit point is the fenced
CAS update of the mutable pointer, not the immutable Parquet write.

This ADR applies that model to catalog and orchestration now, and defines an
optional root-transaction extension for callers that later require a single
workspace-consistent cut across multiple domain heads.

## Decision

Arco control-plane transactions use this shared protocol:

1. Acquire the domain lock and mint a fencing token.
2. Read the current pointer and the immutable manifest snapshot it references.
3. Compute the next manifest state deterministically.
4. Write all new Parquet/JSON artifacts to new object keys only with `DoesNotExist`.
5. Write the new immutable manifest snapshot with `manifest_id`, `previous_manifest_path`, `parent_hash`, `epoch`/`fencing_token`, and a monotonic `commit_ulid`.
6. CAS the mutable pointer from the old object version to the new immutable manifest path.
7. Declare the transaction committed only when pointer CAS succeeds.
8. Run audit-chain writes and legacy-mirror writes after commit as repairable side effects; they must not change a committed response into a failure.

Readers are pointer-first and manifest-driven. They must not scan the ledger or
list object storage for correctness.

### Object Keys and Internal APIs

#### Catalog

Canonical keys:

```text
tenant={tenant}/workspace={workspace}/
locks/catalog.lock.json
ledger/catalog/{event_id}.json
snapshots/catalog/v{snapshot_version}/catalogs.parquet
snapshots/catalog/v{snapshot_version}/namespaces.parquet
snapshots/catalog/v{snapshot_version}/tables.parquet
snapshots/catalog/v{snapshot_version}/columns.parquet
manifests/catalog.pointer.json
manifests/catalog/{manifest_id}.json
manifests/catalog.manifest.json              # repairable legacy mirror
commits/catalog/{commit_id}.json            # repairable audit chain
```

Internal API:

```http
POST /internal/sync-compact
{
  "domain": "catalog" | "lineage" | "search",
  "event_paths": ["ledger/catalog/01J....json"],
  "fencing_token": 42,
  "request_id": "01J...."
}
```

```json
200 OK
{
  "manifest_version": "<pointer object version>",
  "commit_ulid": "01J....",
  "events_processed": 1,
  "snapshot_version": 7,
  "visibility_status": "visible",
  "repair_pending": false
}
```

Notes:

- `visibility_status` is always `visible` for catalog Tier-1 writes.
- `repair_pending=true` means pointer CAS committed but commit-record and/or
  legacy-mirror repair is still outstanding.

#### Orchestration

Canonical keys:

```text
tenant={tenant}/workspace={workspace}/
locks/orchestration.compaction.lock.json
ledger/orchestration/{yyyy-mm-dd}/{event_id}.json
state/orchestration/manifest.pointer.json
state/orchestration/manifests/{manifest_id}.json
state/orchestration/manifest.json                   # repairable legacy mirror
state/orchestration/base/{snapshot_id}/{table}.{hash}.parquet
state/orchestration/l0/{delta_id}/{table}.{hash}.parquet
state/orchestration/rebuilds/{rebuild_id}.json
```

`table` is one of the manifest-tracked projection tables, including at minimum
`runs`, `tasks`, `dep_satisfaction`, `timers`, and `dispatch_outbox`.

Internal APIs:

```http
POST /compact
{
  "event_paths": ["ledger/orchestration/2026-03-28/01J....json"],
  "fencing_token": 42,
  "lock_path": "locks/orchestration.compaction.lock.json",
  "request_id": "01J...."
}
```

```http
POST /rebuild
{
  "rebuild_manifest_path": "state/orchestration/rebuilds/20260328-120000.json",
  "fencing_token": 42,
  "lock_path": "locks/orchestration.compaction.lock.json",
  "request_id": "01J...."
}
```

```json
200 OK
{
  "events_processed": 10,
  "delta_id": "01J....",
  "manifest_revision": "01J....",
  "visibility_status": "visible" | "persisted_not_visible",
  "repair_pending": false
}
```

Notes:

- Replace optional `epoch` semantics with required `fencing_token` plus the
  canonical `lock_path`.
- Controller/API-triggered compaction must require
  `visibility_status="visible"` for success.
- `persisted_not_visible` remains internal-only for recovery/maintenance flows.

#### Optional Root Transactions

Use this only when a caller needs one workspace-consistent cut across multiple
domain heads.

Canonical keys:

```text
tenant={tenant}/workspace={workspace}/
manifests/root.pointer.json
manifests/root/{root_manifest_id}.json
manifests/root.manifest.json                        # repairable legacy mirror
manifests/root_transactions/{tx_id}.json           # idempotency/coordinator record
```

Root snapshot shape:

```json
{
  "root_manifest_id": "00000000000000000019",
  "transaction_id": "018f....",
  "epoch": 42,
  "previous_root_manifest_path": "manifests/root/00000000000000000018.json",
  "parent_hash": "sha256:...",
  "catalog_manifest_path": "manifests/catalog/00000000000000000042.json",
  "lineage_manifest_path": "manifests/lineage/00000000000000000009.json",
  "executions_manifest_path": "manifests/executions/00000000000000000011.json",
  "search_manifest_path": "manifests/search/00000000000000000012.json",
  "orchestration_manifest_path": "state/orchestration/manifests/00000000000000000077.json",
  "published_at": "2026-03-28T12:00:00Z"
}
```

Internal API:

```http
POST /internal/root-transactions/commit
{
  "transaction_id": "018f....",                     // UUIDv7
  "fencing_token": 42,
  "participants": {
    "catalog": "manifests/catalog/00000000000000000042.json",
    "lineage": "manifests/lineage/00000000000000000009.json",
    "executions": "manifests/executions/00000000000000000011.json",
    "search": "manifests/search/00000000000000000012.json",
    "orchestration": "state/orchestration/manifests/00000000000000000077.json"
  },
  "request_id": "01J...."
}
```

```json
200 OK
{
  "root_manifest_id": "00000000000000000019",
  "root_manifest_path": "manifests/root/00000000000000000019.json",
  "pointer_version": "<object version>",
  "visibility_status": "visible",
  "repair_pending": false
}
```

Notes:

- Create `manifests/root_transactions/{tx_id}.json` with `status="preparing"`
  before root publish for idempotency.
- Root pointer CAS is the visibility gate; the coordinator record is not on the
  read path.
- Domains omitted from `participants` are carried forward from the current root
  head unchanged.

### Failure-State Transitions

| Flow | Failure point | Visible state | Caller result | Repair path |
|---|---|---|---|---|
| Catalog | ledger append fails | unchanged | failure | retry append |
| Catalog | immutable snapshot or manifest snapshot write fails | unchanged | failure | retry safely |
| Catalog | pointer CAS loses race | previous head remains visible | conflict/retry | orphan snapshot GC |
| Catalog | pointer CAS succeeds, audit/mirror write fails | new head visible | success with `repair_pending=true` | reconciler writes commit/mirror |
| Orchestration | event read or parquet write fails | unchanged | failure | retry compaction |
| Orchestration | pointer CAS loses race | previous head remains visible | conflict/retry | retry with fresh head; orphan cleanup |
| Orchestration | pointer CAS succeeds, mirror write fails | new head visible | success with `repair_pending=true` | reconciler repairs mirror |
| Root tx | coordinator record create fails | unchanged | failure | retry with same `transaction_id` |
| Root tx | root snapshot write fails | unchanged | failure | retry with same `transaction_id` |
| Root tx | root pointer CAS loses race | previous root remains visible | conflict/retry | retry from fresh root; orphan snapshot GC |
| Root tx | root pointer CAS succeeds, tx finalize/mirror fails | new root visible | success with `repair_pending=true` | reconciler finalizes tx record and mirror |

### Test Plan

- Catalog publish commits only when `manifests/catalog.pointer.json` CAS succeeds;
  stale fencing tokens are rejected.
- Catalog post-commit failures in `commits/catalog/{commit_id}.json` or
  `manifests/catalog.manifest.json` return success with `repair_pending=true`,
  never failure after visibility.
- Orchestration `/compact` rejects missing, expired, stale, or non-canonical
  lock inputs.
- Orchestration readers reconstruct state from base snapshot plus L0 deltas
  only; no ledger reads are required for correctness.
- Orchestration persisted-but-not-visible mode is accepted only for explicit
  maintenance/rebuild paths.
- Root transaction retries with the same `transaction_id` are idempotent and
  return the same committed root head.
- Root readers see all participant heads switch together; per-domain readers
  remain unaffected.
- CAS races leave old pointers visible and new immutable artifacts unreachable
  but safe for GC.

### Assumptions and Defaults

- This ADR remains one unified decision, not split ADRs.
- Internal service APIs may evolve; external end-user APIs remain unchanged.
- Legacy mirror paths stay during migration, but they are explicitly repairable
  side effects, not commit prerequisites.
- Catalog and orchestration remain per-domain serializable by default.
- Root transactions are optional, disabled by default, and only needed for
  callers that require workspace-consistent multi-domain reads.

## Consequences

- The pointer CAS becomes the single visibility and commit gate across control
  plane domains.
- Immutable artifact writes stay append-only, while legacy mirrors and audit
  chain writes move firmly into repairable post-commit work.
- Reader correctness stays pointer-first and manifest-driven, avoiding ledger
  scans and object-store listing as read-path correctness mechanisms.
- Cross-domain workspace-consistent cuts are possible without forcing root
  coordination on every control-plane transaction.

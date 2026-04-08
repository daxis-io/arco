# ADR-034: Fenced Head-Published Control-Plane Transactions

## Status

Proposed

## Context

ADR-032 established immutable manifest snapshots plus pointer CAS as the
publication primitive for control-plane state. ADR-033 added internal
durability modes for orchestration compaction. Catalog and orchestration now
need one shared transaction model with a single commit point, aligned failure
semantics, and explicit treatment of repairable post-commit side effects.

The core requirement is that Parquet remains the immutable projection format.
For catalog and orchestration, visibility is defined by head publication and
the commit point is the fenced CAS update of the mutable pointer, not the
immutable Parquet write. For opt-in root transactions, visibility is defined
by finalizing the per-`tx_id` transaction record that points at an immutable
tx-scoped super-manifest.

This ADR applies that model to catalog and orchestration now, and defines an
optional root-transaction extension for callers that later require a single
workspace-consistent cut across multiple domain heads.

## Decision

Catalog and orchestration use this shared protocol:

1. Acquire the domain lock and mint a fencing token.
2. Read the current pointer and the immutable manifest snapshot it references.
3. Compute the next manifest state deterministically.
4. Write all new Parquet/JSON artifacts to new object keys only with `DoesNotExist`.
5. Write the new immutable manifest snapshot with `manifest_id`, `previous_manifest_path`, `parent_hash`, `epoch`/`fencing_token`, and a monotonic `commit_ulid`.
6. CAS the mutable pointer from the old object version to the new immutable manifest path.
7. Declare the transaction committed only when pointer CAS succeeds.
8. Run audit-chain writes and legacy-mirror writes after commit as repairable side effects; they must not change a committed response into a failure.

Ordinary readers are pointer-first and manifest-driven. Root-token readers are
transaction-record-first and super-manifest-driven. Readers must not scan the
ledger or list object storage for correctness.

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
domain heads. This is opt-in and pinned. It is not a moving workspace head.

Canonical keys:

```text
tenant={tenant}/workspace={workspace}/
locks/root.lock.json
transactions/root/{tx_id}.json                     # mutable transaction record
transactions/root/{tx_id}.manifest.json           # immutable super-manifest
commits/root/{commit_id}.json                     # optional audit receipt
manifests/root.manifest.json                      # existing catalog/bootstrap object
```

Root transaction record shape:

```json
{
  "tx_id": "018f....",
  "status": "VISIBLE",
  "lock_path": "locks/root.lock.json",
  "fencing_token": 42,
  "visible_at": "2026-03-28T12:00:00Z",
  "result": {
    "super_manifest_path": "transactions/root/018f....manifest.json",
    "read_token": "root:018f...."
  }
}
```

Root super-manifest shape:

```json
{
  "tx_id": "018f....",
  "fencing_token": 42,
  "domains": {
    "catalog": {
      "manifest_path": "manifests/catalog/00000000000000000042.json",
      "manifest_id": "00000000000000000042",
      "commit_id": "01J...."
    },
    "orchestration": {
      "manifest_path": "state/orchestration/manifests/00000000000000000077.json",
      "manifest_id": "00000000000000000077",
      "commit_id": "01J...."
    }
  },
  "published_at": "2026-03-28T12:00:00Z"
}
```

Internal API:

```http
POST /internal/root-transactions/commit
{
  "tx_id": "018f....",                              // UUIDv7
  "fencing_token": 42,
  "participants": {
    "catalog": "manifests/catalog/00000000000000000042.json",
    "orchestration": "state/orchestration/manifests/00000000000000000077.json"
  },
  "request_id": "01J...."
}
```

```json
200 OK
{
  "tx_id": "018f....",
  "root_commit_id": "01J....",
  "super_manifest_path": "transactions/root/018f....manifest.json",
  "read_token": "root:018f....",
  "visibility_status": "visible",
  "repair_pending": false
}
```

Notes:

- Create `transactions/root/{tx_id}.json` with `status="PREPARED"` before
  writing the super-manifest.
- Root visibility is defined by finalizing `transactions/root/{tx_id}.json` to
  `status="VISIBLE"` after `transactions/root/{tx_id}.manifest.json` exists.
- Root-token readers resolve the transaction record, then the immutable
  super-manifest, then the referenced immutable domain manifests directly.
- `manifests/root.manifest.json` remains the catalog/bootstrap object and is
  not the root transaction source of truth.
- The current shared root-transaction contract pins `catalog` and
  `orchestration` domain commits only; it does not redefine the existing
  catalog bootstrap root object into a cross-domain source of truth.
- Ordinary catalog/orchestration readers remain domain-local; only root-token
  readers get cross-domain atomicity.
- Domains omitted from `participants` are omitted from that root token. There
  is no implicit carry-forward from a global latest-root pointer.

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
| Root tx | tx record create fails | unchanged | failure | retry with same idempotency key |
| Root tx | participant publish or super-manifest write fails before root visibility | failed root tx is marked `ABORTED`; root token not visible | failure | retry with same idempotency key; runtime claims a fresh root `tx_id` and reuses visible participant commits through root-derived participant idempotency keys |
| Root tx | tx record/idempotency finalize loses one durable write | caller sees failure; surviving visible state is replay-repairable | retry same request | replay repairs the same `tx_id` from the cached visible record |
| Root tx | tx record is `VISIBLE`, audit receipt write fails | root token visible | success with `repair_pending=true` | reconciler writes `commits/root/{commit_id}.json` |

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
- Root transaction retries with the same idempotency key either repair the same
  visible `tx_id` from cached finalize state or claim a fresh root `tx_id`
  after a pre-visibility abort; participant commits are reused through
  root-derived participant idempotency keys.
- Root-token readers resolve the tx record, then the immutable super-manifest,
  then the referenced immutable participant manifests directly.
- Root-token readers see one pinned cross-domain cut; per-domain readers remain
  unchanged and may observe ordinary pointer-flip windows independently.
- `manifests/root.manifest.json` remains the bootstrap object for current
  catalog readers and is not the root transaction correctness boundary.

### Assumptions and Defaults

- This ADR remains one unified decision, not split ADRs.
- Internal service APIs may evolve; external end-user APIs remain unchanged.
- Legacy mirror paths stay during migration, but they are explicitly repairable
  side effects, not commit prerequisites.
- Catalog and orchestration remain per-domain serializable by default.
- Root transactions are opt-in per request, ship in the current transaction
  runtime, and are only needed for callers that require workspace-consistent
  multi-domain reads.

## Consequences

- Pointer CAS remains the visibility and commit gate for catalog and
  orchestration.
- Root transactions use per-`tx_id` record finalization plus an immutable
  super-manifest, not a moving root pointer CAS.
- Immutable artifact writes stay append-only, while legacy mirrors and audit
  chain writes move firmly into repairable post-commit work.
- Reader correctness stays manifest-driven: pointer-first for domain-local
  readers and root-token-first for pinned cross-domain readers.
- Cross-domain workspace-consistent cuts are possible without forcing root
  coordination onto the common read path or turning root into a global
  correctness boundary.

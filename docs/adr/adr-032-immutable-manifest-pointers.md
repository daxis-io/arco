# ADR-032: Immutable Manifest Snapshots + Pointer CAS

## Status

Accepted

## Context

Catalog and orchestration state previously relied on mutable manifest objects.
This created avoidable contention, weak forensic history, and stale-writer risk
during failover. We need a commit primitive that is:

1. Object-store friendly (append-only immutable writes + conditional swap).
2. Explicitly fenceable by epoch.
3. Backward compatible during migration and bootstrap replay.

## Decision

Arco adopts an immutable manifest snapshot model with a mutable pointer per domain.

Catalog domain writers and the orchestration micro-compactor now use this
snapshot+pointer model for visible state publication. Orchestration orphan
reconciliation/GC for abandoned snapshots and L0 directories remains follow-up
operational work.

### Storage layout

Catalog domains:

- Pointer: `manifests/{domain}.pointer.json`
- Immutable snapshots: `manifests/{domain}/{manifest_id}.json`
- Retired compatibility path: `manifests/{domain}.manifest.json` (no longer written for catalog,
  lineage, or search)

Orchestration:

- Pointer: `state/orchestration/manifest.pointer.json`
- Immutable snapshots: `state/orchestration/manifests/{manifest_id}.json`
- Retired compatibility path: `state/orchestration/manifest.json` (no longer written)

### Manifest IDs

- `manifest_id` is a fixed-width, zero-padded 20-digit decimal string.
- IDs are monotonic per domain (`000...0000`, `000...0001`, ...).
- Lexicographic order equals numeric order.

### Publish protocol

1. Read current pointer and resolve its immutable manifest snapshot.
2. Build next immutable manifest snapshot with:
   - `manifest_id`
   - `epoch`
   - `previous_manifest_path`
3. Write immutable snapshot with `DoesNotExist`.
4. CAS-update pointer using object version preconditions.
5. Do not write the retired mutable compatibility path as part of success.

### Epoch fencing

- Pointer payload stores `epoch`.
- New writes are rejected when writer epoch is behind pointer epoch.
- Epoch values are monotonic in manifest succession validation.

### Reader behavior

- Readers resolve the pointer and then load the immutable snapshot it names.
- Legacy mutable manifests are no longer part of the correctness path.

## Consequences

- Pros:
  - Stronger atomic publish semantics aligned with object store CAS.
  - Append-only metadata history and safer failure recovery.
  - Clear stale-writer fencing boundary at pointer publish.
  - Migration can proceed without coordinated downtime.
- Trade-offs:
  - More objects are written (snapshot + pointer instead of one mutable file).
  - Background reconciliation/GC policy is required for orphaned snapshots.

## Invariants

1. `root.manifest.json` remains the stable catalog entrypoint; no coordinated downtime migration.
2. Domain pointer objects are the only visibility gate for immutable snapshots.
3. Immutable snapshot objects are never modified in place.
4. Pointer CAS preconditions are mandatory (generation/ETag/object-version match).
5. Snapshot IDs are strictly monotonic fixed-width decimals per domain.
6. Epoch is monotonic across pointers and published snapshots for each domain.
7. Writer epochs behind pointer epoch are rejected as stale.
8. Readers are pointer-first; mutable legacy manifests are not required for correctness.
9. `root.manifest.json` may retain field aliases for backward-compatible parsing, but new root
   manifests point catalog/lineage/search at pointer paths.
10. Snapshot GC never deletes currently pointer-targeted snapshots.

## Migration Compatibility Matrix

| Writer | Reader | Outcome |
|---|---|---|
| Legacy (mutable manifest only) | Legacy | Baseline behavior |
| Legacy | New (pointer-first + compatibility parsing) | Reads can still succeed while bootstrap is migrated |
| New (snapshot + pointer only) | Legacy | Legacy direct-domain readers break; retired by ADR-034 cleanup |
| New | New | Pointer-selected immutable snapshots |

## Failure-State Truth Table

| Snapshot write | Pointer CAS | State visibility | Recovery path |
|---|---|---|---|
| Fail | Not attempted | No new state visible | Retry publish |
| Success | Fail (CAS race) | Previous pointer remains visible | Safe orphan snapshot; reconciler/GC cleanup |
| Success | Success | New state visible atomically | Continue |

## Rollback Playbook

1. Freeze rollout to new writers.
2. Keep readers pointer-first.
3. Verify root manifest and domain pointers resolve to readable immutable manifests.
4. Disable pointer CAS publishing for affected domains.
5. Run reconciler and orphan snapshot sweeper in dry-run, then enforce mode.
6. Resume writes only after corrective patching; do not rely on retired mutable manifests.

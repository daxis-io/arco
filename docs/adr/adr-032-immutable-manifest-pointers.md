# ADR-032: Immutable Manifest Snapshots + Pointer CAS

## Status

Accepted

## Context

Catalog and orchestration state currently rely on mutable manifest objects. This
creates avoidable contention, weak forensic history, and stale-writer risk during
failover. We need a commit primitive that is:

1. Object-store friendly (append-only immutable writes + conditional swap).
2. Explicitly fenceable by epoch.
3. Backward compatible during migration.

## Decision

Arco adopts an immutable manifest snapshot model with a mutable pointer per domain.

### Storage layout

Catalog domains:

- Legacy compatibility path: `manifests/{domain}.manifest.json`
- Pointer: `manifests/{domain}.pointer.json`
- Immutable snapshots: `manifests/{domain}/{manifest_id}.json`

Orchestration:

- Legacy compatibility path: `state/orchestration/manifest.json`
- Pointer: `state/orchestration/manifest.pointer.json`
- Immutable snapshots: `state/orchestration/manifests/{manifest_id}.json`

### Manifest IDs

- `manifest_id` is a fixed-width, zero-padded 20-digit decimal string.
- IDs are monotonic per domain (`000...0000`, `000...0001`, ...).
- Lexicographic order equals numeric order.

### Publish protocol

1. Read current pointer (or fallback legacy manifest if pointer missing).
2. Build next immutable manifest snapshot with:
   - `manifest_id`
   - `epoch`
   - `previous_manifest_path`
3. Write immutable snapshot with `DoesNotExist`.
4. CAS-update pointer using object version preconditions.
5. Keep a compatibility mirror write to the legacy mutable manifest path during migration.

### Epoch fencing

- Pointer payload stores `epoch`.
- New writes are rejected when writer epoch is behind pointer epoch.
- Epoch values are monotonic in manifest succession validation.

### Reader behavior

- Readers first resolve pointer and then load immutable snapshot.
- If pointer is absent, readers fallback to legacy path.

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
8. Readers are pointer-first with legacy fallback while migration remains active.
9. Legacy stable manifest paths are mirrored on successful publish until old readers are retired.
10. Snapshot GC never deletes currently pointer-targeted snapshots.

## Migration Compatibility Matrix

| Writer | Reader | Outcome |
|---|---|---|
| Legacy (mutable manifest only) | Legacy | Baseline behavior |
| Legacy | New (pointer-first + fallback) | Reads via fallback; no break |
| New (snapshot + pointer + legacy mirror) | Legacy | Reads mirrored legacy manifest |
| New | New | Pointer-selected immutable snapshots |

## Failure-State Truth Table

| Snapshot write | Pointer CAS | State visibility | Recovery path |
|---|---|---|---|
| Fail | Not attempted | No new state visible | Retry publish |
| Success | Fail (CAS race) | Previous pointer remains visible | Safe orphan snapshot; reconciler/GC cleanup |
| Success | Success | New state visible atomically | Continue post-commit bookkeeping |
| Success | Success + post-commit side-effect fail | New state visible | Reconciler repairs bookkeeping/idempotent side-effects |

## Rollback Playbook

1. Freeze rollout to new writers (switch config to legacy write path only).
2. Keep readers in pointer-first mode with legacy fallback enabled.
3. Verify root + domain legacy manifests are current and readable.
4. Disable pointer CAS publishing for affected domains.
5. Run reconciler and orphan snapshot sweeper in dry-run, then enforce mode.
6. Resume legacy-only writes until corrective patch is deployed.

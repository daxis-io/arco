# ADR-018: Tier-1 Write Path Architecture

## Status

Accepted

## Context

Gate 5 hardening requires "IAM-enforced sole writer for state files" but the current
`Tier1Writer` writes Parquet snapshots directly to `snapshots/{domain}/`. This creates
a contradiction with the design principle "Compactor is sole writer of Parquet state"
from the unified platform design.

The current flow is:
1. API acquires distributed lock
2. API reads manifest
3. API writes Parquet snapshots to `snapshots/`
4. API CAS-updates manifest
5. API releases lock

This means **both** API and Compactor have write access to Parquet output paths,
making IAM-based enforcement of "sole writer" impossible.

## Decision

**Compactor is the sole writer of all Parquet state files.**

Tier-1 (DDL) operations become:
1. Acquire distributed lock (for strong consistency)
2. Append event(s) to `ledger/{domain}/`
3. Trigger **synchronous compaction** via RPC to compactor service
4. Wait for compaction to complete (manifest CAS by compactor)
5. Release lock

### Service Responsibilities

| Service | Write Prefixes | Operations |
|---------|---------------|------------|
| API | `ledger/`, `locks/` | Append events, manage locks |
| Compactor | `snapshots/`, `state/`, `manifests/`, `commits/` | Write Parquet, update manifests, audit trail |

### Synchronous Compaction RPC

For Tier-1 strong consistency, API triggers compaction synchronously:

```
POST /internal/sync-compact
{
    "domain": "catalog",
    "event_paths": ["ledger/catalog/01JFXYZ.json"],
    "fencing_token": 42
}

Response:
{
    "manifest_version": "v123",
    "commit_ulid": "01JFXYZ..."
}
```

**Critical:** The RPC takes explicit event paths, NOT a "list events since watermark"
approach. This maintains the invariant that listing is anti-entropy only.

### Compactor Tenancy

The compactor runs in **single-tenant mode**. Tenant/workspace scope is configured
out-of-band via environment variables (not supplied by the request):

```
ARCO_TENANT_ID=acme-corp
ARCO_WORKSPACE_ID=production
ARCO_STORAGE_BUCKET=gs://my-bucket
```

### Fencing Token Flow

The fencing token from the distributed lock is passed through the RPC chain:

```
API (lock holder, token=N)
  → RPC to Compactor (includes token=N)
    → Compactor validates token matches current holder
      → Compactor writes Parquet + manifest with token=N
        → API receives confirmation
          → API releases lock
```

If another process takes over the lock (token=N+1) before compaction completes,
the compactor rejects the stale request.

## Consequences

### Positive

- IAM can enforce sole writer at infrastructure level (not code discipline)
- Clear separation: API = events, Compactor = state
- Fencing token in manifest payload prevents split-brain
- No listing required for correctness (only anti-entropy)

### Negative

- Added latency for Tier-1 DDL operations (~100-500ms for RPC round-trip)
- Compactor becomes critical path for DDL (must be highly available)
- More complex failure modes (API must handle compaction timeout/failure)

### Acceptable Trade-offs

DDL operations (create namespace, create table, etc.) are:
- Low frequency (typically < 100/day per tenant)
- Already require distributed lock acquisition (~50-200ms)
- Strongly consistent by design (blocking is expected)

The added latency is acceptable for the security and simplicity benefits.

## Superseded Conventions

The following patterns from older code are superseded by this ADR:

- `Tier1Writer::write_catalog_snapshot()` writing Parquet directly
- API having `objectUser` on `snapshots/` prefix
- Any pattern where API writes to `snapshots/` or `state/`

The canonical storage layout per ADR-005 remains:

```
{bucket}/{tenant}/{workspace}/
├── ledger/      # Append-only events (API writes)
├── snapshots/   # Tier-1 Parquet snapshots (Compactor writes)
├── state/       # Tier-2 compacted state (Compactor writes)
├── manifests/   # Domain pointers (Compactor writes)
├── locks/       # Distributed locks (API writes)
├── commits/     # Audit trail (API writes)
├── sequence/    # Tier-2 position counters (API writes via CAS)
└── quarantine/  # Dead-letter events (Compactor writes)
```

## Implementation Notes

### Phase 1: Add Ledger + RPC Path

1. Add `append_catalog_event()` to `Tier1Writer`
2. Create `SyncCompactionClient` for RPC to compactor
3. Add `/internal/sync-compact` endpoint to compactor service
4. Modify `Tier1Writer::update()` to use new path

### Phase 2: Remove Direct Parquet Writes

1. Remove `write_catalog_snapshot()` from `Tier1Writer`
2. Remove API IAM permissions for `snapshots/` and `manifests/`
3. Verify IAM smoke test passes (API cannot write snapshots)

### Phase 3: Fencing Enforcement

1. Add `fencing_token` field to manifest payload
2. Enforce monotonicity at publish time
3. Add integration test for stale holder rejection

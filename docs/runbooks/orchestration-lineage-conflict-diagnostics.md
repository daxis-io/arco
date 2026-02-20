# Runbook: Orchestration Lineage Verification and Conflict Diagnostics

## Purpose
Use this runbook to validate that orchestration execution metadata is linked to catalog projections and to diagnose deterministic run-key conflicts.

## Preconditions
- Operator has workspace-scoped API access.
- A run exists with at least one succeeded materialization task.
- `curl` and `jq` are available.

## Inputs
- `BASE_URL` (for example: `http://localhost:8080`)
- `WORKSPACE_ID`
- `RUN_ID`
- `TOKEN`

## 1. Verify execution lineage from run to catalog

### Step 1: Fetch run details

```bash
curl -sS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/api/v1/workspaces/$WORKSPACE_ID/runs/$RUN_ID" | jq '.'
```

Confirm on succeeded task rows:
- `executionLineageRef`
- `deltaTable`
- `deltaVersion`
- `deltaPartition`

### Step 2: Fetch partition status for the same asset/partition

```bash
curl -sS -H "Authorization: Bearer $TOKEN" \
  "$BASE_URL/api/v1/workspaces/$WORKSPACE_ID/partitions?asset=<ASSET_KEY>&limit=100" | jq '.'
```

For the corresponding partition row, confirm:
- `executionLineageRef` matches the run task
- `deltaTable` matches the run task
- `deltaVersion` matches the run task
- `deltaPartition` matches the run task

### Step 3: Cross-verify consistency
Expected invariant for successful materializations:
- `run task output -> task summary lineage fields -> partition status lineage fields`
all match for the same asset + partition.

## 2. Verify deterministic rerun/retry/skip diagnostics

### Step 1: Check rerun reason
For rerun-created runs, `GET /runs/{id}` should include deterministic `rerunReason` values (`from_failure` or `subset`).

### Step 2: Check retry attribution
For tasks with `attempt > 1`, `retryAttribution` should explain why the retry occurred and include attempt/max-attempt context.

### Step 3: Check skip attribution
For skipped tasks, `skipAttribution` should identify deterministic dependency causes (for example, upstream failure/cancelled/skipped).

## 3. Diagnose run-key conflicts with introspection payload

### Step 1: Trigger a run with a `runKey`

```bash
curl -sS -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  "$BASE_URL/api/v1/workspaces/$WORKSPACE_ID/runs" \
  -d '{"selection":["analytics.orders"],"runKey":"ops-conflict-check"}' | jq '.'
```

### Step 2: Re-trigger with same `runKey` but different payload

```bash
curl -sS -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  "$BASE_URL/api/v1/workspaces/$WORKSPACE_ID/runs" \
  -d '{"selection":["analytics.customers"],"runKey":"ops-conflict-check"}' | jq '.'
```

Expected response:
- HTTP `409`
- API body includes `details` payload with:
  - `conflictType = RUN_KEY_FINGERPRINT_MISMATCH`
  - `runKey`
  - `existingFingerprint`
  - `requestedFingerprint`
  - `existingRunId`
  - `existingPlanId`
  - `existingCreatedAt`

## Escalation
Escalate to orchestration on-call if:
- lineage fields are missing for successful materializations,
- run and partition lineage fields diverge for the same materialization,
- conflict diagnostics payload is absent on a fingerprint mismatch,
- rerun/retry/skip diagnostics are nondeterministic across repeated reads.

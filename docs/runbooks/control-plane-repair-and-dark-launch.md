# Control-Plane Repair And Dark Launch

This runbook covers ADR-034 PI-1 operations for:

- visible commits that return `repair_pending=true`
- current-head legacy mirror repair
- current-head catalog commit-record repair
- non-prod dark-launch drills for fenced pointer-CAS publication

The read path remains pointer-first and manifest-driven throughout these procedures. Legacy mirrors and catalog commit records are repaired side effects, not commit prerequisites.

## Preconditions

- Target a non-production tenant/workspace first.
- If either compactor has internal OIDC enabled via `ARCO_INTERNAL_AUTH_*`, fetch an allowlisted ID
  token first and add `-H "Authorization: Bearer $ARCO_INTERNAL_ID_TOKEN"` to the reconcile
  commands below.
- Confirm the compactor services are healthy:

```bash
curl -fsS http://$ARCO_COMPACTOR_HOST/ready
curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/health
```

- Record the current pointer targets before repair:

```bash
curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"catalog","repair":false}'

curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"repair":false}'
```

## Metrics To Watch

- `arco_catalog_repair_pending_total`
- `arco_flow_orch_repair_pending_total`
- `arco_catalog_reconciler_issues_total`
- `arco_catalog_reconciler_repairs_total`
- `arco_flow_orch_reconciler_repair_issues_total`
- `arco_flow_orch_reconciler_repairs_total`
- `arco_flow_orch_compactor_stale_fencing_rejects_total`

## Dry Run

Dry run means `repair=false`. This reports current-head repairable gaps without mutating storage.

Catalog domains:

```bash
curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"catalog","repair":false}' | jq

curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"lineage","repair":false}' | jq

curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"search","repair":false}' | jq
```

Orchestration:

```bash
curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"repair":false}' | jq
```

Expected PI-1 repairable findings:

- catalog `missing_current_head_legacy_mirror` or `stale_current_head_legacy_mirror`
- catalog `missing_current_head_commit_record`
- orchestration `missing_current_head_legacy_manifest_path` with `current_head_legacy_manifest_issue=missing|stale`

## Enforce Repair

Enforce mode runs the same reconciler and applies the repairable items present in the returned
report.

Catalog `repair=true` now defaults to `repairScope=current_head_only`, which repairs only visible
current-head side effects and leaves generic cleanup items untouched. Use `repairScope=full` only
when you explicitly want the reconciler to delete orphaned or old snapshots.

Catalog current-head-only repair:

```bash
curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"catalog","repair":true,"repairScope":"current_head_only"}' | jq

curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"lineage","repair":true,"repairScope":"current_head_only"}' | jq

curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"search","repair":true,"repairScope":"current_head_only"}' | jq
```

Catalog full cleanup repair:

```bash
curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"catalog","repair":true,"repairScope":"full"}' | jq
```

Orchestration:

```bash
curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"repair":true}' | jq
```

Expected results:

- catalog returns repaired legacy mirror and commit-record counts when those side effects were missing
- catalog also repairs stale legacy mirrors when the pointer-target head and mutable mirror diverged
- catalog `repairScope=current_head_only` leaves orphaned and old snapshots in place
- catalog `repairScope=full` additionally repairs generic cleanup items such as orphaned or old snapshots
- orchestration returns repaired legacy manifest count when the pointer-target mirror was missing or stale
- a follow-up dry run reports no current-head repair issues

## Dark-Launch Drill

Run this sequence in non-prod before enabling wider fenced-write rollout:

1. Trigger one catalog write and one orchestration write through the fenced path.
2. Confirm both commit points advance via pointer-target manifests, not legacy mirrors.
3. Inject a post-CAS side-effect failure and confirm the caller receives success with `repair_pending=true`.
4. Confirm readers observe the new head before repair runs.
5. Run the reconcile dry run, then enforce repair.
6. Confirm the follow-up dry run is clean and repair metrics incremented exactly once per repaired side effect.

## Rollback

Rollback does not revert the pointer once a visible CAS succeeds. Instead:

1. Stop or gate the writer that is producing repair-pending outcomes.
2. Run reconcile dry run to enumerate missing current-head side effects.
3. Run enforce repair until current-head repair findings clear.
4. Leave legacy mirrors and commit records consistent with the current pointer target before re-enabling traffic.

## Escalation

Escalate immediately if any of the following occur:

- repeated `repair_pending` increments for the same tenant/workspace after enforce repair
- `arco_flow_orch_compactor_stale_fencing_rejects_total` rises unexpectedly during a dark launch
- reconcile repair returns failures for current-head issues
- readers do not observe the pointer-target manifest that the compactor reports as visible

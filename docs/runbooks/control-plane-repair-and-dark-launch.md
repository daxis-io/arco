# Control-Plane Repair And Dark Launch

This runbook covers ADR-034 PI-1 operations for:

- visible commits that return `repair_pending=true`
- current-head legacy mirror repair
- current-head catalog commit-record repair
- non-prod dark-launch drills for fenced pointer-CAS publication

The read path remains pointer-first and manifest-driven throughout these procedures. Legacy mirrors and catalog commit records are repaired side effects, not commit prerequisites.

## Rollout Flags

PI-1 rollout is controlled on the orchestration compactor by two env vars:

- `ARCO_FLOW_COMPACTOR_REQUEST_MODE`
  - default: `compatibility`
  - supported values: `compatibility`, `fenced_only`
  - `compatibility` preserves PI-1 behavior and allows requests that do not carry the full fenced contract
  - `fenced_only` rejects requests unless they carry both `fencing_token` and canonical `lock_path`
- `ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE`
  - default: `accept`
  - supported values: `accept`, `reject`
  - `accept` allows the legacy `epoch` request field alias during PI-1
  - `reject` returns `400` for requests that still use the legacy `epoch` field alias

These flags are read once during `arco-flow-compactor` startup. Changing shell env vars alone does
not update a running service; apply the new values through the deployment mechanism for the
compactor and restart/redeploy it before running the probes below.

Recommended sequence:

1. Baseline: `ARCO_FLOW_COMPACTOR_REQUEST_MODE=compatibility` and `ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=accept`
2. Non-prod alias gate: keep `compatibility`, switch `ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=reject`, and confirm the legacy-epoch telemetry stays at zero or shows only expected rejected callers
3. Non-prod fenced dark launch: switch to `ARCO_FLOW_COMPACTOR_REQUEST_MODE=fenced_only` and keep `ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=reject`
4. Roll back by restoring `compatibility` first, then `accept` only if older callers still need the alias

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
- `arco_flow_orch_compactor_legacy_epoch_requests_total`
- `arco_flow_orch_compactor_partial_fenced_requests_total`

Related active alerts in `infra/monitoring/alerts.yaml`:

- `ArcoControlPlaneRepairPending`
- `ArcoControlPlaneRepairIssuesDetected`
- `ArcoControlPlaneReconcilerRepairFailures`
- `ArcoFlowStaleFenceRejects`
- `ArcoFlowLegacyEpochPayloadsObserved`

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

Compatibility-mode behavior:

- catalog sync compaction requires canonical `lock_path` when the caller supplies it
- orchestration compaction still accepts legacy request shapes in `compatibility` mode
- orchestration requests using the legacy `epoch` field increment `arco_flow_orch_compactor_legacy_epoch_requests_total`
- orchestration requests rejected by `ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=reject` increment the same metric with `status="rejected"`
- partially populated canonical orchestration requests now return `400` and increment `arco_flow_orch_compactor_partial_fenced_requests_total`

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

1. Configure the baseline rollout flags, restart/redeploy `arco-flow-compactor`, and wait for the
   startup log confirming the loaded rollout config:

```bash
export ARCO_FLOW_COMPACTOR_REQUEST_MODE=compatibility
export ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=accept
```

2. Trigger one catalog write and one orchestration write through the fenced path.
3. Confirm both commit points advance via pointer-target manifests, not legacy mirrors.
4. Inject a post-CAS side-effect failure and confirm the caller receives success with `repair_pending=true`.
5. Confirm readers observe the new head before repair runs.
6. Run the reconcile dry run, then enforce repair.
7. Confirm the follow-up dry run is clean and repair metrics incremented exactly once per repaired side effect.
8. Update the deployment to turn on alias rejection without changing request mode, then
   restart/redeploy `arco-flow-compactor`:

```bash
export ARCO_FLOW_COMPACTOR_REQUEST_MODE=compatibility
export ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=reject
```

9. Replay one known fenced request and one known legacy-epoch request:
   - fenced request should continue to succeed
   - legacy request should fail with `400`
   - `arco_flow_orch_compactor_legacy_epoch_requests_total` should increment with `status="rejected"`
10. Update the deployment to turn on fenced-only mode, then restart/redeploy
    `arco-flow-compactor`:

```bash
export ARCO_FLOW_COMPACTOR_REQUEST_MODE=fenced_only
export ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=reject
```

11. Replay:
   - one fully fenced request with canonical `lock_path`
   - one request with missing `lock_path`
   - one request using legacy `epoch`
12. Expected fenced-only results:
   - canonical fenced request succeeds
   - missing-`lock_path` request fails with `400`
   - legacy-epoch request fails with `400`
   - `ArcoFlowLegacyEpochPayloadsObserved` fires if legacy requests were still present
   - `ArcoFlowStaleFenceRejects` remains quiet unless the test intentionally injects stale fencing

Expected telemetry during the drill:

- `arco_catalog_repair_pending_total` and/or `arco_flow_orch_repair_pending_total` rise only when post-CAS side effects are intentionally failed
- `arco_catalog_reconciler_issues_total` and `arco_flow_orch_reconciler_repair_issues_total` rise during the dry run before repair
- `arco_catalog_reconciler_repairs_total` and `arco_flow_orch_reconciler_repairs_total` rise during enforce repair
- `arco_flow_orch_compactor_legacy_epoch_requests_total` shows `status="accepted"` in baseline compatibility mode and `status="rejected"` once alias rejection is enabled
- `arco_flow_orch_compactor_partial_fenced_requests_total` stays at zero unless a caller sends only one of `fencing_token` or `lock_path`
- `arco_flow_orch_compactor_stale_fencing_rejects_total` rises only when the drill intentionally reuses stale fencing tokens

## Rollback

Rollback does not revert the pointer once a visible CAS succeeds. Instead:

1. Restore orchestration compatibility in the deployment and restart/redeploy
   `arco-flow-compactor` first:

```bash
export ARCO_FLOW_COMPACTOR_REQUEST_MODE=compatibility
export ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=accept
```

2. Stop or gate the writer that is producing repair-pending outcomes.
3. Run reconcile dry run to enumerate missing current-head side effects.
4. Run enforce repair until current-head repair findings clear.
5. Leave legacy mirrors and commit records consistent with the current pointer target before re-enabling traffic.
6. Confirm the legacy-epoch metric stops increasing before attempting another fenced-only rollout.

## Escalation

Escalate immediately if any of the following occur:

- repeated `repair_pending` increments for the same tenant/workspace after enforce repair
- `arco_flow_orch_compactor_stale_fencing_rejects_total` rises unexpectedly during a dark launch
- reconcile repair returns failures for current-head issues
- readers do not observe the pointer-target manifest that the compactor reports as visible

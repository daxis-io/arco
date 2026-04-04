# Control-Plane Repair And Production Cutover

This runbook covers ADR-034 PI-3 operations for:

- default-on fenced orchestration compaction
- production repair automation and reconcile workflows
- production cutover verification and rollback

The read path remains pointer-first and manifest-driven throughout these procedures. Pointer CAS
remains the only commit point. Legacy mirrors and catalog commit records remain repairable side
effects, not commit prerequisites.

## Production Contract

PI-3 production behavior in the checked-in repo:

- `arco-flow-compactor` accepts only canonical fenced orchestration requests carrying
  `fencing_token` and `lock_path`
- the legacy orchestration `epoch` request alias is rejected with HTTP `400`
- partially populated orchestration request shapes are rejected with HTTP `400`
- active API and flow-service writers use the shared fenced append-and-compact path
- PI-1 and PI-2 orchestration compatibility request toggles are removed from this artifact
- if temporary compatibility fallback is required, roll back the affected services to the last
  PI-2-compatible build; there is no runtime compatibility switch in PI-3

## Runtime Controls

Writer and repair targeting:

- `ARCO_FLOW_COMPACTOR_URL`
  - optional on `arco_flow_automation_reconciler`, `arco_flow_dispatcher`,
    `arco_flow_sweeper`, and `arco_flow_timer_ingest`
  - when set, those services append events and compact through the remote orchestration compactor
    using the fenced request contract
  - when unset, those services use inline fenced compaction with the same visibility contract
- `ARCO_ORCH_COMPACTOR_URL`
  - optional on API deployments that emit orchestration ledger events
  - when set, API orchestration writers target the remote orchestration compactor with canonical
    fenced requests
  - API remote orchestration compaction uses the same hardened client semantics as flow-service
    writers: bounded request timeouts, transient retry handling, and automatic bearer auth for
    Cloud Run targets (or static bearer userinfo in local/test URLs)
  - when unset, API orchestration writers use inline fenced compaction with the same visibility
    contract
- `ARCO_COMPACTOR_URL`
  - catalog API writers use this to target the selected catalog compactor deployment

Repair automation defaults in PI-3:

- `ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_MODE`
  - default: `enforce`
  - supported values: `disabled`, `dry_run`, `enforce`
- `ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS`
  - default: `300`
- `ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_SCOPE`
  - default: `current_head_only`
  - supported values: `current_head_only`, `full`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_MODE`
  - default: `enforce`
  - supported values: `disabled`, `dry_run`, `enforce`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS`
  - default: `300`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_SCOPE`
  - default: `current_head_only`
  - supported values: `current_head_only`, `full`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_DOMAINS`
  - default: `catalog,lineage,search`

These env vars are read once during service startup. Apply changes through the deployment mechanism
for the affected service and restart or redeploy before running the probes below.

## Preconditions

- Target the intended production tenant or workspace only after PI-3 code is deployed everywhere in
  scope.
- If either compactor has internal OIDC enabled via `ARCO_INTERNAL_AUTH_*`, fetch an allowlisted ID
  token first and add `-H "Authorization: Bearer $ARCO_INTERNAL_ID_TOKEN"` to the reconcile
  commands below.
- Confirm the compactor services are healthy:

```bash
curl -fsS http://$ARCO_COMPACTOR_HOST/ready
curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/health
```

## Metrics To Watch

- `arco_catalog_repair_pending_total`
- `arco_flow_orch_repair_pending_total`
- `arco_catalog_reconciler_issues_total`
- `arco_catalog_reconciler_repairs_total`
- `arco_flow_orch_reconciler_repair_issues_total`
- `arco_flow_orch_reconciler_repairs_total`
- `arco_catalog_repair_automation_runs_total`
- `arco_flow_orch_repair_automation_runs_total`
- `arco_catalog_repair_backlog_count`
- `arco_flow_orch_repair_backlog_count`
- `arco_catalog_repair_backlog_age_seconds`
- `arco_flow_orch_repair_backlog_age_seconds`
- `arco_catalog_repair_completion_latency_seconds`
- `arco_flow_orch_repair_completion_latency_seconds`
- `arco_catalog_repair_repeat_findings_total`
- `arco_flow_orch_repair_repeat_findings_total`
- `arco_flow_orch_compactor_stale_fencing_rejects_total`
- `arco_flow_orch_compactor_request_contract_rejections_total`

Related active alerts in `infra/monitoring/alerts.yaml`:

- `ArcoControlPlaneRepairPending`
- `ArcoControlPlaneRepairIssuesDetected`
- `ArcoControlPlaneReconcilerRepairFailures`
- `ArcoControlPlaneRepairAutomationFailures`
- `ArcoControlPlaneRepairBacklogAgeHigh`
- `ArcoControlPlaneRepairBacklogCountHigh`
- `ArcoControlPlaneRepairCompletionLatencyHigh`
- `ArcoControlPlaneRepairPendingRateHigh`
- `ArcoFlowStaleFenceRejects`
- `ArcoFlowCompactorCompatibilityRegression`

## Reconcile Commands

Dry-run current-head repair state:

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

curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"repair":false,"repairScope":"current_head_only"}' | jq
```

Enforce current-head repair explicitly:

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

curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"repair":true,"repairScope":"current_head_only"}' | jq
```

Use `repairScope=full` only for explicit cleanup work; PI-3 production posture keeps automation on
`current_head_only`.

## Production Cutover Sequence

1. Deploy `arco-compactor` and `arco-flow-compactor` with explicit PI-3 repair defaults:

```bash
export ARCO_COMPACTOR_REPAIR_AUTOMATION_MODE=enforce
export ARCO_COMPACTOR_REPAIR_AUTOMATION_SCOPE=current_head_only
export ARCO_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS=300
export ARCO_COMPACTOR_REPAIR_AUTOMATION_DOMAINS=catalog,lineage,search

export ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_MODE=enforce
export ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_SCOPE=current_head_only
export ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS=300
```

2. Restart or redeploy both compactor services.

3. Confirm health:

```bash
curl -fsS http://$ARCO_COMPACTOR_HOST/ready
curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/health
```

4. Deploy the core writer surfaces against those compactors:

- API deployment(s) with the intended `ARCO_COMPACTOR_URL`
- API deployment(s) with the intended `ARCO_ORCH_COMPACTOR_URL`, or explicitly accept the inline
  fenced orchestration fallback
- `arco_flow_automation_reconciler`
- `arco_flow_dispatcher`
- `arco_flow_sweeper`
- `arco_flow_timer_ingest`

5. Run the dry-run reconcile commands. Any current-head issues should either be absent or drain to
zero quickly under enforce automation.

6. Confirm green production state:

- `ArcoFlowCompactorCompatibilityRegression` stays quiet
- `ArcoFlowStaleFenceRejects` stays quiet unless a drill intentionally injects stale fencing
- `ArcoControlPlaneRepairAutomationFailures` stays quiet
- repair backlog count returns to zero
- repair backlog age stays below 900 seconds
- repair completion p95 stays below 120 seconds
- visible commits with `repair_pending=true` stay at three or fewer per hour
- pointer-target heads advance and readers observe the new head without relying on mutable mirrors

7. If any current-head side effects remain pending after the deployment, run the explicit enforce
commands above and do not continue expanding traffic until the backlog is clear.

## Rollback Sequence

Rollback never attempts pointer rollback after a visible CAS commit. Keep readers unchanged and
repair side effects first.

If repair automation itself is the problem but the fenced request contract is otherwise healthy:

```bash
export ARCO_COMPACTOR_REPAIR_AUTOMATION_MODE=dry_run
export ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_MODE=dry_run
```

Redeploy the compactors, then confirm backlog age and count stabilize before deciding whether to
return to `enforce`.

If the removed compatibility request path itself is the problem:

1. Pause or scale down the writer deployment that is failing.
2. Keep readers unchanged; do not attempt pointer rollback.
3. Run the dry-run reconcile commands to understand any outstanding repairable side effects.
4. If a compatibility fallback is required, roll back `arco-flow-compactor` and the affected
   writers to the last PI-2-compatible release. This is a temporary escape hatch only.
5. Re-run dry-run and enforce repair until current-head side effects converge.
6. Resume traffic only after the compatibility-dependent caller is either upgraded to PI-3 fenced
   requests or the temporary rollback deployment is explicitly accepted.

## Post-Cutover Expectations

- current production traffic should use only fenced orchestration compactor requests
- any increment of `arco_flow_orch_compactor_request_contract_rejections_total` is a regression,
  not informational telemetry
- stale fencing rejects should remain zero outside of intentional drills
- repair automation should keep current-head legacy side effects converged without widening into
  generic cleanup
- legacy mirrors and commit records remain repairable side effects until a later phase proves safe
  removal

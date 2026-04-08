# Control-Plane Repair And Production Cutover

This runbook covers ADR-034 PI-3 operations for:

- default-on fenced orchestration compaction
- production repair automation and reconcile workflows
- production cutover verification and rollback

The read path remains pointer-first and manifest-driven throughout these procedures. Pointer CAS
remains the only commit point. Catalog and orchestration no longer write legacy mutable mirrors or
catalog commit-chain files as part of visible commits.

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
  - default: `full`
  - supported values: `current_head_only`, `full`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_MODE`
  - default: `enforce`
  - supported values: `disabled`, `dry_run`, `enforce`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS`
  - default: `300`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_SCOPE`
  - default: `full`
  - supported values: `current_head_only`, `full`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_DOMAINS`
  - default: `catalog,lineage,search`

These env vars are read once during service startup. Apply changes through the deployment mechanism
for the affected service and restart or redeploy before running the probes below.

After legacy current-head side-effect removal, `full` is the production default because generic
orphan cleanup is the remaining steady-state work. `current_head_only` is retained only for
request/config compatibility and should stay empty in steady state.

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

- `arco_catalog_reconciler_issues_total`
- `arco_catalog_reconciler_repairs_total`
- `arco_flow_orch_reconciler_orphans_total`
- `arco_flow_orch_reconciler_deletes_total`
- `arco_flow_orch_reconciler_deferred_paths_total`
- `arco_flow_orch_reconciler_skipped_paths_total`
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

- `ArcoControlPlaneRepairIssuesDetected`
- `ArcoControlPlaneReconcilerRepairFailures`
- `ArcoControlPlaneRepairAutomationFailures`
- `ArcoControlPlaneRepairBacklogAgeHigh`
- `ArcoControlPlaneRepairBacklogCountHigh`
- `ArcoControlPlaneRepairCompletionLatencyHigh`
- `ArcoFlowStaleFenceRejects`
- `ArcoFlowCompactorCompatibilityRegression`

## Reconcile Commands

Dry-run pointer-resolved cleanup state:

```bash
curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"catalog","repair":false,"repairScope":"full"}' | jq

curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"lineage","repair":false,"repairScope":"full"}' | jq

curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"search","repair":false,"repairScope":"full"}' | jq

curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"repair":false,"repairScope":"full"}' | jq
```

Enforce cleanup explicitly:

```bash
curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"catalog","repair":true,"repairScope":"full"}' | jq

curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"lineage","repair":true,"repairScope":"full"}' | jq

curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"search","repair":true,"repairScope":"full"}' | jq

curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"repair":true,"repairScope":"full"}' | jq
```

`repairScope=current_head_only` remains accepted for compatibility, but it should no longer find
new work after legacy mirror and catalog commit-record removal.

## Production Cutover Sequence

1. Deploy `arco-compactor` and `arco-flow-compactor` with explicit PI-3 repair defaults:

```bash
export ARCO_COMPACTOR_REPAIR_AUTOMATION_MODE=enforce
export ARCO_COMPACTOR_REPAIR_AUTOMATION_SCOPE=full
export ARCO_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS=300
export ARCO_COMPACTOR_REPAIR_AUTOMATION_DOMAINS=catalog,lineage,search

export ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_MODE=enforce
export ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_SCOPE=full
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

5. Run the dry-run reconcile commands. Any cleanup findings should be limited to orphaned immutable
artifacts or stale directories and should drain under the intended repair scope.

6. Confirm green production state:

- `ArcoFlowCompactorCompatibilityRegression` stays quiet
- `ArcoFlowStaleFenceRejects` stays quiet unless a drill intentionally injects stale fencing
- `ArcoControlPlaneRepairAutomationFailures` stays quiet
- repair backlog count returns to zero
- repair backlog age stays below 900 seconds
- repair completion p95 stays below 120 seconds
- pointer-target heads advance and readers observe the new head without relying on mutable mirrors
- catalog/orchestration visible commits do not recreate removed compatibility artifacts

7. If any reconciliation cleanup remains pending after the deployment, run the explicit enforce
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
3. Run the dry-run reconcile commands to understand any outstanding reconciliation cleanup.
4. If a compatibility fallback is required, roll back `arco-flow-compactor` and the affected
   writers to the last PI-2-compatible release. This is a temporary escape hatch only.
5. Re-run dry-run and enforce repair until cleanup findings converge.
6. Resume traffic only after the compatibility-dependent caller is either upgraded to PI-3 fenced
   requests or the temporary rollback deployment is explicitly accepted.

## Post-Cutover Expectations

- current production traffic should use only fenced orchestration compactor requests
- any increment of `arco_flow_orch_compactor_request_contract_rejections_total` is a regression,
  not informational telemetry
- stale fencing rejects should remain zero outside of intentional drills
- repair automation should focus on pointer-resolved orphan cleanup rather than removed current-head
  mirror or commit-record side effects
- legacy catalog/orchestration mirror writes and catalog commit-chain writes should remain absent

# Alert Rule Existence Proof (G3-006)

Generated UTC: 2026-02-14T04:43:17Z
Source: `infra/monitoring/alerts.yaml`

| Metric | Covered | Alert Rules |
|---|---|---|
| `arco_orch_backlog_depth` | YES | `ArcoOrchBacklogDepthHigh` |
| `arco_orch_compaction_lag_seconds` | YES | `ArcoOrchCompactionLagHigh` |
| `arco_orch_run_key_conflicts` | YES | `ArcoOrchRunKeyConflictsDetected` |
| `arco_orch_controller_reconcile_seconds` | YES | `ArcoOrchControllerReconcileP95High` |
| `arco_orch_slo_target_seconds` | YES | `ArcoOrchSloObservedAboveTarget` |
| `arco_orch_slo_observed_seconds` | YES | `ArcoOrchSloObservedAboveTarget` |
| `arco_orch_slo_breaches_total` | YES | `ArcoOrchSloBreachesIncreasing` |

## Rule Expressions

### `arco_orch_backlog_depth`
- `arco.orchestration/ArcoOrchBacklogDepthHigh`
  - expr: `max by (lane) (arco_orch_backlog_depth) > 200`

### `arco_orch_compaction_lag_seconds`
- `arco.orchestration/ArcoOrchCompactionLagHigh`
  - expr: `max(arco_orch_compaction_lag_seconds) > 30`

### `arco_orch_run_key_conflicts`
- `arco.orchestration/ArcoOrchRunKeyConflictsDetected`
  - expr: `max(arco_orch_run_key_conflicts) > 0`

### `arco_orch_controller_reconcile_seconds`
- `arco.orchestration/ArcoOrchControllerReconcileP95High`
  - expr: `histogram_quantile( 0.95, sum(rate(arco_orch_controller_reconcile_seconds_bucket[5m])) by (le, controller) ) > 2`

### `arco_orch_slo_target_seconds`
- `arco.orchestration/ArcoOrchSloObservedAboveTarget`
  - expr: `max by (slo) (arco_orch_slo_observed_seconds) > on (slo) max by (slo) (arco_orch_slo_target_seconds)`

### `arco_orch_slo_observed_seconds`
- `arco.orchestration/ArcoOrchSloObservedAboveTarget`
  - expr: `max by (slo) (arco_orch_slo_observed_seconds) > on (slo) max by (slo) (arco_orch_slo_target_seconds)`

### `arco_orch_slo_breaches_total`
- `arco.orchestration/ArcoOrchSloBreachesIncreasing`
  - expr: `sum by (slo) (increase(arco_orch_slo_breaches_total[5m])) > 0`

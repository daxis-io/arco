# ADR-034 PI-2 Exit Report

## Status

PI-2 repo scope is complete.

Historical note as of April 8, 2026: this report records the PI-2 canary-state recommendation.
The later PI-3 cleanup changed the steady-state repair automation default from
`current_head_only` to `full`; use `docs/reports/adr-034-pi3-exit.md` and
`docs/runbooks/control-plane-repair-and-dark-launch.md` for the current repo-side contract.

Delivered in this change set:

- active orchestration writer and flow-service callsites now use the shared fenced append-and-compact contract
- current-head repair execution is automated for both catalog and orchestration compactors with `disabled|dry_run|enforce` modes
- repair backlog, repeat-findings, completion-latency, repair-pending-rate, and stale-fence canary signals are wired into active monitoring
- canary controls and rollback criteria are documented against the actual service env vars in use
- PI-2 adoption inventory is checked in at `docs/reports/adr-034-pi2-adoption-inventory.md`

## PI-1 Gate Verification

PI-1 was verified before PI-2 execution. One direct prerequisite blocker was found and closed in-repo:

- orchestration reconcile and repair previously lacked an explicit PI-2-safe current-head-only default
- PI-2 added `repairScope` to orchestration reconcile handling and moved the default to `current_head_only`

No major PI-1 blockers requiring external code decisions remain in-repo.

## Writer And Maintenance Adoption

See `docs/reports/adr-034-pi2-adoption-inventory.md` for the repo-grounded callsite list.

Headline outcome:

- active orchestration API writers were already on the fenced helper path
- remaining active flow-service writers were migrated in PI-2
- catalog API writers and writer internals were already issuing fenced `SyncCompactRequest` commits
- catalog and orchestration reconcile flows now expose explicit current-head-only vs full repair scope controls

## Repair Automation Modes

Catalog compactor:

- `ARCO_COMPACTOR_REPAIR_AUTOMATION_MODE=disabled|dry_run|enforce`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_SCOPE=current_head_only|full`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_DOMAINS`

Orchestration compactor:

- `ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_MODE=disabled|dry_run|enforce`
- `ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_SCOPE=current_head_only|full`
- `ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS`

PI-2 recommendation:

- default to `dry_run` in non-prod first
- use `current_head_only` as the automation scope
- switch to `enforce` only after backlog age/count and stale-fence telemetry are clean

## Canary Plan And Gates

PI-2 ends with a limited canary path, not global default-on behavior.

Canary unit:

- selected non-prod `arco-flow-compactor` deployment
- selected non-prod `arco-compactor` deployment
- selected non-prod API deployment pointed at the target catalog compactor through `ARCO_COMPACTOR_URL`
- selected non-prod flow-service deployments pointed at the target orchestration compactor through `ARCO_FLOW_COMPACTOR_URL`
- one tenant/workspace at a time per environment

Exact progression gates:

- `ArcoControlPlaneRepairAutomationFailures` must stay quiet
- `ArcoControlPlaneRepairBacklogAgeHigh` must stay quiet; backlog age must stay below 900 seconds
- `ArcoControlPlaneRepairBacklogCountHigh` must stay quiet; backlog count must return to zero between enforce runs
- `ArcoControlPlaneRepairCompletionLatencyHigh` must stay quiet; repair completion p95 must stay below 120 seconds
- `ArcoFlowStaleFenceRejects` must stay at zero unless the drill intentionally injects a stale token
- `ArcoControlPlaneRepairPendingRateHigh` must stay quiet; visible commits with `repair_pending=true` must stay at three or fewer per hour during the canary
- `ArcoFlowLegacyEpochPayloadsObserved` must stay quiet once alias rejection is enabled

Compatibility fallback:

- restore `ARCO_FLOW_COMPACTOR_REQUEST_MODE=compatibility`
- keep reader behavior unchanged
- continue repair via dry-run or enforce until current-head side effects converge
- do not attempt pointer rollback after a visible CAS commit

## Monitoring References

Alerts source of truth:

- `infra/monitoring/alerts.yaml`

PI-2 alerts:

- `ArcoControlPlaneRepairAutomationFailures`
- `ArcoControlPlaneRepairBacklogAgeHigh`
- `ArcoControlPlaneRepairBacklogCountHigh`
- `ArcoControlPlaneRepairCompletionLatencyHigh`
- `ArcoControlPlaneRepairPendingRateHigh`

Dashboard:

- `infra/monitoring/dashboard.json`

PI-2 dashboard panels:

- `Repair Automation Runs (15m increase)`
- `Repair Backlog Count`
- `Repair Backlog Age (seconds)`
- `Repair Repeat Findings (15m increase)`
- `Repair Completion p95 (seconds)`

## Runbook References

- `docs/runbooks/control-plane-repair-and-dark-launch.md`

The runbook now covers:

- dry-run vs enforce automation modes
- current-head-only repair scope defaults
- explicit canary targeting
- canary gates and rollback criteria

## Verification

Commands run for PI-2 verification:

```bash
cargo fmt --all
cargo test -p arco-core
cargo test -p arco-catalog
cargo test -p arco-compactor
cargo test -p arco-flow --features test-utils
cargo test -p arco-api --lib
promtool check rules infra/monitoring/alerts.yaml
promtool test rules infra/monitoring/tests/control_plane_repair_alerts.test.yaml
promtool test rules infra/monitoring/tests/observability_gate4_alert_drill.test.yaml
promtool test rules infra/monitoring/tests/observability_orch_alert_drill.test.yaml
```

Observed results:

- `cargo fmt --all` exited `0`
- `cargo test -p arco-core` exited `0`
- `cargo test -p arco-catalog` exited `0`
- `cargo test -p arco-compactor` exited `0`
- `cargo test -p arco-flow --features test-utils` exited `0`
- `cargo test -p arco-api --lib` exited `0`
- `promtool check rules infra/monitoring/alerts.yaml` reported `SUCCESS: 24 rules found`
- `promtool test rules infra/monitoring/tests/control_plane_repair_alerts.test.yaml` reported `SUCCESS`
- `promtool test rules infra/monitoring/tests/observability_gate4_alert_drill.test.yaml` reported `SUCCESS`
- `promtool test rules infra/monitoring/tests/observability_orch_alert_drill.test.yaml` reported `SUCCESS`

Verification note:

- no pre-existing repo test failures remained in the required PI-2 verification set after the PI-2 changes

## PI-3 Carryover

- remove compatibility-mode orchestration compactor request branches
- remove the stored/public `epoch` compatibility alias
- remove compatibility-only unfenced client shims
- decide and execute the production default-on fenced cutover
- compatibility cleanup for any remaining legacy mirrors and request-path aliases

## External Blockers

Actual non-prod canary execution was not possible from this workspace because deployment access and
runtime credentials are not present here.

Concrete missing inputs in this shell:

- no non-prod `ARCO_COMPACTOR_HOST`
- no non-prod `ARCO_FLOW_COMPACTOR_HOST`
- no `ARCO_INTERNAL_ID_TOKEN`
- no deployment credentials to restart/reconfigure the target services

Because those dependencies are absent, PI-2 code, monitoring, tests, runbooks, and evidence can be
completed in-repo here, but the live non-prod canary drill itself cannot be executed from this
environment.

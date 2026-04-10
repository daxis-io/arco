# ADR-034 PI-1 Exit Report

## Status

PI-1 scope is complete in-repo.

Historical note as of April 8, 2026: this report captures the staged compatibility rollout that
preceded fenced-only PI-3. Current production guidance lives in
`docs/reports/adr-034-pi3-exit.md` and `docs/runbooks/control-plane-repair-and-dark-launch.md`.

Completed in this change set:

- catalog sync-compaction server paths now honor the shared request contract
  - canonical `lock_path` is validated server-side
  - `request_id` is emitted into tracing/log surfaces
- orchestration compactor rollout gating is explicit and configurable
  - `ARCO_FLOW_COMPACTOR_REQUEST_MODE=compatibility|fenced_only`
  - `ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=accept|reject`
  - legacy `epoch` payload usage is metered via `arco_flow_orch_compactor_legacy_epoch_requests_total`
  - partial canonical fenced requests are rejected and metered via `arco_flow_orch_compactor_partial_fenced_requests_total`
  - `/compact` and `/rebuild` both log `request_id`
- active monitoring now includes the PI-1 control-plane alerts and dashboard panels
- operator runbook now documents rollout sequencing, compatibility behavior, fenced-only dark launch, telemetry expectations, and rollback
- regression coverage was updated so orchestration replay tests match the already-landed `repair_pending` semantics for legacy-manifest side effects

## PI-1 Exclusions

Still intentionally out of scope for PI-1:

- PI-2, PI-3, PI-4 work
- root transactions
- optional root transactions
- storage migration away from serialized `epoch`
- public API breaking changes
- reader changes away from pointer-first, manifest-driven behavior
- treating legacy mirrors, commit-record writes, or audit-chain writes as commit prerequisites

## Verification

Local verification commands run successfully:

```bash
cargo fmt --all
cargo test -p arco-core
cargo test -p arco-catalog
cargo test -p arco-compactor
cargo test -p arco-flow --features test-utils
cargo test -p arco-api --lib
promtool check rules infra/monitoring/alerts.yaml
promtool test rules infra/monitoring/tests/observability_gate4_alert_drill.test.yaml
promtool test rules infra/monitoring/tests/observability_orch_alert_drill.test.yaml
promtool test rules infra/monitoring/tests/control_plane_repair_alerts.test.yaml
```

Observed outcomes:

- all listed cargo commands exited `0`
- `promtool check rules infra/monitoring/alerts.yaml` reported `SUCCESS: 19 rules found`
- all three `promtool test rules ...` commands reported `SUCCESS`

Verification note:

- `cargo test -p arco-flow --features test-utils` initially exposed a stale expectation in `orchestration_correctness_tests`; the test was updated to match the existing PI-1 `repair_pending` behavior for legacy-manifest side effects, then the full package rerun passed

## Alert Inventory

Active alert source of truth: `infra/monitoring/alerts.yaml`

PI-1 alerts now active:

- `ArcoControlPlaneRepairPending`
- `ArcoControlPlaneRepairIssuesDetected`
- `ArcoControlPlaneReconcilerRepairFailures`
- `ArcoFlowStaleFenceRejects`
- `ArcoFlowLegacyEpochPayloadsObserved`

Historical pointer only:

- `docs/runbooks/prometheus-alerts.yaml` now points back to `infra/monitoring/alerts.yaml` and is no longer a competing rule source

## Dashboard Inventory

Dashboard: `infra/monitoring/dashboard.json`

PI-1 panels added:

- `Control-Plane Repair Pending (15m increase)`
- `Control-Plane Reconcile Issues (15m increase)`
- `Control-Plane Repairs (15m increase)`
- `Stale Fence Rejects (15m increase)`
- `Legacy Epoch Payloads (15m increase)`

## Compatibility Telemetry Expectations

- baseline compatibility mode:
  - `ARCO_FLOW_COMPACTOR_REQUEST_MODE=compatibility`
  - `ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=accept`
  - legacy callers increment `arco_flow_orch_compactor_legacy_epoch_requests_total{status="accepted"}`
  - partial canonical requests missing either `fencing_token` or `lock_path` return `400` and increment `arco_flow_orch_compactor_partial_fenced_requests_total`
- alias rejection mode:
  - `ARCO_FLOW_COMPACTOR_REQUEST_MODE=compatibility`
  - `ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=reject`
  - legacy callers increment `arco_flow_orch_compactor_legacy_epoch_requests_total{status="rejected"}`
- fenced-only mode:
  - `ARCO_FLOW_COMPACTOR_REQUEST_MODE=fenced_only`
  - `ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=reject`
  - fully fenced callers succeed
  - missing-`lock_path` and legacy-epoch callers fail with `400`
  - stale lock holders increment `arco_flow_orch_compactor_stale_fencing_rejects_total`

## Dark-Launch Checklist

Actual non-prod execution was not possible in this environment.

Runnable checklist once service access is available:

The orchestration compactor reads rollout flags during process startup. Apply each flag change
through the deployment mechanism for `arco-flow-compactor` and restart/redeploy the service before
running the probes below.

1. Configure baseline rollout flags and restart/redeploy `arco-flow-compactor`:

```bash
export ARCO_FLOW_COMPACTOR_REQUEST_MODE=compatibility
export ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=accept
```

2. Confirm service health:

```bash
curl -fsS http://$ARCO_COMPACTOR_HOST/ready
curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/health
```

Expected: HTTP `200` from both endpoints.

3. Trigger one catalog write and one orchestration write through the fenced path.

Expected:

- pointer-target manifests advance
- readers observe the new head
- no unexpected `ArcoFlowStaleFenceRejects`

4. Turn on alias rejection in the deployment and restart/redeploy `arco-flow-compactor`:

```bash
export ARCO_FLOW_COMPACTOR_REQUEST_MODE=compatibility
export ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=reject
```

5. Probe a legacy orchestration compactor payload:

```bash
curl -i http://$ARCO_FLOW_COMPACTOR_HOST/compact \
  -H 'Content-Type: application/json' \
  -d '{"event_paths":[],"epoch":7,"request_id":"pi1-legacy-probe"}'
```

Expected:

- HTTP `400`
- `arco_flow_orch_compactor_legacy_epoch_requests_total{endpoint="compact",status="rejected",request_mode="compatibility"}` increments

6. Turn on fenced-only mode in the deployment and restart/redeploy `arco-flow-compactor`:

```bash
export ARCO_FLOW_COMPACTOR_REQUEST_MODE=fenced_only
export ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=reject
```

7. Replay:

- one fully fenced request with canonical `lock_path`
- one request missing `lock_path`
- one legacy-epoch request

Expected:

- fenced request succeeds
- missing-`lock_path` request returns HTTP `400`
- legacy-epoch request returns HTTP `400`
- `ArcoFlowLegacyEpochPayloadsObserved` only fires if legacy requests are still being sent

## Repair Drill Checklist

1. Run dry-run reconciliation:

```bash
curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"catalog","repair":false}' | jq

curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"repair":false}' | jq
```

Expected:

- catalog may report current-head legacy-mirror or commit-record gaps
- orchestration may report missing/stale current-head legacy-manifest gaps

2. Enforce repair:

```bash
curl -fsS http://$ARCO_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"domain":"catalog","repair":true,"repairScope":"current_head_only"}' | jq

curl -fsS http://$ARCO_FLOW_COMPACTOR_HOST/internal/reconcile \
  -H 'Content-Type: application/json' \
  -d '{"repair":true}' | jq
```

Expected:

- repair counters increment once per repaired side effect
- follow-up dry run is clean for current-head issues

3. Roll back rollout flags in the deployment and restart/redeploy `arco-flow-compactor` if needed:

```bash
export ARCO_FLOW_COMPACTOR_REQUEST_MODE=compatibility
export ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE=accept
```

Expected:

- compatibility traffic resumes
- no pointer rollback is attempted
- repair completes before wider traffic resumes

## External Blockers

Only remaining blocker to full non-prod PI-1 closeout from this environment:

- non-prod service access and credentials were unavailable

Concrete missing inputs in this shell:

- no `ARCO_COMPACTOR_HOST`
- no `ARCO_FLOW_COMPACTOR_HOST`
- no `ARCO_INTERNAL_ID_TOKEN`
- no `ARCO_TENANT_ID`
- no `ARCO_WORKSPACE_ID`
- no `ARCO_STORAGE_BUCKET`

Because those dependencies were absent, the dark-launch drill and live repair drill could not be executed against a real non-prod deployment from this workspace. No additional code blockers remain in-repo.

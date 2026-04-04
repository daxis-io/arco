# ADR-034 PI-3 Exit Report

## Status

PI-3 repo-side scope is complete in-tree.

PI-3 remains a repo-side cutover only from this workspace. Live production rollout was not executed
here because deployment hosts and credentials are unavailable in this shell.

## PI-1 And PI-2 Gate Verification

Source-of-truth artifacts reviewed before PI-3 execution:

- `docs/reports/adr-034-pi1-exit.md`
- `docs/reports/adr-034-pi2-adoption-inventory.md`
- `docs/reports/adr-034-pi2-exit.md`

Repo-grounded outcome:

- PI-2 compatibility-only orchestration request branches had already been removed from the active
  `arco-flow-compactor` request handlers in the working tree
- the shared orchestration compaction request structs now require canonical fenced fields and reject
  the legacy `epoch` alias
- the compatibility-only unfenced orchestration client shim was removed
- one direct repo blocker preventing PI-3 verification was found and closed:
  - `crates/arco-core/src/lib.rs` exported `repair_backlog` without docs under
    `#![deny(missing_docs)]`
  - `crates/arco-core/src/repair_backlog.rs` was only a checked-in placeholder test module and had
    to be completed so the crate could build and test cleanly

## Completed PI-3 Scope

- orchestration compactor requests are fenced-only by default and reject removed PI-2 compatibility
  shapes with hard `400` failures
- the stored/public orchestration `epoch` request alias is removed from the active request contract
- the compatibility-only unfenced orchestration compaction client helper is removed
- API and flow-service orchestration writers use the shared fenced append-and-compact path
  - remote orchestration compaction now uses the same hardened fenced client semantics across API
    and flow writers: canonical fencing fields, bounded request timeouts, transient retry handling,
    and remote `409` conflict propagation
- API orchestration writes no longer degrade into append-only behavior when
  `ARCO_ORCH_COMPACTOR_URL` is unset; they now fall back to inline fenced compaction and still
  require visible publication
- catalog and orchestration repair automation defaults are now production-grade:
  `enforce`, `current_head_only`, `300s` cadence
- production alerts, dashboards, and runbooks now treat compatibility-path usage as a regression,
  not informational telemetry

## Compatibility Paths Removed

- `crates/arco-flow/src/bin/arco_flow_compactor.rs`
  - removed runtime compatibility-mode request branching for `/compact` and `/rebuild`
  - removed `ARCO_FLOW_COMPACTOR_REQUEST_MODE`
  - removed `ARCO_FLOW_COMPACTOR_LEGACY_EPOCH_MODE`
- `crates/arco-core/src/orchestration_compaction.rs`
  - removed the legacy `epoch` alias from shared orchestration compaction request shapes
- `crates/arco-flow/src/compaction_client.rs`
  - removed `compact_orchestration_events(...)`
- compatibility telemetry changed from PI-1/PI-2 informational metrics to PI-3 hard-failure
  telemetry:
  - removed `arco_flow_orch_compactor_legacy_epoch_requests_total`
  - removed `arco_flow_orch_compactor_partial_fenced_requests_total`
  - added `arco_flow_orch_compactor_request_contract_rejections_total`

## Production Cutover Packet

Required runtime controls:

- `ARCO_COMPACTOR_REPAIR_AUTOMATION_MODE=enforce`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_SCOPE=current_head_only`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS=300`
- `ARCO_COMPACTOR_REPAIR_AUTOMATION_DOMAINS=catalog,lineage,search`
- `ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_MODE=enforce`
- `ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_SCOPE=current_head_only`
- `ARCO_FLOW_COMPACTOR_REPAIR_AUTOMATION_INTERVAL_SECS=300`
- API deployment config:
  - `ARCO_COMPACTOR_URL` for catalog writers
  - optional `ARCO_ORCH_COMPACTOR_URL` for remote orchestration compaction; if omitted, API
    orchestration writers now use inline fenced compaction
  - when `ARCO_ORCH_COMPACTOR_URL` targets Cloud Run, API remote orchestration compaction uses the
    same internal ID-token behavior as flow-service writers; local/test callers may also encode a
    static bearer token via URL userinfo (`http://bearer:<token>@host`)
- flow-service deployment config:
  - optional `ARCO_FLOW_COMPACTOR_URL` on `arco_flow_automation_reconciler`,
    `arco_flow_dispatcher`, `arco_flow_sweeper`, and `arco_flow_timer_ingest`; if omitted, those
    services use inline fenced compaction

Cutover order:

1. Deploy `arco-compactor` with the PI-3 repair automation defaults above.
2. Deploy `arco-flow-compactor` with the PI-3 repair automation defaults above.
3. Restart or redeploy both compactors.
4. Confirm `http://$ARCO_COMPACTOR_HOST/ready` and `http://$ARCO_FLOW_COMPACTOR_HOST/health`.
5. Deploy API and flow-service writer surfaces against those compactors.
6. Run dry-run reconcile on catalog and orchestration.
7. Confirm repair backlog count returns to zero, backlog age stays below `900s`, repair p95 stays
   below `120s`, `repair_pending=true` remains at three or fewer per hour, stale-fence rejects stay
   quiet, and `ArcoFlowCompactorCompatibilityRegression` stays quiet.
8. If needed, run explicit enforce reconcile until current-head side effects converge before
   expanding traffic.

Rollback order:

1. Do not attempt pointer rollback after a visible CAS commit.
2. If repair automation is unhealthy, drop both compactors to `dry_run` first and redeploy.
3. Pause or scale down the writer deployment that is failing.
4. Run dry-run reconcile to enumerate outstanding repairable current-head side effects.
5. Run enforce reconcile until current-head repair backlog converges.
6. If a removed compatibility path is still required, roll back `arco-flow-compactor` and the
   affected writers to the last PI-2-compatible release as a temporary escape hatch only.
7. Resume traffic only after the compatibility-dependent caller is upgraded or the temporary
   rollback is explicitly accepted.

## Remaining Deferred Cleanup

Intentionally not forced into PI-3:

- legacy mirrors and catalog commit-record side effects remain repairable side effects
  - repo evidence still shows active code and tests depending on legacy mirror presence and repair
  - no safe zero-dependency removal proof was available in this workspace
- internal `expected_epoch` micro-compactor helpers remain for internal fenced/rebuild logic and
  tests; PI-3 removed request/client compatibility shims, not every internal epoch-bearing helper
- root transactions remain out of scope for PI-3

## Verification

Verification commands run for PI-3:

```bash
cargo fmt --all
CARGO_TARGET_DIR=/tmp/arco-pi3-api cargo test -p arco-core
CARGO_TARGET_DIR=/tmp/arco-pi3-api cargo test -p arco-catalog
CARGO_TARGET_DIR=/tmp/arco-pi3-api cargo test -p arco-compactor
CARGO_TARGET_DIR=/tmp/arco-pi3-api cargo test -p arco-flow --features test-utils
CARGO_TARGET_DIR=/tmp/arco-pi3-api cargo test -p arco-api --lib
promtool check rules infra/monitoring/alerts.yaml
promtool test rules infra/monitoring/tests/control_plane_repair_alerts.test.yaml
promtool test rules infra/monitoring/tests/observability_gate4_alert_drill.test.yaml
promtool test rules infra/monitoring/tests/observability_orch_alert_drill.test.yaml
```

Observed results:

- `cargo fmt --all` exited `0`
- `CARGO_TARGET_DIR=/tmp/arco-pi3-api cargo test -p arco-core` exited `0`
- `CARGO_TARGET_DIR=/tmp/arco-pi3-api cargo test -p arco-catalog` exited `0`
- `CARGO_TARGET_DIR=/tmp/arco-pi3-api cargo test -p arco-compactor` exited `0`
- `CARGO_TARGET_DIR=/tmp/arco-pi3-api cargo test -p arco-flow --features test-utils` exited `0`
- `CARGO_TARGET_DIR=/tmp/arco-pi3-api cargo test -p arco-api --lib` exited `0`
- `promtool check rules infra/monitoring/alerts.yaml` reported `SUCCESS: 24 rules found`
- `promtool test rules infra/monitoring/tests/control_plane_repair_alerts.test.yaml` reported
  `SUCCESS`
- `promtool test rules infra/monitoring/tests/observability_gate4_alert_drill.test.yaml`
  reported `SUCCESS`
- `promtool test rules infra/monitoring/tests/observability_orch_alert_drill.test.yaml`
  reported `SUCCESS`

Targeted regression checks added for PI-3:

- legacy orchestration `epoch` request shapes are rejected by shared contract tests
- removed request shapes are rejected and metered by `arco-flow-compactor`
- repair automation defaults to `enforce` plus `current_head_only`
- API orchestration writes remain visible even without `ARCO_ORCH_COMPACTOR_URL`

Additional targeted local checks:

- `CARGO_TARGET_DIR=/tmp/arco-pi3-core cargo test -p arco-core repair_backlog_entry -- --nocapture`
  passed after the prerequisite `repair_backlog` module fix
- `CARGO_TARGET_DIR=/tmp/arco-pi3-api cargo test -p arco-api --lib append_events_and_compact_makes_inline_writes_visible_without_remote_compactor -- --nocapture`
  passed after the API inline fenced fallback change

## External Blockers

Live production rollout could not be executed here because the required deployment/runtime access
is absent in this shell.

Concrete missing inputs:

- no production `ARCO_COMPACTOR_HOST`
- no production `ARCO_FLOW_COMPACTOR_HOST`
- no `ARCO_INTERNAL_ID_TOKEN`
- no deployment credentials for API, catalog compactor, or flow-compactor services
- no host-level or platform-level access to restart/redeploy those services from this workspace

# Gate 3 Evidence

Batch 3 local/code-only Gate 3 evidence artifacts.

## Command Logs

- `command-logs/test_g3_projection_restart.log`
  - `cargo test -p arco-flow cold_restart_preserves_layer2_backfills_ticks_and_sensors -- --nocapture`
- `command-logs/test_g3_run_bridge_convergence.log`
  - `cargo test -p arco-flow --test run_bridge_controller_tests run_bridge_duplicate_and_conflict_requests_converge_after_first_emit -- --nocapture`
- `command-logs/test_g3_runtime_observability.log`
  - `cargo test -p arco-flow --test runtime_observability_tests -- --nocapture`
- `command-logs/search_g3_metrics_names.log`
  - `rg -n "arco_orch_backlog_depth|arco_orch_compaction_lag_seconds|arco_orch_run_key_conflicts|arco_orch_controller_reconcile_seconds" crates/arco-flow/src/metrics.rs crates/arco-flow/src/bin/arco_flow_dispatcher.rs crates/arco-flow/src/orchestration/controllers/`

## Notes

- Dashboard and alert screenshot/proof artifacts for operator metric visibility remain external to local repo execution.

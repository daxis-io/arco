# Gate 2 Verification Notes (Lean Evidence Model)

- Generated UTC: 2026-02-18T20:08:52Z
- Repo HEAD: `ce711a18fb387a2bfc13a4ba93a3d8aa3a071b7e`
- Scope: Gate 2 signals `G2-001..G2-007`

These notes capture commands, exit codes, and concise output snippets only.

## targeted_no_hardcoded_path_literals

- timestamp_utc: 2026-02-18T20:08:52Z
- command: `rg -n '"(ledger|state|events|_catalog|orchestration|manifests|locks|snapshots|commits|sequence|quarantine)/[^"\\n]*"' crates/arco-flow/src/orchestration/ledger.rs crates/arco-flow/src/outbox.rs crates/arco-iceberg/src/pointer.rs crates/arco-iceberg/src/events.rs crates/arco-iceberg/src/idempotency.rs crates/arco-api/src/paths.rs crates/arco-api/src/routes/manifests.rs crates/arco-api/src/routes/orchestration.rs`
- exit_code: 1
- result: PASS

```text
```

## targeted_flow_paths_contracts

- timestamp_utc: 2026-02-18T20:08:53Z
- command: `cargo test -p arco-core --test flow_paths_contracts -- --nocapture`
- exit_code: 0
- result: PASS

```text
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.48s
     Running tests/flow_paths_contracts.rs (target/debug/deps/flow_paths_contracts-f989683cc14ed182)

running 3 tests
test orchestration_and_flow_paths_are_stable ... ok
test api_paths_are_stable ... ok
test iceberg_paths_are_stable ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

```

## targeted_search_anti_entropy_bounded_scan

- timestamp_utc: 2026-02-18T20:08:53Z
- command: `cargo test -p arco-compactor test_search_anti_entropy_uses_bounded_scan_cursor -- --nocapture`
- exit_code: 0
- result: PASS

```text
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.25s
     Running unittests src/main.rs (target/debug/deps/arco_compactor-be5fa8de34c0f216)

running 1 test
test anti_entropy::tests::test_search_anti_entropy_uses_bounded_scan_cursor ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 24 filtered out; finished in 0.01s

```

## targeted_orchestration_schema_contracts

- timestamp_utc: 2026-02-18T20:08:54Z
- command: `cargo test -p arco-flow --test orchestration_schema_contracts -- --nocapture`
- exit_code: 0
- result: PASS

```text
warning: struct `CompactRequest` is never constructed
  --> crates/arco-flow/src/compaction_client.rs:15:8
   |
15 | struct CompactRequest<'a> {
   |        ^^^^^^^^^^^^^^
   |
   = note: `#[warn(dead_code)]` on by default

warning: `arco-flow` (lib) generated 1 warning
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.18s
     Running tests/orchestration_schema_contracts.rs (target/debug/deps/orchestration_schema_contracts-3d6784d31bd25151)

running 2 tests
test generate_golden_schemas ... ignored
test contract_orchestration_parquet_schemas_backward_compatible ... ok

test result: ok. 1 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s

```

## targeted_property_out_of_order_duplicate

- timestamp_utc: 2026-02-18T20:08:54Z
- command: `cargo test -p arco-flow --features test-utils --test property_tests compaction_is_out_of_order_and_duplicate_invariant -- --nocapture`
- exit_code: 0
- result: PASS

```text
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.14s
     Running tests/property_tests.rs (target/debug/deps/property_tests-6717f173ab554bee)

running 1 test
proptest: FileFailurePersistence::SourceParallel set, but failed to find lib.rs or main.rs
test compaction_is_out_of_order_and_duplicate_invariant ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out; finished in 0.17s

```

## targeted_property_crash_replay

- timestamp_utc: 2026-02-18T20:08:54Z
- command: `cargo test -p arco-flow --features test-utils --test property_tests compaction_crash_replay_converges_to_single_pass_state -- --nocapture`
- exit_code: 0
- result: PASS

```text
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.14s
     Running tests/property_tests.rs (target/debug/deps/property_tests-6717f173ab554bee)

running 1 test
proptest: FileFailurePersistence::SourceParallel set, but failed to find lib.rs or main.rs
test compaction_crash_replay_converges_to_single_pass_state ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 9 filtered out; finished in 0.18s

```

## targeted_orchestration_correctness_suite

- timestamp_utc: 2026-02-18T20:08:55Z
- command: `cargo test -p arco-flow --test orchestration_correctness_tests -- --nocapture`
- exit_code: 0
- result: PASS

```text
warning: struct `CompactRequest` is never constructed
  --> crates/arco-flow/src/compaction_client.rs:15:8
   |
15 | struct CompactRequest<'a> {
   |        ^^^^^^^^^^^^^^
   |
   = note: `#[warn(dead_code)]` on by default

warning: `arco-flow` (lib) generated 1 warning
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.13s
     Running tests/orchestration_correctness_tests.rs (target/debug/deps/orchestration_correctness_tests-7aec7a091c06d41a)

running 12 tests
test test_task_queue_idempotency ... ok
test test_anti_entropy_recovers_orphaned_tasks ... ok
test test_controller_determinism_ready_dispatch ... ok
test test_fold_run_requested_is_idempotent_on_same_fingerprint ... ok
test test_fold_run_requested_detects_conflict_on_fingerprint_mismatch ... ok
test test_fold_run_requested_creates_run_key_index ... ok
test test_dependency_correctness ... ok
test test_duplicate_event_is_noop ... ok
test test_order_independence ... ok
test test_controller_determinism_dispatcher ... ok
test test_controller_never_reads_ledger ... ok
test test_compaction_replay_recovers_after_manifest_publish_failure ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s

```

## targeted_partial_write_recovery

- timestamp_utc: 2026-02-18T20:08:58Z
- command: `cargo test -p arco-flow ledger_writer_recovers_after_partial_batch_write_failure -- --nocapture`
- exit_code: 0
- result: PASS

```text
     Running tests/orchestration_runtime_e2e_tests.rs (target/debug/deps/orchestration_runtime_e2e_tests-bb6ccb9a2c01ae65)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s

     Running tests/orchestration_schedule_e2e_tests.rs (target/debug/deps/orchestration_schedule_e2e_tests-f3cd71ce0fb5b3de)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 6 filtered out; finished in 0.00s

     Running tests/orchestration_schema_contracts.rs (target/debug/deps/orchestration_schema_contracts-3d6784d31bd25151)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.00s

     Running tests/orchestration_schema_tests.rs (target/debug/deps/orchestration_schema_tests-6d6dbadc20ca75b6)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 12 filtered out; finished in 0.00s

     Running tests/orchestration_selection_tests.rs (target/debug/deps/orchestration_selection_tests-e86c2f3c6981a67a)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 6 filtered out; finished in 0.00s

     Running tests/orchestration_sensor_e2e_tests.rs (target/debug/deps/orchestration_sensor_e2e_tests-a74da4b0f2070e42)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 6 filtered out; finished in 0.00s

     Running tests/orchestration_sensor_tests.rs (target/debug/deps/orchestration_sensor_tests-2ac7ba478402e845)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s

     Running tests/property_tests.rs (target/debug/deps/property_tests-7925392ad0112351)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/run_bridge_controller_tests.rs (target/debug/deps/run_bridge_controller_tests-6cd3155d14a469e8)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 5 filtered out; finished in 0.00s

     Running tests/runtime_observability_tests.rs (target/debug/deps/runtime_observability_tests-6ae7aa1555f940a4)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 7 filtered out; finished in 0.00s

```

## targeted_pointer_cas_race

- timestamp_utc: 2026-02-18T20:08:59Z
- command: `cargo test -p arco-iceberg test_pointer_store_cas_race_has_single_winner -- --nocapture`
- exit_code: 0
- result: PASS

```text
   |
   = note: `#[warn(dead_code)]` on by default

warning: `arco-flow` (lib) generated 1 warning
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.17s
     Running unittests src/lib.rs (target/debug/deps/arco_iceberg-9078478cff611883)

running 1 test
test pointer::tests::test_pointer_store_cas_race_has_single_winner ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 301 filtered out; finished in 0.02s

     Running tests/audit_wiring.rs (target/debug/deps/audit_wiring-810f61b5128c9d48)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s

     Running tests/commit_flow.rs (target/debug/deps/commit_flow-3f3f4a734009fdfb)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 15 filtered out; finished in 0.00s

     Running tests/config_integration.rs (target/debug/deps/config_integration-7a52ad8c7d26ed56)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s

     Running tests/crate_structure.rs (target/debug/deps/crate_structure-992e3dc5c448b99d)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s

     Running tests/credential_vending.rs (target/debug/deps/credential_vending-7676477595b2c125)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 5 filtered out; finished in 0.00s

     Running tests/openapi_compliance.rs (target/debug/deps/openapi_compliance-ba118bbb07595ad7)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 0.00s

     Running tests/reconciliation_tests.rs (target/debug/deps/reconciliation_tests-d74002f523ca9678)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 8 filtered out; finished in 0.00s

     Running tests/roundtrip.rs (target/debug/deps/roundtrip-31f5d8fdbf3ec914)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 15 filtered out; finished in 0.00s

```

## targeted_runbook_gate2_section

- timestamp_utc: 2026-02-18T20:08:59Z
- command: `rg -n '## Gate 2 Batch 3 Invariant Checks' docs/runbooks/metrics-catalog.md`
- exit_code: 0
- result: PASS

```text
30:## Gate 2 Batch 3 Invariant Checks (2026-02-12)
```

## matrix_fmt_check

- timestamp_utc: 2026-02-18T20:09:00Z
- command: `cargo fmt --all --check`
- exit_code: 0
- result: PASS

```text
```

## matrix_clippy_exact

- timestamp_utc: 2026-02-18T20:09:01Z
- command: `cargo clippy --workspace --all-features -- -D warnings`
- exit_code: 0
- result: PASS

```text
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.53s
```

## matrix_test_workspace_all_features_excl_flow_api

- timestamp_utc: 2026-02-18T20:09:21Z
- command: `cargo test --workspace --all-features --exclude arco-flow --exclude arco-api`
- exit_code: 0
- result: PASS

```text
test crates/arco-core/src/tenant.rs - tenant::TenantId::storage_prefix (line 62) ... ok
test crates/arco-core/src/observability.rs - observability::Redacted (line 53) ... ok
test crates/arco-core/src/catalog_paths.rs - catalog_paths::CatalogPaths (line 101) ... ok
test crates/arco-core/src/id.rs - id (line 9) ... ok
test crates/arco-core/src/audit.rs - audit::TestAuditSink (line 645) ... ok
test crates/arco-core/src/partition.rs - partition (line 30) ... ok
test crates/arco-core/src/observability.rs - observability::init_logging (line 121) ... ok
test crates/arco-core/src/observability.rs - observability::catalog_span (line 152) ... ok
test crates/arco-core/src/observability.rs - observability::orchestration_span (line 173) ... ok
test crates/arco-core/src/observability.rs - observability (line 12) ... ok
test crates/arco-core/src/lib.rs - prelude (line 60) ... ok
test crates/arco-core/src/audit.rs - audit (line 16) ... ok
test crates/arco-core/src/storage_keys.rs - storage_keys (line 25) ... ok
test crates/arco-core/src/tenant.rs - tenant (line 10) ... ok

test result: ok. 16 passed; 0 failed; 6 ignored; 0 measured; 0 filtered out; finished in 0.01s

   Doc-tests arco_delta

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests arco_iceberg

running 5 tests
test crates/arco-iceberg/src/credentials/mod.rs - credentials (line 14) ... ignored
test crates/arco-iceberg/src/lib.rs - (line 30) ... ignored
test crates/arco-iceberg/src/reconciler.rs - reconciler (line 17) ... ignored
test crates/arco-iceberg/src/pointer.rs - pointer::IcebergTablePointer (line 35) ... ok
test crates/arco-iceberg/src/types/commit.rs - types::commit::CommitTableRequestBuilder (line 290) ... ok

test result: ok. 2 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests arco_integration_tests

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests arco_proto

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests arco_test_utils

running 2 tests
test crates/arco-test-utils/src/lib.rs - (line 11) ... ignored
test crates/arco-test-utils/src/simulation.rs - simulation (line 16) ... ignored

test result: ok. 0 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests arco_uc

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

```

## matrix_test_flow_tests

- timestamp_utc: 2026-02-18T20:09:22Z
- command: `cargo test -p arco-flow --tests`
- exit_code: 0
- result: PASS

```text

running 6 tests
test canonicalize_asset_key_accepts_dot_or_slash_formats ... ok
test canonicalize_asset_key_rejects_invalid_inputs ... ok
test selection_does_not_include_downstream_by_default ... ok
test selection_includes_upstream_when_requested ... ok
test selection_fingerprint_is_order_insensitive ... ok
test selection_includes_downstream_only_when_explicit ... ok

test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/orchestration_sensor_e2e_tests.rs (target/debug/deps/orchestration_sensor_e2e_tests-a74da4b0f2070e42)

running 6 tests
test run_key_idempotency_is_enforced_even_without_event_idempotency ... ok
test run_key_conflict_is_durable_across_compactor_reload ... ok
test push_duplicate_message_id_delivery_is_idempotent_end_to_end ... ok
test poll_error_preserves_cursor_and_min_interval_backoff_is_deterministic ... ok
test poll_cas_mismatch_noops_cursor_and_drops_run_requested_end_to_end ... ok
test poll_cursor_is_durable_across_multiple_folds_and_runs_are_deterministic ... ok

test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s

     Running tests/orchestration_sensor_tests.rs (target/debug/deps/orchestration_sensor_tests-2ac7ba478402e845)

running 1 test
test test_sensor_eval_idempotency_dedupes_duplicate_message ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/property_tests.rs (target/debug/deps/property_tests-7925392ad0112351)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/run_bridge_controller_tests.rs (target/debug/deps/run_bridge_controller_tests-6cd3155d14a469e8)

running 5 tests
test run_bridge_skips_when_run_already_exists ... ok
test run_bridge_skips_when_source_fingerprint_does_not_match_index ... ok
test run_bridge_emits_run_triggered_and_plan_created_from_schedule_tick ... ok
test run_bridge_uses_matching_fingerprint_when_conflicts_exist ... ok
test run_bridge_duplicate_and_conflict_requests_converge_after_first_emit ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/runtime_observability_tests.rs (target/debug/deps/runtime_observability_tests-6ae7aa1555f940a4)

running 7 tests
test runtime_config_uses_expected_defaults ... ok
test runtime_config_applies_env_overrides ... ok
test slo_snapshot_detects_compaction_lag_breach ... ok
test runtime_config_rejects_non_positive_values ... ok
test slo_snapshot_detects_run_requested_to_triggered_p95_breach ... ok
test backlog_snapshot_reports_pending_counts_conflicts_and_lag ... ok
test backlog_snapshot_counts_only_actionable_dispatch_and_timer_rows ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

```

## matrix_test_flow_test_utils_tests

- timestamp_utc: 2026-02-18T20:09:29Z
- command: `cargo test -p arco-flow --features test-utils --tests`
- exit_code: 0
- result: PASS

```text

     Running tests/orchestration_sensor_e2e_tests.rs (target/debug/deps/orchestration_sensor_e2e_tests-ca6f04002c698b6f)

running 6 tests
test run_key_idempotency_is_enforced_even_without_event_idempotency ... ok
test run_key_conflict_is_durable_across_compactor_reload ... ok
test push_duplicate_message_id_delivery_is_idempotent_end_to_end ... ok
test poll_error_preserves_cursor_and_min_interval_backoff_is_deterministic ... ok
test poll_cas_mismatch_noops_cursor_and_drops_run_requested_end_to_end ... ok
test poll_cursor_is_durable_across_multiple_folds_and_runs_are_deterministic ... ok

test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s

     Running tests/orchestration_sensor_tests.rs (target/debug/deps/orchestration_sensor_tests-262bf66bb7d53e7e)

running 1 test
test test_sensor_eval_idempotency_dedupes_duplicate_message ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/property_tests.rs (target/debug/deps/property_tests-6717f173ab554bee)

running 10 tests
test task_state_transitions_valid ... ok
test run_state_transitions_valid ... ok
test ulid_format_valid ... ok
test ulid_sorting_yields_chronological_timestamps ... ok
test plan_fingerprint_deterministic ... ok
test plan_stages_bounded ... ok
test event_serialization_roundtrip ... ok
test plan_always_valid_or_valid_rejection ... ok
test compaction_is_out_of_order_and_duplicate_invariant ... ok
test compaction_crash_replay_converges_to_single_pass_state ... ok

test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.17s

     Running tests/run_bridge_controller_tests.rs (target/debug/deps/run_bridge_controller_tests-89f864929371dd17)

running 5 tests
test run_bridge_skips_when_run_already_exists ... ok
test run_bridge_skips_when_source_fingerprint_does_not_match_index ... ok
test run_bridge_emits_run_triggered_and_plan_created_from_schedule_tick ... ok
test run_bridge_uses_matching_fingerprint_when_conflicts_exist ... ok
test run_bridge_duplicate_and_conflict_requests_converge_after_first_emit ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/runtime_observability_tests.rs (target/debug/deps/runtime_observability_tests-b2eebdb6650a0420)

running 7 tests
test runtime_config_applies_env_overrides ... ok
test slo_snapshot_detects_compaction_lag_breach ... ok
test runtime_config_rejects_non_positive_values ... ok
test runtime_config_uses_expected_defaults ... ok
test backlog_snapshot_counts_only_actionable_dispatch_and_timer_rows ... ok
test backlog_snapshot_reports_pending_counts_conflicts_and_lag ... ok
test slo_snapshot_detects_run_requested_to_triggered_p95_breach ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

```

## matrix_test_flow_all_features_tests

- timestamp_utc: 2026-02-18T20:09:37Z
- command: `cargo test -p arco-flow --all-features --tests`
- exit_code: 0
- result: PASS

```text

     Running tests/orchestration_sensor_e2e_tests.rs (target/debug/deps/orchestration_sensor_e2e_tests-48b8868b8edd3177)

running 6 tests
test run_key_idempotency_is_enforced_even_without_event_idempotency ... ok
test run_key_conflict_is_durable_across_compactor_reload ... ok
test push_duplicate_message_id_delivery_is_idempotent_end_to_end ... ok
test poll_error_preserves_cursor_and_min_interval_backoff_is_deterministic ... ok
test poll_cas_mismatch_noops_cursor_and_drops_run_requested_end_to_end ... ok
test poll_cursor_is_durable_across_multiple_folds_and_runs_are_deterministic ... ok

test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s

     Running tests/orchestration_sensor_tests.rs (target/debug/deps/orchestration_sensor_tests-14a4bcefca9ec759)

running 1 test
test test_sensor_eval_idempotency_dedupes_duplicate_message ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/property_tests.rs (target/debug/deps/property_tests-145c7d3007d20d4d)

running 10 tests
test run_state_transitions_valid ... ok
test task_state_transitions_valid ... ok
test ulid_format_valid ... ok
test ulid_sorting_yields_chronological_timestamps ... ok
test plan_fingerprint_deterministic ... ok
test event_serialization_roundtrip ... ok
test plan_stages_bounded ... ok
test plan_always_valid_or_valid_rejection ... ok
test compaction_is_out_of_order_and_duplicate_invariant ... ok
test compaction_crash_replay_converges_to_single_pass_state ... ok

test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.20s

     Running tests/run_bridge_controller_tests.rs (target/debug/deps/run_bridge_controller_tests-d8a1c3ef32d9f026)

running 5 tests
test run_bridge_skips_when_source_fingerprint_does_not_match_index ... ok
test run_bridge_skips_when_run_already_exists ... ok
test run_bridge_emits_run_triggered_and_plan_created_from_schedule_tick ... ok
test run_bridge_uses_matching_fingerprint_when_conflicts_exist ... ok
test run_bridge_duplicate_and_conflict_requests_converge_after_first_emit ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/runtime_observability_tests.rs (target/debug/deps/runtime_observability_tests-0b207230942603a2)

running 7 tests
test runtime_config_applies_env_overrides ... ok
test runtime_config_rejects_non_positive_values ... ok
test runtime_config_uses_expected_defaults ... ok
test slo_snapshot_detects_compaction_lag_breach ... ok
test backlog_snapshot_reports_pending_counts_conflicts_and_lag ... ok
test slo_snapshot_detects_run_requested_to_triggered_p95_breach ... ok
test backlog_snapshot_counts_only_actionable_dispatch_and_timer_rows ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

```

## matrix_test_api_all_features_tests

- timestamp_utc: 2026-02-18T20:09:42Z
- command: `cargo test -p arco-api --all-features --tests`
- exit_code: 0
- result: PASS

```text
     Running tests/openapi_contract.rs (target/debug/deps/openapi_contract-8896767b7700ae1b)

running 1 test
test contract_openapi_matches_implementation ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s

     Running tests/openapi_orchestration_routes.rs (target/debug/deps/openapi_orchestration_routes-d0977aeb8689e9bf)

running 2 tests
test list_history_endpoints_do_not_document_404 ... ok
test orchestration_read_endpoints_are_documented ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s

     Running tests/orchestration_api_tests.rs (target/debug/deps/orchestration_api_tests-154a070d61287f52)

running 7 tests
test test_create_backfill_rejects_range_with_invalid_date_format ... ok
test test_create_backfill_rejects_range_with_start_after_end ... ok
test test_create_backfill_rejects_filter_with_unsupported_keys ... ok
test test_create_backfill_rejects_filter_without_bounds ... ok
test test_create_backfill_accepts_range_selector ... ok
test test_create_backfill_conflicts_on_payload_mismatch ... ok
test test_create_backfill_idempotent_on_key ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.30s

     Running tests/orchestration_parity_gates_m1.rs (target/debug/deps/orchestration_parity_gates_m1-befe5777fd463371)

running 12 tests
test parity_m1_rerun_from_failure_rejects_succeeded_parent ... ok
test parity_m1_rerun_from_failure_plans_only_unsucceeded_tasks ... ok
test parity_m1_deploy_rejects_invalid_manifest_asset_key ... ok
test parity_m1_rejects_invalid_asset_keys ... ok
test parity_m1_rejects_unknown_assets ... ok
test parity_m1_trigger_rejects_reserved_lineage_labels ... ok
test parity_m1_run_key_conflicts_on_payload_mismatch ... ok
test parity_m1_run_key_is_not_reserved_on_bad_request ... ok
test parity_m1_selection_does_not_autofill_tasks ... ok
test parity_m1_run_key_idempotency_is_order_insensitive_for_selection ... ok
test parity_m1_rerun_subset_respects_include_downstream ... ok
test parity_m1_selection_include_downstream_uses_manifest_graph ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.36s

     Running tests/orchestration_schedule_e2e_tests.rs (target/debug/deps/orchestration_schedule_e2e_tests-27105deec64056d8)

running 8 tests
test list_schedule_ticks_missing_schedule_returns_empty_list ... ok
test schedule_upsert_validates_cron_timezone_and_assets ... ok
test deploy_manifest_idempotency_does_not_duplicate_schedule_events ... ok
test schedule_tick_detail_exposes_run_key_and_run_id_linkage ... ok
test schedule_definitions_flow_from_manifest_to_tick_history ... ok
test schedule_tick_detail_surfaces_skip_reason_and_error_message ... ok
test manifest_redeploy_does_not_override_operator_disabled_schedules ... ok
test schedule_crud_enable_disable_roundtrips_and_appears_in_list ... ok

test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.63s

```

## matrix_test_doc_workspace

- timestamp_utc: 2026-02-18T20:09:57Z
- command: `cargo test --doc --workspace`
- exit_code: 0
- result: PASS

```text
running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests arco_flow

running 13 tests
test crates/arco-flow/src/dispatch/cloud_tasks.rs - dispatch::cloud_tasks (line 27) ... ignored
test crates/arco-flow/src/dispatch/mod.rs - dispatch::TaskQueue (line 215) - compile ... ok
test crates/arco-flow/src/leader/mod.rs - leader::LeaderElector (line 97) - compile ... ok
test crates/arco-flow/src/lib.rs - (line 27) - compile ... ok
test crates/arco-flow/src/metrics.rs - metrics (line 43) ... ignored
test crates/arco-flow/src/metrics.rs - metrics (line 24) - compile ... ok
test crates/arco-flow/src/metrics.rs - metrics::TimingGuard (line 240) - compile ... ok
test crates/arco-flow/src/metrics.rs - metrics::time_scheduler_tick (line 295) - compile ... ok
test crates/arco-flow/src/dispatch/memory.rs - dispatch::memory::InMemoryTaskQueue (line 49) ... ok
test crates/arco-flow/src/quota/memory.rs - quota::memory::InMemoryQuotaManager (line 35) ... ok
test crates/arco-flow/src/leader/memory.rs - leader::memory::InMemoryLeaderElector (line 41) ... ok
test crates/arco-flow/src/quota/drr.rs - quota::drr::DrrScheduler (line 92) ... ok
test crates/arco-flow/src/store/memory.rs - store::memory::InMemoryStore (line 33) ... ok

test result: ok. 11 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests arco_iceberg

running 5 tests
test crates/arco-iceberg/src/credentials/mod.rs - credentials (line 14) ... ignored
test crates/arco-iceberg/src/lib.rs - (line 30) ... ignored
test crates/arco-iceberg/src/reconciler.rs - reconciler (line 17) ... ignored
test crates/arco-iceberg/src/types/commit.rs - types::commit::CommitTableRequestBuilder (line 290) ... ok
test crates/arco-iceberg/src/pointer.rs - pointer::IcebergTablePointer (line 35) ... ok

test result: ok. 2 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests arco_integration_tests

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests arco_proto

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests arco_test_utils

running 2 tests
test crates/arco-test-utils/src/lib.rs - (line 11) ... ignored
test crates/arco-test-utils/src/simulation.rs - simulation (line 16) ... ignored

test result: ok. 0 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests arco_uc

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

```

## matrix_cargo_deny

- timestamp_utc: 2026-02-18T20:09:57Z
- command: `cargo deny check bans licenses sources advisories`
- exit_code: 0
- result: PASS

```text
                  └── tonic v0.14.4 (*)

warning[duplicate]: found 2 duplicate entries for crate 'winnow'
    ┌─ /Users/ethanurbanski/arco/.worktrees/codex-prod-go-closure-batch3/Cargo.lock:536:1
    │  
536 │ ╭ winnow 0.6.26 registry+https://github.com/rust-lang/crates.io-index
537 │ │ winnow 0.7.14 registry+https://github.com/rust-lang/crates.io-index
    │ ╰───────────────────────────────────────────────────────────────────┘ lock entries
    │  
    ├ winnow v0.6.26
      └── cron v0.15.0
          ├── arco-api v0.1.0
          │   └── arco-integration-tests v0.1.0
          └── arco-flow v0.1.0
              ├── arco-api v0.1.0 (*)
              ├── arco-integration-tests v0.1.0 (*)
              └── arco-test-utils v0.1.0
                  ├── (dev) arco-api v0.1.0 (*)
                  └── (dev) arco-iceberg v0.1.0
                      ├── arco-api v0.1.0 (*)
                      └── arco-integration-tests v0.1.0 (*)
    ├ winnow v0.7.14
      ├── toml v0.9.10+spec-1.1.0
      │   └── trybuild v1.0.115
      │       └── (dev) arco-core v0.1.0
      │           ├── arco-api v0.1.0
      │           │   └── arco-integration-tests v0.1.0
      │           ├── arco-catalog v0.1.0
      │           │   ├── arco-api v0.1.0 (*)
      │           │   ├── arco-compactor v0.1.0
      │           │   ├── arco-iceberg v0.1.0
      │           │   │   ├── arco-api v0.1.0 (*)
      │           │   │   └── arco-integration-tests v0.1.0 (*)
      │           │   ├── arco-integration-tests v0.1.0 (*)
      │           │   ├── arco-test-utils v0.1.0
      │           │   │   ├── (dev) arco-api v0.1.0 (*)
      │           │   │   └── (dev) arco-iceberg v0.1.0 (*)
      │           │   ├── arco-uc v0.1.0
      │           │   │   └── arco-api v0.1.0 (*)
      │           │   └── xtask v0.1.0
      │           ├── arco-compactor v0.1.0 (*)
      │           ├── arco-delta v0.1.0
      │           │   ├── arco-api v0.1.0 (*)
      │           │   └── arco-integration-tests v0.1.0 (*)
      │           ├── arco-flow v0.1.0
      │           │   ├── arco-api v0.1.0 (*)
      │           │   ├── arco-integration-tests v0.1.0 (*)
      │           │   └── arco-test-utils v0.1.0 (*)
      │           ├── arco-iceberg v0.1.0 (*)
      │           ├── arco-integration-tests v0.1.0 (*)
      │           ├── arco-test-utils v0.1.0 (*)
      │           ├── arco-uc v0.1.0 (*)
      │           └── xtask v0.1.0 (*)
      ├── toml_edit v0.22.27
      │   └── toml v0.8.23
      │       └── xtask v0.1.0 (*)
      └── toml_parser v1.0.6+spec-1.1.0
          └── toml v0.9.10+spec-1.1.0 (*)

advisories ok, bans ok, licenses ok, sources ok
```

## matrix_buf_lint

- timestamp_utc: 2026-02-18T20:09:58Z
- command: `buf lint proto/`
- exit_code: 0
- result: PASS

```text
```

## matrix_buf_breaking

- timestamp_utc: 2026-02-18T20:09:58Z
- command: `buf breaking proto/ --against '.git#branch=main,subdir=proto'`
- exit_code: 0
- result: PASS

```text
```

## integrity_gate_tracker_json_valid

- timestamp_utc: 2026-02-18T20:09:58Z
- command: `jq -e . docs/audits/2026-02-12-prod-readiness/gate-tracker.json >/dev/null`
- exit_code: 0
- result: PASS

```text
```

## targeted_partial_write_recovery_recheck

- timestamp_utc: 2026-02-18T20:14:46Z
- command: `cargo test -p arco-flow ledger_writer_recovers_after_partial_batch_write_failure -- --nocapture`
- exit_code: 0
- result: PASS

```text
warning: `arco-flow` (lib test) generated 1 warning (1 duplicate)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.86s
     Running unittests src/lib.rs (target/debug/deps/arco_flow-44f3b9a6f3e339ce)

running 1 test
test outbox::tests::ledger_writer_recovers_after_partial_batch_write_failure ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 445 filtered out; finished in 0.02s
```

## integrity_tsv_paths_placeholders

- timestamp_utc: 2026-02-18T20:16:51Z
- command: `bash -lc '<gate2 integrity suite: TSV schema, timestamp/exit/result/log_path validation, artifact path existence, placeholder scan>'`
- exit_code: 0
- result: PASS

```text
INTEGRITY_GATE2_SUITE_OK
```

# Orchestration End-to-End Recovery (M1-M3) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Restore orchestration end-to-end reliability by fixing the callback-path break and hardening M1/M2/M3 callback-lineage regressions to PR-ready quality.

**Architecture:** Keep the current append-first event architecture intact while repairing the failing integration seam where `TaskOutput` evolved. Harden coverage at the callback type/handler boundary, API callback mapping boundary, and callback-to-run-read end-to-end boundary.

**Tech Stack:** Rust (`cargo test`), Axum routes, `serde` JSON contracts, `arco-flow` compactor/callback modules, `arco-api` integration tests.

---

## Scope

- Fix compile break in `orchestration_external_worker_e2e` caused by missing Delta fields in `TaskOutput` initializer.
- Add lineage assertions so callback output metadata survives compaction.
- Add callback contract tests ensuring Delta output fields serialize/deserialize and map through API conversion.
- Extend API integration callback path assertions to confirm run-read surfaces include lineage metadata.
- Re-run M1/M2/M3 verification matrix.

## Verification Matrix

- `cargo test -p arco-integration-tests --test orchestration_external_worker_e2e`
- `cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m1`
- `cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m2`
- `cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m3`
- `cargo test -p arco-flow --features test-utils --test orchestration_schedule_e2e_tests`
- `cargo test -p arco-flow --features test-utils --test orchestration_sensor_e2e_tests`
- `cargo test -p arco-api --all-features --test api_integration test_arco_flow_deploy_run_callbacks_and_logs`
- `cargo test -p arco-api --all-features --test orchestration_schedule_e2e_tests`
- `cargo test -p arco-api --all-features --test orchestration_api_tests`

## Acceptance Criteria

- No compile errors in orchestration external worker E2E tests.
- Callback success payload can carry Delta lineage fields (`deltaTable`, `deltaVersion`, `deltaPartition`).
- Callback lineage metadata appears on compacted task state and API run-read response.
- Existing M2/M3 parity coverage remains green.


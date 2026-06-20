#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/run_user_acceptance_pipeline_uat.sh [--deterministic|--with-hygiene|--status] [--dry-run]

Modes:
  --deterministic  Run the CI-safe local reconciliation checks. This is the default.
  --with-hygiene   Run deterministic checks plus local shell/fmt/diff hygiene.
  --status         Print local/live gate readiness without running tests.
  --dry-run        Print commands instead of executing them.
EOF
}

mode="deterministic"
dry_run=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deterministic)
      mode="deterministic"
      ;;
    --with-hygiene)
      mode="with-hygiene"
      ;;
    --status)
      mode="status"
      ;;
    --dry-run)
      dry_run=1
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

run_cmd() {
  printf '+ %s\n' "$*"
  if [[ "$dry_run" -eq 0 ]]; then
    "$@"
  fi
}

print_status() {
  echo "deterministic: ready"
  if [[ -n "${ARCO_UAT_STORAGE_BUCKET:-}" ]]; then
    echo "live-durable: configured"
  else
    echo "live-durable: missing ARCO_UAT_STORAGE_BUCKET"
  fi
  if [[ -n "${ARCO_UAT_API_URL:-}" ]]; then
    echo "live-deployed: configured"
  else
    echo "live-deployed: missing ARCO_UAT_API_URL"
  fi
}

run_deterministic() {
  run_cmd cargo test -p arco-api test_parse_partition_selector
  run_cmd cargo test -p arco-api --test system_tables_api query_exposes_system_orchestration_runs_when_state_is_only_in_l0
  run_cmd cargo test -p arco-flow event_priority_orders_run_triggered_before_plan_created
  run_cmd cargo test -p arco-api test_trigger_run_reemits_when_reservation_exists
  run_cmd cargo test -p arco-flow test_handle_task_completed_failure
  run_cmd cargo test -p arco-integration-tests --test orchestration_external_worker_e2e
}

run_hygiene() {
  run_cmd bash tools/test_user_acceptance_uat_runner.sh
  run_cmd bash tools/test_actionlint_runner.sh
  run_cmd bash tools/test_user_acceptance_evidence_validator.sh
  run_cmd cargo fmt --check
  run_cmd git diff --check
}

case "$mode" in
  deterministic)
    run_deterministic
    ;;
  with-hygiene)
    run_deterministic
    run_hygiene
    ;;
  status)
    print_status
    ;;
esac

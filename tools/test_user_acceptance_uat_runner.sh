#!/usr/bin/env bash
set -euo pipefail

require_contains() {
  local output="$1"
  local expected="$2"
  case "$output" in
    *"$expected"*) ;;
    *)
      echo "expected output to contain: $expected" >&2
      exit 1
      ;;
  esac
}

require_not_contains() {
  local output="$1"
  local unexpected="$2"
  case "$output" in
    *"$unexpected"*)
      echo "expected output not to contain: $unexpected" >&2
      exit 1
      ;;
    *) ;;
  esac
}

deterministic_output="$(scripts/run_user_acceptance_pipeline_uat.sh --deterministic --dry-run)"
require_contains "$deterministic_output" "cargo test -p arco-api test_parse_partition_selector"
require_contains "$deterministic_output" "cargo test -p arco-integration-tests --test orchestration_external_worker_e2e"
require_contains "$deterministic_output" "cargo test -p arco-integration-tests --test user_acceptance_pipeline"
require_not_contains "$deterministic_output" "cargo fmt --check"
require_not_contains "$deterministic_output" "cargo clippy"

hygiene_output="$(scripts/run_user_acceptance_pipeline_uat.sh --with-hygiene --dry-run)"
require_contains "$hygiene_output" "bash tools/test_actionlint_runner.sh"
require_contains "$hygiene_output" "cargo fmt --check"
require_contains "$hygiene_output" "git diff --check"
require_not_contains "$hygiene_output" "cargo clippy"

serial_output="$(scripts/run_user_acceptance_pipeline_uat.sh --with-hygiene --dry-run --serial-cargo --isolated-target /tmp/arco-uat-hygiene-serial)"
require_contains "$serial_output" "CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/tmp/arco-uat-hygiene-serial cargo test -p arco-api test_parse_partition_selector"
require_contains "$serial_output" "CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/tmp/arco-uat-hygiene-serial cargo fmt --check"

status_output="$(scripts/run_user_acceptance_pipeline_uat.sh --status)"
require_contains "$status_output" "deterministic: ready"
require_contains "$status_output" "live-durable: missing ARCO_UAT_STORAGE_BUCKET"
require_contains "$status_output" "live-deployed: missing ARCO_UAT_API_URL"

echo "user acceptance UAT runner smoke passed"

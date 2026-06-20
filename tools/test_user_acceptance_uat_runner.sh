#!/usr/bin/env bash
set -euo pipefail

deterministic_output="$(scripts/run_user_acceptance_pipeline_uat.sh --deterministic --dry-run)"
[[ "$deterministic_output" == *"cargo test -p arco-api test_parse_partition_selector"* ]]
[[ "$deterministic_output" == *"cargo test -p arco-integration-tests --test orchestration_external_worker_e2e"* ]]
[[ "$deterministic_output" != *"cargo fmt --check"* ]]

hygiene_output="$(scripts/run_user_acceptance_pipeline_uat.sh --with-hygiene --dry-run)"
[[ "$hygiene_output" == *"bash tools/test_actionlint_runner.sh"* ]]
[[ "$hygiene_output" == *"cargo fmt --check"* ]]
[[ "$hygiene_output" == *"git diff --check"* ]]

status_output="$(scripts/run_user_acceptance_pipeline_uat.sh --status)"
[[ "$status_output" == *"deterministic: ready"* ]]
[[ "$status_output" == *"live-durable: missing ARCO_UAT_STORAGE_BUCKET"* ]]
[[ "$status_output" == *"live-deployed: missing ARCO_UAT_API_URL"* ]]

echo "user acceptance UAT runner smoke passed"

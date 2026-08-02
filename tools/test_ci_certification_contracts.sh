#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

ci_workflow=".github/workflows/ci.yml"
gcs_workflow=".github/workflows/adr-034-gcs-conformance.yml"
s3_workflow=".github/workflows/s3-conformance.yml"

require_literal() {
  local path="$1"
  local literal="$2"
  local message="$3"
  if ! grep -Fq "${literal}" "${path}"; then
    echo "${message}" >&2
    exit 1
  fi
}

require_literal "${ci_workflow}" 'uv run python -m pytest -v' \
  "Python CI must run the complete configured pytest tree"
if grep -Fq 'pytest tests/integration/test_cli_api.py' "${ci_workflow}"; then
  echo "Python CI must run the configured test tree instead of an integration-file allowlist" >&2
  exit 1
fi

if grep -Fq 'Skip GCS storage conformance suite' "${gcs_workflow}"; then
  echo "GCS conformance prerequisites must fail rather than skip green" >&2
  exit 1
fi
require_literal "${gcs_workflow}" 'Fail on missing GCS conformance configuration' \
  "GCS conformance must name its unconfigured failure step"
require_literal "${gcs_workflow}" 'exit 1' \
  "GCS conformance must fail when provider evidence cannot run"

require_literal "${ci_workflow}" 'sole-writer IAM invariant NOT verified this run' \
  "IAM smoke skips must publish an explicit unverified-evidence warning"
require_literal "${ci_workflow}" 'GITHUB_STEP_SUMMARY' \
  "IAM smoke skip status must remain visible in the workflow summary"

if [[ ! -f "${s3_workflow}" ]]; then
  echo "missing ADR-034 S3 conformance workflow" >&2
  exit 1
fi
require_literal "${s3_workflow}" 's3_backend_satisfies_storage_conformance' \
  "S3 workflow must execute the real-backend CAS conformance test"
require_literal "${s3_workflow}" 'workflow_dispatch:' \
  "S3 certification must be manually runnable before scheduling is configured"
require_literal "${s3_workflow}" 'S3 is NOT certified' \
  "S3 workflow must report the provider as uncertified until evidence passes"
require_literal "${s3_workflow}" 'exit 1' \
  "S3 conformance must fail when provider evidence cannot run"

require_literal "${ci_workflow}" 'bash tools/test_ci_certification_contracts.sh' \
  "normal CI must guard the provider-certification workflow contracts"

echo "CI certification workflow contracts passed"

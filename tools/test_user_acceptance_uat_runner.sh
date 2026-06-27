#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

RUNNER="scripts/run_user_acceptance_pipeline_uat.sh"
CI_WORKFLOW=".github/workflows/ci.yml"
DEFAULT_EVIDENCE_DIR="${REPO_ROOT}/target/uat-evidence"
DEFAULT_EVIDENCE_MARKER="${DEFAULT_EVIDENCE_DIR}/.uat-validation-start"
EXPECTED_API_CODE_VERSION="uat-live"
EXPECTED_API_GIT_SHA="abc123"
EXPECTED_API_IMAGE="us-central1-docker.pkg.dev/example/arco-api:uat-live"

run_with_expected_api() {
  env \
    "ARCO_UAT_EXPECTED_API_CODE_VERSION=${EXPECTED_API_CODE_VERSION}" \
    "ARCO_UAT_EXPECTED_API_GIT_SHA=${EXPECTED_API_GIT_SHA}" \
    "ARCO_UAT_EXPECTED_API_IMAGE=${EXPECTED_API_IMAGE}" \
    "$@"
}

if [[ ! -f "${RUNNER}" ]]; then
  echo "missing ${RUNNER}" >&2
  exit 1
fi
if ! grep -Fq "scripts/run_user_acceptance_pipeline_uat.sh --deterministic" "${CI_WORKFLOW}"; then
  echo "CI workflow must run deterministic user acceptance pipeline UAT" >&2
  exit 1
fi

deterministic_output="$(bash "${RUNNER}" --deterministic --dry-run)"
grep -Fq "cargo test -p arco-integration-tests --test user_acceptance_pipeline" <<<"${deterministic_output}"
if grep -Fq "tools/validate_user_acceptance_evidence.sh" <<<"${deterministic_output}"; then
  echo "deterministic dry-run should not validate live evidence artifacts" >&2
  exit 1
fi
if grep -Fq "cargo clippy" <<<"${deterministic_output}"; then
  echo "deterministic dry-run should not include clippy; use --with-hygiene" >&2
  exit 1
fi
if grep -Fq "cargo fmt" <<<"${deterministic_output}"; then
  echo "deterministic dry-run should not include rustfmt; use --with-hygiene" >&2
  exit 1
fi
if grep -Fq "git diff --check" <<<"${deterministic_output}"; then
  echo "deterministic dry-run should not include git diff checks; use --with-hygiene" >&2
  exit 1
fi

hygiene_output="$(bash "${RUNNER}" --deterministic --with-hygiene --dry-run)"
grep -Fq "cargo test -p arco-integration-tests --test user_acceptance_pipeline" <<<"${hygiene_output}"
grep -Fq "cargo clippy -p arco-integration-tests --test user_acceptance_pipeline -- -D warnings" <<<"${hygiene_output}"
grep -Fq "bash tools/test_user_acceptance_uat_runner.sh" <<<"${hygiene_output}"
grep -Fq "bash tools/test_user_acceptance_evidence_validator.sh" <<<"${hygiene_output}"
grep -Fq "bash tools/test_actionlint_runner.sh" <<<"${hygiene_output}"
grep -Fq "cargo fmt --check" <<<"${hygiene_output}"
grep -Fq "git diff --check" <<<"${hygiene_output}"

status_output="$(bash "${RUNNER}" --status)"
grep -Fq "deterministic: ready" <<<"${status_output}"
grep -Fq "live-durable: missing ARCO_UAT_STORAGE_BUCKET" <<<"${status_output}"
grep -Fq "live-deployed: missing ARCO_UAT_API_URL or ARCO_UAT_CLOUD_RUN_SERVICE/PROJECT_ID" <<<"${status_output}"
grep -Fq "live-deployed-scheduler: not checked (set PROJECT_ID or ARCO_UAT_CLOUD_SCHEDULER_PROJECT for Cloud Run deployed UAT)" <<<"${status_output}"

status_snapshot_path="$(mktemp -d)/snapshots/status.txt"
status_snapshot_output="$(bash "${RUNNER}" --status --status-output "${status_snapshot_path}")"
grep -Fq "deterministic: ready" <<<"${status_snapshot_output}"
grep -Fq "live-deployed: missing ARCO_UAT_API_URL or ARCO_UAT_CLOUD_RUN_SERVICE/PROJECT_ID" "${status_snapshot_path}"
if ! cmp -s <(printf '%s\n' "${status_snapshot_output}") "${status_snapshot_path}"; then
  echo "--status-output should write the exact printed status snapshot" >&2
  exit 1
fi

if bash "${RUNNER}" --status-output >/tmp/arco-uat-status-output-missing.out 2>/tmp/arco-uat-status-output-missing.err; then
  echo "--status-output should require PATH" >&2
  exit 1
fi
grep -Fq "ERROR: --status-output requires PATH" /tmp/arco-uat-status-output-missing.err

ready_status_output="$(
  ARCO_UAT_STORAGE_BUCKET=gs://arco-uat \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  bash "${RUNNER}" --status
)"
grep -Fq "live-durable: ready" <<<"${ready_status_output}"
grep -Fq "live-deployed: ready" <<<"${ready_status_output}"
grep -Fq "live-deployed-scheduler: not checked (set PROJECT_ID or ARCO_UAT_CLOUD_SCHEDULER_PROJECT for Cloud Run deployed UAT)" <<<"${ready_status_output}"
grep -Fq "ARCO_UAT_EVIDENCE_DIR=target/uat-evidence" <<<"${ready_status_output}"

strict_missing_cloud_bin="$(mktemp -d)"
cat >"${strict_missing_cloud_bin}/curl" <<'SH'
#!/usr/bin/env bash
cat <<'JSON'
{
  "service": "arco-api-dev",
  "packageVersion": "0.2.1",
  "codeVersion": "uat-live",
  "gitSha": "abc123",
  "image": "us-central1-docker.pkg.dev/example/arco-api:uat-live",
  "cloudRunRevision": "arco-api-dev-00001-test"
}
JSON
SH
chmod +x "${strict_missing_cloud_bin}/curl"
if PATH="${strict_missing_cloud_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  run_with_expected_api bash "${RUNNER}" --require-live-deployed-ready >/tmp/arco-uat-strict-missing-cloud.out 2>/tmp/arco-uat-strict-missing-cloud.err; then
  echo "--require-live-deployed-ready should require deployed cloud metadata checks" >&2
  exit 1
fi
grep -Fq "live-deployed: not ready (flow scheduler readiness not checked)" /tmp/arco-uat-strict-missing-cloud.out
grep -Fq "live-deployed readiness check failed" /tmp/arco-uat-strict-missing-cloud.err

ready_status_scheduler_bin="$(mktemp -d)"
cat >"${ready_status_scheduler_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" == "scheduler jobs describe" ]]; then
  case "${4:-}" in
  arco-flow-dispatcher-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  arco-flow-sweeper-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  *)
    exit 1
    ;;
  esac
  exit 0
fi
exit 1
SH
chmod +x "${ready_status_scheduler_bin}/gcloud"

proxy_status_output="$(
  PATH="${ready_status_scheduler_bin}:${PATH}" \
  ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev \
  PROJECT_ID=arco-testing-20260320 \
  bash "${RUNNER}" --status
)"
grep -Fq "live-deployed: ready" <<<"${proxy_status_output}"
grep -Fq "ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev" <<<"${proxy_status_output}"
grep -Fq "PROJECT_ID=arco-testing-20260320" <<<"${proxy_status_output}"
grep -Fq "REGION=us-central1" <<<"${proxy_status_output}"
grep -Fq "live-deployed-scheduler: ready for arco-testing-20260320/us-central1 env=dev" <<<"${proxy_status_output}"
grep -Fq "arco-flow-dispatcher-run-dev=ENABLED" <<<"${proxy_status_output}"
grep -Fq "arco-flow-sweeper-run-dev=ENABLED" <<<"${proxy_status_output}"

scheduler_project_status_output="$(
  PATH="${ready_status_scheduler_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  ARCO_UAT_CLOUD_SCHEDULER_PROJECT=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  bash "${RUNNER}" --status
)"
grep -Fq "live-deployed: ready" <<<"${scheduler_project_status_output}"
grep -Fq "live-deployed-scheduler: ready for arco-testing-20260320/us-central1 env=dev" <<<"${scheduler_project_status_output}"
grep -Fq "arco-flow-dispatcher-run-dev=ENABLED" <<<"${scheduler_project_status_output}"
grep -Fq "arco-flow-sweeper-run-dev=ENABLED" <<<"${scheduler_project_status_output}"
grep -Fq "ARCO_UAT_CLOUD_SCHEDULER_PROJECT=arco-testing-20260320" <<<"${scheduler_project_status_output}"
grep -Fq "ARCO_UAT_ENVIRONMENT=dev" <<<"${scheduler_project_status_output}"

strict_ready_bin="$(mktemp -d)"
cat >"${strict_ready_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" == "scheduler jobs describe" ]]; then
  case "${4:-}" in
  arco-flow-dispatcher-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  arco-flow-sweeper-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  *)
    exit 1
    ;;
  esac
  exit 0
fi
if [[ "${1:-} ${2:-} ${3:-}" == "run services describe" ]]; then
  for arg in "$@"; do
    if [[ "$arg" == "--format=value(metadata.labels.arco_deploy_owner)" ]]; then
      printf 'uat-session\n'
      exit 0
    fi
  done
  case "${4:-}" in
  arco-flow-worker-dev)
    cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_FLOW_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_FLOW_WORKSPACE_ID","value":"arco-uat-workspace"}]}]}}}}
JSON
    ;;
  *)
    cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_WORKSPACE_ID","value":"arco-uat-workspace"}]}]}}}}
JSON
    ;;
  esac
  exit 0
fi
exit 1
SH
chmod +x "${strict_ready_bin}/gcloud"
cat >"${strict_ready_bin}/jq" <<'SH'
#!/usr/bin/env bash
python3 -c '
import json
import sys

args=sys.argv[1:]
doc=json.load(sys.stdin)

if "--arg" in args:
    try:
        name=args[args.index("name") + 1]
    except (ValueError, IndexError):
        sys.exit(2)
    for item in doc["spec"]["template"]["spec"]["containers"][0].get("env", []):
        if item.get("name") == name:
            print(item.get("value", ""))
            break
    sys.exit(0)

expr=" ".join(arg for arg in args if not arg.startswith("-"))
if ".state" in expr:
    print(doc.get("state", ""))
elif ".httpTarget.uri" in expr:
    print(doc.get("httpTarget", {}).get("uri", ""))
elif ".httpTarget.oidcToken.audience" in expr:
    print(doc.get("httpTarget", {}).get("oidcToken", {}).get("audience", ""))
elif ".httpTarget.oidcToken.serviceAccountEmail" in expr:
    print(doc.get("httpTarget", {}).get("oidcToken", {}).get("serviceAccountEmail", ""))
elif ".httpTarget.httpMethod" in expr:
    print(doc.get("httpTarget", {}).get("httpMethod", ""))
elif "-e" in args:
    sys.exit(0)
' "$@"
SH
chmod +x "${strict_ready_bin}/jq"
cat >"${strict_ready_bin}/curl" <<'SH'
#!/usr/bin/env bash
cat <<'JSON'
{
  "service": "arco-api-dev",
  "packageVersion": "0.2.1",
  "codeVersion": "uat-live",
  "gitSha": "abc123",
  "image": "us-central1-docker.pkg.dev/example/arco-api:uat-live",
  "cloudRunRevision": "arco-api-dev-00001-test"
}
JSON
SH
chmod +x "${strict_ready_bin}/curl"
strict_ready_status_output="$(
  PATH="${strict_ready_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  PROJECT_ID=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  ARCO_DEPLOY_OWNER=uat-session \
  run_with_expected_api bash "${RUNNER}" --require-live-deployed-ready
)"
grep -Fq "live-deployed: ready" <<<"${strict_ready_status_output}"
grep -Fq "live-deployed-scheduler: ready for arco-testing-20260320/us-central1 env=dev" <<<"${strict_ready_status_output}"
grep -Fq "live-deployed-cloud-run-owner: ready for arco-testing-20260320/us-central1 env=dev" <<<"${strict_ready_status_output}"
grep -Fq "live-deployed-flow-scope: ready for arco-testing-20260320/us-central1 env=dev expected tenant=arco-uat-tenant workspace=arco-uat-workspace" <<<"${strict_ready_status_output}"

status_scheduler_bin="$(mktemp -d)"
cat >"${status_scheduler_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" != "scheduler jobs describe" ]]; then
  exit 1
fi
case "${4:-}" in
arco-flow-dispatcher-run-dev)
  cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
  ;;
arco-flow-sweeper-run-dev)
  cat <<'JSON'
{"state":"PAUSED","httpTarget":{"uri":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
  ;;
*)
  exit 1
  ;;
esac
SH
chmod +x "${status_scheduler_bin}/gcloud"
paused_scheduler_status_output="$(
  PATH="${status_scheduler_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  ARCO_UAT_CLOUD_SCHEDULER_PROJECT=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  bash "${RUNNER}" --status
)"
grep -Fq "live-deployed: not ready (flow scheduler jobs are not ENABLED)" <<<"${paused_scheduler_status_output}"
grep -Fq "live-deployed-scheduler: not ready for arco-testing-20260320/us-central1 env=dev" <<<"${paused_scheduler_status_output}"
grep -Fq "arco-flow-dispatcher-run-dev=ENABLED" <<<"${paused_scheduler_status_output}"
grep -Fq "arco-flow-sweeper-run-dev=PAUSED (expected ENABLED)" <<<"${paused_scheduler_status_output}"

target_mismatch_scheduler_bin="$(mktemp -d)"
cat >"${target_mismatch_scheduler_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" == "scheduler jobs describe" ]]; then
  format=""
  for arg in "$@"; do
    case "$arg" in
    --format=*) format="${arg#--format=}" ;;
    esac
  done
  if [[ "$format" == "value(state)" ]]; then
    printf 'ENABLED\n'
    exit 0
  fi
  case "${4:-}" in
  arco-flow-dispatcher-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  arco-flow-sweeper-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  *)
    exit 1
    ;;
  esac
  exit 0
fi
if [[ "${1:-} ${2:-} ${3:-}" == "run services describe" ]]; then
  for arg in "$@"; do
    if [[ "$arg" == "--format=value(metadata.labels.arco_deploy_owner)" ]]; then
      printf 'uat-session\n'
      exit 0
    fi
  done
  case "${4:-}" in
  arco-flow-worker-dev)
    cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_FLOW_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_FLOW_WORKSPACE_ID","value":"arco-uat-workspace"}]}]}}}}
JSON
    ;;
  *)
    cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_WORKSPACE_ID","value":"arco-uat-workspace"}]}]}}}}
JSON
    ;;
  esac
  exit 0
fi
exit 1
SH
chmod +x "${target_mismatch_scheduler_bin}/gcloud"
cat >"${target_mismatch_scheduler_bin}/jq" <<'SH'
#!/usr/bin/env bash
python3 -c '
import json
import sys

args=sys.argv[1:]
doc=json.load(sys.stdin)

if "--arg" in args:
    try:
        name=args[args.index("name") + 1]
    except (ValueError, IndexError):
        sys.exit(2)
    for item in doc["spec"]["template"]["spec"]["containers"][0].get("env", []):
        if item.get("name") == name:
            print(item.get("value", ""))
            break
    sys.exit(0)

expr=" ".join(arg for arg in args if not arg.startswith("-"))
if ".state" in expr:
    print(doc.get("state", ""))
elif ".httpTarget.uri" in expr:
    print(doc.get("httpTarget", {}).get("uri", ""))
elif ".httpTarget.oidcToken.audience" in expr:
    print(doc.get("httpTarget", {}).get("oidcToken", {}).get("audience", ""))
elif ".httpTarget.oidcToken.serviceAccountEmail" in expr:
    print(doc.get("httpTarget", {}).get("oidcToken", {}).get("serviceAccountEmail", ""))
elif ".httpTarget.httpMethod" in expr:
    print(doc.get("httpTarget", {}).get("httpMethod", ""))
elif "-e" in args:
    sys.exit(0)
' "$@"
SH
chmod +x "${target_mismatch_scheduler_bin}/jq"
cat >"${target_mismatch_scheduler_bin}/curl" <<'SH'
#!/usr/bin/env bash
cat <<'JSON'
{
  "service": "arco-api-dev",
  "packageVersion": "0.2.1",
  "codeVersion": "uat-live",
  "gitSha": "abc123",
  "image": "us-central1-docker.pkg.dev/example/arco-api:uat-live",
  "cloudRunRevision": "arco-api-dev-00001-test"
}
JSON
SH
chmod +x "${target_mismatch_scheduler_bin}/curl"
target_mismatch_scheduler_status_output="$(
  PATH="${target_mismatch_scheduler_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  ARCO_UAT_CLOUD_SCHEDULER_PROJECT=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  bash "${RUNNER}" --status
)"
grep -Fq "live-deployed: not ready (flow scheduler target metadata mismatch)" <<<"${target_mismatch_scheduler_status_output}"
grep -Fq "live-deployed-scheduler: not ready for arco-testing-20260320/us-central1 env=dev" <<<"${target_mismatch_scheduler_status_output}"
grep -Fq "arco-flow-dispatcher-run-dev=ENABLED uri=https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run audience=https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run (expected https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app)" <<<"${target_mismatch_scheduler_status_output}"

if PATH="${target_mismatch_scheduler_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  ARCO_UAT_CLOUD_SCHEDULER_PROJECT=arco-testing-20260320 \
  PROJECT_ID=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  ARCO_DEPLOY_OWNER=uat-session \
  run_with_expected_api bash "${RUNNER}" --require-live-deployed-ready >/tmp/arco-uat-strict-status.out 2>/tmp/arco-uat-strict-status.err; then
  echo "--require-live-deployed-ready should fail when deployed status is not ready" >&2
  exit 1
fi
grep -Fq "live-deployed: not ready (flow scheduler target metadata mismatch)" /tmp/arco-uat-strict-status.out
grep -Fq "live-deployed readiness check failed" /tmp/arco-uat-strict-status.err

combined_scheduler_mismatch_bin="$(mktemp -d)"
cat >"${combined_scheduler_mismatch_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" != "scheduler jobs describe" ]]; then
  exit 1
fi
case "${4:-}" in
arco-flow-dispatcher-run-dev)
  cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
  ;;
arco-flow-sweeper-run-dev)
  cat <<'JSON'
{"state":"PAUSED","httpTarget":{"uri":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
  ;;
*)
  exit 1
  ;;
esac
SH
chmod +x "${combined_scheduler_mismatch_bin}/gcloud"
combined_scheduler_status_output="$(
  PATH="${combined_scheduler_mismatch_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  ARCO_UAT_CLOUD_SCHEDULER_PROJECT=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  bash "${RUNNER}" --status
)"
grep -Fq "live-deployed: not ready (flow scheduler jobs and target metadata are not ready)" <<<"${combined_scheduler_status_output}"
grep -Fq "arco-flow-dispatcher-run-dev=ENABLED uri=https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run audience=https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run (expected https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app)" <<<"${combined_scheduler_status_output}"
grep -Fq "arco-flow-sweeper-run-dev=PAUSED (expected ENABLED)" <<<"${combined_scheduler_status_output}"

combined_deployed_blockers_bin="$(mktemp -d)"
cat >"${combined_deployed_blockers_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" == "scheduler jobs describe" ]]; then
  case "${4:-}" in
  arco-flow-dispatcher-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  arco-flow-sweeper-run-dev)
    cat <<'JSON'
{"state":"PAUSED","httpTarget":{"uri":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  *)
    exit 1
    ;;
  esac
  exit 0
fi
if [[ "${1:-} ${2:-} ${3:-}" == "run services describe" ]]; then
  for arg in "$@"; do
    if [[ "$arg" == "--format=value(metadata.labels.arco_deploy_owner)" ]]; then
      printf 'uat-session\n'
      exit 0
    fi
  done
  case "${4:-}" in
  arco-flow-compactor-dev | arco-flow-dispatcher-dev | arco-flow-sweeper-dev)
    cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_TENANT_ID","value":"tenant-proof2-20260604"},{"name":"ARCO_WORKSPACE_ID","value":"workspace-proof2-20260604"}]}]}}}}
JSON
    ;;
  arco-flow-worker-dev)
    cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_FLOW_TENANT_ID","value":"tenant-proof2-20260604"},{"name":"ARCO_FLOW_WORKSPACE_ID","value":"workspace-proof2-20260604"}]}]}}}}
JSON
    ;;
  *)
    cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_WORKSPACE_ID","value":"arco-uat-workspace"}]}]}}}}
JSON
    ;;
  esac
  exit 0
fi
exit 1
SH
chmod +x "${combined_deployed_blockers_bin}/gcloud"
cat >"${combined_deployed_blockers_bin}/jq" <<'SH'
#!/usr/bin/env bash
python3 -c '
import json
import sys

args=sys.argv[1:]
doc=json.load(sys.stdin)

if "--arg" in args:
    try:
        name=args[args.index("name") + 1]
    except (ValueError, IndexError):
        sys.exit(2)
    for item in doc["spec"]["template"]["spec"]["containers"][0].get("env", []):
        if item.get("name") == name:
            print(item.get("value", ""))
            break
    sys.exit(0)

expr=" ".join(arg for arg in args if not arg.startswith("-"))
if ".state" in expr:
    print(doc.get("state", ""))
elif ".httpTarget.uri" in expr:
    print(doc.get("httpTarget", {}).get("uri", ""))
elif ".httpTarget.oidcToken.audience" in expr:
    print(doc.get("httpTarget", {}).get("oidcToken", {}).get("audience", ""))
elif ".httpTarget.oidcToken.serviceAccountEmail" in expr:
    print(doc.get("httpTarget", {}).get("oidcToken", {}).get("serviceAccountEmail", ""))
elif ".httpTarget.httpMethod" in expr:
    print(doc.get("httpTarget", {}).get("httpMethod", ""))
elif "-e" in args:
    sys.exit(0)
' "$@"
SH
chmod +x "${combined_deployed_blockers_bin}/jq"
combined_deployed_blockers_status_output="$(
  PATH="${combined_deployed_blockers_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  PROJECT_ID=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  ARCO_DEPLOY_OWNER=uat-session \
  bash "${RUNNER}" --status
)"
grep -Fq "live-deployed: not ready (flow scheduler jobs and target metadata are not ready; flow service tenant/workspace mismatch)" <<<"${combined_deployed_blockers_status_output}"
grep -Fq "arco-flow-dispatcher-run-dev=ENABLED uri=https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run audience=https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run (expected https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app)" <<<"${combined_deployed_blockers_status_output}"
grep -Fq "arco-flow-dispatcher-dev=tenant-proof2-20260604/workspace-proof2-20260604 (expected arco-uat-tenant/arco-uat-workspace)" <<<"${combined_deployed_blockers_status_output}"

owner_status_bin="$(mktemp -d)"
cat >"${owner_status_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" == "scheduler jobs describe" ]]; then
  case "${4:-}" in
  arco-flow-dispatcher-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  arco-flow-sweeper-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  *)
    exit 1
    ;;
  esac
  exit 0
fi
if [[ "${1:-} ${2:-} ${3:-}" == "run services describe" ]]; then
  case "${4:-}" in
  arco-api-dev)
    printf 'other-session\n'
    ;;
  arco-compactor-dev | arco-flow-compactor-dev | arco-flow-dispatcher-dev | arco-flow-sweeper-dev | arco-flow-timer-ingest-dev | arco-flow-worker-dev)
    printf 'uat-session\n'
    ;;
  *)
    exit 1
    ;;
  esac
  exit 0
fi
exit 1
SH
chmod +x "${owner_status_bin}/gcloud"
owner_mismatch_status_output="$(
  PATH="${owner_status_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  PROJECT_ID=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  ARCO_DEPLOY_OWNER=uat-session \
  bash "${RUNNER}" --status
)"
grep -Fq "live-deployed: not ready (Cloud Run deploy owner mismatch)" <<<"${owner_mismatch_status_output}"
grep -Fq "live-deployed-cloud-run-owner: not ready for arco-testing-20260320/us-central1 env=dev" <<<"${owner_mismatch_status_output}"
grep -Fq "arco-api-dev=other-session (expected uat-session)" <<<"${owner_mismatch_status_output}"
grep -Fq "arco-flow-worker-dev=uat-session" <<<"${owner_mismatch_status_output}"

scope_status_bin="$(mktemp -d)"
cat >"${scope_status_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" == "scheduler jobs describe" ]]; then
  case "${4:-}" in
  arco-flow-dispatcher-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  arco-flow-sweeper-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  *)
    exit 1
    ;;
  esac
  exit 0
fi
if [[ "${1:-} ${2:-} ${3:-}" == "run services describe" ]]; then
  for arg in "$@"; do
    if [[ "$arg" == "--format=value(metadata.labels.arco_deploy_owner)" ]]; then
      printf 'uat-session\n'
      exit 0
    fi
  done
  case "${4:-}" in
  arco-flow-dispatcher-dev)
    cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_TENANT_ID","value":"tenant-proof2-20260604"},{"name":"ARCO_WORKSPACE_ID","value":"workspace-proof2-20260604"}]}]}}}}
JSON
    ;;
  arco-flow-worker-dev)
    cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_FLOW_TENANT_ID","value":"tenant-proof2-20260604"},{"name":"ARCO_FLOW_WORKSPACE_ID","value":"workspace-proof2-20260604"}]}]}}}}
JSON
    ;;
  *)
    cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_WORKSPACE_ID","value":"arco-uat-workspace"}]}]}}}}
JSON
    ;;
  esac
  exit 0
fi
exit 1
SH
chmod +x "${scope_status_bin}/gcloud"
cat >"${scope_status_bin}/jq" <<'SH'
#!/usr/bin/env bash
python3 -c '
import json
import sys

args=sys.argv[1:]
doc=json.load(sys.stdin)

if "--arg" in args:
    try:
        name=args[args.index("name") + 1]
    except (ValueError, IndexError):
        sys.exit(2)
    for item in doc["spec"]["template"]["spec"]["containers"][0].get("env", []):
        if item.get("name") == name:
            print(item.get("value", ""))
            break
    sys.exit(0)

expr=" ".join(arg for arg in args if not arg.startswith("-"))
if ".state" in expr:
    print(doc.get("state", ""))
elif ".httpTarget.uri" in expr:
    print(doc.get("httpTarget", {}).get("uri", ""))
elif ".httpTarget.oidcToken.audience" in expr:
    print(doc.get("httpTarget", {}).get("oidcToken", {}).get("audience", ""))
elif ".httpTarget.oidcToken.serviceAccountEmail" in expr:
    print(doc.get("httpTarget", {}).get("oidcToken", {}).get("serviceAccountEmail", ""))
elif ".httpTarget.httpMethod" in expr:
    print(doc.get("httpTarget", {}).get("httpMethod", ""))
elif "-e" in args:
    sys.exit(0)
' "$@"
SH
chmod +x "${scope_status_bin}/jq"
scope_mismatch_status_output="$(
  PATH="${scope_status_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  PROJECT_ID=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  ARCO_DEPLOY_OWNER=uat-session \
  bash "${RUNNER}" --status
)"
grep -Fq "live-deployed: not ready (flow service tenant/workspace mismatch)" <<<"${scope_mismatch_status_output}"
grep -Fq "live-deployed-flow-scope: not ready for arco-testing-20260320/us-central1 env=dev expected tenant=arco-uat-tenant workspace=arco-uat-workspace" <<<"${scope_mismatch_status_output}"
grep -Fq "arco-flow-dispatcher-dev=tenant-proof2-20260604/workspace-proof2-20260604 (expected arco-uat-tenant/arco-uat-workspace)" <<<"${scope_mismatch_status_output}"
grep -Fq "arco-flow-worker-dev=tenant-proof2-20260604/workspace-proof2-20260604 (expected arco-uat-tenant/arco-uat-workspace)" <<<"${scope_mismatch_status_output}"

if bash "${RUNNER}" --live-durable --dry-run >/tmp/arco-uat-live-durable.out 2>/tmp/arco-uat-live-durable.err; then
  echo "live durable dry-run should require ARCO_UAT_STORAGE_BUCKET" >&2
  exit 1
fi
grep -Fq "ARCO_UAT_STORAGE_BUCKET is required" /tmp/arco-uat-live-durable.err

durable_output="$(
  ARCO_UAT_STORAGE_BUCKET=gs://arco-uat \
  bash "${RUNNER}" --live-durable --dry-run
)"
grep -Fq -- "--ignored live_user_acceptance_pipeline_runs_against_durable_storage" <<<"${durable_output}"
grep -Fq "ARCO_UAT_EVIDENCE_DIR=${DEFAULT_EVIDENCE_DIR}" <<<"${durable_output}"
grep -Fq "mkdir -p ${DEFAULT_EVIDENCE_DIR}" <<<"${durable_output}"
grep -Fq "touch ${DEFAULT_EVIDENCE_MARKER}" <<<"${durable_output}"
grep -Fq "tools/validate_user_acceptance_evidence.sh --newer-than ${DEFAULT_EVIDENCE_MARKER} --require-kind durable_storage ${DEFAULT_EVIDENCE_DIR}" <<<"${durable_output}"

if run_with_expected_api bash "${RUNNER}" --live-deployed --dry-run >/tmp/arco-uat-live-deployed.out 2>/tmp/arco-uat-live-deployed.err; then
  echo "live deployed dry-run should require ARCO_UAT_API_URL or Cloud Run proxy config" >&2
  exit 1
fi
grep -Fq "ARCO_UAT_API_URL or ARCO_UAT_CLOUD_RUN_SERVICE/PROJECT_ID is required" /tmp/arco-uat-live-deployed.err

if ARCO_UAT_API_URL=https://arco.acceptance.example \
  bash "${RUNNER}" --live-deployed --dry-run >/tmp/arco-uat-live-deployed-missing-provenance.out 2>/tmp/arco-uat-live-deployed-missing-provenance.err; then
  echo "live deployed dry-run should require expected deployed API provenance" >&2
  exit 1
fi
grep -Fq "ARCO_UAT_EXPECTED_API_CODE_VERSION" /tmp/arco-uat-live-deployed-missing-provenance.err
grep -Fq "ARCO_UAT_EXPECTED_API_GIT_SHA" /tmp/arco-uat-live-deployed-missing-provenance.err
grep -Fq "ARCO_UAT_EXPECTED_API_IMAGE" /tmp/arco-uat-live-deployed-missing-provenance.err

deployed_output="$(
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  run_with_expected_api bash "${RUNNER}" --live-deployed --dry-run
)"
grep -Fq "curl -fsS https://arco.acceptance.example/version" <<<"${deployed_output}"
grep -Fq -- "--ignored live_deployed_user_acceptance_pipeline_runs_through_api_and_workers" <<<"${deployed_output}"
grep -Fq "ARCO_UAT_EVIDENCE_DIR=${DEFAULT_EVIDENCE_DIR}" <<<"${deployed_output}"
grep -Fq "mkdir -p ${DEFAULT_EVIDENCE_DIR}" <<<"${deployed_output}"
grep -Fq "touch ${DEFAULT_EVIDENCE_MARKER}" <<<"${deployed_output}"
grep -Fq "tools/validate_user_acceptance_evidence.sh --newer-than ${DEFAULT_EVIDENCE_MARKER} --require-kind deployed_api_worker --expect-api-code-version ${EXPECTED_API_CODE_VERSION} --expect-api-git-sha ${EXPECTED_API_GIT_SHA} --expect-api-image ${EXPECTED_API_IMAGE} ${DEFAULT_EVIDENCE_DIR}" <<<"${deployed_output}"

direct_project_output="$(
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  ARCO_UAT_CLOUD_RUN_PROJECT_ID=arco-testing-20260320 \
  run_with_expected_api bash "${RUNNER}" --live-deployed --dry-run
)"
grep -Fq "ARCO_UAT_API_ACCESS_MODE=direct-url" <<<"${direct_project_output}"
grep -Fq "ARCO_UAT_CLOUD_RUN_PROJECT_ID=arco-testing-20260320" <<<"${direct_project_output}"
grep -Fq "ARCO_UAT_CLOUD_RUN_REGION=us-central1" <<<"${direct_project_output}"

deployed_preflight_output="$(
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  run_with_expected_api bash "${RUNNER}" --live-deployed --preflight-only --dry-run
)"
grep -Fq "curl -fsS https://arco.acceptance.example/version" <<<"${deployed_preflight_output}"
if grep -Fq "cargo test" <<<"${deployed_preflight_output}"; then
  echo "deployed preflight-only dry-run should not invoke Cargo" >&2
  exit 1
fi
if grep -Fq "mkdir -p ${DEFAULT_EVIDENCE_DIR}" <<<"${deployed_preflight_output}"; then
  echo "deployed preflight-only dry-run should not create evidence directories" >&2
  exit 1
fi
if grep -Fq "tools/validate_user_acceptance_evidence.sh" <<<"${deployed_preflight_output}"; then
  echo "deployed preflight-only dry-run should not validate evidence artifacts" >&2
  exit 1
fi

scheduler_preflight_output="$(
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  PROJECT_ID=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  run_with_expected_api bash "${RUNNER}" --live-deployed --preflight-only --dry-run
)"
grep -Fq "curl -fsS https://arco.acceptance.example/version" <<<"${scheduler_preflight_output}"
grep -Fq "gcloud scheduler jobs describe arco-flow-dispatcher-run-dev --project=arco-testing-20260320 --location=us-central1 --format=json\\(state\\,httpTarget\\)" <<<"${scheduler_preflight_output}"
grep -Fq "gcloud scheduler jobs describe arco-flow-sweeper-run-dev --project=arco-testing-20260320 --location=us-central1 --format=json\\(state\\,httpTarget\\)" <<<"${scheduler_preflight_output}"
if grep -Fq "cargo test" <<<"${scheduler_preflight_output}"; then
  echo "scheduler preflight-only dry-run should not invoke Cargo" >&2
  exit 1
fi

proxy_deployed_output="$(
  ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev \
  PROJECT_ID=arco-testing-20260320 \
  ARCO_UAT_CLOUD_RUN_PORT=18087 \
  run_with_expected_api bash "${RUNNER}" --live-deployed --dry-run
)"
grep -Fq "gcloud run services proxy arco-api-dev --project=arco-testing-20260320 --region=us-central1 --port=18087 --quiet" <<<"${proxy_deployed_output}"
grep -Fq "curl -fsS http://127.0.0.1:18087/version" <<<"${proxy_deployed_output}"
grep -Fq "ARCO_UAT_API_URL=http://127.0.0.1:18087" <<<"${proxy_deployed_output}"
grep -Fq "ARCO_UAT_API_ACCESS_MODE=cloud-run-proxy" <<<"${proxy_deployed_output}"
grep -Fq "ARCO_UAT_API_INGRESS_MODE=internal-only" <<<"${proxy_deployed_output}"
grep -Fq "ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev" <<<"${proxy_deployed_output}"
grep -Fq "ARCO_UAT_CLOUD_RUN_PROJECT_ID=arco-testing-20260320" <<<"${proxy_deployed_output}"
grep -Fq "ARCO_UAT_CLOUD_RUN_REGION=us-central1" <<<"${proxy_deployed_output}"

proxy_preflight_output="$(
  ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev \
  PROJECT_ID=arco-testing-20260320 \
  ARCO_UAT_CLOUD_RUN_PORT=18087 \
  run_with_expected_api bash "${RUNNER}" --live-deployed --preflight-only --dry-run
)"
grep -Fq "gcloud run services proxy arco-api-dev --project=arco-testing-20260320 --region=us-central1 --port=18087 --quiet" <<<"${proxy_preflight_output}"
grep -Fq "curl -fsS http://127.0.0.1:18087/version" <<<"${proxy_preflight_output}"
if grep -Fq "cargo test" <<<"${proxy_preflight_output}"; then
  echo "proxy preflight-only dry-run should not invoke Cargo" >&2
  exit 1
fi
if grep -Fq "tools/validate_user_acceptance_evidence.sh" <<<"${proxy_preflight_output}"; then
  echo "proxy preflight-only dry-run should not validate evidence artifacts" >&2
  exit 1
fi

if ARCO_UAT_API_URL=https://arco.acceptance.example \
  ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev \
  PROJECT_ID=arco-testing-20260320 \
  run_with_expected_api bash "${RUNNER}" --live-deployed --dry-run >/tmp/arco-uat-live-deployed-ambiguous.out 2>/tmp/arco-uat-live-deployed-ambiguous.err; then
  echo "live deployed dry-run should reject ambiguous direct URL plus Cloud Run proxy config" >&2
  exit 1
fi
grep -Fq "Set either ARCO_UAT_API_URL or ARCO_UAT_CLOUD_RUN_SERVICE, not both" /tmp/arco-uat-live-deployed-ambiguous.err

fake_bin="$(mktemp -d)"
fake_evidence_dir="$(mktemp -d)/evidence"
cat >"${fake_bin}/curl" <<'SH'
#!/usr/bin/env bash
exit 22
SH
chmod +x "${fake_bin}/curl"
cat >"${fake_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" == "run services describe" ]]; then
  for arg in "$@"; do
    if [[ "$arg" == "--format=value(metadata.labels.arco_deploy_owner)" ]]; then
      printf 'uat-session\n'
      exit 0
    fi
  done
  cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_WORKSPACE_ID","value":"arco-uat-workspace"},{"name":"ARCO_FLOW_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_FLOW_WORKSPACE_ID","value":"arco-uat-workspace"}]}]}}}}
JSON
  exit 0
fi
exit 1
SH
chmod +x "${fake_bin}/gcloud"
if PATH="${fake_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  PROJECT_ID=arco-testing-20260320 \
  ARCO_DEPLOY_OWNER=uat-session \
  ARCO_UAT_EVIDENCE_DIR="${fake_evidence_dir}" \
  ARCO_UAT_PREFLIGHT_TIMEOUT_SECS=1 \
  ARCO_UAT_PREFLIGHT_INTERVAL_SECS=0 \
  run_with_expected_api bash "${RUNNER}" --live-deployed >/tmp/arco-uat-live-deployed-preflight.out 2>/tmp/arco-uat-live-deployed-preflight.err; then
  echo "live deployed run should fail before Cargo when /version preflight fails" >&2
  exit 1
fi
grep -Fq "deployed API /version preflight failed" /tmp/arco-uat-live-deployed-preflight.err
if grep -Fq "cargo test" /tmp/arco-uat-live-deployed-preflight.out; then
  echo "live deployed preflight failure should not run Cargo" >&2
  exit 1
fi
if [[ -e "${fake_evidence_dir}/.uat-validation-start" ]]; then
  echo "live deployed preflight failure should not create an evidence marker" >&2
  exit 1
fi

scheduler_bin="$(mktemp -d)"
scheduler_evidence_dir="$(mktemp -d)/evidence"
cat >"${scheduler_bin}/curl" <<'SH'
#!/usr/bin/env bash
cat <<'JSON'
{
  "service": "arco-api-dev",
  "packageVersion": "0.2.1",
  "codeVersion": "uat-live",
  "gitSha": "abc123",
  "image": "us-central1-docker.pkg.dev/example/arco-api:uat-live",
  "cloudRunRevision": "arco-api-dev-00001-test"
}
JSON
SH
chmod +x "${scheduler_bin}/curl"
cat >"${scheduler_bin}/jq" <<'SH'
#!/usr/bin/env bash
python3 -c '
import json
import sys

args=sys.argv[1:]
doc=json.load(sys.stdin)

if "--arg" in args:
    try:
        name=args[args.index("name") + 1]
    except (ValueError, IndexError):
        sys.exit(2)
    for item in doc["spec"]["template"]["spec"]["containers"][0].get("env", []):
        if item.get("name") == name:
            print(item.get("value", ""))
            break
    sys.exit(0)

expr=" ".join(arg for arg in args if not arg.startswith("-"))
if ".state" in expr:
    print(doc.get("state", ""))
elif ".httpTarget.uri" in expr:
    print(doc.get("httpTarget", {}).get("uri", ""))
elif ".httpTarget.oidcToken.audience" in expr:
    print(doc.get("httpTarget", {}).get("oidcToken", {}).get("audience", ""))
elif ".httpTarget.oidcToken.serviceAccountEmail" in expr:
    print(doc.get("httpTarget", {}).get("oidcToken", {}).get("serviceAccountEmail", ""))
elif ".httpTarget.httpMethod" in expr:
    print(doc.get("httpTarget", {}).get("httpMethod", ""))
elif "-e" in args:
    sys.exit(0)
' "$@"
SH
chmod +x "${scheduler_bin}/jq"
cat >"${scheduler_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" == "run services describe" ]]; then
  for arg in "$@"; do
    if [[ "$arg" == "--format=value(metadata.labels.arco_deploy_owner)" ]]; then
      printf 'uat-session\n'
      exit 0
    fi
  done
  cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_WORKSPACE_ID","value":"arco-uat-workspace"},{"name":"ARCO_FLOW_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_FLOW_WORKSPACE_ID","value":"arco-uat-workspace"}]}]}}}}
JSON
  exit 0
fi
if [[ "${1:-} ${2:-} ${3:-}" == "scheduler jobs describe" ]]; then
  cat <<'JSON'
{"state":"PAUSED","httpTarget":{"uri":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
  exit 0
fi
exit 1
SH
chmod +x "${scheduler_bin}/gcloud"
if PATH="${scheduler_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  PROJECT_ID=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  ARCO_DEPLOY_OWNER=uat-session \
  ARCO_UAT_EVIDENCE_DIR="${scheduler_evidence_dir}" \
  run_with_expected_api bash "${RUNNER}" --live-deployed >/tmp/arco-uat-live-deployed-scheduler.out 2>/tmp/arco-uat-live-deployed-scheduler.err; then
  echo "live deployed run should fail before Cargo when flow scheduler jobs are paused" >&2
  exit 1
fi
grep -Fq "deployed flow scheduler preflight failed: arco-flow-dispatcher-run-dev state is PAUSED; expected ENABLED" /tmp/arco-uat-live-deployed-scheduler.err
if grep -Fq "cargo test" /tmp/arco-uat-live-deployed-scheduler.out; then
  echo "live deployed scheduler preflight failure should not run Cargo" >&2
  exit 1
fi
if [[ -e "${scheduler_evidence_dir}/.uat-validation-start" ]]; then
  echo "live deployed scheduler preflight failure should not create an evidence marker" >&2
  exit 1
fi

scheduler_target_bin="$(mktemp -d)"
scheduler_target_evidence_dir="$(mktemp -d)/evidence"
cat >"${scheduler_target_bin}/curl" <<'SH'
#!/usr/bin/env bash
cat <<'JSON'
{
  "service": "arco-api-dev",
  "packageVersion": "0.2.1",
  "codeVersion": "uat-live",
  "gitSha": "abc123",
  "image": "us-central1-docker.pkg.dev/example/arco-api:uat-live",
  "cloudRunRevision": "arco-api-dev-00001-test"
}
JSON
SH
chmod +x "${scheduler_target_bin}/curl"
cat >"${scheduler_target_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" == "run services describe" ]]; then
  for arg in "$@"; do
    if [[ "$arg" == "--format=value(metadata.labels.arco_deploy_owner)" ]]; then
      printf 'uat-session\n'
      exit 0
    fi
  done
  cat <<'JSON'
{"spec":{"template":{"spec":{"containers":[{"env":[{"name":"ARCO_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_WORKSPACE_ID","value":"arco-uat-workspace"},{"name":"ARCO_FLOW_TENANT_ID","value":"arco-uat-tenant"},{"name":"ARCO_FLOW_WORKSPACE_ID","value":"arco-uat-workspace"}]}]}}}}
JSON
  exit 0
fi
if [[ "${1:-} ${2:-} ${3:-}" == "scheduler jobs describe" ]]; then
  case "${4:-}" in
  arco-flow-dispatcher-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  arco-flow-sweeper-run-dev)
    cat <<'JSON'
{"state":"ENABLED","httpTarget":{"uri":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app/run","httpMethod":"POST","oidcToken":{"audience":"https://arco-flow-sweeper-dev-135245112198.us-central1.run.app","serviceAccountEmail":"arco-invoker-dev@arco-testing-20260320.iam.gserviceaccount.com"}}}
JSON
    ;;
  *)
    exit 1
    ;;
  esac
  exit 0
fi
exit 1
SH
chmod +x "${scheduler_target_bin}/gcloud"
cat >"${scheduler_target_bin}/cargo" <<'SH'
#!/usr/bin/env bash
echo "cargo should not run with Scheduler target mismatch"
exit 1
SH
chmod +x "${scheduler_target_bin}/cargo"
if PATH="${scheduler_target_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  PROJECT_ID=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  ARCO_DEPLOY_OWNER=uat-session \
  ARCO_UAT_EVIDENCE_DIR="${scheduler_target_evidence_dir}" \
  run_with_expected_api bash "${RUNNER}" --live-deployed >/tmp/arco-uat-live-deployed-scheduler-target.out 2>/tmp/arco-uat-live-deployed-scheduler-target.err; then
  echo "live deployed run should fail before Cargo when flow Scheduler OIDC audience includes /run" >&2
  exit 1
fi
grep -Fq "deployed flow scheduler preflight failed: arco-flow-dispatcher-run-dev OIDC audience is https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app/run; expected https://arco-flow-dispatcher-dev-135245112198.us-central1.run.app" /tmp/arco-uat-live-deployed-scheduler-target.err
if grep -Fq "cargo should not run with Scheduler target mismatch" /tmp/arco-uat-live-deployed-scheduler-target.out; then
  echo "live deployed Scheduler target preflight failure should not invoke Cargo" >&2
  exit 1
fi
if [[ -e "${scheduler_target_evidence_dir}/.uat-validation-start" ]]; then
  echo "live deployed Scheduler target preflight failure should not create an evidence marker" >&2
  exit 1
fi

owner_bin="$(mktemp -d)"
owner_evidence_dir="$(mktemp -d)/evidence"
owner_curl_marker="$(mktemp)"
cat >"${owner_bin}/curl" <<SH
#!/usr/bin/env bash
printf 'called\n' >"${owner_curl_marker}"
cat <<'JSON'
{
  "service": "arco-api-dev",
  "packageVersion": "0.2.1",
  "codeVersion": "uat-live",
  "gitSha": "abc123",
  "image": "us-central1-docker.pkg.dev/example/arco-api:uat-live",
  "cloudRunRevision": "arco-api-dev-00001-test"
}
JSON
SH
chmod +x "${owner_bin}/curl"
cat >"${owner_bin}/jq" <<'SH'
#!/usr/bin/env bash
cat >/dev/null
exit 0
SH
chmod +x "${owner_bin}/jq"
cat >"${owner_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" == "scheduler jobs describe" ]]; then
  printf 'ENABLED\n'
  exit 0
fi
if [[ "${1:-} ${2:-} ${3:-}" == "run services describe" ]]; then
  if [[ "${4:-}" == "arco-api-dev" ]]; then
    printf 'other-session\n'
  else
    printf 'uat-session\n'
  fi
  exit 0
fi
exit 1
SH
chmod +x "${owner_bin}/gcloud"
if PATH="${owner_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  PROJECT_ID=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  ARCO_DEPLOY_OWNER=uat-session \
  ARCO_UAT_EVIDENCE_DIR="${owner_evidence_dir}" \
  run_with_expected_api bash "${RUNNER}" --live-deployed >/tmp/arco-uat-live-deployed-owner.out 2>/tmp/arco-uat-live-deployed-owner.err; then
  echo "live deployed run should fail before Cargo when Cloud Run deploy owner labels mismatch" >&2
  exit 1
fi
grep -Fq "deployed Cloud Run owner preflight failed: arco-api-dev current arco_deploy_owner=other-session; expected ARCO_DEPLOY_OWNER=uat-session" /tmp/arco-uat-live-deployed-owner.err
if grep -Fq "cargo test" /tmp/arco-uat-live-deployed-owner.out; then
  echo "live deployed owner preflight failure should not run Cargo" >&2
  exit 1
fi
if [[ -s "${owner_curl_marker}" ]]; then
  echo "live deployed owner preflight failure should not call /version" >&2
  exit 1
fi
if [[ -e "${owner_evidence_dir}/.uat-validation-start" ]]; then
  echo "live deployed owner preflight failure should not create an evidence marker" >&2
  exit 1
fi

scope_bin="$(mktemp -d)"
scope_evidence_dir="$(mktemp -d)/evidence"
scope_curl_marker="$(mktemp)"
cat >"${scope_bin}/curl" <<SH
#!/usr/bin/env bash
printf 'called\n' >"${scope_curl_marker}"
cat <<'JSON'
{
  "service": "arco-api-dev",
  "packageVersion": "0.2.1",
  "codeVersion": "uat-live",
  "gitSha": "abc123",
  "image": "us-central1-docker.pkg.dev/example/arco-api:uat-live",
  "cloudRunRevision": "arco-api-dev-00001-test"
}
JSON
SH
chmod +x "${scope_bin}/curl"
cp "${scope_status_bin}/gcloud" "${scope_bin}/gcloud"
cp "${scope_status_bin}/jq" "${scope_bin}/jq"
if PATH="${scope_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  PROJECT_ID=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  ARCO_DEPLOY_OWNER=uat-session \
  ARCO_UAT_EVIDENCE_DIR="${scope_evidence_dir}" \
  run_with_expected_api bash "${RUNNER}" --live-deployed >/tmp/arco-uat-live-deployed-scope.out 2>/tmp/arco-uat-live-deployed-scope.err; then
  echo "live deployed run should fail before Cargo when flow service tenant/workspace scope mismatches UAT" >&2
  exit 1
fi
grep -Fq "deployed flow service scope preflight failed: arco-flow-dispatcher-dev tenant/workspace tenant-proof2-20260604/workspace-proof2-20260604; expected arco-uat-tenant/arco-uat-workspace" /tmp/arco-uat-live-deployed-scope.err
if grep -Fq "cargo test" /tmp/arco-uat-live-deployed-scope.out; then
  echo "live deployed scope preflight failure should not run Cargo" >&2
  exit 1
fi
if [[ -s "${scope_curl_marker}" ]]; then
  echo "live deployed scope preflight failure should not call /version" >&2
  exit 1
fi
if [[ -e "${scope_evidence_dir}/.uat-validation-start" ]]; then
  echo "live deployed scope preflight failure should not create an evidence marker" >&2
  exit 1
fi

missing_owner_bin="$(mktemp -d)"
missing_owner_evidence_dir="$(mktemp -d)/evidence"
cat >"${missing_owner_bin}/curl" <<'SH'
#!/usr/bin/env bash
cat <<'JSON'
{
  "service": "arco-api-dev",
  "packageVersion": "0.2.1",
  "codeVersion": "uat-live",
  "gitSha": "abc123",
  "image": "us-central1-docker.pkg.dev/example/arco-api:uat-live",
  "cloudRunRevision": "arco-api-dev-00001-test"
}
JSON
SH
chmod +x "${missing_owner_bin}/curl"
cat >"${missing_owner_bin}/jq" <<'SH'
#!/usr/bin/env bash
cat >/dev/null
exit 0
SH
chmod +x "${missing_owner_bin}/jq"
cat >"${missing_owner_bin}/gcloud" <<'SH'
#!/usr/bin/env bash
if [[ "${1:-} ${2:-} ${3:-}" == "scheduler jobs describe" ]]; then
  printf 'ENABLED\n'
  exit 0
fi
exit 1
SH
chmod +x "${missing_owner_bin}/gcloud"
cat >"${missing_owner_bin}/cargo" <<'SH'
#!/usr/bin/env bash
echo "cargo should not run without ARCO_DEPLOY_OWNER"
exit 1
SH
chmod +x "${missing_owner_bin}/cargo"
if PATH="${missing_owner_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  PROJECT_ID=arco-testing-20260320 \
  REGION=us-central1 \
  ARCO_UAT_ENVIRONMENT=dev \
  ARCO_UAT_EVIDENCE_DIR="${missing_owner_evidence_dir}" \
  run_with_expected_api bash "${RUNNER}" --live-deployed >/tmp/arco-uat-live-deployed-missing-owner.out 2>/tmp/arco-uat-live-deployed-missing-owner.err; then
  echo "live deployed run should require ARCO_DEPLOY_OWNER before evidence or Cargo" >&2
  exit 1
fi
grep -Fq "ARCO_DEPLOY_OWNER is required for full deployed UAT runs" /tmp/arco-uat-live-deployed-missing-owner.err
if grep -Fq "cargo should not run without ARCO_DEPLOY_OWNER" /tmp/arco-uat-live-deployed-missing-owner.out; then
  echo "live deployed missing-owner failure should not invoke Cargo" >&2
  exit 1
fi
if [[ -e "${missing_owner_evidence_dir}/.uat-validation-start" ]]; then
  echo "live deployed missing-owner failure should not create an evidence marker" >&2
  exit 1
fi

retry_bin="$(mktemp -d)"
retry_state="$(mktemp)"
retry_evidence_dir="$(mktemp -d)/evidence"
cat >"${retry_bin}/curl" <<SH
#!/usr/bin/env bash
count="\$(cat "${retry_state}" 2>/dev/null || printf '0')"
count="\$((count + 1))"
printf '%s' "\${count}" >"${retry_state}"
if [[ "\${count}" -lt 2 ]]; then
  exit 7
fi
cat <<'JSON'
{
  "service": "arco-api-dev",
  "packageVersion": "0.2.1",
  "codeVersion": "uat-live",
  "gitSha": "abc123",
  "image": "us-central1-docker.pkg.dev/example/arco-api:uat-live",
  "cloudRunRevision": "arco-api-dev-00001-test"
}
JSON
SH
chmod +x "${retry_bin}/curl"
cat >"${retry_bin}/jq" <<'SH'
#!/usr/bin/env bash
cat >/dev/null
exit 0
SH
chmod +x "${retry_bin}/jq"
PATH="${retry_bin}:${PATH}" \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  ARCO_UAT_EVIDENCE_DIR="${retry_evidence_dir}" \
  ARCO_UAT_PREFLIGHT_TIMEOUT_SECS=3 \
  ARCO_UAT_PREFLIGHT_INTERVAL_SECS=0 \
  run_with_expected_api bash "${RUNNER}" --live-deployed --preflight-only
if [[ "$(cat "${retry_state}")" != "2" ]]; then
  echo "live deployed preflight should retry until /version is reachable" >&2
  exit 1
fi
if [[ -e "${retry_evidence_dir}/.uat-validation-start" ]]; then
  echo "live deployed preflight-only retry should not create an evidence marker" >&2
  exit 1
fi

all_output="$(
  ARCO_UAT_STORAGE_BUCKET=gs://arco-uat \
  ARCO_UAT_API_URL=https://arco.acceptance.example \
  run_with_expected_api bash "${RUNNER}" --all --dry-run
)"
validator_count="$(grep -F "tools/validate_user_acceptance_evidence.sh --newer-than ${DEFAULT_EVIDENCE_MARKER} --require-kind durable_storage --require-kind deployed_api_worker --expect-api-code-version ${EXPECTED_API_CODE_VERSION} --expect-api-git-sha ${EXPECTED_API_GIT_SHA} --expect-api-image ${EXPECTED_API_IMAGE} ${DEFAULT_EVIDENCE_DIR}" <<<"${all_output}" | wc -l | tr -d ' ')"
if [[ "${validator_count}" != "1" ]]; then
  echo "--all dry-run should validate live evidence once after live gates" >&2
  exit 1
fi

echo "User acceptance UAT runner dry-run checks passed."

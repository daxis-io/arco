#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

BIN_DIR="$TMPDIR/bin"
STDOUT_LOG="$TMPDIR/stdout.log"
STDERR_LOG="$TMPDIR/stderr.log"
TERRAFORM_LOG="$TMPDIR/terraform.log"
mkdir -p "$BIN_DIR"
export TERRAFORM_LOG

fail() {
  local message="$1"
  echo "FAIL: $message" >&2
  if [[ -f "$STDERR_LOG" ]]; then
    echo "--- stderr ---" >&2
    cat "$STDERR_LOG" >&2
  fi
  if [[ -f "$STDOUT_LOG" ]]; then
    echo "--- stdout ---" >&2
    cat "$STDOUT_LOG" >&2
  fi
  exit 1
}

expect_logged_arg() {
  local expected="$1"
  local message="$2"
  if ! grep -F -- "$expected" "$TERRAFORM_LOG" >/dev/null; then
    fail "$message"
  fi
}

cat >"$BIN_DIR/terraform" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${TERRAFORM_LOG:?}"
exit 0
EOF

for cmd in gcloud curl jq; do
  cat >"$BIN_DIR/$cmd" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 0
EOF
done

chmod +x "$BIN_DIR/terraform" "$BIN_DIR/gcloud" "$BIN_DIR/curl" "$BIN_DIR/jq"

if ! env \
  PATH="$BIN_DIR:$PATH" \
  PROJECT_ID="runtime-project" \
  PROJECT_NUMBER="999888777666" \
  REGION="europe-west1" \
  API_IMAGE="test/api" \
  COMPACTOR_IMAGE="test/compactor" \
  FLOW_COMPACTOR_IMAGE="test/flow-compactor" \
  FLOW_DISPATCHER_IMAGE="test/flow-dispatcher" \
  FLOW_SWEEPER_IMAGE="test/flow-sweeper" \
  FLOW_TIMER_INGEST_IMAGE="test/flow-timer" \
  FLOW_WORKER_IMAGE="test/flow-worker" \
  ENVIRONMENT="dev" \
  bash "$ROOT_DIR/scripts/deploy.sh" --dry-run >"$STDOUT_LOG" 2>"$STDERR_LOG"; then
  fail "deploy.sh dry-run should succeed for ENVIRONMENT=dev without --tfvars"
fi

expect_logged_arg "-var-file=$ROOT_DIR/infra/terraform/environments/dev.tfvars" \
  "terraform plan did not use the default dev tfvars path"
expect_logged_arg "-var=project_id=runtime-project" \
  "terraform plan did not pin project_id to the deploy target"
expect_logged_arg "-var=project_number=999888777666" \
  "terraform plan did not pin project_number to the deploy target"
expect_logged_arg "-var=region=europe-west1" \
  "terraform plan did not pin region to the deploy target"
expect_logged_arg "-var=environment=dev" \
  "terraform plan did not pin environment to the deploy target"
expect_logged_arg "-var=api_image=test/api" \
  "terraform plan did not override api_image from the deploy environment"
expect_logged_arg "-var=compactor_image=test/compactor" \
  "terraform plan did not override compactor_image from the deploy environment"
expect_logged_arg "-var=flow_compactor_image=test/flow-compactor" \
  "terraform plan did not override flow_compactor_image from the deploy environment"
expect_logged_arg "-var=flow_dispatcher_image=test/flow-dispatcher" \
  "terraform plan did not override flow_dispatcher_image from the deploy environment"
expect_logged_arg "-var=flow_sweeper_image=test/flow-sweeper" \
  "terraform plan did not override flow_sweeper_image from the deploy environment"
expect_logged_arg "-var=flow_timer_ingest_image=test/flow-timer" \
  "terraform plan did not override flow_timer_ingest_image from the deploy environment"
expect_logged_arg "-var=flow_worker_image=test/flow-worker" \
  "terraform plan did not override flow_worker_image from the deploy environment"

echo "PASS: deploy.sh dry-run uses the default dev tfvars path and explicit runtime overrides"

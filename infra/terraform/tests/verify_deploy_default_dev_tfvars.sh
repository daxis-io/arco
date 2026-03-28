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
  PROJECT_ID="arco-testing-20260320" \
  PROJECT_NUMBER="135245112198" \
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

EXPECTED_VAR_FILE="-var-file=$ROOT_DIR/infra/terraform/environments/dev.tfvars"
if ! grep -F -- "$EXPECTED_VAR_FILE" "$TERRAFORM_LOG" >/dev/null; then
  fail "terraform plan did not use the default dev tfvars path"
fi

echo "PASS: deploy.sh dry-run resolves the default dev tfvars path"

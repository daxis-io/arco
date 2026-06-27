#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

errors=0

pass() {
  echo "PASS: $1"
}

fail() {
  echo "FAIL: $1" >&2
  errors=$((errors + 1))
}

check_literal() {
  local file="$1"
  local needle="$2"
  local description="$3"

  if grep -Fq "${needle}" "${file}"; then
    pass "${description}"
  else
    fail "${description}"
  fi
}

check_regex() {
  local file="$1"
  local pattern="$2"
  local description="$3"

  if grep -Eq "${pattern}" "${file}"; then
    pass "${description}"
  else
    fail "${description}"
  fi
}

check_regex "Cargo.toml" '^version = "0\.2\.1"$' "Workspace package version is 0.2.1"
check_regex "crates/arco-test-utils/Cargo.toml" '^version = "0\.2\.1"$' "Test utils crate version is 0.2.1"
check_regex "tools/xtask/Cargo.toml" '^version = "0\.2\.1"$' "xtask version is 0.2.1"
check_regex "python/pyproject.toml" '^version = "0\.2\.1"$' "Root Python package version is 0.2.1"
check_regex "python/arco/pyproject.toml" '^version = "0\.2\.1"$' "Flow Python package version is 0.2.1"
check_literal "python/arco/__init__.py" '__version__ = "0.2.1"' "Python SDK version constant is 0.2.1"
check_literal "python/arco/src/arco_flow/__init__.py" '__version__ = "0.2.1"' "Flow Python SDK version constant is 0.2.1"
check_literal "python/arco/src/arco_flow/cli/commands/init.py" 'version = "0.2.1"' "Init template project version is 0.2.1"
check_literal "python/arco/src/arco_flow/cli/commands/init.py" '"arco-flow>=0.2.1",' "Init template dependency floor is 0.2.1"
check_literal "crates/arco-api/openapi.json" '"version": "0.2.1"' "OpenAPI snapshot advertises 0.2.1"
check_regex "CHANGELOG.md" '^## \[0\.2\.1\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$' "Changelog contains a dated 0.2.1 section"
check_literal "release_notes/v0.2.1.md" '# Release Notes for v0.2.1' "Release notes target v0.2.1"

if git grep -n -E '0\.2\.0' -- \
  Cargo.toml \
  crates/arco-test-utils/Cargo.toml \
  tools/xtask/Cargo.toml \
  python/pyproject.toml \
  python/uv.lock \
  python/arco/pyproject.toml \
  python/arco/uv.lock \
  python/arco/__init__.py \
  python/arco/src/arco_flow/__init__.py \
  python/arco/src/arco_flow/cli/commands/init.py \
  crates/arco-api/openapi.json \
  tools/test_user_acceptance_uat_runner.sh \
  tools/test_user_acceptance_evidence_validator.sh \
  tools/xtask/tests/deployed_uat_prereq_repair.rs \
  crates/arco-integration-tests/tests/user_acceptance_pipeline.rs >/dev/null 2>&1; then
  fail "Current release metadata no longer mentions 0.2.0"
else
  pass "Current release metadata no longer mentions 0.2.0"
fi

if [[ "${errors}" -gt 0 ]]; then
  echo "release prep checks failed: ${errors}" >&2
  exit 1
fi

echo "Release prep for v0.2.1 looks correct."

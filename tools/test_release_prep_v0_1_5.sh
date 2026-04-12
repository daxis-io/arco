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

check_missing() {
  local path="$1"
  local description="$2"

  if [[ ! -e "${path}" ]]; then
    pass "${description}"
  else
    fail "${description}"
  fi
}

check_regex "Cargo.toml" '^version = "0\.1\.5"$' "Workspace package version is 0.1.5"
check_regex "crates/arco-test-utils/Cargo.toml" '^version = "0\.1\.5"$' "Test utils crate version is 0.1.5"
check_regex "tools/xtask/Cargo.toml" '^version = "0\.1\.5"$' "xtask version is 0.1.5"
check_regex "python/pyproject.toml" '^version = "0\.1\.5"$' "Root Python package version is 0.1.5"
check_regex "python/arco/pyproject.toml" '^version = "0\.1\.5"$' "Flow Python package version is 0.1.5"
check_literal "python/arco/__init__.py" '__version__ = "0.1.5"' "Python SDK version constant is 0.1.5"
check_literal "python/arco/src/arco_flow/__init__.py" '__version__ = "0.1.5"' "Flow Python SDK version constant is 0.1.5"
check_literal "python/arco/src/arco_flow/cli/commands/init.py" 'version = "0.1.5"' "Init template project version is 0.1.5"
check_literal "python/arco/src/arco_flow/cli/commands/init.py" '"arco-flow>=0.1.5",' "Init template dependency floor is 0.1.5"
check_literal "crates/arco-api/openapi.json" '"version": "0.1.5"' "OpenAPI snapshot advertises 0.1.5"
check_regex "CHANGELOG.md" '^## \[0\.1\.5\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$' "Changelog contains a dated 0.1.5 section"
check_literal "release_notes/v0.1.5.md" '# Release Notes for v0.1.5' "Release notes target v0.1.5"
check_missing "release_notes/v1.4.0.md" "Stale v1.4.0 release notes are removed"
check_literal "ROADMAP.md" '`0.1.x`' "Roadmap speaks in terms of the 0.1.x line"
check_literal "ROADMAP.md" '`2.0.0`' "Roadmap still stages the proto break for 2.0.0"
check_literal "SECURITY.md" '| 0.1.x   | :white_check_mark: |' "Security policy marks 0.1.x as supported"
if grep -R -n -E '1\.4\.0' CHANGELOG.md Cargo.toml ROADMAP.md SECURITY.md crates/arco-api/openapi.json python release_notes tools/xtask/Cargo.toml >/dev/null 2>&1; then
  fail "Release prep files no longer mention 1.4.0"
else
  pass "Release prep files no longer mention 1.4.0"
fi

if [[ "${errors}" -gt 0 ]]; then
  echo "release prep checks failed: ${errors}" >&2
  exit 1
fi

echo "Release prep for v0.1.5 looks correct."

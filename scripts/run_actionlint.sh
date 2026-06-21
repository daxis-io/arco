#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/run_actionlint.sh [--help]

Validates all GitHub Actions workflow YAML files with actionlint.
Set ACTIONLINT_BIN to use a non-default actionlint binary.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ $# -gt 0 ]]; then
  echo "unknown argument: $1" >&2
  usage >&2
  exit 2
fi

actionlint_bin="${ACTIONLINT_BIN:-actionlint}"
if ! command -v "$actionlint_bin" >/dev/null 2>&1; then
  echo "ERROR: actionlint was not found. Install actionlint or set ACTIONLINT_BIN." >&2
  exit 127
fi

workflow_files=()
while IFS= read -r -d '' file; do
  workflow_files+=("$file")
done < <(find .github/workflows -type f \( -name '*.yml' -o -name '*.yaml' \) -print0 | sort -z)

if [[ "${#workflow_files[@]}" -eq 0 ]]; then
  echo "ERROR: no GitHub Actions workflow files found under .github/workflows" >&2
  exit 1
fi

"$actionlint_bin" "${workflow_files[@]}"

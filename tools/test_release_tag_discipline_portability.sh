#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

TAG="v1.4.0"

if [[ ! -f "release_notes/${TAG}.md" ]]; then
  echo "missing fixture release notes for ${TAG}" >&2
  exit 1
fi

if [[ ! -f "CHANGELOG.md" ]]; then
  echo "missing fixture changelog" >&2
  exit 1
fi

env PATH="/usr/bin:/bin" bash tools/check-release-tag-discipline.sh --tag "${TAG}"

echo "Release tag discipline passes without ripgrep on PATH."

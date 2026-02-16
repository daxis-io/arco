#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: tools/check-release-tag-discipline.sh [--tag <tag>] [--changelog <path>] [--release-notes <path>]

Validates release-tag discipline for Gate 1:
  - tag format is release-like (`vMAJOR.MINOR.PATCH` with optional suffix)
  - CHANGELOG contains a dated section for the tag version with at least one bullet
  - release note file exists, has required sections, and has no placeholders
USAGE
}

TAG="${GITHUB_REF_NAME:-}"
CHANGELOG_PATH="CHANGELOG.md"
RELEASE_NOTES_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      TAG="${2:-}"
      shift 2
      ;;
    --changelog)
      CHANGELOG_PATH="${2:-}"
      shift 2
      ;;
    --release-notes)
      RELEASE_NOTES_PATH="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${TAG}" ]]; then
  echo "release tag is required (pass --tag or set GITHUB_REF_NAME)" >&2
  exit 1
fi

if [[ ! "${TAG}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  echo "release tag must match vMAJOR.MINOR.PATCH (optionally suffixed): ${TAG}" >&2
  exit 1
fi

VERSION="${TAG#v}"
if [[ -z "${RELEASE_NOTES_PATH}" ]]; then
  RELEASE_NOTES_PATH="release_notes/${TAG}.md"
fi

if [[ ! -f "${CHANGELOG_PATH}" ]]; then
  echo "missing changelog file: ${CHANGELOG_PATH}" >&2
  exit 1
fi

if [[ ! -f "${RELEASE_NOTES_PATH}" ]]; then
  echo "missing release notes file: ${RELEASE_NOTES_PATH}" >&2
  exit 1
fi

CHANGELOG_HEADER="## [${VERSION}] - "
if ! rg -n "^## \\[${VERSION}\\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$" "${CHANGELOG_PATH}" >/dev/null; then
  echo "missing changelog section for ${VERSION} with date (YYYY-MM-DD)" >&2
  exit 1
fi

CHANGELOG_SECTION="$(
  awk -v section_prefix="${CHANGELOG_HEADER}" '
    index($0, section_prefix) == 1 { in_section = 1; print; next }
    in_section && /^## \[/ { exit }
    in_section { print }
  ' "${CHANGELOG_PATH}"
)"

if ! printf '%s\n' "${CHANGELOG_SECTION}" | rg -q "^- "; then
  echo "changelog section for ${VERSION} must include at least one bullet item" >&2
  exit 1
fi

if ! rg -q "^# Release Notes for ${TAG}$" "${RELEASE_NOTES_PATH}"; then
  echo "release notes title must be '# Release Notes for ${TAG}'" >&2
  exit 1
fi

for section in "## Highlights" "## Changelog Reference" "## Verification"; do
  if ! rg -q "^${section}$" "${RELEASE_NOTES_PATH}"; then
    echo "release notes missing required section '${section}'" >&2
    exit 1
  fi
done

if ! rg -q "CHANGELOG\\.md" "${RELEASE_NOTES_PATH}" || ! rg -q "${VERSION}" "${RELEASE_NOTES_PATH}"; then
  echo "release notes must reference CHANGELOG.md and version ${VERSION}" >&2
  exit 1
fi

if ! rg -q "^- " "${RELEASE_NOTES_PATH}"; then
  echo "release notes must include at least one bullet item" >&2
  exit 1
fi

if rg -n -i "\\b(TODO|TBD|PLACEHOLDER|XXX)\\b" "${RELEASE_NOTES_PATH}" >/dev/null; then
  echo "release notes contain placeholder text" >&2
  exit 1
fi

echo "release-tag discipline checks passed for ${TAG}"

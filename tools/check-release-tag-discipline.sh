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

HAS_RG=0
if command -v rg >/dev/null 2>&1; then
  HAS_RG=1
fi

file_has_regex() {
  local pattern="$1"
  local file="$2"

  if [[ "${HAS_RG}" -eq 1 ]]; then
    rg -q -- "${pattern}" "${file}"
  else
    grep -Eq -- "${pattern}" "${file}"
  fi
}

file_has_fixed() {
  local needle="$1"
  local file="$2"

  if [[ "${HAS_RG}" -eq 1 ]]; then
    rg -Fq -- "${needle}" "${file}"
  else
    grep -Fq -- "${needle}" "${file}"
  fi
}

file_has_regex_i() {
  local pattern="$1"
  local file="$2"

  if [[ "${HAS_RG}" -eq 1 ]]; then
    rg -qi -- "${pattern}" "${file}"
  else
    grep -Eqi -- "${pattern}" "${file}"
  fi
}

stdin_has_regex() {
  local pattern="$1"

  if [[ "${HAS_RG}" -eq 1 ]]; then
    rg -q -- "${pattern}"
  else
    grep -Eq -- "${pattern}"
  fi
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
if ! file_has_regex "^## \\[${VERSION}\\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$" "${CHANGELOG_PATH}"; then
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

if ! printf '%s\n' "${CHANGELOG_SECTION}" | stdin_has_regex "^- "; then
  echo "changelog section for ${VERSION} must include at least one bullet item" >&2
  exit 1
fi

if ! file_has_regex "^# Release Notes for ${TAG}$" "${RELEASE_NOTES_PATH}"; then
  echo "release notes title must be '# Release Notes for ${TAG}'" >&2
  exit 1
fi

for section in "## Highlights" "## Changelog Reference" "## Verification"; do
  if ! file_has_regex "^${section}$" "${RELEASE_NOTES_PATH}"; then
    echo "release notes missing required section '${section}'" >&2
    exit 1
  fi
done

if ! file_has_regex "CHANGELOG\\.md" "${RELEASE_NOTES_PATH}" || ! file_has_fixed "${VERSION}" "${RELEASE_NOTES_PATH}"; then
  echo "release notes must reference CHANGELOG.md and version ${VERSION}" >&2
  exit 1
fi

if ! file_has_regex "^- " "${RELEASE_NOTES_PATH}"; then
  echo "release notes must include at least one bullet item" >&2
  exit 1
fi

if file_has_regex_i '(^|[^[:alnum:]_])(TODO|TBD|PLACEHOLDER|XXX)([^[:alnum:]_]|$)' "${RELEASE_NOTES_PATH}"; then
  echo "release notes contain placeholder text" >&2
  exit 1
fi

echo "release-tag discipline checks passed for ${TAG}"

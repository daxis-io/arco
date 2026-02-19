#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: tools/collect_release_evidence.sh --tag <tag> [--output-root <path>] [--allow-existing]

Collects immutable Gate 1 release evidence for a signed, annotated release tag.
The output directory name is deterministic:
  <output-root>/<tag>/<tag-object-sha>-<commit-sha>
When --allow-existing is supplied, an existing deterministic pack is verified and returned
without rewriting files or manifest state (reuse is allowed only for verify/read flows).
USAGE
}

TAG=""
OUTPUT_ROOT="release_evidence/2026-02-12-prod-readiness/gate-1/collector-packs"
ALLOW_EXISTING="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      TAG="${2:-}"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="${2:-}"
      shift 2
      ;;
    --allow-existing)
      ALLOW_EXISTING="true"
      shift
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
  echo "--tag is required" >&2
  exit 1
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

if [[ ! "${TAG}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  echo "release tag must match vMAJOR.MINOR.PATCH (optionally suffixed): ${TAG}" >&2
  exit 1
fi

if ! git rev-parse --verify -q "refs/tags/${TAG}" >/dev/null; then
  echo "missing git tag: ${TAG}" >&2
  exit 1
fi

if ! git cat-file -e "${TAG}^{tag}" 2>/dev/null; then
  echo "tag must be annotated (lightweight tags are not accepted): ${TAG}" >&2
  exit 1
fi

TAG_OBJECT_SHA="$(git rev-parse "${TAG}^{tag}")"
COMMIT_SHA="$(git rev-list -n 1 "${TAG}")"
TAGGER_EPOCH="$(git for-each-ref --format='%(taggerdate:unix)' "refs/tags/${TAG}")"
TAGGER_ISO8601="$(git for-each-ref --format='%(taggerdate:iso8601-strict)' "refs/tags/${TAG}")"

TAG_OBJECT_CONTENT="$(git cat-file -p "${TAG}^{tag}")"
SIGNATURE_TYPE="none"
if printf '%s\n' "${TAG_OBJECT_CONTENT}" | rg -q '^-----BEGIN PGP SIGNATURE-----$'; then
  SIGNATURE_TYPE="gpg"
elif printf '%s\n' "${TAG_OBJECT_CONTENT}" | rg -q '^-----BEGIN SSH SIGNATURE-----$'; then
  SIGNATURE_TYPE="ssh"
fi

if [[ "${SIGNATURE_TYPE}" == "none" ]]; then
  echo "tag is annotated but not signed: ${TAG}" >&2
  exit 1
fi

SIGNATURE_VERIFIED="false"
SIGNATURE_VERIFICATION_METHOD="signature-material-only"
SIGNERS_FILE="${REPO_ROOT}/.github/release-signers.allowed"
GIT_VERIFY_ARGS=()
if [[ -f "${SIGNERS_FILE}" ]]; then
  GIT_VERIFY_ARGS=(-c gpg.format=ssh -c gpg.ssh.allowedSignersFile="${SIGNERS_FILE}")
fi

if git "${GIT_VERIFY_ARGS[@]}" verify-tag "${TAG}" >/dev/null 2>&1; then
  SIGNATURE_VERIFIED="true"
  SIGNATURE_VERIFICATION_METHOD="git-verify-tag"
elif [[ -n "${GITHUB_TOKEN:-}" && -n "${GITHUB_REPOSITORY:-}" ]]; then
  verification_payload="$(curl -fsSL \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H "Accept: application/vnd.github+json" \
    "https://api.github.com/repos/${GITHUB_REPOSITORY}/git/tags/${TAG_OBJECT_SHA}")"
  verified="$(jq -r '.verification.verified // false' <<<"${verification_payload}")"
  reason="$(jq -r '.verification.reason // "unknown"' <<<"${verification_payload}")"
  if [[ "${verified}" != "true" ]]; then
    echo "release tag signature verification failed for ${TAG}: ${reason}" >&2
    exit 1
  fi
  SIGNATURE_VERIFIED="true"
  SIGNATURE_VERIFICATION_METHOD="github-api:${reason}"
else
  echo "warning: unable to run trusted signature verification for ${TAG}; continuing with signature-material checks only" >&2
fi

"${REPO_ROOT}/tools/check-release-tag-discipline.sh" --tag "${TAG}" >/dev/null

PACK_DIR="${OUTPUT_ROOT}/${TAG}/${TAG_OBJECT_SHA}-${COMMIT_SHA}"

if [[ -d "${PACK_DIR}" ]]; then
  if [[ "${ALLOW_EXISTING}" == "true" ]]; then
    if [[ ! -f "${PACK_DIR}/manifest.sha256" ]]; then
      echo "existing pack missing manifest: ${PACK_DIR}/manifest.sha256" >&2
      exit 1
    fi

    (
      cd "${PACK_DIR}"
      sha256sum -c manifest.sha256 >/dev/null
    )
    echo "${PACK_DIR}"
    exit 0
  fi

  echo "immutable output already exists: ${PACK_DIR}" >&2
  echo "refusing overwrite (use --allow-existing only for verify/read flows)" >&2
  exit 1
fi

mkdir -p "${PACK_DIR}"

CHANGELOG_HEADER="## [${TAG#v}] - "
CHANGELOG_SECTION="$(
  awk -v section_prefix="${CHANGELOG_HEADER}" '
    index($0, section_prefix) == 1 { in_section = 1; print; next }
    in_section && /^## \[/ { exit }
    in_section { print }
  ' CHANGELOG.md
)"

if [[ -z "${CHANGELOG_SECTION}" ]]; then
  echo "unable to extract changelog section for ${TAG#v}" >&2
  exit 1
fi

printf '%s\n' "${TAG_OBJECT_CONTENT}" > "${PACK_DIR}/tag-object.txt"
git show-ref --tags --dereference "${TAG}" | LC_ALL=C sort > "${PACK_DIR}/tag-ref.txt"

cat > "${PACK_DIR}/metadata.env" <<EOF
COLLECTOR_VERSION=1
TAG=${TAG}
TAG_VERSION=${TAG#v}
TAG_OBJECT_SHA=${TAG_OBJECT_SHA}
COMMIT_SHA=${COMMIT_SHA}
TAGGER_EPOCH=${TAGGER_EPOCH}
TAGGER_ISO8601=${TAGGER_ISO8601}
SIGNATURE_TYPE=${SIGNATURE_TYPE}
SIGNATURE_VERIFIED=${SIGNATURE_VERIFIED}
SIGNATURE_VERIFICATION_METHOD=${SIGNATURE_VERIFICATION_METHOD}
EOF

printf '%s\n' "${CHANGELOG_SECTION}" > "${PACK_DIR}/changelog-section.md"
cp "release_notes/${TAG}.md" "${PACK_DIR}/release-notes.md"
cp "RELEASE.md" "${PACK_DIR}/release-process.md"
cp ".github/workflows/ci.yml" "${PACK_DIR}/workflow-ci.yml"
cp ".github/workflows/release-sbom.yml" "${PACK_DIR}/workflow-release-sbom.yml"
if [[ -f ".github/release-signers.allowed" ]]; then
  cp ".github/release-signers.allowed" "${PACK_DIR}/release-signers.allowed"
fi

{
  echo "expected_release_tag=${TAG}"
  echo "sbom_spdx_asset=arco-${TAG}.spdx.json"
  echo "sbom_cyclonedx_asset=arco-${TAG}.cyclonedx.json"
  echo "sbom_manifest_asset=arco-${TAG}.sbom.sha256"
  echo "retention_field=retention-days"
} > "${PACK_DIR}/sbom-artifact-linkage.txt"

(
  cd "${PACK_DIR}"
  LC_ALL=C find . -type f ! -name manifest.sha256 \
    | sed 's|^\./||' \
    | LC_ALL=C sort \
    | while IFS= read -r file; do
        sha256sum "${file}"
      done > manifest.sha256
)

echo "${PACK_DIR}"

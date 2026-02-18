#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

WORKFLOW_PATH=".github/workflows/release-sbom.yml"
COLLECTOR_PATH="tools/collect_release_evidence.sh"

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

check_literal "${WORKFLOW_PATH}" "RELEASE_TAG: \${{ github.event_name == 'workflow_dispatch' && inputs.release_tag || github.ref_name }}" "Workflow resolves RELEASE_TAG from dispatch input or pushed tag ref"
check_literal "${WORKFLOW_PATH}" "ref: \${{ env.RELEASE_TAG }}" "Workflow checks out exact release tag ref"
check_literal "${WORKFLOW_PATH}" "name: Wait for release-tag CI success" "Workflow waits for successful tag CI before publishing SBOM"
check_literal "${WORKFLOW_PATH}" "commits/\${TAG_COMMIT_SHA}/check-runs?per_page=100" "Workflow checks release-tag discipline via commit check-runs API"
check_literal "${WORKFLOW_PATH}" "gpg.ssh.allowedSignersFile=\".github/release-signers.allowed\"" "Workflow verifies signed tags against repository allowed-signers file"
check_literal "${WORKFLOW_PATH}" "verify-tag \"\${RELEASE_TAG}\" > \"arco-\${RELEASE_TAG}.tag-verify.txt\"" "Workflow captures deterministic tag verification transcript"
check_literal "${WORKFLOW_PATH}" "uses: actions/attest-build-provenance@977bb373ede98d70efdf65b84cb5f73e068dcc2a # v3" "Attestation action pinned by full commit SHA"
check_literal "${COLLECTOR_PATH}" 'if [[ -d "${PACK_DIR}" ]]; then' "Collector handles pre-existing deterministic pack directory explicitly"
check_literal "${COLLECTOR_PATH}" "sha256sum -c manifest.sha256" "Collector verifies existing manifest in allow-existing mode"
check_literal "${COLLECTOR_PATH}" "reuse is allowed only for verify/read flows" "Collector documents no-rewrite semantics for allow-existing mode"
check_literal "${COLLECTOR_PATH}" "release-signers.allowed" "Collector snapshots release signer trust roots into immutable evidence packs"

tmp_root="$(mktemp -d)"
cleanup() {
  rm -rf "${tmp_root}"
}
trap cleanup EXIT

test_tag="$(git tag --list 'v*' --sort=-creatordate | head -n 1)"
if [[ -z "${test_tag}" ]]; then
  fail "At least one release-like tag (v*) must exist for immutability regression test"
else
  pack_dir="$(bash tools/collect_release_evidence.sh --tag "${test_tag}" --output-root "${tmp_root}" | tail -n 1)"
  manifest_path="${pack_dir}/manifest.sha256"

  if [[ ! -f "${manifest_path}" ]]; then
    fail "Collector created pack without manifest.sha256"
  else
    before_hash="$(sha256sum "${manifest_path}" | awk '{print $1}')"
    printf '%s\n' "local mutation should not be absorbed" > "${pack_dir}/sentinel.txt"
    bash tools/collect_release_evidence.sh --tag "${test_tag}" --output-root "${tmp_root}" --allow-existing >/dev/null
    after_hash="$(sha256sum "${manifest_path}" | awk '{print $1}')"

    if [[ "${before_hash}" != "${after_hash}" ]]; then
      fail "allow-existing must not rewrite manifest for an existing deterministic pack"
    else
      pass "allow-existing does not rewrite manifest for existing deterministic pack"
    fi

    if grep -Fq "sentinel.txt" "${manifest_path}"; then
      fail "allow-existing must not mutate manifest contents with new files"
    else
      pass "allow-existing leaves manifest contents unchanged"
    fi
  fi
fi

if [[ "${errors}" -gt 0 ]]; then
  echo "release gate-1 hardening checks failed: ${errors}" >&2
  exit 1
fi

echo "All release gate-1 hardening checks passed."

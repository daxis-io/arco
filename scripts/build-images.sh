#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROJECT_ID=""
REGION="us-central1"
REPOSITORY="arco"
TAG=""
PUSH=false
DRY_RUN=false

usage() {
  cat <<'EOF'
Usage: scripts/build-images.sh --project PROJECT_ID --tag TAG [options]

Options:
  --project PROJECT_ID   GCP project ID for Artifact Registry image names
  --tag TAG              Image tag to build
  --region REGION        Artifact Registry region (default: us-central1)
  --repository NAME      Artifact Registry repository name (default: arco)
  --push                 Push images to Artifact Registry
  --dry-run              Print planned commands without building
  -h, --help             Show this help text
EOF
}

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

die() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      PROJECT_ID="${2:-}"
      shift 2
      ;;
    --region)
      REGION="${2:-}"
      shift 2
      ;;
    --repository)
      REPOSITORY="${2:-}"
      shift 2
      ;;
    --tag)
      TAG="${2:-}"
      shift 2
      ;;
    --push)
      PUSH=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

[[ -z "${PROJECT_ID}" ]] && die "--project is required"
[[ -z "${TAG}" ]] && die "--tag is required"

require_cmd docker
if [[ "${PUSH}" == "true" ]]; then
  require_cmd gcloud
fi

readonly REPOSITORY_HOST="${REGION}-docker.pkg.dev"
readonly REPOSITORY_PATH="${REPOSITORY_HOST}/${PROJECT_ID}/${REPOSITORY}"

services=(
  "API_IMAGE|arco-api|arco-api|arco-api|"
  "COMPACTOR_IMAGE|arco-compactor|arco-compactor|arco-compactor|"
  "FLOW_COMPACTOR_IMAGE|arco-flow-compactor|arco-flow|arco_flow_compactor|gcp"
  "FLOW_DISPATCHER_IMAGE|arco-flow-dispatcher|arco-flow|arco_flow_dispatcher|gcp"
  "FLOW_SWEEPER_IMAGE|arco-flow-sweeper|arco-flow|arco_flow_sweeper|gcp"
  "FLOW_TIMER_INGEST_IMAGE|arco-flow-timer-ingest|arco-flow|arco_flow_timer_ingest|gcp"
  "FLOW_WORKER_IMAGE|arco-flow-worker|arco-flow-worker|arco-flow-worker|"
)

ensure_repository() {
  if gcloud artifacts repositories describe "${REPOSITORY}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" >/dev/null 2>&1; then
    return
  fi

  log "Creating Artifact Registry repository ${REPOSITORY} in ${REGION}"
  gcloud artifacts repositories create "${REPOSITORY}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --description="Arco service images"
}

run_or_echo() {
  if [[ "${DRY_RUN}" == "true" ]]; then
    printf '+ '
    printf '%q ' "$@"
    printf '\n'
    return
  fi
  "$@"
}

if [[ "${PUSH}" == "true" && "${DRY_RUN}" != "true" ]]; then
  ensure_repository
fi

for spec in "${services[@]}"; do
  IFS='|' read -r env_var image_name package_name bin_name cargo_features <<<"${spec}"
  image_ref="${REPOSITORY_PATH}/${image_name}:${TAG}"
  log "Building ${image_name} -> ${image_ref}"
  build_cmd=(
    docker build
    --platform linux/amd64
    --build-arg "PACKAGE=${package_name}"
    --build-arg "BIN=${bin_name}"
  )

  if [[ -n "${cargo_features}" ]]; then
    build_cmd+=(--build-arg "CARGO_FEATURES=${cargo_features}")
  fi

  build_cmd+=(
    -t "${image_ref}"
    -f "${ROOT_DIR}/Dockerfile"
    "${ROOT_DIR}"
  )

  run_or_echo "${build_cmd[@]}"

  if [[ "${PUSH}" == "true" ]]; then
    log "Pushing ${image_ref}"
    run_or_echo docker push "${image_ref}"
  fi

  printf 'export %s=%q\n' "${env_var}" "${image_ref}"
done

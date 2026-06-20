#!/usr/bin/env bash
#
# Build and run the no-cloud local data pipeline UAT inside a Linux container.
#
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly DOCKERFILE="${ROOT_DIR}/Dockerfile.local-uat"

IMAGE_TAG="${IMAGE_TAG:-arco-local-pipeline-uat:latest}"
BUILD_IMAGE=true

usage() {
  cat <<EOF
Usage: $0 [--image-tag TAG] [--no-build]

Options:
  --image-tag TAG  Local Docker image tag. Default: ${IMAGE_TAG}
  --no-build       Reuse an existing image tag instead of rebuilding.
  -h, --help       Show this help message.

The container runs:
  bash scripts/run_local_pipeline_uat.sh

This path does not use GCP, Cloud Run, Cloud Scheduler, Cloud Tasks, or GCS.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --image-tag)
    [[ $# -ge 2 ]] || die "--image-tag requires a value"
    IMAGE_TAG="$2"
    shift 2
    ;;
  --no-build)
    BUILD_IMAGE=false
    shift
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    usage
    die "Unknown option: $1"
    ;;
  esac
done

require_cmd docker
[[ -f "$DOCKERFILE" ]] || die "Dockerfile.local-uat not found: $DOCKERFILE"

cd "$ROOT_DIR"

docker info >/dev/null 2>&1 || die "Docker daemon is not available. Start Docker Desktop, then retry."

if [[ "$BUILD_IMAGE" == "true" ]]; then
  docker build \
    --file "$DOCKERFILE" \
    --tag "$IMAGE_TAG" \
    "$ROOT_DIR"
fi

docker run --rm "$IMAGE_TAG"

#!/usr/bin/env bash
#
# Build and publish one Arco Cloud Run container image with Cloud Build.
#
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly CLOUD_BUILD_CONFIG="${ROOT_DIR}/infra/cloudbuild/cloud-run-image.yaml"

PROJECT_ID="${PROJECT_ID:-}"
BIN="${BIN:-}"
IMAGE="${IMAGE:-}"
FEATURES="${FEATURES:-}"
DRY_RUN=false

usage() {
  cat <<EOF
Usage: $0 --project PROJECT_ID --bin BIN --image IMAGE [--dry-run]

Options:
  --project PROJECT_ID  GCP project that runs Cloud Build.
  --bin BIN             Rust binary to build into the image.
  --image IMAGE         Full Artifact Registry image tag to publish.
  --features FEATURES   Optional comma-separated Cargo features for the build.
  --dry-run             Print the Cloud Build command without running it.
  -h, --help            Show this help message.

Supported binaries:
  arco-api
  arco-compactor
  arco_flow_compactor
  arco_flow_dispatcher
  arco_flow_sweeper
  arco_flow_timer_ingest
  arco_flow_worker
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

print_command() {
  local arg
  printf '%s' "$1"
  shift
  for arg in "$@"; do
    printf ' %s' "$arg"
  done
  printf '\n'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --project)
    PROJECT_ID="$2"
    shift 2
    ;;
  --bin)
    BIN="$2"
    shift 2
    ;;
  --image)
    IMAGE="$2"
    shift 2
    ;;
  --features)
    FEATURES="$2"
    shift 2
    ;;
  --dry-run)
    DRY_RUN=true
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

[[ -n "$PROJECT_ID" ]] || die "--project or PROJECT_ID is required"
[[ -n "$BIN" ]] || die "--bin or BIN is required"
[[ -n "$IMAGE" ]] || die "--image or IMAGE is required"
[[ -f "$CLOUD_BUILD_CONFIG" ]] || die "Cloud Build config not found: $CLOUD_BUILD_CONFIG"
[[ -f "${ROOT_DIR}/Dockerfile.cloudrun" ]] || die "Dockerfile.cloudrun not found"

case "$BIN" in
arco-api | arco-compactor | arco_flow_compactor | arco_flow_dispatcher | arco_flow_sweeper | arco_flow_timer_ingest | arco_flow_worker) ;;
*) die "Unsupported --bin '$BIN'" ;;
esac

cmd=(
  gcloud builds submit "$ROOT_DIR"
  "--project=$PROJECT_ID"
  "--config=$CLOUD_BUILD_CONFIG"
  "--substitutions=_BIN=${BIN},_IMAGE=${IMAGE},_FEATURES=${FEATURES}"
)

if [[ "$DRY_RUN" == "true" ]]; then
  print_command "${cmd[@]}"
else
  "${cmd[@]}"
fi

#!/usr/bin/env bash
#
# Run the no-cloud local data pipeline UAT.
#
# This exercises catalog/schema creation, Parquet writes and queries, Delta
# commit, manifest deployment, dispatch, callbacks, and final run/task state
# through the in-process API router backed by in-memory storage.
#
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<EOF
Usage: $0

Runs the local no-cloud data pipeline UAT:
  cargo test -p arco-integration-tests --test local_pipeline_uat -- --nocapture

This script does not use GCP, Cloud Run, Cloud Scheduler, Cloud Tasks, or GCS.
EOF
}

case "${1:-}" in
-h | --help)
  usage
  exit 0
  ;;
"") ;;
*)
  usage
  echo "ERROR: Unknown option: $1" >&2
  exit 1
  ;;
esac

cd "$ROOT_DIR"

echo "Running local no-cloud data pipeline UAT..."
cargo test -p arco-integration-tests --test local_pipeline_uat -- --nocapture

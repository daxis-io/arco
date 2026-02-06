#!/bin/bash
# iam-list-verify.sh
# Verify prefix-scoped IAM list semantics for Arco catalog bucket
#
# SAFETY: This script writes test objects to the bucket. It is designed
# for sandbox/staging environments. Running against production requires
# explicit acknowledgment via --allow-prod flag.
#
# Usage:
#   PROJECT_ID=my-sandbox BUCKET=my-sandbox-arco-catalog ./iam-list-verify.sh
#   PROJECT_ID=my-prod BUCKET=my-prod-arco-catalog ./iam-list-verify.sh --allow-prod

set -euo pipefail

# Parse arguments
ALLOW_PROD=false
for arg in "$@"; do
  case $arg in
    --allow-prod)
      ALLOW_PROD=true
      shift
      ;;
  esac
done

# Require explicit PROJECT_ID and BUCKET
if [[ -z "${PROJECT_ID:-}" ]]; then
  echo "ERROR: PROJECT_ID environment variable is required" >&2
  echo "Usage: PROJECT_ID=your-project BUCKET=your-bucket ./iam-list-verify.sh" >&2
  exit 1
fi

if [[ -z "${BUCKET:-}" ]]; then
  echo "ERROR: BUCKET environment variable is required" >&2
  echo "Usage: PROJECT_ID=your-project BUCKET=your-bucket ./iam-list-verify.sh" >&2
  exit 1
fi

# Safety check for production buckets
if [[ "$BUCKET" == *"-prod"* ]] || [[ "$BUCKET" == *"_prod"* ]] || [[ "$BUCKET" == *"production"* ]]; then
  if [[ "$ALLOW_PROD" != "true" ]]; then
    echo "ERROR: Bucket name '$BUCKET' appears to be a production bucket." >&2
    echo "This script writes test objects and should be run against sandbox/staging." >&2
    echo "To override, pass --allow-prod flag." >&2
    exit 1
  fi
  echo "WARNING: Running against production bucket '$BUCKET' with --allow-prod flag"
fi

ANTI_ENTROPY_SA="arco-compactor-antientropy@${PROJECT_ID}.iam.gserviceaccount.com"
TEST_TENANT="tenant=test-iam"
TEST_WORKSPACE="workspace=verify"
TEST_OBJECTS_CREATED=false

cleanup() {
  if [[ "$TEST_OBJECTS_CREATED" == "true" ]]; then
    echo ""
    echo "Cleaning up test objects..."
    gsutil rm -f "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/ledger/test-event.json" 2>/dev/null || true
    gsutil rm -f "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/commits/test-commit.json" 2>/dev/null || true
    gsutil rm -f "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/state/test-state.parquet" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM

echo "=== IAM List Semantics Verification ==="
echo "Project: ${PROJECT_ID}"
echo "Bucket: ${BUCKET}"
echo "Anti-entropy SA: ${ANTI_ENTROPY_SA}"
echo ""

echo "Step 1: Creating test objects..."
TEST_OBJECTS_CREATED=true
gsutil cp /dev/null "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/ledger/test-event.json"
gsutil cp /dev/null "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/commits/test-commit.json"
gsutil cp /dev/null "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/state/test-state.parquet"

echo "Created test objects in ledger/, commits/, state/"
echo ""

# Step 2: List as anti-entropy SA (should only see ledger/)
echo "Step 2: Listing as anti-entropy SA (impersonated)..."
echo "Expected: Can list ledger/, cannot list commits/ or state/"
echo ""

echo "--- Listing ledger/ prefix ---"
gcloud storage ls "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/ledger/" \
  --impersonate-service-account="${ANTI_ENTROPY_SA}" 2>&1 || true

echo ""
echo "--- Attempting to list commits/ prefix (should fail) ---"
gcloud storage ls "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/commits/" \
  --impersonate-service-account="${ANTI_ENTROPY_SA}" 2>&1 || true

echo ""
echo "--- Attempting to list state/ prefix (should fail) ---"
gcloud storage ls "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/state/" \
  --impersonate-service-account="${ANTI_ENTROPY_SA}" 2>&1 || true

echo ""
echo "--- Attempting to list bucket root (should fail or be empty) ---"
gcloud storage ls "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/" \
  --impersonate-service-account="${ANTI_ENTROPY_SA}" 2>&1 || true

# Step 3: Cleanup (handled by trap, but mark as done for clarity)
echo ""
echo "Step 3: Cleaning up test objects..."
gsutil rm "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/ledger/test-event.json"
gsutil rm "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/commits/test-commit.json"
gsutil rm "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/state/test-state.parquet"
TEST_OBJECTS_CREATED=false

echo ""
echo "=== Verification Complete ==="
echo ""
echo "INTERPRETATION GUIDE:"
echo "  - If ledger/ lists successfully and commits/state/ fail: IAM is working as intended"
echo "  - If all prefixes list successfully: Conditional IAM does NOT constrain list results"
echo "  - If all prefixes fail: Condition may be too restrictive"
echo ""
echo "Document results in release evidence."

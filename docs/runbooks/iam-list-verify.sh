#!/bin/bash
# iam-list-verify.sh
# Verify prefix-scoped IAM list semantics for Arco catalog bucket

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-your-project-id}"
BUCKET="${BUCKET:-${PROJECT_ID}-arco-catalog-prod}"
ANTI_ENTROPY_SA="arco-compactor-antientropy@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=== IAM List Semantics Verification ==="
echo "Project: ${PROJECT_ID}"
echo "Bucket: ${BUCKET}"
echo "Anti-entropy SA: ${ANTI_ENTROPY_SA}"
echo ""

# Step 1: Create test objects in different prefixes
echo "Step 1: Creating test objects..."
TEST_TENANT="tenant=test-iam"
TEST_WORKSPACE="workspace=verify"

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

# Step 3: Cleanup
echo ""
echo "Step 3: Cleaning up test objects..."
gsutil rm "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/ledger/test-event.json"
gsutil rm "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/commits/test-commit.json"
gsutil rm "gs://${BUCKET}/${TEST_TENANT}/${TEST_WORKSPACE}/state/test-state.parquet"

echo ""
echo "=== Verification Complete ==="
echo ""
echo "INTERPRETATION GUIDE:"
echo "  - If ledger/ lists successfully and commits/state/ fail: IAM is working as intended"
echo "  - If all prefixes list successfully: Conditional IAM does NOT constrain list results"
echo "  - If all prefixes fail: Condition may be too restrictive"
echo ""
echo "Document results in release evidence."

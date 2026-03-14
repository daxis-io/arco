# IAM List Semantics Verification Runbook

**Purpose:** Verify that GCP conditional IAM correctly constrains `storage.objects.list` to prefix-scoped results.

**Evidence Policy:** `docs/guide/src/reference/evidence-policy.md`

---

## Background

Arco uses prefix-scoped IAM conditions to constrain service account permissions:

- **Fast-path compactor**: Has NO list permission (intentional)
- **Anti-entropy compactor**: Can list only under `ledger/` prefix

The IAM conditions use `resource.name.matches(...)` with anchored regex patterns (`infra/terraform/iam_conditions.tf:28`).

**Key question**: Does GCP conditional IAM actually constrain the *results* of `storage.objects.list`, or does it just fail the entire list operation if any non-matching objects exist?

---

## Pre-Verification: Understanding GCP IAM Condition Semantics

GCP documentation states that for `storage.objects.list`:
- The condition is evaluated against the **bucket** resource, not individual objects
- The `resource.name` in the condition refers to the bucket path, not object paths

**Implication**: Conditional IAM on `resource.name.matches(...)` for object prefixes may NOT constrain list results as intended. The condition may:
1. Allow listing all objects (if bucket matches)
2. Deny listing entirely (if condition fails)
3. Neither behavior provides prefix-scoped listing

This must be verified empirically.

---

## Verification Script

Run this script in a GCP project with Arco infrastructure deployed:

```bash
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
echo "Attach results to CI artifacts or release assets."
```

---

## Expected Outcomes

### Scenario A: IAM Works As Intended

```
--- Listing ledger/ prefix ---
gs://bucket/tenant=test-iam/workspace=verify/ledger/test-event.json

--- Attempting to list commits/ prefix (should fail) ---
ERROR: ... does not have storage.objects.list access ...

--- Attempting to list state/ prefix (should fail) ---
ERROR: ... does not have storage.objects.list access ...
```

**Action**: Attach verification output to CI artifacts or release assets; no code changes needed.

### Scenario B: Conditional IAM Does NOT Constrain List

```
--- Listing ledger/ prefix ---
gs://bucket/tenant=test-iam/workspace=verify/ledger/test-event.json

--- Attempting to list commits/ prefix (should fail) ---
gs://bucket/tenant=test-iam/workspace=verify/commits/test-commit.json  # <-- UNEXPECTED

--- Attempting to list state/ prefix (should fail) ---
gs://bucket/tenant=test-iam/workspace=verify/state/test-state.parquet  # <-- UNEXPECTED
```

**Action**: This means conditional IAM conditions using `resource.name.matches(...)` do NOT constrain list operations. Options:
1. Accept this limitation and document it (anti-entropy has broader list than intended)
2. Remove anti-entropy list permission and use a different discovery mechanism
3. Use separate buckets for isolation (higher cost, more complexity)

### Scenario C: All Prefixes Fail

```
--- Listing ledger/ prefix ---
ERROR: ... does not have storage.objects.list access ...
```

**Action**: The condition is too restrictive. Verify the regex pattern and ensure it matches the actual object paths.

---

## Nightly CI Integration (Optional)

Add to `.github/workflows/nightly-chaos.yml`:

```yaml
iam-list-semantics:
  runs-on: ubuntu-latest
  if: github.event_name == 'schedule'
  steps:
    - uses: actions/checkout@v4
    - uses: google-github-actions/auth@v2
      with:
        workload_identity_provider: ${{ secrets.WIF_PROVIDER }}
        service_account: ${{ secrets.CI_SA }}
    - run: |
        chmod +x docs/runbooks/iam-list-verify.sh
        ./docs/runbooks/iam-list-verify.sh
      env:
        PROJECT_ID: ${{ vars.GCP_PROJECT_ID }}
```

---

## Evidence Capture Template

After running verification, record results in CI artifacts or release notes using:

```markdown
**IAM list semantics verification (P0-6):**

- Verification date: YYYY-MM-DD
- Verification environment: [PROJECT_ID]
- Result: [Scenario A/B/C]
- Evidence artifact: [link to CI run or manual capture]
- Action taken: [None / Document limitation / Remediation PR]
```

---

## References

- `infra/terraform/iam_conditions.tf:195-208` - Anti-entropy list permission
- `infra/terraform/iam.tf:102` - Custom role for object read without list
- GCP IAM Conditions: https://cloud.google.com/iam/docs/conditions-overview
- GCS IAM: https://cloud.google.com/storage/docs/access-control/iam-permissions

# Runbook: Delta Coordinator + Orchestration Rebuild Disaster Recovery

## Scope
This runbook covers:
- Delta commit coordinator recovery for crash windows between reserve/log/idempotency/finalize.
- Orchestration projection rebuild from explicit ledger manifests (no bucket-wide listing dependency).
- Post-recovery validation checks.

## Required Environment
```bash
export ARCO_TENANT_ID="<tenant>"
export ARCO_WORKSPACE_ID="<workspace>"
export ARCO_STORAGE_BUCKET="<bucket>"
export ARCO_API_URL="https://<arco-api>"
export ARCO_ORCH_COMPACTOR_URL="https://<arco-flow-compactor>"
export ARCO_INTERNAL_TOKEN="<internal-oidc-or-service-token>"
```

## 1) Delta Commit Coordinator Recovery

### 1.1 Inspect current coordinator state
```bash
TABLE_ID="<uuid-v7-table-id>"
gcloud storage cat \
  "gs://${ARCO_STORAGE_BUCKET}/tenant=${ARCO_TENANT_ID}/workspace=${ARCO_WORKSPACE_ID}/delta/coordinator/${TABLE_ID}.json" \
  | jq
```
Expected:
- `inflight` is present when recovering a stuck commit.
- `latest_version` may lag committed delta log version if crash occurred before finalize.

### 1.2 Replay the original commit request (same idempotency key + request body)
```bash
IDEMPOTENCY_KEY="<original-uuid-v7-key>"
cat > /tmp/delta-commit-request.json <<JSON
{
  "read_version": -1,
  "staged_path": "<original-staged-path>",
  "staged_version": "<original-staged-version>",
  "idempotency_key": "${IDEMPOTENCY_KEY}"
}
JSON

curl -sS -X POST \
  "${ARCO_API_URL}/api/v1/delta/tables/${TABLE_ID}/commits" \
  -H "Content-Type: application/json" \
  --data @/tmp/delta-commit-request.json | jq
```
Expected:
- HTTP `200`
- Response includes stable `version` and `delta_log_path` (retries return identical values).

### 1.3 Verify idempotency marker + finalized coordinator state
```bash
KEY_HASH=$(printf '%s' "${IDEMPOTENCY_KEY}" | shasum -a 256 | awk '{print $1}')
PREFIX=${KEY_HASH:0:2}

IDEMP_PATH="delta/idempotency/${TABLE_ID}/${PREFIX}/${KEY_HASH}.json"
gcloud storage ls \
  "gs://${ARCO_STORAGE_BUCKET}/tenant=${ARCO_TENANT_ID}/workspace=${ARCO_WORKSPACE_ID}/${IDEMP_PATH}"

gcloud storage cat \
  "gs://${ARCO_STORAGE_BUCKET}/tenant=${ARCO_TENANT_ID}/workspace=${ARCO_WORKSPACE_ID}/delta/coordinator/${TABLE_ID}.json" \
  | jq '{latest_version, inflight}'
```
Expected:
- Idempotency marker object exists.
- `inflight` is `null`.
- `latest_version` is at least the replayed `version`.

## 2) Orchestration Projection Rebuild (Manifest-Driven)

### 2.1 Build an explicit ledger rebuild manifest
```bash
cat > /tmp/orch-rebuild-manifest.json <<JSON
{
  "event_paths": [
    "ledger/orchestration/2026-02-21/01J1EXAMPLE00000000000001.json",
    "ledger/orchestration/2026-02-21/01J1EXAMPLE00000000000002.json"
  ]
}
JSON

REBUILD_KEY="state/orchestration/rebuilds/$(date +%Y%m%d-%H%M%S).json"
gcloud storage cp /tmp/orch-rebuild-manifest.json \
  "gs://${ARCO_STORAGE_BUCKET}/tenant=${ARCO_TENANT_ID}/workspace=${ARCO_WORKSPACE_ID}/${REBUILD_KEY}"
```
Expected:
- Rebuild manifest uploaded under `state/orchestration/rebuilds/`.

### 2.2 Trigger `/rebuild`
```bash
curl -sS -X POST "${ARCO_ORCH_COMPACTOR_URL}/rebuild" \
  -H "Authorization: Bearer ${ARCO_INTERNAL_TOKEN}" \
  -H "Content-Type: application/json" \
  --data "{\"rebuild_manifest_path\":\"${REBUILD_KEY}\"}" | jq
```
Expected:
- HTTP `200`
- Response contains:
  - `events_processed` (`>= 0`)
  - `manifest_revision` (new revision when state changed)
  - `visibility_status` (`"visible"` or `"persisted_not_visible"`)

### 2.3 Verify manifest watermark advanced
```bash
POINTER=$(gcloud storage cat \
  "gs://${ARCO_STORAGE_BUCKET}/tenant=${ARCO_TENANT_ID}/workspace=${ARCO_WORKSPACE_ID}/state/orchestration/manifest.pointer.json")
MANIFEST_PATH=$(printf '%s' "$POINTER" | jq -r '.manifest_path')

gcloud storage cat \
  "gs://${ARCO_STORAGE_BUCKET}/tenant=${ARCO_TENANT_ID}/workspace=${ARCO_WORKSPACE_ID}/${MANIFEST_PATH}" \
  | jq '.watermarks'
```
Expected:
- `last_visible_event_id` advances to the newest replayed event visible to readers.
- `last_processed_at` updates to current recovery run time.

## 3) Post-Recovery Validation Checklist

Run all DR safety gates:
```bash
cargo test -p arco-delta --all-features --test idempotency_replay -- --nocapture
cargo test -p arco-flow --features test-utils --test orchestration_rebuild_dr -- --nocapture
cargo test -p arco-flow --features test-utils --tests
```
Expected:
- All commands exit `0`.
- `idempotency_replay` proves crash-window replay/finalize safety.
- `orchestration_rebuild_dr` proves rebuild equivalence + stale-watermark recovery without ledger listing.

If any step fails:
- Do not advance rollout.
- Keep compactor/controller automation in guarded mode.
- Capture failing command output and storage object paths for incident follow-up.

# Runbook: Cloud Orchestration Smoke

## Purpose

Use this runbook to verify the honest cloud orchestration loop in a deployed Arco environment:

`manifest deploy -> run trigger -> dispatcher -> Cloud Tasks -> worker -> callback -> SUCCEEDED`

The executable entrypoint is [scripts/cloud-orchestration-smoke.sh](/Users/ethanurbanski/arco/.worktrees/cloud-test-worker/scripts/cloud-orchestration-smoke.sh).

## Preconditions

- Arco services are already deployed.
- `gcloud`, `curl`, and `jq` are installed locally.
- Your operator identity can resolve the target Cloud Run service URLs.
- The target workspace is safe for smoke traffic.
- If Cloud Run IAM is enabled, the script will try `gcloud auth print-identity-token` automatically for invoke auth.

For the current dev environment, the known-good defaults are:

- Project: `arco-testing-20260320`
- Region: `us-central1`
- Environment: `dev`
- Tenant: `tenant-dev`
- Workspace: `workspace-dev`
- Task key: `analytics.users`

## Quick Start

Run the smoke against the current `gcloud` project:

```bash
bash scripts/cloud-orchestration-smoke.sh
```

Run it explicitly against the Arco testing project:

```bash
PROJECT_ID=arco-testing-20260320 \
REGION=us-central1 \
ENVIRONMENT=dev \
TENANT_ID=tenant-dev \
WORKSPACE_ID=workspace-dev \
bash scripts/cloud-orchestration-smoke.sh
```

## What The Script Does

1. Resolves the live `arco-api` and `arco-flow-dispatcher` Cloud Run URLs.
2. Deploys a one-asset manifest for `analytics.users`.
3. Triggers a run with a unique `runKey`.
4. Calls the dispatcher `/run` endpoint on a loop.
5. Polls `GET /api/v1/workspaces/{workspace}/runs/{run_id}` until the task reaches `SUCCEEDED`.

Expected success pattern:

- Manifest deploy returns `201`
- Run trigger returns `201`
- First dispatcher tick usually leaves the task `QUEUED` or `RUNNING`
- A later poll reaches:
  - run `state = SUCCEEDED`
  - task `state = SUCCEEDED`
  - `executionLineageRef.materializationId` present on the task

## Interpreting Results

### Success

The script exits `0` and prints the final run JSON. This proves:

- the API accepted manifest and run traffic
- dispatcher emitted real dispatch work
- Cloud Tasks reached the worker
- the worker sent callbacks back to the API
- callback events folded into the final run state

### Failure

The script exits non-zero and prints the last response body it saw. Use the final state to triage:

- `QUEUED` forever:
  - dispatcher did not emit work or Cloud Tasks did not deliver it
  - if the script shows repeated dispatcher summaries with `ready_dispatch_emitted = 0` and `dispatch_enqueued = 0`, the controller is not seeing any ready work
  - check [task-stuck-dispatched.md](/Users/ethanurbanski/arco/.worktrees/cloud-test-worker/docs/runbooks/task-stuck-dispatched.md)
- `RUNNING` forever:
  - worker likely received the dispatch but callback or compaction stalled
- `FAILED`:
  - inspect worker and API logs for callback failure details

## Manual Follow-Up Commands

Fetch the API URL:

```bash
gcloud run services describe arco-api-dev \
  --project arco-testing-20260320 \
  --region us-central1 \
  --format='value(status.url)'
```

Check the run directly:

```bash
curl -sS \
  -H "X-Tenant-Id: tenant-dev" \
  -H "X-Workspace-Id: workspace-dev" \
  "https://arco-api-dev-wengro33ta-uc.a.run.app/api/v1/workspaces/workspace-dev/runs/<RUN_ID>" | jq '.'
```

Check recent API callback logs:

```bash
gcloud logging read \
  'resource.type="cloud_run_revision" AND resource.labels.service_name="arco-api-dev" AND httpRequest.requestUrl:"/api/v1/tasks/"' \
  --project arco-testing-20260320 \
  --freshness=15m \
  --limit 20 \
  --format='table(timestamp,httpRequest.status,httpRequest.requestMethod,httpRequest.requestUrl)'
```

Check worker dispatch receipt:

```bash
gcloud logging read \
  'resource.type="cloud_run_revision" AND resource.labels.service_name="arco-flow-worker-dev" AND logName="projects/arco-testing-20260320/logs/run.googleapis.com%2Frequests"' \
  --project arco-testing-20260320 \
  --freshness=15m \
  --limit 20 \
  --format='table(timestamp,httpRequest.status,httpRequest.requestMethod,httpRequest.requestUrl)'
```

## Staging And Prod-Style Use

The script assumes the operator can reach the API and dispatcher URLs directly. For internal-only environments:

- run the script from inside the same network boundary, or
- provide `API_URL` and `DISPATCHER_URL` that are reachable from the operator environment

The smoke contract stays the same. Only the network path to the services changes.

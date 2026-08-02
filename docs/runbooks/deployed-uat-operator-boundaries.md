# Deployed UAT operator authority boundaries

Use this runbook before inspecting, repairing, or exercising the deployed Arco
UAT environment. It separates local Cloud SDK access from GCP mutation and
defines when a status snapshot is valid evidence.

## Non-negotiable rules

- Local Cloud SDK access is not cloud-mutation authority. A sandbox approval
  that lets `gcloud` use local credentials does not authorize a deploy, repair,
  IAM change, Scheduler action, storage change, or endpoint invocation.
- Passive metadata inspection must use read-only `describe`, `list`, or log-read
  commands. It must not call deployed endpoints or inspect live object contents.
- Deployed endpoint invocation, including the `/version` check that
  `--require-live-deployed-ready` runs, requires an authorized live UAT
  window even though it does not mutate the GCP control plane.
- Every cloud mutation requires explicit approval, a non-empty
  `ARCO_DEPLOY_OWNER`, and the repository single-owner lock. Dry-run output is
  review material, not mutation authority.
- Concurrent or ad hoc cloud mutation outside the declared owner window
  invalidates every status snapshot in that evidence set. Stop, record the
  interruption, and begin again with a fresh initial snapshot.

## Cloud SDK sandbox access

Cloud SDK stores its credential database, active configuration, access
token cache, logs, and lock files under `~/.config/gcloud` (or
`CLOUDSDK_CONFIG`). A read-only command such as `gcloud run services describe`
may read those files, refresh an access token, or update local cache and lock
state. As a result, a filesystem sandbox can block the command even though the
requested GCP API operation is read-only.

Keep any escalation narrow. A suitable approval request is:

> Allow this read-only `gcloud` describe/status command to access the local
> Cloud SDK credential and configuration cache? It will not update Cloud Run,
> Scheduler, IAM, Terraform, or storage.

That approval permits the named local SDK access and read-only command only. Do
not retrieve or print access tokens, copy credential databases, read secrets,
or broaden it into a mutating command.

## Authority classes

| Class | Examples | Required authority | Evidence meaning |
|---|---|---|---|
| Local SDK access | Read or refresh `~/.config/gcloud`; acquire a local SDK lock | Narrow filesystem/sandbox approval | Enables the named command; proves nothing about deployed state by itself |
| Passive cloud metadata | `gcloud run services describe`, `gcloud scheduler jobs describe`, read-only log queries, `--status` | Read-only cloud access; no mutation approval | Point-in-time metadata snapshot only |
| Deployed invocation | Cloud Run proxy, `/version`, `--preflight-only`, `--require-live-deployed-ready`, `--live-deployed` | Explicit live UAT window; named owner for the full gate | Runtime/provenance evidence for the exact captured revision |
| Cloud mutation | `deploy.sh`, Terraform apply, service update, Scheduler resume/update, IAM change, repair scripts without `--dry-run` | Explicit mutation approval plus `ARCO_DEPLOY_OWNER` and the single-owner lock | Authorized change inside the declared owner window |

`--status-output` writes only a local evidence file. `--status` is passive when
you use it without strict readiness: it reads Scheduler and Cloud Run metadata
but does not invoke `/version`. Strict readiness is different because it starts
the configured Cloud Run proxy and calls `/version` to prove provenance.

## Single-owner evidence sequence

Use one project, region, environment, owner label, expected revision, and
evidence directory for the entire sequence. Record the branch commit and UTC
start time alongside the artifacts.

### 1. Confirm local proxy ownership

The default proxy port is 18080. Before a strict check or full deployed gate,
inspect it without stopping anything:

```bash
lsof -nP -iTCP:18080 -sTCP:LISTEN
```

No output means the port is free. If a process is present, identify its owner
and working context. Do not kill an unknown process. Coordinate with its owner
or select a free port, for example `ARCO_UAT_CLOUD_RUN_PORT=18086`, and retain
that choice for every command and artifact in the sequence.

### 2. Capture the passive initial snapshot

This command reads deployed metadata and writes a local snapshot. Replace the
example provenance values with the reviewed deployment values. Do not present
it as runtime proof because it omits the `/version` call.

For this passive command, `ARCO_DEPLOY_OWNER` supplies the owner label that the
status check expects to find. Setting it does not acquire the repository lock
or authorize mutation.

```bash
ARCO_DEPLOY_OWNER=uat-session \
ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev \
PROJECT_ID=arco-testing-20260320 \
REGION=us-central1 \
ARCO_UAT_TENANT=arco-uat-tenant \
ARCO_UAT_WORKSPACE=arco-uat-workspace \
ARCO_UAT_EXPECTED_API_CODE_VERSION=uat-live \
ARCO_UAT_EXPECTED_API_GIT_SHA=abc123 \
ARCO_UAT_EXPECTED_API_IMAGE=us-central1-docker.pkg.dev/example/arco-api:uat-live \
ARCO_UAT_CLOUD_RUN_PORT=18080 \
./scripts/run_user_acceptance_pipeline_uat.sh --status --status-output target/uat-evidence/live-repair-status/initial-status.txt
```

If the command needs sandbox escalation for Cloud SDK local state, approve only
the read-only metadata operation described above. A failed or partial command
is not a usable snapshot.

### 3. Review repairs before mutation

If the passive snapshot is not ready, review the repository-owned repair plan
without changing GCP:

```bash
ARCO_DEPLOY_OWNER=uat-session \
PROJECT_ID=arco-testing-20260320 \
PROJECT_NUMBER=135245112198 \
FLOW_TENANT_ID=arco-uat-tenant \
FLOW_WORKSPACE_ID=arco-uat-workspace \
ARCO_UAT_EXPECTED_API_CODE_VERSION=uat-live \
ARCO_UAT_EXPECTED_API_GIT_SHA=abc123 \
ARCO_UAT_EXPECTED_API_IMAGE=us-central1-docker.pkg.dev/example/arco-api:uat-live \
./scripts/repair-deployed-uat-prereqs.sh --dry-run --status-output-dir target/uat-evidence/live-repair-status
```

The printed update/resume commands still require separate mutation approval.
After approval, run the same reviewed wrapper without `--dry-run`; it acquires
the local owner lock, rejects a conflicting owner, repairs scope before
Scheduler targets, and performs the strict readiness check before any requested
full UAT gate.

### 4. Require strict readiness before the full gate

Inside the authorized live UAT window, run the exact readiness handoff with the
same values from the initial snapshot:

```bash
ARCO_DEPLOY_OWNER=uat-session \
ARCO_UAT_CLOUD_RUN_SERVICE=arco-api-dev \
PROJECT_ID=arco-testing-20260320 \
REGION=us-central1 \
ARCO_UAT_TENANT=arco-uat-tenant \
ARCO_UAT_WORKSPACE=arco-uat-workspace \
ARCO_UAT_EXPECTED_API_CODE_VERSION=uat-live \
ARCO_UAT_EXPECTED_API_GIT_SHA=abc123 \
ARCO_UAT_EXPECTED_API_IMAGE=us-central1-docker.pkg.dev/example/arco-api:uat-live \
ARCO_UAT_CLOUD_RUN_PORT=18080 \
./scripts/run_user_acceptance_pipeline_uat.sh --require-live-deployed-ready --status-output target/uat-evidence/live-repair-status/final-status.txt
```

Do not run the full deployed gate unless this exits zero. The strict check
proves Scheduler target shape, deploy-owner labels, service tenant/workspace
scope, and exact API provenance for the captured revision.

### 5. Run and retain one coherent gate

Run `--live-deployed` under the same named owner and live UAT approval. Preserve
the initial and final status files, the exact Git SHA and image, structured UAT
evidence, and any failure artifact as one evidence set. Do not combine status,
logs, or runtime output from different revisions or owner windows.

If another session mutates Cloud Run, Scheduler, IAM, Terraform, storage, or the
deployed images during the sequence, mark the evidence invalid even when the
commands appear successful. Re-establish sole ownership, repeat the port check,
and capture a new initial snapshot before proceeding.

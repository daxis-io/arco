# Real-World UAT: Local And Cloud Evidence

Date: 2026-06-09
Last updated: 2026-06-10

## Scope

This report separates deterministic local proof from live GCP deployment proof.
The local tests prove the Arco data pipeline contracts without cloud services.
The cloud probes prove the deployed Cloud Run shape and current blocking state
for scheduled cloud UAT.

## Deterministic Local Proof

| Command | Exit | Key result |
|---|---:|---|
| `docker info --format '{{.ServerVersion}}'` | 0 | Docker daemon was available; server version `29.2.1`. |
| `bash scripts/run_local_pipeline_uat.sh` | 0 | No-cloud local data pipeline UAT wrapper passed. |
| `bash scripts/run_local_pipeline_uat_container.sh --image-tag arco-local-pipeline-uat:local-20260610-optimized` | 0 | Containerized no-cloud local data pipeline UAT passed. Docker build context was reduced to 135.74 MB after excluding local worktrees from `.dockerignore`. |
| `cargo test -p arco-integration-tests --test local_pipeline_uat -- --nocapture` | 0 | `local_pipeline_uat_writes_delta_catalogs_queries_data_and_completes_run` passed. |
| `cargo test -p arco-integration-tests --tests -- --nocapture` | 0 | Integration package passed: catalog flow, idempotency, DataFusion query data, Delta commit coordinator, Delta engine smokes, Iceberg REST catalog, ledger namespace, local pipeline UAT, external worker orchestration, and Unity Catalog smoke tests. |
| `uv run --extra dev pytest tests/integration/test_e2e.py tests/integration/test_cli_api.py -v` from `python/arco` | 0 | 10 Python package integration tests passed. |
| `cargo check -p arco-api --features gcp --bin arco-api --bin arco_flow_worker` | 0 | GCP-feature API and worker binaries compile. |
| `cargo check -p arco-flow --features gcp --bin arco_flow_dispatcher --bin arco_flow_sweeper --bin arco_flow_compactor` | 0 | GCP-feature flow controller binaries compile. |
| `cargo test -p arco-flow --test orchestration_event_log_tests -- --nocapture` | 0 | 9 ADR-041 L0 inbox event-log tests passed after replacing the legacy `payload_json` field name with `payload`. |
| `cargo test -p arco-flow --tests -- --nocapture` | 0 | `arco-flow` package tests passed after the event-log terminology fix, including 468 library tests plus binaries and integration targets. |
| `cargo fmt --all --check` | 0 | Rust formatting check passed. |
| `cargo xtask doctor` | 0 | Toolchain/config doctor passed: Rust, cargo-deny, buf, deny.toml, buf.yaml, and rust-toolchain. |
| `cargo xtask adr-check` | 0 | ADR conformance check passed. |
| `cargo xtask verify-integrity` | 0 | Repo-owned integrity checks passed; live storage checks were skipped because tenant/workspace/bucket inputs were not provided. |
| `cargo xtask engine-boundary-check` | 0 | Engine boundary check passed. |
| `cargo xtask parity-matrix-check` | 0 | Parity matrix evidence check passed. |
| `cargo xtask repo-hygiene-check` | 0 | Repository hygiene check passed after the ADR-041 event-log terminology fix. |
| `cargo xtask ci-parity` | 0 | Local CI parity guardrails passed. |
| `cargo xtask ci` | 0 | Full local CI passed outside the sandbox: doctor, ADR, engine boundary, parity, repo hygiene, proto checks, workspace check/fmt/clippy/tests, `arco-flow` all-feature compile and test-utils suite, docs, and cargo-deny checks. |

## Local Deployment Gate Proof

| Command | Exit | Key result |
|---|---:|---|
| `cargo test -p xtask --test deploy_script -- --nocapture` | 0 | 7 deployment-script regression tests passed, including internal Cloud Run readiness and deploy dry-run guard behavior. |
| `cargo test -p xtask --test cloud_run_image_build -- --nocapture` | 0 | Cloud Run image build dry-run command generation test passed. |
| `cargo test -p xtask --test ci_parity -- --nocapture` | 0 | 8 CI parity tests passed. |
| `docker build --file Dockerfile.cloudrun --build-arg BIN=arco-api --tag arco-api-cloudrun-local:local-20260610 .` | 0 | Production-style Cloud Run API image built locally from the real Cloud Run Dockerfile. Release build finished in 3m 32s and exported `arco-api-cloudrun-local:local-20260610`. |
| `docker run --rm --name arco-api-cloudrun-local-20260610 -e ARCO_DEBUG=true -e ARCO_ENVIRONMENT=dev -e ARCO_API_PUBLIC=false -e ARCO_HTTP_PORT=8080 -e ARCO_GRPC_PORT=9090 -p 28080:8080 -p 29090:9090 arco-api-cloudrun-local:local-20260610` plus `curl --noproxy '*' -fsS http://127.0.0.1:28080/health` | 0 | Production-style API container started locally in debug/in-memory mode and `/health` returned `{"status":"ok"}` with HTTP 200. |
| `./scripts/deploy.sh --env dev --tfvars-file infra/terraform/environments/arco-testing-dev.tfvars --dry-run` with current deployed images | 1 | Terraform initialized and the targeted compactor-first plan refreshed state, but the full plan failed while reading Cloud Tasks queues because `cloudtasks.googleapis.com` returned `BILLING_DISABLED`. |

The local pipeline UAT uses no GCP services. It exercises the real API router,
catalog, in-memory object storage, query path, Delta commit path, manifest
deployment, orchestration dispatch, and task callback contracts.

The repo-owned local entrypoint is:

```bash
bash scripts/run_local_pipeline_uat.sh
```

The repo-owned local container entrypoint is:

```bash
bash scripts/run_local_pipeline_uat_container.sh
```

This requires a running Docker daemon. On this machine, Docker Desktop launched
and Docker server `29.2.1` accepted connections. The optimized container build
transferred a 135.74 MB context and the in-container UAT passed.

The local Cloud Run API smoke used host port `28080` because `127.0.0.1:18080`
was already occupied by an existing `cloud-run` proxy process. Curling `18080`
therefore hit Google Frontend and returned 503, while the same Arco API image on
the unshadowed `28080` mapping returned HTTP 200 from `/health`.

The local pipeline UAT proves:

- catalog and schema creation through the API
- Parquet data write and table registration
- SQL reads through `/api/v1/query-data`
- Delta table registration
- staged Delta commit and committed `_delta_log/00000000000000000000.json`
- Delta log contents including protocol, metadata, add action, and write operation
- catalog/system-table visibility for Parquet and Delta tables
- manifest deployment and run trigger
- ready-dispatch reconciliation
- task callback completion
- final run/task `SUCCEEDED` state with Delta table/version output metadata

## Live Cloud Read-Only Evidence

| Probe | Exit | Key result |
|---|---:|---|
| `gcloud auth list` | 0 | Active account: `ethanurbanski@gmail.com`. |
| `gcloud projects describe arco-testing-20260320 --format=json` | 0 | Project `arco-testing-20260320` is `ACTIVE`, project number `135245112198`. |
| `gcloud run services list --project arco-testing-20260320 --region us-central1 --format=json` | 0 | Cloud Run services are visible and ready, including API, catalog compactor, flow compactor, dispatcher, sweeper, timer ingest, and worker. |
| `gcloud run jobs describe arco-live-pipeline-smoke-dev --project arco-testing-20260320 --region us-central1 --format=json` | 0 | Smoke job is `Ready`, generation 10, execution count 23, latest execution `arco-live-pipeline-smoke-dev-cccds`. |
| `gcloud run jobs executions list --job arco-live-pipeline-smoke-dev --project arco-testing-20260320 --region us-central1 --format=json` | 0 | Latest execution `arco-live-pipeline-smoke-dev-cccds` completed successfully on 2026-06-08T06:16:38.060630Z. |

Observed deployed Cloud Run artifacts:

- API service `arco-api-dev` ready at revision `arco-api-dev-00031-crm`.
- API image:
  `us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-api:live-pipeline-proof-compactor-auth-20260607-022004`.
- Worker service `arco-flow-worker-dev` ready at revision
  `arco-flow-worker-dev-00022-clb`.
- Worker/job image:
  `us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-worker:live-pipeline-proof-delta-contract-20260607-224530`.
- Smoke job target API:
  `https://arco-api-dev-135245112198.us-central1.run.app`.
- Smoke job tenant/workspace:
  `tenant-proof2-20260604` / `workspace-proof2-20260604`.
- Smoke job storage bucket:
  `arco-testing-20260320-arco-catalog-dev`.

## Cloud Scheduler And Billing State

| Probe | Exit | Key result |
|---|---:|---|
| `gcloud billing projects describe arco-testing-20260320 --format=json` | 0 | Rechecked 2026-06-10: project billing link reports `billingEnabled: true` and billing account `billingAccounts/014D5E-A18656-3D0B41`. |
| `gcloud billing accounts describe 014D5E-A18656-3D0B41 --format=json` | 0 | Rechecked 2026-06-10: linked billing account reports `open: false`. |
| `gcloud services list --enabled --project arco-testing-20260320 --filter=name:cloudscheduler.googleapis.com --format=json` | 0 | Cloud Scheduler API is enabled for the project. |
| `gcloud scheduler jobs list --project arco-testing-20260320 --location us-central1 --format=json` | 1 | Rechecked 2026-06-10: Cloud Scheduler rejects the call with `BILLING_DISABLED`. |
| `gcloud scheduler locations list --project arco-testing-20260320 --format=json` | 1 | Cloud Scheduler also rejects location listing with `BILLING_DISABLED`. |
| `gcloud tasks queues list --project arco-testing-20260320 --location us-central1 --format=json` | 1 | Rechecked 2026-06-10: Cloud Tasks rejects queue listing with `BILLING_DISABLED`. |

The root cloud blocker is the linked billing account state, not missing local
credentials and not a missing Cloud Scheduler API enablement. The project-level
billing link still reports enabled, but the billing account itself is closed
(`open: false`), and Cloud Scheduler/Cloud Tasks enforce the billing
requirement at API call time.

## Not Proven In This Pass

- A fresh cloud-triggered Scheduler execution. Scheduler calls are blocked by
  `BILLING_DISABLED`.
- A complete Terraform deploy dry-run. The dry-run reaches provider refresh and
  targeted planning, but the full plan cannot refresh Cloud Tasks queues while
  the linked billing account is closed.
- A fresh mutating Cloud Run job execution on 2026-06-10. The cloud probes were
  intentionally read-only.
- Fresh application log payloads for the latest job execution. Cloud Run
  execution status proves container execution success, but this pass did not
  retrieve application-level logs.
- Cleanup or restoration of any temporary Cloud Run ingress, scale, IAM, or
  proof-namespace drift from earlier live UAT work.

## Current Conclusion

Arco has deterministic local proof that real data can be written, cataloged,
queried, committed as Delta, and tied to an orchestrated run through callbacks.
The local repository/deployment gates also pass through full CI, compile,
hygiene, integrity, ADR, parity, documentation, dependency-policy, and
deploy-script regression coverage. The GCP deployment is visible and Cloud Run
services/jobs are ready, with the latest known smoke job execution succeeding
on 2026-06-08. Scheduled/cloud-queued UAT and complete deploy planning cannot
be exercised until the linked billing account is reopened or the project is
moved to an open billing account.

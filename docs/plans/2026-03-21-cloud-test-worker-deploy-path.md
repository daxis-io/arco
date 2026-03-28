# Cloud Test Worker Deploy Path Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a minimal Arco-owned test worker plus a reproducible container build/deploy path so `arco-testing-20260320` can run the full cloud orchestration loop end-to-end.

**Architecture:** Keep the production boundary intact by adding a narrow, explicitly test-oriented worker runtime that only accepts canonical `WorkerDispatchEnvelope` payloads and reports lifecycle callbacks back to `arco-api`. Pair that with one generic Rust service Docker build path and fix Terraform/deploy wiring so API, compactors, dispatcher, sweeper, timer-ingest, and the test worker can all be built and deployed consistently.

**Tech Stack:** Rust, Axum, Reqwest, Docker, Google Artifact Registry, Terraform, Cloud Run

---

### Task 1: Add the failing worker service tests

**Files:**
- Create: `crates/arco-flow-worker/tests/service_smoke.rs`
- Create: `crates/arco-flow-worker/src/lib.rs`
- Create: `crates/arco-flow-worker/Cargo.toml`
- Modify: `Cargo.toml`

**Step 1: Write the failing test**

Write an integration test that starts a fake callback server, sends a canonical `WorkerDispatchEnvelope` to the worker `/dispatch` route, and asserts:
- `/health` returns `200`
- `/dispatch` accepts the envelope
- worker posts `started` then `completed` to the callback server
- callback requests carry the envelope task token in `Authorization`

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-flow-worker --test service_smoke -- --nocapture`

Expected: FAIL because the crate and worker service do not exist yet.

**Step 3: Write minimal implementation**

Create a small `arco-flow-worker` crate exposing:
- an Axum app builder in `src/lib.rs`
- a binary entrypoint in `src/main.rs`
- `GET /health`
- `POST /dispatch`

The first implementation should:
- parse `WorkerDispatchEnvelope`
- POST `started`
- POST `completed` with deterministic success payload
- return `202 Accepted`

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-flow-worker --test service_smoke -- --nocapture`

Expected: PASS

**Step 5: Commit**

```bash
git add Cargo.toml crates/arco-flow-worker
git commit -m "feat: add cloud test worker runtime"
```

### Task 2: Add the generic container build path

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`
- Create: `scripts/build-images.sh`
- Modify: `docs/guide/src/introduction.md`

**Step 1: Write the failing verification target**

Define the build contract in `scripts/build-images.sh` first:
- inputs for project, region, repository, tag
- per-service image outputs for:
  - `arco-api`
  - `arco-compactor`
  - `arco_flow_compactor`
  - `arco_flow_dispatcher`
  - `arco_flow_sweeper`
  - `arco_flow_timer_ingest`
  - `arco-flow-worker`

Verify the script exits non-zero before the Dockerfile exists.

Run: `bash scripts/build-images.sh --project arco-testing-20260320 --tag dev-latest`

Expected: FAIL because the Docker build context/path is incomplete.

**Step 2: Write minimal implementation**

Add one multi-stage Rust Dockerfile that accepts `PACKAGE` and `BIN` build args and emits a runtime image that starts the selected binary. Add a build script that:
- creates/uses Artifact Registry repo `arco`
- builds Linux/amd64 images
- tags them under `us-central1-docker.pkg.dev/<project>/arco/...`
- optionally pushes them
- prints exportable image env vars for deployment

Document that this worker is a test runtime, not the long-term production worker boundary.

**Step 3: Run verification**

Run:
- `bash scripts/build-images.sh --project arco-testing-20260320 --tag dev-latest --dry-run`
- `docker build --platform linux/amd64 --build-arg PACKAGE=arco-flow-worker --build-arg BIN=arco-flow-worker -t arco-flow-worker:test .`

Expected: both commands succeed

**Step 4: Commit**

```bash
git add Dockerfile .dockerignore scripts/build-images.sh docs/guide/src/introduction.md
git commit -m "build: add rust service image pipeline"
```

### Task 3: Fix Terraform and deploy wiring for the real service contract

**Files:**
- Modify: `infra/terraform/cloud_run.tf`
- Modify: `infra/terraform/cloud_run_flow.tf`
- Modify: `infra/terraform/variables.tf`
- Modify: `infra/terraform/environments/arco-testing-dev.tfvars`
- Modify: `scripts/deploy.sh`

**Step 1: Write the failing verification**

Run:
- `terraform -chdir=infra/terraform validate`
- `PROJECT_ID=arco-testing-20260320 API_IMAGE=x COMPACTOR_IMAGE=x FLOW_COMPACTOR_IMAGE=x FLOW_DISPATCHER_IMAGE=x FLOW_SWEEPER_IMAGE=x FLOW_TIMER_INGEST_IMAGE=x FLOW_WORKER_IMAGE=x ENVIRONMENT=dev bash scripts/deploy.sh --dry-run`

Expected: FAIL because current deploy wiring omits required flow env vars and does not pass `flow_timer_ingest_image`.

**Step 2: Write minimal implementation**

Update Terraform and deploy wiring so:
- API gets task token env vars
- dispatcher and sweeper get callback base URL plus flow task-token env vars
- timer-ingest enablement keys off real required images/tenant/workspace inputs
- `arco-testing-dev.tfvars` points at the Artifact Registry image names instead of placeholder hello images
- `scripts/deploy.sh` requires and exports `FLOW_TIMER_INGEST_IMAGE`

**Step 3: Run verification**

Run:
- `terraform -chdir=infra/terraform fmt`
- `terraform -chdir=infra/terraform validate`
- `PROJECT_ID=arco-testing-20260320 API_IMAGE=test/api COMPACTOR_IMAGE=test/compactor FLOW_COMPACTOR_IMAGE=test/flow-compactor FLOW_DISPATCHER_IMAGE=test/flow-dispatcher FLOW_SWEEPER_IMAGE=test/flow-sweeper FLOW_TIMER_INGEST_IMAGE=test/flow-timer FLOW_WORKER_IMAGE=test/flow-worker ENVIRONMENT=dev bash scripts/deploy.sh --dry-run`

Expected: all commands succeed

**Step 4: Commit**

```bash
git add infra/terraform/cloud_run.tf infra/terraform/cloud_run_flow.tf infra/terraform/variables.tf infra/terraform/environments/arco-testing-dev.tfvars scripts/deploy.sh
git commit -m "infra: wire cloud test worker deployment"
```

### Task 4: Verify end-to-end local and cloud build readiness

**Files:**
- No new files expected

**Step 1: Run focused tests**

Run:
- `cargo test -p arco-flow-worker --test service_smoke -- --nocapture`
- `cargo test -p arco-flow --features test-utils --test worker_dispatch_envelope_tests -- --nocapture`

Expected: PASS

**Step 2: Run build/deploy smoke verification**

Run:
- `bash scripts/build-images.sh --project arco-testing-20260320 --tag dev-latest --dry-run`
- `terraform -chdir=infra/terraform validate`
- `PROJECT_ID=arco-testing-20260320 API_IMAGE=test/api COMPACTOR_IMAGE=test/compactor FLOW_COMPACTOR_IMAGE=test/flow-compactor FLOW_DISPATCHER_IMAGE=test/flow-dispatcher FLOW_SWEEPER_IMAGE=test/flow-sweeper FLOW_TIMER_INGEST_IMAGE=test/flow-timer FLOW_WORKER_IMAGE=test/flow-worker ENVIRONMENT=dev bash scripts/deploy.sh --dry-run`

Expected: PASS

**Step 3: Optional real push/deploy**

Run:
- `bash scripts/build-images.sh --project arco-testing-20260320 --tag dev-latest --push`
- `PROJECT_ID=arco-testing-20260320 API_IMAGE=us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-api:dev-latest COMPACTOR_IMAGE=us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-compactor:dev-latest FLOW_COMPACTOR_IMAGE=us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-compactor:dev-latest FLOW_DISPATCHER_IMAGE=us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-dispatcher:dev-latest FLOW_SWEEPER_IMAGE=us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-sweeper:dev-latest FLOW_TIMER_INGEST_IMAGE=us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-timer-ingest:dev-latest FLOW_WORKER_IMAGE=us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-worker:dev-latest ENVIRONMENT=dev bash scripts/deploy.sh`

Expected: image push succeeds and deploy enters Cloud Run health-gated rollout.

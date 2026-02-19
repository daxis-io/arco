# PR Slicing and Scope Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Convert local-only work into right-sized, behavior-scoped PRs that are easy to review and safe to merge.

**Architecture:** Build a stacked branch queue from `origin/main`, preserving existing commit intent first, then layering uncommitted work in focused slices. Keep infra rollout and observability updates in separate PRs so runtime behavior changes are reviewed independently from deployment shape and alerting.

**Tech Stack:** Git/GitHub, Rust/Cargo, Python/pytest, Terraform.

---

## Snapshot (2026-02-16)

- Unique local commits not on remotes: `103` (`git rev-list --count --branches --not --remotes`)
- Current branch: `codex/manifest-pointer-fencing-durability`
- Divergence from `origin/main`: behind `4`, ahead `2`
- Working tree: `19` tracked modified files + `6` untracked files

## Scope Rules (must hold for every PR)

- One behavior change per PR.
- Keep deploy/IAM/Terraform in a separate PR from runtime logic.
- Keep docs/alerts in a separate PR from infra resources when possible.
- No “cleanup” commits mixed into feature PRs unless required by CI.
- Every PR has explicit verification commands in the PR body.

## Task 1: Capture a Stable Slicing Baseline

**Files:**
- Modify: none

**Step 1: Refresh refs and record baseline**

```bash
git fetch --all --prune
git rev-list --left-right --count origin/main...HEAD
git status --short
```

**Step 2: Snapshot current dirty state into a temporary branch commit**

```bash
git switch -c codex/wip-pr-slice-20260216
git add -A
git commit -m "wip: snapshot before PR slicing"
WIP_SHA=$(git rev-parse HEAD)
echo "$WIP_SHA"
```

**Expected:** One deterministic commit containing all currently uncommitted edits.

## Task 2: PR 1 (Base Durability Semantics)

**PR title:** `durability: immutable manifest pointers and epoch fencing foundation`

**Files:**
- Cherry-pick commit: `955f25a`

**Step 1: Create branch from latest base and apply commit**

```bash
git switch -c codex/pr1-durability-pointer-foundation origin/main
git cherry-pick 955f25a
```

**Step 2: Verify**

```bash
cargo test -p arco-catalog failure_injection -- --nocapture
cargo test -p arco-flow orchestration_parity_gates_m2 -- --nocapture
```

**Step 3: Push and open PR**

```bash
git push -u origin codex/pr1-durability-pointer-foundation
```

## Task 3: PR 2 (Gate-4 Follow-Up on Durability)

**PR title:** `durability: gate-4 follow-up and compaction hardening`

**Files:**
- Cherry-pick commit: `82da29f`

**Depends on:** PR 1

**Step 1: Branch from PR 1 and apply follow-up commit**

```bash
git switch -c codex/pr2-durability-gate4-followup codex/pr1-durability-pointer-foundation
git cherry-pick 82da29f
```

**Step 2: Verify**

```bash
cargo test -p arco-flow orchestration_parity_gates_m2 -- --nocapture
cargo test -p arco-api --lib -- --nocapture
```

**Step 3: Push and open PR targeting PR 1**

```bash
git push -u origin codex/pr2-durability-gate4-followup
```

## Task 4: PR 3 (Runtime Callback/Auth Hardening)

**PR title:** `flow: internal OIDC auth + per-dispatch callback token hardening`

**Files:**
- Restore from `WIP_SHA`:
  - `Cargo.lock`
  - `crates/arco-api/src/routes/tasks.rs`
  - `crates/arco-compactor/src/main.rs`
  - `crates/arco-core/Cargo.toml`
  - `crates/arco-core/src/lib.rs`
  - `crates/arco-core/src/internal_oidc.rs`
  - `crates/arco-flow/Cargo.toml`
  - `crates/arco-flow/src/bin/arco_flow_compactor.rs`
  - `crates/arco-flow/src/bin/arco_flow_dispatcher.rs`
  - `crates/arco-flow/src/bin/arco_flow_sweeper.rs`
  - `crates/arco-flow/src/bin/arco_flow_timer_ingest.rs`
  - `crates/arco-flow/src/orchestration/callbacks/handlers.rs`
  - `crates/arco-flow/src/orchestration/controllers/dispatcher.rs`
  - `python/arco/src/arco_flow/worker/server.py`
  - `python/arco/tests/unit/test_worker_callback_token.py`

**Depends on:** PR 2

**Step 1: Create branch and restore only runtime/auth files**

```bash
git switch -c codex/pr3-flow-runtime-auth-hardening codex/pr2-durability-gate4-followup
WIP_SHA=$(git rev-parse codex/wip-pr-slice-20260216)
git restore --source="$WIP_SHA" -- \
  Cargo.lock \
  crates/arco-api/src/routes/tasks.rs \
  crates/arco-compactor/src/main.rs \
  crates/arco-core/Cargo.toml \
  crates/arco-core/src/lib.rs \
  crates/arco-core/src/internal_oidc.rs \
  crates/arco-flow/Cargo.toml \
  crates/arco-flow/src/bin/arco_flow_compactor.rs \
  crates/arco-flow/src/bin/arco_flow_dispatcher.rs \
  crates/arco-flow/src/bin/arco_flow_sweeper.rs \
  crates/arco-flow/src/bin/arco_flow_timer_ingest.rs \
  crates/arco-flow/src/orchestration/callbacks/handlers.rs \
  crates/arco-flow/src/orchestration/controllers/dispatcher.rs \
  python/arco/src/arco_flow/worker/server.py \
  python/arco/tests/unit/test_worker_callback_token.py
git add \
  Cargo.lock \
  crates/arco-api/src/routes/tasks.rs \
  crates/arco-compactor/src/main.rs \
  crates/arco-core/Cargo.toml \
  crates/arco-core/src/lib.rs \
  crates/arco-core/src/internal_oidc.rs \
  crates/arco-flow/Cargo.toml \
  crates/arco-flow/src/bin/arco_flow_compactor.rs \
  crates/arco-flow/src/bin/arco_flow_dispatcher.rs \
  crates/arco-flow/src/bin/arco_flow_sweeper.rs \
  crates/arco-flow/src/bin/arco_flow_timer_ingest.rs \
  crates/arco-flow/src/orchestration/callbacks/handlers.rs \
  crates/arco-flow/src/orchestration/controllers/dispatcher.rs \
  python/arco/src/arco_flow/worker/server.py \
  python/arco/tests/unit/test_worker_callback_token.py
git commit -m "flow: harden internal auth and callback token scoping"
```

**Step 2: Verify**

```bash
cargo check -p arco-core -p arco-api -p arco-compactor -p arco-flow
cargo test -p arco-api --lib -- --nocapture
cargo test -p arco-flow --lib -- --nocapture
pytest python/arco/tests/unit/test_worker_callback_token.py -q
```

**Step 3: Push and open PR targeting PR 2**

```bash
git push -u origin codex/pr3-flow-runtime-auth-hardening
```

## Task 5: PR 4 (Terraform and CI Rollout for Flow Services)

**PR title:** `infra: provision flow control-plane services and terraform CI validation`

**Files:**
- Restore from `WIP_SHA`:
  - `.github/workflows/terraform-plan-validate.yml`
  - `infra/terraform/cloud_run_flow.tf`
  - `infra/terraform/cloud_tasks.tf`
  - `infra/terraform/iam.tf`
  - `infra/terraform/variables.tf`
  - `infra/terraform/environments/dev.tfvars.example`

**Depends on:** PR 3

**Step 1: Create branch and restore infra-only files**

```bash
git switch -c codex/pr4-flow-terraform-rollout codex/pr3-flow-runtime-auth-hardening
WIP_SHA=$(git rev-parse codex/wip-pr-slice-20260216)
git restore --source="$WIP_SHA" -- \
  .github/workflows/terraform-plan-validate.yml \
  infra/terraform/cloud_run_flow.tf \
  infra/terraform/cloud_tasks.tf \
  infra/terraform/iam.tf \
  infra/terraform/variables.tf \
  infra/terraform/environments/dev.tfvars.example
git add \
  .github/workflows/terraform-plan-validate.yml \
  infra/terraform/cloud_run_flow.tf \
  infra/terraform/cloud_tasks.tf \
  infra/terraform/iam.tf \
  infra/terraform/variables.tf \
  infra/terraform/environments/dev.tfvars.example
git commit -m "infra: add flow cloud run/tasks resources and terraform CI plan checks"
```

**Step 2: Verify**

```bash
terraform -chdir=infra/terraform init -backend=false
terraform -chdir=infra/terraform validate
```

**Step 3: Push and open PR targeting PR 3**

```bash
git push -u origin codex/pr4-flow-terraform-rollout
```

## Task 6: PR 5 (Observability and Queue-Depth Contract)

**PR title:** `observability: add flow go-live alerts and explicit queue-depth semantics`

**Files:**
- Restore from `WIP_SHA`:
  - `crates/arco-flow/src/dispatch/cloud_tasks.rs`
  - `docs/runbooks/metrics-catalog.md`
  - `docs/runbooks/prometheus-alerts.yaml`
  - `infra/monitoring/alerts.yaml`

**Depends on:** PR 3 (can target PR 4 if preferred for a single stack)

**Step 1: Create branch and restore observability files**

```bash
git switch -c codex/pr5-flow-observability-queue-depth codex/pr3-flow-runtime-auth-hardening
WIP_SHA=$(git rev-parse codex/wip-pr-slice-20260216)
git restore --source="$WIP_SHA" -- \
  crates/arco-flow/src/dispatch/cloud_tasks.rs \
  docs/runbooks/metrics-catalog.md \
  docs/runbooks/prometheus-alerts.yaml \
  infra/monitoring/alerts.yaml
git add \
  crates/arco-flow/src/dispatch/cloud_tasks.rs \
  docs/runbooks/metrics-catalog.md \
  docs/runbooks/prometheus-alerts.yaml \
  infra/monitoring/alerts.yaml
git commit -m "observability: add go-live alerts and queue-depth unavailable errors"
```

**Step 2: Verify**

```bash
cargo test -p arco-flow dispatch::cloud_tasks -- --nocapture
rg "ArcoFlowCallback5xxRateHigh|ArcoOrchCallback5xxRateHigh|StaleFenceRejects" \
  docs/runbooks/prometheus-alerts.yaml infra/monitoring/alerts.yaml
```

**Step 3: Push and open PR**

```bash
git push -u origin codex/pr5-flow-observability-queue-depth
```

## Task 7: Local Branch Backlog Triage (Repo Hygiene)

**Goal:** Reduce non-actionable local branch noise before/while shipping stack.

**Immediate delete candidates (`ahead=0`):**
- `ArcoFlowCloudPipelineReadiness`
- `feat/arco-intergration-readiness`
- `feat/gaps-harding`
- `orch-read-api-fixes`
- `orch-read-api-hardening`

**Step 1: Verify each is fully merged where expected**

```bash
for b in \
  ArcoFlowCloudPipelineReadiness \
  feat/arco-intergration-readiness \
  feat/gaps-harding \
  orch-read-api-fixes \
  orch-read-api-hardening; do
  git rev-list --left-right --count origin/main..."$b"
done
```

**Step 2: Delete locally once confirmed**

```bash
git branch -d ArcoFlowCloudPipelineReadiness
git branch -d feat/arco-intergration-readiness
git branch -d feat/gaps-harding
git branch -d orch-read-api-fixes
git branch -d orch-read-api-hardening
```

---

## Definition of Done

- PR 1 through PR 5 are open with explicit file boundaries above.
- Each PR includes verification commands and output in its description.
- At least the five `ahead=0` local branches are removed.
- Remaining local-only branches are intentionally retained (not accidental residue).

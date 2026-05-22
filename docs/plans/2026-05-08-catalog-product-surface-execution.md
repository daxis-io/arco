# Catalog Product Surface Execution Plan

> **For implementers:** Execute this plan task-by-task with verification after each phase.

**Goal:** Execute the catalog product surface plan safely as a multi-phase catalog platform program with review checkpoints, green commits, and no accidental "API parity as architecture" drift.

**Architecture:** Treat the source plan as the product contract and this file as the delivery playbook. Execute in small reviewable phases, starting with design/security contracts, then native state, then enforcement, then adapters and system tables. Every phase must preserve the Arco-native ledger/projection/pointer model and must keep compatibility APIs as adapters over authoritative Arco state.

**Tech Stack:** Rust, Protobuf/prost, Axum, Arrow/Parquet, DataFusion, mdBook, `cargo`, `buf`, `xtask`, git worktrees.

---

## Source Plan

Primary plan:

- catalog product surface plan

Related plans:

- managed Delta correctness governance plan
- system catalog tables plan
- authoritative metastore governance surface plan

## Execution Status (2026-05-22)

The completed proto enhancement plans are closed and must not be reopened for
new catalog product work. New protobuf changes in this program are additive
native metastore-contract expansions inside the catalog product and metastore
governance plans, with `cargo xtask proto-breaking-check` as the compatibility
gate.

Current branch progress:

- Phase 0 contracts landed in `b39cb13`, with follow-up contract alignment in
  `d2d2d19`, `90e45e3`, and `6869d6e`.
- Phase 1 additive native metastore object contracts landed in `d14eba4`.
- Phase 2 initial metastore replay/projection kernel landed in `2ff14e1`.
- Phase 3 identity, grants, permission compilation, and UC `GET /permissions`
  behavior is partially landed. Writer-backed grant mutations and
  `PATCH /permissions` remain pending.
- Phase 4 storage credential and external location create/list/get behavior is
  partially landed over scoped metastore events, compiled metastore `MANAGE`,
  redaction, path canonicalization, and overlap checks. Update/delete lifecycle
  operations, broader bindings, and system-table exposure remain pending.
- Phase 5 credential vending is partially landed for UC table/path credential
  decisions over compiled authorization and published storage governance.
  Provider token material, revocation metadata, volume credentials, and
  model-version credentials remain pending.

Do not restart landed slices unless review requests a corrective follow-up. The
remaining phase work should continue as small green slices with review
checkpoints.

## Execution Rules

- Do not execute the full catalog product plan in one branch.
- Use a dedicated worktree and branch per phase or tightly coupled pair of phases.
- Commit only green slices. Do not commit default-failing tests.
- Stop and ask before changing architecture if the implementation conflicts with the source plan's invariants.
- Keep compatibility routes as adapters over native state.
- Do not register system tables before their authoritative projection exists.
- Do not expose credentials, tokens, secret handles, raw policy payloads, or hidden objects through list/get/search/system-table responses.
- Run verification before claiming a phase is complete.
- Report after each phase with changed files, commands run, command outcomes, open risks, and next recommended phase.

## Phase Map

| Phase | Source Tasks | Status | Branch Suggestion | Review Gate |
|---|---:|---|---|---|
| 0 | Task 0 | Landed | `catalog-product-phase-0-contracts` | Docs/security/API contracts accepted |
| 1 | Task 1 | Landed | `catalog-product-phase-1-native-contracts` | Additive proto/API compatibility gates pass |
| 2 | Task 2 | Initial kernel landed | `catalog-product-phase-2-metastore-kernel` | Replay/projection/publish kernel accepted |
| 3 | Task 3 | Partial | `catalog-product-phase-3-authz` | `GET /permissions` and compiled reads landed; grant writer and `PATCH /permissions` pending |
| 4 | Task 4 | Partial | `catalog-product-phase-4-storage-governance` | Storage credential/external location create/list/get landed; update/delete, bindings, and system tables pending |
| 5 | Tasks 5-6 | Partial | `catalog-product-phase-5-credentials-volumes` | Table/path credential decisions landed; provider minting, revocation metadata, volumes, and model credentials pending |
| 6 | Tasks 7-8 | Pending | `catalog-product-phase-6-governance-lineage` | Governance metadata and discovery accepted |
| 7 | Task 9 | Pending | `catalog-product-phase-7-functions-models` | Metadata-only function/model registry accepted |
| 8 | Task 10 | Pending | `catalog-product-phase-8-system-tables` | ACL-filtered system tables/query sandbox accepted |
| 9 | Task 11 | Pending | `catalog-product-phase-9-conformance-hardening` | Compatibility, hardening, evidence accepted |

## Preflight

**Step 1: Create an isolated workspace**

Run:

```bash
git status --short
git rev-parse --show-toplevel
git branch --show-current
```

Expected: understand the starting branch and whether uncommitted user work exists.

If there is unrelated dirty work, create a new git worktree rather than editing in place.

**Step 2: Read the source plan and current status docs**

Read:

```bash
sed -n '1,220p' docs/guide/src/reference/control-plane-scope.md
sed -n '1,220p' docs/guide/src/reference/system-catalog.md
sed -n '1,220p' docs/plans/2026-05-07-catalog-product-surface.md
```

Expected: source plan is understood before edits.

**Step 3: Baseline verification**

Run the cheapest baseline checks first:

```bash
cargo xtask adr-check
cd docs/guide && mdbook build
```

Expected: baseline docs pass before Phase 0 edits. If they fail, report the existing failure before changing files.

**Step 4: Critical review checkpoint**

Before editing, report:

- any contradiction between source plan and current repo
- any missing command/tool such as `buf`, `cargo-deny`, or `cargo-audit`
- any task that would require committing a default-failing test
- any scope that should be deferred

Proceed only if no blocking concerns remain.

## Phase 0: Contracts First

Execute Task 0 from the source plan.

Deliverables:

- `docs/adr/adr-037-arco-catalog-product-surface.md`
- `docs/adr/adr-038-catalog-threat-model.md`
- `docs/adr/adr-039-catalog-consistency-model.md`
- `docs/guide/src/reference/catalog-privilege-matrix.md`
- `docs/guide/src/reference/catalog-api-contract.md`
- `docs/guide/src/reference/schema-evolution-policy.md`
- `docs/guide/src/reference/credential-vending-security.md`
- scorecard and UC inventory updates

Verification:

```bash
cargo xtask adr-check
cd docs/guide && mdbook build
```

Review output:

- summarize each new contract doc in 2-3 bullets
- list any implementation tasks that changed because of the docs
- stop for review before Phase 1

## Phase 1: Additive Native Contract Rules

Execute Task 1 from the source plan only after Phase 0 is accepted.

Hard requirements:

- no default-failing tests
- additive proto changes only unless explicitly versioned as breaking
- reserved field numbers/names for removed fields
- enum zero values for new enums
- deterministic transaction hashing remains stable

This phase is catalog product work over the frozen `arco.*.v1` surface, not a
continuation of the completed proto hard-cut or pre-freeze expansion plans.

Verification:

```bash
cargo test -p arco-proto --test control_plane_transactions -- --nocapture
cargo test -p arco-catalog --test metastore_product_surface -- --nocapture
buf lint proto/
cargo xtask proto-breaking-check
```

If `buf` or `cargo xtask proto-breaking-check` is unavailable, stop and report the missing tool instead of silently skipping it.

## Phase 2: Metastore Kernel

Execute Task 2 from the source plan.

Deliver the kernel, not every object family. The objective is replay, projection registration, publication, watermarks, redaction, and extension points.

Verification:

```bash
cargo test -p arco-catalog --test metastore_replay_publication -- --nocapture
cargo test -p arco-catalog --test schema_contracts -- --nocapture
```

If this phase adds new projection replay or redaction xtask commands, run them
before commit. Otherwise cover replay and redaction with the focused tests above
and leave the xtask gates in the future hardening list.

Stop for review before starting authorization.

## Phase 3: Identity And Authorization

Execute Task 3 from the source plan.

This is a high-risk phase. Keep it generic first:

- principals
- groups
- group membership revisions
- ownership
- grants
- inherited privileges
- `AuthzDecision`
- explain-access

Do not add volume-specific privilege tests until volume object state exists.
External-location follow-ups should target the landed storage-governance route
and credential-vending boundaries.

Verification:

```bash
cargo test -p arco-catalog --test permission_compilation -- --nocapture
cargo test -p arco-catalog --test authz_decisions -- --nocapture
cargo test -p arco-uc --test permissions_authoritative -- --nocapture
```

If this phase adds a new privilege-matrix xtask command, run it before commit.
Otherwise do not cite that gate as passing evidence; rely on the focused tests
above and leave the xtask gate in the future hardening list.

Review checkpoint must include an authorization matrix summary.

## Phase 4: Storage Governance

Execute Task 4 from the source plan.

Treat path governance as a core security subsystem:

- URI canonicalization
- overlap detection
- managed roots
- external locations
- workspace bindings
- secret metadata separation
- provider preconditions

Verification:

```bash
cargo test -p arco-catalog --test path_governance -- --nocapture
cargo test -p arco-uc --test storage_governance_authoritative -- --nocapture
```

If this phase adds a new redaction xtask command, run it before commit.
Otherwise cover redaction behavior with focused tests and leave the xtask gate
in the future hardening list.

For remaining Phase 4 lifecycle work, stop for review before changing
credential-vending behavior beyond the already landed table/path decisions.

## Phase 5: Credential Vending And Volumes

Execute Tasks 5 and 6 as two commits or two PRs if they grow large.

Order:

1. Finish provider token material, expiry exposure, and revocation metadata for
   the landed table/path credential decision path.
2. Add volumes as governed path objects.
3. Add volume credential vending through the shared engine.

Verification:

```bash
cargo test -p arco-catalog --test credential_vending_decisions -- --nocapture
cargo test -p arco-uc --test credentials_authoritative -- --nocapture
cargo test -p arco-uc --test volumes_authoritative -- --nocapture
```

If this phase adds a new redaction xtask command, run it before commit.
Otherwise cover redaction behavior with focused tests and leave the xtask gate
in the future hardening list.

Do not add model-version credentials in this phase.

## Phase 6: Governance Metadata, Lineage, And Discovery

Execute Tasks 7 and 8.

Keep policy attachments as typed metadata unless a policy engine boundary exists.

Verification:

```bash
cargo test -p arco-catalog --test governance_attachments -- --nocapture
cargo test -p arco-api --test governance_metadata_api -- --nocapture
cargo test -p arco-api --test catalog_discovery_api -- --nocapture
cargo test -p arco-api --test lineage_catalog_objects -- --nocapture
```

Review checkpoint must explicitly discuss whether discovery creates an existence side channel.

## Phase 7: Functions And Models

Execute Task 9.

Boundaries:

- function metadata only
- no UDF execution
- model registry metadata only
- no model serving
- model-version artifact path ownership before model credentials

Verification:

```bash
cargo test -p arco-uc --test functions_authoritative -- --nocapture
cargo test -p arco-uc --test models_authoritative -- --nocapture
```

## Phase 8: System Tables And Query Sandbox

Execute Task 10.

Only register system tables whose owning authoritative projection exists.

Verification:

```bash
cargo test -p arco-api --test catalog_product_system_tables_api -- --nocapture
cargo test -p arco-api --test access_system_tables_api -- --nocapture
cargo test -p arco-api --test storage_system_tables_api -- --nocapture
cargo test -p arco-api --test query_sandbox -- --nocapture
```

If this phase adds new system-table schema or redaction xtask commands, run them
before commit. Otherwise cover schema and redaction behavior with focused tests
and leave the xtask gates in the future hardening list.

Review checkpoint must include:

- table allowlist
- redaction proof
- ACL behavior
- freshness/watermark behavior

## Phase 9: Compatibility And Hardening

Execute Task 11.

This phase closes the program. It must not introduce new product domains.

Verification:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo test --workspace --doc
cargo xtask adr-check
cargo xtask verify-integrity
cargo xtask repo-hygiene-check
cargo xtask engine-boundary-check
cargo xtask parity-matrix-check
buf lint proto/
cargo xtask proto-breaking-check
cargo deny check
cargo audit
cargo semver-checks
cd docs/guide && mdbook build
```

Future GA gates must be implemented in `tools/xtask` before they are required
or reported as passing:

- schema compatibility check
- OpenAPI diff check
- system-table schema compatibility check
- privilege-matrix drift check
- redaction check
- projection replay check
- compatibility fixture check
- benchmark smoke check

If this phase implements one of those commands, run it before commit and record
the exact output in the evidence report. Otherwise add an explicit follow-up and
do not report the missing gate as passing.

## Stop Conditions

Stop and ask before continuing if:

- the source plan conflicts with current code reality
- a task requires a default-failing committed test
- a route would expose object data or credentials before authorization exists
- a system table would expose a projection before the owning authoritative state exists
- secret material appears in a projection, log, trace, error, fixture, or snapshot
- a compatibility adapter needs behavior that weakens native Arco invariants
- verification repeatedly fails after a focused fix attempt
- a task expands into a new product domain not in the source plan

## Execution Prompt

Use this prompt in a fresh implementation session that continues after the current branch
status:

```text
You are in /Users/ethanurbanski/arco.

Use the `executing-plans` skill. Also use `using-git-worktrees` if the current worktree is dirty or if you need an isolated implementation branch.

Execute this catalog product surface execution plan, using the catalog product surface plan as the source product plan.

Start with Preflight and the next unfinished slice only. Do not restart already landed Phase 0-5 behavior unless review explicitly asks for a corrective follow-up.

Hard rules:
- Treat Unity Catalog as prior art and compatibility surface only; Arco-native state is authoritative.
- Do not commit default-failing tests.
- Do not expose credentials, raw tokens, secret handles, hidden objects, or raw policy payloads in list/get/search/system-table responses.
- Keep compatibility APIs as adapters over Arco-native state.
- Stop and ask if the plan conflicts with current code, if a verification command is unavailable, or if implementation would weaken the invariants in the source plan.
- Run the verification commands listed for each phase before reporting completion.

For the next authorization/storage-governance follow-up, implement only one
tight slice from the remaining gaps:
- writer-backed grant persistence or UC `PATCH /permissions`
- storage credential/external location update/delete lifecycle behavior
- provider-backed credential material and revocation metadata
- volume or model credential support after authoritative object ownership lands
- system-table projection exposure after safe Parquet projections land

At the checkpoint, report:
- files changed
- authorization matrix summary
- exact verification commands and outcomes
- any source-plan concerns discovered
- the next unfinished slice

Then stop and wait for review.
```

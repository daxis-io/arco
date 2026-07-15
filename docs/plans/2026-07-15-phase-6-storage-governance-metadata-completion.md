# Phase 6 Storage-Governance Metadata Completion Implementation Plan

**Implementation protocol:** Execute this plan task-by-task. Preserve the
merged Phase 6A hardening, keep the root checkout untouched, and do not broaden
the authority boundary without updating this plan and rerunning the full exit
gate.

**Goal:** Complete Phase 6 by adding non-secret credential-reference metadata,
external-location metadata, and workspace/metastore binding metadata to the
existing control-store domain without moving vending or enforcement authority.

**Architecture:** Continue using the `path-governance-metadata` control-store
domain introduced by Phase 6A. External-location creation commits its metadata
record and companion path-governance declaration atomically. All successful
writes return retained `StateToken`s; reads are token-pinned; compiled-state
readiness binds to a source token and denies closed for missing, stale, or
scope-mismatched state. Projection lag remains diagnostic only.

**Tech Stack:** Rust 2024, `arco-catalog`, `ControlMvpStateStore`, the existing
path-governance predicate model, serde JSON metadata records, and focused Cargo
tests.

---

## Base And Scope

The implementation branch starts directly from `4bfd60b` (`Add Phase 6A
path-governance metadata (#318)`), which is the current `origin/main` at plan
time. All implementation and verification use that integration base.

Files allowed to change:

- `docs/plans/2026-07-15-phase-6-storage-governance-metadata-completion.md`
- `crates/arco-catalog/src/state_store.rs`
- `crates/arco-catalog/src/state_store/path_governance_metadata.rs`
- `crates/arco-catalog/src/state_store/external_location_metadata.rs`
- `crates/arco-catalog/src/state_store/workspace_binding_metadata.rs`
- `crates/arco-catalog/tests/credential_vending_decisions.rs`

Hard non-goals:

- no credential secrets or provider token material;
- no credential-vending authority movement;
- no grant or revocation-sensitive enforcement movement;
- no broad catalog DDL, API, proto, or system-table changes;
- no enforcement reads from lagging projections;
- no snapshots, exports, transaction handles, Phase 7, or Phase 8 work.

## Task 1: Establish The Clean Phase 6 Remainder Base

**Files:**

- Create: this plan

**Step 1: Verify the integration base**

Run:

```bash
git fetch origin --prune
git merge-base --is-ancestor origin/main HEAD
git diff --quiet origin/main...HEAD
```

Expected: all commands exit zero before implementation begins.

**Step 2: Verify the catalog baseline**

Run:

```bash
cargo test -p arco-catalog
```

Expected: the complete package test suite passes before Phase 6B or 6C code is
ported.

**Step 3: Commit the plan**

```bash
git add docs/plans/2026-07-15-phase-6-storage-governance-metadata-completion.md
git commit -m "docs: plan remaining Phase 6 metadata work"
```

## Task 2: Add Phase 6B Credential-Reference And External-Location Metadata

**Files:**

- Modify: `crates/arco-catalog/src/state_store.rs`
- Modify: `crates/arco-catalog/src/state_store/path_governance_metadata.rs`
- Create: `crates/arco-catalog/src/state_store/external_location_metadata.rs`

**Step 1: Preserve the previously verified Phase 6B contract tests**

Port the existing tests for:

- usable state tokens from credential-reference and external-location writes;
- token-pinned reads and token-unavailable status;
- exact credential-reference serialization with no extensible property bag;
- missing credential rejection;
- exact, ancestor, and descendant path conflicts across Phase 6A and 6B;
- non-overlapping sibling paths;
- same-token companion path-declaration readback;
- credential-reference readback at the later external-location token;
- unsupported domain rejection;
- vending independence and projection-lag diagnostics.

**Step 2: Add merged-Phase-6A regression tests before reconciliation**

Add focused tests that require:

- the shared staging helper to reject a declaration whose workspace differs
  from the transaction scope;
- a percent-escaped external-location URI to be canonicalized exactly once and
  produce the same canonical URI in the location record and companion path
  declaration;
- compiled external-location state to carry a source `StateToken` and deny
  closed when its scope differs from the required token.

Run:

```bash
cargo test -p arco-catalog state_store::path_governance_metadata
cargo test -p arco-catalog state_store::external_location_metadata
```

Expected: the new regression tests fail for the intended missing integration
behavior before production reconciliation.

**Step 3: Reconcile the shared path-governance seam**

Extract one crate-private staging helper from the merged Phase 6A writer. Pass
the expected `StateScope` explicitly, validate the declaration workspace in the
helper, and preserve all exact/ancestor/descendant, range-empty,
range-unchanged, and input-set preconditions. Keep metadata-key construction on
the trusted canonical URI without reparsing it.

Add a crate-private constructor that builds a `PathGovernanceDeclaration` from
an already parsed `GovernedPath`. Both direct Phase 6A declarations and Phase
6B external locations must canonicalize raw input once.

**Step 4: Implement the Phase 6B metadata writer**

Add credential-reference records containing only:

- `credential_id`, `name`, `cloud`, `owner`, `lifecycle_state`,
  and `updated_at_ms`.

Credential-reference records intentionally have no arbitrary property bag. A
serialized-field allowlist test must prove that provider tokens, secret
references, encrypted payloads, and caller-defined secret-bearing fields cannot
be persisted through this metadata type.

Add external-location records containing:

- `location_id`, `name`, `canonical_uri`, `credential_id`,
  `path_declaration_id`, `owner`, `lifecycle_state`, `updated_at_ms`, and
  `properties`.

Creating an external location must require an existing credential reference
and atomically stage the location record plus its companion
`EXTERNAL_LOCATION` path declaration. Add token-pinned read/status methods,
diagnostic projection lag, and a token-bound compiled-state readiness type with
missing, stale, scope-mismatch, and ready states.

**Step 5: Verify Phase 6B green**

Run:

```bash
cargo fmt --check
cargo test -p arco-catalog state_store::path_governance_metadata
cargo test -p arco-catalog state_store::external_location_metadata
cargo test -p arco-catalog projection_outbox_acks
cargo test -p arco-catalog credential_vending
cargo test -p arco-catalog shadow
cargo test -p arco-catalog projection
cargo check -p arco-catalog
git diff --check
```

Expected: all commands exit zero and all 17 merged Phase 6A tests remain green.

**Step 6: Commit Phase 6B**

```bash
git add crates/arco-catalog/src/state_store.rs \
  crates/arco-catalog/src/state_store/path_governance_metadata.rs \
  crates/arco-catalog/src/state_store/external_location_metadata.rs
git commit -m "feat(catalog): add external location metadata slice"
```

## Task 3: Add Phase 6C Workspace/Metastore Binding Metadata

**Files:**

- Modify: `crates/arco-catalog/src/state_store.rs`
- Create: `crates/arco-catalog/src/state_store/workspace_binding_metadata.rs`
- Modify: `crates/arco-catalog/tests/credential_vending_decisions.rs`

**Step 1: Add the Phase 6C regression tests**

Add tests for:

- duplicate binding-ID rejection;
- duplicate workspace/metastore pair rejection;
- workspace/scope mismatch rejection;
- token-pinned reads and token-unavailable status;
- compiled binding state missing, stale, and scope-mismatch denial;
- projection lag remaining diagnostic only;
- stale credential-vending authorization returning no credential-bearing
  fields and retaining an audit identifier.

Run:

```bash
cargo test -p arco-catalog state_store::workspace_binding_metadata
cargo test -p arco-catalog --test credential_vending_decisions
```

Expected: the new binding-state scope test fails before the token-bound
compiled-state implementation is present; closure-only proofs may already pass
because they lock existing behavior.

**Step 2: Implement workspace-binding metadata**

Add a crate-private writer in the existing `path-governance-metadata` domain.
Records contain `binding_id`, `workspace_id`, `metastore_id`, `owner`,
`lifecycle_state`, `updated_at_ms`, and `properties`. Reject duplicate IDs,
duplicate workspace/metastore pairs, and records whose workspace differs from
the writer scope. Add token-pinned reads/status, diagnostic projection lag, and
token-bound compiled-state readiness with scope-mismatch denial.

**Step 3: Run the combined Phase 6 gate**

Run:

```bash
cargo fmt --check
cargo test -p arco-catalog state_store::path_governance_metadata
cargo test -p arco-catalog state_store::external_location_metadata
cargo test -p arco-catalog state_store::workspace_binding_metadata
cargo test -p arco-catalog credential_vending
cargo test -p arco-catalog projection_outbox_acks
cargo test -p arco-catalog --test state_store_control_mvp
cargo test -p arco-catalog --test state_store_model
cargo test -p arco-catalog shadow
cargo test -p arco-catalog projection
cargo check -p arco-catalog
git diff --check
```

Expected: all commands exit zero.

**Step 4: Commit Phase 6C**

```bash
git add crates/arco-catalog/src/state_store.rs \
  crates/arco-catalog/src/state_store/workspace_binding_metadata.rs \
  crates/arco-catalog/tests/credential_vending_decisions.rs
git commit -m "feat(catalog): close Phase 6 storage governance metadata"
```

## Task 4: Run The PR-Readiness Gate

**Step 1: Run repository-wide required checks**

Run the local equivalents of the required GitHub contexts:

```bash
cargo xtask repo-hygiene-check
cargo check --workspace --all-features
cargo check -p arco-api --no-default-features --features full
cargo check -p arco-api --no-default-features --features jwt-rust-crypto --tests
cargo fmt --all --check
cargo clippy --workspace --all-features -- -D warnings
cargo test --workspace --all-features --exclude arco-flow --exclude arco-api
cargo test -p arco-api --all-features --tests -- \
  --skip parity_m1_ --skip parity_m2_ --skip parity_m3_
cargo test -p arco-api --all-features --test task_token_contract_tests
cargo test -p arco-api --all-features --test orchestration_parity_gates_m1
cargo test -p arco-flow --features test-utils --no-run
cargo test -p arco-flow --features "test-utils,legacy-scheduler" --no-run
cargo test --doc --workspace
cargo deny check bans licenses sources
cargo deny check advisories
buf lint proto/
cargo test -p arco-core --test ui
```

Run `mdbook build` from `docs/guide` if the documentation tree requires it.
Proto compatibility is unchanged because the allowed diff contains no proto or
baseline files; CI remains authoritative for the remote breaking comparison.

**Step 2: Audit the final diff**

Run:

```bash
git merge-base --is-ancestor origin/main HEAD
git diff --name-status origin/main...HEAD
git diff --stat origin/main...HEAD
git diff --check origin/main...HEAD
git status --short --branch
```

Expected: the branch is clean, contains only the six allowed files, and is
directly based on current `origin/main`.

## Exit Gate

- All four Phase 6 metadata areas are covered: path governance,
  credential-reference metadata, external locations, and workspace/metastore
  bindings.
- External-location and companion path-declaration state commits atomically.
- Exact, ancestor, and descendant conflicts remain fail-closed under the merged
  Phase 6A predicate model.
- Raw paths are canonicalized once.
- Successful writes return usable `StateToken`s and token-pinned reads work.
- Compiled-state readiness denies closed for missing, stale, or
  scope-mismatched source tokens.
- Projection lag does not participate in enforcement decisions.
- Credential records contain no secrets and credential vending ignores the new
  metadata domain.
- Credential vending authority, grants, catalog DDL, APIs, system tables,
  snapshots, exports, and later roadmap phases remain untouched.
- Required local checks pass and the branch is clean and PR-ready without any
  push or PR creation.

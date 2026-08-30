# Cloud-Agnostic State Storage Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the `control/v1` state kernel cloud-neutral at the crate and interface seams while isolating S3, GCS, and Azure construction and qualification in provider crates.

**Architecture:** `arco-core` retains provider-neutral storage contracts and deterministic memory storage. A shared `arco-storage-object-store` adapter translates the upstream object-store interface, three provider crates own provider construction, and `arco-storage` owns runtime selection. The catalog state kernel uses a scoped authority-only view and contains no provider dependency or selection logic.

**Tech Stack:** Rust 1.88, async-trait, bytes, object_store 0.11, Tokio tests, Cargo workspace architecture checks.

---

### Task 1: Enforce the desired crate graph

**Files:**
- Create: `tools/xtask/tests/storage_provider_boundaries.rs`
- Modify: `Cargo.toml`
- Create: `crates/arco-storage-object-store/Cargo.toml`
- Create: `crates/arco-storage-s3/Cargo.toml`
- Create: `crates/arco-storage-gcs/Cargo.toml`
- Create: `crates/arco-storage-azure/Cargo.toml`
- Create: `crates/arco-storage/Cargo.toml`

**Steps:**

1. Add an architecture test that requires all five storage crates, forbids a
   production `object_store` dependency in `arco-core` and `arco-catalog`, and
   requires each provider crate to depend on the provider-neutral core and the
   shared adapter.
2. Run `cargo test -p xtask --test storage_provider_boundaries` and verify it
   fails because the crates and dependency separation do not exist.
3. Add the workspace members and minimal crate manifests/modules.
4. Run the architecture test and keep it red until provider code has moved out
   of core; do not weaken its assertions.

### Task 2: Extract the generic object-store adapter

**Files:**
- Create: `crates/arco-storage-object-store/src/lib.rs`
- Create: `crates/arco-storage-object-store/tests/adapter_contract.rs`
- Modify: `crates/arco-core/src/storage.rs`
- Modify: `crates/arco-core/Cargo.toml`
- Modify: `crates/arco-core/tests/storage_backend_conformance.rs`

**Steps:**

1. Write conformance tests against the wished-for generic adapter using the
   upstream in-memory store, including create, exact-version CAS, stale CAS,
   delete/recreate token rejection, range reads, and non-CAS error preservation.
2. Run the new test and verify it fails because the adapter does not exist.
3. Move the generic adapter and version-token/error translation into
   `arco-storage-object-store`; keep provider builders out of it.
4. Remove the generic adapter and production `object_store` dependency from
   `arco-core`; retain its independently used HTTP types and memory conformance.
5. Run both core memory conformance and shared-adapter conformance tests.

### Task 3: Add independently owned provider adapters

**Files:**
- Create: `crates/arco-storage-s3/src/lib.rs`
- Create: `crates/arco-storage-gcs/src/lib.rs`
- Create: `crates/arco-storage-azure/src/lib.rs`
- Create: provider integration tests under each crate's `tests/`

**Steps:**

1. Add compile-time tests that each provider type satisfies `StorageBackend`,
   and deterministic tests for bucket/container normalization and empty input.
2. Run the tests and verify they fail because the provider types are absent.
3. Implement provider newtypes over the shared adapter, delegating the storage
   interface while retaining provider-specific builders and capability rules.
4. Move the ignored live CAS conformance entry points to the owning provider
   crates. Keep them ignored and credential-gated; do not access cloud services.
5. Run each provider crate's nonignored tests.

### Task 4: Move runtime selection out of core

**Files:**
- Create: `crates/arco-storage/src/lib.rs`
- Create: `crates/arco-storage/tests/provider_selection.rs`
- Modify: runtime Cargo manifests and source files currently calling
  `ObjectStoreBackend::from_bucket`

**Steps:**

1. Add factory contract tests for all supported schemes, bare-name GCS
   compatibility, empty values, and S3 directory-bucket capability selection.
2. Run the tests and verify they fail because the composition interface is
   absent.
3. Implement `arco_storage::from_bucket` returning
   `Arc<dyn StorageBackend>` and route to the three provider crates.
4. Replace runtime imports and calls in API, Flow workers, compactor, and local
   integration harnesses.
5. Run checks for every changed runtime crate.

### Task 5: Restrict the state kernel to authority operations

**Files:**
- Create: `crates/arco-core/src/authority_storage.rs`
- Modify: `crates/arco-core/src/lib.rs`
- Modify: `crates/arco-catalog/src/state_store/control_mvp.rs`
- Modify: `crates/arco-catalog/Cargo.toml`
- Modify: catalog state-store tests

**Steps:**

1. Write tests for a `ScopedAuthorityStore` that exposes read, metadata,
   create-if-absent, and exact-version replace, but no unconditional write,
   delete, list, or signing interface.
2. Run the focused test and verify it fails because the interface is absent.
3. Implement the wrapper over `ScopedStorage` without changing storage
   semantics.
4. Change `ControlMvpStateStore` and its private helpers to retain only the
   authority wrapper plus the previously derived opaque binding identity.
5. Remove the catalog's direct `object_store` dependency and run all state-store
   focused suites.

### Task 6: Align the decision record and qualification language

**Files:**
- Modify: `docs/adr/adr-043-s3-state-token-authority.md`
- Modify: `docs/plans/2026-08-24-phase1-second-audit-remediation.md`

**Steps:**

1. State that `control/v1` is provider-neutral and S3 is the first GA adapter,
   not part of the state-transition identity.
2. Record the provider crate ownership and the independently gated live
   qualification contract.
3. Preserve all existing statements that repository tests do not establish
   provider or production qualification.
4. Run `cargo xtask adr-check` and `git diff --check`.

### Task 7: Verify, review, and commit

**Steps:**

1. Run formatting and architecture checks.
2. Run Clippy for every new storage crate, `arco-core`, and `arco-catalog`.
3. Run provider-neutral core/adapter tests, all catalog state-store focused
   tests, runtime construction tests, doctests, and `cargo test --workspace`.
4. Review the full refactor for provider leakage, compatibility regressions,
   duplicated logic, and misleading qualification claims.
5. Address every P0/P1/P2 finding and repeat affected gates.
6. Commit the implementation as small local commits. Do not push, open a pull
   request, deploy, access cloud credentials, or claim provider qualification.

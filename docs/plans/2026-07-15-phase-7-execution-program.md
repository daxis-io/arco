# Phase 7 Snapshot, Export, Restore, And Durable Handles Execution Program

> **Execution requirements:** Use `executing-plans`,
> `test-driven-development`, `systematic-debugging`, and
> `verification-before-completion` one slice at a time. Keep code mutations
> with one worktree owner and use independent read-only spec-compliance and
> code-quality review gates.

**Goal:** Deliver retained workspace cuts, portable exports, roll-forward
restore, and durable control-plane transaction handles as four separately
reviewed commits without changing production mutation authority.

**Architecture:** Phase 7 layers product/workflow records over the existing
domain-scoped control-store and root-transaction primitives. Snapshot, export,
restore-journal, and handle records are immutable or CAS-selected metadata;
only existing state-store transactions and root transactions can publish
mutation-visible authority. Request-time correctness follows explicit paths and
never discovers state by listing.

**Tech Stack:** Rust 2024, `arco-catalog`, `arco-api`, object-store CAS and
create-if-absent primitives, serde/JSON, SHA-256, ULID, Arrow/Parquet,
DataFusion system tables, Cargo, Buf, and repository `xtask` checks.

---

## Authoritative Base And Provenance

Original execution started from fetched `origin/main` at
`a1b473992ff9afcea247bcc04999ef31b65f0c1b` in:

```text
.worktrees/phase7-snapshot-export-restore-handles
```

After all four slices and Phase 7D review hardening were complete, the
four-commit stack was refreshed onto current `origin/main` at
`1859830df83d6453ddee4beb14d55a339c83dc74`. The only upstream change between
the original execution base and this refreshed integration base was
`Cargo.lock`; the original SHA above remains the authoring and preflight
provenance.

The dirty root checkout is not an implementation source. Its integrity
baseline is:

```text
HEAD: 8acff327d4605bf4c935693b91179f4c1ff5fb8c
status: D docs/plans/2026-06-27-state-store-seam-current-adapter-slice.md
diff sha256: c97fb8097e59d92a79fc3a66fd7020b0666f3b1a0a531afa1665cbc89caf0e1d
```

The June 25 control-store strategy, June 26 combined vision, and June 27
unified execution roadmap are not tracked by this integration base. They were
read only from local history for background. The Phase 7 contract supplied for
this execution is the authoritative specification. Do not copy or stage the
root-local roadmap documents.

Preflight evidence:

- Phase 6A merge `4bfd60b` is an ancestor of `origin/main`.
- Phase 6B/6C merge `a1b4739` is the original fetched execution-base tip.
- `1859830` is the refreshed integration-base tip for the completed stack.
- `state_store.rs`, `path_governance_metadata.rs`,
  `external_location_metadata.rs`, `workspace_binding_metadata.rs`, and
  `credential_vending_decisions.rs` exist on the base.
- `.worktrees/` is ignored by `.gitignore`.
- The complete final verification matrix passed on the unmodified worktree
  before Phase 7 planning or implementation.

## Program Invariants

These apply to every Phase 7 slice:

1. Existing pointer CAS and state-store transaction commits remain the only
   mutation-visibility boundaries.
2. Snapshot/export services write retained-cut metadata, pin revisions, and
   projection-outbox metadata only. They do not write public Parquet or publish
   mutation-visible roots.
3. Restore is roll-forward. It creates strictly newer authority state and never
   mutates historical snapshots or compatibility artifacts.
4. Cross-domain restore and handles are durable, repairable workflows over
   existing domain/root commits, not newly claimed distributed transactions.
5. Every request-time read addresses a known object path. Listing is permitted
   only for background inventory/GC.
6. Opaque `StateToken` and `CheckpointToken` values remain private and
   non-serializable. Durable records contain validated stable references.
7. Old-path compatibility artifacts are read-only and protected only when an
   active retained root names them explicitly.
8. `system.*` tables are read-only, manifest-selected, and expose exact safe
   schemas. They never become correctness or authorization inputs.
9. No credential-vending authority, grant enforcement, DDL transport,
   protobuf tag, CLI, SQL command, retry-policy, or Phase 8 change is in scope.
10. A malformed retention root, uncertain visibility, or missing durable
    recovery evidence fails closed.

## Commit And Review Protocol

Phase 7 has exactly four amendable implementation commits:

1. `feat(catalog): define snapshot export contracts`
2. `feat(catalog): implement workspace snapshot exports`
3. `feat(catalog): add roll-forward snapshot restore`
4. `feat(control-plane): add durable transaction handles`

For each commit:

1. Write its child plan before implementation changes.
2. Add one focused failing test at a time and capture expected red output.
3. Implement the minimum green behavior.
4. Run the child plan's focused tests, checks, formatting, and diff checks.
5. Stage only slice-owned files and commit once.
6. Record base and head SHAs.
7. Dispatch a fresh spec-compliance reviewer against the child plan and diff.
8. Fix every correctness, safety, authority-boundary, and scope blocker; amend
   the commit; rerun verification; and repeat spec review until approved.
9. Dispatch a fresh code-quality reviewer against the final base/head pair.
10. Fix Important/Critical findings, amend, reverify, and re-review.
11. Require a clean worktree before writing the next child plan.

## Slice Gates

### Phase 7A: Snapshot/Export Contract MVP

Child plan:
`docs/plans/2026-07-15-phase-7a-snapshot-export-contract-mvp.md`.

Exit only when versioned canonical records round-trip, stable authority
references resolve without exposing opaque tokens, malformed retention roots
abort GC before deletion, every retained-root category is protected, and
`system.catalog.snapshots` is exact-schema and manifest-selected. No creation
service, restore workflow, durable handle, transport, or authority movement may
exist.

### Phase 7B: Workspace Snapshot/Export Implementation

Write `docs/plans/2026-07-15-phase-7b-workspace-snapshot-export.md` from the
committed 7A surface immediately before implementation.

Exit only when an explicitly configured domain registry can checkpoint every
domain, immutable create-if-absent is retry-safe, export follows and verifies
explicit references without listing, restore preflight is read-only and
complete, and services publish no authority or public Parquet.

### Phase 7C: Roll-Forward Restore

Write `docs/plans/2026-07-15-phase-7c-roll-forward-restore.md` from committed 7B
APIs immediately before implementation.

Exit only when all participants preflight before mutation, restored states
publish strictly newer tokens, omitted domains use an explicit `Omit` or
`Reject` policy, the restore journal makes partial progress repairable and
idempotent, and CAS loss leaves the prior winner visible. If a durable journal
and safe recovery cannot be proven, stop after 7B.

### Phase 7D: Durable Transaction Handles

Write `docs/plans/2026-07-15-phase-7d-durable-transaction-handles.md` from the
final 7C restore/transaction APIs immediately before implementation.

Exit only when typed staged mutations, CAS lifecycle transitions, TTL,
single-return review-token secrecy, crash recovery, low-level receipt
reconciliation, and exact-schema `system.catalog.transactions` are proven.
Arbitrary opaque payloads and secret material must be rejected.

## Final Verification Matrix

Run all child-plan focused suites, then run every command below fresh from the
final clean worktree:

```bash
cargo fmt --all --check
cargo check -p arco-catalog
cargo test -p arco-catalog --test state_store_model -- --nocapture
cargo test -p arco-catalog --test state_store_control_mvp -- --nocapture
cargo test -p arco-catalog gc -- --nocapture
cargo test -p arco-catalog
cargo check -p arco-api
cargo test -p arco-api --test system_tables_api -- --nocapture
cargo test -p arco-api --test control_plane_transactions_api -- --nocapture
cargo test -p arco-api --test root_transaction_protocol -- --nocapture
cargo test -p arco-core --test control_plane_transaction_contracts -- --nocapture
cargo test -p arco-core --test control_plane_transaction_paths_contracts -- --nocapture
cargo test -p arco-proto --test control_plane_transactions -- --nocapture
buf lint proto/
cargo xtask proto-breaking-check
cargo xtask repo-hygiene-check
cargo check --workspace --all-features
cargo clippy --workspace --all-features -- -D warnings
git diff --check
git diff --check origin/main...HEAD
git status --short --branch
```

Filtered commands must execute nonzero relevant tests. Run dependency checks
only if manifests change. Run mdBook checks only if guide content changes.

## Completion Evidence

The final report must include:

- fetched base and prerequisite ancestry;
- branch and worktree paths;
- all five Phase 7 plan paths;
- four final commit SHAs and per-commit file lists;
- captured red/green evidence and the full final matrix;
- review findings and their resolutions;
- any limits or intentionally deferred behavior;
- root HEAD/status/diff-digest comparison;
- clean final worktree status;
- explicit confirmation of no publication.

# Arco Unified Execution Roadmap

**Implementation protocol:** Execute this roadmap task-by-task through small
child plans. Do not broaden scope without updating the child plan and passing
the relevant phase gate.

**Goal:** Turn the Tier-1 authority, Olympia-inspired product contract,
lineage projection, and planner/runtime design docs into one sequenced
execution program.

**Architecture:** Execute seams and contracts before authority migration. The
program keeps one final Tier-1 write authority per migrated scope, derived
watermarked projections, planner-owned semantic lowering, runtime-owned
convergence, and lineage as append-only observations projected into read-only
views.

**Tech Stack:** Rust workspace, object-store-backed control-plane state,
Parquet/Arrow projections, mdBook/docs, GitHub Actions, provider conformance
tests, Arco orchestration/event logs.

---

## Source Documents

This roadmap is an execution guide over these local design docs:

- `docs/plans/2026-06-26-arco-tier1-single-authority-combined-vision.md`
- `docs/plans/2026-06-25-arco-tier1-control-store-strategy.md`
- `docs/plans/2026-06-20-olympia-inspired-arco-strategy.md`
- `docs/plans/2026-06-26-lineage-observation-projection-design.md`
- `docs/plans/2026-06-27-planner-runtime-seam-hardening-design.md`

Use `docs/guide/src/reference/control-plane-scope.md` before every slice to
separate implemented repo behavior from proposed architecture.

## How To Use This Roadmap

This is not a single giant implementation plan. Use it as the parent program.
For each slice:

1. Re-read the source documents named in the phase.
2. Re-check current repo state with `git status --short`.
3. Confirm current implementation status in `docs/guide/src/reference/control-plane-scope.md`.
4. Write a child plan in `docs/plans/YYYY-MM-DD-<slice-name>.md`.
5. Make the child plan exact: files, tests, commands, expected failures, and commit boundary.
6. Implement only the child slice.
7. Stop at the phase gate before broadening scope.

Every implementation slice should be small enough to review as one PR. Prefer
docs-only, tests-only, seam-only, model-only, or one narrow domain behavior
over broad cross-program changes.

## Program Rules

These rules apply to every phase:

1. One migrated Tier-1 scope has exactly one production write authority.
2. The current ledger plus synchronous compactor path is a migration adapter,
   shadowing tool, rollback aid, and retained-history compatibility path, not a
   permanent peer authority for migrated domains.
3. Successful future Tier-1 writes end at committed authority state plus
   `StateToken`; Parquet publication is derived and watermarked.
4. System tables, search, audit, lineage, and derived indexes never become
   enforcement or mutation surfaces.
5. Authorization and credential vending read authority state or fresh-enough
   compiled state and fail closed when stale or missing.
6. Object-store listing is never required for request-time correctness.
7. No two roles get independent CAS authority over the same mutation-visible root.
8. Runtime controllers do not call `PlanCompiler`, resolve asset selections,
   expand partitions, or synthesize semantic tasks.
9. Lineage observations are append-only. Projections can derive graph, search,
   diagnostics, and governance reachability views, but orchestrators do not
   mutate catalog rows to publish lineage.
10. High-frequency runtime telemetry, task heartbeats, logs, and metrics stay
    out of the first strongly consistent control-store tranche.

## Phase List

1. Phase 0: Consolidate language, contracts, and open decisions.
2. Phase 1A: State-store seam with current adapter.
3. Phase 1B: Planner/runtime handoff seam.
4. Phase 2: Contract, conformance, root ownership, provider/IAM matrix.
5. Phase 3A: Deterministic state model.
6. Phase 3B: Object-store control-store MVP.
7. Phase 3C: Prototype correctness, performance, and fallback gate.
8. Phase 4A: Shadow replay and projection equivalence.
9. Phase 4B: Internal read-only comparison reads.
10. Phase 5: First low-risk writable domains.
11. Parallel Lineage Lane: L0 through L6.
12. Phase 6: Storage-governance metadata without vending authority.
13. Phase 7A: Snapshot/export contract MVP.
14. Phase 7B: Workspace snapshot/export implementation.
15. Phase 7C: Roll-forward restore.
16. Phase 7D: Durable transaction handles.
17. Phase 8: Idempotency, grants, and narrow catalog DDL pilots.
18. Phase 9: Per-domain cutover.
19. Phase 10: Ergonomics, rich projections, derived indexes.
20. Phase 11: Retire old Tier-1 authority path.

## Phase 0: Consolidate Language, Contracts, And Open Decisions

**Goal:** Make the architecture legible before code spreads inconsistent
assumptions.

**Source docs:**

- `2026-06-26-arco-tier1-single-authority-combined-vision.md`
- `2026-06-25-arco-tier1-control-store-strategy.md`
- `2026-06-20-olympia-inspired-arco-strategy.md`
- `2026-06-26-lineage-observation-projection-design.md`
- `2026-06-27-planner-runtime-seam-hardening-design.md`

**Deliverables:**

- `docs/adr/adr-0XX-tier1-control-store-single-authority.md`
- `docs/adr/adr-0XX-plan-compiler-runtime-handoff.md`
- `docs/adr/adr-0XX-lineage-observation-projection.md`
- `docs/spec/arco-storage-format-v0.md`
- `docs/spec/object-store-contract.md`
- `docs/spec/state-token-and-checkpoint-contract.md`
- `docs/spec/projection-watermark-contract.md`
- `docs/spec/api-token-exposure-matrix.md`

**Decisions to settle before implementation broadens:**

- control root scope: metastore, workspace, domain-sharded, or another explicit scope;
- `StateToken` exposure by surface: response body, header, metadata, or internal-only;
- default `StateToken` and `CheckpointToken` retention windows;
- revocation freshness budget for enforcement and credential vending;
- first real writable control-store domain;
- planning snapshot transactionality and retention;
- lineage raw observation retention and redaction tiers.

**API token exposure matrix rows:**

- Arco-native API;
- internal service call;
- Iceberg REST compatibility;
- UC-like compatibility;
- SQL/system-table surface;
- worker/runtime callback surface.

**API token exposure matrix columns:**

- `StateToken` body;
- `StateToken` header;
- internal-only token binding;
- `ProjectionWatermark` header/body;
- compatibility risk;
- tests required.

**Gate:**

- No doc implies Parquet/JSON domain manifests remain the final Tier-1
  mutation authority after migration.
- No doc implies the control-store prototype is already accepted production
  architecture.
- The source docs agree on vocabulary for `StateToken`, `CheckpointToken`,
  `ProjectionWatermark`, `WorkspaceSnapshot`, `ExportManifest`,
  `PlanArtifact`, `RunPlanBinding`, `PlanCreated`, `LineageObservation`, and
  `Projection`.

**Verification:**

```bash
cargo xtask adr-check
cargo xtask repo-hygiene-check
git diff --check
```

## Phase 1A: Introduce The State-Store Seam

**Goal:** Put the current authority path behind a narrow state-store interface
without changing product behavior.

**Source docs:**

- `2026-06-25-arco-tier1-control-store-strategy.md`
- `2026-06-26-arco-tier1-single-authority-combined-vision.md`

**Primary module seam:**

- `ArcoStateReader`
- `ArcoStateStore`
- `ArcoStateTxn`
- `ArcoStateAdmin`
- `StateToken`
- `CheckpointToken`
- `TxnOptions`
- `VersionedValue`

**Execution slices:**

1. Write the child plan for the trait boundary and current adapter.
2. Add compile-only traits and domain-neutral types behind a narrow module.
3. Add a current adapter that delegates to the existing ledger plus
   synchronous compactor path.
4. Add capability tests that document which token, checkpoint, range, and
   predicate behaviors are real in the current adapter and which remain
   unsupported.
5. Migrate one low-risk internal domain service call site to accept the seam
   without changing external API behavior.
6. Add import or architecture tests so compatibility API routes do not learn a
   backend-specific file layout.

**Current adapter rule:**

Unsupported `StateToken`, `CheckpointToken`, range, predicate, and `read_at`
behavior must be explicit in type-level capability flags or documented
`Unsupported` errors. Compatibility shims must not return fake tokens that
callers can mistake for retained control-store tokens.

**Gate:**

- External API behavior is unchanged.
- Current path still owns production authority.
- The seam does not overclaim semantics the current adapter cannot provide.
- Domain services can start depending on `ArcoStateStore` without depending on
  SlateDB or a custom segment layout.

**Verification:**

Run the targeted tests from the child plan plus:

```bash
cargo fmt --check
cargo test -p arco-catalog
cargo test -p arco-api control_plane
git diff --check
```

## Phase 1B: Harden Planner/Runtime Handoff

**Goal:** Move semantic lowering out of runtime controllers while preserving
today's wire behavior.

**Source doc:**

- `2026-06-27-planner-runtime-seam-hardening-design.md`

**Target seam:**

- `RunRequested` remains declarative intent.
- `PlanCompiler` owns asset selection, partition expansion, semantic task
  identity, fingerprinting, diagnostics, and explanations.
- `PlanCreated` is the runtime handoff.
- Runtime creates attempts only for plan-declared task keys.

**Execution slices:**

1. Introduce `planning::PlanCompiler` and compatibility compile result types.
2. Move asset-selection-to-`TaskDef` lowering out of runtime controllers.
3. Add `RunPlanner` or `PlanCompilationController` outside runtime ownership.
4. Add `PlanningSnapshotProvider`, even if it is backed by current in-process state.
5. Preserve `PlanCreated { tasks: Vec<TaskDef> }` until downstream consumers
   have a plan-reference path.
6. Add seam tests that fail if `orchestration/**` imports declarative planning
   internals or calls `PlanCompiler`.
7. Add deterministic fingerprint tests after the first mechanical move.

**Gate:**

- Runtime controllers no longer synthesize semantic tasks.
- Runtime controllers do not resolve `AssetSelection`, expand partitions, or
  interpret freshness policy.
- `CompileRequest` includes explicit logical time.
- `PlanningSnapshotProvider` produces a named snapshot token.
- Plan fingerprints exclude `run_id`, event ID, worker identity, queue state,
  runtime capacity, and wall-clock compilation time.
- The first compatibility `PlanCreated` carries or can map to a planning
  snapshot token and plan fingerprint.
- Existing orchestration behavior and wire compatibility are preserved.

**Additional tests:**

- fingerprint invariance across different `run_id` values;
- compatibility mapping from old `PlanCreated { tasks }` to stable task keys
  and fingerprint;
- import-boundary test preventing runtime-to-planning dependency.

**Verification:**

```bash
cargo fmt --check
cargo test -p arco-flow run_bridge
cargo test -p arco-flow orchestration
cargo test -p arco-api orchestration
git diff --check
```

Use narrower commands from the child plan when the exact test names are known.

## Phase 2: Contract, Conformance, Root Ownership, Provider/IAM Matrix

**Goal:** Make correctness testable before authority changes.

**Source docs:**

- `2026-06-20-olympia-inspired-arco-strategy.md`
- `2026-06-25-arco-tier1-control-store-strategy.md`
- `2026-06-26-arco-tier1-single-authority-combined-vision.md`

**Deliverables:**

- `docs/spec/arco-storage-format-v0.md`;
- `docs/spec/object-store-contract.md`;
- `docs/spec/root-ownership-and-iam-contract.md`;
- `docs/spec/provider-capability-matrix.md`;
- `docs/spec/domain-event-archive-retention.md`;
- authority reader contract;
- projection reader contract;
- root-token and snapshot reader contract;
- failure-mode test table;
- GC and retention reachability rules;
- projection watermark contract;
- provider conformance fixtures;
- `tests/iam/root_ownership_hygiene.rs` or equivalent;
- `tests/provider/cas_matrix.rs` or equivalent.

**Required conformance cases:**

- conditional create;
- conditional pointer replacement;
- stable version tokens;
- addressed read-after-write;
- checksums and corruption detection;
- timeout, retry, duplicate request, and partial failure behavior;
- stale writer epoch;
- orphan artifact recovery;
- no listing for request-time correctness;
- `StateToken` expiry;
- `CheckpointToken` expiry;
- stale projection watermark behavior.

**Root ownership and IAM gate:**

- API/control writer cannot write public Parquet projections.
- Projection compactor cannot CAS-publish mutation-visible control roots.
- Snapshot/export services cannot create mutation visibility.
- Production provider cannot be enabled without proven CAS and retry behavior.

**Provider support gate:**

- Local test providers may emulate CAS for tests only.
- Production provider support requires native or proven-safe conditional replace.
- If CAS or version-token semantics are ambiguous, the backend is read-only or disabled.
- CI labels provider tests as required before production enablement.

**Domain event archive retention gate:**

- Txlog retention and domain event archive retention are separate.
- Replay-equivalence tests use the domain event archive contract.
- Export manifests include event archive boundaries.
- GC cannot delete event archive objects required by audit/export policy.

**Gate:**

- A new contributor can understand durable storage, token, projection,
  snapshot, retention, root ownership, provider, and failure behavior from
  specs and tests.
- No production provider can be enabled without explicit CAS and retry
  capability evidence.

**Verification:**

```bash
cargo fmt --check
cargo test -p arco-core storage
cargo test -p arco-catalog protocol
cargo xtask repo-hygiene-check
git diff --check
```

## Phase 3A: Deterministic State Model

**Goal:** Prove the logical authority model without object-store mechanics
masking transaction-model defects.

**Source docs:**

- `2026-06-25-arco-tier1-control-store-strategy.md`
- `2026-06-26-arco-tier1-single-authority-combined-vision.md`

**Deliverables:**

- `arco-state-model` reference backend;
- serializable transaction model;
- point, range, and predicate preconditions;
- logical sequence behavior;
- deterministic event replay;
- folded KV state model;
- idempotent replay;
- failure model independent of object-store behavior.

**Gate:**

- Event replay equals folded KV for every accepted logical sequence.
- Range and predicate preconditions fail closed.
- Idempotent replay produces byte-equivalent state.
- Failed revalidation publishes no partial transaction.
- The model can explain every state transition without object-store artifacts.

**Verification:**

```bash
cargo fmt --check
cargo test -p arco-state-model
git diff --check
```

Adjust package names in the child plan once crates/modules exist.

## Phase 3B: Object-Store Control-Store MVP

**Goal:** Validate the object-store authority path while the old path still owns
production writes.

**Source docs:**

- `2026-06-25-arco-tier1-control-store-strategy.md`
- `2026-06-26-arco-tier1-single-authority-combined-vision.md`

**Deliverables:**

- `arco-state-control-mvp`;
- immutable transaction objects;
- control manifests;
- fenced manifest pointer CAS;
- `StateToken` reads;
- `CheckpointToken` reads;
- bounded manifest-reachable replay;
- projection outbox records;
- orphan and CAS-loss behavior;
- failure-state tests.

**Execution order:**

1. Implement transaction object writes with create-if-absent.
2. Implement immutable manifests and current-pointer CAS.
3. Implement `StateToken` and `CheckpointToken` reads.
4. Implement bounded manifest-reachable replay.
5. Implement projection outbox records reachable only from manifests.
6. Add failure-state tests for CAS loss, orphan transaction objects, corrupt
   artifacts, expired tokens, and writer epoch loss.
7. Defer custom segment formats unless bounded replay misses prototype budgets.

**Gate:**

- One committed transaction yields exactly one visible `StateToken`.
- CAS loss leaves old state visible.
- Orphan txlog or manifest artifacts never become visible without revalidation.
- `read_at(StateToken)` works.
- No listing is used for correctness.
- Replay from manifest-reachable events equals folded KV state.
- Projection outbox records are manifest-reachable only.

**Verification:**

```bash
cargo fmt --check
cargo test -p arco-state-control-mvp
cargo test -p arco-core storage
git diff --check
```

Adjust package names in the child plan once crates/modules exist.

## Phase 3C: Prototype Correctness, Performance, And Fallback Gate

**Goal:** Decide whether the prototype is allowed to advance toward service
reads and low-risk writes.

**Promote only if:**

- correctness and failure-state tests pass;
- provider CAS and retry behavior are proven;
- read-after-write by `StateToken` works;
- model replay equivalence holds;
- object-store MVP replay equivalence holds;
- projection equality can be measured through watermark;
- enforcement and vending can fail closed from authority or fresh-enough
  compiled state;
- operational complexity remains acceptable.

**Performance and operations measurements:**

- warm write p99 for narrow metadata mutation;
- warm point-read p99;
- bounded prefix-scan p99;
- cold writer startup to write-ready;
- manifest-reachable replay bytes;
- projection watermark lag;
- compaction backlog before replay budget breach;
- `StateToken` read-after-write retention.

**Reject promotion if:**

- replay grows without bounded recovery;
- projection lag is invisible to callers;
- cold writer recovery is operationally unacceptable;
- provider timeout or retry semantics are ambiguous;
- corruption or stale-writer tests do not fail closed.

**Fallback if not promoted:**

- keep current synchronous-compactor authority;
- continue with derived indexes and projection acceleration only;
- do not cut over catalog DDL, grants, credential vending, or broad governance.

**Verification:**

Use child-plan benchmark and failure-injection commands. At minimum:

```bash
cargo fmt --check
cargo test -p arco-state-model
cargo test -p arco-state-control-mvp
git diff --check
```

## Phase 4A: Shadow Replay And Projection Equivalence

**Goal:** Prove the new model can reproduce current catalog and governance truth.

**Source docs:**

- `2026-06-25-arco-tier1-control-store-strategy.md`
- `2026-06-26-arco-tier1-single-authority-combined-vision.md`
- `docs/guide/src/reference/control-plane-scope.md`

**Shadow comparisons:**

- object records;
- normalized name indexes;
- table current pointers;
- grants and ownership;
- storage-governance state;
- idempotency records;
- projection watermarks;
- event replay hashes;
- Parquet projection equality through each watermark.

**Execution slices:**

1. Write an importer from current published state into the shadow backend.
2. Add deterministic comparison reports for catalog objects and name indexes.
3. Add storage-governance and grant-state comparisons only where current repo
   behavior is implemented or partial with clear limitations.
4. Compare event replay hashes against folded KV state.
5. Compare Parquet projections through explicit watermarks.
6. Add a CI-safe fixture and a larger opt-in fixture.

**Gate:**

- Shadow state matches current published state for the selected scope.
- Projection equality holds through watermarks.
- No production authority has moved.
- Differences are classified as current-state gap, unsupported scope, stale
  projection, or bug.

**Verification:**

```bash
cargo fmt --check
cargo test -p arco-catalog shadow
cargo test -p arco-catalog projection
git diff --check
```

## Phase 4B: Internal Read-Only Comparison Reads

**Goal:** Exercise service read paths against the shadow backend without
accepting writes.

**Rules:**

- Current synchronous compaction remains the write authority.
- No enforcement, credential vending, or mutation path depends on the shadow backend.
- Comparison reads are internal/operator-only.
- Differences are classified as current-state gap, unsupported scope, stale
  projection, or bug.

**Execution slices:**

1. Select one internal read path with low compatibility risk.
2. Route it through a comparison adapter that reads current authority and shadow.
3. Emit structured comparison results without changing user-visible responses.
4. Add tests for equal, stale, unsupported, and divergent results.
5. Keep freshness and revocation-sensitive paths on current authority.

**Gate:**

- Selected internal read paths produce equivalent results or classified diffs.
- Freshness and revocation-sensitive paths remain on the current authority.
- No production authority has moved.
- No user-visible compatibility API behavior changes.

**Verification:**

```bash
cargo fmt --check
cargo test -p arco-catalog shadow
cargo test -p arco-api control_plane
git diff --check
```

## Phase 5: First Low-Risk Writable Domains

**Goal:** Get production evidence without touching grants, credential vending,
or broad catalog DDL.

**Allowed first writable domains:**

- projection job checkpoints;
- projection outbox acknowledgements;
- non-enforcement watermarks;
- synthetic internal failure-test domains.

**Execution slices:**

1. Pick one domain and document why it is non-enforcement-critical.
2. Add write APIs behind internal or operator-only access.
3. Return or internally bind `StateToken` for successful writes.
4. Expose projection watermark status separately from authority success.
5. Prove compactor outage does not block committed writes for the selected
   domain.
6. Prove old and new paths do not both accept writes for the same scope.

**Performance and operations gate:**

- warm write p99 and warm point-read p99 are within prototype budget;
- projection watermark lag is visible to callers/operators;
- manifest-reachable replay bytes remain bounded;
- compaction backlog alerts before replay budget breach;
- `StateToken` retention supports expected read-after-write windows.

**Gate:**

- Writes return or internally bind usable `StateToken`s.
- Projection watermarks expose freshness.
- Compactor outage does not block committed writes.
- No enforcement path depends on the new backend.
- Old path and control store do not both accept writes for the selected scope.

**Verification:**

Use domain-specific tests from the child plan plus:

```bash
cargo fmt --check
cargo test -p arco-catalog
cargo test -p arco-api
git diff --check
```

## Parallel Lineage Lane

**Goal:** Build lineage as append-only observations and deterministic
watermarked projections without making lineage a catalog mutation path.

**Source doc:**

- `2026-06-26-lineage-observation-projection-design.md`

### Lineage Lane L0: Contract And Schema

Can start after Phase 0.

**Deliverables:**

- observation envelope;
- replay, dedupe, and correction semantics;
- redaction and retention policy;
- worker/materialization producer contract;
- golden Arrow schema plan for `system.lineage.*`;
- OpenLineage compatibility note.

**Gate:**

- OpenLineage is documented as exchange envelope, not internal graph model.
- Raw observation retention and redaction tiers are explicit.
- Observation replay and correction rules are testable.

### Lineage Lane L1: Normalized Observations

Can start after L0.

**Deliverables:**

- append-only lineage observation event family;
- redacted normalized observation projection;
- duplicate/retry idempotency tests;
- late and malformed observation diagnostics.

**Gate:**

- Observations are replayable and idempotent.
- Malformed or unsupported observations produce diagnostics, not graph edges.

### Lineage Lane L2: Identity Diagnostics

Waits for identity snapshot rules.

**Deliverables:**

- identity resolution diagnostics;
- catalog snapshot token semantics;
- historical resolution tests;
- explicit ambiguity, stale alias, deleted object, and replaced object statuses.

**Gate:**

- Resolver records include as-of context.
- Ambiguous identity never silently becomes an exact graph edge.
- Historical observations do not resolve against current catalog names at read time.

### Lineage Lane L3: Versioned Table Edges

Waits for stable resolver and projection infrastructure.

**Deliverables:**

- versioned table-level edges;
- materialization identity;
- object generation, schema version, storage snapshot, and projection watermark fields.

**Gate:**

- Table rename and replace tests prove stable IDs beat mutable names.
- Replay is deterministic and idempotent.

### Lineage Lane L4: Column Edges With Soundness Guards

Waits for stable resolver and extractor soundness rules.

**Deliverables:**

- `system.lineage.column_edges`;
- dependency roles;
- propagation defaults;
- soundness tests proving unsupported column lineage is dropped as a unit.

**Gate:**

- No name-only fallback creates precise-looking column edges.
- Table-level lineage remains when table-level identity is sound.

### Lineage Lane L5: Governance Assertions And Propagation

Waits for stable graph and governance facts.

**Deliverables:**

- asserted governance fact events;
- inferred tag reachability projection;
- source tag assignment IDs;
- dependency roles used;
- explicit `inferred=true` semantics.

**Gate:**

- Inferred propagation is marked inferred and watermarked.
- Asserted facts remain separate from derived propagation.

### Lineage Lane L6: Compatibility And Search Surfaces

Waits for stable native projections.

**Deliverables:**

- Marquez/OpenLineage-style adapters;
- search lineage facets;
- system-table docs for freshness and redaction behavior.

**Gate:**

- Compatibility APIs consume native projections.
- Compatibility APIs cannot mutate catalog rows to write lineage.
- No lineage/search/system-table output becomes an enforcement input.

## Phase 6: Storage-Governance Metadata Without Vending Authority

**Goal:** Move useful governance metadata while keeping enforcement safe.

**Source docs:**

- `2026-06-25-arco-tier1-control-store-strategy.md`
- `2026-06-26-arco-tier1-single-authority-combined-vision.md`

**Candidate metadata:**

- storage credential metadata;
- external location metadata;
- path-governance metadata;
- workspace/metastore binding metadata where appropriate.

**Do not move yet:**

- credential vending authority;
- revocation-sensitive grant enforcement;
- broad catalog DDL.

**Required tests:**

- ancestor path conflict checks;
- descendant path conflict checks;
- range-empty and range-unchanged predicates;
- predicate input-set revalidation;
- stale compiled state denies closed;
- revocation freshness budget;
- projection lag does not affect enforcement.

**Gate:**

- Path governance conflicts are caught.
- Credential vending still denies closed on stale or missing state.
- Enforcement does not read `system.*`, lineage, search, or other lagging
  projections as authority.

## Phase 7A: Snapshot/Export Contract MVP

**Goal:** Define retained cuts and portable exports before building workflow
features on top.

**Source docs:**

- `2026-06-20-olympia-inspired-arco-strategy.md`
- `2026-06-26-arco-tier1-single-authority-combined-vision.md`

**Deliverables:**

- `WorkspaceSnapshot` record shape;
- `ExportManifest` record shape;
- retention pins;
- GC reachability rules;
- read-only `system.catalog.snapshots`;
- no transaction handles yet.

**Gate:**

- Snapshot records pin authority checkpoints or `StateToken`s plus projection watermarks.
- Export manifests include authority, event archive, checkpoint, projection,
  checksum, retention, compatibility, and relocation metadata.
- GC reachability accounts for snapshots, exports, root tokens, review tokens,
  and retained historical artifacts.

## Phase 7B: Workspace Snapshot/Export Implementation

**Goal:** Implement snapshot and export creation without restore or transaction handles.

**Deliverables:**

- `CreateWorkspaceSnapshot`;
- `GetWorkspaceSnapshot`;
- `ExportWorkspaceSnapshot`;
- retained compatibility with old-path artifacts;
- restore preflight only.

**Gate:**

- Exports are restorable and auditable without private context.
- Historical old-path artifacts are readable only through documented
  compatibility rules.
- No snapshot/export service creates mutation visibility.

## Phase 7C: Roll-Forward Restore

**Goal:** Add restore as new visible authority or root/read token publication,
not mutation of old snapshots.

**Deliverables:**

- `RestoreDomainToSnapshot`;
- `RestoreWorkspaceToSnapshot`;
- omitted-domain policy;
- restore preflight diagnostics;
- roll-forward publication.

**Rollback rule:**

After `ControlStoreAuthority` cutover, rollback is roll-forward restore or
compatibility read from retained artifacts. It is not re-enabling old-path
authoritative writes unless an explicit emergency rollback procedure first
transitions the whole scope out of `ControlStoreAuthority`.

**Gate:**

- Restore publishes new visible authority or a root/read token.
- Old snapshots are immutable.
- Omitted-domain behavior is explicit.

## Phase 7D: Durable Transaction Handles

**Goal:** Add resumable transaction workflows after reader, retention, and
snapshot contracts are clear.

**Deliverables:**

- transaction records;
- mutation staging;
- prepare, commit, abort, expire, and recover state machine;
- `system.catalog.transactions`;
- review-token workflow.

**Gate:**

- Transaction handles can expire, abort, recover, or become visible under
  documented rules.
- Transaction system tables are read-only projections.
- Cross-domain workflows are described as durable workflows over single-domain
  commits/checkpoints, not unqualified distributed database transactions.

## Phase 8: Idempotency, Grants, And Narrow Catalog DDL Pilots

**Goal:** Start migrating real Tier-1 semantics only after the substrate is
proven.

**Allowed order:**

1. Idempotency only when the protected mutation also commits in the control store.
2. Grants only after freshness, revocation, and compiled-cache tests pass.
3. Narrow catalog DDL only after name, ID, ownership, table pointer, outbox,
   rollback, compatibility, and projection tests pass.

**Pilot requirements:**

- successful writes return or internally bind `StateToken`;
- projection watermark is exposed on responses where relevant;
- event archive writes are retained;
- folded KV state updates with events;
- Parquet projection equality holds through watermark;
- old-path writes are disabled for the migrated pilot scope.

**Gate:**

- Event replay equals folded KV at each accepted token.
- Parquet projections equal authority through each watermark.
- Revocation and stale-cache behavior deny closed.
- Rollback and restore work from retained snapshots.
- No scope accepts old-path and control-store writes at the same time.

## Phase 9: Per-Domain Cutover

**Goal:** Move one authority scope at a time from old authority to
control-store authority.

For each scope, use this state machine:

```text
OldAuthority
  -> ShadowControlStore
  -> ControlStoreAuthority
  -> RetiredOldAuthority
```

**Cutover checklist:**

1. Identify exact scope and domain.
2. Confirm shadow equivalence.
3. Confirm internal comparison reads.
4. Confirm provider CAS and retry evidence.
5. Confirm snapshot/export compatibility.
6. Disable old-path authoritative writes for the scope.
7. Route writes to control-store authority.
8. Return or internally bind `StateToken` for successful mutations.
9. Publish Parquet projections asynchronously.
10. Expose projection watermarks.
11. Keep old artifacts only for retained snapshot/export/audit compatibility.

**Gate per domain:**

- Exactly one production write authority.
- Compactor outage does not block committed mutations.
- System tables are read-only, watermarked projections.
- Authorization and credential vending do not depend on lagging system tables.
- Provider CAS and retry semantics are proven for the production backend.
- Snapshot/export compatibility is retained.

## Phase 10: Ergonomics, Rich Projections, And Derived Indexes

**Goal:** Add broad user-facing features only after authority and product
contracts are stable.

**Allowed work:**

- `arco tx` CLI;
- optional SQL-like command endpoint outside the DataFusion read-query surface;
- read/write-set receipts;
- action-summary receipts;
- optimistic retry after revalidation and re-authorization;
- derived point-lookup indexes;
- `system.lineage.column_edges`;
- lineage dependency roles;
- governance asserted tags and inferred tag reachability;
- Marquez/OpenLineage-style compatibility adapters;
- search lineage facets.

**Gate:**

- No system table becomes a write surface.
- No lineage/search projection becomes an enforcement input.
- No derived index becomes authority.
- CLI and SQL-like commands route through transaction handles or command APIs.

## Phase 11: Retire Old Tier-1 Authority Path

**Goal:** Finish the migration instead of operating two write systems forever.

**Deliverables:**

- `arco-state-current` removed from production write routing;
- old ledger authority retired per migrated domain;
- retained historical artifacts accessible only through snapshot/export/audit
  compatibility;
- control checkpoints used by snapshot/export;
- event archive retained by policy;
- provider matrix documented for production backends;
- runbooks for writer lease loss, token expiry, checkpoint expiry, projection
  lag, corrupt artifacts, and CAS failures.

**Gate:**

- No targeted Tier-1 domain still accepts authoritative writes through the old
  path.
- Retained historical reads and exports remain documented.
- Operational runbooks exist for every expected failure state.

## Parallel Workstreams

These may proceed in parallel after Phase 0 if each child plan has its own
commit boundary:

- state-store trait plus current adapter;
- planner/runtime seam PRs 1-3;
- lineage L0 and L1;
- object-store provider conformance tests;
- storage, token, root ownership, IAM, event archive, and projection specs.

These must wait:

- control-store MVP waits for provider contract and `StateToken` semantics;
- shadow replay waits for current adapter plus model/MVP backend;
- internal comparison reads wait for shadow equivalence;
- low-risk writable domains wait for shadow replay, comparison reads, and
  failure tests;
- storage-governance metadata waits for range and predicate preconditions;
- credential vending waits for revocation freshness and deny-closed tests;
- grants wait for freshness and revocation tests;
- catalog DDL waits for name, ID, ownership, table pointer, outbox, rollback,
  compatibility, and projection tests;
- snapshot/export implementation waits for checkpoints and tokens to exist;
- durable transaction handles wait for reader, retention, and snapshot contracts;
- CLI, SQL-like commands, optimistic retries, and indexes wait for transaction handles;
- lineage L2 waits for identity snapshot rules;
- lineage L3 and L4 wait for stable resolver and projection infrastructure;
- lineage L5 waits for stable graph and governance facts;
- old authority retirement waits for per-domain cutover evidence.

## Explicit Non-Starts

Do not start with:

- grants;
- broad catalog DDL;
- credential vending;
- CLI syntax;
- optimistic retry;
- custom control-store segments;
- derived point indexes;
- task heartbeats or high-frequency orchestration telemetry in the control store;
- raw lineage observations as the first strongly consistent Tier-1 tranche;
- runtime controllers calling `PlanCompiler`;
- OpenLineage as the internal catalog model;
- orchestrators mutating catalog rows to publish lineage;
- system tables as write or enforcement surfaces.

## Slice Planning Template

Use this template for every child implementation plan:

```markdown
# <Slice Name> Implementation Plan

**Implementation protocol:** Execute this plan task-by-task. Do not broaden
scope without updating this child plan and passing the exit gate.

**Goal:** <one sentence>

**Architecture:** <two or three sentences explaining the seam and authority boundary>

**Tech Stack:** <crates, modules, docs, tests, commands>

---

## Source Docs

- `<source doc>`

## Current-State Audit

- `git status --short`
- current implemented behavior from `docs/guide/src/reference/control-plane-scope.md`
- relevant existing tests and modules

## Scope

In:
- <narrow behavior>

Out:
- <explicit non-goals>

## Tasks

### Task 1: <name>

**Files:**
- Modify: `<path>`
- Test: `<path>`

**Step 1:** Write the failing test.

**Step 2:** Run the test and confirm the expected failure.

**Step 3:** Implement the minimum code.

**Step 4:** Run the focused tests.

**Step 5:** Run format/check gates.

**Step 6:** Commit only this slice.

## Verification

```bash
cargo fmt --check
<focused test command>
git diff --check
```

## Exit Gate

- <measurable gate>
```

## Program Scorecard

Use this scorecard after every phase:

| Area | Question |
|---|---|
| Authority | Does each migrated scope have exactly one writer authority? |
| Tokens | For every migrated control-store scope, does every successful Tier-1 write return or internally bind a usable `StateToken`? |
| Projection | For every migrated scope, are system tables and lineage/search/audit views derived, watermarked, and no stronger than their watermark? |
| Enforcement | Do authorization and credential vending avoid lagging projections? |
| Planning | Does `PlanCompiler` own semantic lowering and does runtime create only attempts? |
| Lineage | Are observations append-only with explicit resolution and soundness status? |
| Snapshots/export | Do snapshots pin authority checkpoints or tokens plus projection watermarks? |
| Migration | Is the old path transitional only for migrated scopes? |
| Telemetry | Are high-frequency runtime events outside the Tier-1 control store? |

If any answer is no, stop broadening the program and write the next child plan
for that gap.

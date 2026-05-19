# Catalog Product Surface Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build Arco into a first-class file-native lakehouse catalog and
metastore for open table formats, with Delta Lake as the primary managed
format. The product surface spans tables, volumes, credentials, grants,
lineage, discovery, governance metadata, and system tables, using Unity Catalog
as prior art rather than as an architectural dependency.

**Architecture:** Extend Arco's native control-plane pattern across the full
catalog product surface: immutable mutations, compaction into typed Parquet
projections, fenced pointer publication, and read-only system/discovery views.
API surfaces, including UC-compatible routes, Arco-native routes,
Iceberg-facing routes, and future SQL surfaces, must be adapters over the same
Arco-owned state. Delta Lake is the first-class managed table format and the
default for new registrations. Iceberg and plain Parquet remain explicit
table-format surfaces with support levels tied to implemented Arco-native
state. Governance decisions use compiled authoritative views; system tables
and search indexes remain derived observability/discovery layers.

**Tech Stack:** Rust, Axum, Protobuf/prost, Arrow/Parquet, DataFusion, object-store CAS, `arco-catalog`, `arco-api`, `arco-uc`, `arco-iceberg`, mdBook, `cargo test`.

---

## Status (2026-05-11)

Active on this branch. This is downstream catalog product work, not a
continuation of the completed proto enhancement plans. The pre-freeze hard cut
is closed, `arco.*.v1` is the durable public proto surface, and
`cargo xtask proto-breaking-check` remains the gate for additive
`metastore.proto` expansion.

Completed or partially landed slices:

- Task 0 contract docs landed in `b39cb13`, with follow-up contract alignment in
  `d2d2d19`, `90e45e3`, and `6869d6e`.
- Task 1 additive native metastore object contracts landed in `d14eba4`.
- Task 2 initial replay/projection kernel landed in `2ff14e1`.

The landed kernel is intentionally narrow: it proves stable-ID replay,
projection allowlisting, schema watermarking, redaction, and all-or-nothing
publication for the initial generic metastore projection. It does not yet make
grants, credentials, object-family CRUD, credential vending, or system-table
visibility production-backed. Tasks 3-11 remain active product work.

## Product Framing

Arco is a catalog and metastore for open lakehouse table formats. It owns table
identity, namespaces, locations, governance state, lineage, and discovery as
file-native control-plane state rather than delegating those responsibilities
to an always-on metastore service.

Delta Lake is the first-class managed table format and the default for new
Arco table registrations. Iceberg-facing APIs and plain Parquet table metadata
are compatibility/product surfaces over the same Arco-owned catalog state, not
separate product architectures.

Arco should use UC as a benchmark for what a mature catalog product must own:

- stable securable object identity
- hierarchical namespaces
- table and non-table data assets
- views as first-class securables
- managed and external storage governance
- grants and inherited privileges
- temporary credential vending
- lineage and discovery
- operational system tables
- auditability and explainability

Longer-term object families such as service credentials, external service
connections, shares, providers, recipients, data products, and business domains
belong on the roadmap, but they should not enter the first delivery tranche
until tables, volumes, grants, storage governance, and credential vending are
production-grade.

The product goal is not "UC parity." It is:

> Arco is the authoritative file-native catalog and metastore for open lakehouse table formats. Delta Lake is the first-class managed format. Compatibility APIs expose Arco state to outside engines, but they do not define Arco's architecture.

## Repo-Grounded Gap Summary

- Catalog/schema/table CRUD is already implemented through authoritative catalog state.
- Table-format contracts support Delta Lake, Iceberg, and plain Parquet. New table registrations default to Delta, while legacy rows without persisted format metadata still read as Parquet.
- Lineage and search have real published projections.
- System catalog tables already expose an explicit allowlist for catalog, lineage, and orchestration projections through `/api/v1/query`.
- `proto/arco/catalog/v1/metastore.proto` defines broader metastore message shapes, but most access, storage, volume, function, model, and governance domains are not yet authoritative runtime projections.
- `crates/arco-uc/src/routes/permissions.rs` is scaffolded and not backed by compiled grants.
- `crates/arco-uc/src/routes/credentials.rs` has placeholder temporary credential behavior.
- UC inventory documents volumes, functions, models, credentials, external locations, and grants as useful prior art, but Arco should implement them natively.
- The existing `docs/plans/2026-04-23-authoritative-metastore-governance-surface.md` is the runtime companion for portions of this product surface; it must continue to treat UC as compatibility prior art, not the product north star.

## Current Status Snapshot

The system-table foundation is real for the first tranche:

- `system.catalog.{catalogs,namespaces,tables,columns,commits}`
- `system.lineage.edges`
- `system.orchestration.{runs,tasks,dep_satisfaction,timers,dispatch_outbox,sensor_state,sensor_evals,partition_status,schedule_definitions,schedule_state,schedule_ticks,backfills,backfill_chunks,run_key_conflicts}`

Upstream proto contract work has landed. This branch also has additive native
metastore object contracts and an initial metastore replay/projection kernel.
The projection kernel is partial: it supports stable-ID replay and the
allowlisted `metastore_objects.parquet` projection, while broader access,
storage, volume, governance, function, and model projections are not
authoritative yet.

Tasks 3-11 remain future runtime and product work. In particular,
`system.access.*`, `system.storage.*`,
`system.catalog.{volumes,functions,registered_models,model_versions}`, and
`system.governance.attachments` tables must not be registered until their
authoritative native state and safe Parquet projections exist.

## Phase 0 Design And Security Contract

Before adding object contracts or route implementations, create the design
artifacts that make the product surface reviewable by engineers, API consumers,
and security reviewers:

| Artifact | Purpose |
|---|---|
| `docs/adr/adr-037-arco-catalog-product-surface.md` | Native Arco catalog product boundary, object families, compatibility-adapter policy, and system-table policy. |
| `docs/adr/adr-038-catalog-threat-model.md` | Threat model for grants, credentials, object IDs, path authorization, system tables, lineage, and audit. |
| `docs/adr/adr-039-catalog-consistency-model.md` | Ledger ordering, CAS/pointer publication, projection freshness, replay guarantees, conflict behavior, and read-after-write expectations. |
| `docs/guide/src/reference/catalog-privilege-matrix.md` | Canonical privilege matrix generated from or tested against code. |
| `docs/guide/src/reference/catalog-api-contract.md` | Resource names, stable IDs, pagination, ETags/generations, idempotency, field masks, filtering, sorting, errors, and versioning. |
| `docs/guide/src/reference/schema-evolution-policy.md` | Compatibility policy for protobuf, Parquet, Arrow, OpenAPI, and system-table schemas. |
| `docs/guide/src/reference/credential-vending-security.md` | Token scope, TTL, access-delegation negotiation, provider behavior, audit, revocation limits, deny reasons, and redaction. |

Phase 0 is design work, not product implementation. It may add generated docs
or contract tests, but it must not introduce route behavior before the
security, consistency, and API contracts are explicit.

## Non-Negotiable Invariants

These invariants are product requirements, security requirements, and test
requirements. They must be named in tests so regressions are easy to diagnose.

| Area | Invariant |
|---|---|
| Identity | Every request has an authenticated principal, optional workload/service identity, group expansion version, workspace scope, metastore scope, and request ID. |
| Object identity | Stable object IDs are enforcement keys; names are mutable aliases only. |
| Authorization | Every read, write, list, query, and credential-mint route authorizes before returning object data, metadata, or credentials. |
| Redaction | Secret material, raw tokens, encrypted credential payloads, private keys, and internal policy payloads never appear in list/get/system-table/discovery responses, logs, traces, or test snapshots. |
| Path ownership | Every governed storage path maps to exactly one table, volume, managed root, or external location authority unless an explicit safe-overlap exception is modeled and tested. |
| Credential vending | Minted credentials are no broader than the authorized object/path, no longer-lived than policy allows, and always audited for allow and deny decisions. |
| Publication | Readers see only a fully published snapshot or compiled view; no API reads half-published projections. |
| Recovery | Crashes during mutation, compaction, projection writing, or pointer publication are recoverable without losing committed mutations. |
| Compatibility | Compatibility adapters declare a support level and preserve native Arco invariants even when external API shapes differ. |
| System tables | System tables are read-only, allowlisted, redacted, freshness-watermarked, and never enforcement inputs. |

## Scope

- Tables: authoritative object metadata, table type, format, protocol/properties, ownership, comments, columns, constraints, location, and lifecycle state.
- Views: model as securable objects and include in the long-term object model even if execution/query expansion is deferred.
- Volumes: governed non-tabular storage objects with stable IDs, path bindings, ownership, grants, temporary credentials, and system-table visibility.
- Credentials: storage credentials, service credentials, external locations, managed roots, workspace bindings, credential vending, expiration, deny reasons, and audit.
- Grants: principals, groups, ownership, inherited privileges, securable hierarchy, mutation APIs, compiled read views, and enforcement hooks.
- Lineage: table/column/job/run lineage, source/sink edges, freshness, impact analysis, and system views.
- Governance metadata: tags, glossary terms, controlled vocabularies, classifications, policy attachments, masking/row-filter placeholders, data domains, and owners/stewards.
- Discovery: search, browse, filters, object summaries, lineage-neighborhood queries, and explain-access diagnostics.
- System tables: catalog, access, storage, Delta, lineage, query, orchestration, and reconciliation views.
- Compatibility: UC-compatible endpoints and Iceberg-facing surfaces as adapters over Arco-native state.

## Non-Goals

- Do not require or embed a Unity Catalog server.
- Do not build an always-on OLTP metastore for authoritative state.
- Do not put system tables, search indexes, or raw OpenAPI parity scaffolding in the enforcement path.
- Do not treat `properties` maps as the authoritative model for governance domains that need typed state.
- Do not attempt every UC endpoint in one tranche; prioritize core catalog product capabilities and enforcement-grade state.
- Do not expose raw ledger files, raw manifest JSON, raw search postings, or sensitive grant/credential internals by default.
- Do not imply policy enforcement for governance attachments until a policy engine boundary exists.

## Catalog Product Principles

- Stable IDs are the primary keys; names are mutable lookup attributes.
- Every securable object has an owner and a clear parent in the hierarchy.
- Every storage path is owned by either a managed root, external location, volume, or table.
- Every temporary credential is derived from an authorized object/path decision and is auditable.
- Grant inheritance is compiled into read-optimized views, but grant mutations remain authoritative events.
- Discovery surfaces are derived and lag-tolerant.
- Enforcement surfaces use published authoritative state or compiled views, never placeholder route responses.
- System tables explain the control plane without becoming the control plane.
- Deny-by-default applies to unknown principals, unknown object types, stale compiled permissions, ambiguous paths, and credential-provider failures.

## Formal Authorization Contract

Authorization is a domain subsystem, not route-local conditionals. Every route
that reads, mutates, lists, vends credentials, or queries sensitive system
tables must call an `AuthzDecision` API with this shape:

```text
input:
  principal_id
  group_ids
  operation
  object_id | path
  object_type
  workspace_id
  request_context
  requested_ttl
  client_capabilities

output:
  decision: allow | deny
  reason_code
  evidence:
    direct_grants
    inherited_grants
    ownership
    storage_binding
    workspace_binding
    policy_attachment_status
  max_credential_scope
  max_ttl
  audit_classification
```

Object-level, property-level, and function-level authorization tests are
required for every endpoint that accepts an object ID, name, path, principal, or
operation.

## Identity And Group Semantics

Identity resolution is part of the catalog product, not middleware trivia. The
first implementation tranche must define and test:

- human users, service principals, workload identities, groups, and external or
  federated subjects
- transitive group membership expansion
- group membership snapshot/revision recorded with every authorization decision
- disabled/deleted principal behavior
- ownership transfer semantics
- break-glass/admin role scope and audit requirements
- whether ownership implies all privileges, grant option, or both
- whether Arco supports explicit deny grants or only absence-of-allow denies
- stale identity or stale group-expansion behavior

## API Contract Requirements

All native APIs and compatibility adapters must define these behaviors before
implementation:

- List APIs are paginated from first release with `page_size`, `page_token`,
  deterministic ordering, and maximum page size.
- Filters and sorts use explicit allowlists; no raw SQL-like filter strings.
- Mutating APIs accept idempotency keys where retries can duplicate work.
- Update/delete APIs use ETags or generation preconditions where stale writes matter.
- Update APIs use field masks rather than ambiguous partial updates.
- Errors use stable machine-readable codes such as `not_found`,
  `permission_denied`, `conflict`, `invalid_path`, `stale_projection`,
  `credential_scope_denied`, and `redacted`.
- Response schemas distinguish admin-visible, owner-visible, and
  ordinary-principal-visible fields.
- Native API versioning, UC-compatible semantics, Iceberg-compatible semantics,
  protobuf schemas, Parquet projections, and system tables each have explicit
  compatibility rules.
- Native error responses use a machine-readable problem-details shape unless a
  compatibility adapter must preserve an external error schema.

## Compatibility Levels

Every route group and generated OpenAPI entry must carry one of these labels:

| Level | Meaning |
|---|---|
| `native` | Arco-native API and semantics; this is the product contract. |
| `compatible-exact` | Endpoint behaves like the referenced external API for supported fields, errors, and lifecycle semantics. |
| `compatible-partial` | Endpoint shape exists, but fields, lifecycle states, or edge semantics are intentionally limited and documented. |
| `scaffolded` | Route exists for discovery or future compatibility; it must not be used for enforcement or production workflows. |
| `planned` | Documented product intent with no route behavior. |

OpenAPI is a contract artifact, not only generated documentation. Add snapshot
tests, OpenAPI diff checks, and route-level compatibility annotations before
marking an adapter production-backed.

## Consistency And Replay Invariants

- Replaying the ledger from genesis yields the same typed state as the latest published projection.
- Projection publication is all-or-nothing: readers see old complete state or new complete state, never a partial projection set.
- A compacted projection is byte-for-byte or semantically equivalent to replay over the compacted range.
- Rename changes names only; object ID, grants, lineage bindings, governance attachments, and audit identity remain stable.
- Delete creates a tombstone or lifecycle transition before physical cleanup.
- Credential scope is always a subset of the governed table, volume, or path decision.
- Secret material is never written to tenant-visible projections, system tables, logs, traces, error messages, or test snapshots.
- Search and discovery may lag, but every response exposes a projection watermark or freshness indicator.
- A deny decision is auditable and explainable without leaking sensitive policy internals.
- System tables are derived observability surfaces and cannot be used as enforcement input.

## Path Governance Semantics

Storage governance must have a dedicated path module and property tests. Cover:

- URI canonicalization for `s3://`, `gs://`, `abfss://`, and local/dev paths
- provider-specific case sensitivity and bucket/container/account equivalence
- trailing slash, duplicate slash, percent-encoding, and dot-segment handling
- table data path versus metadata/log path boundaries
- external location versus managed root separation
- sibling overlap and parent/child overlap
- workspace binding enforcement
- deleted/tombstoned path behavior
- migration and backfill of legacy paths

Provider CAS/precondition semantics must be abstracted explicitly so S3, GCS,
Azure, and local/dev implementations cannot silently weaken publication or
commit safety.

## Credential Vending Decision Contract

Credential vending must be implemented as an authorization engine with provider
plugins, not as route-local response construction.

Decision input:

```text
principal_id
groups_snapshot_version
workspace_id
request_id
operation: read | write | list | delete | model_artifact_read | model_artifact_write
object_ref: table | volume | external_location | model_version | raw_path
requested_path
requested_ttl
client_kind: iceberg | uc | native | sql | internal
catalog_snapshot_version
```

Decision output:

```text
decision: allow | deny
reason_code
reason_message_safe
authorized_object_id
authorized_path_prefixes
provider
credential_kind
expires_at
max_ttl_applied
audit_event_id
```

Hard requirements:

- deny by default
- clamp TTL
- path scope cannot exceed the governed object path
- no token appears in audit rows, system tables, logs, traces, or errors
- every allow and deny emits an audit event
- stale projections deny unless the route explicitly accepts bounded staleness
- model-version credentials are implemented only after model artifact ownership exists

## Query And System-Table Sandbox

The `/api/v1/query` path must remain a constrained read-only system-table and
projection query surface:

- read-only SQL only
- no arbitrary file registration
- no raw object-store paths
- tenant/workspace filters applied before result return
- timeout, memory limit, row limit, and result-size limit
- query audit with request ID and principal
- internal projections are inaccessible unless explicitly allowlisted
- sensitive operational metadata is redacted by schema, not only by route code
- errors must not disclose raw paths, tokens, policy internals, or hidden objects

## Observability And Supply-Chain Gates

GA hardening must include:

- traces and metrics for request IDs, mutation IDs, snapshot versions,
  projection lag, CAS retries, credential decisions, deny counts, audit lag,
  compaction duration, permission compiler duration, system-table query latency,
  and search freshness
- SBOM generation
- dependency allow/deny policy
- secret scanning
- pinned CI actions
- signed/provenance-bearing releases
- OpenSSF Scorecard target
- backup/restore and ledger-replay drills

## Domain Module Layout

Avoid growing `metastore_state.rs`, `reader.rs`, and `writer.rs` into god files.
Implement the product surface through domain modules and keep route crates thin:

```text
crates/arco-catalog/src/metastore/
  mod.rs
  ids.rs
  events.rs
  envelope.rs
  replay.rs
  projections.rs
  publish.rs
crates/arco-catalog/src/identity/
  principals.rs
  groups.rs
  memberships.rs
crates/arco-catalog/src/authz/
  privileges.rs
  grants.rs
  compiler.rs
  decision.rs
  explain.rs
crates/arco-catalog/src/storage_governance/
  credentials.rs
  external_locations.rs
  managed_roots.rs
  path_normalization.rs
  bindings.rs
crates/arco-catalog/src/objects/
  tables.rs
  views.rs
  volumes.rs
  functions.rs
  models.rs
crates/arco-catalog/src/governance/
  tags.rs
  glossary.rs
  classifications.rs
  policies.rs
  attachments.rs
crates/arco-catalog/src/audit/
  events.rs
  projection.rs
```

## Test Hygiene Policy

Do not commit knowingly failing default tests. Use one of these patterns:

- Same-PR TDD: write the failing test, implement the code, and commit only when green.
- Ignored future contract: mark the test `#[ignore = "requires <feature>"]` with a tracking issue.
- Feature-gated future contract: run under a non-default feature such as `pending-catalog-contracts`.
- Evidence-only spec: document the assertion in an ADR until implementation lands.

## Recommended Delivery Order

1. Task 0: product invariants, threat model, security model, consistency model, and compatibility policy.
2. Task 1: protobuf and public contract evolution rules.
3. Task 2: authoritative metastore kernel, replay, projection registry, and publication invariants.
4. Task 3: identity, principals, groups, ownership, grants, inherited permissions, and explain-access.
5. Task 4: storage credentials, external locations, managed roots, workspace bindings, and path ownership.
6. Task 5: credential vending engine for tables and raw governed paths.
7. Task 6: volumes as governed path objects and volume credential vending.
8. Task 7: governance metadata and policy attachments.
9. Task 8: lineage and discovery.
10. Task 9: functions and model registry metadata, then model-version artifact credential vending.
11. Task 10: system tables and query sandbox after owning projections exist.
12. Task 11: compatibility adapters, conformance, production verification, observability, supply-chain hardening, and evidence.

## Verification Matrix

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-targets --all-features`
- `cargo test --workspace --doc`
- `cargo xtask adr-check`
- `cargo xtask verify-integrity`
- `cargo xtask repo-hygiene-check`
- `cargo xtask engine-boundary-check`
- `cargo xtask parity-matrix-check`
- `buf lint proto/`
- `cargo xtask proto-breaking-check`
- `cargo deny check`
- `cargo audit`
- `cargo semver-checks`
- `cd docs/guide && mdbook build`

Future GA gates must be implemented in `tools/xtask` before they are added to
the required verification matrix or referenced as runnable commands:

- schema compatibility check
- OpenAPI diff check
- system-table schema compatibility check
- privilege-matrix drift check
- redaction check
- projection replay check
- compatibility fixture check
- benchmark smoke check

## Acceptance Criteria

- The control-plane scope page reports each catalog product domain truthfully as `Implemented`, `Partial`, or `Planned`.
- Grants, credentials, external locations, volumes, and governance attachments are persisted as Arco-native authoritative state.
- Permission checks and credential vending use compiled authoritative views, not scaffolded responses.
- System tables expose safe operational views for catalog, access, storage, lineage, Delta, and query history.
- UC-compatible endpoints are explicitly documented as compatibility adapters over Arco state.
- New domains have schema contracts, mutation tests, read tests, scoping tests, and docs.
- Every endpoint that accepts object IDs, names, paths, principals, or operations has object-level authorization tests.
- Every response schema has property-level authorization/redaction tests.
- All list APIs are paginated from first release.
- Mutating APIs use idempotency keys and stable request IDs where retry semantics matter.
- Update/delete APIs use ETags or generation preconditions where concurrent modification is possible.
- System-table schemas are treated as public APIs and protected by schema compatibility tests.
- Audit logs are security controls separate from user-facing system tables.
- Rate limits and quotas exist for credential vending, search, system-table queries, and grant mutations.
- No committed default test is expected to fail.
- Every protobuf change passes compatibility checks or is explicitly versioned as breaking.
- Every projection includes a schema version and ledger watermark.
- Path overlap/canonicalization tests cover S3, GCS, Azure, and local/dev paths, even if only GCS vending is implemented first.
- OpenAPI and compatibility route behavior are snapshot-tested.
- Permission compilation is property-tested and benchmarked.
- Backup/restore and ledger replay are verified.
- Secret material is excluded by type/schema, not just route filtering.
- Production docs cover SLOs, metrics, audit retention, recovery, and migration.

## Definition Of Done For A Catalog Domain

A domain must not be marked `Implemented` unless all of the following are true:

1. The authoritative event/mutation exists.
2. The event participates in deterministic hashing and replay.
3. The typed state fold exists.
4. The projection exists and has a golden schema.
5. The projection excludes sensitive fields.
6. The reader/writer API exists.
7. Authorization checks exist.
8. Audit events exist for mutations and access decisions.
9. System-table visibility is documented and redacted.
10. Native API docs exist.
11. Compatibility adapter behavior is documented if applicable.
12. Unit, integration, property, security, and schema tests pass.
13. Failure behavior is tested.
14. Metrics, traces, and logs exist.
15. The scorecard cites exact code paths and test names.
16. Backup/restore and ledger replay have evidence.
17. Compatibility level is documented for every adapter route.

### Task 0: Complete Product, Security, And API Contracts

**Files:**
- Create: `docs/adr/adr-037-arco-catalog-product-surface.md`
- Create: `docs/adr/adr-038-catalog-threat-model.md`
- Create: `docs/adr/adr-039-catalog-consistency-model.md`
- Create: `docs/guide/src/reference/catalog-privilege-matrix.md`
- Create: `docs/guide/src/reference/catalog-api-contract.md`
- Create: `docs/guide/src/reference/schema-evolution-policy.md`
- Create: `docs/guide/src/reference/credential-vending-security.md`
- Modify: `docs/guide/src/reference/control-plane-scope.md`
- Modify: `docs/guide/src/reference/unity-catalog-openapi-inventory.md`
- Modify: `docs/guide/src/concepts/catalog.md`
- Modify: `docs/guide/src/concepts/architecture.md`
- Modify: `docs/plans/2026-04-23-authoritative-metastore-governance-surface.md`

**Step 1: Write the product-surface ADR**

State that Arco's catalog product is native and ledger-backed. UC is prior art for scope and compatibility, not the architectural authority.

The ADR should define:

- object families
- stable-ID rules
- mutation/publication boundaries
- enforcement versus observability boundaries
- compatibility adapter policy
- system-table policy

**Step 2: Write the threat model**

Use a STRIDE-style structure and cover at least:

- object-ID tampering
- name/path confusion
- privilege escalation through inherited grants
- stale group membership
- credential scope expansion
- sensitive system-table disclosure
- lineage spoofing or duplicate lineage events
- projection replay corruption
- leaked secrets in logs, traces, system tables, fixtures, or projections

**Step 3: Write the consistency model**

Define:

- ledger event ordering
- deterministic replay
- pointer publication semantics
- read-after-write expectations
- conflict behavior
- compaction equivalence
- projection watermarks/freshness
- stale, missing, or corrupt projection errors

**Step 4: Write API and schema reference docs**

Document:

- resource names and stable IDs
- pagination
- filtering and sorting allowlists
- ETags/generations
- idempotency keys
- field masks
- error codes
- request IDs
- response redaction classes
- native API, compatibility API, protobuf, Parquet, Arrow, OpenAPI, and system-table versioning rules

**Step 5: Write privilege and credential-vending references**

The privilege matrix must become the canonical product contract for operations
on catalogs, schemas, tables, views, volumes, credentials, external locations,
functions, models, system tables, and future sharing objects.

The credential-vending security reference must define token scope, TTL policy,
access-delegation negotiation, remote-signing extensibility, revocation limits,
provider failure behavior, audit rows, deny reasons, and redaction rules.

**Step 6: Update the control-plane scorecard**

Break broader catalog scope into separate rows:

- grants/RBAC
- storage credentials
- service credentials
- external locations
- managed storage roots
- views
- volumes
- functions
- models/model versions
- shares/providers/recipients
- ownership/tags/classification
- glossary terms/domains
- policy attachments
- temporary credential vending
- access audit
- storage/system tables

Do not mark a row `Implemented` unless code, tests, and docs exist.

**Step 7: Reframe the UC inventory**

Add manual annotations that explain which UC endpoints are:

- prior-art reference only
- compatibility adapter target
- already backed by Arco-native state
- scaffolded

**Step 8: Run doc verification**

Run: `cargo xtask adr-check`

Expected: `SUCCESS`.

Run: `cd docs/guide && mdbook build`

Expected: build completes without broken links.

**Step 9: Commit**

```bash
git add docs/adr/adr-037-arco-catalog-product-surface.md docs/adr/adr-038-catalog-threat-model.md docs/adr/adr-039-catalog-consistency-model.md docs/guide/src/reference/catalog-privilege-matrix.md docs/guide/src/reference/catalog-api-contract.md docs/guide/src/reference/schema-evolution-policy.md docs/guide/src/reference/credential-vending-security.md docs/guide/src/reference/control-plane-scope.md docs/guide/src/reference/unity-catalog-openapi-inventory.md docs/guide/src/concepts/catalog.md docs/guide/src/concepts/architecture.md docs/plans/2026-04-23-authoritative-metastore-governance-surface.md
git commit -m "docs: define catalog product design contract"
```

### Task 1: Finalize Native Metastore Object Contracts

**Files:**
- Modify: `proto/arco/catalog/v1/metastore.proto`
- Modify: `crates/arco-proto/build.rs`
- Modify: `crates/arco-proto/src/lib.rs`
- Modify: `crates/arco-proto/tests/control_plane_transactions.rs`
- Create: `crates/arco-catalog/tests/metastore_product_surface.rs`

**Step 1: Write contract tests**

Add tests that require stable, versioned protobuf contracts for:

- principal
- group membership
- grant
- storage credential
- service credential
- external location
- external service connection
- managed storage root
- workspace binding
- view
- volume
- function
- registered model
- model version
- governance attachment
- policy attachment
- share/provider/recipient roadmap placeholders

**Step 2: Extend message shapes additively**

Extend the existing frozen `metastore.proto` contract so each object has:

- stable ID
- parent reference where applicable
- owner
- lifecycle state
- created/updated timestamps
- comment
- properties only as compatibility metadata
- reserved fields for forward compatibility

Because `arco.*.v1` is now durable, do not rename fields, remove fields, reuse
field numbers, or change ProtoJSON names. Add only backward-compatible fields or
oneof variants and keep `cargo xtask proto-breaking-check` passing.

**Step 3: Validate transaction hashing and serde**

Make sure every new mutation participates in deterministic transaction hashing and validation rules.

**Step 4: Run focused tests**

Run: `cargo test -p arco-proto --test control_plane_transactions -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-catalog --test metastore_product_surface -- --nocapture`

Expected: PASS for implemented contract assertions. Future assertions must be
ignored with a tracking reason or feature-gated under a non-default pending
contract feature; do not commit default failing tests.

Run: `buf lint proto/`

Expected: PASS.

Run: `cargo xtask proto-breaking-check`

Expected: PASS.

**Step 5: Commit**

```bash
git add proto/arco/catalog/v1/metastore.proto crates/arco-proto/build.rs crates/arco-proto/src/lib.rs crates/arco-proto/tests/control_plane_transactions.rs crates/arco-catalog/tests/metastore_product_surface.rs
git commit -m "feat: define native catalog product object contracts"
```

### Task 2: Add Authoritative Metastore State And Projection Kernel

**Files:**
- Create: `crates/arco-catalog/src/metastore/mod.rs`
- Create: `crates/arco-catalog/src/metastore/events.rs`
- Create: `crates/arco-catalog/src/metastore/envelope.rs`
- Create: `crates/arco-catalog/src/metastore/replay.rs`
- Create: `crates/arco-catalog/src/metastore/projections.rs`
- Create: `crates/arco-catalog/src/metastore/publish.rs`
- Create: `crates/arco-catalog/src/identity/mod.rs`
- Create: `crates/arco-catalog/src/authz/mod.rs`
- Create: `crates/arco-catalog/src/storage_governance/mod.rs`
- Create: `crates/arco-catalog/src/objects/mod.rs`
- Create: `crates/arco-catalog/src/governance/mod.rs`
- Create: `crates/arco-catalog/src/audit/mod.rs`
- Modify: `crates/arco-catalog/src/lib.rs`
- Modify: `crates/arco-catalog/src/tier1_events.rs`
- Modify: `crates/arco-catalog/src/tier1_state.rs`
- Modify: `crates/arco-catalog/src/tier1_snapshot.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Modify: `crates/arco-catalog/tests/schema_contracts.rs`
- Create: `crates/arco-catalog/tests/metastore_replay_publication.rs`

**Step 1: Write projection-kernel tests**

Require a pointer-published projection kernel that supports explicit
registration without exposing future object-family tables by default. Initial
tests must cover:

- deterministic replay
- projection registry allowlisting
- projection schema version and ledger watermark
- redaction by schema
- all-or-nothing publication

Future object-family projections such as `volumes.parquet`,
`functions.parquet`, `registered_models.parquet`, `model_versions.parquet`,
`governance_attachments.parquet`, and `policy_attachments.parquet` must be
added in the owning tasks after authoritative state exists. Do not commit
default failing tests for future projections.

**Step 2: Implement state fold**

Fold metastore mutations into typed current state keyed by stable IDs. Names
must be secondary indexes. Keep replay, projection, publication, authorization,
storage governance, object families, governance metadata, and audit in domain
modules rather than growing a single metastore state file.

**Step 3: Implement Parquet projection writer**

Write additive-only schema projections and update manifest-selected paths. Keep
sensitive credential material out of tenant-visible mintable projections.
Include projection watermarks and checksums where practical.

**Step 4: Add replay and publication hardening tests**

Add tests for:

- replay equals projection
- compaction preserves semantics
- CAS/publish failure leaves readers on the old complete projection set
- stale/missing/corrupt projections return explicit errors
- redaction excludes secret fields from projections and system-table sources

**Step 5: Add reader and writer extension points**

Expose the minimal reader/writer extension points that later domain tasks can
use. Do not publish volumes, functions, models, governance, or policy
projections until the owning tasks implement authoritative state and tests.

**Step 6: Run focused tests**

Run: `cargo test -p arco-catalog --test metastore_replay_publication -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-catalog --test schema_contracts -- --nocapture`

Expected: PASS.

**Step 7: Commit**

```bash
git add crates/arco-catalog/src/metastore crates/arco-catalog/src/identity crates/arco-catalog/src/authz crates/arco-catalog/src/storage_governance crates/arco-catalog/src/objects crates/arco-catalog/src/governance crates/arco-catalog/src/audit crates/arco-catalog/src/lib.rs crates/arco-catalog/src/tier1_events.rs crates/arco-catalog/src/tier1_state.rs crates/arco-catalog/src/tier1_snapshot.rs crates/arco-catalog/src/reader.rs crates/arco-catalog/src/writer.rs crates/arco-catalog/tests/schema_contracts.rs crates/arco-catalog/tests/metastore_replay_publication.rs
git commit -m "feat: add native metastore projection kernel"
```

### Task 3: Implement Grants, Principals, And Permission Compilation

**Files:**
- Modify: `crates/arco-catalog/src/authz/privileges.rs`
- Modify: `crates/arco-catalog/src/authz/grants.rs`
- Modify: `crates/arco-catalog/src/authz/compiler.rs`
- Create: `crates/arco-catalog/src/authz/decision.rs`
- Create: `crates/arco-catalog/src/authz/explain.rs`
- Modify: `crates/arco-catalog/src/identity/principals.rs`
- Modify: `crates/arco-catalog/src/identity/groups.rs`
- Modify: `crates/arco-catalog/src/identity/memberships.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Modify: `crates/arco-uc/src/routes/permissions.rs`
- Modify: `crates/arco-uc/src/audit.rs`
- Modify: `docs/guide/src/reference/catalog-privilege-matrix.md`
- Create: `crates/arco-uc/tests/permissions_authoritative.rs`
- Create: `crates/arco-catalog/tests/permission_compilation.rs`
- Create: `crates/arco-catalog/tests/authz_decisions.rs`

**Step 1: Write permission tests**

Cover:

- owner grants
- grant and revoke mutation
- inherited privileges from catalog to schema to table
- transitive group membership expansion
- group membership snapshot/revision recorded in decisions
- disabled/deleted principal behavior
- ownership transfer
- grant option
- explicit deny behavior if supported, or absence-of-allow deny if not
- principal filter support
- rename preserves grants because grants bind to stable IDs
- deny reasons are explainable
- explain-access output matches enforcement

Use minimal generic securable fixtures first. Add volume and external-location
privilege tests in their owning tasks after those object families exist.
- object-level authorization for every supported securable
- property-level authorization/redaction for sensitive grant and credential fields
- function-level authorization for list, read, create, update, delete, manage, and credential-mint operations
- deny-by-default behavior for stale compiled permissions and unknown object types

**Step 2: Implement permission compiler**

Compile grants into read-optimized rows by principal, object ID, object type, privilege, inheritance source, and deny/allow result.

**Step 3: Implement `AuthzDecision`**

Create the authorization subsystem described in this plan's formal contract.
Routes must call this subsystem instead of embedding bespoke permission logic.

**Step 4: Generate or validate the privilege matrix**

Make `docs/guide/src/reference/catalog-privilege-matrix.md` match code. Either
generate it from privilege definitions or add an `xtask` check that fails when
the doc and code diverge.

**Step 5: Make UC permissions authoritative**

Replace empty `privilege_assignments` with compiled assignments. Implement `PATCH` as a real grant/revoke mutation path.

**Step 6: Run focused tests**

Run: `cargo test -p arco-catalog --test permission_compilation -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-catalog --test authz_decisions -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-uc --test permissions_authoritative -- --nocapture`

Expected: PASS.

If this task adds a new `cargo xtask privilege-matrix-check` command to
`tools/xtask`, run it before commit. Otherwise do not cite that gate as passing
evidence; rely on the focused tests above and leave the xtask gate in the future
GA gate list.

**Step 7: Commit**

```bash
git add crates/arco-catalog/src/authz crates/arco-catalog/src/identity crates/arco-catalog/src/reader.rs crates/arco-catalog/src/writer.rs crates/arco-uc/src/routes/permissions.rs crates/arco-uc/src/audit.rs docs/guide/src/reference/catalog-privilege-matrix.md crates/arco-uc/tests/permissions_authoritative.rs crates/arco-catalog/tests/permission_compilation.rs crates/arco-catalog/tests/authz_decisions.rs
git commit -m "feat: implement authoritative grants and permissions"
```

### Task 4: Implement Storage Credentials, External Locations, And Managed Roots

**Files:**
- Modify: `crates/arco-catalog/src/storage_governance/credentials.rs`
- Modify: `crates/arco-catalog/src/storage_governance/external_locations.rs`
- Modify: `crates/arco-catalog/src/storage_governance/managed_roots.rs`
- Modify: `crates/arco-catalog/src/storage_governance/path_normalization.rs`
- Modify: `crates/arco-catalog/src/storage_governance/bindings.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Create: `crates/arco-uc/src/routes/storage_credentials.rs`
- Create: `crates/arco-uc/src/routes/external_locations.rs`
- Modify: `crates/arco-uc/src/routes/mod.rs`
- Modify: `crates/arco-uc/src/router.rs`
- Modify: `crates/arco-uc/src/openapi.rs`
- Create: `crates/arco-catalog/tests/path_governance.rs`
- Create: `crates/arco-uc/tests/storage_governance_authoritative.rs`

**Step 1: Write storage governance tests**

Cover:

- create/list/get/update/delete storage credential metadata
- create/list/get/update/delete external location
- path overlap rejection
- workspace binding enforcement
- managed root and external location separation
- credential secret material is never exposed in normal list/get responses
- URI canonicalization for GCS, S3, Azure, and local/dev paths
- trailing slash, duplicate slash, percent-encoding, and dot-segment handling
- sibling and parent/child overlap
- deleted/tombstoned path behavior
- provider CAS/precondition mapping
- cloud URI canonicalization
- trailing slash normalization
- percent-encoding and path traversal rejection
- managed root cannot overlap external location
- external locations cannot overlap each other unless an explicit rule permits it
- ambiguous paths fail closed

**Step 2: Implement storage governance state**

Persist credentials, external locations, managed roots, and bindings as authoritative state. Store secret references or encrypted payload handles separately from safe metadata projections.

**Step 3: Add UC-compatible route adapters**

Expose compatible routes as adapters over Arco writer/reader methods.

**Step 4: Run focused tests**

Run: `cargo test -p arco-catalog --test path_governance -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-uc --test storage_governance_authoritative -- --nocapture`

Expected: PASS.

If this task adds a new `cargo xtask redaction-check` command to `tools/xtask`,
run it before commit. Otherwise cover redaction with the focused tests above
and leave the xtask gate in the future GA gate list.

**Step 5: Commit**

```bash
git add crates/arco-catalog/src/storage_governance crates/arco-catalog/src/reader.rs crates/arco-catalog/src/writer.rs crates/arco-uc/src/routes/storage_credentials.rs crates/arco-uc/src/routes/external_locations.rs crates/arco-uc/src/routes/mod.rs crates/arco-uc/src/router.rs crates/arco-uc/src/openapi.rs crates/arco-catalog/tests/path_governance.rs crates/arco-uc/tests/storage_governance_authoritative.rs
git commit -m "feat: add authoritative storage governance"
```

### Task 5: Implement Credential Vending Engine And Table/Path Access Audit

**Files:**
- Modify: `crates/arco-uc/src/routes/credentials.rs`
- Modify: `crates/arco-uc/src/audit.rs`
- Modify: `crates/arco-iceberg/src/credentials/mod.rs`
- Modify: `crates/arco-iceberg/src/credentials/gcs.rs`
- Create: `crates/arco-uc/tests/credentials_authoritative.rs`
- Create: `crates/arco-catalog/tests/credential_vending_decisions.rs`

**Step 1: Write credential vending tests**

Cover:

- table read credentials
- table write credentials
- path credentials
- TTL clamping
- denied operation emits audit event
- allowed operation emits audit event
- credential scope never exceeds the governed path
- provider rejects unsupported operations
- credential payload is redacted from debug/log/audit/system-table sources
- stale projection behavior is deny-by-default unless bounded staleness is explicit

Do not include volume or model artifact credentials in this task. Volume
credential support is added after volume state exists. Model-version credential
support is added after model-version artifact path ownership exists.
- client access-delegation capability negotiation
- remote-signing fallback when direct credentials are disabled
- max TTL by operation and object family
- provider outage denies closed and emits audit
- revocation limitation is explicit in response/docs
- token-scope proof is recorded for audit without logging token material

**Step 2: Implement provider abstraction**

Use the existing GCS credential work as the first provider. Keep the interface
open for S3/Azure without blocking this tranche. Model direct temporary
credentials and remote signing as separate delegation mechanisms behind the
same authorization decision.

**Step 3: Replace placeholder route behavior**

`temporary-table-credentials` and `temporary-path-credentials` must evaluate
object/path, operation, grants, binding, TTL, and client capabilities before
issuing credentials.

**Step 4: Materialize access audit projections**

Publish safe audit rows for:

- allow/deny decision
- principal
- operation
- securable/object/path
- reason code
- credential expiry
- request ID
- client capability
- delegation mechanism
- redacted credential scope

**Step 5: Document credential-vending security**

Update `docs/guide/src/reference/credential-vending-security.md` with provider
behavior, TTL clamps, audit classification, and revocation limitations.

**Step 6: Run focused tests**

Run: `cargo test -p arco-catalog --test credential_vending_decisions -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-uc --test credentials_authoritative -- --nocapture`

Expected: PASS.

If this task adds a new `cargo xtask redaction-check` command to `tools/xtask`,
run it before commit. Otherwise cover redaction with the focused tests above
and leave the xtask gate in the future GA gate list.

**Step 7: Commit**

```bash
git add crates/arco-uc/src/routes/credentials.rs crates/arco-uc/src/audit.rs crates/arco-iceberg/src/credentials/mod.rs crates/arco-iceberg/src/credentials/gcs.rs crates/arco-uc/tests/credentials_authoritative.rs crates/arco-catalog/tests/credential_vending_decisions.rs
git commit -m "feat: vend governed temporary credentials"
```

### Task 6: Implement Volumes As Governed Path Objects

**Files:**
- Create: `crates/arco-uc/src/routes/volumes.rs`
- Modify: `crates/arco-uc/src/routes/mod.rs`
- Modify: `crates/arco-uc/src/router.rs`
- Modify: `crates/arco-uc/src/openapi.rs`
- Modify: `crates/arco-catalog/src/objects/volumes.rs`
- Modify: `crates/arco-catalog/src/storage_governance/path_normalization.rs`
- Modify: `crates/arco-catalog/src/authz/privileges.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Create: `crates/arco-uc/tests/volumes_authoritative.rs`

**Step 1: Write volume tests**

Cover:

- create/list/get/update/delete managed volume
- create/list/get/update/delete external volume
- parent catalog/schema validation
- storage path binding validation
- grant inheritance and explicit volume grants
- temporary volume credentials
- rename preserves stable ID and grants
- distinct file-operation privileges for list, read, write, and delete
- volume path cannot escape its governed root
- deletion/tombstone behavior

**Step 2: Implement volume state and routes**

Add authoritative volume CRUD over catalog writer/reader methods and expose UC-compatible adapters.

**Step 3: Extend credential vending for volumes**

Use the shared credential vending engine from Task 5. Add volume read/write
credential decisions only after volume path ownership is authoritative.

**Step 4: Run focused tests**

Run: `cargo test -p arco-uc --test volumes_authoritative -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-uc/src/routes/volumes.rs crates/arco-uc/src/routes/mod.rs crates/arco-uc/src/router.rs crates/arco-uc/src/openapi.rs crates/arco-catalog/src/objects/volumes.rs crates/arco-catalog/src/storage_governance/path_normalization.rs crates/arco-catalog/src/authz/privileges.rs crates/arco-catalog/src/reader.rs crates/arco-catalog/src/writer.rs crates/arco-uc/tests/volumes_authoritative.rs
git commit -m "feat: add governed catalog volumes"
```

### Task 7: Add Governance Metadata And Policy Attachment Primitives

**Files:**
- Modify: `proto/arco/catalog/v1/metastore.proto`
- Modify: `crates/arco-catalog/src/governance/tags.rs`
- Modify: `crates/arco-catalog/src/governance/glossary.rs`
- Modify: `crates/arco-catalog/src/governance/classifications.rs`
- Modify: `crates/arco-catalog/src/governance/policies.rs`
- Modify: `crates/arco-catalog/src/governance/attachments.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Create: `crates/arco-catalog/tests/governance_attachments.rs`
- Create: `crates/arco-api/tests/governance_metadata_api.rs`

**Step 1: Write governance tests**

Cover:

- tags
- classifications
- owners/stewards
- glossary terms and controlled vocabularies
- data domain
- source-of-truth and confidence metadata
- approval status
- masking policy attachment placeholder
- row-filter policy attachment placeholder
- `EnforcementStatus::NotEnforced` for policy placeholders
- stable object ID binding across rename
- inherited metadata read behavior where intended

**Step 2: Implement typed governance attachments**

Use typed attachment records with stable IDs, object IDs, attachment type,
payload, creator, timestamps, lifecycle state, source, confidence, approval
status, and enforcement status. Any required `metastore.proto` changes must be
additive against the frozen `arco.*.v1` baseline.

**Step 3: Add native API surfaces**

Expose minimal Arco-native endpoints for attach/detach/list. Keep policy enforcement out of scope unless a policy engine exists.

**Step 4: Run focused tests**

Run: `cargo test -p arco-catalog --test governance_attachments -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-api --test governance_metadata_api -- --nocapture`

Expected: PASS.

Run: `buf lint proto/`

Expected: PASS if `metastore.proto` changed.

Run: `cargo xtask proto-breaking-check`

Expected: PASS if `metastore.proto` changed.

**Step 5: Commit**

```bash
git add proto/arco/catalog/v1/metastore.proto crates/arco-catalog/src/governance crates/arco-catalog/src/reader.rs crates/arco-catalog/src/writer.rs crates/arco-catalog/tests/governance_attachments.rs crates/arco-api/tests/governance_metadata_api.rs
git commit -m "feat: add catalog governance metadata attachments"
```

### Task 8: Expand Lineage And Discovery Around Catalog Objects

**Files:**
- Modify: `crates/arco-api/src/routes/lineage.rs`
- Modify: `crates/arco-catalog/src/tier1_snapshot.rs`
- Modify: `crates/arco-catalog/src/state.rs`
- Modify: `crates/arco-api/src/routes/catalogs.rs`
- Create: `crates/arco-api/tests/catalog_discovery_api.rs`
- Create: `crates/arco-api/tests/lineage_catalog_objects.rs`

**Step 1: Write discovery tests**

Cover:

- search by owner, tag, classification, format, table type, and object family
- lineage neighborhood for a table
- lineage neighborhood for a volume-derived asset
- explain-access result for a table and a volume
- stale or missing projections return explicit errors
- idempotent lineage event ingestion by event ID
- run/job/dataset identity binding
- late-arriving lineage facets
- column lineage
- duplicate event deduplication

**Step 2: Extend discovery projections**

Project object summaries that include safe governance metadata, ownership, storage class, lineage counts, and freshness indicators.

**Step 3: Align lineage ingestion with event semantics**

Use an event model with explicit event ID, job identity, run identity, dataset
bindings, object IDs, facets, event time, and ingestion time. Late-arriving
facets must update the lineage projection idempotently.

**Step 4: Add explain-access endpoint**

Return why a principal can or cannot access a securable, including grants, inheritance source, storage binding, and deny reason.

**Step 5: Run focused tests**

Run: `cargo test -p arco-api --test catalog_discovery_api -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-api --test lineage_catalog_objects -- --nocapture`

Expected: PASS.

**Step 6: Commit**

```bash
git add crates/arco-api/src/routes/lineage.rs crates/arco-catalog/src/tier1_snapshot.rs crates/arco-catalog/src/state.rs crates/arco-api/src/routes/catalogs.rs crates/arco-api/tests/catalog_discovery_api.rs crates/arco-api/tests/lineage_catalog_objects.rs
git commit -m "feat: expand catalog discovery and lineage"
```

### Task 9: Add Function And Model Registry Object Families

**Files:**
- Create: `crates/arco-uc/src/routes/functions.rs`
- Create: `crates/arco-uc/src/routes/models.rs`
- Modify: `crates/arco-uc/src/routes/mod.rs`
- Modify: `crates/arco-uc/src/router.rs`
- Modify: `crates/arco-uc/src/openapi.rs`
- Modify: `crates/arco-catalog/src/objects/functions.rs`
- Modify: `crates/arco-catalog/src/objects/models.rs`
- Modify: `crates/arco-catalog/src/storage_governance/path_normalization.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Create: `crates/arco-uc/tests/functions_authoritative.rs`
- Create: `crates/arco-uc/tests/models_authoritative.rs`

Prerequisite: defer this task until tables, views, volumes, grants, storage
governance, credential vending, and audit are production-grade. Function
execution and model serving are explicitly out of scope; this task owns
metadata, artifact locations, grants, and model-version artifact credential
decisions only after model-version artifact path ownership is authoritative.
Sensitive system-table ACLs are required before exposing function/model rows in
Task 10, not before implementing their authoritative metadata.

**Step 1: Write object-family tests**

Cover:

- function create/list/get/delete
- registered model create/list/get/update/delete
- model version create/list/get/update/delete/finalize
- artifact storage location validation
- grants on function, registered model, and model version metadata
- stable IDs across rename/update

**Step 2: Implement authoritative state and route adapters**

Keep execution of functions and model serving out of scope. First land
metadata, storage governance, grants, lifecycle, and artifact path ownership.

**Step 3: Add model-version artifact credential decisions**

Extend the shared credential vending engine for model artifact read/write only
after model-version artifact path ownership is implemented. Cover allow and
deny audit rows, TTL clamping, and path scope checks.

**Step 4: Run focused tests**

Run: `cargo test -p arco-uc --test functions_authoritative -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-uc --test models_authoritative -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-uc/src/routes/functions.rs crates/arco-uc/src/routes/models.rs crates/arco-uc/src/routes/mod.rs crates/arco-uc/src/router.rs crates/arco-uc/src/openapi.rs crates/arco-catalog/src/objects/functions.rs crates/arco-catalog/src/objects/models.rs crates/arco-catalog/src/storage_governance/path_normalization.rs crates/arco-catalog/src/reader.rs crates/arco-catalog/src/writer.rs crates/arco-uc/tests/functions_authoritative.rs crates/arco-uc/tests/models_authoritative.rs
git commit -m "feat: add catalog function and model metadata"
```

### Task 10: Expand System Tables For Catalog Operations

**Files:**
- Modify: `crates/arco-api/src/system_tables.rs`
- Modify: `crates/arco-catalog/src/authz/decision.rs`
- Modify: `docs/guide/src/reference/system-catalog.md`
- Create: `crates/arco-api/tests/access_system_tables_api.rs`
- Create: `crates/arco-api/tests/storage_system_tables_api.rs`
- Create: `crates/arco-api/tests/catalog_product_system_tables_api.rs`
- Create: `crates/arco-api/tests/query_sandbox.rs`

Prerequisite: only add a table here after the owning task has published a safe,
authoritative projection. Access tables depend on Tasks 3 and 5. Storage tables
depend on Tasks 4 and 5. Volume tables depend on Task 6. Governance tables
depend on Task 7. Function and model tables depend on Task 9.

**Step 1: Write system-table tests**

Require read-only tables:

- `system.access.grants`
- `system.access.compiled_permissions`
- `system.access.audit`
- `system.access.auth_denies`
- `system.access.credential_mints`
- `system.storage.credentials`
- `system.storage.external_locations`
- `system.storage.managed_roots`
- `system.storage.workspace_bindings`
- `system.catalog.volumes`
- `system.catalog.functions`
- `system.catalog.registered_models`
- `system.catalog.model_versions`
- `system.governance.attachments`

Also require system-table ACL tests. Read-only is not enough: access audit,
credential-mint, deny, and storage-governance rows are sensitive and must be
filtered or denied through `AuthzDecision`.

**Step 2: Register only explicit allowlisted projections**

Extend `system_tables.rs` with explicit mappings. Do not auto-register every
projection file, and do not register planned tables before their backing
projection exists.

**Step 3: Enforce system-table ACLs**

Route system-table query registration through the authorization subsystem.
Ordinary principals must not be able to enumerate other users' grants,
credential mints, denied requests, hidden objects, secret references, or raw
policy payloads.

**Step 4: Redact sensitive fields**

Ensure system tables never expose credential secret material, raw tokens, or raw policy payloads that are not tenant-visible.

**Step 5: Run focused tests**

Run: `cargo test -p arco-api --test catalog_product_system_tables_api -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-api --test access_system_tables_api -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-api --test storage_system_tables_api -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-api --test query_sandbox -- --nocapture`

Expected: PASS.

If this task adds new `cargo xtask system-table-schema-check` or
`cargo xtask redaction-check` commands to `tools/xtask`, run them before
commit. Otherwise cover schema and redaction behavior with the focused tests
above and leave those xtask gates in the future GA gate list.

**Step 6: Commit**

```bash
git add crates/arco-api/src/system_tables.rs crates/arco-catalog/src/authz/decision.rs docs/guide/src/reference/system-catalog.md crates/arco-api/tests/access_system_tables_api.rs crates/arco-api/tests/storage_system_tables_api.rs crates/arco-api/tests/catalog_product_system_tables_api.rs crates/arco-api/tests/query_sandbox.rs
git commit -m "feat: expose catalog product system tables"
```

### Task 11: Compatibility, Final Verification, And Product Scorecard

**Files:**
- Modify: `docs/guide/src/reference/control-plane-scope.md`
- Modify: `docs/guide/src/reference/system-catalog.md`
- Modify: `docs/guide/src/reference/evidence-policy.md`
- Create: `docs/guide/src/reference/catalog-compatibility-matrix.md`
- Create: `docs/operations/backup-restore.md`
- Create: `docs/operations/slo-and-observability.md`
- Create: `docs/security/catalog-control-plane-hardening.md`
- Create: `docs/reports/2026-05-catalog-product-surface-evidence.md`

**Step 1: Update the scorecard and compatibility matrix with evidence**

For each domain, include the authoritative mutation type, state fold module,
projection file, reader method, writer method, enforcement hook, adapter routes,
system tables, tests, docs, and known gaps. Mark incomplete domains `Partial`
or `Planned`.

For each adapter route group, record native state backing, enforcement backing,
compatibility level, tests, and known gaps.

**Step 2: Add operations and hardening docs**

Document SLOs, metrics, traces, logs, audit retention, backup/restore, ledger
replay from zero, corrupted projection recovery, downgrade/rollback,
dependency policy, SBOM/provenance/signing expectations, secret scanning, and
release gates.

**Step 3: Write the evidence report**

Include:

- implemented object families
- implemented enforcement points
- implemented system tables
- compatibility adapters covered
- known product gaps
- exact verification output
- schema diff output
- OpenAPI diff output
- privilege-matrix check output
- redaction check output
- projection replay check output
- security scan output
- supply-chain check output
- benchmark-smoke output
- unresolved risk register

**Step 4: Run full verification**

Run every command in the verification matrix.

Expected: all commands pass.

**Step 5: Commit**

```bash
git add docs/guide/src/reference/control-plane-scope.md docs/guide/src/reference/system-catalog.md docs/guide/src/reference/evidence-policy.md docs/guide/src/reference/catalog-compatibility-matrix.md docs/operations/backup-restore.md docs/operations/slo-and-observability.md docs/security/catalog-control-plane-hardening.md docs/reports/2026-05-catalog-product-surface-evidence.md
git commit -m "docs: record catalog product hardening evidence"
```

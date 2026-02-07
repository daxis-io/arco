# Unity Catalog + Delta Lake Parity Research Packet (Arco)

**Status:** Draft  
**Date:** 2026-02-04  
**Audience:** Catalog/metastore lead researcher + engineering leads  

> External links below are starting points to validate; preview features and OpenAPI surfaces may change.

---

## Purpose

This packet consolidates (1) ecosystem signals relevant to “Unity Catalog parity” for Delta Lake, and (2) Arco-aligned initial findings, risks, and POCs. It is intentionally scoped to Arco’s file-native, browser-direct model (Parquet state, manifest CAS publish, no correctness-critical listing).

## Arco constraints (“sacred”)

- **File-native control plane:** core catalog state lives in Parquet snapshots; readers don’t require a database.
- **Compactor sole writer:** API appends events + triggers compaction; compactor publishes via manifest CAS (`docs/adr/adr-018-tier1-write-path.md`).
- **No correctness-critical listing:** only known-key `GET`/`HEAD` in hot paths; listing is anti-entropy only.
- **v1 scope:** GCS-only, single configured bucket (`docs/adr/adr-030-delta-uc-metastore.md`).

## Executive summary (what matters most)

1. **Delta is moving toward “catalog-managed tables”** where discovery and commit success are catalog-mediated (vs relying purely on object-store “PUT-if-absent”). This is the cleanest “serverless coordinator” parity story for Arco. [1]
2. **Unity Catalog exposes a Delta commit-coordinator API contract (OpenAPI)** with `/delta/preview/commits`, including semantics around “unbackfilled commits”. This contract is a concrete parity target. [4]
3. **Arco’s Delta design (ADR-030) already splits the world into**:
   - **Mode A:** external tables where `_delta_log` + filesystem semantics are source of truth.
   - **Mode B:** Arco-coordinated commits (catalog-managed path).
   Research should treat “Mode B” as “catalog-managed Delta parity,” not only “CAS on `_delta_log`”. (`docs/adr/adr-030-delta-uc-metastore.md`)
4. **Delta Sharing aligns with Arco’s signed-URL strengths** (provider returns file metadata + URLs). A Delta Sharing provider backed by Arco’s manifest allowlist is a natural second POC. [5]
5. **Credential vending is the largest “UC parity” gap.** Signed URLs can be close to equivalent for GCS read access; multi-cloud parity implies STS/SAS/OAuth temporary credentials (or a costly proxy). [4]
6. **Approach:** target UC-compatibility via an API facade that translates UC objects to Arco-native Parquet state, rather than adding an always-on metastore DB. [4]
7. **Recommended POC order:** (1) Delta commit coordinator semantics, (2) Delta Sharing provider, (3) Parquet stats sidecar index.

---

## 1) Ecosystem update to incorporate into parity research

### Delta protocol: catalog-managed tables + catalog-mediated commits

Delta protocol discussions around “catalog-managed tables” frame a direction where the **catalog becomes the source of truth** for discovery and commit success, instead of relying only on filesystem atomic operations. [1]

Delta Lake release communications for Delta Lake 4.0 mention preview support for catalog-managed tables and warn the preview feature may change. [2]

**Why it matters for Arco:** it validates “commit coordination as a catalog service” as a first-class direction, which aligns with Arco’s serverless control-plane model.

---

## 2) Unity Catalog: the “how commits are coordinated” hook

### Unity Catalog OSS OpenAPI is a concrete parity target

The Unity Catalog OSS project publishes an OpenAPI spec. [4]

That spec includes a **Delta commit coordinator surface** under a `DeltaCommits` tag with `/delta/preview/commits` endpoints, described as a commit-coordinator API for managed tables. [4]

High-level behavior indicated by the spec docs:
- **POST `/delta/preview/commits`**: register/coordinate a commit; server limits “unbackfilled” commits and expects clients to backfill.
- **GET `/delta/preview/commits`**: list unbackfilled commits and return the latest table version known.

**Arco takeaway:** treat this API surface as a compatibility contract, not just inspiration.

---

## 3) Findings mapped to research questions

### Research questions (as used in this packet)

- **Q1:** What does “first-class metastore” mean for Delta, especially under catalog-managed tables (discovery + commits)?
- **Q2:** What are UC “table stakes” features vs differentiators for Arco?
- **Q3:** How do we coordinate transactions and access (credentials) without always-on servers?
- **Q4:** How far can “compiled governance” go without engine integration?
- **Q5:** What is the minimum API surface needed for a drop-in replacement / compatibility facade?

### Q1: “First-class metastore” comes in two tiers

**Tier A — external Delta tables (filesystem source of truth)**  
Arco catalogs Delta tables for discovery/governance, but transactional correctness stays in `_delta_log` + object store semantics. This requires:
- Name → location resolution (catalog objects)
- Schema/protocol/features surfaced
- No coordination of `_delta_log` beyond reading it safely (known-key reads, no listing)

This aligns with **ADR-030 Mode A**.

**Tier B — catalog-managed Delta tables (catalog source of truth)**  
Here the catalog mediates discovery and commit success (catalog-managed direction). [1]  
This aligns with **ADR-030 Mode B** and with UC’s explicit commit coordinator API. [4]

**Implication:** parity research should explicitly treat “Mode B” as “catalog-managed Delta,” not only a custom Arco write-path.

### Q1: Delta Sharing fits Arco’s signed URLs model

Delta Sharing is an open protocol where a sharing server returns metadata and URLs for data access. [5][6]

**Arco mapping:** a Delta Sharing provider can be a serverless API that vends short-lived signed URLs for:
- Parquet data files
- required `_delta_log` / checkpoint artifacts
subject to compiled governance allowlists.

### Q2: Unity Catalog parity — table stakes vs differentiators

**Table stakes (high priority):**
- UC object model: catalogs/schemas/tables (CRUD)
- Permissions + grants (object-level minimum)
- Secure external client access (credential vending or equivalent)
- Auditing (who changed what)
- External engine interoperability (Spark first)

UC’s positioning emphasizes governed managed tables and secure non-Databricks access via APIs. [7]

**Where Arco can exceed:**
- Browser-direct reads (DuckDB-WASM) + file-native metadata
- Lineage-by-execution (Servo) rather than SQL parsing lineage
- Compiled governance artifacts for “read without control-plane round trips” where feasible

### Q3: Transaction coordination without servers — the realistic serverless pattern

For catalog-managed Delta parity, object-store CAS on `_delta_log` alone doesn’t resolve “reserved version but missing commit file” failure modes.

UC’s OpenAPI hints at a “commit coordinator + backfill” model via “unbackfilled commits”. [4]

**Serverless commit coordinator pattern (Arco):**
1. Client requests commit coordination (UC-compatible POST).
2. Coordinator records the commit attempt as authoritative (strong, small write).
3. Client backfills the actual commit to `_delta_log/` and acknowledges success.
4. Coordinator exposes unbackfilled commits for repair/backfill and eventually GC.

**Arco alignment:** use Tier-1 strong write semantics (lock/CAS discipline) for coordination metadata, but keep read paths file-native.

### Q3: Credential vending — signed URLs are enough only in the GCS-only case

UC’s OpenAPI includes endpoints to vend temporary credentials for tables/volumes/paths with multi-cloud credential types. [4]

**Arco take:**
- For **GCS-resident data**, signed URLs can be equivalent for read access (tight scope, short TTL).
- For **S3/ADLS parity**, you need:
  - STS/SAS/OAuth credential vending, or
  - an expensive proxy/download layer (conflicts with “$0 at rest” and “browser direct”).

Treat multi-cloud credential vending as a top risk/gap item.

### Q4: Governance without engine integration has a hard ceiling

Compiled governance works best when it can produce a file/partition allowlist. General row-level predicates often require:
- engine-side integration (views/policy planning), or
- a sharing/enforcement plane where the server selects results (Delta Sharing provider can help for recipients).

### Q5: Drop-in replacement — target the OpenAPI facade

The UC OSS OpenAPI spec is the best single “API surface area” anchor. [4]

AWS documents external Spark configured against UC open APIs (and Iceberg REST behind UC for Iceberg). [8]

**Arco mapping:** implement a UC-compat API facade that translates UC objects to Arco’s Parquet-native catalog state (Tier 1), preserving file-native readers.

---

## 4) Arco-aligned file-native implementation mapping (first pass)

### A) Delta table registration + discovery

Store in Tier-1 Parquet state (queryable):
- `table_id` (stable UUID)
- `full_name` (`catalog.schema.table`)
- `table_uri` (scoped path / `gs://...`)
- `format = delta`
- `mode = external | catalog_managed`
- protocol/features flags
- owner + governance references
- optional: latest known version/checkpoint pointer for faster reads

### B) Commit coordinator state

Store as:
- small per-table coordinator state object (JSON) for strong coordination
- append-only Parquet ledger of commit attempts and backfill status (queryable)

(Reuse Tier-1 lock/CAS + compactor invariants; see `docs/adr/adr-018-tier1-write-path.md`.)

### C) Stats enrichment (sidecar Parquet index)

Delta encodes stats in log `add` actions, but engines benefit from a query-native index.

Sidecar Parquet index keyed by `(table_id, snapshot_version, file_path)` with:
- min/max/null_count/distinct_count/row_count/bytes per column

Benefits:
- DuckDB-WASM / DataFusion can prune quickly
- future stats endpoint becomes trivial

### D) Compiled governance artifacts

Treat governance compilation outputs as artifacts:
- object ACL materialization (Parquet)
- column mask expression map (Parquet)
- optional file/partition allowlist manifests for policies that compile

---

## 5) Competitive gap matrix (v0.1)

| System | Catalog scope | Table formats | Engine interoperability | Governance | Lineage | Ops model | Browser-direct / $0-at-rest |
|---|---|---|---|---|---|---|---|
| Unity Catalog (Databricks) | Full metastore + governance | Delta + Iceberg (managed) | Strong (Databricks-native; open APIs for others) | Strong (fine-grained + credential vending) | Strong but often SQL/job derived | Managed service | ❌ |
| Apache Polaris | Iceberg catalog | Iceberg (primary) | Iceberg REST; multi-engine focus [9] | Varies by deployment | Not primary focus | Always-on service | ❌ |
| AWS Glue Data Catalog | Metastore + integrations | Iceberg/Hudi/Delta integrations | Strong in AWS engines | Lake Formation (separate) | Limited native lineage | Managed | ❌ |
| Hive Metastore | Legacy metastore | Table defs; format logic external | Broad (legacy) | Weak | Weak | Always-on server | ❌ |
| DataHub | Discovery/metadata graph | Tracks many systems | Connectors-oriented | Not a transaction metastore | Strong lineage [11] | Always-on | ❌ |
| OpenMetadata | Discovery + quality + lineage | Tracks many systems | Connectors-oriented | Varies by integration | Lineage support [12] | Always-on | ❌ |

**Where Arco is ahead:** serverless economics + browser-direct reads; lineage-by-execution.  
**Where Arco must build:** credential vending and catalog-managed commit coordination parity.

---

## 6) Risk register (v0.1)

1. **Catalog-managed Delta commit coordination requires a highly reliable write-path API** (even if serverless).
2. **Row-level security without engine integration:** file-level compilation won’t cover general predicates.
3. **Cross-cloud access:** signed URLs don’t replace STS/SAS/OAuth for S3/ADLS parity.
4. **Client support maturity:** catalog-managed Delta features are preview/fast-moving; expect compatibility churn. [2]

---

## 7) Prioritized POCs

### POC 1 (highest priority): UC-compatible Delta commit coordinator (serverless)

**Goal:** demonstrate end-to-end catalog-managed commit coordination semantics.

- Implement UC-compatible `/delta/preview/commits` POST/GET behavior (initial subset).
- Persist unbackfilled commits + backfill acknowledgements in object storage.
- Enforce tenant/workspace isolation by storage layout.

**Anchors:** UC OpenAPI `DeltaCommits` + Delta catalog-managed direction. [1][4]

### POC 2: Delta Sharing provider backed by Arco signed URLs

**Goal:** implement recipient flow where Arco vends signed URLs for governed file access.

- Use Delta Sharing protocol spec as contract. [5]
- Implement policy compilation → allowlist selection (where feasible).
- NOTE: ADR-030 currently scopes v1 to a file-manifest API before Delta Sharing; treat this POC as post-v1 unless that ADR is revised.

### POC 3: Parquet stats sidecar index

**Goal:** measurable planning/perf wins for browser/engine reads by avoiding repeated `_delta_log` scans.

---

## 8) Suggested additions to the longer research program

1. **Catalog-managed Delta support reality check**
   - What is the precise client contract expected by Delta clients for catalog-managed commits + discovery?
   - Which engines support it today (Spark, Delta Kernel, Trino/Starburst, etc.)?

2. **Unity Catalog OSS as baseline API requirements**
   - Treat `api/all.yaml` as the canonical surface area inventory. [4]
   - Generate a pinned endpoint inventory (by commit hash) and keep a parity-tracker table in-repo.

---

## 9) Internal Arco docs to start from (repo pointers)

- Tier-1 lock + compactor invariants: `docs/adr/adr-018-tier1-write-path.md`
- Storage layout + scoping: `docs/adr/adr-005-storage-layout.md`, `crates/arco-core/src/scoped_storage.rs`
- Existence privacy boundary: `docs/adr/adr-019-existence-privacy.md`
- Delta UC-like metastore design: `docs/adr/adr-030-delta-uc-metastore.md`
- Iceberg REST CAS + idempotency patterns (reusable for Delta coordinator): `docs/plans/2025-12-22-iceberg-rest-integration-design.md`

---

## 10) Execution plan (research → engineering)

This section turns the packet into an execution-ready program: lock the UC contract, validate Delta catalog-managed semantics, and implement **full Unity Catalog OSS OpenAPI parity** in Arco while preserving Arco invariants (file-native state, compactor sole-writer, no correctness-critical listing).

### Definition of done (success criteria)

**Contract / compatibility**
- Unity Catalog OSS OpenAPI spec (`api/all.yaml`) is **vendored and pinned** (commit recorded) and used as the **source of truth** for “what endpoints exist”.
- Arco-generated OpenAPI for the UC facade **passes compliance checks** against the vendored spec (paths/methods/parameter names/response codes at minimum; schemas as follow-up hardening).
- **All endpoints in the pinned spec** are implemented (no placeholder 404/501 for in-scope functionality).

**Scope defaults**
- **GCS-only functional credential vending**; AWS/Azure credential types return **explicit, spec-shaped “not supported”** errors (tracked as follow-ups).
- **No correctness-critical listing** in hot paths; listing is anti-entropy only.
- **Compactor remains sole writer** for Parquet state; API writes events/locks only.

**Security**
- Tenant/workspace scoping enforced for all UC endpoints.
- Audit events emitted for sensitive operations (auth allow/deny, credential vending allow/deny, commit coordination allow/deny).

---

### Milestones (recommended ordering)

### Milestone status (2026-02-06)

- ✅ M0 complete: pinned UC OSS `all.yaml` fixture + generated endpoint inventory committed.
- ✅ M3 complete: `arco-uc` crate, router wiring, request context, UC error envelope, server mount config.
- ✅ M4 complete: read-only discovery endpoints for catalogs/schemas/tables with pagination.
- ✅ M5 complete (Scope-A): permissions endpoint + temporary table/path credentials + audit decisions.
- ✅ M6 complete (Scope-A): `/delta/preview/commits` GET/POST with managed-table gates and coordinator-backed versioning.
- ✅ M7 contract gate active: OpenAPI compliance test un-ignored and passing against pinned fixture.

#### M0 — Pin the UC contract (prerequisite gate)

**Deliverables**
- Vendor a pinned copy of Unity Catalog OSS OpenAPI:
  - Add: `crates/arco-uc/tests/fixtures/unitycatalog-openapi.yaml`
  - Record upstream commit hash + retrieval date in a short header comment at top of the file.
- Generate and commit a derived endpoint inventory (for human review):
  - Add: `docs/plans/2026-02-04-unity-catalog-openapi-inventory.md`
  - Contents: endpoints grouped by OpenAPI tag + CRUD semantics notes + “required for Spark/Delta” annotation.

**Acceptance**
- Inventory reviewed and signed off as the parity contract for the next milestones.

#### M1 — Research outputs (decision-ready)

**Deliverables**
- “Catalog-managed Delta” protocol summary focused on discovery + commit authority (what the client expects, where the catalog becomes source of truth).
- UC Delta commit coordinator semantics summary for `/delta/preview/commits` including:
  - unbackfilled commit queue model
  - backfill/ack expectations
  - failure modes (reserved version but missing log file) and repair paths
- Delta Sharing contract summary (provider/recipient flow; what can be enforced as file allowlists vs what needs engine integration).

**Acceptance**
- A short “decisions + constraints” addendum: what we will/won’t implement in v1 (especially Delta Sharing vs file-manifest-first per ADR-030).

#### M2 — Architecture + ADR alignment

**Deliverables**
- Update: `docs/adr/adr-030-delta-uc-metastore.md` to explicitly cover:
  - catalog-managed Delta framing (Mode B)
  - commit coordinator parity target (UC `/delta/preview/commits`)
  - how “unbackfilled commits” maps to Arco’s file-native state
- Add a new ADR for UC facade architecture:
  - Create: `docs/adr/adr-031-unity-catalog-api-facade.md`
  - Includes: authority model (what lives in Tier-1 Parquet vs operational metadata), routing/mounting approach, error model approach, and test strategy.

**Acceptance**
- ADRs accepted (or explicitly marked Proposed with a concrete acceptance gate).

#### M3 — UC facade skeleton (code-first)

**Engineering deliverables**
- Create new crate: `crates/arco-uc`
  - Router: `crates/arco-uc/src/router.rs`
  - Request context extension + middleware: `crates/arco-uc/src/context.rs`
  - Error types matching UC spec shape: `crates/arco-uc/src/error.rs`
  - OpenAPI generation via `utoipa`: `crates/arco-uc/src/openapi.rs`
  - Minimal smoke routes:
    - `GET /openapi.json` (or spec-equivalent) for generated spec
    - any “public”/unauth endpoints required by the spec (if any)
- Mount UC router in `crates/arco-api/src/server.rs` behind a config flag:
  - Add config: `unity_catalog.enabled: bool` (default false)
  - Mount prefix must match the vendored OpenAPI contract:
    - If the OpenAPI has `servers.url`, mount at that base path.
    - Otherwise mount at `/` and implement the full path strings from the spec.

**Acceptance**
- `cargo test -p arco-uc` passes.
- UC openapi generation test passes and exposes the expected major path groups.

#### M4 — Implement core object model endpoints

**Engineering deliverables**
- Implement catalogs/schemas/tables endpoints backed by `arco_catalog::{CatalogReader, CatalogWriter}` (Tier-1 strong writes via sync compaction).
- Ensure ID strategy matches existing conventions (UUIDv7) and is stable for rename operations.

**Acceptance**
- Integration test (HTTP) covers a basic CRUD lifecycle for catalog + schema + table within one tenant/workspace.

#### M5 — Permissions + temporary credentials (GCS-only functional)

**Engineering deliverables**
- Implement permissions/grants endpoints with a compiled-grants representation (Tier-1 state), aligned with ADR-030 intent.
- Implement temporary credential vending endpoints:
  - GCS: signed URL delegation (and/or token vending if required by spec)
  - AWS/Azure: explicit “not supported” errors with spec-shaped payloads + audit emission
- Add audit events for credential vending allow/deny.

**Acceptance**
- Integration test verifies:
  - permission checks affect access
  - credential TTL is clamped/bounded
  - denied requests emit auditable events

#### M6 — Delta commit coordinator parity (`/delta/preview/commits`)

**Engineering deliverables**
- Implement the commit coordinator state + ledger consistent with Arco invariants:
  - no correctness-critical listing
  - strong coordination writes (small objects) + file-native queryability for history
- Implement:
  - POST commit registration/coordination
  - GET unbackfilled commits + latest version
  - backfill acknowledgement semantics

**Acceptance**
- Concurrency tests cover:
  - two writers racing for a version
  - crash after reservation but before backfill
  - listing unbackfilled + repair/backfill path
- End-to-end smoke verifies a managed-table commit can be coordinated and reconciled.

#### M7 — Close remaining spec surface + conformance gates

**Engineering deliverables**
- For each remaining OpenAPI tag group not covered above:
  - implement endpoints
  - add targeted integration coverage for non-trivial semantics
- Add OpenAPI compliance tests similar to Iceberg:
  - Add: `crates/arco-uc/tests/openapi_compliance.rs`
  - Assert vendored spec is a subset of generated (paths/methods/params/response codes).

**Acceptance**
- Compliance test passes against pinned spec.
- “Top flows” integration suite passes (CRUD, permissions, credentials, delta commits).

---

### Test strategy (required, not optional)

**Contract tests**
- OpenAPI compliance test (`arco-uc`) comparing generated vs vendored spec.

**Integration tests**
- Add: `crates/arco-integration-tests/tests/unity_catalog_smoke.rs`
  - Uses an in-process Axum server + in-memory storage backend where possible.
  - Validates tenant/workspace scoping + basic endpoint behavior.

**Failure-mode tests**
- Commit coordination conflict and crash recovery.
- Credential vending: TTL clamp + denial audit.

---

### Assumptions / defaults

- Network access is blocked in this environment; the vendored `unitycatalog-openapi.yaml` must be provided out-of-band and pinned by commit hash.
- v1 is GCS-only for functional credential vending; non-GCS credential types return explicit spec-shaped errors.
- Arco invariants remain non-negotiable: compactor sole writer, no correctness-critical listing, file-native state.

## Appendix A: UC OpenAPI endpoint inventory (starter set)

This appendix is intentionally a **starter list** based on references in the UC OSS OpenAPI and parity discussions; verify directly against `api/all.yaml` and generate a full inventory for parity tracking. [4]

High-value endpoints called out in parity discussions include:
- Catalog objects: `/catalogs`, `/schemas`, `/tables`
- Permissions: `/permissions/{securable_type}/{full_name}`
- Temporary credentials: `/temporary-*-credentials` variants
- Delta commit coordination: `/delta/preview/commits` [4]

---

## References

[1]: https://github.com/delta-io/delta/issues/4381
[2]: https://groups.google.com/g/delta-users/c/lTwLnV4S7Dk
[3]: https://www.databricks.com/blog/open-sourcing-unity-catalog
[4]: https://raw.githubusercontent.com/unitycatalog/unitycatalog/main/api/all.yaml
[5]: https://raw.githubusercontent.com/delta-io/delta-sharing/main/PROTOCOL.md
[6]: https://www.databricks.com/blog/2021/05/26/introducing-delta-sharing-an-open-protocol-for-secure-data-sharing.html
[7]: https://docs.databricks.com/aws/en/tables/delta-table
[8]: https://aws.amazon.com/blogs/big-data/use-databricks-unity-catalog-open-apis-for-spark-workloads-on-amazon-emr/
[9]: https://polaris.apache.org/
[10]: https://docs.aws.amazon.com/glue/latest/dg/connect-glu-iceberg-rest.html
[11]: https://docs.datahub.com/docs/features/feature-guides/lineage
[12]: https://docs.open-metadata.org/latest/how-to-guides/data-lineage
[13]: https://docs.databricks.com/aws/en/delta-sharing

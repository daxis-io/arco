# Arco Consolidated Product, Architecture, and Roadmap State

**Reconciled:** 2026-09-01
**Repository baseline:** `origin/main@febdff55590cec7429e94b02ddb08f49b1bfae96`
**Original report baseline:** `1979f0bc401c074637ea7496df7ea936c1dff900`

**Candidate implementation status:** local candidate branch on 2026-09-02;
local evidence only, with no provider qualification, deployment, cutover, or
soak claim.

The one intervening commit is PR #410's UC Delta API configuration groundwork.
It does not construct `ControlMvpStateStore`, qualify a provider, or change
catalog authority, so the authority reconciliation remains unchanged.

## Executive conclusion

Arco's accepted authority model is no longer "metadata is Parquet." It commits
correctness-critical state as immutable object-store artifacts selected by one
exact-CAS head and asynchronously publishes open, watermarked Parquet
projections. The legacy catalog implementation still uses `CatalogWriter` and
synchronous compaction on the reconciled repository baseline. The candidate
implementation routes native, UC, and Iceberg catalog operations through one
exact-root authority adapter, but it remains default-disabled and has not been
deployed.

ADR-043 and the `control/v1` kernel are substantial implemented work, not a
paper design. They include bounded replay anchors, writer fencing, typed
ambiguity reconciliation, checksummed and structurally validated Arrow
segments and indexes, checkpoints, restore machinery, projection intents, a
provider-neutral authority-store capability, deterministic adapters, a
promotion evidence format, and a benchmark harness. Those repository results
are not live-provider qualification or route authority.

S3, GCS, and Azure remain unqualified for catalog authority. Their public
capability records are not a deployment promotion registry, ignored
credentialed tests do not count as successful evidence, and PR #412's IAM
smoke was skipped. Private operations must own credentialed proof, deployed IAM
and infrastructure, promotion state, cutover, and raw evidence.

## Authority model

```text
          native / UC / Iceberg catalog protocols
                         |
                 CatalogAuthority
                         |
               exact per-root binding
                  /              \
       legacy ADR-018          ADR-043 control/v1
 CatalogWriter + ledger       immutable JSON + Arrow
 + sync Parquet compaction    + exact CAS authority head
                  \              /
                   one authority per root
                              |
                 internal winning StateToken
                              |
              durable in-state projection intent
                              |
             best-effort notify + anti-entropy drain
                              |
                async Parquet/system projections
                              |
                 separate projection-ack root
```

`StateToken` pins an authority cut; it is not an authorization grant, a table
snapshot, a projection watermark, or a cross-root transaction. Compatibility
protocols bind it internally during the pilot.

## Corrected implementation state

The authoritative path on every deployed root is still:

```text
CatalogWriter -> ledger append -> synchronous compaction
              -> immutable Parquet manifest -> pointer CAS
```

The current cutover candidate includes:

- provider-neutral `ArcoStateStore`/`ScopedAuthorityStore` contracts;
- deterministic model and memory/fault backends;
- `control/v1` immutable transaction, segment, manifest, checkpoint, and head
  artifacts;
- 64 MiB Arrow segment, 512 KiB index, one-million-row hard limits, and L1
  shards targeted at half those capacities;
- v4 ordered non-overlapping L1 shard sets, bounded replay, durable maintenance
  intent at 16 L0 segments, typed backpressure at 32, and equivalent-state
  exact-CAS consolidation without a logical-sequence increment;
- 64 KiB head, 1 MiB manifest/checkpoint/restore, and 4 MiB transaction and
  aggregate-projection-intent JSON caps;
- index-pruned point/prefix/token reads and bounded authority-pinned scan pages,
  with parent lookup pinned to the retained cut and authenticated-encrypted
  protocol continuations;
- exact CAS, fencing, stable transaction identity, and ambiguity
  reconciliation;
- checksum-bound Arrow/index preflight before decode;
- `StateToken`, snapshot/export, restore, shadow import/comparison, projection
  intent/ack, and promotion-evidence machinery;
- conservative active GC with a separate list/delete capability, retention
  epoch coordination, current-head revalidation, seven-day orphan eligibility,
  and a 30-day manifest/checkpoint floor;
- one shared catalog command/authority model with stable object, name-index,
  column, idempotency, audit, and projection-intent records;
- exact default-legacy root bindings, stable protocol error mapping, and
  native/UC/Iceberg route selection for one configured control root;
- pilot-root rejection of managed Delta and Iceberg metadata commits before
  legacy or storage mutation side effects;
- a fixed catalog projection consumer, restart-safe real-Parquet materializer,
  artifact-before-ack ordering, operator-invoked anti-entropy recovery, durable
  redacted success/failure status, exact poison-intent quarantine without
  watermark advancement, a fail-open process-local post-commit wake,
  fail-closed projection-only routes, and `system.catalog.projection_status`;
- separately constructed S3, GCS, and Azure adapters.

The following remain cutover work rather than current claims:

- provider queue notification and an always-on deployed anti-entropy scheduler;
- complete cloud-neutral operational metrics and alerts;
- private provider evidence, IAM enforcement, infrastructure, promotion,
  cutover, active runtime validation, and the seven-day soak.

Until projection-only readers are wired to the materialized artifacts,
control-bound inventory and browser routes fail closed instead of serving
legacy catalog snapshots. The operator catalog drain performs materialization
before acknowledgement and cannot generically rebind or trim the catalog
source domain, so it cannot advance a watermark without a visible artifact or
advance the logical catalog sequence with worker metadata.

## Fixed decisions for the first milestone

- The pilot root is seeded, synthetic, and non-public.
- Native, UC, and Iceberg catalog operations switch together.
- `StateToken` remains internal to idempotency, audit, projection, and response
  construction.
- The default authority binding is legacy; only the exact pilot scope may
  select `control/v1`.
- Conservative active GC uses seven-day orphan eligibility, 30-day token and
  checkpoint retention, and indefinite retention of the pre-cutover
  snapshot/export and legacy authority artifacts.
- The root must complete seven consecutive clean soak days.
- Catalog projection p99 lag is at most 10 seconds, with no normal-operation
  interval above 60 seconds.
- Planner/runtime migration and ADR-042 lineage expansion are deferred.

## Repository boundary

The public repository owns product code and cloud-neutral contracts:

- provider-neutral authority and storage interfaces;
- S3/GCS/Azure adapters and construction APIs without credentials;
- deterministic conformance/fault backends;
- the catalog authority model, route bindings, projections, metrics, tests,
  public protocol capability declarations, and `arco-cli deploy` product
  manifest upload.

Private `daxis-io/arco-ops` owns live operations:

- Terraform/Cloud Build and Cloud Run deployment/rollback;
- credentialed provider and IAM workflows/harnesses;
- deployed UAT and cloud-specific dashboards/runbooks;
- AWS S3/Lambda/KMS/SQS/IAM/STS/Access Grants qualification;
- cutover orchestration, promotion registry, and raw evidence storage.

Assets must be copied from an exact public SHA with source path and hash in a
provenance manifest before public removal. Public history is not rewritten.
Deterministic local UAT remains public; credentialed/deployed modes move
privately. Provider capability documents describe protocols, not a public
write-enable registry.

## Promotion and hard-cut gate

Private evidence binds the public SHA, image and lock digests, adapter version,
AWS account/region, bucket and Lambda settings, and IAM/KMS policy digests. It
must prove exact conditional semantics, stale-token rejection, opaque token
reuse, no hidden conditional retry, lost-response classification, sustained
25 successful semantic mutations/s for 60 minutes, bounded L0 backlog,
contention behavior, cold/burst Lambda, KMS/IAM/SQS/STS/Access Grants faults,
corruption, recovery, GC races, secondary-region roll-forward restore, a
1.5-second API retry portion, and the 10-second/60-second projection objective.

Any failure blocks cutover. Throughput failure requires a new ADR; it does not
authorize DynamoDB, a resident writer, hidden sharding, or a second authority.

The operational sequence is canonical in
[Catalog Authority Hard Cut](../guide/src/reference/catalog-authority-hard-cut.md).
Before all protocols switch, the legacy writer may remain authoritative. Once
`control/v1` accepts a mutation, rollback to that writer is forbidden.

## Milestone boundary

Success proves one S3-backed synthetic metastore's catalog DDL. It does not
prove customer data, grants/RBAC, credential vending, managed Delta commits,
Iceberg metadata commits, Flow, rich lineage, GCS/Azure, global compactor
retirement, or general production readiness.

The machine-readable layer-by-layer state is maintained in the
[control-plane scorecard](../guide/src/reference/control-plane-scope.md).

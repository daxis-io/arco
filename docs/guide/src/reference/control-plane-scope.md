# Control-Plane Scope

This scorecard separates architecture, repository implementation, local
evidence, private provider qualification, production route wiring, and actual
authority. A check in one column never implies a check in a later column.

Reconciled against `origin/main@febdff55590cec7429e94b02ddb08f49b1bfae96`
on 2026-09-01. The only change after the original report baseline
`1979f0bc401c074637ea7496df7ea936c1dff900` is the UC Delta API configuration
groundwork in PR #410; it does not change the authority assessment.

Candidate implementation columns below include the uncommitted 2026-09-02
working tree. They are local evidence only; the private qualification,
authoritative, cutover, and soak columns deliberately remain negative.

Legend:

- **Yes**: evidence exists at exactly this layer.
- **Partial**: a real but narrower implementation exists.
- **No**: the layer has not been crossed.
- **N/A**: the layer does not apply to that row.

Implementation claims must follow the [evidence policy](./evidence-policy.md).
Private qualification means machine-recorded provider evidence bound to a
public Git SHA; repository tests and ignored credentialed tests do not qualify.

## Authority-transition scorecard

| Area | Accepted design | Implemented | Locally verified | Privately provider-qualified | Route-wired | Authoritative |
|---|---|---|---|---|---|---|
| Legacy catalog DDL (`CatalogWriter` -> ledger -> synchronous compaction -> Parquet manifest) | Yes, as the ADR-018 legacy path | Yes | Yes | Not recorded here | Yes through the shared adapter for every unbound root | Yes for every uncut root |
| `ArcoStateStore` traits and `StateToken` contract | Yes (ADR-043) | Yes | Yes | N/A | Yes for an exact configured control root | No deployed authority |
| Deterministic model store | Yes as a reference oracle | Yes | Yes | N/A | No | No |
| `control/v1` object-store kernel | Yes (ADR-043) | Yes: v4 ordered L1 shards, bounded replay, checksum-bound Arrow/indexes, fencing, ambiguity reconciliation, restore | Yes | No | Yes, default-disabled for one exact root | No |
| Bounded JSON envelopes and index-pruned paginated reads | Yes | Yes: 64 KiB head, 1/4 MiB envelopes, 4 MiB pages, authenticated-encrypted authority-pinned continuations, pinned and query-bound parent lookup, shared L0/L1 physical-read accounting, 64-segment budget, and unchanged native legacy keyset cursors | Yes for deterministic and control-kernel contracts | No | Yes through control authority reads | No |
| Asynchronous L1 maintenance and conservative active GC | Yes | Yes: durable intent at 16 L0s, typed backpressure at 32, separate exact-CAS consolidation/GC capabilities, retention epochs and conservative marking | Yes for deterministic maintenance, recovery, retention, and deletion contracts | No | Worker APIs exist; no deployed scheduler | No |
| Durable projection intents and acknowledgement root | Yes | Partial: intents, fixed consumer, real-Parquet materializer, separate ack root, exact terminal quarantine, monotonic durable redacted status, fail-open process-local notification, and fail-closed projection reads; provider queue delivery and always-on deployed scheduling absent | Yes for artifact-before-ack, retry/terminal recovery, poison-then-valid anti-entropy progress, concurrent status updates, sequence isolation, and status routing | No | Immediate local wake and operator drain exist; no deployed scheduler | No |
| Shared native/UC/Iceberg `CatalogAuthority` | Yes | Yes | Yes, including cross-protocol reads and DDL | No | Yes, selected by exact binding | No deployed control root |
| Exact per-root legacy/control binding | Yes | Yes: one validated tenant/workspace pair, no wildcard form, legacy default | Yes | No | Yes through server/native/UC/Iceberg state | No deployed control root |
| S3 adapter | Yes, first qualification target | Yes | Yes for deterministic adapter contracts | No successful private gate recorded | Selectable by storage composition, not catalog authority | No catalog root |
| GCS adapter | Yes as a future independent target | Yes | Yes for deterministic adapter contracts | No | Selectable by storage composition, not catalog authority | No catalog root |
| Azure adapter | Yes as a future independent target | Yes | Yes for deterministic adapter contracts | No | Selectable by storage composition, not catalog authority | No catalog root |
| Synthetic S3 catalog-DDL pilot | Yes | No | No | No | No | No |

## Product-surface scorecard

| Area | Accepted design | Implemented | Locally verified | Privately provider-qualified | Route-wired | Authoritative |
|---|---|---|---|---|---|---|
| Catalog/schema/table DDL and columns-at-registration | Yes | Yes on legacy and control authorities | Yes, including name/index/cascade/idempotency invariants and cross-protocol reads | Not recorded here | Native, UC, and Iceberg through one adapter | Legacy only in deployed state |
| Table-format catalog records (Delta, Iceberg, Parquet) | Yes | Yes | Yes | N/A | Yes, with protocol-specific gaps | Yes on legacy catalog authority |
| Managed Delta commits | Deferred from pilot | Yes as a separate coordinator; control pilot rejects before side effects | Yes | Not part of this gate | Existing behavior on non-pilot roots; disabled on pilot | Separate table-scoped authority |
| Iceberg metadata commits | Deferred from pilot | Yes on existing route; control pilot rejects before legacy access | Yes | Not part of this gate | Existing behavior on non-pilot roots; disabled on pilot | Not moved to pilot authority |
| Catalog Parquet/system-table projections | Yes, derived only | Legacy synchronous path plus control restart-safe Parquet materializer and status contract | Yes for materialization, restart recovery, redacted failures, and status-table lag | N/A | Legacy yes; control operator drain is wired, while projection-only reads still fail closed | Derived; never authorization authority |
| Grants/RBAC and route-wide authorization | Yes, later milestone | Partial | Partial | No | Partial | Common catalog authority exists; authorization cutover remains deferred |
| Storage credentials and external locations | Yes, later milestone | Partial create/list/get | Partial | No | Partial | Narrow scoped metastore paths only |
| Rich lineage observations and projections | Proposed (ADR-042), deferred | Partial legacy edge surface | Yes for existing scope | N/A | Partial | Legacy lineage domain only |
| Planner/runtime migration | Design direction, deferred | Partial seam | Partial | N/A | Partial | Existing orchestration path remains |
| Workspace snapshot/export/roll-forward restore | Yes | Partial deterministic machinery | Yes | No | No operator route | No production caller |

## Current thesis

The candidate proves a legacy file-native catalog and locally verified,
default-disabled `control/v1` catalog route machinery. It does **not** prove a
provider-qualified, deployed, or authoritative `control/v1` catalog root. The
milestone becomes proven only after provider notification/deployed scheduling
and remaining operations work, a synthetic S3 root's private gate,
simultaneous protocol cutover, old-writer revocation, and the seven-day soak.

## Canonical references

- [Catalog authority hard cut](./catalog-authority-hard-cut.md)
- [ADR-043](../../../adr/adr-043-s3-state-token-authority.md)
- [ADR-018 legacy path](../../../adr/adr-018-tier1-write-path.md)
- [Evidence policy](./evidence-policy.md)
- `docs/reports/2026-09-01-consolidated-product-architecture-roadmap-state.md`

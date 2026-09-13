# Catalog Authority Hard Cut

ADR-043 defines the target catalog authority. ADR-018 remains active for each
root until that exact root completes this hard cut. There is no dual-writer or
fallback period after `control/v1` accepts its first mutation.

```text
native catalog DDL ---------+
UC catalogs/schemas/tables -+--> CatalogAuthorityBinding
Iceberg namespaces/tables --+          |
                                       +-- LegacyCatalogAuthority (default)
                                       |
                                       `-- ControlV1CatalogAuthority
                                                |
                         exact CAS: control/v1/domains/catalog/head/current.json
                                                |
                              committed StateToken + durable projection intent
                                                |
                         best-effort notification wake-up (fail open)
                                                |
                    restart-safe Parquet materializer + separate ack root
```

The candidate implementation commits durable catalog projection intents,
materializes real Parquet artifacts through a restart-safe operator-invoked
anti-entropy drain, acknowledges only after the artifact manifest is visible,
and exposes durable redacted success/failure state through
`system.catalog.projection_status`. A malformed durable intent receives an
exact terminal quarantine record; it remains outside the materialized
watermark and visible as unresolved backlog while later valid intents continue.
The catalog consumer never installs generic binding or trim metadata in the
authority root. Control-bound projection-only inventory and browser routes
still fail closed because they are not yet wired to these artifacts. A
cloud-neutral post-commit notifier schedules an immediate process-local drain
and ignores notification failure after recording the durable intent; the
operator drain remains the restart-safe anti-entropy path. Provider queue
delivery and an always-on deployed scheduler remain private cutover work.

## Pilot contract

The first root is a seeded, non-public synthetic `(tenant_id, workspace_id)`.
Native, UC, and Iceberg catalog reads and permitted DDL switch together. The
default for every other scope remains legacy.

The winning `StateToken` is internal. It binds the idempotency receipt, audit
record, projection intent, and response construction without changing native,
UC, or Iceberg response bodies or headers. Control list cursors carry retained
cuts only inside authenticated encryption shared by the three facades in one
deployment. Startup requires the same private 32-byte cursor key on every
replica; tampering and cursors from a deliberately rotated key fail closed.
Iceberg namespace continuations additionally bind the exact parent filter and
resolve parent existence at the retained authority cut.
Unbound native routes retain their pre-cutover keyset cursor wire format.

Managed Delta commits and Iceberg `commit_table` are disabled for the pilot
root. They must return the stable unsupported/retryable response and must not
reach a legacy writer behind the new catalog authority.

## Cutover sequence

1. Seed deterministic catalogs, schemas, table registrations, columns,
   renames, deletes, conflicts, and idempotent retries.
2. Import exact legacy cuts into an isolated shadow root until objects, names,
   IDs, columns, receipts, watermarks, and cross-protocol reads are equivalent.
3. Retain a pre-cutover snapshot/export and complete a roll-forward restore
   drill in a disposable root.
4. Freeze pilot mutations with `503` and `Retry-After: 5`; drain in-flight
   mutations and synchronous compaction.
5. Record the exact legacy pointer version and hash, run the final import, and
   require byte-normalized/domain-semantic equivalence.
6. Revoke the old API/compactor identity's pilot write access.
7. Deploy the control writer and exact root binding. It can write the pilot
   `control/v1` authority but cannot write legacy authority or projections.
8. Switch all catalog protocols and verify cross-protocol reads before writes
   reopen.
9. Execute a sentinel mutation and verify authority read-after-write, internal
   token binding, durable projection intent, asynchronous watermark advance,
   and no synchronous compactor call.
10. Complete seven consecutive clean days with daily one-hour 25-mutation/s
    windows, contention and ambiguity drills, projection recovery, restore
    inspection, and active GC.

Before step 8, the still-authoritative legacy path may be unfrozen. After the
first `control/v1` mutation is accepted, recovery freezes the root and rolls
forward on `control/v1`; it never re-enables the old writer.

## Fixed pilot decisions

- internal-only `StateToken`;
- synthetic first root;
- all catalog protocol surfaces switch together;
- conservative active GC: seven-day orphan age, 30-day token/checkpoint pins,
  indefinite pre-cutover export and legacy-artifact retention;
- seven-consecutive-day soak;
- projection p99 lag at most 10 seconds and no normal interval above 60
  seconds.

This pilot proves catalog DDL on one S3-backed metastore. It does not prove
customer data migration, grants/RBAC cutover, credential vending, managed
Delta coordination, Iceberg metadata commits, Flow, rich lineage, GCS/Azure,
or general production readiness.

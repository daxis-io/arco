# ADR-030: Delta Lake + UC-Like Metastore/Catalog (Arco-Native)

## Status

Accepted (2026-02-18, Delta-primary cutover release train)

## Context

Arco needs Delta Lake as a first-class table format in the metastore/catalog while
preserving Arco's operating assumptions:

1. Append-only ingest for event sources (DDL and Delta operational facts are events).
2. A compactor is the sole writer of Parquet state.
3. Atomic publish via manifest CAS for Parquet state consumed by readers.
4. Readers do not need the ledger (they read Parquet state + manifests).
5. No bucket-listing dependency in hot paths (no correctness-critical `list`; only
   known-key `GET`/`HEAD`; listing is allowed only for explicit anti-entropy tooling).

Delta tables are Parquet data files plus `_delta_log/` transaction logs. A UC-like
metastore must be able to resolve logical names to locations, expose metadata
(schema/protocol/features), and provide safe write semantics for coordinated commits
without introducing an always-on database or a listing-dependent commit protocol.

## Decision

### Storage Contract (Locked)

All catalog and Delta control-plane state is scoped via `ScopedStorage`:

`tenant={tenant}/workspace={workspace}/...`

v1 is **GCS-only** and Delta tables must live in the Arco-configured storage bucket.
Table locations are referenced as scoped paths or as
`gs://<configured_bucket>/tenant=.../workspace=.../...`.

### Tier-1 Write Model (Locked)

For catalog objects (catalogs/schemas/tables) and grants:

1. API/writer acquires a distributed lock.
2. API/writer reads current Parquet snapshot (validation only).
3. API/writer appends a Tier-1 DDL event (append-only).
4. API triggers Tier1Compactor, which folds events into Parquet snapshots.
5. Tier1Compactor atomically publishes via manifest CAS.
6. API releases the lock.

This preserves ADR-018's "Tier-1 write path" and the "compactor is sole Parquet writer"
invariant.

### UC-Like Metastore Object Model (Arco-Native)

Arco introduces UC-like logical objects with stable IDs:

- **Catalog** (new): `catalog_id`, `catalog_name`
- **Schema**: existing `namespace` becomes a schema, now scoped to a `catalog_id`
- **Table**: existing table model continues, scoped to a schema/namespace

Backwards compatibility is preserved by mapping legacy namespaces to:

- `catalog = "default"`
- `namespace == schema`

### RBAC (Compiled Grants) (MVP)

RBAC is represented as compiled grant rows bound to stable object IDs (not names) so
renames do not invalidate access control keys.

Bootstrap policy: if a workspace has **zero grants**, the first successful metastore
mutation auto-grants the caller `MANAGE` on the default catalog; normal RBAC applies
thereafter.

`grants.parquet` is written as Tier-1 state but is **not** mintable via browser URL
allowlists (Posture A privacy boundary).

### Delta Subsystem

Arco supports two Delta table modes:

- **Mode A (filesystem OCC):** Arco catalogs Delta tables and reads `_delta_log` using
  known-key access (`_last_checkpoint` + sequential commit reads) without any listing
  in correctness paths.
- **Mode B (Arco coordinated commits):** Arco provides a two-phase commit API and a
  per-table coordinator state machine to coordinate commits without relying on
  filesystem atomicity.

#### Catalog-managed Delta framing (Mode B)

Mode B is Arco's implementation of **catalog-managed Delta**: the catalog becomes the
authority for discovery and commit success, rather than relying solely on object-store
atomic operations on `_delta_log`.

**Parity target (contract):** Unity Catalog OSS publishes an OpenAPI surface for Delta
commit coordination under `/delta/preview/commits` (a "commit coordinator" API). Arco
targets this surface as the compatibility contract for managed-table writes.

#### "Unbackfilled commits" mapping

Unity Catalog's commit coordinator model implies an **"unbackfilled commits" queue**:
the coordinator can reserve/record commits before the corresponding `_delta_log`
entries are durably written, and clients later "backfill" those log files.

Arco maps this to a file-native coordinator ledger:

1. **Coordinate (reserve)**: record a pending commit intent as small, strongly
   consistent control-plane state (Tier-1 semantics; no correctness-critical listing).
2. **Backfill**: client writes `_delta_log/{version}.json` (and any referenced files)
   to the table location.
3. **Acknowledge / reconcile**: coordinator marks the commit as backfilled (and may
   trigger projection/compaction), enabling repair paths for "reserved version but
   missing log file" failures.

Mode B uses a staging upload (signed PUT) to avoid large request bodies; Arco is the
sole writer to the real `_delta_log/{version}.json` file for Mode B tables.

Delta "first-class metadata" is produced as Arco-managed Parquet projections (published
via a Delta manifest CAS) so readers do not need to replay the Delta log.

## Consequences

- Arco can offer a UC-like object model and RBAC without an always-on database, while
  preserving browser-direct Parquet reads for non-sensitive metadata.
- Delta Mode B requires Arco API availability for commit coordination (serverless
  scale-to-zero is acceptable), but does not require hot-path bucket listing.
- v1 is intentionally scoped:
  - GCS-only, single configured bucket
  - Delta Sharing (official protocol) not implemented; Arco ships a file-manifest API
    first (paged, latest-only in v1)

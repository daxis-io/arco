# ADR-041 L0 Inbox Slice Verification

Date: 2026-06-06

## Deterministic Local Proof

| Command | Exit | Key result |
|---|---:|---|
| `cargo test -p arco-core --test flow_paths_contracts` | 0 | 9 path contract tests passed, including ADR-041 `_inbox`, receipt, L1 segment, and shard-index path builders. |
| `cargo test -p arco-flow --test orchestration_event_log_tests` | 0 | 9 L0 inbox tests passed for Parquet metadata derivation, scope validation, task-key consistency, storage-scope binding, path-safe storage-scope IDs, duplicate `event_id`, duplicate `producer_id + producer_seq`, create-if-absent writes, and duplicate bundle non-overwrite. |
| `cargo test -p arco-flow` | 0 | `arco-flow` package tests passed, including 467 library tests, the new event-log target, binaries, integration targets, and doc-tests. |
| `cargo test -p arco-core` | 0 | `arco-core` package tests passed, including storage, lock, publish, path, trybuild, and doc-tests. Cloud-backed storage conformance tests were ignored because they require live credentials. |
| `cargo test -p arco-api task` | 0 | Focused API task/callback filter passed, including run-scoped callback task-id lookup and task-token tests. |
| `cargo test -p arco-api system_table` | 0 | Focused API system-table filter passed for allowlist and deferred-table exposure tests. |
| `cargo xtask adr-check` | 0 | ADR conformance check passed. |
| `cargo xtask repo-hygiene-check` | 0 | Repository hygiene checks passed. |
| `cargo fmt --check` | 0 | Formatting check passed. |

## Scope Verified

- The L0 inbox writer writes only `_inbox/orchestration/.../bundle=*.parquet`.
- Stored L0 bundle objects are Parquet-readable and carry one row per bundled
  event.
- The writer rejects bundle metadata whose tenant/workspace does not match the
  `ScopedStorage` tenant/workspace before writing.
- L0 bundles reject mixed `task_key` values for the same attempt.
- L0 bundle metadata declares `format = "parquet"` and
  `compression = "uncompressed"`, matching the `.parquet` object path.
- Duplicate bundle object paths use create-if-absent semantics and do not
  overwrite existing content.
- Bundle, run, producer, and attempt IDs used in L0 object-key segments reject
  path-unsafe values before storage writes.
- The new writer does not write the legacy one-object
  `ledger/orchestration/{date}/{event_id}.json` path.
- The implementation is additive and does not switch callback routing away from
  the existing visible ledger/compactor path.

## Not Verified by This Slice

- Live GCS or S3 object-store behavior. The deterministic tests use Arco's
  storage abstraction and in-memory/object-store-memory conformance coverage.
- Callback ingest routing to L0 bundles.
- L0-to-L1 promotion, L1 shard-index CAS, L2 projection publication, receipt
  segment publication, and missed-notification recovery.
- Active-run DuckDB actors, trusted direct upload, and Quack fan-in.

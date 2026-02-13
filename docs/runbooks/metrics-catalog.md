# Arco Metrics Catalog

This document catalogs all metrics emitted by Arco services, organized by domain and criticality.

## Overview

Arco uses OpenTelemetry for metrics collection. All metrics use the `arco_` prefix.

### Metric Types

| Type | Description | Example |
|------|-------------|---------|
| Counter | Monotonically increasing value | `arco_events_written_total` |
| Gauge | Point-in-time value | `arco_backpressure_pending_lag` |
| Histogram | Distribution of values | `arco_cas_latency_seconds` |

### Common Labels

All metrics include these labels:

| Label | Description |
|-------|-------------|
| `tenant_id` | Tenant identifier |
| `workspace_id` | Workspace identifier |
| `domain` | Catalog domain (catalog, executions, lineage) |
| `service` | Service name (api, compactor) |

---

## Gate 2 Batch 3 Invariant Checks (2026-02-12)

These checks are the local/code-only closure checks for Gate 2 Batch 3.

### 1) Typed-path canonicalization (non-catalog flow/api/iceberg)

No hardcoded storage path literals should remain in scoped modules:

```bash
rg -n '"(ledger|state|events|_catalog|orchestration|manifests|locks|snapshots|commits|sequence|quarantine)/[^"\\n]*"' \
  crates/arco-flow/src/orchestration/ledger.rs \
  crates/arco-flow/src/outbox.rs \
  crates/arco-iceberg/src/pointer.rs \
  crates/arco-iceberg/src/events.rs \
  crates/arco-iceberg/src/idempotency.rs \
  crates/arco-api/src/paths.rs \
  crates/arco-api/src/routes/manifests.rs \
  crates/arco-api/src/routes/orchestration.rs
```

Expected result: no matches.

### 2) Deterministic/property invariants (out-of-order, duplicate, crash replay)

```bash
cargo test -p arco-flow --features test-utils --test property_tests compaction_is_out_of_order_and_duplicate_invariant -- --nocapture
cargo test -p arco-flow --features test-utils --test property_tests compaction_crash_replay_converges_to_single_pass_state -- --nocapture
```

Expected result: all tests pass.

### 3) Failure-injection checks (CAS race, partial writes, compaction replay)

```bash
cargo test -p arco-iceberg test_pointer_store_cas_race_has_single_winner -- --nocapture
cargo test -p arco-flow ledger_writer_recovers_after_partial_batch_write_failure -- --nocapture
cargo test -p arco-flow --test orchestration_correctness_tests test_compaction_replay_recovers_after_manifest_publish_failure -- --nocapture
```

Expected result: all tests pass.

### 4) Batch matrix evidence (release gate input)

Batch 3 command matrix output is archived at:

- `release_evidence/2026-02-12-prod-readiness/phase-3/batch-3-head/command-matrix-status.tsv`
- `release_evidence/2026-02-12-prod-readiness/phase-3/batch-3-head/command-logs/`

---

## Gate 5 Critical Metrics

These metrics are essential for Gate 5 invariant monitoring.

### Storage Operations

#### `arco_storage_operations_total`
**Type:** Counter
**Labels:** `operation`, `prefix`, `result`

Counts all storage operations by type and prefix.

| Operation | Description |
|-----------|-------------|
| `get` | Object read |
| `put_ledger` | Ledger event write |
| `put_state` | State file write |
| `cas_manifest` | Manifest CAS update |
| `list` | Object listing |
| `head` | Object metadata fetch |

**Gate 5 Alert:** `api` service should NEVER increment `put_state` operations.

```promql
# Alert: API writing to state/ prefix
increase(arco_storage_operations_total{service="api", operation="put_state"}[5m]) > 0
```

#### `arco_cas_attempts_total`
**Type:** Counter
**Labels:** `domain`, `result`

Counts CAS attempts with results: `success`, `precondition_failed`, `error`.

#### `arco_cas_latency_seconds`
**Type:** Histogram
**Labels:** `domain`, `result`
**Buckets:** 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10

Latency distribution for CAS operations.

---

### Fencing and Permits

#### `arco_publish_permit_issued_total`
**Type:** Counter
**Labels:** `domain`, `issuer_resource`

Counts publish permits issued.

#### `arco_publish_permit_consumed_total`
**Type:** Counter
**Labels:** `domain`, `result`

Counts permit consumption: `success`, `stale_token`, `cas_failed`.

**Gate 5 Alert:** `stale_token` indicates potential split-brain attempt.

```promql
# Alert: Stale fencing token attempts
increase(arco_publish_permit_consumed_total{result="stale_token"}[5m]) > 0
```

#### `arco_fencing_token_value`
**Type:** Gauge
**Labels:** `domain`

Current fencing token value per domain. Used for monitoring token progression.

---

### Lock Management

#### `arco_lock_acquisitions_total`
**Type:** Counter
**Labels:** `resource`, `result`

Lock acquisition attempts: `acquired`, `contention`, `timeout`, `error`.

#### `arco_lock_held_seconds`
**Type:** Histogram
**Labels:** `resource`
**Buckets:** 0.1, 0.5, 1, 5, 10, 30, 60, 120

Duration locks are held.

#### `arco_lock_renewals_total`
**Type:** Counter
**Labels:** `resource`, `result`

Lock renewal attempts: `success`, `expired`, `error`.

---

### Backpressure

#### `arco_backpressure_pending_lag`
**Type:** Gauge
**Labels:** `domain`

Current pending lag (last_written_position - last_compacted_position).

#### `arco_backpressure_rejections_total`
**Type:** Counter
**Labels:** `domain`, `threshold`

Requests rejected due to backpressure: `soft`, `hard`.

```promql
# Alert: Hard backpressure active
arco_backpressure_pending_lag > 10000
```

---

### Compaction

#### `arco_compaction_runs_total`
**Type:** Counter
**Labels:** `domain`, `trigger`, `result`

Compaction runs by trigger: `sync_rpc`, `notification`, `anti_entropy`.

#### `arco_compaction_events_processed_total`
**Type:** Counter
**Labels:** `domain`

Total events folded during compaction.

#### `arco_compaction_latency_seconds`
**Type:** Histogram
**Labels:** `domain`, `trigger`
**Buckets:** 0.1, 0.5, 1, 5, 10, 30, 60

Compaction duration.

#### `arco_anti_entropy_gaps_found_total`
**Type:** Counter
**Labels:** `domain`

Missed events discovered by anti-entropy.

**Gate 5 Alert:** Gaps indicate potential notification loss.

```promql
# Alert: Anti-entropy finding gaps
increase(arco_anti_entropy_gaps_found_total[1h]) > 10
```

---

### Manifest Integrity

#### `arco_manifest_version`
**Type:** Gauge
**Labels:** `domain`, `manifest_type`

Current manifest version for monitoring progression.

#### `arco_manifest_succession_failures_total`
**Type:** Counter
**Labels:** `domain`, `reason`

Succession validation failures: `version_regression`, `hash_mismatch`, `ulid_regression`.

**Gate 5 Alert:** Any succession failure indicates potential rollback attempt.

```promql
# Alert: Manifest succession failure
increase(arco_manifest_succession_failures_total[5m]) > 0
```

#### `arco_snapshot_artifacts_verified_total`
**Type:** Counter
**Labels:** `domain`, `result`

Artifact verification before publish: `success`, `missing`, `size_mismatch`.

---

### Tier-1 Operations

#### `arco_tier1_operations_total`
**Type:** Counter
**Labels:** `operation`, `result`

DDL operations: `create_namespace`, `drop_namespace`, `create_table`, `drop_table`.

#### `arco_tier1_latency_seconds`
**Type:** Histogram
**Labels:** `operation`
**Buckets:** 0.1, 0.25, 0.5, 1, 2.5, 5, 10

End-to-end Tier-1 operation latency (including sync compaction).

---

### Signed URLs

#### `arco_signed_urls_issued_total`
**Type:** Counter
**Labels:** `operation`, `domain`

Signed URL issuance by operation type.

#### `arco_signed_url_expiry_seconds`
**Type:** Histogram
**Labels:** `operation`
**Buckets:** 60, 300, 600, 1800, 3600, 7200

Requested signed URL expiry times.

---

## Service Health Metrics

### API Service

#### `arco_api_requests_total`
**Type:** Counter
**Labels:** `method`, `endpoint`, `status_code`

HTTP request counts.

#### `arco_api_request_latency_seconds`
**Type:** Histogram
**Labels:** `method`, `endpoint`
**Buckets:** 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5

Request latency distribution.

### Compactor Service

#### `arco_compactor_queue_depth`
**Type:** Gauge
**Labels:** `domain`

Pending compaction work items.

### Flow Orchestration

#### `arco_orch_callbacks_total`
**Type:** Counter
**Labels:** `handler`, `result`

Worker callback outcomes by HTTP-style result code (`200`, `401`, `409`, `500`, ...).

#### `arco_orch_callback_errors_total`
**Type:** Counter
**Labels:** `handler`, `result`

Subset counter for non-2xx callback outcomes.

#### `arco_orch_callback_duration_seconds`
**Type:** Histogram
**Labels:** `handler`

End-to-end callback handler latency.

#### `arco_flow_dispatch_queue_depth`
**Type:** Gauge
**Labels:** `queue`

Dispatch queue depth from queue implementations that expose reliable depth.
Cloud Tasks backends no longer emit a sentinel `0` for unknown depth; use provider backlog metrics instead.

#### `arco_compactor_last_run_timestamp`
**Type:** Gauge
**Labels:** `domain`, `trigger`

Unix timestamp of last compaction run.

---

## Dashboard Queries

### Gate 5 Compliance Dashboard

```promql
# Sole Writer Verification
sum(increase(arco_storage_operations_total{operation="put_state"}[5m])) by (service)

# Fencing Token Progression
arco_fencing_token_value

# Backpressure Status
arco_backpressure_pending_lag

# Manifest Version Progression
arco_manifest_version

# CAS Success Rate
sum(rate(arco_cas_attempts_total{result="success"}[5m])) / sum(rate(arco_cas_attempts_total[5m]))
```

### Operational Health Dashboard

```promql
# API Latency P99
histogram_quantile(0.99, sum(rate(arco_api_request_latency_seconds_bucket[5m])) by (le, endpoint))

# Compaction Throughput
sum(rate(arco_compaction_events_processed_total[5m])) by (domain)

# Lock Contention Rate
sum(rate(arco_lock_acquisitions_total{result="contention"}[5m])) / sum(rate(arco_lock_acquisitions_total[5m]))
```

---

## Alert Definitions

See [prometheus-alerts.yaml](./prometheus-alerts.yaml) for complete alert definitions.

### Critical Alerts (Page Immediately)

| Alert | Condition | Description |
|-------|-----------|-------------|
| `ArcoApiWritingState` | API service writes to state/ | Gate 5 violation |
| `ArcoStaleFencingToken` | Stale token publish attempts | Potential split-brain |
| `ArcoManifestSuccessionFailure` | Rollback/regression detected | Data integrity risk |
| `ArcoHardBackpressure` | Hard threshold exceeded | Service degradation |

### Warning Alerts (Investigate During Business Hours)

| Alert | Condition | Description |
|-------|-----------|-------------|
| `ArcoAntiEntropyGaps` | Gaps found in ledger | Notification reliability issue |
| `ArcoHighCASContention` | CAS failure rate > 10% | Performance degradation |
| `ArcoLongHeldLock` | Lock held > 60s | Potential deadlock |
| `ArcoCompactionBacklog` | Pending lag > soft threshold | Compactor falling behind |

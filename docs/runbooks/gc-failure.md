# Runbook: GC Failure

## Overview

This runbook covers troubleshooting and recovery for garbage collection (GC) failures in the Arco catalog system. GC is responsible for cleaning up:

- **Orphaned snapshots**: Snapshot directories not referenced by any manifest
- **Old ledger events**: Events compacted beyond the retention window
- **Old snapshot versions**: Snapshots beyond the retention count

## Quick Reference

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `arco_gc_run_completed` | GC run finished | No run in 24h |
| `arco_gc_errors_total` | Errors during GC | > 0 |
| `arco_gc_objects_deleted_total` | Objects deleted | - |
| `arco_gc_bytes_reclaimed_total` | Bytes reclaimed | - |

## Symptoms

### 1. Storage costs increasing unexpectedly

**Symptom**: Cloud storage bills growing faster than expected.

**Possible Causes**:
- GC not running (compactor not deployed)
- GC running but failing silently
- Retention policy too conservative

**Diagnosis**:
```bash
# Check GC logs for recent runs
gcloud logging read 'jsonPayload.metric="arco_gc_run_completed"' --limit=10

# Check for GC errors
gcloud logging read 'jsonPayload.metric="arco_gc_errors_total"' --limit=10
```

**Resolution**:
1. Verify compactor is deployed and healthy
2. Check retention policy configuration
3. Run manual GC with dry-run to see what would be deleted

### 2. GC Phase Failure: Orphaned Snapshots

**Symptom**: Error in logs: `gc_orphaned_snapshots failed`

**Possible Causes**:
- Storage backend connectivity issues
- Permission denied reading manifests
- Corrupted manifest JSON

**Diagnosis**:
```bash
# Check manifest accessibility
gsutil cat gs://BUCKET/tenant=TENANT/workspace=WORKSPACE/manifests/root.manifest.json

# Check storage permissions
gcloud storage buckets get-iam-policy gs://BUCKET
```

**Resolution**:
1. Verify service account has `roles/storage.objectUser`
2. Validate manifest JSON is well-formed
3. If manifest is corrupted, restore from backup or recreate

### 3. GC Phase Failure: Compacted Ledger

**Symptom**: Error in logs: `gc_compacted_ledger failed`

**Possible Causes**:
- Ledger files have restricted permissions
- Watermark not advancing (compactor issue)

**Diagnosis**:
```bash
# Check ledger directory
gsutil ls gs://BUCKET/tenant=TENANT/workspace=WORKSPACE/ledger/executions/

# Check watermark in manifest
gsutil cat gs://BUCKET/tenant=TENANT/workspace=WORKSPACE/manifests/executions.manifest.json \
  | jq '.watermarkEventId, .lastCompactionAt'
```

**Resolution**:
1. If watermark is stuck, investigate compactor health
2. Ensure ledger files are accessible to GC service account

### 4. GC Phase Failure: Old Snapshots

**Symptom**: Error in logs: `gc_old_snapshots failed`

**Possible Causes**:
- Snapshot files locked by active readers
- Delete permission denied

**Diagnosis**:
```bash
# List snapshot versions
gsutil ls gs://BUCKET/tenant=TENANT/workspace=WORKSPACE/snapshots/catalog/

# Check current manifest points to which version
gsutil cat gs://BUCKET/tenant=TENANT/workspace=WORKSPACE/manifests/catalog.manifest.json \
  | jq '.snapshotPath'
```

**Resolution**:
1. Verify GC service account has delete permissions
2. Wait for active readers to complete (delay_hours setting)
3. If snapshot is corrupted, it may not be deletable - contact cloud support

## Manual GC Operations

### Dry Run

See what would be deleted without actually deleting:

```rust
use arco_catalog::gc::{GarbageCollector, RetentionPolicy};

let collector = GarbageCollector::new(storage, RetentionPolicy::default());
let report = collector.collect_dry_run().await?;

println!("Would delete {} objects", report.objects_to_delete);
println!("Orphaned snapshots: {:?}", report.orphaned_snapshots);
println!("Old ledger events: {} files", report.old_ledger_events.len());
println!("Old snapshot versions: {:?}", report.old_snapshot_versions);
```

### Force GC with Custom Policy

```rust
let policy = RetentionPolicy {
    keep_snapshots: 5,      // Keep last 5 versions
    delay_hours: 1,         // 1 hour delay (aggressive)
    ledger_retention_hours: 24,  // 24h ledger retention
    max_age_days: 30,       // 30 day max
};

let collector = GarbageCollector::new(storage, policy);
let result = collector.collect().await?;
```

## Retention Policy Guidelines

| Workload | keep_snapshots | delay_hours | ledger_retention_hours | max_age_days |
|----------|----------------|-------------|------------------------|--------------|
| Development | 3 | 1 | 2 | 7 |
| Production (default) | 10 | 24 | 48 | 90 |
| Compliance-heavy | 30 | 48 | 168 | 365 |

## Recovery Procedures

### Accidental Deletion Recovery

If GC accidentally deletes needed data:

1. **Stop GC immediately** - Scale down compactor
2. **Check GCS versioning** - If enabled, objects can be restored:
   ```bash
   gsutil ls -a gs://BUCKET/tenant=TENANT/workspace=WORKSPACE/snapshots/
   gsutil cp gs://BUCKET/path/to/file#generation ./recovered_file
   ```
3. **Restore from manifest backup** - Commits directory has audit trail
4. **Re-compact if needed** - Ledger events may need re-processing

### Storage Runaway Prevention

If storage is growing too fast:

1. **Check for GC failures** - Ensure GC is running successfully
2. **Reduce retention** - Consider more aggressive policy
3. **Investigate data patterns** - High write volume may need partitioning
4. **Check for orphaned data** - Run dry-run to identify cleanup opportunities

## Monitoring Setup

### Recommended Alerts

```yaml
# Alert: GC not running
- alert: ArcoGCNotRunning
  expr: time() - max(arco_gc_run_completed_timestamp) > 86400
  for: 1h
  labels:
    severity: warning
  annotations:
    summary: "GC has not run in 24 hours"

# Alert: GC errors
- alert: ArcoGCErrors
  expr: increase(arco_gc_errors_total[1h]) > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "GC encountered errors"

# Alert: Storage growth
- alert: ArcoStorageGrowth
  expr: rate(gcs_bucket_size_bytes{bucket="arco-catalog"}[24h]) > 1e9
  for: 1h
  labels:
    severity: warning
  annotations:
    summary: "Catalog storage growing > 1GB/day"
```

## Escalation

If issues persist after following this runbook:

1. **Check recent deployments** - Any changes to compactor or GC code?
2. **Review cloud provider status** - Storage API issues?
3. **Escalate to platform team** - May need manual intervention

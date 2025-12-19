# Runbook: Storage Integrity Verification

## Overview

This runbook covers running storage integrity checks against live Arco workspaces. The `cargo xtask verify-integrity` command validates:

- **Root manifest**: Exists and points to valid domain manifests
- **Domain manifests**: Catalog, lineage, executions, search manifests are well-formed
- **Commit chain**: All commit records link correctly back to genesis
- **Snapshot files**: Checksums match, sizes are correct, no missing files
- **Lock freshness**: No stale/expired locks blocking operations

## Quick Reference

| Check | What It Validates | Severity If Failed |
|-------|-------------------|-------------------|
| Root manifest | Entry point exists | Critical |
| Domain manifests | All 4 domains readable | Critical |
| Commit chain | Hash chain integrity | Critical |
| Snapshot files | Data integrity | Critical |
| Lock freshness | No stale locks | Warning (< 1h), Error (> 1h) |

## Running Integrity Checks

### Prerequisites

1. **Credentials**: Ensure you have access to the target bucket
   ```bash
   # GCS
   gcloud auth application-default login

   # AWS
   aws configure
   ```

2. **Environment**: Set the storage bucket (or pass `--bucket`)
   ```bash
   export ARCO_STORAGE_BUCKET=gs://my-arco-bucket
   # or
   export ARCO_STORAGE_BUCKET=s3://my-arco-bucket
   ```

### Repo-Only Checks (CI Mode)

These checks run without storage access and validate local artifacts:

```bash
cargo xtask verify-integrity
```

This validates:
- Golden schema files exist and are well-formed
- ADR-006 (schema evolution policy) exists
- Schema contract tests exist and pass
- PR template has required invariant checklist
- CatalogPaths module exists

### Full Storage Checks

Run against a specific tenant/workspace:

```bash
cargo xtask verify-integrity \
  --tenant=acme-corp \
  --workspace=production \
  --bucket=gs://arco-production
```

Or using environment variable:
```bash
export ARCO_STORAGE_BUCKET=gs://arco-production
cargo xtask verify-integrity --tenant=acme-corp --workspace=production
```

### Verbose Mode

Show detailed output including active locks and commit counts:

```bash
cargo xtask verify-integrity \
  --tenant=acme-corp \
  --workspace=production \
  --verbose
```

### Dry Run Mode

Report issues without failing (useful for dashboards):

```bash
cargo xtask verify-integrity \
  --tenant=acme-corp \
  --workspace=production \
  --dry-run
```

## Understanding Results

### Successful Run

```
Verifying catalog integrity...

=== Golden Schema Files ===
  namespaces.schema.json... [ok]
  tables.schema.json... [ok]
  columns.schema.json... [ok]
  lineage_edges.schema.json... [ok]

=== Workspace Integrity ===
  Scope: tenant=acme-corp, workspace=production
  Bucket: gs://arco-production
  Root manifest... [ok]
  Domain manifests... [ok]
  Commit chain (catalog)... [ok] 47 commits
  Catalog snapshot... [ok]
  Lineage snapshot... [ok]
  Executions state... [ok]
  Search state... [ok]
  Lock freshness... [ok]

All integrity checks passed!
```

### Common Failures

#### 1. Root Manifest Missing

```
Root manifest... [FAIL]
Errors:
  - Root manifest: Failed to read 'manifests/root.manifest.json'
```

**Cause**: Workspace not initialized or bucket path incorrect.

**Resolution**:
```bash
# Verify path exists
gsutil ls gs://BUCKET/tenant=TENANT/workspace=WORKSPACE/manifests/

# If missing, workspace needs initialization
# Contact platform team for workspace bootstrap
```

#### 2. Commit Chain Broken

```
Commit chain (catalog)... [FAIL]
Errors:
  - Commit record missing: commits/catalog/abc123.json
  - Commit hash mismatch for 'commits/catalog/def456.json': expected sha256:xxx, got sha256:yyy
```

**Cause**: Storage corruption, incomplete write, or manual tampering.

**Resolution**:
1. Check if commit file exists but is corrupted:
   ```bash
   gsutil cat gs://BUCKET/.../commits/catalog/abc123.json
   ```
2. If missing, check GCS versioning for recovery
3. If hash mismatch, data may be corrupted - escalate to platform team

#### 3. Snapshot Checksum Mismatch

```
Catalog snapshot... [FAIL]
Errors:
  - Snapshot checksum mismatch for 'snapshots/catalog/v47/namespaces.parquet':
    manifest abc123, actual def456
```

**Cause**: File corruption, incomplete upload, or storage backend issue.

**Resolution**:
1. Try re-downloading the file to verify:
   ```bash
   gsutil cp gs://BUCKET/.../snapshots/catalog/v47/namespaces.parquet /tmp/
   sha256sum /tmp/namespaces.parquet
   ```
2. Check if previous snapshot version is healthy
3. May need to re-compact from ledger events

#### 4. Stale Locks

```
Lock freshness... [FAIL]
Errors:
  - Lock 'locks/catalog.lock.json' expired 3h 45m ago (stale - consider cleanup)
```

**Cause**: Process crashed while holding lock, very old debris.

**Resolution**:
```bash
# View lock contents
gsutil cat gs://BUCKET/.../locks/catalog.lock.json

# If safe to clean up (no active operations)
gsutil rm gs://BUCKET/.../locks/catalog.lock.json
```

#### 5. Recently Expired Lock (Warning)

```
Warnings:
  - Lock 'locks/catalog.lock.json' expired 5m ago (likely crashed process)
```

**Cause**: Recent process crash, normal operational debris.

**Resolution**: Usually no action needed. TTL cleanup or next writer will take over. Only investigate if locks consistently appear.

## Scheduled Verification

### Recommended Schedule

| Environment | Frequency | Mode |
|-------------|-----------|------|
| Development | On-demand | Full |
| Staging | Daily | Full with --verbose |
| Production | Every 6 hours | Full |

### Automation Example

```bash
#!/bin/bash
# storage-integrity-check.sh

set -e

BUCKET="${ARCO_STORAGE_BUCKET:-gs://arco-production}"
TENANTS=("acme-corp" "beta-inc")
WORKSPACES=("production" "staging")

for tenant in "${TENANTS[@]}"; do
  for workspace in "${WORKSPACES[@]}"; do
    echo "Checking $tenant/$workspace..."
    cargo xtask verify-integrity \
      --tenant="$tenant" \
      --workspace="$workspace" \
      --bucket="$BUCKET" \
      || echo "FAILED: $tenant/$workspace"
  done
done
```

### Cloud Scheduler (GCP)

```yaml
# cloud-scheduler-job.yaml
name: arco-integrity-check
schedule: "0 */6 * * *"  # Every 6 hours
httpTarget:
  uri: https://arco-api.example.com/internal/integrity-check
  httpMethod: POST
```

## CI Integration

The CI pipeline runs repo-only checks in the Gates job:

```yaml
# .github/workflows/ci.yml
- run: cargo xtask verify-integrity
```

Storage checks require credentials and are not run in CI. Use scheduled jobs or manual verification for storage integrity.

## Escalation

If integrity issues cannot be resolved:

1. **Gather diagnostics**:
   ```bash
   cargo xtask verify-integrity --tenant=X --workspace=Y --verbose 2>&1 | tee integrity-report.txt
   ```

2. **Check recent changes**: Any deployments, migrations, or manual operations?

3. **Check cloud provider status**: Storage API issues?

4. **Escalate to platform team** with:
   - Full integrity report
   - Tenant/workspace affected
   - Timeline of when issue was detected
   - Any recent operational changes

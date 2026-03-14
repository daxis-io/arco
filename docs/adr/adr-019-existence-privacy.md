# ADR-019: Existence Privacy (Posture B)

## Status

Accepted

## Context

Gate 5 security requirements include a "no-regrets" security posture that addresses
existence privacy - ensuring users cannot discover the existence of assets they are
not authorized to access. The current system (Posture A) has:

- Asset names visible in manifests and indexes
- Search tokens are plaintext
- Signed URLs issued without correlation tracking

This ADR documents the path to Posture B (existence privacy) without blocking the
current launch.

### Problem Statement

In a multi-tenant system where different users have different access levels:
1. A user should not be able to enumerate assets they cannot access
2. Search should not reveal asset existence to unauthorized users
3. Manifest structure should not leak sensitive naming patterns
4. Audit trails must correlate access attempts with user identity

## Decision

**Option 1: Partitioned Snapshots + IAM**

This aligns with the unified platform design which specifies:
> "Partitioned snapshots with IAM-scoped read access per tenant/access-level."

### Architecture

```
snapshots/{domain}/v{version}/
├── partition=public/
│   ├── namespaces.parquet
│   └── tables.parquet
├── partition=internal/
│   ├── namespaces.parquet
│   └── tables.parquet
└── partition=confidential/
    ├── namespaces.parquet
    └── tables.parquet
```

### IAM Conditions

Each access level partition has IAM conditions restricting read access:

```hcl
resource "google_storage_bucket_iam_member" "reader_internal" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectViewer"
  member = "group:internal-users@example.com"

  condition {
    title       = "InternalPartitionAccess"
    description = "Users with internal access can read public and internal partitions"
    expression  = <<-EOT
      resource.name.extract("partition={level}/") == "public" ||
      resource.name.extract("partition={level}/") == "internal"
    EOT
  }
}
```

### Search Index Tokens

Search tokens use salted hashes to prevent enumeration:

```
token = sha256(salt + asset_name + tenant_id + access_level)
```

Search results are filtered server-side based on user's access level before
returning matches.

### Signed URL Auditing

Every signed URL includes correlation metadata:

```rust
struct SignedUrlMetadata {
    /// Correlation ID for audit trail
    correlation_id: Ulid,
    /// User identity from auth context
    user_id: String,
    /// Resource being accessed
    resource_path: String,
    /// Access level required
    access_level: AccessLevel,
    /// Expiry timestamp
    expires_at: DateTime<Utc>,
}
```

Audit log entry created at URL generation time, not at access time.

## Alternatives Considered

### Option 2: Auth Service Materializing Allowed Views

A central auth service filters manifests based on user permissions at query time.

**Pros:**
- Single source of truth for authorization
- No storage duplication

**Cons:**
- Added latency on every manifest read
- Auth service availability becomes critical path
- Complex integration with existing auth infrastructure
- Harder to cache results

**Rejected** because it adds runtime dependencies and latency to the read path.

### Option 3: Encrypted Manifests + Key Distribution

Encrypt sensitive fields in manifests, distribute decryption keys per access level.

**Pros:**
- No server-side filtering needed
- Strong cryptographic guarantees

**Cons:**
- Key management complexity (rotation, distribution, revocation)
- Key hierarchy adds operational burden
- Performance overhead from decryption
- Harder to implement incremental access changes

**Rejected** because key management complexity outweighs benefits for our use case.

## Consequences

### Positive

- No runtime auth service dependency for read path
- Leverages existing GCS IAM infrastructure
- Clear partition boundaries for data governance
- Audit trail at URL generation provides early detection

### Negative

- Some storage duplication (assets may appear in multiple partitions)
- Partition scheme must be defined upfront (changing requires migration)
- Search index size increases with salted tokens

### Acceptable Trade-offs

Storage duplication is acceptable because:
- Parquet files compress well
- Most assets belong to a single access level
- GCS storage is inexpensive compared to compute costs of runtime filtering

## Migration Path

### Phase 1: Current (No Existence Privacy)

Focus on core functionality. All users with workspace access see all assets.

### Phase 2: Partition Infrastructure

1. Add `access_level` column to domain events
2. Modify compactor to write partitioned snapshots
3. Update manifest schema to reference partitioned paths

### Phase 3: IAM Deployment

1. Create IAM conditions for partitioned access
2. Deploy updated IAM bindings
3. Verify with smoke tests

### Phase 4: Search Index Migration

1. Generate salted tokens for new assets
2. Backfill existing assets with salted tokens
3. Update search API to filter by access level
4. Deprecate plaintext search tokens

### Phase 5: Audit Enhancement

1. Add correlation ID to signed URL generation
2. Create audit log ingestion pipeline
3. Build alerting for anomalous access patterns

## Related ADRs

- ADR-018: Tier-1 Write Path Architecture (defines compactor as sole state writer)
- ADR-005: Storage Layout (defines prefix structure)

## References

- `docs/adr/adr-005-storage-layout.md`
- `docs/adr/adr-018-tier1-write-path.md`
- `docs/guide/src/concepts/catalog.md`

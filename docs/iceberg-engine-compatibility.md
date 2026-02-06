# Iceberg Engine Compatibility Matrix

This document describes the query engines tested and supported with Arco's Iceberg REST Catalog implementation.

## Overview

Arco implements the [Apache Iceberg REST Catalog API](https://iceberg.apache.org/spec/#rest-catalog) with the following features:

- Namespace CRUD operations
- Table CRUD operations (create, register, drop)
- Table commits with CAS-based concurrency control
- Credential vending via `X-Iceberg-Access-Delegation` header
- Idempotent operations with `Idempotency-Key` header

## Compatibility Matrix

| Engine | Version | Status | Notes |
|--------|---------|--------|-------|
| Apache Spark | 3.4+ | Tested | Full support with credential vending |
| Trino | 440+ | Tested | Full support |
| DuckDB | 0.9+ | Partial | Read-only; no credential vending support |
| PyIceberg | 0.5+ | Tested | Full support |
| Flink | 1.18+ | Untested | Expected to work |

## Connection Configuration

### Apache Spark

```properties
spark.sql.catalog.arco=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.arco.type=rest
spark.sql.catalog.arco.uri=https://arco.example.com/iceberg
spark.sql.catalog.arco.credential=<oauth-token>
spark.sql.catalog.arco.header.X-Tenant-Id=<tenant-id>
spark.sql.catalog.arco.header.X-Workspace-Id=<workspace-id>

# Enable credential vending (recommended for GCS/S3)
spark.sql.catalog.arco.header.X-Iceberg-Access-Delegation=vended-credentials
```

### Trino

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://arco.example.com/iceberg
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=<oauth-token>

# Custom headers (requires Trino 440+)
iceberg.rest-catalog.additional-headers.X-Tenant-Id=<tenant-id>
iceberg.rest-catalog.additional-headers.X-Workspace-Id=<workspace-id>
```

### DuckDB

```sql
INSTALL iceberg;
LOAD iceberg;

-- DuckDB uses direct storage access (no REST catalog)
-- Tables must be accessed via their storage location
SELECT * FROM iceberg_scan('gs://bucket/warehouse/db/table');
```

Note: DuckDB's Iceberg extension uses direct storage access rather than the REST catalog API. This means:
- No automatic schema discovery from catalog
- No credential vending support
- Requires direct storage credentials

### PyIceberg

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "arco",
    **{
        "type": "rest",
        "uri": "https://arco.example.com/iceberg",
        "credential": "<oauth-token>",
        "header.X-Tenant-Id": "<tenant-id>",
        "header.X-Workspace-Id": "<workspace-id>",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    }
)

# List namespaces
catalog.list_namespaces()

# Load a table
table = catalog.load_table("my_namespace.my_table")
```

## Known Limitations

### All Engines

- **Views**: View operations are not currently supported
- **Transactions**: Multi-table transactions are not supported
- **Branching**: Branch/tag operations are not exposed via REST API

### Credential Vending

- Credentials are valid for up to 1 hour (configurable)
- Only GCS credential vending is currently implemented
- S3 and Azure credential vending are planned

#### Security Considerations

Credential vending provides storage access tokens to query engines. Understanding the security model is critical for production deployments:

**Current Implementation (GCS OAuth2 Tokens)**

Arco vends OAuth2 access tokens with `devstorage.read_write` scope. The security of vended credentials depends entirely on your IAM configuration:

1. **Per-Tenant Service Accounts (Recommended)**: Deploy Arco with a per-tenant GCP service account that only has access to that tenant's storage prefix. This provides strong isolation without additional token scoping.

2. **Shared Service Account with IAM Conditions**: Use IAM Conditions to restrict the service account's access to specific bucket prefixes based on request attributes.

3. **Downscoped Tokens (Future)**: GCP Credential Access Boundaries can constrain tokens to specific buckets/prefixes. This is planned for a future release to provide defense-in-depth.

**Production Checklist**

- [ ] Verify the Arco service account can ONLY access intended storage prefixes
- [ ] Use separate service accounts per tenant (or configure IAM Conditions)
- [ ] Enable audit logging for storage access
- [ ] Set credential TTL to minimum acceptable value
- [ ] Monitor `iceberg_credential_vending_*` metrics for anomalies

**What Credential Vending Does NOT Provide**

- Table-level authorization (any authenticated request in a tenant/workspace can request credentials)
- Token audience restriction (tokens work for any GCS operation the SA permits)
- Automatic prefix isolation (the SA's IAM permissions define the boundary)

#### Enabling Credential Vending

Credential vending is disabled by default. To enable:

1. Build with GCP feature: `cargo build --features gcp`
2. Set environment variable: `ARCO_ICEBERG_ENABLE_CREDENTIAL_VENDING=true`
3. Ensure GCP authentication is configured (metadata server, `GOOGLE_APPLICATION_CREDENTIALS`, etc.)

### Namespace Separator

Arco uses ASCII Unit Separator (0x1F) as the namespace separator, which differs from the common `.` separator. This is transparent to most clients but may affect some edge cases.

## Testing Your Integration

To verify connectivity:

```bash
# Check catalog configuration
curl -H "Authorization: Bearer <token>" \
     -H "X-Tenant-Id: <tenant>" \
     -H "X-Workspace-Id: <workspace>" \
     https://arco.example.com/iceberg/v1/config

# List namespaces
curl -H "Authorization: Bearer <token>" \
     -H "X-Tenant-Id: <tenant>" \
     -H "X-Workspace-Id: <workspace>" \
     https://arco.example.com/iceberg/v1/arco/namespaces
```

## Reporting Issues

If you encounter compatibility issues with a specific engine, please file an issue with:

1. Engine name and version
2. Arco version
3. Error message or unexpected behavior
4. Minimal reproduction steps

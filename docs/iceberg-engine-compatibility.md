# Iceberg Engine Compatibility Matrix

This document describes the query engines tested and supported with Arco's Iceberg REST Catalog implementation.

## Overview

Arco implements the [Apache Iceberg REST Catalog API](https://iceberg.apache.org/spec/#rest-catalog) with the following features:

- Namespace CRUD operations
- Table CRUD operations (create, register, drop)
- Table commits with CAS-based concurrency control
- Credential vending negotiation via `X-Iceberg-Access-Delegation` header
- Idempotent operations with `Idempotency-Key` header

## Compatibility Matrix

| Engine | Version | Status | Notes |
|--------|---------|--------|-------|
| Apache Spark | 3.4+ | Tested | Full support without storage credential vending |
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

# Request credential vending when downscoped provider credentials are available
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
- GCS credential vending is currently fail-closed until downscoped credentials are implemented
- S3 and Azure credential vending are planned

#### Security Considerations

Credential vending provides storage access tokens to query engines. Understanding the security model is critical for production deployments:

**Current Implementation (GCS Downscoped Credentials Required)**

Arco does not vend raw platform OAuth2 access tokens. GCS credential vending returns a service-unavailable error until enforceable downscoped credentials are implemented for the requested table prefix.

1. **Downscoped Tokens (Required)**: GCP Credential Access Boundaries must constrain tokens to the exact governed bucket/prefix and operation mode before Arco can return GCS credentials.

2. **Per-Tenant Service Accounts**: Per-tenant GCP service accounts or IAM Conditions can reduce blast radius for Arco itself, but they are not a substitute for downscoped credentials in the vending path.

3. **Shared Service Account with IAM Conditions**: A shared service account must still be paired with enforceable per-request credential scoping before vending is enabled.

**Production Checklist**

- [ ] Keep credential vending disabled until downscoped provider credentials are available
- [ ] Verify the Arco service account can ONLY access intended storage prefixes
- [ ] Use separate service accounts per tenant or configure IAM Conditions as defense in depth
- [ ] Enable audit logging for storage access
- [ ] Set credential TTL to minimum acceptable value
- [ ] Monitor `iceberg_credential_vending_*` metrics for anomalies

**What Credential Vending Does NOT Provide**

- Table-level authorization (any authenticated request in a tenant/workspace can request credentials)
- Raw platform OAuth2 token vending
- Automatic prefix isolation without downscoped provider credentials

#### Enabling Credential Vending

Credential vending is disabled by default. To enable:

1. Build with GCP feature: `cargo build --features gcp`
2. Set environment variable: `ARCO_ICEBERG_ENABLE_CREDENTIAL_VENDING=true`
3. Configure a downscoped credential implementation before returning provider credentials to clients

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

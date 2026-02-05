# Iceberg REST Catalog Phase A - Implementation Plan

**Status:** In Progress
**Date:** 2025-12-22
**Design Doc:** [2025-12-22-iceberg-rest-integration-design.md](./2025-12-22-iceberg-rest-integration-design.md)

## Scope

Phase A implements the read-only foundation for the Iceberg REST Catalog:
- `/v1/config` endpoint with full spec compliance
- Namespace operations (list, get, HEAD)
- Table operations (list, load with ETag, HEAD)
- Credential vending with X-Iceberg-Access-Delegation

---

## Batch 1: Foundation & Types

### Task 1.1: Create arco-iceberg crate structure
**TDD: Write test that imports crate and verifies module structure**

Steps:
1. Create `crates/arco-iceberg/Cargo.toml` with dependencies
2. Create `crates/arco-iceberg/src/lib.rs` with module declarations
3. Add to workspace in root `Cargo.toml`

Files:
- `crates/arco-iceberg/Cargo.toml`
- `crates/arco-iceberg/src/lib.rs`
- `Cargo.toml` (workspace update)

### Task 1.2: Define Iceberg REST types (request/response structs)
**TDD: Write serde roundtrip tests for all types**

Types needed (from OpenAPI spec):
- `ConfigResponse` (for /v1/config)
- `ListNamespacesResponse`
- `GetNamespaceResponse`
- `ListTablesResponse`
- `LoadTableResponse`
- `IcebergErrorResponse`
- `StorageCredential`

Steps:
1. Write test: deserialize sample JSON → struct → serialize → matches original
2. Create `src/types/config.rs` with `ConfigResponse`
3. Create `src/types/namespace.rs` with namespace types
4. Create `src/types/table.rs` with table types
5. Create `src/types/error.rs` with `IcebergErrorResponse`
6. Create `src/types/credentials.rs` with credential types
7. Create `src/types/mod.rs` to re-export all

Files:
- `crates/arco-iceberg/src/types/*.rs`
- `crates/arco-iceberg/src/types/mod.rs`

### Task 1.3: Define IcebergTablePointer struct
**TDD: Write serde tests + version compatibility test**

From design doc Section 2.1:
```rust
pub struct IcebergTablePointer {
    pub version: u32,
    pub table_uuid: Uuid,
    pub current_metadata_location: String,
    pub current_snapshot_id: Option<i64>,
    pub refs: HashMap<String, SnapshotRef>,
    pub last_sequence_number: i64,
    pub previous_metadata_location: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub updated_by: UpdateSource,
}
```

Steps:
1. Write test: pointer serialization/deserialization
2. Write test: version field validation
3. Create `src/pointer.rs` with struct + impl

Files:
- `crates/arco-iceberg/src/pointer.rs`

---

## Batch 2: Iceberg Error Handling

### Task 2.1: Create IcebergError enum with HTTP mapping
**TDD: Write tests for error → HTTP status code mapping**

Error types from design doc Section 7:
- `BadRequest` → 400
- `Unauthorized` → 401
- `Forbidden` → 403
- `NotFound` → 404
- `Conflict` → 409
- `ServiceUnavailable` → 503 (with Retry-After)
- `InternalError` → 500

Steps:
1. Write test: each error variant maps to correct status code
2. Write test: IcebergErrorResponse serialization matches spec
3. Create `src/error.rs` with `IcebergError` enum
4. Implement `IntoResponse` for Axum compatibility

Files:
- `crates/arco-iceberg/src/error.rs`

### Task 2.2: Implement From<CatalogError> for IcebergError
**TDD: Write tests mapping catalog errors to Iceberg errors**

Steps:
1. Write test: CatalogError::NotFound → IcebergError::NotFound
2. Write test: CatalogError::AlreadyExists → IcebergError::Conflict
3. Implement `From<CatalogError>` trait

Files:
- `crates/arco-iceberg/src/error.rs` (update)

---

## Batch 3: /v1/config Endpoint

### Task 3.1: Implement /v1/config handler
**TDD: Write integration test for /v1/config response**

From design doc Section 3.6:
```json
{
  "defaults": {},
  "overrides": {
    "prefix": "arco",
    "namespace-separator": "%1F"
  },
  "idempotency-key-lifetime": "PT1H",
  "endpoints": [...]
}
```

Steps:
1. Write test: GET /v1/config returns correct JSON structure
2. Write test: response includes all required fields
3. Write test: endpoints list matches supported operations
4. Create `src/routes/config.rs` with handler
5. Add route to router

Files:
- `crates/arco-iceberg/src/routes/config.rs`
- `crates/arco-iceberg/src/routes/mod.rs`

### Task 3.2: Create Iceberg router with /v1 prefix
**TDD: Write test that router mounts correctly**

Steps:
1. Write test: /v1/config is accessible
2. Write test: /iceberg/v1/config is accessible (when nested)
3. Create `src/router.rs` with router builder
4. Integrate with arco-api's main router

Files:
- `crates/arco-iceberg/src/router.rs`
- `crates/arco-api/src/server.rs` (integration)

---

## Batch 4: Namespace Read Operations

### Task 4.1: Implement list namespaces handler
**TDD: Write tests for namespace listing with pagination**

Endpoint: `GET /v1/{prefix}/namespaces`

Steps:
1. Write test: list returns namespaces from catalog
2. Write test: pagination with `pageToken` and `pageSize`
3. Write test: parent namespace filtering (if supported)
4. Write test: empty catalog returns empty list
5. Create `src/routes/namespaces.rs` with list handler

Files:
- `crates/arco-iceberg/src/routes/namespaces.rs`

### Task 4.2: Implement get namespace handler
**TDD: Write tests for namespace retrieval**

Endpoint: `GET /v1/{prefix}/namespaces/{namespace}`

Steps:
1. Write test: get existing namespace returns properties
2. Write test: get non-existent namespace returns 404
3. Write test: namespace identifier is URL-decoded correctly
4. Write test: nested namespaces decode separator correctly (%1F)
5. Add get handler to `src/routes/namespaces.rs`

Files:
- `crates/arco-iceberg/src/routes/namespaces.rs` (update)

### Task 4.3: Implement HEAD namespace handler
**TDD: Write tests for namespace existence check**

Endpoint: `HEAD /v1/{prefix}/namespaces/{namespace}`

Steps:
1. Write test: HEAD existing namespace returns 200
2. Write test: HEAD non-existent namespace returns 404
3. Write test: response has no body
4. Add HEAD handler

Files:
- `crates/arco-iceberg/src/routes/namespaces.rs` (update)

---

## Batch 5: Table Read Operations

### Task 5.1: Implement list tables handler
**TDD: Write tests for table listing**

Endpoint: `GET /v1/{prefix}/namespaces/{namespace}/tables`

Steps:
1. Write test: list returns tables with format=ICEBERG
2. Write test: tables without format=ICEBERG are excluded
3. Write test: pagination support
4. Write test: namespace not found returns 404
5. Create `src/routes/tables.rs` with list handler

Files:
- `crates/arco-iceberg/src/routes/tables.rs`

### Task 5.2: Implement IcebergPointerStore
**TDD: Write tests for pointer CRUD operations**

Operations needed:
- `get(table_uuid) -> (IcebergTablePointer, ObjectVersion)`
- `exists(table_uuid) -> bool`

Steps:
1. Write test: get existing pointer returns pointer + version
2. Write test: get missing pointer returns NotFound
3. Write test: exists returns true/false correctly
4. Create `src/pointer_store.rs` with PointerStore trait + impl

Files:
- `crates/arco-iceberg/src/pointer_store.rs`

### Task 5.3: Implement load table handler (without credentials)
**TDD: Write tests for table loading**

Endpoint: `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`

Steps:
1. Write test: load returns Iceberg metadata from pointer
2. Write test: load non-existent table returns 404
3. Write test: ETag header is set to pointer version
4. Write test: If-None-Match returns 304 when matched
5. Write test: response includes metadata-location
6. Add load handler to `src/routes/tables.rs`

Files:
- `crates/arco-iceberg/src/routes/tables.rs` (update)

### Task 5.4: Implement HEAD table handler
**TDD: Write tests for table existence check**

Endpoint: `HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}`

Steps:
1. Write test: HEAD existing table returns 200 + ETag
2. Write test: HEAD non-existent table returns 404
3. Write test: response has no body
4. Add HEAD handler

Files:
- `crates/arco-iceberg/src/routes/tables.rs` (update)

---

## Batch 6: Credential Vending

### Task 6.1: Implement credential provider trait
**TDD: Write tests for credential generation**

Steps:
1. Write test: credential provider generates valid credentials
2. Write test: credentials include correct prefix
3. Write test: credentials have expiry time
4. Create `src/credentials.rs` with CredentialProvider trait
5. Create mock provider for testing

Files:
- `crates/arco-iceberg/src/credentials.rs`

### Task 6.2: Implement X-Iceberg-Access-Delegation handling
**TDD: Write tests for header parsing and response**

Steps:
1. Write test: no header → no credentials in response
2. Write test: `vended-credentials` → credentials included
3. Write test: `remote-signing` → 400 not supported
4. Write test: credentials appear in LoadTableResponse
5. Update load table handler to check header and include credentials

Files:
- `crates/arco-iceberg/src/routes/tables.rs` (update)

### Task 6.3: Implement /credentials endpoint
**TDD: Write tests for dedicated credentials endpoint**

Endpoint: `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials`

Steps:
1. Write test: returns fresh credentials for table
2. Write test: 404 for non-existent table
3. Write test: credentials match LoadTableResponse format
4. Add credentials handler

Files:
- `crates/arco-iceberg/src/routes/tables.rs` (update)

---

## Batch 7: Namespace Identifier Handling

### Task 7.1: Implement namespace identifier codec
**TDD: Write tests for encoding/decoding with separators**

From design doc:
- Namespace separator: `%1F` (unit separator, URL-encoded)
- Must accept both configured separator AND `%1F`

Steps:
1. Write test: encode single-level namespace
2. Write test: encode multi-level namespace with %1F
3. Write test: decode URL-encoded namespace
4. Write test: decode handles both separator variants
5. Create `src/namespace_id.rs` with encoder/decoder

Files:
- `crates/arco-iceberg/src/namespace_id.rs`

---

## Batch 8: Integration with arco-api

### Task 8.1: Wire Iceberg router into main API
**TDD: Write end-to-end test through main API**

Steps:
1. Write test: /iceberg/v1/config accessible through main API
2. Write test: auth middleware applies to Iceberg routes
3. Write test: rate limiting applies to Iceberg routes
4. Update arco-api server to mount Iceberg router

Files:
- `crates/arco-api/src/server.rs` (update)
- `crates/arco-api/src/routes/mod.rs` (optional: re-export)

### Task 8.2: Add OpenAPI documentation
**TDD: Validate OpenAPI spec is generated correctly**

Steps:
1. Add utoipa annotations to all handlers
2. Add utoipa annotations to all types
3. Write test: OpenAPI spec includes /iceberg/v1/* paths
4. Write test: OpenAPI spec matches Iceberg REST spec

Files:
- `crates/arco-iceberg/src/routes/*.rs` (annotations)
- `crates/arco-iceberg/src/types/*.rs` (annotations)

---

## Batch 9: DataFusion Integration Tests

### Task 9.1: Create test fixtures with Iceberg metadata
**Setup: Create realistic Iceberg metadata files for testing**

Steps:
1. Create test fixture with valid Iceberg metadata JSON
2. Create test fixture with pointer file
3. Create test fixture representing a table with snapshots
4. Place fixtures in `crates/arco-iceberg/tests/fixtures/`

Files:
- `crates/arco-iceberg/tests/fixtures/metadata/*.json`
- `crates/arco-iceberg/tests/fixtures/pointers/*.json`

### Task 9.2: Write integration test with iceberg-rust REST client
**Test: Verify DataFusion can connect and read catalog**

Steps:
1. Set up test server with fixtures
2. Configure iceberg-rust REST catalog client
3. Write test: list namespaces via client
4. Write test: list tables via client
5. Write test: load table metadata via client

Files:
- `crates/arco-iceberg/tests/datafusion_integration.rs`

### Task 9.3: Write credential vending integration test
**Test: Verify credentials work with storage access**

Steps:
1. Write test: vended credentials can access storage
2. Write test: expired credentials are rejected
3. Write test: wrong prefix credentials are rejected

Files:
- `crates/arco-iceberg/tests/credentials_integration.rs`

---

## Verification Checklist

Before completing Phase A:
- [ ] All tests pass (`cargo test --workspace`)
- [ ] No clippy warnings (`cargo clippy --workspace -- -D warnings`)
- [ ] OpenAPI spec generated and validated
- [ ] DataFusion integration test passes
- [ ] Documentation complete for public APIs
- [ ] ETag/If-None-Match behavior verified
- [ ] Credential vending verified with mock provider

---

## Notes

### Test Fixture Strategy
For Phase A (read-only), we need pre-existing Iceberg tables. The test fixtures approach:
1. Create realistic Iceberg metadata files (JSON)
2. Create pointer files pointing to those metadata files
3. Create Arco catalog entries with `format = ICEBERG`
4. Tests populate storage with these fixtures before each test

### Dependency on Arco Catalog
Phase A reads from the existing Arco catalog (namespaces, tables) and maps them to Iceberg format. Tables must have `format = ICEBERG` to be visible through the Iceberg REST API.

### Future Phases
- **Phase B**: Will add write operations (create, commit, drop)
- **Phase C**: Will add reconciliation and GC
- **Phase D**: Will add Servo integration

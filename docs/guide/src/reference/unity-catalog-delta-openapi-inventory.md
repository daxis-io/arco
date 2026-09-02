# Unity Catalog Delta OpenAPI Endpoint Inventory (Pinned)

**Spec SHA256:** `d5f51a8a282d95cd25eb2930d5d43825aede32fcd9b9d51ce05e376d7b433c5e`  
**Spec fixture:** `crates/arco-uc/tests/fixtures/unitycatalog-delta-openapi.yaml`  
**Spec title:** UC Delta API  
**Spec version:** 1.0  
**Servers:**
- `{scheme}://{host}:{port}/api/2.1/unity-catalog`
- `http://localhost:8080/api/2.1/unity-catalog`

<!-- BEGIN GENERATED -->

## DeltaConfiguration

- `GET /delta/v1/config` _(operationId: `getConfig`)_ — Get catalog configuration

## DeltaTables

- `POST /delta/v1/catalogs/{catalog}/schemas/{schema}/staging-tables` _(operationId: `createStagingTable`)_ — Create a staging table
- `POST /delta/v1/catalogs/{catalog}/schemas/{schema}/tables` _(operationId: `createTable`)_ — Create a table
- `DELETE /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}` _(operationId: `deleteTable`)_ — Delete a table
- `GET /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}` _(operationId: `loadTable`)_ — Load table metadata
- `HEAD /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}` _(operationId: `tableExists`)_ — Check if table exists
- `POST /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}` _(operationId: `updateTable`)_ — Update table
- `POST /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/metrics` _(operationId: `reportMetrics`)_ — Report commit metrics
- `POST /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/rename` _(operationId: `renameTable`)_ — Rename a table

## DeltaTemporaryCredentials

- `GET /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials` _(operationId: `getTableCredentials`)_ — Get table credentials
- `GET /delta/v1/staging-tables/{table_id}/credentials` _(operationId: `getStagingTableCredentials`)_ — Get staging table credentials by UUID
- `GET /delta/v1/temporary-path-credentials` _(operationId: `getTemporaryPathCredentials`)_ — Get temporary path credentials

<!-- END GENERATED -->

## Manual annotations

<!-- BEGIN MANUAL -->

<!-- END MANUAL -->

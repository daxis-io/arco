# Unity Catalog OSS OpenAPI Endpoint Inventory (Pinned)

**Spec SHA256:** `129c4155257f89d599e03833fffc9d2534432307a3497df3921ecdebfe5d7f15`  
**Spec fixture:** `crates/arco-uc/tests/fixtures/unitycatalog-openapi.yaml`  
**Spec title:** Unity Catalog API  
**Spec version:** 0.1  
**Servers:**
- `http://localhost:8080/api/2.1/unity-catalog`

<!-- BEGIN GENERATED -->

## Catalogs

- `GET /catalogs` _(operationId: `listCatalogs`)_ — List catalogs
- `POST /catalogs` _(operationId: `createCatalog`)_ — Create a catalog
- `DELETE /catalogs/{name}` _(operationId: `deleteCatalog`)_ — Delete a catalog
- `GET /catalogs/{name}` _(operationId: `getCatalog`)_ — Get a catalog
- `PATCH /catalogs/{name}` _(operationId: `updateCatalog`)_ — Update a catalog

## Credentials

- `GET /credentials` _(operationId: `listCredentials`)_ — List credentials
- `POST /credentials` _(operationId: `createCredential`)_ — Create a credential
- `DELETE /credentials/{name}` _(operationId: `deleteCredential`)_ — Delete a credential
- `GET /credentials/{name}` _(operationId: `getCredential`)_ — Get a credential
- `PATCH /credentials/{name}` _(operationId: `updateCredential`)_ — Update a credential

## DeltaCommits

- `GET /delta/preview/commits` _(operationId: `getCommits`)_ — List unbackfilled Delta table commits.
WARNING: This API is experimental and may change in future versions.

- `POST /delta/preview/commits` _(operationId: `commit`)_ — Commit changes to a specified Delta table.
The server has a limit defined in config on how many unbackfilled commits it can hold.
Clients are expected to do active backfill of the commit after committing to UC. So in
most cases the number of unbackfilled commits should be close to zero or one. But if
clients misbehave and unbackfilled commits accumulate beyond the limit, server will
reject further commits until more backfill is done.
WARNING: This API is experimental and may change in future versions.


## External Locations

- `GET /external-locations` _(operationId: `listExternalLocations`)_ — List external locations
- `POST /external-locations` _(operationId: `createExternalLocation`)_ — Create an external location
- `DELETE /external-locations/{name}` _(operationId: `deleteExternalLocation`)_ — Delete an external location
- `GET /external-locations/{name}` _(operationId: `getExternalLocation`)_ — Get an external location
- `PATCH /external-locations/{name}` _(operationId: `updateExternalLocation`)_ — Update an external location

## Functions

- `GET /functions` _(operationId: `listFunctions`)_ — List functions
- `POST /functions` _(operationId: `createFunction`)_ — Create a function.
WARNING: This API is experimental and will change in future versions.

- `DELETE /functions/{name}` _(operationId: `deleteFunction`)_ — Delete a function
- `GET /functions/{name}` _(operationId: `getFunction`)_ — Get a function

## Grants

- `GET /permissions/{securable_type}/{full_name}` _(operationId: `get`)_ — Get permissions
- `PATCH /permissions/{securable_type}/{full_name}` _(operationId: `update`)_ — Update a permission

## Metastores

- `GET /metastore_summary` _(operationId: `summary`)_ — Get metastore summary

## ModelVersions

- `POST /models/versions` _(operationId: `createModelVersion`)_ — Create a model version.

- `GET /models/{full_name}/versions` _(operationId: `listModelVersions`)_ — List model versions of the specified registered model.
- `DELETE /models/{full_name}/versions/{version}` _(operationId: `deleteModelVersion`)_ — Delete a model version
- `GET /models/{full_name}/versions/{version}` _(operationId: `getModelVersion`)_ — Get a model version
- `PATCH /models/{full_name}/versions/{version}` _(operationId: `updateModelVersion`)_ — Update a model version
- `PATCH /models/{full_name}/versions/{version}/finalize` _(operationId: `finalizeModelVersion`)_ — Finalize a model version

## RegisteredModels

- `GET /models` _(operationId: `listRegisteredModels`)_ — List models
- `POST /models` _(operationId: `createRegisteredModel`)_ — Create a model.
WARNING: This API is experimental and will change in future versions.

- `DELETE /models/{full_name}` _(operationId: `deleteRegisteredModel`)_ — Delete a specified registered model.
- `GET /models/{full_name}` _(operationId: `getRegisteredModel`)_ — Get a specified registered model
- `PATCH /models/{full_name}` _(operationId: `updateRegisteredModel`)_ — Update a registered model

## Schemas

- `GET /schemas` _(operationId: `listSchemas`)_ — List schemas
- `POST /schemas` _(operationId: `createSchema`)_ — Create a schema
- `DELETE /schemas/{full_name}` _(operationId: `deleteSchema`)_ — Delete a schema
- `GET /schemas/{full_name}` _(operationId: `getSchema`)_ — Get a schema
- `PATCH /schemas/{full_name}` _(operationId: `updateSchema`)_ — Update a schema

## Tables

- `POST /staging-tables` _(operationId: `createStagingTable`)_ — Create a staging table
- `GET /tables` _(operationId: `listTables`)_ — List tables
- `POST /tables` _(operationId: `createTable`)_ — Create a table. Only external table creation is supported.
WARNING: This API is experimental and will change in future versions.

- `DELETE /tables/{full_name}` _(operationId: `deleteTable`)_ — Delete a table
- `GET /tables/{full_name}` _(operationId: `getTable`)_ — Get a table

## TemporaryCredentials

- `POST /temporary-model-version-credentials` _(operationId: `generateTemporaryModelVersionCredentials`)_ — Generate temporary model version credentials. These credentials are used by clients to write and retrieve model artifacts from the model versions external storage location.
- `POST /temporary-path-credentials` _(operationId: `generateTemporaryPathCredentials`)_ — Generate temporary path credentials.
- `POST /temporary-table-credentials` _(operationId: `generateTemporaryTableCredentials`)_ — Generate temporary table credentials.
- `POST /temporary-volume-credentials` _(operationId: `generateTemporaryVolumeCredentials`)_ — Generate temporary volume credentials.

## Volumes

- `GET /volumes` _(operationId: `listVolumes`)_ — List Volumes
- `POST /volumes` _(operationId: `createVolume`)_ — Create a Volume
- `DELETE /volumes/{name}` _(operationId: `deleteVolume`)_ — Delete a Volume
- `GET /volumes/{name}` _(operationId: `getVolume`)_ — Get a Volume
- `PATCH /volumes/{name}` _(operationId: `updateVolume`)_ — Update a Volume

<!-- END GENERATED -->

## Manual annotations

<!-- BEGIN MANUAL -->

<!-- END MANUAL -->

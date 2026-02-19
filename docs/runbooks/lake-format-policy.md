# Runbook: Lake Format Policy (Delta-Primary Cutover)

## Status
Active (effective 2026-02-18)

## Policy
- Delta is the default and primary table format for new table creation across API and SDK surfaces.
- Iceberg remains supported for explicit and existing Iceberg tables.
- Legacy records without an explicit format continue to resolve to effective `parquet`.
- Net-new Iceberg feature scope is frozen in this cutover release train; compatibility and stability work remains in scope.

## Enforcement Points
- `POST /api/v1/namespaces/{namespace}/tables`: omitted `format` defaults to `delta`.
- `POST /api/v1/catalogs/{catalog}/schemas/{schema}/tables`: omitted `format` defaults to `delta`; unknown values are rejected with `400`.
- Delta commit endpoints enforce:
  - table exists in catalog metadata
  - effective table format is `delta`
  - `_delta_log` writes derive from table location root
- Python SDK `IoConfig.format` default is `delta`.

## Operational Checks
1. Verify default creation behavior:
   - `cargo test -p arco-api test_table_crud_lifecycle`
   - `cargo test -p arco-api test_register_table_in_schema_defaults_to_delta`
2. Verify format validation and canonicalization:
   - `cargo test -p arco-api test_register_table_rejects_unknown_format`
   - `cargo test -p arco-api test_register_table_in_schema_rejects_unknown_format`
   - `cargo test -p arco-catalog test_register_table_validates_and_canonicalizes_format`
3. Verify Delta commit gating and path derivation:
   - `cargo test -p arco-api test_delta_stage_rejects_missing_table`
   - `cargo test -p arco-api test_delta_stage_rejects_non_delta_table`
   - `cargo test -p arco-integration-tests --test delta_commit_coordinator`
4. Verify legacy fallback contract:
   - `cargo test -p arco-core --test table_format_contracts`

## Rollback Notes
- Rollback from Delta-primary defaults does not require data migration.
- If default behavior must be reverted, preserve read compatibility for:
  - explicit `delta|iceberg|parquet`
  - legacy `format = NULL` as effective `parquet`.

# Unity Catalog OSS OpenAPI Endpoint Inventory (Pinned)

> This inventory is mechanically derived from the pinned Unity Catalog OSS OpenAPI fixture.
> Run `cargo xtask uc-openapi-inventory` after updating
> `crates/arco-uc/tests/fixtures/unitycatalog-openapi.yaml`.

<!-- BEGIN GENERATED -->

_Generated inventory will appear here once the pinned UC spec fixture is populated._

<!-- END GENERATED -->

## Manual annotations

<!-- BEGIN MANUAL -->

### Starter set (from parity research packet Appendix A)

This list is intentionally a starter set until the pinned OpenAPI spec fixture is populated.

**Catalog objects**
- `/catalogs`
- `/schemas`
- `/tables`

**Permissions**
- `/permissions/{securable_type}/{full_name}`

**Temporary credentials**
- `POST /temporary-table-credentials`
- `POST /temporary-path-credentials`

**Delta commit coordination**
- `GET /delta/preview/commits`
- `POST /delta/preview/commits`

<!-- END MANUAL -->

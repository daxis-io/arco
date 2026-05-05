# Proto Hard-Cut Follow-Up Addendum: Slice 2

## Why this batch first

The remaining goals are not equal in blast radius:

- Catalog mutation noun cleanup is a public-contract fix with a contained set of proto, test, and adapter updates.
- Shrinking `arco.common.v1` further requires moving generated types across packages and touching more crates.
- Physically splitting orchestration read/callback models from event contracts is schema-file churn unless a later pass shows a clearly low-risk split.

This addendum narrows the next batch to the safest contract cleanup that still moves the hard-cut forward.

## Scope

1. Rename the remaining public catalog mutation fields to canonical public nouns while preserving field numbers:
   - `CreateCatalogOp.catalog`
   - `CreateSchemaOp.catalog`, `CreateSchemaOp.schema`
   - `RegisterTableOp.catalog`, `RegisterTableOp.schema`, `RegisterTableOp.table`
   - `UpdateTableOp.catalog`, `UpdateTableOp.schema`, `UpdateTableOp.table`
   - `DropTableOp.catalog`, `DropTableOp.schema`, `DropTableOp.table`
   - `RenameTableOp.catalog`, `RenameTableOp.schema`, `RenameTableOp.table`, `RenameTableOp.new_table`
2. Update the targeted proto/API tests and request-hash adapters that depend on those field names.
3. Preserve runtime behavior. Any adapter change must be limited to contract mapping and hashing.

## Explicit deferrals after this batch

- Revisit `arco.common.v1` shrink once the catalog mutation surface is stable and the remaining `TableFormat` / `FileEntry` consumers are isolated.
- Do not split `orchestration.proto` in this batch unless a later review shows the move is nearly mechanical and low churn.

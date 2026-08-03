> Status: Draft Phase 2 contract scaffold.
> Implementation status: Fixture-level conformance only.
> Architecture status: The control-store path is prototype-approved only, not accepted production architecture.
> Compatibility status: Root ownership is an internal control-plane contract.

# Root Ownership And IAM Contract

Root ownership prevents split-brain writes across the current authority path,
future control-store scopes, projections, exports, and table engines.

## Role Matrix

| Role | Owns | Must Not Own |
|---|---|---|
| API/control writer | mutation-visible control root for its active scope | public Parquet roots or table-format data roots |
| Projection compactor | projection roots and derived projection artifacts | CAS authority over mutation-visible control roots |
| Snapshot/export service | retained cuts, export manifests, and package objects | mutation-visible state |
| Query/enforcement service | pinned or published read cuts | control-plane writes or CAS authority |
| Table engine | table-format data through the table-owned protocol | catalog/control-plane mutation roots |
| External engine | external table-format writes allowed by table protocol and grants | catalog/control-plane mutation roots |

## Required Invariants

- Only one role may own CAS authority over a mutation-visible root.
- API/control writers must not write public Parquet projection roots.
- Projection compactors must not CAS mutation-visible control roots.
- Snapshot/export services must not own CAS authority, write control-plane
  state, or create mutation-visible state.
- Query/enforcement services must read pinned or published cuts and must not
  write control-plane state.
- Engines may write table-format data only through their table-owned protocol,
  not through catalog/control-plane mutation roots.
- Engine table-protocol writes are valid only on `table_data` roots.
- Engines must not write public Parquet projection roots or export roots.

## Root Classes

- `control`: mutation-visible root for catalog, metastore, orchestration, or
  another active authority scope.
- `projection`: derived state selected by a projection root or watermark.
- `public_parquet`: derived Parquet artifacts served to system tables,
  governance views, search, audit, or exports.
- `export`: retained package roots and export manifests.
- `table_data`: table-format data and log roots governed by the table protocol.

## Fixture Contract

The fixture at `fixtures/root-ownership-and-iam-v1.json` is the current
machine-checked draft. The conformance test fails if the required representative
role/root entries are missing, if a mutation-visible root has more than one CAS
authority, if the API/control writer can write public Parquet, if the projection
compactor can CAS mutation-visible roots, if snapshot/export services own any
mutation authority, if query/enforcement services mutate control-plane state, or
if engines write public/export roots or use table-protocol writes outside
`table_data` roots.

## Code-Level IAM Follow-Up

This Phase 2 slice intentionally stops at fixture-level hygiene because the
production cloud role layout is not yet grounded for a control-store cutover.
When concrete provider roles or Terraform policies are introduced, add a
code-level IAM test that proves:

- API/control writer identities cannot write public Parquet projection roots;
- projection compactor identities cannot CAS-publish mutation-visible control
  roots;
- snapshot/export identities cannot create mutation-visible state;
- query/enforcement identities have read-only access to pinned or published
  cuts;
- table and external engine identities cannot mutate catalog/control-plane
  roots.

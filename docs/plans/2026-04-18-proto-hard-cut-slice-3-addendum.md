# Proto Hard-Cut Follow-Up Addendum: Slice 3

## Why this batch now

The current post-cut surface still leaves `arco.common.v1` holding at least one domain-specific type:
`TableFormat`.

That enum is a stronger next extraction candidate than `FileEntry` because:

- it is only used by catalog read and mutation contracts
- its downstream adapter/test surface is small and already localized
- moving it materially shrinks `arco.common.v1` without forcing orchestration runtime churn

## Scope

1. Move `TableFormat` from `arco.common.v1` into `arco.catalog.v1`.
2. Update the minimal generated-type consumers in `arco-proto` and `arco-api`.
3. Correct `proto/STYLE.md` so it matches the repository's intended `WIRE_JSON` policy and the reduced common-surface guidance.

## Explicit deferrals after this batch

- Defer moving `FileEntry` until its orchestration contract and runtime-local shape can be handled with similarly low churn.
- Defer splitting `proto/arco/orchestration/v1/orchestration.proto` unless a later pass shows the split is nearly mechanical and yields meaningful clarity beyond generated diff noise.

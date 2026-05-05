# Proto Hard-Cut Follow-Up Addendum: Slice 4

## Why this batch now

With `TableFormat` already moved out, `FileEntry` is the next strongest candidate for shrinking
`arco.common.v1`:

- it is only referenced by the public orchestration `TaskOutput.files` boundary
- no current downstream code appears to depend directly on the generated
  `arco.common.v1.FileEntry` type
- the runtime-local `crates/arco-flow/src/task.rs::FileEntry` can stay untouched because the
  current orchestration mapping layer already isolates it from the public proto contract

## Scope

1. Move `FileEntry` from `arco.common.v1` into `arco.orchestration.v1`.
2. Update `TaskOutput.files` to use the orchestration-owned message.
3. Update only the minimal generated-type, test, and style-policy references needed to reflect the
   reduced `arco.common.v1` surface.

## Explicit deferrals after this batch

- Keep `crates/arco-flow/src/task.rs` runtime-local models stable unless a failing adapter or
  contract test proves otherwise.
- Do not split `proto/arco/orchestration/v1/orchestration.proto` in this batch. The split still
  looks like mostly organizational churn with large generated diff noise and limited contract
  signal.

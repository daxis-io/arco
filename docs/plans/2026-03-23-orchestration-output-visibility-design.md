# Orchestration Output Visibility Contract Design

**Goal:** Define the Daxis-facing orchestration contract for separating task execution success from output visibility, while keeping Arco's detailed publish lifecycle internal.

**Architecture Direction:** Arco's architectural target remains full-write-path object-storage publication semantics, with metadata-first as the rollout phase. The stable protobuf contract should expose consumability outcomes, not the internal storage FSM (`STAGED` / `COMMITTED` / `PROMOTED`).

**Tech Stack:** Protobuf contracts in `proto/arco/v1`, Rust orchestration/runtime in `arco-flow`, internal Parquet projections for orchestration state.

---

## Decisions

### Public contract boundary

- `TaskExecution.state` models execution only.
- `TASK_STATE_SUCCEEDED` means the worker completed successfully.
- Output visibility is modeled separately on `TaskOutput`.
- The detailed publish lifecycle remains internal to Arco storage/projection layers.

### `TaskOutput` presence and visibility

- `TaskExecution.output` is present only once Arco has a stable output identity.
- If `output` is present, `materialization_id` must be set.
- `output` absent means there is no trackable output object yet.
- `TaskOutput.visibility_state` is the public consumability signal.
- New writers must never emit `output` with `visibility_state == OUTPUT_VISIBILITY_STATE_UNSPECIFIED`.
- Legacy/backfilled `_UNSPECIFIED` is tolerated only during migration and treated as non-consumable; Daxis-facing reads should normalize it to `PENDING` where practical.

Recommended public enum:

```protobuf
enum OutputVisibilityState {
  OUTPUT_VISIBILITY_STATE_UNSPECIFIED = 0;
  OUTPUT_VISIBILITY_STATE_PENDING = 1;
  OUTPUT_VISIBILITY_STATE_VISIBLE = 2;
  OUTPUT_VISIBILITY_STATE_FAILED = 3;
}
```

### `TaskOutput` field semantics

Recommended shape:

```protobuf
message TaskOutput {
  MaterializationId materialization_id = 1;
  repeated FileEntry files = 2;
  optional int64 row_count = 3;
  optional int64 byte_size = 4;

  OutputVisibilityState visibility_state = 5;
  google.protobuf.Timestamp published_at = 6;
  TaskError publish_error = 7;
}
```

Rules:

- `files` contains published/readable files only.
- `published_at` is set only when `visibility_state == VISIBLE`.
- `publish_error` is set only when `visibility_state == FAILED`.
- `TaskExecution.error` remains execution-only and is set only when `state == TASK_STATE_FAILED`.
- `row_count` and `byte_size` are `optional int64` so absence is distinct from a real `0`.

Visibility-state behavior:

- `PENDING`: `files` empty, `published_at` unset, `row_count` unset, `byte_size` unset.
- `VISIBLE`: `files` populated, `published_at` set, `row_count`/`byte_size` may be set, including explicit `0`.
- `FAILED`: terminal only after publish retries are exhausted; `files` empty, `publish_error` set, `row_count` unset, `byte_size` unset.

### Run aggregation rules

- Daxis clients must treat output consumability as `task.state == TASK_STATE_SUCCEEDED` and `task.output.visibility_state == VISIBLE`.
- A run stays `RUNNING` while any required output is still `PENDING`.
- A run becomes `FAILED` if any required output reaches terminal `FAILED`.
- A run becomes `SUCCEEDED` only when all required task executions succeeded and all required outputs are `VISIBLE`.

### Required-output inference

- Required output is inferred from `TaskSpec.operation == TASK_OPERATION_MATERIALIZE`.
- No separate `requires_visible_output` field is part of the public contract at this stage.
- `TASK_OPERATION_CHECK` is non-output-producing.
- Legacy `TASK_OPERATION_BACKFILL` is treated as non-output-producing.

### Backfill semantics

- Backfill remains a run/control-plane concern via `RunTrigger.type == TRIGGER_TYPE_BACKFILL` and related metadata.
- New planners/writers should stop emitting `TASK_OPERATION_BACKFILL`.
- Data-writing backfill work should be modeled as ordinary `MATERIALIZE` tasks inside a backfill-triggered run.
- Keep `TASK_OPERATION_BACKFILL` in the proto for compatibility, but deprecate it semantically and in writer behavior.

---

## Migration Notes

- Adding `optional` to existing `int64` fields is wire-compatible, but older generated clients will still collapse absent values to `0` until they regenerate from the updated proto.
- Readers should tolerate legacy records with `output` present and `_UNSPECIFIED` visibility, treating them as non-consumable.
- If needed, Daxis-facing read paths should normalize legacy `_UNSPECIFIED` to `PENDING` before returning responses.

## Implementation Follow-up

- Update `proto/arco/v1/orchestration.proto` to add `OutputVisibilityState`, `published_at`, and `publish_error`, and to make `row_count`/`byte_size` optional.
- Update orchestration runtime and read models so `TaskExecution.state` and `TaskOutput.visibility_state` are aggregated independently.
- Add validation/tests that reject new records with `output` present and `_UNSPECIFIED` visibility.

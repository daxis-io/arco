# dagster-parity-08 — Run UX Gaps (Selection + Reruns)

## References
- Charter: `docs/parity/dagster-parity-01-charter.md`
- TriggerRun API: `crates/arco-api/src/routes/orchestration.rs`
- run_key semantics: `crates/arco-flow/src/orchestration/run_key.rs`
- CLI: `crates/arco-cli/src/commands/run.rs`

## Goal
Close user-facing run UX gaps that define Dagster parity for operators:

- selection semantics (subset execution)
- rerun-from-failure / subset reruns

## Non-Goals
- Dagit parity.
- Auto-materialize (phased elsewhere).

## Deliverables
1. A selection contract:
   - how a user selects a subset of assets/tasks
   - how that affects plan generation
2. Rerun contract:
   - rerun from failure
   - rerun only a subset
   - deterministic run_key / run lineage semantics
3. Proof gates:
   - tests that prevent regression (uncheatable)

## Work Breakdown
### Step 0 — Lock selection semantics
- Define “selection” as:
  - the set of roots to execute
  - and the closure rules (do we include upstream dependencies? downstream? neither?)

Parity-critical invariant (from prior parity docs):
- Selection must not silently auto-materialize downstream unless explicitly requested.

### Step 1 — Implement selection in planning
- Ensure plan generation respects selection.
- Ensure the plan fingerprint includes selection parameters.

Likely touch points:
- API request schema for TriggerRun
- orchestration plan builder

### Step 2 — Implement rerun semantics
- Define a rerun as:
  - same logical job/asset graph
  - new run_id (or derived lineage)
  - only failed subset re-executed
- Ensure run history shows lineage.

### Step 3 — Operator surface
- CLI support:
  - run with selection
  - rerun failed
  - show parent/child run relation

### Step 4 — Proof (tests + CI)
Add “uncheatable” tests proving:
- selecting `{A}` does not execute `{B,C}` when downstream exists
- rerun-from-failure does not re-run succeeded tasks
- run_key behavior remains deterministic

Suggested locations:
- `crates/arco-flow/tests/orchestration_selection_tests.rs` (new)
- `python/arco/tests/integration/` (optional, if Python CLI is used)

## Acceptance Criteria
- Selection semantics are explicit and enforced.
- Rerun workflows behave as operators expect.
- CI prevents regressions.

## Evidence (parity-08)
- Code: `crates/arco-api/src/routes/orchestration.rs:2691` (`rerun_run` endpoint)
- Code: `crates/arco-api/src/routes/orchestration.rs:1823` (lineage extraction) and `crates/arco-api/src/routes/orchestration.rs:1833` (reject reserved lineage labels)
- Tests: `crates/arco-api/tests/orchestration_parity_gates_m1.rs:809` (`parity_m1_rerun_from_failure_plans_only_unsucceeded_tasks`)
- Tests: `crates/arco-api/tests/orchestration_parity_gates_m1.rs:1105` (`parity_m1_rerun_subset_respects_include_downstream`)
- Tests: `crates/arco-api/tests/orchestration_parity_gates_m1.rs:1261` (`parity_m1_rerun_from_failure_rejects_succeeded_parent`)
- Tests: `crates/arco-api/tests/orchestration_parity_gates_m1.rs:1398` (`parity_m1_trigger_rejects_reserved_lineage_labels`)
- CI: `.github/workflows/ci.yml:118` (job `test`: `cargo test -p arco-api --all-features --test orchestration_parity_gates_m1`)

### Q3 operator diagnostics evidence
- Code: `crates/arco-api/src/routes/orchestration.rs` (`rerun_reason_for_kind`, `task_retry_attribution`, `task_skip_attribution`, run-key conflict detail helpers)
- Code: `crates/arco-api/src/error.rs` (`details` payload on API errors)
- Tests: `crates/arco-api/tests/orchestration_parity_gates_m1.rs` (`parity_m1_rerun_from_failure_plans_only_unsucceeded_tasks`, `parity_m1_rerun_subset_respects_include_downstream`, `parity_m1_skip_attribution_is_deterministic_across_edge_ordering`, `parity_m1_run_key_conflicts_on_payload_mismatch`, `parity_m1_rerun_run_key_conflict_surfaces_diagnostics_payload`)
- CI: `.github/workflows/ci.yml` (job `test`: `cargo test -p arco-api --all-features --test orchestration_parity_gates_m1`)

## Operator surface (implemented)

Rust CLI (`arco`):
- Trigger with selection + explicit closure flags:
  - `arco run --asset analytics.a --include-downstream`
- Rerun from failure:
  - `arco run --rerun <PARENT_RUN_ID> --from-failure`
- Subset rerun (roots via `--asset`, optional closure flags):
  - `arco run --rerun <PARENT_RUN_ID> --asset analytics.b --include-downstream`

Python CLI (`arco-flow`):
- Trigger:
  - `arco-flow run raw.events`
  - `arco-flow run --asset analytics.a --include-downstream`
- Rerun from failure:
  - `arco-flow run --rerun <PARENT_RUN_ID> --from-failure`
- Subset rerun:
  - `arco-flow run --rerun <PARENT_RUN_ID> --asset analytics.b --include-downstream`

Notes:
- Both CLIs accept `namespace/name` and normalize to `namespace.name`.
- API wire values for rerun mode are `fromFailure` and `subset`.

Evidence:
- Rust CLI rerun wiring: `crates/arco-cli/src/client.rs`
- Rust CLI UX: `crates/arco-cli/src/commands/run.rs`
- Rust status lineage: `crates/arco-cli/src/commands/status.rs`
- Python SDK rerun wiring: `python/arco/src/arco_flow/client.py`
- Python CLI UX: `python/arco/src/arco_flow/cli/main.py`
- Python run/rerun implementation: `python/arco/src/arco_flow/cli/commands/run.py`
- Python status lineage: `python/arco/src/arco_flow/cli/commands/status.py`
- Python tests: `python/arco/tests/unit/test_client_trigger_run.py`, `python/arco/tests/unit/test_cli_main.py`

## Risks / Edge Cases
- Ambiguity around upstream dependency inclusion (must be explicit).
- Deterministic lineage identifiers.
- Interaction with partitions/backfills.

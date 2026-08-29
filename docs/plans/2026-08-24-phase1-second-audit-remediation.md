# Phase 1 Second-Audit Remediation Evidence

## Status and scope

This record maps the 16 second-audit findings to the local remediation on
`codex/s3-lambda-state-kernel-v1`. The preserved audited commit is
`0c5c155d17f0d50562209e55fd37d27c66e2d3a9`; remediation is additive.

The evidence below qualifies the repository kernel against deterministic and
in-memory fault tests only. It does not qualify a live S3 provider, deployed
IAM, Lambda/API routes, or production behavior.

## Finding-to-fix matrix

| # | Finding | Implemented fix | Named acceptance evidence |
|---:|---|---|---|
| 1 | Checksum-coherent Arrow could reach unsafe footer offsets and lengths before bounds checks. | Preflight uses checked arithmetic for every block and rejects negative, overflowing, or out-of-footer metadata/body bounds before constructing an Arrow reader. | `checksum_coherent_malformed_arrow_cases_fail_closed_without_panics`; unit cases `checksum_coherent_negative_body_length_is_typed_not_a_panic` and `checksum_coherent_huge_body_length_is_typed_without_allocation`. |
| 2 | Arrow schema and unsupported footer features were not fully constrained. | Preflight requires one exact eight-field primitive schema, supported metadata version, no dictionaries or custom metadata, and exactly one record batch, with strict FlatBuffer verifier limits. | `checksum_coherent_malformed_arrow_cases_fail_closed_without_panics`; `malformed_arrow_schema_fails_closed_without_panicking`. |
| 3 | Checksum-valid logical row corruption could pass physical decoding. | Decode rejects unknown kinds, invalid tombstone/value polarity, duplicate physical keys, non-contiguous outbox ordinals, and v3 trim rows without `origin_sequence`. | `checksum_coherent_malformed_arrow_cases_fail_closed_without_panics`; `checksum_coherent_null_origin_l0_trim_fails_closed_without_a_panic`; `duplicate_physical_segment_keys_fail_closed`; `v3_l0_trim_rows_require_origin_sequence`. |
| 4 | Stored batch offsets were not proven to match the Arrow footer. | Index construction and validation use the actual footer blocks; the contract independently parses L0 and L1 footers and compares each offset tuple. | `commit_persists_indexed_l0_and_l1_arrow_segments`. |
| 5 | A landed normal commit followed by a successor could be reported as failed. | Normal commit reconciliation accepts the exact candidate transaction reference in bounded visible lineage. | `landed_commit_followed_by_successor_recovers_from_visible_lineage`. |
| 6 | An unresolved normal head-write failure was indistinguishable from a definite storage failure. | Added `AmbiguousAuthorityOutcome`; unresolved transport errors preserve the original storage failure in the diagnosis. | `landed_commit_with_unrelated_visible_head_is_typed_ambiguous`. |
| 7 | Writer-authority claims and fault tests did not prove exact lost-response reconciliation. | Claims retain canonical candidate bytes, adopt only exact readback, use `ControlMvpPaths::current_pointer()`, and assert each armed fault fired. | `landed_writer_claim_with_lost_response_adopts_exact_claimed_epoch`; normal and restore pointer-fault tests in `state_store_control_mvp`. |
| 8 | Restore rendering depended on the receiving store's live checkpoint interval. | Current v3 plans require a positive `checkpoint_interval`; identity, inspection, rendering, and application use the persisted value. | `restore_plan_replay_anchor_interval_survives_store_reconstruction`; `workspace_restore_recovery_uses_the_planned_checkpoint_interval`. |
| 9 | Recovery of a retired/noncanonical authority reference produced a generic error. | Added `UnsupportedAuthorityFormat` with the `control/v1` hard cut, no-migration statement, and retained-source recovery direction. | `noncanonical_restore_authority_is_a_typed_hard_cut_error`; `workspace_recovery_rejects_a_seeded_old_layout_authority_without_writes`. |
| 10 | Legacy workspace recovery covered only one historical plan shape. | v1 and v2 omit `checkpoint_interval`, remain supersession-only, preserve their checked-in field sets, and are replaced by v3 plans. | `literal_versioned_restore_plan_fixtures_pin_the_compatibility_policy`; `workspace_restore_recovery_migrates_v1_and_v2_participant_plans_and_replans_them`. |
| 11 | Workspace no-write comparisons omitted current authority objects. | `restore_and_state_bytes` includes `/control/v1/domains/` and asserts that authority selection is nonempty. | `preflight_before_mutation_rechecks_pin_after_later_domain_planning`; `workspace_recovery_rejects_a_seeded_old_layout_authority_without_writes`. |
| 12 | Required L1 capacity could fail after candidate artifacts were already published. | Private production segment limits and a module-private reduced-limit seam classify L1 row/byte/index overflow as `MaintenanceBackpressure`; all candidate replay and rendering precedes writes. | `l1_row_byte_and_index_capacity_failures_are_typed_backpressure`; `required_l1_backpressure_precedes_every_candidate_artifact_write`. |
| 13 | Intent/outbox duplicate detection depended on staging order. | Outbox staging includes staged intent IDs; intent staging already includes retained and staged outbox IDs plus staged intents. | `projection_intent_collisions_and_invalid_fields_fail_during_staging`; `duplicate_projection_outbox_ids_fail_at_stage_time_and_domain_stays_trimmable`. |
| 14 | `TxnOptions::request_id` was interpolated before centralized validation. | Both model and control begins require a nonblank, at-most-256-byte, single path-safe component without separators, dot segments, or controls. | `transaction_request_ids_are_validated_before_model_transaction_begin`; `transaction_request_ids_are_validated_before_control_transaction_begin`. |
| 15 | Negative projection and maintenance intent wire boundaries were incomplete. | Deserialization rejects unsupported versions, invalid IDs, zero source sequence, blank manifest provenance, empty projection payload, zero layout generation, and unknown maintenance reasons. | `projection_intent_wire_rejects_every_invalid_contract_boundary`; `layout_maintenance_wire_rejects_every_invalid_contract_boundary`. |
| 16 | ADR and audit evidence overstated or obscured qualification boundaries. | ADR-043 now records all head reconciliation, pre-decode Arrow validation, pre-publication anchor backpressure, durable restore interval, and explicit residual qualification work. | `cargo xtask adr-check`; this matrix; the corrected Task 2 and Task 6 evidence in `2026-08-22-state-kernel-audit-remediation.md`. |

## Exact local acceptance gate

The completed branch must pass:

```text
cargo fmt --all
cargo fmt --all -- --check
git diff --check 0c5c155d17f0d50562209e55fd37d27c66e2d3a9..HEAD
cargo xtask adr-check
cargo clippy -p arco-catalog --all-targets -- -D warnings
cargo test -p arco-catalog --lib state_store::control_mvp::tests
cargo test -p arco-catalog --test state_store_control_mvp
cargo test -p arco-catalog --test state_store_intent_contract
cargo test -p arco-catalog --test state_store_segment_contract
cargo test -p arco-catalog --test state_store_model
cargo test -p arco-catalog --test workspace_snapshot_restore
cargo test -p arco-catalog --test workspace_snapshot_services
cargo test -p arco-catalog --doc
cargo test --workspace --quiet
```

A fresh read-only reviewer must also audit both
`9f3d0ad8..new_head` and `0c5c155d..new_head`. Local merge readiness requires
no remaining P0, P1, or P2 findings in those ranges.

## Residual qualification work

The following remain explicitly outside this remediation:

- live S3 conditional-write and ambiguous-response qualification;
- provider-internal conditional retry behavior;
- JSON artifact byte caps;
- segment consolidation and retention/garbage collection;
- route cutover and deployed production error-envelope mapping;
- AWS IAM, KMS, SQS, STS, Access Grants, regional recovery, performance, and
  production qualification.

No repository-only result in this document certifies those boundaries.

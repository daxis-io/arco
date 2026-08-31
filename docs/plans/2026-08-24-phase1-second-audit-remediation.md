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
| 3 | Checksum-valid logical row corruption could pass physical decoding. | Decode rejects unknown kinds, invalid tombstone/value polarity, duplicate physical keys, non-contiguous outbox ordinals, v3 trim rows without `origin_sequence`, and L1 outbox rows with missing or invalid committed provenance. | `checksum_coherent_malformed_arrow_cases_fail_closed_without_panics`; `checksum_coherent_null_origin_l0_trim_fails_closed_without_a_panic`; `duplicate_physical_segment_keys_fail_closed`; `v3_l0_trim_rows_require_origin_sequence`. |
| 4 | Stored batch offsets were not proven to match the Arrow footer. | Index construction and validation use the actual footer blocks; the contract independently parses L0 and L1 footers and compares each offset tuple. | `commit_persists_indexed_l0_and_l1_arrow_segments`. |
| 5 | A landed normal commit followed by a successor could be reported as failed. | Normal commit reconciliation accepts the exact candidate transaction reference in bounded visible lineage. | `landed_commit_followed_by_successor_recovers_from_visible_lineage`. |
| 6 | An unresolved normal head-write failure was indistinguishable from a definite storage failure. | Added `AmbiguousAuthorityOutcome`; unresolved transport errors preserve the original storage failure in the diagnosis. | `landed_commit_with_unrelated_visible_head_is_typed_ambiguous`. |
| 7 | Writer-authority claims and fault tests did not prove exact lost-response reconciliation. | Claims retain canonical candidate bytes, adopt only exact readback, use `ControlMvpPaths::current_pointer()`, and assert each armed fault fired. | `landed_writer_claim_with_lost_response_adopts_exact_claimed_epoch`; normal and restore pointer-fault tests in `state_store_control_mvp`. |
| 8 | Restore rendering depended on the receiving store's live checkpoint interval. | Current v3 plans require a positive `checkpoint_interval`; identity, inspection, rendering, and application use the persisted value. | `restore_plan_replay_anchor_interval_survives_store_reconstruction`; `workspace_restore_recovery_uses_the_planned_checkpoint_interval`. |
| 9 | Recovery of a retired/noncanonical authority reference produced a generic error. | Added `UnsupportedAuthorityFormat` with the `control/v1` hard cut, no-migration statement, and retained-source recovery direction. Manifest and checkpoint paths must round-trip through the exact canonical builders. | `noncanonical_restore_authority_is_a_typed_hard_cut_error`; `workspace_recovery_rejects_seeded_noncanonical_authority_paths_without_writes`. |
| 10 | Legacy workspace recovery covered only one historical plan shape. | v1 and v2 omit `checkpoint_interval`, remain supersession-only, preserve their checked-in field sets, and are replaced by v3 plans. | `literal_versioned_restore_plan_fixtures_pin_the_compatibility_policy`; `workspace_restore_recovery_migrates_v1_and_v2_participant_plans_and_replans_them`. |
| 11 | Workspace no-write comparisons omitted current authority objects. | `restore_and_state_bytes` includes `/control/v1/domains/` and asserts that authority selection is nonempty. | `preflight_before_mutation_rechecks_pin_after_later_domain_planning`; `workspace_recovery_rejects_seeded_noncanonical_authority_paths_without_writes`. |
| 12 | Required L1 capacity could fail after candidate artifacts were already published. | Private production segment limits and a module-private reduced-limit seam classify L1 row/byte/index overflow as `MaintenanceBackpressure`; all candidate replay and rendering precedes writes. | `l1_row_byte_and_index_capacity_failures_are_typed_backpressure`; `required_l1_backpressure_precedes_every_candidate_artifact_write`. |
| 13 | Intent/outbox duplicate detection depended on staging order. | Outbox staging includes staged intent IDs; intent staging already includes retained and staged outbox IDs plus staged intents. | `projection_intent_collisions_and_invalid_fields_fail_during_staging`; `duplicate_projection_outbox_ids_fail_at_stage_time_and_domain_stays_trimmable`. |
| 14 | `TxnOptions::request_id` was interpolated before centralized validation. | Both model and control begins require a nonblank, at-most-256-byte, single path-safe component without separators, dot segments, or controls. | `transaction_request_ids_are_validated_before_model_transaction_begin`; `transaction_request_ids_are_validated_before_control_transaction_begin`. |
| 15 | Negative projection and maintenance intent wire boundaries were incomplete. | Deserialization rejects unsupported versions, invalid IDs, zero source sequence, blank manifest provenance, empty projection payload, zero layout generation, and unknown maintenance reasons. | `projection_intent_wire_rejects_every_invalid_contract_boundary`; `layout_maintenance_wire_rejects_every_invalid_contract_boundary`. |
| 16 | ADR and audit evidence overstated or obscured qualification boundaries. | ADR-043 now records all head reconciliation, pre-decode Arrow validation, pre-publication anchor backpressure, durable restore interval, and explicit residual qualification work. | `cargo xtask adr-check`; this matrix; the corrected Task 2 and Task 6 evidence in `2026-08-22-state-kernel-audit-remediation.md`. |

## Fresh secondary-audit follow-up

The first read-only review of `9f3d0ad8..a40d1447` and
`0c5c155d..a40d1447` found three P1 gaps. The follow-up commit addresses each
gap and adds a regression:

| Review finding | Follow-up | Evidence |
|---|---|---|
| A restore head write and its reconciliation read could both fail while returning an ordinary storage error. | `apply_restore` now returns `AmbiguousAuthorityOutcome` and preserves both failures when it cannot prove the outcome. | `restore_write_and_readback_failures_are_typed_ambiguous`. |
| A selected L1 segment could retain an outbox row without usable provenance. | L1 reconstruction requires generation zero and an origin sequence in `1..=anchor_sequence`. | `checksum_coherent_malformed_arrow_cases_fail_closed_without_panics` covers null, zero, future, and nonzero-generation variants. |
| The hard-cut check accepted nested checkpoint paths under the canonical prefix. | Checkpoint paths now require one path-safe `.json` component and exact `ControlMvpPaths::checkpoint_object` round-trip equality. | `noncanonical_restore_authority_is_a_typed_hard_cut_error`; `workspace_recovery_rejects_seeded_noncanonical_authority_paths_without_writes`. |

The segment test helper now reseals transaction and manifest envelopes with
mirror structs in production field order. It also rebuilds the index identity
and logical-state checksum for malformed fixtures. The malformed Arrow tests
therefore reach the intended footer, row, and provenance checks.

## Fresh final-audit follow-up

The fresh read-only audit of `9f3d0ad8..87e219ae`,
`0c5c155d..87e219ae`, and `54fb6483..87e219ae` found one additional P2
fail-closed defect plus two storage-boundary risks. The follow-up closes all
three locally:

| Review item | Follow-up | Evidence |
|---|---|---|
| A checksum-coherent manifest or L1 anchor at `logical_sequence = u64::MAX` could panic in checked builds or wrap to zero in release builds. | Every transaction begin, projected-token prediction, commit, replay, and manifest-suffix advancement uses one checked logical-sequence successor that returns `InvariantViolation` on overflow. | `checksum_coherent_terminal_logical_sequence_is_typed_not_a_panic`; `replay_rejects_sequence_zero_after_terminal_logical_sequence_without_panicking`, including the focused release-profile run. |
| Shared adapter convenience constructors could reuse an automatically retrying client for conditional authority writes. | Same-client constructors were removed. Every adapter construction must explicitly identify the conditional-write client, and provider crates continue to configure that client with zero automatic request retries. | `shared_adapter_requires_an_explicit_conditional_write_client`; `provider_builders_preserve_conditional_write_ambiguity`; shared adapter contract tests. |
| Moving the published `arco_core::ObjectStoreBackend` export was an undocumented Rust source break at workspace version 0.2.1. | The Rust workspace advances to 0.3.0 and the changelog gives the exact adapter and runtime-factory migration. Reintroducing the old export would recreate the forbidden core-to-provider dependency cycle. | `published_adapter_relocation_is_a_versioned_breaking_change`; `storage_provider_crates_own_cloud_dependencies`. |

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
- independent live GCS and Azure conditional-write and recovery qualification;
- provider-internal conditional retry behavior;
- JSON artifact byte caps;
- segment consolidation and retention/garbage collection;
- route cutover and deployed production error-envelope mapping;
- AWS IAM, KMS, SQS, STS, Access Grants, regional recovery, performance, and
  production qualification.

No repository-only result in this document certifies those boundaries.

//! Frozen Gate 5 eager reference from 745ed92e25ff7b4ec85aa0be57b02450642fda59.
//! Only admission differs: tests may explicitly force a below-threshold rewrite.
//! Never route durable maintenance through this reference.
use super::{
    AuthorityWritePrecondition, CONTROL_MVP_FORMAT_VERSION, CatalogError,
    ControlMvpMaintenanceOutcome, ControlMvpMaintenanceWorker, ControlMvpManifest,
    ControlMvpPointer, ControlMvpScopeDoc, HistoryAnchor, IMPLEMENTATION,
    LayoutMaintenanceIntentV1, LayoutMaintenanceReason, MAX_CONTROL_JSON_BYTES,
    MAX_HEAD_JSON_BYTES, Result, RewriteEquivalence, WriteResult, ambiguous_authority_outcome,
    encode_envelope_limited, encode_json_limited, invariant_violation, put_immutable_matching,
    sha256_hex,
};

impl ControlMvpMaintenanceWorker {
    /// Executes the original eager algorithm for local cost comparisons.
    ///
    /// # Errors
    /// Returns the same storage, validation, and publication errors as the reference.
    #[allow(clippy::too_many_lines)]
    pub async fn test_eager_consolidate(
        &self,
        force: bool,
    ) -> Result<Option<ControlMvpMaintenanceOutcome>> {
        for _attempt in 0..4 {
            let Some(head) = self
                .store
                .storage
                .head(&self.store.paths.current_pointer())
                .await?
            else {
                return Ok(None);
            };
            let pointer = self.store.load_pointer().await?;
            let source_manifest = self.store.load_manifest_for_pointer(&pointer).await?;
            let intent = match source_manifest.maintenance_intent.clone() {
                Some(intent) => intent,
                None if force => LayoutMaintenanceIntentV1::new(
                    format!("maintain-{}", source_manifest.manifest_id),
                    &self.store.token(
                        source_manifest.manifest_id.clone(),
                        source_manifest.logical_sequence,
                    ),
                    source_manifest
                        .layout_generation
                        .checked_add(1)
                        .ok_or_else(|| invariant_violation("eager reference layout overflow"))?,
                    LayoutMaintenanceReason::L0SegmentCount,
                )?,
                None => return Ok(None),
            };
            let source_token = self
                .store
                .token(
                    source_manifest.manifest_id.clone(),
                    source_manifest.logical_sequence,
                )
                .with_manifest_witness(pointer.manifest_checksum_sha256.clone());
            let state = self.store.replay_for_successor(&source_manifest).await?;
            let candidate_manifest_id = format!(
                "manifest-{:020}-layout-{:020}-rg-{:020}-{}",
                source_manifest.logical_sequence,
                intent.layout_generation(),
                pointer.reclamation_generation,
                super::cost::nonce().to_string().to_ascii_lowercase()
            );
            let rendered = self
                .store
                .render_state_snapshots(&state, &candidate_manifest_id)?;
            let base_states = rendered
                .iter()
                .map(|segment| segment.reference.clone())
                .collect::<Vec<_>>();
            let mut candidate_manifest = ControlMvpManifest {
                history_anchor: HistoryAnchor {
                    sequence: state.logical_sequence,
                    root: state.history_root.clone(),
                },
                history_root: state.history_root.clone(),
                physical_root: String::new(),
                equivalence: Some(RewriteEquivalence {
                    render_source: None,
                    encoding_version: 1,
                    source_manifest_id: source_manifest.manifest_id.clone(),
                    source_manifest_sha256: pointer.manifest_checksum_sha256.clone(),
                    source_history_root: source_manifest.history_root.clone(),
                    source_physical_root: source_manifest.physical_root.clone(),
                    logical_sequence: state.logical_sequence,
                    state_checksum_sha256: state.checksum()?,
                }),
                parent_manifest_sha256: Some(pointer.manifest_checksum_sha256.clone()),
                reclamation_generation: pointer.reclamation_generation,
                format_version: CONTROL_MVP_FORMAT_VERSION,
                implementation: IMPLEMENTATION.to_string(),
                scope: ControlMvpScopeDoc::from(&self.store.scope),
                manifest_id: candidate_manifest_id.clone(),
                logical_sequence: source_manifest.logical_sequence,
                base_manifest_id: Some(source_manifest.manifest_id.clone()),
                writer_epoch: pointer.writer_epoch,
                layout_generation: intent.layout_generation(),
                base_states,
                anchor_states: Vec::new(),
                tx_refs: Vec::new(),
                state_checksum_sha256: source_manifest.state_checksum_sha256.clone(),
                maintenance_intent: None,
            };
            candidate_manifest.physical_root = candidate_manifest.physical_digest()?;
            candidate_manifest.validate(&self.store.scope, &candidate_manifest_id)?;
            let manifest_bytes = encode_envelope_limited(
                "control-mvp-manifest",
                &candidate_manifest,
                MAX_CONTROL_JSON_BYTES,
                "control MVP maintenance manifest",
            )?;
            let candidate_pointer = ControlMvpPointer {
                reclamation_generation: pointer.reclamation_generation,
                format_version: CONTROL_MVP_FORMAT_VERSION,
                implementation: IMPLEMENTATION.to_string(),
                scope: ControlMvpScopeDoc::from(&self.store.scope),
                manifest_id: candidate_manifest_id.clone(),
                logical_sequence: source_manifest.logical_sequence,
                manifest_checksum_sha256: sha256_hex(&manifest_bytes),
                writer_epoch: pointer.writer_epoch,
            };
            let pointer_bytes = encode_json_limited(
                &candidate_pointer,
                MAX_HEAD_JSON_BYTES,
                "control MVP maintenance head",
            )?;

            for segment in &rendered {
                put_immutable_matching(
                    &self.store.storage,
                    &self.store.paths.state_object(&segment.reference.state_id),
                    segment.bytes.clone(),
                    "control MVP maintenance L1 segment already exists with different bytes",
                )
                .await?;
                put_immutable_matching(
                    &self.store.storage,
                    &self.store.paths.segment_index(&segment.reference.state_id),
                    segment.index_bytes.clone(),
                    "control MVP maintenance L1 index already exists with different bytes",
                )
                .await?;
            }
            put_immutable_matching(
                &self.store.storage,
                &self.store.paths.manifest_object(&candidate_manifest_id),
                manifest_bytes,
                "control MVP maintenance manifest already exists with different bytes",
            )
            .await?;
            let publish = self
                .store
                .storage
                .put(
                    &self.store.paths.current_pointer(),
                    pointer_bytes.clone(),
                    AuthorityWritePrecondition::MatchesVersion(head.version),
                )
                .await;
            match publish {
                Ok(WriteResult::Success { .. }) => {
                    return Ok(Some(ControlMvpMaintenanceOutcome {
                        source_token,
                        selected_token: self
                            .store
                            .token(candidate_manifest_id, source_manifest.logical_sequence)
                            .with_manifest_witness(
                                candidate_pointer.manifest_checksum_sha256.clone(),
                            ),
                        layout_generation: intent.layout_generation(),
                    }));
                }
                Ok(WriteResult::PreconditionFailed { .. }) => {}
                Err(error) => {
                    if self
                        .store
                        .get_json(&self.store.paths.current_pointer(), MAX_HEAD_JSON_BYTES)
                        .await
                        .is_ok_and(|visible| visible == pointer_bytes)
                    {
                        return Ok(Some(ControlMvpMaintenanceOutcome {
                            source_token,
                            selected_token: self
                                .store
                                .token(candidate_manifest_id, source_manifest.logical_sequence)
                                .with_manifest_witness(
                                    candidate_pointer.manifest_checksum_sha256.clone(),
                                ),
                            layout_generation: intent.layout_generation(),
                        }));
                    }
                    return Err(ambiguous_authority_outcome(format!(
                        "control MVP maintenance head CAS outcome is unknown: {error}"
                    )));
                }
            }
        }
        Err(CatalogError::CasFailed {
            message: "control MVP maintenance head changed during four replans".to_string(),
        })
    }
}

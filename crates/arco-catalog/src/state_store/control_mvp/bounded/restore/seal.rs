//! Immutable final summary. A seal is evidence for readers, never permission to send HEAD.
use super::certificate::RestoreCertificateV1;
use super::{
    ControlMvpPointer, ControlMvpRestorePlanV7, ControlMvpStateStore, Result, Target,
    invariant_violation, retained,
};
use crate::state_store::StateScope;
use serde::{Deserialize, Serialize};

use super::super::{ArtifactRef, Manifest8};

const RECORD_TYPE: &str = "control_mvp_restore_final_seal";
pub(super) const MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FinalSealV1 {
    record_type: String,
    version: u32,
    scope: StateScope,
    selected_plan_sha256: String,
    owner_generation: u64,
    candidate_id: String,
    original_target: Target,
    candidate_manifest: ArtifactRef,
    certificate: ArtifactRef,
    candidate_head_sha256: String,
    terminal_receipt_count: u64,
    terminal_chain_sha256: String,
    logical_history: String,
    kv_root_hex: String,
    active_id_root_hex: String,
    delivery_order_root_hex: String,
    projection_source: ArtifactRef,
}

impl FinalSealV1 {
    pub(super) fn from_verified(
        plan: &ControlMvpRestorePlanV7,
        selected_plan_sha256: &str,
        manifest: &Manifest8,
        manifest_ref: &ArtifactRef,
        certificate_ref: &ArtifactRef,
        certificate: &RestoreCertificateV1,
    ) -> Result<Self> {
        let fields = &plan.fields;
        let path = super::ControlMvpPaths::new(fields.scope.domain())
            .manifest_object(&fields.candidate_id);
        let manifest_bytes = super::super::super::encode_json_limited(
            manifest,
            super::MAX_CONTROL_JSON_BYTES,
            "restore sealed manifest",
        )?;
        if manifest_ref.path != path
            || manifest_ref.sha256 != super::super::super::sha256_hex(&manifest_bytes)
            || manifest.restore_certificate.as_ref() != Some(certificate_ref)
            || manifest.manifest_id != fields.candidate_id
        {
            return Err(invariant_violation(
                "restore seal candidate artifacts differ",
            ));
        }
        let pointer = ControlMvpPointer {
            reclamation_generation: manifest.reclamation_generation,
            format_version: 8,
            implementation: super::IMPLEMENTATION.into(),
            scope: manifest.scope.clone(),
            manifest_id: manifest.manifest_id.clone(),
            logical_sequence: manifest.logical_sequence,
            manifest_checksum_sha256: manifest_ref.sha256.clone(),
            writer_epoch: manifest.writer_epoch,
            claim_id: None,
        };
        let pointer_bytes = super::super::super::encode_json_limited(
            &pointer,
            super::super::super::MAX_HEAD_JSON_BYTES,
            "restore sealed HEAD",
        )?;
        let terminal = certificate.terminal();
        Ok(Self {
            record_type: RECORD_TYPE.into(),
            version: 1,
            scope: fields.scope.clone(),
            selected_plan_sha256: selected_plan_sha256.into(),
            owner_generation: fields.owner_generation,
            candidate_id: fields.candidate_id.clone(),
            original_target: fields.target.clone(),
            candidate_manifest: manifest_ref.clone(),
            certificate: certificate_ref.clone(),
            candidate_head_sha256: super::super::super::sha256_hex(&pointer_bytes),
            terminal_receipt_count: terminal.receipt_count,
            terminal_chain_sha256: terminal.chain_sha256.clone(),
            logical_history: manifest.logical_history.clone(),
            kv_root_hex: manifest.kv_root.directory_root_hex.clone(),
            active_id_root_hex: manifest.active_id_root.directory_root_hex.clone(),
            delivery_order_root_hex: manifest.delivery_order_root.directory_root_hex.clone(),
            projection_source: manifest
                .projection_source
                .clone()
                .ok_or_else(|| invariant_violation("restore seal lacks projection source"))?,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the seal compares each independently authenticated candidate artifact"
    )]
    pub(super) async fn read_bounded(
        store: &ControlMvpStateStore,
        budget: &mut retained::ReadBudget<'_>,
        plan: &ControlMvpRestorePlanV7,
        selected_plan_sha256: &str,
        manifest: &Manifest8,
        manifest_ref: &ArtifactRef,
        certificate_ref: &ArtifactRef,
        certificate: &RestoreCertificateV1,
    ) -> Result<Self> {
        let path = format!(
            "{}/restore/v7/{}/seal.json",
            store.paths.base_prefix(),
            plan.fields.candidate_id
        );
        let bytes =
            retained::read_bounded_json(store, budget, &path, MAX_BYTES, "restore seal").await?;
        let actual: Self = super::decode_json(&bytes, "restore seal")?;
        if serde_jcs::to_vec(&actual)
            .map_err(|_| invariant_violation("restore seal JCS encoding failed"))?
            != bytes
        {
            return Err(invariant_violation("restore seal is not canonical"));
        }
        let expected = Self::from_verified(
            plan,
            selected_plan_sha256,
            manifest,
            manifest_ref,
            certificate_ref,
            certificate,
        )?;
        if actual != expected {
            return Err(invariant_violation(
                "restore seal differs from verified candidate",
            ));
        }
        Ok(actual)
    }
}

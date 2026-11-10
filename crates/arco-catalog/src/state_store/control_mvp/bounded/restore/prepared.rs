//! Private prepared descriptor; persisted bytes never authorize a HEAD retry.
use super::super::{ArtifactRef, Manifest8};
use super::{
    ControlMvpPointer, ControlMvpRestorePlanV7, Result, Target, invariant_violation,
    prefixed_sha256, valid_raw_digest,
};
use crate::state_store::{PersistedRestoreParticipantPlan, StateScope};
use base64::Engine as _;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedRestoreV1 {
    record_type: String,
    version: u32,
    scope: StateScope,
    candidate_id: String,
    selected_plan_sha256: String,
    owner_generation: u64,
    original_target: Target,
    candidate_manifest: ArtifactRef,
    certificate: ArtifactRef,
    seal: ArtifactRef,
    candidate_head_raw_b64: String,
    candidate_head_sha256: String,
}

impl PreparedRestoreV1 {
    pub(super) fn manifest_digest(&self) -> &str {
        &self.candidate_manifest.sha256
    }

    pub(super) async fn read_bounded(
        store: &super::ControlMvpStateStore,
        budget: &mut super::retained::ReadBudget<'_>,
        plan: &ControlMvpRestorePlanV7,
        selected_plan_sha256: &str,
    ) -> Result<Self> {
        let path = format!(
            "{}/restore/v7/{}/prepared.json",
            store.paths.base_prefix(),
            plan.fields.candidate_id
        );
        let bytes = super::retained::read_bounded_json(
            store,
            budget,
            &path,
            super::MAX_CONTROL_JSON_BYTES,
            "restore prepared candidate",
        )
        .await?;
        let actual: Self = super::decode_json(&bytes, "restore prepared candidate")?;
        if actual.canonical_bytes()? != bytes {
            return Err(invariant_violation(
                "restore prepared bytes are not canonical",
            ));
        }
        let manifest_bytes = super::retained::read_bounded_json(
            store,
            budget,
            &store.paths.manifest_object(&plan.fields.candidate_id),
            super::MAX_CONTROL_JSON_BYTES,
            "restore prepared manifest",
        )
        .await?;
        let token = store
            .token(
                plan.fields.candidate_id.clone(),
                plan.fields.result_logical_sequence,
            )
            .with_manifest_witness(actual.candidate_manifest.sha256.clone());
        let base = store
            .read_bounded_token_from_manifest(&token, &manifest_bytes, budget)
            .await?;
        let manifest = base
            .manifest
            .as_ref()
            .ok_or_else(|| invariant_violation("restore prepared manifest is absent"))?;
        let certificate = manifest
            .restore_certificate
            .as_ref()
            .ok_or_else(|| invariant_violation("restore prepared certificate is absent"))?;
        let expected_seal_path = format!(
            "{}/restore/v7/{}/seal.json",
            store.paths.base_prefix(),
            plan.fields.candidate_id
        );
        if actual.seal.path != expected_seal_path || !valid_raw_digest(&actual.seal.sha256) {
            return Err(invariant_violation(
                "restore prepared seal reference differs",
            ));
        }
        let seal_bytes = super::retained::read_bounded_json(
            store,
            budget,
            &actual.seal.path,
            super::seal::MAX_BYTES,
            "restore prepared seal",
        )
        .await?;
        if super::super::super::sha256_hex(&seal_bytes) != actual.seal.sha256 {
            return Err(invariant_violation(
                "restore prepared seal checksum differs",
            ));
        }
        let expected = Self::from_verified(
            plan,
            selected_plan_sha256,
            manifest,
            &actual.candidate_manifest,
            certificate,
            &actual.seal,
        )?;
        if actual != expected {
            return Err(invariant_violation(
                "restore prepared descriptor differs from candidate",
            ));
        }
        Ok(actual)
    }

    pub(super) fn candidate_head_bytes(&self) -> Result<bytes::Bytes> {
        let bytes = super::binary(&self.candidate_head_raw_b64)?;
        if super::super::super::sha256_hex(&bytes) != self.candidate_head_sha256 {
            return Err(invariant_violation(
                "restore prepared HEAD checksum differs",
            ));
        }
        Ok(bytes::Bytes::from(bytes))
    }

    pub(super) fn from_verified(
        plan: &ControlMvpRestorePlanV7,
        selected_plan_sha256: &str,
        manifest: &Manifest8,
        manifest_ref: &ArtifactRef,
        certificate_ref: &ArtifactRef,
        seal_ref: &ArtifactRef,
    ) -> Result<Self> {
        let fields = &plan.fields;
        let paths = super::ControlMvpPaths::new(fields.scope.domain());
        let prefix = format!("{}/restore/v7/{}", paths.base_prefix(), fields.candidate_id);
        let actual_selected = prefixed_sha256(
            &serde_jcs::to_vec(&PersistedRestoreParticipantPlan::ControlMvpV7(Box::new(
                plan.clone(),
            )))
            .map_err(|_| invariant_violation("restore selected plan encoding failed"))?,
        );
        if fields.candidate_id != manifest.manifest_id {
            return Err(invariant_violation(
                "restore prepared candidate manifest differs",
            ));
        }
        if selected_plan_sha256 != actual_selected
            || fields.scope != manifest.scope
            || manifest_ref.path != paths.manifest_object(&fields.candidate_id)
            || manifest_ref.sha256
                != super::super::super::sha256_hex(&super::super::super::encode_json_limited(
                    manifest,
                    super::MAX_CONTROL_JSON_BYTES,
                    "restore prepared manifest",
                )?)
            || manifest.restore_certificate.as_ref() != Some(certificate_ref)
            || seal_ref.path != format!("{prefix}/seal.json")
            || !valid_raw_digest(&seal_ref.sha256)
        {
            return Err(invariant_violation(
                "restore prepared candidate artifacts differ",
            ));
        }
        if let Target::Present {
            current_pointer_version,
            ..
        } = &fields.target
            && (current_pointer_version.is_empty() || current_pointer_version.len() > 4096)
        {
            return Err(invariant_violation(
                "restore prepared original HEAD version is unbounded",
            ));
        }
        let pointer = ControlMvpPointer {
            reclamation_generation: manifest.reclamation_generation,
            format_version: 8,
            implementation: super::IMPLEMENTATION.into(),
            scope: fields.scope.clone(),
            manifest_id: fields.candidate_id.clone(),
            logical_sequence: manifest.logical_sequence,
            manifest_checksum_sha256: manifest_ref.sha256.clone(),
            writer_epoch: manifest.writer_epoch,
            claim_id: None,
        };
        let pointer_bytes = super::super::super::encode_json_limited(
            &pointer,
            super::super::super::MAX_HEAD_JSON_BYTES,
            "restore prepared HEAD",
        )?;
        Ok(Self {
            record_type: "control_mvp_restore_prepared_candidate".into(),
            version: 1,
            scope: fields.scope.clone(),
            candidate_id: fields.candidate_id.clone(),
            selected_plan_sha256: selected_plan_sha256.into(),
            owner_generation: fields.owner_generation,
            original_target: fields.target.clone(),
            candidate_manifest: manifest_ref.clone(),
            certificate: certificate_ref.clone(),
            seal: seal_ref.clone(),
            candidate_head_raw_b64: base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(&pointer_bytes),
            candidate_head_sha256: super::super::super::sha256_hex(&pointer_bytes),
        })
    }

    pub(super) fn canonical_bytes(&self) -> Result<Vec<u8>> {
        let bytes = serde_jcs::to_vec(self)
            .map_err(|_| invariant_violation("restore prepared encoding failed"))?;
        if bytes.len() > super::MAX_CONTROL_JSON_BYTES {
            return Err(invariant_violation(
                "restore prepared candidate exceeds 1 MiB",
            ));
        }
        Ok(bytes)
    }
}

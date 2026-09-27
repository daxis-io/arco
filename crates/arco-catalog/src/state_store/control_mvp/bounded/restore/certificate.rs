//! Private restore candidate certificate. It is not publication authority.
use super::super::{ArtifactRef, RoleTransition8, physical};
use super::{
    ControlMvpPaths, ControlMvpRestorePlanV7, Result, binary, invariant_violation, prefixed_sha256,
    raw_digest, tagged, valid_raw_digest,
};
use crate::state_store::{RestoreAttemptIdentity, StateScope};
use serde::{Deserialize, Serialize};

const RECORD_TYPE: &str = "control_mvp_restore_certificate";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Terminal {
    pub(super) next_ordinal: u64,
    pub(super) receipt_count: u64,
    pub(super) chain_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(
    clippy::struct_field_names,
    reason = "logical_commit_id is a fixed certificate wire field"
)]
pub(super) struct Logical {
    restore_request_digest: String,
    logical_commit_id: String,
    result_sequence: u64,
    restore_history_sha256: String,
    mutation_count: u64,
    restore_notice_payload_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RestoreCertificateV1 {
    record_type: String,
    version: u32,
    scope: StateScope,
    plan_sha256: String,
    identity: RestoreAttemptIdentity,
    owner_generation: u64,
    candidate_id: String,
    terminal: Terminal,
    source_reference_sha256: String,
    source_manifest_sha256: String,
    target_witness_digest: String,
    output_kv_root_b64: String,
    logical: Logical,
    projection_source: ArtifactRef,
    pub(super) active: RoleTransition8,
    pub(super) delivery: RoleTransition8,
}

impl RestoreCertificateV1 {
    pub(super) async fn read_bounded(
        store: &super::ControlMvpStateStore,
        budget: &mut super::retained::ReadBudget<'_>,
        plan: &ControlMvpRestorePlanV7,
        plan_sha256: &str,
        reference: &ArtifactRef,
    ) -> Result<Self> {
        let fields = &plan.fields;
        let expected_path = format!(
            "{}/restore/v7/{}/certificate.json",
            store.paths.base_prefix(),
            fields.candidate_id
        );
        if fields.scope != store.scope
            || reference.path != expected_path
            || !valid_raw_digest(&reference.sha256)
        {
            return Err(invariant_violation("restore certificate reference differs"));
        }
        let bytes = super::retained::read_bounded_json(
            store,
            budget,
            &reference.path,
            super::MAX_CONTROL_JSON_BYTES,
            "restore certificate",
        )
        .await?;
        super::super::validate_raw_checksum(
            &bytes,
            Some(&reference.sha256),
            "restore certificate checksum",
        )?;
        let value: Self = super::decode_json(&bytes, "restore certificate")?;
        if serde_jcs::to_vec(&value)
            .map_err(|_| invariant_violation("restore certificate JCS encoding failed"))?
            != bytes
        {
            return Err(invariant_violation(
                "restore certificate bytes are not canonical",
            ));
        }
        value.validate_plan(plan, plan_sha256)?;
        let source = super::super::load_projection_source_with_budget(
            store,
            &value.projection_source.sha256,
            Some(budget),
        )
        .await?;
        if source.logical_sequence != fields.result_logical_sequence
            || source.logical_commit_id != raw_digest(&fields.logical_commit_id)?
            || source.kv_root_hex != hex::encode(binary(&value.output_kv_root_b64)?)
        {
            return Err(invariant_violation(
                "restore certificate projection source differs",
            ));
        }
        let directory = super::directory::Directory::new(store.retention.clone(), &store.scope)?;
        for (proof, inherited) in [
            (&value.active, &fields.base_active_id_root_b64),
            (&value.delivery, &fields.base_delivery_order_root_b64),
        ] {
            let old = directory.decode_root(&binary(inherited)?)?;
            let new = directory.decode_root(
                &hex::decode(&proof.new_root_hex)
                    .map_err(|_| invariant_violation("restore notice root is not hex"))?,
            )?;
            let edit = proof
                .edits
                .first()
                .ok_or_else(|| invariant_violation("restore notice edit is absent"))?;
            let edit = super::directory::update::Edit {
                old: edit
                    .old
                    .as_ref()
                    .map(super::super::leaf_from_proof)
                    .transpose()?,
                new: edit
                    .new
                    .iter()
                    .map(super::super::leaf_from_proof)
                    .collect::<Result<Vec<_>>>()?,
            };
            directory
                .verify_update(
                    &old,
                    &new,
                    &[edit],
                    &mut super::directory::ReadBudget::with_retained(budget),
                )
                .await?;
        }
        Ok(value)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "each final verifier output is an authenticated input"
    )]
    pub(super) fn from_final(
        plan: &ControlMvpRestorePlanV7,
        plan_sha256: &str,
        terminal: Terminal,
        kv_root_b64: String,
        restore_history_sha256: &str,
        mutation_count: u64,
        projection_source: &ArtifactRef,
        proofs: &[RoleTransition8; 2],
    ) -> Result<Self> {
        let fields = &plan.fields;
        let value = Self {
            record_type: RECORD_TYPE.into(),
            version: 1,
            scope: fields.scope.clone(),
            plan_sha256: plan_sha256.into(),
            identity: fields.identity.clone(),
            owner_generation: fields.owner_generation,
            candidate_id: fields.candidate_id.clone(),
            terminal,
            source_reference_sha256: fields.source_reference_sha256.clone(),
            source_manifest_sha256: fields.source_manifest.sha256.clone(),
            target_witness_digest: tagged(
                b"arco/control-v2/restore-target-witness-v1",
                &fields.target,
            )?,
            output_kv_root_b64: kv_root_b64,
            logical: Logical {
                restore_request_digest: fields.restore_request_digest.clone(),
                logical_commit_id: fields.logical_commit_id.clone(),
                result_sequence: fields.result_logical_sequence,
                restore_history_sha256: restore_history_sha256.into(),
                mutation_count,
                restore_notice_payload_sha256: prefixed_sha256(&binary(
                    &fields.restore_notice_payload_b64,
                )?),
            },
            projection_source: projection_source.clone(),
            active: proofs[0].clone(),
            delivery: proofs[1].clone(),
        };
        value.validate_plan(plan, plan_sha256)?;
        Ok(value)
    }

    pub(super) fn validate_plan(
        &self,
        plan: &ControlMvpRestorePlanV7,
        plan_sha256: &str,
    ) -> Result<()> {
        raw_digest(plan_sha256)?;
        raw_digest(&self.terminal.chain_sha256)?;
        raw_digest(&self.source_manifest_sha256)?;
        if !valid_raw_digest(&self.logical.restore_history_sha256)
            || !valid_raw_digest(&self.projection_source.sha256)
            || self.terminal.next_ordinal == 0
            || self.terminal.next_ordinal != self.terminal.receipt_count
            || self.output_kv_root_b64.len() != 380
        {
            return Err(invariant_violation(
                "restore certificate digest or terminal is invalid",
            ));
        }
        let fields = &plan.fields;
        let paths = ControlMvpPaths::new(fields.scope.domain());
        if self.record_type != RECORD_TYPE
            || self.version != 1
            || self.scope != fields.scope
            || self.plan_sha256 != plan_sha256
            || self.identity != fields.identity
            || self.owner_generation != fields.owner_generation
            || self.candidate_id != fields.candidate_id
            || self.source_reference_sha256 != fields.source_reference_sha256
            || self.source_manifest_sha256 != fields.source_manifest.sha256
            || self.target_witness_digest
                != tagged(b"arco/control-v2/restore-target-witness-v1", &fields.target)?
            || self.logical.restore_request_digest != fields.restore_request_digest
            || self.logical.logical_commit_id != fields.logical_commit_id
            || self.logical.result_sequence != fields.result_logical_sequence
            || self.logical.restore_notice_payload_sha256
                != prefixed_sha256(&binary(&fields.restore_notice_payload_b64)?)
            || self.projection_source.path
                != format!(
                    "{}/projection-sources/{}.json",
                    paths.base_prefix(),
                    self.projection_source.sha256
                )
        {
            return Err(invariant_violation(
                "restore certificate differs from selected Plan7",
            ));
        }
        binary(&self.output_kv_root_b64)?;
        for (proof, role, root) in [
            (
                &self.active,
                physical::Role::ActiveId,
                &fields.base_active_id_root_b64,
            ),
            (
                &self.delivery,
                physical::Role::DeliveryOrder,
                &fields.base_delivery_order_root_b64,
            ),
        ] {
            let old = hex::encode(binary(root)?);
            let new = hex::decode(&proof.new_root_hex)
                .map_err(|_| invariant_violation("restore notice root is not hex"))?;
            if proof.role != role
                || proof.old_root_hex != old
                || proof.new_root_hex != hex::encode(&new)
                || new.len() != binary(root)?.len()
                || proof.new_root_hex == old
                || !matches!(proof.edits.as_slice(), [edit] if (1..=2).contains(&edit.new.len()))
            {
                return Err(invariant_violation(
                    "restore certificate notice transition differs",
                ));
            }
        }
        Ok(())
    }
}

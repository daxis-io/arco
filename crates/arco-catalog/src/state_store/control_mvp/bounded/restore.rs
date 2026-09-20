//! Bounded restore planning and the exact Plan7 wire contract.
mod units;
use super::super::{
    CatalogError, ControlMvpPaths, ControlMvpPointer, ControlMvpStateStore,
    DurableAuthorityBinding, IMPLEMENTATION, MAX_CONTROL_JSON_BYTES, PersistedAuthorityKind,
    PersistedAuthorityReference, RestoreAttemptIdentity, Result, StateScope, decode_json,
    encode_json, invariant_violation, prefixed_sha256, valid_raw_digest,
};
use super::{directory, logical_v2, retained};
use crate::state_store::RestorePlanningContext;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest as _, Sha256};
pub(in crate::state_store::control_mvp) use units::SingletonEmitAdmission;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Instant {
    seconds: i64,
    nanoseconds: u32,
}
impl From<DateTime<Utc>> for Instant {
    fn from(value: DateTime<Utc>) -> Self {
        Self {
            seconds: value.timestamp(),
            nanoseconds: value.timestamp_subsec_nanos(),
        }
    }
}
impl Instant {
    fn datetime(self) -> Result<DateTime<Utc>> {
        if self.nanoseconds >= 1_000_000_000 {
            return Err(invariant_violation(
                "Plan7 timestamp nanoseconds are invalid",
            ));
        }
        DateTime::from_timestamp(self.seconds, self.nanoseconds)
            .ok_or_else(|| invariant_violation("Plan7 timestamp is out of range"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImmutableObjectWitness {
    path: String,
    byte_size: u64,
    sha256: String,
}
impl ImmutableObjectWitness {
    fn validate(&self, expected_path: &str, limit: usize) -> Result<()> {
        if self.path != expected_path || self.byte_size == 0 || self.byte_size > limit as u64 {
            return Err(invariant_violation(
                "Plan7 immutable object path or size differs",
            ));
        }
        raw_digest(&self.sha256)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Present,
    Absent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Target {
    Present {
        current_pointer_path: String,
        current_pointer_raw_b64: String,
        current_pointer_sha256: String,
        current_pointer_version: String,
        writer_epoch: u64,
        reclamation_generation: u64,
        manifest: ImmutableObjectWitness,
    },
    Absent {
        current_pointer_path: String,
        absence_marker: String,
        observed_writer_epoch: u64,
        observed_reclamation_generation: u64,
        source_parent_writer_epoch: u64,
        source_parent_reclamation_generation: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanFields {
    record_type: String,
    version: u32,
    implementation: String,
    scope: StateScope,
    identity: RestoreAttemptIdentity,
    source: PersistedAuthorityReference,
    source_reference_sha256: String,
    source_manifest: ImmutableObjectWitness,
    source_kv_root_b64: String,
    source_active_id_root_b64: String,
    source_delivery_order_root_b64: String,
    source_logical_sequence: u64,
    source_history_sha256: String,
    source_retention_deadline: Instant,
    durable_authority_binding: DurableAuthorityBinding,
    workspace_request_sha256: String,
    requested_at: Instant,
    execution_deadline: Instant,
    source_deadline: Instant,
    mode: Mode,
    target: Target,
    base_logical_sequence: u64,
    base_history_sha256: String,
    base_kv_root_b64: String,
    base_active_id_root_b64: String,
    base_delivery_order_root_b64: String,
    result_logical_sequence: u64,
    restore_request_digest: String,
    logical_commit_id: String,
    restore_notice_intent_id: String,
    restore_notice_payload_b64: String,
    owner_generation: u64,
    candidate_seed_sha256: String,
    candidate_id: String,
}

/// Immutable authority-8 restore plan with frozen logical and physical identities.
///
/// Decoding validates its internal bindings; it does not make its source readable
/// or grant permission to publish. Those require the selected workspace attempt
/// and authenticated storage witnesses in the live restore invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlMvpRestorePlanV7 {
    fields: PlanFields,
}
impl Serialize for ControlMvpRestorePlanV7 {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        self.fields.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for ControlMvpRestorePlanV7 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let plan = Self {
            fields: PlanFields::deserialize(deserializer)?,
        };
        plan.validate_shape().map_err(serde::de::Error::custom)?;
        Ok(plan)
    }
}

fn raw_digest(value: &str) -> Result<&str> {
    value
        .strip_prefix("sha256:")
        .filter(|raw| valid_raw_digest(raw))
        .ok_or_else(|| invariant_violation("Plan7 digest is not canonical prefixed SHA-256"))
}
fn binary(value: &str) -> Result<Vec<u8>> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| invariant_violation("Plan7 binary is not unpadded base64url"))?;
    if URL_SAFE_NO_PAD.encode(&bytes) != value {
        return Err(invariant_violation("Plan7 binary is not canonical"));
    }
    Ok(bytes)
}
fn jcs<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_jcs::to_vec(value).map_err(|error| CatalogError::Serialization {
        message: format!("Plan7 canonical serialization failed: {error}"),
    })
}
fn tagged<T: Serialize>(tag: &[u8], body: &T) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(tag);
    hash.update([0]);
    let bytes = jcs(body)?;
    #[cfg(any(test, feature = "test-utils"))]
    super::super::record_sha256_work(tag.len() + 1 + bytes.len());
    hash.update(bytes);
    Ok(format!("sha256:{}", hex::encode(hash.finalize())))
}

impl ControlMvpRestorePlanV7 {
    /// Returns the exact retained authority source.
    #[must_use]
    pub const fn source(&self) -> &PersistedAuthorityReference {
        &self.fields.source
    }
    /// Returns the immutable selected participant attempt identity.
    #[must_use]
    pub const fn identity(&self) -> &RestoreAttemptIdentity {
        &self.fields.identity
    }

    pub(crate) fn validate_source_authority_format(&self) -> Result<()> {
        self.validate_shape()
    }

    fn logical_request(&self) -> Result<logical_v2::RestoreRequest> {
        let p = &self.fields;
        logical_v2::RestoreRequest::new(
            &p.scope,
            match p.mode {
                Mode::Present => logical_v2::RestoreMode::Present,
                Mode::Absent => logical_v2::RestoreMode::Absent,
            },
            p.source_logical_sequence,
            raw_digest(&p.source_history_sha256)?,
            p.base_logical_sequence,
            raw_digest(&p.base_history_sha256)?,
            p.identity.restore_id(),
            p.requested_at.seconds,
            p.requested_at.nanoseconds,
        )
    }

    #[allow(
        clippy::too_many_lines,
        reason = "validate the frozen Plan7 fields together before admitting any witness"
    )]
    fn validate_shape(&self) -> Result<()> {
        let p = &self.fields;
        p.scope.validate()?;
        p.source.validate()?;
        let identity = RestoreAttemptIdentity::new(
            p.identity.restore_id(),
            p.identity.attempt(),
            p.identity.domain(),
        )?;
        let paths = ControlMvpPaths::new(p.scope.domain());
        let requested_at = p.requested_at.datetime()?;
        if p.record_type != "control_mvp_restore_plan"
            || p.version != 7
            || p.implementation != IMPLEMENTATION
            || p.identity != identity
            || p.identity.domain() != p.scope.domain()
            || p.source.scope() != &p.scope
            || p.source.implementation() != IMPLEMENTATION
            || p.source.reference_kind() != PersistedAuthorityKind::StateToken
            || p.source.manifest_path() != paths.manifest_object(p.source.manifest_id())
            || p.source_logical_sequence != p.source.logical_sequence()
            || p.source_manifest.sha256 != p.source.manifest_sha256()
            || p.source_reference_sha256
                != prefixed_sha256(&encode_json(&p.source, "Plan7 source reference")?)
            || p.source_retention_deadline.datetime()? != p.source.retention_deadline()
            || p.source_deadline != p.source_retention_deadline
            || requested_at.checked_add_signed(Duration::hours(24))
                != Some(p.execution_deadline.datetime()?)
            || p.owner_generation != p.identity.attempt()
            || p.result_logical_sequence != p.base_logical_sequence.checked_add(1).unwrap_or(0)
        {
            return Err(invariant_violation(
                "Plan7 envelope, source, request window, or owner differs",
            ));
        }
        p.source_manifest
            .validate(p.source.manifest_path(), MAX_CONTROL_JSON_BYTES)?;
        raw_digest(&p.workspace_request_sha256)?;
        for root in [
            &p.source_kv_root_b64,
            &p.source_active_id_root_b64,
            &p.source_delivery_order_root_b64,
            &p.base_kv_root_b64,
            &p.base_active_id_root_b64,
            &p.base_delivery_order_root_b64,
        ] {
            if binary(root)?.is_empty() {
                return Err(invariant_violation("Plan7 root is empty"));
            }
        }
        match &p.target {
            Target::Present {
                current_pointer_path,
                current_pointer_raw_b64,
                current_pointer_sha256,
                current_pointer_version,
                writer_epoch,
                reclamation_generation,
                manifest,
            } => {
                let raw = binary(current_pointer_raw_b64)?;
                if p.mode != Mode::Present
                    || current_pointer_path != &paths.current_pointer()
                    || current_pointer_version.is_empty()
                    || raw.is_empty()
                    || raw.len() > 64 * 1024
                    || prefixed_sha256(&raw) != *current_pointer_sha256
                {
                    return Err(invariant_violation("Plan7 present HEAD witness differs"));
                }
                let pointer: ControlMvpPointer = decode_json(&raw, "Plan7 present HEAD")?;
                pointer.validate_versioned(&p.scope, 8)?;
                manifest.validate(
                    &paths.manifest_object(&pointer.manifest_id),
                    MAX_CONTROL_JSON_BYTES,
                )?;
                if raw_digest(&manifest.sha256)? != pointer.manifest_checksum_sha256
                    || pointer.logical_sequence != p.base_logical_sequence
                    || pointer.writer_epoch != *writer_epoch
                    || pointer.reclamation_generation != *reclamation_generation
                {
                    return Err(invariant_violation("Plan7 target pointer fields differ"));
                }
            }
            Target::Absent {
                current_pointer_path,
                absence_marker,
                observed_writer_epoch,
                observed_reclamation_generation,
                source_parent_writer_epoch,
                source_parent_reclamation_generation,
            } => {
                if p.mode != Mode::Absent
                    || current_pointer_path != &paths.current_pointer()
                    || absence_marker != "does_not_exist"
                    || *observed_writer_epoch == u64::MAX
                    || observed_writer_epoch < source_parent_writer_epoch
                    || observed_reclamation_generation < source_parent_reclamation_generation
                    || p.base_logical_sequence != p.source_logical_sequence
                    || p.base_history_sha256 != p.source_history_sha256
                    || p.base_kv_root_b64 != p.source_kv_root_b64
                    || p.base_active_id_root_b64 != p.source_active_id_root_b64
                    || p.base_delivery_order_root_b64 != p.source_delivery_order_root_b64
                {
                    return Err(invariant_violation(
                        "Plan7 absent target does not inherit its source",
                    ));
                }
            }
        }
        let request = self.logical_request()?;
        let commit = logical_v2::commit_id(
            &p.scope,
            request.prior_history(),
            request.result_sequence(),
            &request.operation()?,
        )?;
        let notice = request.notice(&commit)?;
        if raw_digest(&p.restore_request_digest)? != request.request_digest()?
            || raw_digest(&p.logical_commit_id)? != commit
            || p.restore_notice_intent_id != notice.intent_id()
            || binary(&p.restore_notice_payload_b64)? != notice.payload()
            || self.candidate_seed()? != p.candidate_seed_sha256
            || raw_digest(&p.candidate_seed_sha256)? != p.candidate_id
        {
            return Err(invariant_violation(
                "Plan7 frozen logical or physical identity differs",
            ));
        }
        Ok(())
    }

    fn validate_roots(&self, store: &ControlMvpStateStore) -> Result<()> {
        if store.scope != self.fields.scope
            || store.durable_authority8_binding()? != self.fields.durable_authority_binding
        {
            return Err(invariant_violation(
                "Plan7 scope or durable location differs from trusted configuration",
            ));
        }
        let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
        for root in [
            &self.fields.source_kv_root_b64,
            &self.fields.source_active_id_root_b64,
            &self.fields.source_delivery_order_root_b64,
            &self.fields.base_kv_root_b64,
            &self.fields.base_active_id_root_b64,
            &self.fields.base_delivery_order_root_b64,
        ] {
            directory.decode_root(&binary(root)?)?;
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct CandidateSeedBody<'a> {
    scope: &'a StateScope,
    identity: &'a RestoreAttemptIdentity,
    workspace_request_sha256: &'a str,
    durable_authority_binding: &'a DurableAuthorityBinding,
    source_reference_sha256: &'a str,
    source_manifest_sha256: &'a str,
    source_kv_root_b64: &'a str,
    source_active_id_root_b64: &'a str,
    source_delivery_order_root_b64: &'a str,
    source_logical_sequence: &'a u64,
    source_history_sha256: &'a str,
    requested_at: &'a Instant,
    execution_deadline: &'a Instant,
    source_deadline: &'a Instant,
    mode: &'a Mode,
    target_witness_digest: &'a str,
    base_kv_root_b64: &'a str,
    base_active_id_root_b64: &'a str,
    base_delivery_order_root_b64: &'a str,
    base_logical_sequence: &'a u64,
    base_history_sha256: &'a str,
    result_logical_sequence: &'a u64,
    restore_request_digest: &'a str,
    logical_commit_id: &'a str,
    restore_notice_intent_id: &'a str,
    restore_notice_payload_sha256: &'a str,
    owner_generation: &'a u64,
}

impl ControlMvpRestorePlanV7 {
    pub(crate) fn validate_workspace_request(
        &self,
        request_sha256: &str,
        requested_at: DateTime<Utc>,
    ) -> Result<()> {
        let deadline = requested_at
            .checked_add_signed(Duration::hours(24))
            .ok_or_else(|| invariant_violation("restore execution deadline overflow"))?;
        if self.fields.workspace_request_sha256 != request_sha256
            || self.fields.requested_at != Instant::from(requested_at)
            || self.fields.execution_deadline != Instant::from(deadline)
        {
            return Err(CatalogError::Validation {
                message: "Plan7 differs from original workspace request".into(),
            });
        }
        Ok(())
    }

    fn candidate_seed(&self) -> Result<String> {
        let p = &self.fields;
        let target = tagged(b"arco/control-v2/restore-target-witness-v1", &p.target)?;
        let payload = prefixed_sha256(&binary(&p.restore_notice_payload_b64)?);
        tagged(
            b"arco/control-v2/restore-candidate-v1",
            &CandidateSeedBody {
                scope: &p.scope,
                identity: &p.identity,
                workspace_request_sha256: &p.workspace_request_sha256,
                durable_authority_binding: &p.durable_authority_binding,
                source_reference_sha256: &p.source_reference_sha256,
                source_manifest_sha256: &p.source_manifest.sha256,
                source_kv_root_b64: &p.source_kv_root_b64,
                source_active_id_root_b64: &p.source_active_id_root_b64,
                source_delivery_order_root_b64: &p.source_delivery_order_root_b64,
                source_logical_sequence: &p.source_logical_sequence,
                source_history_sha256: &p.source_history_sha256,
                requested_at: &p.requested_at,
                execution_deadline: &p.execution_deadline,
                source_deadline: &p.source_deadline,
                mode: &p.mode,
                target_witness_digest: &target,
                base_kv_root_b64: &p.base_kv_root_b64,
                base_active_id_root_b64: &p.base_active_id_root_b64,
                base_delivery_order_root_b64: &p.base_delivery_order_root_b64,
                base_logical_sequence: &p.base_logical_sequence,
                base_history_sha256: &p.base_history_sha256,
                result_logical_sequence: &p.result_logical_sequence,
                restore_request_digest: &p.restore_request_digest,
                logical_commit_id: &p.logical_commit_id,
                restore_notice_intent_id: &p.restore_notice_intent_id,
                restore_notice_payload_sha256: &payload,
                owner_generation: &p.owner_generation,
            },
        )
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one frozen Plan7 literal binds the authenticated source and target observations"
)]
pub(in super::super) async fn plan(
    store: &ControlMvpStateStore,
    source: &PersistedAuthorityReference,
    identity: &RestoreAttemptIdentity,
    context: &mut RestorePlanningContext<'_>,
) -> Result<ControlMvpRestorePlanV7> {
    if store.authority_format != 8 {
        return Err(CatalogError::UnsupportedAuthorityFormat {
            message: "Plan7 requires explicit synthetic authority 8".into(),
        });
    }
    let binding = store.durable_authority8_binding()?;
    let observed_now = context.observed_now();
    source.validate()?;
    RestoreAttemptIdentity::new(identity.restore_id(), identity.attempt(), identity.domain())?;
    raw_digest(context.workspace_request_sha256())?;
    if source.manifest_path() != store.paths.manifest_object(source.manifest_id())
        || context
            .requested_at()
            .checked_add_signed(Duration::hours(24))
            != Some(context.execution_deadline())
        || source.scope() != &store.scope
        || source.implementation() != IMPLEMENTATION
        || source.reference_kind() != PersistedAuthorityKind::StateToken
        || identity.domain() != store.scope.domain()
        || context.requested_at() > observed_now
        || observed_now >= context.execution_deadline()
        || observed_now >= source.retention_deadline()
    {
        return Err(invariant_violation(
            "Plan7 source, scope, or original request window differs",
        ));
    }
    let (source_base, source_size) =
        retained::restore_source_with_workspace_io(store, source, context.io()).await?;
    // Source authentication pins HEAD when this source is current. Reuse that
    // exact observation rather than independently selecting a different target.
    let (base, target_size) = if source_base.pointer_version.is_some() {
        (source_base.clone(), source_size)
    } else {
        let mut budget = retained::ReadBudget::with_workspace(context.io().workspace_budget());
        let Some((pointer, version, bytes)) =
            retained::load_current_pointer(store, &mut budget).await?
        else {
            return Err(CatalogError::UnsupportedOperation {
                message: "absent Plan7 target requires an observed external fence witness".into(),
            });
        };
        let manifest_bytes = retained::read_bounded_json(
            store,
            &mut budget,
            &store.paths.manifest_object(&pointer.manifest_id),
            MAX_CONTROL_JSON_BYTES,
            "Plan7 target manifest",
        )
        .await?;
        let token = store
            .token(pointer.manifest_id.clone(), pointer.logical_sequence)
            .with_manifest_witness(pointer.manifest_checksum_sha256.clone());
        let size = manifest_bytes.len();
        let base = store
            .read_bounded_current_from_manifest(
                &token,
                &manifest_bytes,
                &pointer,
                version,
                bytes,
                &mut budget,
            )
            .await?;
        (base, size)
    };
    let source_manifest = source_base
        .manifest
        .as_ref()
        .ok_or_else(|| invariant_violation("Plan7 source lacks manifest"))?;
    let base_manifest = base
        .manifest
        .as_ref()
        .ok_or_else(|| invariant_violation("Plan7 target lacks manifest"))?;
    let pointer_bytes = base
        .pointer_bytes
        .as_ref()
        .ok_or_else(|| invariant_violation("Plan7 target lacks exact HEAD bytes"))?;
    let version = base
        .pointer_version
        .clone()
        .ok_or_else(|| invariant_violation("Plan7 target lacks exact HEAD version"))?;
    let request = logical_v2::RestoreRequest::new(
        &store.scope,
        logical_v2::RestoreMode::Present,
        source_manifest.logical_sequence,
        &source_manifest.logical_history,
        base_manifest.logical_sequence,
        &base_manifest.logical_history,
        identity.restore_id(),
        context.requested_at().timestamp(),
        context.requested_at().timestamp_subsec_nanos(),
    )?;
    let commit = logical_v2::commit_id(
        &store.scope,
        request.prior_history(),
        request.result_sequence(),
        &request.operation()?,
    )?;
    let notice = request.notice(&commit)?;
    let mut plan = ControlMvpRestorePlanV7 {
        fields: PlanFields {
            record_type: "control_mvp_restore_plan".into(),
            version: 7,
            implementation: IMPLEMENTATION.into(),
            scope: store.scope.clone(),
            identity: identity.clone(),
            source: source.clone(),
            source_reference_sha256: prefixed_sha256(&encode_json(
                source,
                "Plan7 source reference",
            )?),
            source_manifest: ImmutableObjectWitness {
                path: source.manifest_path().into(),
                byte_size: source_size as u64,
                sha256: source.manifest_sha256().into(),
            },
            source_kv_root_b64: URL_SAFE_NO_PAD.encode(source_base.kv_root.encode()),
            source_active_id_root_b64: URL_SAFE_NO_PAD.encode(source_base.active_id_root.encode()),
            source_delivery_order_root_b64: URL_SAFE_NO_PAD
                .encode(source_base.delivery_order_root.encode()),
            source_logical_sequence: source_manifest.logical_sequence,
            source_history_sha256: format!("sha256:{}", source_manifest.logical_history),
            source_retention_deadline: source.retention_deadline().into(),
            durable_authority_binding: binding,
            workspace_request_sha256: context.workspace_request_sha256().into(),
            requested_at: context.requested_at().into(),
            execution_deadline: context.execution_deadline().into(),
            source_deadline: source.retention_deadline().into(),
            mode: Mode::Present,
            target: Target::Present {
                current_pointer_path: store.paths.current_pointer(),
                current_pointer_raw_b64: URL_SAFE_NO_PAD.encode(pointer_bytes),
                current_pointer_sha256: prefixed_sha256(pointer_bytes),
                current_pointer_version: version,
                writer_epoch: base.writer_epoch,
                reclamation_generation: base.reclamation_generation,
                manifest: ImmutableObjectWitness {
                    path: store.paths.manifest_object(&base_manifest.manifest_id),
                    byte_size: target_size as u64,
                    sha256: format!(
                        "sha256:{}",
                        base.manifest_digest
                            .as_deref()
                            .ok_or_else(|| invariant_violation(
                                "Plan7 target lacks manifest digest"
                            ))?
                    ),
                },
            },
            base_logical_sequence: base_manifest.logical_sequence,
            base_history_sha256: format!("sha256:{}", base_manifest.logical_history),
            base_kv_root_b64: URL_SAFE_NO_PAD.encode(base.kv_root.encode()),
            base_active_id_root_b64: URL_SAFE_NO_PAD.encode(base.active_id_root.encode()),
            base_delivery_order_root_b64: URL_SAFE_NO_PAD.encode(base.delivery_order_root.encode()),
            result_logical_sequence: request.result_sequence(),
            restore_request_digest: format!("sha256:{}", request.request_digest()?),
            logical_commit_id: format!("sha256:{commit}"),
            restore_notice_intent_id: notice.intent_id().into(),
            restore_notice_payload_b64: URL_SAFE_NO_PAD.encode(notice.payload()),
            owner_generation: identity.attempt(),
            candidate_seed_sha256: String::new(),
            candidate_id: String::new(),
        },
    };
    plan.fields.candidate_seed_sha256 = plan.candidate_seed()?;
    plan.fields.candidate_id = raw_digest(&plan.fields.candidate_seed_sha256)?.into();
    plan.validate_shape()?;
    plan.validate_roots(store)?;
    if jcs(&plan)?.len() > MAX_CONTROL_JSON_BYTES {
        return Err(CatalogError::MaintenanceBackpressure {
            message: "Plan7 exceeds 4 MiB".into(),
        });
    }
    Ok(plan)
}

#[allow(
    clippy::large_futures,
    reason = "native bounded driver retains accounted fixed products on stack"
)]
pub(in super::super) async fn advance(
    store: &ControlMvpStateStore,
    plan: &super::super::PersistedRestoreParticipantPlan,
    context: &mut crate::state_store::RestoreAdvanceContext<'_>,
) -> Result<crate::state_store::RestoreParticipantAdvance> {
    units::advance(store, plan, context).await
}

#[allow(
    clippy::too_many_lines,
    reason = "keep exact HEAD supersession ordering and selected-progress admission together"
)]
pub(in super::super) async fn inspect(
    store: &ControlMvpStateStore,
    persisted: &super::super::PersistedRestoreParticipantPlan,
    context: &mut crate::state_store::RestoreBoundedInspectionContext<'_>,
) -> Result<super::super::RestoreParticipantInspection> {
    use super::super::{PersistedRestoreParticipantPlan, RestoreParticipantInspection};
    if store.authority_format != 8 {
        return Err(CatalogError::UnsupportedAuthorityFormat {
            message: "Plan7 inspection requires explicit synthetic authority 8".into(),
        });
    }
    let PersistedRestoreParticipantPlan::ControlMvpV7(plan) = persisted else {
        return Err(CatalogError::UnsupportedOperation {
            message: "bounded restore inspection requires Plan7".into(),
        });
    };
    plan.validate_shape()?;
    plan.validate_roots(store)?;
    let p = &plan.fields;
    if context.restore_id() != p.identity.restore_id()
        || context.participant_attempt() != p.identity.attempt()
        || context.domain() != p.identity.domain()
        || context.plan_sha256() != prefixed_sha256(&jcs(persisted)?)
    {
        return Err(invariant_violation(
            "bounded inspection capability names another Plan7",
        ));
    }
    let prefix = format!(
        "{}/restore/v7/{}",
        store.paths.base_prefix(),
        p.candidate_id
    );
    let selected_digest = context.plan_sha256().to_owned();
    let io = context.io().workspace_budget();
    if io
        .head(&store.retention, &format!("{prefix}/prepared.json"))
        .await?
        .is_some()
    {
        return Err(CatalogError::AmbiguousAuthorityOutcome {
            message: "prepared Plan7 requires exact candidate reconciliation".into(),
        });
    }
    let has_progress = io
        .head(&store.retention, &format!("{prefix}/selector.json"))
        .await?
        .is_some();
    let Target::Present {
        current_pointer_raw_b64,
        current_pointer_version,
        manifest,
        ..
    } = &p.target
    else {
        return Err(CatalogError::UnsupportedOperation {
            message: "absent Plan7 inspection requires its synthetic fence observer".into(),
        });
    };
    {
        let mut budget = retained::ReadBudget::with_workspace(io);
        let Some((pointer, version, bytes)) =
            retained::load_current_pointer(store, &mut budget).await?
        else {
            return Err(CatalogError::AmbiguousAuthorityOutcome {
                message: "present Plan7 HEAD disappeared".into(),
            });
        };
        if version != *current_pointer_version || bytes.as_ref() != binary(current_pointer_raw_b64)?
        {
            return Ok(RestoreParticipantInspection::Superseded);
        }
        let manifest_bytes = retained::read_bounded_json(
            store,
            &mut budget,
            &store.paths.manifest_object(&pointer.manifest_id),
            MAX_CONTROL_JSON_BYTES,
            "Plan7 inspected target manifest",
        )
        .await?;
        if manifest_bytes.len() as u64 != manifest.byte_size {
            return Err(invariant_violation(
                "Plan7 target manifest size differs from witness",
            ));
        }
        let token = store
            .token(pointer.manifest_id.clone(), pointer.logical_sequence)
            .with_manifest_witness(pointer.manifest_checksum_sha256.clone());
        let base = store
            .read_bounded_current_from_manifest(
                &token,
                &manifest_bytes,
                &pointer,
                version.clone(),
                bytes.clone(),
                &mut budget,
            )
            .await?;
        plan.verify_inspected_manifest_roots(store, &base, &mut budget)
            .await?;
    }
    if has_progress {
        units::inspect_progress(store, plan, &selected_digest, io).await?;
    }
    Ok(RestoreParticipantInspection::Ready)
}

impl ControlMvpRestorePlanV7 {
    async fn verify_inspected_manifest_roots(
        &self,
        store: &ControlMvpStateStore,
        base: &super::Base,
        budget: &mut retained::ReadBudget<'_>,
    ) -> Result<()> {
        let p = &self.fields;
        let manifest = base
            .manifest
            .as_ref()
            .ok_or_else(|| invariant_violation("Plan7 target manifest is absent"))?;
        if manifest.logical_sequence != p.base_logical_sequence
            || manifest.logical_history != raw_digest(&p.base_history_sha256)?
            || base.kv_root.encode() != binary(&p.base_kv_root_b64)?
            || base.active_id_root.encode() != binary(&p.base_active_id_root_b64)?
            || base.delivery_order_root.encode() != binary(&p.base_delivery_order_root_b64)?
        {
            return Err(invariant_violation(
                "Plan7 base fields differ from authenticated target",
            ));
        }
        let source_bytes = retained::read_bounded_json(
            store,
            budget,
            &p.source_manifest.path,
            MAX_CONTROL_JSON_BYTES,
            "Plan7 inspected source manifest",
        )
        .await?;
        if source_bytes.len() as u64 != p.source_manifest.byte_size
            || prefixed_sha256(&source_bytes) != p.source_manifest.sha256
        {
            return Err(invariant_violation("Plan7 source manifest witness differs"));
        }
        let source_manifest: super::Manifest8 =
            decode_json(&source_bytes, "Plan7 source manifest")?;
        source_manifest.validate(&store.scope, p.source.manifest_id())?;
        let directory = directory::Directory::new(store.retention.clone(), &store.scope)?;
        let roots = super::decode_roots(&directory, &source_manifest)?;
        if source_manifest.logical_sequence != p.source_logical_sequence
            || source_manifest.logical_history != raw_digest(&p.source_history_sha256)?
            || roots.0.encode() != binary(&p.source_kv_root_b64)?
            || roots.1.encode() != binary(&p.source_active_id_root_b64)?
            || roots.2.encode() != binary(&p.source_delivery_order_root_b64)?
        {
            return Err(invariant_violation(
                "Plan7 source fields differ from authenticated manifest",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::indexing_slicing)]
    use super::*;

    #[tokio::test]
    async fn native_counter_capture_plan7_identity_and_six_roots() {
        use super::super::super::physical::restore_io::{
            RestorePhysicalIo, RestorePhysicalRoute, UnitPayloadAdmission, decode_with_reservation,
        };
        let (store, plan) = inspection_fixture().await;
        let mut io = RestorePhysicalIo::new(&store, 64 * 1024 * 1024, 0);
        let mut workspace = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut payload = UnitPayloadAdmission::new();
        let mut route = RestorePhysicalRoute::OrdinaryUnit {
            workspace: &mut workspace,
            payload: &mut payload,
        };
        drop(
            decode_with_reservation(&mut io, &mut route, Some(1024 * 1024), || {
                plan.validate_shape()?;
                plan.validate_roots(&store)
            })
            .expect("existing fixture shape and six root codecs"),
        );
        let work = io.native_work_evidence();
        assert!(work.slots[0] > 0);
        assert!(work.slots[1] > 0);
        assert_eq!(work.bounded.directory_references, 6);
        assert_eq!(work.slots[13..32], [0; 19]);
        // The present-target validator decodes its authenticated HEAD JSON.
        assert!(work.slots[32] > 0);
        assert!(work.slots[33] > 0);
        assert!(!work.overflow);
        println!("native-parity plan7 {work:?}");
    }

    pub(super) async fn inspection_fixture() -> (ControlMvpStateStore, ControlMvpRestorePlanV7) {
        inspection_fixture_for_domain("catalog").await
    }

    pub(super) async fn inspection_fixture_for_domain(
        domain: &str,
    ) -> (ControlMvpStateStore, ControlMvpRestorePlanV7) {
        let storage = arco_core::ScopedStorage::new(
            std::sync::Arc::new(arco_core::MemoryBackend::new()),
            "tenant",
            "workspace",
        )
        .expect("storage");
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage,
            StateScope::new("tenant", "workspace", domain),
        )
        .expect("store")
        .with_durable_authority_binding(DurableAuthorityBinding::new([39; 32]));
        inspection_fixture_on_store(store).await
    }

    pub(super) async fn inspection_fixture_on_store(
        store: ControlMvpStateStore,
    ) -> (ControlMvpStateStore, ControlMvpRestorePlanV7) {
        use crate::state_store::{ArcoStateTxn, PersistedAuthorityAdapter, TxnOptions};
        let domain = store.scope.domain();
        let mut txn = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("txn");
        txn.set_logical_operation("inspection", "test", &"ab".repeat(32))
            .expect("operation");
        txn.put(b"key", bytes::Bytes::from_static(b"value"))
            .await
            .expect("put");
        let token = txn.commit_v2().await.expect("commit").token().clone();
        // Keep numeric and RFC3339 timestamp widths identical across
        // default/feature counter fixtures while retaining a live deadline.
        let now = chrono::Timelike::with_nanosecond(&Utc::now(), 123_456_789)
            .expect("fixed fixture timestamp precision");
        let source = store
            .persist_state_reference(&token, now + Duration::days(2))
            .await
            .expect("source");
        let identity =
            RestoreAttemptIdentity::new(format!("rst_{}", ulid::Ulid::from(709_u128)), 1, domain)
                .expect("identity");
        let mut budget = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut context = RestorePlanningContext::new(
            prefixed_sha256(b"request"),
            now,
            now + Duration::hours(24),
            now,
            crate::workspace_io_budget::WorkspaceCaptureIo::new(&store.retention, &mut budget),
        );
        let plan = plan(&store, &source, &identity, &mut context)
            .await
            .expect("plan");
        (store, plan)
    }

    async fn inspect_fixture(
        store: &ControlMvpStateStore,
        plan: ControlMvpRestorePlanV7,
    ) -> (
        Result<super::super::super::RestoreParticipantInspection>,
        usize,
    ) {
        let identity = plan.fields.identity.clone();
        let persisted =
            super::super::super::PersistedRestoreParticipantPlan::ControlMvpV7(Box::new(plan));
        let mut budget = crate::workspace_io_budget::WorkspaceIoBudget::new();
        let mut context = crate::state_store::RestoreBoundedInspectionContext::new(
            identity.restore_id().into(),
            identity.attempt(),
            identity.domain().into(),
            prefixed_sha256(&jcs(&persisted).expect("JCS")),
            crate::workspace_io_budget::WorkspaceCaptureIo::new(&store.retention, &mut budget),
        );
        let result = inspect(store, &persisted, &mut context).await;
        (result, budget.test_accounting().1)
    }

    #[tokio::test]
    async fn plan7_inspection_rejects_forged_target_manifest_size() {
        let (store, mut plan) = inspection_fixture().await;
        let Target::Present { manifest, .. } = &mut plan.fields.target else {
            panic!("present")
        };
        manifest.byte_size += 1;
        plan.fields.candidate_seed_sha256 = plan.candidate_seed().expect("seed");
        plan.fields.candidate_id = raw_digest(&plan.fields.candidate_seed_sha256)
            .expect("digest")
            .into();
        plan.validate_shape()
            .expect("internally consistent forgery");
        assert!(
            inspect_fixture(&store, plan).await.0.is_err(),
            "forged target size became Ready"
        );
    }

    #[tokio::test]
    async fn plan7_inspection_classifies_changed_head_without_successor_reads() {
        let (store, plan) = inspection_fixture().await;
        let Target::Present {
            current_pointer_raw_b64,
            current_pointer_version,
            ..
        } = &plan.fields.target
        else {
            panic!("present")
        };
        let mut pointer: ControlMvpPointer =
            decode_json(&binary(current_pointer_raw_b64).expect("raw"), "pointer")
                .expect("pointer");
        pointer.manifest_id = "missing-successor".into();
        pointer.manifest_checksum_sha256 = "ab".repeat(32);
        pointer.logical_sequence += 1;
        store
            .storage
            .put(
                &store.paths.current_pointer(),
                encode_json(&pointer, "successor").expect("encode"),
                arco_core::AuthorityWritePrecondition::MatchesVersion(
                    current_pointer_version.clone(),
                ),
            )
            .await
            .expect("advance HEAD");
        let (result, operations) = inspect_fixture(&store, plan).await;
        assert!(
            matches!(
                result,
                Ok(super::super::super::RestoreParticipantInspection::Superseded)
            ),
            "{result:?}"
        );
        assert_eq!(
            operations, 5,
            "inspection dereferenced successor after exact CAS lost"
        );
    }

    #[test]
    fn physical_hash_codec_matches_independent_frozen_vectors() {
        // Hash-codec proof only: these ASCII fixtures are deliberately not
        // authenticated Plan7 records or directory/transition witnesses.
        let fixture: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/restore_plan7_physical_hash_vectors.json"
        )))
        .expect("independent vectors");
        let vectors = fixture["vectors"].as_object().expect("vectors");
        assert_eq!(vectors.len(), 8);
        for (name, vector) in vectors {
            let tag = vector["tag"].as_str().expect("tag");
            assert_eq!(
                jcs(&vector["body"]).expect("JCS"),
                vector["jcs_utf8"].as_str().expect("JCS vector").as_bytes(),
                "{name}"
            );
            assert_eq!(
                tagged(tag.as_bytes(), &vector["body"]).expect("hash"),
                format!("sha256:{}", vector["sha256_hex"].as_str().expect("digest")),
                "{name}"
            );
        }
    }
}

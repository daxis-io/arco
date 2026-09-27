//! Authority-8 selected retained-source records.

#[cfg(test)]
use super::put_immutable_matching;
use super::{
    CatalogError, ControlMvpRetainedReader, ControlMvpRetainedSource, ControlMvpStateStore,
    DurableAuthorityBinding, MAX_CONTROL_JSON_BYTES, PersistedAuthorityKind,
    PersistedAuthorityReference, Result, StateScope, decode_json, encode_json_limited,
    invariant_violation, prefixed_sha256, sha256_hex, validation_failed,
};
use crate::state_store::{PreparedRetainedSource, RetainedSourceCaptureContext};
#[cfg(test)]
use arco_core::{AuthorityWritePrecondition, WriteResult};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const POINTER_RECORD_TYPE: &str = "arco.retained-source.pointer";
const DESCRIPTOR_RECORD_TYPE: &str = "arco.retained-source.descriptor";
const RECORD_VERSION: u32 = 1;
const POINTER_LIMIT: usize = 64 * 1024;
const DESCRIPTOR_LIMIT: usize = 4 * 1024 * 1024;
const DIRECTORY_PAGE_LIMIT: usize = 64 * 1024;
const DIRECTORY_DEPTH: usize = 8;
const READ_OBJECT_LIMIT: usize = 16;
const READ_BYTE_LIMIT: usize = 8 * 1024 * 1024;

/// Private prepared path-copy publication. Only authenticated capture constructs it.
pub struct RetainedSourcePublication {
    store: ControlMvpStateStore,
    reference: PersistedAuthorityReference,
    state: PublicationState,
}

enum PublicationState {
    Deferred {
        operation_id: String,
        capture_epoch: u64,
    },
    Staged(Option<crate::retention_coordination::ArmedRetainedPointerIntent>),
}

/// Definitive pointer result, bound to exact intent bytes and reference key.
pub struct VerifiedRetainedPointer {
    visible: bool,
    expected_pointer_raw_sha256: String,
    reference_key_hex: String,
}

impl VerifiedRetainedPointer {
    pub(crate) fn proves_visible(
        &self,
        intent: &crate::retention_coordination::ArmedRetainedPointerIntent,
    ) -> bool {
        self.visible && self.matches(intent)
    }

    pub(crate) fn matches(
        &self,
        intent: &crate::retention_coordination::ArmedRetainedPointerIntent,
    ) -> bool {
        self.expected_pointer_raw_sha256 == intent.expected_pointer_raw_sha256
            && self.reference_key_hex == intent.reference_key_hex
    }
}

impl RetainedSourcePublication {
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "keep authenticated source, path-copy proof, and intended pointer in one scope"
    )]
    async fn stage(
        store: &ControlMvpStateStore,
        reference: &PersistedAuthorityReference,
        binding: DurableAuthorityBinding,
        operation_id: String,
        capture_epoch: u64,
        epoch: &mut crate::retention_coordination::RetentionMutationEpoch,
        budget: &mut crate::workspace_io_budget::WorkspaceIoBudget,
    ) -> Result<Self> {
        use crate::retention_coordination::{
            ArmedRetainedPointerIntent, RetainedPointerIntentPrecondition,
        };
        if verify_selected_member(store, reference, None, budget)
            .await?
            .is_some()
        {
            return Ok(Self {
                store: store.clone(),
                reference: reference.clone(),
                state: PublicationState::Staged(None),
            });
        }
        let selected = budget
            .read_stable(&store.retention, &pointer_path(store), POINTER_LIMIT)
            .await?;
        let directory =
            super::directory::Directory::retained_reference(store.retention.clone(), &store.scope)?;
        let (old, previous, precondition) = match selected {
            Some((bytes, version)) => {
                let pointer = validate_pointer(store, binding, &bytes)?;
                let root = directory.decode_root(&decode_root_hex(
                    &pointer.directory_root_hex,
                    "retained source pointer",
                )?)?;
                (
                    Some(root),
                    Some(pointer),
                    RetainedPointerIntentPrecondition::MatchesVersion { version },
                )
            }
            None => (
                None,
                None,
                RetainedPointerIntentPrecondition::DoesNotExist {},
            ),
        };
        let descriptor = RetainedDescriptor {
            record_type: DESCRIPTOR_RECORD_TYPE.into(),
            version: RECORD_VERSION,
            reference: reference.clone(),
            reference_raw_sha256: sha256_hex(&reference_bytes(reference)?),
            scope: store.scope.clone(),
            implementation: super::IMPLEMENTATION.into(),
            authority_format: 8,
            binding,
            source_manifest_path: reference.manifest_path().into(),
            source_manifest_id: reference.manifest_id().into(),
            source_manifest_raw_sha256: raw_digest(
                reference.manifest_sha256(),
                "retained source manifest",
            )?
            .into(),
            source_logical_sequence: reference.logical_sequence(),
            retention_deadline: reference.retention_deadline(),
            capture_operation_id: operation_id.clone(),
            capture_epoch,
        };
        validate_descriptor(&descriptor, reference, binding)?;
        let bytes =
            encode_json_limited(&descriptor, DESCRIPTOR_LIMIT, "retained source descriptor")?;
        let digest = sha256_hex(&bytes);
        let key = reference_key(reference)?;
        let leaf = super::directory::Leaf {
            first: key.to_vec(),
            last: key.to_vec(),
            rows: 1,
            bytes: u32::try_from(bytes.len())
                .map_err(|_| invariant_violation("retained descriptor length overflow"))?,
            digest: Sha256::digest(&bytes).into(),
        };
        epoch
            .put_immutable_bounded(&descriptor_path(store, &digest), bytes, budget)
            .await?;
        let root = epoch
            .run_bounded_mutation(async {
                let mut io = super::directory::ReadBudget::with_workspace(budget);
                let old = match old {
                    Some(root) => root,
                    None => directory.empty_root_budgeted(&mut io).await?,
                };
                let existing = directory.lookup(&old, &key, &mut io).await?;
                if let Some(existing) = &existing {
                    if existing.first != key || existing.last != key || existing.rows != 1 {
                        return Err(invariant_violation(
                            "retained insertion is not an exact singleton",
                        ));
                    }
                }
                let edits = [super::directory::update::Edit {
                    old: existing,
                    new: vec![leaf],
                }];
                let root = directory.update(&old, &edits, &mut io).await?;
                directory
                    .verify_update(&old, &root, &edits, &mut io)
                    .await?;
                Ok(root)
            })
            .await?;
        let root_bytes = root.encode();
        let previous_generation = previous.as_ref().map_or(0, |pointer| pointer.generation);
        let pointer = RetainedPointer {
            record_type: POINTER_RECORD_TYPE.into(),
            version: RECORD_VERSION,
            scope: store.scope.clone(),
            binding,
            directory_root_hex: hex::encode(&root_bytes),
            previous_generation,
            previous_root_sha256: previous.map(|pointer| pointer.root_sha256),
            generation: previous_generation
                .checked_add(1)
                .ok_or_else(|| invariant_violation("retained generation overflow"))?,
            root_sha256: sha256_hex(&root_bytes),
            last_operation_id: operation_id,
            capture_epoch,
        };
        let bytes = encode_json_limited(&pointer, POINTER_LIMIT, "retained source pointer")?;
        let intent = ArmedRetainedPointerIntent {
            intent_type: "arco.retained-source.pointer-intent".into(),
            version: 1,
            domain: store.scope.domain().into(),
            pointer_path: pointer_path(store),
            reference_key_hex: hex::encode(key),
            expected_pointer_raw_sha256: sha256_hex(&bytes),
            expected_pointer_json: String::from_utf8(bytes.to_vec())
                .map_err(|_| invariant_violation("retained pointer is not UTF-8"))?,
            precondition,
        };
        Ok(Self {
            store: store.clone(),
            reference: reference.clone(),
            state: PublicationState::Staged(Some(intent)),
        })
    }

    pub(crate) async fn publish(
        self,
        epoch: &mut crate::retention_coordination::RetentionMutationEpoch,
        guard: &mut arco_core::lock::LockGuard<arco_core::ScopedStorage>,
        budget: &mut crate::workspace_io_budget::WorkspaceIoBudget,
    ) -> Result<()> {
        if self.reference.retention_deadline() <= super::cost::now() {
            return Err(validation_failed("prepared retained source has expired"));
        }
        let staged = match self.state {
            PublicationState::Deferred {
                operation_id,
                capture_epoch,
            } => {
                if capture_epoch != epoch.epoch() || operation_id != epoch.operation_id() {
                    return Err(validation_failed(
                        "prepared retained source belongs to another capture epoch",
                    ));
                }
                Self::stage(
                    &self.store,
                    &self.reference,
                    self.store.durable_authority8_binding()?,
                    operation_id,
                    capture_epoch,
                    epoch,
                    budget,
                )
                .await?
            }
            PublicationState::Staged(_) => self,
        };
        let PublicationState::Staged(Some(intent)) = staged.state else {
            return Ok(());
        };
        let pointer = validate_pointer(
            &staged.store,
            staged.store.durable_authority8_binding()?,
            intent.expected_pointer_json.as_bytes(),
        )?;
        if pointer.capture_epoch != epoch.epoch()
            || pointer.last_operation_id != epoch.operation_id()
        {
            return Err(validation_failed(
                "prepared retained source belongs to another capture epoch",
            ));
        }
        epoch
            .arm_retained_pointer(intent.clone(), guard, budget)
            .await?;
        let outcome = epoch.send_retained_pointer(guard, budget).await?;
        match verify_selected_member(&staged.store, &staged.reference, Some(&intent), budget).await
        {
            Ok(Some(proof)) => epoch.clear_retained_pointer(proof, guard, budget).await,
            result
                if outcome
                    == crate::retention_coordination::RetainedPointerSend::PreconditionFailed =>
            {
                let Some((bytes, _)) = budget
                    .read_stable(&staged.store.retention, &intent.pointer_path, POINTER_LIMIT)
                    .await?
                else {
                    return Err(CatalogError::AmbiguousAuthorityOutcome {
                        message: "conflicting retained pointer is no longer observable".into(),
                    });
                };
                if bytes.as_ref() == intent.expected_pointer_json.as_bytes() {
                    return result.and_then(|_| {
                        Err(invariant_violation(
                            "armed retained membership could not be verified",
                        ))
                    });
                }
                validate_pointer(
                    &staged.store,
                    staged.store.durable_authority8_binding()?,
                    &bytes,
                )?;
                // Direct PreconditionFailed proves this send did not publish. The
                // stable foreign pointer is terminal conflict, not visible evidence.
                let proof = VerifiedRetainedPointer {
                    visible: false,
                    expected_pointer_raw_sha256: intent.expected_pointer_raw_sha256.clone(),
                    reference_key_hex: intent.reference_key_hex.clone(),
                };
                epoch.clear_retained_pointer(proof, guard, budget).await?;
                Err(CatalogError::PreconditionFailed {
                    message: "retained pointer publication lost its exact precondition".into(),
                })
            }
            Err(error) => Err(error),
            Ok(None) => Err(CatalogError::AmbiguousAuthorityOutcome {
                message: "armed retained membership is absent".into(),
            }),
        }
    }
}

async fn verify_selected_member(
    store: &ControlMvpStateStore,
    reference: &PersistedAuthorityReference,
    intent: Option<&crate::retention_coordination::ArmedRetainedPointerIntent>,
    budget: &mut crate::workspace_io_budget::WorkspaceIoBudget,
) -> Result<Option<VerifiedRetainedPointer>> {
    let selected = budget
        .read_stable(&store.retention, &pointer_path(store), POINTER_LIMIT)
        .await?;
    let Some((bytes, _)) = selected else {
        return if intent.is_none() {
            Ok(None)
        } else {
            Err(CatalogError::AmbiguousAuthorityOutcome {
                message: "armed retained pointer is absent".into(),
            })
        };
    };
    if intent.is_some_and(|intent| bytes.as_ref() != intent.expected_pointer_json.as_bytes()) {
        return Err(CatalogError::AmbiguousAuthorityOutcome {
            message: "armed retained pointer differs from exact intended bytes".into(),
        });
    }
    let binding = store.durable_authority8_binding()?;
    let pointer = validate_pointer(store, binding, &bytes)?;
    let directory =
        super::directory::Directory::retained_reference(store.retention.clone(), &store.scope)?;
    let root = directory.decode_root(&decode_root_hex(
        &pointer.directory_root_hex,
        "retained source pointer",
    )?)?;
    let key = reference_key(reference)?;
    let Some(leaf) = directory
        .lookup(
            &root,
            &key,
            &mut super::directory::ReadBudget::with_workspace(budget),
        )
        .await?
    else {
        return if intent.is_none() {
            Ok(None)
        } else {
            Err(invariant_violation(
                "armed retained pointer omits its exact reference",
            ))
        };
    };
    if leaf.first != key
        || leaf.last != key
        || leaf.rows != 1
        || leaf.bytes == 0
        || leaf.bytes as usize > DESCRIPTOR_LIMIT
    {
        return Err(invariant_violation(
            "armed retained pointer has invalid membership",
        ));
    }
    let digest = hex::encode(leaf.digest);
    let descriptor_bytes = budget
        .read_immutable(
            &store.retention,
            &descriptor_path(store, &digest),
            Some(leaf.bytes as usize),
            &digest,
            DESCRIPTOR_LIMIT,
        )
        .await?;
    let descriptor: RetainedDescriptor = canonical(
        &descriptor_bytes,
        DESCRIPTOR_LIMIT,
        "retained source descriptor",
    )?;
    validate_descriptor(&descriptor, reference, binding)?;
    if intent.is_some()
        && (&descriptor.capture_operation_id, descriptor.capture_epoch)
            != (&pointer.last_operation_id, pointer.capture_epoch)
    {
        return Err(invariant_violation(
            "armed retained descriptor belongs to another capture incarnation",
        ));
    }
    Ok(Some(VerifiedRetainedPointer {
        visible: true,
        expected_pointer_raw_sha256: sha256_hex(&bytes),
        reference_key_hex: hex::encode(key),
    }))
}

pub(super) async fn verify(
    store: &ControlMvpStateStore,
    context: &mut RetainedSourceCaptureContext<'_>,
    reference: &PersistedAuthorityReference,
) -> Result<Option<crate::state_store::VerifiedRetainedSource>> {
    let intent = context.armed_pointer().cloned();
    if let Some(intent) = &intent {
        let pointer = validate_pointer(
            store,
            store.durable_authority8_binding()?,
            intent.expected_pointer_json.as_bytes(),
        )?;
        let genesis = matches!(
            intent.precondition,
            crate::retention_coordination::RetainedPointerIntentPrecondition::DoesNotExist {}
        );
        if pointer.last_operation_id != context.operation_id()
            || pointer.capture_epoch != context.epoch()
            || intent.domain != store.scope.domain()
            || intent.reference_key_hex != hex::encode(reference_key(reference)?)
            || genesis != (pointer.previous_generation == 0)
        {
            return Err(invariant_violation(
                "armed retained source differs from exact epoch or reference",
            ));
        }
    }
    let Some(pointer) =
        verify_selected_member(store, reference, intent.as_ref(), context.io()).await?
    else {
        return Ok(None);
    };
    resolve_with_budget(
        store,
        reference,
        &mut ReadBudget::with_workspace(context.io()),
    )
    .await?;
    Ok(Some(crate::state_store::VerifiedRetainedSource { pointer }))
}

/// Cumulative retained-source resolution admission.
pub(super) struct ReadBudget<'a> {
    objects: usize,
    bytes: usize,
    operations: usize,
    workspace: Option<&'a mut crate::workspace_io_budget::WorkspaceIoBudget>,
}

impl<'a> ReadBudget<'a> {
    pub(super) const fn new() -> Self {
        Self {
            objects: 0,
            bytes: 0,
            operations: 0,
            workspace: None,
        }
    }

    pub(super) fn with_workspace(
        workspace: &'a mut crate::workspace_io_budget::WorkspaceIoBudget,
    ) -> Self {
        Self {
            workspace: Some(workspace),
            ..Self::new()
        }
    }

    pub(super) fn charge_head(&mut self, meta: &arco_core::storage::ObjectMeta) -> Result<()> {
        if let Some(workspace) = &mut self.workspace {
            workspace.charge_head(meta)?;
        }
        Ok(())
    }

    pub(super) fn reserve(&mut self, objects: usize, bytes: usize) -> Result<()> {
        let objects = self
            .objects
            .checked_add(objects)
            .ok_or_else(|| invariant_violation("retained source read object count overflows"))?;
        let bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| invariant_violation("retained source read byte count overflows"))?;
        if objects > READ_OBJECT_LIMIT || bytes > READ_BYTE_LIMIT {
            return Err(CatalogError::MaintenanceBackpressure {
                message: "retained source resolution exceeds its 16-object or 8 MiB budget".into(),
            });
        }
        if let Some(workspace) = &mut self.workspace {
            workspace.reserve_bytes(bytes - self.bytes)?;
        }
        self.objects = objects;
        self.bytes = bytes;
        Ok(())
    }

    pub(super) fn operation(&mut self) -> Result<()> {
        self.operations = self.operations.checked_add(1).ok_or_else(|| {
            invariant_violation("retained source storage operation count overflows")
        })?;
        if self.operations > 4096 {
            return Err(CatalogError::MaintenanceBackpressure {
                message: "retained source resolution exceeds its 4096 storage-operation budget"
                    .into(),
            });
        }
        if let Some(workspace) = &mut self.workspace {
            workspace.charge_operations(1)?;
        }
        Ok(())
    }
}

impl super::directory::ReadInvoice for ReadBudget<'_> {
    fn charge_directory_probe(&mut self, bytes: usize) -> Result<()> {
        self.reserve(1, bytes)?;
        self.operation()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedPointer {
    record_type: String,
    version: u32,
    scope: StateScope,
    binding: DurableAuthorityBinding,
    directory_root_hex: String,
    previous_generation: u64,
    previous_root_sha256: Option<String>,
    generation: u64,
    root_sha256: String,
    last_operation_id: String,
    capture_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedDescriptor {
    record_type: String,
    version: u32,
    reference: PersistedAuthorityReference,
    reference_raw_sha256: String,
    scope: StateScope,
    implementation: String,
    authority_format: u32,
    binding: DurableAuthorityBinding,
    source_manifest_path: String,
    source_manifest_id: String,
    source_manifest_raw_sha256: String,
    source_logical_sequence: u64,
    retention_deadline: chrono::DateTime<chrono::Utc>,
    capture_operation_id: String,
    capture_epoch: u64,
}

fn retained_prefix(store: &ControlMvpStateStore) -> String {
    format!("{}/retained/v1", store.paths.base_prefix())
}

fn pointer_path(store: &ControlMvpStateStore) -> String {
    format!("{}/current.json", retained_prefix(store))
}

fn descriptor_path(store: &ControlMvpStateStore, digest: &str) -> String {
    format!("{}/descriptors/{digest}.json", retained_prefix(store))
}

fn raw_digest<'a>(value: &'a str, context: &str) -> Result<&'a str> {
    let digest = value
        .strip_prefix("sha256:")
        .ok_or_else(|| CatalogError::Validation {
            message: format!("{context} must have a sha256 prefix"),
        })?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CatalogError::Validation {
            message: format!("{context} must contain 64 lowercase hexadecimal characters"),
        });
    }
    Ok(digest)
}

fn valid_raw_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn reference_bytes(reference: &PersistedAuthorityReference) -> Result<Bytes> {
    encode_json_limited(reference, DESCRIPTOR_LIMIT, "retained source reference")
}

fn reference_key(reference: &PersistedAuthorityReference) -> Result<[u8; 32]> {
    let bytes = reference_bytes(reference)?;
    let length = u64::try_from(bytes.len())
        .map_err(|_| invariant_violation("retained source reference length does not fit u64"))?;
    let mut hash = Sha256::new();
    hash.update(b"arco.retained-reference.key.v1\0");
    hash.update(length.to_be_bytes());
    hash.update(&bytes);
    Ok(hash.finalize().into())
}

fn canonical<T>(bytes: &[u8], limit: usize, context: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    if bytes.is_empty() || bytes.len() > limit {
        return Err(invariant_violation(format!(
            "{context} exceeds its encoded size limit"
        )));
    }
    let value: T = decode_json(bytes, context)?;
    if encode_json_limited(&value, limit, context)?.as_ref() != bytes {
        return Err(invariant_violation(format!(
            "{context} is not canonical JSON"
        )));
    }
    Ok(value)
}

fn decode_root_hex(value: &str, context: &str) -> Result<Vec<u8>> {
    let bytes = hex::decode(value)
        .map_err(|_| invariant_violation(format!("{context} has invalid root hex")))?;
    if bytes.is_empty() {
        return Err(invariant_violation(format!("{context} has an empty root")));
    }
    Ok(bytes)
}

async fn read_immutable_json(
    store: &ControlMvpStateStore,
    budget: &mut ReadBudget<'_>,
    path: &str,
    limit: usize,
    expected_size: Option<usize>,
    context: &str,
) -> Result<Bytes> {
    budget.operation()?;
    let meta = store
        .storage
        .head(path)
        .await?
        .ok_or_else(|| CatalogError::Validation {
            message: format!("{context} is missing"),
        })?;
    budget.charge_head(&meta)?;
    let size = usize::try_from(meta.size)
        .map_err(|_| invariant_violation(format!("{context} size does not fit usize")))?;
    if size == 0 || size > limit || expected_size.is_some_and(|expected| expected != size) {
        return Err(invariant_violation(format!(
            "{context} exceeds its encoded size limit"
        )));
    }
    let probe_end = meta
        .size
        .checked_add(1)
        .ok_or_else(|| invariant_violation(format!("{context} probe length overflows")))?;
    let probe_size = size
        .checked_add(1)
        .ok_or_else(|| invariant_violation(format!("{context} probe size overflows")))?;
    budget.reserve(1, probe_size)?;
    budget.operation()?;
    let bytes = store.storage.get_range(path, 0..probe_end).await?;
    if bytes.len() != size {
        return Err(invariant_violation(format!(
            "{context} changed during immutable read"
        )));
    }
    Ok(bytes)
}

pub(super) async fn read_bounded_json(
    store: &ControlMvpStateStore,
    budget: &mut ReadBudget<'_>,
    path: &str,
    limit: usize,
    context: &str,
) -> Result<Bytes> {
    read_immutable_json(store, budget, path, limit, None, context).await
}

async fn load_selected_pointer(
    store: &ControlMvpStateStore,
    budget: &mut ReadBudget<'_>,
    binding: DurableAuthorityBinding,
) -> Result<RetainedPointer> {
    let path = pointer_path(store);
    for _ in 0..4 {
        budget.operation()?;
        let before = store
            .storage
            .head(&path)
            .await?
            .ok_or_else(|| validation_failed("selected retained source pointer is missing"))?;
        budget.charge_head(&before)?;
        let size = usize::try_from(before.size)
            .map_err(|_| invariant_violation("retained source pointer size does not fit usize"))?;
        if before.version.is_empty() || size == 0 || size > POINTER_LIMIT {
            return Err(invariant_violation(
                "selected retained source pointer is invalid",
            ));
        }
        let probe_end = u64::try_from(POINTER_LIMIT)
            .ok()
            .and_then(|limit| limit.checked_add(1))
            .ok_or_else(|| invariant_violation("retained source pointer probe length overflows"))?;
        let probe_size = POINTER_LIMIT
            .checked_add(1)
            .ok_or_else(|| invariant_violation("retained source pointer probe size overflows"))?;
        budget.reserve(1, probe_size)?;
        budget.operation()?;
        let bytes = store.storage.get_range(&path, 0..probe_end).await?;
        budget.operation()?;
        let after =
            store.storage.head(&path).await?.ok_or_else(|| {
                invariant_violation("selected retained source pointer disappeared")
            })?;
        budget.charge_head(&after)?;
        if before.version != after.version {
            continue;
        }
        if after.size != before.size || bytes.len() != size {
            return Err(invariant_violation(
                "selected retained source pointer size changed",
            ));
        }
        let pointer = validate_pointer(store, binding, &bytes)?;
        return Ok(pointer);
    }
    Err(CatalogError::AmbiguousAuthorityOutcome {
        message: "selected retained source pointer did not stabilize".into(),
    })
}

fn validate_pointer(
    store: &ControlMvpStateStore,
    binding: DurableAuthorityBinding,
    bytes: &[u8],
) -> Result<RetainedPointer> {
    let pointer: RetainedPointer = canonical(bytes, POINTER_LIMIT, "retained source pointer")?;
    let root = decode_root_hex(&pointer.directory_root_hex, "retained source pointer")?;
    if pointer.record_type != POINTER_RECORD_TYPE
        || pointer.version != RECORD_VERSION
        || pointer.scope != store.scope
        || pointer.binding != binding
        || pointer.generation == 0
        || pointer.capture_epoch == 0
        || pointer.last_operation_id.is_empty()
        || !valid_raw_digest(&pointer.root_sha256)
        || pointer.root_sha256 != sha256_hex(&root)
        || (pointer.previous_generation == 0) != pointer.previous_root_sha256.is_none()
        || pointer.previous_generation.checked_add(1) != Some(pointer.generation)
        || pointer
            .previous_root_sha256
            .as_deref()
            .is_some_and(|digest| !valid_raw_digest(digest))
    {
        return Err(invariant_violation(
            "selected retained source pointer fields differ",
        ));
    }
    Ok(pointer)
}

pub(super) async fn load_current_pointer(
    store: &ControlMvpStateStore,
    budget: &mut ReadBudget<'_>,
) -> Result<Option<(super::ControlMvpPointer, String, Bytes)>> {
    let path = store.paths.current_pointer();
    for attempt in 0..3 {
        budget.operation()?;
        let Some(before) = store.storage.head(&path).await? else {
            if attempt == 0 {
                return Ok(None);
            }
            return Err(invariant_violation(
                "selected HEAD disappeared while pinning",
            ));
        };
        budget.charge_head(&before)?;
        let size = usize::try_from(before.size)
            .map_err(|_| invariant_violation("selected HEAD size does not fit usize"))?;
        if before.version.is_empty() || size == 0 || size > POINTER_LIMIT {
            return Err(invariant_violation("selected HEAD is invalid"));
        }
        let probe_end = u64::try_from(POINTER_LIMIT)
            .ok()
            .and_then(|limit| limit.checked_add(1))
            .ok_or_else(|| invariant_violation("selected HEAD probe length overflows"))?;
        let probe_size = POINTER_LIMIT
            .checked_add(1)
            .ok_or_else(|| invariant_violation("selected HEAD probe size overflows"))?;
        budget.reserve(1, probe_size)?;
        budget.operation()?;
        let bytes = store.storage.get_range(&path, 0..probe_end).await?;
        budget.operation()?;
        let Some(after) = store.storage.head(&path).await? else {
            continue;
        };
        budget.charge_head(&after)?;
        if before.version != after.version {
            continue;
        }
        if before.size != after.size || bytes.len() != size {
            return Err(invariant_violation(
                "selected HEAD size changed while pinning",
            ));
        }
        let pointer: super::ControlMvpPointer = decode_json(&bytes, "control MVP mutable head")?;
        pointer.validate_versioned(&store.scope, 8)?;
        return Ok(Some((pointer, before.version, bytes)));
    }
    Err(CatalogError::AmbiguousAuthorityOutcome {
        message: "HEAD pin retry budget exhausted".into(),
    })
}

fn validate_descriptor(
    descriptor: &RetainedDescriptor,
    reference: &PersistedAuthorityReference,
    binding: DurableAuthorityBinding,
) -> Result<()> {
    let reference_bytes = reference_bytes(reference)?;
    if descriptor.record_type != DESCRIPTOR_RECORD_TYPE
        || descriptor.version != RECORD_VERSION
        || descriptor.reference != *reference
        || descriptor.reference_raw_sha256 != sha256_hex(&reference_bytes)
        || descriptor.scope != reference.scope().clone()
        || descriptor.implementation != reference.implementation()
        || descriptor.authority_format != 8
        || descriptor.binding != binding
        || descriptor.source_manifest_path != reference.manifest_path()
        || descriptor.source_manifest_id != reference.manifest_id()
        || descriptor.source_manifest_raw_sha256
            != raw_digest(reference.manifest_sha256(), "retained source manifest")?
        || descriptor.source_logical_sequence != reference.logical_sequence()
        || descriptor.retention_deadline != reference.retention_deadline()
        || descriptor.capture_operation_id.is_empty()
        || descriptor.capture_epoch == 0
    {
        return Err(invariant_violation(
            "retained source descriptor fields differ",
        ));
    }
    Ok(())
}

pub(super) async fn resolve(
    store: &ControlMvpStateStore,
    reference: &PersistedAuthorityReference,
) -> Result<ControlMvpRetainedReader> {
    resolve_with_budget(store, reference, &mut ReadBudget::new()).await
}

pub(super) async fn preflight_with_workspace_io(
    store: &ControlMvpStateStore,
    reference: &PersistedAuthorityReference,
    io: &mut crate::workspace_io_budget::WorkspaceCaptureIo<'_>,
) -> Result<()> {
    let mut budget = ReadBudget::with_workspace(io.workspace_budget());
    resolve_with_budget(store, reference, &mut budget).await?;
    Ok(())
}

/// Authenticates the selected authority while the caller owns the workspace
/// epoch.  The returned reference is private to that one capture invocation.
#[allow(
    clippy::too_many_lines,
    reason = "keep current and exact ancestor authentication before private source staging"
)]
pub(super) async fn prepare(
    store: &ControlMvpStateStore,
    context: &mut RetainedSourceCaptureContext<'_>,
    retention_deadline: chrono::DateTime<chrono::Utc>,
) -> Result<PreparedRetainedSource> {
    let capture_authenticated_at = super::cost::now();
    let maximum = capture_authenticated_at
        .checked_add_signed(chrono::Duration::days(30))
        .ok_or_else(|| validation_failed("retained capture deadline overflows"))?;
    if retention_deadline <= capture_authenticated_at || retention_deadline > maximum {
        return Err(validation_failed(
            "retained source deadline exceeds the authenticated thirty-day window",
        ));
    }
    let binding = store.durable_authority8_binding()?;
    if context.operation_id().is_empty() || context.epoch() == 0 {
        return Err(validation_failed(
            "bounded retained source capture context has no operation identity",
        ));
    }
    let operation_id = context.operation_id().to_string();
    let capture_epoch = context.epoch();
    let expected = context.expected_reference;
    if let Some(reference) = expected {
        reference.validate()?;
        if reference.scope() != &store.scope
            || reference.implementation() != super::IMPLEMENTATION
            || reference.reference_kind() != PersistedAuthorityKind::StateToken
            || reference.retention_deadline() != retention_deadline
            || reference.manifest_path() != store.paths.manifest_object(reference.manifest_id())
        {
            return Err(validation_failed(
                "retained retry source differs from its exact capture context",
            ));
        }
    }
    let mut budget = ReadBudget::with_workspace(context.io());
    let (pointer, version, pointer_bytes) = load_current_pointer(store, &mut budget)
        .await?
        .ok_or_else(|| validation_failed("authority-8 source requires a published HEAD"))?;
    let manifest_path = store.paths.manifest_object(&pointer.manifest_id);
    let manifest = read_immutable_json(
        store,
        &mut budget,
        &manifest_path,
        MAX_CONTROL_JSON_BYTES,
        None,
        "bounded retained source manifest",
    )
    .await?;
    let manifest_sha256 = prefixed_sha256(&manifest);
    let token = store
        .token(pointer.manifest_id.clone(), pointer.logical_sequence)
        .with_manifest_witness(
            raw_digest(&manifest_sha256, "bounded retained source manifest")?.to_string(),
        );
    let mut base = store
        .read_bounded_current_from_manifest(
            &token,
            &manifest,
            &pointer,
            version,
            pointer_bytes,
            &mut budget,
        )
        .await?;
    let mut reference = PersistedAuthorityReference::new(
        super::IMPLEMENTATION,
        store.scope.clone(),
        PersistedAuthorityKind::StateToken,
        pointer.manifest_id,
        pointer.logical_sequence,
        manifest_path,
        manifest_sha256,
        None,
        None,
        retention_deadline,
    )?;
    if let Some(expected) = expected {
        let mut ancestors = 0usize;
        while &reference != expected {
            if ancestors == 32 || reference.logical_sequence() <= expected.logical_sequence() {
                return Err(CatalogError::AmbiguousAuthorityOutcome {
                    message: "exact retained source was not authenticated within current ancestry"
                        .into(),
                });
            }
            let parent = base.parent_token(store)?.ok_or_else(|| {
                CatalogError::AmbiguousAuthorityOutcome {
                    message: "current ancestry does not contain the exact retained source".into(),
                }
            })?;
            let path = store.paths.manifest_object(parent.authority_manifest_id());
            let bytes = read_immutable_json(
                store,
                &mut budget,
                &path,
                MAX_CONTROL_JSON_BYTES,
                None,
                "retained source ancestor manifest",
            )
            .await?;
            let parent_base = store
                .read_bounded_token_from_manifest(&parent, &bytes, &mut budget)
                .await?;
            base.verify_parent_transition(store, &parent_base, &mut budget)
                .await?;
            base = parent_base;
            reference = PersistedAuthorityReference::new(
                super::IMPLEMENTATION,
                store.scope.clone(),
                PersistedAuthorityKind::StateToken,
                parent.authority_manifest_id().to_string(),
                parent.logical_sequence(),
                path,
                prefixed_sha256(&bytes),
                None,
                None,
                retention_deadline,
            )?;
            ancestors += 1;
        }
    }
    if context.defer_retained_staging {
        let publication = RetainedSourcePublication {
            store: store.clone(),
            reference: reference.clone(),
            state: PublicationState::Deferred {
                operation_id,
                capture_epoch,
            },
        };
        return Ok(PreparedRetainedSource::new(reference, publication));
    }
    let (epoch, budget) = context.publication_io();
    let publication = RetainedSourcePublication::stage(
        store,
        &reference,
        binding,
        operation_id,
        capture_epoch,
        epoch,
        budget,
    )
    .await?;
    Ok(PreparedRetainedSource::new(reference, publication))
}

async fn resolve_with_budget(
    store: &ControlMvpStateStore,
    reference: &PersistedAuthorityReference,
    budget: &mut ReadBudget<'_>,
) -> Result<ControlMvpRetainedReader> {
    let (token, base, _) = resolve_authenticated_base(store, reference, budget).await?;
    Ok(ControlMvpRetainedReader {
        scope: store.scope.clone(),
        token,
        source: ControlMvpRetainedSource::Bounded {
            store: Box::new(store.clone()),
            base: Box::new(base),
        },
    })
}

pub(super) async fn restore_source_with_workspace_io(
    store: &ControlMvpStateStore,
    reference: &PersistedAuthorityReference,
    io: &mut crate::workspace_io_budget::WorkspaceCaptureIo<'_>,
) -> Result<(super::bounded::Base, usize)> {
    let mut budget = ReadBudget::with_workspace(io.workspace_budget());
    let (_, base, size) = resolve_authenticated_base(store, reference, &mut budget).await?;
    Ok((base, size))
}

#[allow(
    clippy::too_many_lines,
    reason = "selected retained-source authentication is one fail-closed boundary"
)]
async fn resolve_authenticated_base(
    store: &ControlMvpStateStore,
    reference: &PersistedAuthorityReference,
    budget: &mut ReadBudget<'_>,
) -> Result<(super::StateToken, super::bounded::Base, usize)> {
    if reference.reference_kind() != PersistedAuthorityKind::StateToken {
        return Err(CatalogError::UnsupportedAuthorityFormat {
            message: "authority-8 does not support persisted checkpoints".into(),
        });
    }
    let binding = store.durable_authority8_binding()?;
    let manifest_path = store.paths.manifest_object(reference.manifest_id());
    let manifest_bytes = read_immutable_json(
        store,
        budget,
        &manifest_path,
        MAX_CONTROL_JSON_BYTES,
        None,
        "retained source manifest",
    )
    .await?;
    if prefixed_sha256(&manifest_bytes) != reference.manifest_sha256() {
        return Err(invariant_violation(
            "retained source manifest checksum differs from reference",
        ));
    }
    let expected_manifest_digest =
        raw_digest(reference.manifest_sha256(), "retained source manifest")?;
    let current = load_current_pointer(store, budget).await?;
    let current = current.filter(|(pointer, _, _)| {
        pointer.manifest_id == reference.manifest_id()
            && pointer.logical_sequence == reference.logical_sequence()
            && pointer.manifest_checksum_sha256 == expected_manifest_digest
    });
    if current.is_none() {
        let pointer = load_selected_pointer(store, budget, binding).await?;
        let root_bytes = decode_root_hex(&pointer.directory_root_hex, "retained source pointer")?;
        budget.reserve(
            DIRECTORY_DEPTH,
            DIRECTORY_DEPTH * (DIRECTORY_PAGE_LIMIT + 1),
        )?;
        // Admit every possible page request before lookup, including a request
        // that errors or is cancelled. Retained keys are inline, so this also
        // bounds all indirect directory I/O without changing ordinary reads.
        for _ in 0..DIRECTORY_DEPTH {
            budget.operation()?;
        }
        let directory =
            super::directory::Directory::retained_reference(store.retention.clone(), &store.scope)?;
        let root = directory.decode_root(&root_bytes)?;
        let key = reference_key(reference)?;
        let mut directory_budget = super::directory::ReadBudget::new(
            DIRECTORY_DEPTH,
            DIRECTORY_DEPTH * (DIRECTORY_PAGE_LIMIT + 1),
        )?;
        let leaf = directory
            .lookup(&root, &key, &mut directory_budget)
            .await?
            .ok_or_else(|| validation_failed("retained source reference is not selected"))?;
        let descriptor_digest = hex::encode(leaf.digest);
        if leaf.first != key
            || leaf.last != key
            || leaf.rows != 1
            || leaf.bytes == 0
            || usize::try_from(leaf.bytes)
                .ok()
                .is_none_or(|bytes| bytes > DESCRIPTOR_LIMIT)
        {
            return Err(invariant_violation(
                "selected retained source leaf is not an exact singleton",
            ));
        }
        let descriptor_bytes = read_immutable_json(
            store,
            budget,
            &descriptor_path(store, &descriptor_digest),
            DESCRIPTOR_LIMIT,
            Some(leaf.bytes as usize),
            "retained source descriptor",
        )
        .await?;
        if descriptor_bytes.len() != leaf.bytes as usize
            || sha256_hex(&descriptor_bytes) != descriptor_digest
        {
            return Err(invariant_violation(
                "selected retained source descriptor differs from its leaf",
            ));
        }
        let descriptor: RetainedDescriptor = canonical(
            &descriptor_bytes,
            DESCRIPTOR_LIMIT,
            "retained source descriptor",
        )?;
        validate_descriptor(&descriptor, reference, binding)?;
    }
    let token = store
        .token(
            reference.manifest_id().to_string(),
            reference.logical_sequence(),
        )
        .with_manifest_witness(expected_manifest_digest.to_string());
    let base = if let Some((pointer, version, pointer_bytes)) = current {
        store
            .read_bounded_current_from_manifest(
                &token,
                &manifest_bytes,
                &pointer,
                version,
                pointer_bytes,
                budget,
            )
            .await?
    } else {
        store
            .read_bounded_token_from_manifest(&token, &manifest_bytes, budget)
            .await?
    };
    Ok((token, base, manifest_bytes.len()))
}

#[cfg(test)]
impl ControlMvpStateStore {
    /// Creates a fully authenticated selected retained-source index for tests.
    pub(super) async fn install_test_retained_source(
        &self,
        reference: &PersistedAuthorityReference,
    ) -> Result<()> {
        reference.validate()?;
        let binding = self.durable_authority8_binding()?;
        let descriptor = RetainedDescriptor {
            record_type: DESCRIPTOR_RECORD_TYPE.into(),
            version: RECORD_VERSION,
            reference: reference.clone(),
            reference_raw_sha256: sha256_hex(&reference_bytes(reference)?),
            scope: self.scope.clone(),
            implementation: super::IMPLEMENTATION.into(),
            authority_format: 8,
            binding,
            source_manifest_path: reference.manifest_path().to_string(),
            source_manifest_id: reference.manifest_id().to_string(),
            source_manifest_raw_sha256: raw_digest(
                reference.manifest_sha256(),
                "retained source manifest",
            )?
            .to_string(),
            source_logical_sequence: reference.logical_sequence(),
            retention_deadline: reference.retention_deadline(),
            capture_operation_id: "test-retained-source".into(),
            capture_epoch: 1,
        };
        let descriptor_bytes =
            encode_json_limited(&descriptor, DESCRIPTOR_LIMIT, "retained source descriptor")?;
        let descriptor_digest = sha256_hex(&descriptor_bytes);
        put_immutable_matching(
            &self.storage,
            &descriptor_path(self, &descriptor_digest),
            descriptor_bytes.clone(),
            "test retained source descriptor differs",
        )
        .await?;
        let key = reference_key(reference)?;
        let digest: [u8; 32] = hex::decode(&descriptor_digest)
            .map_err(|_| invariant_violation("test retained descriptor digest is invalid"))?
            .try_into()
            .map_err(|_| {
                invariant_violation("test retained descriptor digest length is invalid")
            })?;
        let directory =
            super::directory::Directory::retained_reference(self.retention.clone(), &self.scope)?;
        let mut builder = directory.builder();
        builder
            .push(super::directory::Leaf {
                first: key.to_vec(),
                last: key.to_vec(),
                rows: 1,
                bytes: u32::try_from(descriptor_bytes.len()).map_err(|_| {
                    invariant_violation("test retained descriptor length does not fit u32")
                })?,
                digest,
            })
            .await?;
        let root = builder.finish().await?;
        let root_bytes = root.encode();
        let root_digest = sha256_hex(&root_bytes);
        let pointer = RetainedPointer {
            record_type: POINTER_RECORD_TYPE.into(),
            version: RECORD_VERSION,
            scope: self.scope.clone(),
            binding,
            directory_root_hex: hex::encode(&root_bytes),
            previous_generation: 0,
            previous_root_sha256: None,
            generation: 1,
            root_sha256: root_digest,
            last_operation_id: "test-retained-source".into(),
            capture_epoch: 1,
        };
        let pointer_bytes =
            encode_json_limited(&pointer, POINTER_LIMIT, "retained source pointer")?;
        match self
            .storage
            .put(
                &pointer_path(self),
                pointer_bytes,
                AuthorityWritePrecondition::DoesNotExist,
            )
            .await?
        {
            WriteResult::Success { .. } => Ok(()),
            WriteResult::PreconditionFailed { .. } => Err(CatalogError::PreconditionFailed {
                message: "test retained source pointer already exists".into(),
            }),
        }
    }

    pub(super) async fn install_test_retained_singleton_gap(
        &self,
        reference: &PersistedAuthorityReference,
    ) -> Result<()> {
        self.install_test_retained_source(reference).await?;
        let directory =
            super::directory::Directory::retained_reference(self.retention.clone(), &self.scope)?;
        let mut builder = directory.builder();
        builder
            .push(super::directory::Leaf {
                first: vec![0; 32],
                last: vec![u8::MAX; 32],
                rows: 1,
                bytes: 1,
                digest: [0; 32],
            })
            .await?;
        let root = builder.finish().await?;
        let root_bytes = root.encode();
        let root_digest = sha256_hex(&root_bytes);
        let path = pointer_path(self);
        let current = self.retention.get_raw(&path).await?;
        let mut pointer: RetainedPointer =
            canonical(&current, POINTER_LIMIT, "test retained source pointer")?;
        pointer.directory_root_hex = hex::encode(&root_bytes);
        pointer.root_sha256 = root_digest;
        let bytes = encode_json_limited(&pointer, POINTER_LIMIT, "test retained source pointer")?;
        self.retention
            .put_raw(&path, bytes, arco_core::storage::WritePrecondition::None)
            .await?;
        Ok(())
    }

    pub(super) async fn install_test_retained_generation_jump(
        &self,
        reference: &PersistedAuthorityReference,
    ) -> Result<()> {
        self.install_test_retained_source(reference).await?;
        let path = pointer_path(self);
        let current = self.retention.get_raw(&path).await?;
        let mut pointer: RetainedPointer =
            canonical(&current, POINTER_LIMIT, "test retained source pointer")?;
        pointer.generation = 2;
        let bytes = encode_json_limited(&pointer, POINTER_LIMIT, "test retained source pointer")?;
        self.retention
            .put_raw(&path, bytes, arco_core::storage::WritePrecondition::None)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::state_store::PersistedAuthorityAdapter;
    use arco_core::storage::{ObjectMeta, WritePrecondition};
    use arco_core::{MemoryBackend, ScopedStorage, StorageBackend};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct ObservedBackend {
        inner: MemoryBackend,
        operations: AtomicUsize,
        range_bytes: AtomicUsize,
        range_objects: AtomicUsize,
        directory_objects: AtomicUsize,
        mode: AtomicUsize,
        entered: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl StorageBackend for ObservedBackend {
        async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
            self.operations.fetch_add(1, Ordering::Relaxed);
            self.inner.get(path).await
        }
        async fn get_range(
            &self,
            path: &str,
            range: std::ops::Range<u64>,
        ) -> arco_core::Result<Bytes> {
            self.operations.fetch_add(1, Ordering::Relaxed);
            self.range_objects.fetch_add(1, Ordering::Relaxed);
            self.range_bytes.fetch_add(
                usize::try_from(range.end - range.start).expect("bounded test range"),
                Ordering::Relaxed,
            );
            let mode = self.mode.load(Ordering::Relaxed);
            let page = path.contains("/retained/v1/directory/pages/");
            let descriptor = path.contains("/retained/v1/descriptors/");
            let manifest = path.contains("/manifests/");
            let current = path.ends_with("/head/current.json");
            let retained = path.ends_with("/retained/v1/current.json");
            if page {
                self.directory_objects.fetch_add(1, Ordering::Relaxed);
            }
            if mode == 1 && page {
                return Err(arco_core::Error::storage("injected retained page failure"));
            }
            if (mode == 2 && page)
                || (mode == 10 && manifest)
                || (mode == 11 && descriptor)
                || (mode == 12 && current)
                || (mode == 13 && retained)
            {
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            if (mode == 3 && descriptor) || (mode == 5 && manifest) || (mode == 6 && page) {
                let mut changed = self.inner.get(path).await?.to_vec();
                changed.push(b' ');
                self.inner
                    .put(path, Bytes::from(changed), WritePrecondition::None)
                    .await?;
            }
            if (mode == 7 && current) || (mode == 8 && retained) {
                self.inner.delete(path).await?;
            }
            let result = self.inner.get_range(path, range).await;
            if (mode == 17 && current) || (mode == 18 && retained) {
                self.inner.delete(path).await?;
            }
            if (mode == 19 && current) || (mode == 20 && retained) {
                let bytes = self.inner.get(path).await?;
                self.inner.put(path, bytes, WritePrecondition::None).await?;
            }
            result
        }
        async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
            self.operations.fetch_add(1, Ordering::Relaxed);
            let mut meta = self.inner.head(path).await?;
            if self.mode.load(Ordering::Relaxed) == 4 && path.ends_with("/retained/v1/current.json")
            {
                if let Some(meta) = &mut meta {
                    meta.version = format!("unstable-{}", self.operations.load(Ordering::Relaxed));
                }
            }
            Ok(meta)
        }
        async fn put(
            &self,
            path: &str,
            data: Bytes,
            precondition: WritePrecondition,
        ) -> arco_core::Result<WriteResult> {
            self.inner.put(path, data, precondition).await
        }
        async fn delete(&self, path: &str) -> arco_core::Result<()> {
            self.inner.delete(path).await
        }
        async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
            self.inner.list(prefix).await
        }
        async fn signed_url(
            &self,
            path: &str,
            expiry: std::time::Duration,
        ) -> arco_core::Result<String> {
            self.inner.signed_url(path, expiry).await
        }
    }

    async fn fixture() -> (
        ControlMvpStateStore,
        PersistedAuthorityReference,
        Arc<ObservedBackend>,
    ) {
        let backend = Arc::new(ObservedBackend::default());
        let store = ControlMvpStateStore::new_synthetic_bounded(
            ScopedStorage::new(backend.clone(), "tenant", "workspace").unwrap(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap()
        .with_durable_authority_binding(DurableAuthorityBinding::new([22; 32]));
        let mut txn = store
            .begin_control_txn(super::super::TxnOptions::default())
            .await
            .unwrap();
        txn.set_logical_operation("retained-budget-source", "test", &"a1".repeat(32))
            .unwrap();
        let source = txn.commit_v2().await.unwrap();
        let reference = store
            .persist_state_reference(
                source.token(),
                chrono::Utc::now() + chrono::Duration::days(1),
            )
            .await
            .unwrap();
        store
            .install_test_retained_source(&reference)
            .await
            .unwrap();
        let mut txn = store
            .begin_control_txn(super::super::TxnOptions::default())
            .await
            .unwrap();
        txn.set_logical_operation("retained-budget-advance", "test", &"a1".repeat(32))
            .unwrap();
        txn.commit_v2().await.unwrap();
        backend.operations.store(0, Ordering::Relaxed);
        backend.range_bytes.store(0, Ordering::Relaxed);
        backend.range_objects.store(0, Ordering::Relaxed);
        backend.directory_objects.store(0, Ordering::Relaxed);
        (store, reference, backend)
    }

    #[tokio::test]
    async fn prepared_retained_source_cannot_move_to_another_epoch() {
        use crate::retention_coordination::{RetentionMutationEpoch, RetentionMutationKind};
        use crate::workspace_io_budget::WorkspaceIoBudget;
        let (store, _, _) = fixture().await;
        let before = store
            .retention
            .get_raw(&pointer_path(&store))
            .await
            .unwrap();
        let mut budget = WorkspaceIoBudget::new();
        let mut guard = budget
            .acquire_retention_lock(
                store
                    .retention
                    .as_legacy_scoped()
                    .expect("workspace root")
                    .clone(),
                "proof-epoch",
            )
            .await
            .unwrap();
        let mut first = RetentionMutationEpoch::claim_bounded(
            store
                .retention
                .as_legacy_scoped()
                .expect("workspace root")
                .clone(),
            &mut guard,
            RetentionMutationKind::WorkspaceSnapshotFinalize,
            "first",
            &mut budget,
        )
        .await
        .unwrap();
        let prepared = prepare(
            &store,
            &mut RetainedSourceCaptureContext::new(
                store.scope.clone(),
                "first".into(),
                &mut first,
                &mut budget,
            ),
            chrono::Utc::now() + chrono::Duration::days(1),
        )
        .await
        .unwrap();
        first.settle_bounded(&mut guard, &mut budget).await.unwrap();
        let mut second = RetentionMutationEpoch::claim_bounded(
            store
                .retention
                .as_legacy_scoped()
                .expect("workspace root")
                .clone(),
            &mut guard,
            RetentionMutationKind::WorkspaceSnapshotFinalize,
            "second",
            &mut budget,
        )
        .await
        .unwrap();
        assert!(
            prepared
                .publish(&mut second, &mut guard, &mut budget)
                .await
                .is_err(),
            "private source proof is bound to its original epoch"
        );
        assert_eq!(
            store
                .retention
                .get_raw(&pointer_path(&store))
                .await
                .unwrap(),
            before
        );
    }

    #[tokio::test]
    async fn retained_directory_operations_are_charged_on_success_failure_and_cancellation() {
        for mode in 0..3 {
            let (store, reference, backend) = fixture().await;
            backend.mode.store(mode, Ordering::Relaxed);
            let mut budget = ReadBudget::new();
            if mode == 2 {
                tokio::select! {
                    result = resolve_with_budget(&store, &reference, &mut budget) => {
                        panic!("resolution completed before cancellation: {}", result.is_ok());
                    }
                    () = backend.entered.notified() => {}
                }
            } else {
                let result = resolve_with_budget(&store, &reference, &mut budget).await;
                assert_eq!(result.is_ok(), mode == 0);
            }
            let actual = backend.operations.load(Ordering::Relaxed);
            assert!(
                budget.operations >= actual,
                "mode {mode}: admitted {} operations but executed {actual}",
                budget.operations
            );
            assert!(budget.objects > 0 && budget.bytes > 0);
        }
    }
    #[tokio::test]
    async fn retained_warm_objects_reject_changes_and_deletion_in_every_cache_mode() {
        use super::super::ControlMvpReadCacheConfig;
        for config in [
            None,
            Some(ControlMvpReadCacheConfig::default()),
            Some(ControlMvpReadCacheConfig {
                metadata_bytes: 1024 * 1024,
                decoded_bytes: 4 * 1024 * 1024,
            }),
        ] {
            for suffix in [
                "/retained/v1/current.json",
                "/retained/v1/directory/pages/",
                "/retained/v1/descriptors/",
            ] {
                for delete in [false, true] {
                    let (store, reference, backend) = fixture().await;
                    let store = match config {
                        None => store.without_read_cache(),
                        Some(config) => store.with_read_cache_config(config).unwrap(),
                    };
                    resolve(&store, &reference).await.unwrap();
                    let path = backend
                        .inner
                        .list("")
                        .await
                        .unwrap()
                        .into_iter()
                        .find(|meta| meta.path.contains(suffix))
                        .unwrap()
                        .path;
                    if delete {
                        backend.inner.delete(&path).await.unwrap();
                    } else {
                        let mut bytes = backend.inner.get(&path).await.unwrap().to_vec();
                        bytes[0] ^= 1;
                        backend
                            .inner
                            .put(&path, Bytes::from(bytes), WritePrecondition::None)
                            .await
                            .unwrap();
                    }
                    assert!(
                        resolve(&store, &reference).await.is_err(),
                        "accepted changed object {suffix}, delete={delete}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn retained_append_race_and_unstable_pointer_fail_with_charged_probes() {
        for mode in [3, 4, 5, 6, 7, 8, 17, 18, 19, 20] {
            let (store, reference, backend) = fixture().await;
            backend.mode.store(mode, Ordering::Relaxed);
            let mut budget = ReadBudget::new();
            assert!(
                resolve_with_budget(&store, &reference, &mut budget)
                    .await
                    .is_err()
            );
            assert!(budget.operations >= backend.operations.load(Ordering::Relaxed));
            assert!(budget.bytes <= READ_BYTE_LIMIT && budget.objects <= READ_OBJECT_LIMIT);
            if mode == 4 {
                assert!(
                    budget.bytes >= 4 * (POINTER_LIMIT + 1),
                    "all unstable attempts remain charged"
                );
            }
        }
    }
    #[tokio::test]
    async fn retained_cancelled_probes_cannot_reuse_consumed_budget() {
        for mode in [2, 10, 11, 12, 13] {
            let (store, reference, backend) = fixture().await;
            backend.mode.store(mode, Ordering::Relaxed);
            let mut budget = ReadBudget::new();
            {
                tokio::select! {
                    result = resolve_with_budget(&store, &reference, &mut budget) => {
                        panic!("resolution finished before cancellation, mode={mode}, success={}", result.is_ok());
                    }
                    () = backend.entered.notified() => {}
                }
            }
            assert!(budget.operations >= backend.operations.load(Ordering::Relaxed));
            assert!(budget.bytes >= backend.range_bytes.load(Ordering::Relaxed));
            assert!(budget.objects >= backend.range_objects.load(Ordering::Relaxed));
            let remaining = READ_BYTE_LIMIT - budget.bytes;
            budget.reserve(0, remaining).unwrap();
            let consumed_operations = backend.operations.load(Ordering::Relaxed);
            backend.mode.store(0, Ordering::Relaxed);
            assert!(matches!(
                resolve_with_budget(&store, &reference, &mut budget).await,
                Err(CatalogError::MaintenanceBackpressure { .. })
            ));
            assert_eq!(budget.bytes, READ_BYTE_LIMIT);
            assert_eq!(
                backend.operations.load(Ordering::Relaxed),
                consumed_operations + 1,
                "the next manifest HEAD is charged, but its range read cannot reuse spent capacity"
            );
            resolve(&store, &reference).await.unwrap();
        }
    }

    #[tokio::test]
    async fn retained_depth_eight_lookup_accounts_for_every_backend_probe() {
        let (store, reference, backend) = fixture().await;
        let path = pointer_path(&store);
        let bytes = store.retention.get_raw(&path).await.unwrap();
        let mut pointer: RetainedPointer =
            canonical(&bytes, POINTER_LIMIT, "fixture pointer").unwrap();
        let directory = super::super::directory::Directory::retained_reference(
            store.retention.clone(),
            &store.scope,
        )
        .unwrap();
        let root = directory
            .decode_root(&hex::decode(&pointer.directory_root_hex).unwrap())
            .unwrap();
        let root = directory.test_root_at_depth(&root, 8).await.unwrap();
        let encoded = root.encode();
        pointer.directory_root_hex = hex::encode(&encoded);
        pointer.root_sha256 = sha256_hex(&encoded);
        store
            .retention
            .put_raw(
                &path,
                encode_json_limited(&pointer, POINTER_LIMIT, "fixture pointer").unwrap(),
                WritePrecondition::None,
            )
            .await
            .unwrap();
        backend.operations.store(0, Ordering::Relaxed);
        backend.range_bytes.store(0, Ordering::Relaxed);
        backend.range_objects.store(0, Ordering::Relaxed);
        backend.directory_objects.store(0, Ordering::Relaxed);
        let mut budget = ReadBudget::new();
        resolve_with_budget(&store, &reference, &mut budget)
            .await
            .unwrap();
        assert_eq!(backend.directory_objects.load(Ordering::Relaxed), 8);
        assert_eq!(
            budget.operations,
            backend.operations.load(Ordering::Relaxed)
        );
        assert_eq!(
            budget.objects,
            backend.range_objects.load(Ordering::Relaxed)
        );
        assert!(budget.bytes >= backend.range_bytes.load(Ordering::Relaxed));
    }
    #[tokio::test]
    async fn retained_resolution_consumes_the_enclosing_workspace_budget() {
        use crate::workspace_io_budget::{METADATA_BYTES, OPERATIONS, WorkspaceIoBudget};
        for exhausted_operations in [false, true] {
            let (store, reference, backend) = fixture().await;
            let mut workspace = WorkspaceIoBudget::new();
            if exhausted_operations {
                workspace.charge_operations(OPERATIONS).unwrap();
            } else {
                workspace.reserve_bytes(METADATA_BYTES).unwrap();
            }
            let mut budget = ReadBudget::with_workspace(&mut workspace);
            assert!(
                matches!(
                    resolve_with_budget(&store, &reference, &mut budget).await,
                    Err(CatalogError::MaintenanceBackpressure { .. })
                ),
                "delegated resolution must not reset workspace admission"
            );
            assert_eq!(
                backend.operations.load(Ordering::Relaxed),
                usize::from(!exhausted_operations)
            );
            assert_eq!(backend.range_objects.load(Ordering::Relaxed), 0);
        }
        let (store, reference, _) = fixture().await;
        let mut workspace = WorkspaceIoBudget::new();
        {
            let mut budget = ReadBudget::with_workspace(&mut workspace);
            resolve_with_budget(&store, &reference, &mut budget)
                .await
                .unwrap();
        }
        assert!(workspace.reserve_bytes(METADATA_BYTES).is_err());
        assert!(workspace.charge_operations(OPERATIONS).is_err());
    }
}

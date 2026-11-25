//! Private bounded mark pages. No mark is a deletion capability.
mod planning;
pub use planning::GcMarkedPlan;

use super::super::{
    CatalogError, ControlMvpMaintenanceWorker, ControlMvpPointer, MAX_CONTROL_JSON_BYTES,
    MAX_HEAD_JSON_BYTES, Result, decode_json, directory, encode_json_limited, invariant_violation,
    physical, retained, sha256_hex, valid_raw_digest,
};
use crate::gc::reachability::RetainedAuthorityRoots;
use crate::retention_coordination::RetentionMutationEpoch;
use crate::workspace_io_budget::{RECORD_BYTES, WorkspaceIoBudget};
use arco_core::WriteResult;
use arco_core::storage::WritePrecondition;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const SEED_RECORD: &str = "arco_authority8_gc_mark_seed";
const INVENTORY_LIMIT: usize = 4096;
const WALK_STEPS: usize = 128;
const PAGE_RECORD: &str = "arco_authority8_gc_mark_page";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedManifest {
    id: String,
    raw_hex: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    head_hex: String,
    head_version: String,
    epoch_version: Option<String>,
    retained: Option<(String, String)>,
    pin_inventory: String,
    manifest_inventory: String,
    manifests: Vec<SelectedManifest>,
    required_paths: Vec<String>,
    root_records: Vec<(String, String, u64)>,
    protected_prefixes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Seed {
    record_type: String,
    version: u32,
    scope: crate::state_store::StateScope,
    collection_time: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    snapshot: Snapshot,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarkPage {
    record_type: String,
    version: u32,
    seed: String,
    cursor: GcMarkCursor,
    manifest_id: String,
    root_hex: String,
    next: Option<GcMarkCursor>,
}

#[derive(Debug, Serialize)]
struct Completion {
    record_type: &'static str,
    version: u32,
    seed: String,
    pages: u64,
    last_page_sha256: String,
    manifest_inventory: String,
    pin_inventory: String,
}

/// Untrusted resume convenience; independent verification derives its own cursor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcMarkCursor {
    ordinal: u64,
    manifest: usize,
    role: u8,
    after: Option<Vec<u8>>,
    previous: String,
}

/// One persisted page and its optional resume cursor.
pub struct GcMarkAdvance {
    /// Exact immutable page digest.
    pub page_sha256: String,
    /// Resume convenience, never a coverage certificate.
    pub continuation: Option<GcMarkCursor>,
    /// Conservative cumulative bytes admitted by this invocation.
    pub admitted_bytes: usize,
    /// Storage and proof operations charged to this invocation.
    pub charged_operations: usize,
}

/// Invocation-bounded verifier state. Restart replays from ordinal zero.
/// It is intentionally neither constructible nor deserializable by callers.
#[derive(Clone)]
pub struct GcMarkVerifier {
    config: crate::state_store::WorkspaceCaptureConfig,
    snapshot: Seed,
    protected: Protection,
    walk: Option<directory::GcWalk>,
    page_sha256: Option<String>,
    seed: String,
    cursor: Option<GcMarkCursor>,
    complete: bool,
    pages: u64,
    admission: (usize, usize),
}

// Fixed space, insert-only, and never restored from untrusted storage.
#[derive(Clone)]
struct Protection(Vec<u8>);
impl Protection {
    const BYTES: usize = 1024 * 1024;
    fn new() -> Self {
        Self(vec![0; Self::BYTES])
    }
    fn positions(path: &str, io: &mut WorkspaceIoBudget) -> Result<[usize; 7]> {
        use sha2::{Digest, Sha256};
        io.charge_operations(1)?;
        io.reserve_bytes(path.len() + 128)?;
        super::super::record_sha256_work(path.len());
        let hash = Sha256::digest(path.as_bytes());
        let mut positions = [0; 7];
        for (position, bytes) in positions.iter_mut().zip(hash.chunks_exact(4)) {
            let bytes: [u8; 4] = bytes
                .try_into()
                .map_err(|_| invariant_violation("GC hash width"))?;
            *position = u32::from_le_bytes(bytes) as usize % (Self::BYTES * 8);
        }
        Ok(positions)
    }
    fn insert(&mut self, path: &str, io: &mut WorkspaceIoBudget) -> Result<()> {
        for bit in Self::positions(path, io)? {
            let byte = self
                .0
                .get_mut(bit / 8)
                .ok_or_else(|| invariant_violation("GC filter width"))?;
            *byte |= 1 << (bit % 8);
        }
        Ok(())
    }
    fn contains(&self, path: &str, io: &mut WorkspaceIoBudget) -> Result<bool> {
        Ok(Self::positions(path, io)?.into_iter().all(|bit| {
            self.0
                .get(bit / 8)
                .is_some_and(|byte| byte & (1 << (bit % 8)) != 0)
        }))
    }
}

impl GcMarkVerifier {
    fn new(
        worker: &ControlMvpMaintenanceWorker,
        seed: &str,
        snapshot: Seed,
        io: &mut WorkspaceIoBudget,
    ) -> Result<Self> {
        let scope = &worker.store.scope;
        io.reserve_bytes(
            Protection::BYTES
                + scope.tenant_id().len()
                + scope.domain().len()
                + scope.workspace_id().map_or(0, str::len)
                + scope.metastore_id().map_or(0, str::len)
                + 256,
        )?;
        let mut protected = Protection::new();
        for path in &snapshot.snapshot.required_paths {
            protected.insert(path, io)?;
        }
        let (bytes, operations, _, _) = io.test_accounting();
        Ok(Self {
            config: worker.store.capture_config(),
            snapshot,
            protected,
            walk: None,
            page_sha256: None,
            seed: seed.into(),
            cursor: None,
            complete: false,
            pages: 0,
            admission: (bytes, operations),
        })
    }
    fn validate_worker(&self, worker: &ControlMvpMaintenanceWorker) -> Result<()> {
        if self.config.scope != worker.store.scope
            || self.config.backend != worker.store.binding_identity
            || self.config.authority_format != worker.store.authority_format
            || self.config.durable_binding
                != worker.store.durable_authority_binding.as_deref().copied()
        {
            return Err(invariant_violation(
                "GC session configured authority differs",
            ));
        }
        Ok(())
    }
    fn protects(&self, path: &str, io: &mut WorkspaceIoBudget) -> Result<bool> {
        io.charge_operations(self.snapshot.snapshot.protected_prefixes.len())?;
        Ok(self
            .snapshot
            .snapshot
            .protected_prefixes
            .iter()
            .any(|prefix| path.starts_with(prefix))
            || self.protected.contains(path, io)?)
    }
    fn owned_bytes(&self) -> usize {
        self.protected.0.capacity()
            + 2 * (self.config.scope.tenant_id().len()
                + self.config.scope.domain().len()
                + self.config.scope.workspace_id().map_or(0, str::len)
                + self.config.scope.metastore_id().map_or(0, str::len))
            + self.seed.capacity()
            + self
                .snapshot
                .snapshot
                .manifests
                .iter()
                .map(|m| m.id.capacity() + m.raw_hex.capacity() + 128)
                .sum::<usize>()
            + self.walk.as_ref().map_or(0, directory::GcWalk::owned_bytes)
            + self.cursor.as_ref().map_or(0, |c| {
                c.previous.capacity() + c.after.as_ref().map_or(0, Vec::capacity)
            })
            + self.snapshot.snapshot.head_hex.capacity()
            + self
                .snapshot
                .snapshot
                .retained
                .as_ref()
                .map_or(0, |(b, v)| b.capacity() + v.capacity())
            + self
                .snapshot
                .snapshot
                .root_records
                .iter()
                .map(|(p, v, _)| p.capacity() + v.capacity() + 128)
                .sum::<usize>()
            + self
                .snapshot
                .snapshot
                .required_paths
                .iter()
                .chain(&self.snapshot.snapshot.protected_prefixes)
                .map(|p| p.capacity() + size_of::<String>())
                .sum::<usize>()
            + 4096
    }
    /// True only after independent full-root coverage and a fresh seed check.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.complete
    }
    /// Number of independently replayed pages.
    #[must_use]
    pub const fn verified_pages(&self) -> u64 {
        self.pages
    }

    /// Cumulative admitted bytes and charged operations of the latest verified page.
    #[must_use]
    pub const fn last_admission(&self) -> (usize, usize) {
        self.admission
    }
}

fn backpressure(message: &str) -> CatalogError {
    CatalogError::MaintenanceBackpressure {
        message: message.into(),
    }
}

fn seed_path(worker: &ControlMvpMaintenanceWorker, digest: &str) -> Result<String> {
    if !valid_raw_digest(digest) {
        return Err(invariant_violation("invalid mark seed identity"));
    }
    Ok(format!(
        "{}/gc-marks/v1/{digest}/seed.json",
        worker.store.paths.base_prefix()
    ))
}

fn page_path(worker: &ControlMvpMaintenanceWorker, seed: &str, ordinal: u64) -> Result<String> {
    seed_path(worker, seed)?;
    Ok(format!(
        "{}/gc-marks/v1/{seed}/pages/{ordinal:020}.json",
        worker.store.paths.base_prefix()
    ))
}

fn initial_cursor(seed: &str) -> GcMarkCursor {
    GcMarkCursor {
        ordinal: 0,
        manifest: 0,
        role: 0,
        after: None,
        previous: seed.into(),
    }
}

fn selected_root(
    worker: &ControlMvpMaintenanceWorker,
    seed: &Seed,
    cursor: &GcMarkCursor,
) -> Result<(String, physical::Role, directory::Root)> {
    if !valid_raw_digest(&cursor.previous) {
        return Err(invariant_violation("invalid mark predecessor digest"));
    }
    let selected = seed
        .snapshot
        .manifests
        .get(cursor.manifest)
        .ok_or_else(|| invariant_violation("mark cursor manifest is outside the seed"))?;
    let raw = hex::decode(&selected.raw_hex)
        .map_err(|_| invariant_violation("invalid mark manifest hex"))?;
    let manifest: super::Manifest8 = decode_json(&raw, "mark selected manifest")?;
    manifest.validate(&worker.store.scope, &selected.id)?;
    let role = match cursor.role {
        0 => physical::Role::Kv,
        1 => physical::Role::ActiveId,
        2 => physical::Role::DeliveryOrder,
        _ => return Err(invariant_violation("invalid mark root role")),
    };
    let bytes = hex::decode(&manifest.root(role)?.directory_root_hex)
        .map_err(|_| invariant_violation("invalid mark directory root hex"))?;
    let directory = directory::Directory::new(worker.lifecycle.clone(), &worker.store.scope)?;
    Ok((selected.id.clone(), role, directory.decode_root(&bytes)?))
}

fn next_cursor(
    seed: &Seed,
    cursor: &GcMarkCursor,
    after: Option<&[u8]>,
    previous: &str,
) -> Result<Option<GcMarkCursor>> {
    let mut next = cursor.clone();
    next.ordinal = next
        .ordinal
        .checked_add(1)
        .ok_or_else(|| backpressure("mark ordinal overflow"))?;
    next.previous = previous.into();
    next.after = after.map(<[u8]>::to_vec);
    if next.after.is_none() {
        if next.role < 2 {
            next.role += 1;
        } else {
            next.role = 0;
            next.manifest += 1;
        }
    }
    Ok((next.manifest < seed.snapshot.manifests.len()).then_some(next))
}

async fn inventory(
    worker: &ControlMvpMaintenanceWorker,
    prefix: &str,
    io: &mut WorkspaceIoBudget,
) -> Result<Vec<(String, String, u64)>> {
    let mut result = Vec::new();
    let mut cursor = None;
    loop {
        let page = io
            .list_page(&worker.lifecycle, prefix, cursor.as_deref(), 256)
            .await?;
        for object in page.objects {
            let path = object.path.to_string();
            if !path.starts_with(prefix)
                || object.version.is_empty()
                || result
                    .last()
                    .is_some_and(|(previous, _, _)| previous >= &path)
                || cursor.as_ref().is_some_and(|previous| previous >= &path)
            {
                return Err(invariant_violation("noncanonical mark inventory"));
            }
            io.reserve_bytes(path.len() + object.version.len() + 128)?;
            if result.len() == INVENTORY_LIMIT {
                return Err(backpressure("mark selector inventory exceeds 4096 objects"));
            }
            result.push((path, object.version, object.size));
        }
        match page.next_start_after {
            Some(next) => {
                if result.last().is_none_or(|(path, _, _)| path != &next) {
                    return Err(invariant_violation(
                        "mark inventory cursor is not its last object",
                    ));
                }
                cursor = Some(next);
            }
            None => return Ok(result),
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one refenced seed binds the complete selector and manifest inventory"
)]
async fn capture_snapshot(
    worker: &ControlMvpMaintenanceWorker,
    now: DateTime<Utc>,
    io: &mut WorkspaceIoBudget,
) -> Result<Snapshot> {
    if worker.store.authority_format != 8 {
        return Err(CatalogError::UnsupportedAuthorityFormat {
            message: "immutable marks require private authority 8".into(),
        });
    }
    let scoped = worker
        .lifecycle
        .as_legacy_scoped()
        .ok_or_else(|| invariant_violation("marks require a workspace root"))?;
    let epoch_version =
        RetentionMutationEpoch::gc_dry_run_epoch_version_bounded(scoped, io).await?;
    crate::workspace_restore::deny_gc_while_restore_journal_protected(
        scoped,
        worker.store.scope.domain(),
        io,
    )
    .await?;
    let (head, head_version) = io
        .read_exact_stable_record(
            &worker.lifecycle,
            &worker.store.paths.current_pointer(),
            MAX_HEAD_JSON_BYTES,
        )
        .await?;
    let pointer: ControlMvpPointer = decode_json(&head, "mark HEAD")?;
    pointer.validate_versioned(&worker.store.scope, 8)?;
    let retained_path = retained::pointer_path(&worker.store);
    let retained = if io.head(&worker.lifecycle, &retained_path).await?.is_some() {
        let (bytes, version) = io
            .read_exact_stable_record(&worker.lifecycle, &retained_path, MAX_CONTROL_JSON_BYTES)
            .await?;
        Some((hex::encode(bytes), version))
    } else {
        None
    };
    retained::protect_gc_selected_pointer(
        &worker.store,
        &mut BTreeSet::new(),
        &mut retained::ReadBudget::for_gc(io),
    )
    .await?;
    let pins = inventory(worker, "retention/pins/", io).await?;
    let mut required = BTreeMap::from([(
        pointer.manifest_id.clone(),
        pointer.manifest_checksum_sha256.clone(),
    )]);
    let mut roots = RetainedAuthorityRoots::new(&worker.lifecycle, now);
    let mut roots_seen = 0_usize;
    let mut required_paths = BTreeSet::new();
    let mut protected_prefixes = BTreeSet::new();
    while let Some(root) = roots.next_bounded(io).await? {
        roots_seen += 1;
        if roots_seen > INVENTORY_LIMIT {
            return Err(backpressure("mark exceeds 4096 workspace roots"));
        }
        io.charge_operations(root.required_paths.len() + root.protected_prefixes.len())?;
        for path in root.required_paths {
            io.reserve_bytes(path.len() + 128)?;
            required_paths.insert(path);
        }
        for prefix in root.protected_prefixes {
            io.reserve_bytes(prefix.len() + 128)?;
            protected_prefixes.insert(prefix);
        }
        for reference in root.authorities {
            if reference.scope() != &worker.store.scope {
                continue;
            }
            reference.validate()?;
            let digest = reference
                .manifest_sha256()
                .strip_prefix("sha256:")
                .filter(|digest| valid_raw_digest(digest))
                .ok_or_else(|| invariant_violation("invalid mark retained manifest digest"))?;
            if reference.manifest_path()
                != worker.store.paths.manifest_object(reference.manifest_id())
            {
                return Err(invariant_violation("mark retained manifest path differs"));
            }
            if required
                .insert(reference.manifest_id().to_owned(), digest.to_owned())
                .is_some_and(|previous| previous != digest)
            {
                return Err(invariant_violation("conflicting mark manifest witnesses"));
            }
        }
    }
    let prefix = format!("{}/manifests/", worker.store.paths.base_prefix());
    let objects = inventory(worker, &prefix, io).await?;
    let mut manifests = Vec::new();
    for (path, _, _) in &objects {
        let id = path
            .strip_prefix(&prefix)
            .and_then(|id| id.strip_suffix(".json"))
            .filter(|id| !id.contains('/'))
            .ok_or_else(|| invariant_violation("invalid mark manifest inventory path"))?;
        let bytes = retained::read_bounded_json(
            &worker.store,
            &mut retained::ReadBudget::for_gc(io),
            path,
            MAX_CONTROL_JSON_BYTES,
            "mark selected manifest",
        )
        .await?;
        let digest = sha256_hex(&bytes);
        if required
            .remove(id)
            .is_some_and(|expected| expected != digest)
        {
            return Err(invariant_violation(
                "mark selected manifest witness differs",
            ));
        }
        io.charge_operations(1)?;
        let manifest: super::Manifest8 = decode_json(&bytes, "mark selected manifest")?;
        manifest.validate(&worker.store.scope, id)?;
        let token = worker
            .store
            .token(id.to_owned(), manifest.logical_sequence)
            .with_manifest_witness(digest);
        worker
            .store
            .bounded_base_from_manifest(
                &token,
                &bytes,
                Some(&mut retained::ReadBudget::for_gc(io)),
                None,
            )
            .await?;
        io.reserve_bytes(bytes.len().saturating_mul(2) + id.len() + 128)?;
        manifests.push(SelectedManifest {
            id: id.to_owned(),
            raw_hex: hex::encode(bytes),
        });
    }
    if !required.is_empty() {
        return Err(invariant_violation(
            "mark inventory omits a protected manifest",
        ));
    }
    io.reserve_bytes(RECORD_BYTES * 2)?;
    Ok(Snapshot {
        head_hex: hex::encode(head),
        head_version,
        epoch_version,
        retained,
        pin_inventory: sha256_hex(&encode_json_limited(&pins, RECORD_BYTES, "mark pins")?),
        manifest_inventory: sha256_hex(&encode_json_limited(
            &objects,
            RECORD_BYTES,
            "mark manifests",
        )?),
        manifests,
        root_records: roots.take_record_versions(),
        required_paths: required_paths.into_iter().collect(),
        protected_prefixes: protected_prefixes.into_iter().collect(),
    })
}

async fn put_mark(
    worker: &ControlMvpMaintenanceWorker,
    path: &str,
    bytes: Bytes,
    io: &mut WorkspaceIoBudget,
) -> Result<()> {
    io.reserve_bytes(bytes.len())?;
    io.charge_operations(1)?;
    let outcome = worker
        .lifecycle
        .put_raw(path, bytes.clone(), WritePrecondition::DoesNotExist)
        .await;
    match outcome {
        Ok(ref result) => {
            io.charge_write(result)?;
            if matches!(result, WriteResult::Success { version } if !version.is_empty()) {
                return Ok(());
            }
        }
        Err(error) => {
            let error = CatalogError::from(error);
            if io.charge_error(&error).is_err() {
                return Err(error);
            }
        }
    }
    // An uncertain immutable PUT is resolved only by exact complete readback.
    match io
        .read_exact_stable_record(&worker.lifecycle, path, RECORD_BYTES)
        .await
    {
        Ok((selected, _)) if selected == bytes => Ok(()),
        _ => Err(CatalogError::AmbiguousAuthorityOutcome {
            message: "mark PUT lacks exact immutable readback".into(),
        }),
    }
}

async fn load_seed(
    worker: &ControlMvpMaintenanceWorker,
    digest: &str,
    io: &mut WorkspaceIoBudget,
) -> Result<Seed> {
    let bytes = io
        .read_immutable(
            &worker.lifecycle,
            &seed_path(worker, digest)?,
            None,
            digest,
            RECORD_BYTES,
        )
        .await?;
    io.reserve_bytes(RECORD_BYTES * 2)?;
    let seed: Seed = decode_json(&bytes, "mark seed")?;
    if seed.record_type != SEED_RECORD
        || seed.version != 1
        || seed.scope != worker.store.scope
        || seed.expires_at <= Utc::now()
        || seed.expires_at > Utc::now() + Duration::minutes(30)
        || encode_json_limited(&seed, RECORD_BYTES, "mark seed")? != bytes
    {
        return Err(invariant_violation(
            "invalid, expired or noncanonical mark seed",
        ));
    }
    ensure_fresh_seed(worker, &seed, io).await?;
    Ok(seed)
}

async fn ensure_fresh_seed(
    worker: &ControlMvpMaintenanceWorker,
    seed: &Seed,
    io: &mut WorkspaceIoBudget,
) -> Result<()> {
    if seed.expires_at <= Utc::now() {
        return Err(invariant_violation("mark seed expired"));
    }
    if capture_snapshot(worker, seed.collection_time, io).await? != seed.snapshot {
        return Err(CatalogError::CasFailed {
            message: "mark seed is stale".into(),
        });
    }
    Ok(())
}

async fn ensure_fresh_fence(
    worker: &ControlMvpMaintenanceWorker,
    seed: &Seed,
    io: &mut WorkspaceIoBudget,
) -> Result<()> {
    if seed.scope != worker.store.scope || seed.expires_at <= Utc::now() {
        return Err(invariant_violation("GC session scope differs or expired"));
    }
    let scoped = worker
        .lifecycle
        .as_legacy_scoped()
        .ok_or_else(|| invariant_violation("GC session requires workspace root"))?;
    let epoch = RetentionMutationEpoch::gc_dry_run_epoch_version_bounded(scoped, io).await?;
    crate::workspace_restore::deny_gc_while_restore_journal_protected(
        scoped,
        worker.store.scope.domain(),
        io,
    )
    .await?;
    let head = io
        .head(&worker.lifecycle, &worker.store.paths.current_pointer())
        .await?;
    let retained = io
        .head(&worker.lifecycle, &retained::pointer_path(&worker.store))
        .await?;
    for (path, version, size) in &seed.snapshot.root_records {
        if io
            .head(&worker.lifecycle, path)
            .await?
            .is_none_or(|meta| meta.version != *version || meta.size != *size)
        {
            return Err(CatalogError::CasFailed {
                message: "GC protected workspace record changed".into(),
            });
        }
    }
    let pins = inventory(worker, "retention/pins/", io).await?;
    let manifests = inventory(
        worker,
        &format!("{}/manifests/", worker.store.paths.base_prefix()),
        io,
    )
    .await?;
    io.reserve_bytes(RECORD_BYTES * 2)?;
    if epoch != seed.snapshot.epoch_version
        || head.is_none_or(|m| m.version != seed.snapshot.head_version)
        || retained.map(|m| m.version) != seed.snapshot.retained.as_ref().map(|(_, v)| v.clone())
        || sha256_hex(&encode_json_limited(&pins, RECORD_BYTES, "GC pin fence")?)
            != seed.snapshot.pin_inventory
        || sha256_hex(&encode_json_limited(
            &manifests,
            RECORD_BYTES,
            "GC manifest fence",
        )?) != seed.snapshot.manifest_inventory
    {
        return Err(CatalogError::CasFailed {
            message: "GC sweep seed is stale".into(),
        });
    }
    Ok(())
}

impl ControlMvpMaintenanceWorker {
    /// Writes one bounded immutable page; it never deletes or publishes authority.
    ///
    /// # Errors
    /// Rejects stale or malformed evidence, unavailable storage and exhausted admission.
    pub async fn advance_gc_mark(
        &self,
        digest: &str,
        cursor: Option<&GcMarkCursor>,
    ) -> Result<GcMarkAdvance> {
        let mut io = WorkspaceIoBudget::for_control_gc();
        seed_path(self, digest)?;
        if let Some(cursor) = cursor {
            if cursor.role > 2
                || cursor.manifest >= INVENTORY_LIMIT
                || cursor.after.is_some()
                || cursor.ordinal != (cursor.manifest as u64) * 3 + u64::from(cursor.role)
                || !valid_raw_digest(&cursor.previous)
                || cursor
                    .after
                    .as_ref()
                    .is_some_and(|after| after.len() > super::super::MAX_BLOCK_BYTES)
                || (cursor.ordinal == 0
                    && (cursor.manifest != 0
                        || cursor.role != 0
                        || cursor.after.is_some()
                        || cursor.previous != digest))
            {
                return Err(invariant_violation("invalid mark resume header"));
            }
            io.reserve_bytes(
                size_of::<GcMarkCursor>()
                    + 64
                    + cursor
                        .after
                        .as_ref()
                        .map_or(0, |after| after.len().saturating_mul(2)),
            )?;
        }
        let seed = load_seed(self, digest, &mut io).await?;
        let cursor = cursor.cloned().unwrap_or_else(|| initial_cursor(digest));
        let (manifest_id, _role, root) = selected_root(self, &seed, &cursor)?;
        let mut page = MarkPage {
            record_type: PAGE_RECORD.into(),
            version: 2,
            seed: digest.into(),
            cursor: cursor.clone(),
            manifest_id,
            root_hex: hex::encode(root.encode()),
            next: next_cursor(&seed, &cursor, None, digest)?,
        };
        io.reserve_bytes(RECORD_BYTES * 2)?;
        let bytes = encode_json_limited(&page, RECORD_BYTES, "mark page")?;
        let page_sha256 = sha256_hex(&bytes);
        put_mark(
            self,
            &page_path(self, digest, cursor.ordinal)?,
            bytes,
            &mut io,
        )
        .await?;
        ensure_fresh_seed(self, &seed, &mut io).await?;
        if let Some(next) = &mut page.next {
            next.previous.clone_from(&page_sha256);
        }
        let (admitted_bytes, charged_operations, _, _) = io.test_accounting();
        Ok(GcMarkAdvance {
            page_sha256,
            continuation: page.next,
            admitted_bytes,
            charged_operations,
        })
    }

    /// Starts an independent verifier at the beginning, including after restart.
    ///
    /// # Errors
    /// Rejects stale or malformed evidence, unavailable storage and exhausted admission.
    pub async fn begin_gc_mark_verification(&self, seed: &str) -> Result<GcMarkVerifier> {
        let mut io = WorkspaceIoBudget::for_control_gc();
        let snapshot = load_seed(self, seed, &mut io).await?;
        GcMarkVerifier::new(self, seed, snapshot, &mut io)
    }

    /// Replays one page from authenticated bytes and advances only on success.
    ///
    /// # Errors
    /// Rejects stale or malformed evidence, unavailable storage and exhausted admission.
    pub async fn advance_gc_mark_verification(&self, verifier: &mut GcMarkVerifier) -> Result<()> {
        self.verify_gc_mark_page(verifier, &mut WorkspaceIoBudget::for_control_gc(), None)
            .await
            .map(|_| ())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one independently replayed page and its final refence form one verification boundary"
    )]
    async fn verify_gc_mark_page(
        &self,
        verifier: &mut GcMarkVerifier,
        io: &mut WorkspaceIoBudget,
        candidates: Option<&mut BTreeSet<String>>,
    ) -> Result<Seed> {
        verifier.validate_worker(self)?;
        if verifier.complete {
            return Err(invariant_violation("mark verification is already complete"));
        }
        io.reserve_bytes(
            verifier
                .owned_bytes()
                .saturating_mul(3)
                .saturating_add(2 * 1024 * 1024),
        )?;
        let seed = &verifier.snapshot;
        ensure_fresh_fence(self, seed, io).await?;
        let cursor = verifier
            .cursor
            .clone()
            .unwrap_or_else(|| initial_cursor(&verifier.seed));
        let (manifest_id, role, root) = selected_root(self, seed, &cursor)?;
        let next_cursor = next_cursor(seed, &cursor, None, &verifier.seed)?;
        let mut walk = verifier.walk.clone();
        let mut page_digest = verifier.page_sha256.clone();
        if walk.is_none() {
            let bytes = io
                .read_exact_stable_record(
                    &self.lifecycle,
                    &page_path(self, &verifier.seed, cursor.ordinal)?,
                    RECORD_BYTES,
                )
                .await?
                .0;
            io.reserve_bytes(bytes.len().saturating_mul(4))?;
            let page: MarkPage = decode_json(&bytes, "GC root schedule")?;
            if page.record_type != PAGE_RECORD
                || page.version != 2
                || page.seed != verifier.seed
                || page.cursor != cursor
                || page.manifest_id != manifest_id
                || page.root_hex != hex::encode(root.encode())
                || page.next != next_cursor
                || encode_json_limited(&page, RECORD_BYTES, "GC root schedule")? != bytes
            {
                return Err(invariant_violation(
                    "GC root schedule binding or coverage differs",
                ));
            }
            page_digest = Some(sha256_hex(&bytes));
            walk = Some(directory::GcWalk::new(&root));
        }
        let mut walk = walk.ok_or_else(|| invariant_violation("GC walk is missing"))?;
        let mut protected = verifier.protected.clone();
        let mut indexes = physical::GcIndexCache::new();
        let directory = directory::Directory::new(self.lifecycle.clone(), &self.store.scope)?;
        let mut steps = 0;
        while steps < WALK_STEPS && !walk.is_complete() {
            let mut leaves = Vec::with_capacity(physical::GC_PROBE_WIDTH);
            while steps < WALK_STEPS
                && !walk.is_complete()
                && leaves.len() < physical::GC_PROBE_WIDTH
            {
                let (leaf, paths) = walk
                    .step(&directory, &mut retained::ReadBudget::for_gc(io))
                    .await?;
                steps += 1;
                for path in paths {
                    protected.insert(&path, io)?;
                }
                if let Some(leaf) = leaf {
                    io.reserve_bytes(
                        3 * (size_of::<directory::Leaf>() + leaf.first.len() + leaf.last.len()),
                    )?;
                    let large = leaf.first.len().saturating_add(leaf.last.len()) > 4096;
                    leaves.push(leaf);
                    if large {
                        break;
                    }
                }
                let (bytes, operations, _, _) = io.test_accounting();
                if bytes > 32 * 1024 * 1024 || operations > 2500 {
                    break;
                }
            }
            if !leaves.is_empty() {
                let descriptors = self
                    .store
                    .gc_mark_edges_bounded(
                        role,
                        &leaves,
                        &mut retained::ReadBudget::for_gc(io),
                        &mut indexes,
                    )
                    .await?;
                let mut remaining = leaves.into_iter();
                for descriptor in descriptors {
                    let leaf = remaining
                        .next()
                        .ok_or_else(|| invariant_violation("GC descriptor batch lost its leaf"))?;
                    for path in [
                        format!(
                            "{}/physical/descriptors/{}.json",
                            self.store.paths.base_prefix(),
                            hex::encode(leaf.digest)
                        ),
                        self.store
                            .paths
                            .state_object(&descriptor.segment.segment_id),
                        self.store
                            .paths
                            .segment_index(&descriptor.segment.segment_id),
                    ] {
                        protected.insert(&path, io)?;
                    }
                }
                walk.defer_leaves(remaining)?;
            }
            let (bytes, operations, _, _) = io.test_accounting();
            if bytes > 32 * 1024 * 1024 || operations > 2500 {
                break;
            }
        }
        if let Some(paths) = candidates {
            let mut retained = BTreeSet::new();
            for path in paths.iter() {
                if !seed
                    .snapshot
                    .protected_prefixes
                    .iter()
                    .any(|prefix| path.starts_with(prefix))
                    && !protected.contains(path, io)?
                {
                    retained.insert(path.clone());
                }
            }
            *paths = retained;
        }
        let page_digest =
            page_digest.ok_or_else(|| invariant_violation("GC root schedule digest is missing"))?;
        let root_complete = walk.is_complete();
        let complete = root_complete && next_cursor.is_none();
        if complete {
            let completion = Completion {
                record_type: "arco_authority8_gc_mark_completion",
                version: 2,
                seed: verifier.seed.clone(),
                pages: verifier.pages + 1,
                last_page_sha256: page_digest.clone(),
                manifest_inventory: seed.snapshot.manifest_inventory.clone(),
                pin_inventory: seed.snapshot.pin_inventory.clone(),
            };
            let bytes = encode_json_limited(&completion, RECORD_BYTES, "mark completion")?;
            put_mark(
                self,
                &format!(
                    "{}/gc-marks/v1/{}/complete.json",
                    self.store.paths.base_prefix(),
                    verifier.seed
                ),
                bytes,
                io,
            )
            .await?;
        }
        ensure_fresh_fence(self, seed, io).await?;
        if root_complete {
            verifier.cursor = next_cursor.map(|mut next| {
                next.previous = page_digest;
                next
            });
            verifier.pages += 1;
            verifier.walk = None;
            verifier.page_sha256 = None;
        } else {
            verifier.walk = Some(walk);
            verifier.page_sha256 = Some(page_digest);
        }
        verifier.protected = protected;
        verifier.complete = complete;
        let (bytes, operations, _, _) = io.test_accounting();
        verifier.admission = (bytes, operations);
        Ok(seed.clone())
    }

    /// Starts an immutable authority-8 mark, without changing authority or deleting.
    ///
    /// # Errors
    /// Rejects stale or malformed evidence, unavailable storage and exhausted admission.
    pub async fn begin_gc_mark_at(&self, now: DateTime<Utc>) -> Result<String> {
        self.begin_gc_mark_at_with_admission(now)
            .await
            .map(|(seed, _, _)| seed)
    }

    /// Starts a mark and exports its cumulative byte and operation invoice.
    ///
    /// # Errors
    /// Rejects stale roots, malformed evidence, unavailable storage or exhausted admission.
    pub async fn begin_gc_mark_at_with_admission(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(String, usize, usize)> {
        let mut io = WorkspaceIoBudget::for_control_gc();
        let snapshot = capture_snapshot(self, now, &mut io).await?;
        if capture_snapshot(self, now, &mut io).await? != snapshot {
            return Err(CatalogError::CasFailed {
                message: "mark seed changed during capture".into(),
            });
        }
        let seed = Seed {
            record_type: SEED_RECORD.into(),
            version: 1,
            scope: self.store.scope.clone(),
            collection_time: now,
            expires_at: Utc::now() + Duration::minutes(30),
            snapshot,
        };
        io.reserve_bytes(RECORD_BYTES)?;
        let bytes = encode_json_limited(&seed, RECORD_BYTES, "mark seed")?;
        let digest = sha256_hex(&bytes);
        put_mark(self, &seed_path(self, &digest)?, bytes, &mut io).await?;
        load_seed(self, &digest, &mut io).await?;
        let (bytes, operations, _, _) = io.test_accounting();
        Ok((digest, bytes, operations))
    }
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::super::super::ControlMvpGcCandidate;
    use super::*;
    use crate::state_store::{ControlMvpStateStore, StateScope};
    use arco_core::{MemoryBackend, ScopedStorage};
    use std::sync::Arc;

    #[tokio::test]
    async fn verifier_admission_exports_the_initial_invoice() {
        let (_, _, worker, seed) = empty_mark().await;
        let verifier = worker.begin_gc_mark_verification(&seed).await.unwrap();
        let (bytes, operations) = verifier.last_admission();
        assert!((Protection::BYTES..=64 * 1024 * 1024).contains(&bytes));
        assert!(operations > 0 && operations <= 4096);
    }

    #[tokio::test]
    async fn verified_sweep_cannot_cross_backend_or_authority_configuration() {
        for change_backend in [true, false] {
            let (storage, store, worker, seed, _) = gc_mark_fixture().await;
            produce_all(&worker, &seed).await;
            let mut planning = worker.begin_marked_gc_plan(&seed, [], None).await.unwrap();
            finish_marked_plan(&worker, &mut planning).await;
            let other = if change_backend {
                let other_storage =
                    ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace")
                        .unwrap();
                let other =
                    ControlMvpStateStore::new_synthetic_bounded(other_storage, store.scope.clone())
                        .unwrap();
                other
                    .install_synthetic_genesis(
                        "marked-gc-source",
                        1,
                        512,
                        (0_u64..512).map(|n| super::super::super::SyntheticKvEntry {
                            key: n.to_be_bytes().to_vec(),
                            generation: 1,
                            value: Some(vec![41; 80]),
                        }),
                        0,
                        [],
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    storage
                        .head_raw(&store.paths.current_pointer())
                        .await
                        .unwrap()
                        .unwrap()
                        .version,
                    other
                        .retention
                        .head_raw(&other.paths.current_pointer())
                        .await
                        .unwrap()
                        .unwrap()
                        .version,
                    "the different backends intentionally collide on object version numbers"
                );
                other
            } else {
                store.clone().with_durable_authority_binding(
                    super::super::super::DurableAuthorityBinding::new([99; 32]),
                )
            };
            let other = ControlMvpMaintenanceWorker::new_synthetic_bounded(&other);
            assert!(
                other
                    .continue_marked_gc_plan(&mut planning, [])
                    .await
                    .is_err(),
                "opaque verified state must remain bound to its configured authority"
            );
        }
    }

    #[test]
    fn protection_is_insert_only_and_saturation_only_retains_more() {
        let mut protection = Protection::new();
        let mut io = WorkspaceIoBudget::for_control_gc();
        for n in 0..3000 {
            protection
                .insert(&format!("protected/{n}"), &mut io)
                .unwrap();
        }
        let mut io = WorkspaceIoBudget::for_control_gc();
        for n in 0..3000 {
            assert!(
                protection
                    .contains(&format!("protected/{n}"), &mut io)
                    .unwrap()
            );
        }
        let mut io = WorkspaceIoBudget::for_control_gc();
        let mut retained = 0;
        for n in 0..3000 {
            retained += usize::from(
                protection
                    .contains(&format!("orphan/{n}"), &mut io)
                    .unwrap(),
            );
        }
        assert_eq!(
            retained, 0,
            "measure excess retention in this deterministic sample"
        );
        protection.0.fill(255);
        assert!(protection.contains("never-inserted", &mut io).unwrap());
    }

    #[tokio::test]
    async fn native_mark_budget_failure_does_not_advance_or_publish_completion() {
        let (storage, _, worker, seed, _) = gc_mark_fixture().await;
        produce_all(&worker, &seed).await;
        let mut verifier = worker.begin_gc_mark_verification(&seed).await.unwrap();
        let mut io = WorkspaceIoBudget::for_control_gc();
        io.charge_operations(4096).unwrap();
        assert!(matches!(
            worker
                .verify_gc_mark_page(&mut verifier, &mut io, None)
                .await,
            Err(CatalogError::MaintenanceBackpressure { .. })
        ));
        assert_eq!(verifier.pages, 0);
        assert!(verifier.walk.is_none() && !verifier.complete);
        assert!(verifier.protected.0.iter().all(|byte| *byte == 0));
        assert!(
            storage
                .head_raw(&format!(
                    "{}/gc-marks/v1/{seed}/complete.json",
                    worker.store.paths.base_prefix()
                ))
                .await
                .unwrap()
                .is_none()
        );
        worker
            .advance_gc_mark_verification(&mut verifier)
            .await
            .unwrap();
        assert!(verifier.pages > 0 || verifier.walk.is_some());
    }

    #[tokio::test]
    async fn verified_sweep_rejects_changed_seed_and_candidate_on_subsequent_pages() {
        for mutation in 0..3 {
            let (storage, store, worker, seed, _) = gc_mark_fixture().await;
            produce_all(&worker, &seed).await;
            let orphan = format!(
                "control/directory/v1/domains/catalog/pages/{}",
                sha256_hex(b"late orphan")
            );
            storage
                .put_raw(
                    &orphan,
                    Bytes::from_static(b"old"),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
            let mut planning = worker.begin_marked_gc_plan(&seed, [], None).await.unwrap();
            let plan = finish_marked_plan(&worker, &mut planning).await;
            assert_eq!(plan.continuation(), Some("v8:directory:"));
            if mutation == 0 {
                let head = store.paths.current_pointer();
                let raw = storage.get_raw(&head).await.unwrap();
                storage
                    .put_raw(&head, raw, WritePrecondition::None)
                    .await
                    .unwrap();
                assert!(
                    worker
                        .continue_marked_gc_plan(&mut planning, [])
                        .await
                        .is_err()
                );
            } else {
                worker
                    .continue_marked_gc_plan(&mut planning, [])
                    .await
                    .unwrap();
                let path = if mutation == 1 {
                    orphan.clone()
                } else {
                    store.paths.current_pointer()
                };
                let raw = storage.get_raw(&path).await.unwrap();
                storage
                    .put_raw(&path, raw, WritePrecondition::None)
                    .await
                    .unwrap();
                let before = planning.last_admission();
                for _ in 0..2 {
                    assert!(worker.advance_marked_gc_plan(&mut planning).await.is_err());
                }
                assert_eq!(planning.last_admission(), before);
            }
        }
    }

    async fn empty_mark() -> (
        ScopedStorage,
        ControlMvpStateStore,
        ControlMvpMaintenanceWorker,
        String,
    ) {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        store
            .install_synthetic_genesis("mark-empty", 1, 0, [], 0, [])
            .await
            .unwrap();
        let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
        let seed = worker.begin_gc_mark_at(Utc::now()).await.unwrap();
        (storage, store, worker, seed)
    }

    async fn produce_all(worker: &ControlMvpMaintenanceWorker, seed: &str) -> u64 {
        let mut cursor = None;
        let mut pages = 0;
        loop {
            let result = worker.advance_gc_mark(seed, cursor.as_ref()).await.unwrap();
            pages += 1;
            cursor = result.continuation;
            if cursor.is_none() {
                return pages;
            }
        }
    }

    async fn gc_mark_fixture() -> (
        ScopedStorage,
        ControlMvpStateStore,
        ControlMvpMaintenanceWorker,
        String,
        String,
    ) {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        store
            .install_synthetic_genesis(
                "marked-gc-source",
                1,
                512,
                (0_u64..512).map(|ordinal| super::super::super::SyntheticKvEntry {
                    key: ordinal.to_be_bytes().to_vec(),
                    generation: 1,
                    value: Some(vec![42; 80]),
                }),
                0,
                [],
            )
            .await
            .unwrap();
        let orphan = store.paths.state_object("000-marked-gc-orphan");
        storage
            .put_raw(
                &orphan,
                Bytes::from_static(b"unattached"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
        let seed = worker
            .begin_gc_mark_at(Utc::now() + Duration::days(40))
            .await
            .unwrap();
        (storage, store, worker, seed, orphan)
    }

    #[tokio::test]
    async fn marked_gc_plan_replays_all_roots_before_selecting_only_the_orphan() {
        let (_, _, worker, seed, orphan) = gc_mark_fixture().await;
        let pages = produce_all(&worker, &seed).await;
        assert!(pages > 1);
        let mut planning = worker.begin_marked_gc_plan(&seed, [], None).await.unwrap();
        for ordinal in 0..pages {
            let result = worker.advance_marked_gc_plan(&mut planning).await.unwrap();
            assert_eq!(planning.verified_pages(), ordinal + 1);
            let (bytes, operations) = planning.last_admission();
            assert!(bytes <= 64 * 1024 * 1024 && operations <= 4096);
            if ordinal + 1 < pages {
                assert!(
                    result.is_none(),
                    "partial coverage cannot produce candidates"
                );
            } else {
                let plan = result.expect("complete independently verified candidate page");
                assert_eq!(
                    plan.candidates()
                        .iter()
                        .map(ControlMvpGcCandidate::path)
                        .collect::<Vec<_>>(),
                    vec![orphan.as_str()]
                );
            }
        }
        assert!(worker.advance_marked_gc_plan(&mut planning).await.is_err());
    }

    async fn finish_marked_plan(
        worker: &ControlMvpMaintenanceWorker,
        planning: &mut GcMarkedPlan,
    ) -> super::super::super::ControlMvpGcPlan {
        loop {
            if let Some(plan) = worker.advance_marked_gc_plan(planning).await.unwrap() {
                return plan;
            }
        }
    }

    #[tokio::test]
    async fn marked_gc_plan_protects_directory_pages_and_explicit_paths() {
        let (storage, store, worker, seed, orphan) = gc_mark_fixture().await;
        produce_all(&worker, &seed).await;
        let directory_orphan = format!(
            "control/directory/v1/domains/catalog/pages/{}",
            sha256_hex(b"unattached directory")
        );
        storage
            .put_raw(
                &directory_orphan,
                Bytes::from_static(b"unattached directory"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        let mut planning = worker
            .begin_marked_gc_plan(&seed, [], Some("v8:directory:"))
            .await
            .unwrap();
        let plan = finish_marked_plan(&worker, &mut planning).await;
        assert_eq!(
            plan.candidates()
                .iter()
                .map(ControlMvpGcCandidate::path)
                .collect::<Vec<_>>(),
            vec![directory_orphan.as_str()]
        );
        assert!(plan.continuation().is_none());
        let mut planning = worker
            .begin_marked_gc_plan(&seed, [orphan], None)
            .await
            .unwrap();
        assert!(
            finish_marked_plan(&worker, &mut planning)
                .await
                .candidates()
                .is_empty()
        );
        assert!(
            storage
                .head_raw(&store.paths.current_pointer())
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn marked_gc_plan_restart_replays_despite_a_stored_completion_receipt() {
        let (storage, _, worker, seed, orphan) = gc_mark_fixture().await;
        let pages = produce_all(&worker, &seed).await;
        let mut planning = worker.begin_marked_gc_plan(&seed, [], None).await.unwrap();
        assert!(
            worker
                .advance_marked_gc_plan(&mut planning)
                .await
                .unwrap()
                .is_none()
        );
        drop(planning);
        let mut restarted = worker.begin_marked_gc_plan(&seed, [], None).await.unwrap();
        assert_eq!(restarted.verified_pages(), 0);
        let plan = finish_marked_plan(&worker, &mut restarted).await;
        assert_eq!(restarted.verified_pages(), pages);
        assert_eq!(plan.candidates()[0].path(), orphan);
        storage
            .delete(&page_path(&worker, &seed, 0).unwrap())
            .await
            .unwrap();
        let mut restarted = worker.begin_marked_gc_plan(&seed, [], None).await.unwrap();
        assert!(worker.advance_marked_gc_plan(&mut restarted).await.is_err());
        assert_eq!(
            restarted.verified_pages(),
            0,
            "completion bytes cannot replace a missing page"
        );
    }

    #[tokio::test]
    async fn marked_gc_plan_refences_candidate_versions_without_committing_failed_progress() {
        for mutation in 0..3 {
            let (storage, _, worker, seed, orphan) = gc_mark_fixture().await;
            let pages = produce_all(&worker, &seed).await;
            let mut planning = worker.begin_marked_gc_plan(&seed, [], None).await.unwrap();
            for _ in 1..pages {
                assert!(
                    worker
                        .advance_marked_gc_plan(&mut planning)
                        .await
                        .unwrap()
                        .is_none()
                );
            }
            let admission = planning.last_admission();
            match mutation {
                0 => {
                    let bytes = storage.get_raw(&orphan).await.unwrap();
                    storage
                        .put_raw(&orphan, bytes, WritePrecondition::None)
                        .await
                        .unwrap();
                }
                1 => {
                    storage
                        .put_raw(
                            &orphan,
                            Bytes::from_static(b"changed size"),
                            WritePrecondition::None,
                        )
                        .await
                        .unwrap();
                }
                _ => storage.delete(&orphan).await.unwrap(),
            }
            for _ in 0..2 {
                assert!(matches!(
                    worker.advance_marked_gc_plan(&mut planning).await,
                    Err(CatalogError::CasFailed { .. })
                ));
                assert_eq!(planning.verified_pages(), pages - 1);
                assert_eq!(planning.last_admission(), admission);
            }
        }
    }

    #[tokio::test]
    async fn marked_gc_plan_denies_changed_head_manifest_epoch_and_selectors() {
        for mutation in 0..5 {
            let (storage, store, worker, seed, _) = gc_mark_fixture().await;
            produce_all(&worker, &seed).await;
            let mut planning = worker.begin_marked_gc_plan(&seed, [], None).await.unwrap();
            assert!(
                worker
                    .advance_marked_gc_plan(&mut planning)
                    .await
                    .unwrap()
                    .is_none()
            );
            let path = match mutation {
                0 => store.paths.current_pointer(),
                1 => {
                    let pointer: ControlMvpPointer = decode_json(
                        &storage
                            .get_raw(&store.paths.current_pointer())
                            .await
                            .unwrap(),
                        "test HEAD",
                    )
                    .unwrap();
                    store.paths.manifest_object(&pointer.manifest_id)
                }
                2 => crate::retention_coordination::RETENTION_MUTATION_EPOCH_PATH.into(),
                3 => "retention/pins/pin_01ARZ3NDEKTSV4RRFFQ69G5FAV/latest.json".into(),
                _ => retained::pointer_path(&store),
            };
            let bytes = if mutation < 2 {
                storage.get_raw(&path).await.unwrap()
            } else {
                Bytes::from_static(b"{}")
            };
            storage
                .put_raw(&path, bytes, WritePrecondition::None)
                .await
                .unwrap();
            assert!(
                worker.advance_marked_gc_plan(&mut planning).await.is_err(),
                "mutation {mutation}"
            );
            assert_eq!(planning.verified_pages(), 1);
        }
    }

    #[tokio::test]
    async fn marked_gc_plan_rejects_tampered_coverage_without_losing_retry_state() {
        for mutation in 0..4 {
            let (storage, _, worker, seed, _) = gc_mark_fixture().await;
            produce_all(&worker, &seed).await;
            let mut planning = worker.begin_marked_gc_plan(&seed, [], None).await.unwrap();
            let path = page_path(&worker, &seed, 0).unwrap();
            let original = storage.get_raw(&path).await.unwrap();
            let mut page: MarkPage = decode_json(&original, "test page").unwrap();
            match mutation {
                0 => page.version = 1,
                1 => page.manifest_id = "omitted-root".into(),
                2 => page.next = None,
                _ => page.cursor.previous = "ab".repeat(32),
            }
            storage
                .put_raw(
                    &path,
                    encode_json_limited(&page, RECORD_BYTES, "tampered page").unwrap(),
                    WritePrecondition::None,
                )
                .await
                .unwrap();
            assert!(worker.advance_marked_gc_plan(&mut planning).await.is_err());
            assert_eq!(planning.verified_pages(), 0);
            storage
                .put_raw(&path, original, WritePrecondition::None)
                .await
                .unwrap();
            assert!(
                worker
                    .advance_marked_gc_plan(&mut planning)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(planning.verified_pages(), 1);
        }
    }

    #[tokio::test]
    async fn marked_gc_plan_bounds_extra_protection_and_rejects_out_of_scope_cursors() {
        let (_, _, worker, seed, orphan) = gc_mark_fixture().await;
        assert!(
            worker
                .begin_marked_gc_plan(&seed, ["other-domain/path".into()], None)
                .await
                .is_err()
        );
        for cursor in [
            "v7:authority:",
            "v8:authority:control/v1/domains/other/",
            "v8:directory:control/directory/v1/domains/other/",
        ] {
            assert!(
                worker
                    .begin_marked_gc_plan(&seed, [], Some(cursor))
                    .await
                    .is_err()
            );
        }
        let extra = std::iter::repeat_with(|| orphan.clone()).take(4097);
        assert!(matches!(
            worker.begin_marked_gc_plan(&seed, extra, None).await,
            Err(CatalogError::MaintenanceBackpressure { .. })
        ));
    }

    #[tokio::test]
    async fn marked_gc_plan_restarts_across_inventory_pages_and_both_prefixes() {
        let (storage, store, worker, seed, orphan) = gc_mark_fixture().await;
        let mut expected = BTreeSet::from([orphan]);
        for ordinal in 0..300 {
            let path = store
                .paths
                .state_object(&format!("more-marked-orphan-{ordinal:04}"));
            storage
                .put_raw(
                    &path,
                    Bytes::from_static(b"orphan"),
                    WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
            expected.insert(path);
        }
        let directory_orphan = format!(
            "control/directory/v1/domains/catalog/pages/{}",
            sha256_hex(b"continuation directory orphan")
        );
        storage
            .put_raw(
                &directory_orphan,
                Bytes::from_static(b"orphan"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        expected.insert(directory_orphan);
        produce_all(&worker, &seed).await;
        let mut continuation = None;
        let mut collected = BTreeSet::new();
        let mut pages = 0;
        loop {
            let mut planning = worker
                .begin_marked_gc_plan(&seed, [], continuation.as_deref())
                .await
                .unwrap();
            assert_eq!(planning.verified_pages(), 0);
            let plan = finish_marked_plan(&worker, &mut planning).await;
            assert!(plan.candidates().len() <= 256);
            for candidate in plan.candidates() {
                assert!(expected.contains(candidate.path()));
                assert!(collected.insert(candidate.path().to_owned()));
            }
            pages += 1;
            assert!(pages <= 8);
            continuation = plan.continuation().map(str::to_owned);
            if continuation.is_none() {
                break;
            }
        }
        assert!(
            pages >= 3,
            "multiple authority pages and the directory prefix must be visited"
        );
        assert_eq!(collected, expected);
    }

    #[tokio::test]
    async fn mark_completion_persists_a_receipt_but_restart_reverifies_coverage() {
        let (storage, store, worker, seed) = empty_mark().await;
        let pages = produce_all(&worker, &seed).await;
        let mut verifier = worker.begin_gc_mark_verification(&seed).await.unwrap();
        while !verifier.is_complete() {
            worker
                .advance_gc_mark_verification(&mut verifier)
                .await
                .unwrap();
        }
        assert_eq!(verifier.verified_pages(), pages);
        let path = format!(
            "{}/gc-marks/v1/{seed}/complete.json",
            store.paths().base_prefix()
        );
        assert!(
            storage.head_raw(&path).await.unwrap().is_some(),
            "verified completion receipt"
        );
        let restarted = worker.begin_gc_mark_verification(&seed).await.unwrap();
        assert!(
            !restarted.is_complete(),
            "persisted receipt cannot confer coverage"
        );
        assert_eq!(restarted.verified_pages(), 0);
    }

    #[tokio::test]
    async fn mark_rejects_forged_completion_without_pages() {
        let (storage, store, worker, seed) = empty_mark().await;
        let path = format!(
            "{}/gc-marks/v1/{seed}/complete.json",
            store.paths().base_prefix()
        );
        storage
            .put_raw(
                &path,
                Bytes::from_static(b"{\"complete\":true,\"count\":3}"),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        let mut verifier = worker.begin_gc_mark_verification(&seed).await.unwrap();
        assert!(
            worker
                .advance_gc_mark_verification(&mut verifier)
                .await
                .is_err()
        );
        assert!(!verifier.is_complete());
        assert_eq!(verifier.verified_pages(), 0);
    }

    #[tokio::test]
    async fn mark_rejects_tampered_tree_leaf_cursor_and_chain_without_advancing_verifier() {
        for mutation in 0..7 {
            let (storage, _, worker, seed) = empty_mark().await;
            worker.advance_gc_mark(&seed, None).await.unwrap();
            let path = page_path(&worker, &seed, 0).unwrap();
            let raw = storage.get_raw(&path).await.unwrap();
            let mut page: MarkPage = decode_json(&raw, "test mark").unwrap();
            match mutation {
                0 => page.version = 1,
                1 => page.record_type = "forged".into(),
                2 => page.cursor.previous = "ab".repeat(32),
                3 => {
                    page.next = None;
                }
                4 => page.root_hex.replace_range(0..2, "ff"),
                5 => {
                    page.cursor.after = Some(b"skip".to_vec());
                }
                _ => {
                    page.manifest_id = "other-manifest".into();
                }
            }
            storage
                .put_raw(
                    &path,
                    encode_json_limited(&page, RECORD_BYTES, "forged page").unwrap(),
                    WritePrecondition::None,
                )
                .await
                .unwrap();
            let mut verifier = worker.begin_gc_mark_verification(&seed).await.unwrap();
            assert!(
                worker
                    .advance_gc_mark_verification(&mut verifier)
                    .await
                    .is_err(),
                "mutation {mutation}"
            );
            assert!(!verifier.is_complete());
            assert_eq!(verifier.verified_pages(), 0);
        }
    }

    #[tokio::test]
    async fn mark_rejects_stale_head_and_expired_or_altered_seed() {
        let (storage, _, worker, digest) = empty_mark().await;
        let raw = storage
            .get_raw(&seed_path(&worker, &digest).unwrap())
            .await
            .unwrap();
        for expired in [true, false] {
            let mut seed: Seed = decode_json(&raw, "test seed").unwrap();
            if expired {
                seed.expires_at = Utc::now() - Duration::seconds(1);
            } else {
                seed.snapshot.manifests.clear();
            }
            let bytes = encode_json_limited(&seed, RECORD_BYTES, "altered seed").unwrap();
            let forged = sha256_hex(&bytes);
            storage
                .put_raw(
                    &seed_path(&worker, &forged).unwrap(),
                    bytes,
                    WritePrecondition::DoesNotExist,
                )
                .await
                .unwrap();
            assert!(worker.advance_gc_mark(&forged, None).await.is_err());
            assert!(worker.begin_gc_mark_verification(&forged).await.is_err());
        }
        let head = worker.store.paths.current_pointer();
        let bytes = storage.get_raw(&head).await.unwrap();
        storage
            .put_raw(&head, bytes, WritePrecondition::None)
            .await
            .unwrap();
        assert!(matches!(
            worker.advance_gc_mark(&digest, None).await,
            Err(CatalogError::CasFailed { .. })
        ));
    }

    #[tokio::test]
    async fn mark_seed_rejects_a_forged_far_future_expiry() {
        let (storage, _, worker, digest) = empty_mark().await;
        let raw = storage
            .get_raw(&seed_path(&worker, &digest).unwrap())
            .await
            .unwrap();
        let mut seed: Seed = decode_json(&raw, "test seed").unwrap();
        seed.expires_at = Utc::now() + Duration::days(365);
        let bytes = encode_json_limited(&seed, RECORD_BYTES, "forged expiry").unwrap();
        let digest = sha256_hex(&bytes);
        storage
            .put_raw(
                &seed_path(&worker, &digest).unwrap(),
                bytes,
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        assert!(
            worker.begin_gc_mark_verification(&digest).await.is_err(),
            "a self-consistent digest must not extend mark admission"
        );
    }

    #[tokio::test]
    async fn mark_physical_graph_rejects_corrupt_missing_and_changed_versions() {
        for mutation in 0..5 {
            let (storage, store, worker, seed, _) = gc_mark_fixture().await;
            produce_all(&worker, &seed).await;
            let objects = storage
                .list(&format!("{}/", store.paths.base_prefix()))
                .await
                .unwrap();
            let descriptor = objects
                .iter()
                .find(|o| o.as_str().contains("/physical/descriptors/"))
                .unwrap();
            let raw = storage.get_raw(descriptor.as_str()).await.unwrap();
            let physical: physical::Descriptor = decode_json(&raw, "test descriptor").unwrap();
            let path = match mutation {
                0 | 1 => descriptor.to_string(),
                2 => store.paths.segment_index(&physical.segment.segment_id),
                _ => store.paths.state_object(&physical.segment.segment_id),
            };
            if mutation == 1 {
                storage.delete(&path).await.unwrap();
            } else {
                let bytes = if mutation >= 3 {
                    storage.get_raw(&path).await.unwrap()
                } else {
                    Bytes::from_static(b"corrupt")
                };
                storage
                    .put_raw(&path, bytes, WritePrecondition::None)
                    .await
                    .unwrap();
            }
            let mut verifier = worker.begin_gc_mark_verification(&seed).await.unwrap();
            assert!(
                worker
                    .advance_gc_mark_verification(&mut verifier)
                    .await
                    .is_err(),
                "mutation {mutation}"
            );
            assert_eq!(verifier.verified_pages(), 0);
            assert!(!verifier.is_complete());
        }
    }

    #[tokio::test]
    async fn mark_seed_rejects_a_changed_canonical_workspace_selector() {
        use crate::workspace_snapshot::{
            RetentionPinLatest, RetentionPinRevision, RetentionTarget, encode_retention_pin_latest,
            encode_retention_pin_revision, retention_pin_latest_path, retention_pin_revision_path,
        };
        let (storage, _, worker, digest) = empty_mark().await;
        let now = Utc::now();
        let pin = format!("pin_{}", ulid::Ulid::from(1900_u128));
        let target = format!("snap_{}", ulid::Ulid::from(1901_u128));
        let revision = RetentionPinRevision::new(
            &pin,
            1,
            RetentionTarget::snapshot(&target).unwrap(),
            now - Duration::days(2),
            now - Duration::days(1),
            None,
        )
        .unwrap();
        let raw = encode_retention_pin_revision(&revision).unwrap();
        let path = retention_pin_revision_path(&pin, 1).unwrap();
        storage
            .put_raw(
                &path,
                Bytes::copy_from_slice(&raw),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        let latest =
            RetentionPinLatest::new(&pin, 1, path, format!("sha256:{}", sha256_hex(&raw))).unwrap();
        storage
            .put_raw(
                &retention_pin_latest_path(&pin).unwrap(),
                Bytes::from(encode_retention_pin_latest(&latest).unwrap()),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap();
        assert!(matches!(
            worker.advance_gc_mark(&digest, None).await,
            Err(CatalogError::CasFailed { .. })
        ));
    }

    #[tokio::test]
    async fn mark_rejects_oversized_persisted_material_before_verifier_progress() {
        let (storage, _, worker, seed) = empty_mark().await;
        worker.advance_gc_mark(&seed, None).await.unwrap();
        let path = page_path(&worker, &seed, 0).unwrap();
        storage
            .put_raw(
                &path,
                Bytes::from(vec![b' '; RECORD_BYTES + 1]),
                WritePrecondition::None,
            )
            .await
            .unwrap();
        let mut verifier = worker.begin_gc_mark_verification(&seed).await.unwrap();
        assert!(
            worker
                .advance_gc_mark_verification(&mut verifier)
                .await
                .is_err()
        );
        assert_eq!(verifier.verified_pages(), 0);
    }

    #[tokio::test]
    async fn mark_seed_is_immutable_and_does_not_publish_authority() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        store
            .install_synthetic_genesis("mark-seed", 1, 0, [], 0, [])
            .await
            .unwrap();
        let head = storage
            .get_raw(&store.paths().current_pointer())
            .await
            .unwrap();
        let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
        let seed = worker
            .begin_gc_mark_at(Utc::now())
            .await
            .expect("persisted mark seed");
        assert_eq!(seed.len(), 64);
        assert_eq!(
            storage
                .get_raw(&store.paths().current_pointer())
                .await
                .unwrap(),
            head
        );
        let path = format!(
            "{}/gc-marks/v1/{seed}/seed.json",
            store.paths().base_prefix()
        );
        assert!(storage.head_raw(&path).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn mark_pages_resume_and_completion_replays_all_roles_after_restart() {
        let storage =
            ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant", "workspace").unwrap();
        let store = ControlMvpStateStore::new_synthetic_bounded(
            storage.clone(),
            StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        let entries = (0_u64..8448).map(|ordinal| super::super::super::SyntheticKvEntry {
            key: [vec![b'k'; 65], ordinal.to_be_bytes().to_vec()].concat(),
            generation: 1,
            value: Some(vec![42; 80]),
        });
        store
            .install_synthetic_genesis("mark-pages", 1, 8448, entries, 0, [])
            .await
            .unwrap();
        let worker = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
        let seed = worker.begin_gc_mark_at(Utc::now()).await.unwrap();
        let mut cursor = None;
        let mut pages = 0;
        loop {
            let result = worker
                .advance_gc_mark(&seed, cursor.as_ref())
                .await
                .expect("immutable mark page");
            pages += 1;
            cursor = result.continuation;
            if cursor.is_none() {
                break;
            }
        }
        assert!(
            pages == 3,
            "schedules cover roots; native traversal independently covers all leaves"
        );
        let restarted = ControlMvpMaintenanceWorker::new_synthetic_bounded(&store);
        for _ in 0..2 {
            let mut verifier = restarted.begin_gc_mark_verification(&seed).await.unwrap();
            while !verifier.is_complete() {
                restarted
                    .advance_gc_mark_verification(&mut verifier)
                    .await
                    .expect("independent coverage");
            }
            assert_eq!(verifier.verified_pages(), pages);
        }
    }
}

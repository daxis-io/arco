//! Bounded immutable index over logical block references; no authority publication.
//!
//! A trusted root authenticates physical directory structure. A returned [`Leaf`]
//! is only a candidate block: the caller must decode its authenticated bytes and
//! validate actual endpoints, count, and logical contents before using values.
//! Page hashes must never substitute for logical history or rewrite equivalence.
use super::{
    MAX_BLOCK_BYTES, MAX_SEGMENT_BYTES, invariant_violation, put_immutable_matching,
    validation_failed,
};
use crate::error::{CatalogError, Result};
use crate::state_store::StateScope;
use arco_core::{AuthorityRoot, RootStorage, ScopedAuthorityStore, ScopedStorage};
use bytes::Bytes;
use sha2::{Digest, Sha256};
#[cfg(any(test, feature = "test-utils"))]
use std::collections::BTreeMap;

pub(super) mod restore;
pub(super) mod update;

const PAGE_MAGIC: &[u8; 8] = b"ARCODIR1";
const PAGE_MAGIC_V2: &[u8; 8] = b"ARCODIR2";
const KEY_MAGIC_V2: &[u8; 8] = b"ARCOKEY2";
const ROOT_MAGIC: &[u8; 8] = b"ARCOROT1";
const FANOUT: usize = 128;
const DEPTH: usize = 8;
const PAGE_LIMIT: usize = 64 * 1024;
const HEADER_BYTES: usize = 43;
const PUBLICATION_SALT_BYTES: usize = 32;
const KEY_V2_FLAG: u32 = 1 << 31;
const KEY_V2_HEADER_BYTES: usize = 8 + PUBLICATION_SALT_BYTES;
const INLINE_KEY_BYTES: usize = 64;
const NODE_BYTES: usize = 245;
const ROOT_BYTES: usize = 40 + NODE_BYTES;
const OBJECT_LIMIT: usize = 4096;
const PAGE_PROBE_LIMIT: usize = 4 * 1024 * 1024;
const KEY_CACHE_ENTRIES: usize = 1024;
const KEY_CACHE_PAYLOAD_BYTES: usize = 128 * 1024;
const _: () = assert!(
    KEY_CACHE_ENTRIES * size_of::<CachedKey>()
        + size_of::<Vec<CachedKey>>()
        + KEY_CACHE_PAYLOAD_BYTES
        <= 256 * 1024
);

#[derive(Clone, Debug)]
struct CachedKey {
    scope: [u8; 32],
    digest: [u8; 32],
    bytes: Box<[u8]>,
}
const _: () = assert!(DEPTH * PAGE_PROBE_LIMIT + 2 * (MAX_BLOCK_BYTES + 1) <= MAX_SEGMENT_BYTES);
const _: () = assert!(DEPTH * (FANOUT * 2 + 1) + 2 <= OBJECT_LIMIT);

/// Metadata from an authenticated logical block encoder, including tombstone rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Leaf {
    pub first: Vec<u8>,
    pub last: Vec<u8>,
    pub rows: u64,
    pub bytes: u32,
    pub digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct KeyRef {
    digest: [u8; 32],
    bytes: u32,
    inline: [u8; INLINE_KEY_BYTES],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Node {
    depth: u8,
    bytes: u32,
    rows: u64,
    first: KeyRef,
    last: KeyRef,
    digest: [u8; 32],
}

/// Must be bound by an authenticated authority before use by an authority reader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Root {
    scope: [u8; 32],
    node: Node,
}
impl Root {
    /// Number of directory pages on a path from this root to a leaf descriptor.
    pub const fn depth(&self) -> u8 {
        self.node.depth
    }

    /// Logical row count committed by the root's directory summary.
    pub const fn row_count(&self) -> u64 {
        self.node.rows
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(ROOT_BYTES);
        out.extend_from_slice(ROOT_MAGIC);
        out.extend_from_slice(&self.scope);
        encode_node(&mut out, &self.node);
        out
    }
}

#[derive(Clone)]
pub struct Directory {
    storage: ScopedAuthorityStore,
    scope: [u8; 32],
    prefix: String,
    publication_salt: Option<Box<[u8; PUBLICATION_SALT_BYTES]>>,
}
/// Streaming writer. A failed storage operation poisons the writer; rebuild from
/// the ordered input to reconcile immutable objects without publishing partial work.
pub struct Builder {
    directory: Directory,
    levels: [Vec<Node>; DEPTH],
    last: Option<Vec<u8>>,
    failed: bool,
}
#[derive(Debug)]
pub struct Cursor {
    root: Root,
    lower: Vec<u8>,
    upper: Option<Vec<u8>>,
    after: Vec<u8>,
}
impl Cursor {
    #[cfg(any(test, feature = "test-utils"))]
    pub(in super::super) fn after(&self) -> &[u8] {
        &self.after
    }
}
#[derive(Debug)]
pub struct Page {
    pub leaves: Vec<Leaf>,
    pub next: Option<Cursor>,
}

/// Cumulative conservative probe accounting, including failed reads and retries.
pub struct ReadBudget<'a> {
    workspace: Option<&'a mut crate::workspace_io_budget::WorkspaceIoBudget>,
    retained: Option<&'a mut (dyn ReadInvoice + Send)>,
    object_limit: usize,
    byte_limit: usize,
    pub objects: usize,
    pub bytes: usize,
    // Paths enter here only after the corresponding immutable object is authenticated.
    authenticated_paths: Option<Vec<String>>,
    // Authenticated immutable fence bytes live only for this bounded read operation.
    cached_keys: Vec<CachedKey>,
    cached_key_bytes: usize,
    #[cfg(any(test, feature = "test-utils"))]
    proof_io: ProofIo<'a>,
}

#[cfg(any(test, feature = "test-utils"))]
#[derive(Default)]
enum ProofIo<'a> {
    #[default]
    None,
    #[allow(
        clippy::box_collection,
        reason = "the capture control block must not enlarge every directory coroutine frame"
    )]
    Capture(Box<BTreeMap<String, Bytes>>),
    Replay(&'a BTreeMap<String, Bytes>),
}

/// Charges a directory probe to an enclosing authority-specific read budget.
///
/// The directory keeps its local structural ceiling while the enclosing reader
/// owns the public invocation and retained-source accounting.
pub(in super::super) trait ReadInvoice: Send {
    fn charge_directory_probe(&mut self, bytes: usize) -> Result<()>;
}
impl Default for ReadBudget<'_> {
    fn default() -> Self {
        Self {
            object_limit: OBJECT_LIMIT,
            byte_limit: MAX_SEGMENT_BYTES,
            objects: 0,
            bytes: 0,
            authenticated_paths: None,
            cached_keys: Vec::new(),
            cached_key_bytes: 0,
            workspace: None,
            retained: None,
            #[cfg(any(test, feature = "test-utils"))]
            proof_io: ProofIo::None,
        }
    }
}
impl<'a> ReadBudget<'a> {
    #[cfg(any(test, feature = "test-utils"))]
    pub(in super::super) fn capturing(retained: &'a mut (dyn ReadInvoice + Send)) -> Self {
        Self {
            proof_io: ProofIo::Capture(Box::default()),
            ..Self::tracking_with_retained(retained)
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(in super::super) fn replaying(
        retained: &'a mut (dyn ReadInvoice + Send),
        objects: &'a BTreeMap<String, Bytes>,
    ) -> Self {
        Self {
            proof_io: ProofIo::Replay(objects),
            ..Self::tracking_with_retained(retained)
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(in super::super) fn take_captured_objects(&mut self) -> BTreeMap<String, Bytes> {
        match &mut self.proof_io {
            ProofIo::Capture(objects) => std::mem::take(objects.as_mut()),
            _ => BTreeMap::new(),
        }
    }
    pub(in super::super) fn with_workspace(
        workspace: &'a mut crate::workspace_io_budget::WorkspaceIoBudget,
    ) -> Self {
        Self {
            workspace: Some(workspace),
            ..Self::default()
        }
    }

    pub(in super::super) fn with_retained(retained: &'a mut (dyn ReadInvoice + Send)) -> Self {
        Self {
            retained: Some(retained),
            ..Self::default()
        }
    }

    pub fn new(objects: usize, bytes: usize) -> Result<Self> {
        if objects > OBJECT_LIMIT || bytes > MAX_SEGMENT_BYTES {
            return Err(validation_failed(
                "directory read budget exceeds hard ceilings",
            ));
        }
        Ok(Self {
            object_limit: objects,
            byte_limit: bytes,
            objects: 0,
            bytes: 0,
            authenticated_paths: None,
            cached_keys: Vec::new(),
            cached_key_bytes: 0,
            workspace: None,
            retained: None,
            #[cfg(any(test, feature = "test-utils"))]
            proof_io: ProofIo::None,
        })
    }
    fn charge(&mut self, bytes: usize) -> Result<()> {
        if self.objects >= self.object_limit || bytes > self.byte_limit.saturating_sub(self.bytes) {
            return Err(capacity("directory read budget exhausted"));
        }
        if let Some(retained) = &mut self.retained {
            retained.charge_directory_probe(bytes)?;
        } else if let Some(workspace) = &mut self.workspace {
            workspace.reserve_bytes(bytes)?;
            workspace.charge_operations(1)?;
        }
        self.objects += 1;
        self.bytes += bytes;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(in super::super) fn tracking_with_retained(
        retained: &'a mut (dyn ReadInvoice + Send),
    ) -> Self {
        Self {
            retained: Some(retained),
            authenticated_paths: Some(Vec::new()),
            ..Self::default()
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(in super::super) fn take_authenticated_paths(&mut self) -> Vec<String> {
        self.authenticated_paths
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }
}

/// Opaque bounded DFS progress. Only authenticated parent pages can add nodes.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone, Debug)]
pub(in super::super) struct GcWalk {
    stack: Vec<Node>,
    pending: Vec<Leaf>,
    keys: Vec<CachedKey>,
    key_bytes: usize,
}

#[cfg(any(test, feature = "test-utils"))]
impl GcWalk {
    pub(in super::super) const PENDING_LIMIT: usize = 8;

    pub(in super::super) fn new(root: &Root) -> Self {
        Self {
            stack: vec![root.node],
            pending: Vec::new(),
            keys: Vec::new(),
            key_bytes: 0,
        }
    }

    pub(in super::super) fn is_complete(&self) -> bool {
        self.stack.is_empty() && self.pending.is_empty()
    }

    pub(in super::super) fn defer_leaves(
        &mut self,
        leaves: impl DoubleEndedIterator<Item = Leaf> + ExactSizeIterator,
    ) -> Result<()> {
        if self.pending.len().saturating_add(leaves.len()) > Self::PENDING_LIMIT {
            return Err(capacity("GC pending leaf limit exceeded"));
        }
        self.pending.extend(leaves.rev());
        Ok(())
    }

    pub(in super::super) fn owned_bytes(&self) -> usize {
        self.stack.capacity() * size_of::<Node>()
            + self.pending.capacity() * size_of::<Leaf>()
            + self
                .pending
                .iter()
                .map(|leaf| leaf.first.capacity() + leaf.last.capacity())
                .sum::<usize>()
            + self.keys.capacity() * size_of::<CachedKey>()
            + self.key_bytes
    }

    pub(in super::super) async fn step(
        &mut self,
        directory: &Directory,
        invoice: &mut (dyn ReadInvoice + Send),
    ) -> Result<(Option<Leaf>, Vec<String>)> {
        if !self.pending.is_empty() {
            invoice.charge_directory_probe(0)?;
            return Ok((self.pending.pop(), Vec::new()));
        }
        let node = self
            .stack
            .pop()
            .ok_or_else(|| invariant_violation("GC walk is complete"))?;
        let mut budget = ReadBudget::tracking_with_retained(invoice);
        budget.cached_keys = std::mem::take(&mut self.keys);
        budget.cached_key_bytes = self.key_bytes;
        // A full cache must keep admitting the next subtree's fences.
        if node.depth > 0 && budget.cached_keys.len() > KEY_CACHE_ENTRIES - 2 * FANOUT {
            budget.cached_keys.clear();
            budget.cached_key_bytes = 0;
        }
        let leaf = if node.depth > 0 {
            let children = directory
                .read_page(node, b"", None, None, &mut budget)
                .await?;
            if self.stack.len() + children.len() > DEPTH * FANOUT {
                return Err(capacity(
                    "GC directory frontier exceeds its depth/fanout bound",
                ));
            }
            self.stack.extend(children.into_iter().rev());
            None
        } else {
            Some(Leaf {
                first: directory.read_key(node.first, &mut budget).await?.to_vec(),
                last: directory.read_key(node.last, &mut budget).await?.to_vec(),
                rows: node.rows,
                bytes: node.bytes,
                digest: node.digest,
            })
        };
        let paths = budget.take_authenticated_paths();
        self.keys = std::mem::take(&mut budget.cached_keys);
        self.key_bytes = budget.cached_key_bytes;
        Ok((leaf, paths))
    }
}

impl Directory {
    #[cfg(any(test, feature = "test-utils"))]
    pub(in super::super) fn mark_cursor(&self, root: &Root, after: &[u8]) -> Result<Cursor> {
        self.decode_root(&root.encode())?;
        if after.len() > MAX_BLOCK_BYTES {
            return Err(validation_failed("mark cursor exceeds directory key limit"));
        }
        Ok(Cursor {
            root: root.clone(),
            lower: Vec::new(),
            upper: None,
            after: after.to_vec(),
        })
    }
    pub fn new(storage: impl Into<RootStorage>, scope: &StateScope) -> Result<Self> {
        Self::with_role(
            storage,
            scope,
            b"arco.directory.scope.v1\0",
            format!("control/directory/v1/domains/{}", scope.domain()),
        )
    }

    /// Constructs the retained-reference directory role. Its roots and pages
    /// are intentionally incompatible with ordinary physical directories.
    pub(in super::super) fn retained_reference(
        storage: impl Into<RootStorage>,
        scope: &StateScope,
    ) -> Result<Self> {
        Self::with_role(
            storage,
            scope,
            b"arco.directory.role.retained-reference.v1\0",
            format!(
                "control/v1/domains/{}/retained/v1/directory",
                scope.domain()
            ),
        )
    }

    fn with_role(
        storage: impl Into<RootStorage>,
        scope: &StateScope,
        role: &[u8],
        prefix: String,
    ) -> Result<Self> {
        scope.validate()?;
        let storage = storage.into();
        if storage.tenant_id() != scope.tenant_id() || storage.scope().root() != scope.root() {
            return Err(validation_failed(
                "directory storage and state scopes differ",
            ));
        }
        let mut hash = Sha256::new();
        hash.update(role);
        let mut parts: Vec<&str> = vec![scope.tenant_id()];
        match scope.root() {
            AuthorityRoot::Workspace { workspace_id } => parts.push(workspace_id.as_str()),
            AuthorityRoot::Metastore { metastore_id } => {
                parts.push("root=metastore");
                parts.push(metastore_id.as_str());
            }
            AuthorityRoot::TenantIdentity => parts.push("root=identity"),
            _ => {
                return Err(validation_failed(
                    "unsupported authority root for directory scope",
                ));
            }
        }
        parts.push(scope.domain());
        for part in &parts {
            hash.update((part.len() as u64).to_le_bytes());
            hash.update(part.as_bytes());
        }
        #[cfg(any(test, feature = "test-utils"))]
        super::record_sha256_work(
            role.len() + parts.iter().map(|part| 8 + part.len()).sum::<usize>(),
        );
        ScopedStorage::validate_path(&prefix)?;
        Ok(Self {
            storage: ScopedAuthorityStore::new(storage),
            scope: hash.finalize().into(),
            prefix,
            publication_salt: None,
        })
    }

    #[cfg(test)]
    fn with_publication_salt(mut self, salt: [u8; PUBLICATION_SALT_BYTES]) -> Self {
        self.publication_salt = Some(Box::new(salt));
        self
    }

    pub(in super::super) fn for_publication(mut self, candidate_id: &str) -> Result<Self> {
        if !super::integrity::valid_immutable_id(candidate_id) {
            return Err(validation_failed("invalid directory publication identity"));
        }
        let mut hash = Sha256::new();
        hash.update(b"arco.directory.publication.v2\0");
        hash.update(self.scope);
        hash.update((candidate_id.len() as u64).to_le_bytes());
        hash.update(candidate_id.as_bytes());
        self.publication_salt = Some(Box::new(hash.finalize().into()));
        Ok(self)
    }
    pub fn builder(&self) -> Builder {
        Builder {
            directory: self.clone(),
            levels: std::array::from_fn(|_| Vec::new()),
            last: None,
            failed: false,
        }
    }
    pub(in super::super) async fn empty_root(&self) -> Result<Root> {
        Ok(Root {
            scope: self.scope,
            node: self.write_page(1, &[]).await?,
        })
    }
    pub(in super::super) async fn empty_root_budgeted(
        &self,
        budget: &mut ReadBudget<'_>,
    ) -> Result<Root> {
        Ok(Root {
            scope: self.scope,
            node: self.write_page_budgeted(1, &[], budget).await?,
        })
    }
    /// Computes the canonical empty reference without persisting a page.
    pub(in super::super) fn empty_root_reference(&self) -> Result<Root> {
        Ok(Root {
            scope: self.scope,
            node: self.page_node(1, &[])?.0,
        })
    }
    pub(in super::super) fn is_empty_root_shape(&self, root: &Root) -> Result<bool> {
        validate_node(&root.node, true)?;
        Ok(root.scope == self.scope && root.node.depth == 1 && root.node.rows == 0)
    }
    /// Structural parsing only; the enclosing authority must authenticate these bytes.
    pub fn decode_root(&self, bytes: &[u8]) -> Result<Root> {
        if bytes.len() != ROOT_BYTES {
            return Err(invariant_violation("invalid directory root length"));
        }
        let mut input = bytes;
        if take::<8>(&mut input)? != *ROOT_MAGIC || take::<32>(&mut input)? != self.scope {
            return Err(invariant_violation(
                "invalid directory root version or scope",
            ));
        }
        let node = decode_node(&mut input)?;
        validate_node(&node, true)?;
        if node.depth == 0 {
            return Err(invariant_violation("directory root cannot be a bare block"));
        }
        Ok(Root {
            scope: self.scope,
            node,
        })
    }
    fn digest(&self, kind: &[u8], bytes: &[u8]) -> [u8; 32] {
        #[cfg(any(test, feature = "test-utils"))]
        super::record_sha256_work(
            b"arco.directory.object.v1\0".len() + 8 + kind.len() + 32 + 8 + bytes.len(),
        );
        let mut hash = Sha256::new();
        hash.update(b"arco.directory.object.v1\0");
        hash.update((kind.len() as u64).to_le_bytes());
        hash.update(kind);
        hash.update(self.scope);
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
        hash.finalize().into()
    }
    fn path(&self, kind: &str, digest: &[u8; 32]) -> String {
        format!("{}/{kind}/{}", self.prefix, hex::encode(digest))
    }
    fn key_ref(&self, bytes: &[u8]) -> Result<KeyRef> {
        if bytes.len() > MAX_BLOCK_BYTES
            || (self.publication_salt.is_some()
                && bytes.len() > INLINE_KEY_BYTES
                && bytes.len() > MAX_BLOCK_BYTES - KEY_V2_HEADER_BYTES)
        {
            return Err(validation_failed("directory fence exceeds limit"));
        }
        let digest = if self.publication_salt.is_some() && bytes.len() > INLINE_KEY_BYTES {
            self.digest(b"keys", &self.key_payload(bytes))
        } else {
            self.digest(b"keys", bytes)
        };
        let mut inline = [0; INLINE_KEY_BYTES];
        if bytes.len() <= INLINE_KEY_BYTES {
            let prefix = inline
                .get_mut(..bytes.len())
                .ok_or_else(|| invariant_violation("inline fence length"))?;
            prefix.copy_from_slice(bytes);
        }
        Ok(KeyRef {
            inline,
            digest,
            bytes: u32::try_from(bytes.len())
                .map_err(|_| capacity("directory object length overflow"))?
                | if bytes.len() > INLINE_KEY_BYTES && self.publication_salt.is_some() {
                    KEY_V2_FLAG
                } else {
                    0
                },
        })
    }
    fn key_payload(&self, bytes: &[u8]) -> Vec<u8> {
        let Some(salt) = self
            .publication_salt
            .as_ref()
            .filter(|_| bytes.len() > INLINE_KEY_BYTES)
        else {
            return bytes.to_vec();
        };
        let mut encoded = Vec::with_capacity(KEY_V2_HEADER_BYTES + bytes.len());
        encoded.extend_from_slice(KEY_MAGIC_V2);
        encoded.extend_from_slice(salt.as_ref());
        encoded.extend_from_slice(bytes);
        encoded
    }
    async fn write_key(&self, bytes: &[u8]) -> Result<KeyRef> {
        let reference = self.key_ref(bytes)?;
        if bytes.len() > INLINE_KEY_BYTES {
            put_immutable_matching(
                &self.storage,
                &self.path("keys", &reference.digest),
                Bytes::from(self.key_payload(bytes)),
                "directory key collision",
            )
            .await?;
        }
        Ok(reference)
    }
    async fn write_key_budgeted(
        &self,
        bytes: &[u8],
        budget: &mut ReadBudget<'_>,
    ) -> Result<KeyRef> {
        let reference = self.key_ref(bytes)?;
        if bytes.len() > INLINE_KEY_BYTES {
            self.write_object_budgeted(
                "keys",
                &reference.digest,
                Bytes::from(self.key_payload(bytes)),
                budget,
            )
            .await?;
        }
        Ok(reference)
    }

    async fn write_page_budgeted(
        &self,
        depth: u8,
        children: &[Node],
        budget: &mut ReadBudget<'_>,
    ) -> Result<Node> {
        let (node, bytes) = self.page_node(depth, children)?;
        self.write_object_budgeted("pages", &node.digest, bytes.into(), budget)
            .await?;
        Ok(node)
    }

    async fn write_object_budgeted(
        &self,
        kind: &str,
        digest: &[u8; 32],
        bytes: Bytes,
        budget: &mut ReadBudget<'_>,
    ) -> Result<()> {
        let path = self.path(kind, digest);
        let Some(workspace) = budget.workspace.as_deref_mut() else {
            return put_immutable_matching(
                &self.storage,
                &path,
                bytes,
                if kind == "keys" {
                    "directory key collision"
                } else {
                    "directory page collision"
                },
            )
            .await;
        };
        workspace.reserve_bytes(bytes.len())?;
        workspace.charge_operations(1)?;
        let result = self
            .storage
            .put(
                &path,
                bytes.clone(),
                arco_core::AuthorityWritePrecondition::DoesNotExist,
            )
            .await?;
        workspace
            .charge_write(&result)
            .map_err(|_| CatalogError::AmbiguousAuthorityOutcome {
                message: "directory PUT response exceeded workspace admission".into(),
            })?;
        match result {
            arco_core::WriteResult::Success { .. } => Ok(()),
            arco_core::WriteResult::PreconditionFailed { .. } => {
                let probe = bytes
                    .len()
                    .checked_add(1)
                    .ok_or_else(|| capacity("directory collision probe overflow"))?;
                workspace.reserve_bytes(probe)?;
                workspace.charge_operations(1)?;
                if self.storage.get_range(&path, 0..probe as u64).await? != bytes {
                    return Err(invariant_violation("directory immutable collision"));
                }
                Ok(())
            }
        }
    }

    async fn read_object(
        &self,
        kind: &str,
        digest: &[u8; 32],
        length: usize,
        budget: &mut ReadBudget<'_>,
    ) -> Result<Bytes> {
        let probe = length
            .checked_add(1)
            .ok_or_else(|| capacity("directory probe overflow"))?;
        #[cfg(any(test, feature = "test-utils"))]
        let probe_admission = if matches!(&budget.proof_io, ProofIo::Capture(_)) {
            probe
                .checked_add(length + 256)
                .ok_or_else(|| capacity("directory capture overflow"))?
        } else {
            probe
        };
        #[cfg(not(any(test, feature = "test-utils")))]
        let probe_admission = probe;
        budget.charge(probe_admission)?;
        let path = self.path(kind, digest);
        #[cfg(any(test, feature = "test-utils"))]
        let bytes = if let ProofIo::Replay(objects) = &budget.proof_io {
            objects
                .get(&path)
                .cloned()
                .ok_or_else(|| invariant_violation("captured directory proof is incomplete"))?
        } else {
            self.storage.get_range(&path, 0..probe as u64).await?
        };
        #[cfg(not(any(test, feature = "test-utils")))]
        let bytes = self.storage.get_range(&path, 0..probe as u64).await?;
        if bytes.len() != length || self.digest(kind.as_bytes(), &bytes) != *digest {
            return Err(invariant_violation(
                "directory object length or digest mismatch",
            ));
        }
        if let Some(paths) = &mut budget.authenticated_paths {
            paths.push(path.clone());
        }
        #[cfg(any(test, feature = "test-utils"))]
        if let ProofIo::Capture(objects) = &mut budget.proof_io {
            objects
                .entry(path)
                .or_insert_with(|| Bytes::copy_from_slice(&bytes));
        }
        Ok(bytes)
    }
    async fn read_key(&self, key: KeyRef, budget: &mut ReadBudget<'_>) -> Result<Bytes> {
        validate_key_shape(key)?;
        if key_raw_bytes(key) <= INLINE_KEY_BYTES {
            #[cfg(any(test, feature = "test-utils"))]
            if !matches!(&budget.proof_io, ProofIo::None) {
                budget.charge(0)?;
            }
            let bytes = key
                .inline
                .get(..key_raw_bytes(key))
                .ok_or_else(|| invariant_violation("inline fence length"))?;
            if key.digest != self.digest(b"keys", bytes) {
                return Err(invariant_violation(
                    "inline directory fence digest mismatch",
                ));
            }
            return Ok(Bytes::copy_from_slice(bytes));
        }
        let slot = budget
            .cached_keys
            .partition_point(|cached| (cached.scope, cached.digest) < (self.scope, key.digest));
        if let Some(cached) = budget
            .cached_keys
            .get(slot)
            .filter(|cached| cached.scope == self.scope && cached.digest == key.digest)
        {
            if cached.bytes.len() != key_object_bytes(key) {
                return Err(invariant_violation(
                    "cached directory fence length mismatch",
                ));
            }
            return Ok(Bytes::copy_from_slice(key_contents(key, &cached.bytes)?));
        }
        let bytes = self
            .read_object("keys", &key.digest, key_object_bytes(key), budget)
            .await?;
        key_contents(key, &bytes)?;
        if budget.cached_keys.len() < KEY_CACHE_ENTRIES
            && bytes.len() <= KEY_CACHE_PAYLOAD_BYTES.saturating_sub(budget.cached_key_bytes)
        {
            if budget.cached_keys.capacity() == 0 {
                budget.cached_keys = Vec::with_capacity(KEY_CACHE_ENTRIES);
            }
            // Copy exactly the authenticated payload; do not retain a backend's larger buffer.
            let retained = bytes.to_vec().into_boxed_slice();
            budget.cached_key_bytes += retained.len();
            budget.cached_keys.insert(
                slot,
                CachedKey {
                    scope: self.scope,
                    digest: key.digest,
                    bytes: retained,
                },
            );
        }
        Ok(if key.bytes & KEY_V2_FLAG == 0 {
            bytes
        } else {
            bytes.slice(KEY_V2_HEADER_BYTES..)
        })
    }
    fn page_node(&self, depth: u8, children: &[Node]) -> Result<(Node, Vec<u8>)> {
        if depth == 0 || depth as usize > DEPTH || children.len() > FANOUT {
            return Err(capacity("directory page fanout or depth exceeded"));
        }
        if page_probe_bytes(children)?
            + self
                .publication_salt
                .as_ref()
                .map_or(0, |_| PUBLICATION_SALT_BYTES)
            > PAGE_PROBE_LIMIT
        {
            return Err(capacity("directory page fence-probe budget exceeded"));
        }
        let mut rows = 0_u64;
        for child in children {
            validate_node(child, false)?;
            if child.depth + 1 != depth {
                return Err(invariant_violation("unbalanced directory writer"));
            }
            rows = rows
                .checked_add(child.rows)
                .ok_or_else(|| capacity("directory row count overflow"))?;
        }
        let mut bytes = Vec::with_capacity(
            HEADER_BYTES
                + children.len() * NODE_BYTES
                + self
                    .publication_salt
                    .as_ref()
                    .map_or(0, |_| PUBLICATION_SALT_BYTES),
        );
        bytes.extend_from_slice(if self.publication_salt.is_some() {
            PAGE_MAGIC_V2
        } else {
            PAGE_MAGIC
        });
        bytes.extend_from_slice(&self.scope);
        bytes.push(depth);
        let count =
            u16::try_from(children.len()).map_err(|_| capacity("directory count overflow"))?;
        bytes.extend_from_slice(&count.to_le_bytes());
        if let Some(salt) = &self.publication_salt {
            bytes.extend_from_slice(salt.as_ref());
        }
        for node in children {
            encode_node(&mut bytes, node);
        }
        if bytes.len() > PAGE_LIMIT {
            return Err(capacity("directory page bytes exceeded"));
        }
        let digest = self.digest(b"pages", &bytes);
        let empty = KeyRef {
            digest: self.digest(b"keys", b""),
            bytes: 0,
            inline: [0; INLINE_KEY_BYTES],
        };
        let node = Node {
            depth,
            bytes: u32::try_from(bytes.len())
                .map_err(|_| capacity("directory object length overflow"))?,
            rows,
            first: children.first().map_or(empty, |n| n.first),
            last: children.last().map_or(empty, |n| n.last),
            digest,
        };
        Ok((node, bytes))
    }
    #[cfg(test)]
    pub(in super::super) async fn test_root_at_depth(
        &self,
        root: &Root,
        depth: u8,
    ) -> Result<Root> {
        self.decode_root(&root.encode())?;
        let mut node = root.node;
        while node.depth < depth {
            node = self.write_page(node.depth + 1, &[node]).await?;
        }
        Ok(Root {
            scope: self.scope,
            node,
        })
    }

    async fn write_page(&self, depth: u8, children: &[Node]) -> Result<Node> {
        let (node, bytes) = self.page_node(depth, children)?;
        put_immutable_matching(
            &self.storage,
            &self.path("pages", &node.digest),
            bytes.into(),
            "directory page collision",
        )
        .await?;
        Ok(node)
    }
    async fn read_page(
        &self,
        node: Node,
        lower: &[u8],
        upper: Option<&[u8]>,
        after: Option<&[u8]>,
        budget: &mut ReadBudget<'_>,
    ) -> Result<Vec<Node>> {
        validate_node(&node, true)?;
        let bytes = self
            .read_object("pages", &node.digest, node.bytes as usize, budget)
            .await?;
        let children = decode_page(&bytes, &self.scope, node.depth)?;
        if page_probe_bytes(&children)?
            + if bytes.starts_with(PAGE_MAGIC_V2) {
                PUBLICATION_SALT_BYTES
            } else {
                0
            }
            > PAGE_PROBE_LIMIT
        {
            return Err(invariant_violation(
                "directory page fence-probe budget exceeded",
            ));
        }
        let mut sum = 0_u64;
        let mut previous: Option<Bytes> = None;
        let mut selected = Vec::new();
        for child in &children {
            validate_node(child, false)?;
            if child.depth + 1 != node.depth {
                return Err(invariant_violation("unbalanced directory page"));
            }
            sum = sum
                .checked_add(child.rows)
                .ok_or_else(|| invariant_violation("directory count overflow"))?;
            let first = self.read_key(child.first, budget).await?;
            let last = self.read_key(child.last, budget).await?;
            if first > last || previous.as_ref().is_some_and(|p| p >= &first) {
                return Err(invariant_violation(
                    "overlapping or reversed directory boundaries",
                ));
            }
            if last.as_ref() >= lower
                && upper.is_none_or(|u| first.as_ref() < u)
                && after.is_none_or(|a| last.as_ref() > a)
            {
                selected.push(*child);
            }
            previous = Some(last);
        }
        let empty = KeyRef {
            digest: self.digest(b"keys", b""),
            bytes: 0,
            inline: [0; INLINE_KEY_BYTES],
        };
        if sum != node.rows
            || children.first().map_or(empty, |n| n.first) != node.first
            || children.last().map_or(empty, |n| n.last) != node.last
            || (children.is_empty() && node.depth != 1)
        {
            return Err(invariant_violation("directory child summary mismatch"));
        }
        Ok(selected)
    }
    /// Finds a candidate block, without claiming the key exists inside that block.
    /// Returns the predecessor leaf by its first endpoint, or the first leaf.
    pub(in super::super) async fn floor_leaf(
        &self,
        root: &Root,
        key: &[u8],
        budget: &mut ReadBudget<'_>,
    ) -> Result<Option<Leaf>> {
        if root.scope != self.scope || key.len() > MAX_BLOCK_BYTES || root.node.depth == 0 {
            return Err(validation_failed("invalid directory floor scope or key"));
        }
        validate_node(&root.node, true)?;
        let mut node = root.node;
        while node.depth > 0 {
            let children = self.read_page(node, b"", None, None, budget).await?;
            let Some(first) = children.first() else {
                return Ok(None);
            };
            let mut selected = *first;
            for child in children {
                if self.read_key(child.first, budget).await?.as_ref() > key {
                    break;
                }
                selected = child;
            }
            node = selected;
        }
        Ok(Some(Leaf {
            first: self.read_key(node.first, budget).await?.to_vec(),
            last: self.read_key(node.last, budget).await?.to_vec(),
            rows: node.rows,
            bytes: node.bytes,
            digest: node.digest,
        }))
    }

    pub async fn lookup(
        &self,
        root: &Root,
        key: &[u8],
        budget: &mut ReadBudget<'_>,
    ) -> Result<Option<Leaf>> {
        if root.scope != self.scope || root.node.depth == 0 || key.len() > MAX_BLOCK_BYTES {
            return Err(validation_failed("invalid directory point scope or key"));
        }
        let mut node = root.node;
        while node.depth > 0 {
            let children = self.read_page(node, key, None, None, budget).await?;
            let Some(child) = children.first() else {
                return Ok(None);
            };
            node = *child;
        }
        let first = self.read_key(node.first, budget).await?;
        if first.as_ref() > key {
            return Ok(None);
        }
        let last = self.read_key(node.last, budget).await?;
        Ok(Some(Leaf {
            first: first.to_vec(),
            last: last.to_vec(),
            rows: node.rows,
            bytes: node.bytes,
            digest: node.digest,
        }))
    }
    /// Returns intersecting blocks in order. Resume only with this exact root/query.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan(
        &self,
        root: &Root,
        lower: &[u8],
        upper: Option<&[u8]>,
        limit: usize,
        cursor: Option<&Cursor>,
        budget: &mut ReadBudget<'_>,
    ) -> Result<Page> {
        if root.scope != self.scope {
            return Err(invariant_violation("directory root scope mismatch"));
        }
        validate_node(&root.node, true)?;
        if root.node.depth == 0
            || limit == 0
            || limit > FANOUT
            || lower.len() > MAX_BLOCK_BYTES
            || upper.is_some_and(|u| u.len() > MAX_BLOCK_BYTES || u < lower)
        {
            return Err(validation_failed("invalid directory scan bounds"));
        }
        if let Some(c) = cursor {
            if c.root != *root || c.lower != lower || c.upper.as_deref() != upper {
                return Err(validation_failed("directory cursor root or query mismatch"));
            }
        }
        if upper == Some(lower) {
            return Ok(Page {
                leaves: Vec::new(),
                next: None,
            });
        }
        let after = cursor.map(|c| c.after.as_slice());
        let mut stack = vec![root.node];
        let mut leaves: Vec<Leaf> = Vec::new();
        while let Some(node) = stack.pop() {
            if node.depth > 0 {
                let children = self.read_page(node, lower, upper, after, budget).await?;
                stack.extend(children.into_iter().rev());
            } else {
                if leaves.len() == limit {
                    let last = leaves
                        .last()
                        .ok_or_else(|| invariant_violation("empty directory page continuation"))?;
                    return Ok(Page {
                        next: Some(Cursor {
                            root: root.clone(),
                            lower: lower.to_vec(),
                            upper: upper.map(<[u8]>::to_vec),
                            after: last.last.clone(),
                        }),
                        leaves,
                    });
                }
                let first = self.read_key(node.first, budget).await?.to_vec();
                let last = self.read_key(node.last, budget).await?.to_vec();
                leaves.push(Leaf {
                    first,
                    last,
                    rows: node.rows,
                    bytes: node.bytes,
                    digest: node.digest,
                });
            }
        }
        Ok(Page { leaves, next: None })
    }
}

impl Builder {
    pub async fn push(&mut self, leaf: Leaf) -> Result<()> {
        #[cfg(any(test, feature = "test-utils"))]
        super::cost::bounded_work(super::cost::BoundedWork {
            streaming_builder_inputs: 1,
            ..Default::default()
        });
        if self.failed {
            return Err(invariant_violation("directory builder requires restart"));
        }
        if leaf.first.len() > MAX_BLOCK_BYTES
            || leaf.last.len() > MAX_BLOCK_BYTES
            || leaf.first > leaf.last
            || leaf.rows == 0
            || leaf.bytes == 0
            || leaf.bytes as usize > MAX_SEGMENT_BYTES
            || (leaf.rows > 1 && leaf.bytes as usize > MAX_BLOCK_BYTES)
            || self.last.as_ref().is_some_and(|k| k >= &leaf.first)
        {
            return Err(validation_failed("invalid or unordered directory leaf"));
        }
        self.failed = true;
        let node = Node {
            depth: 0,
            first: self.directory.write_key(&leaf.first).await?,
            last: self.directory.write_key(&leaf.last).await?,
            rows: leaf.rows,
            bytes: leaf.bytes,
            digest: leaf.digest,
        };
        self.push_node(node).await?;
        self.last = Some(leaf.last);
        self.failed = false;
        Ok(())
    }
    async fn push_node(&mut self, mut node: Node) -> Result<()> {
        loop {
            let level = node.depth as usize;

            let added_probe_bytes =
                page_probe_bytes(std::slice::from_ref(&node))? - HEADER_BYTES - 1;
            let page = self
                .levels
                .get_mut(level)
                .ok_or_else(|| capacity("directory depth exceeded"))?;
            if !page.is_empty()
                && (page.len() == FANOUT
                    || page_probe_bytes(page)? + added_probe_bytes > PAGE_PROBE_LIMIT)
            {
                let parent = self.directory.write_page(node.depth + 1, page).await?;
                page.clear();
                page.push(node);
                node = parent;
            } else {
                page.push(node);
                return Ok(());
            }
        }
    }
    #[allow(
        clippy::indexing_slicing,
        reason = "level comes from position over the fixed nonempty level array"
    )]
    pub async fn finish(mut self) -> Result<Root> {
        if self.failed {
            return Err(invariant_violation("directory builder requires restart"));
        }
        loop {
            let Some(level) = self.levels.iter().position(|v| !v.is_empty()) else {
                let node = self.directory.write_page(1, &[]).await?;
                return Ok(Root {
                    scope: self.directory.scope,
                    node,
                });
            };
            if level > 0
                && self.levels[level].len() == 1
                && self.levels[level + 1..].iter().all(Vec::is_empty)
            {
                return Ok(Root {
                    scope: self.directory.scope,
                    node: self.levels[level][0],
                });
            }
            let node = self
                .directory
                .write_page(
                    u8::try_from(level + 1).map_err(|_| capacity("directory depth overflow"))?,
                    &self.levels[level],
                )
                .await?;
            self.levels[level].clear();
            if node.depth as usize == DEPTH {
                return Ok(Root {
                    scope: self.directory.scope,
                    node,
                });
            }
            self.push_node(node).await?;
        }
    }
}

fn capacity(message: &str) -> CatalogError {
    CatalogError::MaintenanceBackpressure {
        message: message.into(),
    }
}
fn validate_key_shape(key: KeyRef) -> Result<()> {
    let length = key_raw_bytes(key);
    if key_object_bytes(key) > MAX_BLOCK_BYTES
        || (key.bytes & KEY_V2_FLAG != 0 && length <= INLINE_KEY_BYTES)
    {
        return Err(invariant_violation("oversized directory fence"));
    }
    let padding = if length <= INLINE_KEY_BYTES {
        key.inline
            .get(length..)
            .ok_or_else(|| invariant_violation("inline fence length"))?
    } else {
        &key.inline
    };
    if padding.iter().any(|byte| *byte != 0) {
        return Err(invariant_violation("noncanonical directory fence padding"));
    }
    Ok(())
}
fn validate_node(node: &Node, empty: bool) -> Result<()> {
    validate_key_shape(node.first)?;
    validate_key_shape(node.last)?;
    if node.depth as usize > DEPTH
        || (node.rows == 0 && !(empty && node.depth == 1))
        || key_raw_bytes(node.first) > MAX_BLOCK_BYTES
        || key_raw_bytes(node.last) > MAX_BLOCK_BYTES
        || node.bytes == 0
        || (node.depth == 0
            && (node.bytes as usize > MAX_SEGMENT_BYTES
                || (node.rows > 1 && node.bytes as usize > MAX_BLOCK_BYTES)))
        || (node.depth > 0 && !(HEADER_BYTES..=PAGE_LIMIT).contains(&(node.bytes as usize)))
    {
        return Err(invariant_violation("invalid directory reference bounds"));
    }
    Ok(())
}
fn page_probe_bytes(children: &[Node]) -> Result<usize> {
    if children.len() > FANOUT {
        return Err(capacity("directory fanout exceeded"));
    }
    let mut total = HEADER_BYTES + 1 + children.len() * NODE_BYTES;
    for child in children {
        validate_node(child, false)?;
        for key in [child.first, child.last] {
            if key_raw_bytes(key) > INLINE_KEY_BYTES {
                total += key_object_bytes(key) + 1;
            }
        }
    }
    Ok(total)
}
fn key_raw_bytes(key: KeyRef) -> usize {
    (key.bytes & !KEY_V2_FLAG) as usize
}
fn key_object_bytes(key: KeyRef) -> usize {
    key_raw_bytes(key)
        + if key.bytes & KEY_V2_FLAG != 0 {
            KEY_V2_HEADER_BYTES
        } else {
            0
        }
}
fn key_contents(key: KeyRef, bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() != key_object_bytes(key) {
        return Err(invariant_violation("directory fence length mismatch"));
    }
    if key.bytes & KEY_V2_FLAG == 0 {
        return Ok(bytes);
    }
    if bytes.get(..8) != Some(KEY_MAGIC_V2) {
        return Err(invariant_violation("invalid directory fence version"));
    }
    bytes
        .get(KEY_V2_HEADER_BYTES..)
        .ok_or_else(|| invariant_violation("truncated directory fence version"))
}
fn encode_node(out: &mut Vec<u8>, node: &Node) {
    #[cfg(any(test, feature = "test-utils"))]
    super::cost::bounded_work(super::cost::BoundedWork {
        directory_references: 1,
        ..Default::default()
    });
    out.push(node.depth);
    out.extend_from_slice(&node.bytes.to_le_bytes());
    out.extend_from_slice(&node.rows.to_le_bytes());
    for key in [node.first, node.last] {
        out.extend_from_slice(&key.digest);
        out.extend_from_slice(&key.bytes.to_le_bytes());
        out.extend_from_slice(&key.inline);
    }
    out.extend_from_slice(&node.digest);
}
fn take<const N: usize>(input: &mut &[u8]) -> Result<[u8; N]> {
    let (bytes, rest) = input
        .split_at_checked(N)
        .ok_or_else(|| invariant_violation("truncated directory reference"))?;
    let mut value = [0; N];
    value.copy_from_slice(bytes);
    *input = rest;
    Ok(value)
}
fn decode_node(input: &mut &[u8]) -> Result<Node> {
    #[cfg(any(test, feature = "test-utils"))]
    super::cost::bounded_work(super::cost::BoundedWork {
        directory_references: 1,
        ..Default::default()
    });
    Ok(Node {
        depth: take::<1>(input)?[0],
        bytes: u32::from_le_bytes(take(input)?),
        rows: u64::from_le_bytes(take(input)?),
        first: KeyRef {
            digest: take(input)?,
            bytes: u32::from_le_bytes(take(input)?),
            inline: take(input)?,
        },
        last: KeyRef {
            digest: take(input)?,
            bytes: u32::from_le_bytes(take(input)?),
            inline: take(input)?,
        },
        digest: take(input)?,
    })
}
fn decode_page(bytes: &[u8], scope: &[u8; 32], depth: u8) -> Result<Vec<Node>> {
    if !(HEADER_BYTES..=PAGE_LIMIT).contains(&bytes.len()) {
        return Err(invariant_violation("invalid directory page length"));
    }
    let mut input = bytes;
    let magic = take::<8>(&mut input)?;
    if (magic != *PAGE_MAGIC && magic != *PAGE_MAGIC_V2)
        || take::<32>(&mut input)? != *scope
        || take::<1>(&mut input)?[0] != depth
        || depth == 0
        || depth as usize > DEPTH
    {
        return Err(invariant_violation("invalid directory page header"));
    }
    let count = usize::from(u16::from_le_bytes(take(&mut input)?));
    if magic == *PAGE_MAGIC_V2 {
        let _salt = take::<PUBLICATION_SALT_BYTES>(&mut input)?;
    }
    if count > FANOUT
        || bytes.len()
            != HEADER_BYTES
                + count * NODE_BYTES
                + if magic == *PAGE_MAGIC_V2 {
                    PUBLICATION_SALT_BYTES
                } else {
                    0
                }
    {
        return Err(invariant_violation(
            "invalid directory page count or length",
        ));
    }
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(decode_node(&mut input)?);
    }
    Ok(nodes)
}

#[cfg(test)]
mod tests;

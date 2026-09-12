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
use arco_core::{ScopedAuthorityStore, ScopedStorage};
use bytes::Bytes;
use sha2::{Digest, Sha256};

const PAGE_MAGIC: &[u8; 8] = b"ARCODIR1";
const ROOT_MAGIC: &[u8; 8] = b"ARCOROT1";
const FANOUT: usize = 128;
const DEPTH: usize = 8;
const PAGE_LIMIT: usize = 64 * 1024;
const HEADER_BYTES: usize = 43;
const INLINE_KEY_BYTES: usize = 64;
const NODE_BYTES: usize = 245;
const ROOT_BYTES: usize = 40 + NODE_BYTES;
const OBJECT_LIMIT: usize = 4096;
const PAGE_PROBE_LIMIT: usize = 4 * 1024 * 1024;
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
#[derive(Debug)]
pub struct Page {
    pub leaves: Vec<Leaf>,
    pub next: Option<Cursor>,
}

/// Cumulative conservative probe accounting, including failed reads and retries.
pub struct ReadBudget {
    object_limit: usize,
    byte_limit: usize,
    pub objects: usize,
    pub bytes: usize,
}
impl Default for ReadBudget {
    fn default() -> Self {
        Self {
            object_limit: OBJECT_LIMIT,
            byte_limit: MAX_SEGMENT_BYTES,
            objects: 0,
            bytes: 0,
        }
    }
}
impl ReadBudget {
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
        })
    }
    fn charge(&mut self, bytes: usize) -> Result<()> {
        if self.objects >= self.object_limit || bytes > self.byte_limit.saturating_sub(self.bytes) {
            return Err(capacity("directory read budget exhausted"));
        }
        self.objects += 1;
        self.bytes += bytes;
        Ok(())
    }
}

impl Directory {
    pub fn new(storage: ScopedStorage, scope: &StateScope) -> Result<Self> {
        scope.validate()?;
        if storage.tenant_id() != scope.tenant_id()
            || storage.scope().workspace_id() != Some(scope.workspace_id())
        {
            return Err(validation_failed(
                "directory storage and state scopes differ",
            ));
        }
        let mut hash = Sha256::new();
        hash.update(b"arco.directory.scope.v1\0");
        for part in [scope.tenant_id(), scope.workspace_id(), scope.domain()] {
            hash.update((part.len() as u64).to_le_bytes());
            hash.update(part.as_bytes());
        }
        let prefix = format!("control/directory/v1/domains/{}", scope.domain());
        ScopedStorage::validate_path(&prefix)?;
        Ok(Self {
            storage: ScopedAuthorityStore::new(storage),
            scope: hash.finalize().into(),
            prefix,
        })
    }
    pub fn builder(&self) -> Builder {
        Builder {
            directory: self.clone(),
            levels: std::array::from_fn(|_| Vec::new()),
            last: None,
            failed: false,
        }
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
    async fn write_key(&self, bytes: &[u8]) -> Result<KeyRef> {
        let digest = self.digest(b"keys", bytes);
        if bytes.len() > MAX_BLOCK_BYTES {
            return Err(validation_failed("directory fence exceeds limit"));
        }
        let mut inline = [0; INLINE_KEY_BYTES];
        if bytes.len() <= INLINE_KEY_BYTES {
            let prefix = inline
                .get_mut(..bytes.len())
                .ok_or_else(|| invariant_violation("inline fence length"))?;
            prefix.copy_from_slice(bytes);
        } else {
            put_immutable_matching(
                &self.storage,
                &self.path("keys", &digest),
                Bytes::copy_from_slice(bytes),
                "directory key collision",
            )
            .await?;
        }
        Ok(KeyRef {
            inline,
            digest,
            bytes: u32::try_from(bytes.len())
                .map_err(|_| capacity("directory object length overflow"))?,
        })
    }
    async fn read_object(
        &self,
        kind: &str,
        digest: &[u8; 32],
        length: usize,
        budget: &mut ReadBudget,
    ) -> Result<Bytes> {
        let probe = length
            .checked_add(1)
            .ok_or_else(|| capacity("directory probe overflow"))?;
        budget.charge(probe)?;
        let bytes = self
            .storage
            .get_range(&self.path(kind, digest), 0..probe as u64)
            .await?;
        if bytes.len() != length || self.digest(kind.as_bytes(), &bytes) != *digest {
            return Err(invariant_violation(
                "directory object length or digest mismatch",
            ));
        }
        Ok(bytes)
    }
    async fn read_key(&self, key: KeyRef, budget: &mut ReadBudget) -> Result<Bytes> {
        validate_key_shape(key)?;
        if key.bytes as usize <= INLINE_KEY_BYTES {
            let bytes = key
                .inline
                .get(..key.bytes as usize)
                .ok_or_else(|| invariant_violation("inline fence length"))?;
            if key.digest != self.digest(b"keys", bytes) {
                return Err(invariant_violation(
                    "inline directory fence digest mismatch",
                ));
            }
            return Ok(Bytes::copy_from_slice(bytes));
        }
        self.read_object("keys", &key.digest, key.bytes as usize, budget)
            .await
    }
    async fn write_page(&self, depth: u8, children: &[Node]) -> Result<Node> {
        if depth == 0 || depth as usize > DEPTH || children.len() > FANOUT {
            return Err(capacity("directory page fanout or depth exceeded"));
        }
        if page_probe_bytes(children)? > PAGE_PROBE_LIMIT {
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
        let mut bytes = Vec::with_capacity(HEADER_BYTES + children.len() * NODE_BYTES);
        bytes.extend_from_slice(PAGE_MAGIC);
        bytes.extend_from_slice(&self.scope);
        bytes.push(depth);
        let count =
            u16::try_from(children.len()).map_err(|_| capacity("directory count overflow"))?;
        bytes.extend_from_slice(&count.to_le_bytes());
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
        put_immutable_matching(
            &self.storage,
            &self.path("pages", &digest),
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
        budget: &mut ReadBudget,
    ) -> Result<Vec<Node>> {
        validate_node(&node, true)?;
        let bytes = self
            .read_object("pages", &node.digest, node.bytes as usize, budget)
            .await?;
        let children = decode_page(&bytes, &self.scope, node.depth)?;
        if page_probe_bytes(&children)? > PAGE_PROBE_LIMIT {
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
    pub async fn lookup(
        &self,
        root: &Root,
        key: &[u8],
        budget: &mut ReadBudget,
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
        budget: &mut ReadBudget,
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
    if key.bytes as usize > MAX_BLOCK_BYTES {
        return Err(invariant_violation("oversized directory fence"));
    }
    let padding = if key.bytes as usize <= INLINE_KEY_BYTES {
        key.inline
            .get(key.bytes as usize..)
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
        || node.first.bytes as usize > MAX_BLOCK_BYTES
        || node.last.bytes as usize > MAX_BLOCK_BYTES
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
            if key.bytes as usize > INLINE_KEY_BYTES {
                total += key.bytes as usize + 1;
            }
        }
    }
    Ok(total)
}
fn encode_node(out: &mut Vec<u8>, node: &Node) {
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
    if take::<8>(&mut input)? != *PAGE_MAGIC
        || take::<32>(&mut input)? != *scope
        || take::<1>(&mut input)?[0] != depth
        || depth == 0
        || depth as usize > DEPTH
    {
        return Err(invariant_violation("invalid directory page header"));
    }
    let count = usize::from(u16::from_le_bytes(take(&mut input)?));
    if count > FANOUT || bytes.len() != HEADER_BYTES + count * NODE_BYTES {
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

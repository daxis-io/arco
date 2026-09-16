//! Native restore traversal of a caller-authenticated directory root.
use super::super::physical::restore_io::{
    AccountedBytes, DIRECTORY_FIXED_ALLOCATION_BYTES, RestorePhysicalIo, RestorePhysicalRoute,
    WorkingValue, decode_with_reservation, directory_scope_reservation,
};
use super::{
    DEPTH, Directory, FANOUT, INLINE_KEY_BYTES, KeyRef, Leaf, MAX_BLOCK_BYTES, Node,
    PAGE_PROBE_LIMIT, Result, Root, decode_page, invariant_violation, page_probe_bytes,
    validate_key_shape, validate_node,
};

pub(in super::super) struct Position {
    pub leaf: Leaf,
    pub path: Vec<([u8; 32], u32)>,
}

/// The caller supplies the exact root authenticated by its selected Plan7 role.
/// The result proves directory membership only; descriptor/index/payload checks
/// are still required before these candidate rows can support restore coverage.
pub(in super::super) async fn first_after(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    root: &Root,
    after: Option<&[u8]>,
) -> Result<WorkingValue<Option<Position>>> {
    let result = first_after_inner(io, route, root, after, false).await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

/// Inclusive endpoint lookup for authenticating a saved restore cursor.
pub(in super::super) async fn first_at_or_after(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    root: &Root,
    key: &[u8],
) -> Result<WorkingValue<Option<Position>>> {
    let result = first_after_inner(io, route, root, Some(key), true).await;
    if result.is_err() {
        io.stop(route);
    }
    result
}

#[allow(
    clippy::too_many_lines,
    reason = "keep the bounded path walk and each allocation admission together"
)]
async fn first_after_inner(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    root: &Root,
    after: Option<&[u8]>,
    inclusive: bool,
) -> Result<WorkingValue<Option<Position>>> {
    let store = io.store();
    let reservation = directory_scope_reservation(store)?;
    let directory = decode_with_reservation(io, route, Some(reservation), || {
        let directory = Directory::new(store.retention.clone(), &store.scope)?;
        if root.scope != directory.scope
            || root.node.depth == 0
            || after.is_some_and(|a| a.len() > MAX_BLOCK_BYTES)
        {
            return Err(invariant_violation(
                "invalid restore directory root or boundary",
            ));
        }
        validate_node(&root.node, true)?;
        Ok(directory)
    })?;
    let directory = directory.value();
    let mut node = root.node;
    let mut path = [([0; 32], 0_u32); DEPTH];
    let mut depth = 0;
    while node.depth > 0 {
        let bytes = io
            .directory_object(route, false, &node.digest, node.bytes as usize)
            .await?;
        if size_of::<Node>() > 256 {
            return Err(super::capacity("unqualified directory node layout"));
        }
        let reservation = DIRECTORY_FIXED_ALLOCATION_BYTES + FANOUT * size_of::<Node>();
        let children = decode_with_reservation(io, route, Some(reservation), || {
            decode_children(directory, node, bytes.as_slice())
        })?;
        drop(bytes);
        let mut previous: Option<Fence<'_>> = None;
        let mut selected = None;
        // Validate the complete unfiltered vector, including all siblings after
        // the selected branch, before trusting any exclusion or end marker.
        for (index, child) in children.value().iter().enumerate() {
            let first = read_fence(io, route, directory, &child.first).await?;
            let last = read_fence(io, route, directory, &child.last).await?;
            let checked =
                decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
                    if first.as_slice() > last.as_slice()
                        || previous
                            .as_ref()
                            .is_some_and(|p| p.as_slice() >= first.as_slice())
                    {
                        return Err(invariant_violation(
                            "overlapping or reversed directory boundaries",
                        ));
                    }
                    Ok(())
                })?;
            drop(checked);
            if selected.is_none()
                && after.is_none_or(|a| last.as_slice() > a || (inclusive && last.as_slice() == a))
            {
                let index = u32::try_from(index)
                    .map_err(|_| invariant_violation("directory child index overflow"))?;
                selected = Some((index, *child));
            }
            previous = Some(last);
        }
        let Some((index, child)) = selected else {
            return decode_with_reservation(
                io,
                route,
                Some(DIRECTORY_FIXED_ALLOCATION_BYTES),
                || Ok(None),
            );
        };
        *path
            .get_mut(depth)
            .ok_or_else(|| invariant_violation("restore directory path exceeds depth"))? =
            (node.digest, index);
        depth += 1;
        node = child;
    }
    let first = read_fence(io, route, directory, &node.first).await?;
    let last = read_fence(io, route, directory, &node.last).await?;
    let reservation = DIRECTORY_FIXED_ALLOCATION_BYTES
        .checked_add(first.as_slice().len())
        .and_then(|bytes| bytes.checked_add(last.as_slice().len()))
        .and_then(|bytes| bytes.checked_add(depth.checked_mul(size_of::<([u8; 32], u32)>())?))
        .ok_or_else(|| super::capacity("directory position allocation overflow"))?;
    decode_with_reservation(io, route, Some(reservation), || {
        Ok(Some(Position {
            leaf: Leaf {
                first: first.as_slice().to_vec(),
                last: last.as_slice().to_vec(),
                rows: node.rows,
                bytes: node.bytes,
                digest: node.digest,
            },
            path: path
                .get(..depth)
                .ok_or_else(|| invariant_violation("restore directory path exceeds depth"))?
                .to_vec(),
        }))
    })
}

fn decode_children(directory: &Directory, node: Node, bytes: &[u8]) -> Result<Vec<Node>> {
    if bytes.len() != node.bytes as usize || directory.digest(b"pages", bytes) != node.digest {
        return Err(invariant_violation(
            "restore directory page digest or length mismatch",
        ));
    }
    let children = decode_page(bytes, &directory.scope, node.depth)?;
    if page_probe_bytes(&children)? > PAGE_PROBE_LIMIT {
        return Err(invariant_violation(
            "directory page fence-probe budget exceeded",
        ));
    }
    let mut sum = 0_u64;
    for child in &children {
        validate_node(child, false)?;
        if child.depth + 1 != node.depth {
            return Err(invariant_violation("unbalanced directory page"));
        }
        sum = sum
            .checked_add(child.rows)
            .ok_or_else(|| invariant_violation("directory count overflow"))?;
    }
    let empty = directory.key_ref(b"")?;
    if sum != node.rows
        || children.first().map_or(empty, |n| n.first) != node.first
        || children.last().map_or(empty, |n| n.last) != node.last
        || (children.is_empty() && node.depth != 1)
    {
        return Err(invariant_violation("directory child summary mismatch"));
    }
    Ok(children)
}

enum Fence<'key> {
    Inline(&'key [u8]),
    External(AccountedBytes),
}
impl Fence<'_> {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline(bytes) => bytes,
            Self::External(bytes) => bytes.as_slice(),
        }
    }
}

async fn read_fence<'key>(
    io: &mut RestorePhysicalIo<'_>,
    route: &mut RestorePhysicalRoute<'_, '_>,
    directory: &Directory,
    key: &'key KeyRef,
) -> Result<Fence<'key>> {
    // Page validation has already checked padding and length, but no exclusions
    // rely on its inline fence digest until it is checked here too.
    if key.bytes as usize <= INLINE_KEY_BYTES {
        let checked =
            decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
                validate_key_shape(*key)?;
                let bytes = key
                    .inline
                    .get(..key.bytes as usize)
                    .ok_or_else(|| invariant_violation("inline fence length"))?;
                if directory.digest(b"keys", bytes) != key.digest {
                    return Err(invariant_violation(
                        "inline directory fence digest mismatch",
                    ));
                }
                Ok(bytes)
            })?;
        return Ok(Fence::Inline(checked.value()));
    }
    let bytes = io
        .directory_object(route, true, &key.digest, key.bytes as usize)
        .await?;
    let checked =
        decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
            if bytes.as_slice().len() != key.bytes as usize
                || directory.digest(b"keys", bytes.as_slice()) != key.digest
            {
                return Err(invariant_violation(
                    "directory fence length or digest mismatch",
                ));
            }
            Ok(())
        })?;
    drop(checked);
    Ok(Fence::External(bytes))
}

#[cfg(test)]
pub(in super::super) async fn overlapping_root(directory: &Directory) -> Root {
    let key = directory.key_ref(b"key").expect("key");
    let child = Node {
        depth: 0,
        bytes: 100,
        rows: 1,
        first: key,
        last: key,
        digest: [1; 32],
    };
    Root {
        scope: directory.scope,
        node: directory
            .write_page(1, &[child, child])
            .await
            .expect("forged page"),
    }
}

#[cfg(test)]
pub(in super::super) async fn dense_eight_level_path(directory: &Directory) -> Root {
    // A path fixture only: unselected subtrees are represented by summaries,
    // as in the existing directory depth test, not 128^8 executed writes.
    const KEY_BYTES: usize = 16_260;
    let mut selected: Option<Node> = None;
    for depth in 1_u8..=8 {
        let span = 2 * 128_u64.pow(u32::from(depth - 1));
        let mut children = Vec::new();
        for index in 0_u64..128 {
            let mut first = vec![0; KEY_BYTES];
            first[..8].copy_from_slice(&(index * span).to_be_bytes());
            let mut last = vec![0; KEY_BYTES];
            last[..8].copy_from_slice(&((index + 1) * span - 1).to_be_bytes());
            let child = Node {
                depth: depth - 1,
                bytes: selected.map_or(100, |node| node.bytes),
                rows: span,
                first: directory.write_key(&first).await.expect("first fence"),
                last: directory.write_key(&last).await.expect("last fence"),
                digest: if index == 0 {
                    selected.map_or([1; 32], |node| node.digest)
                } else {
                    [2; 32]
                },
            };
            if index == 0 {
                if let Some(previous) = selected {
                    assert_eq!(child, previous);
                }
            }
            children.push(child);
        }
        assert_eq!(
            page_probe_bytes(&children).expect("bounded page probes"),
            4_194_220
        );
        selected = Some(
            directory
                .write_page(depth, &children)
                .await
                .expect("selected path page"),
        );
    }
    Root {
        scope: directory.scope,
        node: selected.expect("eight path pages"),
    }
}

/// Opaque frontier for the charged native final builder.
pub(in super::super) struct NativeBuilder {
    directory: Directory,
    levels: [Vec<Node>; DEPTH],
    last: Vec<u8>,
    has_last: bool,
    failed: bool,
}

impl NativeBuilder {
    pub(in super::super) fn reservation() -> Result<usize> {
        if size_of::<Node>() > 256 {
            return Err(super::capacity("unqualified directory builder node layout"));
        }
        Ok(DEPTH * FANOUT * size_of::<Node>() + MAX_BLOCK_BYTES)
    }

    pub(in super::super) fn new(directory: Directory) -> Self {
        Self {
            directory,
            levels: std::array::from_fn(|_| Vec::with_capacity(FANOUT)),
            last: Vec::with_capacity(MAX_BLOCK_BYTES),
            has_last: false,
            failed: false,
        }
    }

    #[cfg(test)]
    pub(in super::super) fn seed_depth_frontier(
        &mut self,
        all_levels: bool,
    ) -> Result<super::Builder> {
        // Simulated authenticated summaries only, not 128^8 executed leaf writes.
        let mut legacy = self.directory.builder();
        for level in 0..DEPTH {
            if !all_levels && level != DEPTH - 1 {
                continue;
            }
            for index in 0..FANOUT {
                let ordinal = (DEPTH - 1 - level) * FANOUT + index;
                let key = u64::try_from(ordinal)
                    .map_err(|_| super::capacity("fixture ordinal"))?
                    .to_be_bytes();
                let reference = self.directory.key_ref(&key)?;
                let node = Node {
                    depth: u8::try_from(level).map_err(|_| super::capacity("fixture depth"))?,
                    bytes: 100,
                    rows: 1,
                    first: reference,
                    last: reference,
                    digest: [7; 32],
                };
                self.levels
                    .get_mut(level)
                    .ok_or_else(|| super::capacity("fixture level"))?
                    .push(node);
                legacy
                    .levels
                    .get_mut(level)
                    .ok_or_else(|| super::capacity("fixture level"))?
                    .push(node);
            }
        }
        Ok(legacy)
    }

    pub(in super::super) async fn push(
        &mut self,
        io: &mut RestorePhysicalIo<'_>,
        route: &mut RestorePhysicalRoute<'_, '_>,
        leaf: &Leaf,
    ) -> Result<()> {
        let checked =
            decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
                #[cfg(any(test, feature = "test-utils"))]
                super::super::cost::bounded_work(super::super::cost::BoundedWork {
                    streaming_builder_inputs: 1,
                    ..Default::default()
                });
                if self.failed {
                    return Err(invariant_violation(
                        "native directory builder requires restart",
                    ));
                }
                if leaf.first.len() > MAX_BLOCK_BYTES
                    || leaf.last.len() > MAX_BLOCK_BYTES
                    || leaf.first > leaf.last
                    || leaf.rows == 0
                    || leaf.bytes == 0
                    || leaf.bytes as usize > super::MAX_SEGMENT_BYTES
                    || (leaf.rows > 1 && leaf.bytes as usize > MAX_BLOCK_BYTES)
                    || (self.has_last && self.last >= leaf.first)
                {
                    return Err(super::validation_failed(
                        "invalid or unordered directory leaf",
                    ));
                }
                self.failed = true;
                Ok(())
            })?;
        drop(checked);
        let first = self.write_key(io, route, &leaf.first).await?;
        let last = self.write_key(io, route, &leaf.last).await?;
        self.push_node(
            io,
            route,
            Node {
                depth: 0,
                first,
                last,
                rows: leaf.rows,
                bytes: leaf.bytes,
                digest: leaf.digest,
            },
        )
        .await?;
        let completed =
            decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
                self.last.clear();
                self.last.extend_from_slice(&leaf.last);
                self.has_last = true;
                self.failed = false;
                Ok(())
            })?;
        drop(completed);
        Ok(())
    }

    async fn write_key(
        &self,
        io: &mut RestorePhysicalIo<'_>,
        route: &mut RestorePhysicalRoute<'_, '_>,
        bytes: &[u8],
    ) -> Result<KeyRef> {
        let key =
            decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
                self.directory.key_ref(bytes)
            })?;
        if bytes.len() > INLINE_KEY_BYTES {
            super::super::physical::restore_io::write_directory_output(
                io,
                route,
                true,
                &key.value().digest,
                bytes,
            )
            .await?;
        }
        Ok(*key.value())
    }

    async fn push_node(
        &mut self,
        io: &mut RestorePhysicalIo<'_>,
        route: &mut RestorePhysicalRoute<'_, '_>,
        mut node: Node,
    ) -> Result<()> {
        loop {
            let level = usize::from(node.depth);
            let flush =
                decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
                    let added =
                        page_probe_bytes(std::slice::from_ref(&node))? - super::HEADER_BYTES - 1;
                    let page = self
                        .levels
                        .get_mut(level)
                        .ok_or_else(|| super::capacity("directory depth exceeded"))?;
                    if !page.is_empty()
                        && (page.len() == FANOUT
                            || page_probe_bytes(page)? + added > PAGE_PROBE_LIMIT)
                    {
                        Ok(true)
                    } else {
                        page.push(node);
                        Ok(false)
                    }
                })?;
            if !*flush.value() {
                return Ok(());
            }
            drop(flush);
            let page = self
                .levels
                .get(level)
                .ok_or_else(|| invariant_violation("builder level disappeared"))?;
            let parent = self.write_page(io, route, node.depth + 1, page).await?;
            let changed =
                decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
                    let page = self
                        .levels
                        .get_mut(level)
                        .ok_or_else(|| invariant_violation("builder level disappeared"))?;
                    page.clear();
                    page.push(node);
                    Ok(())
                })?;
            drop(changed);
            node = parent;
        }
    }

    pub(in super::super) async fn finish(
        &mut self,
        io: &mut RestorePhysicalIo<'_>,
        route: &mut RestorePhysicalRoute<'_, '_>,
    ) -> Result<WorkingValue<Root>> {
        let ready =
            decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
                if self.failed {
                    return Err(invariant_violation(
                        "native directory builder requires restart",
                    ));
                }
                self.failed = true;
                Ok(())
            })?;
        drop(ready);
        let node = loop {
            let Some(level) = self.levels.iter().position(|page| !page.is_empty()) else {
                break self.write_page(io, route, 1, &[]).await?;
            };
            let page = self
                .levels
                .get(level)
                .ok_or_else(|| invariant_violation("builder level disappeared"))?;
            if level > 0 && page.len() == 1 && self.levels.iter().skip(level + 1).all(Vec::is_empty)
            {
                break *page
                    .first()
                    .ok_or_else(|| invariant_violation("builder root disappeared"))?;
            }
            let depth =
                u8::try_from(level + 1).map_err(|_| super::capacity("directory depth overflow"))?;
            let parent = self.write_page(io, route, depth, page).await?;
            let cleared =
                decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
                    self.levels
                        .get_mut(level)
                        .ok_or_else(|| invariant_violation("builder level disappeared"))?
                        .clear();
                    Ok(())
                })?;
            drop(cleared);
            if usize::from(parent.depth) == DEPTH {
                break parent;
            }
            self.push_node(io, route, parent).await?;
        };
        decode_with_reservation(io, route, Some(DIRECTORY_FIXED_ALLOCATION_BYTES), || {
            Ok(Root {
                scope: self.directory.scope,
                node,
            })
        })
    }

    async fn write_page(
        &self,
        io: &mut RestorePhysicalIo<'_>,
        route: &mut RestorePhysicalRoute<'_, '_>,
        depth: u8,
        children: &[Node],
    ) -> Result<Node> {
        use super::super::physical::restore_io::{encode_with_reservation, write_directory_output};
        let encoded = encode_with_reservation(
            io,
            route,
            DIRECTORY_FIXED_ALLOCATION_BYTES + super::PAGE_LIMIT,
            || self.directory.page_node(depth, children),
        )?;
        write_directory_output(
            io,
            route,
            false,
            &encoded.value().0.digest,
            &encoded.value().1,
        )
        .await?;
        Ok(encoded.value().0)
    }
}

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

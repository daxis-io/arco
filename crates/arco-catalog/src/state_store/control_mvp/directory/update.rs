//! Bounded path replacement. Leaf semantics are checked by the owning authority.
use super::{
    Bytes, Directory, FANOUT, HEADER_BYTES, Leaf, Node, PAGE_PROBE_LIMIT, ReadBudget, Result, Root,
    capacity, invariant_violation, page_probe_bytes, validate_node,
};

#[derive(Clone, Debug)]
pub(in super::super) struct Edit {
    pub old: Option<Leaf>,
    pub new: Vec<Leaf>,
}

struct Change {
    key: Vec<u8>,
    old: Option<Node>,
    new: Vec<Node>,
}

impl Directory {
    pub(in super::super) async fn update(
        &self,
        root: &Root,
        edits: &[Edit],
        budget: &mut ReadBudget,
    ) -> Result<Root> {
        self.check_update_root(root)?;
        let changes = self.changes(edits)?;
        if changes.is_empty() {
            return Ok(root.clone());
        }
        for edit in edits {
            for leaf in &edit.new {
                self.write_key(&leaf.first).await?;
                self.write_key(&leaf.last).await?;
            }
        }
        let changes = changes.iter().collect::<Vec<_>>();
        let mut nodes = self.rewrite(root.node, &changes, budget).await?;
        if nodes.is_empty() {
            nodes.push(self.write_page(1, &[]).await?);
        }
        while nodes.len() > 1 {
            let depth = nodes
                .first()
                .ok_or_else(|| capacity("missing update root"))?
                .depth;
            nodes = self.pack_update(depth + 1, &nodes, budget).await?;
        }
        let mut node = *nodes
            .first()
            .ok_or_else(|| capacity("missing update root"))?;
        while node.depth > 1 {
            let children = self.read_page(node, b"", None, None, budget).await?;
            let [only] = children.as_slice() else {
                break;
            };
            node = *only;
        }
        Ok(Root {
            scope: self.scope,
            node,
        })
    }

    pub(in super::super) async fn verify_update(
        &self,
        old: &Root,
        new: &Root,
        edits: &[Edit],
        budget: &mut ReadBudget,
    ) -> Result<()> {
        self.check_update_root(old)?;
        self.check_update_root(new)?;
        let changes = self.changes(edits)?;
        if changes.is_empty() {
            return if old == new {
                Ok(())
            } else {
                Err(invariant_violation(
                    "empty transition changed directory root",
                ))
            };
        }
        let changes = changes.iter().collect::<Vec<_>>();
        let mut pages = Vec::new();
        let mut nodes = self
            .verify_rewrite(old.node, &changes, budget, &mut pages)
            .await?;
        if nodes.is_empty() {
            let (node, _) = self.page_node(1, &[])?;
            Self::remember_page(&mut pages, node, Vec::new())?;
            nodes.push(node);
        }
        while nodes.len() > 1 {
            let depth = nodes
                .first()
                .ok_or_else(|| capacity("missing verified update root"))?
                .depth;
            nodes = self
                .verify_pack(depth + 1, &nodes, budget, &mut pages)
                .await?;
        }
        let mut expected = *nodes
            .first()
            .ok_or_else(|| capacity("missing verified update root"))?;
        while expected.depth > 1 {
            let children = self
                .expected_page_children(expected, &pages, budget)
                .await?;
            let [only] = children.as_slice() else {
                break;
            };
            pages.retain(|(node, _)| *node != expected);
            expected = *only;
        }
        if expected != new.node {
            return Err(invariant_violation(
                "transition did not use canonical directory layout",
            ));
        }
        for (node, children) in pages {
            if self.read_page(node, b"", None, None, budget).await? != children {
                return Err(invariant_violation("verified directory page changed"));
            }
        }
        Ok(())
    }

    fn check_update_root(&self, root: &Root) -> Result<()> {
        if root.scope != self.scope || root.node.depth == 0 {
            return Err(invariant_violation("invalid update root scope or depth"));
        }
        validate_node(&root.node, true)
    }

    fn leaf_node(&self, leaf: &Leaf) -> Result<Node> {
        if leaf.first > leaf.last {
            return Err(invariant_violation("reversed update leaf endpoints"));
        }
        let node = Node {
            depth: 0,
            first: self.key_ref(&leaf.first)?,
            last: self.key_ref(&leaf.last)?,
            rows: leaf.rows,
            bytes: leaf.bytes,
            digest: leaf.digest,
        };
        validate_node(&node, false)?;
        Ok(node)
    }

    fn changes(&self, edits: &[Edit]) -> Result<Vec<Change>> {
        if edits.len() > 16 || edits.iter().map(|e| e.new.len()).sum::<usize>() > 32 {
            return Err(capacity(
                "directory update exceeds selected/replacement block limit",
            ));
        }
        let mut changes = Vec::new();
        for edit in edits {
            let anchor = edit
                .old
                .as_ref()
                .or_else(|| edit.new.first())
                .ok_or_else(|| invariant_violation("empty directory edit"))?;
            if edit.new.windows(2).any(|pair| match pair {
                [a, b] => a.last >= b.first,
                _ => true,
            }) {
                return Err(invariant_violation(
                    "unordered directory replacement leaves",
                ));
            }
            changes.push(Change {
                key: anchor.first.clone(),
                old: edit
                    .old
                    .as_ref()
                    .map(|leaf| self.leaf_node(leaf))
                    .transpose()?,
                new: edit
                    .new
                    .iter()
                    .map(|leaf| self.leaf_node(leaf))
                    .collect::<Result<_>>()?,
            });
        }
        changes.sort_by(|a, b| a.key.cmp(&b.key));
        if changes.windows(2).any(|pair| match pair {
            [a, b] => a.key == b.key,
            _ => true,
        }) {
            return Err(invariant_violation("duplicate directory update paths"));
        }
        Ok(changes)
    }

    async fn route_changes<'a>(
        &self,
        children: &[Node],
        changes: &[&'a Change],
        budget: &mut ReadBudget,
    ) -> Result<Vec<Vec<&'a Change>>> {
        let mut firsts = Vec::new();
        for child in children {
            firsts.push(self.read_key(child.first, budget).await?);
        }
        let mut groups = vec![Vec::new(); children.len()];
        for change in changes {
            let slot = firsts
                .partition_point(|first| first.as_ref() <= change.key.as_slice())
                .saturating_sub(1);
            groups
                .get_mut(slot)
                .ok_or_else(|| invariant_violation("empty internal update page"))?
                .push(*change);
        }
        Ok(groups)
    }

    async fn edited_leaves(
        &self,
        leaves: Vec<Node>,
        changes: &[&Change],
        budget: &mut ReadBudget,
    ) -> Result<Vec<Node>> {
        let mut removed = Vec::new();
        for change in changes {
            if let Some(old) = change.old {
                let slot = leaves
                    .iter()
                    .position(|node| *node == old)
                    .ok_or_else(|| invariant_violation("replaced leaf is absent from old root"))?;
                if removed.contains(&slot) {
                    return Err(invariant_violation(
                        "multiple replacements claim one old directory leaf",
                    ));
                }
                removed.push(slot);
            }
        }
        let mut leaves = leaves
            .into_iter()
            .enumerate()
            .filter_map(|(slot, leaf)| (!removed.contains(&slot)).then_some(leaf))
            .collect::<Vec<_>>();
        for change in changes {
            leaves.extend_from_slice(&change.new);
        }
        let mut keyed = Vec::new();
        for leaf in leaves {
            keyed.push((self.read_key(leaf.first, budget).await?, leaf));
        }
        keyed.sort_by(|a, b| a.0.cmp(&b.0));
        let nodes = keyed.into_iter().map(|(_, node)| node).collect::<Vec<_>>();
        self.check_order(&nodes, budget).await?;
        Ok(nodes)
    }

    async fn check_order(&self, nodes: &[Node], budget: &mut ReadBudget) -> Result<()> {
        let mut last: Option<Bytes> = None;
        for node in nodes {
            let first = self.read_key(node.first, budget).await?;
            let end = self.read_key(node.last, budget).await?;
            if first > end || last.as_ref().is_some_and(|prior| prior >= &first) {
                return Err(invariant_violation("overlapping directory update coverage"));
            }
            last = Some(end);
        }
        Ok(())
    }

    async fn pack_update(
        &self,
        depth: u8,
        nodes: &[Node],
        budget: &mut ReadBudget,
    ) -> Result<Vec<Node>> {
        self.check_order(nodes, budget).await?;
        let mut output = Vec::new();
        let mut page = Vec::new();
        for node in nodes {
            let added = page_probe_bytes(std::slice::from_ref(node))? - HEADER_BYTES - 1;
            if !page.is_empty()
                && (page.len() == FANOUT || page_probe_bytes(&page)? + added > PAGE_PROBE_LIMIT)
            {
                output.push(self.write_page(depth, &page).await?);
                page.clear();
            }
            page.push(*node);
        }
        if !page.is_empty() {
            output.push(self.write_page(depth, &page).await?);
        }
        Ok(output)
    }

    async fn rewrite(
        &self,
        node: Node,
        changes: &[&Change],
        budget: &mut ReadBudget,
    ) -> Result<Vec<Node>> {
        if changes.is_empty() {
            return Ok(vec![node]);
        }
        let children = self.read_page(node, b"", None, None, budget).await?;
        let replacement = if node.depth == 1 {
            self.edited_leaves(children, changes, budget).await?
        } else {
            let groups = self.route_changes(&children, changes, budget).await?;
            let mut replacement = Vec::new();
            for (child, group) in children.into_iter().zip(groups) {
                replacement.extend(Box::pin(self.rewrite(child, &group, budget)).await?);
            }
            replacement
        };
        self.pack_update(node.depth, &replacement, budget).await
    }

    async fn verify_rewrite(
        &self,
        node: Node,
        changes: &[&Change],
        budget: &mut ReadBudget,
        pages: &mut Vec<(Node, Vec<Node>)>,
    ) -> Result<Vec<Node>> {
        if changes.is_empty() {
            return Ok(vec![node]);
        }
        let children = self.read_page(node, b"", None, None, budget).await?;
        let replacement = if node.depth == 1 {
            self.verify_edited_leaves(children, changes, budget).await?
        } else {
            let groups = self
                .verify_route_changes(&children, changes, budget)
                .await?;
            let mut replacement = Vec::new();
            for (child, group) in children.into_iter().zip(groups) {
                replacement
                    .extend(Box::pin(self.verify_rewrite(child, &group, budget, pages)).await?);
            }
            replacement
        };
        self.verify_pack(node.depth, &replacement, budget, pages)
            .await
    }

    async fn verify_route_changes<'a>(
        &self,
        children: &[Node],
        changes: &[&'a Change],
        budget: &mut ReadBudget,
    ) -> Result<Vec<Vec<&'a Change>>> {
        let mut firsts = Vec::new();
        for child in children {
            firsts.push(self.read_key(child.first, budget).await?);
        }
        let mut groups = vec![Vec::new(); children.len()];
        for change in changes {
            let slot = firsts
                .partition_point(|first| first.as_ref() <= change.key.as_slice())
                .saturating_sub(1);
            groups
                .get_mut(slot)
                .ok_or_else(|| invariant_violation("empty internal verified update page"))?
                .push(*change);
        }
        Ok(groups)
    }

    async fn verify_edited_leaves(
        &self,
        leaves: Vec<Node>,
        changes: &[&Change],
        budget: &mut ReadBudget,
    ) -> Result<Vec<Node>> {
        let mut removed = Vec::new();
        for change in changes {
            if let Some(old) = change.old {
                let slot = leaves
                    .iter()
                    .position(|node| *node == old)
                    .ok_or_else(|| invariant_violation("replaced leaf is absent from old root"))?;
                if removed.contains(&slot) {
                    return Err(invariant_violation(
                        "multiple replacements claim one old directory leaf",
                    ));
                }
                removed.push(slot);
            }
        }
        let mut result = leaves
            .into_iter()
            .enumerate()
            .filter_map(|(slot, leaf)| (!removed.contains(&slot)).then_some(leaf))
            .collect::<Vec<_>>();
        for change in changes {
            result.extend_from_slice(&change.new);
        }
        let mut keyed = Vec::new();
        for leaf in result {
            keyed.push((self.read_key(leaf.first, budget).await?, leaf));
        }
        keyed.sort_by(|a, b| a.0.cmp(&b.0));
        let result = keyed.into_iter().map(|(_, leaf)| leaf).collect::<Vec<_>>();
        self.verify_order(&result, budget).await?;
        Ok(result)
    }

    async fn verify_order(&self, nodes: &[Node], budget: &mut ReadBudget) -> Result<()> {
        let mut last: Option<Bytes> = None;
        for node in nodes {
            let first = self.read_key(node.first, budget).await?;
            let end = self.read_key(node.last, budget).await?;
            if first > end || last.as_ref().is_some_and(|prior| prior >= &first) {
                return Err(invariant_violation("overlapping directory update coverage"));
            }
            last = Some(end);
        }
        Ok(())
    }

    async fn verify_pack(
        &self,
        depth: u8,
        nodes: &[Node],
        budget: &mut ReadBudget,
        pages: &mut Vec<(Node, Vec<Node>)>,
    ) -> Result<Vec<Node>> {
        self.verify_order(nodes, budget).await?;
        let mut output = Vec::new();
        let mut page = Vec::new();
        for node in nodes {
            let added = page_probe_bytes(std::slice::from_ref(node))? - HEADER_BYTES - 1;
            if !page.is_empty()
                && (page.len() == FANOUT || page_probe_bytes(&page)? + added > PAGE_PROBE_LIMIT)
            {
                let (node, _) = self.page_node(depth, &page)?;
                Self::remember_page(pages, node, page.clone())?;
                output.push(node);
                page.clear();
            }
            page.push(*node);
        }
        if !page.is_empty() {
            let (node, _) = self.page_node(depth, &page)?;
            Self::remember_page(pages, node, page)?;
            output.push(node);
        }
        Ok(output)
    }

    fn remember_page(
        pages: &mut Vec<(Node, Vec<Node>)>,
        node: Node,
        children: Vec<Node>,
    ) -> Result<()> {
        if let Some((_, recorded)) = pages.iter().find(|(recorded, _)| *recorded == node) {
            return if *recorded == children {
                Ok(())
            } else {
                Err(invariant_violation(
                    "directory page digest has conflicting children",
                ))
            };
        }
        pages.push((node, children));
        Ok(())
    }

    async fn expected_page_children(
        &self,
        node: Node,
        pages: &[(Node, Vec<Node>)],
        budget: &mut ReadBudget,
    ) -> Result<Vec<Node>> {
        if let Some((_, children)) = pages.iter().find(|(recorded, _)| *recorded == node) {
            return Ok(children.clone());
        }
        self.read_page(node, b"", None, None, budget).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::super::tests::{build, directory, leaf};
    use super::*;

    #[tokio::test]
    async fn transition_rejects_chained_replacement_as_old_authority() {
        let dir = directory("catalog");
        let old = build(&dir, 1).await;
        let edits = [
            Edit {
                old: Some(leaf(0)),
                new: vec![leaf(1)],
            },
            Edit {
                old: Some(leaf(1)),
                new: vec![leaf(2)],
            },
        ];
        let mut builder = dir.builder();
        builder.push(leaf(2)).await.unwrap();
        let forged = builder.finish().await.unwrap();
        let update_accepted = dir
            .update(&old, &edits, &mut ReadBudget::default())
            .await
            .is_ok();
        let verifier_accepted = dir
            .verify_update(&old, &forged, &edits, &mut ReadBudget::default())
            .await
            .is_ok();
        assert!(
            !update_accepted && !verifier_accepted,
            "update accepted={update_accepted}; verifier accepted={verifier_accepted}"
        );
    }

    #[tokio::test]
    async fn transition_rejects_gratuitous_root_wrapping() {
        let dir = directory("catalog");
        let old = build(&dir, 1).await;
        let wrapped = Root {
            scope: old.scope,
            node: dir
                .write_page(old.node.depth + 1, &[old.node])
                .await
                .unwrap(),
        };
        assert!(
            dir.verify_update(&old, &wrapped, &[], &mut ReadBudget::default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn transition_rejects_wrapping_a_changed_root() {
        let dir = directory("catalog");
        let old = build(&dir, 1).await;
        let mut replacement = leaf(0);
        replacement.digest = [23; 32];
        let edits = [Edit {
            old: Some(leaf(0)),
            new: vec![replacement],
        }];
        let changed = dir
            .update(&old, &edits, &mut ReadBudget::default())
            .await
            .unwrap();
        let wrapped = Root {
            scope: changed.scope,
            node: dir
                .write_page(changed.node.depth + 1, &[changed.node])
                .await
                .unwrap(),
        };
        assert!(
            dir.verify_update(&old, &wrapped, &edits, &mut ReadBudget::default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn transition_rejects_regrouped_untouched_children() {
        let dir = directory("catalog");
        let old = build(&dir, 513).await;
        let mut replacement = leaf(257);
        replacement.digest = [42; 32];
        let edits = [Edit {
            old: Some(leaf(257)),
            new: vec![replacement.clone()],
        }];
        let leaves = (0..513)
            .map(|number| {
                if number == 257 {
                    dir.leaf_node(&replacement)
                } else {
                    dir.leaf_node(&leaf(number))
                }
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let mut start = 0;
        let mut children = Vec::new();
        for count in [127, 128, 128, 128, 2] {
            children.push(
                dir.write_page(1, &leaves[start..start + count])
                    .await
                    .unwrap(),
            );
            start += count;
        }
        let regrouped = Root {
            scope: old.scope,
            node: dir.write_page(2, &children).await.unwrap(),
        };
        assert!(
            dir.verify_update(&old, &regrouped, &edits, &mut ReadBudget::default())
                .await
                .is_err()
        );
    }

    fn fenced_leaf(group: u8, first: u8, last: u8) -> Leaf {
        let mut lower = vec![group; 80];
        let mut upper = lower.clone();
        lower[79] = first;
        upper[79] = last;
        Leaf {
            first: lower,
            last: upper,
            rows: 2,
            bytes: 100,
            digest: [group ^ first ^ last; 32],
        }
    }

    #[tokio::test]
    async fn path_copy_handles_long_fences_and_exact_edit_limits() {
        let dir = directory("catalog");
        let mut builder = dir.builder();
        for group in 0..16 {
            builder.push(fenced_leaf(group, 0, 9)).await.unwrap();
        }
        let old = builder.finish().await.unwrap();
        let edits = (0..16)
            .map(|group| Edit {
                old: Some(fenced_leaf(group, 0, 9)),
                new: vec![fenced_leaf(group, 0, 4), fenced_leaf(group, 5, 9)],
            })
            .collect::<Vec<_>>();
        let changed = dir
            .update(&old, &edits, &mut ReadBudget::default())
            .await
            .unwrap();
        dir.verify_update(&old, &changed, &edits, &mut ReadBudget::default())
            .await
            .unwrap();

        let too_many_edits = (0..17)
            .map(|group| Edit {
                old: Some(fenced_leaf(group, 0, 9)),
                new: Vec::new(),
            })
            .collect::<Vec<_>>();
        assert!(
            dir.update(&old, &too_many_edits, &mut ReadBudget::default())
                .await
                .is_err()
        );
        assert!(
            dir.update(
                &old,
                &[Edit {
                    old: None,
                    new: (0..33).map(|number| fenced_leaf(number, 0, 0)).collect(),
                }],
                &mut ReadBudget::default(),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn collapsed_transition_does_not_depend_on_unattached_intermediate_page() {
        let storage = arco_core::ScopedStorage::new(
            std::sync::Arc::new(arco_core::MemoryBackend::new()),
            "tenant",
            "workspace",
        )
        .unwrap();
        let dir = Directory::new(
            storage.clone(),
            &super::super::super::StateScope::new("tenant", "workspace", "catalog"),
        )
        .unwrap();
        let old = build(&dir, 129).await;
        let edits = [Edit {
            old: Some(leaf(128)),
            new: Vec::new(),
        }];
        let new = dir
            .update(&old, &edits, &mut ReadBudget::default())
            .await
            .unwrap();
        assert_eq!(new.node.depth, 1);
        let (discarded, _) = dir.page_node(2, &[new.node]).unwrap();
        storage
            .delete(&dir.path("pages", &discarded.digest))
            .await
            .unwrap();
        dir.verify_update(&old, &new, &edits, &mut ReadBudget::default())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn path_copy_splits_and_collapses_a_full_page() {
        let dir = directory("catalog");
        let old = build(&dir, 128).await;
        let edits = [Edit {
            old: None,
            new: vec![leaf(128)],
        }];
        let split = dir
            .update(&old, &edits, &mut ReadBudget::default())
            .await
            .unwrap();
        assert_eq!(split.node.depth, 2);
        dir.verify_update(&old, &split, &edits, &mut ReadBudget::default())
            .await
            .unwrap();
        let edits = [Edit {
            old: Some(leaf(128)),
            new: vec![],
        }];
        let collapsed = dir
            .update(&split, &edits, &mut ReadBudget::default())
            .await
            .unwrap();
        assert_eq!(collapsed, old);
        dir.verify_update(&split, &collapsed, &edits, &mut ReadBudget::default())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn path_copy_work_tracks_depth_and_rejects_extra_changes() {
        let dir = directory("catalog");
        for count in [128, 513, 16_385] {
            let old = build(&dir, count).await;
            let mut changed = leaf(count / 2);
            changed.digest = [42; 32];
            let edits = [Edit {
                old: Some(leaf(count / 2)),
                new: vec![changed],
            }];
            let mut budget = ReadBudget::default();
            let new = dir.update(&old, &edits, &mut budget).await.unwrap();
            dir.verify_update(&old, &new, &edits, &mut budget)
                .await
                .unwrap();
            assert!(
                budget.objects <= 4 * usize::from(old.node.depth) * 2,
                "{} probes at depth {}",
                budget.objects,
                old.node.depth
            );
            let extra = [Edit {
                old: Some(leaf(0)),
                new: vec![],
            }];
            let forged = dir
                .update(&new, &extra, &mut ReadBudget::default())
                .await
                .unwrap();
            assert!(
                dir.verify_update(&old, &forged, &edits, &mut ReadBudget::default())
                    .await
                    .is_err()
            );
            assert!(
                dir.update(
                    &old,
                    &[edits[0].clone(), edits[0].clone()],
                    &mut ReadBudget::default()
                )
                .await
                .is_err()
            );
        }
    }

    #[tokio::test]
    async fn path_copy_replaces_one_leaf_and_retains_the_old_root() {
        let dir = directory("catalog");
        let old = build(&dir, 513).await;
        let mut replacement = leaf(257);
        replacement.digest = [42; 32];
        let edits = [Edit {
            old: Some(leaf(257)),
            new: vec![replacement.clone()],
        }];
        let new = dir
            .update(&old, &edits, &mut ReadBudget::default())
            .await
            .unwrap();
        assert_eq!(
            dir.lookup(&new, &replacement.first, &mut ReadBudget::default())
                .await
                .unwrap(),
            Some(replacement)
        );
        assert_eq!(
            dir.lookup(&old, &leaf(257).first, &mut ReadBudget::default())
                .await
                .unwrap(),
            Some(leaf(257))
        );
        dir.verify_update(&old, &new, &edits, &mut ReadBudget::default())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn transition_rejects_omitted_untouched_siblings() {
        let dir = directory("catalog");
        let old = build(&dir, 513).await;
        let forged = build(&dir, 512).await;
        assert!(
            dir.verify_update(&old, &forged, &[], &mut ReadBudget::default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn path_copy_inserts_gaps_and_collapses_empty_roots() {
        let dir = directory("catalog");
        let empty = build(&dir, 0).await;
        let edit = Edit {
            old: None,
            new: vec![leaf(1), leaf(2)],
        };
        let inserted = dir
            .update(&empty, &[edit.clone()], &mut ReadBudget::default())
            .await
            .unwrap();
        assert_eq!(
            dir.scan(&inserted, b"", None, 128, None, &mut ReadBudget::default())
                .await
                .unwrap()
                .leaves,
            vec![leaf(1), leaf(2)]
        );
        dir.verify_update(&empty, &inserted, &[edit], &mut ReadBudget::default())
            .await
            .unwrap();
        let trims = [
            Edit {
                old: Some(leaf(1)),
                new: vec![],
            },
            Edit {
                old: Some(leaf(2)),
                new: vec![],
            },
        ];
        let cleared = dir
            .update(&inserted, &trims, &mut ReadBudget::default())
            .await
            .unwrap();
        assert_eq!(cleared, empty);
        dir.verify_update(&inserted, &cleared, &trims, &mut ReadBudget::default())
            .await
            .unwrap();
    }
}

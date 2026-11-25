//! A bounded, read-only candidate page consuming independently replayed marks.
use super::super::super::ControlMvpGcPlan;
use super::{
    BTreeSet, CatalogError, ControlMvpMaintenanceWorker, GcMarkVerifier, Result,
    RetainedAuthorityRoots, WorkspaceIoBudget, ensure_fresh_fence, invariant_violation, load_seed,
    retained,
};

/// Opaque progress for one candidate page; restart replays coverage from zero.
pub struct GcMarkedPlan {
    verifier: GcMarkVerifier,
    plan: Option<ControlMvpGcPlan>,
    admission: (usize, usize),
    continuation: Option<String>,
}

impl GcMarkedPlan {
    /// Number of independently replayed proof pages.
    #[must_use]
    pub const fn verified_pages(&self) -> u64 {
        self.verifier.verified_pages()
    }

    /// Admitted bytes and charged operations of the latest advance.
    #[must_use]
    pub const fn last_admission(&self) -> (usize, usize) {
        self.admission
    }
}

fn plan_owned_bytes(plan: &ControlMvpGcPlan) -> usize {
    plan.candidates.iter().fold(
        plan.head_version
            .capacity()
            .saturating_add(plan.continuation.as_ref().map_or(0, String::capacity))
            .saturating_add(
                plan.candidates
                    .capacity()
                    .saturating_mul(size_of::<super::super::super::ControlMvpGcCandidate>()),
            ),
        |bytes, candidate| {
            bytes
                .saturating_add(candidate.path.capacity())
                .saturating_add(candidate.version.capacity())
        },
    )
}

impl ControlMvpMaintenanceWorker {
    /// Starts one read-only inventory page at the immutable seed's collection cut.
    ///
    /// # Errors
    /// Rejects stale evidence, invalid cursors/protection, unavailable storage or exhausted admission.
    pub async fn begin_marked_gc_plan(
        &self,
        digest: &str,
        additional_protected_paths: impl IntoIterator<Item = String>,
        continuation: Option<&str>,
    ) -> Result<GcMarkedPlan> {
        let mut io = WorkspaceIoBudget::for_control_gc();
        let seed = load_seed(self, digest, &mut io).await?;
        let (mut plan, _) = self
            .inventory_gc_page_bounded(
                seed.collection_time,
                additional_protected_paths,
                continuation,
                &mut io,
            )
            .await?;
        if plan.head_version != seed.snapshot.head_version {
            return Err(CatalogError::CasFailed {
                message: "marked GC inventory HEAD differs from its seed".into(),
            });
        }
        io.reserve_bytes(
            plan_owned_bytes(&plan)
                .saturating_mul(2)
                .saturating_add(digest.len() + 256),
        )?;
        let mut paths = plan
            .candidates
            .iter()
            .map(|c| c.path.clone())
            .collect::<BTreeSet<_>>();
        retained::protect_gc_selected_pointer(
            &self.store,
            &mut paths,
            &mut retained::ReadBudget::for_gc(&mut io),
        )
        .await?;
        let mut roots = RetainedAuthorityRoots::new(&self.lifecycle, seed.collection_time);
        while let Some(root) = roots.next_bounded(&mut io).await? {
            io.charge_operations(
                root.required_paths
                    .len()
                    .saturating_add(root.protected_prefixes.len()),
            )?;
            for path in root.required_paths {
                paths.remove(&path);
            }
            for prefix in root.protected_prefixes {
                paths.retain(|path| !path.starts_with(&prefix));
            }
        }
        ensure_fresh_fence(self, &seed, &mut io).await?;
        plan.candidates.retain(|c| paths.contains(&c.path));
        let verifier = GcMarkVerifier::new(self, digest, seed, &mut io)?;
        let (bytes, operations, _, _) = io.test_accounting();
        Ok(GcMarkedPlan {
            verifier,
            continuation: None,
            plan: Some(plan),
            admission: (bytes, operations),
        })
    }

    /// Admits the next inventory page using the same independently verified sweep.
    /// A restart must use `begin_marked_gc_plan` and rebuild verification once.
    ///
    /// # Errors
    /// Rejects incomplete, exhausted or stale sessions and exhausted invocation budgets.
    pub async fn continue_marked_gc_plan(
        &self,
        planning: &mut GcMarkedPlan,
        additional_protected_paths: impl IntoIterator<Item = String>,
    ) -> Result<()> {
        let mut io = WorkspaceIoBudget::for_control_gc();
        let plan = self
            .prepare_next_marked_gc_plan(planning, additional_protected_paths, &mut io)
            .await?;
        ensure_fresh_fence(self, &planning.verifier.snapshot, &mut io).await?;
        let (bytes, operations, _, _) = io.test_accounting();
        planning.plan = Some(plan);
        planning.admission = (bytes, operations);
        Ok(())
    }

    /// Returns the next independently protected page in one bounded invocation.
    ///
    /// # Errors
    /// Rejects incomplete sessions, stale roots, changed candidates, exhausted inventory or admission.
    pub async fn next_marked_gc_plan_page(
        &self,
        planning: &mut GcMarkedPlan,
        additional_protected_paths: impl IntoIterator<Item = String>,
    ) -> Result<ControlMvpGcPlan> {
        let mut io = WorkspaceIoBudget::for_control_gc();
        let plan = self
            .prepare_next_marked_gc_plan(planning, additional_protected_paths, &mut io)
            .await?;
        refence_candidates(self, plan.candidates.iter(), &mut io).await?;
        ensure_fresh_fence(self, &planning.verifier.snapshot, &mut io).await?;
        let (bytes, operations, _, _) = io.test_accounting();
        planning.continuation.clone_from(&plan.continuation);
        planning.admission = (bytes, operations);
        Ok(plan)
    }

    async fn prepare_next_marked_gc_plan(
        &self,
        planning: &GcMarkedPlan,
        additional_protected_paths: impl IntoIterator<Item = String>,
        io: &mut WorkspaceIoBudget,
    ) -> Result<ControlMvpGcPlan> {
        planning.verifier.validate_worker(self)?;
        if planning.plan.is_some() || !planning.verifier.complete {
            return Err(invariant_violation(
                "GC sweep has an unfinished inventory page",
            ));
        }
        let cursor = planning
            .continuation
            .as_deref()
            .ok_or_else(|| invariant_violation("GC sweep inventory is complete"))?;
        io.reserve_bytes(planning.verifier.owned_bytes().saturating_mul(2))?;
        ensure_fresh_fence(self, &planning.verifier.snapshot, io).await?;
        let (mut plan, _) = self
            .inventory_gc_page_bounded(
                planning.verifier.snapshot.collection_time,
                additional_protected_paths,
                Some(cursor),
                io,
            )
            .await?;
        if plan.head_version != planning.verifier.snapshot.snapshot.head_version {
            return Err(CatalogError::CasFailed {
                message: "GC sweep HEAD changed".into(),
            });
        }
        io.reserve_bytes(plan_owned_bytes(&plan).saturating_mul(2))?;
        // Full workspace/retained validation occurred at admission; every immutable managed
        // graph object is in the filter, and changed selectors invalidate this session.
        let mut candidates = Vec::with_capacity(plan.candidates.len());
        for candidate in plan.candidates {
            if !planning.verifier.protects(&candidate.path, io)? {
                candidates.push(candidate);
            }
        }
        plan.candidates = candidates;
        Ok(plan)
    }

    /// Replays one proof page, returning candidates only after complete coverage.
    ///
    /// The inventory and verifier remain unchanged after failure or cancellation.
    /// A completed plan grants no deletion authority.
    ///
    /// # Errors
    /// Rejects changed or incomplete proofs, changed candidates, unavailable storage or exhausted admission.
    pub async fn advance_marked_gc_plan(
        &self,
        planning: &mut GcMarkedPlan,
    ) -> Result<Option<ControlMvpGcPlan>> {
        planning.verifier.validate_worker(self)?;
        let plan = planning
            .plan
            .as_ref()
            .ok_or_else(|| invariant_violation("marked GC plan is already returned"))?;
        let mut io = WorkspaceIoBudget::for_control_gc();
        let cursor_bytes = planning.verifier.cursor.as_ref().map_or(0, |c| {
            c.previous
                .capacity()
                .saturating_add(c.after.as_ref().map_or(0, Vec::capacity))
        });
        let owned = plan_owned_bytes(plan)
            .saturating_add(planning.verifier.seed.capacity())
            .saturating_add(cursor_bytes)
            .saturating_add(size_of::<GcMarkedPlan>());
        io.reserve_bytes(
            owned
                .saturating_mul(2)
                .saturating_add(plan.candidates.len().saturating_mul(128)),
        )?;
        io.charge_operations(plan.candidates.len())?;
        let mut paths = plan
            .candidates
            .iter()
            .map(|c| c.path.clone())
            .collect::<BTreeSet<_>>();
        io.reserve_bytes(planning.verifier.owned_bytes().saturating_mul(3))?;
        let mut next = planning.verifier.clone();
        let seed = if next.complete {
            ensure_fresh_fence(self, &next.snapshot, &mut io).await?;
            let mut remaining = BTreeSet::new();
            for path in &paths {
                if !next.protects(path, &mut io)? {
                    remaining.insert(path.clone());
                }
            }
            paths = remaining;
            next.snapshot.clone()
        } else {
            self.verify_gc_mark_page(&mut next, &mut io, Some(&mut paths))
                .await?
        };
        if next.complete {
            refence_candidates(
                self,
                plan.candidates
                    .iter()
                    .filter(|candidate| paths.contains(&candidate.path)),
                &mut io,
            )
            .await?;
            ensure_fresh_fence(self, &seed, &mut io).await?;
        }
        let complete = next.complete;
        let (bytes, operations, _, _) = io.test_accounting();
        planning
            .plan
            .as_mut()
            .ok_or_else(|| invariant_violation("marked GC inventory is missing"))?
            .candidates
            .retain(|c| paths.contains(&c.path));
        planning.verifier = next;
        planning.admission = (bytes, operations);
        Ok(if complete {
            planning.continuation = planning.plan.as_ref().and_then(|p| p.continuation.clone());
            planning.plan.take()
        } else {
            None
        })
    }
}

async fn refence_candidates<'a>(
    worker: &ControlMvpMaintenanceWorker,
    candidates: impl Iterator<Item = &'a super::super::super::ControlMvpGcCandidate> + Send,
    io: &mut WorkspaceIoBudget,
) -> Result<()> {
    for candidate in candidates {
        let current = io.head(&worker.lifecycle, &candidate.path).await?;
        if current.is_none_or(|meta| {
            meta.version.is_empty()
                || meta.version != candidate.version
                || meta.size != candidate.size
        }) {
            return Err(CatalogError::CasFailed {
                message: "marked GC candidate version or size changed".into(),
            });
        }
    }
    Ok(())
}

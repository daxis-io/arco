//! Optional local counters. Phases are scoped to future polls, never across
//! suspension or task migration. Nested counters partition, not add to, totals.
#[cfg(feature = "test-utils")]
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
};
#[cfg(feature = "test-utils")]
thread_local! {
    static PHASE: Cell<&'static str> = const { Cell::new("request") };
    static WORK: RefCell<BTreeMap<&'static str,[u64;36]>> = const { RefCell::new(BTreeMap::new()) };
    static BOUNDED_WORK: RefCell<BTreeMap<&'static str, BoundedWork>> = const { RefCell::new(BTreeMap::new()) };
}
pub(super) struct PhaseGuard {
    #[cfg(feature = "test-utils")]
    prior: &'static str,
}

/// Work that the authority-8 bounded path must report independently of the
/// historical fixed phase slots.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct BoundedWork {
    /// Arrow rows presented to a decoder, including a rejected malformed batch.
    pub decoded_rows: u64,
    /// Rows semantically checked by transition proofs.
    pub transition_proof_rows: u64,
    /// Unique physical blocks selected by one bounded operation.
    pub selected_blocks: u64,
    /// Physical blocks rewritten by one bounded operation.
    pub rewritten_blocks: u64,
    /// Directory child references decoded or encoded, including failed decodes.
    pub directory_references: u64,
    /// Inputs presented to the streaming directory builder, including rejections.
    pub streaming_builder_inputs: u64,
}

/// Records authority-8 bounded work without consuming historical slot space.
pub(super) fn bounded_work(work: BoundedWork) {
    #[cfg(feature = "test-utils")]
    BOUNDED_WORK.with(|all| {
        let mut all = all.borrow_mut();
        let total = all.entry(current()).or_default();
        total.decoded_rows += work.decoded_rows;
        total.transition_proof_rows += work.transition_proof_rows;
        total.selected_blocks += work.selected_blocks;
        total.rewritten_blocks += work.rewritten_blocks;
        total.directory_references += work.directory_references;
        total.streaming_builder_inputs += work.streaming_builder_inputs;
    });
    #[cfg(not(feature = "test-utils"))]
    let _ = work;
}

#[cfg(feature = "test-utils")]
pub(super) fn take_bounded_work() -> BTreeMap<&'static str, BoundedWork> {
    BOUNDED_WORK.with(|work| std::mem::take(&mut *work.borrow_mut()))
}
impl PhaseGuard {
    pub(super) fn enter(phase: &'static str) -> Self {
        let _ = phase;
        #[cfg(feature = "test-utils")]
        WORK.with(|work| {
            work.borrow_mut().entry(phase).or_insert([0; 36]);
        });
        Self {
            #[cfg(feature = "test-utils")]
            prior: PHASE.with(|p| p.replace(phase)),
        }
    }
}
impl Drop for PhaseGuard {
    fn drop(&mut self) {
        #[cfg(feature = "test-utils")]
        PHASE.with(|p| p.set(self.prior));
    }
}
pub(super) async fn phase<F: Future>(name: &'static str, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        let _guard = PhaseGuard::enter(name);
        future.as_mut().poll(cx)
    })
    .await
}
#[cfg(feature = "test-utils")]
pub(super) fn record(slot: usize, count: usize) {
    WORK.with(|work| {
        if let Some(value) = work
            .borrow_mut()
            .entry(current())
            .or_insert([0; 36])
            .get_mut(slot)
        {
            *value += count as u64;
        }
    });
}
#[cfg(feature = "test-utils")]
pub(super) fn current() -> &'static str {
    PHASE.with(Cell::get)
}
#[cfg(feature = "test-utils")]
pub(super) fn take() -> BTreeMap<&'static str, [u64; 36]> {
    WORK.with(|work| std::mem::take(&mut *work.borrow_mut()))
}

/// Direct SHA work in retained-root codecs, separate from control-store helpers.
#[cfg(feature = "test-utils")]
pub fn record_retention_hash(bytes: usize) {
    record(17, 1);
    record(18, bytes);
}

/// Subdivide shared reads only while selecting a maintenance unit. Other callers
/// retain their enclosing operation/reconstruction phase.
pub(super) async fn selection_read<F: Future>(name: &'static str, future: F) -> F::Output {
    #[cfg(feature = "test-utils")]
    if current() == "maintenance-selection" {
        return phase(name, future).await;
    }
    let _ = name;
    future.await
}

/// Synchronous allocation boundaries are nested in the existing poll measurement.
/// The measured classes are disjoint; their sum is a subset of total allocations.
#[inline]
#[allow(clippy::expect_used)] // The synchronous closure is invoked exactly once.
pub(super) fn allocated<T>(slot: usize, operation: impl FnOnce() -> T) -> T {
    #[cfg(feature = "test-utils")]
    {
        let mut result = None;
        let info = allocation_counter::measure(|| result = Some(operation()));
        record(
            slot,
            usize::try_from(info.count_total).unwrap_or(usize::MAX),
        );
        record(
            slot + 1,
            usize::try_from(info.bytes_total).unwrap_or(usize::MAX),
        );
        result.expect("allocation measurement invokes its closure")
    }
    #[cfg(not(feature = "test-utils"))]
    {
        let _ = slot;
        operation()
    }
}

#[inline]
pub(super) fn now() -> chrono::DateTime<chrono::Utc> {
    #[cfg(feature = "test-utils")]
    {
        arco_core::test_inputs::now()
    }
    #[cfg(not(feature = "test-utils"))]
    {
        chrono::Utc::now()
    }
}

#[cfg(all(test, feature = "test-utils"))]
#[allow(clippy::indexing_slicing)] // Fixed key created by the assertion's preceding record.
mod tests {
    use super::{BoundedWork, bounded_work, take, take_bounded_work};

    #[test]
    fn bounded_work_is_reported_without_changing_historical_phase_slots() {
        take();
        take_bounded_work();
        bounded_work(BoundedWork {
            decoded_rows: 2,
            transition_proof_rows: 3,
            selected_blocks: 4,
            rewritten_blocks: 5,
            directory_references: 6,
            streaming_builder_inputs: 7,
        });
        bounded_work(BoundedWork {
            decoded_rows: 20,
            transition_proof_rows: 30,
            selected_blocks: 40,
            rewritten_blocks: 50,
            directory_references: 60,
            streaming_builder_inputs: 70,
        });

        assert!(take().is_empty());
        assert_eq!(
            take_bounded_work()["request"],
            BoundedWork {
                decoded_rows: 22,
                transition_proof_rows: 33,
                selected_blocks: 44,
                rewritten_blocks: 55,
                directory_references: 66,
                streaming_builder_inputs: 77,
            }
        );
    }
}
#[inline]
pub(super) fn nonce() -> ulid::Ulid {
    #[cfg(feature = "test-utils")]
    {
        arco_core::test_inputs::nonce()
    }
    #[cfg(not(feature = "test-utils"))]
    {
        ulid::Ulid::new()
    }
}

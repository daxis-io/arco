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
}
pub(super) struct PhaseGuard {
    #[cfg(feature = "test-utils")]
    prior: &'static str,
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

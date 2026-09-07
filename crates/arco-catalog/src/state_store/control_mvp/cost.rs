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
    static WORK: RefCell<BTreeMap<&'static str,[u64;15]>> = const { RefCell::new(BTreeMap::new()) };
}
pub(super) struct PhaseGuard {
    #[cfg(feature = "test-utils")]
    prior: &'static str,
}
impl PhaseGuard {
    pub(super) fn enter(phase: &'static str) -> Self {
        let _ = phase;
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
            .or_default()
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
pub(super) fn take() -> BTreeMap<&'static str, [u64; 15]> {
    WORK.with(|work| std::mem::take(&mut *work.borrow_mut()))
}

//! Opt-in, thread-local observation of projection stages for local measurements.
//! Poll allocation counts exclude suspension; elapsed time includes suspension.
//! No measurement is active unless a test driver calls `start` on its thread.
#[cfg(feature = "test-utils")]
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
};

#[cfg(feature = "test-utils")]
thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static CURRENT: Cell<Option<&'static str>> = const { Cell::new(None) };
    static SAMPLES: RefCell<BTreeMap<&'static str, Sample>> = const { RefCell::new(BTreeMap::new()) };
}

/// One aggregated stage, disjoint from the other named stages.
#[cfg(feature = "test-utils")]
#[derive(Default, serde::Serialize)]
pub struct Sample {
    /// Number of stage invocations.
    pub calls: u64,
    /// Elapsed wall time, including waits within this stage.
    pub elapsed_nanos: u128,
    /// Allocations during future polls.
    pub allocation_calls: u64,
    /// Cumulative allocated bytes during future polls.
    pub allocation_bytes: u64,
}

/// Enables observation on the calling thread; use a current-thread test runtime.
#[cfg(feature = "test-utils")]
pub fn start() {
    SAMPLES.with(|samples| samples.borrow_mut().clear());
    ACTIVE.with(|active| active.set(true));
}

/// Ends observation and returns the recorded stages.
#[cfg(feature = "test-utils")]
#[must_use]
pub fn finish() -> BTreeMap<&'static str, Sample> {
    ACTIVE.with(|active| active.set(false));
    SAMPLES.with(|samples| std::mem::take(&mut *samples.borrow_mut()))
}

/// Stage of the currently executing poll, for storage boundary counters.
#[cfg(feature = "test-utils")]
pub fn current() -> Option<&'static str> {
    CURRENT.with(Cell::get)
}

/// Observes one non-overlapping stage only while local measurement is enabled.
#[doc(hidden)]
pub async fn phase<F: Future>(name: &'static str, future: F) -> F::Output {
    #[cfg(feature = "test-utils")]
    if ACTIVE.with(Cell::get) {
        struct Restore(Option<&'static str>);
        impl Drop for Restore {
            fn drop(&mut self) {
                CURRENT.with(|current| current.set(self.0));
            }
        }
        let started = std::time::Instant::now();
        let mut sample = Sample {
            calls: 1,
            ..Sample::default()
        };
        let mut future = std::pin::pin!(future);
        let output = std::future::poll_fn(|cx| {
            let _restore = Restore(CURRENT.with(|current| current.replace(Some(name))));
            let mut result = std::task::Poll::Pending;
            let info = allocation_counter::measure(|| result = future.as_mut().poll(cx));
            sample.allocation_calls += info.count_total;
            sample.allocation_bytes += info.bytes_total;
            result
        })
        .await;
        sample.elapsed_nanos = started.elapsed().as_nanos();
        SAMPLES.with(|samples| {
            let mut samples = samples.borrow_mut();
            let total = samples.entry(name).or_default();
            total.calls += sample.calls;
            total.elapsed_nanos += sample.elapsed_nanos;
            total.allocation_calls += sample.allocation_calls;
            total.allocation_bytes += sample.allocation_bytes;
        });
        return output;
    }
    let _ = name;
    future.await
}

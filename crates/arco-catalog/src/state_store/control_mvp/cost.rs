//! Optional local counters. Phases are scoped to future polls, never across
//! suspension or task migration. Nested counters partition, not add to, totals.
#[cfg(any(test, feature = "test-utils"))]
use std::cell::RefCell;
#[cfg(feature = "test-utils")]
use std::{cell::Cell, collections::BTreeMap};
#[cfg(feature = "test-utils")]
thread_local! {
    static PHASE: Cell<&'static str> = const { Cell::new("request") };
    static WORK: RefCell<BTreeMap<&'static str,[u64;36]>> = const { RefCell::new(BTreeMap::new()) };
    static BOUNDED_WORK: RefCell<BTreeMap<&'static str, BoundedWork>> = const { RefCell::new(BTreeMap::new()) };
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct NativeWork {
    pub(super) slots: [u64; 36],
    pub(super) bounded: BoundedWork,
    pub(super) overflow: bool,
}

impl Default for NativeWork {
    fn default() -> Self {
        Self {
            slots: [0; 36],
            bounded: BoundedWork::default(),
            overflow: false,
        }
    }
}

// Fixed synchronous capture; the guard cannot migrate to another thread.
#[cfg(any(test, feature = "test-utils"))]
thread_local! {
    static NATIVE_WORK: RefCell<Option<NativeWork>> = const { RefCell::new(None) };
}

#[cfg(any(test, feature = "test-utils"))]
pub(super) struct NativeCapture {
    prior: Option<NativeWork>,
    finished: bool,
    thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl NativeCapture {
    pub(super) fn begin() -> Self {
        Self {
            prior: NATIVE_WORK.with(|work| work.replace(Some(NativeWork::default()))),
            finished: false,
            thread: std::marker::PhantomData,
        }
    }

    pub(super) fn finish(mut self) -> NativeWork {
        let work = NATIVE_WORK.with(|work| work.replace(self.prior.take()));
        self.finished = true;
        work.unwrap_or_else(|| NativeWork {
            overflow: true,
            ..Default::default()
        })
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Drop for NativeCapture {
    fn drop(&mut self) {
        if !self.finished {
            NATIVE_WORK.with(|work| work.replace(self.prior.take()));
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub(super) fn native_capture_active() -> bool {
    NATIVE_WORK.with(|work| work.borrow().is_some())
}

#[cfg(any(test, feature = "test-utils"))]
fn capture(update: impl FnOnce(&mut NativeWork)) -> bool {
    NATIVE_WORK.with(|work| {
        work.borrow_mut().as_mut().is_some_and(|work| {
            update(work);
            true
        })
    })
}

#[cfg(any(test, feature = "test-utils"))]
fn add(total: &mut u64, count: u64, overflow: &mut bool) {
    let next = total.checked_add(count);
    *overflow |= next.is_none();
    *total = next.unwrap_or(u64::MAX);
}

#[cfg(any(test, feature = "test-utils"))]
impl NativeWork {
    pub(super) fn merge(&mut self, work: Self) {
        self.overflow |= work.overflow;
        for (total, count) in self.slots.iter_mut().zip(work.slots) {
            add(total, count, &mut self.overflow);
        }
        self.add_bounded(work.bounded);
    }

    fn add_bounded(&mut self, work: BoundedWork) {
        for (total, count) in [
            (&mut self.bounded.decoded_rows, work.decoded_rows),
            (
                &mut self.bounded.transition_proof_rows,
                work.transition_proof_rows,
            ),
            (&mut self.bounded.selected_blocks, work.selected_blocks),
            (&mut self.bounded.rewritten_blocks, work.rewritten_blocks),
            (
                &mut self.bounded.directory_references,
                work.directory_references,
            ),
            (
                &mut self.bounded.streaming_builder_inputs,
                work.streaming_builder_inputs,
            ),
        ] {
            add(total, count, &mut self.overflow);
        }
    }
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
    #[cfg(any(test, feature = "test-utils"))]
    if capture(|total| total.add_bounded(work)) {
        return;
    }
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
        if !native_capture_active() {
            WORK.with(|work| {
                work.borrow_mut().entry(phase).or_insert([0; 36]);
            });
        }
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
#[cfg(any(test, feature = "test-utils"))]
#[cfg_attr(
    not(feature = "test-utils"),
    allow(
        clippy::needless_return,
        reason = "the feature-only legacy fallback requires the early return after capture"
    )
)]
pub(super) fn record(slot: usize, count: usize) {
    if capture(|work| {
        if let (Some(total), Ok(count)) = (work.slots.get_mut(slot), u64::try_from(count)) {
            add(total, count, &mut work.overflow);
        } else {
            work.overflow = true;
        }
    }) {
        return;
    }
    #[cfg(feature = "test-utils")]
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
    #[cfg(any(test, feature = "test-utils"))]
    if cfg!(feature = "test-utils") || native_capture_active() {
        let mut result = None;
        // Catch inside measure so its fixed stack is popped before rethrowing.
        // The outer native boundary converts the panic after counters are kept.
        let info = allocation_counter::measure(|| {
            result = Some(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                operation,
            )));
        });
        record(
            slot,
            usize::try_from(info.count_total).unwrap_or(usize::MAX),
        );
        record(
            slot.checked_add(1).unwrap_or(usize::MAX),
            usize::try_from(info.bytes_total).unwrap_or(usize::MAX),
        );
        return match result.expect("allocation measurement invokes its closure") {
            Ok(value) => value,
            Err(panic) => std::panic::resume_unwind(panic),
        };
    }
    let _ = slot;
    operation()
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

#[cfg(test)]
#[allow(clippy::indexing_slicing)] // Fixed counter arrays and exact test slots.
mod native_tests {
    use super::*;

    fn bounded(n: u64) -> BoundedWork {
        BoundedWork {
            decoded_rows: n,
            transition_proof_rows: n + 1,
            selected_blocks: n + 2,
            rewritten_blocks: n + 3,
            directory_references: n + 4,
            streaming_builder_inputs: n + 5,
        }
    }

    #[test]
    fn native_counter_capture_fixed_layout_and_zero_heap() {
        let mut result = NativeWork::default();
        let allocations = allocation_counter::measure(|| {
            let outer = NativeCapture::begin();
            for slot in 0..36 {
                record(slot, slot + 1);
            }
            bounded_work(bounded(2));
            let child = NativeCapture::begin();
            record(0, 900);
            bounded_work(bounded(900));
            let phase = PhaseGuard::enter("native-capture-child");
            assert_eq!(child.finish().slots[0], 900);
            drop(phase);
            result = outer.finish();
        });
        assert_eq!(allocations.count_total, 0);
        assert_eq!(allocations.bytes_total, 0);
        assert_eq!(result.slots, std::array::from_fn(|i| i as u64 + 1));
        assert_eq!(result.bounded, bounded(2));
        assert!(!result.overflow);
        let mut total = result;
        total.merge(result);
        assert_eq!(total.slots, std::array::from_fn(|i| (i as u64 + 1) * 2));
        assert_eq!(
            total.bounded,
            BoundedWork {
                decoded_rows: 4,
                transition_proof_rows: 6,
                selected_blocks: 8,
                rewritten_blocks: 10,
                directory_references: 12,
                streaming_builder_inputs: 14
            }
        );
        assert!(!total.overflow);
        assert!(!native_capture_active());
        for bytes in [
            size_of::<NativeWork>(),
            size_of::<Option<NativeWork>>(),
            size_of::<NativeCapture>(),
        ] {
            assert!(bytes <= 1024, "fixed counter storage {bytes}");
        }
        println!(
            "native-capture-layout work={} option={} guard={}",
            size_of::<NativeWork>(),
            size_of::<Option<NativeWork>>(),
            size_of::<NativeCapture>()
        );
    }

    #[test]
    fn native_counter_capture_checked_slots_fields_and_merge() {
        for slot in 0..36 {
            let capture = NativeCapture::begin();
            record(slot, usize::MAX);
            record(slot, 1);
            let work = capture.finish();
            assert!(work.overflow);
            assert_eq!(work.slots[slot], u64::MAX);
            let mut total = NativeWork::default();
            total.slots[slot] = u64::MAX;
            let mut one = NativeWork::default();
            one.slots[slot] = 1;
            total.merge(one);
            assert!(total.overflow);
        }
        let capture = NativeCapture::begin();
        record(36, 0);
        assert!(capture.finish().overflow);
        let max = BoundedWork {
            decoded_rows: u64::MAX,
            transition_proof_rows: u64::MAX,
            selected_blocks: u64::MAX,
            rewritten_blocks: u64::MAX,
            directory_references: u64::MAX,
            streaming_builder_inputs: u64::MAX,
        };
        let capture = NativeCapture::begin();
        bounded_work(max);
        bounded_work(bounded(1));
        let work = capture.finish();
        assert_eq!(work.bounded, max);
        assert!(work.overflow);
        let mut aggregate = NativeWork {
            bounded: max,
            ..Default::default()
        };
        aggregate.merge(NativeWork {
            bounded: bounded(1),
            ..Default::default()
        });
        assert!(aggregate.overflow);
        assert_eq!(aggregate.bounded, max);
    }

    #[test]
    fn native_counter_capture_drop_unwind_and_thread_isolation() {
        let parent = NativeCapture::begin();
        record(0, 3);
        drop(NativeCapture::begin());
        let panic = std::panic::catch_unwind(|| {
            let _child = NativeCapture::begin();
            record(0, 99);
            panic!("capture unwind");
        });
        assert!(panic.is_err());
        let worker = std::thread::spawn(|| {
            assert!(!native_capture_active());
            let capture = NativeCapture::begin();
            record(0, 7);
            capture.finish()
        })
        .join()
        .expect("thread");
        assert_eq!(worker.slots[0], 7);
        assert_eq!(parent.finish().slots[0], 3);
        assert!(!native_capture_active());
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn native_counter_capture_preserves_legacy_phase_maps_and_cells() {
        take();
        take_bounded_work();
        let prior = PhaseGuard::enter("native-test-legacy");
        super::super::record_sha256_work(5);
        bounded_work(bounded(1));
        let cells = super::super::TEST_SHA256_WORK.with(Cell::get);
        let capture = NativeCapture::begin();
        {
            let _nested = PhaseGuard::enter("native-test-captured");
            super::super::record_sha256_work(11);
            bounded_work(bounded(11));
        }
        assert_eq!(current(), "native-test-legacy");
        let native = capture.finish();
        assert_eq!(native.slots[0..2], [1, 11]);
        assert_eq!(native.bounded, bounded(11));
        assert_eq!(super::super::TEST_SHA256_WORK.with(Cell::get), cells);
        let maps = take();
        assert_eq!(maps.len(), 1);
        assert_eq!(maps["native-test-legacy"][0..2], [1, 5]);
        let maps = take_bounded_work();
        assert_eq!(maps.len(), 1);
        assert_eq!(maps["native-test-legacy"], bounded(1));
        drop(prior);
    }
}

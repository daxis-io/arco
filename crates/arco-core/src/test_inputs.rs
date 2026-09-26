//! Opt-in deterministic clock and identifiers for local measurement fixtures.
//! The guard is thread-bound; unguarded code uses the real clock and entropy.
use chrono::{DateTime, Utc};
use std::{cell::Cell, marker::PhantomData, rc::Rc};
use ulid::Ulid;

thread_local! {
    static INPUTS: Cell<Option<(DateTime<Utc>, u128)>> = const { Cell::new(None) };
    /// The highest serial any `FixedInputs::at` scope on this thread reached,
    /// so successive scopes keep issuing distinct identifiers.
    static SERIAL_FLOOR: Cell<u128> = const { Cell::new(0) };
}

/// Restores the previous fixture inputs when dropped on its originating thread.
pub struct FixedInputs {
    prior: Option<(DateTime<Utc>, u128)>,
    /// `at` scopes carry their identifier serial forward on drop; `scoped`
    /// restores it so nested repeatable scopes replay the same identifiers.
    keep_serial: bool,
    thread_bound: PhantomData<Rc<()>>,
}
impl FixedInputs {
    /// Starts a repeatable fixture at 2030-01-01 UTC with sequential ULIDs.
    #[must_use]
    #[allow(clippy::expect_used)] // This constant is a representable UTC instant.
    pub fn scoped() -> Self {
        let now = DateTime::from_timestamp(1_893_456_000, 0).expect("fixed fixture timestamp");
        Self {
            prior: INPUTS.with(|state| state.replace(Some((now, 0)))),
            keep_serial: false,
            thread_bound: PhantomData,
        }
    }

    /// Pins the fixture clock to `now` while keeping identifiers unique:
    /// the identifier serial continues from wherever this thread's previous
    /// `at` scope (or an enclosing scope) left it, and is carried forward on
    /// drop. Use successive `at` scopes to simulate time passing.
    #[must_use]
    pub fn at(now: DateTime<Utc>) -> Self {
        let serial = INPUTS
            .with(|state| state.get().map_or(0, |(_, serial)| serial))
            .max(SERIAL_FLOOR.with(Cell::get));
        Self {
            prior: INPUTS.with(|state| state.replace(Some((now, serial)))),
            keep_serial: true,
            thread_bound: PhantomData,
        }
    }
}
impl Drop for FixedInputs {
    fn drop(&mut self) {
        INPUTS.with(|state| {
            if self.keep_serial {
                let serial = state.get().map_or(0, |(_, serial)| serial);
                SERIAL_FLOOR.with(|floor| floor.set(floor.get().max(serial)));
                state.set(self.prior.map(|(now, prior)| (now, prior.max(serial))));
            } else {
                state.set(self.prior);
            }
        });
    }
}

/// Returns the fixture instant, or the real clock outside a fixture scope.
#[must_use]
pub fn now() -> DateTime<Utc> {
    INPUTS.with(|state| state.get().map_or_else(Utc::now, |(now, _)| now))
}

/// Returns a unique fixture ULID, or real entropy outside a fixture scope.
#[must_use]
#[allow(clippy::expect_used)] // A fixture cannot allocate 2^128 identifiers.
pub fn nonce() -> Ulid {
    INPUTS.with(|state| {
        let Some((now, serial)) = state.get() else {
            return Ulid::new();
        };
        let next = serial.checked_add(1).expect("fixture nonce exhaustion");
        state.set(Some((now, next)));
        Ulid::from_parts(1_893_456_000_000, next)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_inputs_are_repeatable_and_restore_nested_scopes() {
        let first;
        let second;
        {
            let _outer = FixedInputs::scoped();
            first = nonce();
            {
                let _inner = FixedInputs::scoped();
                assert_eq!(nonce(), first);
            }
            second = nonce();
            assert_ne!(first, second);
            assert_eq!(now().timestamp(), 1_893_456_000);
        }
        assert!(INPUTS.with(|state| state.get().is_none()));
        let _repeat = FixedInputs::scoped();
        assert_eq!(nonce(), first);
        assert_eq!(nonce(), second);
    }

    #[test]
    fn at_scopes_move_the_clock_and_never_repeat_identifiers() {
        let t1 = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let t2 = t1 + chrono::Duration::hours(2);
        let mut issued = std::collections::BTreeSet::new();
        {
            let _first = FixedInputs::at(t1);
            assert_eq!(now(), t1);
            assert!(issued.insert(nonce()));
            assert!(issued.insert(nonce()));
        }
        assert!(INPUTS.with(|state| state.get().is_none()));
        {
            let _second = FixedInputs::at(t2);
            assert_eq!(now(), t2);
            assert!(issued.insert(nonce()), "a later scope continues the serial");
            {
                let _nested = FixedInputs::at(t1);
                assert_eq!(now(), t1);
                assert!(issued.insert(nonce()));
            }
            assert_eq!(now(), t2, "dropping a nested scope restores the clock");
            assert!(issued.insert(nonce()), "and keeps the serial moving");
        }
        assert_eq!(issued.len(), 5);
    }
}

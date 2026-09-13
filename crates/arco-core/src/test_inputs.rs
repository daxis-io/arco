//! Opt-in deterministic clock and identifiers for local measurement fixtures.
//! The guard is thread-bound; unguarded code uses the real clock and entropy.
use chrono::{DateTime, Utc};
use std::{cell::Cell, marker::PhantomData, rc::Rc};
use ulid::Ulid;

thread_local! {
    static INPUTS: Cell<Option<(DateTime<Utc>, u128)>> = const { Cell::new(None) };
}

/// Restores the previous fixture inputs when dropped on its originating thread.
pub struct FixedInputs {
    prior: Option<(DateTime<Utc>, u128)>,
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
            thread_bound: PhantomData,
        }
    }
}
impl Drop for FixedInputs {
    fn drop(&mut self) {
        INPUTS.with(|state| state.set(self.prior));
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
}

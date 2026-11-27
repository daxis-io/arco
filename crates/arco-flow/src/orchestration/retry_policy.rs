//! Pinned Rivers retry semantics for accepted Flow worker-pull plans.

use std::time::Duration;

use rivers_retry::{Backoff, FailureReason, RetryOn, RetryPolicy};

/// Server-owned run label for the retry policy in the accepted plan.
pub const RETRY_POLICY_LABEL: &str = "arco.retry_policy";
/// Pinned Rivers implementation with Arco's attempt and failure mapping.
pub const RIVERS_RETRY_PROFILE: &str = "rivers-d7cbc206-compat-v1";

pub(super) fn retry_delay(
    max_attempts: u32,
    attempt: u32,
    retryable: bool,
) -> Option<chrono::Duration> {
    let policy = RetryPolicy {
        // Arco includes the first attempt. Rivers counts additional attempts.
        max_retries: max_attempts.checked_sub(1)?,
        backoff: Some(Backoff::exponential(
            Duration::from_secs(30),
            2.0,
            0.0,
            Some(Duration::from_secs(900)),
        )),
        retry_on: if retryable {
            RetryOn::All
        } else {
            RetryOn::Match {
                exceptions: vec![],
                reasons: vec![],
            }
        },
        escalate: None,
    };
    if attempt == 0 || !rivers_retry::should_retry(&policy, FailureReason::Error, &[], attempt) {
        return None;
    }
    // The cap is reached at seven, which also avoids wrapping the upstream exponent.
    chrono::Duration::from_std(rivers_retry::compute_delay(&policy, attempt.min(7), 0.5)).ok()
}

#[cfg(test)]
mod tests {
    use super::retry_delay;

    #[test]
    fn accepted_attempt_budget_and_delay_match_the_arco_contract() {
        for (budget, attempt, retryable, seconds) in [
            (1, 1, true, None),
            (2, 1, true, Some(30)),
            (2, 2, true, None),
            (3, 1, false, None),
            (3, 2, true, Some(60)),
            (10, 7, true, Some(900)),
            (u32::MAX, u32::MAX - 1, true, Some(900)),
            (u32::MAX, u32::MAX, true, None),
            (0, 1, true, None),
            (3, 0, true, None),
        ] {
            assert_eq!(
                retry_delay(budget, attempt, retryable).map(|delay| delay.num_seconds()),
                seconds
            );
        }
    }
}

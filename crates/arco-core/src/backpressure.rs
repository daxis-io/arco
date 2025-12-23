//! Backpressure system with two-stage soft/hard thresholds (Gate 5).
//!
//! This module implements position-based backpressure that does NOT require listing.
//! Lag is computed from monotonic position watermarks.
//!
//! # Architecture (Patch 10)
//!
//! ```text
//! Ingestion                           Compaction
//!     │                                    │
//!     ├── Write event                      │
//!     ├── Increment last_written_position  │
//!     │                                    │
//!     │                         ┌──────────┤
//!     │                         │          │
//!     │                    Compact events  │
//!     │                         │          │
//!     │                    Update manifest │
//!     │                         │          │
//!     │               last_compacted_position
//!     │                         │          │
//!     ▼                         ▼          │
//!  ┌─────────────────────────────────┐     │
//!  │  pending_lag = written - compacted   │
//!  └─────────────────────────────────┘     │
//! ```
//!
//! # Two-Stage Thresholds
//!
//! | Stage | Lag Range | Behavior |
//! |-------|-----------|----------|
//! | Normal | `lag < soft` | Accept all requests |
//! | Soft | `soft <= lag < hard` | Accept with warning, prioritize compaction |
//! | Hard | `lag >= hard` | Reject with `retry_after` |
//!
//! # No Listing Required
//!
//! Positions come from:
//! - `last_written_position`: Incremented on each ledger append (in-memory or Servo metadata)
//! - `last_compacted_position`: Read from manifest `position_watermark` field
//!
//! This avoids expensive listing operations on the critical path.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Backpressure state computed from watermark positions.
///
/// This struct is computed per-domain and used to evaluate whether
/// to accept, warn, or reject incoming requests.
///
/// # Example
///
/// ```rust
/// use arco_core::backpressure::BackpressureState;
/// use std::time::Duration;
///
/// let state = BackpressureState::new("catalog")
///     .with_positions(1000, 800)
///     .with_thresholds(100, 500);
///
/// // Lag is 200 (1000 - 800), which is between soft (100) and hard (500)
/// match state.evaluate() {
///     arco_core::backpressure::BackpressureDecision::AcceptWithWarning { pending_lag } => {
///         // Log warning, prioritize compaction
///         assert_eq!(pending_lag, 200);
///     }
///     _ => panic!("expected warning"),
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackpressureState {
    /// Domain identifier (e.g., "catalog", "lineage").
    pub domain: String,

    /// Last written position (from Servo/ingestion).
    ///
    /// This is incremented on each ledger append and tracked
    /// in-memory or persisted in Servo metadata.
    pub last_written_position: u64,

    /// Last compacted position (from manifest watermark).
    ///
    /// This is read from the domain manifest's `position_watermark` field.
    pub last_compacted_position: u64,

    /// Soft threshold (position lag).
    ///
    /// When lag exceeds this, requests are accepted with warnings.
    pub soft_threshold: u64,

    /// Hard threshold (position lag).
    ///
    /// When lag exceeds this, requests are rejected with `retry_after`.
    pub hard_threshold: u64,
}

impl BackpressureState {
    /// Creates a new backpressure state for a domain.
    #[must_use]
    pub fn new(domain: impl Into<String>) -> Self {
        Self {
            domain: domain.into(),
            last_written_position: 0,
            last_compacted_position: 0,
            soft_threshold: 1000,  // Default: 1000 events
            hard_threshold: 10000, // Default: 10000 events
        }
    }

    /// Sets the position watermarks.
    #[must_use]
    pub fn with_positions(mut self, written: u64, compacted: u64) -> Self {
        self.last_written_position = written;
        self.last_compacted_position = compacted;
        self
    }

    /// Sets the soft/hard thresholds.
    #[must_use]
    pub fn with_thresholds(mut self, soft: u64, hard: u64) -> Self {
        self.soft_threshold = soft;
        self.hard_threshold = hard;
        self
    }

    /// Computes the position lag (no listing required).
    ///
    /// Returns the number of positions behind compaction.
    #[must_use]
    pub fn pending_lag(&self) -> u64 {
        self.last_written_position
            .saturating_sub(self.last_compacted_position)
    }

    /// Evaluates the backpressure decision based on current lag.
    ///
    /// Returns:
    /// - `Accept`: Normal operation, lag is below soft threshold
    /// - `AcceptWithWarning`: Lag is between soft and hard thresholds
    /// - `Reject`: Lag exceeds hard threshold, return `retry_after`
    #[must_use]
    pub fn evaluate(&self) -> BackpressureDecision {
        let lag = self.pending_lag();

        if lag >= self.hard_threshold {
            BackpressureDecision::Reject {
                retry_after: Self::compute_retry_after(lag, self.hard_threshold),
            }
        } else if lag >= self.soft_threshold {
            BackpressureDecision::AcceptWithWarning { pending_lag: lag }
        } else {
            BackpressureDecision::Accept
        }
    }

    /// Computes a reasonable `retry_after` duration based on lag severity.
    ///
    /// The idea is: the more behind we are, the longer the client should wait.
    fn compute_retry_after(lag: u64, hard_threshold: u64) -> Duration {
        // Base delay is 5 seconds
        let base_secs = 5;

        // Scale up based on how far over the hard threshold we are
        let overage_ratio = if hard_threshold > 0 {
            lag.saturating_sub(hard_threshold) / hard_threshold
        } else {
            0
        };

        // Cap at 60 seconds
        let total_secs = (base_secs + overage_ratio).min(60);
        Duration::from_secs(total_secs)
    }

    /// Updates the written position after an append.
    pub fn record_write(&mut self) {
        self.last_written_position += 1;
    }

    /// Updates the compacted position after compaction.
    pub fn update_compacted_position(&mut self, position: u64) {
        self.last_compacted_position = position;
    }
}

/// Backpressure decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackpressureDecision {
    /// Accept the request (normal operation).
    Accept,

    /// Accept the request with a warning (soft threshold exceeded).
    ///
    /// The system should:
    /// - Log the warning
    /// - Prioritize compaction for this domain
    /// - Consider alerting operators
    AcceptWithWarning {
        /// Current position lag.
        pending_lag: u64,
    },

    /// Reject the request with `retry_after` (hard threshold exceeded).
    ///
    /// The client should retry after the specified duration.
    Reject {
        /// How long the client should wait before retrying.
        retry_after: Duration,
    },
}

impl BackpressureDecision {
    /// Returns true if the request should be rejected.
    #[must_use]
    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::Reject { .. })
    }

    /// Returns the retry-after duration if rejected.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Reject { retry_after } => Some(*retry_after),
            _ => None,
        }
    }
}

/// Error returned when backpressure rejects a request.
#[derive(Debug, Clone)]
pub struct BackpressureError {
    /// Domain that is overloaded.
    pub domain: String,

    /// How long the client should wait before retrying.
    pub retry_after: Duration,

    /// Current position lag.
    pub lag: u64,

    /// Hard threshold that was exceeded.
    pub threshold: u64,
}

impl std::fmt::Display for BackpressureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "backpressure: domain '{}' lag ({}) exceeds threshold ({}), retry after {:?}",
            self.domain, self.lag, self.threshold, self.retry_after
        )
    }
}

impl std::error::Error for BackpressureError {}

/// Backpressure registry for multiple domains.
///
/// Tracks backpressure state across all domains and provides
/// a unified interface for checking/updating.
#[derive(Debug, Default)]
pub struct BackpressureRegistry {
    /// Per-domain backpressure state.
    domains: std::collections::HashMap<String, BackpressureState>,

    /// Default thresholds for new domains.
    default_soft_threshold: u64,
    default_hard_threshold: u64,
}

impl BackpressureRegistry {
    /// Creates a new registry with default thresholds.
    #[must_use]
    pub fn new(soft_threshold: u64, hard_threshold: u64) -> Self {
        Self {
            domains: std::collections::HashMap::new(),
            default_soft_threshold: soft_threshold,
            default_hard_threshold: hard_threshold,
        }
    }

    /// Gets or creates the backpressure state for a domain.
    pub fn get_or_create(&mut self, domain: &str) -> &mut BackpressureState {
        self.domains.entry(domain.to_string()).or_insert_with(|| {
            BackpressureState::new(domain)
                .with_thresholds(self.default_soft_threshold, self.default_hard_threshold)
        })
    }

    /// Evaluates backpressure for a domain.
    ///
    /// Returns `Ok(())` if the request should proceed, or `Err` if rejected.
    ///
    /// # Errors
    ///
    /// Returns an error if the domain is over the hard threshold.
    pub fn check(&mut self, domain: &str) -> Result<BackpressureDecision, BackpressureError> {
        let state = self.get_or_create(domain);
        let decision = state.evaluate();

        match &decision {
            BackpressureDecision::Reject { retry_after } => Err(BackpressureError {
                domain: domain.to_string(),
                retry_after: *retry_after,
                lag: state.pending_lag(),
                threshold: state.hard_threshold,
            }),
            _ => Ok(decision),
        }
    }

    /// Records a write to a domain.
    pub fn record_write(&mut self, domain: &str) {
        self.get_or_create(domain).record_write();
    }

    /// Updates the compacted position for a domain.
    pub fn update_compacted(&mut self, domain: &str, position: u64) {
        self.get_or_create(domain)
            .update_compacted_position(position);
    }

    /// Returns all domains with their current lag.
    #[must_use]
    pub fn all_lags(&self) -> Vec<(&str, u64)> {
        self.domains
            .iter()
            .map(|(domain, state)| (domain.as_str(), state.pending_lag()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_lag_calculation() {
        let state = BackpressureState::new("catalog").with_positions(1000, 800);
        assert_eq!(state.pending_lag(), 200);
    }

    #[test]
    fn test_pending_lag_no_underflow() {
        // Compacted ahead of written (shouldn't happen but be safe)
        let state = BackpressureState::new("catalog").with_positions(100, 200);
        assert_eq!(state.pending_lag(), 0);
    }

    #[test]
    fn test_accept_below_soft() {
        let state = BackpressureState::new("catalog")
            .with_positions(100, 50)
            .with_thresholds(100, 1000);

        // Lag is 50, soft is 100 -> Accept
        assert_eq!(state.evaluate(), BackpressureDecision::Accept);
    }

    #[test]
    fn test_accept_with_warning() {
        let state = BackpressureState::new("catalog")
            .with_positions(1000, 800)
            .with_thresholds(100, 500);

        // Lag is 200, between soft (100) and hard (500) -> Warning
        match state.evaluate() {
            BackpressureDecision::AcceptWithWarning { pending_lag } => {
                assert_eq!(pending_lag, 200);
            }
            other => panic!("expected warning, got {other:?}"),
        }
    }

    #[test]
    fn test_reject_above_hard() {
        let state = BackpressureState::new("catalog")
            .with_positions(10000, 0)
            .with_thresholds(100, 500);

        // Lag is 10000, above hard (500) -> Reject
        match state.evaluate() {
            BackpressureDecision::Reject { retry_after } => {
                assert!(retry_after >= Duration::from_secs(5));
            }
            other => panic!("expected reject, got {other:?}"),
        }
    }

    #[test]
    fn test_registry_tracks_domains() {
        let mut registry = BackpressureRegistry::new(100, 500);

        registry.record_write("catalog");
        registry.record_write("catalog");
        registry.record_write("lineage");

        assert_eq!(registry.get_or_create("catalog").last_written_position, 2);
        assert_eq!(registry.get_or_create("lineage").last_written_position, 1);
    }

    #[test]
    fn test_registry_check_returns_decision() {
        let mut registry = BackpressureRegistry::new(100, 500);

        // Within limits
        for _ in 0..50 {
            registry.record_write("catalog");
        }
        assert!(registry.check("catalog").is_ok());

        // Exceeds soft
        for _ in 0..100 {
            registry.record_write("catalog");
        }
        let decision = registry.check("catalog").expect("should accept");
        assert!(matches!(
            decision,
            BackpressureDecision::AcceptWithWarning { .. }
        ));
    }

    #[test]
    fn test_registry_check_rejects() {
        let mut registry = BackpressureRegistry::new(100, 500);

        for _ in 0..600 {
            registry.record_write("catalog");
        }

        let err = registry.check("catalog").expect_err("should reject");
        assert_eq!(err.domain, "catalog");
        assert!(err.retry_after >= Duration::from_secs(5));
    }

    #[test]
    fn test_no_listing_required() {
        // This test documents the critical invariant:
        // BackpressureState computes lag from position watermarks,
        // NOT from listing ledger objects.
        //
        // Positions come from:
        // - last_written_position: incremented on append
        // - last_compacted_position: from manifest watermark
        //
        // No ListStore trait, no list() calls.

        let state = BackpressureState::new("catalog")
            .with_positions(1000, 900)
            .with_thresholds(50, 200);

        // Lag computed without listing
        assert_eq!(state.pending_lag(), 100);

        // Evaluation without listing
        let decision = state.evaluate();
        assert!(matches!(
            decision,
            BackpressureDecision::AcceptWithWarning { .. }
        ));
    }

    #[test]
    fn test_retry_after_scales_with_lag() {
        // More lag = longer retry
        let state_low = BackpressureState::new("a")
            .with_positions(600, 0)
            .with_thresholds(100, 500);

        let state_high = BackpressureState::new("b")
            .with_positions(50000, 0)
            .with_thresholds(100, 500);

        match (state_low.evaluate(), state_high.evaluate()) {
            (
                BackpressureDecision::Reject { retry_after: low },
                BackpressureDecision::Reject { retry_after: high },
            ) => {
                // High lag should have longer retry
                assert!(high >= low);
            }
            _ => panic!("both should reject"),
        }
    }

    #[test]
    fn test_backpressure_error_display() {
        let err = BackpressureError {
            domain: "catalog".to_string(),
            retry_after: Duration::from_secs(10),
            lag: 5000,
            threshold: 500,
        };

        let msg = err.to_string();
        assert!(msg.contains("catalog"));
        assert!(msg.contains("5000"));
        assert!(msg.contains("500"));
    }
}

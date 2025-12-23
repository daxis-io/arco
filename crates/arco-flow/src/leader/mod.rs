//! Leader election for distributed scheduler coordination.
//!
//! The [`LeaderElector`] trait provides a pluggable mechanism for leader election,
//! separate from storage concerns. This separation enables:
//!
//! - **Testing**: Use [`InMemoryLeaderElector`] for unit tests
//! - **Production**: Use Postgres advisory locks or etcd leases
//! - **Flexibility**: Switch implementations without changing scheduler logic
//!
//! ## Design Principles
//!
//! - **Leases, not locks**: Leaders hold time-bounded leases, not indefinite locks
//! - **Heartbeat renewal**: Leaders must renew periodically or lose leadership
//! - **Graceful handoff**: Leaders can voluntarily release leadership
//!
//! ## Safety
//!
//! Leader election ensures only one scheduler instance processes work at a time,
//! preventing double-dispatch and ensuring exactly-once semantics for task
//! state transitions.

pub mod memory;

use std::time::Duration;

use async_trait::async_trait;

use crate::error::Result;

/// Result of a leadership acquisition attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipResult {
    /// Successfully acquired leadership.
    Acquired {
        /// Lease token that must be used for renewal.
        lease_token: String,
        /// Duration until the lease expires.
        lease_duration: Duration,
    },
    /// Leadership is held by another instance.
    NotLeader {
        /// Identifier of the current leader, if known.
        current_leader: Option<String>,
    },
}

impl LeadershipResult {
    /// Returns true if leadership was acquired.
    #[must_use]
    pub const fn is_leader(&self) -> bool {
        matches!(self, Self::Acquired { .. })
    }

    /// Returns the lease token if leadership was acquired.
    #[must_use]
    pub fn lease_token(&self) -> Option<&str> {
        match self {
            Self::Acquired { lease_token, .. } => Some(lease_token),
            Self::NotLeader { .. } => None,
        }
    }
}

/// Result of a lease renewal attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenewalResult {
    /// Successfully renewed the lease.
    Renewed {
        /// New lease duration.
        lease_duration: Duration,
    },
    /// Lease has expired or was taken by another leader.
    Lost,
    /// The provided lease token is invalid.
    InvalidToken,
}

impl RenewalResult {
    /// Returns true if the lease was successfully renewed.
    #[must_use]
    pub const fn is_renewed(&self) -> bool {
        matches!(self, Self::Renewed { .. })
    }
}

/// Leader election abstraction for distributed scheduler coordination.
///
/// Implementations must provide:
/// - Lease-based leadership with configurable duration
/// - Heartbeat renewal to maintain leadership
/// - Graceful release for orderly shutdown
///
/// ## Example Usage
///
/// ```rust,no_run
/// use std::time::Duration;
///
/// use arco_flow::error::Result;
/// use arco_flow::leader::{LeaderElector, LeadershipResult};
///
/// async fn process_scheduler_tasks() {}
///
/// async fn run_scheduler<L: LeaderElector>(elector: &L, instance_id: &str) -> Result<()> {
///     loop {
///         match elector.try_acquire("scheduler", instance_id).await? {
///             LeadershipResult::Acquired { lease_token, .. } => {
///                 // We are the leader - process work
///                 process_scheduler_tasks().await;
///
///                 // Renew lease periodically
///                 elector.renew("scheduler", &lease_token).await?;
///             }
///             LeadershipResult::NotLeader { .. } => {
///                 // Wait and retry
///                 tokio::time::sleep(Duration::from_secs(5)).await;
///             }
///         }
///     }
/// }
/// ```
///
/// ## Thread Safety
///
/// All methods are `Send + Sync` to support concurrent access from async tasks.
#[async_trait]
pub trait LeaderElector: Send + Sync {
    /// Attempts to acquire leadership for a lock key.
    ///
    /// # Arguments
    ///
    /// * `lock_key` - Identifier for the lock (e.g., "scheduler", "compactor")
    /// * `instance_id` - Unique identifier for this instance
    ///
    /// # Returns
    ///
    /// - `LeadershipResult::Acquired` with lease token and duration if successful
    /// - `LeadershipResult::NotLeader` with current leader info if leadership is held
    async fn try_acquire(&self, lock_key: &str, instance_id: &str) -> Result<LeadershipResult>;

    /// Renews an existing lease.
    ///
    /// Must be called before the lease expires to maintain leadership.
    ///
    /// # Arguments
    ///
    /// * `lock_key` - The lock key being renewed
    /// * `lease_token` - The token from the original acquisition
    ///
    /// # Returns
    ///
    /// - `RenewalResult::Renewed` if successful
    /// - `RenewalResult::Lost` if the lease expired or was taken
    /// - `RenewalResult::InvalidToken` if the token is invalid
    async fn renew(&self, lock_key: &str, lease_token: &str) -> Result<RenewalResult>;

    /// Voluntarily releases leadership.
    ///
    /// Call this during orderly shutdown to allow faster failover.
    ///
    /// # Arguments
    ///
    /// * `lock_key` - The lock key to release
    /// * `lease_token` - The token from the original acquisition
    ///
    /// # Returns
    ///
    /// - `true` if successfully released
    /// - `false` if the lease was already expired or held by another instance
    async fn release(&self, lock_key: &str, lease_token: &str) -> Result<bool>;

    /// Returns the current leader for a lock key, if any.
    ///
    /// # Arguments
    ///
    /// * `lock_key` - The lock key to check
    ///
    /// # Returns
    ///
    /// The instance ID of the current leader, or `None` if no leader exists.
    async fn current_leader(&self, lock_key: &str) -> Result<Option<String>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leadership_result_is_leader() {
        let acquired = LeadershipResult::Acquired {
            lease_token: "token".to_string(),
            lease_duration: Duration::from_secs(30),
        };
        assert!(acquired.is_leader());

        let not_leader = LeadershipResult::NotLeader {
            current_leader: Some("other".to_string()),
        };
        assert!(!not_leader.is_leader());
    }

    #[test]
    fn leadership_result_lease_token() {
        let acquired = LeadershipResult::Acquired {
            lease_token: "my-token".to_string(),
            lease_duration: Duration::from_secs(30),
        };
        assert_eq!(acquired.lease_token(), Some("my-token"));

        let not_leader = LeadershipResult::NotLeader {
            current_leader: None,
        };
        assert_eq!(not_leader.lease_token(), None);
    }

    #[test]
    fn renewal_result_is_renewed() {
        let renewed = RenewalResult::Renewed {
            lease_duration: Duration::from_secs(30),
        };
        assert!(renewed.is_renewed());

        assert!(!RenewalResult::Lost.is_renewed());
        assert!(!RenewalResult::InvalidToken.is_renewed());
    }
}

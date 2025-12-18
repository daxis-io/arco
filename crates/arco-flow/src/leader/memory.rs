//! In-memory leader elector implementation for testing.
//!
//! This module provides [`InMemoryLeaderElector`], a simple in-memory implementation
//! of the [`LeaderElector`] trait suitable for testing and development.
//!
//! ## Limitations
//!
//! - **NOT suitable for production**: No cross-process coordination
//! - **Single-process only**: Leadership is not shared across process boundaries
//! - **No persistence**: All state is lost when the process exits

use std::collections::HashMap;
use std::sync::{PoisonError, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ulid::Ulid;

use super::{LeaderElector, LeadershipResult, RenewalResult};
use crate::error::{Error, Result};

/// Lease information for a lock.
#[derive(Debug, Clone)]
struct Lease {
    /// The instance holding the lease.
    instance_id: String,
    /// Unique token for this lease.
    token: String,
    /// When the lease expires.
    expires_at: DateTime<Utc>,
}

/// In-memory leader elector for testing.
///
/// Provides a simple, thread-safe implementation of the [`LeaderElector`] trait
/// using `RwLock` for synchronization.
///
/// ## Example
///
/// ```rust
/// use arco_flow::leader::memory::InMemoryLeaderElector;
/// use std::time::Duration;
///
/// let elector = InMemoryLeaderElector::new(Duration::from_secs(30));
/// // Use elector in tests...
/// ```
#[derive(Debug)]
pub struct InMemoryLeaderElector {
    leases: RwLock<HashMap<String, Lease>>,
    lease_duration: Duration,
}

impl Default for InMemoryLeaderElector {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

/// Converts a lock poison error to a storage error.
fn poison_err<T>(_: PoisonError<T>) -> Error {
    Error::storage("lock poisoned")
}

impl InMemoryLeaderElector {
    /// Creates a new in-memory leader elector with the specified lease duration.
    #[must_use]
    pub fn new(lease_duration: Duration) -> Self {
        Self {
            leases: RwLock::new(HashMap::new()),
            lease_duration,
        }
    }

    /// Generates a new unique lease token.
    fn generate_token() -> String {
        Ulid::new().to_string()
    }
}

#[async_trait]
impl LeaderElector for InMemoryLeaderElector {
    async fn try_acquire(&self, lock_key: &str, instance_id: &str) -> Result<LeadershipResult> {
        let mut leases = self.leases.write().map_err(poison_err)?;
        let now = Utc::now();

        // Check if there's an existing valid lease
        if let Some(lease) = leases.get(lock_key) {
            if lease.expires_at > now {
                // Lease is still valid - check if we already own it
                if lease.instance_id == instance_id {
                    // We already have the lease - renew it
                    let new_lease = Lease {
                        instance_id: instance_id.to_string(),
                        token: Self::generate_token(),
                        expires_at: now
                            + chrono::Duration::from_std(self.lease_duration)
                                .unwrap_or(chrono::Duration::seconds(30)),
                    };
                    let token = new_lease.token.clone();
                    leases.insert(lock_key.to_string(), new_lease);
                    drop(leases);

                    return Ok(LeadershipResult::Acquired {
                        lease_token: token,
                        lease_duration: self.lease_duration,
                    });
                }

                // Someone else has the lease
                let current_leader = lease.instance_id.clone();
                drop(leases);

                return Ok(LeadershipResult::NotLeader {
                    current_leader: Some(current_leader),
                });
            }
            // Lease has expired - fall through to acquire
        }

        // Acquire the lease
        let new_lease = Lease {
            instance_id: instance_id.to_string(),
            token: Self::generate_token(),
            expires_at: now
                + chrono::Duration::from_std(self.lease_duration)
                    .unwrap_or(chrono::Duration::seconds(30)),
        };
        let token = new_lease.token.clone();
        leases.insert(lock_key.to_string(), new_lease);
        drop(leases);

        Ok(LeadershipResult::Acquired {
            lease_token: token,
            lease_duration: self.lease_duration,
        })
    }

    async fn renew(&self, lock_key: &str, lease_token: &str) -> Result<RenewalResult> {
        let mut leases = self.leases.write().map_err(poison_err)?;
        let now = Utc::now();

        let Some(lease) = leases.get_mut(lock_key) else {
            drop(leases);
            return Ok(RenewalResult::Lost);
        };

        // Validate the token matches
        if lease.token != lease_token {
            drop(leases);
            return Ok(RenewalResult::InvalidToken);
        }

        // Check if the lease has expired
        if lease.expires_at <= now {
            drop(leases);
            return Ok(RenewalResult::Lost);
        }

        // Renew the lease by extending expiration without rotating the token.
        lease.expires_at = now
            + chrono::Duration::from_std(self.lease_duration)
                .unwrap_or(chrono::Duration::seconds(30));
        drop(leases);

        Ok(RenewalResult::Renewed {
            lease_duration: self.lease_duration,
        })
    }

    async fn release(&self, lock_key: &str, lease_token: &str) -> Result<bool> {
        let mut leases = self.leases.write().map_err(poison_err)?;

        let Some(lease) = leases.get(lock_key) else {
            drop(leases);
            return Ok(false);
        };

        // Validate the token matches
        if lease.token != lease_token {
            drop(leases);
            return Ok(false);
        }

        // Remove the lease
        leases.remove(lock_key);
        drop(leases);

        Ok(true)
    }

    async fn current_leader(&self, lock_key: &str) -> Result<Option<String>> {
        let leases = self.leases.read().map_err(poison_err)?;
        let now = Utc::now();

        let result = leases.get(lock_key).and_then(|lease| {
            if lease.expires_at > now {
                Some(lease.instance_id.clone())
            } else {
                None
            }
        });
        drop(leases);

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_leadership_when_no_leader() -> Result<()> {
        let elector = InMemoryLeaderElector::new(Duration::from_secs(30));

        let result = elector.try_acquire("scheduler", "instance-1").await?;

        assert!(result.is_leader());
        assert!(result.lease_token().is_some());

        Ok(())
    }

    #[tokio::test]
    async fn cannot_acquire_when_another_holds_lease() -> Result<()> {
        let elector = InMemoryLeaderElector::new(Duration::from_secs(30));

        // First instance acquires
        let result1 = elector.try_acquire("scheduler", "instance-1").await?;
        assert!(result1.is_leader());

        // Second instance cannot acquire
        let result2 = elector.try_acquire("scheduler", "instance-2").await?;
        assert!(!result2.is_leader());
        match result2 {
            LeadershipResult::NotLeader { current_leader } => {
                assert_eq!(current_leader, Some("instance-1".to_string()));
            }
            _ => panic!("Expected NotLeader"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn same_instance_can_reacquire() -> Result<()> {
        let elector = InMemoryLeaderElector::new(Duration::from_secs(30));

        // Acquire first time
        let result1 = elector.try_acquire("scheduler", "instance-1").await?;
        assert!(result1.is_leader());
        let token1 = result1.lease_token().unwrap().to_string();

        // Acquire again (should get a new token)
        let result2 = elector.try_acquire("scheduler", "instance-1").await?;
        assert!(result2.is_leader());
        let token2 = result2.lease_token().unwrap().to_string();

        // Tokens should be different (new lease)
        assert_ne!(token1, token2);

        Ok(())
    }

    #[tokio::test]
    async fn renew_with_valid_token() -> Result<()> {
        let elector = InMemoryLeaderElector::new(Duration::from_secs(30));

        let acquire_result = elector.try_acquire("scheduler", "instance-1").await?;
        let token = acquire_result.lease_token().unwrap();

        let renew_result = elector.renew("scheduler", token).await?;
        assert!(renew_result.is_renewed());

        Ok(())
    }

    #[tokio::test]
    async fn renew_allows_multiple_renewals_with_same_token() -> Result<()> {
        let elector = InMemoryLeaderElector::new(Duration::from_secs(30));

        let acquire_result = elector.try_acquire("scheduler", "instance-1").await?;
        let token = acquire_result.lease_token().unwrap().to_string();

        let renew_result = elector.renew("scheduler", &token).await?;
        assert!(renew_result.is_renewed());

        let renew_result = elector.renew("scheduler", &token).await?;
        assert!(renew_result.is_renewed());

        Ok(())
    }

    #[tokio::test]
    async fn renew_preserves_token_for_release() -> Result<()> {
        let elector = InMemoryLeaderElector::new(Duration::from_secs(30));

        let acquire_result = elector.try_acquire("scheduler", "instance-1").await?;
        let token = acquire_result.lease_token().unwrap().to_string();

        let renew_result = elector.renew("scheduler", &token).await?;
        assert!(renew_result.is_renewed());

        let released = elector.release("scheduler", &token).await?;
        assert!(released);

        Ok(())
    }

    #[tokio::test]
    async fn renew_with_invalid_token() -> Result<()> {
        let elector = InMemoryLeaderElector::new(Duration::from_secs(30));

        // Acquire
        let _ = elector.try_acquire("scheduler", "instance-1").await?;

        // Try to renew with wrong token
        let renew_result = elector.renew("scheduler", "wrong-token").await?;
        assert_eq!(renew_result, RenewalResult::InvalidToken);

        Ok(())
    }

    #[tokio::test]
    async fn renew_nonexistent_lease() -> Result<()> {
        let elector = InMemoryLeaderElector::new(Duration::from_secs(30));

        let renew_result = elector.renew("scheduler", "some-token").await?;
        assert_eq!(renew_result, RenewalResult::Lost);

        Ok(())
    }

    #[tokio::test]
    async fn release_leadership() -> Result<()> {
        let elector = InMemoryLeaderElector::new(Duration::from_secs(30));

        // Acquire
        let acquire_result = elector.try_acquire("scheduler", "instance-1").await?;
        let token = acquire_result.lease_token().unwrap();

        // Verify leader
        let leader = elector.current_leader("scheduler").await?;
        assert_eq!(leader, Some("instance-1".to_string()));

        // Release
        let released = elector.release("scheduler", token).await?;
        assert!(released);

        // Verify no leader
        let leader = elector.current_leader("scheduler").await?;
        assert_eq!(leader, None);

        Ok(())
    }

    #[tokio::test]
    async fn release_with_wrong_token() -> Result<()> {
        let elector = InMemoryLeaderElector::new(Duration::from_secs(30));

        // Acquire
        let _ = elector.try_acquire("scheduler", "instance-1").await?;

        // Try to release with wrong token
        let released = elector.release("scheduler", "wrong-token").await?;
        assert!(!released);

        // Leader should still be set
        let leader = elector.current_leader("scheduler").await?;
        assert_eq!(leader, Some("instance-1".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn current_leader_when_no_lease() -> Result<()> {
        let elector = InMemoryLeaderElector::new(Duration::from_secs(30));

        let leader = elector.current_leader("scheduler").await?;
        assert_eq!(leader, None);

        Ok(())
    }

    #[tokio::test]
    async fn multiple_lock_keys_are_independent() -> Result<()> {
        let elector = InMemoryLeaderElector::new(Duration::from_secs(30));

        // Acquire different locks
        let result1 = elector.try_acquire("scheduler", "instance-1").await?;
        assert!(result1.is_leader());

        let result2 = elector.try_acquire("compactor", "instance-2").await?;
        assert!(result2.is_leader());

        // Verify independent leaders
        let scheduler_leader = elector.current_leader("scheduler").await?;
        assert_eq!(scheduler_leader, Some("instance-1".to_string()));

        let compactor_leader = elector.current_leader("compactor").await?;
        assert_eq!(compactor_leader, Some("instance-2".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn expired_lease_can_be_taken() -> Result<()> {
        // Use a very short lease duration
        let elector = InMemoryLeaderElector::new(Duration::from_millis(1));

        // Acquire
        let _ = elector.try_acquire("scheduler", "instance-1").await?;

        // Wait for lease to expire
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Another instance can now acquire
        let result = elector.try_acquire("scheduler", "instance-2").await?;
        assert!(result.is_leader());

        // Verify new leader
        let leader = elector.current_leader("scheduler").await?;
        assert_eq!(leader, Some("instance-2".to_string()));

        Ok(())
    }
}

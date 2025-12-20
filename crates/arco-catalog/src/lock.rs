//! Re-exported distributed lock primitives from `arco-core`.
//!
//! The lock implementation lives in `arco-core` to keep fencing token minting
//! private to the lock module while allowing catalog code to use it.

pub use arco_core::lock::{
    DistributedLock, LockGuard, LockInfo, DEFAULT_LOCK_TTL, DEFAULT_MAX_RETRIES,
};

/// Path constants for lock files.
pub use arco_core::lock::paths;

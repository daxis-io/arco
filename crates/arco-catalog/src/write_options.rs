//! Write options for catalog mutations.
//!
//! This module defines a single `WriteOptions` struct that carries:
//! - Idempotency context for safe retries
//! - Optimistic concurrency control (`if_match`)
//! - Actor and request metadata for auditing/tracing
//!
//! This is the catalog-facing equivalent of HTTP request context.

/// Strongly-typed idempotency key for write operations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// Creates a new idempotency key.
    #[must_use]
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// Returns the key as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Snapshot version used for optimistic locking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotVersion(u64);

impl SnapshotVersion {
    /// Creates a new snapshot version.
    #[must_use]
    pub const fn new(version: u64) -> Self {
        Self(version)
    }

    /// Returns the version value.
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Write options for all mutating catalog operations.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Idempotency key for safe retries.
    pub idempotency_key: Option<IdempotencyKey>,
    /// Optimistic lock: fail if current version doesn't match.
    pub if_match: Option<SnapshotVersion>,
    /// Actor performing the write (service/user).
    pub actor: Option<String>,
    /// Request ID for tracing/correlation.
    pub request_id: Option<String>,
}

impl WriteOptions {
    /// Creates options with an idempotency key.
    #[must_use]
    pub fn with_idempotency(key: impl Into<String>) -> Self {
        Self {
            idempotency_key: Some(IdempotencyKey::new(key)),
            ..Self::default()
        }
    }

    /// Creates options with an `if-match` snapshot version.
    #[must_use]
    pub fn with_if_match(version: u64) -> Self {
        Self {
            if_match: Some(SnapshotVersion::new(version)),
            ..Self::default()
        }
    }

    /// Sets the idempotency key for safe retries.
    #[must_use]
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(IdempotencyKey::new(key));
        self
    }

    /// Sets the actor performing the write (service/user).
    #[must_use]
    pub fn with_actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }

    /// Sets a request ID for tracing/correlation.
    #[must_use]
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

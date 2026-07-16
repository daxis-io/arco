//! Write options for catalog mutations.
//!
//! This module defines a single `WriteOptions` struct that carries:
//! - Idempotency context for safe retries
//! - Optimistic concurrency control (`if_match`)
//! - Actor and request metadata for auditing/tracing
//!
//! This is the catalog-facing equivalent of HTTP request context.

use crate::writer::CatalogTransactionRequest;

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

/// Exact transaction identity used by crash-recoverable catalog writers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogTransactionIdentity {
    /// Canonical low-level transaction ULID.
    pub(crate) tx_id: String,
    /// Canonical reviewed request hash bound to the transaction.
    pub(crate) request_hash: String,
    /// Tenant scope proven by exact durable-handle authority.
    pub(crate) tenant_id: String,
    /// Workspace scope proven by exact durable-handle authority.
    pub(crate) workspace_id: String,
    /// Exact request identity owned by the frozen participant.
    pub(crate) request_id: String,
    /// Exact idempotency identity owned by the frozen participant.
    pub(crate) idempotency_key: String,
    /// Durable handle that froze the participant.
    pub(crate) handle_id: String,
    /// Positive staged-mutation ordinal owned by the handle.
    pub(crate) ordinal: u64,
    /// Digest of the exact immutable staged mutation.
    pub(crate) staged_sha256: String,
    /// Typed reviewed request reconstructed from the exact staged mutation.
    pub(crate) reviewed_request: CatalogTransactionRequest,
    /// Whether an exact mutable low-level claim currently authorizes execution.
    pub(crate) mutation_authorized: bool,
}

impl CatalogTransactionIdentity {
    /// Returns the exact low-level transaction ID proven by durable handle authority.
    #[must_use]
    pub fn tx_id(&self) -> &str {
        &self.tx_id
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
    /// Optional exact transaction identity for durable event-path recovery.
    pub(crate) transaction_identity: Option<CatalogTransactionIdentity>,
    /// Request hash recomputed by the selected catalog transaction method.
    pub(crate) validated_transaction_request_hash: Option<String>,
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

    /// Enables exact-path recovery with a writer-authorized frozen handle identity.
    ///
    /// The opaque identity can only be obtained after the catalog writer
    /// exact-reads durable handle, staged-mutation, claim, and transaction
    /// authority.
    #[doc(hidden)]
    #[must_use]
    pub fn with_transaction_identity(mut self, identity: CatalogTransactionIdentity) -> Self {
        self.transaction_identity = Some(identity);
        self.validated_transaction_request_hash = None;
        self
    }
}

//! State-store seam for future Tier-1 authority backends.
//!
//! The current adapter intentionally exposes only capability discovery. It does
//! not delegate production reads or writes, and it must not mint synthetic
//! state tokens for today's ledger plus synchronous compactor path.

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::{CatalogError, Result};

/// Opaque retained authority token for a future state-store scope.
///
/// External crates cannot mint authority tokens directly.
///
/// ```compile_fail
/// use arco_catalog::{StateScope, StateToken};
///
/// let scope = StateScope::new("tenant", "workspace", "catalog");
/// let _token = StateToken::new(scope, 1, "manifest-1");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateToken {
    scope: StateScope,
    logical_sequence: u64,
    authority_manifest_id: String,
}

impl StateToken {
    /// Creates a state token value for crate-local tests.
    #[cfg(test)]
    #[must_use]
    fn for_test(
        scope: StateScope,
        logical_sequence: u64,
        authority_manifest_id: impl Into<String>,
    ) -> Self {
        Self {
            scope,
            logical_sequence,
            authority_manifest_id: authority_manifest_id.into(),
        }
    }

    /// Returns the authority scope named by this token.
    #[must_use]
    pub const fn scope(&self) -> &StateScope {
        &self.scope
    }

    /// Returns the logical authority sequence named by this token.
    #[must_use]
    pub const fn logical_sequence(&self) -> u64 {
        self.logical_sequence
    }

    /// Returns the authority manifest identifier named by this token.
    #[must_use]
    pub fn authority_manifest_id(&self) -> &str {
        &self.authority_manifest_id
    }
}

/// Opaque retained checkpoint token for longer-lived retained reads.
///
/// External crates cannot mint checkpoint tokens directly.
///
/// ```compile_fail
/// use arco_catalog::{CheckpointToken, StateScope};
///
/// let scope = StateScope::new("tenant", "workspace", "catalog");
/// let _token = CheckpointToken::new(scope, "checkpoint-1");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointToken {
    scope: StateScope,
    checkpoint_id: String,
}

impl CheckpointToken {
    /// Returns the authority scope retained by this checkpoint.
    #[must_use]
    pub const fn scope(&self) -> &StateScope {
        &self.scope
    }

    /// Returns the checkpoint identifier.
    #[must_use]
    pub fn checkpoint_id(&self) -> &str {
        &self.checkpoint_id
    }
}

/// Options for opening a future state-store transaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TxnOptions {
    scope: Option<StateScope>,
    request_id: Option<String>,
}

impl TxnOptions {
    /// Creates transaction options for an optional authority scope.
    #[must_use]
    pub const fn new(scope: Option<StateScope>) -> Self {
        Self {
            scope,
            request_id: None,
        }
    }

    /// Adds a request identifier to the transaction options.
    #[must_use]
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// Returns the requested authority scope, if one was provided.
    #[must_use]
    pub const fn scope(&self) -> Option<&StateScope> {
        self.scope.as_ref()
    }

    /// Returns the request identifier, if one was provided.
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }
}

/// Options for creating a future retained authority checkpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckpointOptions {
    scope: Option<StateScope>,
    min_retention_seconds: Option<u64>,
}

impl CheckpointOptions {
    /// Creates checkpoint options for an optional authority scope.
    #[must_use]
    pub const fn new(scope: Option<StateScope>) -> Self {
        Self {
            scope,
            min_retention_seconds: None,
        }
    }

    /// Adds a minimum retention request in seconds.
    #[must_use]
    pub const fn with_min_retention_seconds(mut self, seconds: u64) -> Self {
        self.min_retention_seconds = Some(seconds);
        self
    }

    /// Returns the requested authority scope, if one was provided.
    #[must_use]
    pub const fn scope(&self) -> Option<&StateScope> {
        self.scope.as_ref()
    }

    /// Returns the requested minimum retention in seconds, if one was provided.
    #[must_use]
    pub const fn min_retention_seconds(&self) -> Option<u64> {
        self.min_retention_seconds
    }
}

/// Value plus generation evidence observed from a future state-store backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    bytes: Bytes,
    generation: Option<u64>,
}

impl VersionedValue {
    /// Creates a versioned value.
    #[must_use]
    pub const fn new(bytes: Bytes, generation: Option<u64>) -> Self {
        Self { bytes, generation }
    }

    /// Returns the stored value bytes.
    #[must_use]
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns generation evidence, if the backend exposed one.
    #[must_use]
    pub const fn generation(&self) -> Option<u64> {
        self.generation
    }
}

/// Half-open byte-key range used for range reads and preconditions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    start: Vec<u8>,
    end: Vec<u8>,
}

impl KeyRange {
    /// Creates a half-open key range `[start, end)`.
    #[must_use]
    pub fn new(start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) -> Self {
        Self {
            start: start.into(),
            end: end.into(),
        }
    }

    /// Returns the inclusive start key.
    #[must_use]
    pub fn start(&self) -> &[u8] {
        &self.start
    }

    /// Returns the exclusive end key.
    #[must_use]
    pub fn end(&self) -> &[u8] {
        &self.end
    }
}

/// Point and range inputs declared by a semantic predicate.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PredicateInputSet {
    point_keys: Vec<Vec<u8>>,
    ranges: Vec<KeyRange>,
}

impl PredicateInputSet {
    /// Creates a predicate input set from point keys and key ranges.
    #[must_use]
    pub fn new(point_keys: Vec<Vec<u8>>, ranges: Vec<KeyRange>) -> Self {
        Self { point_keys, ranges }
    }

    /// Returns point keys observed by the predicate.
    #[must_use]
    pub fn point_keys(&self) -> &[Vec<u8>] {
        &self.point_keys
    }

    /// Returns key ranges observed by the predicate.
    #[must_use]
    pub fn ranges(&self) -> &[KeyRange] {
        &self.ranges
    }
}

/// Key/value pair returned from state-store scans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPair {
    key: Vec<u8>,
    value: VersionedValue,
}

impl KvPair {
    /// Creates a key/value pair.
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>, value: VersionedValue) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }

    /// Returns the key bytes.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value and generation evidence.
    #[must_use]
    pub const fn value(&self) -> &VersionedValue {
        &self.value
    }
}

/// Authority scope addressed by state-store tokens and transactions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateScope {
    tenant_id: String,
    workspace_id: String,
    domain: String,
}

impl StateScope {
    /// Creates an authority scope.
    #[must_use]
    pub fn new(
        tenant_id: impl Into<String>,
        workspace_id: impl Into<String>,
        domain: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            workspace_id: workspace_id.into(),
            domain: domain.into(),
        }
    }

    /// Returns the tenant identifier.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the workspace identifier.
    #[must_use]
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    /// Returns the state-store domain name.
    #[must_use]
    pub fn domain(&self) -> &str {
        &self.domain
    }
}

/// Backend capabilities exposed by a state-store implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateStoreCapabilities {
    /// Stable implementation identifier.
    implementation: &'static str,
    /// Whether retained `StateToken` reads and issuance are supported.
    retained_state_tokens: bool,
    /// Whether retained checkpoints are supported.
    checkpoints: bool,
    /// Whether addressed historical reads through `read_at` are supported.
    read_at: bool,
    /// Whether write transactions are supported.
    transactions: bool,
    /// Whether range preconditions are supported.
    range_preconditions: bool,
    /// Whether semantic predicate input-set preconditions are supported.
    predicate_preconditions: bool,
}

impl StateStoreCapabilities {
    /// Returns the explicit capabilities of the current-authority adapter.
    #[must_use]
    pub const fn arco_state_current() -> Self {
        Self {
            implementation: CurrentStateStore::IMPLEMENTATION,
            retained_state_tokens: false,
            checkpoints: false,
            read_at: false,
            transactions: false,
            range_preconditions: false,
            predicate_preconditions: false,
        }
    }

    /// Returns the stable implementation identifier.
    #[must_use]
    pub const fn implementation(&self) -> &'static str {
        self.implementation
    }

    /// Returns whether retained `StateToken` reads and issuance are supported.
    #[must_use]
    pub const fn retained_state_tokens(&self) -> bool {
        self.retained_state_tokens
    }

    /// Returns whether retained checkpoints are supported.
    #[must_use]
    pub const fn checkpoints(&self) -> bool {
        self.checkpoints
    }

    /// Returns whether addressed historical reads through `read_at` are supported.
    #[must_use]
    pub const fn read_at(&self) -> bool {
        self.read_at
    }

    /// Returns whether write transactions are supported.
    #[must_use]
    pub const fn transactions(&self) -> bool {
        self.transactions
    }

    /// Returns whether range preconditions are supported.
    #[must_use]
    pub const fn range_preconditions(&self) -> bool {
        self.range_preconditions
    }

    /// Returns whether semantic predicate input-set preconditions are supported.
    #[must_use]
    pub const fn predicate_preconditions(&self) -> bool {
        self.predicate_preconditions
    }
}

/// Read-only state-store operations.
#[async_trait]
pub trait ArcoStateReader: Send + Sync {
    /// Reads the current value for a key.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot perform the read.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Scans current key/value pairs by prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot perform the scan.
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvPair>>;

    /// Opens a retained reader at a specific state token.
    ///
    /// # Errors
    ///
    /// Returns an error when retained token reads are unsupported or invalid.
    async fn read_at(&self, token: StateToken) -> Result<Box<dyn ArcoStateReader>>;
}

/// Administrative state-store operations.
#[async_trait]
pub trait ArcoStateAdmin: Send + Sync {
    /// Returns this implementation's capability matrix.
    fn capabilities(&self) -> StateStoreCapabilities;

    /// Issues a token for current retained state.
    ///
    /// # Errors
    ///
    /// Returns an error when retained state tokens are unsupported.
    async fn current_state_token(&self) -> Result<StateToken>;

    /// Creates a retained checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when checkpoints are unsupported.
    async fn checkpoint(&self, opts: CheckpointOptions) -> Result<CheckpointToken>;
}

/// Combined state-store read, admin, and transaction surface.
#[async_trait]
pub trait ArcoStateStore: ArcoStateReader + ArcoStateAdmin {
    /// Begins a write transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when transactions are unsupported.
    async fn begin_txn(&self, opts: TxnOptions) -> Result<Box<dyn ArcoStateTxn>>;
}

/// Mutable state-store transaction.
#[async_trait]
pub trait ArcoStateTxn: Send + Sync {
    /// Reads a value inside the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot perform the read.
    async fn get(&mut self, key: &[u8]) -> Result<Option<VersionedValue>>;

    /// Scans key/value pairs by prefix inside the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot perform the scan.
    async fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Vec<KvPair>>;

    /// Stages a value write.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot stage the write.
    async fn put(&mut self, key: &[u8], value: Bytes) -> Result<()>;

    /// Stages a value delete.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend cannot stage the delete.
    async fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Asserts that a key is absent at commit time.
    ///
    /// # Errors
    ///
    /// Returns an error when the assertion cannot be recorded or validated.
    async fn assert_absent(&mut self, key: &[u8]) -> Result<()>;

    /// Asserts that a key has the expected generation at commit time.
    ///
    /// # Errors
    ///
    /// Returns an error when the assertion cannot be recorded or validated.
    async fn assert_generation(&mut self, key: &[u8], generation: u64) -> Result<()>;

    /// Asserts that a key range is empty at commit time.
    ///
    /// # Errors
    ///
    /// Returns an error when range preconditions are unsupported or invalid.
    async fn assert_range_empty(&mut self, range: KeyRange) -> Result<()>;

    /// Asserts that a key range is unchanged at commit time.
    ///
    /// # Errors
    ///
    /// Returns an error when range preconditions are unsupported or invalid.
    async fn assert_range_unchanged(
        &mut self,
        range: KeyRange,
        observed_generation: u64,
    ) -> Result<()>;

    /// Records point and range inputs used by a semantic predicate.
    ///
    /// # Errors
    ///
    /// Returns an error when predicate input tracking is unsupported or invalid.
    async fn read_set(
        &mut self,
        keys: &[Vec<u8>],
        ranges: &[KeyRange],
    ) -> Result<PredicateInputSet>;

    /// Asserts that previously declared predicate inputs are unchanged.
    ///
    /// # Errors
    ///
    /// Returns an error when predicate preconditions are unsupported or invalid.
    async fn assert_inputs_unchanged(&mut self, inputs: PredicateInputSet) -> Result<()>;

    /// Commits the transaction and returns the resulting state token.
    ///
    /// # Errors
    ///
    /// Returns an error when commit fails or transactions are unsupported.
    async fn commit(self: Box<Self>) -> Result<StateToken>;

    /// Rolls back the transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when rollback fails or transactions are unsupported.
    async fn rollback(self: Box<Self>) -> Result<()>;
}

/// Capability-only adapter for today's ledger plus synchronous compactor path.
#[derive(Debug, Clone, Copy, Default)]
pub struct CurrentStateStore;

impl CurrentStateStore {
    /// Stable implementation identifier for the current-authority adapter.
    pub const IMPLEMENTATION: &'static str = "arco-state-current";

    /// Creates a current-authority state-store adapter.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ArcoStateReader for CurrentStateStore {
    async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        Err(unsupported("point reads through arco-state-current"))
    }

    async fn scan_prefix(&self, _prefix: &[u8]) -> Result<Vec<KvPair>> {
        Err(unsupported("range reads through arco-state-current"))
    }

    async fn read_at(&self, _token: StateToken) -> Result<Box<dyn ArcoStateReader>> {
        Err(unsupported("StateToken reads through arco-state-current"))
    }
}

#[async_trait]
impl ArcoStateAdmin for CurrentStateStore {
    fn capabilities(&self) -> StateStoreCapabilities {
        StateStoreCapabilities::arco_state_current()
    }

    async fn current_state_token(&self) -> Result<StateToken> {
        Err(unsupported(
            "StateToken issuance through arco-state-current",
        ))
    }

    async fn checkpoint(&self, _opts: CheckpointOptions) -> Result<CheckpointToken> {
        Err(unsupported(
            "CheckpointToken issuance through arco-state-current",
        ))
    }
}

#[async_trait]
impl ArcoStateStore for CurrentStateStore {
    async fn begin_txn(&self, _opts: TxnOptions) -> Result<Box<dyn ArcoStateTxn>> {
        Err(unsupported("transactions through arco-state-current"))
    }
}

fn unsupported(operation: &str) -> CatalogError {
    CatalogError::UnsupportedOperation {
        message: format!(
            "{operation} are not supported; the current adapter is a capability surface only"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_unsupported<T>(result: Result<T>, expected: &str) {
        match result {
            Err(CatalogError::UnsupportedOperation { .. }) => {}
            Err(error) => panic!("expected UnsupportedOperation for {expected}, got {error:?}"),
            Ok(_) => panic!("expected UnsupportedOperation for {expected}"),
        }
    }

    #[tokio::test]
    async fn current_state_store_rejects_read_at_with_internal_token() {
        let token = StateToken::for_test(
            StateScope::new("tenant", "workspace", "catalog"),
            1,
            "manifest-1",
        );

        assert_unsupported(CurrentStateStore::new().read_at(token).await, "read_at");
    }
}

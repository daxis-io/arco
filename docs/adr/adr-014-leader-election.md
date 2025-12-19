# ADR-014: Leader Election Strategy

## Status
Accepted

## Context
Arco's scheduler needs leader election to ensure:
- Only one scheduler instance is active at a time
- Failover happens quickly when the leader dies
- Split-brain scenarios are prevented
- No external coordination service is required (optional)

Cloud environments provide various primitives:
- GCP: Spanner transactions, Cloud Pub/Sub ordering
- AWS: DynamoDB conditional writes, SQS FIFO
- Generic: PostgreSQL advisory locks, Redis SETNX

## Decision
Define a `LeaderElector` trait with pluggable implementations:

### Trait Definition
```rust
#[async_trait]
pub trait LeaderElector: Send + Sync {
    /// Attempts to acquire leadership.
    async fn try_acquire(&self) -> Result<LeadershipResult>;

    /// Renews current leadership (heartbeat).
    async fn renew(&self) -> Result<RenewalResult>;

    /// Releases leadership voluntarily.
    async fn release(&self) -> Result<()>;
}
```

### Lease-Based Model
- Leader holds a time-limited lease (default: 30 seconds)
- Leader must renew before expiry (heartbeat every 10 seconds)
- Lease expiry allows automatic failover

### Implementations
1. **InMemoryLeaderElector**: For testing, single-process only
2. **PostgresLeaderElector**: Uses `pg_advisory_lock` or row-level locking (optional, via `postgres` feature)
3. **Future**: Cloud-native implementations (Spanner, DynamoDB)

### Fencing
Leaders receive a monotonically increasing `fencing_token` on acquisition. All state mutations must include this token to prevent stale leaders from corrupting state.

## Consequences

### Positive
- Pluggable implementations for different environments
- Automatic failover on leader crash
- Fencing tokens prevent split-brain data corruption
- No external service dependency for basic testing

### Negative
- Lease-based model has unavoidable delay on failover
- Fencing tokens add complexity to state mutations
- Clock skew can cause premature lease expiry
- PostgreSQL implementation requires database connection

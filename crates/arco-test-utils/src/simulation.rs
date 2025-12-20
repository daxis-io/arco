//! Deterministic simulation harness for testing distributed scenarios.
//!
//! This module provides tools for deterministic failure injection and
//! distributed system testing. It allows tests to reproduce complex
//! failure scenarios reliably.
//!
//! # Key Features
//!
//! - **Deterministic RNG**: Seeded random number generator for reproducible tests
//! - **Fault Injection**: Configurable failure points for storage, network, and locks
//! - **Clock Control**: Simulated time that can be advanced manually
//! - **Operation Recording**: Detailed logs of all operations for debugging
//!
//! # Example
//!
//! ```rust,ignore
//! use std::time::Duration;
//!
//! use arco_test_utils::simulation::{FaultConfig, OperationType, SimulationHarness};
//!
//! let harness = SimulationHarness::with_seed(12345);
//!
//! // Configure 10% storage write failures.
//! harness.configure_faults(FaultConfig {
//!     storage_write_failure_rate: 0.1,
//!     ..Default::default()
//! });
//!
//! let correlation_id = harness.new_correlation_id();
//! let failed = harness.should_fail_storage_write();
//! harness.record(
//!     OperationType::StorageWrite {
//!         path: "state/catalog/snapshots.parquet".to_string(),
//!         success: !failed,
//!     },
//!     failed,
//!     &correlation_id,
//! );
//!
//! harness.clock().advance(Duration::from_secs(1));
//! let ops = harness.operations();
//! assert_eq!(ops.len(), 1);
//! assert_eq!(ops[0].is_success(), !failed);
//! ```

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Configuration for fault injection.
#[derive(Debug, Clone, Default)]
pub struct FaultConfig {
    /// Probability of storage read failures (0.0 - 1.0).
    pub storage_read_failure_rate: f64,

    /// Probability of storage write failures (0.0 - 1.0).
    pub storage_write_failure_rate: f64,

    /// Probability of CAS failures (simulates contention).
    pub cas_failure_rate: f64,

    /// Probability of lock acquisition failures.
    pub lock_failure_rate: f64,

    /// Probability of lock renewal failures.
    pub lock_renewal_failure_rate: f64,

    /// Probability of RPC timeouts.
    pub rpc_timeout_rate: f64,

    /// Simulated network latency range (min, max) in milliseconds.
    pub network_latency_range: Option<(u64, u64)>,

    /// Probability of network partition (complete disconnection).
    pub network_partition_rate: f64,
}

impl FaultConfig {
    /// Creates a config with no faults (for baseline testing).
    #[must_use]
    pub fn no_faults() -> Self {
        Self::default()
    }

    /// Creates a config simulating a flaky network.
    #[must_use]
    pub fn flaky_network() -> Self {
        Self {
            rpc_timeout_rate: 0.05,
            network_latency_range: Some((10, 500)),
            network_partition_rate: 0.01,
            ..Default::default()
        }
    }

    /// Creates a config simulating contended storage.
    #[must_use]
    pub fn contended_storage() -> Self {
        Self {
            cas_failure_rate: 0.2,
            lock_failure_rate: 0.1,
            lock_renewal_failure_rate: 0.05,
            ..Default::default()
        }
    }

    /// Creates a config with chaos mode (everything can fail).
    #[must_use]
    pub fn chaos() -> Self {
        Self {
            storage_read_failure_rate: 0.05,
            storage_write_failure_rate: 0.1,
            cas_failure_rate: 0.15,
            lock_failure_rate: 0.1,
            lock_renewal_failure_rate: 0.1,
            rpc_timeout_rate: 0.1,
            network_latency_range: Some((5, 1000)),
            network_partition_rate: 0.02,
        }
    }
}

/// Type of operation recorded by the simulation.
#[derive(Debug, Clone)]
pub enum OperationType {
    /// Storage read operation.
    StorageRead {
        /// The storage path that was read.
        path: String,
        /// Whether the read succeeded.
        success: bool,
    },
    /// Storage write operation.
    StorageWrite {
        /// The storage path that was written.
        path: String,
        /// Whether the write succeeded.
        success: bool,
    },
    /// CAS (Compare-And-Swap) operation.
    Cas {
        /// The storage path for the CAS operation.
        path: String,
        /// Whether the CAS succeeded.
        success: bool,
        /// Whether this failure was due to contention.
        contention: bool,
    },
    /// Lock acquisition.
    LockAcquire {
        /// The resource being locked.
        resource: String,
        /// Whether the lock was acquired.
        success: bool,
        /// The fencing token if acquired.
        token: Option<u64>,
    },
    /// Lock release.
    LockRelease {
        /// The resource being released.
        resource: String,
        /// Whether the release succeeded.
        success: bool,
    },
    /// RPC call.
    Rpc {
        /// The endpoint called.
        endpoint: String,
        /// Whether the RPC succeeded.
        success: bool,
        /// The latency in milliseconds.
        latency_ms: u64,
    },
    /// Custom operation for test-specific tracking.
    Custom {
        /// Name of the custom operation.
        name: String,
        /// Details about the operation.
        details: String,
    },
}

/// A recorded operation with timestamp and metadata.
#[derive(Debug, Clone)]
pub struct RecordedOperation {
    /// Simulated timestamp when operation occurred.
    pub timestamp: DateTime<Utc>,
    /// Type of operation.
    pub operation: OperationType,
    /// Whether this was a retry of a previous operation.
    pub is_retry: bool,
    /// Correlation ID for tracking related operations.
    pub correlation_id: String,
}

impl RecordedOperation {
    /// Returns true if this operation was a retry.
    #[must_use]
    pub fn is_retry(&self) -> bool {
        self.is_retry
    }

    /// Returns true if the operation succeeded.
    #[must_use]
    pub fn is_success(&self) -> bool {
        match &self.operation {
            OperationType::StorageRead { success, .. }
            | OperationType::StorageWrite { success, .. }
            | OperationType::Cas { success, .. }
            | OperationType::LockAcquire { success, .. }
            | OperationType::LockRelease { success, .. }
            | OperationType::Rpc { success, .. } => *success,
            OperationType::Custom { .. } => true,
        }
    }
}

/// Simulated clock for deterministic time control.
#[derive(Debug)]
pub struct SimulatedClock {
    /// Base time (start of simulation).
    base: DateTime<Utc>,
    /// Elapsed milliseconds since base.
    elapsed_ms: AtomicU64,
}

impl SimulatedClock {
    /// Creates a new simulated clock starting at the given time.
    #[must_use]
    pub fn new(base: DateTime<Utc>) -> Self {
        Self {
            base,
            elapsed_ms: AtomicU64::new(0),
        }
    }

    /// Creates a clock anchored at a deterministic epoch (Unix epoch).
    #[must_use]
    pub fn deterministic() -> Self {
        let base = Utc
            .timestamp_millis_opt(0)
            .single()
            .expect("valid epoch timestamp");
        Self::new(base)
    }

    /// Returns the current simulated time.
    #[must_use]
    pub fn now(&self) -> DateTime<Utc> {
        let elapsed = self.elapsed_ms.load(Ordering::Relaxed);
        self.base + chrono::Duration::milliseconds(elapsed as i64)
    }

    /// Advances the clock by the given duration.
    pub fn advance(&self, duration: Duration) {
        self.elapsed_ms
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    /// Advances the clock to a specific point in time.
    ///
    /// # Panics
    ///
    /// Panics if the target time is before the base or current simulated time.
    pub fn advance_to(&self, target: DateTime<Utc>) {
        if target < self.base {
            panic!(
                "Cannot move clock before base: base={:?}, target={:?}",
                self.base, target
            );
        }
        let target_ms = (target - self.base)
            .num_milliseconds()
            .try_into()
            .expect("non-negative target duration");
        let current = self.elapsed_ms.load(Ordering::Relaxed);
        assert!(
            target_ms >= current,
            "Cannot move clock backwards: current={current}ms, target={target_ms}ms"
        );
        self.elapsed_ms.store(target_ms, Ordering::Relaxed);
    }

    /// Returns elapsed time since simulation start.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        Duration::from_millis(self.elapsed_ms.load(Ordering::Relaxed))
    }
}

impl Default for SimulatedClock {
    fn default() -> Self {
        Self::new(Utc::now())
    }
}

/// Deterministic simulation harness for distributed system testing.
pub struct SimulationHarness {
    /// Seed used for RNG.
    seed: u64,
    /// Deterministic RNG.
    rng: Mutex<StdRng>,
    /// Fault configuration.
    fault_config: Mutex<FaultConfig>,
    /// Simulated clock.
    clock: SimulatedClock,
    /// Recorded operations.
    operations: Mutex<VecDeque<RecordedOperation>>,
    /// Maximum operations to keep (prevents unbounded memory).
    max_operations: usize,
    /// Current correlation ID counter.
    correlation_counter: AtomicU64,
}

impl SimulationHarness {
    /// Creates a new simulation harness with the given seed.
    ///
    /// The seed ensures deterministic behavior - the same seed will
    /// produce the same sequence of failures and timing.
    #[must_use]
    pub fn with_seed(seed: u64) -> Self {
        Self::with_seed_and_clock(seed, SimulatedClock::deterministic())
    }

    /// Creates a new simulation harness with a random seed.
    ///
    /// The seed is printed to stderr for reproducibility.
    #[must_use]
    pub fn random() -> Self {
        let seed = rand::random();
        eprintln!("Simulation seed: {seed}");
        Self::with_seed_and_clock(seed, SimulatedClock::default())
    }

    /// Returns the seed used for this simulation.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Configures fault injection.
    pub fn configure_faults(&self, config: FaultConfig) {
        *self.fault_config.lock().expect("lock poisoned") = config;
    }

    /// Returns a reference to the simulated clock.
    #[must_use]
    pub fn clock(&self) -> &SimulatedClock {
        &self.clock
    }

    /// Generates a new correlation ID for tracking related operations.
    #[must_use]
    pub fn new_correlation_id(&self) -> String {
        let id = self.correlation_counter.fetch_add(1, Ordering::Relaxed);
        format!("sim-{:08x}", id)
    }

    /// Determines if a fault should occur based on the given probability.
    #[must_use]
    pub fn should_fail(&self, probability: f64) -> bool {
        if probability <= 0.0 {
            return false;
        }
        if probability >= 1.0 {
            return true;
        }
        self.rng.lock().expect("lock poisoned").r#gen::<f64>() < probability
    }

    /// Determines if storage read should fail.
    #[must_use]
    pub fn should_fail_storage_read(&self) -> bool {
        let config = self.fault_config.lock().expect("lock poisoned");
        self.should_fail(config.storage_read_failure_rate)
    }

    /// Determines if storage write should fail.
    #[must_use]
    pub fn should_fail_storage_write(&self) -> bool {
        let config = self.fault_config.lock().expect("lock poisoned");
        self.should_fail(config.storage_write_failure_rate)
    }

    /// Determines if CAS should fail due to contention.
    #[must_use]
    pub fn should_fail_cas(&self) -> bool {
        let config = self.fault_config.lock().expect("lock poisoned");
        self.should_fail(config.cas_failure_rate)
    }

    /// Determines if lock acquisition should fail.
    #[must_use]
    pub fn should_fail_lock(&self) -> bool {
        let config = self.fault_config.lock().expect("lock poisoned");
        self.should_fail(config.lock_failure_rate)
    }

    /// Determines if RPC should timeout.
    #[must_use]
    pub fn should_timeout_rpc(&self) -> bool {
        let config = self.fault_config.lock().expect("lock poisoned");
        self.should_fail(config.rpc_timeout_rate)
    }

    /// Returns a simulated network latency duration.
    #[must_use]
    pub fn network_latency(&self) -> Duration {
        let config = self.fault_config.lock().expect("lock poisoned");
        match config.network_latency_range {
            Some((min, max)) => {
                let ms = self.rng.lock().expect("lock poisoned").gen_range(min..=max);
                Duration::from_millis(ms)
            }
            None => Duration::ZERO,
        }
    }

    /// Records an operation.
    pub fn record(&self, operation: OperationType, is_retry: bool, correlation_id: &str) {
        let mut ops = self.operations.lock().expect("lock poisoned");

        // Trim old operations if we're at capacity
        while ops.len() >= self.max_operations {
            ops.pop_front();
        }

        ops.push_back(RecordedOperation {
            timestamp: self.clock.now(),
            operation,
            is_retry,
            correlation_id: correlation_id.to_string(),
        });
    }

    /// Returns all recorded operations.
    #[must_use]
    pub fn operations(&self) -> Vec<RecordedOperation> {
        self.operations
            .lock()
            .expect("lock poisoned")
            .iter()
            .cloned()
            .collect()
    }

    /// Returns operations matching a filter.
    #[must_use]
    pub fn operations_where<F>(&self, filter: F) -> Vec<RecordedOperation>
    where
        F: Fn(&RecordedOperation) -> bool,
    {
        self.operations
            .lock()
            .expect("lock poisoned")
            .iter()
            .filter(|op| filter(op))
            .cloned()
            .collect()
    }

    /// Clears all recorded operations.
    pub fn clear_operations(&self) {
        self.operations.lock().expect("lock poisoned").clear();
    }

    /// Returns count of failed operations.
    #[must_use]
    pub fn failure_count(&self) -> usize {
        self.operations_where(|op| !op.is_success()).len()
    }

    /// Returns count of retry operations.
    #[must_use]
    pub fn retry_count(&self) -> usize {
        self.operations_where(|op| op.is_retry()).len()
    }

    fn with_seed_and_clock(seed: u64, clock: SimulatedClock) -> Self {
        Self {
            seed,
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
            fault_config: Mutex::new(FaultConfig::default()),
            clock,
            operations: Mutex::new(VecDeque::new()),
            max_operations: 10000,
            correlation_counter: AtomicU64::new(0),
        }
    }
}

impl Default for SimulationHarness {
    fn default() -> Self {
        Self::random()
    }
}

/// Builder for creating simulation scenarios.
#[derive(Debug, Default)]
pub struct ScenarioBuilder {
    seed: Option<u64>,
    fault_config: FaultConfig,
    initial_time: Option<DateTime<Utc>>,
}

impl ScenarioBuilder {
    /// Creates a new scenario builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the RNG seed for reproducibility.
    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Sets the fault configuration.
    #[must_use]
    pub fn with_faults(mut self, config: FaultConfig) -> Self {
        self.fault_config = config;
        self
    }

    /// Sets the initial simulated time.
    #[must_use]
    pub fn with_start_time(mut self, time: DateTime<Utc>) -> Self {
        self.initial_time = Some(time);
        self
    }

    /// Builds the simulation harness.
    #[must_use]
    pub fn build(self) -> SimulationHarness {
        let harness = match self.seed {
            Some(seed) => SimulationHarness::with_seed(seed),
            None => SimulationHarness::random(),
        };
        harness.configure_faults(self.fault_config);

        if let Some(time) = self.initial_time {
            harness.clock.advance_to(time);
        }

        harness
    }
}

/// Creates an `Arc` wrapper for easy sharing across async tasks.
#[must_use]
pub fn shared_harness(harness: SimulationHarness) -> Arc<SimulationHarness> {
    Arc::new(harness)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_failures() {
        // Same seed should produce same failure sequence
        let h1 = SimulationHarness::with_seed(42);
        let h2 = SimulationHarness::with_seed(42);

        h1.configure_faults(FaultConfig {
            storage_write_failure_rate: 0.5,
            ..Default::default()
        });
        h2.configure_faults(FaultConfig {
            storage_write_failure_rate: 0.5,
            ..Default::default()
        });

        let results1: Vec<bool> = (0..100).map(|_| h1.should_fail_storage_write()).collect();
        let results2: Vec<bool> = (0..100).map(|_| h2.should_fail_storage_write()).collect();

        assert_eq!(results1, results2);
    }

    #[test]
    fn test_clock_advancement() {
        let harness = SimulationHarness::with_seed(0);
        let start = harness.clock().now();

        harness.clock().advance(Duration::from_secs(10));
        let after = harness.clock().now();

        assert_eq!((after - start).num_seconds(), 10);
    }

    #[test]
    fn test_operation_recording() {
        let harness = SimulationHarness::with_seed(0);
        let corr_id = harness.new_correlation_id();

        harness.record(
            OperationType::StorageWrite {
                path: "test/path".to_string(),
                success: true,
            },
            false,
            &corr_id,
        );

        let ops = harness.operations();
        assert_eq!(ops.len(), 1);
        assert!(ops[0].is_success());
    }

    #[test]
    fn test_scenario_builder() {
        let harness = ScenarioBuilder::new()
            .with_seed(12345)
            .with_faults(FaultConfig::chaos())
            .build();

        assert_eq!(harness.seed(), 12345);
    }

    #[test]
    fn test_seeded_clock_is_deterministic() {
        let h1 = SimulationHarness::with_seed(99);
        let h2 = SimulationHarness::with_seed(99);

        assert_eq!(h1.clock().now(), h2.clock().now());
    }
}

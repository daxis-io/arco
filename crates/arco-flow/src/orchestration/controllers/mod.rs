//! Orchestration controllers for the event-driven architecture.
//!
//! Controllers read state from Parquet projections (base + L0 deltas) and emit
//! events to the ledger. They never read from the ledger directly.
//!
//! ## Controller Types
//!
//! - **`ReadyDispatch`**: Reads READY tasks, emits `DispatchRequested` events
//! - **Dispatcher**: Reads `dispatch_outbox`, creates Cloud Tasks, emits `DispatchEnqueued`
//! - **Timer**: Reads timers, creates Cloud Tasks timers, emits `TimerEnqueued`
//! - **Anti-Entropy**: Scans for stuck work and creates repair actions
//!
//! ## Design Principles
//!
//! 1. **Stateless**: Controllers can be scaled horizontally
//! 2. **Idempotent**: Same input produces same output
//! 3. **Parquet-only reads**: Never read from the ledger
//! 4. **Event emission**: Write intent/acknowledgement facts to ledger
//!
//! ## Watermark Freshness Guard
//!
//! All controllers check watermark freshness before making decisions.
//! If compaction is lagging, controllers skip actions to avoid making
//! incorrect decisions based on stale state. See ADR-022 for details.

pub mod anti_entropy;
pub mod backfill;
pub mod dispatch;
pub mod dispatcher;
pub mod partition_status;
pub mod ready_dispatch;
pub mod run_bridge;
pub mod schedule;
pub mod sensor;
pub mod sensor_evaluator;
pub mod timer;
pub mod timer_handlers;

pub use anti_entropy::*;
pub use backfill::*;
pub use dispatch::*;
pub use dispatcher::*;
pub use partition_status::*;
pub use ready_dispatch::*;
pub use run_bridge::*;
pub use schedule::*;
pub use sensor::*;
pub use sensor_evaluator::*;
pub use timer::*;
pub use timer_handlers::*;

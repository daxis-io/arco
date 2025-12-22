//! Event-driven orchestration for Servo.
//!
//! This module implements the orchestration domain following the unified platform
//! pattern (ADR-020). It provides:
//!
//! - **Event types**: `OrchestrationEvent` envelope with run, task, and timer events
//! - **Schemas**: Parquet schemas for runs, tasks, dependencies, timers, and outbox
//! - **Compaction**: Micro-compactor for near-real-time Parquet visibility
//! - **Controllers**: Stateless reconciliation from Parquet projections
//!
//! ## Architecture
//!
//! Controllers reconcile from Parquet projections (base snapshot + L0 deltas),
//! never from the ledger. This enables:
//!
//! - Horizontal scaling of controllers
//! - Read-only access to state (no coordination needed)
//! - Replayable decision-making
//!
//! ## Event Flow
//!
//! ```text
//! Worker/Controller → Event → Ledger → Compactor → Parquet → Controller
//!                       ↓
//!                  Pub/Sub notification
//!                       ↓
//!                  Micro-compactor
//! ```
//!
//! ## Key Design Decisions
//!
//! - **ADR-020**: Orchestration as unified domain
//! - **ADR-021**: Cloud Tasks naming convention (dual-identifier pattern)
//! - **ADR-022**: Per-edge dependency satisfaction for duplicate-safe readiness

pub mod callbacks;
pub mod compactor;
pub mod controllers;
pub mod events;
pub mod ids;
pub mod ledger;
pub mod run_key;

pub use callbacks::{
    CallbackError, CallbackResult, ErrorCategory, HeartbeatRequest, HeartbeatResponse,
    TaskCompletedRequest, TaskCompletedResponse, TaskError, TaskMetrics, TaskOutput,
    TaskStartedRequest, TaskStartedResponse, WorkerOutcome,
};
pub use ledger::{LedgerWriter, OrchestrationLedgerWriter};
pub use run_key::{
    FingerprintPolicy, ReservationResult, RunKeyReservation, get_reservation,
    reserve_run_key, reservation_path,
};

//! Orchestration controllers for the event-driven architecture.
//!
//! Controllers read state from Parquet projections (base + L0 deltas) and emit
//! events to the ledger. They never read from the ledger directly.
//!
//! ## Controller Types
//!
//! - **Dispatcher**: Reads dispatch_outbox, creates Cloud Tasks, emits DispatchEnqueued
//! - **Timer**: Reads timers, creates Cloud Tasks timers, emits TimerEnqueued
//! - **Anti-Entropy**: Scans for stuck work and creates repair actions
//!
//! ## Design Principles
//!
//! 1. **Stateless**: Controllers can be scaled horizontally
//! 2. **Idempotent**: Same input produces same output
//! 3. **Parquet-only reads**: Never read from the ledger
//! 4. **Event emission**: Write intent/acknowledgement facts to ledger

pub mod dispatch;

pub use dispatch::*;

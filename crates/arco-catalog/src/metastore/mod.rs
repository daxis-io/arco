//! Native metastore domain kernel.
//!
//! This module owns the route-independent substrate for future catalog product
//! domains: event envelopes, deterministic replay, safe projection registration,
//! and pointer-publication planning.

pub mod envelope;
pub mod events;
pub mod ledger;
pub mod projections;
pub mod publish;
pub mod replay;

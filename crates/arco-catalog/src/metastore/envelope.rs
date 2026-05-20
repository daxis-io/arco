//! Metastore envelope helpers.

use serde::{Deserialize, Serialize};

use crate::error::Result;

use super::events::MetastoreEvent;

/// Stored metastore event plus deterministic hash evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetastoreEnvelope {
    /// Event payload.
    pub event: MetastoreEvent,
    /// Deterministic `sha256:` hash over the event.
    pub event_hash: String,
}

impl MetastoreEnvelope {
    /// Builds an envelope for an event.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical event hashing fails.
    pub fn new(event: MetastoreEvent) -> Result<Self> {
        let event_hash = event.deterministic_hash()?;
        Ok(Self { event, event_hash })
    }

    /// Validates that the stored event hash matches the payload.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical event hashing fails.
    pub fn validate_hash(&self) -> Result<bool> {
        Ok(self.event.deterministic_hash()? == self.event_hash)
    }
}

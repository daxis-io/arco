//! Pointer-publication planning for metastore projections.

use super::projections::ProjectionSet;

/// Outcome of the pointer compare-and-swap step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointerPublishResult {
    /// Pointer was published.
    Published,
    /// Pointer CAS failed and readers must remain on the previous set.
    CasFailed,
}

/// Projection set visible through a successfully published pointer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedProjectionSet {
    /// Immutable manifest identifier.
    pub manifest_id: String,
    /// Ledger watermark event ID.
    pub ledger_watermark: String,
    /// Projection files in this set.
    pub projections: ProjectionSet,
}

impl PublishedProjectionSet {
    /// Creates an empty visible projection set.
    #[must_use]
    pub fn empty(manifest_id: impl Into<String>, ledger_watermark: impl Into<String>) -> Self {
        Self {
            manifest_id: manifest_id.into(),
            ledger_watermark: ledger_watermark.into(),
            projections: ProjectionSet { files: Vec::new() },
        }
    }

    /// Creates a visible projection set from built projections.
    #[must_use]
    pub fn new(
        manifest_id: impl Into<String>,
        ledger_watermark: impl Into<String>,
        projections: ProjectionSet,
    ) -> Self {
        Self {
            manifest_id: manifest_id.into(),
            ledger_watermark: ledger_watermark.into(),
            projections,
        }
    }
}

/// Selects the reader-visible projection set after pointer publication.
///
/// This function models the all-or-nothing publication boundary: failed pointer
/// movement leaves readers on the previous complete set.
#[must_use]
pub fn complete_pointer_publication(
    previous: PublishedProjectionSet,
    candidate: PublishedProjectionSet,
    result: PointerPublishResult,
) -> PublishedProjectionSet {
    match result {
        PointerPublishResult::Published => candidate,
        PointerPublishResult::CasFailed => previous,
    }
}

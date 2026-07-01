//! Compatibility re-exports for planner-owned selection semantics.

pub use crate::planning::selection::{
    AssetGraph, SelectionOptions, build_task_defs_for_selection, canonicalize_asset_key,
    compute_selection_fingerprint,
};

//! Canonical path builders for flow/orchestration storage objects.

/// Typed flow/orchestration path API.
pub struct FlowPaths;

impl FlowPaths {
    /// Prefix for orchestration ledger events.
    pub const ORCHESTRATION_LEDGER_PREFIX: &str = "ledger/orchestration";
    /// Prefix for flow execution ledgers.
    pub const FLOW_LEDGER_PREFIX: &str = "ledger/flow";
    /// Prefix for orchestration state objects.
    pub const ORCHESTRATION_STATE_PREFIX: &str = "state/orchestration";
    /// Canonical orchestration manifest path.
    pub const ORCHESTRATION_MANIFEST_PATH: &str = "state/orchestration/manifest.json";

    /// Canonical path for orchestration event objects.
    #[must_use]
    pub fn orchestration_event_path(date: &str, event_id: &str) -> String {
        format!(
            "{}/{date}/{event_id}.json",
            Self::ORCHESTRATION_LEDGER_PREFIX
        )
    }

    /// Canonical path for flow event objects.
    #[must_use]
    pub fn flow_event_path(domain: &str, date: &str, event_id: &str) -> String {
        format!(
            "{}/{domain}/{date}/{event_id}.json",
            Self::FLOW_LEDGER_PREFIX
        )
    }

    /// Canonical orchestration manifest path.
    #[must_use]
    pub const fn orchestration_manifest_path() -> &'static str {
        Self::ORCHESTRATION_MANIFEST_PATH
    }

    /// Canonical path for orchestration L0 directory.
    #[must_use]
    pub fn orchestration_l0_dir(delta_id: &str) -> String {
        format!("{}/l0/{delta_id}", Self::ORCHESTRATION_STATE_PREFIX)
    }
}

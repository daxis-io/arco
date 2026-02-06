pub const ORCHESTRATION_LEDGER_PREFIX: &str = "ledger/orchestration";
pub const FLOW_LEDGER_PREFIX: &str = "ledger/flow";
pub const ORCHESTRATION_STATE_PREFIX: &str = "state/orchestration";
pub const ORCHESTRATION_MANIFEST_PATH: &str = "state/orchestration/manifest.json";

pub fn orchestration_event_path(date: &str, event_id: &str) -> String {
    format!("{ORCHESTRATION_LEDGER_PREFIX}/{date}/{event_id}.json")
}

pub fn flow_event_path(domain: &str, date: &str, event_id: &str) -> String {
    format!("{FLOW_LEDGER_PREFIX}/{domain}/{date}/{event_id}.json")
}

pub fn orchestration_manifest_path() -> &'static str {
    ORCHESTRATION_MANIFEST_PATH
}

pub fn orchestration_l0_dir(delta_id: &str) -> String {
    format!("{ORCHESTRATION_STATE_PREFIX}/l0/{delta_id}")
}

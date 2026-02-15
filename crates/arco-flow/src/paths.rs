pub const ORCHESTRATION_LEDGER_PREFIX: &str = "ledger/orchestration";
pub const FLOW_LEDGER_PREFIX: &str = "ledger/flow";
pub const ORCHESTRATION_STATE_PREFIX: &str = "state/orchestration";
pub const ORCHESTRATION_MANIFEST_PATH: &str = "state/orchestration/manifest.json";
pub const ORCHESTRATION_MANIFEST_POINTER_PATH: &str = "state/orchestration/manifest.pointer.json";
pub const ORCHESTRATION_MANIFEST_SNAPSHOT_PREFIX: &str = "state/orchestration/manifests";
pub const ORCHESTRATION_COMPACTION_LOCK_PATH: &str = "locks/orchestration.compaction.lock.json";

pub fn orchestration_event_path(date: &str, event_id: &str) -> String {
    format!("{ORCHESTRATION_LEDGER_PREFIX}/{date}/{event_id}.json")
}

pub fn flow_event_path(domain: &str, date: &str, event_id: &str) -> String {
    format!("{FLOW_LEDGER_PREFIX}/{domain}/{date}/{event_id}.json")
}

pub fn orchestration_manifest_path() -> &'static str {
    ORCHESTRATION_MANIFEST_PATH
}

pub fn orchestration_manifest_pointer_path() -> &'static str {
    ORCHESTRATION_MANIFEST_POINTER_PATH
}

pub fn orchestration_manifest_snapshot_path(manifest_id: &str) -> String {
    format!("{ORCHESTRATION_MANIFEST_SNAPSHOT_PREFIX}/{manifest_id}.json")
}

pub fn orchestration_compaction_lock_path() -> &'static str {
    ORCHESTRATION_COMPACTION_LOCK_PATH
}

pub fn orchestration_l0_dir(delta_id: &str) -> String {
    format!("{ORCHESTRATION_STATE_PREFIX}/l0/{delta_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orchestration_manifest_pointer_paths_are_stable() {
        assert_eq!(
            orchestration_manifest_pointer_path(),
            "state/orchestration/manifest.pointer.json"
        );
        assert_eq!(
            orchestration_manifest_snapshot_path("00000000000000000001"),
            "state/orchestration/manifests/00000000000000000001.json"
        );
        assert_eq!(
            orchestration_compaction_lock_path(),
            "locks/orchestration.compaction.lock.json"
        );
    }
}

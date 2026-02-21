use arco_core::FlowPaths;

const ORCHESTRATION_MANIFEST_POINTER_PATH: &str = "state/orchestration/manifest.pointer.json";
const ORCHESTRATION_MANIFEST_SNAPSHOT_PREFIX: &str = "state/orchestration/manifests";
const ORCHESTRATION_COMPACTION_LOCK_PATH: &str = "locks/orchestration.compaction.lock.json";

pub fn orchestration_event_path(date: &str, event_id: &str) -> String {
    FlowPaths::orchestration_event_path(date, event_id)
}

/// Returns the canonical storage path for a flow event payload.
pub fn flow_event_path(domain: &str, date: &str, event_id: &str) -> String {
    FlowPaths::flow_event_path(domain, date, event_id)
}

/// Returns the canonical orchestration manifest path.
pub fn orchestration_manifest_path() -> &'static str {
    FlowPaths::orchestration_manifest_path()
}

/// Returns the canonical orchestration manifest pointer path.
pub fn orchestration_manifest_pointer_path() -> &'static str {
    ORCHESTRATION_MANIFEST_POINTER_PATH
}

/// Returns the canonical orchestration manifest snapshot path.
pub fn orchestration_manifest_snapshot_path(manifest_id: &str) -> String {
    format!("{ORCHESTRATION_MANIFEST_SNAPSHOT_PREFIX}/{manifest_id}.json")
}

/// Returns the canonical orchestration compaction lock path.
#[must_use]
pub fn orchestration_compaction_lock_path() -> &'static str {
    ORCHESTRATION_COMPACTION_LOCK_PATH
}

/// Returns the canonical orchestration L0 directory for a delta id.
pub fn orchestration_l0_dir(delta_id: &str) -> String {
    FlowPaths::orchestration_l0_dir(delta_id)
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
